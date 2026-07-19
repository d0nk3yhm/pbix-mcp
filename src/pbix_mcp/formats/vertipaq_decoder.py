"""
VertiPaq Decoder -- read table data from ABF column files (IDF, IDFMETA, DICT).

This is the inverse of vertipaq_encoder.py.  It decodes the binary column-store
format used by Power BI PBIX files to reconstruct table rows.

Public API
----------
decode_dictionary(dict_bytes)  -> list
    Decode a dictionary blob and return the list of unique values.

decode_idf(idf_bytes, bit_width, row_count) -> list[int]
    Decode an IDF blob and return per-row dictionary indices.

decode_idfmeta(meta_bytes) -> dict
    Parse an IDFMETA blob and return segment metadata (row_count, bit_width, has_nulls, etc.).

read_table_from_abf(abf_bytes, table_name, metadata_db_bytes) -> dict
    High-level: read a full table from ABF, returns {columns: [...], rows: [...]}.
"""

from __future__ import annotations

import math
import os
import sqlite3
import struct
import tempfile

from pbix_mcp.errors import InvalidPBIXError
from pbix_mcp.formats.vertipaq_encoder import (
    AMO_BOOLEAN,
    AMO_DATETIME,
    AMO_DECIMAL,
    AMO_FLOAT64,
    AMO_INT64,
    AMO_STRING,
    DICT_TYPE_REAL,
    DICT_TYPE_STRING,
    STRING_STORE_BEGIN,
    STRING_STORE_END,
    _align_bit_width,
)

# AMO type code to friendly name
_AMO_TO_TYPE_NAME = {
    AMO_STRING: "String",
    AMO_INT64: "Int64",
    AMO_FLOAT64: "Float64",
    AMO_DATETIME: "DateTime",
    AMO_DECIMAL: "Decimal",
    AMO_BOOLEAN: "Boolean",
}


# ---------------------------------------------------------------------------
# IDFMETA decoder
# ---------------------------------------------------------------------------

# Tag bytes (same as encoder)
_TAG_CP_OPEN = b"\x3C\x31\x3A\x43\x50\x00"
_TAG_CS_OPEN = b"\x3C\x31\x3A\x43\x53\x00"
_TAG_SS_OPEN = b"\x3C\x31\x3A\x53\x53\x00"
_TAG_SS_CLOSE = b"\x53\x53\x3A\x31\x3E\x00"
_TAG_CS_CLOSE = b"\x43\x53\x3A\x31\x3E\x00"
_TAG_CP_CLOSE = b"\x43\x50\x3A\x31\x3E\x00"


def decode_idfmeta(meta_bytes: bytes) -> dict:
    """
    Parse an IDFMETA blob and extract key segment metadata.

    Returns a dict with:
        row_count: int
        min_data_id: int
        max_data_id: int
        has_nulls: bool
        bit_width: int  (aligned, from u32_b field)
        is_row_number: bool
        rle_runs: int
    """
    buf = meta_bytes
    pos = 0

    def _find(tag: bytes, start: int = 0) -> int:
        idx = buf.find(tag, start)
        if idx < 0:
            raise ValueError(f"Tag {tag!r} not found in IDFMETA at offset {start}")
        return idx

    # Skip CP open tag + version
    pos = _find(_TAG_CP_OPEN) + len(_TAG_CP_OPEN)
    # version_one: uint64
    pos += 8

    # CS0 open
    pos = _find(_TAG_CS_OPEN, pos) + len(_TAG_CS_OPEN)
    # records: uint64
    row_count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8
    # one: uint64 (0 for RowNumber, 1 for data)
    one_field = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8
    is_row_number = (one_field == 0)
    # u32_a: uint32
    u32_a = struct.unpack_from("<I", buf, pos)[0]
    pos += 4
    # u32_b: uint32 -- encodes bit width as 0xABA36 + N
    u32_b = struct.unpack_from("<I", buf, pos)[0]
    pos += 4

    # Extract bit width from u32_b
    # For RowNumber columns, u32_b = 0xABA5B (special marker)
    # For data columns, u32_b = 0xABA36 + aligned_bit_width
    _BIT_WIDTH_BASE = 0xABA36
    if u32_b >= _BIT_WIDTH_BASE and u32_b < _BIT_WIDTH_BASE + 64:
        bit_width = u32_b - _BIT_WIDTH_BASE
    else:
        bit_width = 0  # RowNumber or unknown

    # Skip bookmark_bits(u64), storage_alloc_size(u64), storage_used_size(u64),
    # segment_needs_resizing(u8), compression_info(u32)
    pos += 8 + 8 + 8 + 1 + 4

    # SS block
    pos = _find(_TAG_SS_OPEN, pos) + len(_TAG_SS_OPEN)
    # distinct_states: uint64
    pos += 8
    # min_data_id: uint32
    min_data_id = struct.unpack_from("<I", buf, pos)[0]
    pos += 4
    # max_data_id: uint32
    max_data_id = struct.unpack_from("<I", buf, pos)[0]
    pos += 4
    # original_min: uint32
    pos += 4
    # r_l_e_sort_order: int64
    pos += 8
    # row_count (in SS): uint64
    ss_row_count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8
    # has_nulls: uint8
    has_nulls = struct.unpack_from("<B", buf, pos)[0] != 0
    pos += 1
    # r_l_e_runs: uint64
    rle_runs = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8

    return {
        "row_count": row_count,
        "min_data_id": min_data_id,
        "max_data_id": max_data_id,
        "has_nulls": has_nulls,
        "bit_width": bit_width,
        "is_row_number": is_row_number,
        "rle_runs": rle_runs,
        "u32_a": u32_a,
        "u32_b": u32_b,
    }


def decode_idfmeta_segments(meta_bytes: bytes) -> list[dict]:
    """Parse the per-segment metadata of an IDFMETA.

    A column split across several VertiPaq segments carries one CS/SS block per
    segment inside a single CP; each segment has its OWN records / bit width /
    min_data_id (the value domain shifts between segments). Returns a list of
    ``{records, bit_width, min_data_id, has_nulls}`` in storage order (length 1
    for the common single-segment column). Used to decode multi-segment columns
    (import tables beyond ~1,048,576 rows) instead of reading only segment 0.
    """
    buf = meta_bytes
    _BW_BASE = 0xABA36
    segs: list[dict] = []
    scan = 0
    while True:
        ss = buf.find(_TAG_SS_OPEN, scan)
        if ss < 0:
            break
        # The CS0 that opened this segment is the CS_OPEN immediately before SS
        # (CS1 comes after SS_CLOSE). Its bit width is the 4th field.
        cs = buf.rfind(_TAG_CS_OPEN, 0, ss)
        bit_width = 0
        if cs >= 0:
            cp = cs + len(_TAG_CS_OPEN) + 8 + 8 + 4  # records(u64), one(u64), u32_a(u32)
            if cp + 4 <= len(buf):
                u32_b = struct.unpack_from("<I", buf, cp)[0]
                if _BW_BASE <= u32_b < _BW_BASE + 64:
                    bit_width = u32_b - _BW_BASE
        sp = ss + len(_TAG_SS_OPEN) + 8  # skip distinct_states(u64)
        min_data_id = struct.unpack_from("<I", buf, sp)[0]
        sp += 4 + 4 + 4 + 8  # min(4) already read -> skip max(4), orig(4), rle_sort(8)
        records = struct.unpack_from("<Q", buf, sp)[0]
        sp += 8
        has_nulls = struct.unpack_from("<B", buf, sp)[0] != 0
        segs.append({
            "records": records,
            "bit_width": bit_width,
            "min_data_id": min_data_id,
            "has_nulls": has_nulls,
        })
        scan = ss + len(_TAG_SS_OPEN)
    return segs


# ---------------------------------------------------------------------------
# Dictionary decoder
# ---------------------------------------------------------------------------

def decode_dictionary(dict_bytes: bytes) -> tuple[int, list]:
    """
    Decode a VertiPaq dictionary blob.

    Returns (dict_type, values) where:
        dict_type: DICT_TYPE_LONG (0), DICT_TYPE_REAL (1), or DICT_TYPE_STRING (2)
        values: list of decoded values
    """
    buf = dict_bytes
    pos = 0

    # dictionary_type: int32
    dict_type = struct.unpack_from("<i", buf, pos)[0]
    pos += 4

    # hash_information: 6 x int32
    pos += 24  # skip 6 * 4 bytes

    if dict_type == DICT_TYPE_STRING:
        return dict_type, _decode_string_dictionary(buf, pos)
    else:
        return dict_type, _decode_numeric_dictionary(buf, pos, dict_type)


# Compressed string-store character-set identifiers (MS-XLDM §2.7.4.1.4):
#   0x000aba91 single charset — only the low byte of each UTF-16 char is
#               Huffman-encoded; CharacterSetUsed is the common high byte.
#   0x000aba92 general        — every byte of the UTF-16LE stream is encoded.
_HUFFMAN_SINGLE = 0x000ABA91
_HUFFMAN_GENERAL = 0x000ABA92


def _decode_compressed_page(page: dict) -> list[str]:
    """Decode one Huffman-compressed dictionary page via the xmhuffman kernel.

    ``page`` carries the parsed compressed-page fields plus ``offsets`` (the
    per-string start bit offsets from the record-handle vector, ascending).
    Canonical-Huffman format is Microsoft's MS-XLDM §2.7.4. The heavy lifting
    (bit walk + charset reinsertion) is delegated to the MIT ``xmhuffman``
    primitive, mirroring how the ZIP layer delegates XPress9 to
    ``xpress9-python``.
    """
    try:
        import xmhuffman
    except ImportError as e:  # pragma: no cover - dependency is declared
        raise ImportError(
            "Reading Huffman-compressed string dictionaries requires the "
            "'xmhuffman' package (pip install xmhuffman)."
        ) from e

    buf = page["comp_buf"]
    enc = page["encode_array"]
    offsets = page["offsets"]
    total_bits = page["total_bits"]
    if page["charset_id"] == _HUFFMAN_GENERAL:
        raw = xmhuffman.decode_page(buf, enc, offsets, total_bits, swap=True)
        # trailing odd byte (if any) is padding; drop it before UTF-16LE decode
        return [b[: len(b) & ~1].decode("utf-16-le", errors="ignore") for b in raw]
    cb = page["charset_used"]
    if cb == 0:
        raw = xmhuffman.decode_page(buf, enc, offsets, total_bits, swap=True)
        return [b.decode("latin-1") for b in raw]
    raw = xmhuffman.decode_page(
        buf, enc, offsets, total_bits, swap=True,
        charset_mode="single", charset_byte=cb,
    )
    return [b.decode("utf-16-le") for b in raw]


def _decode_string_dictionary(buf: bytes, pos: int) -> list[str]:
    """Decode a string dictionary from position pos onward.

    Handles both uncompressed and Huffman-compressed pages. The on-disk
    layout is: PageLayout, then N DictionaryPage blocks, then ONE shared
    DictionaryRecordHandlesVector (record handles carry `(offset, page_id)`
    in data-id order — char offset for uncompressed pages, start-bit offset
    for compressed pages).
    """
    from collections import defaultdict

    # store_string_count: int64
    struct.unpack_from("<q", buf, pos)[0]
    pos += 8
    # f_store_compressed: int8
    pos += 1
    # store_longest_string: int64
    pos += 8
    # store_page_count: int64
    page_count = struct.unpack_from("<q", buf, pos)[0]
    pos += 8

    pages: list[dict] = []
    for _ in range(page_count):
        # DictionaryPage header
        pos += 8   # page_mask: uint64
        pos += 1   # page_contains_nulls: uint8
        pos += 8   # page_start_index: uint64
        pos += 8   # page_string_count: uint64
        page_compressed = struct.unpack_from("<B", buf, pos)[0]
        pos += 1

        mark = buf[pos:pos + 4]
        if mark != STRING_STORE_BEGIN:
            raise ValueError(f"Expected STRING_STORE_BEGIN at {pos}, got {mark!r}")
        pos += 4

        if page_compressed == 0:
            pos += 8  # remaining_store_available: uint64
            pos += 8  # buffer_used_characters: uint64
            alloc_size = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
            char_buf = buf[pos:pos + alloc_size]
            pos += alloc_size
            pages.append({"compressed": False, "char_buf": char_buf})
        else:
            total_bits = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
            charset_id = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
            len_comp_buf = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
            charset_used = 0
            if charset_id == _HUFFMAN_SINGLE:
                charset_used = struct.unpack_from("<B", buf, pos)[0]
                pos += 1
            pos += 4  # ui_decode_bits: uint32 (decoder uses a flat table)
            encode_array = buf[pos:pos + 128]
            pos += 128
            pos += 8  # ui64_buffer_size: uint64
            comp_buf = buf[pos:pos + len_comp_buf]
            pos += len_comp_buf
            pages.append({
                "compressed": True, "total_bits": total_bits,
                "charset_id": charset_id, "charset_used": charset_used,
                "encode_array": encode_array, "comp_buf": comp_buf,
            })

        end_mark = buf[pos:pos + 4]
        if end_mark != STRING_STORE_END:
            raise ValueError(f"Expected STRING_STORE_END at {pos}, got {end_mark!r}")
        pos += 4

    # ONE shared DictionaryRecordHandlesVector after all pages
    elem_count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8
    pos += 4  # element_size: uint32 (== 8)
    offsets_by_page: dict[int, list[int]] = defaultdict(list)
    for _ in range(elem_count):
        offset = struct.unpack_from("<I", buf, pos)[0]
        pos += 4
        page_id = struct.unpack_from("<I", buf, pos)[0]
        pos += 4
        offsets_by_page[page_id].append(offset)

    strings: list[str] = []
    for page_id, page in enumerate(pages):
        offsets = offsets_by_page.get(page_id, [])
        if page["compressed"]:
            page["offsets"] = offsets  # start bits, already in data-id (ascending) order
            strings.extend(_decode_compressed_page(page))
        else:
            char_buf = page["char_buf"]
            for char_offset in offsets:
                byte_offset = char_offset * 2
                s_end = byte_offset
                while s_end + 1 < len(char_buf):
                    if struct.unpack_from("<H", char_buf, s_end)[0] == 0:
                        break
                    s_end += 2
                strings.append(
                    char_buf[byte_offset:s_end].decode("utf-16-le", errors="replace")
                )

    return strings


def _decode_numeric_dictionary(buf: bytes, pos: int, dict_type: int) -> list:
    """Decode a numeric (int64 or float64) dictionary."""
    # VectorOfVectors header
    # element_count: uint64
    count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8
    # element_size: uint32
    elem_size = struct.unpack_from("<I", buf, pos)[0]
    pos += 4

    values = []
    for _ in range(count):
        if dict_type == DICT_TYPE_REAL:
            val = struct.unpack_from("<d", buf, pos)[0]
            pos += 8
        elif elem_size == 4:
            val = struct.unpack_from("<i", buf, pos)[0]
            pos += 4
        else:
            val = struct.unpack_from("<q", buf, pos)[0]
            pos += 8
        values.append(val)

    return values


# ---------------------------------------------------------------------------
# IDF decoder (RLE + bit-packed hybrid)
# ---------------------------------------------------------------------------

def decode_idf(
    idf_bytes: bytes, bit_width: int, row_count: int, rle_base: int = 0,
    segments: "list[tuple[int, int]] | None" = None,
) -> list[int]:
    """
    Decode an IDF blob and return per-row dictionary indices.

    The IDF uses an RLE + bit-packed hybrid encoding:
      primary_segment_size: uint64  (always 16 = 128 bytes / 8)
      primary_segment[16]: array of (data_value: u32, repeat_value: u32)
        - data_value == 0xFFFFFFFF means "read repeat_value values from bit-packed sub_segment"
        - Otherwise, repeat data_value exactly repeat_value times
      sub_segment_size: uint64  (count of uint64 words)
      sub_segment[]: array of uint64 (bit-packed values)

    ``rle_base``: value subtracted from every RLE ``data_value``. In Power BI
    Desktop segments the bit-packed sub-segment stores indices RELATIVE to the
    segment minimum (``data_id - min_data_id``), but an RLE run stores the
    ABSOLUTE ``data_id``. Passing ``rle_base = min_data_id - null_offset``
    re-bases RLE runs onto the same relative scale as the bit-packed values, so
    a column that mixes both (or is pure-RLE, e.g. a single-value column)
    decodes consistently. The default 0 preserves the legacy behaviour for
    callers that don't need the adjustment (our own encoder never emits RLE).
    """
    buf = idf_bytes
    indices: list[int] = []
    pos = 0
    seg_idx = 0
    # An .idf concatenates one block per VertiPaq segment (Power BI splits an
    # import table into ~1,048,576-row segments). Walk EVERY segment and
    # concatenate — stopping after the first one silently truncated tables past
    # one segment. Each segment can have its own bit width / re-base scale
    # (different value domain), so `segments` carries per-segment (bit_width,
    # rle_base); when it is None the single bit_width/rle_base applies to all.
    while pos < len(buf):
        if segments is not None:
            if seg_idx >= len(segments):
                break
            seg_bw, seg_rle_base, seg_bp_add = segments[seg_idx]
        else:
            seg_bw, seg_rle_base, seg_bp_add = bit_width, rle_base, 0
        seg_indices, pos = _decode_idf_segment_at(
            buf, pos, seg_bw, seg_rle_base, seg_bp_add
        )
        indices.extend(seg_indices)
        seg_idx += 1
        if row_count and len(indices) >= row_count and segments is None:
            # Single-param callers pass one segment's row_count; don't over-read
            # trailing segments they didn't ask about.
            break

    # Truncate to exact row count (sub-segment may have padding)
    return indices[:row_count] if row_count else indices


def _decode_idf_segment_at(
    buf: bytes, pos: int, bit_width: int, rle_base: int, bitpacked_add: int = 0
):
    """Decode ONE segment of an .idf starting at ``pos``.

    Returns ``(indices, next_pos)`` — the segment's per-row indices and the byte
    offset immediately after this segment (start of the next segment, if any).

    ``bitpacked_add`` shifts this segment's bit-packed values onto a shared
    scale. Across the segments of one column the dictionary is GLOBAL but each
    segment stores bit-packed values relative to its OWN ``min_data_id``; adding
    ``segment.min_data_id - global_min`` lines every segment up with the shared
    dictionary. It is 0 for single-segment columns (the common case).
    """
    # primary_segment_size: uint64 (should be 16)
    ps_count = struct.unpack_from("<Q", buf, pos)[0]

    # Read primary segment entries
    primary_entries = []
    p = pos + 8
    for _ in range(ps_count):
        dv = struct.unpack_from("<I", buf, p)[0]
        rv = struct.unpack_from("<I", buf, p + 4)[0]
        p += 8
        if dv == 0 and rv == 0:
            continue  # zero entry -- padding
        primary_entries.append((dv, rv))

    # Advance past full primary segment (skip any remaining padding)
    p = pos + 8 + ps_count * 8

    # sub_segment_size: uint64 (word count)
    ss_word_count = struct.unpack_from("<Q", buf, p)[0]
    p += 8

    # Read sub-segment uint64 words
    sub_words = []
    for _ in range(ss_word_count):
        sub_words.append(struct.unpack_from("<Q", buf, p)[0])
        p += 8

    # Pre-decode the ENTIRE bit-packed sub-segment into one flat array. The
    # values for every bit-packed primary entry are stored contiguously here
    # (values_per_word per u64 word, low bits first), so a bit-packed entry
    # consumes the next `rv` of them by value-offset — NOT by whole words. The
    # earlier word-at-a-time approach discarded the unused tail of the last
    # word of each group, which misaligned every later group in a mixed segment.
    values_per_word = 64 // bit_width if bit_width > 0 else 0
    mask = (1 << bit_width) - 1 if bit_width > 0 else 0
    bitpacked_values: list[int] = []
    if values_per_word:
        for word in sub_words:
            for j in range(values_per_word):
                bitpacked_values.append(((word >> (j * bit_width)) & mask) + bitpacked_add)

    # Decode primary entries. A primary entry is a bit-packed marker when
    # `data_value + bit_packed_offset == 0xFFFFFFFF` (Power BI stores each
    # successive marker as 0xFFFFFFFF minus the number of bit-packed values
    # already consumed), otherwise it is an RLE run of `data_value`.
    indices = []
    bit_packed_offset = 0
    for dv, rv in primary_entries:
        if (dv + bit_packed_offset) & 0xFFFFFFFF == 0xFFFFFFFF:
            indices.extend(bitpacked_values[bit_packed_offset:bit_packed_offset + rv])
            bit_packed_offset += rv
        else:
            # RLE: repeat dv exactly rv times. Re-base absolute data_ids onto
            # the same relative scale as the bit-packed sub-segment values.
            indices.extend([dv - rle_base] * rv)

    return indices, p


# ---------------------------------------------------------------------------
# Value reconstruction helpers
# ---------------------------------------------------------------------------

def _oa_date_to_python(oa_days: float):
    """Convert OLE Automation date (days since 1899-12-30) to Python datetime."""
    import datetime as _dt
    epoch = _dt.datetime(1899, 12, 30)
    try:
        return epoch + _dt.timedelta(days=oa_days)
    except (OverflowError, ValueError):
        return oa_days  # Return raw value if conversion fails


def _reconstruct_value_encoded(n, data_type: str):
    """Convert a VALUE-encoded numeric ``n`` to a Python-friendly value.

    Value encoding (DictionaryStorage.Type=2, no external dictionary) stores the
    scaled integer directly in the segment; the real value is
    ``n = (data_id + BaseId) / Magnitude`` (already applied by the caller). This
    then maps ``n`` to the column's declared type. Verified byte-for-byte against
    pbixray over the whole corpus (Int64 keys/counts, DateTime) — e.g. an OLE
    serial 45748 -> 2025-04-01. Magnitude carries the decimal/currency scale, so
    ``n`` is the final numeric value for Float64/Decimal (do NOT divide again).
    """
    if data_type == "DateTime":
        return _oa_date_to_python(n)
    if data_type == "Boolean":
        return bool(int(round(n)))
    if data_type == "Int64":
        return int(round(n))
    if data_type in ("Float64", "Decimal"):
        return float(n)
    return n


def _reconstruct_value(dict_value, data_type: str, is_null: bool):
    """Convert a dictionary value back to a Python-friendly representation."""
    if is_null:
        return None
    if data_type == "String":
        return dict_value
    elif data_type == "Boolean":
        return bool(dict_value)
    elif data_type == "DateTime":
        return _oa_date_to_python(dict_value)
    elif data_type == "Decimal":
        return dict_value / 10000.0
    elif data_type == "Float64":
        return float(dict_value)
    elif data_type == "Int64":
        return int(dict_value)
    else:
        return dict_value


# ---------------------------------------------------------------------------
# High-level table reader
# ---------------------------------------------------------------------------

def read_table_from_abf(
    abf_bytes: bytes,
    table_name: str,
    metadata_db_bytes: bytes,
) -> dict:
    """
    Read a full table from an ABF blob.

    Parameters
    ----------
    abf_bytes : bytes
        Decompressed ABF blob.
    table_name : str
        Name of the table to read.
    metadata_db_bytes : bytes
        Raw bytes of metadata.sqlitedb extracted from ABF.

    Returns
    -------
    dict with keys:
        columns: list[str]  -- column names
        rows: list[list]    -- row data
    """
    from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file

    # Get column info from metadata
    tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    try:
        tmp_db.write(metadata_db_bytes)
        tmp_db.close()
        conn = sqlite3.connect(tmp_db.name)
        conn.row_factory = sqlite3.Row

        # Get table ID
        table_row = conn.execute(
            "SELECT ID FROM [Table] WHERE Name = ?", (table_name,)
        ).fetchone()
        if table_row is None:
            conn.close()
            raise ValueError(f"Table '{table_name}' not found in metadata")
        table_id = table_row["ID"]

        # Get columns (Type=1 is data, Type=3 is calculated; Type=2 is RowNumber).
        # Also pull the DictionaryStorage encoding so we can decode VALUE-encoded
        # columns (DictionaryStorage.Type=2, no external .dictionary file):
        # BaseId + Magnitude reconstruct value = (data_id + BaseId) / Magnitude.
        columns = conn.execute(
            """SELECT c.ID, c.ExplicitName, c.ExplicitDataType, c.IsHidden,
                      c.ColumnStorageID, c.Type,
                      ds.Type AS DSType, ds.BaseId AS DSBaseId,
                      ds.Magnitude AS DSMagnitude
               FROM [Column] c
               LEFT JOIN ColumnStorage cs ON cs.ID = c.ColumnStorageID
               LEFT JOIN DictionaryStorage ds ON ds.ID = cs.DictionaryStorageID
               WHERE c.TableID = ? AND c.Type IN (1, 3)
               ORDER BY c.ID""",
            (table_id,),
        ).fetchall()

        # Get partition info
        partition = conn.execute(
            "SELECT ID FROM [Partition] WHERE TableID = ? ORDER BY ID LIMIT 1",
            (table_id,),
        ).fetchone()

        conn.close()
    finally:
        os.unlink(tmp_db.name)

    if not columns:
        return {"columns": [], "rows": []}

    file_log = list_abf_files(abf_bytes)

    # Build a quick lookup for ABF files by partial path matching
    # Real PBIX file paths follow this convention:
    #   Dictionary:  {TableName} ({TableID}).tbl\0.{TableName} ({TableID}).{ColName} ({ColID}).dictionary
    #   IDF:         {TableName} ({TableID}).tbl\{PartID}.prt\0.{TableName} ({TableID}).{ColName} ({ColID}).0.idf
    #   IDFMETA:     same as IDF but .idfmeta
    # Our encoder uses:
    #   {TableName}.tbl\{PartNum}.prt\column.{ColName}
    #   {TableName}.tbl\{PartNum}.prt\column.{ColName}meta
    #   {TableName}.tbl\{PartNum}.prt\column.{ColName}.dict

    # Read each column's data
    col_data = []  # list of (col_name, data_type, values_list)
    # Columns whose VertiPaq files EXIST but fail to decode. These are genuine
    # data-loss situations (corrupt store, or a missing decoder dependency such
    # as xmhuffman for Huffman-compressed string dictionaries). We collect them
    # and raise AFTER the loop rather than silently returning a blank/dropped
    # column — silent data loss is the worst possible failure for this tool.
    decode_errors: list[tuple[str, str, str]] = []  # (col_name, stage, message)

    for col in columns:
        col_name = col["ExplicitName"]
        col_id = col["ID"]
        amo_type = col["ExplicitDataType"]
        data_type = _AMO_TO_TYPE_NAME.get(amo_type, "String")
        # DictionaryStorage.Type=2 => VALUE encoding (no external .dictionary):
        # values reconstruct from raw index + BaseId + Magnitude.
        ds_type = col["DSType"] if "DSType" in col.keys() else None
        ds_base_id = col["DSBaseId"] if "DSBaseId" in col.keys() else None
        ds_magnitude = col["DSMagnitude"] if "DSMagnitude" in col.keys() else None

        # Find column files in ABF using multiple matching strategies
        idf_entry = None
        meta_entry = None
        dict_entry = None

        # Strategy 1: Match by column ID (real PBIX files use IDs in paths)
        # e.g., ".ColName (1442)." in the path
        col_id_pattern = f"({col_id})"

        # Strategy 2: Match by column name within table context
        # For files generated by our encoder: "column.ColName"

        for entry in file_log:
            path = entry["Path"]

            # Skip H$ (hierarchy) and R$ (relationship) system table files
            if path.startswith("H$") or path.startswith("R$"):
                continue

            # Check if this file belongs to the right table.
            #   Real PBIX:  "<sanitized name> (TableID).tbl\..."
            #   Our encoder: "TableName.tbl\..."
            # Match on the TableID token "(TableID).tbl" rather than the name:
            # Power BI sanitizes '_', '-', '#', etc. to spaces in ABF paths (so
            # 'fct_Orders' -> 'fct Orders (14).tbl'), and calc / field-parameter
            # tables get a generic 'Table (id)' / 'Parameter (id)' folder — a
            # name match misses all of these and silently returns 0 columns.
            # The numeric TableID is stable across every one of those cases.
            is_table_file = (
                f"({table_id}).tbl" in path
                or path.startswith(f"{table_name}.tbl")
            )

            if not is_table_file:
                continue

            # Match by column ID in path (real PBIX format)
            if col_id_pattern in path and "RowNumber" not in path:
                if path.endswith(".idfmeta"):
                    meta_entry = entry
                elif path.endswith(".idf"):
                    idf_entry = entry
                elif path.endswith(".dictionary"):
                    dict_entry = entry
                continue

            # Match by column name (our encoder format)
            if f"column.{col_name}" in path:
                if path.endswith("meta"):
                    meta_entry = entry
                elif path.endswith(".dict"):
                    dict_entry = entry
                elif "column." in path and not path.endswith("meta") and ".dict" not in path and ".hidx" not in path:
                    idf_entry = entry

        if meta_entry is None:
            # Column might not have VertiPaq data (calculated column, etc.)
            col_data.append((col_name, data_type, None))
            continue

        # Read IDFMETA to get row_count and bit_width
        meta_bytes = read_abf_file(abf_bytes, meta_entry)
        try:
            meta_info = decode_idfmeta(meta_bytes)
        except Exception as e:
            decode_errors.append((col_name, "idfmeta", f"{type(e).__name__}: {e}"))
            col_data.append((col_name, data_type, None))
            continue

        row_count = meta_info["row_count"]

        # Multi-segment columns (import tables beyond one VertiPaq segment,
        # ~1,048,576 rows) carry one CS/SS block per segment. decode_idfmeta
        # returns only the first; parse them all so we can decode every segment.
        try:
            seg_meta_list = decode_idfmeta_segments(meta_bytes)
        except Exception:
            seg_meta_list = []
        if len(seg_meta_list) > 1:
            row_count = sum(s["records"] for s in seg_meta_list)

        if meta_info["is_row_number"]:
            # RowNumber column -- skip
            col_data.append((col_name, data_type, None))
            continue

        # Read dictionary
        dict_values = []
        if dict_entry is not None:
            dict_bytes_raw = read_abf_file(abf_bytes, dict_entry)
            try:
                _, dict_values = decode_dictionary(dict_bytes_raw)
            except Exception as e:
                decode_errors.append((col_name, "dictionary", f"{type(e).__name__}: {e}"))
                col_data.append((col_name, data_type, None))
                continue

        # Read IDF
        if idf_entry is not None and row_count > 0:
            idf_bytes_raw = read_abf_file(abf_bytes, idf_entry)
            bit_width = meta_info["bit_width"]
            if bit_width == 0:
                # Try to infer bit width from dictionary size
                n_distinct = len(dict_values) + (1 if meta_info["has_nulls"] else 0)
                if n_distinct <= 2:
                    bit_width = 1
                elif n_distinct > 0:
                    bit_width = _align_bit_width(max(1, math.ceil(math.log2(n_distinct))))
                else:
                    bit_width = 1

            # Re-base RLE runs (absolute data_ids) onto the same relative scale
            # as bit-packed values, so single-value / low-cardinality columns
            # (RLE-encoded by Power BI Desktop) decode correctly.
            _null_offset = 1 if meta_info["has_nulls"] else 0
            _rle_base = meta_info["min_data_id"] - _null_offset
            # For a multi-segment column the dictionary is shared but each
            # segment stores values relative to its own min_data_id; build
            # per-segment (bit_width, rle_base, bitpacked_add) so every segment
            # lands on the same global scale as segment 0 (= meta_info min).
            _seg_params = None
            if len(seg_meta_list) > 1:
                _gmin = seg_meta_list[0]["min_data_id"]
                _seg_params = [
                    (s["bit_width"] or bit_width, _gmin - _null_offset,
                     s["min_data_id"] - _gmin)
                    for s in seg_meta_list
                ]
            try:
                indices = decode_idf(
                    idf_bytes_raw, bit_width, row_count,
                    rle_base=_rle_base, segments=_seg_params,
                )
            except Exception as e:
                decode_errors.append((col_name, "idf", f"{type(e).__name__}: {e}"))
                col_data.append((col_name, data_type, None))
                continue

            # VALUE-encoded column (no external dictionary, DictionaryStorage.
            # Type=2): reconstruct value = (data_id + BaseId) / Magnitude.
            # `indices` are on the re-based scale (data_id - min_data_id +
            # null_offset), so data_id = idx + _rle_base and the real value is
            # (idx + _rle_base + BaseId) / Magnitude — using _rle_base (not the
            # raw min_data_id) keeps bit-packed and RLE runs consistent for a
            # nullable column. Verified byte-for-byte against pbixray across the
            # corpus (Int64 keys/counts, DateTime OLE dates, currency mag=10000,
            # score mag=1e9).
            if dict_entry is None and ds_type == 2:
                base = ds_base_id if ds_base_id is not None else 0
                mag = ds_magnitude if ds_magnitude else 1.0
                values = []
                for idx in indices:
                    if _null_offset and idx == 0:
                        values.append(None)  # the reserved blank/NULL slot
                    else:
                        values.append(
                            _reconstruct_value_encoded((idx + _rle_base + base) / mag, data_type)
                        )
                col_data.append((col_name, data_type, values))
                continue

            # Map indices to values (hash / dictionary encoding)
            has_nulls = meta_info["has_nulls"]
            null_offset = 1 if has_nulls else 0
            values = []
            for idx in indices:
                if has_nulls and idx == 0:
                    values.append(_reconstruct_value(None, data_type, True))
                else:
                    dict_idx = idx - null_offset
                    if 0 <= dict_idx < len(dict_values):
                        values.append(
                            _reconstruct_value(dict_values[dict_idx], data_type, False)
                        )
                    else:
                        values.append(None)
            col_data.append((col_name, data_type, values))
        elif row_count == 0:
            col_data.append((col_name, data_type, []))
        else:
            col_data.append((col_name, data_type, None))

    # Fail LOUD if any column's VertiPaq files existed but could not be decoded.
    # Returning here would silently drop those columns (they read back blank),
    # which is exactly the "large String column silently vanished" data-loss
    # class. Surface it instead so the caller sees which columns failed and why.
    if decode_errors:
        details = "; ".join(f"'{n}' ({stage} — {msg})" for n, stage, msg in decode_errors)
        hint = ""
        if any("xmhuffman" in msg.lower() for _, _, msg in decode_errors):
            hint = (" Reading Huffman-compressed string dictionaries requires the "
                    "'xmhuffman' package — install it with `pip install xmhuffman`.")
        raise InvalidPBIXError(
            f"Failed to decode {len(decode_errors)} column(s) in table "
            f"'{table_name}': {details}. Refusing to return partial data because "
            f"the affected column(s) would silently read as blank.{hint}"
        )

    # Filter out columns with no data (None means we couldn't read it)
    valid_cols = [(name, dt, vals) for name, dt, vals in col_data if vals is not None]

    if not valid_cols:
        return {"columns": [], "rows": []}

    # Determine row count from first valid column
    n_rows = len(valid_cols[0][2]) if valid_cols else 0

    # Build rows
    col_names = [name for name, _, _ in valid_cols]
    rows = []
    for i in range(n_rows):
        row = []
        for _, _, vals in valid_cols:
            if i < len(vals):
                row.append(vals[i])
            else:
                row.append(None)
        rows.append(row)

    return {"columns": col_names, "rows": rows}
