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


def _decode_string_dictionary(buf: bytes, pos: int) -> list[str]:
    """Decode a string dictionary from position pos onward."""
    # store_string_count: int64
    count = struct.unpack_from("<q", buf, pos)[0]
    pos += 8
    # f_store_compressed: int8
    pos += 1
    # store_longest_string: int64
    pos += 8
    # store_page_count: int64
    page_count = struct.unpack_from("<q", buf, pos)[0]
    pos += 8

    strings: list[str] = []

    for _ in range(page_count):
        # DictionaryPage header
        # page_mask: uint64
        pos += 8
        # page_contains_nulls: uint8
        pos += 1
        # page_start_index: uint64
        pos += 8
        # page_string_count: uint64
        page_string_count = struct.unpack_from("<Q", buf, pos)[0]
        pos += 8
        # page_compressed: uint8
        page_compressed = struct.unpack_from("<B", buf, pos)[0]
        pos += 1

        # string_store_begin_mark: 4 bytes
        mark = buf[pos:pos + 4]
        if mark != STRING_STORE_BEGIN:
            raise ValueError(f"Expected STRING_STORE_BEGIN at {pos}, got {mark!r}")
        pos += 4

        if page_compressed == 0:
            # UncompressedStrings
            # remaining_store_available: uint64
            pos += 8
            # buffer_used_characters: uint64
            buffer_used_chars = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
            # allocation_size: uint64
            alloc_size = struct.unpack_from("<Q", buf, pos)[0]
            pos += 8
            # character buffer (UTF-16LE)
            char_buf = buf[pos:pos + alloc_size]
            pos += alloc_size
        else:
            # Compressed strings -- not common in our generated files,
            # but we should handle the basic case
            raise ValueError("Compressed string dictionaries not yet supported")

        # string_store_end_mark
        end_mark = buf[pos:pos + 4]
        if end_mark != STRING_STORE_END:
            raise ValueError(f"Expected STRING_STORE_END at {pos}, got {end_mark!r}")
        pos += 4

        # DictionaryRecordHandlesVector
        # element_count: uint64
        elem_count = struct.unpack_from("<Q", buf, pos)[0]
        pos += 8
        # element_size: uint32
        elem_size = struct.unpack_from("<I", buf, pos)[0]
        pos += 4

        for j in range(elem_count):
            # StringRecordHandle: char_offset(u32), page_id(u32)
            char_offset = struct.unpack_from("<I", buf, pos)[0]
            pos += 4
            _page_id = struct.unpack_from("<I", buf, pos)[0]
            pos += 4

            # Read string from char_buf at byte offset = char_offset * 2
            byte_offset = char_offset * 2
            # Find null terminator
            s_end = byte_offset
            while s_end + 1 < len(char_buf):
                ch = struct.unpack_from("<H", char_buf, s_end)[0]
                if ch == 0:
                    break
                s_end += 2
            s = char_buf[byte_offset:s_end].decode("utf-16-le", errors="replace")
            strings.append(s)

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

def decode_idf(idf_bytes: bytes, bit_width: int, row_count: int) -> list[int]:
    """
    Decode an IDF blob and return per-row dictionary indices.

    The IDF uses an RLE + bit-packed hybrid encoding:
      primary_segment_size: uint64  (always 16 = 128 bytes / 8)
      primary_segment[16]: array of (data_value: u32, repeat_value: u32)
        - data_value == 0xFFFFFFFF means "read repeat_value values from bit-packed sub_segment"
        - Otherwise, repeat data_value exactly repeat_value times
      sub_segment_size: uint64  (count of uint64 words)
      sub_segment[]: array of uint64 (bit-packed values)
    """
    buf = idf_bytes
    pos = 0

    # primary_segment_size: uint64 (should be 16)
    ps_count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8

    # Read primary segment entries
    primary_entries = []
    for _ in range(ps_count):
        dv = struct.unpack_from("<I", buf, pos)[0]
        rv = struct.unpack_from("<I", buf, pos + 4)[0]
        pos += 8
        if dv == 0 and rv == 0:
            # Zero entry -- skip (padding)
            continue
        primary_entries.append((dv, rv))

    # Advance past full primary segment (skip any remaining padding)
    pos = 8 + ps_count * 8

    # sub_segment_size: uint64 (word count)
    ss_word_count = struct.unpack_from("<Q", buf, pos)[0]
    pos += 8

    # Read sub-segment uint64 words
    sub_words = []
    for _ in range(ss_word_count):
        w = struct.unpack_from("<Q", buf, pos)[0]
        sub_words.append(w)
        pos += 8

    # Decode primary entries to produce index list
    indices = []
    sub_pos = 0  # index into sub_words

    values_per_word = 64 // bit_width if bit_width > 0 else 0
    mask = (1 << bit_width) - 1 if bit_width > 0 else 0

    for dv, rv in primary_entries:
        if dv == 0xFFFFFFFF:
            # Bit-packed: read rv values from sub-segment
            remaining = rv
            while remaining > 0 and sub_pos < len(sub_words):
                word = sub_words[sub_pos]
                sub_pos += 1
                for j in range(values_per_word):
                    if remaining <= 0:
                        break
                    val = (word >> (j * bit_width)) & mask
                    indices.append(val)
                    remaining -= 1
        else:
            # RLE: repeat dv exactly rv times
            indices.extend([dv] * rv)

    # Truncate to exact row count (sub-segment may have padding)
    return indices[:row_count]


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

        # Get columns (Type=1 is data, Type=3 is calculated; Type=2 is RowNumber)
        columns = conn.execute(
            """SELECT c.ID, c.ExplicitName, c.ExplicitDataType, c.IsHidden,
                      c.ColumnStorageID, c.Type
               FROM [Column] c
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

    for col in columns:
        col_name = col["ExplicitName"]
        col_id = col["ID"]
        amo_type = col["ExplicitDataType"]
        data_type = _AMO_TO_TYPE_NAME.get(amo_type, "String")

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

            # Check if this file belongs to the right table
            # Real PBIX: "TableName (TableID).tbl"
            # Our encoder: "TableName.tbl"
            is_table_file = False
            if f"{table_name} ({table_id})" in path:
                is_table_file = True
            elif path.startswith(f"{table_name}.tbl"):
                is_table_file = True

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
        except Exception:
            col_data.append((col_name, data_type, None))
            continue

        row_count = meta_info["row_count"]

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
            except Exception:
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

            try:
                indices = decode_idf(idf_bytes_raw, bit_width, row_count)
            except Exception:
                col_data.append((col_name, data_type, None))
                continue

            # Map indices to values
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
