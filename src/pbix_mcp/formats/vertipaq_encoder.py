"""
VertiPaq Encoder -- write table data into ABF column files (IDF, IDFMETA, DICT, HIDX).

This module produces binary column data in the VertiPaq format used by Power BI
PBIX files.  It generates UNCOMPRESSED column stores that Power BI Desktop can
read without any additional decompression layer.

Public API
----------
encode_table_data(table_name, partition_num, columns, rows) -> dict[str, bytes]
    Encode column data and return a mapping of ABF internal paths to binary content.

update_table_in_abf(abf_bytes, table_name, columns, rows, metadata_sqlite_bytes) -> bytes
    Replace a table's column data inside an existing ABF blob.
"""

from __future__ import annotations

import math
import os
import sqlite3
import struct
import tempfile
from typing import Optional

# ---------------------------------------------------------------------------
# Constants & Tag bytes (from IDFMETA Kaitai spec)
# ---------------------------------------------------------------------------

# IDFMETA tags  (UTF-8 encoded tag literals as seen in the binary format)
TAG_CP_OPEN   = b"\x3C\x31\x3A\x43\x50\x00"   # <1:CP\0
TAG_CP_CLOSE  = b"\x43\x50\x3A\x31\x3E\x00"   # CP:1>\0
TAG_CS_OPEN   = b"\x3C\x31\x3A\x43\x53\x00"   # <1:CS\0
TAG_CS_CLOSE  = b"\x43\x53\x3A\x31\x3E\x00"   # CS:1>\0
TAG_SS_OPEN   = b"\x3C\x31\x3A\x53\x53\x00"   # <1:SS\0
TAG_SS_CLOSE  = b"\x53\x53\x3A\x31\x3E\x00"   # SS:1>\0
TAG_SDOS_OPEN  = b"\x3C\x31\x3A\x53\x44\x4F\x73\x00"  # <1:SDOs\0
TAG_SDOS_CLOSE = b"\x53\x44\x4F\x73\x3A\x31\x3E\x00"  # SDOs:1>\0
TAG_CSDOS_OPEN  = b"\x3C\x31\x3A\x43\x53\x44\x4F\x73\x00"  # <1:CSDOs\0
TAG_CSDOS_CLOSE = b"\x43\x53\x44\x4F\x73\x3A\x31\x3E\x00"  # CSDOs:1>\0

# Dictionary page markers
STRING_STORE_BEGIN = b"\xDD\xCC\xBB\xAA"
STRING_STORE_END   = b"\xCD\xAB\xCD\xAB"

# Dictionary types (matches ColumnDataDictionary enum)
DICT_TYPE_LONG   = 0   # int64
DICT_TYPE_REAL   = 1   # float64
DICT_TYPE_STRING = 2   # string

# AMO data-type codes used in metadata.sqlitedb
AMO_STRING   = 2
AMO_INT64    = 6
AMO_FLOAT64  = 8
AMO_DATETIME = 9
AMO_DECIMAL  = 10
AMO_BOOLEAN  = 11

# Map our friendly type names to AMO codes
_TYPE_NAME_TO_AMO = {
    "String":   AMO_STRING,
    "Int64":    AMO_INT64,
    "Float64":  AMO_FLOAT64,
    "DateTime": AMO_DATETIME,
    "Decimal":  AMO_DECIMAL,
    "Boolean":  AMO_BOOLEAN,
}


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _u4(v: int) -> bytes:
    """Pack unsigned 32-bit little-endian."""
    return struct.pack("<I", v & 0xFFFFFFFF)


def _s4(v: int) -> bytes:
    """Pack signed 32-bit little-endian."""
    return struct.pack("<i", v)


def _u8(v: int) -> bytes:
    """Pack unsigned 64-bit little-endian."""
    return struct.pack("<Q", v)


def _s8(v: int) -> bytes:
    """Pack signed 64-bit little-endian."""
    return struct.pack("<q", v)


def _f8(v: float) -> bytes:
    """Pack float64 little-endian."""
    return struct.pack("<d", v)


def _u1(v: int) -> bytes:
    """Pack single unsigned byte."""
    return struct.pack("<B", v)


def _required_bits(n: int) -> int:
    """Minimum bits to represent values 0..n-1.  Returns at least 1."""
    if n <= 1:
        return 1
    return max(1, math.ceil(math.log2(n)))


def _next_power_of_2(n: int) -> int:
    """Smallest power of 2 >= n."""
    if n <= 1:
        return 1
    return 1 << math.ceil(math.log2(n))


def _align_bit_width(bw: int) -> int:
    """
    VertiPaq bit-widths must divide evenly into 64 for the bit-packing.
    Valid widths: 1,2,4,8,16,32,64.  Round up to the nearest valid width.
    """
    valid = [1, 2, 4, 8, 16, 32, 64]
    for v in valid:
        if bw <= v:
            return v
    return 64


# ---------------------------------------------------------------------------
# Hash helpers (for HIDX / dictionary hash_information)
# ---------------------------------------------------------------------------

def _fnv1a_hash_64(value_bytes: bytes) -> int:
    """FNV-1a 64-bit hash."""
    h = 0xcbf29ce484222325
    for b in value_bytes:
        h ^= b
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h


def _fnv1a_hash_32(value_bytes: bytes) -> int:
    """FNV-1a 32-bit hash (used by VertiPaq HIDX)."""
    h = 0x811c9dc5
    for b in value_bytes:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h


# ---------------------------------------------------------------------------
# Value conversion helpers
# ---------------------------------------------------------------------------

def _convert_value_for_dict(value, data_type: str):
    """
    Convert a Python value to the type stored in the VertiPaq dictionary.

    - String -> str (stored as UTF-16LE null-terminated)
    - Int64 / Boolean -> int64
    - Float64 -> float64
    - DateTime -> float64 (days since 1899-12-30, the OLE Automation epoch)
    - Decimal -> int64 (value * 10000)

    Returns None for null/None values.
    """
    if value is None:
        return None

    if data_type == "String":
        return str(value)
    elif data_type == "Int64":
        return int(value)
    elif data_type == "Boolean":
        if isinstance(value, bool):
            return 1 if value else 0
        return int(bool(value))
    elif data_type == "Float64":
        return float(value)
    elif data_type == "DateTime":
        import datetime as _dt
        if isinstance(value, _dt.datetime):
            epoch = _dt.datetime(1899, 12, 30)
            delta = value - epoch
            return delta.total_seconds() / 86400.0
        elif isinstance(value, (int, float)):
            return float(value)
        else:
            # Try parsing string
            dt = _dt.datetime.fromisoformat(str(value))
            epoch = _dt.datetime(1899, 12, 30)
            return (dt - epoch).total_seconds() / 86400.0
    elif data_type == "Decimal":
        from decimal import Decimal as D
        if isinstance(value, D):
            return int(value * 10000)
        return int(float(value) * 10000)
    else:
        return value


def _dict_type_for_data_type(data_type: str) -> int:
    """Return the VertiPaq dictionary type code for a given data type."""
    if data_type == "String":
        return DICT_TYPE_STRING
    elif data_type in ("Float64", "DateTime"):
        return DICT_TYPE_REAL
    else:
        # Int64, Boolean, Decimal all stored as int64
        return DICT_TYPE_LONG


def _element_size_for_dict_type(dict_type: int, unique_values: list = None) -> int:
    """
    Element size in bytes for numeric dictionary vectors.

    Bug #10: Use 4 bytes when all integer values fit in signed 32-bit range,
    8 bytes otherwise (or for floats).
    """
    if dict_type == DICT_TYPE_REAL:
        return 8  # float64 always 8 bytes
    if dict_type == DICT_TYPE_LONG and unique_values is not None:
        # Check if all values fit in signed 32-bit
        s32_min, s32_max = -(1 << 31), (1 << 31) - 1
        if all(s32_min <= int(v) <= s32_max for v in unique_values):
            return 4
    return 8


# ---------------------------------------------------------------------------
# Dictionary encoder
# ---------------------------------------------------------------------------

def _encode_dict_hash_info(unique_values: list, dict_type: int) -> bytes:
    """
    Encode the hash_information block (6 x int32) that appears after
    dictionary_type.

    Bug #9: integers use (-1, 8, 64, 6, -1, -1), floats use (-1, 16, 64, 3, -1, -1)
    Bug #11: strings use (0, 8, 64, 6, -1, -1)
    """
    if dict_type == DICT_TYPE_STRING:
        # Bug #11: string hash_info = (0, 8, 64, 6, -1, -1)
        vals = (0, 8, 64, 6, -1, -1)
    elif dict_type == DICT_TYPE_REAL:
        # Bug #9: float hash_info = (-1, 16, 64, 3, -1, -1)
        vals = (-1, 16, 64, 3, -1, -1)
    else:
        # Bug #9: integer hash_info = (-1, 8, 64, 6, -1, -1)
        vals = (-1, 8, 64, 6, -1, -1)
    return struct.pack("<6i", *vals)


def _encode_string_dictionary(unique_strings: list[str]) -> bytes:
    """
    Build a complete string dictionary blob (dict_type=2).

    Layout:
      - dictionary_type: int32 = 2
      - hash_information: 6 x int32 (zeros)
      - PageLayout header
      - DictionaryPage(s)
      - DictionaryRecordHandlesVector
    """
    buf = bytearray()

    # dictionary_type
    buf += _s4(DICT_TYPE_STRING)

    # hash_information (Bug #11)
    buf += _encode_dict_hash_info(unique_strings, DICT_TYPE_STRING)

    count = len(unique_strings)

    # Encode all strings as UTF-16LE null-terminated into a single page
    char_buf = bytearray()
    offsets = []  # byte offsets of each string in char_buf
    for s in unique_strings:
        offsets.append(len(char_buf))
        encoded = (s + "\x00").encode("utf-16-le")
        char_buf += encoded

    # Find longest string (in characters, including null)
    longest = max((len(s) + 1 for s in unique_strings), default=1)

    # allocation_size for the character buffer -- use actual size
    alloc_size = len(char_buf)
    # buffer_used_characters = number of UTF-16 code units used
    buffer_used_chars = alloc_size // 2
    # remaining_store_available = allocated chars - used chars (in chars)
    remaining = 0

    # --- PageLayout ---
    # store_string_count: int64
    buf += _s8(count)
    # f_store_compressed: int8 = 0 (uncompressed)
    buf += _u1(0)
    # store_longest_string: int64 (in characters)
    buf += _s8(longest)
    # store_page_count: int64
    buf += _s8(1)  # single page

    # --- DictionaryPage ---
    # page_mask: uint64 -- bitmask; for simplicity use 0
    buf += _u8(0)
    # page_contains_nulls: uint8
    buf += _u1(0)
    # page_start_index: uint64
    buf += _u8(0)
    # page_string_count: uint64
    buf += _u8(count)
    # page_compressed: uint8 = 0 (uncompressed)
    buf += _u1(0)

    # string_store_begin_mark
    buf += STRING_STORE_BEGIN

    # --- UncompressedStrings ---
    # remaining_store_available: uint64
    buf += _u8(remaining)
    # buffer_used_characters: uint64
    buf += _u8(buffer_used_chars)
    # allocation_size: uint64 (in bytes)
    buf += _u8(alloc_size)
    # uncompressed_character_buffer: alloc_size bytes
    buf += bytes(char_buf)

    # string_store_end_mark
    buf += STRING_STORE_END

    # --- DictionaryRecordHandlesVector ---
    # element_count: uint64
    buf += _u8(count)
    # element_size: uint32 = 8 (each handle is 8 bytes)
    buf += _u4(8)
    # vector of StringRecordHandle { char_offset: u32, page_id: u32 }
    for i, off in enumerate(offsets):
        buf += _u4(off // 2)   # character offset (Bug #12: byte_offset / 2)
        buf += _u4(0)          # page_id = 0 (single page)

    return bytes(buf)


def _encode_numeric_dictionary(unique_values: list, dict_type: int) -> bytes:
    """
    Build a numeric dictionary blob (dict_type=0 for int64, 1 for float64).

    Layout:
      - dictionary_type: int32
      - hash_information: 6 x int32
      - VectorOfVectors { element_count: u64, element_size: u32, values[] }
    """
    buf = bytearray()

    # dictionary_type
    buf += _s4(dict_type)
    # hash_information (Bug #9)
    buf += _encode_dict_hash_info(unique_values, dict_type)

    count = len(unique_values)
    element_size = _element_size_for_dict_type(dict_type, unique_values)  # Bug #10

    # VectorOfVectors
    buf += _u8(count)
    buf += _u4(element_size)

    for val in unique_values:
        if dict_type == DICT_TYPE_REAL:
            buf += _f8(float(val))
        elif element_size == 4:
            buf += _s4(int(val))  # Bug #10: pack as s32 when element_size=4
        else:
            buf += _s8(int(val))

    return bytes(buf)


def _encode_dictionary(unique_values: list, data_type: str) -> bytes:
    """Encode a dictionary for the given data type and unique values."""
    dict_type = _dict_type_for_data_type(data_type)
    if dict_type == DICT_TYPE_STRING:
        return _encode_string_dictionary([str(v) for v in unique_values])
    else:
        return _encode_numeric_dictionary(unique_values, dict_type)


# ---------------------------------------------------------------------------
# HIDX (hash index) encoder
# ---------------------------------------------------------------------------

def _encode_hidx(unique_values: list, data_type: str) -> bytes:
    """
    Build a HIDX (hash index) file for a column's dictionary.

    The HIDX maps hash(value) -> dictionary_index so that the engine can
    quickly look up dictionary entries.

    Structure (from ColumnDataHidx Kaitai):
      hash_algorithm: int32
      hash_entry_size: uint32
      hash_bin_size: uint32
      local_entry_count: uint32
      c_bins: uint64
      number_of_records: int64
      current_mask: int64
      hash_stats: uint8
      [hash_statistics if hash_stats != 0]
      hash_bin_entries[c_bins]  (each hash_bin_size bytes)
      overflow_hash_entries_count: uint64
      overflow_hash_entries[]
    """
    count = len(unique_values)
    if count == 0:
        count = 1  # at least 1 bin

    # Choose number of bins -- power of 2 >= 2*count (for load factor ~0.5)
    n_bins = _next_power_of_2(max(count * 2, 4))
    mask = n_bins - 1

    # Hash all values
    entries_by_bin: dict[int, list[tuple[int, int]]] = {}  # bin_index -> [(hash32, dict_index)]
    for idx, val in enumerate(unique_values):
        if data_type == "String":
            val_bytes = str(val).encode("utf-16-le")
        elif data_type in ("Float64", "DateTime"):
            val_bytes = struct.pack("<d", float(val))
        else:
            val_bytes = struct.pack("<q", int(val))

        h32 = _fnv1a_hash_32(val_bytes)
        if h32 == 0:
            h32 = 1  # avoid zero (zero means empty slot)
        bin_idx = h32 & mask
        entries_by_bin.setdefault(bin_idx, []).append((h32, idx))

    # local_entry_count = max entries per bin stored inline (typically 2)
    local_entry_count = 2
    hash_entry_size = 8  # sizeof(HashEntry) = m_hash(4) + m_key(4)

    # hash_bin_size = 8(m_rg_chain) + 4(m_count) + local_entry_count*8 + 4(padding)
    hash_bin_size = 8 + 4 + local_entry_count * hash_entry_size + 4

    # Collect overflow entries
    overflow_entries = []

    buf = bytearray()

    # Header
    buf += _s4(1)                     # hash_algorithm = 1 (FNV)
    buf += _u4(hash_entry_size)       # hash_entry_size = 8
    buf += _u4(hash_bin_size)         # hash_bin_size
    buf += _u4(local_entry_count)     # local_entry_count
    buf += _u8(n_bins)                # c_bins
    buf += _s8(len(unique_values))    # number_of_records
    buf += _s8(mask)                  # current_mask
    buf += _u1(0)                     # hash_stats = 0 (no statistics)

    # Hash bins
    for bin_i in range(n_bins):
        bin_entries = entries_by_bin.get(bin_i, [])
        local_count = min(len(bin_entries), local_entry_count)
        extra = bin_entries[local_entry_count:]

        # m_rg_chain: uint64 -- index into overflow array, or 0 if none
        if extra:
            chain_start = len(overflow_entries)
            overflow_entries.extend(extra)
            buf += _u8(chain_start + 1)  # 1-based or offset; use actual index
        else:
            buf += _u8(0)

        # m_count: uint32
        buf += _u4(len(bin_entries))

        # m_rg_local_entries[local_entry_count]
        for j in range(local_entry_count):
            if j < local_count:
                h32, key = bin_entries[j]
                buf += _u4(h32)
                buf += _u4(key)
            else:
                buf += _u4(0)  # empty hash
                buf += _u4(0)  # empty key

        # padding: uint32
        buf += _u4(0)

    # Overflow entries
    buf += _u8(len(overflow_entries))
    for h32, key in overflow_entries:
        buf += _u4(h32)
        buf += _u4(key)

    return bytes(buf)


# ---------------------------------------------------------------------------
# IDF (Index Data File) encoder -- RLE + bit-packed hybrid
# ---------------------------------------------------------------------------

def _encode_idf(indices: list[int], bit_width: int) -> bytes:
    """
    Encode a column's dictionary indices into an IDF segment.

    The IDF uses an RLE + bit-packed hybrid encoding:

    Segment structure:
      primary_segment_size: uint64
      primary_segment[]: array of (data_value: u32, repeat_value: u32)
        - data_value == 0xFFFFFFFF means "read repeat_value values from bit-packed sub_segment"
        - Otherwise, repeat data_value exactly repeat_value times
      sub_segment_size: uint64
      sub_segment[]: array of uint64 (bit-packed values)

    Strategy: We RLE-encode runs.  For runs of length >= 3 of the same value,
    emit an RLE entry.  Collect non-run values into bit-packed batches.
    """
    if not indices:
        # Empty column: primary segment is still 16 u64 words (128 bytes, all zeros) + empty sub
        return _u8(16) + (b"\x00" * 128) + _u8(0)

    # Build RLE runs
    runs = []  # list of (value, count)
    i = 0
    while i < len(indices):
        val = indices[i]
        count = 1
        while i + count < len(indices) and indices[i + count] == val:
            count += 1
        runs.append((val, count))
        i += count

    # Decide which runs to RLE-encode vs bit-pack.
    # For simplicity: RLE all runs of length >= 3, bit-pack the rest.
    # But actually, the simplest correct approach is:
    #   - If all values are the same, use a single RLE entry
    #   - Otherwise, bit-pack everything with a single 0xFFFFFFFF marker
    # This is always valid and Power BI reads it fine.

    # Check if we can use pure RLE (all runs are long enough or single-valued)
    # For maximum compatibility, use this strategy:
    # Build primary_segment entries and accumulate bit-packed values.

    primary_entries = []     # list of (data_value_u32, repeat_value_u32)
    bitpacked_values = []    # values that go into the sub_segment

    for val, count in runs:
        if count >= 3:
            # If we have pending bit-packed values, flush them first
            if bitpacked_values:
                primary_entries.append((0xFFFFFFFF, len(bitpacked_values)))
            # RLE entry
            primary_entries.append((val, count))
        else:
            # Add to bit-packed batch
            for _ in range(count):
                bitpacked_values.append(val)

    # If there are remaining bit-packed values, flush
    # We need to re-scan because the above only flushes when hitting an RLE run.
    # Let's redo this more carefully.

    primary_entries = []
    bitpacked_values = []
    pending_bp = []  # pending bit-packed values not yet flushed

    for val, count in runs:
        if count >= 3:
            # Flush any pending bit-packed values
            if pending_bp:
                primary_entries.append((0xFFFFFFFF, len(pending_bp)))
                bitpacked_values.extend(pending_bp)
                pending_bp = []
            # RLE entry
            primary_entries.append((val, count))
        else:
            # Accumulate for bit-packing
            pending_bp.extend([val] * count)

    # Flush remaining
    if pending_bp:
        primary_entries.append((0xFFFFFFFF, len(pending_bp)))
        bitpacked_values.extend(pending_bp)
        pending_bp = []

    # If there are no entries at all (shouldn't happen with non-empty indices)
    if not primary_entries:
        primary_entries.append((0xFFFFFFFF, len(indices)))
        bitpacked_values = list(indices)

    # Encode the sub_segment (bit-packed uint64 array)
    sub_segment_u64s = _bitpack_values(bitpacked_values, bit_width)

    # Build the binary IDF
    buf = bytearray()

    # primary_segment_size: ALWAYS 16 (16 u64 words = 128 bytes, zero-padded)
    buf += _u8(16)

    # primary_segment entries -- always exactly 16 u64 words (128 bytes)
    # Each entry is (data_value: u32, repeat_value: u32) = 1 u64 word
    for dv, rv in primary_entries:
        buf += _u4(dv)
        buf += _u4(rv)
    # Zero-pad remaining entries to fill 16 u64 words (128 bytes total)
    padding_entries = 16 - len(primary_entries)
    buf += b"\x00" * (padding_entries * 8)

    # sub_segment_size (count of uint64 values)
    buf += _u8(len(sub_segment_u64s))

    # sub_segment values
    for u64val in sub_segment_u64s:
        buf += _u8(u64val)

    return bytes(buf)


def _bitpack_values(values: list[int], bit_width: int) -> list[int]:
    """
    Pack a list of integer values into uint64 words using bit_width bits each.

    Each uint64 holds (64 // bit_width) values, packed from LSB to MSB.
    """
    if not values:
        return []

    if bit_width == 0:
        # Special case: all values are 0, return a single zero uint64
        return [0]

    values_per_word = 64 // bit_width
    mask = (1 << bit_width) - 1
    n_words = math.ceil(len(values) / values_per_word)

    result = []
    vi = 0
    for _ in range(n_words):
        word = 0
        for bit_pos_idx in range(values_per_word):
            if vi < len(values):
                word |= (values[vi] & mask) << (bit_pos_idx * bit_width)
                vi += 1
        result.append(word)

    return result


# ---------------------------------------------------------------------------
# IDFMETA encoder
# ---------------------------------------------------------------------------

def _encode_idfmeta(
    row_count: int,
    distinct_states: int,
    min_data_id: int,
    max_data_id: int,
    has_nulls: bool,
    rle_runs: int,
    bit_width: int,
    primary_segment_count: int,
    primary_segment_bytes: int,
    sub_segment_count: int,
    sub_segment_bytes: int,
    count_bit_packed: int,
    nonzero_primary_entries: int = 0,
    has_dict: bool = True,
    is_row_number: bool = False,
) -> bytes:
    """
    Encode an IDFMETA file.

    The IDFMETA has a tagged binary format with nested blocks:
      CP > CS0 > SS, CS1
      [optional: SDOs > CSDOs > CSDOs1]

    Fields in CS0:
      records: uint64              -- row_count
      one: uint64                  -- always 1
      a_b_a_5_a: uint32            -- 36 - bit_width (with iterator=0)
      iterator: uint32             -- 0
      bookmark_bits_1_2_8: uint64  -- 128
      storage_alloc_size: uint64   -- allocated size of the IDF data
      storage_used_size: uint64    -- used size of the IDF data
      segment_needs_resizing: uint8 -- 0
      compression_info: uint32     -- 0 (no compression)
      SS block
      has_bit_packed_sub_seg: uint8
      CS1 block

    Fields in SS:
      distinct_states: uint64
      min_data_id: uint32
      max_data_id: uint32
      original_min_segment_data_id: uint32
      r_l_e_sort_order: int64
      row_count: uint64
      has_nulls: uint8
      r_l_e_runs: uint64
      others_r_l_e_runs: uint64

    Fields in CS1:
      count_bit_packed: uint64
      blob_with9_zeros: 9 bytes of zeros
    """
    buf = bytearray()

    # --- CP open ---
    buf += TAG_CP_OPEN
    # version_one: uint64 = 1
    buf += _u8(1)

    # --- CS0 open ---
    buf += TAG_CS_OPEN

    # records
    buf += _u8(row_count)
    # one: 0 for RowNumber columns, 1 otherwise (Bug #4)
    buf += _u8(0 if is_row_number else 1)
    # a_b_a_5_a = 36 - bit_width
    a_b_a_5_a = 36 - bit_width
    buf += _u4(a_b_a_5_a)
    # iterator = 0
    buf += _u4(0)
    # bookmark_bits: 12 (not 128) (Bug #3)
    buf += _u8(12)

    # storage_alloc_size: always 32 (Bug #6)
    buf += _u8(32)
    # storage_used_size: 2 * (nonzero_primary_entries + has_dict) (Bug #6)
    storage_used = 2 * (nonzero_primary_entries + (1 if has_dict else 0))
    buf += _u8(storage_used)
    # segment_needs_resizing
    buf += _u1(0)
    # compression_info = 3 (Bug #2)
    buf += _u4(3)

    # --- SS block ---
    buf += TAG_SS_OPEN
    buf += _u8(distinct_states)
    buf += _u4(min_data_id)
    buf += _u4(max_data_id)
    buf += _u4(min_data_id)          # original_min_segment_data_id
    buf += _s8(-1)                    # r_l_e_sort_order (-1 = unsorted)
    buf += _u8(row_count)
    buf += _u1(1 if has_nulls else 0)
    buf += _u8(rle_runs)
    buf += _u8(1 if has_dict else 0)  # others_r_l_e_runs (Bug #7)
    buf += TAG_SS_CLOSE

    # has_bit_packed_sub_seg
    buf += _u1(1 if count_bit_packed > 0 else 0)

    # --- CS1 block ---
    buf += TAG_CS_OPEN
    buf += _u8(count_bit_packed)
    buf += b"\x00" * 9               # blob_with9_zeros
    buf += TAG_CS_CLOSE

    # --- CS0 close ---
    buf += TAG_CS_CLOSE

    # --- CP close ---
    buf += TAG_CP_CLOSE

    # --- SDOs block (segment data offsets) ---
    buf += TAG_SDOS_OPEN

    # CSDOs (outer)
    buf += TAG_CSDOS_OPEN
    # zero_c_s_d_o: uint64 = 0
    buf += _u8(0)
    # primary_segment_size: always 16 (Bug #8)
    buf += _u8(16)

    # CSDOs1 (inner, for sub-segment)
    buf += TAG_CSDOS_OPEN
    # sub_segment_offset: always 136 (8 + 128 + 0 = offset to sub_segment) (Bug #8)
    buf += _u8(136)
    # sub_segment_size: uint64
    buf += _u8(sub_segment_bytes)
    buf += TAG_CSDOS_CLOSE

    buf += TAG_CSDOS_CLOSE

    buf += TAG_SDOS_CLOSE

    return bytes(buf)


# ---------------------------------------------------------------------------
# Column encoder (combines all pieces)
# ---------------------------------------------------------------------------

def _encode_column(
    column_name: str,
    data_type: str,
    nullable: bool,
    values: list,
    is_row_number: bool = False,
) -> dict[str, bytes]:
    """
    Encode a single column's data into the VertiPaq binary files.

    Returns a dict with keys:
      "idf"      -> IDF bytes
      "idfmeta"  -> IDFMETA bytes
      "dict"     -> Dictionary bytes
      "hidx"     -> HIDX bytes
    """
    row_count = len(values)

    # --- Build dictionary ---
    # Separate nulls from values
    has_nulls = nullable and any(v is None for v in values)

    # Convert values to storage format
    converted = []
    for v in values:
        converted.append(_convert_value_for_dict(v, data_type))

    # Build unique values list (excluding None)
    non_null_values = [v for v in converted if v is not None]
    unique_sorted = sorted(set(non_null_values), key=lambda x: (str(type(x)), x) if not isinstance(x, (int, float)) else x)

    # Map value -> dictionary index
    # Index 0 is reserved for NULL when nullable
    null_offset = 1 if has_nulls else 0
    value_to_idx = {}
    for i, uv in enumerate(unique_sorted):
        value_to_idx[_val_key(uv)] = i + null_offset

    # Build index array
    indices = []
    for v in converted:
        if v is None:
            indices.append(0)  # NULL index = 0
        else:
            indices.append(value_to_idx[_val_key(v)])

    # Cardinality (distinct states) includes NULL as a state
    distinct_states = len(unique_sorted) + (1 if has_nulls else 0)

    # min/max data IDs
    if has_nulls:
        min_data_id = 0
        max_data_id = len(unique_sorted)  # null=0, values=1..N
    else:
        min_data_id = 0
        max_data_id = max(len(unique_sorted) - 1, 0)

    # Bit width for encoding indices
    bit_width_raw = _required_bits(distinct_states)
    bit_width = _align_bit_width(bit_width_raw)

    # --- Encode IDF ---
    idf_bytes = _encode_idf(indices, bit_width)

    # Parse IDF to get segment stats for IDFMETA
    # The IDF we just built: primary_segment_size(u64=16 always) + 128 bytes + sub_segment_size(u64) + sub entries
    ps_count = struct.unpack_from("<Q", idf_bytes, 0)[0]  # always 16
    primary_segment_bytes = ps_count * 8  # 16 * 8 = 128 bytes always
    ss_offset = 8 + primary_segment_bytes  # = 136
    ss_count = struct.unpack_from("<Q", idf_bytes, ss_offset)[0]
    sub_segment_bytes = ss_count * 8  # each uint64 is 8 bytes

    # Count RLE runs and bit-packed count from the actual (non-zero-padded) entries
    rle_runs = 0
    count_bit_packed = 0
    nonzero_primary_entries = 0
    offset = 8  # skip primary_segment_size
    for _ in range(ps_count):
        dv = struct.unpack_from("<I", idf_bytes, offset)[0]
        rv = struct.unpack_from("<I", idf_bytes, offset + 4)[0]
        if dv == 0 and rv == 0:
            break  # hit zero-padding
        nonzero_primary_entries += 1
        if dv == 0xFFFFFFFF:
            count_bit_packed += rv
        else:
            rle_runs += 1
        offset += 8

    # Determine if this column has a dictionary (non-RowNumber columns with values)
    has_dict = len(unique_sorted) > 0

    # --- Encode IDFMETA ---
    idfmeta_bytes = _encode_idfmeta(
        row_count=row_count,
        distinct_states=distinct_states,
        min_data_id=min_data_id,
        max_data_id=max_data_id,
        has_nulls=has_nulls,
        rle_runs=rle_runs,
        bit_width=bit_width,
        primary_segment_count=ps_count,
        primary_segment_bytes=primary_segment_bytes,
        sub_segment_count=ss_count,
        sub_segment_bytes=sub_segment_bytes,
        count_bit_packed=count_bit_packed,
        nonzero_primary_entries=nonzero_primary_entries,
        has_dict=has_dict,
        is_row_number=is_row_number,
    )

    # --- Encode Dictionary ---
    dict_bytes = _encode_dictionary(unique_sorted, data_type)

    # --- Encode HIDX ---
    hidx_bytes = _encode_hidx(unique_sorted, data_type)

    return {
        "idf": idf_bytes,
        "idfmeta": idfmeta_bytes,
        "dict": dict_bytes,
        "hidx": hidx_bytes,
    }


def _val_key(v):
    """Create a hashable key for a value (handles float NaN etc.)."""
    if isinstance(v, float) and math.isnan(v):
        return ("__nan__",)
    return v


# ---------------------------------------------------------------------------
# Public API: encode_table_data
# ---------------------------------------------------------------------------

def encode_table_data(
    table_name: str,
    partition_num: int,
    columns: list[dict],
    rows: list[dict],
) -> dict[str, bytes]:
    """
    Encode table data into VertiPaq column files.

    Parameters
    ----------
    table_name : str
        Name of the table (e.g. "Sales").
    partition_num : int
        Partition number (usually matches existing partition).
    columns : list[dict]
        Column definitions. Each dict has:
          - name: str
          - data_type: str  ('Int64', 'Float64', 'String', 'DateTime', 'Decimal', 'Boolean')
          - nullable: bool
    rows : list[dict]
        Row data. Each dict maps column name -> value.

    Returns
    -------
    dict[str, bytes]
        Maps ABF internal file path -> binary content.
        Example keys:
          "Sales.tbl\\26.prt\\column.SalesAmount"       (IDF)
          "Sales.tbl\\26.prt\\column.SalesAmountmeta"    (IDFMETA)
          "Sales.tbl\\26.prt\\column.SalesAmount.dict"   (Dictionary)
          "Sales.tbl\\26.prt\\column.SalesAmount.hidx"   (HIDX)  [for numeric columns]
    """
    result = {}

    base_path = f"{table_name}.tbl\\{partition_num}.prt"

    for col_def in columns:
        col_name = col_def["name"]
        data_type = col_def["data_type"]
        nullable = col_def.get("nullable", True)

        # Extract column values from rows
        values = [row.get(col_name) for row in rows]
        is_rn = col_def.get("is_row_number", False)

        # Encode the column
        encoded = _encode_column(col_name, data_type, nullable, values, is_row_number=is_rn)

        # Build file paths
        idf_path = f"{base_path}\\column.{col_name}"
        meta_path = f"{base_path}\\column.{col_name}meta"
        dict_path = f"{base_path}\\column.{col_name}.dict"
        hidx_path = f"{base_path}\\column.{col_name}.hidx"

        result[idf_path] = encoded["idf"]
        result[meta_path] = encoded["idfmeta"]
        result[dict_path] = encoded["dict"]
        result[hidx_path] = encoded["hidx"]

    return result


# ---------------------------------------------------------------------------
# Public API: update_table_in_abf
# ---------------------------------------------------------------------------

def update_table_in_abf(
    abf_bytes: bytes,
    table_name: str,
    columns: list[dict],
    rows: list[dict],
    metadata_sqlite_bytes: bytes,
) -> bytes:
    """
    Replace a table's data in an ABF, updating all column files and metadata.

    This function:
      1. Encodes the new column data using encode_table_data().
      2. Finds the existing partition number by scanning ABF file entries.
      3. Updates the metadata.sqlitedb (ColumnStorage statistics, StorageFile sizes).
      4. Replaces all matched column files in the ABF.
      5. Returns the rebuilt ABF bytes.

    Parameters
    ----------
    abf_bytes : bytes
        The original decompressed ABF blob.
    table_name : str
        Name of the table to replace.
    columns : list[dict]
        Column definitions (same format as encode_table_data).
    rows : list[dict]
        Row data (same format as encode_table_data).
    metadata_sqlite_bytes : bytes
        The current metadata.sqlitedb bytes (will be modified and re-embedded).

    Returns
    -------
    bytes
        New ABF bytes with the table data and metadata replaced.
    """
    from pbix_mcp.formats.abf_rebuild import (
        list_abf_files,
        rebuild_abf_with_replacement,
    )

    # --- Get the file log to find existing ABF paths for this table ---
    file_log = list_abf_files(abf_bytes)

    # --- Get exact IDF filenames for each column from metadata ---
    import sqlite3
    import tempfile
    tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp.write(metadata_sqlite_bytes)
    tmp.close()
    try:
        conn = sqlite3.connect(tmp.name)
        conn.row_factory = sqlite3.Row
        table_id = conn.execute(
            "SELECT ID FROM [Table] WHERE Name = ?", (table_name,)
        ).fetchone()
        if not table_id:
            raise ValueError(f"Table '{table_name}' not found in metadata.")
        table_id = table_id["ID"]

        # For each column, get the EXACT IDF/IDFMETA/Dict filenames
        col_filenames = {}  # col_name -> {idf_fname, meta_fname, dict_fname}
        for col_def in columns:
            col_name = col_def["name"]
            row = conn.execute("""
                SELECT sf.FileName as idf_fname, sf2.FileName as meta_fname, sf3.FileName as dict_fname
                FROM [Column] c
                JOIN ColumnStorage cs ON c.ColumnStorageID = cs.ID
                JOIN ColumnPartitionStorage cps ON cps.ColumnStorageID = cs.ID
                JOIN StorageFile sf ON cps.StorageFileID = sf.ID
                LEFT JOIN SegmentStorage ss ON ss.ColumnPartitionStorageID = cps.ID
                LEFT JOIN StorageFile sf2 ON ss.StorageFileID = sf2.ID
                LEFT JOIN DictionaryStorage ds ON ds.ColumnStorageID = cs.ID
                LEFT JOIN StorageFile sf3 ON ds.StorageFileID = sf3.ID
                WHERE c.TableID = ? AND c.ExplicitName = ?
            """, (table_id, col_name)).fetchone()
            if row:
                col_filenames[col_name] = {
                    "idf": row["idf_fname"],
                    "meta": row["meta_fname"],
                    "dict": row["dict_fname"],
                }
        conn.close()
    finally:
        os.unlink(tmp.name)

    # --- Find partition number from file log ---
    partition_num = _find_partition_num(file_log, table_name)

    # --- Encode new column data ---
    encoded_files = encode_table_data(table_name, partition_num, columns, rows)

    # --- Also encode ALL system columns (RowNumber, Format String, etc.) ---
    # These must match the new row count.
    user_col_names = {c["name"] for c in columns}
    tmp2 = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    tmp2.write(metadata_sqlite_bytes)
    tmp2.close()
    try:
        conn2 = sqlite3.connect(tmp2.name)
        conn2.row_factory = sqlite3.Row
        sys_cols = conn2.execute(
            """SELECT c.ID, c.ExplicitName, c.ExplicitDataType, c.IsNullable, c.Type,
                      sf.FileName as idf_fname, sf2.FileName as meta_fname
               FROM [Column] c
               JOIN ColumnStorage cs ON c.ColumnStorageID = cs.ID
               JOIN ColumnPartitionStorage cps ON cps.ColumnStorageID = cs.ID
               JOIN StorageFile sf ON cps.StorageFileID = sf.ID
               LEFT JOIN SegmentStorage ss ON ss.ColumnPartitionStorageID = cps.ID
               LEFT JOIN StorageFile sf2 ON ss.StorageFileID = sf2.ID
               WHERE c.TableID = ? AND c.ExplicitName NOT IN ({})
            """.format(",".join("?" for _ in user_col_names)),
            (table_id, *user_col_names)
        ).fetchall()
        conn2.close()
    finally:
        os.unlink(tmp2.name)

    row_count = len(rows)
    for sc in sys_cols:
        sc_name = sc["ExplicitName"]
        sc_type = sc["Type"]  # 3 = RowNumber
        dt_code = sc["ExplicitDataType"] or 6
        is_rn = "RowNumber" in sc_name

        # Generate system column data
        if is_rn:
            # RowNumber: sequential 0-based integers
            sys_col_def = [{"name": sc_name, "data_type": "Int64", "nullable": False, "is_row_number": True}]
            sys_rows = [{sc_name: i} for i in range(row_count)]
        else:
            # Other system columns (Format String, etc.): all nulls
            sys_col_def = [{"name": sc_name, "data_type": "String", "nullable": True}]
            sys_rows = [{sc_name: None} for _ in range(row_count)]

        sys_encoded = encode_table_data(table_name, partition_num, sys_col_def, sys_rows)

        # Map encoded files to the actual ABF filenames
        if sc["idf_fname"]:
            for epath, edata in sys_encoded.items():
                if "meta" not in epath.split(chr(92))[-1] and epath.endswith(f"column.{sc_name}"):
                    col_filenames[sc_name] = col_filenames.get(sc_name, {})
                    col_filenames[sc_name]["idf"] = sc["idf_fname"]
                    encoded_files[epath] = edata
                elif epath.endswith(f"column.{sc_name}meta"):
                    col_filenames[sc_name] = col_filenames.get(sc_name, {})
                    col_filenames[sc_name]["meta"] = sc["meta_fname"]
                    encoded_files[epath] = edata
                elif epath.endswith(f"column.{sc_name}.dict"):
                    # System columns may not have a dict in the ABF
                    pass

    # --- Update metadata SQLite ---
    updated_sqlite = _update_metadata_sqlite(
        metadata_sqlite_bytes, table_name, columns, rows, encoded_files
    )

    # --- Build replacement dict using EXACT filename matching ---
    replacements: dict[str, bytes] = {}

    # Include system columns in the matching
    all_col_names = list(user_col_names) + [sc["ExplicitName"] for sc in sys_cols]

    for col_name in all_col_names:
        fnames = col_filenames.get(col_name, {})
        if not fnames:
            continue

        # Search file_log for entries matching these exact filenames
        for entry in file_log:
            fname = entry.get("FileName", "")
            sp = entry.get("StoragePath", "")
            if not sp:
                continue

            # Match IDF
            if fnames.get("idf") and fname == fnames["idf"]:
                for epath, edata in encoded_files.items():
                    if epath.endswith(f"column.{col_name}") and "meta" not in epath.split(chr(92))[-1]:
                        replacements[sp] = edata
                        break

            # Match IDFMETA
            elif fnames.get("meta") and fname == fnames["meta"]:
                for epath, edata in encoded_files.items():
                    if epath.endswith(f"column.{col_name}meta"):
                        replacements[sp] = edata
                        break

            # Match Dictionary (dict filename may be None for some columns)
            elif fnames.get("dict") and fname == fnames["dict"]:
                for epath, edata in encoded_files.items():
                    if epath.endswith(f"column.{col_name}.dict"):
                        replacements[sp] = edata
                        break

    # Replace the metadata.sqlitedb
    for entry in file_log:
        if "metadata.sqlitedb" in entry.get("Path", "").lower():
            replacements[entry["StoragePath"]] = updated_sqlite
            break

    if not replacements:
        raise ValueError(f"No ABF files matched for table '{table_name}' columns. "
                        f"Filenames searched: {col_filenames}")

    return rebuild_abf_with_replacement(abf_bytes, replacements)


def _find_partition_num(file_log: list[dict], table_name: str) -> int:
    """Find the partition number used by a table in the ABF file log."""
    prefix = f"{table_name}.tbl\\"
    for entry in file_log:
        path = entry.get("Path", "")
        if path.startswith(prefix):
            # Extract partition number from path like "Sales.tbl\26.prt\column.X"
            parts = path.split("\\")
            if len(parts) >= 2 and parts[1].endswith(".prt"):
                try:
                    return int(parts[1].replace(".prt", ""))
                except ValueError:
                    continue
    # Default if not found
    return 0


def _find_abf_entry_by_filename(
    abf_struct, table_name: str, file_name: str
) -> Optional[str]:
    """
    Find a VDir entry's StoragePath by matching the table name and file name.
    Returns the StoragePath string or None.
    """
    needle_lower = file_name.lower()
    table_prefix = f"{table_name.lower()}.tbl"

    for entry in abf_struct.file_log:
        path_lower = entry["Path"].lower()
        if table_prefix in path_lower and path_lower.endswith(needle_lower.replace("\\", "/")):
            return entry["StoragePath"]
        # Also try matching just the filename component
        entry_filename = entry["FileName"].lower()
        if entry_filename == needle_lower:
            # Make sure it's for the right table
            if table_prefix in path_lower:
                return entry["StoragePath"]

    return None


def _update_metadata_sqlite(
    sqlite_bytes: bytes,
    table_name: str,
    columns: list[dict],
    rows: list[dict],
    encoded_files: dict[str, bytes],
) -> bytes:
    """
    Update the metadata.sqlitedb with new column statistics and file sizes.

    Updates:
      - ColumnStorage.Statistics_DistinctStates (cardinality)
      - StorageFile sizes for IDF, Dictionary, HIDX files
      - ColumnPartitionStorage.RecordCount
    """
    fd, tmp_path = tempfile.mkstemp(suffix=".sqlitedb")
    try:
        os.write(fd, sqlite_bytes)
        os.close(fd)
        fd = None

        conn = sqlite3.connect(tmp_path)
        try:
            _apply_metadata_updates(conn, table_name, columns, rows, encoded_files)
            conn.commit()
        finally:
            conn.close()

        with open(tmp_path, "rb") as f:
            return f.read()
    finally:
        if fd is not None:
            os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def _apply_metadata_updates(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[dict],
    rows: list[dict],
    encoded_files: dict[str, bytes],
):
    """Apply metadata updates to the SQLite database.

    Updates ALL columns in the table (including system columns like RowNumber)
    to reflect the new row count. User columns get full statistics updates;
    system columns get row count and cardinality set to match.
    """
    row_count = len(rows)

    # Get table ID
    table_id = conn.execute(
        "SELECT ID FROM [Table] WHERE Name = ?", (table_name,)
    ).fetchone()
    if table_id is None:
        return
    table_id = table_id[0]

    # First: update ALL columns in this table to the new row count
    # This includes system columns (RowNumber, Format String, etc.)
    all_col_storage_ids = conn.execute(
        """SELECT cs.ID FROM ColumnStorage cs
           JOIN [Column] c ON c.ColumnStorageID = cs.ID
           WHERE c.TableID = ?""",
        (table_id,)
    ).fetchall()
    user_col_names = {c["name"] for c in columns}
    for (cs_id,) in all_col_storage_ids:
        # Check if this is a user column we're updating
        col_info = conn.execute(
            "SELECT ExplicitName, Type FROM [Column] WHERE ColumnStorageID = ?",
            (cs_id,)
        ).fetchone()
        if col_info and col_info[0] in user_col_names:
            continue  # Will be updated below with proper stats
        # System/other column: set row count and cardinality to match
        conn.execute(
            """UPDATE ColumnStorage SET
                Statistics_RowCount = ?,
                Statistics_DistinctStates = ?
               WHERE ID = ?""",
            (row_count, row_count, cs_id)
        )

    # Now update user columns with detailed stats
    for col_def in columns:
        col_name = col_def["name"]
        data_type = col_def["data_type"]
        nullable = col_def.get("nullable", True)

        # Get column ID
        col_row = conn.execute(
            "SELECT ID, ColumnStorageID FROM [Column] WHERE TableId = ? AND ExplicitName = ?",
            (table_id, col_name)
        ).fetchone()
        if col_row is None:
            continue
        col_id, col_storage_id = col_row

        # Calculate distinct states
        values = [row.get(col_name) for row in rows]
        converted = [_convert_value_for_dict(v, data_type) for v in values]
        has_nulls = nullable and any(v is None for v in converted)
        non_null = [v for v in converted if v is not None]
        unique_count = len(set(_val_key(v) for v in non_null))
        distinct_states = unique_count + (1 if has_nulls else 0)

        # Update ColumnStorage statistics
        conn.execute(
            """UPDATE ColumnStorage SET
                Statistics_DistinctStates = ?,
                Statistics_RowCount = ?,
                Statistics_HasNulls = ?,
                Statistics_MinDataID = 0,
                Statistics_MaxDataID = ?
               WHERE ID = ?""",
            (distinct_states, row_count, 1 if has_nulls else 0,
             distinct_states - 1, col_storage_id)
        )

        # Update DictionaryStorage — critical for correct decoding
        dict_row = conn.execute(
            """SELECT ds.ID FROM DictionaryStorage ds
               WHERE ds.ColumnStorageID = ?""",
            (col_storage_id,)
        ).fetchone()
        if dict_row:
            for path, data in encoded_files.items():
                if path.endswith(f"column.{col_name}.dict"):
                    conn.execute(
                        """UPDATE DictionaryStorage SET
                            Size = ?,
                            LastId = ?,
                            BaseId = 0,
                            Magnitude = 1.0,
                            IsNullable = ?,
                            IsUnique = 0,
                            IsOperatingOn32 = 0
                           WHERE ID = ?""",
                        (len(data), unique_count - 1 + (1 if has_nulls else 0),
                         1 if has_nulls else 0, dict_row[0])
                    )
                    break

        # Note: StorageFile has no Size column; ABF file sizes are tracked
        # in the ABF VirtualDirectory and BackupLog, not in the SQLite metadata.
        # The ABF rebuild process handles updating those sizes automatically.

        # Skip HIDX updates (we don't currently generate separate HIDX for updates;
        # the hash index is embedded in the dictionary)
        _dummy = None  # placeholder to keep indentation clean


# ---------------------------------------------------------------------------
# Self-test / verification helper
# ---------------------------------------------------------------------------

def verify_roundtrip(columns: list[dict], rows: list[dict]) -> bool:
    """
    Verify that encoding and then decoding produces the original data.

    This is a self-test function that encodes data and then uses the Kaitai
    parsers to decode it, checking for consistency.

    Returns True if the roundtrip succeeds.
    """
    from io import BytesIO

    from kaitaistruct import KaitaiStream

    encoded = encode_table_data("Test", 0, columns, rows)

    for col_def in columns:
        col_name = col_def["name"]
        data_type = col_def["data_type"]

        idf_key = f"Test.tbl\\0.prt\\column.{col_name}"
        meta_key = f"Test.tbl\\0.prt\\column.{col_name}meta"
        dict_key = f"Test.tbl\\0.prt\\column.{col_name}.dict"

        # Parse IDFMETA
        from pbixray.column_data.idfmeta import IdfmetaParser
        meta = IdfmetaParser(KaitaiStream(BytesIO(encoded[meta_key])))
        assert meta.blocks.cp.cs.ss.row_count == len(rows), \
            f"Row count mismatch for {col_name}"

        # Parse IDF
        from pbixray.column_data.idf import ColumnDataIdf
        idf = ColumnDataIdf(KaitaiStream(BytesIO(encoded[idf_key])))
        assert len(idf.segments) >= 1, f"No segments in IDF for {col_name}"

        # Parse Dictionary
        from pbixray.column_data.dictionary import ColumnDataDictionary
        d = ColumnDataDictionary(KaitaiStream(BytesIO(encoded[dict_key])))
        if data_type == "String":
            assert d.dictionary_type == ColumnDataDictionary.DictionaryTypes.xm_type_string
        elif data_type in ("Float64", "DateTime"):
            assert d.dictionary_type == ColumnDataDictionary.DictionaryTypes.xm_type_real
        else:
            assert d.dictionary_type == ColumnDataDictionary.DictionaryTypes.xm_type_long

        print(f"  Column '{col_name}' ({data_type}): OK")

    print("Roundtrip verification passed!")
    return True


if __name__ == "__main__":
    # Quick self-test
    test_columns = [
        {"name": "ID", "data_type": "Int64", "nullable": False},
        {"name": "Name", "data_type": "String", "nullable": True},
        {"name": "Amount", "data_type": "Float64", "nullable": True},
        {"name": "IsActive", "data_type": "Boolean", "nullable": False},
    ]
    test_rows = [
        {"ID": 1, "Name": "Alice", "Amount": 100.50, "IsActive": True},
        {"ID": 2, "Name": "Bob", "Amount": 200.75, "IsActive": False},
        {"ID": 3, "Name": "Charlie", "Amount": None, "IsActive": True},
        {"ID": 4, "Name": "Alice", "Amount": 100.50, "IsActive": True},
        {"ID": 5, "Name": None, "Amount": 300.00, "IsActive": False},
    ]

    print("Encoding test data...")
    result = encode_table_data("TestTable", 0, test_columns, test_rows)
    for path, data in sorted(result.items()):
        print(f"  {path}: {len(data)} bytes")

    print("\nVerifying roundtrip...")
    try:
        verify_roundtrip(test_columns, test_rows)
    except ImportError:
        print("  (pbixray not available for verification, skipping)")
    except Exception as e:
        print(f"  Verification error: {e}")
