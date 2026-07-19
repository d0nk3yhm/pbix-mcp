"""
Regression tests for VertiPaq segment decoding against the exact Power BI
Desktop on-disk format:

  * value-encoded columns (DictionaryStorage.Type=2, no external dictionary):
    value = (data_id + BaseId) / Magnitude, with OLE-date semantics;
  * RLE runs store the ABSOLUTE data_id (bit-packed values are relative to the
    segment minimum) — they must be re-based to line up;
  * a segment may contain MULTIPLE bit-packed groups, and each successive
    bit-packed marker is 0xFFFFFFFF minus the number of bit-packed values
    already consumed (not a fixed 0xFFFFFFFF), with all bit-packed values stored
    contiguously in the sub-segment.

These are crafted from raw bytes so they run in CI without any private PBIX
corpus. The whole formula is additionally cross-checked against pbixray over the
Desktop corpus in local runs (see the PBIX_TEST_SAMPLES-gated test below).
"""

import datetime
import os
import struct

import pytest

from pbix_mcp.formats.vertipaq_decoder import (
    _reconstruct_value_encoded,
    decode_idf,
)

pytestmark = pytest.mark.unit


def _build_idf(primary_entries, sub_values, bit_width):
    """Assemble a minimal single-segment IDF blob.

    primary_entries: list of (data_value, repeat_value).
    sub_values: the flat list of bit-packed values (contiguous across groups).
    """
    ps_count = 16
    buf = bytearray()
    buf += struct.pack("<Q", ps_count)
    for i in range(ps_count):
        if i < len(primary_entries):
            dv, rv = primary_entries[i]
        else:
            dv, rv = 0, 0
        buf += struct.pack("<II", dv & 0xFFFFFFFF, rv)
    # sub-segment: pack sub_values, values_per_word = 64 // bit_width
    vpw = 64 // bit_width
    words = []
    for k in range(0, len(sub_values), vpw):
        chunk = sub_values[k:k + vpw]
        w = 0
        for j, v in enumerate(chunk):
            w |= (v & ((1 << bit_width) - 1)) << (j * bit_width)
        words.append(w)
    buf += struct.pack("<Q", len(words))
    for w in words:
        buf += struct.pack("<Q", w)
    return bytes(buf)


class TestDecodeIdf:
    def test_rle_rebase(self):
        # One RLE run of the absolute data_id 10, re-based by min_data_id=3.
        idf = _build_idf([(10, 5)], [], bit_width=4)
        assert decode_idf(idf, 4, 5, rle_base=3) == [7, 7, 7, 7, 7]

    def test_rle_default_base_unchanged(self):
        # Backward-compat: default rle_base=0 leaves the RLE value as stored.
        idf = _build_idf([(10, 5)], [], bit_width=4)
        assert decode_idf(idf, 4, 5) == [10, 10, 10, 10, 10]

    def test_single_bitpacked_group(self):
        # One bit-packed group of 3 values (our own encoder's shape).
        idf = _build_idf([(0xFFFFFFFF, 3)], [1, 2, 3], bit_width=4)
        assert decode_idf(idf, 4, 3, rle_base=3) == [1, 2, 3]

    def test_multi_bitpacked_groups_decreasing_marker(self):
        # THE FactSales bug: a second bit-packed group's marker is
        # 0xFFFFFFFF - (values already consumed), and all bit-packed values are
        # stored contiguously in the sub-segment.
        entries = [
            (0xFFFFFFFF, 3),          # group A: consumes bitpacked[0:3]
            (10, 2),                  # RLE run of data_id 10 -> 7,7
            (0xFFFFFFFF - 3, 2),      # group B marker (offset 3): bitpacked[3:5]
        ]
        idf = _build_idf(entries, [1, 2, 3, 4, 5], bit_width=4)
        assert decode_idf(idf, 4, 7, rle_base=3) == [1, 2, 3, 7, 7, 4, 5]

    def test_partial_word_alignment_across_groups(self):
        # bit_width=9 -> 7 values per 64-bit word. A first group of 5 values
        # leaves a partial word; the second group must continue at value-offset
        # 5, not at the next whole word.
        vals = list(range(1, 13))  # 12 values -> 2 words (7 + 5)
        entries = [
            (0xFFFFFFFF, 5),          # bitpacked[0:5] = 1..5
            (100, 1),                 # RLE data_id 100 -> 97
            (0xFFFFFFFF - 5, 7),      # bitpacked[5:12] = 6..12
        ]
        idf = _build_idf(entries, vals, bit_width=9)
        assert decode_idf(idf, 9, 13, rle_base=3) == [1, 2, 3, 4, 5, 97, 6, 7, 8, 9, 10, 11, 12]


class TestValueEncodedReconstruction:
    def test_int64(self):
        assert _reconstruct_value_encoded(42.0, "Int64") == 42
        assert isinstance(_reconstruct_value_encoded(42.0, "Int64"), int)

    def test_datetime_ole_serial(self):
        # OLE serial 45748 == 2025-04-01 (verified against pbixray on dimDate).
        v = _reconstruct_value_encoded(45748, "DateTime")
        assert v == datetime.datetime(2025, 4, 1)

    def test_boolean(self):
        assert _reconstruct_value_encoded(1, "Boolean") is True
        assert _reconstruct_value_encoded(0, "Boolean") is False


@pytest.mark.integration
def test_corpus_matches_pbixray():
    """Cross-check every data column of the Desktop corpus against pbixray.

    Skipped unless PBIX_TEST_SAMPLES points at a directory of Desktop-authored
    PBIX files AND pbixray is installed. This is the ground-truth gate that
    proved value/hash/RLE/mixed decoding byte-for-byte during development.
    """
    samples = os.environ.get("PBIX_TEST_SAMPLES", "")
    if not samples or not os.path.isdir(samples):
        pytest.skip("PBIX_TEST_SAMPLES not set")
    pbixray = pytest.importorskip("pbixray")
    import glob
    import zipfile

    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
    from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

    def eq(a, b):
        try:
            import pandas as pd
            if isinstance(b, pd.Timestamp):
                b = b.to_pydatetime()
            if b is pd.NA:
                b = None
        except Exception:
            pass
        if a is None or (isinstance(a, float) and a != a):
            return b is None or (isinstance(b, float) and b != b)
        if isinstance(a, datetime.datetime) and isinstance(b, datetime.datetime):
            return abs((a - b).total_seconds()) < 1
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return abs(float(a) - float(b)) < 1e-6
        return str(a) == str(b)

    files = glob.glob(os.path.join(samples, "*.pbix"))
    if not files:
        pytest.skip("no PBIX files in PBIX_TEST_SAMPLES")
    checked = 0
    for path in files:
        with zipfile.ZipFile(path) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        meta = read_metadata_sqlite(abf)
        px = pbixray.PBIXRay(path)
        for tname in list(px.tables):
            pdf = px.get_table(tname)
            td = read_table_from_abf(abf, tname, meta)
            # A table pbixray reads with data must NOT decode to zero columns —
            # that is the silent whole-table data loss the '_'/'-' matcher bug
            # caused (and which the per-column loop below would otherwise skip).
            if pdf.shape[1] > 0 and pdf.shape[0] > 0:
                assert td["columns"], f"{os.path.basename(path)}:{tname} decoded to 0 columns"
            for cname in td["columns"]:
                if cname not in pdf.columns:
                    continue
                ci = td["columns"].index(cname)
                got = [r[ci] for r in td["rows"]]
                truth = list(pdf[cname])
                assert len(got) == len(truth)
                assert all(eq(a, b) for a, b in zip(got, truth)), f"{tname}.{cname}"
                checked += 1
    assert checked > 0
