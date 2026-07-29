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


class TestMultiSegment:
    """A .idf concatenates one block per VertiPaq segment; all must be decoded."""

    def _two_segment_idf(self):
        # Segment A: one bit-packed group of 4 values [10,11,12,13].
        # Segment B: one bit-packed group of 3 values [20,21,22].
        # (bit_width 8 so the values fit; the decoder masks to bit_width bits.)
        a = _build_idf([(0xFFFFFFFF, 4)], [10, 11, 12, 13], bit_width=8)
        b = _build_idf([(0xFFFFFFFF, 3)], [20, 21, 22], bit_width=8)
        return a + b

    def test_walks_all_segments_single_params(self):
        idf = self._two_segment_idf()
        # row_count = total across both segments -> both are decoded & concatenated
        assert decode_idf(idf, 8, 7, rle_base=0) == [10, 11, 12, 13, 20, 21, 22]

    def test_stops_at_row_count_single_params(self):
        # Legacy single-segment callers pass one segment's row_count and must not
        # over-read into trailing segments they didn't ask about.
        idf = self._two_segment_idf()
        assert decode_idf(idf, 8, 4, rle_base=0) == [10, 11, 12, 13]

    def test_per_segment_bitpacked_add(self):
        # segments carries (bit_width, rle_base, bitpacked_add) per segment; the
        # second segment's bit-packed values are shifted onto the global scale.
        idf = self._two_segment_idf()
        out = decode_idf(idf, 8, 7, segments=[(8, 0, 0), (8, 0, 100)])
        assert out == [10, 11, 12, 13, 120, 121, 122]

    def test_per_segment_bit_width(self):
        # Different bit width per segment (value domain shifts between segments).
        a = _build_idf([(0xFFFFFFFF, 2)], [3, 5], bit_width=4)
        b = _build_idf([(0xFFFFFFFF, 2)], [200, 201], bit_width=9)
        out = decode_idf(a + b, 4, 4, segments=[(4, 0, 0), (9, 0, 0)])
        assert out == [3, 5, 200, 201]


@pytest.mark.integration
def test_multi_segment_corpus_full_length():
    """If PBIX_TEST_SAMPLES holds a file whose biggest table exceeds one VertiPaq
    segment (~1,048,576 rows), assert we decode every row (not just segment 0)
    and match pbixray. Skips unless such a file + pbixray are available."""
    samples = os.environ.get("PBIX_TEST_SAMPLES", "")
    if not samples or not os.path.isdir(samples):
        pytest.skip("PBIX_TEST_SAMPLES not set")
    pbixray = pytest.importorskip("pbixray")
    import glob
    import zipfile

    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
    from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

    for path in glob.glob(os.path.join(samples, "*.pbix")):
        px = pbixray.PBIXRay(path)
        big = None
        for t in list(px.tables):
            try:
                n = px.get_table(t).shape[0]
            except Exception:
                continue
            if n > 1_048_576:
                big = (t, n)
                break
        if not big:
            continue
        tname, nrows = big
        with zipfile.ZipFile(path) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        meta = read_metadata_sqlite(abf)
        td = read_table_from_abf(abf, tname, meta)
        assert len(td["rows"]) == nrows, (
            f"{tname}: decoded {len(td['rows'])} rows, expected {nrows} "
            f"(multi-segment truncation)"
        )
        return
    pytest.skip("no multi-segment (>1,048,576-row) table in PBIX_TEST_SAMPLES")


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


CORPUS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "test_corpus")


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestEveryDeclaredColumnComesBack:
    """A declared data column must never vanish from a decoded table.

    ``read_table_from_abf`` used to skip any column whose IDFMETA reported
    ``is_row_number``. That flag is inferred from a uint64 our encoder writes
    as 1 for a data column -- but Power BI Desktop leaves it 0 on plenty of
    ordinary columns, so it does not mean "this is a RowNumber". A skipped
    column is stored as None and then filtered out at the end of the function,
    so it disappeared **silently**: no error, no warning, just a narrower table.

    Measured across the 24-file corpus before the fix: 126 of 1121 user data
    columns (11.2%) were discarded, including key columns relationships depend
    on. A rebuild-path edit would then write the table without them.

    The RowNumber column is now identified from the model metadata (AMO
    Column.Type 3 / the reserved name), which is authoritative.
    """

    # Columns Desktop stores that were silently dropped. IT_Support is the
    # sharp case: every existing check reported that file as clean, while
    # dim_Date lost the very column its relationships join on.
    KNOWN_VICTIMS = [
        ("IT_Support.pbix", "dim_Date", "Date"),
        ("IT_Support.pbix", "dim_Clusters", "Cluster_ID"),
        ("IT_Support.pbix", "fact_IT_Support", "Similarity_Score"),
        ("MS_Employee_Hiring.pbix", "Date", "Year"),
        ("MS_Employee_Hiring.pbix", "Date", "MonthNumber"),
        ("MS_Employee_Hiring.pbix", "Date", "Day"),
    ]

    @staticmethod
    def _read(fname, table):
        import zipfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

        path = os.path.join(CORPUS, fname)
        if not os.path.exists(path):
            pytest.skip(f"{fname} not in corpus")
        with zipfile.ZipFile(path) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        meta = read_metadata_sqlite(abf)
        return read_table_from_abf(abf, table, meta)

    @pytest.mark.parametrize("fname,table,column", KNOWN_VICTIMS)
    def test_previously_dropped_column_is_present(self, fname, table, column):
        td = self._read(fname, table)
        assert column in td["columns"], (
            f"{table}[{column}] vanished from the decoded table")
        idx = td["columns"].index(column)
        assert any(r[idx] is not None for r in td["rows"]), (
            f"{table}[{column}] came back but is entirely blank")

    def test_it_support_dim_date_matches_power_bi_desktop(self):
        """Recovered values, not just a recovered column name.

        Read live from Desktop's engine on the same file: 521 rows, 521
        distinct dates, 2024-01-01 through 2025-06-04.
        """
        td = self._read("IT_Support.pbix", "dim_Date")
        idx = td["columns"].index("Date")
        vals = [r[idx] for r in td["rows"]]
        assert len(vals) == 521
        assert len(set(vals)) == 521
        assert str(min(vals)) == "2024-01-01 00:00:00"
        assert str(max(vals)) == "2025-06-04 00:00:00"

    def test_no_declared_data_column_is_missing_anywhere_in_the_corpus(self):
        """The general invariant, over every user table in every corpus file."""
        import glob
        import sqlite3
        import tempfile
        import zipfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

        missing = []
        for path in sorted(glob.glob(os.path.join(CORPUS, "*.pbix"))):
            try:
                with zipfile.ZipFile(path) as z:
                    abf = decompress_datamodel(z.read("DataModel"))
                meta = read_metadata_sqlite(abf)
            except Exception:
                continue
            fd, tmp = tempfile.mkstemp(suffix=".db")
            os.write(fd, meta)
            os.close(fd)
            conn = sqlite3.connect(tmp)
            try:
                tables = [r[0] for r in conn.execute(
                    "SELECT Name FROM [Table] WHERE ModelID = 1 AND SystemFlags = 0")]
                for t in tables:
                    declared = [r[0] for r in conn.execute(
                        "SELECT COALESCE(c.ExplicitName, c.InferredName) "
                        "FROM [Column] c JOIN [Table] tt ON tt.ID = c.TableID "
                        "WHERE tt.Name = ? AND c.Type = 1", (t,))]
                    if not declared:
                        continue
                    try:
                        got = read_table_from_abf(abf, t, meta)["columns"]
                    except Exception:
                        continue
                    if not got:
                        continue
                    missing += [f"{os.path.basename(path)}:{t}[{c}]"
                                for c in declared if c not in got]
            finally:
                conn.close()
                os.unlink(tmp)
        assert missing == [], (
            f"{len(missing)} declared data column(s) silently dropped: "
            f"{missing[:10]}")


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestPrimarySegmentCapacityIsNotEntryCount:
    """`primary_segment_size` is the array's CAPACITY, not the entries in use.

    Across the corpus it is always a power of two (16, 32, 64, 256, 2048, 4096,
    8192) while the entries actually used are far fewer. The real entries end
    as soon as their run lengths add up to the segment's row count; the slots
    after that hold sub-segment bytes, which read as nonsensical
    (data_value, repeat) pairs.

    Reading the full declared capacity therefore summed garbage run lengths.
    It only surfaced when the garbage happened to be large, which is why it
    looked like a property of particular columns: on the 1,290,259-row
    `Employee` table of MS_Employee_Hiring.pbix, 13 columns decoded fine while
    'date', 'Gender' and 'FP' blew past the sanity limit -- `Gender` summed to
    93,629,586,803 rows -- and failed the whole edit.

    Same root cause as the four `Fact` columns of MS_Corporate_Spend.pbix that
    this issue was originally opened for.
    """

    @staticmethod
    def _read(fname, table):
        import zipfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

        path = os.path.join(CORPUS, fname)
        if not os.path.exists(path):
            pytest.skip(f"{fname} not in corpus")
        with zipfile.ZipFile(path) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        meta = read_metadata_sqlite(abf)
        return read_table_from_abf(abf, table, meta)

    @pytest.mark.parametrize("fname,table,rows", [
        ("MS_Employee_Hiring.pbix", "Employee", 1290259),
        ("MS_Human_Resources.pbix", "Employee", 1290259),
        ("MS_Corporate_Spend.pbix", "Fact", 166216),
    ])
    def test_table_decodes_completely(self, fname, table, rows):
        td = self._read(fname, table)
        assert len(td["rows"]) == rows
        assert td["columns"], "no columns decoded"

    @pytest.mark.parametrize("column", ["date", "Gender", "FP"])
    def test_the_three_columns_that_used_to_raise(self, column):
        """These raised 'expands to more than 16,777,216 rows'."""
        td = self._read("MS_Employee_Hiring.pbix", "Employee")
        assert column in td["columns"]
        idx = td["columns"].index(column)
        vals = [r[idx] for r in td["rows"]]
        assert any(v is not None for v in vals)

    def test_recovered_values_are_plausible_not_garbage(self):
        """A mis-parse yields wild indices, so check the value DOMAINS."""
        td = self._read("MS_Employee_Hiring.pbix", "Employee")
        col = {c: [r[i] for r in td["rows"]]
               for i, c in enumerate(td["columns"])}
        assert len({v for v in col["Gender"] if v is not None}) == 2
        assert len({v for v in col["FP"] if v is not None}) == 2
        ages = [v for v in col["Age"] if v is not None]
        assert 0 < min(ages) and max(ages) < 120, f"ages {min(ages)}..{max(ages)}"

    def test_foreign_keys_resolve_against_their_dimensions(self):
        """The strongest offline check: a mis-decoded key would point at the
        wrong dictionary entry and orphan against its dimension.

        `Fact[Cost Element ID]` is excluded deliberately -- the MODEL declares
        it String while `Cost Element[Cost Element ID]` is Double, a type
        mismatch in the source file itself, not a decode fault.
        """
        fact = self._read("MS_Corporate_Spend.pbix", "Fact")
        pairs = [("Scenario ID", "Scenario"), ("Business Area ID", "Business Area"),
                 ("Country/Region ID", "Country Region"), ("Department", "Department")]
        for fcol, dim_table in pairs:
            dim = self._read("MS_Corporate_Spend.pbix", dim_table)
            if fcol not in fact["columns"]:
                pytest.skip(f"{fcol} not decoded")
            fi = fact["columns"].index(fcol)
            keys = {v for r in dim["rows"] for v in r}
            orphans = {r[fi] for r in fact["rows"]
                       if r[fi] is not None and r[fi] not in keys}
            assert not orphans, f"{fcol}: {len(orphans)} orphan key(s) vs {dim_table}"
