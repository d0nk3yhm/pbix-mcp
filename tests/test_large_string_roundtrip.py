"""
Regression tests for round-2 issue #8: large / high-cardinality String columns.

A String column whose character store crosses the uncompressed->Huffman
threshold (8192 chars) switches to the compressed dictionary path. These tests
lock in that:

  * the compressed path round-trips EXACTLY (encoder <-> decoder), across the
    boundary and at high cardinality, including non-latin charsets and the
    multi-page regime;
  * building a real model with such a column and reading it back preserves every
    value (no silent blanking / column drop) — the reporter's data-loss surface;
  * if a column's VertiPaq files exist but cannot be decoded (e.g. xmhuffman
    missing, or a corrupt store), the reader FAILS LOUD instead of silently
    dropping the column;
  * pbix_doctor labels plain Import tables (Partition.Type=4) correctly and does
    NOT count them as calculated (Type=2).

The 891-distinct case here mirrors the Titanic "Name" column that regressed in a
downstream fork; the D: compressed encoder is additionally Power BI Desktop
verified (a 891-distinct compressed store opens clean).
"""

import os

import pytest

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.errors import InvalidPBIXError
from pbix_mcp.formats.model_reader import ModelReader
from pbix_mcp.formats.vertipaq_decoder import decode_dictionary
from pbix_mcp.formats.vertipaq_encoder import (
    _COMPRESS_CHAR_THRESHOLD,
    _encode_string_dictionary,
)

pytestmark = pytest.mark.unit


def _medium_strings(n):
    # ~22 chars each -> crosses the 8192-char compress threshold around N~=356
    return [f"unique value number {i:05d}" for i in range(n)]


class TestCompressedDictRoundtrip:
    """Encoder <-> decoder round-trip for the compressed string path."""

    @pytest.mark.parametrize("n", [300, 356, 357, 400, 512, 900, 5000])
    def test_medium_string_exact_roundtrip(self, n):
        strings = _medium_strings(n)
        blob = _encode_string_dictionary(strings)
        _dtype, vals = decode_dictionary(blob)
        assert vals == strings, f"round-trip mismatch at N={n}"
        assert all(v for v in vals), "no value may decode to blank"

    def test_boundary_actually_switches_to_compressed(self):
        # Derive N from the real threshold so this stays correct if the medium
        # string width or the threshold changes.
        per = len(_medium_strings(1)[0]) + 1  # chars-per-string incl. null
        n_below = _COMPRESS_CHAR_THRESHOLD // per
        below = _medium_strings(n_below)
        above = _medium_strings(n_below + 40)
        assert sum(len(s) + 1 for s in below) <= _COMPRESS_CHAR_THRESHOLD
        assert sum(len(s) + 1 for s in above) > _COMPRESS_CHAR_THRESHOLD
        # both must round-trip regardless of which path they take
        for strings in (below, above):
            _t, vals = decode_dictionary(_encode_string_dictionary(strings))
            assert vals == strings

    @pytest.mark.parametrize(
        "label,strings",
        [
            ("cjk", [f"乗客の名前 番号 {i}" for i in range(500)]),
            ("emoji", [f"passenger \U0001F600\U0001F6A2 num {i}" for i in range(500)]),
            ("latin1", [f"Passbegér Ñoño Müller #{i}" for i in range(500)]),
            ("whitespace", [f'  tab\tand "quote" ,comma; #{i}' for i in range(300)]),
        ],
    )
    def test_charset_exact_roundtrip(self, label, strings):
        _t, vals = decode_dictionary(_encode_string_dictionary(strings))
        assert vals == strings, f"charset {label} did not round-trip"

    def test_multi_page_roundtrip(self):
        # Long strings that force >1 compressed page (page cap 2^19 chars).
        strings = [
            f"padded passenger descriptive entry number {i:07d} tail"
            for i in range(20000)
        ]
        _t, vals = decode_dictionary(_encode_string_dictionary(strings))
        assert vals == strings
        assert len(vals) == 20000


class TestEndToEndModelRoundtrip:
    """Build a real model with a high-cardinality String column and read it back."""

    @pytest.mark.parametrize("n", [300, 400, 891, 2000])
    def test_string_column_preserved(self, n, tmp_path):
        names = _medium_strings(n)
        rows = [{"Name": names[i], "Id": i} for i in range(n)]
        cols = [
            {"name": "Name", "data_type": "String"},
            {"name": "Id", "data_type": "Int64"},
        ]
        path = str(tmp_path / f"big_{n}.pbix")
        b = PBIXBuilder("Big")
        b.add_table("T", cols, rows=rows)
        b.add_page("Page 1")
        b.save(path, validate=True)

        td = ModelReader(path).get_table("T", max_rows=0)
        assert "Name" in td["columns"], "String column must not be dropped"
        ci = td["columns"].index("Name")
        got = [r[ci] for r in td["rows"]]
        assert got == names, f"String values lost/reordered at N={n}"
        assert all(v for v in got), "no value may read back blank"


class TestFailLoudOnUndecodableColumn:
    """The reader must raise, not silently drop, when a data column won't decode."""

    def test_dictionary_decode_failure_raises(self, tmp_path, monkeypatch):
        names = _medium_strings(400)
        rows = [{"Name": names[i], "Id": i} for i in range(400)]
        cols = [
            {"name": "Name", "data_type": "String"},
            {"name": "Id", "data_type": "Int64"},
        ]
        path = str(tmp_path / "boom.pbix")
        b = PBIXBuilder("Boom")
        b.add_table("T", cols, rows=rows)
        b.add_page("Page 1")
        b.save(path, validate=True)

        # Sanity: it reads fine before we sabotage the decoder.
        ok = ModelReader(path).get_table("T", max_rows=0)
        assert "Name" in ok["columns"]

        # Simulate an undecodable compressed page (e.g. xmhuffman missing).
        import pbix_mcp.formats.vertipaq_decoder as vd

        real = vd.decode_dictionary

        def sabotage(bs):
            dtype, vals = real(bs)
            if any(isinstance(v, str) and v.startswith("unique value") for v in vals):
                raise RuntimeError("simulated xmhuffman decode failure")
            return dtype, vals

        monkeypatch.setattr(vd, "decode_dictionary", sabotage)

        with pytest.raises(InvalidPBIXError) as exc:
            ModelReader(path).get_table("T", max_rows=0)
        # The error must name the column and not silently succeed.
        assert "Name" in str(exc.value)


class TestRebuildDoesNotWipeTableOnDecodeFailure:
    """A decode failure during a metadata rebuild must ABORT (leaving the file
    untouched), not silently rebuild the affected table with zero rows.

    Regression for the critical issue that the fail-loud reader raise, combined
    with the rebuild callers' previous ``except Exception: … rows=[]`` fallback,
    escalated a single undecodable column into a whole-table wipe on save.
    """

    def test_rebuild_aborts_and_preserves_file(self, tmp_path, monkeypatch):
        from pbix_mcp import server

        path = str(tmp_path / "rebuild_guard.pbix")
        b = PBIXBuilder("RG")
        b.add_table(
            "Big",
            [{"name": "Name", "data_type": "String"}, {"name": "Id", "data_type": "Int64"}],
            rows=[{"Name": _medium_strings(400)[i], "Id": i} for i in range(400)],
        )
        b.add_table("Small", [{"name": "K", "data_type": "Int64"}], rows=[{"K": 1}, {"K": 2}])
        b.add_page("Page 1")
        b.save(path, validate=True)
        size_before = os.path.getsize(path)

        server.pbix_open(path, alias="rebuild_guard")
        info = server._ensure_open("rebuild_guard")

        import pbix_mcp.formats.vertipaq_decoder as vd

        real = vd.decode_dictionary

        def sabotage(bs):
            dtype, vals = real(bs)
            if any(isinstance(v, str) and v.startswith("unique value") for v in vals):
                raise RuntimeError("simulated compressed-page decode failure")
            return dtype, vals

        monkeypatch.setattr(vd, "decode_dictionary", sabotage)
        try:
            with pytest.raises(InvalidPBIXError) as exc:
                server._rebuild_datamodel(
                    info,
                    extra_measures=[{
                        "table": "Small", "name": "M",
                        "expression": "COUNTROWS(Small)", "description": "",
                        "format_string": None,
                    }],
                )
            assert "Big" in str(exc.value)
        finally:
            monkeypatch.setattr(vd, "decode_dictionary", real)
            try:
                server.pbix_close("rebuild_guard")
            except Exception:
                pass

        # File must be untouched, and Big must keep all 400 rows.
        assert os.path.getsize(path) == size_before, "file changed despite aborted edit"
        td = ModelReader(path).get_table("Big", max_rows=0)
        ci = td["columns"].index("Name")
        assert sum(1 for r in td["rows"] if r[ci]) == 400, "Big rows were wiped"


class TestDoctorCalcTableLabel:
    """pbix_doctor must not label plain Import tables as calculated."""

    def test_import_tables_not_counted_as_calculated(self, tmp_path):
        from pbix_mcp import server

        path = str(tmp_path / "imports.pbix")
        b = PBIXBuilder("Imports")
        for t in ("Alpha", "Beta", "Gamma"):
            b.add_table(
                t,
                [{"name": "K", "data_type": "Int64"}, {"name": "V", "data_type": "String"}],
                rows=[{"K": 1, "V": "a"}, {"K": 2, "V": "b"}],
            )
        b.add_page("Page 1")
        b.save(path, validate=True)

        alias = "doctor_calc_test"
        server.pbix_open(path, alias=alias)
        try:
            out = server.pbix_doctor(alias)
        finally:
            try:
                server.pbix_close(alias)
            except Exception:
                pass

        calc_line = next(
            (ln for ln in out.splitlines() if "Calculated tables" in ln), ""
        )
        assert calc_line, "doctor output missing Calculated tables line"
        # 3 plain import tables -> must report None, not "3 calculated tables"
        assert "None" in calc_line, f"import tables mislabeled: {calc_line!r}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
