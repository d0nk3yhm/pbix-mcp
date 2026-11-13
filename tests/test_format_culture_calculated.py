"""FORMAT's culture outside a measure: calculated columns and tables, and a
calculation item's format-string expression.

A measure's FORMAT follows the model's culture (``Model.Culture``); the
calculated-column and calculated-table paths built their contexts without it,
so an en-AU model's calculated column printed en-US dates while its measures
printed en-AU ones. NOT measured on Desktop for columns and tables: their
culture is presumed the model's, as a measure's. The en-AU values are the ones
measured for FORMAT(..., "en-AU") (tests/test_format_named_dates_by_culture.py).

A refused FORMAT (a named date format in a culture whose patterns the fork does
not hold) is BLANK and noted unsupported. Inside a calculation item's
format-string expression, ``"x " & FORMAT(...)`` then gave the truncated "x ";
the measure now keeps no item format and names the refusal.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime

import pytest

from pbix_mcp.dax import calc_tables as ct
from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

SHORT = 'FORMAT(DATE(2026,1,6), "Short Date")'
TABLE_EXPR = f'ROW("d", {SHORT})'


def _metadata(tmp_path, culture):
    path = tmp_path / "meta.db"
    con = sqlite3.connect(str(path))
    con.execute("CREATE TABLE [Table] (ID INTEGER, Name TEXT, ModelID INTEGER)")
    con.execute("CREATE TABLE [Column] (ID INTEGER PRIMARY KEY, ExplicitName TEXT, "
                "Expression TEXT, TableID INTEGER)")
    con.execute("CREATE TABLE [Model] (ID INTEGER, Culture TEXT)")
    con.execute("INSERT INTO [Model] VALUES (1, ?)", (culture,))
    con.execute("INSERT INTO [Table] VALUES (1, 'T', 1)")
    con.execute("INSERT INTO [Column] (ExplicitName, Expression, TableID) VALUES ('F', ?, 1)", (SHORT,))
    con.commit()
    con.close()
    return path.read_bytes()


def _table():
    return {"T": {"columns": ["D"], "rows": [[datetime(2026, 1, 6)]]}}


class TestTheLoadPath:
    @pytest.mark.parametrize("culture,want", [("en-AU", "6/01/2026"), ("en-US", "1/6/2026"), (None, "1/6/2026")])
    def test_a_calculated_column(self, tmp_path, culture, want):
        tables = ct._evaluate_calculated_columns(_table(), _metadata(tmp_path, "x"), [], culture)
        assert tables["T"]["rows"][0][-1] == want

    @pytest.mark.parametrize("culture,want", [("en-AU", "6/01/2026"), ("en-US", "1/6/2026")])
    def test_a_calculated_table(self, culture, want):
        got = ct._evaluate_table_expression(TABLE_EXPR, "X", {"columns": ["d"], "calc_columns": []},
                                            _table(), [], culture)
        assert got["rows"] == [[want]]

    @pytest.mark.parametrize("culture,want", [("en-AU", "6/01/2026"), ("en-US", "1/6/2026")])
    def test_load_calculated_tables(self, monkeypatch, tmp_path, culture, want):
        import zipfile

        from pbix_mcp.formats import abf_rebuild, datamodel_roundtrip
        path = tmp_path / "meta.db"
        con = sqlite3.connect(str(path))
        con.execute("CREATE TABLE [Table] (ID INTEGER, Name TEXT, ModelID INTEGER)")
        con.execute("CREATE TABLE [Partition] (TableID INTEGER, Type INTEGER, QueryDefinition TEXT)")
        con.execute("CREATE TABLE [Column] (ID INTEGER PRIMARY KEY, ExplicitName TEXT, "
                    "Expression TEXT, TableID INTEGER)")
        con.execute("INSERT INTO [Table] VALUES (1, 'T', 1), (2, 'X', 1)")
        con.execute("INSERT INTO [Partition] VALUES (2, 2, ?)", (TABLE_EXPR,))
        con.execute("INSERT INTO [Column] (ExplicitName, Expression, TableID) VALUES ('F', ?, 1), ('d', NULL, 2)",
                    (SHORT,))
        con.commit()
        con.close()
        meta = path.read_bytes()
        pbix = tmp_path / "m.pbix"
        with zipfile.ZipFile(pbix, "w") as z:
            z.writestr("DataModel", b"x")
        monkeypatch.setattr(datamodel_roundtrip, "decompress_datamodel", lambda b: b)
        monkeypatch.setattr(abf_rebuild, "read_metadata_sqlite", lambda b: meta)
        tables = ct.load_calculated_tables(str(pbix), _table(), [], culture=culture)
        assert tables["X"]["rows"] == [[want]]
        assert tables["T"]["rows"][0][-1] == want

    def test_the_model_culture_reaches_the_load(self, monkeypatch, tmp_path):
        from pbix_mcp import server as S
        seen = {}

        def fake_load(path, tables, relationships=None, culture=None):
            seen["culture"] = culture
            return tables
        monkeypatch.setattr(ct, "load_calculated_tables", fake_load)
        monkeypatch.setattr(S, "_read_model_culture", lambda alias: "en-AU")
        p = str(tmp_path / "c.pbix")
        tables = [{"name": "T", "columns": [{"name": "A", "data_type": "Double"}], "rows": [{"A": 1.0}]}]
        assert json.loads(S.pbix_create(p, "cu323", json.dumps(tables)))["success"]
        try:
            S._dax_cache.pop("cu323", None)
            ctx = S._get_dax_context("cu323")
            assert seen["culture"] == "en-AU"
            assert ctx["culture"] == "en-AU"
        finally:
            S._open_files.pop("cu323", None)
            S._dax_cache.pop("cu323", None)


class TestTheAuthoringPaths:
    @pytest.mark.parametrize("culture,want", [("en-AU", "6/01/2026"), ("en-US", "1/6/2026")])
    def test_a_row_context_column(self, culture, want):
        vals, err = ct.evaluate_row_context_column(
            ["D"], [[datetime(2026, 1, 6)]], 'FORMAT(T[D], "Short Date")', "T", _table(), [],
            culture=culture)
        assert err is None
        assert vals == [want]

    @pytest.mark.parametrize("culture,want", [("en-AU", "6/01/2026"), ("en-US", "1/6/2026")])
    def test_a_table_expression(self, culture, want):
        result, err = ct.evaluate_calc_table_expression(TABLE_EXPR, _table(), {}, [], culture=culture)
        assert err is None
        assert result["rows"] == [[want]]

    def test_a_refused_format_refuses_the_column(self):
        vals, err = ct.evaluate_row_context_column(
            ["D"], [[datetime(2026, 1, 6)]], 'FORMAT(T[D], "Short Date")', "T", _table(), [],
            culture="de-DE")
        assert vals is None and "FORMAT" in err

    @pytest.fixture()
    def built(self, tmp_path):
        from pbix_mcp import server as S
        alias = "cu323b"
        p = str(tmp_path / "c.pbix")
        tables = [{"name": "T", "columns": [{"name": "D", "data_type": "DateTime"}],
                   "rows": [{"D": "2026-01-06T00:00:00"}]}]
        assert json.loads(S.pbix_create(p, alias, json.dumps(tables)))["success"]
        assert json.loads(S.pbix_set_model_culture(alias, "en-AU"))["success"]
        yield S, alias
        S._open_files.pop(alias, None)
        S._dax_cache.pop(alias, None)

    def _cells(self, S, alias, table):
        S._dax_cache.pop(alias, None)
        t = S._get_dax_context(alias)["tables"][table]
        return t["columns"], t["rows"]

    def test_an_en_au_model_column_through_the_tool(self, built):
        S, alias = built
        r = json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "T", "F", 'FORMAT(T[D], "Short Date")'))
        assert r["success"], r
        cols, rows = self._cells(S, alias, "T")
        assert rows[0][cols.index("F")] == "6/01/2026"

    def test_an_en_au_model_table_through_the_tool(self, built):
        S, alias = built
        r = json.loads(S.pbix_datamodel_add_calculated_table(alias, "X", TABLE_EXPR))
        assert r["success"], r
        cols, rows = self._cells(S, alias, "X")
        assert rows[0][cols.index("d")] == "6/01/2026"


class TestItemFormatStrings:
    T = {"columns": ["v"], "rows": [[1.0]]}

    def _run(self, culture, measure="SUM(T[v])", fmt=f'"0 x " & {SHORT}'):
        m = {"S": measure}
        group = {"table": "CG", "column": "Name", "precedence": 0,
                 "items": [{"name": "Dated", "expression": "SELECTEDMEASURE()", "ordinal": 0,
                            "format_string": fmt}]}
        tables = {"T": self.T, "CG": {"columns": ["Name", "Ordinal"], "rows": [["Dated", 0]]}}
        de.set_calculation_groups(m, [group], {})
        de._engine.unsupported_functions.clear()
        de._engine.unsupported_by_measure.clear()
        return de.evaluate_format_strings(["S"], tables, m, {"CG.Name": ["Dated"]}, culture=culture)

    def test_a_known_culture_formats(self):
        # a control: it passes without the fix too
        assert self._run("en-AU") == {"S": "0 x 6/01/2026"}

    def test_a_refused_format_gives_no_item_format(self):
        got = self._run("de-DE")
        assert "S" not in got
        assert any("FORMAT" in f for f in de._engine.unsupported_by_measure.get("S", ()))

    def test_a_measure_referenced_by_the_format_string_is_its_own(self):
        m = {"S": "SUM(T[v])", "Label": 'FORMAT(DATE(2026,1,6), "Short Date", "de-DE")'}
        group = {"table": "CG", "column": "Name", "precedence": 0,
                 "items": [{"name": "Dated", "expression": "SELECTEDMEASURE()", "ordinal": 0,
                            "format_string": '"0 x " & [Label]'}]}
        tables = {"T": self.T, "CG": {"columns": ["Name", "Ordinal"], "rows": [["Dated", 0]]}}
        de.set_calculation_groups(m, [group], {})
        got = de.evaluate_format_strings(["S"], tables, m, {"CG.Name": ["Dated"]})
        assert "S" not in got

    @pytest.mark.parametrize("measure", [
        "COALESCE(NOSUCHFN(1), 7)",
        'LEN(FORMAT(DATE(2026,1,6), "Short Date", "de-DE")) + 7',
    ])
    def test_what_the_measure_notes_keeps_the_format(self, measure):
        # SELECTEDMEASURE()'s own unsupported notes are the measure's, not the
        # format string's: the format string still decides
        got = self._run(None, measure=measure, fmt='IF(SELECTEDMEASURE() > 5, "#,0.00", "0")')
        assert got == {"S": "#,0.00"}
        assert de._engine.unsupported_functions          # the measure's note stands
