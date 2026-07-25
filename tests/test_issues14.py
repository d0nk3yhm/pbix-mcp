"""Regression tests for issues-14.

Covers:
  * the FILTER fix — a bare column reference in a FILTER condition evaluated to
    an unresolved marker, so FILTER returned NO rows (and every measure built
    on it was wrong);
  * `pbix_datamodel_add_calculated_table` — evaluate a table expression,
    materialize the rows, stamp Desktop's calc-table metadata, and REFUSE the
    shapes this engine cannot reproduce;
  * `pbix_evaluate_dax_grouped` — one call returns per-group measure values;
  * `pbix_datamodel_modify_relationship` — in-place relationship edits.
"""
import json
import os
import sqlite3
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.dax import engine as dax_engine
from pbix_mcp.dax.calc_tables import (
    calc_table_unsupported_reason,
    evaluate_calc_table_expression,
)
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
from pbix_mcp.formats.model_reader import ModelReader

SALES = {
    "Sales": {
        "columns": ["Product", "Category", "Amount"],
        "rows": [["Widget", "HW", 100.0], ["Gadget", "EL", 200.0],
                 ["Doo", "HW", 50.0]],
    }
}


def _ev(expr, measures=None):
    eng = dax_engine.DAXEngine()
    ctx = dax_engine.DAXContext(SALES, measures or {}, None, None, None, [])
    r = eng._eval_expr(expr, ctx)
    if isinstance(r, list):
        return [{k: v for k, v in d.items() if not k.startswith("__")}
                for d in r]
    return r


class TestFilterFix:
    def test_bare_column_condition_returns_rows(self):
        rows = _ev("FILTER(Sales, Sales[Amount] > 90)")
        assert [r["Product"] for r in rows] == ["Widget", "Gadget"]

    def test_quoted_table_name(self):
        rows = _ev("FILTER('Sales', Sales[Amount] > 90)")
        assert len(rows) == 2

    def test_text_equality(self):
        rows = _ev('FILTER(Sales, Sales[Category] = "HW")')
        assert [r["Product"] for r in rows] == ["Widget", "Doo"]

    def test_compound_condition(self):
        rows = _ev('FILTER(Sales, Sales[Amount] > 60 && Sales[Category] = "HW")')
        assert [r["Product"] for r in rows] == ["Widget"]

    def test_countrows_over_filter(self):
        assert _ev("COUNTROWS(FILTER(Sales, Sales[Amount] > 90))") == 2

    def test_calculate_with_filter(self):
        assert _ev(
            "CALCULATE(SUM(Sales[Amount]), FILTER(Sales, Sales[Amount] > 90))"
        ) == 300.0

    def test_aggregation_condition_keeps_filter_context_path(self):
        # A condition containing an aggregation is NOT row-substituted: it stays
        # on the pre-existing filter-context route, where SUM() is evaluated
        # within the iterated row's context (so it behaves like a per-row sum).
        # Asserted to pin that this fix did not disturb that path.
        rows = _ev("FILTER(Sales, SUM(Sales[Amount]) > 90)")
        assert [r["Product"] for r in rows] == ["Widget", "Gadget"]


class TestCalcTableEvaluator:
    @pytest.mark.parametrize("expr,cols,nrows", [
        ("TOPN(2, Sales, Sales[Amount])", ["Product", "Category", "Amount"], 2),
        ("FILTER(Sales, Sales[Amount] > 90)",
         ["Product", "Category", "Amount"], 2),
        ("DISTINCT(Sales[Category])", ["Category"], 2),
        ("VALUES(Sales[Product])", ["Product"], 3),
        ("GENERATESERIES(1, 4, 1)", ["Value"], 4),
        ('DATATABLE("Name", STRING, "N", INTEGER, {{"a",1},{"b",2}})',
         ["Name", "N"], 2),
        ('ADDCOLUMNS(Sales, "Margin", Sales[Amount]*0.1)',
         ["Product", "Category", "Amount", "Margin"], 3),
    ])
    def test_supported_shapes(self, expr, cols, nrows):
        assert calc_table_unsupported_reason(expr) is None
        res, err = evaluate_calc_table_expression(expr, SALES)
        assert err is None, err
        assert res["columns"] == cols
        assert len(res["rows"]) == nrows

    def test_datatable_keeps_every_column(self):
        res, err = evaluate_calc_table_expression(
            'DATATABLE("Name", STRING, "N", INTEGER, {{"a",1},{"b",2}})', SALES)
        assert err is None
        assert res["rows"] == [["a", 1], ["b", 2]]

    @pytest.mark.parametrize("expr", [
        'SUMMARIZE(Sales, Sales[Category], "T", SUM(Sales[Amount]))',
        'SELECTCOLUMNS(Sales, "P", Sales[Product])',
        'GROUPBY(Sales, Sales[Category])',
    ])
    def test_lossy_shapes_refused_by_gate(self, expr):
        assert calc_table_unsupported_reason(expr) is not None

    def test_unsupported_function_refused(self):
        res, err = evaluate_calc_table_expression(
            "MEDIANX(Sales, Sales[Amount])", SALES)
        assert res is None and "unsupported" in err

    def test_calendar_date_table(self):
        """Implementing DATE() also made CALENDAR() usable, so a real Date
        dimension can now be authored as a calculated table."""
        res, err = evaluate_calc_table_expression(
            "CALENDAR(DATE(2024,1,1), DATE(2024,1,5))", SALES)
        assert err is None, err
        assert res["columns"] == ["Date"]
        assert len(res["rows"]) == 5

    def test_empty_expression_refused(self):
        assert calc_table_unsupported_reason("") is not None


# ---------------------------------------------------------------------------
# End-to-end tools
# ---------------------------------------------------------------------------
@pytest.fixture
def sales_pbix(tmp_path):
    p = str(tmp_path / "m.pbix")
    b = PBIXBuilder("M")
    b.add_table("Sales", [
        {"name": "Product", "data_type": "String"},
        {"name": "Category", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
        {"name": "Cost", "data_type": "Double"},
    ], rows=[
        {"Product": "W", "Category": "HW", "Amount": 100.0, "Cost": 60.0},
        {"Product": "G", "Category": "EL", "Amount": 200.0, "Cost": 150.0},
        {"Product": "D", "Category": "HW", "Amount": 50.0, "Cost": 20.0},
    ])
    b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
    b.add_page("P")
    b.save(p)
    return p


def _table_meta(alias, tname):
    dm = os.path.join(server._open_files[alias]["work_dir"], "DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(open(dm, "rb").read()))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        c = sqlite3.connect(tmp)
        c.row_factory = sqlite3.Row
        t = c.execute("SELECT ID, SystemFlags FROM [Table] WHERE Name = ?",
                      (tname,)).fetchone()
        if not t:
            return None
        p = c.execute(
            "SELECT Type, SystemFlags, QueryDefinition FROM [Partition] "
            "WHERE TableID = ?", (t["ID"],)).fetchone()
        cols = c.execute(
            "SELECT InferredName, Type, ExplicitDataType, SourceColumn "
            "FROM [Column] WHERE TableID = ? AND Type = 4 ORDER BY ID",
            (t["ID"],)).fetchall()
        out = {"table_system_flags": t["SystemFlags"],
               "partition_type": p["Type"],
               "partition_flags": p["SystemFlags"],
               "query": p["QueryDefinition"],
               "calc_cols": [dict(x) for x in cols]}
        c.close()
        return out
    finally:
        os.unlink(tmp)


class TestAddCalculatedTable:
    def test_desktop_shape_and_values(self, sales_pbix, tmp_path):
        alias = "ct1"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Categories", "DISTINCT(Sales[Category])"))
            assert out["success"], out
            m = _table_meta(alias, "Categories")
            assert m["partition_type"] == 2          # calculated partition
            assert m["partition_flags"] == 2
            assert m["table_system_flags"] == 2
            assert m["query"] == "DISTINCT(Sales[Category])"
            assert [c["InferredName"] for c in m["calc_cols"]] == ["Category"]
            assert m["calc_cols"][0]["ExplicitDataType"] == 1   # Automatic
            assert m["calc_cols"][0]["SourceColumn"] == "[Category]"

            saved = str(tmp_path / "saved.pbix")
            server.pbix_save(alias, saved, overwrite=True, backup=False)
            td = ModelReader(saved).get_table("Categories", max_rows=10)
            assert sorted(r[0] for r in td["rows"]) == ["EL", "HW"]
        finally:
            server.pbix_close(alias)

    def test_second_calc_table_preserves_first(self, sales_pbix, tmp_path):
        alias = "ct2"
        server.pbix_open(sales_pbix, alias)
        try:
            assert json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Cats", "DISTINCT(Sales[Category])"))["success"]
            assert json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Big", "FILTER(Sales, Sales[Amount] > 90)"))["success"]
            assert _table_meta(alias, "Cats")["partition_type"] == 2
            assert _table_meta(alias, "Big")["partition_type"] == 2
            saved = str(tmp_path / "s2.pbix")
            server.pbix_save(alias, saved, overwrite=True, backup=False)
            r = ModelReader(saved)
            assert len(r.get_table("Big", max_rows=10)["rows"]) == 2
            assert len(r.get_table("Cats", max_rows=10)["rows"]) == 2
        finally:
            server.pbix_close(alias)

    def test_composes_with_calculated_columns_both_orders(self, sales_pbix):
        alias = "ct3"
        server.pbix_open(sales_pbix, alias)
        try:
            assert json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Prods", "VALUES(Sales[Product])"))["success"]
            # calc column AFTER a calc table exists (used to be refused)
            assert json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Margin",
                "Sales[Amount] - Sales[Cost]"))["success"]
            # and another calc table after a calc column
            assert json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Cats", "DISTINCT(Sales[Category])"))["success"]
            assert _table_meta(alias, "Prods")["partition_type"] == 2
            assert _table_meta(alias, "Cats")["partition_type"] == 2
        finally:
            server.pbix_close(alias)

    def test_refuses_lossy_expression(self, sales_pbix):
        alias = "ct4"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Bad",
                'SUMMARIZE(Sales, Sales[Category], "T", SUM(Sales[Amount]))'))
            assert out["success"] is False
            assert out["error_code"] == "UNSUPPORTED_CALC_TABLE"
        finally:
            server.pbix_close(alias)

    def test_refuses_duplicate_table_name(self, sales_pbix):
        alias = "ct5"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_table(
                alias, "Sales", "DISTINCT(Sales[Category])"))
            assert out["success"] is False
            assert out["error_code"] == "TABLE_EXISTS"
        finally:
            server.pbix_close(alias)

    def test_refuses_calc_column_on_calc_table(self, sales_pbix):
        alias = "ct6"
        server.pbix_open(sales_pbix, alias)
        try:
            server.pbix_datamodel_add_calculated_table(
                alias, "Prods", "VALUES(Sales[Product])")
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Prods", "X", 'Prods[Product] & "!"'))
            assert out["success"] is False
        finally:
            server.pbix_close(alias)


class TestEvaluateDaxGrouped:
    def test_per_group_values_match_truth(self, sales_pbix):
        alias = "g1"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Category"))
            assert out["success"], out
            d = out["data"]
            got = {g["key"]["Category"]: g["values"]["Total"]
                   for g in d["groups"]}
            assert got == {"HW": 150.0, "EL": 200.0}
            assert d["group_count"] == 2 and d["truncated"] is False
        finally:
            server.pbix_close(alias)

    def test_truncation_reported(self, sales_pbix):
        alias = "g2"
        server.pbix_open(sales_pbix, alias)
        try:
            d = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Product", max_groups=2))["data"]
            assert d["returned"] == 2
            assert d["group_count"] == 3
            assert d["truncated"] is True
        finally:
            server.pbix_close(alias)

    def test_composite_key(self, sales_pbix):
        alias = "g3"
        server.pbix_open(sales_pbix, alias)
        try:
            d = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Category,Sales.Product"))["data"]
            assert d["group_by"] == ["Sales.Category", "Sales.Product"]
            keys = {(g["key"]["Category"], g["key"]["Product"])
                    for g in d["groups"]}
            assert keys == {("HW", "W"), ("EL", "G"), ("HW", "D")}
        finally:
            server.pbix_close(alias)

    def test_missing_group_by(self, sales_pbix):
        alias = "g4"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", ""))
            assert out["success"] is False
            assert out["error_code"] == "INVALID_INPUT"
        finally:
            server.pbix_close(alias)

    def test_unknown_column(self, sales_pbix):
        alias = "g5"
        server.pbix_open(sales_pbix, alias)
        try:
            out = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Nope"))
            assert out["success"] is False
            assert out["error_code"] == "COLUMN_NOT_FOUND"
        finally:
            server.pbix_close(alias)


@pytest.fixture
def pred_pbix(tmp_path):
    p = str(tmp_path / "pred.pbix")
    b = PBIXBuilder("Pred")
    b.add_table("Sales", [
        {"name": "Product", "data_type": "String"},
        {"name": "Region", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
        {"name": "Date", "data_type": "DateTime"},
    ], rows=[
        {"Product": "Widget", "Region": "North", "Amount": 100.0,
         "Date": "2024-01-15T00:00:00"},
        {"Product": "Gadget", "Region": "South", "Amount": 200.0,
         "Date": "2024-02-20T00:00:00"},
        {"Product": "Doohickey", "Region": "North", "Amount": 50.0,
         "Date": "2024-03-05T00:00:00"},
        {"Product": "Gizmo", "Region": "East", "Amount": 400.0,
         "Date": "2024-04-10T00:00:00"},
    ])
    b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
    b.add_page("P")
    b.save(p)
    return p


def _total(alias, fc):
    r = json.loads(server.pbix_evaluate_dax(
        alias, "Total", json.dumps(fc) if fc else "",
        apply_default_filters=False))
    return r.get("results", [{}])[0].get("value")


class TestPredicateFilterContext:
    """filter_context accepts structured predicates, not just In-sets."""

    @pytest.mark.parametrize("fc,expected", [
        (None, 750.0),
        # LIST form must behave EXACTLY as before
        ({"Sales.Region": ["North"]}, 150.0),
        ({"Sales.Region": ["North", "South"]}, 350.0),
        # comparisons
        ({"Sales.Amount": {"op": ">", "value": 100}}, 600.0),
        ({"Sales.Amount": {"op": ">=", "value": 100}}, 700.0),
        ({"Sales.Amount": {"op": "<", "value": 100}}, 50.0),
        ({"Sales.Amount": {"op": "<>", "value": 400}}, 350.0),
        # range
        ({"Sales.Amount": {"between": [100, 200]}}, 300.0),
        # text
        ({"Sales.Region": {"contains": "or"}}, 150.0),
        ({"Sales.Region": {"starts_with": "S"}}, 200.0),
        ({"Sales.Region": {"ends_with": "th"}}, 350.0),
        ({"Sales.Region": {"in": ["North", "East"]}}, 550.0),
        ({"Sales.Region": {"not_in": ["North"]}}, 600.0),
        # dates
        ({"Sales.Date": {"between": ["2024-02-01", "2024-03-31"]}}, 250.0),
        ({"Sales.Date": {"relative_date": {
            "last": 60, "unit": "day", "anchor": "2024-03-10"}}}, 350.0),
        # predicate AND list together
        ({"Sales.Amount": {"op": ">", "value": 100},
          "Sales.Region": ["East"]}, 400.0),
        # two predicate keys on one column are ANDed
        ({"Sales.Amount": {"op": ">", "value": 60, "between": [0, 150]}}, 100.0),
    ])
    def test_predicates(self, pred_pbix, fc, expected):
        alias = "p_" + str(abs(hash(str(fc))))[:6]
        server.pbix_open(pred_pbix, alias)
        try:
            assert _total(alias, fc) == expected
        finally:
            server.pbix_close(alias)

    def test_matcher_list_semantics_unchanged(self):
        m = dax_engine.make_value_matcher(["a", "b"])
        assert m("a") and m("b") and not m("c")
        # values are compared as strings, exactly as before
        assert dax_engine.make_value_matcher([1])("1")

    def test_matcher_rejects_unknown_predicate(self):
        with pytest.raises(ValueError):
            dax_engine.make_value_matcher({"bogus": 1})

    def test_matcher_rejects_bad_operator(self):
        with pytest.raises(ValueError):
            dax_engine.make_value_matcher({"op": "~", "value": 1})("x")

    def test_is_blank(self):
        m = dax_engine.make_value_matcher({"is_blank": True})
        assert m(None) and m("") and not m("x")


class TestGroupedTopN:
    def test_top_n_desc(self, pred_pbix):
        alias = "tn_d"
        server.pbix_open(pred_pbix, alias)
        try:
            d = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Product", top_n=2))["data"]
            assert [g["key"]["Product"] for g in d["groups"]] == \
                ["Gizmo", "Gadget"]
            assert d["order_by"] == "Total"
        finally:
            server.pbix_close(alias)

    def test_bottom_n_asc(self, pred_pbix):
        alias = "tn_a"
        server.pbix_open(pred_pbix, alias)
        try:
            d = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Product", top_n=2, order="asc"))["data"]
            assert [g["key"]["Product"] for g in d["groups"]] == \
                ["Doohickey", "Widget"]
        finally:
            server.pbix_close(alias)

    def test_blank_groups_sink_in_both_directions(self, pred_pbix):
        alias = "tn_b"
        server.pbix_open(pred_pbix, alias)
        try:
            fc = json.dumps({"Sales.Amount": {"op": ">", "value": 75}})
            for order in ("asc", "desc"):
                d = json.loads(server.pbix_evaluate_dax_grouped(
                    alias, "Total", "Sales.Product", filter_context=fc,
                    order_by="Total", order=order))["data"]
                # the filtered-out group has no value and must be last
                assert d["groups"][-1]["values"]["Total"] is None
        finally:
            server.pbix_close(alias)

    def test_invalid_order_by(self, pred_pbix):
        alias = "tn_i"
        server.pbix_open(pred_pbix, alias)
        try:
            out = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Product", order_by="Nope"))
            assert out["success"] is False
            assert out["error_code"] == "INVALID_INPUT"
        finally:
            server.pbix_close(alias)

    def test_top_n_with_predicate_filter(self, pred_pbix):
        alias = "tn_p"
        server.pbix_open(pred_pbix, alias)
        try:
            fc = json.dumps({"Sales.Amount": {"op": ">", "value": 75}})
            d = json.loads(server.pbix_evaluate_dax_grouped(
                alias, "Total", "Sales.Product", filter_context=fc,
                top_n=2))["data"]
            assert [(g["key"]["Product"], g["values"]["Total"])
                    for g in d["groups"]] == [("Gizmo", 400.0),
                                              ("Gadget", 200.0)]
        finally:
            server.pbix_close(alias)


class TestDatePartFunctions:
    """YEAR/MONTH/DAY/QUARTER & co. were not implemented at all — any
    expression using one evaluated to BLANK and reported an unsupported
    function, which blocked date-part calculated columns entirely."""

    def _ev(self, expr):
        eng = dax_engine.DAXEngine()
        ctx = dax_engine.DAXContext({}, {}, None, None, None, [])
        val = eng._eval_expr(expr, ctx)
        assert not eng.unsupported_functions, eng.unsupported_functions
        return val

    @pytest.mark.parametrize("expr,expected", [
        ('YEAR("2024-01-15T00:00:00")', 2024),
        ('MONTH("2024-07-05")', 7),
        ('DAY("2024-07-05")', 5),
        ('QUARTER("2024-01-31")', 1),
        ('QUARTER("2024-07-05")', 3),
        ('QUARTER("2024-12-31")', 4),
        ('HOUR("2024-07-05 13:45:30")', 13),
        ('MINUTE("2024-07-05 13:45:30")', 45),
        ('SECOND("2024-07-05 13:45:30")', 30),
        # 2024-01-15 is a Monday: type 1 -> Sun=1..Sat=7 => 2; type 2 -> Mon=1
        ('WEEKDAY("2024-01-15")', 2),
        ('WEEKDAY("2024-01-15",2)', 1),
        ('WEEKDAY("2024-01-15",3)', 0),
        ('YEAR(DATE(2024,1,15))', 2024),
        # month overflow rolls into the next year, as DAX does
        ('MONTH(DATE(2024,13,1))', 1),
        ('YEAR(DATE(2024,13,1))', 2025),
        # EOMONTH / EDATE incl. leap-year clamping
        ('DAY(EOMONTH("2024-01-15",0))', 31),
        ('MONTH(EOMONTH("2024-01-15",1))', 2),
        ('DAY(EOMONTH("2024-01-15",1))', 29),
        ('DAY(EDATE("2024-01-31",1))', 29),
    ])
    def test_date_parts(self, expr, expected):
        assert self._ev(expr) == expected

    def test_blank_on_non_date(self):
        assert self._ev('YEAR("not a date")') is None


class TestDateHierarchyRecipe:
    """issues-14 item 4: a date drill hierarchy built from supported
    primitives (date-part calc columns + pbix_add_hierarchy)."""

    def test_end_to_end(self, tmp_path):
        p = str(tmp_path / "dh.pbix")
        b = PBIXBuilder("DH")
        b.add_table("Sales", [{"name": "Date", "data_type": "DateTime"},
                              {"name": "Amount", "data_type": "Double"}],
                    rows=[{"Date": "2024-01-15T00:00:00", "Amount": 100.0},
                          {"Date": "2024-07-05T00:00:00", "Amount": 300.0},
                          {"Date": "2025-03-11T00:00:00", "Amount": 400.0}])
        b.add_page("P")
        b.save(p)
        alias = "dhr"
        server.pbix_open(p, alias)
        try:
            for name, dax in [("Year", "YEAR(Sales[Date])"),
                              ("Quarter", "QUARTER(Sales[Date])"),
                              ("Month", "MONTH(Sales[Date])"),
                              ("Day", "DAY(Sales[Date])")]:
                out = json.loads(server.pbix_datamodel_add_calculated_column(
                    alias, "Sales", name, dax, "Int64"))
                assert out["success"], out
            levels = [{"name": n, "column": n}
                      for n in ("Year", "Quarter", "Month", "Day")]
            out = json.loads(server.pbix_add_hierarchy(
                alias, "Sales", "Date Hierarchy", json.dumps(levels)))
            assert out["success"], out

            saved = str(tmp_path / "dh_out.pbix")
            server.pbix_save(alias, saved, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias)

        # hierarchy levels in drill order, calc columns still calculated
        with zipfile.ZipFile(saved) as z:
            meta = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.write(fd, meta)
        os.close(fd)
        try:
            c = sqlite3.connect(tmp)
            c.row_factory = sqlite3.Row
            h = c.execute(
                "SELECT h.ID, h.Name FROM Hierarchy h JOIN [Table] t "
                "ON h.TableID = t.ID WHERE t.Name = 'Sales'").fetchone()
            assert h["Name"] == "Date Hierarchy"
            lv = c.execute(
                "SELECT l.Name FROM Level l WHERE l.HierarchyID = ? "
                "ORDER BY l.Ordinal", (h["ID"],)).fetchall()
            assert [x["Name"] for x in lv] == ["Year", "Quarter", "Month", "Day"]
            calc = [r["ExplicitName"] for r in c.execute(
                "SELECT ExplicitName FROM [Column] col JOIN [Table] t "
                "ON col.TableID = t.ID WHERE t.Name='Sales' AND col.Type=2")]
            assert sorted(calc) == ["Day", "Month", "Quarter", "Year"]
            c.close()
        finally:
            os.unlink(tmp)

    def test_datetime_values_substitute_as_literals(self):
        """A datetime cell must go into the expression QUOTED — a bare
        2024-01-15 00:00:00 is unparseable and broke every date expression."""
        from datetime import datetime as _dt

        from pbix_mcp.dax.calc_tables import evaluate_row_context_column
        cols = ["Date"]
        rows = [[_dt(2024, 1, 15)], [_dt(2025, 7, 5)]]
        tables = {"S": {"columns": cols, "rows": rows}}
        vals, err = evaluate_row_context_column(
            cols, rows, "YEAR(S[Date])", "S", tables, [])
        assert err is None, err
        assert vals == [2024, 2025]


@pytest.fixture
def rel_pbix(tmp_path):
    p = str(tmp_path / "r.pbix")
    b = PBIXBuilder("R")
    b.add_table("Sales", [{"name": "Product", "data_type": "String"},
                          {"name": "Amount", "data_type": "Double"}],
                rows=[{"Product": "W", "Amount": 100.0},
                      {"Product": "G", "Amount": 200.0}])
    b.add_table("Products", [{"name": "Product", "data_type": "String"},
                             {"name": "Cat", "data_type": "String"}],
                rows=[{"Product": "W", "Cat": "HW"},
                      {"Product": "G", "Cat": "EL"}])
    b.add_relationship("Sales", "Product", "Products", "Product")
    b.add_page("P")
    b.save(p)
    return p


def _rel_row(alias):
    dm = os.path.join(server._open_files[alias]["work_dir"], "DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(open(dm, "rb").read()))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        c = sqlite3.connect(tmp)
        r = c.execute(
            "SELECT IsActive, CrossFilteringBehavior, FromCardinality, "
            "ToCardinality FROM Relationship").fetchone()
        c.close()
        return r
    finally:
        os.unlink(tmp)


class TestModifyRelationship:
    def test_toggle_active_and_crossfilter(self, rel_pbix):
        alias = "r1"
        server.pbix_open(rel_pbix, alias)
        try:
            assert _rel_row(alias) == (1, 1, 2, 1)
            out = json.loads(server.pbix_datamodel_modify_relationship(
                alias, "Sales", "Product", "Products", "Product",
                cross_filter_direction="both", is_active="false"))
            assert out["success"], out
            active, xf, _f, _t = _rel_row(alias)
            assert active == 0 and xf == 2
        finally:
            server.pbix_close(alias)

    def test_change_cardinality(self, rel_pbix):
        alias = "r2"
        server.pbix_open(rel_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_modify_relationship(
                alias, "Sales", "Product", "Products", "Product",
                cardinality="1:1"))
            assert out["success"], out
            _a, _x, fcard, tcard = _rel_row(alias)
            assert (fcard, tcard) == (1, 1)
        finally:
            server.pbix_close(alias)

    def test_nothing_to_change(self, rel_pbix):
        alias = "r3"
        server.pbix_open(rel_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_modify_relationship(
                alias, "Sales", "Product", "Products", "Product"))
            assert out["success"] is False
            assert out["error_code"] == "NOTHING_TO_CHANGE"
        finally:
            server.pbix_close(alias)

    def test_not_found(self, rel_pbix):
        alias = "r4"
        server.pbix_open(rel_pbix, alias)
        try:
            out = json.loads(server.pbix_datamodel_modify_relationship(
                alias, "Sales", "Nope", "Products", "Product", is_active="true"))
            assert out["success"] is False
            assert out["error_code"] == "RELATIONSHIP_NOT_FOUND"
        finally:
            server.pbix_close(alias)

    def test_invalid_values(self, rel_pbix):
        alias = "r5"
        server.pbix_open(rel_pbix, alias)
        try:
            for kw in ({"cardinality": "banana"},
                       {"cross_filter_direction": "sideways"},
                       {"is_active": "maybe"}):
                out = json.loads(server.pbix_datamodel_modify_relationship(
                    alias, "Sales", "Product", "Products", "Product", **kw))
                assert out["success"] is False
                assert out["error_code"] == "INVALID_ARGUMENT"
        finally:
            server.pbix_close(alias)


def test_zip_intact_after_calc_table(sales_pbix, tmp_path):
    """A calc-table edit must not damage the package."""
    alias = "z1"
    server.pbix_open(sales_pbix, alias)
    try:
        server.pbix_datamodel_add_calculated_table(
            alias, "Cats", "DISTINCT(Sales[Category])")
        out = str(tmp_path / "z.pbix")
        server.pbix_save(alias, out, overwrite=True, backup=False)
    finally:
        server.pbix_close(alias)
    with zipfile.ZipFile(out) as z:
        assert "DataModel" in z.namelist()
        assert "Report/Layout" in z.namelist()
        assert z.testzip() is None
