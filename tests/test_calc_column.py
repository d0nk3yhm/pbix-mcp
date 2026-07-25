"""Regression tests for issues-13 item 2: authoring calculated columns.

`pbix_datamodel_add_calculated_column` evaluates a row-context DAX expression,
materializes the values into VertiPaq, and stamps the column as a Desktop-shape
calculated column (Type=2 + Expression) so the service recomputes it on refresh.
Expressions our per-row engine can't reproduce faithfully (aggregations,
CALCULATE, RELATED, cross-table) are REFUSED rather than stored wrong.
"""
import json
import os
import sqlite3
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.dax.calc_tables import (
    calc_column_unsupported_reason,
    evaluate_row_context_column,
)
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


# ---------------------------------------------------------------------------
# Reliability gate + evaluator (pure)
# ---------------------------------------------------------------------------
class TestGate:
    @pytest.mark.parametrize("expr", [
        "Sales[Amount] - Sales[Cost]",
        "Sales[Amount] * 1.25",
        'IF(Sales[Amount] > 100, "High", "Low")',
        'Sales[Cat] & "-" & FORMAT(Sales[Amount], "0")',
        "DIVIDE(Sales[Amount] - Sales[Cost], Sales[Amount])",
        "ROUND(Sales[Amount] * 0.1, 2)",
    ])
    def test_allowed(self, expr):
        assert calc_column_unsupported_reason(expr, "Sales") is None

    @pytest.mark.parametrize("expr,frag", [
        ("SUM(Sales[Amount])", "SUM"),
        ("CALCULATE(SUM(Sales[Amount]))", "CALCULATE"),
        ("RANKX(ALL(Sales), Sales[Amount])", "RANKX"),
        ("RELATED(Products[Name])", "another table"),
        ("Sales[Amount] + Other[Y]", "another table"),
        ("", "empty"),
    ])
    def test_refused(self, expr, frag):
        reason = calc_column_unsupported_reason(expr, "Sales")
        assert reason is not None and frag in reason

    def test_evaluator_values(self):
        cols = ["Amount", "Cost"]
        rows = [[100.0, 60.0], [200.0, 150.0]]
        tables = {"Sales": {"columns": cols, "rows": rows}}
        vals, err = evaluate_row_context_column(
            cols, rows, "Sales[Amount] - Sales[Cost]", "Sales", tables, [])
        assert err is None and vals == [40.0, 50.0]

    def test_evaluator_refuses_unresolved(self):
        cols = ["Amount"]
        rows = [[1.0], [2.0]]
        tables = {"Sales": {"columns": cols, "rows": rows}}
        vals, err = evaluate_row_context_column(
            cols, rows, "Sales[Amount] + Sales[Nope]", "Sales", tables, [])
        assert vals is None and err is not None


# ---------------------------------------------------------------------------
# End-to-end tool
# ---------------------------------------------------------------------------
def _read_all_columns(pbix_path, table):
    """Read a table INCLUDING Type=2 calc columns (verify stored values)."""
    import pbix_mcp.formats.vertipaq_decoder as vd
    src = open(vd.__file__).read().replace(
        "c.Type IN (1, 3, 4)", "c.Type IN (1, 2, 3, 4)")
    ns: dict = {}
    exec(compile(src, vd.__file__, "exec"), ns)
    with zipfile.ZipFile(pbix_path) as z:
        abf = decompress_datamodel(z.read("DataModel"))
    meta = read_metadata_sqlite(abf)
    td = ns["read_table_from_abf"](abf, table, meta)
    return td["columns"], td["rows"]


def _column_meta(alias, table, column):
    dm = os.path.join(server._open_files[alias]["work_dir"], "DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(open(dm, "rb").read()))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        c = sqlite3.connect(tmp)
        row = c.execute(
            "SELECT col.Type, col.ExplicitDataType, col.InferredDataType, "
            "col.Expression, col.SourceColumn FROM [Column] col "
            "JOIN [Table] t ON col.TableID = t.ID "
            "WHERE t.Name = ? AND col.ExplicitName = ?", (table, column)
        ).fetchone()
        c.close()
        return row
    finally:
        os.unlink(tmp)


@pytest.fixture
def sales_model(tmp_path):
    p = str(tmp_path / "cc.pbix")
    b = PBIXBuilder("CC")
    b.add_table("Sales", [
        {"name": "Product", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
        {"name": "Cost", "data_type": "Double"},
    ], rows=[
        {"Product": "Widget", "Amount": 100.0, "Cost": 60.0},
        {"Product": "Gadget", "Amount": 200.0, "Cost": 150.0},
        {"Product": "Doohickey", "Amount": 50.0, "Cost": 20.0},
    ])
    b.add_page("P")
    b.save(p)
    return p


class TestAddCalculatedColumn:
    def test_materializes_and_stamps(self, sales_model):
        alias = "cc_basic"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Margin", "Sales[Amount] - Sales[Cost]"))
            assert out["success"], out
            server.pbix_save(alias, sales_model, overwrite=True, backup=False)

            typ, edt, idt, expr, srccol = _column_meta(alias, "Sales", "Margin")
            assert typ == 2                      # calculated column
            assert edt == 1                      # ExplicitDataType Automatic
            assert idt == 8                      # InferredDataType Double
            assert expr == "Sales[Amount] - Sales[Cost]"
            assert srccol is None

            cols, rows = _read_all_columns(sales_model, "Sales")
            mi = cols.index("Margin")
            assert [r[mi] for r in rows] == [40.0, 50.0, 30.0]
        finally:
            server.pbix_close(alias)

    def test_calc_column_referencing_calc_column(self, sales_model):
        alias = "cc_chain"
        server.pbix_open(sales_model, alias)
        try:
            assert json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Margin",
                "Sales[Amount] - Sales[Cost]"))["success"]
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "MarginPct",
                "DIVIDE(Sales[Margin], Sales[Amount])", "Double"))
            assert out["success"], out
            server.pbix_save(alias, sales_model, overwrite=True, backup=False)

            # both calc columns present + correct
            assert _column_meta(alias, "Sales", "Margin")[0] == 2
            assert _column_meta(alias, "Sales", "MarginPct")[0] == 2
            cols, rows = _read_all_columns(sales_model, "Sales")
            mi, pi = cols.index("Margin"), cols.index("MarginPct")
            assert [r[mi] for r in rows] == [40.0, 50.0, 30.0]
            assert [round(r[pi], 4) for r in rows] == [0.4, 0.25, 0.6]
        finally:
            server.pbix_close(alias)

    def test_explicit_int_type(self, sales_model):
        alias = "cc_int"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "AmtInt", "Sales[Amount]", "Int64"))
            assert out["success"], out
            assert _column_meta(alias, "Sales", "AmtInt")[2] == 6  # Int64
        finally:
            server.pbix_close(alias)

    @pytest.mark.parametrize("expr,code", [
        ("SUM(Sales[Amount])", "UNSUPPORTED_CALC"),
        ("RELATED(Products[X])", "UNSUPPORTED_CALC"),
        ("CALCULATE(SUM(Sales[Amount]))", "UNSUPPORTED_CALC"),
    ])
    def test_refuses_unsupported(self, sales_model, expr, code):
        alias = "cc_ref"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "X", expr))
            assert out["success"] is False
            assert out["error_code"] == code
        finally:
            server.pbix_close(alias)

    def test_refuses_duplicate_column(self, sales_model):
        alias = "cc_dup"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Amount", "Sales[Cost] * 2"))
            assert out["success"] is False
            assert out["error_code"] == "COLUMN_EXISTS"
        finally:
            server.pbix_close(alias)

    def test_refuses_measure_name_collision(self, sales_model):
        alias = "cc_meas"
        server.pbix_open(sales_model, alias)
        try:
            server.pbix_datamodel_add_measure(
                alias, "Sales", "Total", "SUM(Sales[Amount])")
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Total", "Sales[Amount] * 2"))
            assert out["success"] is False
            assert out["error_code"] == "NAME_COLLIDES_MEASURE"
        finally:
            server.pbix_close(alias)

    def test_invalid_data_type(self, sales_model):
        alias = "cc_badtype"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "X", "Sales[Amount]", "banana"))
            assert out["success"] is False
            assert out["error_code"] == "INVALID_DATA_TYPE"
        finally:
            server.pbix_close(alias)

    def test_measures_survive(self, sales_model):
        """Adding a calc column preserves existing measures (rebuild path)."""
        alias = "cc_meas2"
        server.pbix_open(sales_model, alias)
        try:
            server.pbix_datamodel_add_measure(
                alias, "Sales", "Total Sales", "SUM(Sales[Amount])")
            assert json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "Margin",
                "Sales[Amount] - Sales[Cost]"))["success"]
            server.pbix_save(alias, sales_model, overwrite=True, backup=False)
            dm = os.path.join(server._open_files[alias]["work_dir"], "DataModel")
            meta = read_metadata_sqlite(
                decompress_datamodel(open(dm, "rb").read()))
            fd, tmp = tempfile.mkstemp(suffix=".db")
            os.write(fd, meta)
            os.close(fd)
            try:
                c = sqlite3.connect(tmp)
                m = c.execute(
                    "SELECT Expression FROM Measure WHERE Name='Total Sales'"
                ).fetchone()
                c.close()
                assert m and m[0] == "SUM(Sales[Amount])"
            finally:
                os.unlink(tmp)
        finally:
            server.pbix_close(alias)
