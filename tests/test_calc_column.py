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
        ("CALCULATE(SUM(Sales[Amount]))", "CALCULATE"),
        ("RANKX(ALL(Sales), Sales[Amount])", "RANKX"),
        ("RELATED(Products[Name])", "another table"),
        ("Sales[Amount] + Other[Y]", "another table"),
        ("SUMX(Sales, Sales[Amount])", "SUMX"),
        ("", "empty"),
    ])
    def test_refused(self, expr, frag):
        reason = calc_column_unsupported_reason(expr, "Sales")
        assert reason is not None and frag in reason

    @pytest.mark.parametrize("expr", [
        "SUM(Sales[Amount])",
        "MIN(Sales[Amount])",
        "MAX( 'Sales'[Amount] )",
        "(Sales[Amount] - MIN(Sales[Amount])) * 2",
    ])
    def test_plain_aggregate_over_own_column_is_allowed(self, expr):
        """A calculated column has no filter context beyond its own row, so
        MIN('T'[C]) really is the minimum of the whole column on every row.
        Refusing these blocked real files whose only obstacle was an
        expression of the form ([Year] - MIN([Year])) * 12 + [MonthNumber]."""
        assert calc_column_unsupported_reason(expr, "Sales") is None

    @pytest.mark.parametrize("expr,why", [
        # MIN/MAX have a SECOND, scalar overload. Masking it as an aggregate
        # hid its column reference from the row substitution and the engine
        # evaluated a bare reference with no row context: 0 on every row.
        ("MIN(Sales[Amount], 0)", "two-argument scalar MIN"),
        ("MAX(Sales[Amount], 0)", "two-argument scalar MAX"),
        # A bare TABLE argument is invisible to the cross-table check (which
        # only sees `Name[`), to _unresolved_refs and to _refs_any_column.
        ("COUNTROWS(Products)", "table argument, another table"),
        ("COUNTROWS(Sales)", "table argument"),
        # Our implementations disagree with DAX about BLANK/'' counting.
        ("DISTINCTCOUNT(Sales[Amount])", "blank counting differs"),
        ("COUNT(Sales[Amount])", "blank counting differs"),
        # DAX spells these with a DOT; the underscore spellings alone left the
        # real names matching neither set, so nothing refused them.
        ("STDEV.P(Sales[Amount])", "dotted statistical name"),
        ("VAR.S(Sales[Amount])", "dotted statistical name"),
        ("PERCENTILEX.INC(Sales, Sales[Amount], 0.5)", "dotted X-iterator"),
        # An aggregate over a column that does not exist answers 0, not an error.
        ("MIN(Sales[Nope])", "column does not exist"),
        ("MIN('Other'[Amount])", "column of another table"),
    ])
    def test_aggregate_shapes_that_would_store_zeros_are_refused(self, expr, why):
        """Each of these was ACCEPTED by the first cut of the relaxation and
        materialized 0 (or the else-branch) on every row, reporting success."""
        reason = calc_column_unsupported_reason(
            expr, "Sales", ["Amount", "Cost", "Cat", "Product"])
        assert reason is not None, f"{expr} should be refused ({why})"

    def test_a_column_named_after_an_aggregate_is_not_mistaken_for_one(self):
        """`[Total Count (n)]` is a legal column name, not a COUNT( call.

        Scanning the raw text chopped the name in half, which either refused a
        perfectly good expression or -- with an unbalanced paren inside the
        name -- swallowed the closing ']' and blinded the reference check.
        """
        from pbix_mcp.dax.calc_tables import _mask_aggregate_calls
        expr = "'Sales'[Amount] + 'Sales'[Total Count (n)]"
        _, spans = _mask_aggregate_calls(expr)
        assert spans == []
        assert calc_column_unsupported_reason(
            expr, "Sales", ["Amount", "Total Count (n)"]) is None

    def test_a_date_column_compares_equal_to_an_aggregate_of_itself(self):
        """The "earliest record" flag, a standard calc-column idiom.

        Row substitution writes a DateTime as a quoted ISO string while the
        masked aggregate returns a native datetime, so the comparison was never
        equal and the flag was 0 on every row.
        """
        import datetime as _dt
        cols = ["D"]
        rows = [[_dt.datetime(2020, 1, 5)], [_dt.datetime(2021, 6, 9)],
                [_dt.datetime(2019, 3, 1)]]
        tables = {"T": {"columns": cols, "rows": [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, "IF('T'[D] = MIN('T'[D]), 1, 0)", "T", tables, [])
        assert err is None, err
        assert vals == [0, 0, 1]


    @pytest.mark.parametrize("expr", [
        'IF(T[A]="MIN(", 1, 2)',
        'CONCATENATE("SUM(x)", T[B])',
        '"a ""SUM("" b"',
        'IF(T[Cat] = "MIN(", T[A], T[B])',
    ])
    def test_aggregate_name_inside_a_string_is_not_a_call(self, expr):
        """A literal containing an aggregate name must not be masked.

        The masker searched the raw text, so `IF(T[Cat] = "MIN(", T[A], T[B])`
        masked from the "MIN(" inside the string to the end of the expression.
        That hid T[A] and T[B] from BOTH the per-row substitution and the
        unresolved-reference check, so they evaluated to blank with nothing
        reporting it — a silent wrong value.
        """
        from pbix_mcp.dax.calc_tables import _mask_aggregate_calls
        _, spans = _mask_aggregate_calls(expr)
        assert spans == [], f"masked a string literal as a call: {spans}"

    def test_string_literal_holding_an_aggregate_name_evaluates_correctly(self):
        cols = ["Cat", "A", "B"]
        rows = [["x", 1, 10], ["MIN(", 2, 20], ["y", 3, 30]]
        tables = {"T": {"columns": cols, "rows": [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, 'IF(T[Cat] = "MIN(", T[A], T[B])', "T", tables, [])
        assert err is None, err
        assert vals == [10, 2, 30]

    def test_parenthesis_inside_a_string_does_not_unbalance_the_mask(self):
        from pbix_mcp.dax.calc_tables import (
            _mask_aggregate_calls,
            _unmask_aggregate_calls,
        )
        expr = 'MIN(T[A]) & "text )with paren("'
        masked, spans = _mask_aggregate_calls(expr)
        assert spans == ["MIN(T[A])"]
        assert _unmask_aggregate_calls(masked, spans) == expr

    def test_aggregate_is_not_collapsed_to_the_current_row(self):
        """The trap this must not fall into.

        Per-row substitution rewrites references to the target table's columns
        as that row's literal. Reaching inside the aggregate turns
        MIN(Sales[Amount]) into MIN(100) -- the row's own value, a different
        wrong answer on every row, silently."""
        cols = ["Amount"]
        rows = [[100.0], [200.0], [50.0]]
        tables = {"Sales": {"columns": cols, "rows": [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, "Sales[Amount] - MIN(Sales[Amount])", "Sales",
            tables, [])
        assert err is None, err
        # minimum is 50 for every row, not the row's own Amount
        assert vals == [50.0, 150.0, 0.0]

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
        ("RELATED(Products[X])", "UNSUPPORTED_CALC"),
        ("CALCULATE(SUM(Sales[Amount]))", "UNSUPPORTED_CALC"),
        ("SUMX(Sales, Sales[Amount])", "UNSUPPORTED_CALC"),
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
            server.pbix_close(alias, force=True)

    def test_aggregate_over_own_table_is_written_with_the_column_total(
            self, sales_model):
        """SUM over the column, not the row -- the same value on every row."""
        alias = "cc_agg"
        server.pbix_open(sales_model, alias)
        try:
            out = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Sales", "TotalAmount", "SUM(Sales[Amount])", "Double"))
            assert out["success"], out
            server.pbix_save(alias, sales_model, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)
        cols, rows = _read_all_columns(sales_model, "Sales")
        amt = [r[cols.index("Amount")] for r in rows]
        tot = [r[cols.index("TotalAmount")] for r in rows]
        assert tot == [sum(amt)] * len(rows), (
            f"expected the column total {sum(amt)} on every row, got {tot}")

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
