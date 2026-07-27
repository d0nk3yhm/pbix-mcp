"""DAX evaluation that returned a WRONG VALUE rather than an error.

Each of these produced a plausible-looking number or string instead of failing,
which is the worst shape a bug can take: nothing surfaces it, so it propagates
into materialized column data and into any answer built on it. All three were
found by running the engine against real reports rather than synthetic input.

  * MIN/MAX over a date or text column returned 0
  * FORMAT(<ISO date string>, "MMMM") returned the raw timestamp, not "January"
  * an unqualified [Column] reference in a calculated column evaluated to blank

The last one matters most: `[Date]` is how Power BI itself writes calculated
columns, including every auto date/time table it generates.
"""
import datetime

import pytest

from pbix_mcp import server
from pbix_mcp.dax import engine as dax_engine


@pytest.fixture
def ctx():
    tables = {
        "T": {
            "columns": ["Date", "Amount", "Name"],
            "rows": [
                [datetime.datetime(2020, 3, 5), 10, "beta"],
                [datetime.datetime(2021, 7, 19), 42, "alpha"],
                [datetime.datetime(2019, 1, 2), 7, "gamma"],
            ],
        }
    }
    return dax_engine.DAXEngine(), dax_engine.DAXContext(
        tables, {}, None, None, None, [])


class TestMinMaxOverNonNumericColumns:
    def test_min_of_a_date_column(self, ctx):
        eng, c = ctx
        assert eng._eval_expr("MIN(T[Date])", c) == datetime.datetime(2019, 1, 2)

    def test_max_of_a_date_column(self, ctx):
        eng, c = ctx
        assert eng._eval_expr("MAX(T[Date])", c) == datetime.datetime(2021, 7, 19)

    def test_min_max_of_a_text_column(self, ctx):
        eng, c = ctx
        assert eng._eval_expr("MIN(T[Name])", c) == "alpha"
        assert eng._eval_expr("MAX(T[Name])", c) == "gamma"

    def test_numeric_behaviour_is_unchanged(self, ctx):
        eng, c = ctx
        assert eng._eval_expr("MIN(T[Amount])", c) == 7
        assert eng._eval_expr("MAX(T[Amount])", c) == 42

    def test_nested_in_year_which_is_the_real_world_shape(self, ctx):
        """`YEAR(MIN(Table[Date]))` is what Power BI generates for every auto
        date/time table. Returning 0 from MIN made YEAR return nothing."""
        eng, c = ctx
        assert eng._eval_expr("YEAR(MIN(T[Date]))", c) == 2019
        assert eng._eval_expr("YEAR(MAX(T[Date]))", c) == 2021

    def test_two_argument_form_still_works(self, ctx):
        eng, c = ctx
        assert eng._eval_expr("MIN(3, 7)", c) == 3
        assert eng._eval_expr("MAX(3, 7)", c) == 7


class TestFormatOfDateStrings:
    @pytest.mark.parametrize("expr,expected", [
        ('FORMAT("2015-03-04", "MMMM")', "March"),
        ('FORMAT(DATE(2015, 3, 4), "MMMM")', "March"),
        ('FORMAT("2015-03-04", "yyyy-MM-dd")', "2015-03-04"),
    ])
    def test_date_patterns_apply_to_string_dates(self, ctx, expr, expected):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == expected

    @pytest.mark.parametrize("expr,expected", [
        ('FORMAT(1234.5, "#,##0.00")', "1,234.50"),
        ('FORMAT(0.25, "0.0%")', "25.0%"),
    ])
    def test_numeric_formats_are_unchanged(self, ctx, expr, expected):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == expected

    def test_a_non_date_string_is_left_alone(self, ctx):
        """Coercion must not invent a date out of arbitrary text."""
        eng, c = ctx
        assert eng._eval_expr('FORMAT("hello", "MMMM")', c) == "hello"


class TestUnqualifiedColumnReferences:
    """`[Date]` inside a calculated column means "this table's Date". It is the
    idiomatic form and the one Desktop generates; only the qualified forms
    resolved, so such columns materialized blank and the edit was refused."""

    ROWS = [{"Date": datetime.datetime(2015, 3, 4)}]
    COLS = [{"name": "Date", "data_type": "DateTime"}]

    def _materialize(self, specs, table="T"):
        _c, rows, _r = server._materialize_table_calc_columns(
            table, self.COLS, self.ROWS, specs, [])
        return rows[0]

    def test_unqualified_reference_resolves(self):
        row = self._materialize([{"column": "Y", "expression": "YEAR([Date])"}])
        assert row["Y"] == 2015

    def test_qualified_references_still_work(self):
        assert self._materialize(
            [{"column": "Y", "expression": "YEAR(T[Date])"}])["Y"] == 2015
        assert self._materialize(
            [{"column": "Y", "expression": "YEAR('T'[Date])"}])["Y"] == 2015

    def test_the_full_auto_date_column_set(self):
        """Exactly the six columns Power BI puts on an auto date/time table."""
        row = self._materialize([
            {"column": "Year", "expression": "YEAR([Date])"},
            {"column": "MonthNo", "expression": "MONTH([Date])"},
            {"column": "Month", "expression": 'FORMAT([Date], "MMMM")'},
            {"column": "QuarterNo", "expression": "INT(([MonthNo] + 2) / 3)"},
            {"column": "Quarter", "expression": '"Qtr " & [QuarterNo]'},
            {"column": "Day", "expression": "DAY([Date])"},
        ])
        assert row["Year"] == 2015
        assert row["MonthNo"] == 3
        assert row["Month"] == "March"
        assert row["QuarterNo"] == 1
        assert row["Quarter"] == "Qtr 1"
        assert row["Day"] == 4

    def test_a_calc_column_may_reference_an_earlier_calc_column(self):
        row = self._materialize([
            {"column": "MonthNo", "expression": "MONTH([Date])"},
            {"column": "QuarterNo", "expression": "INT(([MonthNo] + 2) / 3)"},
        ])
        assert row["QuarterNo"] == 1

    def test_table_names_with_hyphens(self):
        """Auto date/time tables are named with a GUID, so hyphens are the
        norm rather than the exception."""
        row = self._materialize(
            [{"column": "Y", "expression": "YEAR([Date])"}],
            table="DateTableTemplate_6de7953b-39de-41ab-b96b-cebbc3f3ccc1")
        assert row["Y"] == 2015


class TestQualifierRewriteIsSurgical:
    """The rewrite that makes `[Col]` resolve must not touch anything else."""

    COLUMNS = ["Date", "QuarterNo", "MonthNo"]

    def _q(self, expr):
        return server._qualify_bare_column_refs(expr, "T", self.COLUMNS)

    def test_bare_reference_is_qualified(self):
        assert self._q("YEAR([Date])") == "YEAR('T'[Date])"

    @pytest.mark.parametrize("expr", ["T[Date]", "'T'[Date]", "Other[Date]"])
    def test_already_qualified_is_untouched(self, expr):
        assert self._q(expr) == expr

    def test_string_literals_are_untouched(self):
        assert self._q('"a [Date] label"') == '"a [Date] label"'
        assert self._q('"Qtr " & [QuarterNo]') == '"Qtr " & \'T\'[QuarterNo]'

    def test_names_that_are_not_columns_are_untouched(self):
        """A measure reference must not be rewritten into a column."""
        assert self._q("[Total Sales]") == "[Total Sales]"

    def test_no_known_columns_is_a_no_op(self):
        assert server._qualify_bare_column_refs("YEAR([Date])", "T", []) == \
            "YEAR([Date])"
