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


class TestDateDiffCountsBoundariesNotElapsedTime:
    """DATEDIFF returns interval BOUNDARIES crossed, not elapsed time.

    The obvious implementation — floor the elapsed difference — is wrong on
    every boundary, and wrong quietly: it returns a plausible number one lower.
    Against the twelve rows below, taken live from Power BI Desktop's own
    engine, ``(end - start).days // 7`` disagrees on five of ten distinct
    dates, so this class of mistake would have shipped unnoticed.

    Ground truth: Power BI Desktop was opened on test_corpus/
    MS_Regional_Sales.pbix and its workspace Analysis Services engine queried
    directly for Opportunities[Weeks Open], whose expression is

        ABS(DATEDIFF(
            Opportunities[Opportunity Created On],
            IF(ISBLANK(Opportunities[CloseDate]) = TRUE(),
               TODAY(), Opportunities[CloseDate]),
            WEEK))

    Rows with a blank CloseDate are excluded because TODAY() makes them
    time-dependent.
    """

    # (close date, weeks Power BI Desktop computed) from a 2021-11-09 start.
    DESKTOP = [
        ("2022-01-23", 11), ("2022-01-30", 12), ("2022-02-02", 12),
        ("2022-02-11", 13), ("2022-02-21", 15), ("2022-02-27", 16),
        ("2022-03-03", 16), ("2022-03-23", 19), ("2022-04-02", 20),
        ("2022-04-10", 22),
    ]

    @pytest.mark.parametrize("close,weeks", DESKTOP)
    def test_matches_power_bi_desktop(self, ctx, close, weeks):
        eng, c = ctx
        got = eng._eval_expr(
            f'DATEDIFF("2021-11-09", "{close}", WEEK)', c)
        assert got == weeks, f"2021-11-09 -> {close}: expected {weeks}, got {got}"

    def test_the_naive_implementation_would_disagree(self):
        """Guards the guard: if this ever stops failing, the cases above are
        no longer discriminating and need replacing with harder ones."""
        start = datetime.date(2021, 11, 9)
        naive_wrong = sum(
            1 for close, weeks in self.DESKTOP
            if (datetime.date.fromisoformat(close) - start).days // 7 != weeks)
        assert naive_wrong >= 4, (
            "elapsed-time DATEDIFF now agrees on almost every case; pick "
            "dates that straddle more week boundaries")

    @pytest.mark.parametrize("expr,want,why", [
        ('DATEDIFF(DATE(2023,12,31), DATE(2024,1,1), YEAR)', 1,
         "one day apart, but one year boundary"),
        ('DATEDIFF(DATE(2024,1,1), DATE(2024,12,31), YEAR)', 0,
         "same calendar year"),
        ('DATEDIFF(DATE(2024,1,31), DATE(2024,2,1), MONTH)', 1,
         "one day apart, but one month boundary"),
        ('DATEDIFF(DATE(2024,1,1), DATE(2024,1,31), MONTH)', 0, "same month"),
        ('DATEDIFF(DATE(2024,3,31), DATE(2024,4,1), QUARTER)', 1, "Q1 -> Q2"),
        ('DATEDIFF(DATE(2024,1,1), DATE(2024,3,31), QUARTER)', 0, "both Q1"),
        ('DATEDIFF(DATE(2024,1,6), DATE(2024,1,7), WEEK)', 1,
         "Saturday -> Sunday: a DAX week starts on Sunday"),
        ('DATEDIFF(DATE(2024,1,7), DATE(2024,1,13), WEEK)', 0,
         "Sunday..Saturday is one week"),
        ('DATEDIFF(DATE(2024,1,1), DATE(2024,1,15), DAY)', 14, "plain days"),
        ('DATEDIFF(DATE(2024,1,15), DATE(2024,1,1), DAY)', -14,
         "reversed arguments give a negative, not an error"),
    ])
    def test_boundary_semantics_per_interval(self, ctx, expr, want, why):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == want, why

    def test_datediff_is_not_reported_unsupported(self, ctx):
        """The gate refuses any expression naming an unsupported function, so a
        registered-but-unlisted DATEDIFF would still block every file."""
        eng, c = ctx
        eng._eval_expr('DATEDIFF(DATE(2024,1,1), DATE(2024,2,1), DAY)', c)
        assert not eng.unsupported_functions


class TestRoundingDirectionOnNegativeNumbers:
    """INT floors; TRUNC and ROUNDDOWN truncate. They differ only below zero.

    Every value below was read from Power BI Desktop's own engine. Python's
    ``int()`` truncates, so using it for INT was off by one for every negative
    number -- and off by a whole bin in the binning idiom ``INT(x / 5) * 5``
    that Power BI's own "New group" feature generates. It returned a plausible
    number, never an error.
    """

    @pytest.mark.parametrize("expr,desktop", [
        ("INT(-1.5)", -2),
        ("INT(-0.1)", -1),
        ("INT(-2.0)", -2),
        ("INT(1.5)", 1),
        # Truncating instead of flooring puts these in the NEXT bin up.
        ("INT(-1612/5)*5", -1615),
        ("INT(-1608/5)*5", -1610),
        ("INT(77/5)*5", 75),
    ])
    def test_int_floors(self, ctx, expr, desktop):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == desktop

    @pytest.mark.parametrize("expr,desktop", [
        ("TRUNC(-1.5)", -1),
        ("ROUNDDOWN(-1.5,0)", -1),
        ("ROUNDDOWN(-2.7,0)", -2),
        ("ROUNDDOWN(1.9,0)", 1),
        ("ROUNDDOWN(3.14159,2)", 3.14),
        ("ROUNDDOWN(-3.14159,2)", -3.14),
        ("ROUNDUP(1.1,0)", 2),
        ("ROUNDUP(-1.1,0)", -2),
    ])
    def test_rounddown_and_roundup_go_relative_to_zero(self, ctx, expr, desktop):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == pytest.approx(desktop)

    def test_int_and_rounddown_disagree_below_zero(self, ctx):
        """The whole point: if these ever agree, one of them is wrong."""
        eng, c = ctx
        assert eng._eval_expr("INT(-1.5)", c) != eng._eval_expr(
            "ROUNDDOWN(-1.5,0)", c)


class TestWeekNumStartsTheYearAtWeekOne:
    """WEEKNUM is not ISO: the week containing January 1 is always week 1.

    ``date.isocalendar()[1]`` gives 2024-01-01 week 1 but 2021-01-01 week 53,
    because ISO assigns early-January days to the previous year. DAX never
    does. Values below are from Power BI Desktop.
    """

    @pytest.mark.parametrize("expr,desktop", [
        ("WEEKNUM(DATE(2024,1,1))", 1),
        ("WEEKNUM(DATE(2024,1,6))", 1),     # Saturday, still week 1
        ("WEEKNUM(DATE(2024,1,7))", 2),     # Sunday starts week 2
        ("WEEKNUM(DATE(2024,12,31))", 53),
        ("WEEKNUM(DATE(2021,1,1))", 1),     # ISO would say 53
        ("WEEKNUM(DATE(2021,1,2))", 1),
        ("WEEKNUM(DATE(2021,1,3))", 2),
        ("WEEKNUM(DATE(2024,1,1),2)", 1),   # type 2: week starts Monday
        ("WEEKNUM(DATE(2024,1,7),2)", 1),
        ("WEEKNUM(DATE(2024,1,8),2)", 2),
    ])
    def test_matches_power_bi_desktop(self, ctx, expr, desktop):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == desktop

    def test_iso_week_would_disagree(self):
        """2021-01-01 is the case that separates DAX from ISO."""
        assert datetime.date(2021, 1, 1).isocalendar()[1] == 53
