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
import os

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


CORPUS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "test_corpus")


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestAggregateInACalculatedColumnUsesTheWholeColumn:
    """`([Year] - MIN([Year])) * 12 + [MonthNumber]` on a real model.

    A calculated column has no filter context beyond its own row, so MIN() here
    is the minimum of the ENTIRE column -- the same number on every row. The
    per-row evaluator substitutes each reference to the target table's columns
    with that row's literal, which would turn MIN([Year]) into MIN(2015): the
    row's own year, a different wrong answer per row, written into VertiPaq
    with nothing reporting it.

    Ground truth read live from Power BI Desktop's Analysis Services engine on
    test_corpus/MS_Employee_Hiring.pbix: MIN(Year) is 2010, so 2015-11 is
    (2015-2010)*12+11 = 71. Collapsing the aggregate to the row would give 11.
    """

    # (Year, MonthNumber) -> MonthIncrementNumber, as Desktop computed it.
    DESKTOP = {
        (2010, 1): 1, (2015, 11): 71, (2015, 12): 72, (2016, 1): 73,
        (2016, 2): 74, (2016, 3): 75, (2016, 4): 76, (2016, 5): 77,
        (2016, 6): 78, (2016, 7): 79, (2016, 9): 81, (2016, 10): 82,
        (2016, 11): 83,
    }

    def test_matches_power_bi_desktop_on_the_real_model(self):
        import zipfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

        src = os.path.join(CORPUS, "MS_Employee_Hiring.pbix")
        if not os.path.exists(src):
            pytest.skip("MS_Employee_Hiring.pbix not in corpus")
        with zipfile.ZipFile(src) as z:
            abf = decompress_datamodel(z.read("DataModel"))
        meta = read_metadata_sqlite(abf)
        td = read_table_from_abf(abf, "Date", meta)
        cols, rows = td["columns"], td["rows"]
        # Guards the guard: these two were among the columns the decoder used
        # to drop, which made this expression unevaluatable in the first place.
        assert "Year" in cols and "MonthNumber" in cols

        numeric = {"Year", "MonthNumber", "PeriodNumber", "QtrNumber", "Day"}
        col_defs = [{"name": c,
                     "data_type": "Int64" if c in numeric else "String"}
                    for c in cols]
        data_rows = [dict(zip(cols, r)) for r in rows]
        _, out_rows, _ = server._materialize_table_calc_columns(
            "Date", col_defs, data_rows,
            [{"column": "MonthIncrementNumber",
              "expression": "([Year]-MIN([Year]))*12 +[MonthNumber]"}], [])

        seen = {}
        for r in out_rows:
            seen.setdefault((r["Year"], r["MonthNumber"]),
                            r["MonthIncrementNumber"])
        wrong = {k: (want, seen.get(k)) for k, want in self.DESKTOP.items()
                 if seen.get(k) != want}
        assert wrong == {}, f"disagrees with Power BI Desktop: {wrong}"

    def test_collapsing_the_aggregate_would_disagree(self):
        """If MIN were taken per row, 2015-11 would be 11 rather than 71."""
        assert self.DESKTOP[(2015, 11)] != 11


class TestBlankDoesNotPropagateThroughAComparison:
    """DAX coerces BLANK to the ZERO of the other operand's type, then compares.

    Returning BLANK (None) from the comparison instead made every test against
    a blank fall to the ELSE branch. On a bucketing expression -- the shape
    Power BI's own binning and countless hand-written columns use --

        IF(T[x] < 30, 20, IF(T[x] < 45, 30, IF(T[x] < 60, 45, 80)))

    every blank row scored 80 where Desktop scores it 20. A plausible number,
    never an error, materialized into VertiPaq.

    Measured on test_corpus/MS_Life_Expectancy.pbix before the fix:
    Indicators[Basic drinking water services (% of population)] disagreed with
    Desktop on 70,562 of 72,645 rows.

    Every expectation below was read live from Power BI Desktop's engine.
    """

    @pytest.mark.parametrize("expr,desktop", [
        # numeric: BLANK -> 0
        ('IF(BLANK() < 50, 1, 0)', 1),
        ('IF(BLANK() >= 50, 1, 0)', 0),
        ('IF(BLANK() = 0, 1, 0)', 1),
        ('IF(BLANK() > -1, 1, 0)', 1),
        ('IF(BLANK() < -1, 1, 0)', 0),
        ('IF(BLANK() <> 50, 1, 0)', 1),
        # text: BLANK -> ""
        ('IF(BLANK() = "", 1, 0)', 1),
        ('IF(BLANK() <> "", 1, 0)', 0),
        ('IF(BLANK() < "a", 1, 0)', 1),
        # boolean: BLANK -> FALSE
        ('IF(BLANK() = FALSE(), 1, 0)', 1),
        ('IF(BLANK() = TRUE(), 1, 0)', 0),
        # date: BLANK -> the zero date, 1899-12-30
        ('IF(BLANK() < DATE(2024,1,1), 1, 0)', 1),
        ('IF(BLANK() > DATE(2024,1,1), 1, 0)', 0),
        ('IF(BLANK() = DATE(1899,12,30), 1, 0)', 1),
        # blank vs blank
        ('IF(BLANK() = BLANK(), 1, 0)', 1),
        ('IF(BLANK() <> BLANK(), 1, 0)', 0),
        ('IF(BLANK() <= BLANK(), 1, 0)', 1),
        ('IF(BLANK() >= BLANK(), 1, 0)', 1),
        ('IF(BLANK() < BLANK(), 1, 0)', 0),
    ])
    def test_matches_power_bi_desktop(self, ctx, expr, desktop):
        eng, c = ctx
        assert eng._eval_expr(expr, c) == desktop

    def test_the_bucketing_shape_that_was_wrong(self, ctx):
        """The exact idiom, on a blank input: Desktop says 20, we said 80."""
        eng, c = ctx
        assert eng._eval_expr(
            "IF(BLANK()<30,20,IF(BLANK()<45,30,IF(BLANK()<60,45,80)))", c) == 20

    @pytest.mark.parametrize("expr,want", [
        ("BLANK() + 5", 5),
        ("BLANK() * 5", 0),
        ('BLANK() & "x"', "x"),
    ])
    def test_arithmetic_is_untouched(self, ctx, expr, want):
        """Only COMPARISONS coerce; this guards against over-reaching."""
        eng, c = ctx
        assert eng._eval_expr(expr, c) == want

    def test_a_blank_column_value_buckets_like_desktop(self, ctx):
        """Through the row-context evaluator, not just literal BLANK()."""
        from pbix_mcp.dax.calc_tables import evaluate_row_context_column
        cols = ["v"]
        rows = [[None], [10.0], [55.0], [95.0]]
        tables = {"T": {"columns": cols, "rows": [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows,
            'IF(T[v] < 50, "low", IF(T[v] < 90, "mid", "high"))',
            "T", tables, [])
        assert err is None, err
        # the blank row lands in "low" with Desktop, not "high"
        assert vals == ["low", "low", "mid", "high"]


class TestStringLiteralsAreNotTornApart:
    """Two ways a perfectly ordinary text value was silently destroyed.

    Both were found by an adversarial review of the calculated-column path and
    both survived 3-of-3 independent attempts to refute them.
    """

    @staticmethod
    def _eval(cols, rows, expr, table="T"):
        from pbix_mcp.dax.calc_tables import evaluate_row_context_column
        tables = {table: {"columns": cols, "rows": [list(r) for r in rows]}}
        return evaluate_row_context_column(cols, rows, expr, table, tables, [])

    @pytest.mark.parametrize("expr,want", [
        ('\'T\'[A] & "u//v" & \'T\'[B]', ["xu//vy", "pu//vq"]),
        ('\'T\'[A] & "a--b" & \'T\'[B]', ["xa--by", "pa--bq"]),
        ('\'T\'[A] & "https://x" & \'T\'[B]', ["xhttps://xy", "phttps://xq"]),
    ])
    def test_comment_markers_inside_a_string_are_not_comments(self, expr, want):
        """`--` and `//` inside a literal deleted the rest of the expression.

        Comments were stripped with a plain regex over the raw text, so a URL,
        an ISO range, or a double dash in prose truncated the line -- taking
        whole column references with it. The expression still evaluated, to a
        plausible wrong value, and the unresolved-reference check could not
        help: it only sees the text AFTER stripping, by which point the
        reference is already gone.
        """
        vals, err = self._eval(["A", "B"], [["x", "y"], ["p", "q"]], expr)
        assert err is None, err
        assert vals == want

    @pytest.mark.parametrize("expr,want", [
        ("1 + 2 // trailing", "1 + 2 "),
        ("1 -- note\n+ 2", "1 \n+ 2"),
        ("/* block */ 5", " 5"),
    ])
    def test_real_comments_are_still_removed(self, expr, want):
        """Guards the guard: the fix must not stop stripping actual comments."""
        from pbix_mcp.dax.calc_tables import strip_dax_comments
        assert strip_dax_comments(expr) == want

    def test_a_double_quote_in_the_data_survives(self):
        """DAX escapes a quote by DOUBLING it, so `6" pipe` is `"6"" pipe"`.

        The literal parser required exactly two quote characters, so any such
        value fell through and came back BLANK -- deleting inches, dimensions
        and any quoted phrase from a calculated column that referenced it.
        """
        vals, err = self._eval(
            ["Product"], [['6" pipe'], ["plain"], ['10" x 4"']],
            '\'T\'[Product] & " x"')
        assert err is None, err
        assert vals == ['6" pipe x', "plain x", '10" x 4" x']

    def test_a_lone_string_literal_with_escaped_quotes_evaluates(self):
        from pbix_mcp.dax import engine as eng_mod
        eng = eng_mod.DAXEngine()
        c = eng_mod.DAXContext({}, {}, None, None, None, [])
        assert eng._eval_expr('"a ""quoted"" b"', c) == 'a "quoted" b'
        assert eng._eval_expr('"plain"', c) == "plain"
