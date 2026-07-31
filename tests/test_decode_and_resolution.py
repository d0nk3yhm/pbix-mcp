"""Defects found by widening the Desktop comparison beyond the grand total.

The grand-total sweep compares one cell per measure. Comparing every measure
UNDER A FILTER CONTEXT, and cross-checking the SAME table decoded out of two
corpus files, exposed the rest.
"""
from __future__ import annotations

from datetime import datetime

from pbix_mcp.dax import engine as de
from pbix_mcp.formats.vertipaq_decoder import (
    DAXDateTime,
    _oa_date_to_python,
    _reconstruct_value,
    _reconstruct_value_encoded,
)

TABLES = {
    "Fact": {"columns": ["k", "v"], "rows": [["a", 10], ["b", 20]]},
    "Fact A": {"columns": ["k", "v"], "rows": [["a", 1]]},
    "Dim": {"columns": ["k", "grp"], "rows": [["a", "X"], ["b", "Y"]]},
}
RELS = [{"FromTable": "Fact", "FromColumn": "k",
         "ToTable": "Dim", "ToColumn": "k", "IsActive": 1}]


def ev(expr, *, homes=None, defs=None, filters=None):
    all_defs = {"M": expr}
    all_defs.update(defs or {})
    ctx = de.DAXContext(TABLES, all_defs, None, None, filters, RELS)
    ctx.measure_tables = homes or {}
    return de.DAXEngine().evaluate_measure("M", ctx)


class TestValueEncodedDecimalScale:
    """A Decimal column is stored as value x 10,000 whichever way it is
    encoded. The value-encoded branch skipped the /10000 the dictionary branch
    applies, so every such column read 10,000x too large.

    Proved without Desktop: the SAME 397-row AdventureWorks Product table
    decoded out of two corpus files -- one dictionary-encoded, one
    value-encoded -- disagreed by exactly 10,000x on List Price and Standard
    Cost, and agrees exactly once both paths scale.
    """

    def test_both_paths_agree(self):
        raw = 14315000            # 1431.50 stored as a fixed decimal
        assert _reconstruct_value_encoded(raw, "Decimal") == 1431.50
        assert _reconstruct_value(raw, "Decimal", False) == 1431.50

    def test_float64_is_not_scaled(self):
        assert _reconstruct_value_encoded(1431.5, "Float64") == 1431.5

    def test_int64_and_boolean_unaffected(self):
        assert _reconstruct_value_encoded(45748, "Int64") == 45748
        assert _reconstruct_value_encoded(1, "Boolean") is True


class TestDateTimePrecision:
    """.NET counts 100-ns ticks; Python's datetime resolves to 1 us. The
    decoder carries the original serial so `[start] * 86400000` round-trips."""

    def test_the_serial_survives_the_datetime(self):
        d = _oa_date_to_python(43936.91993719136)
        assert isinstance(d, DAXDateTime) and isinstance(d, datetime)
        assert d.oa_serial == 43936.91993719136
        assert d.oa_serial * 86400000 == 3796149882573.333

    def test_reconstructing_from_the_datetime_alone_is_not_enough(self):
        """The naive path rounds twice and lands one ULP away -- this is the
        error the stored serial exists to avoid."""
        d = _oa_date_to_python(43936.91993719136)
        naive = (d - datetime(1899, 12, 30)).total_seconds() / 86400.0
        assert naive != d.oa_serial


class TestBareColumnUsesTheMeasureHomeTable:
    """Several tables can own the same column name. DAX resolves an unqualified
    [Column] against the table the MEASURE IS DEFINED ON; refusing returned
    BLANK for all six measures of MS_Revenue_Opportunities' Fact table."""

    def test_ambiguous_name_resolves_to_the_home_table(self):
        assert ev("SUM([v])", homes={"M": "Fact"}) == 30
        assert ev("SUM([v])", homes={"M": "Fact A"}) == 1

    def test_no_home_table_still_refuses_rather_than_guessing(self):
        assert ev("SUM([v])") is None

    def test_an_unambiguous_name_needs_no_home_table(self):
        assert ev("SUM([grp])") is None       # text column: nothing to sum
        assert ev("COUNTROWS(FILTER(Dim, [grp] = \"X\"))") == 1


class TestAllVersusAllSelected:
    """ALL(Table) clears filters that reach the table THROUGH a relationship;
    ALLSELECTED(Table) restores the query context instead of clearing it."""

    FILTERS = {"Dim.grp": ["X"]}

    def test_all_clears_the_propagated_filter(self):
        assert ev("CALCULATE(SUM(Fact[v]), ALL(Fact))",
                  filters=self.FILTERS) == 30

    def test_without_all_the_propagation_still_applies(self):
        assert ev("SUM(Fact[v])", filters=self.FILTERS) == 10

    def test_allselected_keeps_the_query_filter(self):
        # The query context IS the Dim.grp filter, so ALLSELECTED restores it
        # rather than removing it.
        assert ev("CALCULATE(SUM(Fact[v]), ALLSELECTED(Fact))",
                  filters=self.FILTERS) == 10


class TestAllOnlySuppressesTheFiltersItCleared:
    """ALL(Table) stops the propagation that was LIVE when it ran -- not
    propagation from a filter created LATER inside a nested CALCULATE.

    The suppression used to flag the table for the rest of the evaluation.
    Verified against Power BI Desktop on MS_AI_Sample, where Cases relates to
    Owners:

        CALCULATE(AVERAGE('Cases'[CSAT]), 'Owners'[Manager]="Low, Spencer")
            = 4.13796627491058
        CALCULATE(CALCULATE(AVERAGE('Cases'[CSAT]),
                            'Owners'[Manager]="Low, Spencer"), ALL('Cases'))
            = 4.13796627491058   <- we returned the global 4.2706
        ...the same nested COUNTROWS: Desktop 3914, we returned 10000.

    This was NOT introduced by the reverted table-filter change; it was found
    while diagnosing why that change broke [Actives], and it predates it.
    """

    FILTERS = {"Dim.grp": ["X"]}

    def test_a_filter_created_inside_all_still_propagates(self):
        assert ev('CALCULATE(CALCULATE(SUM(Fact[v]), Dim[grp] = "X"), '
                  'ALL(Fact))') == 10

    def test_all_still_clears_a_filter_that_was_live(self):
        """The behaviour ALL exists for, and the MS_Covid_Tracking case: a
        filter already in force when ALL runs IS cleared, propagation and all."""
        assert ev("CALCULATE(SUM(Fact[v]), ALL(Fact))",
                  filters=self.FILTERS) == 30

    def test_the_two_rules_compose(self):
        """Outer Dim filter cleared by ALL, inner Dim filter re-applied: the
        inner one wins, so this is row 'a' only."""
        assert ev('CALCULATE(CALCULATE(SUM(Fact[v]), Dim[grp] = "X"), '
                  'ALL(Fact))', filters={"Dim.grp": ["Y"]}) == 10


class TestTableFilterArgumentReplacesPropagation:
    """A MULTI-COLUMN table filter argument replaces the filter context of the
    tables it covers, propagation included -- like ALL(Table), and scoped by the
    same snapshot.

    Desktop, filtering the related Owners[Manager] on MS_AI_Sample:

        CALCULATE(AVERAGE('Cases'[CSAT]), FILTER(ALL('Cases'), 1=1)) = 4.2706
        CALCULATE(COUNTROWS('Cases'),     FILTER(ALL('Cases'), 1=1)) = 10000
        AVERAGE('Cases'[CSAT])  (no ALL)                  = 4.13796627491058

    The row set and the SELECTEDVALUE were already right; the aggregate inside
    was still being evaluated against that manager's 3,914 rows. The four
    [CSAT Impact*] measures are 1 - AllAvgExcept/AllAvg over this shape and came
    out +-0.03 where Desktop gives exactly 0.

    A first attempt at this suppressed the covered tables OUTRIGHT and took
    MS_Employee_Hiring's [Actives] from Desktop's 32,401 to 1,260,817 --
    `CALCULATE([EmpCount], FILTER(Employee, ...))`, where the blanket flag also
    blocked the Date[PeriodNumber] filter [EmpCount] creates LATER. The last
    test below is that shape, and it is the one the earlier scoping test
    missed: it asserted ALL(Fact[v]) directly and never wrapped it in FILTER.
    """

    FILTERS = {"Dim.grp": ["X"]}

    def test_filter_over_all_clears_the_propagated_filter(self):
        assert ev("CALCULATE(SUM(Fact[v]), FILTER(ALL(Fact), 1=1))",
                  filters=self.FILTERS) == 30

    def test_it_agrees_with_the_bare_all(self):
        assert ev("CALCULATE(SUM(Fact[v]), ALL(Fact))",
                  filters=self.FILTERS) == ev(
                      "CALCULATE(SUM(Fact[v]), FILTER(ALL(Fact), 1=1))",
                      filters=self.FILTERS) == 30

    def test_the_bare_table_expression_was_already_right(self):
        assert ev("COUNTROWS(FILTER(ALL(Fact), 1=1))",
                  filters=self.FILTERS) == 2

    def test_a_single_column_filter_does_NOT_suppress_propagation(self):
        assert ev("CALCULATE(SUM(Fact[v]), ALL(Fact[v]))",
                  filters=self.FILTERS) == 10

    def test_a_later_nested_filter_still_propagates(self):
        """The [Actives] shape: FILTER(Fact, ...) with nothing live to
        suppress, then a nested CALCULATE creates a filter. Suppressing the
        table outright blocked it and inflated the answer ~39x on the corpus."""
        assert ev('CALCULATE(CALCULATE(SUM(Fact[v]), Dim[grp] = "X"), '
                  'FILTER(Fact, 1=1))') == 10


class TestPropagationFollowsTableExpansion:
    """Desktop's propagation rule, pinned on MS_Employee_Hiring in one query:
    a filter on a TABLE filters its EXPANDED table -- it reaches the one-side
    dimensions the table points at -- while a filter on a COLUMN does not.

        MAX('Date'[PeriodNumber])                                      201612
        CALCULATE(same, FILTER(Employee, ISBLANK(Employee[TermDate])))  201412
        CALCULATE(same, Employee[FP] = "FT")                            201612

    Both earlier implementations satisfied exactly one of the two anchors:
    the symmetric index broke the column case (Agents_Performance's
    SELECTEDVALUE answered 213 for Desktop's BLANK, so [Rank Filtering *]
    returned 1 for Desktop's 0), and the directional commit 6a4896a broke the
    table case ([Actives] went from Desktop's 32,401 to None). This class is
    both anchors in miniature; every test must hold at once.
    """

    def test_a_column_filter_does_not_reach_the_one_side(self):
        assert ev("CALCULATE(COUNTROWS(Dim), Fact[v] = 10)") == 2

    def test_selectedvalue_on_the_one_side_stays_blank(self):
        assert ev("CALCULATE(SELECTEDVALUE(Dim[grp]), Fact[v] = 10)") is None

    def test_a_table_filter_reaches_the_one_side(self):
        """The [Actives] shape: FILTER(Fact, ...) filters the expanded table,
        so Dim is restricted to the rows Fact points at."""
        assert ev("CALCULATE(COUNTROWS(Dim), FILTER(Fact, Fact[v] = 10))") == 1

    def test_an_aggregate_over_the_one_side_sees_the_expansion(self):
        """MAX over the dimension under a fact TABLE filter -- the corpus
        shape: MAX('Date'[PeriodNumber]) dropping 201612 -> 201412."""
        assert ev("CALCULATE(SELECTEDVALUE(Dim[grp]), "
                  "FILTER(Fact, Fact[v] = 10))") == "X"

    def test_one_to_many_still_flows_both_shapes(self):
        assert ev('CALCULATE(SUM(Fact[v]), Dim[grp] = "X")') == 10
        assert ev("SUM(Fact[v])", filters={"Dim.grp": ["X"]}) == 10


class TestOneRowTableIsAScalar:
    """A measure must evaluate to a value. `LASTDATE('Year'[Date])` returned
    the internal row-dict list, whose str() leaked 72 characters of Python
    repr into the report."""

    def test_a_single_cell_table_collapses(self):
        got = ev("LASTNONBLANK(Dim[grp], 1)")
        assert got == "Y"

    def test_it_also_collapses_inside_a_concatenation(self):
        assert ev('"v=" & LASTNONBLANK(Dim[grp], 1)') == "v=Y"


class TestFirstLastNonBlankTakeATableExpression:
    """MS_Life_Expectancy writes `LASTNONBLANK(ALL(Years[Years]), [...])` in
    five measures; requiring a bare column reference blanked every one."""

    def test_table_expression_first_argument(self):
        assert ev("LASTNONBLANK(ALL(Dim[grp]), 1)") == "Y"
        assert ev("FIRSTNONBLANK(ALL(Dim[grp]), 1)") == "X"

    def test_column_reference_still_works(self):
        assert ev("LASTNONBLANK(Dim[grp], 1)") == "Y"


class TestConcatenateRendersLikeTheOperator:
    """CONCATENATE used `str(x or '')`, which dropped a legitimate 0 --
    MS_Regional_Sales read "...current value is " for Desktop's
    "...current value is 0"."""

    def test_zero_is_not_dropped(self):
        assert ev('CONCATENATE("value is ", 0)') == "value is 0"

    def test_blank_is_still_empty(self):
        assert ev('CONCATENATE("value is ", BLANK())') == "value is "

    def test_numbers_use_the_dax_15_digit_form(self):
        assert ev('CONCATENATE("x=", 1/3)') == "x=0.333333333333333"
