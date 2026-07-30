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


class TestTableFilterReplacesPropagation:
    """A MULTI-COLUMN table filter argument replaces the whole filter context
    of the table it covers -- propagation included -- exactly as ALL(Table)
    does. Only adding the row values left the related dimension's filter in
    force on top of them.

    Verified against Power BI Desktop on MS_AI_Sample, filtering the related
    Owners[Manager]:

        CALCULATE(AVERAGE('Cases'[CSAT]), FILTER(ALL('Cases'), 1=1)) = 4.2706
        CALCULATE(COUNTROWS('Cases'),     FILTER(ALL('Cases'), 1=1)) = 10000
        AVERAGE('Cases'[CSAT])  (no ALL)                  = 4.13796627491058

    The TABLE EXPRESSION was never wrong: COUNTROWS(FILTER(ALL('Cases'), ...))
    already returned 10,000 and SELECTEDVALUE already returned BLANK. What was
    wrong is the filter context that row set establishes -- an aggregate inside
    the CALCULATE was still evaluated against that manager's 3,914 rows, so the
    CALCULATE form of the same count was 3,914 too. The four [CSAT Impact*]
    measures are 1 - AllAvgExcept/AllAvg over this shape and came out +-0.03
    where Desktop gives exactly 0.
    """

    FILTERS = {"Dim.grp": ["X"]}

    def test_filter_over_all_clears_the_propagated_filter(self):
        assert ev("CALCULATE(SUM(Fact[v]), FILTER(ALL(Fact), 1=1))",
                  filters=self.FILTERS) == 30

    def test_it_agrees_with_the_bare_all(self):
        bare = ev("CALCULATE(SUM(Fact[v]), ALL(Fact))", filters=self.FILTERS)
        wrapped = ev("CALCULATE(SUM(Fact[v]), FILTER(ALL(Fact), 1=1))",
                     filters=self.FILTERS)
        assert bare == wrapped == 30

    def test_the_calculate_form_counts_every_row(self):
        """Desktop: CALCULATE(COUNTROWS('Cases'), FILTER(ALL('Cases'),1=1))
        = 10000 under the Owners[Manager] filter. Ours answered 3,914."""
        assert ev("CALCULATE(COUNTROWS(Fact), FILTER(ALL(Fact), 1=1))",
                  filters=self.FILTERS) == 2

    def test_the_bare_table_expression_was_already_right(self):
        """COUNTROWS(FILTER(ALL(T), ...)) counts the returned TABLE and needed
        no fix -- it returned 10,000 before and after. Keeping it here stops a
        future change to the table path from being blamed on this one."""
        assert ev("COUNTROWS(FILTER(ALL(Fact), 1=1))",
                  filters=self.FILTERS) == 2

    def test_a_single_column_filter_does_NOT_suppress_propagation(self):
        """ALL(Fact[v]) replaces the filter on ONE column; a filter reaching
        the table through a relationship still applies, so only row 'a' is
        visible. Suppressing here would have been the over-broad fix."""
        assert ev("CALCULATE(SUM(Fact[v]), ALL(Fact[v]))",
                  filters=self.FILTERS) == 10


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
