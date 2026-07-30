"""Four defects found by diffing the whole corpus against Desktop.

Each one produced a plausible answer rather than an error, and each was found
by comparing every measure of a corpus file against the value Power BI
Desktop's own engine returns for it.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from pbix_mcp.dax import engine as de

# fact[D] is an Int64 EXCEL SERIAL, dim[D] a real datetime -- exactly how
# IT_Support stores its dates. 45292 = 2024-01-01, 45293 = 2024-01-02.
TABLES = {
    "dim": {"columns": ["D", "Year"],
            "rows": [[datetime(2024, 1, 1), 2024], [datetime(2024, 1, 2), 2024],
                     [datetime(2025, 1, 1), 2025]]},
    "fact": {"columns": ["D", "End", "Q", "C", "N"],
             "rows": [[45292, 45296, "a", 1, 10],
                      [45292, 45293, "a", 1, 20],
                      [45293, 45298, "b", 2, 30],
                      [45658, 45659, "b", 2, 40]]},   # 45658 = 2025-01-01
}
RELS = [{"FromTable": "fact", "FromColumn": "D",
         "ToTable": "dim", "ToColumn": "D", "IsActive": 1}]


def ev(expr, extra_defs=None):
    defs = {"M": expr}
    defs.update(extra_defs or {})
    ctx = de.DAXContext(TABLES, defs, "dim", "D", None, RELS)
    return de.DAXEngine().evaluate_measure("M", ctx)


class TestSerialDatesAreDates:
    """A DATE IS A NUMBER in DAX, and a model may store one as Int64."""

    def test_datediff_over_serials(self):
        assert ev("DATEDIFF(45292, 45296, DAY)") == 4

    def test_datediff_over_a_serial_column(self):
        # This was BLANK on every row, which also made `DATEDIFF(...) <= 3`
        # keep every row (BLANK <= 3 is TRUE) instead of filtering.
        assert ev("SUMX(fact, DATEDIFF(fact[D], fact[End], DAY))") == 4 + 1 + 5 + 1

    def test_a_serial_compares_equal_to_a_date_literal(self):
        assert ev("COUNTROWS(FILTER(fact, fact[D] = DATE(2024,1,1)))") == 2

    def test_the_relationship_still_joins_across_the_type_gap(self):
        """dim is datetime, fact is Int64 -- str() per side never matched, so
        EVERY dimension filter reduced the fact to zero rows."""
        assert ev("CALCULATE(COUNTROWS(fact), dim[Year] = 2024)") == 3
        assert ev("CALCULATE(COUNTROWS(fact), dim[Year] = 2025)") == 1
        assert ev("COUNTROWS(fact)") == 4


class TestAllOverSeveralColumns:
    """ALL / ALLSELECTED matched their argument with an UNANCHORED regex, so
    every column after the first was silently dropped."""

    @pytest.mark.parametrize("fn", ["ALL", "ALLSELECTED"])
    def test_distinct_combinations_not_just_the_first_column(self, fn):
        # (a,1) (b,2) -- two pairs, while [Q] alone has 2 and [C] alone has 2,
        # so a wrong answer here is not distinguishable by count alone; the
        # SUMMARIZE test below is the one that proves the columns survive.
        assert ev(f"COUNTROWS({fn}(fact[Q], fact[C]))") == 2

    @pytest.mark.parametrize("fn", ["ALL", "ALLSELECTED"])
    def test_summarize_can_group_by_both(self, fn):
        """SUMMARIZE over the dropped column found no group-by column and
        returned NO ROWS, so every VAR built on it went blank."""
        got = ev(f'COUNTROWS(SUMMARIZE({fn}(fact[Q], fact[C]), '
                 f'fact[Q], fact[C], "X", 1))')
        assert got == 2

    def test_summarize_accepts_a_table_expression_not_just_a_name(self):
        got = ev('COUNTROWS(SUMMARIZE(ALL(fact[Q]), fact[Q], "X", 1))')
        assert got == 2


class TestAllSelectedRestoresTheOuterContext:
    """ALLSELECTED keeps the filters from OUTSIDE the measure and drops the
    ones CALCULATE applied inside it. Approximated as VALUES, it never removed
    a filter on its own column."""

    def test_it_removes_an_inner_calculate_filter(self):
        assert ev('CALCULATE(COUNTROWS(ALLSELECTED(fact[Q])), fact[Q] = "a")') == 2

    def test_all_agrees(self):
        assert ev('CALCULATE(COUNTROWS(ALL(fact[Q])), fact[Q] = "a")') == 2

    def test_values_still_honours_the_filter(self):
        assert ev('CALCULATE(COUNTROWS(VALUES(fact[Q])), fact[Q] = "a")') == 1


class TestRankxTies:
    """RANKX's fifth argument was parsed but never read."""

    DEFS = {"V": "SUM(fact[N])"}

    def test_skip_is_the_default(self):
        # values per Q: a -> 30, b -> 70; ranking 30 DESC among {30,70}
        assert ev('RANKX(ALL(fact[Q]), [V], 30, DESC)', self.DEFS) == 2

    def test_dense_counts_distinct_values(self):
        got = ev('RANKX(ALL(fact[C]), [V], 30, DESC, Dense)', self.DEFS)
        assert got == 2

    def test_dense_and_skip_differ_when_values_repeat(self):
        """Three rows, two of them tied above the ranked value: SKIP says 3,
        DENSE says 2. Reading the argument is the whole point."""
        tables = {"T": {"columns": ["k", "v"],
                        "rows": [["x", 10], ["y", 10], ["z", 1]]}}
        defs = {"V": "SUM(T[v])", "M": None}

        def run(expr):
            defs2 = dict(defs)
            defs2["M"] = expr
            c = de.DAXContext(tables, defs2, None, None, None, [])
            return de.DAXEngine().evaluate_measure("M", c)

        assert run('RANKX(ALL(T[k]), [V], 1, DESC)') == 3
        assert run('RANKX(ALL(T[k]), [V], 1, DESC, Dense)') == 2


class TestCaseInsensitiveMeasureNames:
    """DAX identifiers are case-insensitive. A bare `[MEASURE]` reference
    passed the existence check and then missed the exact dict lookup, returning
    a SILENT BLANK -- one misspelling blanked nine MS_Competitive_Marketing
    measures."""

    def test_bare_reference_in_any_casing(self):
        defs = {"Total Units": "SUM(fact[N])"}
        assert ev("[TOTAL UNITS]", defs) == 100
        assert ev("[total units]", defs) == 100
        assert ev("[Total Units]", defs) == 100

    def test_it_still_resolves_through_calculate(self):
        defs = {"Total Units": "SUM(fact[N])"}
        assert ev('CALCULATE([TOTAL UNITS], fact[Q] = "a")', defs) == 30

    def test_a_genuinely_missing_measure_is_still_blank(self):
        assert ev("[No Such Measure]") is None
