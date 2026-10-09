"""Issue #153: a table filter over several columns of one table filters the
COMBINATIONS its rows have.

FILTER(ALL(T[a], T[b]), ...) as a CALCULATE filter wrote one filter per column,
each column's values, so a combination none of its rows has came back whenever
its values each appeared in some row: T4's (b1, c2), left out, still counted
(12, Desktop 11). Its columns' values stay the filters on the columns, and a
filter on their combinations is added where those values alone admit more --
as TREATAS onto several columns, a CROSSJOIN's rows and SUMMARIZE's do (#103,
#108, #117). An inner filter on one of the columns keeps the projection onto
the others. SUMMARIZE's extension columns over such a source likewise.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b117.py)."""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit


def _rel(many, many_col, one, one_col):
    return {"FromTable": many, "FromColumn": many_col, "ToTable": one, "ToColumn": one_col,
            "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


def _eval(expr):
    m = dict(MEASURES)
    m["p"] = expr
    return de.evaluate_measures_smart(["p"], MODEL, m, {}, relationships=RELS,
                                      date_tables={}, simulate_row_context=False)["p"]


# build_b117.py: Orders2 -> Dim -> Cat, Fact3 -> Dim3 (Dim3 b unused), and T4
# with no key, where (a1, b1, c2) is the row a condition on B and C leaves out
MODEL = {
    "Cat": {"columns": ["Zone", "ZoneName", "Weight"], "rows": [["z1", "Zone 1", 10], ["z2", "Zone 2", 20]]},
    "Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
    "Orders2": {"columns": ["Region", "Qty"], "rows": [["N", 1], ["N", 2], ["S", 3], ["W", 4], ["W", 5]]},
    "Dim3": {"columns": ["Key", "Grp"], "rows": [["a", "g1"], ["b", "g1"], ["c", "g2"]]},
    "Fact3": {"columns": ["Key", "V"], "rows": [["a", 1], ["a", 3], ["c", 2]]},
    "T4": {"columns": ["A", "B", "C", "V"],
           "rows": [["a1", "b1", "c1", 1], ["a1", "b2", "c2", 2], ["a1", "b1", "c2", 1], ["a2", "b2", "c1", 8]]},
}
RELS = [_rel("Orders2", "Region", "Dim", "Region"), _rel("Dim", "Zone", "Cat", "Zone"),
        _rel("Fact3", "Key", "Dim3", "Key")]
MEASURES = {"cDim": "COUNTROWS(Dim)"}

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152)
    ('t01', 'CALCULATE(SUM(T4[V]), FILTER(ALL(T4[A], T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")))', 11),
    ('t02', 'CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")))', 11),
    ('t06', 'CALCULATE(SUM(T4[V]), KEEPFILTERS(FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2"))))', 11),
    ('t07', 'CALCULATE(COUNTROWS(T4), FILTER(ALL(T4[A], T4[B]), T4[A] = "a2" || T4[B] = "b1"))', 3),
    ('t09', 'CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")), T4[A] = "a1")', 3),
    ('t10', 'COUNTROWS(FILTER(T4, CALCULATE(COUNTROWS(T4), FILTER(ALL(T4[B], T4[C]), T4[B] = "b1" || T4[C] = "c1")) > 0))', 3),
    ('t21', 'CALCULATE(CALCULATE(SUM(T4[V]), KEEPFILTERS(T4[B] = "b1")), FILTER(ALL(T4[B], T4[C]), (T4[B] = "b1" && T4[C] = "c1") || (T4[B] = "b2" && T4[C] = "c2")))', 1),
    ('t24', 'CALCULATE(CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), (T4[B] = "b1" && T4[C] = "c1") || (T4[B] = "b2" && T4[C] = "c2"))), T4[C] = "c2")', 3),
    ('t26', 'CALCULATE(COUNTROWS(VALUES(T4[C])), FILTER(ALL(T4[B], T4[C]), (T4[B] = "b1" && T4[C] = "c1") || (T4[B] = "b2" && T4[C] = "c2")), T4[B] = "b2")', 1),
    ('c24', 'SUMX(SUMMARIZE(FILTER(ALL(T4[A], T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")), T4[A], "v", SUM(T4[V])), [v])', 11),
    ('t05', 'SUMX(FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")), CALCULATE(SUM(T4[V])))', 11),
    ('t08', 'CALCULATE(COUNTROWS(T4), CALCULATETABLE(SUMMARIZE(T4, T4[A], T4[B]), T4[C] = "c1"))', 3),
    ('t15', 'CALCULATE(CALCULATE(SUM(T4[V]), KEEPFILTERS(FILTER(ALL(T4[B], T4[C]), T4[C] = "c2"))), T4[B] = "b2")', 2),
    ('t16', 'CALCULATE(CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), T4[C] = "c2")), T4[B] = "b2")', 3),
    ('t20', 'CALCULATE(CALCULATE(SUM(T4[V]), T4[B] = "b1"), FILTER(ALL(T4[B], T4[C]), (T4[B] = "b1" && T4[C] = "c1") || (T4[B] = "b2" && T4[C] = "c2")))', 2),
    ('t22', 'CALCULATE(SUM(T4[V]), SUMMARIZE(FILTER(T4, T4[V] > 1), T4[A], T4[B]))', 10),
    ('t23', 'CALCULATE(SUM(T4[V]), FILTER(ALLSELECTED(T4[B], T4[C]), T4[C] = "c1"))', 9),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    assert _eval(expr) == pytest.approx(want)


def test_no_combination_filter_where_the_values_suffice():
    ctx = de.DAXContext(MODEL, MEASURES, relationships=RELS)
    rows = de._engine._fn_filter('ALL(T4[B], T4[C]), T4[C] = "c2"', ctx)
    _cols, values, combo = de._engine._columns_table_filter(ctx, rows, rows[0])
    assert sorted(values["T4.B"]) == ["b1", "b2"] and values["T4.C"] == ["c2"] and combo is None
    rows = de._engine._fn_filter('ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")', ctx)
    _cols, _values, combo = de._engine._columns_table_filter(ctx, rows, rows[0])
    assert combo is not None and sorted(map(tuple, combo[1]["rows"])) == [
        ("b1", "c1"), ("b2", "c1"), ("b2", "c2")]
