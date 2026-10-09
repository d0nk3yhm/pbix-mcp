"""Issue #154: a table filter of a table's ROWS filters those rows.

FILTER(T, ...) / FILTER(ALL(T), ...) as a CALCULATE filter wrote one filter per
column, each column's values among the rows, and a row the argument left out
came back when each of its values appeared in a row it kept: T4 has no key,
and (a1, b1, c2, 1) returned (12, Desktop 11). Where the values admit rows of
the table that are not the argument's -- counted against the table's rows they
admit on their own -- a filter on the rows' combinations is added; where they
admit none, as they mostly do (a key, a column the condition alone decides),
nothing changes. SUMMARIZE's extension columns over such a source likewise.

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
    ('t03', 'CALCULATE(SUM(T4[V]), FILTER(T4, NOT (T4[B] = "b1" && T4[C] = "c2")))', 11),
    ('t04', 'CALCULATE(SUM(T4[V]), FILTER(ALL(T4), NOT (T4[B] = "b1" && T4[C] = "c2")))', 11),
    ('c25', 'SUMX(SUMMARIZE(FILTER(T4, NOT (T4[B] = "b1" && T4[C] = "c2")), T4[A], "v", SUM(T4[V])), [v])', 11),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    assert _eval(expr) == pytest.approx(want)


def _combos(table_expr):
    ctx = de.DAXContext(MODEL, MEASURES, relationships=RELS)
    rows = de._engine._eval_expr(table_expr, ctx)
    groups: dict = {}
    for r in rows:
        for c in MODEL[r["__table__"]]["columns"]:
            groups.setdefault(f"{r['__table__']}.{c}", []).append(r[c])
    return de._engine._whole_rows_combos(ctx, rows, groups, {rows[0]["__table__"]})


def test_only_where_the_values_admit_other_rows():
    assert _combos('FILTER(T4, NOT (T4[B] = "b1" && T4[C] = "c2"))')
    # one column decides, or every row is kept: each column's values are the rows
    assert _combos('FILTER(T4, T4[B] = "b1")') == {}
    assert _combos('FILTER(Orders2, Orders2[Qty] >= 2 || Orders2[Region] = "W")') == {}
    assert _combos("FILTER(T4, TRUE())") == {}
