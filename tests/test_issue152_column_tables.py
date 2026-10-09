"""Issue #152: ALL / ALLSELECTED of several columns is a table of COLUMNS,
not the table's rows.

Its rows were marked as the table's (``__row__``), so as a CALCULATE filter --
FILTER(ALL(T[a], T[b]), ...), a running total over 'Date'[Year] and
'Date'[Month] -- it went through the branch for a table's rows: it removed
every filter of the table, an iterator's own row included, blocked the
relationships into it and, carrying every column, filtered its expanded table;
a transition over its rows expanded too. Desktop: it filters its own columns
and nothing else. The rows are _ColumnsRow now, and such a table filter has
its own branch.

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
    ('t25', 'SUMX(VALUES(T4[A]), CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), (T4[B] = "b1" && T4[C] = "c1") || (T4[B] = "b2" && T4[C] = "c2"))))', 3),
    ('t11', 'CALCULATE(CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2"))), T4[A] = "a1")', 3),
    ('t12', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 3)), Dim[Zone] = "z1")', 1),
    ('t18', 'CALCULATE(IF(ISFILTERED(Dim), 1, 0), FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 3))', 0),
    ('c21', 'CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 3))', 3),
    ('c22', 'CALCULATE(COUNTROWS(Dim3), FILTER(ALL(Fact3[Key], Fact3[V]), Fact3[V] >= 2))', 3),
    ('c20', 'SUMX(ALL(Orders2[Region], Orders2[Qty]), CALCULATE(COUNTROWS(Dim)))', 15),
    ('t13', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), FILTER(ALL(Orders2), Orders2[Qty] >= 3)), Dim[Zone] = "z1")', 3),
    ('t14', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), FILTER(ALL(Orders2[Qty]), Orders2[Qty] >= 3)), Dim[Zone] = "z1")', 1),
    ('t17', 'CALCULATE(IF(ISFILTERED(T4[A]), 1, 0), FILTER(ALL(T4[B], T4[C]), T4[C] = "c2"))', 0),
    ('t19', 'CALCULATE(IF(ISFILTERED(Orders2[Qty]), 1, 0), FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 3))', 1),
    ('c10', 'SUMX(SUMMARIZE(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty], "d", COUNTROWS(Dim)), [d])', 15),
    ('c14', 'SUMX(SUMMARIZE(FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 2), Orders2[Region], "d", COUNTROWS(Dim)), [d])', 9),
    ('c15', 'SUMX(SUMMARIZE(FILTER(ALL(Fact3[Key], Fact3[V]), Fact3[V] >= 2), Fact3[Key], "n", COUNTROWS(Dim3)), [n])', 6),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    assert _eval(expr) == pytest.approx(want)


def test_the_rows_are_rows_of_columns():
    ctx = de.DAXContext(MODEL, MEASURES, relationships=RELS)
    rows = de._engine._fn_all("Orders2[Region], Orders2[Qty]", ctx)
    assert rows and all(isinstance(r, de._ColumnsRow) for r in rows)
    sel = de._engine._fn_allselected("Orders2[Region], Orders2[Qty]", ctx)
    assert sel and all(isinstance(r, de._ColumnsRow) for r in sel)
