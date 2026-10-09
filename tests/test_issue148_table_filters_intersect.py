"""Issue #148: a table filter argument intersects what it must keep.

The filter arguments of one CALCULATE all apply, and KEEPFILTERS keeps the
filters in force. A multi-column table filter removed every filter on its
table's columns first, so a second FILTER(Orders, ...) in the same CALCULATE
threw the first away (2 orders, Desktop 1), and KEEPFILTERS(FILTER(ALL(Orders),
...)) replaced the outer Orders[Region] = "N" (2, Desktop BLANK).

Expected values: Power BI Desktop 2.152 over ADOMD (build_b115b.py)."""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit


def _rel(many, many_col, one, one_col):
    return {"FromTable": many, "FromColumn": many_col, "ToTable": one, "ToColumn": one_col,
            "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


def _eval(expr, tables, measures, rels, date_tables=None):
    m = dict(measures)
    m["p"] = expr
    return de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels,
                                      date_tables=date_tables or {}, simulate_row_context=False)["p"]


def _check(got, want):
    if want is None:
        assert got is None
    elif isinstance(want, bool):
        assert got is want
    elif isinstance(want, str):
        assert got == want
    else:
        assert got == pytest.approx(want)


# build_b115b.py: a snowflake Orders -> Dim -> Cat, a second fact Orders2 on Dim,
# and Orph -> Dim2 with an orphan key kX
FULL = {
    "Cat": {"columns": ["Zone", "ZoneName", "Weight"], "rows": [["z1", "Zone 1", 10], ["z2", "Zone 2", 20]]},
    "Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
    "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]},
    "Orders2": {"columns": ["Region", "Qty"], "rows": [["N", 1], ["N", 2], ["S", 3], ["W", 4], ["W", 5]]},
    "Dim2": {"columns": ["Key", "Name"], "rows": [["k1", "n1"], ["k2", "n2"]]},
    "Orph": {"columns": ["Key", "Amt"], "rows": [["k1", 10], ["kX", 20]]},
}
FULL_RELS = [_rel("Orders", "Region", "Dim", "Region"), _rel("Orders2", "Region", "Dim", "Region"),
             _rel("Dim", "Zone", "Cat", "Zone"), _rel("Orph", "Key", "Dim2", "Key")]
FULL_MEASURES = {"cDim": "COUNTROWS(Dim)", "cOrders2": "COUNTROWS(Orders2)", "cDim2": "COUNTROWS(Dim2)"}

B115B = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115b.py
    ('d01', 'CALCULATE(COUNTROWS(Orders), FILTER(Orders, Orders[Revenue] > 100), FILTER(Orders, Orders[Revenue] < 200))', 1),
    ('d02', 'CALCULATE(COUNTROWS(Orders), FILTER(ALL(Orders), Orders[Revenue] > 100), FILTER(ALL(Orders), Orders[Revenue] < 200))', 1),
    ('d03', 'CALCULATE(COUNTROWS(Dim), FILTER(Orders, Orders[Revenue] > 100), FILTER(Orders2, Orders2[Qty] < 4))', 1),
    ('d04', 'CALCULATE(COUNTROWS(Orders), FILTER(Orders, Orders[Revenue] > 100), Orders[Revenue] < 200)', 1),
    ('d05', 'CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders), Orders[Revenue] > 100), FILTER(ALL(Orders2), Orders2[Qty] < 4))', 1),
    ('k02', 'CALCULATE(CALCULATE(COUNTROWS(Orders), KEEPFILTERS(FILTER(ALL(Orders), Orders[Revenue] > 100))), Orders[Region] = "N")', None),
]


@pytest.mark.parametrize("probe,expr,want", B115B, ids=[p for p, _e, _w in B115B])
def test_b115b(probe, expr, want):
    _check(_eval(expr, FULL, FULL_MEASURES, FULL_RELS), want)
