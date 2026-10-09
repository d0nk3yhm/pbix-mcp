"""Issue #114: a table filter argument filters its EXPANDED table.

FILTER(Orders, ...) -- and the other spellings of a table filter, KEEPFILTERS,
FILTER(ALL(Orders), ...), CALCULATETABLE(Orders, ...), a bare Orders,
ADDCOLUMNS / TOPN / VALUES of it -- filters the columns of the one-side tables
its rows reach. Those columns are filtered themselves: ISFILTERED(Dim) is TRUE,
REMOVEFILTERS / ALL / ALLEXCEPT of one of them leaves the projection onto the
others, the filter overwrites an outer filter on them, and it reaches the facts
that share the dimension. The engine kept a table filter on the table's own
columns and let them reach the one side through the relationship.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b114.py,
build_b115.py, build_b115b.py)."""
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


ORDERS = {"Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
          "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]}}
ORDERS_RELS = [_rel("Orders", "Region", "Dim", "Region")]
ORDERS_MEASURES = {"cDim": "COUNTROWS(Dim)"}

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

B114 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b114.py; e10 is #117, e17 / e18 #115
    ('e01', 'CALCULATE(ISFILTERED(Dim), FILTER(Orders, Orders[Revenue] > 100), ALL(Dim))', True),
    ('e02', 'CALCULATE(CALCULATE(ISFILTERED(Dim), ALL(Dim)), FILTER(Orders, Orders[Revenue] > 100))', False),
    ('e03', 'CALCULATE(CALCULATE(ISCROSSFILTERED(Dim), ALL(Dim)), FILTER(Orders, Orders[Revenue] > 100))', False),
    ('e04', 'CALCULATE(CALCULATE(ISFILTERED(Orders), ALL(Dim)), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e05', 'CALCULATE(ISFILTERED(Dim), KEEPFILTERS(FILTER(Orders, Orders[Revenue] > 100)))', True),
    ('e06', 'CALCULATE(ISFILTERED(Dim), FILTER(ALL(Orders), Orders[Revenue] > 100))', True),
    ('e07', 'CALCULATE(ISFILTERED(Dim), CALCULATETABLE(Orders, Orders[Revenue] > 100))', True),
    ('e08', 'CALCULATE(ISFILTERED(Dim), Orders)', True),
    ('e09', 'CALCULATE(ISFILTERED(Dim), SUMMARIZE(Orders, Orders[Region]))', False),
    ('e11', 'CALCULATE(ISFILTERED(Dim[Region]), SUMMARIZE(Orders, Dim[Zone]))', False),
    ('e12', 'CALCULATE(ISFILTERED(Orders), SUMMARIZE(Orders, Dim[Zone]))', False),
    ('e13', 'CALCULATE(ISCROSSFILTERED(Dim), VALUES(Orders[Region]))', False),
    ('e14', 'CALCULATE(SUMX(Orders, IF(ISFILTERED(Orders[Revenue]), 1, 0)), Orders[Revenue] > 100)', 2),
    ('e15', 'SUMX(Orders, IF(ISFILTERED(Orders[Revenue]), 1, 0))', 0),
    ('e16', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Orders[Revenue]), 1, 0)))', 3),
    ('e19', 'CALCULATE(CALCULATE(ISFILTERED(Orders), ALLEXCEPT(Orders, Orders[Region])), Orders[Region] = "N")', True),
    ('e20', 'CALCULATE(CALCULATE(ISFILTERED(Dim), ALLEXCEPT(Orders, Orders[Region])), Dim[Zone] = "z1")', False),
    ('e21', 'CALCULATE(CALCULATE(ISCROSSFILTERED(Orders), ALLEXCEPT(Orders, Dim[Zone])), Dim[Zone] = "z1")', True),
    ('e22', 'CALCULATE(ISFILTERED(Dim), FILTER(Orders, Orders[Revenue] > 100), REMOVEFILTERS(Dim[Zone]))', True),
    ('e23', 'CALCULATE(CALCULATE(ISFILTERED(Dim[Zone]), REMOVEFILTERS(Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', False),
    ('e24', 'CALCULATE(CALCULATE(ISFILTERED(Dim[Region]), REMOVEFILTERS(Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e25', 'CALCULATE(CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', 2),
    ('e26', 'CALCULATE(CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('e27', 'CALCULATE(CALCULATE(ISFILTERED(Dim), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e28', 'CALCULATE(CALCULATE(ISFILTERED(Dim[Zone]), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e29', 'CALCULATE(CALCULATE(ISFILTERED(Dim), ALLEXCEPT(Dim, Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e30', 'CALCULATE(CALCULATE(COUNTROWS(Dim), ALLEXCEPT(Dim, Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('e31', 'CALCULATE(COUNTROWS(Dim), FILTER(Orders, Orders[Revenue] > 100))', 2),
    ('e32', 'CALCULATE(CALCULATE(ISFILTERED(Orders[Revenue]), REMOVEFILTERS(Orders[Revenue])), FILTER(Orders, Orders[Revenue] > 100))', False),
    ('e33', 'CALCULATE(CALCULATE(ISFILTERED(Orders[Region]), REMOVEFILTERS(Orders[Revenue])), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e34', 'CALCULATE(ISFILTERED(Dim), FILTER(Dim, Dim[Zone] = "z1"), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e35', 'CALCULATE(ISCROSSFILTERED(Orders), ALL(Dim), Dim[Zone] = "z1")', True),
    ('e36', 'CALCULATE(CALCULATE(ISCROSSFILTERED(Orders), REMOVEFILTERS(Orders)), Dim[Zone] = "z1")', False),
    ('e37', 'CALCULATE(CALCULATE(ISFILTERED(Orders), REMOVEFILTERS(Dim)), FILTER(Orders, Orders[Revenue] > 100))', True),
    ('e38', 'CALCULATE(ISFILTERED(Orders), ALLSELECTED(Orders))', False),
    ('e39', 'CALCULATE(CALCULATE(ISFILTERED(Orders), ALLSELECTED(Orders)), Orders[Region] = "N")', True),
]

B115 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115.py
    ('v50', 'CALCULATE(CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('v51', 'CALCULATE(CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', 2),
    ('v52', 'CALCULATE(CALCULATE(COUNTROWS(Dim), ALLEXCEPT(Dim, Dim[Zone])), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('v53', 'CALCULATE(CALCULATE(SUM(Orders[Revenue]), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', 400),
    ('v54', 'CALCULATE(CALCULATE(COUNTROWS(Dim), ALL(Dim[Region])), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('v55', 'CALCULATE(CALCULATE(COUNTROWS(Dim), ALL(Dim)), FILTER(Orders, Orders[Revenue] > 100))', 3),
]

B115B = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115b.py; k02 is #148
    ('a01', 'CALCULATE(COUNTROWS(Orders2), FILTER(Orders, Orders[Revenue] > 100))', 3),
    ('a02', 'CALCULATE(COUNTROWS(Orders2), FILTER(Orders, Orders[Revenue] < 100))', 2),
    ('a03', 'CALCULATE(COUNTROWS(Cat), FILTER(Orders, Orders[Revenue] < 100))', 1),
    ('a04', 'CALCULATE(SUM(Cat[Weight]), FILTER(Orders, Orders[Revenue] < 100))', 10),
    ('a05', 'CALCULATE(IF(ISFILTERED(Cat[ZoneName]), 1, 0), FILTER(Orders, Orders[Revenue] < 100))', 1),
    ('a06', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), REMOVEFILTERS(Dim[Region])), FILTER(Orders, Orders[Revenue] < 100))', 3),
    ('a07', 'CALCULATE(CALCULATE(COUNTROWS(Cat), ALL(Dim)), FILTER(Orders, Orders[Revenue] < 100))', 2),
    ('a08', 'CALCULATE(CALCULATE(COUNTROWS(Dim), Orders[Revenue] = 150), FILTER(Orders, Orders[Revenue] > 100))', 2),
    ('a09', 'CALCULATE(CALCULATE(COUNTROWS(Dim), KEEPFILTERS(Orders[Revenue] = 150)), FILTER(Orders, Orders[Revenue] > 100))', 2),
    ('a10', 'CALCULATE(COUNTROWS(Orders), CALCULATETABLE(Orders2, Orders2[Qty] = 3))', 1),
    ('a11', 'CALCULATE(COUNTROWS(Orders), FILTER(Orders2, Orders2[Qty] >= 4))', 1),
    ('a12', 'CALCULATE(COUNTROWS(Dim2), FILTER(Orph, Orph[Amt] > 15))', None),
    ('k01', 'CALCULATE(CALCULATE(COUNTROWS(Dim), KEEPFILTERS(FILTER(ALL(Orders), Orders[Revenue] > 100))), Dim[Zone] = "z2")', 1),
    ('k03', 'CALCULATE(CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders), Orders[Revenue] > 100)), Dim[Zone] = "z2")', 2),
    ('k04', 'CALCULATE(CALCULATE(COUNTROWS(Orders), FILTER(ALL(Orders), Orders[Revenue] > 100)), Dim[Zone] = "z2")', 2),
    ('k05', 'CALCULATE(CALCULATE(COUNTROWS(Orders), FILTER(Orders, Orders[Revenue] > 0)), Dim[Zone] = "z2")', 1),
    ('k06', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), FILTER(ALL(Orders), Orders[Revenue] > 100)), Dim[Zone] = "z2")', 3),
    ('k07', 'CALCULATE(CALCULATE(COUNTROWS(Orders2), KEEPFILTERS(FILTER(ALL(Orders), Orders[Revenue] > 100))), Dim[Zone] = "z2")', 2),
    ('k08', 'CALCULATE(CALCULATE(COUNTROWS(Cat), FILTER(ALL(Orders), Orders[Revenue] > 100)), Cat[Zone] = "z2")', 2),
    ('k09', 'CALCULATE(CALCULATE(COUNTROWS(Dim), ALL(Orders)), Dim[Zone] = "z2")', 3),
    ('k10', 'CALCULATE(CALCULATE(COUNTROWS(Cat), ALL(Orders)), Cat[Zone] = "z2")', 2),
    ('m01', 'CALCULATE(IF(ISFILTERED(Dim), 1, 0), SUMMARIZE(Orders, Orders[Region], Orders[Revenue]))', 0),
    ('m02', 'CALCULATE(IF(ISFILTERED(Dim), 1, 0), SELECTCOLUMNS(Orders, "Region", Orders[Region], "Revenue", Orders[Revenue]))', 0),
    ('m04', 'CALCULATE(IF(ISFILTERED(Dim), 1, 0), ADDCOLUMNS(Orders, "x", 1))', 1),
    ('m05', 'CALCULATE(COUNTROWS(Dim), TOPN(1, Orders, Orders[Revenue]))', 1),
    ('m06', 'CALCULATE(COUNTROWS(Dim), VALUES(Orders))', 3),
    ('m07', 'CALCULATE(COUNTROWS(Dim), DISTINCT(Orders))', 3),
    ('m08', 'CALCULATE(COUNTROWS(Dim), CALCULATETABLE(Orders, Orders[Revenue] > 100))', 2),
    ('m09', 'CALCULATE(COUNTROWS(Dim), FILTER(Orders, Orders[Region] = "N"))', 1),
    ('m10', 'CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders), TRUE()))', 3),
    ('m11', 'CALCULATE(CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders), TRUE())), Dim[Zone] = "z2")', 3),
    ('m12', 'CALCULATE(COUNTROWS(Dim), SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Orders[Revenue]))', 3),
]


@pytest.mark.parametrize("probe,expr,want", B114, ids=[p for p, _e, _w in B114])
def test_b114(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B115, ids=[p for p, _e, _w in B115])
def test_b115_a_dimension_column_taken_out(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B115B, ids=[p for p, _e, _w in B115B])
def test_b115b(probe, expr, want):
    _check(_eval(expr, FULL, FULL_MEASURES, FULL_RELS), want)
