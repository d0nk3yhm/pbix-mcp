"""Issue #115: the context transition of a table's row filters the row's
EXPANDED table.

CALCULATE and a measure reference inside SUMX(Orders, ...) see each one-side
dimension at the row's related member: the columns of every table the row
reaches many-to-one (a snowflake's second hop too) take the related row's
values, and the blank row's BLANKs for a key that matches nothing. They are
filters of their own, so ISFILTERED sees them, REMOVEFILTERS of one leaves the
others, ALL of the dimension or of the fact takes them out, and they reach the
facts that share the dimension. A row of SUMMARIZE or SELECTCOLUMNS filters
its columns only. DATEADD / RELATEDTABLE in an iteration see the same.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe
(build_b115.py, build_b116.py, build_b114.py, build_b115b.py)."""
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

B115 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115.py
    ('v01', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim)))', 3),
    ('v02', 'SUMX(Orders, CALCULATE(DISTINCTCOUNT(Dim[Zone])))', 3),
    ('v03', 'SUMX(VALUES(Orders[Region]), CALCULATE(COUNTROWS(Dim)))', 9),
    ('v04', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders)))', 3),
    ('v05', 'SUMX(Orders, [cDim])', 3),
    ('v06', 'SUMX(Dim, CALCULATE(COUNTROWS(Orders)))', 3),
    ('v07', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALL(Orders)))', 9),
    ('v08', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALL(Dim)))', 9),
    ('v09', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Orders[Revenue])))', 3),
    ('v10', 'SUMX(FILTER(Orders, Orders[Revenue] > 100), CALCULATE(COUNTROWS(Dim)))', 2),
    ('v11', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), Dim[Zone] = "z1"))', 2),
    ('v12', 'SUMX(Orders, CALCULATE(SUMX(Dim, 1)))', 3),
]

B116 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b116.py r16, left out of #116's tests
    ('r16', 'SUMX(Orders, COUNTROWS(RELATEDTABLE(Dim)))', 3),
]

B114 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b114.py
    ('e17', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Dim[Zone]), 1, 0)))', 3),
    ('e18', 'SUMX(Orders, CALCULATE(IF(ISCROSSFILTERED(Dim), 1, 0)))', 3),
]

B115B = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115b.py; x10 is #117
    ('t01', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Region])))', 5),
    ('t02', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Dim[Zone])))', 3),
    ('t03', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALLEXCEPT(Dim, Dim[Zone])))', 5),
    ('t04', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), Dim[Region] = "N"))', 2),
    ('t05', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), KEEPFILTERS(Dim[Zone] = "z1")))', 2),
    ('t06', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders), REMOVEFILTERS(Orders[Region], Orders[Revenue])))', 3),
    ('t07', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders), ALL(Orders[Region])))', 3),
    ('t08', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), REMOVEFILTERS(Orders[Region], Orders[Revenue])))', 3),
    ('t09', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), Orders[Revenue] = 50))', 3),
    ('t10', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), KEEPFILTERS(Orders[Revenue] = 50)))', 3),
    ('t11', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Dim[Zone]), 1, 0)))', 3),
    ('t12', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Cat[ZoneName]), 1, 0)))', 3),
    ('t13', 'SUMX(Orders, CALCULATE(IF(ISINSCOPE(Dim[Zone]), 1, 0)))', 3),
    ('t14', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Orders2[Qty]), 1, 0)))', 0),
    ('t15', 'SUMX(Orders, CALCULATE(IF(ISCROSSFILTERED(Orders2), 1, 0)))', 3),
    ('t16', 'SUMX(Orders, CALCULATE(COUNTROWS(ALLSELECTED(Dim))))', 9),
    ('t17', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALLSELECTED(Dim)))', 9),
    ('t18', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALLSELECTED(Orders)))', 9),
    ('t19', 'SUMX(Orders, CALCULATE(IF(ISFILTERED(Dim), 1, 0), ALL(Dim[Zone])))', 3),
    ('s01', 'SUMX(Orders, CALCULATE(COUNTROWS(Cat)))', 3),
    ('s02', 'SUMX(Orders, CALCULATE(SUM(Cat[Weight])))', 40),
    ('s03', 'SUMX(Orders, CALCULATE(COUNTROWS(Cat), ALL(Dim)))', 6),
    ('s04', 'SUMX(Orders, CALCULATE(COUNTROWS(Cat), ALL(Dim[Zone])))', 3),
    ('s05', 'SUMX(Orders, CALCULATE(COUNTROWS(Dim), ALL(Cat)))', 3),
    ('s06', 'SUMX(Dim, CALCULATE(COUNTROWS(Cat)))', 3),
    ('s07', 'SUMX(Dim, CALCULATE(SUM(Cat[Weight])))', 40),
    ('s08', 'SUMX(Orders, CALCULATE(COUNTROWS(Cat), REMOVEFILTERS(Dim[Region])))', 3),
    ('f01', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2)))', 5),
    ('f02', 'SUMX(Orders, [cOrders2])', 5),
    ('f03', 'SUMX(Orders, CALCULATE(SUM(Orders2[Qty])))', 15),
    ('f04', 'SUMX(Orders2, CALCULATE(SUM(Orders[Revenue])))', 750),
    ('f05', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2), ALL(Dim)))', 15),
    ('f06', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2), ALL(Orders)))', 15),
    ('f07', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2), REMOVEFILTERS(Dim[Region])))', 8),
    ('f08', 'SUMX(FILTER(Orders, CALCULATE(COUNTROWS(Orders2)) >= 2), Orders[Revenue])', 300),
    ('f09', 'COUNTROWS(FILTER(Orders, CALCULATE(COUNTROWS(Dim)) = 1))', 3),
    ('f10', 'CONCATENATEX(Orders, CALCULATE(SELECTEDVALUE(Dim[Zone])), ",", Orders[Region], ASC)', 'z1,z1,z2'),
    ('f11', 'CONCATENATEX(Orders, CALCULATE(SELECTEDVALUE(Cat[ZoneName])), ",", Orders[Region], ASC)', 'Zone 1,Zone 1,Zone 2'),
    ('f12', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2), Dim[Zone] = "z1"))', 3),
    ('o01', 'SUMX(Orph, CALCULATE(COUNTROWS(Dim2)))', 1),
    ('o02', 'SUMX(Orph, CALCULATE(COUNTROWS(VALUES(Dim2[Name]))))', 2),
    ('o03', 'SUMX(Orph, CALCULATE(DISTINCTCOUNT(Dim2[Key])))', 1),
    ('o04', 'CONCATENATEX(Orph, CALCULATE(SELECTEDVALUE(Dim2[Name], "none")), ",", Orph[Key], ASC)', 'n1,'),
    ('o05', 'SUMX(Orph, CALCULATE(IF(ISBLANK(SELECTEDVALUE(Dim2[Name])), 1, 0)))', 1),
    ('o06', 'SUMX(Orph, CALCULATE(COUNTROWS(Orph)))', 2),
    ('o07', 'SUMX(Orph, CALCULATE(COUNTROWS(Dim2), REMOVEFILTERS(Orph[Amt])))', 1),
    ('x01', 'SUMX(ADDCOLUMNS(Orders, "x", 1), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x02', 'SUMX(SELECTCOLUMNS(Orders, "r", Orders[Region]), CALCULATE(COUNTROWS(Dim)))', 9),
    ('x03', 'SUMX(SELECTCOLUMNS(Orders, "r", Orders[Region], "v", Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))', 9),
    ('x04', 'SUMX(ALL(Orders), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x05', 'SUMX(CALCULATETABLE(Orders), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x06', 'SUMX(TOPN(2, Orders, Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))', 2),
    ('x07', 'SUMX(VALUES(Orders), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x08', 'SUMX(DISTINCT(Orders), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x09', 'SUMX(SUMMARIZE(Orders, Orders[Region], Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))', 9),
    ('x11', 'SUMX(ADDCOLUMNS(Orders, "c", CALCULATE(COUNTROWS(Dim))), [c])', 3),
    ('x12', 'SUMX(RELATEDTABLE(Orders), CALCULATE(COUNTROWS(Dim)))', 3),
    ('x13', 'SUMX(Dim, SUMX(RELATEDTABLE(Orders2), CALCULATE(COUNTROWS(Orders))))', 5),
    ('x14', 'SUMX(FILTER(ALL(Orders), Orders[Revenue] > 100), CALCULATE(COUNTROWS(Cat)))', 2),
    ('x15', 'SUMX(CROSSJOIN(Orders, Cat), CALCULATE(COUNTROWS(Dim)))', 3),
    ('r01', 'SUMX(Orders, COUNTROWS(RELATEDTABLE(Orders2)))', 5),
    ('r02', 'SUMX(Orders2, COUNTROWS(RELATEDTABLE(Orders)))', 5),
    ('r03', 'SUMX(Orders, CALCULATE(COUNTROWS(Orders2), ALL(Orders2[Qty])))', 5),
    ('n01', 'SUMX(Dim, SUMX(Orders, CALCULATE(COUNTROWS(Dim))))', 9),
    ('n02', 'SUMX(Orders, SUMX(Dim, CALCULATE(COUNTROWS(Dim))))', 9),
    ('n03', 'SUMX(Dim, SUMX(Orders, CALCULATE(COUNTROWS(Orders))))', 9),
    ('n05', 'SUMX(Orders, SUMX(Orders2, CALCULATE(COUNTROWS(Dim))))', 15),
    ('n06', 'SUMX(Orders, SUMX(Orders, CALCULATE(COUNTROWS(Orders))))', 9),
    ('n07', 'SUMX(Dim, SUMX(Orders, [cDim]))', 9),
    ('m03', 'SUMX(SELECTCOLUMNS(Orders, "Region", Orders[Region], "Revenue", Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))', 9),
]


@pytest.mark.parametrize("probe,expr,want", B115, ids=[p for p, _e, _w in B115])
def test_b115(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B116, ids=[p for p, _e, _w in B116])
def test_b116_relatedtable_over_a_fact_row(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B114, ids=[p for p, _e, _w in B114])
def test_b114_isfiltered_after_the_transition(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B115B, ids=[p for p, _e, _w in B115B])
def test_b115b(probe, expr, want):
    _check(_eval(expr, FULL, FULL_MEASURES, FULL_RELS), want)
