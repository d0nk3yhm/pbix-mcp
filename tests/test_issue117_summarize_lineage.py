"""Issue #117: SUMMARIZE takes its groups from the source rows, and each
column keeps its own table's lineage.

SUMMARIZE(<table expression>, <related column>) built its groups from the
whole table (COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Region] = "W"),
Dim[Zone])) was 2, Desktop 1) and tagged every column with the base table, so
as a CALCULATE filter Dim[Zone] filtered a column Orders does not have, and a
transition over the rows filtered nothing: ADDCOLUMNS(SUMMARIZE(Orders,
Dim[Zone]), "r", CALCULATE(SUM(Orders[Revenue]))) summed to 900, not 450. A
group of several columns filters their combinations, and an extension column
sees its group's source rows, expanded to their dimensions.

A source of COLUMNS -- ALL / ALLSELECTED of several columns, VALUES, another
SUMMARIZE -- is no rows of its table: its rows filter the columns they carry
and expand to nothing, and a group column it does not carry is an error. A
CROSSJOIN's rows filter each part's columns.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b117.py,
build_b115.py, build_b114.py, build_b115b.py)."""
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

# build_b117.py, the model of build_b115b.py plus Dim3 / Fact3 (Dim3 b unused): a snowflake Orders -> Dim -> Cat, a second fact Orders2 on Dim,
# and Orph -> Dim2 with an orphan key kX
B117M = {
    "Cat": {"columns": ["Zone", "ZoneName", "Weight"], "rows": [["z1", "Zone 1", 10], ["z2", "Zone 2", 20]]},
    "Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
    "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]},
    "Orders2": {"columns": ["Region", "Qty"], "rows": [["N", 1], ["N", 2], ["S", 3], ["W", 4], ["W", 5]]},
    "Dim2": {"columns": ["Key", "Name"], "rows": [["k1", "n1"], ["k2", "n2"]]},
    "Orph": {"columns": ["Key", "Amt"], "rows": [["k1", 10], ["kX", 20]]},
    "Dim3": {"columns": ["Key", "Grp"], "rows": [["a", "g1"], ["b", "g1"], ["c", "g2"]]},
    "Fact3": {"columns": ["Key", "V"], "rows": [["a", 1], ["a", 3], ["c", 2]]},
    # no key; (a1, b1, c2) is the row a condition on B and C leaves out
    "T4": {"columns": ["A", "B", "C", "V"],
           "rows": [["a1", "b1", "c1", 1], ["a1", "b2", "c2", 2], ["a1", "b1", "c2", 1], ["a2", "b2", "c1", 8]]},
}
B117M_RELS = [_rel("Orders", "Region", "Dim", "Region"), _rel("Orders2", "Region", "Dim", "Region"),
             _rel("Dim", "Zone", "Cat", "Zone"), _rel("Orph", "Key", "Dim2", "Key"),
             _rel("Fact3", "Key", "Dim3", "Key")]
B117M_MEASURES = {"cDim": "COUNTROWS(Dim)", "cOrders2": "COUNTROWS(Orders2)", "cDim2": "COUNTROWS(Dim2)",
                  "h01": "SUM(Orders2[Qty])"}
ERROR = "ERROR"     # Desktop refuses the expression

B117 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b117.py
    ('g01', 'COUNTROWS(SUMMARIZE(Orders, Dim[Zone]))', 2),
    ('g02', 'COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Dim[Zone]))', 2),
    ('g03', 'COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Dim[Zone]))', 1),
    ('g04', 'COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 1),
    ('g05', 'CONCATENATEX(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Dim[Zone]), Dim[Zone], ",")', 'z1'),
    ('g06', 'CONCATENATEX(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone]), Orders[Region] & Dim[Zone], ",", Orders[Region], ASC)', 'Sz1,Wz2'),
    ('g07', 'COUNTROWS(SUMMARIZE(Orders, Cat[ZoneName]))', 2),
    ('g08', 'COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Cat[ZoneName]))', 1),
    ('g09', 'COUNTROWS(SUMMARIZE(Orph, Dim2[Name]))', 2),
    ('g10', 'COUNTROWS(SUMMARIZE(Orders2, Dim[Zone], Cat[ZoneName]))', 2),
    ('g11', 'COUNTROWS(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 4), Dim[Zone]))', 1),
    ('g12', 'COUNTROWS(SUMMARIZE(CALCULATETABLE(Orders, Orders[Revenue] > 100), Dim[Zone]))', 2),
    ('g13', 'COUNTROWS(SUMMARIZE(TOPN(1, Orders, Orders[Revenue]), Dim[Zone]))', 1),
    ('g14', 'COUNTROWS(SUMMARIZE(ALL(Orders), Dim[Zone]))', 2),
    ('g15', 'CALCULATE(COUNTROWS(SUMMARIZE(Orders, Dim[Zone])), Orders[Region] = "N")', 1),
    ('f01', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 250),
    ('f02', 'CALCULATE(COUNTROWS(Dim), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 1),
    ('f03', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone]))', 400),
    ('f04', 'CALCULATE(COUNTROWS(Orders), SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone]))', 2),
    ('f05', 'CALCULATE(COUNTROWS(Dim), SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone]))', 3),
    ('f06', 'CALCULATE(IF(ISFILTERED(Orders[Region]), 1, 0), SUMMARIZE(Orders, Orders[Region], Dim[Zone]))', 1),
    ('f07', 'CALCULATE(IF(ISFILTERED(Dim[Region]), 1, 0), SUMMARIZE(Orders, Orders[Region], Dim[Zone]))', 0),
    ('f08', 'CALCULATE(IF(ISFILTERED(Dim[Zone]), 1, 0), SUMMARIZE(Orders, Orders[Region], Dim[Zone]))', 1),
    ('f09', 'CALCULATE(IF(ISFILTERED(Dim), 1, 0), SUMMARIZE(Orders, Dim[Zone]))', 1),
    ('f10', 'CALCULATE(IF(ISFILTERED(Orders), 1, 0), SUMMARIZE(Orders, Dim[Zone]))', 0),
    ('f11', 'CALCULATE(SUM(Orders2[Qty]), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 9),
    ('f12', 'CALCULATE(COUNTROWS(Cat), SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Cat[ZoneName]))', 1),
    ('f13', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Cat[ZoneName]))', 200),
    ('f14', 'CALCULATE(SUM(Orders[Revenue]), KEEPFILTERS(SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone])))', 250),
    ('f15', 'CALCULATE(CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone])), Dim[Zone] = "z1")', None),
    ('i01', 'SUMX(SUMMARIZE(Orders, Dim[Zone]), CALCULATE(COUNTROWS(Dim)))', 3),
    ('i02', 'SUMX(SUMMARIZE(Orders, Dim[Zone]), CALCULATE(SUM(Orders[Revenue])))', 450),
    ('i03', 'SUMX(SUMMARIZE(Orders, Orders[Region], Dim[Zone]), CALCULATE(COUNTROWS(Dim)))', 5),
    ('i04', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Dim[Zone]), CALCULATE(SUM(Orders[Revenue])))', 450),
    ('i05', 'MAXX(SUMMARIZE(Orders, Dim[Zone]), CALCULATE(COUNTROWS(Orders2)))', 3),
    ('i06', 'SUMX(SUMMARIZE(Orders, Cat[ZoneName]), CALCULATE(SUM(Cat[Weight])))', 30),
    ('e01', 'SUMX(SUMMARIZE(Orders, Dim[Zone], "r", SUM(Orders[Revenue])), [r])', 450),
    ('e02', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Dim[Zone], "r", SUM(Orders[Revenue])), [r])', 400),
    ('e03', 'MAXX(SUMMARIZE(Orders, Dim[Zone], "n", COUNTROWS(Orders)), [n])', 2),
    ('e04', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone], "r", SUM(Orders[Revenue])), [r])', 400),
    ('e05', 'SUMX(SUMMARIZE(Orders, Dim[Zone], "d", COUNTROWS(Dim)), [d])', 3),
    ('e06', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Dim[Zone], "d", COUNTROWS(Dim)), [d])', 1),
    ('e07', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Dim[Zone], "r", SUM(Orders[Revenue])), [r])', 50),
    ('e08', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "q", SUM(Orders2[Qty])), [q])', 14),
    ('e09', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "c", CALCULATE(COUNTROWS(Orders2), ALL(Orders2))), [c])', 10),
    ('e10', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "s", CALCULATE(COUNTROWS(Orders2), ALLSELECTED(Orders2))), [s])', 8),
    ('e11', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "x", COUNTROWS(Orders)), [x])', 3),
    ('e12', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "y", CALCULATE(COUNTROWS(Orders2), REMOVEFILTERS(Orders2[Qty]))), [y])', 5),
    ('e13', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Orders2[Region], "q", SUM(Orders2[Qty])), [q])', 14),
    ('e14', 'SUMX(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Dim[Zone], "z", CALCULATE(SUM(Orders2[Qty]), ALL(Dim))), [z])', 14),
    ('e15', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Cat[ZoneName], "w", SUM(Cat[Weight])), [w])', 10),
    ('e16', 'SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] < 100), Dim[Zone], "dr", COUNTROWS(VALUES(Dim[Region]))), [dr])', 1),
    ('e17', 'SUMX(SUMMARIZE(Fact3, Dim3[Grp], "n", COUNTROWS(Dim3)), [n])', 2),
    ('e18', 'CONCATENATEX(SUMMARIZE(Fact3, Dim3[Grp], "k", CONCATENATEX(VALUES(Dim3[Key]), Dim3[Key], "")), [k], ",", [k], ASC)', 'a,c'),
    ('e19', 'SUMX(ADDCOLUMNS(SUMMARIZE(Fact3, Dim3[Grp]), "n", CALCULATE(COUNTROWS(Dim3))), [n])', 3),
    ('e20', 'COUNTROWS(SUMMARIZE(Fact3, Dim3[Grp]))', 2),
    ('e21', 'SUMX(SUMMARIZE(Fact3, Dim3[Grp], "v", SUM(Fact3[V])), [v])', 6),
    ('e22', 'SUMX(SUMMARIZE(Fact3, Dim3[Grp], "a", CALCULATE(COUNTROWS(Dim3), ALL(Fact3))), [a])', 6),
    ('b01', 'CALCULATE(COUNTROWS(Orders2), SUMMARIZE(FILTER(Orders2, Orders2[Qty] > 1), Orders2[Region], Orders2[Qty]))', 4),
    ('b02', 'CALCULATE(IF(ISFILTERED(Orders2[Qty]), 1, 0), SUMMARIZE(Orders2, Orders2[Region], Orders2[Qty]))', 1),
    ('b03', 'SUMX(SUMMARIZE(Orders2, Orders2[Region], Orders2[Qty]), CALCULATE(COUNTROWS(Orders2)))', 5),
    ('b04', 'COUNTROWS(SUMMARIZE(FILTER(Orders2, Orders2[Qty] > 1), Orders2[Region]))', 3),
    ('a01', 'SUMX(ADDCOLUMNS(SUMMARIZE(Orders, Dim[Zone]), "r", CALCULATE(SUM(Orders[Revenue]))), [r])', 450),
    ('a02', 'SUMX(ADDCOLUMNS(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Dim[Zone]), "r", CALCULATE(SUM(Orders[Revenue]))), [r])', 450),
    ('a03', 'MAXX(ADDCOLUMNS(SUMMARIZE(Orders, Dim[Zone]), "n", CALCULATE(COUNTROWS(Orders))), [n])', 2),
]

B117C = [   # a source of COLUMNS, a CROSSJOIN, another SUMMARIZE -- build_b117.py
    ('c01', 'SUMX(SUMMARIZE(FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 2), Orders2[Region], "q", SUM(Orders2[Qty])), [q])', 14),
    ('c02', 'SUMX(SUMMARIZE(ALLSELECTED(Orders2[Region], Orders2[Qty]), Orders2[Region], Orders2[Qty], "n", COUNTROWS(Orders2)), [n])', 5),
    ('c03', 'SUMX(SUMMARIZE(ALLSELECTED(Orders2[Region], Orders2[Qty]), Orders2[Region], "k", COUNTX(FILTER(ALLSELECTED(Orders2[Qty]), CALCULATE(SUM(Orders2[Qty])) >= 2), 1)), [k])', 4),
    ('c04', 'SUMX(SUMMARIZE(FILTER(ALL(Dim[Region], Dim[Zone]), Dim[Region] <> "S"), Dim[Zone], "r", SUM(Orders[Revenue])), [r])', 300),
    ('c05', 'COUNTROWS(SUMMARIZE(ALL(Orders[Region]), Dim[Zone]))', ERROR),
    ('c06', 'SUMX(SUMMARIZE(VALUES(Orders2[Region]), Orders2[Region], "q", SUM(Orders2[Qty])), [q])', 15),
    ('c07', 'SUMX(SUMMARIZE(ALLSELECTED(Orders2[Region], Orders2[Qty]), Orders2[Region], Orders2[Qty], "t", [h01], "k", COALESCE(COUNTX(FILTER(ALLSELECTED(Orders2[Qty]), [h01] >= 2), 1), 0)), [t] * 10 + [k])', 157),
    ('c08', 'SUMX(SUMMARIZE(FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 2), Orders2[Region], "a", CALCULATE(SUM(Orders2[Qty]), ALL(Orders2[Qty]))), [a])', 15),
    ('c09', 'SUMX(SUMMARIZE(FILTER(CROSSJOIN(VALUES(Dim[Zone]), VALUES(Orders2[Qty])), Orders2[Qty] >= 4), Dim[Zone], "q", SUM(Orders2[Qty])), [q])', 9),
    ('c10', 'SUMX(SUMMARIZE(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty], "d", COUNTROWS(Dim)), [d])', 15),
    ('c11', 'SUMX(SUMMARIZE(ALL(Orders2), Orders2[Qty], "d", COUNTROWS(Dim)), [d])', 5),
    ('c12', 'SUMX(SUMMARIZE(FILTER(VALUES(Orders2[Qty]), Orders2[Qty] >= 3), Orders2[Qty], "r", COUNTROWS(VALUES(Orders2[Region]))), [r])', 3),
    ('c13', 'CALCULATE(SUMX(SUMMARIZE(ALLSELECTED(Orders2[Region], Orders2[Qty]), Orders2[Region], "q", SUM(Orders2[Qty])), [q]), Orders2[Qty] >= 3)', 12),
    ('c14', 'SUMX(SUMMARIZE(FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 2), Orders2[Region], "d", COUNTROWS(Dim)), [d])', 9),
    ('c15', 'SUMX(SUMMARIZE(FILTER(ALL(Fact3[Key], Fact3[V]), Fact3[V] >= 2), Fact3[Key], "n", COUNTROWS(Dim3)), [n])', 6),
    ('c16', 'SUMX(SUMMARIZE(CROSSJOIN(VALUES(Dim3[Grp]), FILTER(VALUES(Fact3[V]), Fact3[V] >= 2)), Dim3[Grp], "v", SUM(Fact3[V])), [v])', 5),
    ('c17', 'SUMX(SUMMARIZE(SUMMARIZE(FILTER(Orders2, Orders2[Qty] >= 2), Orders2[Region], Orders2[Qty]), Orders2[Region], "q", SUM(Orders2[Qty])), [q])', 14),
    ('c18', 'SUMX(SUMMARIZE(ADDCOLUMNS(FILTER(Orders2, Orders2[Qty] >= 2), "x", 1), Dim[Zone], "d", COUNTROWS(Dim)), [d])', 3),
    ('c19', 'SUMX(SUMMARIZE(CROSSJOIN(FILTER(Orders2, Orders2[Qty] >= 4), VALUES(Cat[ZoneName])), Dim[Zone], "d", COUNTROWS(Dim)), [d])', 1),
    ('c23', 'COUNTROWS(SUMMARIZE(ALL(Orders2[Region], Orders2[Qty]), Dim[Zone]))', ERROR),
]

B115 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115.py
    ('v30', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 250),
    ('v31', 'CALCULATE(COUNTROWS(Dim), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 1),
    ('v32', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Orders[Region], Dim[Zone]))', 400),
    ('v33', 'COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))', 1),
    ('v34', 'CALCULATE(SUM(Orders[Revenue]), SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Orders[Region]))', 250),
]

B114 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b114.py
    ('e09', 'CALCULATE(ISFILTERED(Dim), SUMMARIZE(Orders, Orders[Region]))', False),
    ('e10', 'CALCULATE(ISFILTERED(Dim), SUMMARIZE(Orders, Dim[Zone]))', True),
    ('e11', 'CALCULATE(ISFILTERED(Dim[Region]), SUMMARIZE(Orders, Dim[Zone]))', False),
    ('e12', 'CALCULATE(ISFILTERED(Orders), SUMMARIZE(Orders, Dim[Zone]))', False),
]

B115B = [   # (probe, expression, Power BI Desktop 2.152) -- build_b115b.py
    ('x09', 'SUMX(SUMMARIZE(Orders, Orders[Region], Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))', 9),
    ('x10', 'SUMX(SUMMARIZE(Orders, Dim[Zone]), CALCULATE(COUNTROWS(Dim)))', 3),
]


@pytest.mark.parametrize("probe,expr,want", B117, ids=[p for p, _e, _w in B117])
def test_b117(probe, expr, want):
    _check(_eval(expr, B117M, B117M_MEASURES, B117M_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B117C, ids=[p for p, _e, _w in B117C])
def test_b117_sources(probe, expr, want):
    de._engine.eval_errors.clear()
    got = _eval(expr, B117M, B117M_MEASURES, B117M_RELS)
    if want == ERROR:
        assert got is None
        assert "not found in the input table" in de._engine.eval_errors.get("p", "")
    else:
        _check(got, want)


@pytest.mark.parametrize("probe,expr,want", B115, ids=[p for p, _e, _w in B115])
def test_b115(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B114, ids=[p for p, _e, _w in B114])
def test_b114(probe, expr, want):
    _check(_eval(expr, ORDERS, ORDERS_MEASURES, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B115B, ids=[p for p, _e, _w in B115B])
def test_b115b(probe, expr, want):
    _check(_eval(expr, B117M, B117M_MEASURES, B117M_RELS), want)
