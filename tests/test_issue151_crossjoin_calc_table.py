"""Issue #151: a CROSSJOIN calculated table takes each part's columns.

The calculated-table evaluator returned a CROSSJOIN row's internal keys -- the
right table's ``_2_``-prefixed names and the ``__parts__`` tuple (#108) -- as
its columns, so pbix_datamodel_add_calculated_table failed with INTERNAL_ERROR
("unhashable type: 'dict'"). A row's columns are now each part's columns in
order, then the columns ADDCOLUMNS added; a name two tables share is refused,
as Desktop refuses it. SUMMARIZE over several tables keeps the order written.

Expected shapes: Power BI Desktop 2.152, the tables added through its own TOM
library and recalculated (tom_calc_tables.ps1, the model of build_b117.py).
A table the tool added, opened in Desktop and refreshed, reads the same data
as built.
"""
from __future__ import annotations

import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.dax.calc_tables import evaluate_calc_table_expression

pytestmark = pytest.mark.unit


def _rel(many, many_col, one, one_col):
    return {"FromTable": many, "FromColumn": many_col, "ToTable": one, "ToColumn": one_col,
            "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


# the model of build_b117.py: Orders -> Dim -> Cat
B117M = {
    "Cat": {"columns": ["Zone", "ZoneName", "Weight"], "rows": [["z1", "Zone 1", 10], ["z2", "Zone 2", 20]]},
    "Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
    "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]},
}
B117M_RELS = [_rel("Orders", "Region", "Dim", "Region"), _rel("Dim", "Zone", "Cat", "Zone")]
B117M_MEASURES: dict = {}

DESKTOP = [   # (name, expression, Desktop's columns or None when refused, rows)
    ('CJ1', "CROSSJOIN(VALUES(Dim[Zone]), VALUES(Orders[Region]))", ['Zone', 'Region'], 6),
    ('CJ2', "CROSSJOIN(VALUES(Dim[Region]), VALUES(Orders[Region]))", None, None),
    ('CJ3', "CROSSJOIN(Cat, VALUES(Orders[Region]))", ['Zone', 'ZoneName', 'Weight', 'Region'], 6),
    ('CJ4', 'ADDCOLUMNS(CROSSJOIN(VALUES(Dim[Zone]), VALUES(Orders[Region])), "r", '
            'CALCULATE(SUM(Orders[Revenue])))', ['Zone', 'Region', 'r'], 6),
    ('SM1', "SUMMARIZE(Orders, Dim[Zone], Orders[Region])", ['Zone', 'Region'], 3),
    ('SM2', 'ADDCOLUMNS(SUMMARIZE(Orders, Dim[Zone]), "r", CALCULATE(SUM(Orders[Revenue])))', ['Zone', 'r'], 2),
    ('SM3', 'SUMMARIZE(Orders, Cat[ZoneName], "r", SUM(Orders[Revenue]))', ['ZoneName', 'r'], 2),
    ('SM4', "SUMMARIZE(Orders, Orders[Region], Dim[Region])", None, None),
]


@pytest.mark.parametrize("name,expr,cols,nrows", DESKTOP, ids=[d[0] for d in DESKTOP])
def test_shape_matches_desktop(name, expr, cols, nrows):
    result, err = evaluate_calc_table_expression(expr, B117M, B117M_MEASURES, B117M_RELS)
    if cols is None:
        assert result is None and "comes from two tables" in err
    else:
        assert err is None, err
        assert result["columns"] == cols
        assert len(result["rows"]) == nrows


def test_values_of_the_added_columns():
    result, err = evaluate_calc_table_expression(DESKTOP[3][1], B117M, B117M_MEASURES, B117M_RELS)
    assert err is None, err
    got = sorted((r[0], r[1], r[2]) for r in result["rows"])
    # Desktop: N/z1 50, S/z1 150, W/z2 250, every other combination BLANK
    assert got == sorted([("z1", "N", 50.0), ("z1", "S", 150.0), ("z1", "W", None),
                          ("z2", "N", None), ("z2", "S", None), ("z2", "W", 250.0)])


def _mk(tmp_path, alias):
    p = str(tmp_path / f"{alias}.pbix")
    r = json.loads(S.pbix_create(p, alias, json.dumps([
        {"name": "T1", "columns": [{"name": "A", "data_type": "String"}],
         "rows": [{"A": "a1"}, {"A": "a2"}]},
        {"name": "T2", "columns": [{"name": "B", "data_type": "String"}],
         "rows": [{"B": "b1"}, {"B": "b2"}]},
        {"name": "T3", "columns": [{"name": "A", "data_type": "String"}],
         "rows": [{"A": "x"}]}])))
    assert r["success"], r
    return p


def test_tool_adds_a_crossjoin_table(tmp_path):
    alias = "cj151"
    p = _mk(tmp_path, alias)
    try:
        r = json.loads(S.pbix_datamodel_add_calculated_table(
            alias, "CJ", "CROSSJOIN(VALUES(T1[A]), VALUES(T2[B]))"))
        assert r["success"], r
        assert "Columns: A, B" in r["message"] and "Rows materialized: 4" in r["message"]
        assert json.loads(S.pbix_save(alias, output_path=p, overwrite=True))["success"]
        S.pbix_close(alias)
        assert json.loads(S.pbix_open(p, alias + "_r"))["success"]
        data = json.loads(S.pbix_get_table_data(alias + "_r", "CJ"))["message"]
        for v in ("a1", "a2", "b1", "b2"):
            assert v in data
    finally:
        for a in (alias, alias + "_r"):
            S._open_files.pop(a, None)
            S._dax_cache.pop(a, None)


def test_tool_refuses_a_name_two_tables_share(tmp_path):
    alias = "cj151dup"
    _mk(tmp_path, alias)
    try:
        r = json.loads(S.pbix_datamodel_add_calculated_table(
            alias, "CJ", "CROSSJOIN(VALUES(T1[A]), VALUES(T3[A]))"))
        assert not r["success"]
        assert "comes from two tables" in r["message"]
    finally:
        S._open_files.pop(alias, None)
        S._dax_cache.pop(alias, None)
