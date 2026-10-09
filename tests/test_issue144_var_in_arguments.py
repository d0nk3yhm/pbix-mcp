"""Issue #144: a VAR block anywhere but at the start of an expression evaluates.

The analyzer read ANY text holding the words VAR and RETURN as one VAR block,
so SUMX(T, VAR x = 1 RETURN x), IF(c, VAR ... RETURN ..., ...) and
CALCULATE(VAR ... RETURN ..., ...) were split at the wrong places and read
BLANK or a wrong value. A VAR block is now only an expression that starts with
VAR; one in an argument is evaluated when the argument is.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b144.py, and r30 of
build_b116.py), one EVALUATE ROW per probe, generated from its output.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
          "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]}}
RELS = [{"FromTable": "Orders", "FromColumn": "Region", "ToTable": "Dim", "ToColumn": "Region",
         "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b144.py's output
    ('var_sumx_const', 'SUMX(Orders, VAR t = 1 RETURN t)', 3),
    ('var_sumx_row', 'SUMX(Orders, VAR t = Orders[Revenue] RETURN t * 2)', 900),
    ('var_if', 'IF(TRUE(), VAR a = 5 RETURN a + 1, 0)', 6),
    ('var_calculate', 'CALCULATE(VAR s = SUM(Orders[Revenue]) RETURN s, Orders[Region] = "N")', 50),
    ('var_filter_cond', 'COUNTROWS(FILTER(Orders, VAR v = Orders[Revenue] RETURN v > 100))', 2),
    ('var_addcolumns', 'SUMX(ADDCOLUMNS(Orders, "x", VAR r = Orders[Revenue] RETURN r / 50), [x])', 9),
    ('var_nested_blocks', 'SUMX(Orders, VAR a = Orders[Revenue] RETURN VAR b = a + 1 RETURN b)', 453),
    ('var_paren', '(VAR t = 3 RETURN t) * 2', 6),
    ('var_divide', 'DIVIDE(VAR n = 10 RETURN n, VAR d = 4 RETURN d)', 2.5),
    ('r30', 'SUMX(Orders, VAR t = FILTER(Orders, Orders[Revenue] > 100) RETURN COUNTROWS(t))', 6),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    got = de.evaluate_measures_smart(["p"], TABLES, {"p": expr}, {}, relationships=RELS,
                                     simulate_row_context=False)["p"]
    assert got == pytest.approx(want)


def test_a_measure_that_starts_with_var_is_still_a_var_block():
    got = de.evaluate_measures_smart(["p"], TABLES, {"p": "VAR a = 2 VAR b = a * 3 RETURN a + b"}, {},
                                     relationships=RELS, simulate_row_context=False)["p"]
    assert got == 8
