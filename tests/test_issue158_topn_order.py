"""Issue #158: TOPN orders by every order pair, by any value, and keeps ties.

TOPN ordered by its first expression only and only by numbers -- any other
value scored 0 and kept the table's input order, so TOPN(1, VALUES(D[Date]),
D[Date], ASC) returned the first row rather than the earliest date -- and it
cut at n where DAX returns every row tied with the n-th.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b158.py)."""
from __future__ import annotations

from datetime import datetime

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

MODEL = {"D": {"columns": ["Date", "Name", "N", "Code", "Flag", "Opt", "Grp"],
               "rows": [[datetime(2024, 1, 3), "b", 2, "B2", True, 5, "y"],
                        [datetime(2024, 1, 1), "c", 3, "a1", False, None, "x"],
                        [datetime(2024, 1, 2), "a", 1, "_9", True, 7, "x"]]}}
MEASURES = {"MaxName": "MAX(D[Name])"}

DESKTOP = [
    ('d_desc', 'CONCATENATEX(TOPN(1, VALUES(D[Date]), D[Date], DESC), FORMAT(D[Date], "yyyy-mm-dd"), ",")', '2024-01-03'),
    ('d_asc', 'CONCATENATEX(TOPN(1, VALUES(D[Date]), D[Date], ASC), FORMAT(D[Date], "yyyy-mm-dd"), ",")', '2024-01-01'),
    ('d_asc2', 'CONCATENATEX(TOPN(2, D, D[Date], ASC), D[Name], ",", D[Name], ASC)', 'a,c'),
    ('t_asc', 'CONCATENATEX(TOPN(1, D, D[Name], ASC), D[Name], ",")', 'a'),
    ('t_desc2', 'CONCATENATEX(TOPN(2, D, D[Name], DESC), D[Name], ",", D[Name], ASC)', 'b,c'),
    ('t_case', 'CONCATENATEX(TOPN(1, D, D[Code], ASC), D[Name], ",")', 'a'),
    ('t_meas', 'CONCATENATEX(TOPN(1, D, [MaxName], DESC), D[Name], ",")', 'c'),
    ('b_asc', 'CONCATENATEX(TOPN(1, D, D[Flag], ASC), D[Name], ",", D[Name], ASC)', 'c'),
    ('blank_asc', 'CONCATENATEX(TOPN(1, D, D[Opt], ASC), D[Name], ",", D[Name], ASC)', 'c'),
    ('blank_desc', 'CONCATENATEX(TOPN(1, D, D[Opt], DESC), D[Name], ",", D[Name], ASC)', 'a'),
    ('n_asc', 'CONCATENATEX(TOPN(1, D, D[N], ASC), D[Name], ",")', 'a'),
    ('two_keys', 'CONCATENATEX(TOPN(2, D, D[Grp], ASC, D[Date], DESC), D[Name], ",", D[Name], ASC)', 'a,c'),
    ('ties', 'CONCATENATEX(TOPN(1, D, D[Grp], ASC), D[Name], ",", D[Name], ASC)', 'a,c'),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES, p=expr)
    got = de.evaluate_measures_smart(["p"], MODEL, m, {}, simulate_row_context=False)["p"]
    assert str(got) == want
