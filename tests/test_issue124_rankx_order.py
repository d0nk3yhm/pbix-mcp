"""Issue #124: RANKX's order -- omitted, empty, 0, FALSE, FALSE() and DESC rank
high to low; 1, TRUE and ASC low to high.

_fn_rankx tested 'DESC' in the argument text, so 0, FALSE and an omitted order
ranked ascending. Expected values: Power BI Desktop 2.152 over ADOMD, grouped
by T[c] (values a 10, b 30, c 20, d 30).
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["c", "v"], "rows": [["a", 10], ["b", 30], ["c", 20], ["d", 30]]}}
MEASURES = {
    "S": "SUM(T[v])",
    "r_om": "RANKX(ALL(T[c]), [S])", "r_0": "RANKX(ALL(T[c]), [S], , 0)",
    "r_f": "RANKX(ALL(T[c]), [S], , FALSE)", "r_fn": "RANKX(ALL(T[c]), [S], , FALSE())",
    "r_1": "RANKX(ALL(T[c]), [S], , 1)", "r_t": "RANKX(ALL(T[c]), [S], , TRUE)",
    "r_asc": "RANKX(ALL(T[c]), [S], , ASC)", "r_desc": "RANKX(ALL(T[c]), [S], , DESC)",
    "r_dense": "RANKX(ALL(T[c]), [S], , , DENSE)", "r_dense_asc": "RANKX(ALL(T[c]), [S], , ASC, DENSE)",
    "r_skip": "RANKX(ALL(T[c]), [S], , DESC, SKIP)", "r_v25": "RANKX(ALL(T[c]), [S], 25)",
    "r_v25_asc": "RANKX(ALL(T[c]), [S], 25, ASC)", "r_v25_0": "RANKX(ALL(T[c]), [S], 25, 0)",
}
COLS = ["S", "r_om", "r_0", "r_f", "r_fn", "r_1", "r_t", "r_asc", "r_desc", "r_dense", "r_dense_asc",
        "r_skip", "r_v25", "r_v25_asc", "r_v25_0"]
DESKTOP = {   # member -> Desktop's row in COLS order
    "a": [10, 4, 4, 4, 4, 1, 1, 1, 4, 3, 1, 4, 3, 3, 3],
    "b": [30, 1, 1, 1, 1, 3, 3, 3, 1, 1, 3, 1, 3, 3, 3],
    "c": [20, 3, 3, 3, 3, 2, 2, 2, 3, 2, 2, 3, 3, 3, 3],
    "d": [30, 1, 1, 1, 1, 3, 3, 3, 1, 1, 3, 1, 3, 3, 3],
}
CELLS = [(c, m, v) for c, row in DESKTOP.items() for m, v in zip(COLS, row)]


@pytest.mark.parametrize("member,measure,want", CELLS, ids=[f"{c}:{m}" for c, m, _ in CELLS])
def test_matches_desktop(member, measure, want):
    got = de.evaluate_measures_smart([measure], TABLES, MEASURES, {"T.c": [member]},
                                     simulate_row_context=False, group_by={"T.c"}, selected_filters={})
    assert got[measure] == want
