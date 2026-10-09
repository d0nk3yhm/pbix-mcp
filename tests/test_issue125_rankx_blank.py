"""Issue #125: RANKX treats BLANK as 0 for numbers and as "" for text, in the
ranked value and in every row's value -- and ranks text and dates.

The engine returned BLANK for a BLANK value, dropped rows whose expression was
BLANK, and ranked numbers only. Expected values: Power BI Desktop 2.152 over
ADOMD, grouped by Reps[agent]: Won a 10, b 30 (25 + 5), c 20, d 30, e BLANK (no
deals), f -5; Tag (text) a "m", b "x", c "z", d "b", e BLANK, f "a".
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"Reps": {"columns": ["agent"], "rows": [[a] for a in "abcdef"]},
          "Deals": {"columns": ["agent", "amount", "tag"],
                    "rows": [["a", 10, "m"], ["b", 25, "c"], ["b", 5, "x"], ["c", 20, "z"], ["d", 30, "b"],
                             ["f", -5, "a"]]}}
RELS = [{"FromTable": "Deals", "FromColumn": "agent", "ToTable": "Reps", "ToColumn": "agent", "IsActive": 1,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {
    "Won": "SUM(Deals[amount])", "Tag": "MAX(Deals[tag])",
    "k_desc": "RANKX(ALL(Reps[agent]), [Won])", "k_asc": "RANKX(ALL(Reps[agent]), [Won], , ASC)",
    "k_dense": "RANKX(ALL(Reps[agent]), [Won], , DESC, DENSE)",
    "k_asc_dense": "RANKX(ALL(Reps[agent]), [Won], , ASC, DENSE)",
    "k_vb": "RANKX(ALL(Reps[agent]), [Won], BLANK())", "k_vb_asc": "RANKX(ALL(Reps[agent]), [Won], BLANK(), ASC)",
    "k_txt_asc": "RANKX(ALL(Reps[agent]), [Tag], , ASC)", "k_txt_desc": "RANKX(ALL(Reps[agent]), [Tag], , DESC)",
    "k_wrapped": "IF(ISBLANK([Won]), BLANK(), RANKX(ALL(Reps[agent]), [Won]))",
}
COLS = ["k_desc", "k_asc", "k_dense", "k_asc_dense", "k_vb", "k_vb_asc", "k_txt_asc", "k_txt_desc", "k_wrapped"]
DESKTOP = {   # member -> Desktop's row in COLS order
    "a": [4, 3, 3, 3, 5, 2, 4, 3, 4],
    "b": [1, 5, 1, 5, 5, 2, 5, 2, 1],
    "c": [3, 4, 2, 4, 5, 2, 6, 1, 3],
    "d": [1, 5, 1, 5, 5, 2, 3, 4, 1],
    "f": [6, 1, 5, 1, 5, 2, 2, 5, 6],
    "e": [5, 2, 4, 2, 5, 2, 1, 6, None],    # no deals: BLANK ranks as 0 (and "" as text)
}
CELLS = [(a, m, v) for a, row in DESKTOP.items() for m, v in zip(COLS, row)]


@pytest.mark.parametrize("member,measure,want", CELLS, ids=[f"{a}:{m}" for a, m, _ in CELLS])
def test_matches_desktop(member, measure, want):
    got = de.evaluate_measures_smart([measure], TABLES, MEASURES, {"Reps.agent": [member]}, relationships=RELS,
                                     simulate_row_context=False, group_by={"Reps.agent"}, selected_filters={})
    assert got[measure] == want
