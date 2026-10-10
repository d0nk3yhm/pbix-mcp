"""Issue #181 (OpenBI's doc 59): evaluate_per_dimension answers for a
dimension joined on a TEXT key.

Since #163 the engine propagates a text key with its ASCII case folded
(_get_cross_table_filters gives 'p1' for "P1"), and evaluate_per_dimension
still bucketed the fact rows by str(cell): no row matched, and every member
-- the blank one too -- was BLANK, while evaluate_measures_batch with the same
filter was right. OpenBI's category charts over product or country codes drew
blank bars.

Expected values: Power BI Desktop 2.152 over ADOMD, as built and after its
own refresh (the Desktop oracle toolkit's build_b179.py): the keys' case
differs between the tables, and one fact key (P4) has no dimension row."""
from __future__ import annotations

import json
import warnings

import pytest

from pbix_mcp.dax import engine as dax

pytestmark = pytest.mark.unit

DIM = [["p1", "A"], ["P2", "B"], ["p3", "A"]]
FACT = [["P1", 100], ["p2", 250], ["p1", 50], ["P3", 7], ["P4", 1000]]
TABLES = {"DIM": {"columns": ["PK", "CAT"], "rows": DIM}, "FACT": {"columns": ["PK", "REV"], "rows": FACT}}
RELS = [{"FromTable": "FACT", "FromColumn": "PK", "ToTable": "DIM", "ToColumn": "PK", "IsActive": 1}]
MEASURES = {"Revenue": "SUM(FACT[REV])"}
# Desktop: SUMMARIZECOLUMNS(DIM[CAT] / DIM[PK], "r", [Revenue]); None is the blank member
DESKTOP_BY_CAT = {"A": 157, "B": 250, None: 1000}
DESKTOP_BY_KEY = {"p1": 150, "P2": 250, "p3": 7, None: 1000}


@pytest.mark.parametrize("dimension,column,want", [("DIM.CAT", "CAT", DESKTOP_BY_CAT),
                                                    ("DIM.PK", "PK", DESKTOP_BY_KEY)])
def test_per_dimension_matches_desktop(dimension, column, want):
    got = dax.evaluate_per_dimension(["Revenue"], TABLES, dict(MEASURES), None, dimension, "DIM", column,
                                     list(want), None, None, RELS)["Revenue"]
    assert got == want


def test_the_issue_repro():
    tables = {"DIM": {"columns": ["PK", "CAT"], "rows": [["P1", "A"], ["P2", "B"]]},
              "FACT": {"columns": ["PK", "REV"], "rows": [["P1", 100], ["P2", 250], ["P1", 50]]}}
    got = dax.evaluate_per_dimension(["Revenue"], tables, {"Revenue": "SUM('FACT'[REV])"}, None, "DIM.CAT",
                                     "DIM", "CAT", ["A", "B"], None, None, RELS)
    assert got == {"Revenue": {"A": 150, "B": 250}}


def test_each_member_equals_a_filtered_evaluation():
    for member in ("A", "B"):
        alone = dax.evaluate_measures_batch(["Revenue"], TABLES, dict(MEASURES), filter_context={"DIM.CAT": [member]},
                                            relationships=RELS)["Revenue"]
        assert alone == DESKTOP_BY_CAT[member]


def test_the_server_tool_on_a_built_model(tmp_path):
    from pbix_mcp import server as S
    from pbix_mcp.builder import PBIXBuilder

    b = PBIXBuilder("issue181")
    b.add_table("DIM", [{"name": "PK", "data_type": "String"}, {"name": "CAT", "data_type": "String"}],
                rows=[{"PK": k, "CAT": c} for k, c in DIM])
    b.add_table("FACT", [{"name": "PK", "data_type": "String"}, {"name": "REV", "data_type": "Int64"}],
                rows=[{"PK": k, "REV": v} for k, v in FACT])
    b.add_relationship("FACT", "PK", "DIM", "PK")
    b.add_measure("FACT", "Revenue", "SUM(FACT[REV])")
    b.add_page("Page 1")
    path = str(tmp_path / "issue181.pbix")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        b.save(path)
    assert json.loads(S.pbix_open(path, "issue181"))["success"]
    try:
        msg = json.loads(S.pbix_evaluate_dax_per_dimension("issue181", "Revenue", "DIM.CAT"))["message"]
    finally:
        S.pbix_close("issue181")
    rows = {line.split()[0]: line.split()[-1] for line in msg.splitlines()[4:] if line.strip()}
    assert rows == {"A": "157", "B": "250", "(Blank)": "1,000"}
