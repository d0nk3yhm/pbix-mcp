"""Issue #143: PBIXBuilder writes a projection's `active` flag as Desktop does.

`add_page` wrote "active": true on every entry of every role, so a chart built
with two Category fields opened EXPANDED in Power BI Desktop (every level
active is the expand-all state), where Desktop's own default for a two-field
axis is its top level. Desktop's files (the community dashboards and templates
in test_samples) mark a drillable role's FIRST entry active, every entry only
when expanded, and never a measure well:

- Category of every chart and map, a matrix's Rows / Columns, a slicer's
  Values, a scatter's X, a decomposition tree's ExplainBy: first entry active;
- Y, the Values of cards and tables, Tooltips, Size, Series, a chart's
  small-multiples Rows: no key.

A visual config's "expanded": True asks for every level active.
"""
from __future__ import annotations

import json
import os
import zipfile

import pytest

from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit

C = {"table": "Cal", "column": "Year"}
Q = {"table": "Cal", "column": "Quarter"}
S = {"measure": "S"}


def _projections(tmp_path, visuals):
    b = PBIXBuilder()
    b.add_table("Cal", [{"name": "Year", "data_type": "Int64"}, {"name": "Quarter", "data_type": "String"},
                        {"name": "V", "data_type": "Int64"}],
                rows=[{"Year": 2016, "Quarter": "Q4", "V": 6}, {"Year": 2017, "Quarter": "Q1", "V": 5}])
    b.add_measure("Cal", "S", "SUM(Cal[V])")
    for i, v in enumerate(visuals):
        v.setdefault("name", f"v{i}")
        v.setdefault("x", 10 + 110 * i)
        v.setdefault("y", 10)
        v.setdefault("width", 100)
        v.setdefault("height", 100)
    b.add_page("P", visuals=visuals)
    path = os.path.join(str(tmp_path), "p.pbix")
    b.save(path)
    lay = json.loads(zipfile.ZipFile(path).read("Report/Layout").decode("utf-16-le"))
    out = []
    for vc in lay["sections"][0]["visualContainers"]:
        proj = json.loads(vc["config"])["singleVisual"]["projections"]
        out.append({role: "".join("A" if e.get("active") is True else "-" for e in entries)
                    for role, entries in proj.items()})
    return out


def test_a_two_field_axis_opens_at_its_top_level(tmp_path):
    got = _projections(tmp_path, [{"type": "clusteredColumnChart",
                                   "config": {"roles": {"Category": [C, Q], "Y": [S]}}}])
    assert got == [{"Category": "A-", "Y": "-"}]


def test_expanded_marks_every_level(tmp_path):
    got = _projections(tmp_path, [{"type": "clusteredColumnChart",
                                   "config": {"expanded": True, "roles": {"Category": [C, Q], "Y": [S]}}}])
    assert got == [{"Category": "AA", "Y": "-"}]


@pytest.mark.parametrize("visual,roles,want", [
    ("pivotTable", {"Rows": [C, Q], "Columns": [Q], "Values": [S]}, {"Rows": "A-", "Columns": "A", "Values": "-"}),
    ("slicer", {"Values": [C, Q]}, {"Values": "A-"}),
    ("scatterChart", {"Category": [C], "X": [S], "Y": [S], "Size": [S]},
     {"Category": "A", "X": "A", "Y": "-", "Size": "-"}),
    ("barChart", {"Category": [C], "Y": [S], "Tooltips": [S], "Series": [Q], "Rows": [Q]},
     {"Category": "A", "Y": "-", "Tooltips": "-", "Series": "-", "Rows": "-"}),
    ("tableEx", {"Values": [C, Q, S]}, {"Values": "---"}),
    ("card", {"Values": [S]}, {"Values": "-"}),
])
def test_roles_follow_desktop(tmp_path, visual, roles, want):
    assert _projections(tmp_path, [{"type": visual, "config": {"roles": roles}}]) == [want]


def test_the_fixed_shapes(tmp_path):
    got = _projections(tmp_path, [
        {"type": "card", "config": {"measure": "S"}},
        {"type": "slicer", "config": {"column": C}},
        {"type": "tableEx", "config": {"columns": [C, S]}},
        {"type": "clusteredColumnChart", "config": {"category": C, "measure": "S"}},
    ])
    assert got == [{"Values": "-"}, {"Values": "A"}, {"Values": "--"}, {"Category": "A", "Y": "-"}]
