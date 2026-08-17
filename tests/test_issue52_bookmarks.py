"""Issue #52: bookmark state capture and the bookmark's internal name.

1. `pbix_add_bookmark` recorded `singleVisual: {}` for every visual it
   targeted, so applying a bookmark restored NOTHING — a slicer selection
   live at capture time was simply lost, and no bookmark could clear one
   either (OpenBI's QlikView ClearAll maps to exactly that).
2. Neither `pbix_add_bookmark` nor `pbix_get_bookmarks` returned the
   INTERNAL name an action button's bookmark action must reference, so
   callers had to read Report/Layout raw and match on a display name that
   is not even unique.

Measured the way the issue measured: save -> close -> reopen -> read the
layout back out of the saved file.
"""
import json

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit


def _build_minimal_pbix(path):
    b = PBIXBuilder("T")
    b.add_table("Sales", [
        {"name": "Region", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
    ], rows=[{"Region": "West", "Amount": 9.99},
             {"Region": "East", "Amount": 4.0}])
    b.save(path)


#: A slicer selection, in the shape Power BI actually keeps one: the general
#: card's `filter` property holds a filter object whose `filter` is the query
#: (Version / From / Where) — the same nesting pbix_get_filters reads.
_SEL_PROPS = {
    "filter": {
        "filter": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
            "Where": [{"Condition": {"In": {
                "Expressions": [{"Column": {
                    "Expression": {"SourceRef": {"Source": "s"}},
                    "Property": "Region"}}],
                "Values": [[{"Literal": {"Value": "'West'"}}]]}}}],
        }
    }
}


def _selected_report(tmp_path, alias):
    """Two visuals, the slicer carrying a live 'West' selection."""
    p = str(tmp_path / "bm52.pbix")
    _build_minimal_pbix(p)
    assert json.loads(server.pbix_open(p, alias))["success"]
    server.pbix_add_visual(alias, 0, "slicer", x=10, y=10,
                           width=100, height=80)
    server.pbix_add_visual(alias, 0, "columnChart", x=120, y=10,
                           width=200, height=120)
    work = server._open_files[alias]["work_dir"]
    page = server._get_layout(work)["sections"][0]
    cfg = json.loads(page["visualContainers"][0]["config"])
    cfg.setdefault("singleVisual", {}).setdefault("objects", {})["general"] = [
        {"properties": json.loads(json.dumps(_SEL_PROPS))}]
    assert json.loads(server.pbix_update_visual_json(
        alias, 0, 0, json.dumps(cfg)))["success"]
    names = [json.loads(vc["config"])["name"]
             for vc in server._get_layout(work)["sections"][0][
                 "visualContainers"]]
    return names


def _reopened_bookmarks(alias, tmp_path):
    out = str(tmp_path / "saved52.pbix")
    assert json.loads(server.pbix_save(
        alias, output_path=out, overwrite=True))["success"]
    server._open_files.pop(alias, None)
    alias2 = alias + "_r"
    assert json.loads(server.pbix_open(out, alias2))["success"]
    try:
        layout = server._get_layout(server._open_files[alias2]["work_dir"])
        return json.loads(layout["config"])["bookmarks"]
    finally:
        server._open_files.pop(alias2, None)
        server._dax_cache.pop(alias2, None)


def _containers(bm):
    sections = bm["explorationState"]["sections"]
    return sections[next(iter(sections))]["visualContainers"]


class TestBookmarkCapturesVisualState:
    def test_live_selection_is_captured_and_survives_reopen(self, tmp_path):
        alias = "bm52a"
        try:
            names = _selected_report(tmp_path, alias)
            server.pbix_add_bookmark(alias, "West only")
            bm = _reopened_bookmarks(alias, tmp_path)[-1]
            gen = _containers(bm)[names[0]]["singleVisual"][
                "objects"]["general"][0]
            where = gen["properties"]["filter"]["filter"]["Where"]
            assert where[0]["Condition"]["In"]["Values"][0][0][
                "Literal"]["Value"] == "'West'"
        finally:
            server._open_files.pop(alias, None)

    def test_clear_selections_records_an_empty_selection(self, tmp_path):
        """A Clear-all bookmark records the filter WITHOUT its Where — an
        explicit "nothing selected" state. An ABSENT filter would mean "do
        not override" and would clear nothing."""
        alias = "bm52b"
        try:
            names = _selected_report(tmp_path, alias)
            server.pbix_add_bookmark(alias, "Clear all",
                                     clear_selections=True)
            bm = _reopened_bookmarks(alias, tmp_path)[-1]
            inner = _containers(bm)[names[0]]["singleVisual"]["objects"][
                "general"][0]["properties"]["filter"]["filter"]
            assert "Where" not in inner
            assert inner["From"]  # the target column is still identified
            assert "'West'" not in json.dumps(bm)
        finally:
            server._open_files.pop(alias, None)

    def test_capture_can_be_switched_off(self, tmp_path):
        alias = "bm52c"
        try:
            names = _selected_report(tmp_path, alias)
            server.pbix_add_bookmark(alias, "Display only",
                                     capture_visual_state=False)
            bm = _reopened_bookmarks(alias, tmp_path)[-1]
            assert _containers(bm)[names[0]]["singleVisual"] == {}
        finally:
            server._open_files.pop(alias, None)

    def test_hidden_visual_keeps_display_mode_alongside_state(self, tmp_path):
        """The visibility half already worked; capturing data state must not
        cost it, and still no "visible" anywhere (OpenBI #2)."""
        alias = "bm52d"
        try:
            names = _selected_report(tmp_path, alias)
            server.pbix_add_bookmark(alias, "Hide chart",
                                     hidden_visuals=names[1])
            c = _containers(_reopened_bookmarks(alias, tmp_path)[-1])
            assert c[names[1]]["singleVisual"]["display"]["mode"] == "hidden"
            assert c[names[0]]["singleVisual"]["objects"]["general"]
            assert "display" not in c[names[0]]["singleVisual"]
            assert '"visible"' not in json.dumps(c)
        finally:
            server._open_files.pop(alias, None)

    def test_visual_level_filters_are_captured(self, tmp_path):
        """A visual's own filters live on the CONTAINER, beside config, as a
        plain array — but a BOOKMARK stores a FiltersState object keyed by
        how the filter is identified (PBIR bookmark schema 1.0.0), so the
        array is rewritten to `byName`, not copied through."""
        alias = "bm52g"
        try:
            names = _selected_report(tmp_path, alias)
            work = server._open_files[alias]["work_dir"]
            layout = server._get_layout(work)
            layout["sections"][0]["visualContainers"][1]["filters"] = \
                json.dumps([{"name": "f1", "type": "Categorical",
                             "ordinal": 3, "displayName": "drop me"}])
            server._set_layout(work, layout)
            server.pbix_add_bookmark(alias, "With visual filter")
            c = _containers(_reopened_bookmarks(alias, tmp_path)[-1])
            f = c[names[1]]["filters"]["byName"]["f1"]
            assert f["type"] == "Categorical"
            # FilterContainerState sets additionalProperties: false, so
            # container-only keys must NOT ride along
            assert "ordinal" not in f and "displayName" not in f
        finally:
            server._open_files.pop(alias, None)

    def test_nameless_container_filter_is_skipped(self, tmp_path):
        """`name` is required on a FilterContainerState — a filter without
        one cannot be represented and must be dropped, not emitted."""
        alias = "bm52h"
        try:
            names = _selected_report(tmp_path, alias)
            work = server._open_files[alias]["work_dir"]
            layout = server._get_layout(work)
            layout["sections"][0]["visualContainers"][1]["filters"] = \
                json.dumps([{"type": "Categorical"}])
            server._set_layout(work, layout)
            server.pbix_add_bookmark(alias, "Nameless")
            c = _containers(_reopened_bookmarks(alias, tmp_path)[-1])
            assert "filters" not in c[names[1]]
        finally:
            server._open_files.pop(alias, None)


class TestBookmarkInternalNameIsReturned:
    def _card_report(self, tmp_path, alias):
        p = str(tmp_path / "bm52n.pbix")
        _build_minimal_pbix(p)
        assert json.loads(server.pbix_open(p, alias))["success"]
        server.pbix_add_visual(alias, 0, "card", x=10, y=10,
                               width=100, height=80)

    def test_add_returns_the_internal_name(self, tmp_path):
        alias = "bm52e"
        try:
            self._card_report(tmp_path, alias)
            r = json.loads(server.pbix_add_bookmark(alias, "West only"))
            assert r["success"], r
            name = r["data"]["name"]
            assert name.startswith("Bookmark") and name != "Bookmark"
            layout = server._get_layout(server._open_files[alias]["work_dir"])
            assert json.loads(layout["config"])["bookmarks"][-1][
                "name"] == name
            assert r["data"]["displayName"] == "West only"
            assert r["data"]["index"] == 0
            assert name in r["message"]
        finally:
            server._open_files.pop(alias, None)

    def test_get_lists_internal_name_beside_display_name(self, tmp_path):
        alias = "bm52f"
        try:
            self._card_report(tmp_path, alias)
            a = json.loads(server.pbix_add_bookmark(alias, "Same label"))
            b = json.loads(server.pbix_add_bookmark(alias, "Same label"))
            assert a["data"]["name"] != b["data"]["name"]
            g = json.loads(server.pbix_get_bookmarks(alias))
            rows = g["data"]["bookmarks"]
            assert [r["name"] for r in rows] == [a["data"]["name"],
                                                 b["data"]["name"]]
            # two bookmarks may legitimately share a display name — the
            # internal name is what tells them apart
            assert {r["displayName"] for r in rows} == {"Same label"}
            assert a["data"]["name"] in g["message"]
        finally:
            server._open_files.pop(alias, None)
