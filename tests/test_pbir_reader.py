"""Regression tests for issues-16 — reading PBIR reports.

Every report authored in the Power BI SERVICE downloads as PBIR: there is no
`Report/Layout` at all, only a `Report/definition/` tree. `_get_layout` returned
None for those files, so consumers saw "no layout"; and the PBIR converter that
did exist dropped nearly every semantic field, most fatally the visual NAME (so
no visual could be addressed) and the field bindings (so no visual could query).

These tests build a PBIR file from scratch — no external sample needed — and
pin: the fallback, the full field mapping, and the refusal to write a
synthesized layout back over a PBIR file.
"""
import json
import os
import uuid
import zipfile

import pytest

from pbix_mcp import server


def _write_pbir(root: str):
    """Create a minimal but representative PBIR tree: two pages (one a tooltip),
    a visual bound to a column, a hidden visual bound to a measure, a sync
    group, visual + page filters, and a mobile layout."""
    d = os.path.join(root, "Report", "definition")
    pages = os.path.join(d, "pages")
    os.makedirs(pages, exist_ok=True)
    with open(os.path.join(d, "version.json"), "w") as f:
        json.dump({"version": "2.0.0"}, f)
    with open(os.path.join(d, "report.json"), "w") as f:
        json.dump({"$schema": "report/2.0.0"}, f)
    with open(os.path.join(pages, "pages.json"), "w") as f:
        json.dump({"pageOrder": ["pageA", "pageB"],
                   "activePageName": "pageB"}, f)

    # --- page A: normal page, one visible slicer on a COLUMN ---
    pa = os.path.join(pages, "pageA")
    os.makedirs(os.path.join(pa, "visuals", "visA"), exist_ok=True)
    with open(os.path.join(pa, "page.json"), "w") as f:
        json.dump({"name": "pageA", "displayName": "Page 1",
                   "displayOption": "FitToPage", "width": 1280, "height": 720,
                   "filterConfig": {"filters": [{"name": "pf1"}]}}, f)
    with open(os.path.join(pa, "visuals", "visA", "visual.json"), "w") as f:
        json.dump({
            "name": "visA",
            "position": {"x": 10, "y": 20, "z": 3, "height": 280,
                         "width": 300, "tabOrder": 7},
            "visual": {
                "visualType": "slicer",
                "query": {"queryState": {"Values": {"projections": [{
                    "field": {"Column": {
                        "Expression": {"SourceRef": {"Entity": "Sales"}},
                        "Property": "Category"}},
                    "queryRef": "Sales.Category",
                    "nativeQueryRef": "Category",
                    "active": True}]}}},
                "objects": {"data": [{"properties": {}}]},
                "syncGroup": {"groupName": "Category", "fieldChanges": True,
                              "filterChanges": True},
                "drillFilterOtherVisuals": True,
            },
            "filterConfig": {"filters": [{"name": "vf1", "type": "Categorical"}]},
        }, f)
    with open(os.path.join(pa, "visuals", "visA", "mobile.json"), "w") as f:
        json.dump({"position": {"x": 0, "y": 0, "z": 0, "height": 324,
                                "width": 324, "tabOrder": 0}}, f)

    # --- page B: TOOLTIP page, one HIDDEN visual bound to a MEASURE ---
    pb = os.path.join(pages, "pageB")
    os.makedirs(os.path.join(pb, "visuals", "visB"), exist_ok=True)
    with open(os.path.join(pb, "page.json"), "w") as f:
        json.dump({"name": "pageB", "displayName": "Page 2",
                   "displayOption": "FitToPage", "width": 1280, "height": 720,
                   "type": "Tooltip"}, f)
    with open(os.path.join(pb, "visuals", "visB", "visual.json"), "w") as f:
        json.dump({
            "name": "visB",
            "position": {"x": 5, "y": 6, "z": 1, "height": 100,
                         "width": 200, "tabOrder": 2},
            "isHidden": True,
            "visual": {
                "visualType": "card",
                "query": {"queryState": {"Values": {"projections": [{
                    "field": {"Measure": {
                        "Expression": {"SourceRef": {"Entity": "Sales"}},
                        "Property": "Total"}},
                    "queryRef": "Sales.Total",
                    "active": True}]}}},
                "syncGroup": {"groupName": "G", "fieldChanges": False,
                              "filterChanges": True},
            },
        }, f)


@pytest.fixture(scope="module")
def pbir_dir(tmp_path_factory):
    root = str(tmp_path_factory.mktemp("pbir"))
    _write_pbir(root)
    return root


@pytest.fixture(scope="module")
def layout(pbir_dir):
    return server._get_layout(pbir_dir)


@pytest.fixture
def pbir_pbix(tmp_path_factory):
    """Zip the PBIR tree into a .pbix so the tools can open it."""
    root = str(tmp_path_factory.mktemp("pbirzip"))
    _write_pbir(root)
    path = os.path.join(root, "report.pbix")
    with zipfile.ZipFile(path, "w") as z:
        for base, _dirs, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(base, fn)
                z.write(full, os.path.relpath(full, root))
        z.writestr("Version", "1.28")
    return path


def _visual(layout, page_idx, vis_idx=0):
    vc = layout["sections"][page_idx]["visualContainers"][vis_idx]
    return vc, json.loads(vc["config"])


class TestFallback:
    def test_is_pbir_detected(self, pbir_dir):
        assert server._is_pbir(pbir_dir) is True

    def test_get_layout_falls_back(self, layout):
        """_get_layout returned None for PBIR — consumers saw 'no layout'."""
        assert layout is not None
        assert len(layout["sections"]) == 2

    def test_marked_as_pbir(self, layout):
        assert layout["__pbir__"] is True

    def test_classic_file_unaffected(self, tmp_path):
        from pbix_mcp.builder import PBIXBuilder
        p = str(tmp_path / "classic.pbix")
        b = PBIXBuilder("C")
        b.add_table("T", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_page("Page 1")
        b.save(p)
        wd = str(tmp_path / "wd")
        os.makedirs(wd)
        with zipfile.ZipFile(p) as z:
            z.extractall(wd)
        assert server._is_pbir(wd) is False
        lay = server._get_layout(wd)
        assert lay and not lay.get("__pbir__")


class TestPageMapping:
    def test_page_order_honoured(self, layout):
        assert [s["name"] for s in layout["sections"]] == ["pageA", "pageB"]

    def test_display_names_not_guids(self, layout):
        assert [s["displayName"] for s in layout["sections"]] == \
            ["Page 1", "Page 2"]

    def test_canvas_size(self, layout):
        for s in layout["sections"]:
            assert s["width"] == 1280 and s["height"] == 720

    def test_display_option_normalized_to_classic_int(self, layout):
        """PBIR stores the enum NAME, classic Report/Layout stores an int.

        `_get_layout` promises a legacy-shaped document, so callers must see
        one type regardless of which format the report is stored in —
        otherwise every consumer needs a format check for this one field.
        """
        for s in layout["sections"]:
            assert s["displayOption"] == 1  # 1 == "FitToPage"

    def test_tooltip_page_identifiable(self, layout):
        assert "type" not in layout["sections"][0]
        assert layout["sections"][1]["type"] == "Tooltip"

    def test_active_page_marked(self, layout):
        assert layout["sections"][1].get("isActive") is True

    def test_page_filters(self, layout):
        assert json.loads(layout["sections"][0]["filters"]) == [{"name": "pf1"}]


class TestVisualMapping:
    def test_visual_is_addressable_by_name(self, layout):
        """config.name was never set — no visual could be looked up."""
        _vc, cfg = _visual(layout, 0)
        assert cfg["name"] == "visA"
        _vc2, cfg2 = _visual(layout, 1)
        assert cfg2["name"] == "visB"

    def test_full_geometry(self, layout):
        vc, _cfg = _visual(layout, 0)
        assert (vc["x"], vc["y"], vc["z"]) == (10, 20, 3)
        assert (vc["width"], vc["height"]) == (300, 280)
        assert vc["tabOrder"] == 7

    def test_visual_type_and_syncgroup_preserved(self, layout):
        _vc, cfg = _visual(layout, 0)
        sv = cfg["singleVisual"]
        assert sv["visualType"] == "slicer"
        assert sv["syncGroup"]["groupName"] == "Category"

    def test_projections_present(self, layout):
        """singleVisual had no projections at all, so visuals rendered empty."""
        _vc, cfg = _visual(layout, 0)
        proj = cfg["singleVisual"]["projections"]
        assert list(proj) == ["Values"]
        assert proj["Values"][0]["queryRef"] == "Sales.Category"
        assert proj["Values"][0]["active"] is True

    def test_prototype_query_discriminates_column(self, layout):
        _vc, cfg = _visual(layout, 0)
        pq = cfg["singleVisual"]["prototypeQuery"]
        assert pq["From"] == [{"Name": "s", "Entity": "Sales", "Type": 0}]
        sel = pq["Select"][0]
        assert "Column" in sel and sel["Name"] == "Sales.Category"
        # the entity ref is rewritten to the From alias, classic-style
        assert sel["Column"]["Expression"]["SourceRef"] == {"Source": "s"}
        assert sel["Column"]["Property"] == "Category"

    def test_prototype_query_discriminates_measure(self, layout):
        _vc, cfg = _visual(layout, 1)
        sel = cfg["singleVisual"]["prototypeQuery"]["Select"][0]
        assert "Measure" in sel and sel["Name"] == "Sales.Total"

    def test_hidden_visual_marked(self, layout):
        vc0, _ = _visual(layout, 0)
        vc1, _ = _visual(layout, 1)
        assert "isHidden" not in vc0
        assert vc1["isHidden"] is True

    def test_visual_filters(self, layout):
        vc, _cfg = _visual(layout, 0)
        assert json.loads(vc["filters"])[0]["name"] == "vf1"

    def test_mobile_layout_read(self, layout):
        vc, _cfg = _visual(layout, 0)
        assert vc["mobile"]["position"]["width"] == 324

    def test_raw_query_not_leaked(self, layout):
        """The raw PBIR `query` block is replaced by projections/prototypeQuery."""
        _vc, cfg = _visual(layout, 0)
        assert "query" not in cfg["singleVisual"]


class TestWriteBack:
    """PBIR reports are edited IN PLACE in their Report/definition tree.

    The guarantee that makes this safe: each page/visual is patched onto the
    ORIGINAL file it was read from, so a write that changes nothing changes
    nothing on disk — including fields the reader doesn't model.
    """

    def test_noop_write_is_byte_faithful(self, tmp_path):
        root = str(tmp_path / "noop")
        _write_pbir(root)
        before = {}
        for base, _d, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(base, fn)
                with open(full, encoding="utf-8") as f:
                    before[os.path.relpath(full, root)] = json.load(f)

        lay = server._get_layout(root)
        server._set_layout(root, lay)          # write back UNCHANGED

        after = {}
        for base, _d, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(base, fn)
                with open(full, encoding="utf-8") as f:
                    after[os.path.relpath(full, root)] = json.load(f)
        assert set(before) == set(after)
        for rel in before:
            assert before[rel] == after[rel], rel

    def test_unmodelled_fields_survive_an_edit(self, tmp_path):
        """A field the converter never maps must not be lost when the caller
        edits something else on the same visual."""
        root = str(tmp_path / "keep")
        _write_pbir(root)
        vfile = os.path.join(root, "Report", "definition", "pages", "pageA",
                             "visuals", "visA", "visual.json")
        with open(vfile) as f:
            raw = json.load(f)
        raw["howCreated"] = "InsertVisualButton"          # not modelled
        raw["visual"]["query"]["sortDefinition"] = {"isDefaultSort": True}
        with open(vfile, "w") as f:
            json.dump(raw, f)

        lay = server._get_layout(root)
        lay["sections"][0]["visualContainers"][0]["x"] = 999   # unrelated edit
        server._set_layout(root, lay)

        with open(vfile) as f:
            out = json.load(f)
        assert out["position"]["x"] == 999
        assert out["howCreated"] == "InsertVisualButton"
        assert out["visual"]["query"]["sortDefinition"] == {"isDefaultSort": True}
        # and the bindings weren't rewritten either
        assert out["visual"]["query"]["queryState"]["Values"]["projections"][0][
            "field"]["Column"]["Expression"]["SourceRef"] == {"Entity": "Sales"}

    def test_edits_persist(self, tmp_path):
        root = str(tmp_path / "edit")
        _write_pbir(root)
        lay = server._get_layout(root)
        lay["sections"][0]["displayName"] = "Renamed"
        lay["sections"][0]["visualContainers"][0]["width"] = 640
        server._set_layout(root, lay)

        again = server._get_layout(root)
        assert again["sections"][0]["displayName"] == "Renamed"
        assert again["sections"][0]["visualContainers"][0]["width"] == 640

    def test_removing_a_visual_deletes_its_folder(self, tmp_path):
        root = str(tmp_path / "del")
        _write_pbir(root)
        vdir = os.path.join(root, "Report", "definition", "pages", "pageA",
                            "visuals", "visA")
        assert os.path.isdir(vdir)
        lay = server._get_layout(root)
        lay["sections"][0]["visualContainers"] = []
        server._set_layout(root, lay)
        assert not os.path.isdir(vdir)

    def test_removing_a_page_deletes_it_and_updates_order(self, tmp_path):
        root = str(tmp_path / "delpage")
        _write_pbir(root)
        lay = server._get_layout(root)
        lay["sections"] = [lay["sections"][0]]
        server._set_layout(root, lay)
        pages = os.path.join(root, "Report", "definition", "pages")
        assert not os.path.isdir(os.path.join(pages, "pageB"))
        with open(os.path.join(pages, "pages.json")) as f:
            meta = json.load(f)
        assert meta["pageOrder"] == ["pageA"]
        assert meta["activePageName"] == "pageA"   # was pageB, which is gone

    def test_never_plants_a_classic_layout(self, tmp_path):
        root = str(tmp_path / "noplant")
        _write_pbir(root)
        lay = server._get_layout(root)
        server._set_layout(root, lay)
        assert not os.path.exists(os.path.join(root, "Report", "Layout"))

    def test_synthesized_layout_refused_without_a_tree(self, tmp_path):
        """A PBIR-derived layout must not be written into a dir that has no
        Report/definition tree to patch."""
        from pbix_mcp.errors import UnsupportedFormatError
        wd = str(tmp_path / "plain")
        os.makedirs(os.path.join(wd, "Report"))
        with pytest.raises(UnsupportedFormatError):
            server._set_layout(wd, {"sections": [], "__pbir__": True})


class TestToolSurface:
    pass

    def test_report_format_reports_pbir_and_writable(self, pbir_pbix):
        alias = "pf_" + uuid.uuid4().hex[:8]
        server.pbix_open(pbir_pbix, alias)
        try:
            out = json.loads(server.pbix_report_format(alias))
            assert out["success"], out
            d = out["data"]
            assert d["format"] == "PBIR" and d["is_pbir"] is True
            assert d["readable"] is True
            assert d["writable"] is True
            assert d["pages"] == 2 and d["visuals"] == 2
        finally:
            server.pbix_close(alias, force=True)

    def test_layout_tools_can_edit_a_pbir_report(self, pbir_pbix, tmp_path):
        alias = "pw_" + uuid.uuid4().hex[:8]
        out_path = str(tmp_path / "edited.pbix")
        server.pbix_open(pbir_pbix, alias)
        try:
            added = json.loads(server.pbix_add_page(alias, "Third Page"))
            assert added["success"], added
            removed = json.loads(server.pbix_remove_visual(alias, 1, 0))
            assert removed["success"], removed
            server.pbix_save(alias, out_path, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        alias2 = "pr_" + uuid.uuid4().hex[:8]
        server.pbix_open(out_path, alias2)
        try:
            lay = server._get_layout(server._open_files[alias2]["work_dir"])
            assert [s["displayName"] for s in lay["sections"]] == \
                ["Page 1", "Page 2", "Third Page"]
            assert lay["sections"][1]["visualContainers"] == []
        finally:
            server.pbix_close(alias2, force=True)

        with zipfile.ZipFile(out_path) as z:
            assert "Report/Layout" not in z.namelist()


class TestBookmarks:
    """PBIR keeps bookmarks in `definition/bookmarks/`, but `_set_layout_pbir`
    only ever wrote the pages tree. `pbix_add_bookmark` therefore reported
    success and silently discarded the bookmark on every service-authored
    report — the worst failure shape, because nothing surfaced the loss.
    """

    def _open(self, pbir_pbix):
        alias = "bm_" + uuid.uuid4().hex[:8]
        server.pbix_open(pbir_pbix, alias)
        return alias

    def test_add_bookmark_persists_to_disk(self, pbir_pbix, tmp_path):
        out = str(tmp_path / "bm.pbix")
        alias = self._open(pbir_pbix)
        try:
            r = json.loads(server.pbix_add_bookmark(alias, "My Bookmark"))
            assert r["success"], r
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        with zipfile.ZipFile(out) as z:
            names = z.namelist()
            bfiles = [n for n in names
                      if n.startswith("Report/definition/bookmarks/")]
            assert any(n.endswith(".bookmark.json") for n in bfiles), bfiles
            assert "Report/definition/bookmarks/bookmarks.json" in bfiles
            # No classic Layout may be planted alongside the PBIR tree.
            assert "Report/Layout" not in names

            doc = json.loads(
                z.read(next(n for n in bfiles
                            if n.endswith(".bookmark.json"))).decode("utf-8-sig"))
            assert doc["displayName"] == "My Bookmark"
            # Required by the published bookmark schema.
            assert "sections" in doc["explorationState"]
            assert "activeSection" in doc["explorationState"]
            assert doc["$schema"].endswith("/bookmark/1.0.0/schema.json")
            # `byColumn` is not part of the PBIR FiltersState and appears in no
            # Desktop-authored bookmark.
            assert "byColumn" not in doc["explorationState"].get("filters", {})

            meta = json.loads(
                z.read("Report/definition/bookmarks/bookmarks.json")
                .decode("utf-8-sig"))
            assert [i["name"] for i in meta["items"]] == [doc["name"]]

    def test_bookmark_round_trips_through_reader(self, pbir_pbix, tmp_path):
        out = str(tmp_path / "bm2.pbix")
        alias = self._open(pbir_pbix)
        try:
            server.pbix_add_bookmark(alias, "Round Trip")
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        alias2 = self._open(out)
        try:
            r = json.loads(server.pbix_get_bookmarks(alias2))
            assert r["success"], r
            assert "Round Trip" in r["message"]
        finally:
            server.pbix_close(alias2, force=True)

    def test_page_only_edit_does_not_delete_bookmarks(self, pbir_pbix, tmp_path):
        """A caller that never touches `config` must not lose bookmarks."""
        step1 = str(tmp_path / "s1.pbix")
        alias = self._open(pbir_pbix)
        try:
            server.pbix_add_bookmark(alias, "Keep Me")
            server.pbix_save(alias, step1, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        step2 = str(tmp_path / "s2.pbix")
        alias2 = self._open(step1)
        try:
            server.pbix_add_page(alias2, "Unrelated Page")
            server.pbix_save(alias2, step2, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias2, force=True)

        alias3 = self._open(step2)
        try:
            r = json.loads(server.pbix_get_bookmarks(alias3))
            assert "Keep Me" in r["message"], r
        finally:
            server.pbix_close(alias3, force=True)

    def test_remove_bookmark_deletes_the_file(self, pbir_pbix, tmp_path):
        step1 = str(tmp_path / "r1.pbix")
        alias = self._open(pbir_pbix)
        try:
            server.pbix_add_bookmark(alias, "Doomed")
            server.pbix_save(alias, step1, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        step2 = str(tmp_path / "r2.pbix")
        alias2 = self._open(step1)
        try:
            r = json.loads(server.pbix_remove_bookmark(alias2, 0))
            assert r["success"], r
            server.pbix_save(alias2, step2, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias2, force=True)

        with zipfile.ZipFile(step2) as z:
            assert not [n for n in z.namelist()
                        if n.endswith(".bookmark.json")]
