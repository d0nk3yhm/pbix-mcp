"""Report-editing primitives: rename / reorder / hide / duplicate / move.

These are table-stakes operations for any report editor, and every one of them
was previously only reachable by hand-editing raw layout JSON — `pbix_add_page`
and `pbix_remove_page` existed with nothing in between, and visual geometry was
readable (`pbix_get_visual_positions`) with no way to write it back.

Each test runs against BOTH storage formats: classic `Report/Layout` and the
PBIR tree the service produces, because the tools go through
`_get_layout`/`_set_layout` and must behave identically on either.
"""
import json
import os
import uuid
import zipfile

import pytest

from pbix_mcp import server
from tests.test_pbir_reader import _write_pbir


@pytest.fixture
def classic_pbix(tmp_path):
    from pbix_mcp.builder import PBIXBuilder

    path = str(tmp_path / "classic.pbix")
    b = PBIXBuilder("Classic")
    b.add_table("T", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
    b.add_page("Page 1")
    b.add_page("Page 2")
    b.add_page("Page 3")
    b.save(path)
    return path


@pytest.fixture
def pbir_pbix(tmp_path):
    root = str(tmp_path / "pbirsrc")
    os.makedirs(root, exist_ok=True)
    _write_pbir(root)
    path = str(tmp_path / "pbir.pbix")
    with zipfile.ZipFile(path, "w") as z:
        for cur, _dirs, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(cur, fn)
                z.write(full, os.path.relpath(full, root))
        z.writestr("Version", "1.28")
    return path


class _Session:
    """Open, act, save, reopen — so assertions see what actually persisted."""

    def __init__(self, path, tmp_path):
        self.path = path
        self.tmp_path = tmp_path
        self.alias = "re_" + uuid.uuid4().hex[:8]

    def __enter__(self):
        server.pbix_open(self.path, self.alias)
        return self

    def __exit__(self, *exc):
        server.pbix_close(self.alias, force=True)

    def layout(self):
        return server._get_layout(server._open_files[self.alias]["work_dir"])

    def pages(self):
        return [s.get("displayName") for s in self.layout()["sections"]]

    def save_reopen(self):
        out = str(self.tmp_path / f"out_{uuid.uuid4().hex[:6]}.pbix")
        server.pbix_save(self.alias, out, overwrite=True, backup=False)
        return _Session(out, self.tmp_path)


def _ok(raw):
    d = json.loads(raw)
    assert d["success"], d
    return d


def _by_name(containers, name):
    """Find a container by its config name.

    PBIR reads visuals from a sorted directory listing, so a freshly created
    visual is not necessarily last — position in the list carries no meaning
    (z-order does).
    """
    for vc in containers:
        if json.loads(vc.get("config", "{}")).get("name") == name:
            return vc
    raise AssertionError(f"visual {name!r} not found")


@pytest.fixture(params=["classic", "pbir"])
def report(request, classic_pbix, pbir_pbix, tmp_path):
    return (classic_pbix if request.param == "classic" else pbir_pbix,
            tmp_path, request.param)


class TestRenamePage:
    def test_rename_by_index_persists(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            before = s.pages()
            _ok(server.pbix_rename_page(s.alias, "0", "Renamed First"))
            after = s.save_reopen()
        with after as s2:
            assert s2.pages()[0] == "Renamed First"
            assert s2.pages()[1:] == before[1:]

    def test_rename_by_display_name(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            original = s.pages()[1]
            _ok(server.pbix_rename_page(s.alias, original, "By Name"))
            assert s.pages()[1] == "By Name"

    def test_internal_name_is_not_changed(self, report):
        """Bookmarks, drillthrough and page navigation reference the internal
        `name`; renaming the label must not break them."""
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            before = [sec["name"] for sec in s.layout()["sections"]]
            _ok(server.pbix_rename_page(s.alias, "0", "New Label"))
            assert [sec["name"] for sec in s.layout()["sections"]] == before

    def test_unknown_page_is_an_error(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            d = json.loads(server.pbix_rename_page(s.alias, "Nope", "X"))
            assert not d["success"]

    def test_empty_name_rejected(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            d = json.loads(server.pbix_rename_page(s.alias, "0", "  "))
            assert not d["success"]


class TestReorderPages:
    def test_reverse_order_persists(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            before = s.pages()
            _ok(server.pbix_reorder_pages(s.alias, ",".join(reversed(before))))
            after = s.save_reopen()
        with after as s2:
            assert s2.pages() == list(reversed(before))

    def test_partial_order_keeps_the_rest(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            before = s.pages()
            _ok(server.pbix_reorder_pages(s.alias, before[-1]))
            assert s.pages() == [before[-1]] + before[:-1]

    def test_duplicate_reference_rejected(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            d = json.loads(server.pbix_reorder_pages(s.alias, "0,0"))
            assert not d["success"]

    def test_empty_order_rejected(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            assert not json.loads(
                server.pbix_reorder_pages(s.alias, " , "))["success"]


class TestPageVisibility:
    def test_hide_and_show_persist(self, report):
        """Read back in the CLASSIC shape regardless of storage format —
        otherwise every caller needs a format check for this one field."""
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            _ok(server.pbix_set_page_visibility(s.alias, "0", True))
            after = s.save_reopen()
        with after as s2:
            sec = s2.layout()["sections"][0]
            assert json.loads(sec["config"])["visibility"] == 1

            _ok(server.pbix_set_page_visibility(s2.alias, "0", False))
            again = s2.save_reopen()
        with again as s3:
            sec = s3.layout()["sections"][0]
            assert json.loads(sec["config"])["visibility"] == 0

    def test_pbir_stores_the_enum_name_on_disk(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            _ok(server.pbix_set_page_visibility(s.alias, "0", True))
            out = str(tmp_path / "hidden.pbix")
            server.pbix_save(s.alias, out, overwrite=True, backup=False)
        with zipfile.ZipFile(out) as z:
            page = next(n for n in z.namelist()
                        if n.endswith("pageA/page.json"))
            doc = json.loads(z.read(page).decode("utf-8-sig"))
            assert doc["visibility"] == "HiddenInViewMode"


class TestDuplicatePage:
    def test_copy_lands_after_the_source(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            before = s.pages()
            _ok(server.pbix_duplicate_page(s.alias, "0"))
            after = s.save_reopen()
        with after as s2:
            assert s2.pages() == [before[0], before[0] + " (copy)"] + before[1:]

    def test_custom_name(self, report):
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            _ok(server.pbix_duplicate_page(s.alias, "0", "My Copy"))
            assert s.pages()[1] == "My Copy"

    def test_copy_gets_fresh_identities(self, report):
        """Two pages sharing a `name` — or two visuals sharing one — collide in
        bookmarks and page navigation."""
        path, tmp_path, _fmt = report
        with _Session(path, tmp_path) as s:
            _ok(server.pbix_duplicate_page(s.alias, "0", "Copy"))
            after = s.save_reopen()
        with after as s2:
            secs = s2.layout()["sections"]
            names = [sec["name"] for sec in secs]
            assert len(names) == len(set(names)), names

            vnames = []
            for sec in secs:
                for vc in sec.get("visualContainers", []) or []:
                    n = json.loads(vc.get("config", "{}")).get("name")
                    if n:
                        vnames.append(n)
            assert len(vnames) == len(set(vnames)), vnames

    def test_visuals_are_carried_over(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            src_count = len(s.layout()["sections"][0]["visualContainers"])
            assert src_count > 0
            _ok(server.pbix_duplicate_page(s.alias, "0", "Copy"))
            after = s.save_reopen()
        with after as s2:
            assert len(s2.layout()["sections"][1]["visualContainers"]) == \
                src_count


class TestMoveVisual:
    def test_move_and_resize_persist(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            _ok(server.pbix_move_visual(s.alias, "0", 0, x=400, y=120,
                                        width=500, height=320))
            after = s.save_reopen()
        with after as s2:
            vc = s2.layout()["sections"][0]["visualContainers"][0]
            assert (vc["x"], vc["y"], vc["width"], vc["height"]) == \
                (400, 120, 500, 320)

    def test_partial_move_leaves_other_axes_alone(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            before = s.layout()["sections"][0]["visualContainers"][0]
            w, h = before["width"], before["height"]
            _ok(server.pbix_move_visual(s.alias, "0", 0, x=42))
            vc = s.layout()["sections"][0]["visualContainers"][0]
            assert vc["x"] == 42
            assert (vc["width"], vc["height"]) == (w, h)

    def test_no_op_call_is_rejected(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            d = json.loads(server.pbix_move_visual(s.alias, "0", 0))
            assert not d["success"]

    def test_visual_index_out_of_range(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            d = json.loads(server.pbix_move_visual(s.alias, "0", 99, x=1))
            assert not d["success"]

    def test_classic_layouts_block_kept_in_sync(self, classic_pbix, tmp_path):
        """Classic containers repeat geometry inside config.layouts; a stale
        copy is what Desktop actually renders."""
        with _Session(classic_pbix, tmp_path) as s:
            lay = s.layout()
            lay["sections"][0]["visualContainers"] = [{
                "x": 0, "y": 0, "z": 0, "width": 100, "height": 100,
                "config": json.dumps({
                    "name": "v1",
                    "layouts": [{"id": 0, "position": {
                        "x": 0, "y": 0, "z": 0,
                        "width": 100, "height": 100}}],
                }),
            }]
            server._set_layout(server._open_files[s.alias]["work_dir"], lay)

            _ok(server.pbix_move_visual(s.alias, "0", 0, x=250, y=60))
            vc = s.layout()["sections"][0]["visualContainers"][0]
            pos = json.loads(vc["config"])["layouts"][0]["position"]
            assert (vc["x"], vc["y"]) == (250, 60)
            assert (pos["x"], pos["y"]) == (250, 60)


class TestDuplicateVisual:
    def test_same_page_copy_is_offset(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            src = s.layout()["sections"][0]["visualContainers"][0]
            x0, y0 = src["x"], src["y"]
            d = _ok(server.pbix_duplicate_visual(s.alias, "0", 0))
            copy_name = d["message"].rsplit("'", 2)[1]
            after = s.save_reopen()
        with after as s2:
            vcs = s2.layout()["sections"][0]["visualContainers"]
            assert len(vcs) == 2
            vc = _by_name(vcs, copy_name)
            assert (vc["x"], vc["y"]) == (x0 + 20, y0 + 20)

    def test_copy_gets_a_new_name(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            _ok(server.pbix_duplicate_visual(s.alias, "0", 0))
            vcs = s.layout()["sections"][0]["visualContainers"]
            names = [json.loads(v["config"])["name"] for v in vcs]
            assert len(names) == len(set(names)), names

    def test_copy_to_another_page(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            before = len(s.layout()["sections"][1]["visualContainers"])
            _ok(server.pbix_duplicate_visual(s.alias, "0", 0, target_page="1"))
            after = s.save_reopen()
        with after as s2:
            assert len(s2.layout()["sections"][1]["visualContainers"]) == \
                before + 1

    def test_cross_page_copy_keeps_original_position(self, pbir_pbix, tmp_path):
        """Offsetting only makes sense when the copy would overlap its source."""
        with _Session(pbir_pbix, tmp_path) as s:
            src = s.layout()["sections"][0]["visualContainers"][0]
            x0, y0 = src["x"], src["y"]
            d = _ok(server.pbix_duplicate_visual(s.alias, "0", 0,
                                                 target_page="1"))
            copy_name = d["message"].rsplit("'", 2)[1]
            vcs = s.layout()["sections"][1]["visualContainers"]
            vc = _by_name(vcs, copy_name)
            assert (vc["x"], vc["y"]) == (x0, y0)


class TestNoLayoutPlanted:
    def test_pbir_edits_never_write_a_classic_layout(self, pbir_pbix, tmp_path):
        with _Session(pbir_pbix, tmp_path) as s:
            _ok(server.pbix_rename_page(s.alias, "0", "R"))
            _ok(server.pbix_duplicate_page(s.alias, "0", "D"))
            _ok(server.pbix_move_visual(s.alias, "0", 0, x=5))
            _ok(server.pbix_set_page_visibility(s.alias, "0", True))
            out = str(tmp_path / "final.pbix")
            server.pbix_save(s.alias, out, overwrite=True, backup=False)
        with zipfile.ZipFile(out) as z:
            assert "Report/Layout" not in z.namelist()


class TestCategoryLabelColorByVisualType:
    """Issue #45: categoryLabels.color wrote `categoryLabelFontColor` — the
    MULTI-ROW card's property — regardless of visual type, so a plain card
    (which renders `categoryLabels.color`) silently never changed colour.
    The property name is now keyed off the visual type. Verified the way the
    issue measured it: write, save, reopen from disk, read the exact path."""

    @pytest.mark.parametrize("vtype,want,not_want", [
        ("card", "color", "categoryLabelFontColor"),
        ("multiRowCard", "categoryLabelFontColor", "color"),
    ])
    def test_property_name_matches_renderer(self, tmp_path, vtype, want,
                                            not_want):
        import uuid as _uuid
        alias = "cl_" + _uuid.uuid4().hex[:8]
        p = str(tmp_path / f"{vtype}.pbix")
        tables = [{"name": "T", "columns": [
            {"name": "V", "data_type": "Double"}], "rows": [{"V": 1.0}]}]
        assert json.loads(server.pbix_create(
            p, alias, json.dumps(tables)))["success"]
        cfg = json.dumps({"singleVisual": {
            "visualType": vtype,
            "projections": {"Values": [{"queryRef": "T.V"}]},
            "prototypeQuery": {"Version": 2,
                "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [{"Column": {
                    "Expression": {"SourceRef": {"Source": "t"}},
                    "Property": "V"}, "Name": "T.V"}]}}})
        assert json.loads(server.pbix_add_visual(
            alias, 0, vtype, 40, 40, 200, 100, cfg))["success"]
        r = json.loads(server.pbix_format_visual(
            alias, 0, 0, json.dumps({"categoryLabels": {"color": "#1B6EE0"}})))
        assert r.get("success"), r
        out = str(tmp_path / "saved.pbix")
        assert json.loads(server.pbix_save(
            alias, output_path=out, overwrite=True))["success"]
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)

        alias2 = alias + "_r"
        assert json.loads(server.pbix_open(out, alias2))["success"]
        try:
            raw = json.loads(json.loads(
                server.pbix_get_layout_raw(alias2))["message"])
            sv = json.loads(raw["sections"][0]["visualContainers"][0]
                            ["config"])["singleVisual"]
            props = sv["objects"]["categoryLabels"][0]["properties"]
            assert want in props, props
            assert "#1B6EE0" in json.dumps(props[want])
            assert not_want not in props, props
        finally:
            server._open_files.pop(alias2, None)
            server._dax_cache.pop(alias2, None)


class TestShapeAndButtonFormatting:
    """Issues #47 + #48: pbix_format_visual wrote shape/button properties no
    Desktop-authored visual carries. Measured ground truth (write -> save ->
    reopen -> readback): rotation lives on its OWN card (rotation.shapeAngle
    on `shape`, rotation.angle on basicShape — never shape.rotation), the
    stroke colour is lineColor (outline card on `shape`, line card on
    basicShape — never the generic outline.color), geometry is
    shape.tileShape / general.shapeType, and a button's action lives in
    vcObjects.visualLink (objects.visualLink is a dead bucket) with webUrl
    supported and an objects.text default-state label."""

    @pytest.fixture()
    def gap_report(self, tmp_path):
        import uuid as _uuid
        alias = "gp_" + _uuid.uuid4().hex[:8]
        p = str(tmp_path / "gaps.pbix")
        tables = [{"name": "T", "columns": [
            {"name": "V", "data_type": "Double"}], "rows": [{"V": 1.0}]}]
        assert json.loads(server.pbix_create(
            p, alias, json.dumps(tables)))["success"]
        for vt in ("shape", "basicShape", "actionButton"):
            assert json.loads(server.pbix_add_visual(
                alias, 0, vt, 40, 40, 200, 100, ""))["success"]
        yield alias, p
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)

    def _saved_visuals(self, alias, tmp_path):
        out = str(tmp_path / "saved.pbix")
        assert json.loads(server.pbix_save(
            alias, output_path=out, overwrite=True))["success"]
        server._open_files.pop(alias, None)
        alias2 = alias + "_r"
        assert json.loads(server.pbix_open(out, alias2))["success"]
        raw = json.loads(json.loads(
            server.pbix_get_layout_raw(alias2))["message"])
        server._open_files.pop(alias2, None)
        server._dax_cache.pop(alias2, None)
        return [json.loads(vc["config"])["singleVisual"]
                for vc in raw["sections"][0]["visualContainers"]]

    def test_shape_writes_measured_property_names(self, gap_report, tmp_path):
        alias, _p = gap_report
        assert json.loads(server.pbix_format_visual(alias, 0, 0, json.dumps({
            "shape": {"rotation": 30, "tileShape": "line"},
            "outline": {"show": True, "weight": 2, "color": "#0000FF"},
        })))["success"]
        sv = self._saved_visuals(alias, tmp_path)[0]
        o = sv["objects"]
        rot = o["rotation"][0]["properties"]
        assert "shapeAngle" in rot and "30" in json.dumps(rot["shapeAngle"])
        shp = o["shape"][0]["properties"]
        assert "tileShape" in shp and "'line'" in json.dumps(shp["tileShape"])
        assert "rotation" not in shp
        ol = o["outline"][0]["properties"]
        assert "lineColor" in ol and "#0000FF" in json.dumps(ol["lineColor"])
        assert "color" not in ol

    def test_basicshape_writes_measured_property_names(self, gap_report,
                                                       tmp_path):
        alias, _p = gap_report
        assert json.loads(server.pbix_format_visual(alias, 0, 1, json.dumps({
            "shape": {"rotation": 45, "geometry": "rectangle"},
            "outline": {"color": "#FF0000", "weight": 3},
        })))["success"]
        sv = self._saved_visuals(alias, tmp_path)[1]
        o = sv["objects"]
        assert "angle" in o["rotation"][0]["properties"]
        assert "shapeType" in o["general"][0]["properties"]
        ln = o["line"][0]["properties"]
        assert "lineColor" in ln and "#FF0000" in json.dumps(ln["lineColor"])

    def test_action_writes_vcobjects_with_weburl_and_text_label(
            self, gap_report, tmp_path):
        alias, _p = gap_report
        assert json.loads(server.pbix_format_visual(alias, 0, 2, json.dumps({
            "text": {"text": "Open docs", "show": True},
            "action": {"show": True, "type": "WebUrl",
                       "webUrl": "https://example.com/x"},
        })))["success"]
        sv = self._saved_visuals(alias, tmp_path)[2]
        vl = sv["vcObjects"]["visualLink"][0]["properties"]
        assert "'WebUrl'" in json.dumps(vl["type"])
        assert "https://example.com/x" in json.dumps(vl["webUrl"])
        assert "show" in vl
        # the dead bucket must NOT be written
        assert "visualLink" not in sv["objects"]
        txt = sv["objects"]["text"][0]
        assert txt["selector"] == {"id": "default"}
        assert "'Open docs'" in json.dumps(txt["properties"]["text"])

    def test_non_shape_visuals_keep_legacy_spellings(self, gap_report,
                                                     tmp_path):
        # a table's outline keeps the generic color property — only shape
        # vintages get the per-visual names
        alias, _p = gap_report
        assert json.loads(server.pbix_add_visual(
            alias, 0, "tableEx", 300, 40, 200, 100, ""))["success"]
        assert json.loads(server.pbix_format_visual(alias, 0, 3, json.dumps({
            "outline": {"color": "#123456"}})))["success"]
        sv = self._saved_visuals(alias, tmp_path)[3]
        ol = sv["objects"]["outline"][0]["properties"]
        assert "color" in ol and "lineColor" not in ol
