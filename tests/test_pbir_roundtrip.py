"""PBIR round-trip: state that lives outside the pages tree.

`_set_layout_pbir` originally persisted only pages and visuals. Everything else
a classic tool mutates through `_get_layout`/`_set_layout` — container-level
formatting, visual sort, resource registrations, custom-visual declarations,
report-level filters and settings — was applied to a synthesized dict and then
silently discarded on save. The tool returned success either way, so on a
service-authored report the change simply evaporated.

A 125-tool sweep found thirteen instances of that one bug. These tests pin the
two mappings that close them:

  * `visual.visualContainerObjects` <-> classic `singleVisual.vcObjects`
    (placement verified against 70/70 visuals in the service-authored corpus;
    `visualContainer` sets `additionalProperties: false` and does NOT permit it
    at the top level — the PBIP exporter used to emit it there)
  * report-level state <-> `Report/definition/report.json`

Every test reads the SAVED bytes back, never the in-memory view.
"""
import base64
import json
import os
import shutil
import uuid
import zipfile

import pytest

from pbix_mcp import server
from tests.test_pbir_reader import _write_pbir

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==")

SRC_TITLE = "'ZZ_SOURCE_TITLE'"
SRC_COLOR = "'#AABBCC'"


def _title_obj(text):
    return [{"properties": {"text": {"expr": {"Literal": {"Value": text}}}}}]


@pytest.fixture
def pbir(tmp_path):
    """A PBIR report whose first visual carries container formatting AND a sort,
    plus a report.json with a themeCollection — the state that used to vanish."""
    root = str(tmp_path / "src")
    os.makedirs(root, exist_ok=True)
    _write_pbir(root)

    d = os.path.join(root, "Report", "definition")
    vpath = os.path.join(d, "pages", "pageA", "visuals", "visA", "visual.json")
    with open(vpath) as f:
        vis = json.load(f)
    vis["visual"]["visualContainerObjects"] = {
        "title": _title_obj(SRC_TITLE),
        "background": [{"properties": {"color": {"solid": {"color": {
            "expr": {"Literal": {"Value": SRC_COLOR}}}}}}}],
    }
    vis["visual"].setdefault("query", {})["sortDefinition"] = {
        "sort": [{"field": {"Column": {
            "Expression": {"SourceRef": {"Entity": "Sales"}},
            "Property": "Category"}}, "direction": "Descending"}],
        "isDefaultSort": False,
    }
    with open(vpath, "w") as f:
        json.dump(vis, f)

    rpath = os.path.join(d, "report.json")
    with open(rpath, "w") as f:
        json.dump({
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/"
                       "item/report/definition/report/1.0.0/schema.json",
            "themeCollection": {"baseTheme": {
                "name": "CY24SU06",
                "reportVersionAtImport": {"visual": "1.0.0", "report": "1.0.0",
                                          "page": "1.0.0"},
                "type": "SharedResources"}},
            "resourcePackages": [{
                "name": "SharedResources", "type": "SharedResources",
                "items": [{"name": "CY24SU06",
                           "path": "BaseThemes/CY24SU06.json",
                           "type": "BaseTheme"}]}],
        }, f)

    path = str(tmp_path / "r.pbix")
    with zipfile.ZipFile(path, "w") as z:
        for cur, _dirs, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(cur, fn)
                z.write(full, os.path.relpath(full, root))
        z.writestr("Version", "1.28")
    return path


class _Edit:
    def __init__(self, path, tmp_path):
        self.path, self.tmp_path = path, tmp_path
        self.alias = "rt_" + uuid.uuid4().hex[:8]

    def __enter__(self):
        server.pbix_open(self.path, self.alias)
        return self

    def __exit__(self, *exc):
        try:
            server.pbix_close(self.alias, force=True)
        except Exception:
            pass

    def save(self):
        out = str(self.tmp_path / f"o_{uuid.uuid4().hex[:6]}.pbix")
        server.pbix_save(self.alias, out, overwrite=True, backup=False)
        return out


def _ok(raw):
    d = json.loads(raw)
    assert d["success"], d
    return d


def _defn(path):
    """All Report/definition text, for presence assertions."""
    with zipfile.ZipFile(path) as z:
        return "\n".join(
            z.read(n).decode("utf-8-sig", "replace") for n in z.namelist()
            if n.startswith("Report/definition"))


def _report_json(path):
    with zipfile.ZipFile(path) as z:
        return json.loads(
            z.read("Report/definition/report.json").decode("utf-8-sig"))


def _visuals(path):
    with zipfile.ZipFile(path) as z:
        return [json.loads(z.read(n).decode("utf-8-sig"))
                for n in sorted(z.namelist()) if n.endswith("visual.json")]


def _vco(v):
    return (v.get("visual") or {}).get("visualContainerObjects") or {}


# Internal bookkeeping the converter adds to the in-memory view. None of it may
# ever reach disk.
_SENTINELS = ("__pbir_sort__", "__pbir_visual__", "__pbir_page__",
              "__pbir_file__", "__pbir_schema__", "__pbir__")

# Classic-shape spellings that must never appear in a PAGE or VISUAL file.
# They ARE legitimate inside a bookmark's explorationState, which models
# captured visual state using the classic vocabulary, so this list is only
# applied to page.json / visual.json.
_CLASSIC_LEAKS = ("vcObjects", "prototypeQuery", "singleVisual")


def _page_and_visual_text(path):
    with zipfile.ZipFile(path) as z:
        return "\n".join(
            z.read(n).decode("utf-8-sig", "replace") for n in z.namelist()
            if n.endswith("page.json") or n.endswith("visual.json"))


class TestFixtureIsMeaningful:
    """Guards the guard — if the fixture lacked this state, every assertion
    below would pass vacuously."""

    def test_fixture_has_container_formatting_and_sort(self, pbir):
        vs = _visuals(pbir)
        assert any(_vco(v) for v in vs)
        assert any("sortDefinition" in ((v.get("visual") or {}).get("query") or {})
                   for v in vs)

    def test_container_formatting_is_never_top_level(self, pbir):
        """visualContainer forbids it at the top level; the service always puts
        it under `visual`."""
        for v in _visuals(pbir):
            assert "visualContainerObjects" not in v


class TestContainerFormatting:
    def test_reader_exposes_it_as_vcObjects(self, pbir):
        with _Edit(pbir, None) as e:
            lay = server._get_layout(server._open_files[e.alias]["work_dir"])
            cfg = json.loads(lay["sections"][0]["visualContainers"][0]["config"])
            assert SRC_TITLE in json.dumps(cfg["singleVisual"]["vcObjects"])

    def test_untouched_visual_keeps_it(self, pbir, tmp_path):
        """A page-only edit must not strip formatting off an untouched visual."""
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_rename_page(e.alias, "0", "Renamed"))
            out = e.save()
        assert SRC_TITLE in _defn(out)

    def test_format_visual_persists(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_format_visual(e.alias, 0, 0, json.dumps(
                {"title": {"text": "ZZ_NEW_TITLE", "show": True}})))
            out = e.save()
        assert "ZZ_NEW_TITLE" in _defn(out)

    def test_set_visual_property_persists(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_visual_property(
                e.alias, 0, 0,
                "singleVisual.vcObjects.title.0.properties.text.expr."
                "Literal.Value", '"\'ZZ_PROP\'"'))
            out = e.save()
        assert "ZZ_PROP" in _defn(out)

    def test_numeric_path_segment_makes_an_array(self, pbir, tmp_path):
        """`title.0.properties` must build a LIST. Building {"0": ...} produced
        JSON the published schema rejects."""
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_visual_property(
                e.alias, 0, 0,
                "singleVisual.vcObjects.border.0.properties.show", "true"))
            out = e.save()
        border = next(_vco(v)["border"] for v in _visuals(out) if _vco(v))
        assert isinstance(border, list), border

    def test_duplicate_visual_carries_it(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_duplicate_visual(e.alias, "0", 0))
            out = e.save()
        styled = [v for v in _visuals(out) if SRC_TITLE in json.dumps(_vco(v))]
        assert len(styled) == 2, "the copy came out unstyled"

    def test_duplicate_page_carries_it(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_duplicate_page(e.alias, "0", "Copy"))
            out = e.save()
        styled = [v for v in _visuals(out) if SRC_TITLE in json.dumps(_vco(v))]
        assert len(styled) == 2

    def test_recolor_reaches_container_colours(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_recolor(e.alias, json.dumps({"#AABBCC": "#DDEE99"})))
            out = e.save()
        text = _defn(out)
        assert "#DDEE99" in text and "#AABBCC" not in text


class TestVisualSort:
    def test_existing_sort_survives_an_unrelated_edit(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_move_visual(e.alias, "0", 0, x=321))
            out = e.save()
        assert any("sortDefinition" in ((v.get("visual") or {}).get("query") or {})
                   for v in _visuals(out))

    def test_duplicate_keeps_the_sort(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_duplicate_visual(e.alias, "0", 0))
            out = e.save()
        sorted_ = [v for v in _visuals(out)
                   if "sortDefinition" in ((v.get("visual") or {}).get("query") or {})]
        assert len(sorted_) == 2

    def test_set_visual_sort_writes_entity_refs(self, pbir, tmp_path):
        """PBIR sortDefinition must carry Entity refs, not query aliases."""
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_visual_sort(
                e.alias, 0, 0, sort_by="Sales.Category", sort_direction="asc"))
            out = e.save()
        sd = next(((v.get("visual") or {}).get("query") or {}).get("sortDefinition")
                  for v in _visuals(out)
                  if ((v.get("visual") or {}).get("query") or {}).get("sortDefinition"))
        assert sd["sort"][0]["direction"] == "Ascending"
        blob = json.dumps(sd)
        assert '"Entity"' in blob and '"Source"' not in blob


class TestReportLevelState:
    def test_add_image_declares_the_resource(self, pbir, tmp_path):
        """Docs: 'Every resource file must have a corresponding entry in the
        report.json file.' Without it the image never renders."""
        img = str(tmp_path / "logo.png")
        with open(img, "wb") as f:
            f.write(PNG)
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_add_image(e.alias, 0, img, x=1, y=1,
                                      width=40, height=40))
            out = e.save()
        names = [i.get("name")
                 for p in _report_json(out).get("resourcePackages", [])
                 for i in (p.get("items") or [])]
        assert any(str(n).endswith(".png") for n in names), names

    def test_resource_types_use_pbir_enum_names(self, pbir, tmp_path):
        img = str(tmp_path / "logo.png")
        with open(img, "wb") as f:
            f.write(PNG)
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_add_image(e.alias, 0, img, x=1, y=1,
                                      width=40, height=40))
            out = e.save()
        for p in _report_json(out).get("resourcePackages", []):
            assert isinstance(p.get("type"), str), p
            for i in p.get("items") or []:
                assert isinstance(i.get("type"), str), i

    def test_set_theme_registers_a_custom_theme(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_theme(e.alias, json.dumps(
                {"name": "ZZTheme", "dataColors": ["#112233"]})))
            out = e.save()
        tc = _report_json(out).get("themeCollection") or {}
        assert "customTheme" in tc
        # Required by the published schema on every theme slot.
        assert tc["customTheme"].get("reportVersionAtImport") is not None

    def test_set_theme_keeps_the_reports_real_base_theme(self, pbir, tmp_path):
        """Substituting a classic built-in default would contradict
        resourcePackages, which still declares the original."""
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_theme(e.alias, json.dumps({"name": "ZZTheme"})))
            out = e.save()
        tc = _report_json(out)["themeCollection"]
        assert tc["baseTheme"]["name"] == "CY24SU06"

    def test_report_level_filters_persist(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_filters(e.alias, json.dumps(
                [{"name": "ZZFILTER", "type": "Categorical"}]), page_index=-1))
            out = e.save()
        assert "ZZFILTER" in json.dumps(
            _report_json(out).get("filterConfig") or {})

    def test_settings_go_to_report_json(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_settings(e.alias, json.dumps(
                {"exportDataMode": "None"})))
            out = e.save()
        assert (_report_json(out).get("settings") or {}).get(
            "exportDataMode") == "None"
        with zipfile.ZipFile(out) as z:
            assert "Report/Settings" not in z.namelist(), \
                "a second, conflicting settings document was created"

    def test_get_settings_reads_report_json(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_set_settings(e.alias, json.dumps(
                {"exportDataMode": "None"})))
            out = e.save()
        with _Edit(out, tmp_path) as e2:
            d = _ok(server.pbix_get_settings(e2.alias))
            assert "exportDataMode" in d["message"]

    def test_page_only_edit_does_not_wipe_report_state(self, pbir, tmp_path):
        """The reader surfaces report-level state onto the layout; the writer
        must not treat 'absent from this edit' as 'delete'."""
        before = _report_json(pbir)
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_add_page(e.alias, "Another"))
            out = e.save()
        after = _report_json(out)
        assert after.get("resourcePackages") == before.get("resourcePackages")
        assert after.get("themeCollection") == before.get("themeCollection")


class TestNoOpFidelity:
    def test_read_write_cycle_changes_nothing(self, pbir, tmp_path):
        """The strongest guard against the reader inventing state that the
        writer then persists."""
        with _Edit(pbir, tmp_path) as e:
            wd = server._open_files[e.alias]["work_dir"]
            server._set_layout(wd, server._get_layout(wd))
            out = e.save()
        with zipfile.ZipFile(pbir) as a, zipfile.ZipFile(out) as b:
            for n in a.namelist():
                if not n.startswith("Report/definition"):
                    continue
                assert json.loads(a.read(n).decode("utf-8-sig")) == \
                    json.loads(b.read(n).decode("utf-8-sig")), n

    def test_no_internal_sentinels_reach_disk(self, pbir, tmp_path):
        with _Edit(pbir, tmp_path) as e:
            _ok(server.pbix_duplicate_page(e.alias, "0", "Copy"))
            out = e.save()
        text = _defn(out)
        for sentinel in _SENTINELS:
            assert sentinel not in text, sentinel
        pv = _page_and_visual_text(out)
        for leak in _CLASSIC_LEAKS:
            assert leak not in pv, leak


CORPUS = os.environ.get("PBIX_TEST_SAMPLES", "test_corpus")
CORPUS_PBIR = [os.path.join(CORPUS, n) for n in
               ("IT_Support.pbix", "Ecommerce_Conversion.pbix")]


@pytest.mark.slow
@pytest.mark.parametrize("sample", CORPUS_PBIR)
class TestRealServiceReportFidelity:
    """The synthetic fixture is small and hand-built. These are real
    service-authored reports (50 and 22 visuals) carrying the full range of
    PBIR features — custom visuals, registered resources, themes, container
    formatting, sorts, drillthrough. A converter regression shows up here first.
    """

    def _skip_missing(self, sample):
        if not os.path.exists(sample):
            pytest.skip("needs the public test corpus "
                        "(scripts/download_test_corpus.py)")

    def test_read_write_is_byte_faithful(self, sample, tmp_path):
        """Reading and writing back without editing anything must not change a
        single definition file. This is the strongest guard against the reader
        inventing state or the writer dropping it."""
        self._skip_missing(sample)
        work = str(tmp_path / "w.pbix")
        shutil.copy(sample, work)
        alias = "fid_" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            wd = server._open_files[alias]["work_dir"]
            server._set_layout(wd, server._get_layout(wd))
            out = str(tmp_path / "o.pbix")
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)

        with zipfile.ZipFile(sample) as a, zipfile.ZipFile(out) as b:
            names = [n for n in a.namelist()
                     if n.startswith("Report/definition") and n.endswith(".json")]
            assert names, "fixture is not PBIR"
            assert set(names) <= set(b.namelist()), "files were dropped"
            for n in names:
                assert json.loads(a.read(n).decode("utf-8-sig")) == \
                    json.loads(b.read(n).decode("utf-8-sig")), n

    def test_container_formatting_survives_an_edit(self, sample, tmp_path):
        """These reports carry real container formatting on most visuals; an
        unrelated page edit must not strip any of it."""
        self._skip_missing(sample)
        before = sum(1 for v in _visuals(sample) if _vco(v))
        assert before > 0, "fixture carries no container formatting"

        work = str(tmp_path / "w.pbix")
        shutil.copy(sample, work)
        alias = "fmt_" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            _ok(server.pbix_add_page(alias, "ZZ Unrelated"))
            out = str(tmp_path / "o.pbix")
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)
        assert sum(1 for v in _visuals(out) if _vco(v)) == before

    def test_no_sentinels_leak_into_a_real_report(self, sample, tmp_path):
        """These reports have bookmarks, so they exercise the bookmark writer's
        bookkeeping keys as well as the visual converter's."""
        self._skip_missing(sample)
        work = str(tmp_path / "w.pbix")
        shutil.copy(sample, work)
        alias = "sen_" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            _ok(server.pbix_add_page(alias, "ZZ Unrelated"))
            _ok(server.pbix_add_bookmark(alias, "ZZ Bookmark"))
            out = str(tmp_path / "o.pbix")
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)
        text = _defn(out)
        for sentinel in _SENTINELS:
            assert sentinel not in text, sentinel
        pv = _page_and_visual_text(out)
        for leak in _CLASSIC_LEAKS:
            assert leak not in pv, leak

    def test_report_level_state_survives_an_edit(self, sample, tmp_path):
        self._skip_missing(sample)
        before = _report_json(sample)
        work = str(tmp_path / "w.pbix")
        shutil.copy(sample, work)
        alias = "rls_" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            _ok(server.pbix_add_page(alias, "ZZ Unrelated"))
            out = str(tmp_path / "o.pbix")
            server.pbix_save(alias, out, overwrite=True, backup=False)
        finally:
            server.pbix_close(alias, force=True)
        after = _report_json(out)
        for key in ("resourcePackages", "publicCustomVisuals",
                    "themeCollection", "settings"):
            assert after.get(key) == before.get(key), key
