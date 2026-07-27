"""pbix_doctor's report-definition checks.

Each check here corresponds to a defect class found by auditing all 125 tools:
state a tool wrote that never reached disk, a reference that stopped resolving,
or a classic-shaped value written into a PBIR document. A report can be
perfectly schema-valid and still fail every one of them.

Two properties matter equally and are both tested:

  * the check FIRES on a deliberately broken report — otherwise it is
    decorative, and a green doctor means nothing;
  * the check STAYS QUIET on 24 real reports — a doctor that cries wolf on
    Microsoft's own samples will be ignored, and then it protects nobody.

The severity split is deliberate. A dangling bookmark step is something Power BI
tolerates (Microsoft ships samples with them), so it warns. A page whose folder
and `name` disagree makes the page vanish, so it fails.
"""
import json
import os
import shutil
import uuid
import zipfile

import pytest

from pbix_mcp import server
from tests.test_pbir_reader import _write_pbir


def _zip_dir(root, path):
    with zipfile.ZipFile(path, "w") as z:
        for cur, _dirs, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(cur, fn)
                z.write(full, os.path.relpath(full, root))
        z.writestr("Version", "1.28")
    return path


@pytest.fixture
def pbir_root(tmp_path):
    root = str(tmp_path / "src")
    os.makedirs(root, exist_ok=True)
    _write_pbir(root)
    d = os.path.join(root, "Report", "definition")
    with open(os.path.join(d, "report.json"), "w") as f:
        json.dump({"$schema": "report/1.0.0",
                   "resourcePackages": [], "publicCustomVisuals": []}, f)
    return root


def _doctor(path):
    alias = "dr_" + uuid.uuid4().hex[:8]
    server.pbix_open(path, alias)
    try:
        return json.loads(server.pbix_doctor(alias))["message"]
    finally:
        server.pbix_close(alias, force=True)


def _line(report, name):
    for ln in report.split("\n"):
        if name in ln:
            return ln.strip()
    raise AssertionError(f"check {name!r} not present in doctor output")


def _status(report, name):
    ln = _line(report, name)
    if ln.startswith("✅"):
        return "ok"
    if ln.startswith("⚠"):
        return "warn"
    return "fail"


class TestChecksStayQuietOnHealthyReports:
    def test_clean_pbir_passes_every_check(self, pbir_root, tmp_path):
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "ok.pbix")))
        for name in ("Registered resources", "Custom visual registration",
                     "Page / visual naming", "Bookmark references",
                     "PBIR page tree", "PBIR visual tree",
                     "PBIR naming convention", "PBIR classic-shape leaks",
                     "PBIR enum fields"):
            assert _status(out, name) == "ok", _line(out, name)


class TestResourceRegistration:
    def test_undeclared_resource_file_fails(self, pbir_root, tmp_path):
        """Microsoft: every resource file needs an entry in report.json.
        An undeclared file simply never renders."""
        res = os.path.join(pbir_root, "Report", "StaticResources",
                           "RegisteredResources")
        os.makedirs(res, exist_ok=True)
        with open(os.path.join(res, "orphan.png"), "wb") as f:
            f.write(b"\x89PNG\r\n\x1a\n")
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "Registered resources") == "fail"
        assert "orphan.png" in _line(out, "Registered resources")

    def test_declared_but_missing_file_fails(self, pbir_root, tmp_path):
        d = os.path.join(pbir_root, "Report", "definition")
        with open(os.path.join(d, "report.json"), "w") as f:
            json.dump({"$schema": "report/1.0.0", "resourcePackages": [{
                "name": "RegisteredResources", "type": "RegisteredResources",
                "items": [{"name": "ghost.png", "path": "ghost.png",
                           "type": "Image"}]}]}, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "Registered resources") == "fail"
        assert "ghost.png" in _line(out, "Registered resources")


class TestCustomVisualRegistration:
    def _use_type(self, pbir_root, visual_type):
        vp = os.path.join(pbir_root, "Report", "definition", "pages", "pageA",
                          "visuals", "visA", "visual.json")
        with open(vp) as f:
            doc = json.load(f)
        doc["visual"]["visualType"] = visual_type
        with open(vp, "w") as f:
            json.dump(doc, f)

    def test_unregistered_custom_visual_warns(self, pbir_root, tmp_path):
        self._use_type(pbir_root, "someVisual8D7CFFDA2E7E400C9474F41B9EDBBA58")
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        # A warning, not a failure: it may come from the tenant's org store,
        # which no file inspection can confirm.
        assert _status(out, "Custom visual registration") == "warn"

    def test_embedded_pbiviz_satisfies_registration(self, pbir_root, tmp_path):
        """Most of Microsoft's samples register private visuals this way.
        Checking only publicCustomVisuals reports them all as broken."""
        vt = "someVisual8D7CFFDA2E7E400C9474F41B9EDBBA58"
        self._use_type(pbir_root, vt)
        cv = os.path.join(pbir_root, "Report", "CustomVisuals", vt)
        os.makedirs(cv, exist_ok=True)
        with open(os.path.join(cv, "package.json"), "w") as f:
            f.write("{}")
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "ok.pbix")))
        assert _status(out, "Custom visual registration") == "ok"

    def test_builtin_visual_types_are_not_flagged(self, pbir_root, tmp_path):
        self._use_type(pbir_root, "clusteredColumnChart")
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "ok.pbix")))
        assert _status(out, "Custom visual registration") == "ok"


class TestNaming:
    def test_duplicate_page_name_fails(self, pbir_root, tmp_path):
        pb = os.path.join(pbir_root, "Report", "definition", "pages", "pageB",
                          "page.json")
        with open(pb) as f:
            doc = json.load(f)
        doc["name"] = "pageA"          # collides with the other page
        with open(pb, "w") as f:
            json.dump(doc, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "Page / visual naming") == "fail"

    def test_visual_names_may_repeat_across_pages(self, pbir_root, tmp_path):
        """Visual names are scoped per page — bookmarks address them as
        sections[page].visualContainers[visual]. Microsoft's AI sample reuses
        74 visual names across pages, and that is legitimate."""
        vb = os.path.join(pbir_root, "Report", "definition", "pages", "pageB",
                          "visuals", "visB", "visual.json")
        with open(vb) as f:
            doc = json.load(f)
        doc["name"] = "visA"
        with open(vb, "w") as f:
            json.dump(doc, f)
        # rename the folder to match, so the tree check stays happy
        vdir = os.path.dirname(vb)
        os.rename(vdir, os.path.join(os.path.dirname(vdir), "visA"))
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "ok.pbix")))
        assert _status(out, "Page / visual naming") == "ok"


class TestBookmarkReferences:
    def test_dangling_bookmark_reference_warns(self, pbir_root, tmp_path):
        bdir = os.path.join(pbir_root, "Report", "definition", "bookmarks")
        os.makedirs(bdir, exist_ok=True)
        with open(os.path.join(bdir, "bm1.bookmark.json"), "w") as f:
            json.dump({"$schema": "bookmark/1.0.0", "name": "bm1",
                       "displayName": "Stale",
                       "explorationState": {"version": "1.2",
                                            "activeSection": "pageGONE",
                                            "sections": {}}}, f)
        with open(os.path.join(bdir, "bookmarks.json"), "w") as f:
            json.dump({"$schema": "bookmarksMetadata/1.0.0",
                       "items": [{"name": "bm1"}]}, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "Bookmark references") == "warn"
        assert "pageGONE" in _line(out, "Bookmark references")


class TestPBIRTreeIntegrity:
    def test_page_name_folder_mismatch_fails(self, pbir_root, tmp_path):
        pj = os.path.join(pbir_root, "Report", "definition", "pages", "pageA",
                          "page.json")
        with open(pj) as f:
            doc = json.load(f)
        doc["name"] = "somethingElse"
        with open(pj, "w") as f:
            json.dump(doc, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR page tree") == "fail"

    def test_page_missing_from_pageorder_fails(self, pbir_root, tmp_path):
        pj = os.path.join(pbir_root, "Report", "definition", "pages",
                          "pages.json")
        with open(pj) as f:
            meta = json.load(f)
        meta["pageOrder"] = ["pageA"]      # pageB exists but is unlisted
        with open(pj, "w") as f:
            json.dump(meta, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR page tree") == "fail"

    def test_visual_name_folder_mismatch_fails(self, pbir_root, tmp_path):
        vj = os.path.join(pbir_root, "Report", "definition", "pages", "pageA",
                          "visuals", "visA", "visual.json")
        with open(vj) as f:
            doc = json.load(f)
        doc["name"] = "notVisA"
        with open(vj, "w") as f:
            json.dump(doc, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR visual tree") == "fail"

    def test_illegal_folder_name_fails(self, pbir_root, tmp_path):
        pages = os.path.join(pbir_root, "Report", "definition", "pages")
        os.rename(os.path.join(pages, "pageB"),
                  os.path.join(pages, "page B!"))
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR naming convention") == "fail"


class TestPBIRShapeChecks:
    def test_classic_shape_leak_fails(self, pbir_root, tmp_path):
        """A `singleVisual` key in a PBIR visual means the converter wrote the
        legacy shape into a file Power BI reads with the new one."""
        vj = os.path.join(pbir_root, "Report", "definition", "pages", "pageA",
                          "visuals", "visA", "visual.json")
        with open(vj) as f:
            doc = json.load(f)
        doc["singleVisual"] = {"visualType": "slicer"}
        with open(vj, "w") as f:
            json.dump(doc, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR classic-shape leaks") == "fail"

    def test_classic_int_enum_fails(self, pbir_root, tmp_path):
        """The exact defect that shipped in 0.9.35: a page carrying the classic
        integer displayOption imports fine and then will not open."""
        pj = os.path.join(pbir_root, "Report", "definition", "pages", "pageA",
                          "page.json")
        with open(pj) as f:
            doc = json.load(f)
        doc["displayOption"] = 0
        with open(pj, "w") as f:
            json.dump(doc, f)
        out = _doctor(_zip_dir(pbir_root, str(tmp_path / "b.pbix")))
        assert _status(out, "PBIR enum fields") == "fail"
        assert "displayOption" in _line(out, "PBIR enum fields")


CORPUS = os.environ.get("PBIX_TEST_SAMPLES", "test_corpus")


@pytest.mark.slow
class TestNoFalsePositivesOnRealReports:
    """The calibration half. These are real reports from Microsoft and the
    community; any hard failure here is the doctor being wrong."""

    @pytest.mark.parametrize("sample", sorted(
        os.path.basename(p) for p in
        __import__("glob").glob(os.path.join(CORPUS, "*.pbix"))) or ["_none"])
    def test_no_hard_failures(self, sample, tmp_path):
        path = os.path.join(CORPUS, sample)
        if sample == "_none" or not os.path.exists(path):
            pytest.skip("needs the public test corpus "
                        "(scripts/download_test_corpus.py)")
        work = str(tmp_path / sample)
        shutil.copy(path, work)
        out = _doctor(work)
        failures = [ln.strip() for ln in out.split("\n")
                    if ln.strip().startswith("❌")
                    and any(k in ln for k in (
                        "Registered resources", "Custom visual registration",
                        "Page / visual naming", "Bookmark references",
                        "PBIR "))]
        assert not failures, "\n".join(failures)
