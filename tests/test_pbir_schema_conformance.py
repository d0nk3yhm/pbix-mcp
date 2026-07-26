"""Validate what the PBIR writer emits against Microsoft's OWN JSON schemas.

Every PBIR file declares a `$schema` on developer.microsoft.com. Checking our
output against those schemas tests the writer against the format owner's
contract rather than against our own reader — which is the only way to catch a
field we round-trip consistently but shape wrongly. It already caught two live
defects: a page written with the classic integer `displayOption`, and bookmarks
emitted with a `byColumn` filter bucket that PBIR does not define.

Marked `integration` because it needs network access on a cold schema cache.
"""
import json
import os
import sys
import uuid
import zipfile

import pytest

from pbix_mcp import server

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def validator():
    try:
        import validate_pbir_schemas as v
    except ImportError:  # pragma: no cover
        pytest.skip("scripts/validate_pbir_schemas.py not importable")
    try:
        v.fetch_schema(
            "https://developer.microsoft.com/json-schemas/fabric/item/report/"
            "definition/pagesMetadata/1.1.0/schema.json")
    except Exception as exc:  # pragma: no cover - offline
        pytest.skip(f"PBIR schemas unreachable and not cached: {exc}")
    return v


def _pbir_pbix(tmp_path):
    """A minimal service-shaped PBIR report, zipped."""
    from tests.test_pbir_reader import _write_pbir

    root = str(tmp_path / "src")
    os.makedirs(root, exist_ok=True)
    _write_pbir(root)

    # _write_pbir writes placeholder $schema values; stamp the real published
    # ones so the fixture is a faithful stand-in for a service download.
    base = ("https://developer.microsoft.com/json-schemas/fabric/item/report/"
            "definition/")
    d = os.path.join(root, "Report", "definition")
    _stamp(os.path.join(d, "version.json"),
           base + "versionMetadata/1.0.0/schema.json")
    # report.json 1.0.0 requires themeCollection + layoutOptimization; a real
    # service download carries both.
    _stamp(os.path.join(d, "report.json"), base + "report/1.0.0/schema.json",
           themeCollection={"baseTheme": {"name": "CY24SU06",
                                          "reportVersionAtImport": "5.55",
                                          "type": "SharedResources"}},
           layoutOptimization="None")
    _stamp(os.path.join(d, "pages", "pages.json"),
           base + "pagesMetadata/1.1.0/schema.json")
    for pid in ("pageA", "pageB"):
        _stamp(os.path.join(d, "pages", pid, "page.json"),
               base + "page/2.1.0/schema.json")
    for pid, vid in (("pageA", "visA"), ("pageB", "visB")):
        _stamp(os.path.join(d, "pages", pid, "visuals", vid, "visual.json"),
               base + "visualContainer/1.0.0/schema.json")
    _stamp(os.path.join(d, "pages", "pageA", "visuals", "visA", "mobile.json"),
           base + "visualContainerMobileState/1.0.0/schema.json")

    path = str(tmp_path / "report.pbix")
    with zipfile.ZipFile(path, "w") as z:
        for cur, _dirs, files in os.walk(os.path.join(root, "Report")):
            for fn in files:
                full = os.path.join(cur, fn)
                z.write(full, os.path.relpath(full, root))
        z.writestr("Version", "1.28")
    return path


def _stamp(path, schema_url, **extra):
    with open(path) as f:
        doc = json.load(f)
    doc["$schema"] = schema_url
    doc.update(extra)
    with open(path, "w") as f:
        json.dump(doc, f)


def _edit_and_save(src, out):
    alias = "sc_" + uuid.uuid4().hex[:8]
    server.pbix_open(src, alias)
    try:
        assert json.loads(server.pbix_add_page(alias, "Added Page"))["success"]
        assert json.loads(
            server.pbix_add_visual(alias, 0, "clusteredColumnChart",
                                   x=400, y=120, width=500, height=320)
        )["success"]
        assert json.loads(
            server.pbix_add_bookmark(alias, "A Bookmark"))["success"]
        server.pbix_save(alias, out, overwrite=True, backup=False)
    finally:
        server.pbix_close(alias, force=True)


class TestPBIRSchemaConformance:
    def test_untouched_report_is_schema_clean(self, validator, tmp_path):
        """Guards the guard: if the fixture itself failed validation, a later
        clean result would prove nothing."""
        src = _pbir_pbix(tmp_path)
        checked, errors = validator.validate_pbix(__import__("pathlib").Path(src))
        assert checked > 0
        assert errors == []

    def test_edited_report_conforms(self, validator, tmp_path):
        import pathlib

        src = _pbir_pbix(tmp_path)
        out = str(tmp_path / "edited.pbix")
        _edit_and_save(src, out)

        checked, errors = validator.validate_pbix(pathlib.Path(out))
        assert checked > 0
        assert errors == [], "\n".join(errors)

    def test_new_page_uses_the_enum_name_not_the_classic_int(
            self, validator, tmp_path):
        """`displayOption` is an int in Report/Layout and a string in PBIR.

        pbix_add_page builds a classic-shaped section, so without conversion
        the int leaked straight through and Desktop rejected the page.
        """
        src = _pbir_pbix(tmp_path)
        out = str(tmp_path / "edited.pbix")
        _edit_and_save(src, out)

        with zipfile.ZipFile(out) as z:
            pages = [n for n in z.namelist() if n.endswith("/page.json")]
            assert len(pages) == 3
            for n in pages:
                doc = json.loads(z.read(n).decode("utf-8-sig"))
                assert doc["displayOption"] == "FitToPage", n
