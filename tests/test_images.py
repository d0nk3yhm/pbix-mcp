"""Image / registered-resource authoring (issues-10).

Ground truth for the container + registration shapes is Desktop-authored:
test_corpus/GeoSales_Dashboard.pbix (two image visuals, RegisteredResources
type-100 items, `<Default Extension="png" ContentType=""/>`).
"""
import base64
import json
import os
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit

# 1x1 PNG / GIF / JPEG headers — real magic bytes, tiny payloads
PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmM"
    "IQAAAABJRU5ErkJggg==")
GIF = base64.b64decode("R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF" + b"\x00" * 32 + b"\xff\xd9"
SVG = b"<?xml version='1.0'?><svg xmlns='http://www.w3.org/2000/svg'/>"
WEBP = b"RIFF\x24\x00\x00\x00WEBPVP8 " + b"\x00" * 24


def _build(path):
    b = PBIXBuilder("T")
    b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
    b.save(path)


def _open(tmp_path, alias, name="img.pbix"):
    p = str(tmp_path / name)
    _build(p)
    server.pbix_open(p, alias)
    server.pbix_add_page(alias, "P1")
    return p


def _layout(alias):
    return server._get_layout(server._open_files[alias]["work_dir"])


def _reg_items(alias):
    for pkg in _layout(alias).get("resourcePackages", []):
        inner = pkg.get("resourcePackage", pkg)
        if inner.get("name") == "RegisteredResources":
            return inner.get("items", [])
    return []


def _content_types(alias):
    ct = os.path.join(server._open_files[alias]["work_dir"], "[Content_Types].xml")
    with open(ct, encoding="utf-8") as f:
        return f.read()


def _cleanup(*aliases):
    for a in aliases:
        server._open_files.pop(a, None)


class TestSniffAndNaming:
    def test_magic_byte_sniffing(self):
        assert server._sniff_image_ext(PNG) == "png"
        assert server._sniff_image_ext(JPEG) == "jpg"
        assert server._sniff_image_ext(GIF) == "gif"
        assert server._sniff_image_ext(WEBP) == "webp"
        assert server._sniff_image_ext(SVG) == "svg"
        assert server._sniff_image_ext(b"<svg viewBox='0 0 1 1'/>") == "svg"
        # NOT an image — a name/extension claim must never decide the type
        assert server._sniff_image_ext(b"MZ\x90\x00executable") is None
        assert server._sniff_image_ext(b"") is None
        assert server._sniff_image_ext(b"<html><body>hi</body></html>") is None

    def test_item_name_sanitization(self):
        # path components stripped, unsafe chars replaced, extension forced
        assert server._sanitize_item_name("../../etc/passwd", "png") == "passwd.png"
        assert server._sanitize_item_name("my logo!.jpeg", "png") == "my_logo.png"
        assert server._sanitize_item_name("", "png") == "image.png"
        assert server._sanitize_item_name("a/b/c.png", "png") == "c.png"
        assert len(server._sanitize_item_name("x" * 200, "png")) <= 84

    def test_size_cap_and_source_exclusivity(self, tmp_path):
        big = str(tmp_path / "big.png")
        with open(big, "wb") as f:
            f.write(PNG + b"\x00" * (server._IMAGE_MAX_BYTES + 1))
        with pytest.raises(Exception) as ei:
            server._load_resource_bytes(big, "")
        assert "limit" in str(ei.value)
        with pytest.raises(Exception):        # both sources
            server._load_resource_bytes(big, base64.b64encode(PNG).decode())
        with pytest.raises(Exception):        # neither source
            server._load_resource_bytes("", "")

    def test_data_uri_accepted(self):
        uri = "data:image/png;base64," + base64.b64encode(PNG).decode()
        assert server._load_resource_bytes("", uri) == PNG


class TestAddImage:
    def test_desktop_exact_container(self, tmp_path):
        alias = "img1"
        try:
            _open(tmp_path, alias)
            src = str(tmp_path / "logo.png")
            with open(src, "wb") as f:
                f.write(PNG)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_path=src, x=40, y=40, width=300, height=200))
            assert out["success"], out
            item = out["data"]["item_name"]
            assert item == "logo.png"

            vc = _layout(alias)["sections"][0]["visualContainers"][-1]
            cfg = json.loads(vc["config"])
            sv = cfg["singleVisual"]
            # container: Desktop keys incl. tabOrder + z
            assert set(vc) >= {"x", "y", "z", "width", "height", "tabOrder",
                               "config", "filters"}
            assert vc["filters"] == "[]"
            assert cfg["howCreated"] == "InsertVisualButton"
            pos = cfg["layouts"][0]["position"]
            assert pos["z"] == vc["z"] and pos["tabOrder"] == vc["tabOrder"]
            assert sv["visualType"] == "image"
            assert sv["drillFilterOtherVisuals"] is True
            # imageUrl -> ResourcePackageItem
            expr = sv["objects"]["general"][0]["properties"]["imageUrl"]["expr"]
            assert expr["ResourcePackageItem"] == {
                "PackageName": "RegisteredResources", "PackageType": 1,
                "ItemName": item}
            # scaling lives under objects.imageScaling (NOT general)
            scal = sv["objects"]["imageScaling"][0]["properties"]
            assert scal["imageScalingType"]["expr"]["Literal"]["Value"] == "'Fit'"
            # padding 0D on all four sides
            pad = sv["vcObjects"]["padding"][0]["properties"]
            assert set(pad) == {"left", "top", "right", "bottom"}
            assert all(v["expr"]["Literal"]["Value"] == "0D" for v in pad.values())

            # registration: file + resourcePackages + Content_Types
            res = os.path.join(server._open_files[alias]["work_dir"], "Report",
                               "StaticResources", "RegisteredResources", item)
            assert os.path.exists(res)
            with open(res, "rb") as f:
                assert f.read() == PNG
            assert {"type": 100, "path": item, "name": item} in _reg_items(alias)
            assert 'Extension="png"' in _content_types(alias)
        finally:
            _cleanup(alias)

    def test_z_steps_by_1000(self, tmp_path):
        alias = "img2"
        try:
            _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            zs = []
            for i in range(3):
                out = json.loads(server.pbix_add_image(
                    alias, 0, image_base64=b64, name=f"i{i}"))
                assert out["success"], out
                zs.append(_layout(alias)["sections"][0]["visualContainers"][-1]["z"])
            assert zs == [0, 1000, 2000]
        finally:
            _cleanup(alias)

    def test_scaling_values_and_omission(self, tmp_path):
        alias = "img3"
        try:
            _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            for given, want in (("fill", "'Fill'"), ("NORMAL", "'Normal'"),
                                ("Fit", "'Fit'")):
                out = json.loads(server.pbix_add_image(
                    alias, 0, image_base64=b64, name="s", scaling=given))
                assert out["success"], out
                cfg = json.loads(
                    _layout(alias)["sections"][0]["visualContainers"][-1]["config"])
                lit = (cfg["singleVisual"]["objects"]["imageScaling"][0]
                       ["properties"]["imageScalingType"]["expr"]["Literal"]["Value"])
                assert lit == want
            # empty scaling omits the object entirely (Desktop does this too)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=b64, name="s", scaling=""))
            assert out["success"], out
            cfg = json.loads(
                _layout(alias)["sections"][0]["visualContainers"][-1]["config"])
            assert "imageScaling" not in cfg["singleVisual"]["objects"]
            # bad scaling fails loud
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=b64, name="s", scaling="stretch"))
            assert out["success"] is False and "Fit, Fill, or Normal" in out["message"]
        finally:
            _cleanup(alias)

    def test_rejects_non_image_and_bad_page(self, tmp_path):
        alias = "img4"
        try:
            _open(tmp_path, alias)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(b"MZ\x90 not an image").decode()))
            assert out["success"] is False
            assert "Unrecognized image data" in out["message"]
            out = json.loads(server.pbix_add_image(
                alias, 99, image_base64=base64.b64encode(PNG).decode()))
            assert out["success"] is False and "out of range" in out["message"]
        finally:
            _cleanup(alias)

    def test_survives_save_reopen(self, tmp_path):
        alias, alias2 = "img5", "img5b"
        try:
            p = _open(tmp_path, alias)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG).decode(),
                name="logo"))
            item = out["data"]["item_name"]
            assert json.loads(server.pbix_save(alias))["success"]
            # the resource is IN the saved zip, with its Content_Types Default
            with zipfile.ZipFile(p) as zf:
                names = zf.namelist()
                assert f"Report/StaticResources/RegisteredResources/{item}" in names
                assert zf.read(
                    f"Report/StaticResources/RegisteredResources/{item}") == PNG
                assert 'Extension="png"' in zf.read("[Content_Types].xml").decode()
            server.pbix_close(alias, force=True)
            server.pbix_open(p, alias2)
            assert any(i["name"] == item for i in _reg_items(alias2))
        finally:
            _cleanup(alias, alias2)


class TestContentTypesFallback:
    """Issue #2a: the json-anchored replace silently no-opped on documents
    whose [Content_Types].xml has no json Default (the repo's own fixtures)."""

    def test_no_json_default_still_declares_extension(self, tmp_path):
        alias = "ct1"
        try:
            _open(tmp_path, alias)
            ct = os.path.join(server._open_files[alias]["work_dir"],
                              "[Content_Types].xml")
            with open(ct, "w", encoding="utf-8") as f:
                f.write('<?xml version="1.0" encoding="utf-8"?><Types '
                        'xmlns="http://schemas.openxmlformats.org/package/'
                        '2006/content-types"></Types>')
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG).decode(),
                name="logo"))
            assert out["success"], out
            xml = _content_types(alias)
            assert '<Default Extension="png" ContentType=""/>' in xml
            assert xml.rstrip().endswith("</Types>")
        finally:
            _cleanup(alias)

    def test_idempotent_and_case_insensitive(self, tmp_path):
        alias = "ct2"
        try:
            _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            json.loads(server.pbix_add_image(alias, 0, image_base64=b64, name="a"))
            json.loads(server.pbix_add_image(alias, 0, image_base64=b64, name="b"))
            assert _content_types(alias).count('Extension="png"') == 1
        finally:
            _cleanup(alias)

    def test_legacy_sourcepath_hook_also_fixed(self, tmp_path):
        """The undocumented pbix_add_visual hook shares the helpers now."""
        alias = "ct3"
        try:
            _open(tmp_path, alias)
            ct = os.path.join(server._open_files[alias]["work_dir"],
                              "[Content_Types].xml")
            with open(ct, "w", encoding="utf-8") as f:
                f.write('<?xml version="1.0"?><Types xmlns="http://schemas.'
                        'openxmlformats.org/package/2006/content-types">'
                        '</Types>')
            src = str(tmp_path / "old.png")
            with open(src, "wb") as f:
                f.write(PNG)
            cfg = json.dumps({"singleVisual": {"objects": {"general": [
                {"properties": {"imageUrl": {"sourcePath": src}}}]}}})
            out = json.loads(server.pbix_add_visual(
                alias, 0, "image", config_json=cfg))
            assert out["success"], out
            assert 'Extension="png"' in _content_types(alias)
            items = _reg_items(alias)
            assert items and items[0]["type"] == 100
            vc = _layout(alias)["sections"][0]["visualContainers"][-1]
            expr = (json.loads(vc["config"])["singleVisual"]["objects"]
                    ["general"][0]["properties"]["imageUrl"]["expr"])
            assert expr["ResourcePackageItem"]["ItemName"] == items[0]["name"]
        finally:
            _cleanup(alias)


class TestRegisterResourceAndSetImage:
    def test_register_types_and_reuse(self, tmp_path):
        alias = "reg1"
        try:
            _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            out = json.loads(server.pbix_register_resource(
                alias, "logo", image_base64=b64))
            assert out["success"] and out["data"]["item_name"] == "logo.png"
            # identical bytes under the same name reuse the item (no dupes)
            out = json.loads(server.pbix_register_resource(
                alias, "logo", image_base64=b64))
            assert out["data"]["item_name"] == "logo.png"
            assert len(_reg_items(alias)) == 1
            # DIFFERENT bytes under the same name must NOT clobber — same
            # extension, so the name collides and gets uniquified
            other_png = PNG + b"\x00"
            out = json.loads(server.pbix_register_resource(
                alias, "logo", image_base64=base64.b64encode(other_png).decode()))
            assert out["success"], out
            assert out["data"]["item_name"] == "logo_1.png"
            res_dir = os.path.join(server._open_files[alias]["work_dir"], "Report",
                                   "StaticResources", "RegisteredResources")
            with open(os.path.join(res_dir, "logo.png"), "rb") as f:
                assert f.read() == PNG          # original untouched
            assert len(_reg_items(alias)) == 2
            # a different FORMAT gets its own extension (no collision) and its
            # own Content_Types Default
            out = json.loads(server.pbix_register_resource(
                alias, "logo", image_base64=base64.b64encode(GIF).decode()))
            assert out["success"] and out["data"]["item_name"] == "logo.gif"
            assert 'Extension="gif"' in _content_types(alias)
            # non-image resource types are JSON in Desktop files (corpus:
            # a type-200 TopoJSON shape map, type-202 BaseThemes/*.json)
            topo = base64.b64encode(
                json.dumps({"type": "Topology", "objects": {}}).encode()).decode()
            out = json.loads(server.pbix_register_resource(
                alias, "us-states", image_base64=topo, resource_type="shapeMap"))
            assert out["success"], out
            assert out["data"]["item_name"] == "us-states.json"
            assert any(i["type"] == 200 and i["name"] == "us-states.json"
                       for i in _reg_items(alias))
            assert 'Extension="json"' in _content_types(alias)
            # an IMAGE registered as a theme must be REJECTED (Power BI could
            # not consume it) — and non-JSON as a shape map likewise
            out = json.loads(server.pbix_register_resource(
                alias, "theme", image_base64=b64, resource_type="customTheme"))
            assert out["success"] is False and "must be JSON" in out["message"]
            # ...and JSON registered as an image is rejected too
            out = json.loads(server.pbix_register_resource(
                alias, "notimg", image_base64=topo, resource_type="image"))
            assert out["success"] is False
            assert "Unrecognized image data" in out["message"]
            out = json.loads(server.pbix_register_resource(
                alias, "x", image_base64=b64, resource_type="nope"))
            assert out["success"] is False and "Unknown resource_type" in out["message"]
        finally:
            _cleanup(alias)

    def test_set_image_repoint_and_scaling(self, tmp_path):
        alias = "set1"
        try:
            _open(tmp_path, alias)
            json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG).decode(),
                name="first"))

            def item_of(idx=0):
                cfg = json.loads(
                    _layout(alias)["sections"][0]["visualContainers"][idx]["config"])
                return (cfg["singleVisual"]["objects"]["general"][0]["properties"]
                        ["imageUrl"]["expr"]["ResourcePackageItem"]["ItemName"])

            assert item_of() == "first.png"
            # replace with new bytes — registers a new item, repoints
            out = json.loads(server.pbix_set_image(
                alias, 0, 0, image_base64=base64.b64encode(GIF).decode(),
                name="second", scaling="Fill"))
            assert out["success"], out
            assert item_of() == "second.gif"
            cfg = json.loads(
                _layout(alias)["sections"][0]["visualContainers"][0]["config"])
            lit = (cfg["singleVisual"]["objects"]["imageScaling"][0]["properties"]
                   ["imageScalingType"]["expr"]["Literal"]["Value"])
            assert lit == "'Fill'"
            # the OLD resource is deliberately left in place
            assert any(i["name"] == "first.png" for i in _reg_items(alias))

            # repoint at an already-registered item by name
            out = json.loads(server.pbix_set_image(
                alias, 0, 0, item_name="first.png"))
            assert out["success"], out
            assert item_of() == "first.png"
            # unknown item name fails loud, listing what IS registered
            out = json.loads(server.pbix_set_image(
                alias, 0, 0, item_name="nope.png"))
            assert out["success"] is False
            assert "not registered" in out["message"] and "first.png" in out["message"]
            # scaling-only change
            out = json.loads(server.pbix_set_image(alias, 0, 0, scaling="Normal"))
            assert out["success"], out
            assert item_of() == "first.png"
            # no-op call fails loud
            out = json.loads(server.pbix_set_image(alias, 0, 0))
            assert out["success"] is False and "Nothing to change" in out["message"]
        finally:
            _cleanup(alias)

    def test_set_image_rejects_non_image_visual(self, tmp_path):
        alias = "set2"
        try:
            _open(tmp_path, alias)
            server.pbix_add_visual(alias, 0, "card", x=10, y=10)
            out = json.loads(server.pbix_set_image(alias, 0, 0, scaling="Fit"))
            assert out["success"] is False
            assert "not an image visual" in out["message"]
            out = json.loads(server.pbix_set_image(alias, 0, 99, scaling="Fit"))
            assert out["success"] is False and "out of range" in out["message"]
        finally:
            _cleanup(alias)

    def test_traversal_contained(self, tmp_path):
        """A hostile item name must never escape RegisteredResources."""
        alias = "trav"
        try:
            _open(tmp_path, alias)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG).decode(),
                name="../../../../evil"))
            assert out["success"], out
            item = out["data"]["item_name"]
            assert "/" not in item and ".." not in item
            wd = server._open_files[alias]["work_dir"]
            assert os.path.exists(os.path.join(
                wd, "Report", "StaticResources", "RegisteredResources", item))
        finally:
            _cleanup(alias)


class TestReviewRoundHardening:
    """Regressions from the pre-release adversarial review of this round."""

    def test_case_only_collision_keeps_package_consistent(self, tmp_path):
        """On a case-insensitive FS (macOS/Windows — where Desktop runs)
        'Logo.png' and 'logo.png' are ONE file: keeping the caller's casing
        registered a layout item + visual reference for a part that never
        landed in the .pbix."""
        alias, alias2 = "case1", "case1b"
        try:
            p = _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            a = json.loads(server.pbix_add_image(
                alias, 0, image_base64=b64, name="Logo"))["data"]["item_name"]
            b = json.loads(server.pbix_add_image(
                alias, 0, image_base64=b64, name="logo"))["data"]["item_name"]
            assert a == "Logo.png"
            assert b == a, "case-variant of identical bytes must reuse the item"
            assert len(_reg_items(alias)) == 1
            # different bytes under a case-variant name uniquify instead
            c = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG + b"\x00").decode(),
                name="LOGO"))["data"]["item_name"]
            assert c not in ("Logo.png", "logo.png", "LOGO.png"), c

            # EVERY registered item must exist as a part of the saved file,
            # and every visual reference must resolve to one
            assert json.loads(server.pbix_save(alias))["success"]
            with zipfile.ZipFile(p) as zf:
                parts = {n.rsplit("/", 1)[-1] for n in zf.namelist()
                         if "RegisteredResources/" in n}
                lay = json.loads(zf.read("Report/Layout").decode("utf-16-le"))
            items = []
            for pkg in lay.get("resourcePackages", []):
                inner = pkg.get("resourcePackage", pkg)
                if inner.get("name") == "RegisteredResources":
                    items = [i["name"] for i in inner["items"]]
            assert set(items) <= parts, (items, parts)
            for vc in lay["sections"][0]["visualContainers"]:
                cfg = json.loads(vc["config"])
                sv = cfg.get("singleVisual", {})
                if sv.get("visualType") != "image":
                    continue
                ref = (sv["objects"]["general"][0]["properties"]["imageUrl"]
                       ["expr"]["ResourcePackageItem"]["ItemName"])
                assert ref in parts, f"dangling image reference {ref!r}"
        finally:
            _cleanup(alias, alias2)

    def test_svg_variants_sniffed(self):
        """Real-world SVGs: BOM, XML declaration, DOCTYPE, leading comment."""
        body = b"<svg xmlns='http://www.w3.org/2000/svg'><rect/></svg>"
        assert server._sniff_image_ext(b"\xef\xbb\xbf" + body) == "svg"
        assert server._sniff_image_ext(b"<?xml version='1.0'?>\n" + body) == "svg"
        assert server._sniff_image_ext(
            b'<!DOCTYPE svg PUBLIC "-//W3C//DTD SVG 1.1//EN" '
            b'"http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd">\n' + body) == "svg"
        assert server._sniff_image_ext(b"<!-- made by hand -->\n" + body) == "svg"
        assert server._sniff_image_ext(
            b"\xef\xbb\xbf<?xml version='1.0'?><!-- c -->" + body) == "svg"
        # arbitrary XML that merely MENTIONS svg is not an SVG
        assert server._sniff_image_ext(
            b"<?xml version='1.0'?><config><note>use svg here</note></config>") is None
        assert server._sniff_image_ext(b"<html><svg/></html>") is None

    def test_additional_raster_formats(self):
        """BMP/TIFF/ICO were embeddable through the legacy hook — keep them."""
        assert server._sniff_image_ext(b"BM" + b"\x00" * 40) == "bmp"
        assert server._sniff_image_ext(b"II*\x00" + b"\x00" * 20) == "tiff"
        assert server._sniff_image_ext(b"MM\x00*" + b"\x00" * 20) == "tiff"
        assert server._sniff_image_ext(b"\x00\x00\x01\x00" + b"\x00" * 20) == "ico"

    def test_line_wrapped_base64_accepted(self):
        """MIME / `base64` / openssl output wraps at 64-76 columns."""
        raw = base64.b64encode(PNG).decode()
        wrapped = "\n".join(raw[i:i + 16] for i in range(0, len(raw), 16))
        assert server._load_resource_bytes("", wrapped) == PNG
        assert server._load_resource_bytes("", " " + raw + "\n") == PNG

    def test_set_image_rejects_ambiguous_source(self, tmp_path):
        alias = "amb"
        try:
            _open(tmp_path, alias)
            b64 = base64.b64encode(PNG).decode()
            json.loads(server.pbix_add_image(alias, 0, image_base64=b64, name="a"))
            out = json.loads(server.pbix_set_image(
                alias, 0, 0, image_base64=b64, item_name="a.png"))
            assert out["success"] is False and "not both" in out["message"]
        finally:
            _cleanup(alias)

    def test_directory_at_target_name_uniquifies(self, tmp_path):
        alias = "dir1"
        try:
            _open(tmp_path, alias)
            res = os.path.join(server._open_files[alias]["work_dir"], "Report",
                               "StaticResources", "RegisteredResources")
            os.makedirs(os.path.join(res, "logo.png"), exist_ok=True)
            out = json.loads(server.pbix_add_image(
                alias, 0, image_base64=base64.b64encode(PNG).decode(),
                name="logo"))
            assert out["success"], out
            assert out["data"]["item_name"] != "logo.png"
        finally:
            _cleanup(alias)

    def test_legacy_hook_never_persists_sourcepath(self, tmp_path):
        """The private key must not leak the author's local path into the
        saved report, and a missing file must fail loud."""
        alias = "leak"
        try:
            _open(tmp_path, alias)
            src = str(tmp_path / "logo.png")
            with open(src, "wb") as f:
                f.write(PNG)
            cfg = json.dumps({"singleVisual": {"objects": {"general": [
                {"properties": {"imageUrl": {"sourcePath": src}}}]}}})
            assert json.loads(server.pbix_add_visual(
                alias, 0, "image", config_json=cfg))["success"]
            blob = json.dumps(_layout(alias))
            assert "sourcePath" not in blob
            assert src not in blob
            # missing file -> loud error, not a silent image-less visual
            bad = json.dumps({"singleVisual": {"objects": {"general": [
                {"properties": {"imageUrl": {
                    "sourcePath": "/no/such/file.png"}}}]}}})
            out = json.loads(server.pbix_add_visual(
                alias, 0, "image", config_json=bad))
            assert out["success"] is False and "not found" in out["message"]
        finally:
            _cleanup(alias)
