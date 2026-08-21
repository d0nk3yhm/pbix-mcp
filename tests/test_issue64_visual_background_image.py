"""Issue #64: a visual background image was accepted, dropped, and reported
as applied.

`pbix_format_visual` ignored every image key on the `background` card, wrote
`show` regardless, and answered "Formatted visual N on page M: background" —
naming the card as though it had landed. A caller could not tell honoured
from silently discarded.

Power BI has no background image on a VISUAL. Verified independently of the
report, across 168 local reports: page backgrounds carry an `image` 30
times; visual backgrounds carry it ZERO times out of 1,507 property
occurrences, and the vocabulary is exactly show / color / transparency.

So the fix is refuse-by-name, not implement — and the refusal has to name the
property, because the whole point is that the caller learns at the call site.
"""
import base64
import json
import struct
import zlib

import pytest

from pbix_mcp import server as S

pytestmark = pytest.mark.unit


def _png_bytes():
    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data)))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">II", 1, 1) + b"\x08\x06\x00\x00\x00")
            + chunk(b"IDAT", b"x\x9cc\x00\x01\x00\x00\x05\x00\x01")
            + chunk(b"IEND", b""))


B64 = base64.b64encode(_png_bytes()).decode()


@pytest.fixture()
def carded(tmp_path):
    alias = "i64_" + tmp_path.name[-6:]
    p = str(tmp_path / "t.pbix")
    assert json.loads(S.pbix_create(p, alias, json.dumps([{
        "name": "T", "columns": [{"name": "V", "data_type": "Double"}],
        "rows": [{"V": 1.0}]}])))["success"]
    assert json.loads(S.pbix_add_visual(
        alias, 0, "card", 40, 40, 200, 100, ""))["success"]
    yield alias, p, tmp_path
    for a in (alias, alias + "_r"):
        S._open_files.pop(a, None)
        S._dax_cache.pop(a, None)


class TestVisualBackgroundImageIsRefusedByName:
    @pytest.mark.parametrize("spec,named", [
        ({"image_base64": B64, "scaling": "Fit"}, "background.image_base64"),
        ({"image": {"image_base64": B64}}, "background.image"),
        ({"image_path": "logo.png"}, "background.image_path"),
        ({"imageUrl": "http://x/y.png"}, "background.imageUrl"),
    ])
    def test_refused_and_names_the_property(self, carded, spec, named):
        alias, _p, _t = carded
        r = json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": spec})))
        assert not r["success"], r
        msg = r["message"]
        assert named in msg, msg
        # it must not merely fail: it must say WHY and where to go instead
        assert "pbix_format_page" in msg
        assert "Nothing was written" in msg

    def test_refusal_leaves_no_orphan_resource(self, carded):
        """The old path registered nothing; a refusal must not start
        registering one either."""
        alias, _p, _t = carded
        json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": {"image_base64": B64}})))
        res = json.loads(S.pbix_list_resources(alias))["message"]
        assert "No resources found" in res, res

    def test_refusal_does_not_write_the_card(self, carded):
        """A refused call must leave the visual untouched, not half-written."""
        alias, p, tmp = carded
        json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": {"image_base64": B64}})))
        out = str(tmp / "saved.pbix")
        assert json.loads(S.pbix_save(
            alias, output_path=out, overwrite=True))["success"]
        S.pbix_close(alias)
        a2 = alias + "_r"
        assert json.loads(S.pbix_open(out, a2))["success"]
        raw = json.loads(json.loads(S.pbix_get_layout_raw(a2))["message"])
        sv = json.loads(raw["sections"][0]["visualContainers"][0][
            "config"])["singleVisual"]
        assert "background" not in (sv.get("vcObjects") or {})


class TestColourBackgroundStillWorks:
    """The control from the report — this path must be untouched."""

    def test_colour_and_transparency_are_honoured(self, carded):
        alias, p, tmp = carded
        assert json.loads(S.pbix_format_visual(alias, 0, 0, json.dumps(
            {"background": {"color": "#D6E7F8",
                            "transparency": 0}})))["success"]
        out = str(tmp / "c.pbix")
        assert json.loads(S.pbix_save(
            alias, output_path=out, overwrite=True))["success"]
        S.pbix_close(alias)
        a2 = alias + "_r"
        assert json.loads(S.pbix_open(out, a2))["success"]
        raw = json.loads(json.loads(S.pbix_get_layout_raw(a2))["message"])
        sv = json.loads(raw["sections"][0]["visualContainers"][0][
            "config"])["singleVisual"]
        props = sv["vcObjects"]["background"][0]["properties"]
        assert "#D6E7F8" in json.dumps(props["color"])
        assert "true" in json.dumps(props["show"])

    def test_explicit_show_alone_is_honoured(self, carded):
        alias, _p, _t = carded
        assert json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": {"show": True}})))["success"]


class TestEmptyBackgroundNoLongerLooksApplied:
    """`show` used to be fabricated even when nothing else was recognised,
    which is what made a dropped image report as success."""

    def test_empty_background_is_refused(self, carded):
        alias, _p, _t = carded
        r = json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": {}})))
        assert not r["success"]
        assert "no recognised properties" in r["message"]

    def test_unknown_only_background_is_refused(self, carded):
        alias, _p, _t = carded
        r = json.loads(S.pbix_format_visual(
            alias, 0, 0, json.dumps({"background": {"nonsense": 1}})))
        assert not r["success"]
        assert "background" in r["message"]


class TestPageBackgroundImageStillWorks:
    """Refusing at visual level must not touch the page-level tool, which is
    where a background image legitimately lives."""

    def test_page_image_is_still_honoured(self, carded):
        alias, _p, tmp = carded
        img = tmp / "wall.png"
        img.write_bytes(_png_bytes())
        r = json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"background": {"image_path": str(img), "scaling": "Fit"}})))
        assert r["success"], r
        res = json.loads(S.pbix_list_resources(alias))["message"]
        assert "wall.png" in res, res
