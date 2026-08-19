"""Issues #57 and #59: page-level formatting and per-column table formatting.

#57 — a report page could not be given a background colour or a wallpaper
image at all, so every converted sheet landed on a plain white canvas even
when the source painted the whole sheet.

#59 — no per-column property of a table/matrix was expressible: there was no
`columnWidth` card, and `columnFormatting` wrote a selector-less entry, so a
property could not be aimed at one column.

Shapes are measured against Desktop-authored files in the local corpus:

    columnWidth       [{"properties": {"value": {"expr": {"Literal":
                        {"Value": "288D"}}}},
                       "selector": {"metadata": "Table.Column"}}, ...]
    columnFormatting  [{"properties": {"alignment": ..., "labelPrecision": ...},
                       "selector": {"metadata": "Table.Column"}}, ...]
    page background   [{"properties": {"color": {"solid": {...}},
                        "transparency": {"expr": {"Literal":
                        {"Value": "0D"}}}}}]

The page CARD NAMES are Microsoft's own: the published PBIR page schema
(page/2.1.0) defines `background` (canvas) and `outspace` (wallpaper) with
the same {color, image, transparency} shape.
"""
import json
import struct
import zlib

import pytest

from pbix_mcp import server as S
from pbix_mcp.server import _build_format_objects as B

pytestmark = pytest.mark.unit


def _png_bytes():
    def chunk(tag, data):
        return (struct.pack(">I", len(data)) + tag + data
                + struct.pack(">I", zlib.crc32(tag + data)))
    ihdr = struct.pack(">II", 1, 1) + b"\x08\x06\x00\x00\x00"
    return (b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", ihdr)
            + chunk(b"IDAT", b"x\x9cc\x00\x01\x00\x00\x05\x00\x01")
            + chunk(b"IEND", b""))


def _mk(tmp_path, alias, fname="p.pbix"):
    p = str(tmp_path / fname)
    r = json.loads(S.pbix_create(p, alias, json.dumps(
        [{"name": "Sales",
          "columns": [{"name": "Region", "data_type": "String"},
                      {"name": "Amount", "data_type": "Double"}],
          "rows": [{"Region": "W", "Amount": 1.0}]}])))
    assert r["success"], r
    return p


def _reopen_layout(alias, p):
    assert json.loads(S.pbix_save(
        alias, output_path=p, overwrite=True))["success"]
    S.pbix_close(alias)
    a2 = alias + "_r"
    assert json.loads(S.pbix_open(p, a2))["success"]
    raw = json.loads(json.loads(S.pbix_get_layout_raw(a2))["message"])
    return a2, raw


class TestPerColumnTableFormatting:
    """Issue #59."""

    def test_column_width_writes_the_measured_shape(self):
        got = B({"columnWidth": {"Sales.Amount": 258.5,
                                 "Sales.Region": 168}})["_objects"]
        entries = got["columnWidth"]
        assert len(entries) == 2
        by_col = {e["selector"]["metadata"]: e for e in entries}
        amount = by_col["Sales.Amount"]
        assert amount["properties"]["value"]["expr"]["Literal"]["Value"] == \
            "258.5D"
        assert by_col["Sales.Region"]["selector"] == \
            {"metadata": "Sales.Region"}

    def test_column_formatting_takes_a_per_column_selector(self):
        got = B({"columnFormatting": {
            "Sales.Amount": {"alignment": "Center", "decimalPlaces": 1},
            "Sales.Region": {"alignment": "Left"}}})["_objects"]
        entries = got["columnFormatting"]
        assert len(entries) == 2
        by_col = {e["selector"]["metadata"]: e["properties"] for e in entries}
        amt = by_col["Sales.Amount"]
        assert amt["alignment"]["expr"]["Literal"]["Value"] == "'Center'"
        assert amt["labelPrecision"]["expr"]["Literal"]["Value"] == "1L"
        assert "labelPrecision" not in by_col["Sales.Region"]

    def test_per_column_colour_and_font(self):
        got = B({"columnFormatting": {
            "Sales.Amount": {"fontColor": "#FF0000", "backColor": "#00FF00",
                             "fontSize": 11, "bold": True}}})["_objects"]
        props = got["columnFormatting"][0]["properties"]
        assert "#FF0000" in json.dumps(props["fontColor"])
        assert "#00FF00" in json.dumps(props["backColor"])
        # the literal SPELLING of a font size is the codebase-wide _pbi_lit
        # convention (shared by every card), not something measured for this
        # property, so only the value is pinned here
        assert props["fontSize"]["expr"]["Literal"]["Value"].startswith("11")
        assert props["bold"]["expr"]["Literal"]["Value"] == "true"

    def test_flat_form_still_writes_a_selectorless_entry(self):
        """Backwards compatibility: the old shape must keep working."""
        got = B({"columnFormatting": {"alignment": "Center"}})["_objects"]
        assert len(got["columnFormatting"]) == 1
        assert "selector" not in got["columnFormatting"][0]

    def test_widths_survive_save_and_reopen(self, tmp_path):
        alias = "cw59"
        p = _mk(tmp_path, alias)
        try:
            assert json.loads(S.pbix_add_visual(
                alias, 0, "tableEx", 40, 40, 400, 300, ""))["success"]
            assert json.loads(S.pbix_format_visual(alias, 0, 0, json.dumps({
                "columnWidth": {"Sales.Amount": 258.5},
                "columnFormatting": {
                    "Sales.Amount": {"alignment": "Center"}}})))["success"]
            a2, raw = _reopen_layout(alias, p)
            sv = json.loads(raw["sections"][0]["visualContainers"][0][
                "config"])["singleVisual"]
            cw = sv["objects"]["columnWidth"][0]
            assert cw["selector"] == {"metadata": "Sales.Amount"}
            assert "258.5D" in json.dumps(cw["properties"])
            cf = sv["objects"]["columnFormatting"][0]
            assert cf["selector"] == {"metadata": "Sales.Amount"}
        finally:
            for a in (alias, alias + "_r"):
                S._open_files.pop(a, None)
                S._dax_cache.pop(a, None)


class TestPageFormatting:
    """Issue #57."""

    @pytest.fixture()
    def opened(self, tmp_path):
        alias = "pg57"
        p = _mk(tmp_path, alias, "pg.pbix")
        yield alias, p, tmp_path
        for a in (alias, alias + "_r"):
            S._open_files.pop(a, None)
            S._dax_cache.pop(a, None)

    def test_background_colour_matches_the_measured_shape(self, opened):
        alias, p, _tmp = opened
        assert json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"background": {"color": "#F2F6F6",
                            "transparency": 0}})))["success"]
        _a2, raw = _reopen_layout(alias, p)
        bg = json.loads(raw["sections"][0]["config"])["objects"]["background"]
        props = bg[0]["properties"]
        assert "#F2F6F6" in json.dumps(props["color"])
        # Desktop writes an integral transparency as "0D", not "0.0D"
        assert props["transparency"]["expr"]["Literal"]["Value"] == "0D"

    def test_a_bare_string_is_accepted_as_the_colour(self, opened):
        alias, p, _tmp = opened
        assert json.loads(S.pbix_format_page(
            alias, 0, json.dumps({"background": "#102030"})))["success"]
        _a2, raw = _reopen_layout(alias, p)
        bg = json.loads(raw["sections"][0]["config"])["objects"]["background"]
        assert "#102030" in json.dumps(bg[0]["properties"]["color"])

    def test_wallpaper_image_registers_and_persists(self, opened):
        alias, p, tmp = opened
        img = tmp / "sheet.png"
        img.write_bytes(_png_bytes())
        r = json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"wallpaper": {"image_path": str(img), "scaling": "Fill"}})))
        assert r["success"], r
        # `wallpaper` is the friendly name for Power BI's `outspace` card
        assert r["data"]["cards"] == ["outspace"]
        _a2, raw = _reopen_layout(alias, p)
        objs = json.loads(raw["sections"][0]["config"])["objects"]
        blob = json.dumps(objs["outspace"])
        assert "ResourcePackageItem" in blob
        assert "RegisteredResources" in blob
        assert "'Fill'" in blob

    def test_both_cards_in_one_call_and_merging(self, opened):
        alias, p, tmp = opened
        assert json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"background": {"color": "#111111"},
             "wallpaper": {"color": "#222222"}})))["success"]
        # a second call must MERGE, not replace, the sibling properties
        assert json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"background": {"transparency": 25}})))["success"]
        _a2, raw = _reopen_layout(alias, p)
        objs = json.loads(raw["sections"][0]["config"])["objects"]
        bg = objs["background"][0]["properties"]
        assert "#111111" in json.dumps(bg["color"])
        assert bg["transparency"]["expr"]["Literal"]["Value"] == "25D"
        assert "#222222" in json.dumps(objs["outspace"][0]["properties"])

    def test_unknown_key_is_refused_by_name(self, opened):
        alias, _p, _tmp = opened
        r = json.loads(S.pbix_format_page(
            alias, 0, json.dumps({"notACard": {"color": "#fff"}})))
        assert not r["success"]
        assert "notACard" in r["message"]

    def test_bad_page_index_is_refused(self, opened):
        alias, _p, _tmp = opened
        r = json.loads(S.pbix_format_page(
            alias, 99, json.dumps({"background": "#fff"})))
        assert not r["success"]
        assert "out of range" in r["message"]

    def test_invalid_scaling_names_the_problem(self, opened):
        alias, _p, tmp = opened
        img = tmp / "s2.png"
        img.write_bytes(_png_bytes())
        r = json.loads(S.pbix_format_page(alias, 0, json.dumps(
            {"background": {"image_path": str(img), "scaling": "Nope"}})))
        assert not r["success"]
        assert "scaling" in r["message"].lower()
