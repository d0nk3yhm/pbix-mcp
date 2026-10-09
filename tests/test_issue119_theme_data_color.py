"""Issue #119: a ThemeDataColor's ColorId indexes the colour picker's theme
row -- pure white, pure black, then the first eight data colours.

_resolve_theme_color read dataColors[ColorId], so pbix_extract_colors reported
every "White" as the theme's first data colour, and pbix_recolor rewrote white
and grey fills to the new primary when it remapped that colour (Desktop then
painted a recoloured Matrix Bubble Chart's cells magenta). Ground truth is
Power BI Desktop's own code (desktop.min.js, DataColorPalette):
basePickerColors = [#FFFFFF, #000000] + the first 8 data colours, and
getThemeDataColor returns basePickerColors[ColorId]. The Percent shade is
Desktop's function too: the vectors below are its output, run verbatim under
Node (7,560 colour/percent pairs, every one equal to the port).
"""
from __future__ import annotations

import json

import pytest

from pbix_mcp import server as S

pytestmark = pytest.mark.unit

PAL = ["#118DFF", "#12239E", "#E66C37", "#6B007B", "#E044A7", "#744EC2", "#D9B300", "#D64550",
       "#197278", "#1AAB40"]


@pytest.mark.parametrize("cid,want", [(0, "#FFFFFF"), (1, "#000000")]
                         + [(i + 2, PAL[i]) for i in range(8)] + [(10, None), (11, None)])
def test_colorid_indexes_the_picker_row(cid, want):
    assert S._resolve_theme_color(PAL, cid, 0) == want


def test_a_short_palette_has_fewer_picker_colours():
    assert S._resolve_theme_color(PAL[:3], 4, 0) == "#E66C37"
    assert S._resolve_theme_color(PAL[:3], 5, 0) is None
    assert S._resolve_theme_color([], 2, 0) is None
    assert S._resolve_theme_color([], 0, -0.1) == "#E6E6E6"     # white needs no theme


# (ColorId over PAL, Percent, Desktop's shade)
DESKTOP_SHADES = [
    (0, -0.1, "#E6E6E6"), (0, -0.2, "#CCCCCC"), (0, -0.3, "#B3B3B3"), (0, -0.5, "#808080"),
    (0, -0.6, "#666666"), (1, 0.1, "#1A1A1A"), (1, 0.2, "#333333"), (1, 0.4, "#666666"),
    (1, 0.6, "#999999"), (2, 0.6, "#A0D1FF"), (2, -0.25, "#0D6ABF"), (2, -0.5, "#094780"),
    (3, 0.4, "#717BC5"),
]


@pytest.mark.parametrize("cid,pct,want", DESKTOP_SHADES)
def test_shades_are_desktops(cid, pct, want):
    assert S._resolve_theme_color(PAL, cid, pct) == want


def _report(tmp_path, refs):
    """A report themed PAL[:3] with one shape per (ColorId, Percent) ref, as
    its background colour."""
    alias = "tdc_" + tmp_path.name[-6:]
    p = str(tmp_path / "t.pbix")
    assert json.loads(S.pbix_create(p, alias, json.dumps([{
        "name": "T", "columns": [{"name": "V", "data_type": "Double"}], "rows": [{"V": 1.0}]}])))["success"]
    assert json.loads(S.pbix_set_theme(alias, json.dumps({"name": "t119", "dataColors": PAL[:3]})))["success"]
    for i, _ in enumerate(refs):
        assert json.loads(S.pbix_add_visual(alias, 0, "shape", 20 + 210 * i, 20, 200, 120, ""))["success"]
    layout = json.loads(json.loads(S.pbix_get_layout_raw(alias))["message"])
    for vc, (cid, pct) in zip(layout["sections"][0]["visualContainers"], refs):
        cfg = json.loads(vc["config"])
        cfg.setdefault("singleVisual", {}).setdefault("vcObjects", {})["background"] = [{"properties": {
            "color": {"solid": {"color": {"expr": {"ThemeDataColor": {"ColorId": cid, "Percent": pct}}}}}}}]
        vc["config"] = json.dumps(cfg)
    assert json.loads(S.pbix_set_layout_raw(alias, json.dumps(layout)))["success"]
    return alias


def _backgrounds(alias):
    layout = json.loads(json.loads(S.pbix_get_layout_raw(alias))["message"])
    out = []
    for vc in layout["sections"][0]["visualContainers"]:
        expr = json.loads(vc["config"])["singleVisual"]["vcObjects"]["background"][0][
            "properties"]["color"]["solid"]["color"]["expr"]
        out.append(expr.get("ThemeDataColor") or expr.get("Literal"))
    return out


def test_extract_colors_reports_what_desktop_draws(tmp_path):
    alias = _report(tmp_path, [(0, -0.1), (1, 0), (2, 0), (12, 0)])
    try:
        msg = json.loads(S.pbix_extract_colors(alias))["message"]
    finally:
        S.pbix_close(alias)
    by_hex = {ln.split()[0]: ln for ln in msg.splitlines()[1:] if ln.strip().startswith("#")}
    assert "[ThemeDataColor:0,-0.1]" in by_hex["#E6E6E6"]       # 0.9.120: #0F7EE5
    assert "[ThemeDataColor:1,0.0]" in by_hex["#000000"]
    assert "[ThemeDataColor:2,0.0]" in by_hex["#118DFF"]
    unresolved = [ln for ln in msg.splitlines() if "unresolved ThemeDataColor" in ln]
    assert unresolved and "[ThemeDataColor:12,0.0]" in unresolved[0]


def test_recolor_leaves_white_and_black_alone(tmp_path):
    alias = _report(tmp_path, [(0, -0.1), (1, 0), (2, 0), (2, 0.4), (12, 0)])
    try:
        assert json.loads(S.pbix_recolor(alias, json.dumps({"#118DFF": "#C2185B"})))["success"]
        got = _backgrounds(alias)
    finally:
        S.pbix_close(alias)
    assert got[0] == {"ColorId": 0, "Percent": -0.1}             # 0.9.120: '#C2185B'
    assert got[1] == {"ColorId": 1, "Percent": 0}
    assert got[2] == {"Value": "'#C2185B'"}                      # the remapped data colour
    assert got[3] == {"Value": "'#C2185B'"}
    assert got[4] == {"ColorId": 12, "Percent": 0}               # no such colour: left alone
