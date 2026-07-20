"""Tests for the HTML custom-visual tools and template library.

Covers the turnkey create/view/edit flow (embedding + String-bound container +
content measure), the DAX-literal escaping round-trip, and the professional
HTML/SVG template builders (including HTML-escaping of user text).
"""
import json

import pytest

from pbix_mcp import html_templates as ht
from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

# ---- DAX literal round-trip ------------------------------------------------

def test_html_dax_literal_roundtrip():
    html = '<div class="x" data-q="a&b">Hi "there" <b>bold</b></div>'
    lit = server._html_to_dax_literal(html)
    # a DAX string literal: double-quoted, embedded quotes doubled
    assert lit.startswith('"') and lit.endswith('"')
    assert server._decode_html_dax_literal(lit) == html


def test_decode_rejects_data_driven():
    # a concatenation expression is not a single literal -> not decodable to html
    assert server._decode_html_dax_literal('"a" & FORMAT(1,"0")') is None
    assert server._decode_html_dax_literal('SUM(T[x])') is None


# ---- template library ------------------------------------------------------

def test_templates_render_and_escape():
    h = ht.render("table", {"headers": ["Item", "Qty"],
                            "rows": [["<script>evil()</script>", 3]]})
    assert "<script>evil()</script>" not in h      # escaped
    assert "&lt;script&gt;" in h
    assert ht.render("kpi_card", {"title": "T", "value": "9", "spark": [1, 2, 3]})
    assert "<svg" in ht.render("gauge", {"title": "g", "percent": 42})


def test_template_unknown_and_bad_keys():
    with pytest.raises(ValueError):
        ht.render("does_not_exist", {})
    with pytest.raises(ValueError):
        ht.render("badge", {"text": "x", "bogus": 1})


def test_gauge_clamps_percent():
    # out-of-range percentages must not blow up or produce NaN paths
    assert "<svg" in ht.render("gauge", {"title": "g", "percent": 250})
    assert "<svg" in ht.render("gauge", {"title": "g", "percent": -10})


# ---- end-to-end via the server tools --------------------------------------

def _open_report(tmp_path, alias):
    p = str(tmp_path / "r.pbix")
    b = PBIXBuilder("T")
    b.add_table("Sales", [{"name": "Region", "data_type": "String"},
                          {"name": "Amt", "data_type": "Int64"}],
                rows=[{"Region": "N", "Amt": 10}, {"Region": "S", "Amt": 20}])
    b.save(p)
    server.pbix_open(p, alias)
    # ensure a page exists
    server.pbix_add_page(alias, "Page 1")
    return server._open_files[alias]["work_dir"]


def test_add_get_set_html_visual(tmp_path):
    alias = "hv1"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1

        r = json.loads(server.pbix_add_html_visual(
            alias, page, html="<div class=\"c\">Hello &amp; world</div>",
            x=10, y=10, width=300, height=200, measure_name="Card HTML"))
        assert r["success"], r

        # embedding: guid folder + publicCustomVisuals registration
        layout = server._get_layout(wd)
        assert server._HTML_VISUAL_GUID in layout.get("publicCustomVisuals", [])
        # the container is String-bound with query + dataTransforms
        got = json.loads(server.pbix_get_html_visual(alias, page))["data"]
        assert got["count"] == 1
        v = got["visuals"][0]
        assert v["measure_name"] == "Card HTML"
        assert v["data_driven"] is False
        assert "Hello &amp; world" in v["html"]

        # verify the dataTransforms bind the content measure as String (type 1)
        sec = layout["sections"][page]
        vc = sec["visualContainers"][v["visual_index"]]
        dt = json.loads(vc["dataTransforms"])
        assert dt["selects"][0]["type"]["underlyingType"] == 1

        # edit the content
        s = json.loads(server.pbix_set_html_visual(
            alias, page, visual_index=v["visual_index"],
            html="<b>EDITED</b>"))
        assert s["success"], s
        got2 = json.loads(server.pbix_get_html_visual(alias, page))["data"]
        assert "<b>EDITED</b>" in got2["visuals"][0]["html"]
    finally:
        server._open_files.pop(alias, None)


def test_add_html_visual_with_category_crossfilter(tmp_path):
    """A category_field must bind a second column so the visual receives
    per-value selection identities (the cross-filter path)."""
    alias = "hvc"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1
        r = json.loads(server.pbix_add_html_visual(
            alias, page,
            html="<span data-pbix-select='N'>North</span><span data-pbix-select='S'>South</span>",
            measure_name="Map HTML", category_field="Sales.Region"))
        assert r["success"], r
        layout = server._get_layout(wd)
        vc = layout["sections"][page]["visualContainers"][-1]
        sv = json.loads(vc["config"])["singleVisual"]
        # projections carry both content and category
        assert "category" in sv["projections"]
        # prototypeQuery has the measure + the category column
        sel = sv["prototypeQuery"]["Select"]
        assert any("Measure" in s for s in sel) and any("Column" in s for s in sel)
        # dataTransforms tags the category role
        dt = json.loads(vc["dataTransforms"])
        roles = {s["queryName"]: s.get("roles", {}) for s in dt["selects"]}
        assert any(rl.get("category") for rl in roles.values())
        assert any(rl.get("content") for rl in roles.values())
    finally:
        server._open_files.pop(alias, None)


def test_category_field_unknown_errors(tmp_path):
    alias = "hvcx"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1
        r = json.loads(server.pbix_add_html_visual(
            alias, page, html="<i>x</i>", measure_name="M2 HTML",
            category_field="Nope.NoColumn"))
        assert r["success"] is False
    finally:
        server._open_files.pop(alias, None)


def test_add_html_visual_via_template(tmp_path):
    alias = "hv2"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1
        r = json.loads(server.pbix_add_html_visual(
            alias, page, template="kpi_card",
            template_spec_json=json.dumps({"title": "Rev", "value": "1.2M",
                                           "spark": [1, 3, 2, 5]}),
            measure_name="KPI HTML"))
        assert r["success"], r
        got = json.loads(server.pbix_get_html_visual(alias, page))["data"]
        assert got["count"] == 1
        assert "svg" in got["visuals"][0]["html"].lower()
    finally:
        server._open_files.pop(alias, None)


def test_add_html_visual_rejects_ambiguous_content(tmp_path):
    alias = "hv3"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1
        # both html and dax -> error, and neither -> error
        both = json.loads(server.pbix_add_html_visual(
            alias, page, html="<i>x</i>", dax='"y"'))
        assert both["success"] is False
        neither = json.loads(server.pbix_add_html_visual(alias, page))
        assert neither["success"] is False
    finally:
        server._open_files.pop(alias, None)


def test_remove_custom_visual_deregisters(tmp_path):
    alias = "hv4"
    try:
        wd = _open_report(tmp_path, alias)
        page = len(server._get_layout(wd)["sections"]) - 1
        server.pbix_add_html_visual(alias, page, html="<i>x</i>",
                                    measure_name="M HTML")
        assert server._HTML_VISUAL_GUID in server._get_layout(wd)["publicCustomVisuals"]
        rr = json.loads(server.pbix_remove_custom_visual(alias, server._HTML_VISUAL_GUID))
        assert rr["success"], rr
        assert server._HTML_VISUAL_GUID not in server._get_layout(wd).get(
            "publicCustomVisuals", [])
    finally:
        server._open_files.pop(alias, None)
