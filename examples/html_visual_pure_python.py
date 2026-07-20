"""Pure-Python (NO MCP client) demo of the pbix-mcp HTML-visual feature.

pbix-mcp exposes every capability as an ``@mcp.tool()`` function in
``pbix_mcp.server``.  Those decorators do NOT hide the underlying callables:
each one is still a plain, importable Python function.  So an application such
as OpenBI can drive the whole feature *in-process* — no MCP server, no stdio
transport, no JSON-RPC — simply by importing and calling the functions.

This script proves the "custom HTML / CSS / SVG visual" feature (added in
pbix-mcp 0.9.23) works end-to-end from plain Python.  It:

  (a) builds a tiny data model from scratch with ``pbix_mcp.builder.PBIXBuilder``
      (a ``Sales`` table with a ``Region`` column + rows) and saves a .pbix;
  (b) opens it via ``pbix_mcp.server.pbix_open``;
  (c) adds a report page;
  (d) adds three KPI-style HTML visuals — one from a raw ``html=`` string,
      one from a built-in ``template=``, and one *data-driven* via ``dax=``;
  (e) adds a cross-filtering HTML visual (``category_field="Sales.Region"``)
      whose SVG tags each region with ``data-pbix-select="<region>"`` so a
      click filters the rest of the report;
  (f) calls ``pbix_mcp.html_templates.render(...)`` directly to show the
      pure-Python template API (returns an HTML string, raises on error);
  (g) lists the visuals with ``pbix_get_html_visual`` and prints a summary;
  (h) edits one visual's content with ``pbix_set_html_visual``;
  (i) saves the result.

CONTRACT REMINDERS (the "pure-Python usage" rules):
  * Every ``pbix_*`` server function returns a JSON **string** (the
    ``ToolResponse`` envelope).  Parse it with ``json.loads`` and check
    ``obj["success"]``.  ``to_text()`` uses ``exclude_none=True``, so the
    ``data`` key is ABSENT on responses that carry no payload (e.g. the
    add/save calls) — always use ``obj.get("data")``, never ``obj["data"]``.
  * ``pbix_mcp.html_templates`` functions (``render`` and the builder fns) are
    a different shape: they return raw HTML **strings** and raise exceptions —
    there is no JSON envelope.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

# Make the demo runnable straight from a checkout (matches the other examples).
# When pbix-mcp is pip-installed this line is harmless; when it is not, it lets
# the repo's ``src/`` win.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pbix_mcp import html_templates  # pure-python HTML builders
from pbix_mcp.builder import PBIXBuilder  # build a model from scratch
from pbix_mcp.server import (  # the "MCP" tools, as plain fns
    pbix_add_html_visual,
    pbix_add_page,
    pbix_close,
    pbix_get_html_visual,
    pbix_open,
    pbix_save,
    pbix_set_html_visual,
)


def call(json_text: str, label: str) -> dict:
    """Parse a server function's JSON return value and assert success.

    Returns the decoded envelope dict so callers can read ``.get("data")``.
    """
    obj = json.loads(json_text)
    if not obj.get("success"):
        raise AssertionError(
            f"{label} FAILED: [{obj.get('error_code')}] {obj.get('message')}")
    print(f"  ok  {label}: {obj.get('message', '').splitlines()[0] if obj.get('message') else ''}")
    return obj


def main() -> int:
    work = tempfile.mkdtemp(prefix="pbix_html_demo_")
    pbix_path = os.path.join(work, "html_visual_demo.pbix")
    alias = "htmldemo"

    # ------------------------------------------------------------------ (a)
    # Build a tiny model from scratch: one Sales fact table with a Region
    # dimension column + a couple of measures, then write it to a .pbix.
    print("[a] Building model with PBIXBuilder ...")
    b = PBIXBuilder("HTMLVisualDemo")
    b.add_table(
        "Sales",
        [
            {"name": "OrderID", "data_type": "Int64"},
            {"name": "Region", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
        ],
        rows=[
            {"OrderID": 1, "Region": "North", "Amount": 1200.0},
            {"OrderID": 2, "Region": "South", "Amount": 900.0},
            {"OrderID": 3, "Region": "East", "Amount": 1500.0},
            {"OrderID": 4, "Region": "West", "Amount": 700.0},
            {"OrderID": 5, "Region": "North", "Amount": 800.0},
            {"OrderID": 6, "Region": "East", "Amount": 1100.0},
        ],
    )
    b.add_measure("Sales", "Total Revenue", "SUM(Sales[Amount])",
                  format_string="$#,0")
    b.add_measure("Sales", "Order Count", "COUNTROWS(Sales)")
    saved = b.save(pbix_path)
    print(f"    saved model -> {saved}")

    # ------------------------------------------------------------------ (f)
    # Pure-python template API, used directly (returns an HTML string, no JSON).
    # This is the exact call the turnkey template=/html= paths make internally.
    print("[f] Rendering HTML with pbix_mcp.html_templates.render(...) ...")
    bar_html = html_templates.render("bar_chart", {
        "title": "Revenue by Region",
        "items": [["North", 2000], ["South", 900], ["East", 2600], ["West", 700]],
        "value_suffix": "",
    })
    assert bar_html.startswith("<div") and "<svg" in bar_html, "render() should return HTML"
    # Builder functions are equally importable and also return raw HTML strings:
    pill_html = html_templates.badge("LIVE", color="#2E86DE", filled=True)
    assert pill_html.startswith("<span"), "badge() should return HTML"
    print(f"    render() returned {len(bar_html)} chars of HTML (raw string, not JSON)")

    # ------------------------------------------------------------------ (b)
    print("[b] Opening the .pbix via pbix_open ...")
    call(pbix_open(saved, alias), "pbix_open")

    try:
        # -------------------------------------------------------------- (c)
        print("[c] Adding a report page ...")
        call(pbix_add_page(alias, "HTML Showcase"), "pbix_add_page")
        page = 1  # builder made page 0 ("Page 1"); the new page is index 1

        # -------------------------------------------------------------- (d1)
        # Raw html= string (KPI card built by the pure-python template API).
        print("[d] Adding KPI visual from raw html= ...")
        kpi_html = html_templates.kpi_card(
            title="TOTAL REVENUE",
            value="$6,200",
            subtitle="FY snapshot (static HTML)",
            spark=[3, 5, 4, 8, 6, 9],
        )
        call(pbix_add_html_visual(
            alias, page_index=page, html=kpi_html,
            css="body{margin:0}",              # inlined as a leading <style>
            measure_name="HTML KPI (raw)",
            x=40, y=40, width=360, height=220,
        ), "pbix_add_html_visual(html=...)")

        # -------------------------------------------------------------- (d2)
        # Built-in template= path — renders the same kind of card for you.
        print("[d] Adding KPI visual from template= ...")
        call(pbix_add_html_visual(
            alias, page_index=page,
            template="kpi_card",
            template_spec_json=json.dumps({
                "title": "ORDER COUNT",
                "value": "6",
                "subtitle": "template=kpi_card",
                "accent": "#1B7F5A",
                "spark": [1, 2, 2, 3, 4, 6],
            }),
            measure_name="HTML KPI (template)",
            x=440, y=40, width=360, height=220,
        ), "pbix_add_html_visual(template=...)")

        # -------------------------------------------------------------- (d3)
        # Data-driven dax= path — the measure value IS live HTML, rebuilt on
        # every filter context via FORMAT()/& concatenation + SELECTEDVALUE.
        print("[d] Adding data-driven visual from dax= ...")
        data_dax = (
            '"<div style=\'font-family:Segoe UI,Arial,sans-serif;padding:18px;'
            "border-radius:12px;background:#F6FAFE;box-sizing:border-box;height:100%;'>\""
            ' & "<div style=\'font-size:12px;letter-spacing:.12em;color:#6B7A8D;\'>'
            'TOTAL REVENUE (LIVE DAX)</div>"'
            ' & "<div style=\'font-size:40px;font-weight:800;color:#1B4F8A;line-height:1.1;\'>"'
            ' & FORMAT(SUM(Sales[Amount]), "$#,0") & "</div>"'
            ' & "<div style=\'color:#6B7A8D;font-size:13px;margin-top:6px;\'>Region in focus: "'
            ' & COALESCE(SELECTEDVALUE(Sales[Region]), "All regions") & "</div></div>"'
        )
        call(pbix_add_html_visual(
            alias, page_index=page, dax=data_dax,
            measure_name="HTML Data-Driven",
            x=40, y=290, width=360, height=200,
        ), "pbix_add_html_visual(dax=...)")

        # -------------------------------------------------------------- (e)
        # Cross-filter visual: an SVG whose clickable <g> per region carries
        # data-pbix-select="<region>". Binding category_field="Sales.Region"
        # wires those clicks to a real selection that filters the whole report.
        print("[e] Adding cross-filter visual (category_field=Sales.Region) ...")
        regions = [("North", 2000), ("South", 900), ("East", 2600), ("West", 700)]
        peak = max(v for _, v in regions)
        bars = []
        for i, (name, val) in enumerate(regions):
            y0 = i * 34 + 6
            w = 220 * (val / peak)
            # data-pbix-select tags the identity; the visual turns a click into
            # a selection of Sales[Region] = <name> and cross-filters the report.
            bars.append(
                f"<g data-pbix-select=\"{html_templates.esc(name)}\" style=\"cursor:pointer;\">"
                f"<text x='0' y='{y0 + 15}' font-size='12' fill='#1B2A3A'>{html_templates.esc(name)}</text>"
                f"<rect x='60' y='{y0 + 3}' width='220' height='18' rx='4' fill='#E3ECF5'/>"
                f"<rect x='60' y='{y0 + 3}' width='{w:.1f}' height='18' rx='4' fill='#2E86DE'/>"
                f"</g>")
        cross_html = (
            "<div style=\"font-family:Segoe UI,Arial,sans-serif;padding:14px 16px;"
            "height:100%;box-sizing:border-box;\">"
            "<div style='font-weight:700;color:#1B4F8A;margin-bottom:8px;'>"
            "Revenue by Region (click to cross-filter)</div>"
            f"<svg viewBox='0 0 300 {len(regions) * 34 + 10}' style='width:100%;'>"
            f"{''.join(bars)}</svg></div>")
        call(pbix_add_html_visual(
            alias, page_index=page, html=cross_html,
            category_field="Sales.Region",     # <- makes it a real slicer
            measure_name="HTML Cross-Filter",
            x=440, y=290, width=520, height=220,
        ), "pbix_add_html_visual(category_field=...)")

        # -------------------------------------------------------------- (g)
        print("[g] Listing HTML visuals with pbix_get_html_visual ...")
        got = call(pbix_get_html_visual(alias), "pbix_get_html_visual")
        data = got.get("data") or {}
        print(f"    count = {data.get('count')}")
        for v in data.get("visuals", []):
            kind = "data-driven (dax)" if v["data_driven"] else "static html"
            html_len = "n/a" if v["html"] is None else f"{len(v['html'])} chars"
            print(f"      page {v['page_index']} / visual {v['visual_index']}: "
                  f"measure='{v['measure_name']}' on [{v['measure_table']}] "
                  f"[{kind}, html={html_len}]")

        # -------------------------------------------------------------- (h)
        print("[h] Editing one visual with pbix_set_html_visual ...")
        edited_html = html_templates.render("kpi_card", {
            "title": "TOTAL REVENUE",
            "value": "$6,200",
            "subtitle": "edited via pbix_set_html_visual",
            "accent": "#8E44AD",
            "spark": [2, 4, 3, 7, 6, 9, 8],
        })
        call(pbix_set_html_visual(
            alias, measure_name="HTML KPI (raw)", html=edited_html,
        ), "pbix_set_html_visual")

        # Confirm the edit landed.
        recheck = call(pbix_get_html_visual(alias, page_index=page),
                       "pbix_get_html_visual (recheck)")
        for v in (recheck.get("data") or {}).get("visuals", []):
            if v["measure_name"] == "HTML KPI (raw)":
                assert v["html"] and "edited via pbix_set_html_visual" in v["html"], \
                    "edit did not take effect"
                print("    verified: edited content is present in the measure")

        # -------------------------------------------------------------- (i)
        print("[i] Saving ...")
        out_path = os.path.join(work, "html_visual_demo_out.pbix")
        call(pbix_save(alias, output_path=out_path, overwrite=True),
             "pbix_save")
        print(f"    saved -> {out_path}  ({os.path.getsize(out_path):,} bytes)")
    finally:
        # Always release the work_dir / alias.
        call(pbix_close(alias, force=True), "pbix_close")

    print("\nSUCCESS: pure-Python HTML-visual flow completed with 4 HTML visuals "
          "(raw html, template, data-driven dax, cross-filter).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
