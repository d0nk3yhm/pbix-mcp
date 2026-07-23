# Custom HTML / CSS / SVG visuals

> **Desktop-only.** The bundled HTML visual is intentionally uncertified, and
> the Power BI **service** blocks uncertified file-embedded visuals under
> default tenant trust policy — a report that looks perfect in Desktop ships
> **blank** to app.powerbi.com. Use HTML visuals deliberately; for content
> that must render in the service, use the portable rails in
> [rich-content.md](rich-content.md) (native visuals + SVG data-URI image
> measures, and certified Deneb references).

pbix-mcp ships **its own** Power BI custom visual — `PBIX HTML` — that renders an
HTML / CSS / SVG (and inline `<script>`) string produced by a DAX measure. That
lets you build anything HTML can express — KPI cards, SVG charts / gauges / maps,
badges, custom tables, callouts — as first-class report visuals, and (optionally)
have them **cross-filter the rest of the report** like a native visual.

- The visual is bundled at `src/pbix_mcp/assets/pbix_html_visual/` (~3.8 KB, no
  third-party dependency), GUID `pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7`.
- It renders in its **own DOM** (not a sandboxed iframe), which is why it can wire
  real selection / cross-filtering.
- **Desktop-verified** in real Power BI Desktop.

Everything here works from an MCP client **and** from plain Python — see
[Pure-Python usage](#pure-python-usage). The OpenBI runtime uses the pure-Python
path.

---

## Quick start

```python
import json
from pbix_mcp import server as pbix

pbix.pbix_open(r"C:\reports\sales.pbix", "rep")
pbix.pbix_add_page("rep", "Highlights")           # page index 0 if it's the first

# A gradient KPI card with an SVG sparkline (via the template library)
res = json.loads(pbix.pbix_add_html_visual(
    "rep", page_index=0, template="kpi_card",
    template_spec_json=json.dumps({
        "title": "REVENUE (YTD)", "value": "1.24M", "subtitle": "+8.1% vs LY",
        "spark": [3, 5, 4, 8, 6, 9, 7, 11],
    }),
    x=40, y=40, width=360, height=210, measure_name="Revenue KPI"))
assert res["success"], res

pbix.pbix_save("rep")
pbix.pbix_close("rep")
```

Open the file in Power BI Desktop — the card renders.

---

## The four tools

Every tool returns a **JSON string** (`ToolResponse.to_text()`); parse it with
`json.loads`. Success is in `["success"]`; structured payloads are in `["data"]`.

### `pbix_add_html_visual` — create (turnkey)

```
pbix_add_html_visual(
    alias, page_index=0,
    html="", dax="", template="", template_spec_json="",   # pick ONE content source
    x=40, y=40, width=480, height=320,
    measure_name="", measure_table="", css="",
    category_field="",                                       # cross-filter (optional)
    pbiviz_path="",                                          # your own .pbiviz (optional)
) -> json
```

One call: embeds the bundled visual, authors a DAX measure whose **string value is
the HTML**, and places a fully data-bound container (the content measure binds as
`String`). Provide **exactly one** content source:

| Source | Use it for |
| --- | --- |
| `html` | A raw HTML/CSS/SVG string. Double-quotes are escaped into the DAX literal for you — write normal `class="x"` attributes. |
| `css` | Optional; inlined as a leading `<style>…</style>` block when used with `html`. |
| `dax` | A full DAX **string** expression, for **data-driven** HTML (e.g. `"<b>" & FORMAT([Total],"#,0") & "</b>"`, `SELECTEDVALUE(T[C],"—")`). |
| `template` + `template_spec_json` | A built-in template rendered into `html` for you (see [Templates](#template-library)). |

`measure_name` defaults to `"HTML Visual N"`; `measure_table` defaults to the first
model table (the measure just needs a home).

### `pbix_get_html_visual` — view

```
pbix_get_html_visual(alias, page_index=-1) -> json   # -1 = all pages
```

`data = {count, visuals: [{page_index, visual_index, position, measure_table,
measure_name, dax_expression, html, data_driven}]}`. `html` is the decoded HTML for
a plain-literal measure, or `null` when the content is a data-driven DAX expression
(`data_driven: true`).

### `pbix_set_html_visual` — edit

```
pbix_set_html_visual(alias, page_index=0, visual_index=-1,
                     html="", dax="", css="", measure_name="") -> json
```

Locate the visual by `page_index` + `visual_index` (as reported by
`pbix_get_html_visual`) or by the `measure_name` it is bound to, then replace its
content (the container / position / binding are untouched — only the measure's DAX
changes).

### `pbix_html_template` — professional snippets

```
pbix_html_template(kind="", spec_json="") -> json    # no kind => catalog in data.templates
```

Renders an escaping-safe HTML/SVG snippet (`data.html`) you can pass to
`pbix_add_html_visual(html=…)`, or use `pbix_add_html_visual(template=…,
template_spec_json=…)` to do both at once. See [Templates](#template-library).

### Embedding any third-party `.pbiviz`

`pbix_add_custom_visual(alias, pbiviz_path)` embeds **any** custom visual: it reads
the GUID from the package manifest, extracts the `.pbiviz` verbatim into
`Report/CustomVisuals/<guid>/`, and registers the GUID in the top-level
`Layout["publicCustomVisuals"]` array — exactly how Power BI Desktop embeds a
custom visual. Place it with `pbix_add_visual(..., visual_type="<guid>")`.
`pbix_remove_custom_visual(alias, guid)` removes the files and de-registers it.

---

## Authoring the HTML / DAX measure

The content is a DAX measure that returns a string. Rules:

- **Inline everything.** The visual sandbox blocks external requests — no
  `<script src>`, `<link>`, remote `<img>`/font, or `@import`. Embed images as
  base64 `data:` URIs and use system fonts (`Segoe UI, Arial`).
- **Inline CSS** in a `<style>` block in the same string (or pass `css=`).
- **Keep it under ~32,000 characters** — Analysis Services silently truncates a
  longer text cell. `pbix_add_html_visual` raises before you cross the line.
- **Static HTML** → pass `html=`; it's wrapped as a DAX literal (`"…"` with `""`
  escaping) automatically.
- **Data-driven HTML** → pass `dax=` and build the string in DAX:
  ```
  "<div class='card'><span>" & FORMAT([Total Sales], "$#,0") & "</span>"
  & "<small>" & SELECTEDVALUE(Region[Name], "All regions") & "</small></div>"
  ```
  In a DAX string literal use single-quoted HTML attributes (`class='card'`), or
  double every `"`.
- Inline `<script>` runs (the visual re-executes it on each update), so JS-driven
  graphs work — but still no external script/network access.

---

## Cross-filtering (make clicks filter the report)

Two directions:

- **Others → the HTML visual — already automatic.** The content is a live DAX
  measure, so when a slicer or another visual cross-filters the page, the measure
  re-evaluates under the new filter context and the HTML re-renders.
- **The HTML visual → others — opt in with `category_field`.** Bind a column and
  tag clickable elements; clicking one filters everything bound to that field.

### How to wire it

1. Pass `category_field="Table[Column]"` (or `Table.Column`, or a unique bare
   `Column`). This binds the column so Power BI hands the visual one **selection
   identity per distinct value**.
2. In your HTML/SVG, put `data-pbix-select="<the category value>"` on each element
   that should be clickable. The attribute value must equal a value of the bound
   column.

Clicking a tagged element:

- **filters / cross-highlights every other visual** bound to that field (and, via
  model relationships, related tables) — exactly like clicking a native bar;
- **Ctrl/Cmd-click** multi-selects, **background click** clears, **right-click**
  opens the report context menu;
- unselected tagged elements **dim** to 35 % opacity for feedback.

### Example — a clickable SVG "map" that filters the page

```python
import json
from pbix_mcp import server as pbix

# model: Ports[Code] in {"S","C","Q"} relates to the fact table's Embarked column
svg = """
<div style="font-family:Segoe UI;padding:8px;height:100%;box-sizing:border-box;">
  <div style="font-weight:700;color:#1B4F8A;">Embarkation port — click to filter</div>
  <svg viewBox="0 0 480 250" style="width:100%;height:88%;">
    <rect x="0" y="0" width="480" height="250" rx="10" fill="#EAF3FB"/>
    <g data-pbix-select="S" style="cursor:pointer;">
      <circle cx="110" cy="150" r="40" fill="#2E86DE"/>
      <text x="110" y="156" text-anchor="middle" fill="#fff" font-weight="800">S</text>
    </g>
    <g data-pbix-select="C" style="cursor:pointer;">
      <circle cx="260" cy="120" r="34" fill="#48A0F0"/>
      <text x="260" y="126" text-anchor="middle" fill="#fff" font-weight="800">C</text>
    </g>
    <g data-pbix-select="Q" style="cursor:pointer;">
      <circle cx="390" cy="150" r="28" fill="#20C997"/>
      <text x="390" y="155" text-anchor="middle" fill="#fff" font-weight="800">Q</text>
    </g>
  </svg>
</div>"""

json.loads(pbix.pbix_add_html_visual(
    "rep", page_index=1, html=svg,
    x=766, y=404, width=490, height=292,
    measure_name="Embark Map", category_field="Ports[Code]"))
```

Clicking the **S** circle filters the page to Southampton; **S** stays bright, **C**
and **Q** dim.

**Cohesive SVG vs per-category fragments.** When your `html` is a single full
SVG/HTML string tagged with `data-pbix-select` (the map above), the visual renders
it **once**. If instead your `dax` measure returns a *different* fragment per
category value (each carrying its own `data-pbix-select`), the visual concatenates
them — handy for lists / bar charts where every row is one clickable category.

---

## Template library

`pbix_mcp.html_templates` builds professional, **HTML-escaped** snippets (safe to
interpolate user text). Use them via `pbix_html_template` / the `template=` argument,
or import and call directly (they return plain HTML strings, not JSON).

| kind | spec keys |
| --- | --- |
| `kpi_card` | `title, value, subtitle?, accent?, spark?[numbers]` |
| `bar_chart` | `title, items:[[label,value],…], accent?, value_suffix?` |
| `gauge` | `title, percent, accent?, center_label?` |
| `table` | `headers:[…], rows:[[…],…], accent?, align_right_from?` |
| `progress` | `title, items:[[label,percent],…], accent?` |
| `badge` | `text, color?, filled?` |

```python
from pbix_mcp import html_templates as ht
html = ht.render("bar_chart", {"title": "By region",
                               "items": [["North", 63], ["South", 47], ["East", 24]]})
# -> a self-contained <div>…<svg>…</svg></div> string, user text escaped
```

---

## Pure-Python usage

Every tool is an ordinary importable function — no MCP client required. This is the
path OpenBI-style code uses.

```python
import json
from pbix_mcp import server as pbix          # the tool functions
from pbix_mcp import html_templates as ht     # object-returning template builders
from pbix_mcp.builder import PBIXBuilder       # build a model from scratch

def ok(resp):                                  # tiny helper: raise on failure
    r = json.loads(resp)
    if not r.get("success"):
        raise RuntimeError(r.get("message") or r.get("error_code"))
    return r

# 1. build + open a model
b = PBIXBuilder("Demo")
b.add_table("Sales", [{"name": "Region", "data_type": "String"},
                      {"name": "Amt", "data_type": "Int64"}],
            rows=[{"Region": "North", "Amt": 10}, {"Region": "South", "Amt": 20}])
b.save("demo.pbix")
ok(pbix.pbix_open("demo.pbix", "d"))
ok(pbix.pbix_add_page("d", "Page 1"))

# 2. add HTML visuals (html / template / dax / cross-filter) — all pure python
ok(pbix.pbix_add_html_visual("d", 0, html=ht.render("badge", {"text": "LIVE"}),
                             measure_name="Badge"))
ok(pbix.pbix_add_html_visual("d", 0, template="gauge",
                             template_spec_json=json.dumps({"title": "SLA", "percent": 88}),
                             measure_name="SLA", x=40, y=250))
ok(pbix.pbix_add_html_visual(
    "d", 0, dax='"<b>" & FORMAT(COUNTROWS(Sales), "#,0") & " rows</b>"',
    measure_name="Row Count", x=40, y=470))

# 3. view / edit / save
data = ok(pbix.pbix_get_html_visual("d", 0))["data"]
print(data["count"], "HTML visuals")
ok(pbix.pbix_save("d"))
ok(pbix.pbix_close("d"))
```

A runnable version is at [`examples/html_visual_pure_python.py`](../examples/html_visual_pure_python.py).

Notes for pure-Python callers:

- Server functions return JSON strings — wrap them (`ok()` above) to get objects /
  raise on failure.
- `html_templates` functions return HTML **strings** and raise `ValueError` on bad
  input — no JSON envelope.
- The bundled `.pbiviz` ships inside the wheel, so `pbix_add_html_visual` works with
  no extra setup.

---

## Limits & notes

- No external network/resources (sandbox) — inline everything, base64 images only.
- Content measure < ~32,000 characters.
- Legacy `Report/Layout` PBIX only (the PBIR `Report/definition` format isn't
  supported for embedding yet).
- `category_field` cross-filtering needs the bound column to be related (through the
  model) to the visuals you want to filter — same as any native visual.

## Rebuilding the bundled visual

Source is under `src/pbix_mcp/assets/pbix_html_visual/visual_src/`
(`visual.ts`, `capabilities.json`, `pbiviz.json`). To rebuild, scaffold a
powerbi-visuals-tools project, drop those files in, and
`npx --yes powerbi-visuals-tools@latest package` (needs Node + the
`powerbi-visuals-api` matching `pbiviz.json`'s `apiVersion`). Keep the GUID
unchanged; bump `version`. Replace the `.pbiviz` under `assets/pbix_html_visual/`.
