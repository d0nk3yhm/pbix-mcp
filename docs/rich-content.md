# Rich content that renders in the Power BI service

How to author dynamic, designed content that renders **identically in the
Power BI service** (app.powerbi.com) on any tenant — no tenant-admin favors,
no uncertified visuals. Two portable rails, both service-verified:

- **Rail A** — native visuals + DAX→SVG data-URI image measures + state
  tables/slicers/field parameters. Portable *everywhere*: service, Desktop,
  PDF export, subscriptions, certified-only tenants.
- **Rail B** — [Deneb](https://github.com/deneb-viz/deneb) (MIT, certified
  AppSource visual) interpreting declarative Vega-Lite/Vega JSON stored in
  the visual's config. Certified visuals auto-load from AppSource for report
  consumers.

The embedded HTML visual (`docs/html-visuals.md`) remains the only path for
arbitrary HTML/JS — but it is **Desktop-only**: uncertified file-embedded
visuals import into the service and render **empty** under default tenant
trust policy. Use it deliberately, never as the default.

> Service verification (2026-07-22): a report authored entirely through
> pbix-mcp with the exact shapes below — a `publicCustomVisuals` Deneb
> reference, `objects.vega` string-Literal spec, dataset-role binding, and an
> SVG data-URI measure with `DataCategory='ImageUrl'` in a native table — was
> uploaded to app.powerbi.com (certified-only tenant policy) and rendered
> correctly: the service auto-loaded Deneb from AppSource and drew the
> Vega-Lite chart, and the native table rendered the SVG image measure.

## Rail B — referencing Deneb (or any certified AppSource visual)

Registration is one GUID string — zero file parts:

```python
pbix_reference_public_visual(alias, "deneb7E15AEF80B9E4D4F8E12924291ECE89A")
```

This appends the GUID to the Layout's top-level `publicCustomVisuals` array
(deduped; created when missing). Nothing is extracted into
`Report/CustomVisuals/`, `[Content_Types].xml` is untouched, and
`resourcePackages` is never involved (that array is images/themes only).
De-register with `pbix_remove_custom_visual(alias, guid)` — its folder branch
is a no-op for reference-only registrations.

Place the visual with `pbix_add_visual(alias, page, visual_type="<guid>")`
and a `config_json` carrying:

1. **The spec** in `singleVisual.objects.vega` — string Literals (note the
   DAX-style `'…'` wrapping with embedded single quotes doubled):

```jsonc
"objects": {
  "vega": [{ "properties": {
    "provider":   { "expr": { "Literal": { "Value": "'vegaLite'" } } },
    "version":    { "expr": { "Literal": { "Value": "'6.4.1'" } } },
    "jsonSpec":   { "expr": { "Literal": { "Value": "'<spec JSON, single quotes doubled>'" } } },
    "jsonConfig": { "expr": { "Literal": { "Value": "'{}'" } } }
  }}]
}
```

2. **The data binding**: Deneb has a single data role `dataset` — project
   every bound field into it and consume the named dataset in the spec
   (`"data": {"name": "dataset"}` for Vega-Lite).

Deneb's dataset contract for generated specs: field names are Power BI
display names with `\ " . [ ]` each replaced by `_`; never generate names
colliding with Deneb's reserved fields (`__row__`, `__selected__`,
`__drill__`, `__drill_flat__`, and per-measure `__highlight*` /
`__format*` suffixes); certified 1.9.1.0 windows the dataset at 10,000 rows;
the certified build has **no WebAccess** — no `data.url`, no remote images.

## Rail A — SVG image measures (`DataCategory = 'ImageUrl'`)

A measure returning `data:image/svg+xml;utf8,<svg …>` renders as a live
vector image in table/matrix cells once its DataCategory is `ImageUrl`.
Because the SVG is computed by DAX it re-renders under any filter context.

Authoring surface:

- `pbix_svg_measure(kind, spec_json, alias=…, measure_name=…)` — generates
  hygiene-hardened DAX from templates (`data_bar`, `bullet`, `pill`,
  `icon_updown`, `sparkline`) and, in turnkey mode, authors the measure with
  `DataCategory='ImageUrl'` in one call. Call with no `kind` for the catalog.
- `pbix_datamodel_add_measure(..., data_category="ImageUrl")` /
  `pbix_datamodel_modify_measure(..., new_data_category="ImageUrl")` for
  hand-written DAX.
- `pbix_set_table_data` accepts a per-column `"data_category"` key for
  static image columns.

DataCategory values survive rebuild-based edits (set_table_data,
add_relationship, …) — the rebuild carries them through.

Hand-written SVG measure rules (the templates already obey all of these):

- **utf8, never base64** — base64 wastes ~33% of the budget.
- **Stay under ~32,000 characters** (the Analysis Services text cap;
  externally documented as 32,766 — pbix-mcp enforces a conservative 32,000).
- **Percent-encode `#`** (`#2E86DE` → `%232E86DE`) — a raw `#` starts the
  URI fragment and truncates the image.
- **Single-quote SVG attributes** to avoid doubling every `"` in the DAX
  literal.
- **Locale-proof numeric interpolation**: `FORMAT(INT(x), "0")` — plain
  `FORMAT(x, "0.#")` emits a comma decimal under many locales and corrupts
  coordinates.

## Images and logos

`pbix_add_image` registers the bytes as a report resource and places a
Desktop-exact image visual in one call:

```python
pbix_add_image(alias, page_index=0, image_path="/path/logo.png",
               x=40, y=40, width=200, height=80, scaling="Fit")
```

Callers holding bytes (an upload, a data URI) pass `image_base64` instead —
the engine never fetches remote URLs itself. `pbix_register_resource`
registers a resource without placing a visual (images, shape maps, themes),
and `pbix_set_image` repoints or restyles an existing image visual (new
bytes, an already-registered `item_name`, and/or `scaling`).

Registration always covers the three touchpoints Desktop uses — the bytes
under `Report/StaticResources/RegisteredResources/`, the
`[Content_Types].xml` extension Default, and the `resourcePackages` item —
and the file type comes from magic bytes (PNG/JPEG/GIF/WebP/SVG), never from
the filename. Images render everywhere: Desktop, the service, PDF export,
subscriptions. Parameter contracts: [tool-contracts.md](tool-contracts.md#image--resource-tools).

## Rail A — field parameters

`pbix_datamodel_add_field_parameter(alias, name, fields_json)` authors the
complete Desktop shape (diffed against Desktop-authored ground truth):
calculated partition holding the `{("Display", NAMEOF('T'[Field]), n), …}`
tuple set with full static VertiPaq storage, the `ParameterMetadata`
ExtendedProperty on the hidden Fields column, display column sorted by the
hidden Order column, and the display→Fields group-by wiring — so Desktop and
the service treat it as a real field parameter (field-swapping in visuals).

```python
pbix_datamodel_add_field_parameter(alias, "Metric Selector", json.dumps([
    {"display": "Revenue", "ref": "Sales[Total Revenue]"},
    {"display": "Units",   "ref": "Sales[Total Units]"},
]))
```

Refs accept `Table[Field]` / `'Table'[Field]` (column or measure) and are
validated against the model. Field parameters survive rebuild-based edits
(the rebuild recognizes the shape and re-stamps it) and multiple parameters
coexist. The manual alternative — a disconnected table via
`pbix_set_table_data` + a `SWITCH(SELECTEDVALUE(…))` measure — still works
and also survives rebuilds.

## Reading Desktop rich content back

`read_table_from_abf` (and everything built on it: `pbix_get_table_data`,
the DAX engine, rebuild preservation) reads Desktop calc-table columns
(Type=4) — including Desktop-authored field parameters — with types resolved
via `InferredDataType` when `ExplicitDataType` is "automatic".

## Choosing a rail

| Content | Rail | Tool |
| --- | --- | --- |
| KPI bars/bullets/pills/sparklines in tables | A | `pbix_svg_measure` |
| Arbitrary charts beyond native visuals | B | `pbix_reference_public_visual` + Deneb spec |
| Field/measure switching via slicer | A | `pbix_datamodel_add_field_parameter` |
| Clickable links in cells | A | `data_category="WebUrl"` |
| Logos / static images | A | `pbix_add_image` |
| Arbitrary HTML/JS (Desktop-only, opt-in) | — | `pbix_add_html_visual` |
