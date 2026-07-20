# Tool Contracts

## Response Format

Every MCP tool returns a JSON string via `ToolResponse.to_text()`:

```json
{
  "success": true,
  "message": "Opened 'report.pbix' as 'report'",
  "data": null,
  "warnings": []
}
```

Error responses include a stable error code:

```json
{
  "success": false,
  "error_code": "PBIX_INVALID",
  "message": "File not found: /path/to/missing.pbix",
  "warnings": []
}
```

DAX evaluation returns extended results:

```json
{
  "success": true,
  "results": [
    {"name": "Total Sales", "value": 470533.0, "status": "ok"},
    {"name": "Unknown Metric", "value": null, "status": "unsupported", "error_message": "Uses unsupported function(s): MEDIANX"}
  ],
  "warnings": ["1 unsupported DAX function(s): MEDIANX"]
}
```

## Error Codes

| Code | Exception Class | Description |
|------|----------------|-------------|
| `PBIX_INVALID` | `InvalidPBIXError` | File is not a valid PBIX/PBIT |
| `FORMAT_UNSUPPORTED` | `UnsupportedFormatError` | Unsupported file format variant |
| `LAYOUT_JSON_INVALID` | `LayoutParseError` | Report layout JSON could not be parsed |
| `DATAMODEL_DECOMPRESS_FAILED` | `DataModelCompressionError` | XPress9 decompression failed |
| `ABF_REBUILD_FAILED` | `ABFRebuildError` | ABF archive operation failed |
| `METADATA_SQL_FAILED` | `MetadataSQLError` | SQLite metadata operation failed |
| `DAX_UNSUPPORTED_FUNCTION` | `DAXUnsupportedError` | DAX function not implemented |
| `DAX_EVAL_FAILED` | `DAXEvaluationError` | DAX evaluation failed at runtime |
| `DAX_PARSE_FAILED` | `DAXParseError` | DAX expression parse failure |
| `UNSAFE_WRITE` | `UnsafeWriteError` | Destructive write without confirmation |
| `SESSION_ERROR` | `SessionError` | File session error |
| `FILE_NOT_OPEN` | `FileNotOpenError` | Requested alias not open |
| `FILE_ALREADY_OPEN` | `FileAlreadyOpenError` | File/alias already open |

## Safety Defaults

| Tool | Parameter | Default | Behavior |
|------|-----------|---------|----------|
| `pbix_save` | `overwrite` | `False` | Refuses to overwrite existing files unless explicit |
| `pbix_save` | `backup` | `True` | Creates .bak backup before overwriting |
| `pbix_save` | `strip_sensitivity_label` | `False` | Removes MSIP sensitivity labels when True |
| `pbix_close` | `force` | `False` | Refuses to close with unsaved changes |

## Tool Categories (105 tools)

### Create & File Management (5)
`pbix_create` · `pbix_open` · `pbix_save` · `pbix_close` · `pbix_list_open`

### Report Layout & Visuals (21)
Visual CRUD, page management, filters, positions, bookmarks (add/remove), settings, layout read/write, default filter extraction.

### DAX Engine (4)
Measure evaluation, per-dimension evaluation, calculated columns, cache management.

### DataModel Read (16)
Schema, measures, relationships, Power Query, columns, table data, data sources, metadata, CSV export (single/all), value search, SQL-like query, table profiling, data diff.

### DataModel Write (21)
Metadata SQL read/write, measure CRUD, column modification, relationship CRUD, table removal, field parameters, calculation groups, TMDL export, PBIP export, decompress/recompress, ABF file ops, table data write, value replace.

### Resources, Themes & Custom Visuals (13)
Static resources, theme read/write, color extraction/recolor, linguistic schema, custom visual import/remove (GUID embedded into `Report/CustomVisuals/` + `publicCustomVisuals`), and turnkey HTML/CSS/SVG visual authoring — create/view/edit plus a template renderer. Detailed contracts in [Custom Visual & HTML Tools](#custom-visual--html-tools).

### DataMashup (2)
M code read/write.

### Row-Level Security (3)
RLS role CRUD, filter expression evaluation against data.

### Perspectives (3)
Create/list/remove perspectives for filtered model views.

### User Hierarchies (3)
Create/list/remove drill-down hierarchies (e.g. Country > State > City).

### Cultures & Translations (4)
Add cultures, translate table/column/measure names, list/remove cultures.

### Partition Management (3)
List/remove M partitions. `pbix_add_partition` blocked for PBIX (needs PartitionStorage), works for PBIP/TMDL export.

### Incremental Refresh (2)
Get/set incremental refresh policies. `pbix_set_incremental_refresh` works for files with a data source (source_csv/source_db); embedded-only files return a clear error (same as PBI Desktop).

### Diagnostics & Security (5)
17-point diagnostic (`pbix_doctor`), report documentation (`pbix_document`), file diff (`pbix_diff`), performance analysis (`pbix_performance`), password extraction (`pbix_get_password`).

## Custom Visual & HTML Tools

Detailed contracts for the custom-visual embedding tools and the turnkey HTML / CSS / SVG visual authoring tools (0.9.23), built on the bundled **PBIX HTML** custom visual (GUID `pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7`, shipped in `src/pbix_mcp/assets/pbix_html_visual/`).

Embedding follows Power BI Desktop exactly: the `.pbiviz` is extracted verbatim into `Report/CustomVisuals/<guid>/` and the GUID is registered in the top-level `Layout["publicCustomVisuals"]` array. The GUID is always read from the `.pbiviz` manifest — never fabricated. Only the legacy `Report/Layout` format is supported; PBIR (`Report/definition`) returns `LAYOUT_JSON_INVALID`.

### `pbix_add_custom_visual`

Embed any `.pbiviz` package into the report and register its GUID.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `pbiviz_path` | str | required | Absolute path to the `.pbiviz` file |

Reads the GUID from the package manifest, extracts the package into `Report/CustomVisuals/<guid>/`, and appends the GUID to `Layout["publicCustomVisuals"]` (deduped, idempotent on re-import). Place the visual on a page with `pbix_add_visual(..., visual_type="<guid>")`. **Returns** a message-only envelope (`data: null`). **Errors:** `LAYOUT_JSON_INVALID` (no legacy layout / PBIR, invalid manifest GUID, or extraction failure), `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "Custom visual 'PBIX HTML' imported successfully!\n  GUID: pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7\n  Version: 1.1.0.0  (apiVersion 5.11.0)\n  Files: 3 extracted to Report/CustomVisuals/pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7/\n  Registered in publicCustomVisuals.",
  "data": null,
  "warnings": []
}
```

### `pbix_remove_custom_visual`

Remove an embedded custom visual by GUID.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `visual_name` | str | required | The visual GUID (the `Report/CustomVisuals/` folder name / `publicCustomVisuals` entry) |

Deletes `Report/CustomVisuals/<guid>/`, removes the GUID from `publicCustomVisuals`, and strips any legacy `resourcePackages` entry keyed on the name. **Returns** a message-only envelope. **Errors:** `LAYOUT_JSON_INVALID`, `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "Custom visual 'pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7' removed from report (files + publicCustomVisuals registration).",
  "data": null,
  "warnings": []
}
```

### `pbix_add_html_visual`

Turnkey: embed the bundled PBIX HTML visual (or your own `pbiviz_path`), author a DAX measure whose string value **is** the HTML, and place a fully `String`-bound visual container that renders it — all in one call.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `page_index` | int | `0` | Zero-based page to place the visual on |
| `html` | str | `""` | Raw HTML / CSS / SVG string (double-quotes escaped for you). Mutually exclusive with `dax` / `template` |
| `dax` | str | `""` | Full DAX string expression producing the HTML (data-driven content via `FORMAT()` / `&` concat and `SELECTEDVALUE`) |
| `x` | float | `40` | X position in report px |
| `y` | float | `40` | Y position in report px |
| `width` | float | `480` | Width in report px |
| `height` | float | `320` | Height in report px |
| `measure_name` | str | `""` | Name for the content measure (auto-named `HTML Visual N` if empty) |
| `measure_table` | str | `""` | Table to hold the measure (first model table if empty) |
| `css` | str | `""` | CSS inlined as a leading `<style>` block (used with `html`) |
| `pbiviz_path` | str | `""` | Path to your own HTML-rendering `.pbiviz` to embed instead of the bundled one |
| `template` | str | `""` | Built-in template name rendered into `html` for you (see `pbix_html_template`) |
| `template_spec_json` | str | `""` | JSON spec for `template` |
| `category_field` | str | `""` | Column that makes the visual cross-filter the report (see below) |

Provide **exactly one** content source: `html` (or `template`) **or** `dax`. Inline all CSS (`<style>`) and assets (base64 `data:` URIs — external URLs are blocked by the visual sandbox); keep the rendered HTML under ~32000 chars or it is rejected. **Returns** a message-only envelope. **Errors:** `LAYOUT_JSON_INVALID` (no legacy layout, `page_index` out of range, both/neither content source, HTML too long, invalid `template` / `template_spec_json`), `MEASURE_ADD_FAILED` (content measure could not be authored), `FILE_NOT_OPEN`.

**Cross-filtering (`category_field`).** Pass a column as `Table[Column]`, `Table.Column`, or bare `Column` to bind a `category` role, turning the visual into an interactive slicer like a native visual. Tag clickable elements in your HTML/SVG with `data-pbix-select="<category value>"`; clicking one selects that value's identity and filters every other visual bound to the same field. Ctrl/Cmd-click multi-selects, a background click clears the selection, right-click opens the context menu, and unselected regions dim.

```html
<svg viewBox="0 0 200 60">
  <rect data-pbix-select="East" x="0"   y="0" width="90" height="60" fill="#4C78A8"/>
  <rect data-pbix-select="West" x="100" y="0" width="90" height="60" fill="#F58518"/>
</svg>
```

```json
{
  "success": true,
  "message": "HTML visual placed on 'Overview' (visual index 3).\n  Custom visual: PBIX HTML (pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7)\n  Content measure: 'HTML Visual 1' on table 'Sales'\n  Position: (40,40) 480x320\n  View with pbix_get_html_visual; edit with pbix_set_html_visual.",
  "data": null,
  "warnings": []
}
```

### `pbix_get_html_visual`

List the PBIX HTML visuals in the report with their position, bound content measure, and decoded HTML.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `page_index` | int | `-1` | Restrict to one zero-based page (`-1` = all pages) |

**Returns** `data = {count, visuals: [...]}`. Each `visuals` entry has: `page_index`, `visual_index`, `position` (`{x, y, width, height}`), `measure_table`, `measure_name`, `dax_expression` (the raw measure expression), `html` (the decoded HTML for a plain string-literal measure, else `null`), and `data_driven` (`true` when the measure is a DAX expression that can't be losslessly decoded to plain HTML). **Errors:** `LAYOUT_JSON_INVALID`, `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "1 HTML visual(s) in the report.",
  "data": {
    "count": 1,
    "visuals": [
      {
        "page_index": 0,
        "visual_index": 3,
        "position": {"x": 40, "y": 40, "width": 480, "height": 320},
        "measure_table": "Sales",
        "measure_name": "HTML Visual 1",
        "dax_expression": "\"<style>.k{font:600 28px system-ui}</style><div class='k'>1.2M</div>\"",
        "html": "<style>.k{font:600 28px system-ui}</style><div class='k'>1.2M</div>",
        "data_driven": false
      }
    ]
  },
  "warnings": []
}
```

### `pbix_set_html_visual`

Edit an existing HTML visual's content by updating its bound DAX measure. The container (position, size, binding) is untouched.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `page_index` | int | `0` | Page of the target visual (used with `visual_index`) |
| `visual_index` | int | `-1` | Visual index on the page (`-1` = first HTML visual on the page, or the one matching `measure_name`) |
| `html` | str | `""` | New raw HTML / CSS / SVG (mutually exclusive with `dax`) |
| `dax` | str | `""` | New full DAX string expression (for data injection) |
| `css` | str | `""` | CSS inlined as a leading `<style>` block (used with `html`) |
| `measure_name` | str | `""` | Target by bound measure name instead of `page_index` / `visual_index` |

Provide **exactly one** of `html` or `dax`. **Returns** a message-only envelope. **Errors:** `LAYOUT_JSON_INVALID` (no legacy layout, both/neither content source, HTML too long), `HTML_VISUAL_NOT_FOUND` (no visual matched), `MEASURE_MODIFY_FAILED`, `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "HTML visual content updated (measure 'HTML Visual 1' on table 'Sales').",
  "data": null,
  "warnings": []
}
```

### `pbix_html_template`

Render a professional, HTML-escaped snippet for use as HTML-visual content. Call with no `kind` to list the catalog; otherwise the ready HTML is returned in `data.html`.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `kind` | str | `""` | Template name (empty = list the catalog) |
| `spec_json` | str | `""` | JSON object with the template's parameters |

Templates and their spec keys (`?` = optional):

- `kpi_card` — `{title, value, subtitle?, accent?, spark?[numbers]}`
- `bar_chart` — `{title, items:[[label,value],...], accent?, value_suffix?}`
- `gauge` — `{title, percent, accent?, center_label?}`
- `table` — `{headers:[...], rows:[[...],...], accent?, align_right_from?}`
- `progress` — `{title, items:[[label,percent],...], accent?}`
- `badge` — `{text, color?, filled?}`

With no `kind`, `data = {templates: {...}}` maps each name to its accepted spec keys. With a `kind`, `data = {html: "<...>"}`. Pass the result to `pbix_add_html_visual(html=...)`, or skip this call and use `pbix_add_html_visual(template=..., template_spec_json=...)` to render and place in one step. **Errors:** `BAD_SPEC` (invalid or non-object `spec_json`), `BAD_TEMPLATE` (unknown `kind`, or a spec key the template rejects), `INTERNAL_ERROR`.

```json
{
  "success": true,
  "message": "Rendered 'kpi_card' (612 chars).",
  "data": {
    "html": "<div style=\"font:600 13px system-ui;color:#64748B\">Revenue</div>..."
  },
  "warnings": []
}
```
