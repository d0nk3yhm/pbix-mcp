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
| `DAX_MEASURE_NOT_FOUND` | `DAXMeasureNotFoundError` | Requested measure does not exist in the model |
| `DIMENSION_INVALID` | `DimensionParseError` | Dimension reference is not in `Table.Column` format |
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

## Tool Categories (131 tools)

### Create & File Management (7)
`pbix_create` · `pbix_open` · `pbix_open_pbip` · `pbix_save` · `pbix_close` · `pbix_list_open` · `pbix_report_format`

**PBIP projects** — `pbix_open_pbip` opens a PBIP project folder (TMDL model half + report half, whether the report is this project's `report.json` or a Desktop-authored PBIR `definition/` tree) as a live session that every other tool operates on. `pbix_save` with no `output_path` writes edits back into the project folder (TMDL re-exported into `<name>.SemanticModel/definition/`, report half mirrored back); `pbix_save` with a `.pbix` `output_path` converts the project to a PBIX. The model is schema-only until refreshed from its partition sources — TMDL carries no row data.

**Report formats** — a `.pbix` stores its report either as the **classic** single `Report/Layout` document, or as **PBIR** (`Report/definition/`, a tree of per-page/per-visual JSON), which is what every report authored in the Power BI *service* downloads as. **Both are fully read AND written** through the same entry points. Reading converts a PBIR tree to the classic shape (page names/size/type, visual names, geometry incl. z/tabOrder, `projections` + a synthesized `prototypeQuery` so column-vs-measure is recoverable, hidden state, filters, sync groups, mobile layout). Writing patches each page/visual back onto the **original file it was read from**, so fields this converter doesn't model (custom visual settings, `sortDefinition`, `howCreated`, …) survive an edit untouched — a write that changes nothing changes nothing on disk. Added pages/visuals are created, removed ones deleted, and `pages.json` `pageOrder`/`activePageName` kept in step; a classic `Report/Layout` is never planted alongside the tree. Resource and custom-visual registration works on both (`resourcePackages`/`publicCustomVisuals` live in `Report/definition/report.json` for PBIR, with that format's flat package shape and string item types). Call `pbix_report_format` for an open file's format and whether it is writable.

### Report Layout & Visuals (29)
Visual CRUD, visual-level sort authoring (`pbix_set_visual_sort`), page management, filters, positions, bookmarks (add/remove), settings, layout read/write, default filter extraction.

**Editing primitives** — `pbix_rename_page`, `pbix_reorder_pages`, `pbix_set_page_visibility`, `pbix_duplicate_page`, `pbix_move_visual`, `pbix_duplicate_visual`. All behave identically on classic and PBIR. Renaming a page changes only `displayName`: the internal `name` is an identity that bookmarks, drillthrough and page navigation reference. Duplicating a page or visual assigns fresh identities to the copy (and to every visual on a copied page), since two objects sharing a `name` collide in bookmarks and navigation. `pbix_move_visual` writes the container geometry — `x`/`y`/`z`/`width`/`height` live on the container, not in the config JSON, so `pbix_set_visual_property` cannot reach them — and keeps the classic `config.layouts` copy in step. Pass `-1` for any axis to leave it unchanged. Page order: references not listed in `page_order` keep their relative order after the ones that are.

**Format-normalized fields** — `_get_layout` returns the classic shape whichever format the file uses, so callers never branch on format: `displayOption` is an int (PBIR stores the enum name), page visibility is `config.visibility` as an int (PBIR stores an enum name), and bookmarks are in `config.bookmarks` (PBIR stores them under `definition/bookmarks/`). Each is converted back on write.

### DAX Engine (5)
Measure evaluation, per-dimension evaluation, **grouped (GROUP-BY) evaluation** (`pbix_evaluate_dax_grouped` — evaluates measures for every group key in ONE call and returns structured per-group rows; the fact rows are bucketed by the propagated join key once instead of one evaluation per category value, so a chart bound to thousands of categories is a single call. Measures the fast path can't bucket are still evaluated exactly, per group. `max_groups` defaults to 3500 — Power BI's own data-reduction window — and truncation is reported via `group_count`/`truncated`), calculated columns, cache management.

**Filter-context values** — each `filter_context` entry is either a LIST (In-set: `{"dim-Geo.State": ["WA","OR"]}`) or a structured PREDICATE dict evaluated natively, so a caller need not enumerate a high-cardinality column's matching values first: `{"op": ">", "value": 100}` (`>`, `>=`, `<`, `<=`, `=`, `<>`), `{"between": [lo, hi]}`, `{"in": [...]}`, `{"not_in": [...]}`, `{"contains"|"starts_with"|"ends_with": "text"}` (case-insensitive), `{"relative_date": {"last": 7, "unit": "day"|"week"|"month"|"year", "anchor": "<ISO date, default today>"}}`, `{"is_blank": true|false}`. Multiple keys in one dict are ANDed; comparisons are numeric when both sides are numbers, date-aware when both parse as dates, text otherwise.

**Default-filter contract** (`pbix_evaluate_dax` / `pbix_evaluate_dax_per_dimension`): a non-empty `filter_context` always wins. With an empty `filter_context`, `apply_default_filters` controls whether the report's persisted default slicer selections are auto-applied — `pbix_evaluate_dax` defaults to **True** (historic behavior), `pbix_evaluate_dax_per_dimension` defaults to **False** (its historic raw iteration; the iterated dimension's own key is always owned by the per-value loop). Pass the flag explicitly for identical behavior across both tools. `page_index` scopes the applied defaults: `-1` (default) merges every page's slicers; `>= 0` applies only that page's — the service scopes a slicer's default selection to its own page, so pass the page a visual lives on to reproduce that visual's service number. `apply_default_filters=False` evaluates the raw, truly unfiltered model.

### DataModel Read (16)
Schema, measures, relationships, Power Query, columns, table data, data sources, metadata, CSV export (single/all), value search, SQL-like query, table profiling, data diff.

### DataModel Write (30)
Metadata SQL read/write, measure CRUD (incl. `pbix_datamodel_set_measure_category` — set or CLEAR a measure's DataCategory without touching the expression), column modification, **calculated-column authoring** (`pbix_datamodel_add_calculated_column` — evaluates a row-context DAX expression, materializes the values into VertiPaq, and stamps the column `Type=2` + Expression so the service recomputes it on refresh; refuses aggregations/CALCULATE/RELATED it can't reproduce per-row rather than storing wrong values) **and removal** (`pbix_datamodel_remove_calculated_column` — the inverse: drops the `Type=2` metadata AND the materialized VertiPaq values, re-materializing every other calculated column/table; refuses when a dependent calculated column reads the one being removed, naming the dependents), **calculated-table authoring** (`pbix_datamodel_add_calculated_table` — evaluates a table expression, materializes the rows, and stamps Desktop's calc-table metadata (partition `Type=2` carrying the DAX, data columns `Type=4`). Supported shapes are the ones this engine reproduces exactly: `DATATABLE`, `GENERATESERIES`, `DISTINCT`, `VALUES`, `FILTER`, `TOPN`, `ADDCOLUMNS`, a bare table reference; `SUMMARIZE`/`SUMMARIZECOLUMNS`/`SELECTCOLUMNS`/`GROUPBY` are refused because their extension columns are dropped. Calculated columns and calculated tables compose in either order — each add re-materializes the others), relationship CRUD **incl. in-place edit** (`pbix_datamodel_modify_relationship` — change cardinality / cross-filter direction / active flag without remove + re-add; `is_active` and `cross_filter_direction` are a metadata-only splice that also works on models the rebuild path refuses, while a cardinality change re-runs the rebuild so the R$ join indexes match), table removal, field parameters, calculation groups, TMDL export **and import** (`pbix_import_tmdl` — the exporter's inverse: parses a TMDL folder or single document into a working schema-only PBIX with tables, calculated columns, measures, relationships, hierarchies, partition M/DAX sources, shared M expressions, roles, extended properties (field parameters) and lineage tags; export → import → export reproduces the same TMDL files byte-for-byte), PBIP export, decompress/recompress, ABF file ops, table data write (incl. `pbix_append_table_rows` — ADDS rows to an existing table instead of replacing, and NDJSON `rows_path` streaming sources on `pbix_create`/append for large loads, issue #46), value replace, **sort-by-column** (`pbix_set_sort_by_column` / `pbix_get_sort_by_columns` — orders one column by another's values, e.g. "Month Name" by "Month Number"; the model stores an ID rather than a name, so names are resolved for you, including `InferredName` columns that carry no `ExplicitName`. Self-sorts and A↔B cycles are refused rather than written into a model that then opens broken; passing an empty `sort_by_column` clears the sort-by back to the `0` Desktop stores).

### Resources, Themes & Custom Visuals (18)
Static resources, image / registered-resource authoring (`pbix_add_image`, `pbix_register_resource`, `pbix_set_image` — see [Image & Resource Tools](#image--resource-tools)), theme read/write, color extraction/recolor, linguistic schema, custom visual import/remove (GUID embedded into `Report/CustomVisuals/` + `publicCustomVisuals`), reference-only registration of certified AppSource visuals by GUID (`pbix_reference_public_visual` — zero file payload, e.g. Deneb), turnkey HTML/CSS/SVG visual authoring — create/view/edit plus a template renderer — and SVG data-URI image-measure codegen (`pbix_svg_measure`). Detailed contracts in [Custom Visual & HTML Tools](#custom-visual--html-tools); recipes in [rich-content.md](rich-content.md).

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

### Partition Management (4)
List/remove M partitions and set a table's M query definition (`pbix_set_partition_m`, metadata-only — Power BI runs the new M on next Refresh). `pbix_add_partition` blocked for PBIX (needs PartitionStorage), works for PBIP/TMDL export.

### Incremental Refresh (2)
Get/set incremental refresh policies. `pbix_set_incremental_refresh` works for files with a data source (source_csv/source_db); embedded-only files return a clear error (same as PBI Desktop).

### Diagnostics & Security (5)
20-point diagnostic (`pbix_doctor`), report documentation (`pbix_document`), file diff (`pbix_diff`), performance analysis (`pbix_performance`), password extraction (`pbix_get_password`).

## Image & Resource Tools

Registering a file resource touches three places (all Desktop-verified against `test_corpus/GeoSales_Dashboard.pbix`): the bytes at `Report/StaticResources/RegisteredResources/<item>`, a `<Default Extension="<ext>" ContentType=""/>` in `[Content_Types].xml`, and a type-tagged entry in the layout's top-level `resourcePackages` `RegisteredResources` package (`path` == `name` == the bare filename). Item types: **100** image, **200** shape map, **201** custom theme, **202** base theme.

Common to all three tools: the file type is decided by the **content**, never by a filename or a caller's claim — images (type 100) must be PNG/JPEG/GIF/WebP/BMP/TIFF/ICO/SVG (magic bytes; SVG detection tolerates a BOM, XML declaration, DOCTYPE and comments but requires an `<svg>` root), and shape maps / themes (200/201/202) must be **JSON**, the form Desktop stores them in; **5 MB** cap; item names are sanitized to `[A-Za-z0-9._-]`, forced to the sniffed extension, contained inside `RegisteredResources`, and uniquified rather than overwriting a different resource (identical bytes under the same name reuse it). The engine never fetches remote URLs; callers holding one fetch it and pass bytes.

### `pbix_add_image`

Register an image and place an image visual — one call.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `page_index` | int | `0` | Zero-based page index |
| `image_path` | str | `""` | Local image file (exactly one of image_path / image_base64) |
| `image_base64` | str | `""` | Base64 bytes; a full `data:image/png;base64,…` URI is accepted |
| `name` | str | `""` | Item name (extension is replaced with the sniffed one) |
| `x` / `y` | int | `40` | Position in pixels (clamped to the page) |
| `width` / `height` | int | `300` / `200` | Size in pixels |
| `scaling` | str | `"Fit"` | `Fit`, `Fill`, or `Normal` (case-insensitive); empty omits the `imageScaling` object, which Desktop also does |

The container matches Desktop's own insert: `howCreated: "InsertVisualButton"`, 1000-step `z` and `tabOrder` (on the container AND `layouts[0].position`), `drillFilterOtherVisuals`, `objects.general` holding the `ImageUrl` ResourcePackageItem expr (`PackageType: 1`), `objects.imageScaling` (**not** under `general`), and `vcObjects.padding` `0D` on all four sides. **Returns** `data.item_name`, `data.visual_index`, `data.visual_name`, `data.format`. **Errors:** `LAYOUT_JSON_INVALID` (unrecognized image data, bad scaling, page index out of range, both/neither image source, oversize, or a file with neither a classic `Report/Layout` nor a readable `Report/definition/pages.json` — both formats are otherwise fully supported), `FILE_NOT_OPEN`.

### `pbix_register_resource`

Register a file resource without placing a visual.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `name` | str | required | Desired item name |
| `image_path` | str | `""` | Local file (exactly one of image_path / image_base64) |
| `image_base64` | str | `""` | Base64 bytes (data: URI and line-wrapped base64 accepted) |
| `resource_type` | str | `"image"` | `image` (100, image payload), `shapeMap` (200), `customTheme` (201), `baseTheme` (202) — the last three take **JSON** |

**Returns** `data.item_name`, `data.bytes`, `data.format`. **Errors:** `LAYOUT_JSON_INVALID` (unknown resource_type, unrecognized data, source/size problems), `FILE_NOT_OPEN`.

### `pbix_set_image`

Repoint or restyle an existing image visual.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `page_index` | int | required | Zero-based page index |
| `visual_index` | int | required | Zero-based visual index (must be an `image` visual) |
| `image_path` / `image_base64` | str | `""` | New bytes — registers a fresh resource and repoints (mutually exclusive with `item_name`) |
| `item_name` | str | `""` | Point at an already-registered item instead (validated against the package) |
| `name` | str | `""` | Item name to use when registering new bytes |
| `scaling` | str | `""` | `Fit` / `Fill` / `Normal`; unchanged when empty |

At least one of bytes / `item_name` / `scaling` is required (else `LAYOUT_JSON_INVALID`, "Nothing to change"). The previously referenced resource is left in place — another visual may reference the same item. **Returns** `data.item_name`, `data.visual_index`. **Errors:** `LAYOUT_JSON_INVALID` (not an image visual, index out of range, unknown item_name — the message lists what IS registered), `FILE_NOT_OPEN`.

## Visual Sort Authoring

`pbix_set_visual_sort(alias, page_index, visual_index, sort_by="", sort_direction="desc")` sets or clears the visual-level sort; `pbix_add_visual(..., sort_by=..., sort_direction=...)` authors it at creation. Both write the Desktop-style `prototypeQuery.OrderBy` clause AND the same clause in the compiled `query` (empty `sort_by` on `pbix_set_visual_sort` clears the sort).

| Parameter | Accepted values |
|-----------|----------------|
| `sort_by` | One of the visual's own fields, as a bare name (`Pipeline Value`), DAX-style reference (`[Pipeline Value]`, `'Table'[Col]`, `Table[Col]` — DAX `''` quote-escapes supported), or queryRef (`Table.Field`). Matching is case-sensitive first, then case-insensitive. |
| `sort_direction` | `asc` / `ascending` / `desc` / `descending` (case-insensitive; also numeric `1` / `2`). Default `desc`. |

**Errors** (surfaced as `LAYOUT_JSON_INVALID`): invalid direction; the visual has no `prototypeQuery.Select` (no data binding); `sort_by` matching none of the visual's fields — the message lists the fields that ARE available; a matched field with an unsupported expression shape (only Column, Measure, and Aggregation selects can be sorted — not HierarchyLevel). Sorting by a bare numeric value-role column follows the implicit-Sum rewrite into the OrderBy `Aggregation` expression, exactly as Desktop stores such sorts.

## Custom Visual & HTML Tools

Detailed contracts for the custom-visual embedding tools and the turnkey HTML / CSS / SVG visual authoring tools (0.9.23), built on the bundled **PBIX HTML** custom visual (GUID `pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7`, shipped in `src/pbix_mcp/assets/pbix_html_visual/`).

Embedding follows Power BI Desktop exactly: the `.pbiviz` is extracted verbatim into `Report/CustomVisuals/<guid>/` and the GUID is registered in the report's `publicCustomVisuals` array — at the top level of `Report/Layout` on classic files, in `Report/definition/report.json` on PBIR files (both formats are fully supported). The GUID is always read from the `.pbiviz` manifest — never fabricated. `LAYOUT_JSON_INVALID` fires only for a file with neither layout format, or a PBIR file whose `report.json` is missing or corrupt.

### `pbix_add_custom_visual`

Embed any `.pbiviz` package into the report and register its GUID.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `pbiviz_path` | str | required | Absolute path to the `.pbiviz` file |

Reads the GUID from the package manifest, extracts the package into `Report/CustomVisuals/<guid>/`, and appends the GUID to the report's `publicCustomVisuals` array (`Report/Layout` on classic files, `Report/definition/report.json` on PBIR; deduped, idempotent on re-import). Place the visual on a page with `pbix_add_visual(..., visual_type="<guid>")`. **Returns** a message-only envelope (`data: null`). **Errors:** `LAYOUT_JSON_INVALID` (neither layout format present or missing/corrupt PBIR `report.json`, invalid manifest GUID, or extraction failure), `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "Custom visual 'PBIX HTML' imported successfully!\n  GUID: pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7\n  Version: 1.1.0.0  (apiVersion 5.11.0)\n  Files: 3 extracted to Report/CustomVisuals/pbixHtml5C3A2F1E9B7D46A8C0E1D2F3A4B5C6D7/\n  Registered in publicCustomVisuals.",
  "data": null,
  "warnings": []
}
```

### `pbix_reference_public_visual`

Reference a **public (AppSource) custom visual** by GUID — registration only, zero file payload. Certified visuals (e.g. Deneb, GUID `deneb7E15AEF80B9E4D4F8E12924291ECE89A`) are resolved by the Power BI service **from AppSource** for report consumers; the service-verified recipe (spec in `objects.vega`, single `dataset` role) is in [rich-content.md](rich-content.md).

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `alias` | str | required | Alias of the open file |
| `guid` | str | required | The visual's marketplace GUID, registered VERBATIM (letters/digits/underscores/hyphens — legacy `PBI_CV_<GUID>` hyphenated ids are accepted; the service resolves by exact GUID, so no normalization is ever applied) |

Appends the GUID to the report's `publicCustomVisuals` array (created when missing, deduped) — `Report/Layout` on classic files, `Report/definition/report.json` on PBIR. No `Report/CustomVisuals/` folder, no `[Content_Types].xml` change, `resourcePackages` untouched. De-register with `pbix_remove_custom_visual` (its folder branch is a no-op for references). **Returns** the resulting array in `data.publicCustomVisuals`. **Errors:** `LAYOUT_JSON_INVALID` (invalid GUID, or a file with neither layout format / missing or corrupt PBIR `report.json`), `FILE_NOT_OPEN`.

```json
{
  "success": true,
  "message": "Public visual 'deneb7E15AEF80B9E4D4F8E12924291ECE89A' registered in publicCustomVisuals.\nThe service auto-loads certified visuals from AppSource; place one with:\n  pbix_add_visual(alias, page_index, visual_type=\"deneb7E15AEF80B9E4D4F8E12924291ECE89A\", ...)",
  "data": {"publicCustomVisuals": ["deneb7E15AEF80B9E4D4F8E12924291ECE89A"]},
  "warnings": []
}
```

### `pbix_svg_measure`

Generate DAX for an **SVG data-URI image measure** — optionally author it directly. With `DataCategory='ImageUrl'` the measure renders as a live, filter-context-aware vector image in table/matrix cells (Desktop, service, PDF export, subscriptions) with zero custom visuals. Rules baked in: utf8 (never base64), `%23`-encoded colors, single-quoted SVG attributes, locale-proof `FORMAT(INT(…),"0")` interpolation, ~32k budget check.

| Argument | Type | Default | Description |
|----------|------|---------|-------------|
| `kind` | str | `""` | Template: `data_bar`, `bullet`, `pill`, `icon_updown`, `sparkline` (empty = list templates) |
| `spec_json` | str | `""` | JSON object of template parameters; dynamic parts are DAX sub-expressions (e.g. `"value": "[Total Revenue]"`) |
| `alias` | str | `""` | With `measure_name`: also ADD the measure (DataCategory=ImageUrl) |
| `measure_table` | str | `""` | Home table for the added measure (default: first table) |
| `measure_name` | str | `""` | Name for the added measure |

**Returns** `data.dax` (the generated expression), `data.chars`, `data.added`. **Errors:** `BAD_SPEC` (invalid spec_json / add without both alias+measure_name), `BAD_TEMPLATE` (unknown kind, bad parameter, over-budget), `FILE_NOT_OPEN`, or the propagated measure-add error.

```json
{
  "success": true,
  "message": "Rendered 'data_bar' DAX (407 chars). Added measure 'Rev Bar' on 'Sales' with DataCategory=ImageUrl.",
  "data": {"dax": "VAR _r0 = DIVIDE([Total Revenue], 2000)\n...", "chars": 407, "added": true},
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
