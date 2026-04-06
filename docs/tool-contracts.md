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

## Tool Categories (101 tools)

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

### Resources, Themes & Custom Visuals (9)
Static resources, theme read/write, color extraction/recolor, linguistic schema, custom visual import/remove.

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
Get/set incremental refresh policies. `pbix_set_incremental_refresh` blocked — requires DataMashup with RangeStart/RangeEnd M parameters.

### Diagnostics & Security (5)
17-point diagnostic (`pbix_doctor`), report documentation (`pbix_document`), file diff (`pbix_diff`), performance analysis (`pbix_performance`), password extraction (`pbix_get_password`).
