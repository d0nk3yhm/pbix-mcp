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
| `pbix_close` | `force` | `False` | Refuses to close with unsaved changes |

## Tool Categories

### Create & File Management (5)
- `pbix_create` — Create PBIX from scratch with tables, measures, row data
- `pbix_open` — Extract PBIX to temp dir for editing
- `pbix_save` — Repack temp dir into PBIX (safe defaults)
- `pbix_close` — Release temp dir (force flag for dirty state)
- `pbix_list_open` — Show all open file sessions

### Report Layout & Visuals (20)
Visual CRUD, page management, filters, positions, bookmarks (add/remove), settings.

### DAX Engine (4)
Measure evaluation, per-dimension evaluation, calculated columns, cache management.

### DataModel Read (8)
Schema, measures, relationships, Power Query, columns, table data.

### DataModel Write (16)
Metadata SQL, measure CRUD, column modification, field parameters, calculation groups,
TMDL export, decompress/recompress, ABF file ops.

### Resources, Themes & Custom Visuals (7)
Static resources, theme read/write, linguistic schema, custom visual import/remove.

### DataMashup (2)
M code read/write.

### Row-Level Security (3)
RLS role CRUD, filter expression evaluation against data.

### Incremental Refresh (2)
Set/get incremental refresh policies with archive/refresh windows and change detection.

### Diagnostics & Security (2)
8-point health check, password extraction.
