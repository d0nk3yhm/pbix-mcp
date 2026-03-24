# Architecture

## Overview

pbix-mcp is an MCP (Model Context Protocol) server that provides programmatic access to Power BI `.pbix` and `.pbit` files. It exposes 69 tools via stdio transport.

## Module Layout

```
src/pbix_mcp/
  server.py              # MCP tool definitions (69 tools)
  cli.py                 # Entry point with --log-level flag
  builder.py             # PBIX creation from scratch
  errors.py              # Typed exception hierarchy
  logging_config.py      # Structured logging (normal/debug/trace)
  dax/
    engine.py            # Best-effort DAX evaluator (156 functions)
    calc_tables.py       # Calculated table + column evaluation
  formats/
    abf_rebuild.py       # ABF archive read/write/build
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding
  models/
    responses.py         # Pydantic response models (JSON output)
    requests.py          # Pydantic input models
```

## Data Flow

### Reading a PBIX

1. `pbix_open` extracts the ZIP to a temp directory
2. Report/Layout is parsed as JSON for visual/page operations
3. DataModel is XPress9-compressed; decompress to get ABF archive
4. ABF contains SQLite metadata + VertiPaq column data
5. PBIXRay reads materialized table data from VertiPaq
6. calc_tables.py evaluates calculated tables/columns from metadata DAX
7. DAX engine evaluates measures against the loaded data

### Writing a PBIX

1. Modifications are applied to files in the temp directory
2. `pbix_save` repacks the temp directory into a ZIP
3. SecurityBindings are auto-removed to prevent corruption
4. DataModel is stored uncompressed (it's already XPress9)

### Creating from Scratch

1. `PBIXBuilder` generates ABF metadata (SQLite) with tables/measures
2. VertiPaq encoder writes actual row data into column segments
3. ABF is XPress9-compressed into a DataModel
4. Report/Layout JSON is generated with a default page
5. Everything is packaged as a valid ZIP

## Error Handling

Every tool catches `PBIXMCPError` (typed) before `Exception` (generic).
All responses are JSON via `ToolResponse.to_text()`:

```json
{"success": true, "message": "...", "data": ..., "warnings": [...]}
{"success": false, "error_code": "PBIX_INVALID", "message": "..."}
```

## DAX Engine

The engine is a best-effort evaluator, not a strict Analysis Services runtime.
It handles 156 functions across 10 categories. Key design decisions:

- Returns `None` for unsupported functions (tracked in `unsupported_functions`)
- Returns 0 for circular references
- Uses heuristics for date-table detection (column named "Date" in a table)
- Auto-applies default slicer filters from report layout
- Smart SELECTEDVALUE fallback for parameter-driven measures
