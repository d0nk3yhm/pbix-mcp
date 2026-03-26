# Architecture

## Overview

pbix-mcp is an MCP (Model Context Protocol) server that provides programmatic access to Power BI `.pbix` and `.pbit` files. It exposes 69 tools via stdio transport.

## Module Layout

```
src/pbix_mcp/
  server.py              # MCP tool definitions (69 tools)
  cli.py                 # Entry point with --log-level flag
  builder.py             # PBIX creation (entirely from scratch — metadata, VertiPaq, ABF, layout)
  builder_v2.py          # Template-free ABF + ZIP generation
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

### Creating a PBIX

**Everything is generated from scratch** — no templates or skeletons. The entire PBIX binary format has been reversed and reimplemented: PBIX ZIP shell, ABF binary container (signature, header, VirtualDirectory, BackupLog), XMLA database document (db.xml), metadata SQLite (63 system tables), VertiPaq column storage, and report layout JSON. The only non-generated artifact is a 144-byte CryptKey constant (Microsoft RSA key BLOB, GUID-independent).

1. `PBIXBuilder` generates clean SQLite metadata (DATASOURCEVERSION=2) — only user-specified tables, columns, and measures
2. Key PBI annotations are written: PBI_IsFromSource (ObjectType=7), PBI_ResultType, SummarizationSetBy, PBI_QueryOrder, __PBI_TimeIntelligenceEnabled
3. Fixed RowNumber GUID (2662979B-1795-4F74-8F37-6A1BA8059B61) ensures stable attribute hierarchy references
4. VertiPaq encoder writes actual row data into column segments using pure bitpack (RLE disabled — slightly less space-efficient but correct). Verified with 6 tables, 36 columns, 5 relationships, 25 rows, 3 pages, 14 visuals (Northwind showcase)
5. H$ attribute hierarchy tables are generated with sorted POS_TO_ID/ID_TO_POS using NoSplit<32>
6. R$ relationship index tables use +3 DATA_ID_OFFSET padding and 1-based row indices (verified byte-exact against PBI Desktop ground truth)
7. Relationships auto-detect Many/One sides; From=Many (fact table), To=One (dimension table)
8. ABF binary container is built from scratch — signature, header, VDir, BackupLog, db.xml, CryptKey, all data files laid out sequentially
9. ABF is XPress9-compressed into a DataModel
10. Report/Layout JSON is generated from scratch with a default page and visuals (table, pieChart, clusteredBarChart, card, slicer all supported)
11. PBIX ZIP shell is generated from scratch (Version, Content_Types, DiagramLayout, Settings, Metadata); packaged as a valid PBIX
12. For database sources, M expressions use `Item` key (not `Name`) for MySQL/PostgreSQL table navigation

### Data Source Support

| Source | Import Mode | DirectQuery | Refresh Verified |
|--------|------------|-------------|------------------|
| Embedded data | Yes | N/A | N/A |
| CSV files | Yes | N/A | Yes |
| SQLite | Yes | N/A | Yes |
| SQL Server | Yes | Yes | Yes |
| MySQL | Yes | Yes (MariaDB ODBC 3.1) | Yes |
| PostgreSQL | Yes | Yes (native) | Yes |
| Excel | Yes | N/A | Yes |
| JSON/API | Yes | N/A | Yes |
| Azure SQL | Yes | Yes | Yes |

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
