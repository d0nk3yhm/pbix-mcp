# pbix-mcp

[![CI](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml)

An MCP server for **creating**, reading, writing, and evaluating Power BI `.pbix` and `.pbit` files. Exposes 59 tools covering report creation from scratch (with actual row data), layout editing, visual management, DAX evaluation, RLS security, password extraction, VertiPaq data, and binary format internals.

## Quick Start

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e .
```

### Claude Desktop

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "pbix-mcp-server"
    }
  }
}
```

### Claude Code

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "pbix-mcp-server"
    }
  }
}
```

### Generic MCP (stdio)

```bash
pbix-mcp-server
```

## Stability

| Feature | Status | Notes |
|---------|--------|-------|
| PBIX creation from scratch | **Stable** | Full DataModel + layout + VertiPaq row data, no Power BI Desktop needed |
| Report layout read/write | **Stable** | Pages, visuals, filters, positions, bookmarks |
| Visual add/remove | **Stable** | Cards, charts, shapes/buttons, images, textboxes, slicers |
| Visual property editing | **Stable** | Dot-path and full JSON |
| DAX measure CRUD | **Stable** | Add, modify, remove via metadata SQL |
| DAX evaluation (154 functions) | **Stable** | 99.5% non-BLANK across 204 measures from 4 dashboards |
| Password extraction | **Stable** | Extracts embedded passwords from password-gated dashboards |
| Metadata SQL read/write | **Stable** | Full SQLite access to tables, columns, relationships |
| Default slicer filter extraction | **Stable** | Legacy Layout JSON and PBIR format |
| Table data read (PBIXRay) | **Stable** | All materialized VertiPaq tables |
| Calculated table evaluation | **Stable** | DATATABLE, GENERATESERIES, CALENDAR, field parameters |
| XPress9 decompress/recompress | **Stable** | Byte-exact round-trip verified |
| ABF archive manipulation | **Stable** | List, extract, replace internal files |
| VertiPaq table data write | **Stable** | String, Int64, Double, DateTime, Decimal column types |
| DataMashup (M code) editing | **Stable** | Read/write Power Query expressions |
| File save/repack | **Stable** | Auto-backup on overwrite, SecurityBindings auto-removed |
| Calculated column evaluation | **Stable** | Per-row DAX expression evaluation |
| Row-Level Security (RLS) | **Stable** | Read/write roles, evaluate filter expressions |
| Diagnostic tool (`pbix_doctor`) | **Stable** | 8-point health check |

## Known Limitations

- **PBIR format** is read-only for filter extraction; layout write requires legacy format
- **1 out of 204 tested measures** returns BLANK (requires per-employee RANKX visual row context)
- **VertiPaq write** encodes String, Int64, Double, DateTime, Decimal; Boolean type not yet supported
- **Created PBIX files** contain valid VertiPaq data but Power BI Desktop may need a refresh to fully index the data

## Tools (59)

### Create & File Management (5)
`pbix_create` — **Create a new PBIX from scratch** (tables, measures, relationships → valid DataModel)
`pbix_open` · `pbix_save` · `pbix_close` · `pbix_list_open`

### Report Layout & Visuals (19)
`pbix_add_visual` — **Add any visual type** (card, chart, table, shape/button, image, textbox, slicer)
`pbix_remove_visual` — Remove a visual from a page
`pbix_get_pages` · `pbix_add_page` · `pbix_remove_page` · `pbix_get_page_visuals` · `pbix_get_visual_detail` · `pbix_get_visual_positions` · `pbix_set_visual_property` · `pbix_update_visual_json` · `pbix_get_layout_raw` · `pbix_set_layout_raw` · `pbix_get_filters` · `pbix_set_filters` · `pbix_get_default_filters` · `pbix_get_settings` · `pbix_set_settings` · `pbix_get_bookmarks`

### DAX Engine (3)
`pbix_evaluate_dax` · `pbix_evaluate_dax_per_dimension` · `pbix_clear_dax_cache`

### DataModel Read (7)
`pbix_get_model_schema` · `pbix_get_model_measures` · `pbix_get_model_relationships` · `pbix_get_model_power_query` · `pbix_get_model_columns` · `pbix_get_table_data` · `pbix_list_tables`

### DataModel Write (13)
`pbix_datamodel_query_metadata` · `pbix_datamodel_modify_metadata` · `pbix_datamodel_add_measure` · `pbix_datamodel_modify_measure` · `pbix_datamodel_remove_measure` · `pbix_datamodel_modify_column` · `pbix_datamodel_decompress` · `pbix_datamodel_recompress` · `pbix_datamodel_replace_file` · `pbix_datamodel_extract_file` · `pbix_datamodel_list_abf_files` · `pbix_set_table_data` · `pbix_update_table_rows`

### Resources & Theme (5)
`pbix_list_resources` · `pbix_get_theme` · `pbix_set_theme` · `pbix_get_linguistic_schema` · `pbix_set_linguistic_schema`

### DataMashup (2)
`pbix_get_m_code` · `pbix_set_m_code`

### Row-Level Security (3)
`pbix_get_rls_roles` — **Read all RLS roles and filter expressions**
`pbix_set_rls_role` — **Create/update RLS roles with DAX filters**
`pbix_evaluate_rls` — **Evaluate RLS filter against actual data to see which rows are visible**

### Diagnostics & Security (3)
`pbix_doctor` · `pbix_get_metadata` · `pbix_get_password` — **Extract embedded passwords from protected dashboards**

## Creating Reports from Scratch

Build a complete PBIX file without Power BI Desktop:

```
> pbix_create("sales_report.pbix", "sales",
    tables_json='[{"name": "Sales", "columns": [{"name": "Amount", "data_type": "Double"}, {"name": "Product", "data_type": "String"}]}]',
    measures_json='[{"table": "Sales", "name": "Total Sales", "expression": "SUM(Sales[Amount])"}]')

Created 'sales_report.pbix' (2,550 bytes) and opened it.
```

Then add visuals:

```
> pbix_add_visual("sales", 0, "card", x=20, y=20, width=200, height=150)
> pbix_add_visual("sales", 0, "clusteredBarChart", x=240, y=20, width=500, height=350)
> pbix_add_visual("sales", 0, "shape", x=20, y=400, width=150, height=50)
> pbix_save("sales")
```

Supported visual types: `card`, `table`, `matrix`, `slicer`, `clusteredBarChart`, `clusteredColumnChart`, `lineChart`, `areaChart`, `pieChart`, `donutChart`, `treemap`, `map`, `filledMap`, `shape` (buttons), `image`, `textbox`, `kpi`, `gauge`, `waterfallChart`, `funnel`, `scatterChart`, and any custom visual type.

For images, use `pbix_add_visual` with `visual_type="image"` and set the image URL via `config_json`:

```
> pbix_add_visual("sales", 0, "image", x=20, y=20, width=200, height=100,
    config_json='{"singleVisual": {"objects": {"general": [{"properties": {"imageUrl": {"expr": {"Literal": {"Value": "https://example.com/logo.png"}}}}}]}}}')
```

For buttons/shapes with text, use `visual_type="shape"`:

```
> pbix_add_visual("sales", 0, "shape", x=20, y=500, width=200, height=50,
    config_json='{"singleVisual": {"objects": {"text": [{"properties": {"text": {"expr": {"Literal": {"Value": "Click Me"}}}}}]}}}')
```

## DAX Engine

154 functions across 10 categories. Tested against 4 real-world dashboards (204 measures total, 99% non-BLANK, 0 crashes).

| Category | Functions |
|----------|-----------|
| Aggregation | `SUM`, `AVERAGE`, `COUNT`, `COUNTROWS`, `MIN`, `MAX`, `DISTINCTCOUNT`, `PRODUCT`, `MEDIAN`, `COUNTBLANK` |
| Iterators | `SUMX`, `MAXX`, `MINX`, `AVERAGEX`, `COUNTX`, `COUNTAX`, `CONCATENATEX`, `RANKX`, `FILTER`, `GENERATE`, `GENERATEALL` |
| Table | `TOPN`, `ADDCOLUMNS`, `SUMMARIZE`, `SUMMARIZECOLUMNS`, `SELECTCOLUMNS`, `DISTINCT`, `UNION`, `EXCEPT`, `INTERSECT`, `CROSSJOIN`, `DATATABLE`, `ROW`, `TREATAS` |
| Time Intelligence | `CALCULATE`, `DATEADD`, `SAMEPERIODLASTYEAR`, `TOTALYTD/MTD/QTD`, `PREVIOUSMONTH/QUARTER/YEAR`, `NEXTMONTH/QUARTER/YEAR`, `PARALLELPERIOD`, `DATESYTD/MTD/QTD`, `STARTOF/ENDOF`, `FIRSTDATE/LASTDATE`, `DATESBETWEEN`, `DATESINPERIOD`, `CALENDAR`, `CALENDARAUTO`, `OPENING/CLOSINGBALANCE` |
| Filter | `REMOVEFILTERS`, `ALL`, `ALLEXCEPT`, `ALLSELECTED`, `KEEPFILTERS`, `VALUES`, `SELECTEDVALUE`, `HASONEVALUE`, `HASONEFILTER`, `ISFILTERED`, `ISCROSSFILTERED` |
| Logic | `IF`, `SWITCH`, `AND`, `OR`, `NOT`, `ISBLANK`, `IFERROR`, `COALESCE`, `CONTAINS`, `TRUE`, `FALSE` |
| Math | `DIVIDE`, `ABS`, `ROUND`, `INT`, `CEILING`, `FLOOR`, `MOD`, `POWER`, `SQRT`, `LOG`, `LOG10`, `LN`, `EXP`, `SIGN`, `TRUNC`, `EVEN`, `ODD`, `FACT`, `GCD`, `LCM`, `PI`, `RAND`, `RANDBETWEEN` |
| Text | `CONCATENATE`, `FORMAT`, `LEFT`, `RIGHT`, `MID`, `LEN`, `UPPER`, `LOWER`, `PROPER`, `TRIM`, `SUBSTITUTE`, `REPLACE`, `REPT`, `SEARCH`, `FIND`, `CONTAINSSTRING`, `EXACT`, `UNICHAR`, `UNICODE`, `VALUE`, `COMBINEVALUES` |
| Relationship | `RELATED`, `RELATEDTABLE`, `USERELATIONSHIP`, `CROSSFILTER`, `EARLIER`, `EARLIEST` |
| Information | `LOOKUPVALUE`, `ISNUMBER`, `ISTEXT`, `ISNONTEXT`, `ISLOGICAL`, `ISERROR`, `USERNAME`, `USERPRINCIPALNAME` |

### Verified Against Power BI Desktop

| Measure | Power BI | DAX Engine | Match |
|---------|----------|------------|-------|
| Sales (Year=2015) | $470,532 | $470,533 | ✅ |
| Profit Margin | 13.1% | 13.1% | ✅ |
| Sales LY | $484,247 | $484,247 | ✅ |
| Sales Change | -2.8% | -2.8% | ✅ |
| California Sales | $88,444 | $88,444 | ✅ |
| Technology Sales | $162,781 | $162,781 | ✅ |

## Safety

- `pbix_save` creates automatic `.bak` backups before overwriting
- `pbix_close` refuses to discard unsaved changes unless `force=True`
- SecurityBindings are auto-removed on repack (prevents corruption)
- All write operations are applied to temp directories, not directly to the original file

## Testing

```bash
# Fast tests (no PBIX files needed, runs from fresh clone)
pytest -m "not slow"

# All tests (requires private PBIX test corpus)
pytest -v
```

| Suite | Tests | Marker | Needs PBIX? |
|-------|-------|--------|-------------|
| `test_dax_engine.py` | 55 | `unit` | No |
| `test_dax_accuracy.py` | 50 | `unit` | No |
| `test_golden.py` | 9 | `golden` | Partial (2 skip gracefully) |
| `test_fixtures.py` | 18 | `unit` | No (ships with repo) |
| `test_cross_report.py` | 19 | `slow`, `integration` | Yes (4 private PBIX files) |

**138 tests pass from a fresh clone.** 19 integration tests require private PBIX files and skip gracefully.

## Architecture

```
PBIX file (ZIP)
├── Report/Layout          ← JSON: pages, visuals, filters
├── Report/LinguisticSchema ← XML: Q&A config
├── Report/StaticResources/ ← Themes, images, custom visuals
├── DataMashup             ← Binary + inner ZIP: M code
├── DataModel              ← XPress9 compressed → ABF archive
│   ├── metadata.sqlitedb  ← SQLite: tables, columns, measures, relationships
│   ├── *.tbl\*.prt\*.idf  ← VertiPaq: column data (RLE + bit-packed)
│   ├── *.idfmeta          ← Segment statistics
│   ├── *.dict             ← Dictionary encoding
│   └── *.hidx             ← Hash index
├── Settings               ← JSON
├── Metadata               ← JSON
└── [Content_Types].xml    ← Package manifest
```

### Package Layout

```
src/pbix_mcp/
  server.py              # MCP server (59 tools)
  cli.py                 # Entry point (pbix-mcp-server)
  builder.py             # PBIX file builder (create from scratch)
  errors.py              # Typed exceptions with stable error codes
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (154 functions)
    calc_tables.py       # Calculated table support
  formats/
    abf_rebuild.py       # ABF archive format
    datamodel_roundtrip.py  # XPress9 compress/decompress
    vertipaq_encoder.py  # VertiPaq column encoding
  models/
    responses.py         # Pydantic response models
    requests.py          # Pydantic request models
```

## Development

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e ".[dev]"
pytest -m "not slow"
ruff check src/ tests/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for project conventions and [SUPPORT.md](SUPPORT.md) for what counts as a bug vs unsupported behavior.

## License

MIT
