# pbix-mcp

[![CI](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml)

An MCP server for **creating**, reading, writing, and evaluating Power BI `.pbix` and `.pbit` files. Exposes 60 tools covering report creation from scratch (with actual row data), layout editing, visual management, DAX evaluation, RLS security, password extraction, VertiPaq data, and binary format internals.

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
# With debug logging:
pbix-mcp-server --log-level debug
```

## Stability

| Feature | Status | Notes |
|---------|--------|-------|
| PBIX creation from scratch | **Stable** | Arbitrary tables with row data, measures, H$ hierarchies — loads in PBI Desktop |
| Report layout read/write | **Stable** | Pages, visuals, filters, positions, bookmarks |
| Visual add/remove | **Stable** | Cards, charts, shapes/buttons, images, textboxes, slicers |
| Visual property editing | **Stable** | Dot-path and full JSON |
| DAX measure CRUD | **Stable** | Add, modify, remove via metadata SQL |
| DAX evaluation (156 functions) | **Stable** | Best-effort evaluator; see accuracy notes below |
| Metadata SQL read/write | **Stable** | Full SQLite access to tables, columns, relationships |
| Default slicer filter extraction | **Stable** | Legacy Layout JSON and PBIR format |
| Table data read (PBIXRay) | **Stable** | All materialized VertiPaq tables |
| Calculated table evaluation | **Stable** | DATATABLE, GENERATESERIES, CALENDAR, field parameters |
| XPress9 decompress/recompress | **Stable** | Byte-exact round-trip verified |
| ABF archive manipulation | **Stable** | List, extract, replace internal files |
| VertiPaq table data write | **Stable** | String, Int64, Double, DateTime, Decimal column types |
| DataMashup (M code) editing | **Stable** | Read/write Power Query expressions |
| File save/repack | **Stable** | Auto-backup on overwrite, SecurityBindings auto-removed |
| Calculated column evaluation | **Beta** | Per-row DAX expression evaluation; tested with synthetic data |
| Password extraction | **Beta** | Regex scan of DAX measures for embedded passwords |
| Row-Level Security (RLS) | **Beta** | Read/write roles, evaluate filter expressions against data |
| Diagnostic tool (`pbix_doctor`) | **Beta** | 8-point health check |

## Known Limitations

- **DAX engine is best-effort** — designed for practical evaluation, not semantic parity with Analysis Services. Unsupported functions return `None` with status `"unsupported"`, circular references raise `DAXEvaluationError`. See [docs/supported-dax.md](docs/supported-dax.md) for full details.
- **PBIR format** is read-only for filter extraction; layout write requires legacy format
- **1 out of 204 tested measures** returns BLANK (requires per-employee RANKX visual row context)
- **Performance** — tables >100K rows trigger a warning; the DAX engine operates on in-memory Python data
- **Import mode only** — DirectQuery files are detected on open and rejected with a clear error
- **From-scratch tables** — String, Int64, and Double columns work with arbitrary cardinalities and cross-table relationships (RELATED, filtering)
- **H$ hierarchy tables** — columns with >2 distinct values use MatType=3 (no sorted dimension browsing); full H$ NoSplit encoding is implemented but not yet wired up for arbitrary cardinalities

## Tools (60)

### Create & File Management (5)
`pbix_create` · `pbix_open` · `pbix_save` · `pbix_close` · `pbix_list_open`

### Report Layout & Visuals (18)
`pbix_add_visual` · `pbix_remove_visual` · `pbix_get_pages` · `pbix_add_page` · `pbix_remove_page` · `pbix_get_page_visuals` · `pbix_get_visual_detail` · `pbix_get_visual_positions` · `pbix_set_visual_property` · `pbix_update_visual_json` · `pbix_get_layout_raw` · `pbix_set_layout_raw` · `pbix_get_filters` · `pbix_set_filters` · `pbix_get_default_filters` · `pbix_get_settings` · `pbix_set_settings` · `pbix_get_bookmarks`

### DAX Engine (4)
`pbix_evaluate_dax` · `pbix_evaluate_dax_per_dimension` · `pbix_evaluate_calculated_columns` · `pbix_clear_dax_cache`

### DataModel Read (8)
`pbix_get_model_schema` · `pbix_get_model_measures` · `pbix_get_model_relationships` · `pbix_get_model_power_query` · `pbix_get_model_columns` · `pbix_get_table_data` · `pbix_list_tables` · `pbix_get_metadata`

### DataModel Write (13)
`pbix_datamodel_query_metadata` · `pbix_datamodel_modify_metadata` · `pbix_datamodel_add_measure` · `pbix_datamodel_modify_measure` · `pbix_datamodel_remove_measure` · `pbix_datamodel_modify_column` · `pbix_datamodel_decompress` · `pbix_datamodel_recompress` · `pbix_datamodel_replace_file` · `pbix_datamodel_extract_file` · `pbix_datamodel_list_abf_files` · `pbix_set_table_data` · `pbix_update_table_rows`

### Resources & Theme (5)
`pbix_list_resources` · `pbix_get_theme` · `pbix_set_theme` · `pbix_get_linguistic_schema` · `pbix_set_linguistic_schema`

### DataMashup (2)
`pbix_get_m_code` · `pbix_set_m_code`

### Row-Level Security (3)
`pbix_get_rls_roles` · `pbix_set_rls_role` · `pbix_evaluate_rls`

### Diagnostics & Security (2)
`pbix_doctor` · `pbix_get_password`

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

## DAX Engine

156 functions across 10 categories. This is a **best-effort evaluator** — it produces correct results for common patterns but does not aim for semantic parity with Analysis Services.

| Category | Functions |
|----------|-----------|
| Aggregation | `SUM`, `AVERAGE`, `COUNT`, `COUNTROWS`, `MIN`, `MAX`, `DISTINCTCOUNT`, `PRODUCT`, `MEDIAN`, `COUNTBLANK` |
| Iterators | `SUMX`, `MAXX`, `MINX`, `AVERAGEX`, `COUNTX`, `COUNTAX`, `CONCATENATEX`, `RANKX`, `FILTER`, `GENERATE`, `GENERATEALL` |
| Table | `TOPN`, `ADDCOLUMNS`, `SUMMARIZE`, `SUMMARIZECOLUMNS`, `SELECTCOLUMNS`, `DISTINCT`, `UNION`, `EXCEPT`, `INTERSECT`, `CROSSJOIN`, `DATATABLE`, `ROW`, `TREATAS` |
| Time Intelligence | `CALCULATE`, `DATEADD`, `SAMEPERIODLASTYEAR`, `TOTALYTD`, `TOTALMTD`, `TOTALQTD`, `PREVIOUSMONTH`, `PREVIOUSQUARTER`, `PREVIOUSYEAR`, `NEXTMONTH`, `NEXTQUARTER`, `NEXTYEAR`, `PARALLELPERIOD`, `DATESYTD`, `DATESMTD`, `DATESQTD`, `STARTOFMONTH`, `STARTOFQUARTER`, `STARTOFYEAR`, `ENDOFMONTH`, `ENDOFQUARTER`, `ENDOFYEAR`, `FIRSTDATE`, `LASTDATE`, `DATESBETWEEN`, `DATESINPERIOD`, `CALENDAR`, `CALENDARAUTO`, `OPENINGBALANCEMONTH`, `OPENINGBALANCEQUARTER`, `OPENINGBALANCEYEAR`, `CLOSINGBALANCEMONTH`, `CLOSINGBALANCEQUARTER`, `CLOSINGBALANCEYEAR` |
| Filter | `REMOVEFILTERS`, `ALL`, `ALLEXCEPT`, `ALLSELECTED`, `KEEPFILTERS`, `VALUES`, `SELECTEDVALUE`, `HASONEVALUE`, `HASONEFILTER`, `ISFILTERED`, `ISCROSSFILTERED` |
| Logic | `IF`, `SWITCH`, `AND`, `OR`, `NOT`, `ISBLANK`, `IFERROR`, `COALESCE`, `CONTAINS`, `TRUE`, `FALSE` |
| Math | `DIVIDE`, `ABS`, `ROUND`, `INT`, `CEILING`, `FLOOR`, `MOD`, `POWER`, `SQRT`, `LOG`, `LOG10`, `LN`, `EXP`, `SIGN`, `TRUNC`, `EVEN`, `ODD`, `FACT`, `GCD`, `LCM`, `PI`, `RAND`, `RANDBETWEEN`, `CURRENCY`, `FIXED` |
| Text | `CONCATENATE`, `FORMAT`, `LEFT`, `RIGHT`, `MID`, `LEN`, `UPPER`, `LOWER`, `PROPER`, `TRIM`, `SUBSTITUTE`, `REPLACE`, `REPT`, `SEARCH`, `FIND`, `CONTAINSSTRING`, `CONTAINSSTRINGEXACT`, `EXACT`, `UNICHAR`, `UNICODE`, `VALUE`, `COMBINEVALUES` |
| Relationship | `RELATED`, `RELATEDTABLE`, `USERELATIONSHIP`, `CROSSFILTER`, `EARLIER`, `EARLIEST`, `PATHITEM`, `PATHLENGTH`, `PATHCONTAINS` |
| Information | `LOOKUPVALUE`, `ISNUMBER`, `ISTEXT`, `ISNONTEXT`, `ISLOGICAL`, `ISERROR`, `USERNAME`, `USERPRINCIPALNAME`, `BLANK`, `GENERATESERIES` |

### Accuracy

Tested against 4 real-world Power BI dashboards (204 measures total). **All 4 dashboards are publicly available** from [Dashboard-Design/Power-BI-Design-Files](https://github.com/Dashboard-Design/Power-BI-Design-Files) (MIT License, Sajjad Ahmadi). Anyone can download them and reproduce these results.

| Dashboard | Source Path | Measures | Non-BLANK | Accuracy |
|-----------|------------|----------|-----------|----------|
| GeoSales | `Full Dashboards/GeoSales Dashboard - Azure Map/` | 71 | 70 | 98.6% |
| Agents Performance | `Full Dashboards/Agents Performance - Dashboard/` | 42 | 42 | 100% |
| Ecommerce Conversion | `Full Dashboards/Ecommerce Conversion Dashboard/` | 70 | 70 | 100% |
| IT Support | `Full Dashboards/IT Support Performance Dashboard/` | 21 | 21 | 100% |
| **Total** | | **204** | **203** | **99.5%** |

The 1 BLANK measure requires per-employee RANKX visual row context that doesn't exist at report level.

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

# Download public test corpus (4 dashboards, MIT licensed)
python scripts/download_test_corpus.py --output-dir test_corpus

# Run integration tests against the corpus
PBIX_TEST_SAMPLES=test_corpus pytest tests/test_cross_report.py -v
```

| Suite | Tests | Marker | Needs PBIX? |
|-------|-------|--------|-------------|
| `test_dax_engine.py` | 55 | `unit` | 6 skip without private files |
| `test_dax_accuracy.py` | 50 | `unit` | No |
| `test_golden.py` | 15 | `golden` | 2 skip without private files |
| `test_fixtures.py` | 18 | `unit` | No (ships with repo) |
| `test_beta_features.py` | 10 | `unit` | No |
| `test_cross_report.py` | 19 | `slow`, `integration` | Yes (4 public PBIX dashboards) |

**From a fresh clone: ~163 tests pass, ~8 skip gracefully, 19 integration tests skip.** The skipped tests require the public test corpus. Download it with `python scripts/download_test_corpus.py`, then set `PBIX_TEST_SAMPLES=test_corpus`.

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
  server.py              # MCP server (60 tools)
  cli.py                 # Entry point (pbix-mcp-server --log-level debug)
  builder.py             # PBIX file builder (create from scratch)
  errors.py              # Typed exceptions with stable error codes
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (156 functions, best-effort)
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
mypy src/pbix_mcp/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for project conventions and [SUPPORT.md](SUPPORT.md) for what counts as a bug vs unsupported behavior.

## License

MIT
