# pbix-mcp

[![CI](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/d0nk3yhm/pbix-mcp/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/pbix-mcp)](https://pypi.org/project/pbix-mcp/)
[![Downloads](https://img.shields.io/pypi/dm/pbix-mcp)](https://pypi.org/project/pbix-mcp/)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

An MCP server for **creating**, reading, writing, and evaluating Power BI `.pbix` and `.pbit` files — **no Power BI Desktop required**. The entire PBIX binary format has been independently reversed and reimplemented in pure Python — no templates, no skeletons, no Microsoft binaries. Generated files open in PBI Desktop with full interactivity: view data, add measures, create visuals, and refresh — verified with PBI Desktop March 2026.

Exposes 101 tools covering report creation (all 6 data types, cross-table relationships, CSV/SQLite/SQL Server/MySQL/PostgreSQL/Excel/JSON/Azure SQL data sources, DirectQuery, and DAX measures), layout editing, visual management, bookmarks, custom visuals, field parameters, calculation groups, TMDL export, incremental refresh, DAX evaluation (156 functions), RLS security, and binary format internals.

See [CHANGELOG.md](CHANGELOG.md) for version history.

## Try It

Generate a complete 3-page Northwind Analytics Dashboard in under a second:

```bash
pip install pbix-mcp
python examples/create_showcase.py
```

Creates a PBIX with 6 tables, 5 relationships (including chained cross-table lookups), 4 DAX measures, 3 pages, and 14 visuals. Open `showcase_northwind.pbix` in Power BI Desktop — everything works: slicers, cross-filtering, drill-through.

## Quick Start

```bash
git clone https://github.com/d0nk3yhm/pbix-mcp.git
cd pbix-mcp
pip install -e .
```

### Claude Desktop / Claude Code

Add to your MCP config file:

| Platform | Config file |
|----------|------------|
| Claude Desktop (macOS) | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Claude Desktop (Windows) | `%APPDATA%\Claude\claude_desktop_config.json` |
| Claude Code | `~/.claude/settings.json` (Linux/macOS) or `%USERPROFILE%\.claude\settings.json` (Windows) |

```json
{
  "mcpServers": {
    "powerbi-editor": {
      "command": "pbix-mcp-server"
    }
  }
}
```

> **Windows note:** If `pbix-mcp-server` is not on PATH, use the full Python path:
> ```json
> {
>   "mcpServers": {
>     "powerbi-editor": {
>       "command": "python",
>       "args": ["-m", "pbix_mcp.cli"]
>     }
>   }
> }
> ```

### Codex Desktop (OpenAI)

1. `pip install git+https://github.com/d0nk3yhm/pbix-mcp.git`
2. Open Codex Desktop → Settings → MCP → Add Server
3. Configure:
   - **Name**: `powerbi-editor`
   - **Command**: `pbix-mcp-server`
   - **Arguments**: *(leave empty)*

### Generic MCP (stdio)

```bash
pbix-mcp-server
# With debug logging:
pbix-mcp-server --log-level debug
```

## Format Reversal Status

Every layer of the PBIX binary format has been independently reversed and reimplemented. No templates, skeletons, or Microsoft binaries are used.

| Layer | Status | Implementation |
|-------|--------|----------------|
| PBIX ZIP shell | **Reversed** | Version, Content_Types, DiagramLayout, Settings, Metadata — generated constants |
| Report/Layout JSON | **Reversed** | Pages, visuals, data bindings, filters — `_build_layout()` |
| ABF binary container | **Reversed** | 72-byte signature, BackupLogHeader, VirtualDirectory, BackupLog — `build_abf_clean()` |
| XMLA Load document (db.xml) | **Reversed** | 28 xmlns namespaces, CompatibilityLevel=1550, TabularMetadata — `generate_db_xml()` |
| CryptKey.bin | **Constant** | 144-byte RSA key BLOB (Microsoft crypto format; GUID-independent constant) |
| Metadata SQLite | **Reversed** | 68 system tables matching PBI March 2026 schema — `create_empty_metadata_db()` |
| VertiPaq column storage | **Reversed** | IDF (bit-packed), IDFMETA (segment stats), dictionary (Long/Real/String), HIDX (hash index) |
| H$ attribute hierarchies | **Reversed** | NoSplit<32> POS_TO_ID + ID_TO_POS for all cardinalities |
| R$ relationship indexes | **Reversed** | NoSplit<N> INDEX encoding with +3 DATA_ID_OFFSET padding and 1-based row indices (verified byte-exact against PBI Desktop ground truth) |
| XPress9 compression | **Reversed** | Custom compress/decompress with reversed chunk framing, headers, and multi-thread format; core algorithm via [xpress9-python](https://github.com/Hugoberry/xpress9-python) (MIT) |

The only non-generated artifact is the 144-byte CryptKey constant. This is a Microsoft RSA key BLOB that requires `rskeymgmt` infrastructure to generate. The key is GUID-independent — any valid key works with any database ID. Random bytes produce `PFE_INVALID_CRYPT_KEY`.

## Stability

| Feature | Status | Notes |
|---------|--------|-------|
| PBIX creation | **Stable** | Multi-table with all 6 data types, relationships, H$ hierarchies, and measures. Generated files support full PBI Desktop editing (add measures, columns, visuals) |
| Cross-table relationships | **Stable** | R$ system tables with NoSplit INDEX encoding (+3 padding, 1-based row indices); cross-table visuals, RELATED(), and cross-table filtering verified byte-exact against PBI Desktop ground truth |
| Refreshable CSV sources | **Stable** | `source_csv` parameter creates M expressions referencing external CSV files; click Refresh in PBI Desktop to re-import |
| SQLite database sources | **Stable** | `source_db` with ODBC driver; data imported at build, Refresh re-reads from DB |
| SQL Server / MySQL / PostgreSQL database sources | **Stable** | `source_db` Import and DirectQuery for all. MySQL DQ requires MariaDB ODBC 3.1 (`type: 'mariadb'`) |
| Excel data sources | **Stable** | `source_db` with `type: 'excel'` — Import mode |
| JSON/API data sources | **Stable** | `source_db` with `type: 'json'` — Import mode from REST APIs and JSON files |
| Azure SQL data sources | **Stable** | `source_db` with `type: 'azuresql'` — Import and DirectQuery |
| Data source switching | **Stable** | `pbix_update_data_source` — lightweight connection string change without full DataModel rebuild. Switch between SQL Server, PostgreSQL, MySQL, CSV, Excel, JSON, SQLite, Azure SQL. Switch Import/DirectQuery mode. Verified with live MSSQL→PostgreSQL→CSV roundtrip |
| DirectQuery mode | **Stable** | `mode='directquery'` with SQL Server, PostgreSQL, and MySQL (via MariaDB ODBC 3.1) — live database queries, no refresh needed |
| VertiPaq table data write | **Stable** | Create and roundtrip (set_table_data, update_table_rows, replace_value) via full builder rebuild |
| Data export | **Stable** | `pbix_export_table_csv`, `pbix_export_all_tables_csv` — export any table(s) to CSV, all rows, proper quoting, ISO dates |
| Data search & query | **Stable** | `pbix_find_value` across tables, `pbix_query_table` SQL-like WHERE/AND/OR/LIKE/IN/ORDER BY, `pbix_table_stats` profiling, `pbix_data_diff` row-level file comparison |
| Roundtrip DataModel modify | **Stable** | Add/remove tables, relationships, measures on existing files. Metadata-only changes (measures, RLS, column properties) use binary splice for PBI Desktop files; structural changes use full builder rebuild |
| H$ attribute hierarchies | **Stable** | NoSplit<32> POS_TO_ID + ID_TO_POS for all cardinalities; MaterializationType=0 |
| Report layout read/write | **Stable** | Pages, visuals, filters, positions, bookmarks |
| Visual add/remove | **Stable** | Cards, charts, shapes/buttons, images, textboxes, slicers — with full data bindings (projections + prototypeQuery) |
| Visual formatting | **Stable** | `pbix_format_visual` — human-readable API for titles, backgrounds, borders, drop shadows, padding, spacing, data labels, legend, axis, colors, table headers, and 25+ more categories. Ground truth validated against 9 PBI Desktop templates |
| Color extraction & recolor | **Stable** | `pbix_extract_colors` scans themes + all visuals for hex literals AND ThemeDataColor references (resolved to hex). `pbix_recolor` does global find-and-replace across themes + layout, converting ThemeDataColor refs to direct hex. Verified with full brand compliance recolor (531 colors → SG Armaturen palette) |
| Visual property editing | **Stable** | Dot-path and full JSON |
| DAX measure CRUD | **Stable** | Add, modify, remove via binary splice (PBI Desktop files) or full builder rebuild. Sequential adds supported with automatic MAXID tracking |
| DAX evaluation (156 functions) | **Stable** | Best-effort evaluator; see accuracy notes below |
| Metadata SQL read/write | **Stable** | Full SQLite access to tables, columns, relationships |
| Default slicer filter extraction | **Stable** | Legacy Layout JSON and PBIR format |
| Table data read | **Stable** | Native VertiPaq decoder — all materialized tables (no external dependencies) |
| Calculated table evaluation | **Stable** | DATATABLE, GENERATESERIES, CALENDAR, field parameters |
| XPress9 decompress/recompress | **Stable** | Byte-exact round-trip verified |
| ABF archive manipulation | **Stable** | List, extract, replace internal files |
| DataMashup (M code) editing | **Stable** | Read/write Power Query expressions |
| File save/repack | **Stable** | Auto-backup on overwrite, SecurityBindings auto-removed, optional MSIP sensitivity label stripping |
| Calculated column evaluation | **Beta** | Per-row DAX expression evaluation; tested with synthetic data |
| Password extraction | **Beta** | Regex scan of DAX measures for embedded passwords |
| Row-Level Security (RLS) | **Stable** | Read, write, and evaluate RLS roles. `set_rls_role` uses binary splice — roles persist across save/reopen. MAXID-based ID allocation. Verified on PBI Desktop files |
| Bookmark creation | **Beta** | Create/remove bookmarks with page targeting and visual visibility state |
| Field Parameters | **Blocked** | `pbix_datamodel_add_field_parameter` blocked — needs full DataModel rebuild to generate VertiPaq storage for new table |
| Calculation Groups | **Blocked** | `pbix_datamodel_add_calculation_group` blocked — needs full DataModel rebuild to generate VertiPaq storage for new table |
| TMDL Export | **Stable** | Export data model as Git-friendly TMDL text files via `pbix_export_tmdl`. Validated with Adventure Works DW 2020 — correct partition types, CrossFilteringBehavior, model properties, shared expressions |
| PBIP Export | **Stable** | Convert PBIX to PBIP (Power BI Project) folder structure via `pbix_export_pbip` — full TMDL semantic model + report layout + static resources, ready for Git |
| Perspectives | **Stable** | Create/list/remove perspectives via `pbix_add_perspective`, `pbix_get_perspectives`, `pbix_remove_perspective` |
| User Hierarchies | **Stable** | Create/list/remove drill-down hierarchies via `pbix_add_hierarchy`, `pbix_get_hierarchies`, `pbix_remove_hierarchy`. Works with builder-created and PBI Desktop files |
| Cultures & Translations | **Stable** | Add cultures, translate table/column/measure names via `pbix_add_culture`, `pbix_add_translations`, `pbix_get_cultures`, `pbix_remove_culture` |
| Partition Management | **Partial** | List/remove partitions via `pbix_get_partitions`, `pbix_remove_partition`. `pbix_add_partition` blocked for PBIX (needs PartitionStorage in VertiPaq), works for PBIP/TMDL export |
| Sensitivity Labels | **Stable** | Strip MSIP sensitivity labels via `pbix_save(strip_sensitivity_label=True)` |
| Custom Visuals | **Beta** | Import .pbiviz packages via `pbix_add_custom_visual`, place with `pbix_add_visual` |
| Incremental Refresh | **Blocked** | `pbix_set_incremental_refresh` blocked — requires DataMashup with RangeStart/RangeEnd M parameters |
| Report diff (`pbix_diff`) | **Stable** | Compare two PBIX files — tables, columns, measures, relationships, pages/visuals, data sources, theme colors. Shows added/removed/changed |
| Report documentation (`pbix_document`) | **Stable** | Auto-generate full report documentation (markdown + .docx) — tables, columns, measures, relationships, data sources, pages/visuals, RLS roles, theme colors |
| Performance analysis (`pbix_performance`) | **Stable** | Flags oversized tables, empty tables, wide schemas, high-cardinality strings, complex measures, inactive/bidirectional relationships, orphaned tables |
| Diagnostic tool (`pbix_doctor`) | **Stable** | 17-point comprehensive diagnostic — data sources, storage modes, columns, relationships, measures, RLS, VertiPaq row counts, table/storage consistency, referential integrity, Expression/DataMashup consistency, MAXID |

## Known Limitations

- **DAX engine is best-effort** — designed for practical evaluation, not semantic parity with Analysis Services. Unsupported functions return `None` with status `"unsupported"`, circular references raise `DAXEvaluationError`. See [docs/supported-dax.md](docs/supported-dax.md) for full details.
- **PBIR format** — PBI Desktop (March 2026) has rendering bugs with PBIR decomposed format. PBIP export uses legacy report format (version 1.0) which works reliably.
- **1 out of 204 tested measures** returns BLANK (requires per-employee RANKX visual row context)
- **Performance** — tables >100K rows trigger a warning; the DAX engine operates on in-memory Python data
- **Opening existing DirectQuery files** — layout, measures, and metadata editing work; DAX evaluation and table reads return clear errors since data lives in the remote source (this is inherent to DirectQuery — the data isn't in the file)
- **Creating DirectQuery files** — fully working with SQL Server (LocalDB), PostgreSQL 16, and MySQL 9.6 (via MariaDB adapter); requires a running database server and initial data snapshot
- **CryptKey.bin** — the 144-byte RSA key BLOB cannot be generated without Microsoft's crypto infrastructure (`rskeymgmt`). A known-valid GUID-independent constant is used.
- **Embedded VertiPaq data** — verified working with 11 tables, 72 columns, 13 relationships, 121K+ rows (Adventure Works DW 2020) and 6 tables, 36 columns, 5 relationships, 25 rows, 3 pages, 14 visuals (Northwind showcase)
- **RLE encoding** — disabled in the VertiPaq encoder (pure bitpack used). Slightly less space-efficient but correct
- **Adding partitions to PBIX** — `pbix_add_partition` is blocked for PBIX files (needs PartitionStorage in VertiPaq). Works for PBIP/TMDL export. Reading and removing existing partitions works.
- **Full DataModel rebuild** — `set_table_data`, `update_table_rows`, `add/remove_relationship`, `remove_table` trigger a full DataModel rebuild via the builder pipeline. Most other tools (`add_measure`, `modify_measure`, `modify_column`, `set_rls_role`, `add_perspective`, `add_culture`, `add_translations`, `update_data_source`, etc.) use a lightweight metadata-only path.


## Tools (101)

### Create & File Management (5)
`pbix_create` · `pbix_open` · `pbix_save` · `pbix_close` · `pbix_list_open`

### Report Layout & Visuals (21)
`pbix_add_visual` · `pbix_remove_visual` · `pbix_format_visual` · `pbix_get_pages` · `pbix_add_page` · `pbix_remove_page` · `pbix_get_page_visuals` · `pbix_get_visual_detail` · `pbix_get_visual_positions` · `pbix_set_visual_property` · `pbix_update_visual_json` · `pbix_get_layout_raw` · `pbix_set_layout_raw` · `pbix_get_filters` · `pbix_set_filters` · `pbix_get_default_filters` · `pbix_get_settings` · `pbix_set_settings` · `pbix_get_bookmarks` · `pbix_add_bookmark` · `pbix_remove_bookmark`

### DAX Engine (4)
`pbix_evaluate_dax` · `pbix_evaluate_dax_per_dimension` · `pbix_evaluate_calculated_columns` · `pbix_clear_dax_cache`

### DataModel Read (16)
`pbix_get_model_schema` · `pbix_get_model_measures` · `pbix_get_model_relationships` · `pbix_get_model_power_query` · `pbix_get_model_columns` · `pbix_get_table_data` · `pbix_list_tables` · `pbix_get_metadata` · `pbix_list_data_sources` · `pbix_update_data_source` · `pbix_export_table_csv` · `pbix_export_all_tables_csv` · `pbix_find_value` · `pbix_query_table` · `pbix_table_stats` · `pbix_data_diff`

### DataModel Write (21)
`pbix_datamodel_query_metadata` · `pbix_datamodel_modify_metadata` · `pbix_datamodel_add_measure` · `pbix_datamodel_modify_measure` · `pbix_datamodel_remove_measure` · `pbix_datamodel_modify_column` · `pbix_datamodel_add_relationship` · `pbix_datamodel_remove_relationship` · `pbix_datamodel_remove_table` · `pbix_datamodel_decompress` · `pbix_datamodel_recompress` · `pbix_datamodel_replace_file` · `pbix_datamodel_extract_file` · `pbix_datamodel_list_abf_files` · `pbix_set_table_data` · `pbix_update_table_rows` · `pbix_datamodel_add_field_parameter` · `pbix_datamodel_add_calculation_group` · `pbix_export_tmdl` · `pbix_export_pbip` · `pbix_replace_value`

### Resources, Themes & Custom Visuals (9)
`pbix_list_resources` · `pbix_get_theme` · `pbix_set_theme` · `pbix_extract_colors` · `pbix_recolor` · `pbix_get_linguistic_schema` · `pbix_set_linguistic_schema` · `pbix_add_custom_visual` · `pbix_remove_custom_visual`

### DataMashup (2)
`pbix_get_m_code` · `pbix_set_m_code`

### Row-Level Security (3)
`pbix_get_rls_roles` · `pbix_set_rls_role` · `pbix_evaluate_rls`

### Perspectives (3)
`pbix_get_perspectives` · `pbix_add_perspective` · `pbix_remove_perspective`

### User Hierarchies (3)
`pbix_get_hierarchies` · `pbix_add_hierarchy` · `pbix_remove_hierarchy`

### Cultures & Translations (4)
`pbix_get_cultures` · `pbix_add_culture` · `pbix_add_translations` · `pbix_remove_culture`

### Partition Management (3)
`pbix_get_partitions` · `pbix_add_partition` · `pbix_remove_partition`

### Incremental Refresh (2)
`pbix_set_incremental_refresh` · `pbix_get_incremental_refresh`

### Diagnostics & Security (5)
`pbix_doctor` · `pbix_document` · `pbix_diff` · `pbix_performance` · `pbix_get_password`

## Creating Reports

Build a complete multi-table PBIX with relationships and cross-table DAX — no Power BI Desktop needed:

```python
from pbix_mcp.builder import PBIXBuilder

builder = PBIXBuilder()

# Dimension table
builder.add_table('Products', [
    {'name': 'ProductID', 'data_type': 'Int64'},
    {'name': 'Product',   'data_type': 'String'},
    {'name': 'UnitPrice', 'data_type': 'Double'},
], rows=[
    {'ProductID': 1, 'Product': 'Widget A',    'UnitPrice': 29.99},
    {'ProductID': 2, 'Product': 'Widget B',    'UnitPrice': 49.99},
    {'ProductID': 3, 'Product': 'Gadget X',    'UnitPrice': 14.99},
])

# Fact table
builder.add_table('Sales', [
    {'name': 'OrderID',   'data_type': 'Int64'},
    {'name': 'ProductID', 'data_type': 'Int64'},
    {'name': 'Qty',       'data_type': 'Int64'},
    {'name': 'Region',    'data_type': 'String'},
], rows=[
    {'OrderID': 1001, 'ProductID': 1, 'Qty': 5,  'Region': 'North'},
    {'OrderID': 1002, 'ProductID': 2, 'Qty': 3,  'Region': 'South'},
    {'OrderID': 1003, 'ProductID': 3, 'Qty': 20, 'Region': 'East'},
])

# Cross-table relationship (from=many, to=one)
builder.add_relationship('Sales', 'ProductID', 'Products', 'ProductID')

# Measures (including cross-table RELATED)
builder.add_measure('Sales', 'Total Qty', 'SUM(Sales[Qty])')
builder.add_measure('Sales', 'Total Revenue',
    'SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))')

builder.save('sales_report.pbix')
```

Opens in Power BI Desktop with full interactivity — slicers, cross-filtering, and all DAX measures work.

### Refreshable CSV Sources

Point tables at external CSV files so data can be refreshed in Power BI Desktop:

```python
builder.add_table('Sales', [
    {'name': 'OrderID',   'data_type': 'Int64'},
    {'name': 'ProductID', 'data_type': 'Int64'},
    {'name': 'Qty',       'data_type': 'Int64'},
], rows=sales_data,
   source_csv=r'C:\Data\sales.csv')  # M expression references this CSV
```

The initial data snapshot is embedded in the PBIX. When opened in Power BI Desktop, clicking **Refresh** re-imports from the CSV file. Edit the CSV → Refresh → data updates live.

### Database Sources (SQL Server / SQLite / MySQL / PostgreSQL / Excel / JSON / Azure SQL)

Connect tables to databases so data can be refreshed from the DB:

```python
# SQL Server (built-in PBI connector — works with LocalDB, Express, full)
builder.add_table('Orders', [
    {'name': 'OrderID', 'data_type': 'Int64'},
    {'name': 'Qty',     'data_type': 'Int64'},
], rows=orders_data,
   source_db={'type': 'sqlserver', 'server': r'(localdb)\MSSQLLocalDB',
              'database': 'MyDB', 'table': 'Orders'})

# SQLite (requires SQLite3 ODBC Driver — http://www.ch-werner.de/sqliteodbc/)
builder.add_table('Orders', [
    {'name': 'OrderID', 'data_type': 'Int64'},
    {'name': 'Qty',     'data_type': 'Int64'},
], rows=orders_data,
   source_db={'type': 'sqlite', 'path': r'C:\Data\mydb.sqlite', 'table': 'orders'})

# MySQL (built-in PBI connector — verified with MySQL 9.6)
builder.add_table('Orders', [
    {'name': 'OrderID', 'data_type': 'Int64'},
    {'name': 'Qty',     'data_type': 'Int64'},
], rows=orders_data,
   source_db={'type': 'mysql', 'server': 'localhost', 'database': 'mydb',
              'table': 'orders', 'port': 3306})

# PostgreSQL (built-in PBI connector — verified with PostgreSQL 16)
builder.add_table('Orders', [
    {'name': 'order_id', 'data_type': 'Int64'},
    {'name': 'qty',      'data_type': 'Int64'},
], rows=orders_data,
   source_db={'type': 'postgresql', 'server': 'localhost', 'database': 'mydb',
              'table': 'orders', 'port': 5432, 'schema': 'public'})

# MariaDB adapter (for MySQL DirectQuery — requires MariaDB ODBC 3.1 Driver)
builder.add_table('Orders', [
    {'name': 'OrderID', 'data_type': 'Int64'},
    {'name': 'Qty',     'data_type': 'Int64'},
], rows=orders_data,
   mode='directquery',
   source_db={'type': 'mariadb', 'server': 'localhost', 'database': 'mydb',
              'table': 'orders', 'port': 3306})
```

Data is **Import mode** by default — a snapshot is embedded in the PBIX at build time. Clicking **Refresh** in Power BI Desktop re-reads from the database. The report works offline between refreshes.

### DirectQuery (Live Database Queries)

For true live queries (no refresh needed — data updates instantly):

```python
builder.add_table('Orders', [
    {'name': 'OrderID', 'data_type': 'Int64'},
    {'name': 'Qty',     'data_type': 'Int64'},
], rows=snapshot_data,  # Initial snapshot (required)
   mode='directquery',
   source_db={'type': 'sqlserver', 'server': r'(localdb)\MSSQLLocalDB',
              'database': 'MyDB', 'table': 'Orders'})
```

DirectQuery creates a PBIX with `Partition.Mode=1` and a `Sql.Database()` M expression. Power BI Desktop queries the database live — INSERT/UPDATE/DELETE in the database is reflected instantly without clicking Refresh.

> **Note:** DirectQuery requires a running database server. Verified with SQL Server (LocalDB), PostgreSQL 16, and MySQL 9.6 (via MariaDB adapter). All three also support Import mode with Refresh. The `rows` parameter provides an initial data snapshot embedded in the PBIX.

### Via MCP Tool

```json
{
  "tool": "pbix_create",
  "arguments": {
    "file_path": "report.pbix",
    "tables_json": "[{\"name\": \"Sales\", \"columns\": [{\"name\": \"Amount\", \"data_type\": \"Double\"}], \"rows\": [{\"Amount\": 100}], \"source_csv\": \"C:/Data/sales.csv\"}]",
    "measures_json": "[{\"table\": \"Sales\", \"name\": \"Total\", \"expression\": \"SUM(Sales[Amount])\"}]",
    "relationships_json": "[{\"from_table\": \"Sales\", \"from_column\": \"ProductID\", \"to_table\": \"Products\", \"to_column\": \"ProductID\"}]"
  }
}
```

### Switching Data Sources (No Rebuild)

Change connection strings on existing PBIX files without regenerating the DataModel — lightweight metadata-only update:

```python
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel, compress_datamodel
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite, rebuild_abf_with_modified_sqlite
from pbix_mcp.builder import _build_m_expression
import zipfile, io

# Open existing PBIX
with open('report.pbix', 'rb') as f:
    original = f.read()

z = zipfile.ZipFile(io.BytesIO(original))
abf = decompress_datamodel(z.read('DataModel'))

# Switch Sales table from SQL Server to PostgreSQL DirectQuery
def switch_source(conn):
    conn.row_factory = __import__('sqlite3').Row
    row = conn.execute(
        "SELECT p.ID, t.ID as tid FROM Partition p "
        "JOIN [Table] t ON p.TableID = t.ID WHERE t.Name = 'Sales'"
    ).fetchone()
    cols = [{'name': c['ExplicitName'],
             'data_type': {6:'Int64', 8:'Double', 2:'String'}[c['ExplicitDataType']]}
            for c in conn.execute(
                'SELECT ExplicitName, ExplicitDataType FROM [Column] '
                'WHERE TableID = ? AND Type = 1', (row['tid'],))]
    new_m = _build_m_expression('Sales', cols, source_db={
        'type': 'postgresql', 'server': 'pg.example.com', 'port': 5432,
        'database': 'analytics', 'table': 'sales', 'schema': 'public',
    }, is_directquery=True)
    conn.execute('UPDATE Partition SET QueryDefinition=?, Mode=1 WHERE ID=?',
                 (new_m, row['ID']))
    conn.commit()

new_abf = rebuild_abf_with_modified_sqlite(abf, switch_source)
new_dm = compress_datamodel(new_abf)

# Write back — only DataModel changes, rest of PBIX untouched
buf = io.BytesIO()
with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as z_out:
    with zipfile.ZipFile(io.BytesIO(original)) as z_in:
        for item in z_in.infolist():
            if item.filename == 'DataModel':
                z_out.writestr(item.filename, new_dm, compress_type=zipfile.ZIP_STORED)
            else:
                z_out.writestr(item, z_in.read(item.filename))
with open('report.pbix', 'wb') as f:
    f.write(buf.getvalue())
```

Supports all source types: `sqlserver`, `postgresql`, `mysql`, `mariadb`, `sqlite`, `csv`, `excel`, `json`, `azuresql`. Set `is_directquery=True` and `Mode=1` for DirectQuery, or `is_directquery=False` and `Mode=0` for Import.

### Via MCP (Claude / Codex)

Just ask in plain English:

> "List all data sources in my report"

> "Switch the Sales table from SQL Server to PostgreSQL on pg.example.com, database analytics, DirectQuery"

> "Change the CSV path for Products to C:\Data\new_products.csv"

The AI reads the current connections via `pbix_list_data_sources`, then calls `pbix_update_data_source` with the right parameters. No rebuild — only the connection metadata is updated.

### Supported Data Types

| Type | Status | Dictionary Format |
|------|--------|-------------------|
| `String` | Stable | External UTF-16LE with hash table |
| `Int64` | Stable | External 32-bit entries (IsOperatingOn32=1) |
| `Double` | Stable | External 64-bit IEEE 754 entries |
| `DateTime` | Stable | External 64-bit entries (same encoding as Double) |
| `Decimal` | Stable | External 32-bit entries (value x 10000, IsOperatingOn32=1) |
| `Boolean` | Stable | External 32-bit entries (0/1, IsOperatingOn32=1) |

### VertiPaq Binary Format

Every component of the VertiPaq columnar storage engine is independently implemented:

- **IDF** — Bit-packed encoding for data columns (RLE disabled; pure bitpack is slightly less space-efficient but correct)
- **IDFMETA** — Segment statistics with tagged CP/CS/SS/SDOs blocks
- **Dictionary** — Type-specific encoding (Long/Real/String) with hash tables
- **H$ system tables** — Attribute hierarchy POS_TO_ID + ID_TO_POS using NoSplit<32> encoding
- **R$ system tables** — Relationship join INDEX using NoSplit<N> encoding; +3 DATA_ID_OFFSET padding, 1-based row indices into TO table (derived from PBI Desktop ground truth binary comparison)
- **Compression class IDs** — Determined through binary format analysis (u32_a/u32_b selectors)
- **XPress9** — Custom implementation of Power BI's DataModel compression format: reversed chunk framing, header signatures, single-thread and multi-thread container formats. The core XPress9 algorithm uses [xpress9-python](https://github.com/Hugoberry/xpress9-python) as a primitive; the full read/write/modify pipeline is original work
- **ABF** — Full archive generation: STREAM_STORAGE_SIGNATURE, BackupLogHeader, VirtualDirectory, BackupLog XML, data file layout

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
| Sales (Year=2015) | $470,532 | $470,533 | Yes |
| Profit Margin | 13.1% | 13.1% | Yes |
| Sales LY | $484,247 | $484,247 | Yes |
| Sales Change | -2.8% | -2.8% | Yes |
| California Sales | $88,444 | $88,444 | Yes |
| Technology Sales | $162,781 | $162,781 | Yes |

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

**From a fresh clone: 200 tests collected, 173 passed, 27 skipped, 0 failures.** The skipped tests require the public test corpus or private PBIX files. Download the corpus with `python scripts/download_test_corpus.py`, then set `PBIX_TEST_SAMPLES=test_corpus`.

## Architecture

```
PBIX file (ZIP)
├── Version                ← "1.28" UTF-16-LE (8 bytes)
├── [Content_Types].xml    ← OOXML package manifest
├── DiagramLayout          ← JSON: model diagram state
├── Settings               ← JSON: report settings
├── Metadata               ← JSON: file metadata
├── Report/Layout          ← JSON: pages, visuals, filters, data bindings
└── DataModel              ← XPress9 compressed → ABF archive
    ├── BackupLogHeader    ← XML: VDir offset, data offset, file count
    ├── ADDITIONAL_LOG     ← UTF-16: product name
    ├── PARTITIONS         ← UTF-16: partition marker
    ├── db.xml             ← XMLA Load document (28 namespaces)
    ├── CryptKey.bin       ← 144-byte RSA key BLOB (constant)
    ├── metadata.sqlitedb  ← SQLite: 68 system tables (Table, Column, Measure, Relationship, ...)
    ├── *.tbl\*.prt\*.idf  ← VertiPaq: bit-packed column data
    ├── *.idfmeta          ← Segment statistics (CP/CS/SS/SDOs)
    ├── *.dictionary       ← Dictionary encoding (Long/Real/String + hash)
    ├── H$*.tbl\...        ← Attribute hierarchy tables (NoSplit<32>)
    ├── R$*.tbl\...        ← Relationship index tables (NoSplit<N>)
    ├── BackupLog          ← XML: FileGroups, file paths, storage mappings
    └── VirtualDirectory   ← XML: file offsets and sizes
```

### Package Layout

```
src/pbix_mcp/
  server.py              # MCP server (101 tools)
  cli.py                 # Entry point (pbix-mcp-server --log-level debug)
  builder.py             # PBIX builder (metadata, VertiPaq, layout, relationships)
  builder_v2.py          # Template-free ABF + ZIP generation
  errors.py              # Typed exceptions with stable error codes
  logging_config.py      # Diagnostic logging (normal/debug/trace)
  dax/
    engine.py            # DAX evaluator (156 functions, best-effort)
    calc_tables.py       # Calculated table support
  formats/
    abf_rebuild.py       # ABF archive reader and rebuilder
    datamodel_roundtrip.py  # XPress9 compress/decompress
    metadata_schema.py   # SQLite metadata schema (63 tables)
    model_reader.py      # Native VertiPaq table data reader (replaces PBIXRay)
    vertipaq_decoder.py  # VertiPaq IDF/dictionary/HIDX decoder
    vertipaq_encoder.py  # VertiPaq column encoding + NoSplit<N> encoder
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

See [CONTRIBUTING.md](CONTRIBUTING.md) for project conventions, [SUPPORT.md](SUPPORT.md) for what counts as a bug vs unsupported behavior, and [examples/](examples/) for runnable sample scripts.

## Examples

| Script | What it does |
|--------|-------------|
| [`create_showcase.py`](examples/create_showcase.py) | **Full showcase** — 6 tables, 5 relationships, 4 measures, 3 pages, 14 visuals |
| [`create_from_csv.py`](examples/create_from_csv.py) | Build a report from CSV files with Refresh support |
| [`create_from_sqlite.py`](examples/create_from_sqlite.py) | Build a report connected to SQLite database |
| [`create_directquery.py`](examples/create_directquery.py) | Live DirectQuery report connected to SQL Server |
| [`create_star_schema.py`](examples/create_star_schema.py) | Multi-relationship star schema (3 dimensions + 1 fact) |
| [`create_all_types.py`](examples/create_all_types.py) | Demonstrate all 6 data types |

## Roadmap

- **TMDL import** — import models from TMDL files (export already implemented)
- **Composite models** — mixed Import + DirectQuery tables in the same report
- **PBIR layout write** — write reports in the new PBIR format alongside legacy

## Architecture Notes

### Incremental vs Full Rebuild

The builder generates the entire DataModel each time — metadata SQLite, VertiPaq column data, ABF container, and XPress9 compression. All offsets, checksums, and cross-references are computed from first principles.

For **modifying existing PBIX files** (adding a measure, changing a visual), the MCP server operates differently: it opens the file, modifies the specific layer (SQLite metadata for measures, JSON for layout), and repacks — **without touching the VertiPaq binary data**. This is true incremental editing.

| Operation | Approach | Why |
|-----------|----------|-----|
| Create new PBIX | Full build | Every byte generated from code |
| Add/modify measure | Incremental | Only SQLite metadata modified |
| Edit visual/layout | Incremental | Only Report/Layout JSON modified |
| Add table to existing file | Full DataModel rebuild | VertiPaq offsets change |
| Change M code | Incremental | Only DataMashup modified |

### No Microsoft Dependencies

This project is **100% Python** with zero Microsoft DLLs or SDKs. Every layer of the PBIX format — from the ZIP shell to the VertiPaq column encoding — is independently reversed and implemented. The XPress9 compression uses [xpress9-python](https://github.com/Hugoberry/xpress9-python) (MIT) as a low-level primitive; the Power BI DataModel container format (chunk framing, headers, multi-thread support, full read/write/modify round-trip) is original work in `datamodel_roundtrip.py`.

## Purpose & Interoperability

This project is an **independent reimplementation** of the Power BI `.pbix` file format, created for the purpose of **interoperability** — enabling AI agents, automation tools, and non-Windows platforms to create, read, and write Power BI files.

- **No Microsoft source code** was used. All binary format knowledge was derived through independent analysis of file structures and publicly observable behavior.
- **Interoperability rights**: In both the [EU (Directive 2009/24/EC, Article 6)](https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=celex%3A32009L0024) and [US (DMCA 1201(f))](https://www.law.cornell.edu/uscode/text/17/1201), reverse engineering for interoperability purposes is a protected right that supersedes contractual restrictions.
- **Functional specification**: The binary format documentation in [`docs/vertipaq-spec.md`](docs/vertipaq-spec.md) describes functional information (data layouts, compression formats, metadata schemas) necessary for cross-platform compatibility.

This project is not affiliated with, endorsed by, or associated with Microsoft Corporation. "Power BI" and "PBIX" are trademarks of Microsoft.

## License

MIT
