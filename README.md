# Power BI PBIX MCP Server

A Model Context Protocol (MCP) server that gives AI assistants (Claude, Codex, etc.) **full read/write access to every layer** of Power BI `.pbix` and `.pbit` files — including a **built-in DAX evaluation engine**.

## What It Does

This MCP server treats PBIX files as structured containers and exposes **51 tools** for granular manipulation of every component — from report layout and visuals down to individual VertiPaq column data, DAX measure expressions, and **live DAX computation**.

**Every single byte is accessible. Every layer is writable. Every measure is computable.**

### Layer Coverage

| Layer | Read | Write | Details |
|-------|------|-------|---------|
| ZIP Structure | ✅ | ✅ | Extract, repack, SecurityBindings auto-cleanup |
| Report Layout | ✅ | ✅ | Pages, visuals, positions, configs |
| Visual Properties | ✅ | ✅ | Any property via dot-path or full JSON |
| Visual Positions | ✅ | — | Parent group offset resolution (absolute coords) |
| Report Filters | ✅ | ✅ | Report-level, page-level, and default slicer state |
| Settings | ✅ | ✅ | Report configuration |
| Themes | ✅ | ✅ | Read/write theme JSON |
| Resources & Images | ✅ | ✅ | List and replace via ABF |
| Bookmarks | ✅ | ✅ | Via layout JSON |
| Linguistic Schema | ✅ | ✅ | Q&A language config |
| DataMashup (M Code) | ✅ | ✅ | Power Query expressions |
| DataModel (XPress9) | ✅ | ✅ | Byte-exact decompress/recompress |
| ABF Archive | ✅ | ✅ | List, extract, replace any internal file |
| Metadata SQLite | ✅ | ✅ | Full SQL read/write access |
| DAX Measures | ✅ | ✅ | Add, modify, remove |
| **DAX Evaluation** | ✅ | — | **Compute any measure with filter context** |
| Column Properties | ✅ | ✅ | Via metadata SQL |
| Relationships | ✅ | ✅ | Via metadata SQL |
| **VertiPaq Table Data** | ✅ | ✅ | **Read and write actual row data** |
| Power Query (model) | ✅ | ✅ | Via metadata SQL |

## Quick Start

### Prerequisites

```bash
pip install mcp pbixray xpress9 pandas kaitaistruct apsw
```

### Install for Claude Desktop

Add to your Claude Desktop config (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "python",
      "args": ["F:/PowerBI_MCP_editor/pbix_mcp_server.py"]
    }
  }
}
```

### Install for Claude Code

Add to your project's `.claude/settings.json`:

```json
{
  "mcpServers": {
    "powerbi": {
      "command": "python",
      "args": ["pbix_mcp_server.py"],
      "cwd": "/path/to/PowerBI_MCP_editor"
    }
  }
}
```

### Install for Codex / Other MCP Clients

Run the server directly:

```bash
python pbix_mcp_server.py
```

The server communicates via stdio using the MCP JSON-RPC protocol.

## Tools Reference

### File Management (4 tools)
- `pbix_open` — Open a .pbix/.pbit file for editing
- `pbix_save` — Save/repack modified file
- `pbix_close` — Close file and clean up
- `pbix_list_open` — List all open files

### Report Layout (16 tools)
- `pbix_get_pages` / `pbix_add_page` / `pbix_remove_page`
- `pbix_get_page_visuals` / `pbix_get_visual_detail`
- `pbix_get_visual_positions` — **Absolute positions with parent group resolution**
- `pbix_set_visual_property` / `pbix_update_visual_json`
- `pbix_get_layout_raw` / `pbix_set_layout_raw`
- `pbix_get_filters` / `pbix_set_filters`
- `pbix_get_default_filters` — **Extract default slicer selections**
- `pbix_get_settings` / `pbix_set_settings`
- `pbix_get_bookmarks` / `pbix_get_metadata`

### Resources & Theme (4 tools)
- `pbix_list_resources` / `pbix_get_theme` / `pbix_set_theme`
- `pbix_get_linguistic_schema` / `pbix_set_linguistic_schema`

### DataMashup (2 tools)
- `pbix_get_m_code` / `pbix_set_m_code` — Power Query M expressions

### DataModel Read (7 tools)
- `pbix_get_model_schema` / `pbix_get_model_measures`
- `pbix_get_model_relationships` / `pbix_get_model_power_query`
- `pbix_get_model_columns` / `pbix_get_table_data` / `pbix_list_tables`

### DAX Evaluation Engine (3 tools) — NEW
- `pbix_evaluate_dax` — **Evaluate any DAX measure with optional filter context**
- `pbix_evaluate_dax_per_dimension` — **Evaluate measures per dimension value (e.g., Sales per State)**
- `pbix_clear_dax_cache` — Clear cached data for fresh evaluation

### DataModel Write (13 tools)
- `pbix_datamodel_query_metadata` — Run SQL on metadata
- `pbix_datamodel_modify_metadata` — Execute SQL DDL/DML
- `pbix_datamodel_add_measure` / `pbix_datamodel_modify_measure` / `pbix_datamodel_remove_measure`
- `pbix_datamodel_modify_column`
- `pbix_datamodel_decompress` / `pbix_datamodel_recompress`
- `pbix_datamodel_replace_file` / `pbix_datamodel_extract_file`
- `pbix_datamodel_list_abf_files`
- `pbix_set_table_data` — **Write actual row data (VertiPaq)**
- `pbix_update_table_rows` — Update rows inferring schema from existing table

## DAX Evaluation Engine

The built-in DAX engine can compute any measure expression against the embedded VertiPaq data, supporting:

**30+ DAX functions:**
- Aggregation: `SUM`, `AVERAGE`, `COUNT`, `COUNTROWS`, `MIN`, `MAX`, `DISTINCTCOUNT`
- Iteration: `SUMX`, `MAXX`, `FILTER`
- Math: `DIVIDE`, `ABS`, `ROUND`, `INT`
- Logic: `IF`, `SWITCH`, `AND`, `OR`, `NOT`, `ISBLANK`
- Time Intelligence: `CALCULATE`, `DATEADD`, `SAMEPERIODLASTYEAR`, `REMOVEFILTERS`
- Filter: `ALL`, `ALLSELECTED`, `VALUES`, `SELECTEDVALUE`
- Text: `CONCATENATE`, `FORMAT`
- Variables: `VAR` / `RETURN` blocks with scope management

**Relationship-based filter propagation:**
- Automatically propagates dimension filters through model relationships (star-schema)
- Supports date dimension → fact table joins (Year/Month/Date filters)
- Works with any dimension: Geography, Product, Customer, etc.

### Usage Examples

#### Evaluate measures with a filter
```
> pbix_evaluate_dax("my_report", "Sales,Profit Margin,Sales LY", '{"dim-Date.Year": [2015]}')

DAX Evaluation Results (3 measures):
  Sales: $470,532.51
  Profit Margin: 13.1%
  Sales LY: $484,247.50
```

#### Evaluate per dimension (e.g., Sales by State)
```
> pbix_evaluate_dax_per_dimension("my_report", "Sales,Sales LY", "dim-Geo.State", '{"dim-Date.Year": [2015]}', 5)

DAX per dim-Geo.State (49 values, showing 5):

Value                              Sales         Sales LY
---------------------------------------------------------
California                       88,443.84        91,303.53
New York                         80,310.27        64,841.78
Texas                            34,531.11        50,700.72
```

#### Get default slicer filters
```
> pbix_get_default_filters("my_report")

Default slicer filters:
  dim-Date.Year: [2015]
  Trend vs. Table.Toggle: ['Bar']

Use as filter_context in pbix_evaluate_dax:
  {"dim-Date.Year": [2015], "Trend vs. Table.Toggle": ["Bar"]}
```

#### Get visual positions with group resolution
```
> pbix_get_visual_positions("my_report", 0)

Visual positions (absolute, 33 visuals):
  [0] cardVisual              at (1004,60) 101x322
  [10] card                   at (1086,109) 42x14  [child of group]
```

## Other Usage Examples

### Open a file and inspect it

```
> pbix_open("report.pbix", "my_report")
> pbix_get_pages("my_report")
> pbix_list_tables("my_report")
> pbix_get_table_data("my_report", "Sales", 10)
```

### Modify DAX measures

```
> pbix_datamodel_add_measure("my_report", "Sales", "Total Revenue", "SUM(Sales[Amount])")
> pbix_datamodel_modify_measure("my_report", "Total Revenue", "SUMX(Sales, Sales[Qty] * Sales[Price])")
```

### Write table data

```
> pbix_set_table_data("my_report", "Currency", '{"columns": [{"name": "Code", "data_type": "String", "nullable": false}], "rows": [{"Code": "USD"}, {"Code": "EUR"}]}')
```

### Run SQL on the metadata

```
> pbix_datamodel_query_metadata("my_report", "SELECT Name, Expression FROM Measure")
> pbix_datamodel_modify_metadata("my_report", "UPDATE [Table] SET Description='Modified' WHERE Name='Sales'")
```

### Save

```
> pbix_save("my_report", "modified_report.pbix")
```

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

### Modules

| File | Purpose |
|------|---------|
| `pbix_mcp_server.py` | MCP server — 51 tools for full PBIX read/write + DAX evaluation |
| `dax_engine.py` | DAX expression evaluator with 30+ functions and relationship propagation |
| `datamodel_roundtrip.py` | XPress9 decompress/compress for DataModel |
| `abf_rebuild.py` | ABF archive format — read, modify, rebuild |
| `vertipaq_encoder.py` | VertiPaq column encoder — IDF, IDFMETA, dictionary, HIDX |

## How It Works

1. **Open**: Extracts PBIX ZIP to a temp directory
2. **Read/Modify**: Operates on extracted components (JSON, SQLite, binary)
3. **DAX Evaluation**: Loads VertiPaq data via PBIXRay, evaluates expressions with relationship-based filter propagation
4. **DataModel writes**: Decompress XPress9 → parse ABF → modify → rebuild ABF → recompress
5. **VertiPaq writes**: Encode column data (dictionary + RLE/bit-packed IDF) → replace in ABF
6. **Save**: Repack everything into a valid PBIX ZIP (SecurityBindings auto-removed)

## Requirements

- Python 3.10+
- `mcp` >= 1.0.0 (Model Context Protocol SDK)
- `pbixray` >= 0.5.0 (PBIX decompression and reading)
- `xpress9` (XPress9 compression/decompression)
- `pandas` (data handling)
- `kaitaistruct` (binary format parsing)
- `apsw` (SQLite for pbixray)

## License

MIT
