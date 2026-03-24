# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0] - 2026-03-24

### Added
- **From-scratch metadata generation**: DATASOURCEVERSION=2, clean SQLite metadata — output files contain only user-specified tables/columns/measures. Note: the ABF binary container still uses a template skeleton for system files (db.xml, CryptKey.bin, BackupLog format); only metadata, VertiPaq data, and layout are generated from scratch
- **Excel data source**: `source_db={'type': 'excel', ...}` for Import mode
- **JSON/API data source**: `source_db={'type': 'json', ...}` for Import mode from REST APIs and JSON files
- **Azure SQL data source**: `source_db={'type': 'azuresql', ...}` for Import and DirectQuery
- **Key PBI annotations**: PBI_IsFromSource (ObjectType=7), PBI_ResultType, SummarizationSetBy, PBI_QueryOrder, __PBI_TimeIntelligenceEnabled — matching PBI Desktop output
- **Fixed RowNumber GUID**: 2662979B-1795-4F74-8F37-6A1BA8059B61 for stable attribute hierarchy references
- **Relationship direction convention**: From=Many (fact), To=One (dimension) matching PBI Desktop convention
- **M expression Item key navigation**: MySQL/PostgreSQL use `Item` key (not `Name`) for table navigation in M expressions

### Fixed
- VertiPaq encoder verified working with 6 tables, 36 columns, 5 relationships, 25 rows, 3 pages, 14 visuals (Northwind showcase)
- All 5 visual types verified: table, pieChart, clusteredBarChart, card, slicer
- DirectQuery Refresh verified for PostgreSQL (native), MySQL (via MariaDB ODBC 3.1), SQL Server
- Import mode Refresh verified for all database types
- Test suite: 173 passed, 27 skipped, 0 failures

## [0.3.0] - 2026-03-23

### Added
- **Bookmark creation**: `pbix_add_bookmark` / `pbix_remove_bookmark` — page targeting, visual visibility state
- **Field Parameters**: `pbix_datamodel_add_field_parameter` — slicer-driven column/measure switchers
- **Calculation Groups**: `pbix_datamodel_add_calculation_group` — dynamic measure modifiers (YTD, QTD, PY)
- **TMDL Export**: `pbix_export_tmdl` — export data model as Git-friendly text files
- **Custom Visuals**: `pbix_add_custom_visual` / `pbix_remove_custom_visual` — import .pbiviz packages, register in resourcePackages, place with `pbix_add_visual`
- **Incremental Refresh**: `pbix_set_incremental_refresh` / `pbix_get_incremental_refresh` — configure date-based partition policies with archive/refresh windows, change detection, and hybrid mode support
- **PostgreSQL data source**: `source_db={'type': 'postgresql', ...}` — verified with PostgreSQL 16

### Fixed
- MySQL/PostgreSQL M expression navigation key: `Name` → `Item` (fixes "key didn't match any rows" on Refresh)

## [0.2.0] - 2026-03-23

### Added
- **DirectQuery mode**: `mode='directquery'` creates live database connections (SQL Server verified with LocalDB)
- **SQL Server data source**: `source_db={'type': 'sqlserver', ...}` for Import and DirectQuery
- **SQLite data source**: `source_db={'type': 'sqlite', ...}` with ODBC driver
- **MySQL data source**: `source_db={'type': 'mysql', ...}` — verified with MySQL 9.6
- **PostgreSQL data source**: `source_db={'type': 'postgresql', ...}` — verified with PostgreSQL 16
- **CSV refreshable sources**: `source_csv` parameter creates M expressions for Refresh in PBI Desktop
- **Boolean data type**: full support (IsOperatingOn32=1, 0/1 values)
- **Decimal data type**: full support (value × 10000, IsOperatingOn32=1)
- **NoSplit<N> encoder**: reverse-engineered binary format for R$ relationship INDEX and H$ hierarchy tables
- **R$ relationship system tables**: cross-table RELATED() and filtering work in PBI Desktop
- **H$ attribute hierarchy tables**: NoSplit<32> POS_TO_ID/ID_TO_POS for DAX dimension support
- **RowNumber AttributeHierarchy**: fixes MDNaiveCoordCell::InitPrototype assertion
- **Template neutralization**: template external file references auto-neutralized on build
- Example scripts in `examples/` directory

### Fixed
- Compression class IDs fully reverse-engineered from xmsrv.dll via Ghidra (u32_a/u32_b selectors)
- DictionaryStorage.IsOperatingOn32=1 for Int64/Decimal/Boolean (was causing PFE_FILESTORE_CORRUPTION)
- Double column support: added "Double" to encoder type mappings
- DirectQuery detection: Mode=1 (not Type=6 which is PolicyRange)
- SMS.Type=2 for H$ tables (was 3, causing DBCC_SEGMENT_CORRUPT)
- Zero-division error when building tables with 0 distinct values

## [0.1.0] - 2026-03-20

### Added
- Initial release as installable Python package (`pip install -e .`)
- 60 MCP tools for full PBIX/PBIT read/write access
- `pbix_create`: build PBIX files from scratch with actual row data
- `pbix_add_visual` / `pbix_remove_visual`: visual management
- `pbix_get_rls_roles` / `pbix_set_rls_role` / `pbix_evaluate_rls`: Row-Level Security (beta)
- `pbix_get_password`: extract embedded passwords (beta)
- `pbix_doctor`: 8-point diagnostic health check (beta)
- DAX evaluation engine with 156 functions (best-effort evaluator)
- Calculated table support (DATATABLE, GENERATESERIES, CALENDAR, field parameters)
- Calculated column evaluation (beta; per-row DAX expressions)
- Auto-applied default slicer filters from report layout (legacy + PBIR)
- Smart SELECTEDVALUE/ISFILTERED fallback for parameter-driven measures
- VertiPaq table data read/write (String, Int64, Double, DateTime, Decimal)
- XPress9 DataModel decompress/recompress (byte-exact round-trip)
- ABF archive manipulation (template skeleton used for system files; user data injected from scratch)
- PBIXBuilder: programmatic PBIX creation with row data
- Pydantic response models (ToolResponse, DAXEvalResponse, DAXResult)
- Typed exception hierarchy (errors.py with 12 exception classes)
- Diagnostic logging (normal/debug/trace via PBIX_MCP_LOG_LEVEL or --log-level)
- CI pipeline for Python 3.10-3.13 (ruff, mypy, pytest, coverage)
- ~163 passing tests from fresh clone, ~8 skip without private files
- 19 cross-report integration tests (require private PBIX corpus)
- Public PBIX fixtures (basic_layout, basic_measures)
- SUPPORT.md, CONTRIBUTING.md, issue templates
