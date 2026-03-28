# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.6.0] - 2026-03-28

### Added
- **Full roundtrip modify** — existing PBIX files can now be heavily modified: add/remove tables, relationships, measures, update table data, add visuals, pages, themes, bookmarks, filters. All DataModel modifications go through the builder pipeline for guaranteed consistency.
- **3 new tools**: `pbix_datamodel_add_relationship`, `pbix_datamodel_remove_relationship`, `pbix_datamodel_remove_table` — add/remove relationships and tables on existing files (72 tools total)
- **`_rebuild_datamodel()` pipeline** — centralized function for all DataModel modifications: supports table updates, new tables, new measures, new relationships, removals, and cascading deletes
- **`pbix_doctor` 17-point diagnostics** — 4 new integrity checks: table/storage consistency, metadata referential integrity, Expression/DataMashup consistency, MAXID validation
- **`ModelReader` work_dir support** — read tools now return fresh data after modifications (not stale original file)

### Fixed
- **`_modify_metadata_sqlite` full builder rebuild** — all metadata-only tools (add_measure, modify_measure, set_rls_role, etc.) now do full DataModel rebuilds via the builder pipeline instead of patching ABFs, which caused TMCacheManager crashes
- **`set_rls_role` bypassed `_modify_metadata_sqlite`** — had its own inline `rebuild_abf_with_replacement` call that produced corrupt ABFs. Now routes through the builder rebuild
- **`rebuild_abf_with_replacement` discovered fundamentally broken** — any post-build ABF modification corrupts the file structure. All roundtrip tools now avoid it entirely
- **Stale `.cpython-310.pyc` bytecode** — MCP used Python 3.10 but bytecode caches prevented code updates from taking effect
- **16 `except Exception` handlers** — `e.message` crash on generic exceptions, fixed to `str(e)`
- **Expression.Kind enum validation** — `set_incremental_refresh` no longer inserts invalid Expression rows
- **`_repack_pbix` excludes `.sqlitedb` files** — prevents stale metadata from corrupting saved PBIX files

### Known Limitations
- **RLS write (set_rls_role)** silently drops Role/TablePermission rows — the builder doesn't generate RLS metadata. Read and evaluate work correctly.
- **Field parameters, calculation groups, incremental refresh** remain blocked — need full DataModel rebuild with VertiPaq storage generation

## [0.5.6] - 2026-03-28

### Fixed
- CI: ruff lint errors (unsorted imports, unused imports in vertipaq_decoder.py)
- CI: mypy baseline updated 158 → 175 for new vertipaq_decoder/model_reader files

## [0.5.5] - 2026-03-28

### Changed
- **Removed PBIXRay dependency**: table data reading now uses a fully native VertiPaq decoder (`vertipaq_decoder.py` + `model_reader.py`). No external binary format dependencies.
- **Removed pandas and kaitaistruct dependencies**: the native decoder handles all IDF, dictionary, and HIDX parsing directly

### Fixed
- **H$ path collision in VertiPaq decoder**: H$ attribute hierarchy files were overwriting real column data during extraction due to path prefix matching. Decoder now correctly separates H$ system tables from data columns.
- **Roundtrip measure add**: adding a measure via `pbix_datamodel_add_measure` now inserts all 12 required Measure fields (FormatString, ModifiedTime, StructureModifiedTime, etc.) and syncs MAXID — measures added through the MCP can be used immediately in PBI Desktop

### Verified
- Full MCP roundtrip: create PBIX → add visuals with data bindings → open in PBI Desktop → visuals populated with data → add new measure interactively → measure evaluates correctly

## [0.5.4] - 2026-03-27

### Fixed
- **PBI Desktop interactivity**: generated PBIX files can now be edited interactively in Power BI Desktop — add measures, columns, and other objects without errors. Two issues fixed:
  - MAXID in metadata was not updated after ID allocation, causing ID conflicts when PBI Desktop tried to create new objects
  - Column metadata updated to match PBI March 2026 schema requirements (ExpressionContext + StringIndexingBehavior fields)
- **Metadata schema updated to 68 tables** (was 63): added BindingInfo, StringIndexStorage, ColumnIndexStorage, DeltaTableColumnStorage, Function, CalendarColumnGroup tables and new columns across existing tables to match PBI March 2026

### Verified
- Generated PBIX files open in PBI Desktop March 2026, display all visuals correctly, AND allow adding new measures/columns interactively
- Northwind showcase: 6 tables, 5 relationships, 14 visuals — all working with full PBI Desktop editing support

## [0.5.0] - 2026-03-26

### Added
- **Template-free PBIX generation**: the entire PBIX binary format is now generated from scratch — ABF binary container (signature, BackupLogHeader, VirtualDirectory, BackupLog), XMLA database document (db.xml with 28 xmlns namespaces), metadata SQLite (63 system tables), VertiPaq column storage, and report layout. Zero templates, zero skeletons.
- **Pre-build validation**: validates tables, columns, measures, relationships, and visuals before generating binary output, with clear error messages
- **Auto-detect relationship direction**: builder automatically detects Many/One sides by checking unique values; swaps From/To to match PBI convention (From=Many, To=One)

### Fixed
- **R$ relationship indexes (ground truth verified)**: R$ INDEX tables now use +3 DATA_ID_OFFSET padding at positions 0-2, with 1-based row indices into the TO table. RecordCount = distinct_FK_values + 3. Verified byte-exact against PBI Desktop ground truth binary. This was the root cause of cross-table relationship lookup failures (wrong/shifted dimension values in visuals).
- **R$ distinct FK count**: R$ RecordCount uses count of distinct FK values (not total row count), derived from analysis of the VertiPaq sparse relationship index initialization
- **H$ attribute hierarchy sort order**: POS_TO_ID/ID_TO_POS now use the same dictionary order as the VertiPaq encoder (sorted for numerics, insertion-order for strings). Mismatch previously caused hierarchy lookup failures.
- **IDF bit_width alignment**: IDFMETA u32_b compression class selector now aligns with the IDF encoding bit width. Both computed from `ceil(log2(distinct_count))`, not `ceil(log2(max_data_id+1))`. Mismatch previously caused `QuerySystemError` crashes on String columns.
- **Dictionary ordering**: String dictionaries use insertion order (matching PBI Desktop behavior); numeric dictionaries use sorted order
- **ColumnStorage statistics**: R$ ColumnStorage uses exact values matching PBI Desktop: distinct=1, min=2, max=2, orig_min=2, rows=0
- **IDFMETA bookmark_bits**: uses row_count (not fixed 24) for data columns

### Verified
- **Northwind Analytics Dashboard**: 6 tables, 36 columns, 5 relationships (including chained Regions→Customers→Orders), 25 rows, 4 DAX measures, 3 pages, 14 visuals — all cross-table lookups correct
- **Binary comparison**: R$ IDF, IDFMETA, SMS RecordCount, ColumnStorage, and DictionaryStorage values match PBI Desktop ground truth byte-for-byte

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
- **NoSplit<N> encoder**: documented binary format for R$ relationship INDEX and H$ hierarchy tables
- **R$ relationship system tables**: cross-table RELATED() and filtering work in PBI Desktop
- **H$ attribute hierarchy tables**: NoSplit<32> POS_TO_ID/ID_TO_POS for DAX dimension support
- **RowNumber AttributeHierarchy**: fixes MDNaiveCoordCell::InitPrototype assertion
- **Template neutralization**: template external file references auto-neutralized on build
- Example scripts in `examples/` directory

### Fixed
- Compression class IDs determined through binary format analysis (u32_a/u32_b selectors)
- DictionaryStorage.IsOperatingOn32=1 for Int64/Decimal/Boolean (was causing PFE_FILESTORE_CORRUPTION)
- Double column support: added "Double" to encoder type mappings
- DirectQuery detection: Mode=1 (not Type=6 which is PolicyRange)
- SMS.Type=2 for H$ tables (was 3, causing DBCC_SEGMENT_CORRUPT)
- Zero-division error when building tables with 0 distinct values

## [0.1.0] - 2026-03-20

### Added
- Initial release as installable Python package (`pip install -e .`)
- 69 MCP tools for full PBIX/PBIT read/write access
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
- ~173 passing tests from fresh clone, ~8 skip without private files
- 19 cross-report integration tests (require private PBIX corpus)
- Public PBIX fixtures (basic_layout, basic_measures)
- SUPPORT.md, CONTRIBUTING.md, issue templates
