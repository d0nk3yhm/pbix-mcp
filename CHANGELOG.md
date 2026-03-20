# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-20

### Added
- Initial release as installable Python package (`pip install -e .`)
- 60 MCP tools for full PBIX/PBIT read/write access
- `pbix_create`: build PBIX files from scratch with actual row data
- `pbix_add_visual` / `pbix_remove_visual`: visual management
- `pbix_get_rls_roles` / `pbix_set_rls_role` / `pbix_evaluate_rls`: Row-Level Security
- `pbix_get_password`: extract embedded passwords from protected dashboards
- `pbix_doctor`: 8-point diagnostic health check
- DAX evaluation engine with 154 functions (99.5% non-BLANK across 204 measures)
- Calculated table support (DATATABLE, GENERATESERIES, CALENDAR, field parameters)
- Calculated column evaluation (per-row DAX expressions)
- Auto-applied default slicer filters from report layout (legacy + PBIR)
- Smart SELECTEDVALUE/ISFILTERED fallback for parameter-driven measures
- VertiPaq table data read/write (String, Int64, Double, DateTime, Decimal)
- XPress9 DataModel decompress/recompress (byte-exact round-trip)
- ABF archive build from scratch / manipulation
- PBIXBuilder: programmatic PBIX creation with row data
- Pydantic response models (ToolResponse, DAXEvalResponse, DAXResult)
- Typed exception hierarchy (errors.py)
- Diagnostic logging (normal/debug/trace via PBIX_MCP_LOG_LEVEL)
- CI pipeline for Python 3.10-3.13
- 138 unit/golden/fixture tests, 19 cross-report integration tests
- Public PBIX fixtures (basic_layout, basic_measures, from_scratch, with_data)
- SUPPORT.md, CONTRIBUTING.md, issue templates
