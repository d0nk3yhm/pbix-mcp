# Support

## Supported Environments

| | Status |
|---|--------|
| Python 3.10+ | Supported |
| Windows | Primary development platform |
| macOS / Linux | Should work, not regularly tested |
| .pbix files | Fully supported |
| .pbit files | Supported (template format) |

## Feature Stability

### Stable
- PBIX creation from scratch (with actual row data)
- File open/close/save/repack (auto-backup, force flags)
- Report layout read/write (pages, visuals, filters, positions)
- Visual add/remove (cards, charts, shapes, images, textboxes, slicers)
- DAX measure read/write/evaluate (156 functions, best-effort)
- Calculated table evaluation (DATATABLE, GENERATESERIES, CALENDAR, field parameters)
- Metadata SQL read/write
- Table data read (native VertiPaq decoder)
- VertiPaq table data write (String, Int64, Double, DateTime, Decimal, Boolean)
- ABF archive manipulation (list, extract, replace, build from scratch)
- DataMashup (M code) read/write
- XPress9 DataModel decompress/recompress (byte-exact round-trip)

### Beta
- Calculated column evaluation (per-row DAX; tested with synthetic data only)
- Row-Level Security read/write/evaluate (implemented, limited test coverage)
- Password extraction from protected dashboards (regex-based, limited test coverage)
- Diagnostic health check (`pbix_doctor`) (implemented, limited test coverage)

### Known Limitations
- DAX engine is best-effort, not a strict runtime — unsupported functions return `None` with status `"unsupported"`, circular references raise `DAXEvaluationError`
- PBIR format is read-only for filter extraction (no layout write)
- Performance — tables >100K rows trigger a warning; the engine operates on in-memory Python data
- **Creating DirectQuery files** — fully working with SQL Server, PostgreSQL 16, and MySQL 9.6 (via MariaDB adapter)
- **Opening existing DirectQuery files** — layout, measures, and metadata editing work; DAX evaluation and table reads return clear errors since data lives in the remote source
- 1 out of 204 tested measures returns BLANK (requires per-employee RANKX visual row context)

## Bug Reports

Use the [GitHub issue tracker](https://github.com/d0nk3yhm/pbix-mcp/issues).

A good bug report includes:
- Package version (`pip show pbix-mcp`)
- Python version
- OS
- The MCP tool that failed
- Error message or unexpected output
- Whether a public fixture reproduces it

## What Counts as a Bug vs Unsupported

**Bug**: A documented feature produces wrong results or crashes.

**Unsupported**: A DAX function, encoding type, or file variant that is not listed as supported.
If something is unsupported, you'll see a clear error message rather than silent failure.
