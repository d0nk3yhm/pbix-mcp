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
- DAX measure read/write/evaluate (154 functions, 99.5% non-BLANK)
- Calculated table evaluation (DATATABLE, GENERATESERIES, CALENDAR, field parameters)
- Calculated column evaluation (per-row DAX expressions)
- Row-Level Security read/write/evaluate
- Metadata SQL read/write
- Table data read (via PBIXRay)
- VertiPaq table data write (String, Int64, Double, DateTime, Decimal)
- ABF archive manipulation (list, extract, replace, build from scratch)
- DataMashup (M code) read/write
- XPress9 DataModel decompress/recompress (byte-exact round-trip)
- Password extraction from protected dashboards
- Diagnostic health check (pbix_doctor)

### Known Limitations
- PBIR format is read-only for filter extraction (no layout write)
- VertiPaq Boolean column type not yet supported
- Performance degrades on large tables (millions of rows) — in-memory Python
- Import mode only — DirectQuery, composite models, live connections not supported
- Created PBIX files may need a refresh in Power BI Desktop to fully index data
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
