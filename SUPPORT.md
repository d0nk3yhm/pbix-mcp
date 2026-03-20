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
- File open/close/save/repack
- Report layout read/write
- Visual property read/write
- DAX measure read/write/evaluate (154 functions)
- Metadata SQL read/write
- Table data read (via PBIXRay)

### Experimental
- VertiPaq table data write (works but limited type support)
- ABF direct file manipulation
- DataMashup (M code) editing

### Known Limitations
- Calculated columns are not evaluated (only calculated tables)
- Row-level security expressions are not evaluated
- Some DAX functions may return approximate results for edge cases
- VertiPaq write does not support all column encoding types
- PBIR format is read-only for filter extraction (no layout write)

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
