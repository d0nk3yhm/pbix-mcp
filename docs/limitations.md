# Known Limitations

## DAX Engine

The DAX engine is a **best-effort evaluator** (156 functions, 99.5% accuracy on 204 real-world measures), not a strict Analysis Services runtime.

| Behavior | What happens | Impact |
|----------|-------------|--------|
| Unsupported function | Returns `None`, tracked in `unsupported_functions` | Measure result is `None` with status "unsupported" |
| Circular reference | Returns 0 | Incorrect result, no error raised |
| Date-table detection | Heuristic: looks for column named "Date" | May pick wrong table in ambiguous models |
| SUMX with infix arithmetic | `SUMX(T, Col1 * Col2)` returns 0 | Row-level arithmetic parsing is limited |
| Large tables | In-memory Python, no VertiPaq compression | Performance degrades at millions of rows |
| RANKX visual row context | Returns BLANK | 1 out of 204 tested measures affected |

## File Formats

| Format | Support |
|--------|---------|
| .pbix (Import mode) | Full read/write/create from scratch |
| .pbit (Template) | Full read/write |
| DirectQuery (create) | ✅ Supported — SQL Server verified, MySQL/PostgreSQL same pattern |
| DirectQuery (open existing) | Read-only for layout/measures/metadata; DAX eval unavailable (data lives in remote source) |
| Composite models | Not tested |
| Live connections | Not supported |
| PBIR layout | Read-only for filter extraction; layout write requires legacy format |

## Data Sources (from-scratch creation)

| Source | Import Mode | DirectQuery | Notes |
|--------|------------|-------------|-------|
| Embedded data | ✅ | N/A | Default — data in PBIX, no external source |
| CSV files | ✅ | N/A | `source_csv` — Refresh re-imports from CSV |
| SQLite | ✅ | N/A | `source_db` — requires SQLite3 ODBC driver |
| SQL Server | ✅ | ✅ | `source_db` — verified with LocalDB |
| MySQL | ✅ | ❌ N/A | `source_db` — verified with MySQL 9.6. DirectQuery not supported by built-in MySQL connector (use MariaDB adapter) |
| PostgreSQL | ✅ | ✅ | `source_db` — verified with PostgreSQL 16. Import and DirectQuery both working |

## Supported Data Types

| Type | Status | Dictionary Format |
|------|--------|-------------------|
| String | ✅ Stable | External UTF-16LE with hash table |
| Int64 | ✅ Stable | External 32-bit entries (IsOperatingOn32=1) |
| Double | ✅ Stable | External 64-bit IEEE 754 entries |
| DateTime | ✅ Stable | External 64-bit entries (same encoding as Double) |
| Decimal | ✅ Stable | External 32-bit entries (value × 10000, IsOperatingOn32=1) |
| Boolean | ✅ Stable | External 32-bit entries (0/1, IsOperatingOn32=1) |

## Builder

- Template-based: new tables are added alongside the template's existing `financials` table
- Template external file references are auto-neutralized on build (prevents refresh errors)
- H$ attribute hierarchy tables use hardcoded template bytes for 2-distinct-value columns; larger cardinalities use MatType=3 (functional but no sorted dimension browsing)
- Power BI Desktop may need a refresh to fully index data after opening a from-scratch PBIX

## Performance

The DAX engine operates on in-memory Python data structures. For large tables:
- Tables are loaded fully into memory as Python lists
- No columnar compression or VertiPaq-style optimization
- Expect degraded performance above ~100K rows
- Consider using filter context to limit evaluated data
