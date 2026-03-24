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
| .pbix (Import mode) | Full read/write/create. Metadata/data/layout from scratch; ABF container uses template skeleton. Open, Refresh verified for all database types |
| .pbit (Template) | Full read/write |
| DirectQuery (create) | Open, live data, Refresh all verified — PostgreSQL (native), MySQL (MariaDB ODBC 3.1), SQL Server |
| DirectQuery (open existing) | Read-only for layout/measures/metadata; DAX eval unavailable (data lives in remote source) |
| Composite models | Not tested |
| Live connections | Not supported |
| PBIR layout | Read-only for filter extraction; layout write requires legacy format |

## Data Sources (from-scratch creation)

| Source | Import Mode | DirectQuery | Notes |
|--------|------------|-------------|-------|
| Embedded data | Open ✅, Refresh N/A | N/A | Default — data in PBIX, no external source |
| CSV files | Open ✅, Refresh ✅ | N/A | `source_csv` — Refresh re-imports from CSV |
| SQLite | Open ✅, Refresh ✅ | N/A | `source_db` — requires SQLite3 ODBC driver |
| SQL Server | Open ✅, Refresh ✅ | Open ✅, Live ✅, Refresh ✅ | `source_db` — verified with LocalDB |
| MySQL | Open ✅, Refresh ✅ | Open ✅, Live ✅, Refresh ✅ | DirectQuery requires MariaDB ODBC 3.1 driver (`type: 'mariadb'`) |
| PostgreSQL | Open ✅, Refresh ✅ | Open ✅, Live ✅, Refresh ✅ | `source_db` — verified with PostgreSQL 16 (native DirectQuery) |
| Excel | Open ✅, Refresh ✅ | N/A | `source_db` with `type: 'excel'` |
| JSON/API | Open ✅, Refresh ✅ | N/A | `source_db` with `type: 'json'` |
| Azure SQL | Open ✅, Refresh ✅ | Open ✅, Live ✅, Refresh ✅ | `source_db` with `type: 'azuresql'` |

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

### What is generated from scratch
- **Metadata SQLite**: clean DATASOURCEVERSION=2 — only user-specified tables, columns, and measures, no template data
- **VertiPaq column data**: all IDF segments, dictionaries, H$ hierarchy tables, R$ relationship tables generated independently. Verified with 4 tables, 8 columns, 3 relationships
- **Report layout JSON**: pages, visuals, filters generated from scratch. Supported visuals: table, pieChart, clusteredBarChart, card, slicer

### What uses a template skeleton
- **ABF binary container**: the ABF container format has not been fully reverse-engineered for from-scratch generation. The template skeleton provides the system file structure (db.xml, CryptKey, BackupLog format) that msmdsrv requires for database restore
- **Template dead weight**: the template's Financial Sample VertiPaq files are still physically present in the ABF but are ignored by the clean metadata — they add ~600KB of dead weight
- **PBIX OPC wrapper**: template files provide the ZIP/OPC structure

### Other builder notes
- **RLE encoding disabled**: pure bitpack used for IDF segments — slightly less space-efficient but correct
- Fixed RowNumber GUID: 2662979B-1795-4F74-8F37-6A1BA8059B61
- Relationship direction: From=Many (fact), To=One (dimension) matching PBI convention
- Key annotations: PBI_IsFromSource (ObjectType=7), PBI_ResultType, SummarizationSetBy, PBI_QueryOrder, __PBI_TimeIntelligenceEnabled
- M expression navigation uses `Item` key (not `Name`) for MySQL/PostgreSQL
- Power BI Desktop may need a refresh to fully index data after opening a created PBIX

## Performance

The DAX engine operates on in-memory Python data structures. For large tables:
- Tables are loaded fully into memory as Python lists
- No columnar compression or VertiPaq-style optimization
- Expect degraded performance above ~100K rows
- Consider using filter context to limit evaluated data
