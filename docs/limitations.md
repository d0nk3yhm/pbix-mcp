# Known Limitations

## DAX Engine

The DAX engine is a **best-effort evaluator**, not a strict Analysis Services runtime.

| Behavior | What happens | Impact |
|----------|-------------|--------|
| Unsupported function | Returns `None`, tracked in `unsupported_functions` | Measure result is `None` with status "unsupported" |
| Circular reference | Returns 0 | Incorrect result, no error raised |
| Date-table detection | Heuristic: looks for column named "Date" | May pick wrong table in ambiguous models |
| SUMX with infix arithmetic | `SUMX(T, Col1 * Col2)` returns 0 | Row-level arithmetic parsing is limited |
| Large tables | In-memory Python, no VertiPaq compression | Performance degrades at millions of rows |

## File Formats

| Format | Support |
|--------|---------|
| .pbix (Import mode) | Full read/write |
| .pbit (Template) | Full read/write |
| DirectQuery | Not supported — detected and error returned |
| Composite models | Not supported |
| Live connections | Not supported |
| PBIR layout | Read-only for filter extraction; layout write requires legacy format |

## VertiPaq Write

| Column Type | Support |
|-------------|---------|
| String | Supported |
| Int64 | Supported |
| Double | Supported |
| DateTime | Supported |
| Decimal | Supported |
| Boolean | Supported |

## Builder

- `[Content_Types].xml` includes Override entries for all components
- DataMashup is included as a minimal placeholder
- Power BI Desktop may need a refresh to fully index data after opening a from-scratch PBIX

## Performance

The DAX engine operates on in-memory Python data structures. For large tables:
- Tables are loaded fully into memory as Python lists
- No columnar compression or VertiPaq-style optimization
- Expect degraded performance above ~100K rows
- Consider using filter context to limit evaluated data
