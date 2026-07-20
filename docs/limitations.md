# Known Limitations

## DAX Engine

The DAX engine is a **best-effort evaluator** (156 functions, 99.5% accuracy on 204 real-world measures), not a strict Analysis Services runtime.

| Behavior | What happens | Impact |
|----------|-------------|--------|
| Unsupported function | Returns `None`, tracked in `unsupported_functions` | Measure result is `None` with status "unsupported" |
| Circular reference | Raises `DAXEvaluationError`, caught by graceful degradation | Measure result is `None`; no infinite loop |
| Date-table detection | Heuristic: looks for column named "Date" | May pick wrong table in ambiguous models |
| Bare-table iterators | `TOPN`/`RANKX` over a bare table can return BLANK (`SUMX`/`MAXX`/`AVERAGEX`/`MINX`/`COUNTX` handle it correctly as of 0.9.9) | Iterate `TOPN`/`RANKX` over `VALUES()`/`ALL(Table[Col])` where possible |
| Runaway measures | A measure is bounded to a fixed number of sub-expression evaluations; a non-terminating/expansion runaway degrades to BLANK rather than hanging | A single pathologically-slow (e.g. O(n²)) measure can still be slow but no longer hangs indefinitely on runaway expansion |
| Large tables | In-memory Python, no VertiPaq compression | Performance degrades at millions of rows |
| RANKX visual row context | Returns BLANK | 1 out of 204 tested measures affected |

## File Formats

| Format | Support |
|--------|---------|
| .pbix (Import mode) | Full read/write/create. Entire PBIX generated from scratch — no templates. Open, Refresh verified for all database types |
| .pbit (Template) | Full read/write |
| DirectQuery (create) | Open, live data, Refresh all verified — PostgreSQL (native), MySQL (MariaDB ODBC 3.1), SQL Server |
| DirectQuery (open existing) | Read-only for layout/measures/metadata; DAX eval unavailable (data lives in remote source) |
| Composite models | Not tested |
| Live connections | Not supported |
| PBIR layout | Read-only for filter extraction; layout write requires legacy format |

## HTML / CSS / SVG visuals

The bundled `PBIX HTML` custom visual (`pbix_add_html_visual`, see
[html-visuals.md](html-visuals.md)) has these constraints:

| Limitation | Detail |
|--------|---------|
| No external resources | Power BI's visual sandbox blocks all network requests — no `<script src>`, `<link>`, remote `<img>`/font, or `@import`. Inline everything; embed images as base64 `data:` URIs. |
| Content size | The HTML content measure must stay under ~32,000 characters (Analysis Services silently truncates a longer text cell); `pbix_add_html_visual` raises before the limit. |
| Legacy layout only | Embedding the custom visual requires the legacy `Report/Layout` format; the PBIR `Report/definition` format is not yet supported. |
| Cross-filter needs a bound field | `category_field` cross-filtering only affects visuals reachable (through model relationships) from the bound column — same as any native visual. |
| Uncertified | The visual uses `innerHTML` and is intentionally uncertified (not published to AppSource). |

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
| String | ✅ Stable | UTF-16LE with hash table; large dictionaries canonical-Huffman-compressed (MS-XLDM §2.7.4, read + write) |
| Int64 | ✅ Stable | External 32-bit entries (IsOperatingOn32=1) |
| Double | ✅ Stable | External 64-bit IEEE 754 entries |
| DateTime | ✅ Stable | External 64-bit entries (same encoding as Double) |
| Decimal | ✅ Stable | External 32-bit entries (value × 10000, IsOperatingOn32=1) |
| Boolean | ✅ Stable | External 32-bit entries (0/1, IsOperatingOn32=1) |

## Builder

### What is generated from scratch
Every layer of the PBIX is generated from scratch — no templates or skeletons:

- **PBIX ZIP shell**: Version, Content_Types, DiagramLayout, Settings, Metadata — all generated constants
- **ABF binary container**: signature, BackupLogHeader, VirtualDirectory, BackupLog — `build_abf_clean()`
- **XMLA Load document (db.xml)**: 28 xmlns namespaces, CompatibilityLevel=1550 — `generate_db_xml()`
- **CryptKey.bin**: 144-byte RSA key BLOB constant (Microsoft crypto format; GUID-independent)
- **Metadata SQLite**: clean DATASOURCEVERSION=2, 68 system tables — only user-specified tables, columns, and measures
- **VertiPaq column data**: all IDF segments, dictionaries, H$ hierarchy tables, R$ relationship tables. Verified with 6 tables, 36 columns, 5 relationships, 25 rows, 3 pages, 14 visuals (Northwind showcase). Cross-table lookups verified byte-exact against PBI Desktop ground truth.
- **Report layout JSON**: pages, visuals, filters generated from scratch. Supported visuals: table, pieChart, clusteredBarChart, clusteredColumnChart, card, slicer
- **XPress9 compression**: custom compress/decompress with reversed chunk framing and headers

### VertiPaq encoding details
- **IDF bit_width**: computed from `ceil(log2(distinct_count))`, aligned to valid NoSplit widths {1,2,3,4,5,6,7,8,9,10,12,16,21,32}
- **IDFMETA u32_b**: must match IDF bit_width exactly — mismatch causes `QuerySystemError`
- **Dictionary order**: String columns use insertion order; numeric columns use sorted order
- **R$ INDEX**: +3 DATA_ID_OFFSET padding at positions 0-2; values are 1-based row indices into TO table
- **R$ RecordCount**: distinct FK values + 3 (not total FK rows)
- **H$ POS_TO_ID**: must use same sort order as encoder's dictionary
- **RLE encoding disabled**: pure bitpack used for IDF segments — slightly less space-efficient but correct

### Empty tables (`rows=[]`)

Supported as of 0.9.5 — a table with columns but no rows opens and queries in
PBI Desktop (`COUNTROWS` returns blank, `VALUES` returns an empty set). The
zero-row representation mirrors Desktop's own empty-table convention:

- Zero-row partition: `SegmentMapStorage` RecordCount=0, SegmentCount=1,
  RecordsPerSegment=0 (Type=3), Partition Type=4 / Mode=0 / DataView=3
- `AttributeHierarchyStorage.MaterializationType=2` with `DistinctDataCount=0`
  and no H$ system table (Desktop uses MatType=2 for its own zero-row table;
  MatType=3 is reserved for the RowNumber of a *populated* table)
- Empty **string** stores carry **no dictionary page** (`store_page_count=0`).
  A page with `allocation_size=0` has a null character buffer, which Analysis
  Services' string-store consistency check rejects
  (`PFE_XM_DBCC_STRINGSTORE_CORRUPT`) — a store with no strings has no page.

### Relationship semantics
The builder and every datamodel-edit path preserve and can author non-default
relationship traits (verified byte-for-byte against Power BI Desktop-authored
files, and round-tripped: files open with no repair prompt, correct glyphs in
Manage relationships):

| Trait | Encoding | Support |
|-------|----------|---------|
| Active / inactive | `IsActive` 1/0 (storage unchanged) | ✅ author + preserve |
| Cross-filter single / both | `CrossFilteringBehavior` 1/2 (storage unchanged) | ✅ author + preserve |
| Many-to-one (default) | `2→1`, single R$ index | ✅ |
| One-to-many | `1→2`, single R$ index | ✅ author + preserve |
| Many-to-many | `2→2`, **no** storage (`StorageID=0`, no R$ table) | ✅ author + preserve |
| One-to-one | `1→1`, cross-filter forced Both, **two** R$ indexes (`RelationshipStorageID` + `RelationshipStorage2ID`, a forward + reverse mirror R$ table) | ✅ author + preserve (verified byte-for-byte against Desktop; a 1:1 with only the single forward index fails to load) |

Before 0.9.10 any datamodel edit (add measure, modify column, …) silently reset
every relationship to active / single-direction / many-to-one; that data loss is
fixed — existing semantics now survive the rebuild.

### Editing models with calculated or measure-only tables
DataModel-edit tools come in two flavors:

- **Surgical** (edit the metadata in place, no rebuild) — work on **all** models:
  `pbix_datamodel_add_measure`, `modify_measure`, `remove_measure`,
  `modify_column`, `set_rls_role`, `modify_metadata`.
- **Rebuild** (regenerate the whole DataModel from scratch) — `add_relationship`,
  `remove_relationship`, `remove_table`, `add_field_parameter`,
  `add_calculation_group`, `set_table_data`, `update_table_rows`, `replace_value`.

**Measure-only container tables** (a table that holds only measures, e.g. a
`_Measures` table — no data columns) **are preserved** through a rebuild, so the
rebuild tools work on the very common "measures table + import tables" model
shape.

The rebuild tools still can't reproduce **calculated tables** (a `DATATABLE` /
`GENERATESERIES` / DAX-defined table) or **calculated columns** (`Column.Type`
2 or 4 — a DAX column on a normal table, or a calc-table column): Power BI
computes their VertiPaq data from a DAX expression, and the builder can't
recompute it — a rebuild would reopen those tables empty or drop the calculated
column. Rather than corrupt the file, a rebuild tool **refuses with a clear
error** (`MODEL_EDIT_UNSUPPORTED`) that names the offending tables and points at
the surgical tools (which work on all models). Full support for editing models
with **calculated** tables/columns needs verbatim VertiPaq preservation (copying
the computed column bytes through untouched, rather than re-encoding them) and is
a tracked follow-up.

### Other builder notes
- Fixed RowNumber GUID: 2662979B-1795-4F74-8F37-6A1BA8059B61
- Relationship direction: default many-to-one auto-detects Many/One sides
  (From=Many/fact, To=One/dimension); explicit cardinality or the preservation
  path keeps the caller's / source file's orientation verbatim
- Key annotations: PBI_IsFromSource (ObjectType=7), PBI_ResultType, SummarizationSetBy, PBI_QueryOrder, __PBI_TimeIntelligenceEnabled
- M expression navigation uses `Item` key (not `Name`) for MySQL/PostgreSQL

## Performance

The DAX engine operates on in-memory Python data structures. For large tables:
- Tables are loaded fully into memory as Python lists
- No columnar compression or VertiPaq-style optimization
- Expect degraded performance above ~100K rows
- Consider using filter context to limit evaluated data
