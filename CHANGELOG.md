# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.6] - 2026-07-17

### Fixed
- **DAX: filters on multi-hop (snowflake) dimensions were silently dropped.** A filter on a dimension two or more hops from the fact table (e.g. `Regions → Customers → Orders`) found no direct relationship and was dropped, so every group returned the unfiltered grand total. The engine now walks the relationship graph (`DAXContext._find_rel_path` / `_propagate_filter_path`) and propagates the filter hop by hop. The traversal honours the default single cross-filter direction (one→many), so a filter cannot leak across a shared fact to a sibling dimension, and an empty intermediate key-set now yields zero rows (BLANK) instead of the grand total. The direct single-hop/star-schema path is unchanged. (found while building OpenBI)
- **`pbix_save` cleared the `modified` flag when exporting a copy.** Saving to a different `output_path` marked the session clean even though the original file never received the edits — a subsequent `pbix_close` (without `force`) then silently discarded the work-dir changes. The flag is now cleared only when the save targets the original file path. (found while building OpenBI)
- **`pbix_get_default_filters` returned a bare, non-JSON string on success**, breaking the response envelope every other tool honours (a client's `json.loads` failed on success but worked on error). It now returns a `ToolResponse` envelope and exposes the parsed selections in `data.filters`. (found while building OpenBI)
- **Grouped visuals: absolute coordinates were written into `singleVisualGroup` children.** `pbix_add_visual` stored page-absolute `x`/`y` even when the visual declared a `parentGroupName`, whereas the read side (`pbix_get_visual_positions`) treats a grouped child's coordinates as group-relative — so positions round-tripped incorrectly. The write side now converts absolute coordinates to group-relative when a matching `singleVisualGroup` parent exists. (found while building OpenBI)

### Security
- **Hardened PBIX/ZIP extraction against decompression bombs and path traversal.** `_extract_pbix` now validates every archive member up front (`_validate_zip_members`): total and per-file uncompressed-size caps, a per-member compression-ratio guard, a member-count cap, a realpath containment check, and rejection of symlink entries — a malicious `.pbix` is refused before any byte is written. (Python's `extractall` already strips `..`/absolute paths and never materialises symlinks; these limits add the missing size caps and defence-in-depth containment.)

### Verified
- Multi-hop propagation covered by a 3-table snowflake unit suite (two-hop filter applies, distinct-per-value, empty intermediate → BLANK, single-hop unchanged, no sibling leak across a shared fact).
- Extraction hardening covered by crafted zip-bomb, path-traversal, symlink, and too-many-members fixtures plus a benign-archive control.
- Full test suite: 251 collected, 223 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 172 (CI baseline 175).
- No MAXID defect found in this repo (the builder writes `MAXID == max object id`, the correct Power BI high-water mark); an invariant regression test was added regardless. The `except Exception`/`e.code` crash reported from an older tree was already fixed in 0.9.3 (all broad handlers use `getattr(e, "code", …)`).

## [0.9.5] - 2026-07-17

### Fixed
- **Empty tables (`rows=[]`) now open in PBI Desktop** — previously any file containing a table with columns but no rows was rejected at load with `PFE_XM_DBCC_STRINGSTORE_CORRUPT` ("DBCC failed while checking the string store"), and Desktop fell back to an empty database. Two independent defects, both now corrected against Desktop's own zero-row table as ground truth:
  - **Empty string store emitted a page.** A zero-string dictionary was written as `store_page_count=1` with a page whose `allocation_size=0` — a zero-size/NULL character buffer, which Analysis Services' string-store consistency check rejects outright. A store with no strings now carries **no page at all** (`store_page_count=0`). This was the load-blocker, and it is String-specific: numeric columns encode an empty dictionary as a plain 0-count vector with no page, which is why empty *numeric* tables were unaffected.
  - **`AttributeHierarchyStorage.MaterializationType` for zero-row tables.** Desktop uses MatType=**2** with `DistinctDataCount=0` for its own zero-row table's RowNumber (MatType=3 is used only for the RowNumber of a *populated* table, and never on a user column). The builder wrote 3 unconditionally; empty tables now use 2.
- This removes the 0.9.3 Known Limitation. `_pre_build_checks()` still notes an empty table, but only as information — it is no longer a defect.

### Verified
- **Full empty-table sweep in live PBI Desktop (March 2026) via ADOMD — 13/13 pass.** Each file is checked for the real model loading (not Desktop's empty-fallback database), every expected table present via `INFO.TABLES()`, and exact row counts:
  - Empty table for **every data type** — String, Int64, Double, DateTime, Decimal, Boolean — individually, and all six together.
  - **Populated** all-six-types control (regression guard): loads with correct rows and columns.
  - Empty **+ populated** tables in one model: both present, populated rows and measure evaluate correctly.
  - Three empty tables in one model; empty table carrying measures (which evaluate); empty table with `nullable: false` columns.
  - **Relationship pointing at an empty dimension**: loads and the fact-side aggregation evaluates correctly across the join to a zero-row table.
- Zero-row structure matches Desktop ground truth: `SegmentMapStorage` RecordCount=0 / SegmentCount=1 / RecordsPerSegment=0, Partition Type=4 / Mode=0 / DataView=3, no phantom H$ system tables, no dangling storage references.
- Regression tests pin the conventions for all six data types (no page for an empty string store, MatType=2 + DDC=0 on empty tables, MatType=3 retained on populated RowNumber).
- Full test suite: 234 collected, 206 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 169 (CI baseline 175).

## [0.9.4] - 2026-07-16

### Added
- **Huffman-compressed string dictionaries — read and write** (MS-XLDM §2.7.4). Power BI Desktop stores string columns whose dictionary exceeds ~16 KB of UTF-16 text as canonical-Huffman-compressed pages; pbix-mcp previously could neither read them (raised `ValueError`) nor produce them. Both directions are now implemented:
  - **Reading:** `pbix-mcp` decodes compressed string dictionaries via the new `xmhuffman` dependency (an MIT canonical-Huffman primitive, mirroring how the ZIP layer already delegates XPress9 to `xpress9-python`). Verified against the public test corpus: all 89 string dictionaries across the 4 dashboards decode, byte-exact against the `pbixray` reference — including `IT_Support`'s `Body` column (11,917 strings across 9 compressed pages).
  - **Writing:** string dictionaries above the size threshold are now emitted as Huffman-compressed pages (package-merge length-limited canonical Huffman, single-charset for Latin text / general UTF-16LE otherwise, paginated at 2^19 chars/page to match Desktop). This closes the last gap in the VertiPaq string-store reversal and produces much smaller files for large text columns.

### Dependencies
- Added `xmhuffman>=0.3.0` (MIT) — the canonical-Huffman string-store primitive. Same role and provenance as the existing `xpress9` dependency.

### Verified
- Compressed-page encoding is **byte-identical to Power BI Desktop's own output** given Desktop's code lengths (validated by reproducing the `Body` and `Subject` compressed buffers from the corpus exactly — offsets, `total_bits`, and every byte incl. the even+2 trailing pad).
- Full round-trip through the real encoder/decoder on diverse inputs (ASCII, Latin-1 accents, CJK, non-BMP emoji, mixed Unicode, high-cardinality, heavy-duplicate, single-/two-distinct, multi-page > 2^19 chars).
- Generated compressed files open and query correctly in **PBI Desktop (March 2026)** via its live Analysis Services engine (ADOMD): single-page (501 strings), multi-page (2 pages, 1,501 strings), and Unicode general-mode (601 strings; emoji + CJK + Cyrillic) — `VALUES`, `SUMMARIZECOLUMNS`, and `TOPN` all return the exact strings.
- Full test suite: 222 collected, 194 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 169 (CI baseline 175).

### Documentation
- `docs/vertipaq-spec.md`: documented the compressed string-store page (canonical Huffman, charset modes, pair-swap, pagination) with the MS-XLDM reference.

## [0.9.3] - 2026-07-16

### Added
- **`PBIXBuilder.add_measure` `format_string=` parameter** (keyword-only) — measures can now carry a display format code (`"$#,0.00"`, `"0.0%"`, `"#,0"`) that is persisted to `Measure.FormatString` and rendered by PBI Desktop. `pbix_create` `measures_json` accepts an optional `"format_string"` (and `"description"`) per measure; the table-modification rebuild paths preserve it.

### Fixed
- **Measure `FormatString` silently dropped** — the `INSERT INTO [Measure]` statement hardcoded `FormatString` to NULL, and `server.py` passed `format_string` into `add_measure`'s `description` positional, so every requested format landed in the measure description and no measure ever carried a format. The INSERT now binds a real placeholder (empty string is treated as "no format"), and `format_string` is keyword-only so positional misuse raises immediately.
- **Measure `Description` lost on rebuild** — the table-modification paths (`pbix_datamodel_remove_table`, column modify) re-read measures without `Description` and overwrote it (previously with the format string). Both SELECTs now fetch and preserve it.
- **`add_table` cryptic failure on malformed rows** — non-dict rows (lists/tuples) used to surface as `'list' object has no attribute 'keys'` deep inside `save()`. `add_table` now raises a `TypeError` naming the table, the offending row index, and an example payload, at call time.
- **DBCC string-store corruption on embedded string columns (`PFE_XM_DBCC_STRINGSTORE_CORRUPT`)** — four independent encoder defects made PBI Desktop reject generated files at load (table dropped by the IMBI parallel loader, or file refused):
  - **NULL values**: the bit-packed IDF width was computed from the distinct value count, but NULL occupies raw slot 0 with values shifted to 1..N — a column with 2 distinct values + NULL overflowed its 1-bit encoding. Width now covers N+1 states (ground truth: IT_Support corpus `Body`/`Answer`).
  - **NULL values**: `max_data_id` over-counted by one (`3 + N` instead of `3 + N - 1`), desynchronizing IDFMETA/ColumnStorage stats from the dictionary.
  - **Non-BMP text (emoji)**: `store_longest_string` counted Python characters instead of UTF-16 code units, under-reporting surrogate-pair strings — a file whose longest string contained emoji failed to open.
  - **Empty strings**: `""` values became zero-length dictionary records, which AS rejects; PBI Desktop itself never writes `""` into a string dictionary (0 occurrences across all string columns of 4 real Desktop-built dashboards). Empty strings now canonicalize to NULL/blank, matching Desktop import semantics.
- **NULL/blank hierarchy: blank member missing from H$** — when a column has NULLs, PBI Desktop's attribute hierarchy contains the BLANK member at sorted position 0 (`POS_TO_ID[0]=2`, `ID_TO_POS[2]=0`, `RecordsPerSegment=distinct+1`, `AttributeHierarchyStorage.DistinctDataCount=distinct+1` — IT_Support ground truth). Without it, `VALUES()`/`SUMMARIZECOLUMNS`/`TOPN` over a nullable column failed against the live engine even though the file loaded.
- **NULL columns: `compression_info` must be 2** — Desktop writes `compression_info=2` in IDFMETA exactly for `has_nulls` columns (3 otherwise; verified across all 23 columns of the IT_Support fact table). The encoder wrote a constant 3, breaking hierarchy materialization for nullable string columns.
- **R$ relationship index built in the wrong order for string FKs — silently wrong joins** — R$ is indexed by the FK column's **data_id** (dictionary order: insertion for strings, sorted for numerics), verified against PBI Desktop ground truth (basic_measures `fct Orders → dim Customer` string relationship: 400/400 R$ slots match insertion order, 0/400 match sorted order). The builder filled R$ slots in *sorted* order, so any string FK whose insertion order differed from sorted order silently joined rows to the **wrong dimension records** — queries succeeded but returned incorrect data. FK keys also canonicalize like the dictionary ("" → blank, no slot). Verified post-fix with an engineered exact-value join check against the live engine (insertion ≠ sorted + blank + NULL FK rows: every label sums correct).
- **`""` in `nullable: false` String columns aliased to the first dictionary value** *(found by adversarial review)* — null presence is now derived from converted values regardless of the declared `nullable` flag, so canonicalized empty strings always get a real null slot instead of colliding with index 0.
- **Empty tables (`rows=[]`): phantom H$ shells removed** — H$ table/partition/storage shells were inserted before the `distinct == 0` guard, leaving phantom system tables with dangling `SegmentMapStorage` references. Empty columns now correctly use `MaterializationType=3` with no H$ artifacts.
- **H$ hierarchy built with the wrong column type** — the H$ writer reused a stale `data_type` from a previous loop (always the last column's type), sending String columns down the numeric branch: `POS_TO_ID`/`ID_TO_POS` mapped sorted positions to the wrong strings, so column sort order and hierarchy navigation were silently wrong in Desktop. The loop now re-reads each column's declared type.
- **H$ `POS_TO_ID` padding wrote reserved data id 2** — PBI Desktop pads the trailing `RecordCount - distinct` slots with zeros (54/54 ground-truth H$ files); the builder wrote a stray `2` (a reserved id below the store's first real entry). Both the from-scratch and roundtrip writers now pad with zeros.
- **IDFMETA `bookmark_bits` diverged at scale** — the encoder wrote `row_count` where Desktop writes `ceil(log2(5 * (rows + 1)))` (verified against all 22 pure-bitpack ground-truth segments; the two values nearly coincide at tiny row counts, which is why small files loaded).
- **Generic exceptions crashed the MCP error path** — 12 `except Exception` handlers called `e.code`, which only exists on `PBIXMCPError`; a plain `ValueError` (e.g. adding a duplicate measure) crashed the handler with `AttributeError` instead of returning a clean tool error. Now uses `getattr(e, "code", None)`.

### Verified
- Stress battery of 12 generated PBIX shapes opened in PBI Desktop (March 2026) and queried through its live Analysis Services instance (ADOMD): ASCII baseline (100 distinct), scale (5,000 distinct / ~300 KB string store), full unicode (Norwegian/CJK/emoji-as-longest/combining/line-separator), empty strings + duplicates, sparse string NULLs, numeric NULLs, string-key relationships (with and without blank/NULL FK rows), an engineered exact-value join check (insertion ≠ sorted keys: every per-label sum correct), formatted measures, and the 6-table Northwind showcase. Gauntlet per file: `VALUES`, `HASONEVALUE`+`VALUES`, `SELECTEDVALUE`, string-equality filter measure, `SUMMARIZECOLUMNS`, `TOPN` sort order (blank member sorts first, matching Desktop), storage DMVs, and `TMSCHEMA_MEASURES.FormatString` round-trip (`$#,0.00` / `0.0%` read back from the engine).
- Before the fixes, the same engine rejected the unicode, empty-string, and NULL shapes at load and mis-joined string-key relationships (verified reproductions); the DAX patterns flagged by downstream (`HASONEVALUE`+`VALUES`, `SELECTEDVALUE` with default, `TREATAS` over strings) all evaluate correctly against the live engine after the fixes.
- MCP layer end-to-end: report created through the actual tool layer (`pbix_create` with `format_string` measures → `pbix_save` → `pbix_open` → metadata SQL readback), `pbix_datamodel_add_measure`, both full-rebuild paths (`pbix_set_table_data`, `pbix_datamodel_remove_table` — measures keep `format_string` + `description`), and malformed-row rejection with a clean tool error. Same flow repeated over the real stdio JSON-RPC transport (`python -m pbix_mcp.cli`: initialize → tools/list (101) → tools/call), and the MCP-built files verified in PBI Desktop.
- Full test suite: 217 collected, 190 passed, 27 skipped (corpus-dependent), 0 failures; ruff clean; mypy under the 175 CI baseline.

### Known Limitations
- **Truly empty tables (`rows=[]`) still fail to open in PBI Desktop** even with consistent metadata — Desktop has no ground-truth representation for a never-processed embedded table. The pre-build check warns explicitly. Workarounds: add at least one row, or use `source_csv`/`source_db` so Refresh populates the table.

### Documentation
- `docs/development.md`: corrected the stale mypy baseline note (CI gate is 175; current count 168).
- `CONTRIBUTING.md`: `test_cross_report.py` needs the **public** test corpus (`python scripts/download_test_corpus.py`), not private files; updated test counts.
- `README.md`: updated test counts.

## [0.9.2] - 2026-04-08

### Fixed
- **`pbix_recolor` per-selector color spread** — colors now spread evenly across the full theme palette instead of using sequential indices. For 2 measures in an 8-color palette, uses indices 0 and 4 (maximum contrast) instead of 0 and 1 (nearly identical).
- **`pbix_recolor` category-based coloring for all chart types** — per-category data selectors now fire for bar/column charts with single measure + category axis (e.g., Profit by Region), not just pie/donut/treemap/funnel.
- **`pbix_recolor` empty theme palette fallback** — when theme file is missing from PBIX (not saved before close), generates an 8-shade gradient from the primary color instead of falling back to a single-color list.
- **`pbix_recolor` auto-extend identity map guard** — auto-extend no longer runs on identity maps (`#X -> #X`), preventing theme palette corruption when recolor is used just to apply smart defaults.

### Verified
- End-to-end MCP test: 4 cards (distinct blue backgrounds with contrast-fixed text), bar chart (2 measures spread), donut (4 categories spread), column chart (4 categories spread), table — all distinct colors, verified in PBI Desktop.

## [0.9.1] - 2026-04-07

### Fixed
- **Textbox visuals now Fabric-compatible** (closes #1) — `pbix_add_visual` for textbox type now adds `layouts` array and `drillFilterOtherVisuals`, strips `horizontalTextAlignment` (rejected by Fabric), converts `fontSize` from `px` to `pt`, and fixes double-nested `paragraphs` structures. Verified working in both PBI Desktop and Microsoft Fabric.

## [0.9.0] - 2026-04-07

### Added
- **`pbix_add_visual` image auto-embed** — image visuals with `sourcePath` in config automatically embed the local file into `RegisteredResources`, register it in `Content_Types.xml`, add `resourcePackages` entry, and reference via `ResourcePackageItem`. Adds `layouts`, `drillFilterOtherVisuals`, `filters` to match PBI Desktop ground truth.
- **`pbix_format_visual` alternating row colors** — `values.backColorPrimary/Secondary`, `fontColorPrimary/Secondary` for explicit table row styling. `grid.gridHorizontalColor/gridVerticalColor`.
- **`pbix_add_visual` bounds clamping** — visual positions are clamped to page dimensions so visuals never go off-page.
- **Builder explicit page dimensions** — `_build_layout()` now sets `width: 1280, height: 720` on pages (previously omitted, causing PBI Desktop to use narrower defaults).

### Changed
- **`pbix_recolor` strips borders by default** — all visual borders set to `show=false` during recolor. Users can re-enable via `pbix_format_visual`.
- **`pbix_recolor` removes pie/donut backgrounds** — PBI Desktop uses hardcoded gray leader lines that clash with dark backgrounds. Slices are already colored by dataPoint.
- **`pbix_recolor` card defaults** — title hidden, categoryLabels shown (less redundant). calloutValue and categoryLabels get readable colors on dark backgrounds.
- **`pbix_recolor` theme foreground contrast** — checks `foreground` vs `background` after recoloring theme. Fixes theme-inherited text (leader lines, axis defaults, textClasses).

### Fixed
- **`pbix_recolor` chart axis/legend/labels contrast** — injects `categoryAxis.labelColor`, `valueAxis.labelColor`, `legend.labelColor`, and `labels.color` when chart background is dark. Handles both missing and existing entries with unreadable colors. Skipped for pie/donut (bg stripped).
- **`pbix_recolor` table row contrast** — checks `backColorPrimary`/`Secondary` vs `fontColorPrimary`/`Secondary` and `columnHeaders.backColor` vs `fontColor`.
- **Contrast pass `objects` reference** — `sv.setdefault("objects", {})` instead of detached `sv.get("objects", {})`.
- **Contrast pass `vtype` variable** — was undefined, preventing chart-type-specific logic.

### Verified
- Kitchen Equipment report: created, themed, recolored to Emerald via MCP — all cards, charts, tables, and logo image correct. 5-image dice pattern placement verified in PBI Desktop.

## [0.8.5] - 2026-04-07

### Added
- **`pbix_recolor` automatic text contrast** — after recoloring, walks every visual and checks text-vs-background contrast using WCAG 2.0 luminance. Fixes title, subtitle, card label, axis/legend colors that would be unreadable (e.g., white text on light amber background). Uses contrast ratio threshold of 3.0 (WCAG AA for large text).
- **`pbix_recolor` auto-extend palette** — unmapped theme `dataColors` are automatically assigned to new palette colors by cycling. Eliminates stray old-palette colors in donut/pie category series and card backgrounds without requiring the user to map every single theme color.
- **`pbix_recolor` auto-generated table styling** — tables/matrices with no pre-existing row colors get themed alternating row backgrounds (25% and 10% tints of primary palette color), readable text colors (WCAG contrast), bold column headers with palette primary background, and grid lines in palette color.
- **`pbix_format_visual` alternating row colors** — new `values` properties: `backColorPrimary`, `backColorSecondary`, `fontColorPrimary`, `fontColorSecondary` for explicit alternating row styling. New `grid` properties: `gridHorizontalColor`, `gridVerticalColor`.

### Fixed
- **`pbix_format_visual` dataColors per-selector support** — multi-measure charts now get per-series `dataPoint` entries with `{"selector": {"metadata": "Table.Measure"}}`. Multi-category charts (donut, pie, treemap, funnel) get per-category entries with `{"selector": {"data": [{scopeId: {Comparison: ...}}]}}`. Single-color fallback preserved for simple charts.
- **`pbix_recolor` per-visual dataPoint injection** — after replacing hex colors and ThemeDataColor references, walks every chart visual and injects per-selector `dataPoint` entries from the new theme palette. Ensures ALL chart series/categories get explicit colors after recoloring. Supports 18 chart types.

### Verified
- End-to-end Ocean Blue → Sunset recolor: 40 hex replacements + 2 contrast fixes + zero old palette colors remaining. Light amber card auto-switched from white to dark text. Tables got themed alternating rows.
- toy_store_blue → red: 51 hex replacements + 4 charts colored + 2 tables grid-styled + zero blue colors remaining
- Auto-generated table styling: green → purple palette verified — row tints, header colors, and text contrast all correct

## [0.8.4] - 2026-04-06

### Unblocked
- **`pbix_datamodel_add_field_parameter`** — Blocked → **Stable**. Creates field parameter table with full VertiPaq storage via `_rebuild_datamodel`.
- **`pbix_datamodel_add_calculation_group`** — Blocked → **Stable**. Creates table via rebuild, then splices CalculationGroup + CalculationItem metadata. Partition Type=7 (CalculationGroup source), DiscourageImplicitMeasures=1.
- **`pbix_set_incremental_refresh`** — Blocked → **Stable**. Works for files with data sources (source_csv/source_db). Returns clear error for embedded-only files (by design, same as PBI Desktop).

### Verified
- All three features tested via MCP tools: create → save → close → reopen → verify data survives → PBI Desktop opens with correct tables, data, and measure.

## [0.8.3] - 2026-04-06

### Fixed
- **DAX cache staleness** — cache cleared on `pbix_close`, `pbix_save`, and all mutation paths. DAX evaluations now always reflect current data after `set_table_data` or other mutations.
- **RLS persistence across rebuilds** — `_rebuild_datamodel` reads existing RLS roles and re-applies them via metadata splice. RLS roles no longer silently dropped after `set_table_data` or `update_table_rows`.
- **System tables hidden** — H$/R$/U$ internal tables filtered from `pbix_list_tables` output.

### Documentation
- Fixed metadata table count: 63 → 68 across README, architecture.md, limitations.md
- Rewrote tool-contracts.md: all 101 tools with correct category counts
- Replaced stale PBIXRay references with native VertiPaq decoder
- Clarified DAX stability label to "Stable API / best-effort semantic parity"

### Verified
- Full 16-step roundtrip regression: create → RLS → DAX → save → close → reopen → verify DAX + RLS → mutate data → verify DAX reflects change → verify RLS survives rebuild → second roundtrip → PBI Desktop validation

## [0.8.2] - 2026-04-06

### Fixed
- **`pbix_add_hierarchy` unblocked for PBIX files** — user hierarchies now work in PBI Desktop. Uses unmaterialized HierarchyStorage (MaterializationType=-1, no U$ table needed). PBI Desktop creates the U$ tree data on first refresh. Validated with both builder-created files and Adventure Works DW 2020.
- **`PBIXBuilder.add_user_hierarchy()`** — new builder API creates hierarchies with correct metadata chain (Hierarchy + Level + HierarchyStorage), `IsAvailableInMDX=1` on referenced columns, and `LevelDefinition` format matching PBI Desktop exactly.
- **`_rebuild_datamodel` preserves hierarchies** — existing user hierarchies survive DataModel rebuilds (add/remove relationship, set_table_data, etc.)

### Changed
- User Hierarchies stability: **Partial** → **Stable** — full create/list/remove support for PBIX files

## [0.8.1] - 2026-04-06

### Added
- **13 new tools** (101 tools total):
  - **Perspectives**: `pbix_get_perspectives`, `pbix_add_perspective`, `pbix_remove_perspective` — create filtered model views for different user groups
  - **User Hierarchies**: `pbix_get_hierarchies`, `pbix_add_hierarchy`, `pbix_remove_hierarchy` — read/remove drill-down hierarchies
  - **Cultures & Translations**: `pbix_get_cultures`, `pbix_add_culture`, `pbix_add_translations`, `pbix_remove_culture` — multilingual metadata support
  - **Partition Management**: `pbix_get_partitions`, `pbix_add_partition`, `pbix_remove_partition` — read/remove M (Power Query) partitions
- **`pbix_save` strip_sensitivity_label parameter** — remove MSIP sensitivity labels from saved files

### Blocked
- **`pbix_add_hierarchy`** — blocked for PBIX files (needs H$ VertiPaq system tables). Works for PBIP/TMDL export.
- **`pbix_add_partition`** — blocked for PBIX files (needs PartitionStorage in VertiPaq). Works for PBIP/TMDL export.

### Fixed
- **ObjectTranslation ObjectType mapping**: TOM uses 3=Table, 4=Column, 8=Measure, 9=Hierarchy, 10=Level (was incorrectly 1-5)
- **ObjectTranslation Property mapping**: TOM uses 1=Caption, 2=Description, 3=DisplayFolder (was incorrectly 0-2)
- **`pbix_add_translations` if/elif branches**: Fixed to match corrected ObjectType values (3/4/8/9 instead of 1/2/3/4)
- **`pbix_get_cultures` display query**: Fixed LEFT JOIN ObjectType values for correct object name resolution

### Verified
- Adventure Works DW 2020: pure MCP-only test — added "Sales Analyst" perspective with 4 tables (Product filtered to 3 columns), added nb-NO culture with 3 table translations (Salg, Produkt, Kunde), verified all 6 existing hierarchies preserved, strip_sensitivity_label removes MSIP warning — file opens in PBI Desktop March 2026 without errors

## [0.7.0] - 2026-04-06

### Added
- **`pbix_export_pbip`** — convert PBIX to PBIP (Power BI Project) folder structure (88 tools total). Creates a complete PBIP project with:
  - `.pbip` root pointer
  - `.Report/` with legacy Layout JSON and static resources (images, themes)
  - `.SemanticModel/` with full TMDL (tables, columns, measures, relationships, roles, expressions)
  - `.gitignore` for PBI cache files

### Fixed
- **TMDL export**: Fixed `CrossFilteringBehavior` mapping (TOM: 1=OneDirection, 2=BothDirections, 3=Automatic — was off by one)
- **TMDL export**: Fixed partition type mapping (Type 4=M/Power Query, Type 2=Calculated DAX — was inverted)
- **TMDL export**: Added `defaultPowerBIDataSourceVersion: powerBI_V3` to model.tmdl for enhanced metadata support
- **TMDL export**: Added `expressions.tmdl` for shared M parameters (SqlServerInstance, SqlServerDatabase, etc.)
- **TMDL export**: Removed `description` properties from tables, columns, measures, expressions, and roles (PBI Desktop's TMDL parser rejects them)

### Verified
- Adventure Works DW 2020: exported to PBIP, opened in PBI Desktop March 2026 — 11 tables with correct columns/types, 13 relationships with correct cardinality, 3 shared M parameters, report page renders with original visuals (image + textbox), model view shows all relationship lines

## [0.6.9] - 2026-03-30

### Added
- **7 new data tools** (87 tools total):
  - **`pbix_export_table_csv`** — export a single table's data to CSV (all rows, quoted strings, ISO dates)
  - **`pbix_export_all_tables_csv`** — export every data table to a folder of CSVs
  - **`pbix_find_value`** — search for a string across all tables and columns, returns table.column locations with match counts
  - **`pbix_query_table`** — SQL-like WHERE filter with `=`, `!=`, `>`, `>=`, `<`, `<=`, `LIKE`, `IN`, `AND`/`OR`, column projection, ORDER BY
  - **`pbix_table_stats`** — per-column profiling: min/max/avg/distinct/nulls, string length stats, top 5 values
  - **`pbix_data_diff`** — row-level diff between two files' tables with key matching (added/removed/changed)
  - **`pbix_replace_value`** — find and replace ALL occurrences of a value in a column (builder-safe, uses full rebuild)

### Verified
- Adventure Works DW 2020: exported 10 tables (121K+ rows in Sales), profiled Customer (18,485 rows, 4 columns, top 5 values per column), found "Seattle" in 2 tables (96 matches), queried Sales with `Order Quantity > 20` returning 1,253 rows ordered DESC
- Replace value: created test file with 4 Products rows, replaced "Hardware" → "Physical" (3 rows), saved, reopened in PBI Desktop — values display correctly in Data view and visual grids

## [0.6.8] - 2026-03-30

### Added
- **`pbix_performance`** — performance analysis tool (80 tools total). Flags oversized tables (>100K rows), empty tables, wide schemas (>20 columns), high-cardinality string columns, complex measures (multi-table refs, deep nesting), inactive relationships, bidirectional relationships, and orphaned tables.

### Verified
- Adventure Works DW 2020: correctly flagged 2 medium tables, 1 empty table, 2 inactive relationships, 2 bidirectional relationships, 33 hidden columns, 11 calculated columns.

## [0.6.7] - 2026-03-30

### Fixed
- **RLS write now persists** — `set_rls_role` promoted from Beta to Stable. Uses binary splice (`_modify_metadata_only`) instead of the old rebuild path that silently dropped Role/TablePermission rows. MAXID-based ID allocation prevents conflicts.
- **`get_rls_roles` Windows crash** — fixed WinError 32 temp file lock (SQLite held file open during cleanup) and `sqlite3.Row.get()` AttributeError.

### Verified
- Adventure Works DW 2020: added "US Only" RLS role filtering `'Sales Territory'[Country] = "United States"`, saved, reopened — role persists, file opens in PBI Desktop without errors.

## [0.6.6] - 2026-03-30

### Added
- **`pbix_diff`** — compare two open PBIX files and show what changed (79 tools total). Compares tables (added/removed/row count changes), columns, DAX measures (added/removed/expression changes), relationships, pages & visuals, data sources, and theme colors. Both files must be open.

### Verified
- Adventure Works original vs modified: correctly detected 3 added measures + 1 added page
- Briqlab original vs SG recolored: correctly detected 469 removed theme colors + 10 added

## [0.6.5] - 2026-03-30

### Added
- **`pbix_document`** — auto-generate comprehensive report documentation (78 tools total). Returns markdown in the MCP response AND saves a `.docx` file. Covers: tables with row/column counts, column details per table, DAX measures with expressions, relationships, data sources (M expression excerpts), pages with visual inventory, RLS roles, and theme color palette.

### Verified
- Adventure Works DW 2020: 11 tables, 328K rows, 13 relationships, 11 data sources, 1 page — all documented correctly in both markdown and docx output.

## [0.6.4] - 2026-03-30

### Fixed
- **`pbix_extract_colors` now detects ThemeDataColor references** — previously only found hex literals (`'#RRGGBB'`), completely missing `ThemeDataColor` numeric references (`ColorId` + `Percent`) that PBI uses extensively for visual colors. Now resolves them to actual hex values using the active theme's dataColors palette and reports them with source location.
- **`pbix_recolor` now converts ThemeDataColor to hex** — when a ThemeDataColor reference resolves to a color in the replacement map, it's converted to a direct `Literal` hex value. Handles both escaped (config strings inside JSON) and non-escaped variants. Previously left ThemeDataColor refs untouched, causing visuals to keep old colors despite theme changes.
- **`pbix_set_theme` writes to RegisteredResources** — custom themes stored in RegisteredResources (used by many real-world reports) are now updated alongside BaseThemes. Previously only wrote to BaseThemes, leaving the active custom theme unchanged.
- **`pbix_recolor` replaces in both theme locations** — BaseThemes AND RegisteredResources JSON files are scanned and updated.

### Verified
- **SG Armaturen brand compliance test** — Briqlab airport dashboard recolored from teal to SG brand palette using only MCP tools (`pbix_extract_colors` → `pbix_recolor` → `pbix_set_theme`). All 531 original colors replaced. Zero non-brand colors remaining. Logo swapped. Verified in PBI Desktop March 2026.

## [0.6.3] - 2026-03-30

### Added
- **`pbix_format_visual`** — comprehensive visual formatting tool (75 tools total). Accepts human-readable JSON and generates PBI's internal `objects`/`vcObjects` structure. Ground truth validated against 9 PBI Desktop template files (670+ unique properties mapped).
  - **vcObjects (15 categories)**: title, subtitle, background, border, dropShadow, padding, spacing, divider, visualHeader, visualTooltip, visualLink, visualHeaderTooltip, stylePreset, altText, lockAspect
  - **objects (25 categories)**: legend, dataLabels, categoryAxis, valueAxis, dataColors, grid, columnHeaders, values, total, outline, shape, fill, line, categoryLabels, slices, smallMultiples, rowHeaders, subTotals, referenceLine, donut, bubbles, markers, imageScaling, card, cardTitle, columnFormatting, zoom, general
- **Auto-reload MCP server** — monitors `src/pbix_mcp/*.py` for changes and hot-reloads modules before the next tool call. Preserves open file state across reloads. No Claude Code restart needed for code changes to existing tools.

### Fixed
- **Title text property**: PBI Desktop uses `"text"` not `"titleText"` for visual titles in `vcObjects`
- **Color format**: all colors now use PBI's `{"solid": {"color": expr}}` wrapper (title fontColor, background color, border color, data point fill)
- **Auto-reload state preservation**: `_OPEN_FILES` dict is saved and restored across module reloads

### Verified
- **Executive Dashboard showcase**: 10 visuals on Adventure Works DW 2020 — dark header bar with logo, 4 color-coded KPI cards with drop shadows, clustered bar chart with legend and data labels, donut chart, formatted table with dark header row. All rendering correctly in PBI Desktop March 2026.
- Formatting ground truth extracted from 9 real PBI Desktop template files (670+ unique object properties, 87 vcObject properties)

## [0.6.2] - 2026-03-29

### Added
- **`splice_metadata_in_abf`** — binary splice function for modifying metadata inside PBI Desktop-generated ABFs. Replaces the file data at its exact offset without re-serializing any XML, preserving byte-identical ABF structure. Handles both UTF-8 (PBI Desktop) and UTF-16-LE (builder) ABF encodings automatically.

### Fixed
- **PBI Desktop file modification** — existing customer PBIX files (created by PBI Desktop) can now be modified via MCP. Previously, `rebuild_abf_with_replacement` corrupted the ABF structure by re-serializing XML with different whitespace/encoding, shifting offsets and causing `TMCacheManager::CreateEmptyCollectionsForAllParents` crashes. The new binary splice approach preserves the original ABF byte layout.
- **MAXID-based ID allocation** — `add_measure` now reads the global MAXID counter from DBPROPERTIES instead of scanning per-table MAX(ID). PBI Desktop files use a single global ID counter across all object types (tables, columns, relationships, measures, hierarchies). Using per-table MAX(ID) produced IDs that collided with system objects, causing `TMCacheManager` crashes.
- **MAXID update after add_measure** — `add_measure` now updates DBPROPERTIES.MAXID after inserting, so sequential `add_measure` calls get fresh IDs. Previously, the second call would reuse the same MAXID and fail with an IntegrityError.
- **UTF-16 BOM in `_xml_to_utf16_bytes`** — fixed `.encode("utf-16")` (which adds a BOM) to `.encode("utf-16-le")` (no BOM) for ABF structural XML serialization. PBI Desktop's ABF uses UTF-16-LE without BOM; the spurious BOM shifted all offsets by 2 bytes per XML section.

### Verified
- **Adventure Works DW 2020 full roundtrip** — 11 tables (121K+ rows in Sales), 13 relationships, 3 new DAX measures (Total Sales, Total Cost, Profit Margin), new "Sales Dashboard" page with 5 visuals (cards, bar chart, table) — all rendering correctly with live data in PBI Desktop March 2026
- Sequential `add_measure` x3 via MCP — no ID collisions, all measures evaluate correctly
- Original report pages and visuals preserved intact

## [0.6.1] - 2026-03-28

### Added
- **`pbix_list_data_sources`** — list all data source connections per table (type, server, database, mode)
- **`pbix_update_data_source`** — lightweight connection string switching without full DataModel rebuild. Supports SQL Server, PostgreSQL, MySQL, MariaDB, SQLite, CSV, Excel, JSON/Web, Azure SQL. Switch Import/DirectQuery mode.
- **`_modify_metadata_only`** — lightweight metadata-only path for changes that don't affect VertiPaq binary data

### Verified
- Live roundtrip: MSSQL Import → PostgreSQL DirectQuery → CSV Import, all via MCP
- 74 tools total

## [0.6.0] - 2026-03-28

### Added
- **Full roundtrip modify** — existing PBIX files can now be heavily modified: add/remove tables, relationships, measures, update table data, add visuals, pages, themes, bookmarks, filters. All DataModel modifications go through the builder pipeline for guaranteed consistency.
- **3 new tools** (72 total):
  - `pbix_datamodel_add_relationship` — add cross-table relationships with R$ indexes
  - `pbix_datamodel_remove_relationship` — remove relationships
  - `pbix_datamodel_remove_table` — remove tables with cascading measures/relationships
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
