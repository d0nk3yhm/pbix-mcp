# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.9.133] - 2026-10-10

The engine joins a relationship's text keys as Desktop does: with their ASCII case folded, and a key the one side holds more than once through its last row (#163, found verifying #160 and #162). Checked against Power BI Desktop 2.152 over ADOMD: `build_b165.py`'s 26 probes all match (0.9.132: 10).

The corpus census (1,508 measures) changes only measures that use RAND or RANDBETWEEN (14 of the 15 this time); it runs in 2,933 s, against 2,860 s on 0.9.132. `regress_check.py` finds no regression across its Desktop batteries.

### Fixed — the engine joins text keys as Desktop does (issue #163)

**What was wrong:** the engine matched a relationship key by its exact text, and joined every row of a key the one side holds more than once. Desktop differs on both counts. Its column store compares a text key with the ASCII letters' case folded (#43, #109), also across the two tables. A relationship joins a repeated one-side key to its last row, the one its index points at (#162).

**What Desktop does** (`build_b165.py`, a model the same as built and after Desktop's refresh):

- F's `K1` and `k2` reach D's `k1` and `K2`.
- With D's `k3` in rows C and D:
  - RELATED of F's `k3` row is D;
  - `D[name] = "C"` reaches no fact row;
  - RELATEDTABLE of C is empty.
- A filter from the many side reaches every row that holds the key. `FILTER(F, F[v] = 100)` keeps both C and D, and so does `CROSSFILTER(..., BOTH)`.

**The fix:**

- **Folding:** a join spells a text key with its ASCII letters folded (`_jstr`). This covers the key aliases, the fact-side index, the unmatched keys behind the blank row, and RELATED's lookup.
- **One -> many hop:** a repeated key's earlier rows select nothing (`_non_owner_rows`, empty when every key is unique, which is the common case).
- **RELATED:** it returns the key's last row.
- **Unchanged:** many-to-many relationships, which join every row, and the many -> one direction.

**Pinned** by `tests/test_issue163_relationship_text_keys.py`: the 26 probes, through pbix-mcp's reader and engine on a built model. **16 fail on 0.9.132.**

## [0.9.132] - 2026-10-10

A built model stores its text as Desktop's import stores it, and the engine answers for that text as Desktop does. Verifying #156 turned up two rules of Desktop's column store: its import strips trailing whitespace (#159), and it keeps the empty text apart from BLANK (#161). Measuring those turned up four errors: the builder's hierarchy ignored the case fold (#160), its index joined a repeated key to the wrong row (#162), and the engine mishandled the empty text (#164) and one-row tables in text functions (#165). Checked against Power BI Desktop 2.152 over ADOMD, as built against after Desktop's own refresh:

| Battery | 0.9.132 | 0.9.131 |
|---|---|---|
| `build_b159.py`: 15 groups of spellings that differ by whitespace | 35 of 35 queries | 13 |
| `build_b159b.py`: every C0 / C1 control and Unicode space at the end of a text, `""`, whitespace-only text | 15 of 15 | 6 |
| `build_b161.py`: `""` and BLANK | 7 of 7 | 0 |
| `build_b161c.py`: `""` in a column past the compression threshold | 10 of 10 | 2 |
| `build_b160.py`: values that differ only by ASCII case | 11 of 15; the other 4 differ only in the order of `ç` / `Ç`, which the collation ties (#156) | 7 |
| `build_b160r.py` and `build_b159.py`'s second model: a key the one side holds more than once | 5 of 5 and 3 of 3 | 3 and 0 |
| `build_b163.py`: `""` in DAX and text functions over a one-row table, the engine against Desktop | 38 of 38 | 14 |

The corpus census (1,508 measures) changes only the 15 measures that use RAND or RANDBETWEEN, as 0.9.131's did; it runs in 2,860 s, against 2,942 s on 0.9.131. `regress_check.py` finds no regression across its Desktop batteries.

### Fixed — an imported text is stored without its trailing whitespace (issue #159)

**What was wrong:** the builder stored each text as supplied. Desktop's import strips trailing whitespace, storing `"ab "` as `"ab"`. A built model therefore counted `"ab"` and `"ab "` as two values until Desktop refreshed it: in VALUES, DISTINCTCOUNT, groupings and filters. The same held for every table `pbix_set_table_data`, `pbix_update_table_rows` and `pbix_append_table_rows` write.

**What Desktop does:** measured by `build_b159.py` and `build_b159b.py`, the latter with every C0 / C1 control and Unicode space as the last character of a text.

- **What it strips:** U+0001 to U+0020 from the end of a text, that is every C0 control (tab, line feed, carriage return, ...) and the space, any number and mix of them.
- **What it stores:** the stripped spelling, even when the unstripped one comes first. `"cd "` then `"cd"` are stored as `"cd"`.
- **Filters:** a filter on the unstripped text finds nothing, whether CALCULATE, TREATAS or FILTER.
- **What it keeps:** DEL, U+0080 to U+009F, the no-break space and every other Unicode space, and leading and inner whitespace. A whitespace-only text becomes `""` (#161).
- **DAX does not strip:** a calculated table's `DATATABLE(..., {{" "}})` and a calculated column's `" "` are stored as they are, in Desktop-saved files of the corpus.

**The fix:** `vertipaq_encoder.import_text` strips U+0001 to U+0020 from the end of a text. The builder passes every imported text through it once, so the dictionary, the hierarchy, the relationship index, the statistics and the warnings all see the stored text.

- Calculated columns and calculated tables keep their text.
- The partition's "Enter data" M keeps the supplied text, so a refresh in Desktop strips it the same way.
- A pre-build warning, `trailing_whitespace`, says how many values were stripped.

**Pinned** by `tests/test_issue159_import_trailing_whitespace.py`: 127 tests generated from Desktop's output. They cover the stored values, 32 filter probes of 5 forms each, the 91-character sweep, a calculated column, the warning and the M source. **114 fail on 0.9.131.**

### Fixed — an empty text `""` is a value of its own (issue #161)

**What was wrong:** an empty text was stored as BLANK, since 0.9.3. Desktop's import keeps `""` apart from BLANK:

- ISBLANK is FALSE for it;
- `TREATAS({""}, ...)` and `==` select it alone;
- COUNTA and DISTINCTCOUNTNOBLANK count it;
- the MIN of a column is `""`.

As built, all 7 of `build_b161.py`'s queries answered differently.

**Why it was stored as BLANK:** in 0.9.3, a `""` record made Desktop refuse the model with "Database consistency checks (DBCC) failed while checking the string store". The record was not the cause. Desktop writes `""` as an ordinary zero-length record and flags its page `page_contains_nulls`. That holds for all 10 such pages in the Desktop-saved files of the corpus, and the 714 pages without one carry 0. The same model with the flag at 0 still fails in Desktop 2.152 with that DBCC error, and with the flag at 1 it loads.

**The fix:** the encoder stores `""` as a zero-length record and sets `page_contains_nulls` on its page. A dictionary that holds `""` stays on one uncompressed page, as Desktop's does: `build_b161c.py`'s column of 13,313 characters is past the compression threshold, and Desktop wrote it uncompressed with `""` but Huffman-compressed without. The encoder now writes that dictionary byte for byte as Desktop's own. A compressed page holding `""` made Desktop refuse the model with the same DBCC error. The builder's null slot, hierarchy and relationship index treat it as a value: the hierarchy holds BLANK, then `""` before every other text, with MinValue `""`. Pass `None` for BLANK.

**Pinned** by `tests/test_issue161_empty_text.py`: the stored values, the dictionary record and its flag, the uncompressed page, the hierarchy, and 25 of Desktop's answers through the reader and the engine. **24 of 29 fail on 0.9.131.**

### Fixed — a built text column's hierarchy numbers its dictionary (issue #160)

**What was wrong:** the builder folds a text column's values that differ only by ASCII case onto the first spelling (#43), as Desktop's import does. But it numbered the attribute hierarchy (H$) with every spelling. The hierarchy held data ids past the dictionary's end, and the values after the first fold pointed at the wrong entries. In Desktop, until a refresh:

- `COUNTROWS(VALUES(T[c]))` was 14 for 8 values;
- ORDER BY, MIN, TOPN and CONCATENATEX's ordering were scrambled;
- `MAX(T[c])` failed with an internal error (pfshdata.cpp).

Every built model with such a column has had this since 0.9.88. The relationship index keyed its text the same way, so its slots lost step with a folded key column. The orientation guess for a plain many-to-one with no order given also took a column for unique when it was unique only before the fold.

**The fix:** the hierarchy, the relationship index and the orientation guess all take a text's identity from the encoder (`_convert_value_for_dict`, `_val_key`), so each numbers exactly what the dictionary holds. Tables that `pbix_set_table_data` rewrites already used the encoder's own hierarchy and were not affected.

**Checked:** `build_b160.py`'s 15 queries answer the same as built and after a refresh, except the order of `ç` / `Ç` in 4 of them. The collation finds those two equal, and Desktop's order among such values follows no rule we can see (#156).

**Pinned** by `tests/test_issue160_case_variant_hierarchy.py`: 3 tests. **All 3 fail on 0.9.131.**

### Fixed — a key the one side of a relationship holds more than once reaches its last row (issue #162)

**What was wrong:** the relationship index joined a repeated one-side key to its first row, and Desktop's refresh joins its last. In `build_b160r.py`:

- `k1` of rows A, C and `K1` (D) reaches D;
- `k2` / `k2 ` reaches F;
- `k3` reaches G.

RELATED, and groupings by the one side's other columns, differed until Desktop refreshed the model. Desktop's UI refuses such a relationship, but a model that has one loads and refreshes without an error.

**The fix:** the index maps each key to its last row, using the dictionary's identity for text keys (#160).

**Pinned** by `tests/test_issue162_duplicate_one_side_keys.py`: the R$ index against Desktop's RELATED. **It fails on 0.9.131**: there the orientation guess took F for the one side, so F had no index at all.

**Not changed:** the engine's own joins. RELATED and filter propagation look a key up by its exact value and reach every row that holds it, where Desktop joins one row per key. That is #163.

### Fixed — the engine treats the empty text as a value, not BLANK (issue #164)

**What was wrong:** `ISBLANK("")` was TRUE, COUNTX and COUNTAX skipped `""`, and the MIN of a column and COALESCE passed over it. FIRSTNONBLANK(T[c], T[c]) skipped a `""` value too. Desktop-saved models can hold `""` (7 files of the corpus do), and built models do from this release (#161).

**What Desktop does** (`build_b163.py`):

- `ISBLANK("")` and `ISBLANK("" & BLANK())` are FALSE, and a column's `""` rows are not blank;
- `COUNTX(T, T[c])` counts them;
- `MIN(T[c])` and `MINX` return `""`;
- `COALESCE("", "x")` is `""`;
- `FIRSTNONBLANK(T[c], T[c])` returns a `""` value.

COUNTBLANK counts `""` as blank, and `= ""` matches BLANK, as before.

**Pinned** by `tests/test_issue163_empty_text_in_dax.py`: 22 of Desktop's answers. **11 fail on 0.9.131.**

### Fixed — text functions take a one-row table's value (issue #165)

**What was wrong:** these functions applied `str()` to their argument:

- LEN, UPPER, LOWER, PROPER;
- LEFT, RIGHT, MID, TRIM, SUBSTITUTE, REPLACE, REPT;
- SEARCH, FIND, CONTAINSSTRING, CONTAINSSTRINGEXACT, EXACT;
- UNICODE, VALUE, COMBINEVALUES.

So `LEN(FIRSTNONBLANK(T[c], 1))` measured the internal row list's Python text (56), and `UPPER(TOPN(1, VALUES(T[c]), T[c]))` returned that text. `str(x or '')` also turned a 0 into `""` and a date into Python's text.

**The fix:** they convert their arguments as DAX does, as `&` already did:

- a one-row table is its value;
- BLANK is `""`;
- a number is its DAX text (`0` is `"0"`);
- a date is its text, so `LEN(LASTDATE(T[d]))` is 8 (`"4/2/2024"`).

**Pinned** by `tests/test_issue164_text_functions_one_row_tables.py`: 16 of Desktop's answers. **13 fail on 0.9.131.**

## [0.9.131] - 2026-10-09

Text is compared and ordered in the model's collation, as Power BI Desktop does it, both in the engine (#157) and in the models the builder writes (#156). TOPN orders by any value and every order pair, and keeps ties (#158). Found while verifying #136. Checked against Power BI Desktop 2.152 over ADOMD:

| Check | 0.9.131 | 0.9.130 |
|---|---|---|
| expression comparisons of text (62 pairs, `build_b157*.py`) | 62 of 62 | 35 |
| `build_b156.py`: MIN / MAX / FIRSTNONBLANK / CONCATENATEX / TOPN of text | 11 of 11 | 4 |
| `build_b158.py`: TOPN | 13 of 13 | 6 |
| a built model: as built = after Desktop's refresh (`build_b156.py`) | 12 of 12 queries | 5 |
| a built column of 3,017 random strings, order against Desktop's (`build_b157d.py`) | 14 adjacent swaps, all ties or the edge cases below | — |

The corpus census (1,508 measures) changes only the 15 measures that use RAND or RANDBETWEEN, directly or through another measure; it runs in 2,942 s, against 2,954 s on 0.9.129. `regress_check.py` finds no regression across its Desktop batteries; the 7 TOPN misses of `build_b158.py` are fixed.

### Fixed — the engine compares and orders text in the model's collation (issue #157)

**What was wrong:** text was compared by code point after case-folding. `"_x" < "1x"` and `"Ölund" < "Zeta"` were FALSE. MIN, MAX, FIRSTNONBLANK, LASTNONBLANK and CONCATENATEX's ordering followed the code points: MIN of `b, APAC, Americas, apple, Zeta, a10, a2, Bz, ba, _x, 1x, éclair, eclair, Ölund, olive` was `1x` and MAX `éclair`, where Desktop says `_x` and `Zeta`.

**What Desktop does:** it compares text as Windows' NLS sorting does for the model's locale, at the version of its own sort tables:

- case-insensitively;
- an accented letter with its letter, the accent deciding only after the letters, left to right (`cote < coté < côte < côté`);
- punctuation before the digits, the digits before the letters (`_x < 1x < a10 < a2 < Americas < APAC < apple`);
- `æ = ae`, `ß = ss`, `² = 2`;
- hyphens and apostrophes weighing nothing until everything else is equal (`coop < co-op < cop`), and at the end of a text more than an accent (`é < e-`).

**The fix:** `pbix_mcp.dax.collation`, a pure-Python collation built from Windows NLS 6.1's sort weights, measured per character in context (all 65,536 BMP and surrogate code units). Desktop's own differences were measured over ADOMD:

- 1,321 characters its older table does not know are ignorable, `ẞ` among them;
- Cyrillic `ї` and `й` are letters of their own;
- the trailing-punctuation rule above.

Over 16,307 single characters (the whole BMP, sampled CJK and Hangul, the emoji blocks) Desktop's order and the collation's disagree only among exotic characters: Arabic presentation forms, Thaana, combining marks on their own. Two things are not reproduced:

- texts that begin with a combining mark;
- the width level Desktop's column sort uses for `²` / `2`. Its expressions ignore that level, as the collation does.

Expression comparisons, MIN / MAX (one and two arguments), MINX / MAXX, FIRSTNONBLANK / LASTNONBLANK(VALUE), CONCATENATEX's ordering, RANKX of text, the window functions' ordering, TOPN, TOPNSKIP and SAMPLE all use it. A column filter's `=` and `<>` keep the column store's comparison (ASCII case only, #107).

**Pinned** by `tests/test_issue157_text_collation.py`: 77 tests generated from Desktop's output. They cover 3 Desktop sort orders (b156, 185 strings of b157, 54 hyphen strings of b157g), 62 expression pairs, 11 aggregates and the collation's ties. **33 of the 73 engine tests fail on 0.9.130**, and the rest need the new module.

### Fixed — a built model orders text as Desktop does (issue #156)

**What was wrong:** `PBIXBuilder` wrote each text column's attribute hierarchy (the H$ table's POS_TO_ID) in Python's code-point order. Desktop's engine reads that order for ORDER BY, SUMMARIZECOLUMNS, TOPN, MIN / MAX of text and sorted visuals, so a built file sorted `APAC` before `Americas` and `_x` after `Zeta` until Desktop refreshed it. The tables `pbix_set_table_data` rewrites were affected the same way.

**The fix:** the hierarchy is written in the collation's order, with code points breaking ties between values a column keeps apart (`é` / `É`). Desktop's own order among those follows no rule we can see. The stored MinValue / MaxValue follow suit.

**Checked:** `build_b156.py`'s 12 queries now answer the same as built and after Desktop's refresh; 0.9.130 differed on 7. A built column of 3,017 random strings sorts as Desktop's refresh sorts it, apart from 7 ties, 6 texts that begin with a combining mark and the `²` / `-2` width case.

**Pinned** by `tests/test_issue156_built_text_order.py`: the built hierarchy's order and its Min / MaxValue against Desktop's. **1 fails on 0.9.130.**

### Fixed — TOPN orders by any value and every order pair, and keeps ties (issue #158)

**What was wrong:** TOPN ordered by its first expression only, and only by numbers. Any other value scored 0 and kept the table's input order, so `TOPN(1, VALUES(D[Date]), D[Date], ASC)` returned the first row, not the earliest date. The latest date came out right only when the table listed it first. TOPN also cut at n, where DAX returns every row tied with the n-th.

**The fix:**

- every `orderBy [, order]` pair, in turn;
- dates, text and TRUE / FALSE by value, BLANK first (`_order_key`);
- the ties at the n-th row.

TOPNSKIP and SAMPLE order by the same key.

**Checked:** `build_b158.py`'s 13 probes match Desktop: dates and text ascending and descending, a measure returning text, TRUE / FALSE, BLANK, two order pairs, a tie.

**Pinned** by `tests/test_issue158_topn_order.py`: 13 tests. **7 fail on 0.9.130.**

## [0.9.130] - 2026-10-09

A built model's columns are available in MDX, as Desktop's are (#136). Working on it turned up #155: a table rewrite in a model with a renamed calculation-group column produced a file Desktop would not open. Checked in Power BI Desktop 2.152, as built and after Desktop's own refresh. The DAX engine is unchanged.

### Fixed — every column of a built model is available in MDX (issue #136)

**What was wrong:** `PBIXBuilder` wrote `Column.IsAvailableInMDX = 0` on every data column outside a user hierarchy. Desktop writes 1 on every column. MDX clients, such as Analyze in Excel or an MDX query, saw no attribute hierarchy. `SELECT {[Measures].[S]} ON 0 FROM [Model] WHERE ([T].[c].&[a])` returned an empty cell.

The server's table rewrite (`pbix_set_table_data`) rebuilds the whole model through the builder. One edit of a Desktop-authored file therefore took its columns out of MDX: 34 of Awesome Chocolates' 109 data columns.

**The fix:** the builder writes 1 on every column. The attribute-hierarchy storage was already there (an H$ table for each column), so the flag is all it takes.

**Checked** with `build_b136.py` (new):

- Desktop's `MDSCHEMA_HIERARCHIES` lists every column, and MDX slicers and member sets answer as DAX does. That covers text, decimal, whole-number and date columns, TRUE/FALSE, the BLANK member (`[T].[c].&`), a related table, a user hierarchy, a hidden table and an empty one.
- Every cell and schema row is the same as built and after Desktop's refresh, on 16 MDX cells and 9 probes.
- On 0.9.129 every slicer was empty.

Two cells answer empty in Desktop's own processing too: a Decimal key written `&[1.5]`, and a column that is all BLANK, whose hierarchy holds only `[All]`.

**Pinned** by `tests/test_issue136_mdx_columns.py`: a built model's data columns, and the same after a table rewrite. **2 fail on 0.9.129.**

### Fixed — a table rewrite keeps a calculation group's source columns (issue #155)

**What was wrong:** the rebuild names each column's SourceColumn after the column. A calculation group's two data columns must read Desktop's fixed source columns, `Name` (text) and `Ordinal` (whole number), whatever they are called. Once the group's column had been renamed, any `pbix_set_table_data` produced a file Desktop refuses. Awesome Chocolates names its column `Time`, and Desktop said: "Calculation group table 'Time Intelligence' supports maximum two data columns: one is string datatype with source column 'Name' and the other is integer datatype with source column 'Ordinal'."

**The fix:** the rebuild already re-wires the group (CalculationGroupID, partition Type 7, no QueryDefinition). It now restores the two source columns too.

**Checked:** Awesome Chocolates, with `dim-Products` rewritten, opens in Desktop. It answers as the original does in 9 of 9 DAX queries (the three calculation items included) and 7 of 7 MDX cells, as built and after a refresh. All 40 of its attribute hierarchies are listed.

**Pinned** by `tests/test_issue155_calc_group_rewrite.py`: a renamed group column after a table rewrite. **1 fails on 0.9.129.**

## [0.9.129] - 2026-10-09

SUMMARIZE takes its groups from the source rows and keeps each column's lineage (#117, from OpenBI). Working on it turned up four more:

- ALL / ALLSELECTED of several columns was taken for the table's rows (#152);
- a table filter on several columns of one table kept each column's values, not the combinations its rows have (#153), and so did a table filter of a table's rows (#154);
- a CROSSJOIN calculated table could not be added (#151).

And a measure referenced in an iteration over a fact table costs about what CALCULATE does again (#150, a cost 0.9.128 added). Checked against Power BI Desktop 2.152 over ADOMD:

| Battery | 0.9.129 | 0.9.128 |
|---|---|---|
| `build_b117.py` (117 probes) | 115 of 115, and Desktop's 2 errors | 45 |
| `build_b115.py` | 55 of 55 | 51 |
| `build_b114.py` | 39 of 39 | 38 |
| `build_b115b.py` | 112 of 112 | 111 |
| Desktop calculated tables (TOM) | 8 of 8 shapes | — |

Every other Desktop probe answers as before. The corpus census (1,508 measures) changes only the 15 that use RAND or RANDBETWEEN. The 1,463 that finish on both versions take 786 s, against 837 s on 0.9.128. The 45 that run into the 30-second budget answer on neither. How long one runs before the budget stops it varies from run to run, and the same measures in the same order take the same time on both (2,954 s in all, against 2,890 s).

### Fixed — SUMMARIZE by a related table's column (issue #117)

**What was wrong:** `SUMMARIZE(<source>, <a related table's column>)` had two faults.

- **The groups came from the whole table.** A table-expression source was ignored. `COUNTROWS(SUMMARIZE(FILTER(Orders, Orders[Region] = "W"), Dim[Zone]))` was 2; Desktop says 1.
- **Every column carried the base table's lineage.** Tagged `{'__table__': 'Orders', 'Zone': ...}`, the columns broke in three places:
  - as a CALCULATE filter, `Dim[Zone]` filtered a column Orders does not have: 450 instead of Desktop's 250;
  - a transition over the rows filtered nothing. `ADDCOLUMNS(SUMMARIZE(Orders, Dim[Zone]), "r", CALCULATE(SUM(Orders[Revenue])))` summed to 900, not 450, as a measure and as a calculated table;
  - `Dim[Zone]` inside an iteration over the rows read as a column marker (`CONCATENATEX` printed `N('Dim', 'Zone')`).

**The fix:**

- The groups are the distinct combinations among the source rows. Each related column is read through the source's expanded table, BLANK for a key that matches nothing (#82). The columns come back in the order written.
- A group of one column is a row of that column's table. A group of several columns carries one part per table (as a CROSSJOIN row does, #108) beside the merged columns. As a filter it filters their combinations (#103): two columns of one table now filter both, where only the first used to (Desktop: 4 rows, not 5).
- An extension column is evaluated in its group's context, as Desktop does. That context includes the group's source rows expanded to their dimensions:
  - `SUMX(SUMMARIZE(FILTER(Orders, Orders[Revenue] > 100), Dim[Zone], "r", SUM(Orders[Revenue])), [r])` is 400, the kept orders' revenue (the engine said 450);
  - a dimension member that no row of the group uses drops out (2, not 3);
  - ALLSELECTED inside puts the source rows back (8).
- A source's rows are read part by part, so every kind of source works the same way:
  - **A source of columns** (ALL or ALLSELECTED of several columns, VALUES, another SUMMARIZE): its rows filter the columns they carry and nothing more. They have no expanded table, even when they carry every column of their table. Desktop: `SUMMARIZE(FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 2), Orders2[Region], "q", SUM(Orders2[Qty]))` sums the kept quantities (14), and the same with `"d", COUNTROWS(Dim)` gives 9, with no Dim row filtered. IT Support's `Time Wasting Patterns` (over `ALLSELECTED(fact[Cluster_ID], fact[Queue])`) keeps Desktop's 4 alerts.
  - **A CROSSJOIN source:** its rows filter every part's columns (9, not 15).
  - **A group column the source does not carry and cannot reach** is an error, as Desktop says: `SUMMARIZE(ALL(Orders[Region]), Dim[Zone])` names a column not in the input table.
- SUMMARIZECOLUMNS, which has no source table, still evaluates its expressions in the group's columns only.

**Pinned** by `tests/test_issue117_summarize_lineage.py`: 96 tests generated from Desktop's output. **56 fail on 0.9.128.**

### Fixed — ALL / ALLSELECTED of several columns is a table of columns, not the table's rows (issue #152)

**What was wrong:** `ALL(T[a], T[b])` returns the combinations of those columns, but its rows were marked as rows of `T`.

- **As a CALCULATE filter**, `FILTER(ALL(T[a], T[b]), ...)` went through the branch for a table's rows. This is the running-total shape over `'Date'[Year]` and `'Date'[Month]`. The branch removed every filter of `T`, including its other columns and an iterator's own row, and blocked the relationships into `T`. When the columns were all of `T`'s, it also filtered `T`'s expanded table.
- **In a transition**, rows that carried every column of `T` filtered its expanded table.

Desktop, with T4(A, B, C, V) and Orders2 many-to-one Dim:

- `SUMX(VALUES(T4[A]), CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), ...)))` is 3 (the engine said 24);
- under `Dim[Zone] = "z1"`, `FILTER(ALL(Orders2[Region], Orders2[Qty]), Orders2[Qty] >= 3)` leaves 1 order (3), and `ISFILTERED(Dim)` stays FALSE (TRUE);
- `SUMX(ALL(Orders2[Region], Orders2[Qty]), CALCULATE(COUNTROWS(Dim)))` is 15 (5).

**The fix:** the rows are rows of a table of columns, as SUMMARIZE's are. As a filter argument, such a table:

- replaces the filters on its own columns;
- intersects them under KEEPFILTERS;
- leaves the table's other filters and relationships in place;
- has no expanded table.

**Pinned** by `tests/test_issue152_column_tables.py`: 15 tests. **8 fail on 0.9.128.**

### Fixed — a table filter over several columns keeps the combinations its rows have (issue #153)

**What was wrong:** `FILTER(ALL(T[a], T[b]), ...)` as a CALCULATE filter wrote each column's values as a filter of its own. A combination none of its rows has came back whenever its values each appeared in some row. T4 has no key. Leaving out (b1, c2), `CALCULATE(SUM(T4[V]), FILTER(ALL(T4[B], T4[C]), NOT (T4[B] = "b1" && T4[C] = "c2")))` is 11 in Desktop; the engine said 12. The same happened under KEEPFILTERS, beside a filter on another column, and in SUMMARIZE's extension columns over such a source.

**The fix:**

- Each column's values stay the filters on the columns.
- Where those values admit combinations no row has, a filter on the rows' combinations is added. TREATAS onto several columns, a CROSSJOIN's rows and SUMMARIZE's already work this way (#103, #108).
- An inner filter on one of the columns keeps the projection onto the others (Desktop: 2).

**Pinned** by `tests/test_issue153_column_combinations.py`: 18 tests. **13 fail on 0.9.128.**

### Fixed — a table filter of a table's rows keeps those rows (issue #154)

**What was wrong:** `FILTER(T, ...)` and `FILTER(ALL(T), ...)` wrote each column's values among the rows. A row the argument left out came back when each of its values appeared in a row it kept. With T4's row (a1, b1, c2, 1), `CALCULATE(SUM(T4[V]), FILTER(T4, NOT (T4[B] = "b1" && T4[C] = "c2")))` is 11 in Desktop; the engine said 12. SUMMARIZE's extension columns over such a source did the same.

**The fix:**

- The engine counts the rows of the table that each column's values admit on their own, and compares them with the argument's rows.
- Where the values admit more, a filter on the rows' combinations is added.
- Mostly they admit no more, because a key or a column the condition alone decides tells the rows apart. Then nothing changes.
- SUMMARIZE runs the check once over all its source rows.
- A table's rows as a filter also replace an outer filter on combinations that names their columns, keeping its other tables' part.

**Pinned** by `tests/test_issue154_whole_row_combinations.py`: 4 tests. **4 fail on 0.9.128.**

### Fixed — a CROSSJOIN calculated table takes each part's columns (issue #151)

**What was wrong:** the calculated-table evaluator returned a CROSSJOIN row's internal keys as its columns: the right table's `_2_`-prefixed names and the `__parts__` tuple (#108). `pbix_datamodel_add_calculated_table` then failed with `INTERNAL_ERROR: unhashable type: 'dict'`.

**The fix:** a row's columns are each part's columns in order, then the columns ADDCOLUMNS added. A name two tables share is refused, as Desktop refuses it ("a column named Region already exists"). A SUMMARIZE over several tables keeps the order written.

**Checked:** Desktop's own calculated tables, added through its TOM library and recalculated (`tom_calc_tables.ps1`), give the same columns and row counts for all 8 expressions, the two refusals included. A CROSSJOIN table the tool added reads the same data in Desktop before and after a refresh.

**Pinned** by `tests/test_issue151_crossjoin_calc_table.py`: 11 tests. **9 fail on 0.9.128.**

### Fixed — a measure reference inside a fact-row iteration costs what CALCULATE does (issue #150)

**What was wrong:** 0.9.128 (#115) gave every fact-row transition one filter per dimension column. `evaluate_measure` copied every filter's values into its memo key and sorted the key, so `SUMX(Fact, [S])` took twice as long as the same iteration written with CALCULATE.

**The fix:**

- The memo key is the context's own signature, built once per filter set.
- Each filter is signed once and carries its signature; a row's dimension filters take theirs from the expansion's cache.
- A context derived from another signs only what it changed.

On the #115 benchmark (20,000 fact rows, warm), `SUMX(Fact, [S])` takes 2.9 s, against 5.0 s on 0.9.128 and 1.7 s on 0.9.127.

**Pinned** by `tests/test_issue150_memo_key_speed.py`: the measure-reference iteration within 1.45x of the CALCULATE one (0.9.128: 1.8x), and the key is the context's signature. **2 fail on 0.9.128.**

## [0.9.128] - 2026-10-09

A table's row, as an iterator transitions it, and a table filter argument now filter their EXPANDED table, as Desktop does. Both came from OpenBI: #115 (the transition) and #114 (the table filter). Working on them turned up three more fixes:

- DATEADD inside an iterator (#147);
- table filters that replaced filters they must intersect (#148);
- a memo that took two large filters for one (#149).

Checked against Power BI Desktop 2.152 over ADOMD:

| Battery | 0.9.128 | 0.9.127 | Notes |
|---|---|---|---|
| `build_b115b.py` (112 probes) | 111 | 42 | The miss is #117. Covers a snowflake, a dimension shared by two facts, an orphan key, and which table expressions keep the expanded table. |
| `build_b113.py` | 276 of 276 | 270 | ISFILTERED of a dimension under a fact-table filter (#114). |
| `build_b114.py` | 38 of 39 | 25 | The miss is #117's SUMMARIZE lineage. |
| `build_b115.py` | 51 of 55 | 42 | The 4 misses are #117's SUMMARIZE lineage. |
| `build_b116.py` | 40 of 40 | 39 | |
| `build_b147.py` | 24 of 24 | 8 | |
| `build_b149.py` | 8 of 8 | 3 | |

Every other Desktop probe answers as before.

### Fixed — the context transition of a table's row filters its expanded table (issue #115)

**What was wrong:** an iterated row's transition filtered the row's own columns only. A filter on the many side does not reach the one side, so CALCULATE or a measure inside `SUMX(Orders, ...)` saw every dimension whole. Desktop, with Orders many-to-one Dim and three orders:

- `SUMX(Orders, CALCULATE(COUNTROWS(Dim)))` is 3, one dimension row per order (the engine said 9);
- over a snowflake Orders → Dim → Cat, `SUMX(Orders, CALCULATE(SUM(Cat[Weight])))` is 40 (90);
- `SUMX(Orders, CALCULATE(COUNTROWS(Orders2)))`, where Orders2 is a fact that shares Dim, is 5 (15);
- `SUMX(Orders, CALCULATE(IF(ISFILTERED(Dim[Zone]), 1, 0)))` is 3 (0).

**The fix:** CALCULATE, a measure reference, time intelligence and RELATEDTABLE now transition each iterated whole row into filters on its expanded table. That is every column of each table the row reaches many-to-one, holding the related row's values, or BLANK for a key that matches nothing. These are filters of their own:

- `REMOVEFILTERS(Dim[Region])` leaves the zone in force (5);
- `ALL(Dim)` and `ALL(Orders)` take them out (9);
- ALLSELECTED puts back the related rows of every iterated row (9);
- an inner row keeps its own columns: `SUMX(Dim, SUMX(Orders, CALCULATE(COUNTROWS(Dim))))` is 9, as #116 pinned;
- in a CROSSJOIN row, each part's own columns win over another part's expansion.

**Which rows keep the expanded table:**

- **Yes:** a row of the table itself, and a row of FILTER, ALL, VALUES, DISTINCT, TOPN, CALCULATETABLE, RELATEDTABLE or ADDCOLUMNS over it.
- **No:** a row of SUMMARIZE or SELECTCOLUMNS, even when it carries every column. `SUMX(SUMMARIZE(Orders, Orders[Region], Orders[Revenue]), CALCULATE(COUNTROWS(Dim)))` is 9.

**Speed:** a row's dimension filters are built once per iteration and dimension member, with their signatures once per model. They are not propagated back to the iterated table while the row's own key filter is in force. The corpus census (1,508 measures) takes 2,890 s, against 2,869 s on 0.9.127. A fact-row iteration that transitions every row now carries one filter per dimension column, so it is slower. On a 20,000-row fact with three 16-column dimensions, with the model's caches warm:

| Expression | 0.9.128 | 0.9.127 |
|---|---|---|
| `SUMX(Fact, CALCULATE(SUM(Fact[Amt])))` | 2.5 s | 1.3 s |
| `SUMX(Fact, [S])` | 5.0 s | 1.7 s |
| `SUMX(Fact, Fact[Qty] * Fact[Amt])`, with no transition | no change | |

**Pinned** by `tests/test_issue115_transition_expanded.py`: 85 tests. **61 fail on 0.9.127.**

### Fixed — a table filter argument filters its expanded table (issue #114)

**What was wrong:** `FILTER(Orders, ...)` as a CALCULATE filter was kept on Orders' own columns, which also reached the one side through the relationship. Desktop treats it as a filter on the expanded table:

- `CALCULATE(ISFILTERED(Dim), FILTER(Orders, Orders[Revenue] > 100))` is TRUE (FALSE);
- after `REMOVEFILTERS(Dim[Region])`, Dim is still filtered by the zones of the kept orders: 3 rows (2);
- an outer filter on the dimension is overwritten: `CALCULATE(CALCULATE(COUNTROWS(Dim), FILTER(ALL(Orders), Orders[Revenue] > 100)), Dim[Zone] = "z2")` is 2 (1);
- the filter reaches a fact that shares the dimension: `CALCULATE(COUNTROWS(Orders2), FILTER(Orders, Orders[Revenue] > 100))` is 3 (5).

**The fix:**

- The rows' expanded table is written as filters on the one-side columns, the same expansion as #115.
- The one-hop many-to-one propagation the engine kept for table filters (`_expanded_keys`) is gone.
- Under KEEPFILTERS, those columns intersect with the filters in force.

**Pinned** by `tests/test_issue114_table_filter_expanded.py`: 74 tests. **28 fail on 0.9.127.**

### Fixed — DATEADD / SAMEPERIODLASTYEAR inside an iterator shift the row's dates (issue #147)

**What was wrong:** CALCULATE's fast path for `DATEADD(<column>, ...)` and `SAMEPERIODLASTYEAR(<column>)` read the dates of the context the rows were opened in. Each month of `SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH)))` shifted the whole selection: 630, where Desktop says 210.

**The fix:** the column form transitions the row first. Desktop reads it as `CALCULATETABLE(DISTINCT(<dates>))`, as PREVIOUSMONTH and a measure reference already did.

**Pinned** by `tests/test_issue147_dateadd_in_iterator.py`: 24 tests from `build_b147.py`, over a marked and an unmarked calendar. **16 fail on 0.9.127.**

### Fixed — table filter arguments intersect what they must keep (issue #148)

**What was wrong:** a multi-column table filter removed every filter on its table's columns before applying its rows.

- A second `FILTER(Orders, ...)` in the same CALCULATE threw the first away: 2 orders, where Desktop says 1.
- `KEEPFILTERS(FILTER(ALL(Orders), ...))` replaced an outer `Orders[Region] = "N"`: 2, where Desktop says BLANK.

**The fix:**

- The filter arguments of one CALCULATE intersect on any column they share.
- KEEPFILTERS keeps the filters in force on every column its rows filter.

**Pinned** by `tests/test_issue148_table_filters_intersect.py`: 6 tests. **3 fail on 0.9.127.**

### Fixed — two filters of more than 4096 values no longer share a memo entry (issue #149)

**What was wrong:** the memo of the filters that reach a table through relationships is model-wide. It signed a filter of more than 4096 values as None. With a 6000-day calendar, `CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2015, 1, 1)))` was served the result for the dates before 2014, so `[b] - [a]` was 0 where Desktop says 365. A running total over a long calendar repeated one value. The ALL() snapshot compared signatures the same way.

**The fix:** a filter too large for the short signature is signed by a digest of its sorted values (`_filter_sig_exact`).

**Pinned** by `tests/test_issue149_large_filter_signature.py`: 9 tests. **6 fail on 0.9.127.**

## [0.9.127] - 2026-10-09

Date arithmetic is typed as Desktop types it (#146, found verifying #145), and PBIXBuilder marks a projection `active` as Desktop does, as OpenBI reported (#143). Checked against Power BI Desktop 2.152 over ADOMD:

- the date-arithmetic battery (`build_b146.py`, 36 probes) matches in full, against 20 on 0.9.126, and so do `build_b145.py`'s date − date probes (9; 5);
- every other Desktop probe answers as before.

### Fixed — date arithmetic is typed as Desktop types it (issue #146)

- **What was wrong:** the engine made date − date a number of days, a choice made in 0.9.79 (#24 r22) and never measured. Desktop:
  - `DATE(2024, 3, 31) - DATE(2024, 1, 1)` is the DateTime 1900-03-30 (90 days), and `"n:" & (…)` is "n:3/30/1900";
  - `TIME(18, 0, 0) - TIME(6, 0, 0)` is 12:00:00, and `DATE(…) - TIME(6, 0, 0)` a date;
  - `50000 - DATE(…)` and `BLANK() - DATE(…)` are numbers.

  So `-` keeps its LEFT operand's type, while `+` is a date when either side is one. Also:
  - ISNUMBER of a date is TRUE (the engine said FALSE);
  - `SUMX(Dt, Dt[Date] - DATE(2024, 1, 1))` is the DateTime 1911-03-18 and `AVERAGEX(Dt, Dt[Date])` the number 45337 (the engine skipped dates, giving 4095 and BLANK);
  - `IF(TRUE(), <date - date>, 0)` is 90: a date beside a number literal is a number.
- **The fix:**
  - `_fold_arith` keeps the left operand's type for `-`;
  - ISNUMBER counts dates;
  - SUMX adds dates and returns a date when every value is one, and AVERAGEX averages their serials;
  - IF types a date result beside a number literal as a number;
  - a comparison of a date with a number uses its serial, so `d1 - d2 < 0` still works;
  - FORMAT applies a number format to a date's serial (`FORMAT(d1 - d2, "0")` is "90").
- **The builder's type inference agrees** (`infer_measure_data_type`): `d1 - d2` is DateTime (9), `50000 - d` Double, and IF with a number-literal branch Double.
- **A calculated column the model declares numeric keeps its type** on a rebuild: a date − date result is written as its serial, as Desktop converts it, instead of retyping the column DateTime.
- **Tests that pinned the 0.9.79 choice now expect Desktop's answer:** `test_issue24_type_info.py`, `test_dax_silent_wrong.py`, `test_compiled_plans.py` (the tenure-days calculated column holds the DateTimes of 516 and 214 days).
- **Pinned** by `tests/test_issue146_date_arithmetic_types.py`: 45 tests generated from Desktop's output. **20 fail on 0.9.126.**

### Fixed — PBIXBuilder marks a projection `active` as Desktop does (issue #143)

- **What was wrong:** `add_page` wrote `"active": true` on every projection entry of every role. A chart built with two Category fields therefore opened EXPANDED in Desktop: every level active is the expand-all state. Desktop's own default for a two-field axis is its top level.
- **Desktop's files** (the community dashboards and templates in `test_samples`, 36 files):
  - a drillable role marks its FIRST entry active, and every entry only when expanded (`A-` / `AA`). These roles are Category of every chart and map, a matrix's Rows / Columns, a slicer's Values, a scatter's X, and a decomposition tree's ExplainBy;
  - Y, the Values of cards and tables, Tooltips, Size, Series and a chart's small-multiples Rows never carry the key.
- **The fix:** the builder writes that (`_role_has_levels`). A visual config's `"expanded": true` asks for every level active.
- **Pinned** by `tests/test_issue143_projection_active.py`: 9 tests. **9 fail on 0.9.126.**

## [0.9.126] - 2026-10-09

A row context filters nothing until CALCULATE or a measure reference turns it into a filter, as OpenBI reported (#116). Found verifying it: a VAR block inside a function argument (#144), and the one-date tables of time intelligence used as values (#145). Checked against Power BI Desktop 2.152 over ADOMD:

- the row-context battery (`build_b116.py`) matches 39 of 40, against 14 on 0.9.125. The other is #115;
- #118's ALLSELECTED battery (`build_b118.py`) matches in full, 420 of 420 (0.9.125: 405): its last 15 cells were this issue's;
- `build_b115.py` gains 3 cells (42 of 55);
- three new batteries match in full: CALCULATE's filter arguments in an iterator (`build_b143.py`, 24; 0.9.125: 16), row contexts and VAR blocks (`build_b144.py`, 31; 8), and dates as values (`build_b145.py`, the 18 probes that are this release's; 5);
- every other Desktop probe answers as before.

### Fixed — a row context filters nothing until a transition (issue #116)

- **What was wrong:** the engine applied an iterator's row to the filter context at once, which CALCULATE and measure references need, so everything else inside the iterator saw only the current row:
  - `SUMX(Orders, COUNTROWS(Orders))` was 3, Desktop 9, and `SUMX(Orders, MAXX(Orders, Orders[Revenue]))` 450, Desktop 750;
  - `COUNTROWS(FILTER(Orders, COUNTROWS(Orders) = 3))` was BLANK, Desktop 3: an aggregate in FILTER's condition reads the context outside FILTER;
  - `SUMX(VALUES(D[Month]), CALCULATE([S], VALUES(D[Date])))` gave each month its own dates. Desktop evaluates CALCULATE's filter arguments before the transition and sees every date in every month (1488, 3 × the total);
  - `SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Revenue] < EARLIER(Orders[Revenue]))))` was BLANK, Desktop 3: EARLIER returned the column, so the row was compared with itself.
- **The fix:**
  - a row context's data reads (columns, rows, the blank row) come from the context the rows were opened in (`DAXContext._row_root`). CALCULATE and measure references still see the transition;
  - time-intelligence functions over a date column, and RELATEDTABLE, see the transition too: Desktop reads their column as `CALCULATETABLE(DISTINCT(<dates>))`. `SUMX(VALUES(D[Month]), CALCULATE([S], DATESYTD(D[Date])))` gives each month its own year to date (772);
  - EARLIER / EARLIEST walk the enclosing row contexts, and a column of another table reads the innermost row over that table: `SUMX(Dim, COUNTROWS(FILTER(Orders, Orders[Region] = Dim[Region])))` is 3;
  - a function that takes a column (VALUES, DISTINCT, COUNT, DISTINCTCOUNT, SELECTEDVALUE, HASONEVALUE, RELATED, LOOKUPVALUE, …) takes the column, not the current row's value (`_column_arg`);
  - FILTER binds its row instead of writing the row's values into the condition's text, which also rewrote the column a function takes (EARLIER, FIRSTDATE).
- **Two tests now follow Desktop's rule:** `FILTER(Sales, SUM(Sales[Amount]) > 90)` keeps every row, since the SUM is the total (`test_issues14.py`), and `VALUES(P[S])` in a row context over P is the column's values (`test_calculate_predicates.py`).
- **Pinned** by `tests/test_issue116_row_context.py`: 85 tests generated from Desktop's output (`build_b116.py`, `build_b143.py`, `build_b144.py`). **47 fail on 0.9.125.**

### Fixed — a VAR block inside a function argument evaluates (issue #144)

- **What was wrong:** the expression analyzer read any text holding the words VAR and RETURN as one VAR block. `SUMX(T, VAR x = … RETURN …)`, `IF(c, VAR … RETURN …, …)` and `CALCULATE(VAR … RETURN …, …)` were split at the wrong places: `SUMX(Orders, VAR t = 1 RETURN t)` was BLANK (Desktop 3), and `CALCULATE(VAR s = SUM(Orders[Revenue]) RETURN s, Orders[Region] = "N")` was 1 (Desktop 50).
- **The fix:** a VAR block is an expression that starts with VAR. One in an argument is evaluated when the argument is.
- **Pinned** by `tests/test_issue144_var_in_arguments.py`: 11 tests, 10 from Desktop's output (`build_b144.py`, `build_b116.py`). **10 fail on 0.9.125.**

### Fixed — the one-date tables of time intelligence are dates as values (issue #145)

- **What was wrong:** FIRSTDATE, LASTDATE, STARTOF… / ENDOF…, LASTNONBLANK and the other functions that return dates gave each date as 'YYYY-MM-DD' text. As a CALCULATE filter that worked; as a value it was no date. `MAXX(Dt, FIRSTDATE(Dt[Date]))` and `YEAR(LASTDATE(Dt[Date]))` were BLANK (Desktop 31 March 2024 and 2024), and `"d:" & FIRSTDATE(Dt[Date])` was "d:2024-01-01" (Desktop "d:1/1/2024").
- **The fix:** the rows carry the column's own cell. Iterators, comparisons, arithmetic, date coercion, FORMAT and DIVIDE read a one-row, one-column table as its value.
- **Pinned** by `tests/test_issue145_time_intelligence_dates.py`: 18 tests from Desktop's output (`build_b145.py`). **13 fail on 0.9.125.**

### Still open

- #115: a fact row's transition does not filter its dimensions. `SUMX(Orders, COUNTROWS(RELATEDTABLE(Dim)))` is 9, Desktop 3.
- #146: date − date is a DateTime in Desktop and a number of days in the engine (`build_b146.py`).

## [0.9.125] - 2026-10-09

Awesome Chocolates' calculation items answer at once (#142). Under QOQ, `Sales Previous`, `Sales Change`, `Sales CF` and `Sales color` took 70–110 s each in 0.9.124; each now takes under a second, with the same values. Checked against Power BI Desktop 2.152 over ADOMD:

- Desktop's `SUMMARIZECOLUMNS('Time Intelligence'[Time], …)` query matches in every cell: 18 measures under each of WOW, MOM and QOQ;
- the two-item slicer and QOQ by region match too, 27 more cells;
- every Desktop probe from earlier releases answers as before.

### Fixed — a measure that re-filters a date table through its date column is evaluated once per iteration (issue #142)

- **What was slow:** the QOQ item filters the 731 dates by `[Max Previous Quarter Not Blank]`, one row at a time. That measure is `CALCULATE([Max Quarter Not Blank], DATEADD(FILTER(ALLSELECTED('dim-Date'[Date]), NOT ISBLANK([Sales Actual])), -1, QUARTER))`. The memo missed on every row: 731 × 731 inner transitions.
- **Why its value cannot depend on the row:**
  - ALLSELECTED(D[Date]) restores the iteration's dates (#118);
  - each date's transition clears the date table's other filters (#137);
  - DATEADD of a table shifts the table's own dates (#138, #141);
  - the CALCULATE filter on the date column clears the rest (#78).
- **The fix:** the #131 shield's proof now also accepts a CALCULATE whose one filter is dates of a date column that clears its table (`_shield_date_filter`, `_shield_date_rows`): FILTER over ALL / ALLSELECTED(D[Date]) with a condition that reads the table only through the row, or DATEADD / SAMEPERIODLASTYEAR of that. The memo then keys such a measure by the iteration, as for #131, and evaluates it once per iteration.
- **Not accepted,** because each reads a filter on the table:
  - ALL(D[Date]) or ALLSELECTED(D[Date]) as a modifier;
  - DATEADD of the column itself;
  - a condition that reads another column, or MAX(D[Date]);
  - KEEPFILTERS, or a second filter argument;
  - a date column whose filter clears nothing.
- **Pinned** by `tests/test_issue142_shield_date_filters.py`: 24 tests. **6 fail on 0.9.124.** Values are identical with and without the shield under four slicers. The 8 per-row probes of `tests/test_issue138_dateadd_table_dates.py` now take 0.1 s instead of 20 s, so they lose the `slow` marker.

## [0.9.124] - 2026-10-09

Four defects found verifying #118 on Awesome Chocolates, whose QOQ calculation item now matches Power BI Desktop end to end. Checked against Power BI Desktop 2.152 over ADOMD:

- under the QOQ item, `Sales Previous`, `Sales Change`, `Sales CF` and `Sales color` answer Desktop's 8,063,111.25, −0.335, `#ff383f` and `#D60007`. 0.9.123 gave BLANK, BLANK, `#15b2ad` and `#0F7572`;
- four batteries match in full: date-table row transitions (`build_b137.py`, 74 probes; 0.9.123: 56), DATEADD over a table (`build_b138.py`, 60; 18), numeric arguments (`build_b139.py`, 82; 17) and DATEADD with no visible date (`build_b141.py`, 24; 8);
- every Desktop probe from earlier releases still matches. #133's battery gains 3 cells: ABS, DIVIDE and POWER of non-numeric text raise.

### Fixed — a row transition on a date column clears the date table's other filters (issue #137)

- **What was wrong:** take `FILTER(ALLSELECTED(D[Date]), NOT ISBLANK([M]))` evaluated in an outer row's transition over D. Each date's `[M]` still saw the outer row's other columns. `MINX(D, CALCULATE(MAXX(FILTER(ALLSELECTED(D[Date]), NOT ISBLANK([M])), D[Date])))` answered 31 January, the last sale in the outer row's month. Desktop answers the last sale date, 31 March, for every row.
- **Desktop's rule** (`build_b137.py`; a marked and an unmarked calendar answer alike): a filter on a DateTime column that joins a relationship, or on a marked date table's date column, clears the table's other filters (#78). The engine applied the rule only when CALCULATE wrote the filter. Desktop applies it to a row transition as well:
  - `SUMX(VALUES(D[Month]), CALCULATE(COUNTROWS(FILTER(ALL(D[Date]), CALCULATE(COUNTROWS(D)) > 0))))` is 273, every date in each of the three months. The engine counted 91;
  - `MINX(D, CALCULATE(COUNTROWS(FILTER(ALL(D[Date]), CALCULATE(COUNTROWS(D)) > 0)), D[Month] = 1))` is 91: an explicit Month filter is cleared the same way;
  - the filters a transition writes itself stay: `SUMX(FILTER(D, D[Month] = 2), CALCULATE(COUNTROWS(D)))` is 29.
- **The fix:** every row transition goes through one function, `_transitioned_ctx`, which applies the rule to the filters it writes. That covers the iterators, FILTER over whole rows, SUMMARIZE / SUMMARIZECOLUMNS groups and FIRSTNONBLANKVALUE.
- **Pinned** by `tests/test_issue137_date_transition_clears.py`: 74 tests, every expected value generated from Desktop's output. **18 fail on 0.9.123.**

### Fixed — DATEADD / SAMEPERIODLASTYEAR over a table expression shift the table's own dates (issue #138)

- **What was wrong:** the engine read a table expression's dates back under the current context, with only the date column's filter replaced. So an outer row's month, quarter or weekday cut the table down before the shift, and when nothing was left it shifted every date of the calendar. `CALCULATE(COUNTROWS(DATEADD(<February 2024>, 1, MONTH)), D[Month] = 202305)` was 516; Desktop gives 31.
- **What it decided:** Awesome Chocolates' `[Max Previous Quarter Not Blank]` is `CALCULATE([Max Quarter Not Blank], DATEADD(FILTER(ALLSELECTED('dim-Date'[Date]), NOT ISBLANK([Sales Actual])), -1, QUARTER))`. Under each quarter the engine answered the quarter before it. Desktop answers 20234 for every quarter and every date row, and the QOQ item filters its dates by it.
- **The fix:** `_shift_date_table` shifts exactly the table's dates (`_dateadd_dates(..., source=)`). A column reference, `DATEADD(D[Date], 1, MONTH)`, still shifts the dates the context leaves, as in Desktop.
- **Pinned** by `tests/test_issue138_dateadd_table_dates.py`: 60 tests from Desktop's output over a marked and an unmarked calendar. **42 fail on 0.9.123.** The 8 per-row probes take about 20 s each, so they are marked `slow`.

### Fixed — DIVIDE and the math functions convert their arguments as arithmetic does (issue #139)

- **What was wrong:** DIVIDE and every math function returned BLANK for numeric text, TRUE / FALSE and dates. CEILING and FLOOR returned the text unchanged. Desktop converts them as arithmetic does (#133):
  - `DIVIDE(3, "3")` is 1, `ABS("-3")` 3, `SIN("0")` 0, `ROUND(TRUE(), 0)` 1 and `ABS(DATE(2024, 1, 2))` 45293;
  - `DIVIDE("3,5", 1)` is 35 in an en-US model;
  - text that is no number raises: `DIVIDE("abc", 3)`, `DIVIDE("", 3)`, `SIN("x")`;
  - a text alternate result stays text: `DIVIDE(3, 0, "07") & ""` is "07".
- **What it decided:** Awesome Chocolates' QOQ item divides `_CQ - _PQ` by `_PQ` over `SELECTEDMEASURE()`, and `[Sales CF]` is `CONVERT([Sales Actual], STRING)`. The engine's DIVIDE was BLANK, so `IF(_QOQ < 0, "#ff383f", "#15b2ad")` coloured a 33.5 % fall `#15b2ad`.
- **The fix:** one conversion, `_num_operand`, now feeds:
  - DIVIDE's numerator and denominator;
  - ABS, ROUND / ROUNDUP / ROUNDDOWN, INT, TRUNC, CEILING, FLOOR, MOD, POWER, SQRT, EXP, LN, LOG, LOG10, SIGN, EVEN, ODD, FACT, GCD, LCM and MROUND;
  - the helpers behind the trigonometric, combinatoric, bitwise and statistical functions.
- **Pinned** by `tests/test_issue139_numeric_text_arguments.py`: 83 tests, 82 of them from Desktop's output (`build_b139.py`). **66 fail on 0.9.123.**

### Fixed — DATEADD / SAMEPERIODLASTYEAR of a date column shift nothing when no date is visible (issue #141)

- **What was wrong:** when the context left no date visible, the engine shifted the whole calendar. `CALCULATE(COUNTROWS(DATEADD(D[Date], 1, MONTH)), D[Month] = 199001)` counted 516, `SAMEPERIODLASTYEAR` 181, and `CALCULATE(CALCULATE([FS], DATEADD(D[Date], 1, MONTH)), D[Month] = 199001)` read a sales total of 13,962. Desktop answers BLANK for every one (`build_b141.py`, on the calendars of `build_b138.py`).
- **The fix:** `_dateadd_dates` shifts the dates that are visible, and none shifts to none, as NEXTDAY / PREVIOUSDAY already did. A filter on the fact, which does not reach the calendar, still leaves every date visible (516 in Desktop and the engine).
- **Pinned** by `tests/test_issue141_dateadd_no_visible_dates.py`: 24 tests from Desktop's output. **16 fail on 0.9.123.**

### Still open

- Under the QOQ item the four Chocolates measures take 70–110 s each, where Desktop answers at once.

## [0.9.123] - 2026-10-09

ALLSELECTED keeps the measure's own CALCULATE filters, as OpenBI reported (#118), and puts back what an iteration iterated. Checked against Power BI Desktop 2.152 over ADOMD:

- a 420-cell ALLSELECTED battery (`build_b118.py`) has 405 cells matching; 0.9.122 matched 158. The other 15 are #116;
- #120's 400-cell matrix now matches in full, the 10 explicit-filter cells included;
- on the community reports, Awesome Chocolates matches all 49 measures (0.9.122: 45), the Financial Sample's Arrow Chart all 49 (45), and Target Line Bar Charts 10 of 11 (8);
- every Desktop probe from earlier releases still matches; #115's battery gained 22 cells and a date-table battery (`build_b137.py`) 4.

### Fixed — ALLSELECTED keeps the measure's own CALCULATE filters (issue #118)

- **What was wrong:** ALLSELECTED put back the query's selection and dropped everything else, a CALCULATE filter inside the measure included. `CALCULATE(CALCULATE([Sales], ALLSELECTED(T)), T[Cat] = "A")` answered the 75 total where Desktop gives A's 40. The community dashboards hit it through `DATEADD` inside ALLSELECTED: Awesome Chocolates' `Max Previous Quarter Not Blank` and the Financial Sample's `Sales PM` read the current period.
- **Desktop 2.152's rules,** measured one cell per query under ten query shapes (`build_b118.py`):
  - **an explicit CALCULATE filter stays**, in every form: modifier or table, table, column or no argument. The DATEADD filter survives an inner `ALLSELECTED('Date')`;
  - **a filter an iteration put there goes:** a grouping goes back to the query's selection, and a row transition to the iteration's *rows* (its shadow filter context). `SUMX(FILTER(ALL(T[Cat]), T[Cat] = "A"), CALCULATE(COUNTROWS(ALLSELECTED(T))))` is 2, not 4, and `SUMX(T, CALCULATE(COUNTROWS(ALLSELECTED(T))))` under a zone grouping is 2 rows each;
  - **the column form keeps its own columns only**, explicit filters on the others included: `CALCULATE(COUNTROWS(ALLSELECTED(Dt[Date])), Dt[Month] = 2)` is all 91 dates;
  - **FILTER's row transition puts the row in scope:** `COUNTROWS(FILTER(T, CALCULATE(IF(ISINSCOPE(T[Cat]), 1, 0)) = 1))` is 4.
- **The fix:**
  - a row transition's filter (`RowContextValues`) carries its iteration's rows. Nineteen iterator call sites (SUMX, MAXX, RANKX, TOPN, ADDCOLUMNS, ...) pass them, as do FILTER over whole rows (plain lists before), SUMMARIZE / SUMMARIZECOLUMNS groups and FIRSTNONBLANKVALUE;
  - ALLSELECTED replaces only an iteration's filter: with the distinct values among those rows, else the query's selection. It keeps every other filter;
  - the measure memo keys a transition by its iteration's rows, so the same row under two iterations is not one value. The #131 shield now leaves out only a filter that ALLSELECTED restores the same way for every row: a transition or a grouping.
  - what ALLSELECTED puts back is no new date filter. A filter on a DateTime column that joins a relationship clears the table's other filters (#78), but Desktop keeps the row's month in `MINX(D, CALCULATE(CALCULATE(COUNTROWS(D), ALLSELECTED(D[Date]))))`: 29, the shortest month (`build_b137.py`).
- **Found verifying it:** IT Support's `Time Wasting Patterns` evaluates `ALLSELECTED(fact[Queue])` inside SUMMARIZE's extension columns. With SUMMARIZE's groups as plain lists it counted 1 alert where Desktop counts 4. They are now an iteration's rows, and the measure matches.
- **Still open:** a FILTER condition that holds an aggregate and reads the row's own column, such as `Dt[Month] = MAXX(FILTER(ALLSELECTED(Dt), ...), Dt[Month])`. That is #116. Also, an outer row's other columns are not seen inside `FILTER(ALLSELECTED(D[Date]), ...)` in Desktop (`build_b137.py`: `MINX(D, CALCULATE(MAXX(FILTER(ALLSELECTED(D[Date]), NOT ISBLANK([M])), D[Date])))` is the last sale date, 31 March). The engine still sees them and answers 31 January (#137).
- **Pinned** by `tests/test_issue118_allselected_explicit.py`: 409 tests, every expected value generated from Desktop's output. **247 fail on 0.9.122.** `tests/test_issue120_allselected_table.py` gains the 16 explicit-filter cells it left out, and `tests/test_storage_and_scope.py` now expects Desktop's 1 for an inner filter on the ALLSELECTED column.

### Fixed — the measure memo keys an iteration only where ALLSELECTED can read it (issue #140)

- **Found by this release's corpus census, before it shipped:** with #118's iteration in every memo key, a measure that FILTER and then AVERAGEX evaluate over the same rows ran twice. Agents Performance's `Employees Avg MTD Sales - Adjusted`, `Employees Avg Total Discount - Adjusted` and `Rank Filtering Employyees MTD` took 41–43 s instead of 22–23 s (each alone in a fresh process), so the census's 30 s cap read BLANK. Their values were Desktop's all along.
- **The fix:** only ALLSELECTED reads an iteration's rows. A measure's key carries the iteration only when the measure can reach ALLSELECTED: in its own text, in a measure it references (transitively), or in any calculation item (`_reads_shadow`). The three measures are back to 23–28 s.
- **Pinned** by `tests/test_issue140_memo_iterations.py`: 9 tests. **7 fail on #118's commit alone**, 8 on 0.9.122. FILTER's and AVERAGEX's passes evaluate a measure once per row between them, and a measure that reaches ALLSELECTED keeps each pass's own value (2 + 2, not 10 + 10).

## [0.9.122] - 2026-10-09

Calculation groups are applied (OpenBI doc 52). Also three fixes reported by OpenBI (docs 53, 54 and 55), six found while verifying them, one found by the 0.9.121 corpus census (#132), and one reported by @allanon2 (PR #123). All were checked against Power BI Desktop 2.152 over ADOMD:

- all 133 date probes (DATESINPERIOD, DATEADD, SAMEPERIODLASTYEAR) match; 0.9.121 matched 41;
- all 549 calculation-group cells of three batteries match, errors included, and all 80 item format strings read over MDX;
- VARs are lazy and IFERROR / ISERROR react to errors only: 24 of 25 probes match, against 15;
- all 126 RANKX cells match;
- the date-part join matches Desktop on its own auto date/time file and on Desktop-switched relationships;
- on the two community reports built on Microsoft's Financial Sample, 45 of 49 and 8 of 11 measures match, against 3 and 2;
- every Desktop probe from earlier releases still matches.

### Added — calculation groups are applied (issue #121)

- **What was wrong:** a filter on a calculation group's column did not apply the selected item, and the SELECTEDMEASURE family was not implemented. Every measure evaluated as if the group did not exist. On Awesome Chocolates' QOQ page, `Sales Change` read 34,042,511.25, the plain Sales, where Desktop shows the quarter-over-quarter change.
- **The definitions:**
  - `ModelReader.calculation_groups` reads each group: its table, its item column (SourceColumn "Name", e.g. `'Time Intelligence'[Time]`), its precedence, and its items with their expressions and format-string expressions.
  - `ModelReader.measure_format_strings` reads the measures' static and dynamic format strings.
  - The server attaches both to the model's measures dict (`engine.set_calculation_groups`), so every evaluation over the model applies them.
  - `evaluate_measures_batch`, `evaluate_measures_smart` and `evaluate_measure` also take `calculation_groups=` and `measure_formats=`, for a caller that builds its own model, as OpenBI does.
- **Desktop 2.152's rules,** measured one cell per query (`build_b121.py`, `build_b121b.py`):
  - an item applies when the filter on its group's item column lets exactly one item through, whether from a grouping, a TREATAS slicer or a CALCULATE filter. Several items, none, or a filter on another column (`CG[Ordinal] = 2`) apply nothing, and a one-item group needs the filter too;
  - it applies once per measure reference made outside it: `S2 = [S]` under Double is 120, not 240, and Plus1 of `[S] + [S]` is 121;
  - the item's own measure references are plain: `RefS = [S] * 10` is 600 for every measure;
  - the group of higher precedence is the outer one: Double (0) with Add100 (10) is Add100(Double(S)) = 220. A lower group still applies to the measure references in the expression of a higher group's item: `Ref10 = [S] * 10` with Double is 1200;
  - a CALCULATE that selects another item applies it inside. `CALCULATE([S], CG[Name] = "Plus1")` under Double is 122, while re-selecting the same item does nothing: `CALCULATE([S], CG[Name] = "Double")` under Double is 120. Sideways recursion works the same way: an item `CALCULATE(SELECTEDMEASURE(), CG[Name] = "Double")` is Double.
- **SELECTEDMEASURE, SELECTEDMEASURENAME, ISSELECTEDMEASURE and SELECTEDMEASUREFORMATSTRING** are evaluated inside items. Outside one they raise Desktop's error ("no measure reference in the current context"). An error inside SELECTEDMEASURE() is the item's error.
- **Format strings:** `engine.evaluate_format_strings` returns the format a selected item gives a measure, and `pbix_evaluate_dax` reports it as `format_string`. Desktop over MDX (`build_b121c.py`, cell FORMAT_STRING, the item format strings set through Desktop's own TOM):
  - the outermost item that has a format-string expression decides; an item without one passes to the next, and then to the measure's own format;
  - in that expression SELECTEDMEASURE() is the value being formatted, the item included: `IF(SELECTEDMEASURE() > 100, "#,0", "0.00")` on Triple of 60 is "#,0".
- **Measured:** all 189 + 280 + 80 cells of the three batteries match Desktop, errors included, and so do all 80 format strings. On Awesome Chocolates under QOQ, `Sales Actual` (5,361,207.75), `Sales` and `Max Quarter Not Blank` match Desktop. The previous-quarter values do not yet: `Sales Previous`, and with it `Sales Change` and the CF and colour measures. Their `[Max Previous Quarter Not Blank]` needs #118, because Desktop keeps the DATEADD filter inside ALLSELECTED. They also take 95–160 s each here: a per-date FILTER of a date measure.
- **Pinned** by `tests/test_issue121_calculation_groups.py` (200 tests) and `tests/test_issue121_calc_group_rules.py` (360). Every expected value was generated from Desktop's output. **559 fail on 0.9.121.**

### Fixed — text that is no number is an error in arithmetic (issue #133)

- **What was wrong:** text that is no number gave BLANK in arithmetic, where Desktop raises *Cannot convert value 'x' of type Text to type Number*. The BLANK then folded into confident numbers: Add100 over Double of a text measure answered 100, and IFERROR / ISERROR never saw an error.
- **The fix:**
  - `+ - * /` with such text raises Desktop's error. Numeric and date text still converts, now also with the model culture's group separator, a currency symbol, and a culture-ordered date (`"3,5" + 0` is 35 and `"$3" + 0` is 3 in en-US);
  - unary minus converts as binary arithmetic does: `-"3"` is −3, `-TRUE()` is −1, `-DATE(2024, 1, 1)` is −45292. It raises on other text.
- **Measured:** 30 of Desktop's 40 probes (`build_b133.py`) match; 0.9.121 matched 14. **Unchanged, and different from Desktop:**
  - comparisons between text and a number or a Boolean (`"3" = 3` and `TRUE() = 1` raise in Desktop), because the engine compares ISO date strings internally;
  - ABS, POWER, DIVIDE and SUMX given text.
- **Pinned** by `tests/test_issue133_text_arithmetic.py`: 31 tests. **17 fail on 0.9.121.**

### Fixed — a VAR is evaluated when it is first used (issue #134)

- **What was wrong:** every VAR was evaluated up front. DAX evaluates a variable the first time it is used, in the filter context where it is defined, and never if it is not. So an unused variable that failed failed the measure: `VAR x = ERROR("boom") RETURN 5` was an error where Desktop gives 5. With #133 `"a" * 1` is such a failure. And a calculation item like Awesome Chocolates' QOQ computed all five of its period VARs for every measure, though its SWITCH uses one.
- **The fix:** a VAR is a thunk (`_LazyVar`), forced at its first use and remembered, in the context and with the variables it was defined with. A variable's error is not caught where the variable is used: Desktop fails `VAR x = ERROR("boom") RETURN IFERROR(x, 9)`, and so does the engine.
- **Pinned** in `tests/test_issue134_135_lazy_vars_iferror.py`: the lazy-VAR probes plus a test that a used variable is evaluated once. **4 fail on 0.9.121.**

### Fixed — IFERROR / ISERROR react to errors, not to BLANK (issue #135)

- **What was wrong:** IFERROR returned the fallback when its argument was BLANK, and ISERROR answered `val is None`, a rule from when the engine reported errors as BLANK. Desktop: `IFERROR(BLANK(), 5)` is BLANK, `ISERROR(BLANK())` FALSE, and `IFERROR(DIVIDE(1, BLANK()), 5)` BLANK. So a measure like `IFERROR(DIVIDE([a], [b]), 0)` read 0 where Desktop shows nothing. A division by zero, on the other hand, *is* an error there: `IFERROR(1 / 0, 5)` is 5.
- **The fix:** both catch a raised error, an infinite or NaN result, and a referenced measure that failed. The engine still degrades such a measure to BLANK, so they count the degradations around their argument (Desktop: `IFERROR([Bad], 13)` is 13).
- **Measured:** 24 of the 25 probes of `build_b134.py` match, lazy VARs included, against 15 on 0.9.121. The 25th, `VAR x = [Bad] RETURN x`, fails in Desktop; the engine still degrades a referenced measure's error to BLANK outside IFERROR / ISERROR.
- **Pinned** in `tests/test_issue134_135_lazy_vars_iferror.py`: 25 tests in all. **5 fail on 0.9.121** for this issue.

### Fixed — DATESINPERIOD around 29 February and month ends (issue #122)

- **What was wrong:** the YEAR branch built `datetime(year + offset, month, day)`, which raised on 29 February, so a moving-annual total was BLANK that day. MONTH and QUARTER clamped the day number but did not map a month end to a month end.
- **Desktop 2.152:** the far end shifts as DATEADD's month-end rule does, and it is exclusive:
  - one YEAR back from 2024-02-29 is 2023-03-01..2024-02-29, 366 days;
  - from 2025-02-28 it is 2024-03-01..2025-02-28, 365 days, because a month end maps to a month end;
  - one YEAR forward from 2024-02-29 ends 2025-02-27;
  - one MONTH back from 30 April is 1..30 April;
  - one QUARTER back from 30 November starts 1 September.
- **The fix:** MONTH, QUARTER and YEAR all shift through `_shift_date`.
- **Pinned** by `tests/test_issue122_datesinperiod_month_ends.py`: 13 tests, 5 fail on 0.9.121.

### Fixed — RANKX's order argument (issue #124)

- **What was wrong:** the order was read as "DESC appears in the text", so an omitted or empty order, `0`, `FALSE` and `FALSE()` all ranked low to high.
- **The fix:** those rank high to low, and `1`, `TRUE` and `ASC` rank low to high, as in Desktop. Any other expression is evaluated.
- **Pinned** by `tests/test_issue124_rankx_order.py`: 60 cells, 15 fail on 0.9.121.

### Fixed — RANKX with BLANK, text and dates (issue #125)

- **What was wrong:** a BLANK value returned BLANK, rows whose expression was BLANK were left out, and only numbers were ranked.
- **The fix:**
  - BLANK counts as 0 for numbers and as `""` for text, in the value and in every row. Desktop ranks a member with no deals 5th of 6, above a −5;
  - text ranks case-insensitively, as the formula engine compares;
  - dates rank as their serials;
  - a member wrapped in `IF(ISBLANK(...), BLANK(), RANKX(...))` stays BLANK.
- **Pinned** by `tests/test_issue125_rankx_blank.py`: 54 cells, 39 fail on 0.9.121.

### Fixed — a DateTime with a time of day joins by date part only across DatePartOnly (issue #126)

- **What was wrong:** the two engine paths disagreed:
  - `evaluate_per_dimension` matched exact values, so a 12:30 departure found no row of an auto date/time calendar. Briqlab's "Monthly Flight Trends" was BLANK in all 12 months;
  - the per-value path matched date parts on every relationship.
- **Desktop 2.152:**
  - a relationship with `JoinOnDateBehavior` = DatePartOnly, which auto date/time writes, joins on the date. Briqlab's 300 departures all land on their months;
  - DateAndTime, the default, joins exact values only. On a date-only calendar the times go to the blank member.

  This was confirmed by switching one relationship to DatePartOnly through Desktop's own TOM and recalculating.
- **The fix:**
  - the reader passes `JoinOnDateBehavior` to the engine. When a `LocalDateTable_*` relationship arrives without it, DatePartOnly is assumed, since Desktop writes that on every one;
  - a DatePartOnly hop compares date parts in its keys, its fact lookup, its blank row, multi-hop paths and the per-dimension buckets;
  - every other join aliases a date-time to its bare date only at midnight.
- **Pinned** by `tests/test_issue126_date_part_joins.py`: 7 tests, 6 fail on 0.9.121.

### Fixed — DATEADD / SAMEPERIODLASTYEAR over a table of dates (issue #127)

- **What was wrong:** only a column reference was parsed. A table expression such as `DATESBETWEEN(...)` or `FILTER(ALL('Date'[Date]), ...)` returned a marker, so COUNTROWS and MINX saw nothing, and as a CALCULATE filter the shift was dropped without a trace. `CALCULATE(COUNTROWS('Date'), DATEADD(DATESBETWEEN(… March …), -1, MONTH))` counted the whole calendar (1,096) where Desktop counts February's 29.
- **The fix:** the table's dates are shifted as a column's visible dates are, and applied as the filter on that column.
- **Pinned** by `tests/test_issue127_dateadd_table_arg.py`: 8 tests, all failing on 0.9.121.

### Fixed — PBIXBuilder stores a DatePartOnly relationship as Desktop does (issue #128)

- **What was wrong:** `add_relationship(..., join_on_date_behavior=2)` wrote the flag but stored an exact join: `RelationshipStorage.DefinitionType` 0 and an exact R$ index. Desktop reads the join from that storage, so the date part was never used, even after a full refresh.
- **The fix:** `DefinitionType` 2, index flag 8, and an R$ index built by each key's date part. These are what Desktop's own auto date/time relationships carry.
- **Measured:** Desktop 2.152 answers January 3, February 2 and blank 1 on the built file, and the same after a full refresh.
- **Pinned** by `tests/test_issue128_builder_date_part_join.py`: 2 tests, both failing on 0.9.121.

### Fixed — DATEADD keeps a single date's day number (issue #129)

- **What was wrong:** both ends of each shifted run went through the month-end rule, so 30 April alone one MONTH back was 31 March.
- **Desktop 2.152:**
  - a run's start, and a single date, keep their day number, clamped: 30 Apr → 30 Mar, 29 Feb + 1 MONTH → 29 Mar, 30 Nov − 1 QUARTER → 30 Aug;
  - only the end of a run of two or more days that closes its month moves to the target month's end: 29–30 Apr → 29–31 Mar, 30 Apr–2 May → 30 Mar–2 Apr.
- **Pinned** by `tests/test_issue129_dateadd_month_end.py`: 22 cases. All fail on 0.9.121, whose table form returned nothing (#127); 12 cells are #129's own.

### Fixed — built reports use the modern visual header (issue #130, PR #123 by @allanon2)

- **What was wrong:** `PBIXBuilder` wrote report settings without `useStylableVisualContainerHeader: true` ("Use the modern visual header with updated styling options"). Desktop drew a built report's visuals in the legacy container: rendered side by side in Desktop 2.152, their content sat about 13–18 px lower than in the same report with the setting.
- **The fix:** the builder writes the setting. All 26 Desktop-authored reports in the local corpus carry it.
- **Credit:** reported by @allanon2 in PR #123, which made the same one-line change. Solved and verified on our side.
- **Pinned** by `tests/test_issue130_builder_modern_header.py`, which fails on 0.9.121.

### Fixed — a column whose name begins or ends with a space is found (issue #132)

- **What was wrong:** Microsoft's Financial Sample names its sales column `' Sales'`, with a leading space, and its reports read it as `financials[ Sales]`. Every parse of a reference strips the bracketed name, and the column lookup then compared `'Sales'` with `' Sales'` exactly. So `SUM(financials[ Sales])` found no column. Every measure built on it was BLANK: PY Sales, MTD / QTD / YTD, MOM / QOQ / YOY and their percentages, Current Year, Current Month. Desktop gives 118,726,350.26 for Sales.
- **The fix:**
  - the column lookup falls back to names compared without leading or trailing spaces;
  - a row reads its value by the model's spelling of the name;
  - the unresolvable-reference check compares stripped names too.
- **Measured:** Arrow Chart - Microsoft Financial Dataset matches 45 of 49 measures, against 3 on 0.9.121, and Target Line Bar Charts 8 of 11, against 2. Of the rest:
  - `Sales PM` and `Sales MOM` are #118: Desktop keeps the DATEADD filter inside ALLSELECTED;
  - `Max x-axis Adj` runs out of time.
- **Pinned** by `tests/test_issue132_column_name_spaces.py`: 14 tests (aggregates, an iterator, a CALCULATE filter, FILTER, VALUES, a slicer key, and the Arrow Chart report against Desktop). **13 fail on 0.9.121.**

## [0.9.121] - 2026-10-09

Two fixes reported by OpenBI (docs 50 and 51), and one found by the release's own corpus census (#131).

- **ALLSELECTED:** checked against Power BI Desktop 2.152 over ADOMD. A 400-cell matrix has 390 cells matching; 0.9.120 matched 139. The other 10 are #118.
- **Colours:** checked against Desktop's own code and a render. The recoloured Desktop-authored template keeps its white and grey cells.
- **No regressions:** every Desktop probe from earlier releases still matches.

### Fixed — a palette colour's ColorId counts white and black first (issue #119)

- **What was wrong:** a `ThemeDataColor` reference was read as `dataColors[ColorId]`. Desktop's colour picker row is pure white, pure black, then the theme's first eight data colours, and ColorId indexes that row. So:
  - `pbix_extract_colors` reported "White, 10% darker", the commonest panel fill, as a blue (`#0F7EE5`) instead of `#E6E6E6`;
  - `pbix_recolor`, remapping the theme's first data colour, rewrote every white and grey fill to the new colour. Desktop 2.152 then painted the Matrix Bubble Chart template's cells magenta.
- **Ground truth:** Desktop's own code (`desktop.min.js`). `DataColorPalette.basePickerColors` is `[#FFFFFF, #000000]` plus `_.take(dataColors, 8)`, and `getThemeDataColor` returns `basePickerColors[ColorId]`. Two consequences:
  - 0 and 1 are fixed white and black, not the theme's background and foreground, which answers the question OpenBI's ledger had left open;
  - ColorId 10 and up name no colour.
- **The fix:**
  - ColorId resolves through that row;
  - the Percent shade is a port of Desktop's own function. Each channel moves |Percent| of the way to white or black, rounded as JS `Math.round` does. Run verbatim under Node, Desktop's function and the port agree on all 7,560 colour/percent pairs tried;
  - a reference that names no colour is listed as unresolved, and `pbix_recolor` leaves it alone. Before, it was drawn as a made-up `#808080`.
- **Measured:** on Matrix Bubble Chart, recolouring the first data colour now leaves its seven white and grey references as theme references, and Desktop renders them as before. 0.9.120 turned all seven into the new colour.
- **Pinned** by `tests/test_issue119_theme_data_color.py`: 28 tests, all failing on 0.9.120. They cover the picker row, Desktop's shades, and the two tools on a built report.

### Fixed — ALLSELECTED(<table>) as a table returns the selected rows (issue #120)

- **What was wrong:** used as a table, iterated by COUNTROWS, MINX, MAXX, SUMX, FILTER or CONCATENATEX, `ALLSELECTED(T)` returned a marker only CALCULATE understands, so the iterator saw no rows. Matrix Bubble Chart's `MINX(ALLSELECTED('Date'), [Sales])` was BLANK, so no bubble drew.
- **Desktop 2.152's rules,** measured one slicer at a time:
  - **the table form** is the table's rows under every selection that reaches it. A `D[Zone]` slicer leaves `ALLSELECTED(T)` two of four rows;
  - **the column form** keeps only the selection on its own columns. `COUNTROWS(ALLSELECTED(T[Region]))` stays 2 under slicers on `T[Amount]`, `T[Cat]`, `D[Zone]` and `D[Region]`, and is 1 only under a `T[Region]` slicer. 0.9.120 applied every selection to the column form too, so it gave 1 under those slicers, as did OpenBI's port, whose expected value in doc 51 was 1;
  - a grouping is never part of the selection, in either form.
- **The fix:** the table form returns the table's rows under the selection, plus the blank row when the selection keeps it. They are full rows, like `ALL(T)`'s, so an iterator transitions over each one. The column and multi-column forms keep only the selection on their own columns. As a CALCULATE filter argument ALLSELECTED is unchanged.
- **Measured:** Desktop's answers for 25 measures under 11 query shapes (ungrouped, grouped, under a slicer, grouped under a slicer). 390 of 400 cells match, against 139 on 0.9.120. The 10 that don't keep an explicit CALCULATE filter inside the measure, which is #118.
- **Pinned** by `tests/test_issue120_allselected_table.py`: 384 tests, every expected value generated from Desktop's output. **245 fail on 0.9.120.**
- **Still open (#116):** the running-total shape `FILTER(ALL(T), T[c] <= MAX(T[c]))` keeps no rows, with ALL and ALLSELECTED alike, because FILTER binds no row context when its condition holds an aggregate.

### Fixed — "the last week with sales" is evaluated once, not once per date (issue #131)

- **What was wrong:** once ALLSELECTED(<table>) returned its rows (#120), Awesome Chocolates' `Selection max date` and `Selection Sales Calculation` ran out of time and returned BLANK. Desktop answers 2024-02-26 and 34,042,511.25. Both FILTER the 731 dates by `'dim-Date'[WeeknYear] = [Max Week Not Blank]`, where `Max Week Not Blank = MAXX(FILTER(ALLSELECTED('dim-Date'), NOT ISBLANK([Sales Actual])), 'dim-Date'[WeeknYear])`. FILTER puts each date's columns in the filter context, so the measure memo, keyed on the whole context, evaluated `[Max Week Not Blank]` 731 times over 731 dates each.
- **Why that was waste:** the value cannot depend on any filter on `'dim-Date'`. ALLSELECTED restores the selection, and every `[Sales Actual]` inside runs under a whole `'dim-Date'` row, which replaces the caller's `'dim-Date'` filters.
- **The fix:** the memo key leaves out the filters on such a table. The proof is syntactic and conservative. The measure may be built only from:
  - literals, operators, VAR/RETURN, and functions of their arguments alone;
  - measures with the same property;
  - MAXX / MINX / SUMX / AVERAGEX / COUNTX / COUNTROWS over ALL(T), ALLSELECTED(T) or FILTER over them, whose row expression reads only the row's columns and measure references.
  The table may have no BLANK cell. Anything else keeps the full key, such as a top-level aggregate, VALUES or CALCULATE. It holds while ALLSELECTED restores the outermost selection; #118 has to revisit it.
- **Measured:** `Selection max date` takes 0.24 s, down from over 60 s. All 49 measures of the file take 4.8 s. Both measures match Desktop.
- **Pinned** by `tests/test_issue131_memo_shield.py`: 21 tests. **19 fail before the fix**; the 20th, the values themselves, was already right, only slow, and the corpus test is skipped without the file. They cover which measures may and may not be shielded, values with and without the shield under four slicers, and one evaluation instead of one per row.

## [0.9.120] - 2026-10-08

Three fixes reported by OpenBI's port of the engine (OpenBI docs 48 and 49), plus one found while verifying them.

- **ROUND and ISFILTERED:** checked against Power BI Desktop 2.152 over ADOMD. All 32 cells match; 0.9.119 got 19 of them wrong.
- **ISFILTERED / ISCROSSFILTERED battery:** 276 cells, all matching Desktop except the 6 that belong to #114. 0.9.119 got 75 wrong.
- **PBIR groups:** checked on a Desktop-authored PBIR report, which Desktop opens after an edit.
- **No regressions:** across 402 Desktop-measured probes, 0.9.120 fixes 95 cells and gets none wrong that 0.9.119 got right.

The batteries also exposed five older engine defects, filed as #114–#118 and still open:
- a table filter does not act on its expanded table (#114);
- a context transition over a fact row does not filter its dimensions (#115);
- a table expression in a row context sees only the current row (#116);
- SUMMARIZE by a related column (#117);
- ALLSELECTED drops explicit filters (#118).

### Fixed — ROUND rounds half away from zero (issue #110)

- **What was wrong:** `ROUND` was Python's `round()`, which rounds a tie to the even neighbour and works on the binary double. Desktop rounds half away from zero, on the decimal value:

  | expression | Desktop 2.152 | 0.9.119 |
  |---|---|---|
  | `ROUND(2.5, 0)` | 3 | 2 |
  | `ROUND(-2.5, 0)` | -3 | -2 |
  | `ROUND(0.5, 0)` | 1 | 0 |
  | `ROUND(1250, -2)` | 1300 | 1200 |
  | `ROUND(5, -1)` | 10 | 0 |
  | `ROUND(1.005, 2)` | 1.01 | 1.0 |
  | `ROUND(0.125, 2)` | 0.13 | 0.12 |
  | `ROUND(2.675, 2)` | 2.68 | 2.67 |
  | `ROUND(1.45, 1)` | 1.5 | 1.4 |

- **The fix:** ROUND rounds the shortest decimal form of the number half away from zero, the same arithmetic `FORMAT` already used. Negative digits round to tens and hundreds, and a whole number stays whole. `ROUNDUP` and `ROUNDDOWN` were already right and are unchanged.
- **Pinned** by `tests/test_issue110_round_half_away.py`: 20 tests, 13 fail on 0.9.119. The 7 that pass are values where the two rules agree, such as `ROUND(15, -1)` = 20 and `ROUND(2.345, 2)` = 2.35, plus a BLANK guard.

### Fixed — ISFILTERED(<table>) sees a filter on the table's columns (issue #111)

- **What was wrong:** a bare table name evaluated to the table's rows, so `ISFILTERED(<table>)` never matched a filter and was always FALSE. Desktop 2.152 answers TRUE to all of these, and 0.9.119 answered FALSE:
  - `CALCULATE(ISFILTERED(Orders), Orders[Revenue] > 100)`, and the quoted `'Orders'`;
  - `CALCULATE(ISFILTERED(Orders), Orders[Revenue] > 100, ALL(Orders))`;
  - `CALCULATE(ISFILTERED(Dim), Dim[Zone] = "z1")`;
  - `ISFILTERED(Dim)` on every row of a grouping by `Dim[Zone]`.
- **The fix:** a table name, bare or quoted, is recognised before it is evaluated. `ISFILTERED(<table>)` is TRUE when any of the table's columns has a direct filter, including a TREATAS filter on several columns (#103).
- **Unchanged, as in Desktop:** a filter that only reaches the table through a relationship does not count. `CALCULATE(ISFILTERED(Orders), Dim[Zone] = "z1")` stays FALSE, and so does `ISFILTERED(Orders)` under a grouping by `Dim[Zone]`.
- **Still open (#114):** Desktop also counts a table filter's expanded table as a direct filter. `ISFILTERED(Dim)` is TRUE under `FILTER(Orders, ...)`, and the engine still says FALSE.
- **Pinned** by `tests/test_issue111_isfiltered_table.py`: 9 tests, 6 fail on 0.9.119. The 3 that pass are the column form, the unfiltered table and the cross-filter case.

### Fixed — ISCROSSFILTERED sees every filter that reaches the table (issue #113)

- **What was wrong:** like ISFILTERED, `ISCROSSFILTERED(<table>)` was never TRUE. The column form also missed a filter on another column of its own table, and a filter on column combinations. Desktop 2.152 answers TRUE and 0.9.119 answered FALSE to, for example:
  - `CALCULATE(ISCROSSFILTERED(Orders), Orders[Revenue] > 100)`;
  - `CALCULATE(ISCROSSFILTERED(Orders[Revenue]), Orders[Region] = "N")`;
  - `CALCULATE(ISCROSSFILTERED(Orders), Dim[Zone] = "z1")`, where the one side filters the many side;
  - `CALCULATE(ISCROSSFILTERED(Dim), FILTER(Orders, ...))`;
  - a filter crossing a bidirectional relationship;
  - `TREATAS({("N", "z1")}, Dim[Region], Dim[Zone])`, which reaches `Orders`.
- **The fix:** both functions share one argument parser, which recognises a table by its name and reads a column as a reference, without evaluating it. Then:
  - `ISCROSSFILTERED(<table or column>)` is TRUE when anything filters the table: a direct filter on any of its columns, a filter on column combinations that reaches it, or a relationship that carries another table's filter to it. Desktop answers the column form exactly as the table form.
  - Inside an iterator, both functions read the filter context outside it until CALCULATE or a measure reference makes the row a filter. Desktop: `CALCULATE(SUMX(Orders, IF(ISFILTERED(Orders[Revenue]), 1, 0)), Orders[Revenue] > 100)` is 2 (0.9.119: 0), and without the outer filter it is 0.
- **Unchanged, as in Desktop:** these stay FALSE:
  - a column filter on the many side does not cross a single-direction relationship: `CALCULATE(ISCROSSFILTERED(Dim), Orders[Revenue] > 100)`;
  - `ALL(Orders)`;
  - `REMOVEFILTERS(Orders)` over an outer filter on `Orders`.

  A filter key naming a column the table does not have filters nothing, so it does not count. That keeps `CALCULATE(ISFILTERED(Orders), SUMMARIZE(Orders, Dim[Zone]))` FALSE, as in Desktop, while #117 is open.
- **Pinned** by `tests/test_issue113_iscrossfiltered.py`: 236 tests, every expected value Desktop's. **64 fail on 0.9.119.** They cover:
  - the battery, as 210 single cells plus 5 grouped rows;
  - 21 edges: iterators, nested ALL / ALLEXCEPT / REMOVEFILTERS, and table expressions as filters.

  The 6 cells that belong to #114 are left out.

### Fixed — PBIR visual groups survive the classic layout (issue #112)

- **What was wrong:** the PBIR reader dropped a visual's `parentGroupName` and a group's `visualGroup`, so a PBIR report's groups disappeared:
  - every grouped visual read back as top-level, with its group-relative position. A matrix inside a group at (325, 502) was drawn at the page's top-left.
  - every group read back as a visual with an empty `singleVisual`.

  The Ecommerce Conversion Dashboard template groups 10 of its 22 visuals, and 0.9.119 read none of them as grouped.
- **The fix:** the reader builds the classic shape Desktop writes (all 138 groups in the local corpus):
  - a child gets `config.parentGroupName` and keeps its group-relative x/y; `pbix_get_visual_positions` adds the group's origin back;
  - a group gets `config.singleVisualGroup` `{displayName, groupMode, isHidden}` and no `singleVisual`.

  PBIR spells the mode `"ScaleMode"` / `"ScrollMode"`; classic stores Desktop's enum number (0 / 1). The PBIR writer writes these fields back only when they change: a visual moved into or out of a group, or a group renamed, re-moded, hidden or shown.
- **Measured:** on the template, a save without edits leaves every `visual.json` as it was. Renaming a group and ungrouping a child changes exactly those two files, and Desktop 2.152 opens and renders the edited report.
- **Pinned** by `tests/test_issue112_pbir_groups.py`: 4 tests, 3 fail on 0.9.119. The one that passes is the guard that a save without edits changes nothing.

## [0.9.119] - 2026-10-08

Three fixes found while verifying 0.9.118. Each is checked against Power BI Desktop 2.152 over ADOMD, through the 0.9.118 battery extended to 44 queries:

- **Engine:** all 2,046 cells match, through `pbix_evaluate_dax` and `pbix_evaluate_dax_grouped`.
- **Builder:** the built and the Desktop-refreshed model answer all 44 queries identically.

### Fixed — text compares case-insensitively in expressions (issue #107)

- **What was wrong:** the expression evaluator compared text with Python's case-sensitive operators everywhere. Desktop's formula engine compares text linguistically and case-insensitively. Desktop 2.152 answers 1 to all of these, and 0.9.118 answered 0 or BLANK:
  - `IF("North" = "north", 1, 0)` (`==` too);
  - `IF("a" < "B", 1, 0)`;
  - `FILTER(F, F[Region] = "north")`;
  - `SWITCH("north", "North", 1, 0)`;
  - `LOOKUPVALUE(.., F[Region], "south")`;
  - `"NORTH" IN VALUES(F[Region])`;
  - `CONTAINS(F, F[Region], "north")`;
  - `CONTAINSROW(VALUES(F[Region]), "north")`.

  In the same way `COUNTROWS(DISTINCT({"a", "A", "b"}))` is 2, `MAXX({"a", "B"}, [Value])` is `"B"`, and a predicate `F[Region] <> "north"` excludes North.
- **The formula engine folds fully:** `"Ä" = "ä"`, `"Σ" = "σ"` and `"Straße" = "STRASSE"` are TRUE. A column's stored values fold ASCII letters only (#102, #109).
- **The fix:**
  - comparison operators, `_compare` (IN, predicates), SWITCH, LOOKUPVALUE (now through the column's typed index), CONTAINS and CONTAINSROW compare text case-insensitively;
  - `DISTINCT` over a table expression dedupes the same way, keeping the first spelling;
  - `MAXX` / `MINX` order text case-insensitively.
- **Unchanged, as in Desktop:** `FIND`, `EXACT` and `SUBSTITUTE` stay case-sensitive, and accents still count (`"é" = "e"` is FALSE).
- **Pinned** by `tests/test_issue107_text_comparisons.py`: 24 tests, 18 fail on 0.9.118. The 6 that pass are the case-sensitive and accent controls, plus TOPN's ordering, which was already right.

### Fixed — iterating a CROSSJOIN reads every table's columns (issue #108)

- **What was wrong:** CROSSJOIN merged each row's right part under `_2_`-prefixed keys, so an iterator's row context could not read the second table's columns:
  - `COUNTROWS(FILTER(CROSSJOIN(VALUES(Region[Region]), VALUES(Prod[Cat])), Prod[Cat] = "B"))` was BLANK, where Desktop says 3;
  - `CONCATENATEX` printed `('Prod', 'Cat')`;
  - `SUMX(CROSSJOIN(..), [m])`'s context transition left the right table unfiltered;
  - a FILTER over a CROSSJOIN used as a CALCULATE filter, or fed to `TREATAS` (the one cell #103 left open), lost the right table.
- **The fix:**
  - each CROSSJOIN row keeps its parts, one row per source table, and CROSSJOIN takes any number of tables;
  - the row context reads each part, and the context transition filters all of them;
  - TREATAS takes the parts' values in order;
  - a FILTER over a CROSSJOIN used as a CALCULATE filter becomes a filter on column combinations (#103), not on the left table's rows.
- **Measured:** every cell matches Desktop. Examples: `SUMX(CROSSJOIN(..), [s3])` is 31, `CALCULATE([s3], FILTER(CROSSJOIN(..), Region = "S" || Cat = "B"))` is 14, and the TREATAS form is 14 at the total and 2 / 4 / 8 by region.
- **Pinned** by `tests/test_issue108_crossjoin_rows.py`: 21 tests, 17 fail on 0.9.118. The 4 that pass are row counts and cells where the first table's column alone decides the answer.

### Fixed — PBIXBuilder folds only what the column store folds (issue #109)

- **What was wrong:** the #43 fold (`vertipaq_encoder._val_key`) used `str.casefold()`. Desktop 2.152's own import of 14 names keeps 13 distinct values; only `Apple` / `apple` fold. These stay apart:
  - `Äpfel` / `äpfel`, `Øl` / `øl`, `Éa` / `éa`;
  - `Σίγμα` / `σίγμα`, `Дом` / `дом`;
  - `Straße` / `Strasse`.

  The builder stored 7 values: it rewrote the caller's data and `DISTINCTCOUNT` dropped. Its warning called `ß` / `ss` a case difference. `pbix_doctor` reported a Desktop-made file holding `Äpfel` and `äpfel` as one Desktop "WILL refuse to load".
- **The fix:** one key, `vertipaq_encoder.column_text_key`, folds ASCII letters only. The encoder, the pre-build warning and the doctor's dictionary check all use it.
- **Measured:** the built model and Desktop's refresh of the same rows now answer alike, with 13 distinct values. The extended battery's built and refreshed answers are identical on all 44 queries.
- **Pinned** by `tests/test_issue109_builder_ascii_fold.py`: 2 tests, both fail on 0.9.118.

## [0.9.118] - 2026-10-08

Seven fixes. Six come from pull requests by @allanon2 (#94–#99), reviewed read-only, reproduced and solved here; the seventh, #101, was found while verifying them.

Every engine change is checked against Power BI Desktop 2.152 over ADOMD: 41 queries and 1,941 cells through two tool paths (`pbix_evaluate_dax` and `pbix_evaluate_dax_grouped`). All of them match, except 14 cells that belong to #107 and #108, filed for the next release.

### Fixed — ALLSELECTED(Table) acts on the table's expanded table (issue #100, PR #94 by @allanon2)

- **What was wrong:** as a CALCULATE modifier, `ALLSELECTED(Fact)` dropped the grouping filters on `Fact`'s own columns only. DAX drops them on the table's **expanded** table, so a visual grouped by a dimension column kept that grouping. The standard share-of-total measure, `DIVIDE(SUM(Fact[v]), CALCULATE(SUM(Fact[v]), ALLSELECTED(Fact)))`, read 1 on every row of a visual grouped by `Dim[Name]`, where Desktop reads 0.356 / 0.444 / 0.2.
- **The fix:** the filters removed and the outer selection restored are now those on every table of the expanded table (`DAXContext.expanded_tables`):
  - the one side of each active many-to-one relationship, followed transitively (a snowflake);
  - both ends of a one-to-one;
  - not a many-to-many relationship, which expands nothing.
- **A slicer is an outer selection and stays**, as before.
- **Measured:** Fact -> Dim -> Cat, Dim <-> Twin one-to-one, Fact <-> Tags many-to-many. Every cell matches Desktop: 320 + 210 (two tool paths), where 0.9.117 missed 103 + 67.
- **Pinned** by `tests/test_issue100_allselected_expanded_table.py`: 23 tests, 16 fail on 0.9.117. The 7 that pass are controls (many-to-many, a fact column, a slicer without a grouping).

### Fixed — ALL / REMOVEFILTERS / ALLEXCEPT on a table act on its expanded table (issue #101)

- **Found while verifying #100.** `ALL(Table)` dropped the table's own column filters, then stopped every filter reaching the table through any relationship.
- **What DAX does:** it removes the filters on every column of the expanded table and leaves every other filter in force. Desktop 2.152 differed from the engine in three ways:
  - **The dimension's own filter.** `CALCULATE(COUNTROWS(Dim), ALL(Fact))` grouped by `Dim[Name]` is 3, not 1.
  - **A kept dimension column.** `CALCULATE(SUM(Fact[v]), ALLEXCEPT(Fact, Dim[Name]))` grouped by `Dim[Name]` is each member's 16 / 20 / 9, not the 45 total. That is the common "category total" pattern.
  - **Filters from outside the expanded table.** A filter arriving over a many-to-many or a bidirectional relationship stays. `ALL(Fact)` under `Tags[TagName]` is the tag's 27 / 18, and `COUNTROWS(Dim)` under `ALL(Dim)` still sees a `Fact2` filter that crosses a both-ways relationship (2, not 3).
- **The fix:** all three modifiers remove the expanded table's filters (`DAXContext.without_columns`). Of the filters still live, only two kinds are kept away from the table:
  - a table-filter argument on a many-side table;
  - a date-table filter with no relationship.

  `ALLCROSSFILTERED` keeps removing cross filters too. A filter created later, inside a nested CALCULATE, still applies, as before.
- **Measured:** 345 + 315 cells match Desktop, where 0.9.117 missed 108.
- **Pinned** by `tests/test_issue101_all_expanded_table.py`: 27 tests, 19 fail on 0.9.117.

### Fixed — a filter on a column matches values as DAX compares them (issue #102, PR #95 by @allanon2)

- **What was wrong:** list filters went through `str()` keys with float and calendar-day aliases. That covers `filter_context`, `T[C] = v`, `T[C] IN {...}`, single-column TREATAS, and the grouped tools' per-group filter.
  - `'01'` also selected `'1'`.
  - A date-time selected every time of its day.
  - `"north"` selected nothing.
- **The fix:** a filter on the column itself uses a typed index, and `make_value_matcher` (and `IN` / `NOT IN`) follows the same rules:
  - text stays text, and matches case-insensitively for **ASCII letters only**, as the column store compares values;
  - numbers compare by value whatever their storage (#39 kept);
  - date-times are moments, and a date is midnight (the DATESMTD representation case kept).

  Relationship joins keep their own key spellings. A caller may still pass a number for a text code, or a numeric string for a number, but never `'01'` for `1`.
- **Measured:** every cell matches Desktop, including grouped by `F2[Code]` and by `F2[At]`; 0.9.117 missed 88 + 60 cells. On the ASCII-only rule, Desktop's own import of 14 names keeps 13 distinct values: only `Apple` / `apple` fold. `Äpfel` / `äpfel`, `Øl` / `øl`, `Éa` / `éa`, `Σίγμα` / `σίγμα`, `Дом` / `дом` and `Straße` / `Strasse` stay apart, and a filter on `"ÄPFEL"` selects `Äpfel` alone. Expressions compare differently, with `"Ä" = "ä"` and `"ß" = "ss"`; that is #107.
- **Pinned** by `tests/test_issue102_filter_value_matching.py`: 32 tests, 20 fail on 0.9.117.

### Fixed — TREATAS onto several columns filters their combinations (issue #103, PR #99 by @allanon2)

- **What was wrong:** a row constructor `("N", "A")` evaluated as BLANK, and TREATAS kept only its first column. So `CALCULATE(m, TREATAS({("N", "A"), ("W", "B")}, Region[Region], Prod[Cat]))`, the filter Desktop writes for a two-column Include filter, answered BLANK silently.
- **The fix:**
  - A row constructor is one row with columns `Value1`, `Value2`, …
  - TREATAS onto several columns is one filter on their combinations. Each table sees the columns of its expanded table, so a dimension sees its own column's projection.
  - Plain TREATAS replaces the outer filters on its columns, and `KEEPFILTERS` intersects.
  - An inner CALCULATE filter on one of the columns leaves the outer combinations' projection onto the others, as DAX does.
  - A row of the wrong width is an evaluation error, not a blank.
- **Measured:** every cell matches Desktop, except one. A FILTER over a CROSSJOIN fed to TREATAS still differs, because iterating a CROSSJOIN cannot read the second table's columns (#108, filed separately; 0.9.117 has the same gap).
- **Pinned** by `tests/test_issue103_treatas_tuples.py`: 23 tests, 16 fail on 0.9.117.

### Fixed — PBIXBuilder: one query alias per table, and visual roles, aggregations, objects, filters and interactions (issue #104, PR #98 by @allanon2)

- **The bug:** `_alias_for` computed the aliases already taken after assigning the new one, minus the candidate itself, so it never saw a collision. `Region` and `Range` both got `r`, and a visual over both pointed at one source twice. Each table now gets its own alias (`r`, `r1`).
- **The feature:** `add_page` visuals may bind any roles with `{"roles": {role: [field, ...]}}`:
  - a matrix's Rows / Columns / Values;
  - a chart's Series;
  - several values;
  - implicit aggregations: sum, average, distinctCount, min, max, countNonNull, median, stdDev, variance.

  Codes and query-ref names are Desktop's own, from `customVisualsHost.js`. Code 2 is `Count(...)`, the field well's Count (Distinct); the PR's `DistinctCount(...)` name is not one Desktop writes. A bare `count` is refused as ambiguous.

  A visual may also carry formatting `objects` and visual-level `filters`. A page takes `filters` and `interactions` (its section config's `relationships`), and every page now writes `filters`, as every Desktop-authored page does.
- **Pinned** by `tests/test_issue104_builder_roles.py`: 4 tests, all fail on 0.9.117.

### Fixed — unsupported DAX functions are reported per measure (issue #105, PR #96 by @allanon2)

- **What was wrong:** `pbix_evaluate_dax` listed the unsupported functions once per call.
  - A caller could not tell which result depended on one. The engine reads an unsupported call as BLANK and carries on, so `NOSUCHFN(1) + SUM(T[v])` returns a plausible total.
  - Every BLANK in the call was labelled `"unsupported"`, a legitimately empty measure included.
- **The fix:** each result carries `unsupported_functions`, the functions its own evaluation hit, directly or through the measures it references. That includes a referenced measure served from the measure cache. Only a measure whose own evaluation hit one is `"unsupported"`. The call-wide warning is unchanged.
- **Pinned** by `tests/test_issue105_unsupported_per_measure.py`: 2 tests; 1 fails on 0.9.117 (the other is a clean-call control).

### Fixed — evaluations on different threads no longer corrupt each other (issue #106, PR #97 by @allanon2)

- **What was wrong:** the module-level `DAXEngine` carries per-evaluation state: the deadline, the ALLSELECTED snapshots, unsupported functions and errors. Under 8 threads, 264 of 336 `pbix_evaluate_dax` answers differed from the same calls made one at a time; an ALLSELECTED share read 1.0001 for 1.0.
- **The fix:** `_engine` is one `DAXEngine` per thread. The module-level caches lock their insert, evict and id-assign steps. Two relationship sets interned at once could otherwise get the same id, which keys the cross-filter memo.
- **Not affected before:** the stdio MCP server runs tools on its event-loop thread. The fix is for hosts that call the tools from several threads.
- **Pinned** by `tests/test_issue106_thread_local_engine.py`: 2 tests, both fail on 0.9.117.

### Filed for the next release

- **#107:** text comparisons in expressions (`=`, `<`, `IN`, `SWITCH`, `LOOKUPVALUE`, `CONTAINS`) are case-sensitive. #102 covers filter values only.
- **#108:** iterating a CROSSJOIN cannot read the second table's columns.
- **#109:** `PBIXBuilder` folds non-ASCII case pairs and `ß` / `ss` into one value, where Power BI keeps them distinct.

## [0.9.117] - 2026-10-08

### Fixed — a fill colour is written where Desktop reads it (issue #93)

- **What was wrong:** `pbix_format_visual` wrote `{"fill": {color,
  transparency, show}}` as one selector-less entry on every visual. On
  `shape`, `actionButton`, `pageNavigator` and `bookmarkNavigator`, the fill
  card is per interaction state, as their outline is (#74). Desktop reads
  `fillColor` and `transparency` only from the state entries, so the
  requested fill was dropped and the call answered success.
  - Desktop 2.152 painted a shape in the theme's default blue and left the
    button and page navigator unfilled.
- **The fix:** on those four visuals `show` now goes on the selector-less
  entry, and `fillColor` / `transparency` on `{"id": "default"}`. Desktop then
  renders the requested `#F2C80F` on the shape, the button and the
  navigator's page button.
- **Other visuals:** the legacy `basicShape` has no states and keeps its single
  selector-less entry, as does every other visual.
- **`fillColor` is accepted alongside `color`.** Power BI's own name wins, and
  a `color` it beats is reported as unread.
- **Measured:**
  - Desktop's report theme schema (5.71) declares a fill card on exactly these
    four visuals.
  - In the 37 Desktop-authored reports of the local corpus, the colour is
    never selector-less: shape `fillColor` 50 times on `{"id": "default"}`,
    actionButton 36, bookmarkNavigator 1.
- **Pinned** by `tests/test_issue93_fill_states.py`: 6 tests, 5 fail on
  0.9.116. The one that passes is the `basicShape` control, unchanged.

## [0.9.116] - 2026-10-08

### Fixed — every whole-number double is spelled as Desktop spells it (issue #92)

- **What was wrong:** #84 fixed the container cards (padding, spacing,
  border, drop shadow), but every other `D` literal still came out as
  Python's float repr. That covered font sizes on the title, subtitle, legend,
  data labels and both axes, the background's transparency, and every other
  float-valued property: `'13.0D'` where Desktop writes `'13D'`.
- **The fix is general, not per property:** the literal writer (`_pbi_lit`)
  spells a whole-number double bare, and fractions keep theirs (`'10.5D'`).
  `_pbi_double_lit` (#62, #84) now just delegates to it.
- **Measured on the Desktop-authored reports in the local corpus:** across 37
  of them, Desktop writes 6,204 whole-number `D` literals, and every one is
  bare. That includes all 1,421 font sizes and 684 transparencies. Not one
  `N.0D` occurs, so there is no property for which the old spelling was
  right. OpenBI's census of 27 files (its doc 46) agrees property by
  property.
- **Pinned** by `tests/test_issue92_whole_number_doubles.py`: 7 tests, 4 fail
  on 0.9.115. The 3 that pass are the fraction controls, which were already
  right.

## [0.9.115] - 2026-10-07

### Fixed — the grouped and per-dimension tools resolve a bare `[Column]` through the measure's home table (PR #89, by @allanon2)

- **What was wrong:** `pbix_evaluate_dax_grouped` and
  `pbix_evaluate_dax_per_dimension` evaluate through `evaluate_measures_batch`,
  which never received the measures' home tables. So a measure with an
  unqualified column (`AVERAGE([Revenue])`) came back **None in every group**,
  with `success: true`, while `pbix_evaluate_dax` resolved it.
- **The fix:** `evaluate_measures_batch` now takes `measure_tables` and
  `model_columns`, and both tools pass them.
- **Verified in Power BI Desktop** (2.152, ADOMD) on a model where three tables
  own a `Revenue` column. Both tools now return Desktop's values: N 20 / 1000 /
  5 / 50, S 20 / 2000 / 5 / 100.
- **Review additions:** a test through both tools themselves (the PR's tests
  call the engine function), and aligned indentation at the composite
  `group_by` call.

### Fixed — `pbix_get_m_code` reads the DataMashup PackageParts by their length prefix (PR #90, by @allanon2)

- **What was wrong:** the reader scanned to the **last**
  end-of-central-directory record. When the MS-QDEFF Metadata ends with its
  own content zip, that record belongs to the wrong archive, and the reader
  returned `[No Section1.m found. Archive contains: []]`.
- **The fix:** it now reads `PackageParts` by the length prefix the MS-QDEFF
  specification defines.
- **Verified on Microsoft's own samples:** the 2018 Fuzzy Matching demo `.pbix`
  and the COVID-19 US Tracking `.pbit`, downloaded from
  microsoft/powerbi-desktop-samples at the commits that added them, with their
  bytes matching the repository's git blob hashes. Both now return their M. A
  Metadata that ends with a *non-empty* zip, where the old scan read the wrong
  archive's entries, reads correctly too.

### Fixed — `pbix_set_m_code` rewrites only the PackageParts (issue #91)

- **What was wrong:** the writer had the same last-record scan, and it was
  destructive. On both samples above it returned success and left a
  DataMashup that no longer parsed: all queries, Permissions and Metadata
  gone. On a synthetic part with an empty content zip it shrank the part from
  378 to 34 bytes and left the length field stale.
- **The fix:** the writer now replaces only the `PackageParts` zip and its
  length. Permissions, Metadata and PermissionBindings stay byte-identical,
  and every package entry keeps its name, timestamp and compression. The old
  scan remains only for a part without the MS-QDEFF prefix. Reader and writer
  share one locator, `_datamashup_package_parts`.
- **A package with no `Section1.m`** is now reported as a failure. Before, the
  writer reported success without writing anything.
- **Verified in Power BI Desktop 2.152:** the Fuzzy Matching demo edited
  through `pbix_set_m_code` (`People` gains `Table.FirstN(…, 5)`) opens, and
  its Power Query editor shows the added step with a 5-row preview.
- **Pending changes, not data:** the model keeps its last-applied query until
  the edit is applied, as with any Power Query edit. The DataMashup records
  each query's last-applied M (`LastAnalysisServicesFormulaText`), and the
  writer deliberately does not rewrite it.

- **Pinned by 10 tests; 8 fail on 0.9.114:**

  | test file | tests | fail on 0.9.114 |
  |---|---|---|
  | `tests/test_batch_measure_home_tables.py` | 3 | 3 |
  | `tests/test_datamashup_package_parts.py` | 2 | 1 |
  | `tests/test_issue91_datamashup_writer.py` | 5 | 4 |

  The two that pass on 0.9.114 are controls: a part with no metadata content,
  and one without the MS-QDEFF prefix.

## [0.9.114] - 2026-10-07

### Fixed — CALCULATE applies its modifiers before its filter arguments (issue #87)

- **What was wrong:** CALCULATE applied its arguments in the order they were
  written, so a modifier written *after* a filter argument removed that
  filter. `CALCULATE([S], Cust[Region] = "West", ALL(Cust))` returned the
  all-regions total.
- **What DAX does:** it applies the modifiers first, wherever they are
  written, and only then the filter arguments. The modifiers are `ALL`,
  `ALLEXCEPT`, `ALLSELECTED`, `ALLNOBLANKROW`, `REMOVEFILTERS`,
  `USERELATIONSHIP` and `CROSSFILTER`. The engine now does the same, keeping
  the written order within each group.
- **`KEEPFILTERS`** intersects with the filter context left *after* the
  modifiers. `CALCULATE([S], ALL(Cust), KEEPFILTERS(Cust[Region] = "West"))`
  intersected West with an `East` filter that `ALL(Cust)` had already removed,
  and returned blank.
- **Time intelligence** (the `CALCULATE` branch and `TOTALYTD/QTD/MTD`) now
  derives its context instead of rebuilding it. The rebuilt context dropped
  the `ALL(table)` snapshot, so a filter `ALL` had removed came back through
  the relationships.
- **A table whose name starts with `All`** (for example `Allocation[Type] =
  "A"`) is no longer taken for an `ALL…` modifier.
- **Measured on Power BI Desktop** (2.152, ADOMD): 38 shapes under two outer
  contexts now match, against 17 before.
- **No regressions found:** the blank-member battery (147/147) and the
  #78–#80 harness (5,954 + 5,890 cells) still match Desktop. Every measure of
  Agents Performance, GeoSales, Ecommerce, IT Support and the AI sample gives
  the same value as on 0.9.113. The only exceptions are measures built on
  `RAND()`.

### Fixed — the row-context simulation answers the same in every run (issue #88)

- **What was wrong:** when a measure is BLANK and reads `ISFILTERED` /
  `SELECTEDVALUE`, `evaluate_measures_smart` (with the library default
  `simulate_row_context=True`) tries one value of the column. It took the
  first value of a Python `set`, whose order for strings follows the
  per-process hash seed, so the answer changed between runs. IT Support's
  "Filters Applied Values" measure came back as "Type: Incident" in one run
  and "Type: Request" in the next.
- **Now:** it tries the first value in data order. `pbix_evaluate_dax` was not
  affected, since it turns the simulation off unless asked.

- **Pinned by 40 tests; 23 fail on 0.9.113:**

  | test file | tests | fail on 0.9.113 |
  |---|---|---|
  | `tests/test_issue87_calculate_modifier_order.py` | 39 | 22 |
  | `tests/test_issue88_simulation_determinism.py` | 1 | 1 |

  `test_issue88` runs the same evaluation under six hash seeds.

## [0.9.113] - 2026-10-07

### Fixed — the builder stores an unmatched fact key the way Desktop does (issue #81)

- **What was wrong:** a fact key with no row in the dimension was joined to
  the dimension's **first** row, so its values were silently added to a real
  member.
  - The relationship index (R$) holds one slot per distinct key, pointing at
    the dimension row.
  - An unmatched key fell back to row 0, which was written as row 1.
  - Desktop writes 0 there, the blank member. That was read back from the same
    model after Desktop refreshed it.
- **`RelationshipIndexStorage.Flags` is now 1 when any rows reach the blank
  member,** through an unmatched key or a blank key. Without the flag the rows
  do land on the blank member, but `VALUES` does not show it.
  - A census of the Desktop-authored corpus agrees: the flag sits on the
    relationships with unmatched or blank keys. In Adventure Works it is set
    only on `Sales[ShipDateKey]`, the one relationship with blank keys.
- **Verified in Power BI Desktop** (2.152, ADOMD): 147 queries give the same
  answers on the built file and after Desktop's own full refresh. They cover
  integer, string, blank-key, inactive, bidirectional, snowflake, 1:1 and
  many-to-many relationships.

### Fixed — the DAX engine models the blank (unknown) member (issue #82)

- When a relationship joins a row whose key matches no row of the one side,
  Power BI gives that table a **blank row** and puts those rows on it. The
  engine had it only for `ALL` over the key column and for multi-hop filters.
  Measured on Desktop, it now models where the row exists and who sees it:
  - **Which tables get it:** active and inactive relationships, and both
    sides of a 1:1. Never for many-to-many. It is carried one level up a
    snowflake.
  - **Shown by:** `VALUES` (column and table), `ALL`, `ALLSELECTED`, `FILTERS`,
    column predicates, `SUMMARIZE` over the fact, `SELECTEDVALUE` /
    `HASONEVALUE`, and grouping.
  - **Hidden from:** `DISTINCT`, `ALLNOBLANKROW`, a bare table reference,
    `COUNTROWS(T)`, `DISTINCTCOUNT`, `COUNTA` and `MIN` / `MAX`.
  - **Filters that keep BLANK** select the rows on it, directly or across hops.
  - **Over a bidirectional relationship,** fact rows on unmatched keys narrow
    the dimension to the blank row.
  - **ISINSCOPE is TRUE on the blank row after a context transition.** This
    closes what #80 left open.
- **Three CALCULATE filters applied no filter at all:** `ISBLANK(T[C])`,
  `NOT ISBLANK(T[C])` and `T[C] = BLANK()`. They now filter, with DAX's
  coercion:
  - `BLANK() = 0` and `BLANK() = ""` are TRUE, so `T[Key] = 0` keeps the blank
    row as well;
  - `ISBLANK` is strict.
- **`VALUES(<table>)` returned nothing.** It now returns the visible rows,
  duplicates kept, plus the blank row.
- **Corrected along the way,** each one measured:
  - `ALL(column)` dropped stored BLANK values. Desktop keeps them, and so does
    `ALLNOBLANKROW`.
  - `CONCATENATEX` skipped a BLANK instead of joining it as `""`. Desktop gives
    `"X,Y,Z,"`.
  - A comparison against BLANK now uses the other side's zero, so
    `BLANK() < 1` is TRUE.
- **Faster, not slower:** an evaluation context no longer rebuilds the
  relationship indexes. They are now built once per relationship list. A
  `FILTER` over 40,000 rows creates about 50,000 contexts, and rebuilding the
  indexes for each one was the largest single cost of such a measure. On the
  AI sample, `Forecast` takes 3.8 s against 5.1 s on 0.9.112, and
  `Close %` 2.9 s against 4.5 s. On Agents Performance all 102 measures give
  the same values as on 0.9.112, and none is slower.
- **Result:**
  - **147 of 147** Desktop queries match; 0.9.112 matched 49.
  - The #78–#80 harness, rebuilt with the fixed builder, matches **all 5,954
    cells** through `pbix_evaluate_dax` and **all 5,890** through the grouped
    tool. 0.9.112's engine misses 10 and 5 of them. On the file 0.9.112's
    builder wrote, the same harness had two cells off because of #81; on the
    rebuilt file, none are.

### Fixed — results under `USERELATIONSHIP` / `CROSSFILTER` no longer leak into other evaluations (issue #83)

- **The leak:** the relationship-propagation memo is model-wide and outlives
  a call, and its key left out the relationship set. So a `USERELATIONSHIP`
  context and a plain one served each other's results. For example,
  `CALCULATE([QTotal], QD[Name] = "Q1")` answered 1 instead of 7 after a
  `USERELATIONSHIP` query. The key now includes the relationship set.
- **The context:** the one built for `USERELATIONSHIP` / `CROSSFILTER` kept
  only the filters. It now keeps the grouping, the selection, the culture, the
  measure home tables and the `ALL` snapshots. In a grouped query under
  `USERELATIONSHIP`, Desktop gives `ISINSCOPE` TRUE and `ALLSELECTED` = 3, and
  so does the engine now.

### Fixed — `TREATAS` as a `CALCULATE` filter (issue #85)

- **It was never applied.** `_fn_treatas` returns a tuple marker, while
  CALCULATE looked for a dict that nothing produces. So
  `CALCULATE(e, TREATAS(...))` returned the unfiltered value.
- **It now behaves as in Desktop:**
  - it replaces the column's filter, and `KEEPFILTERS` intersects;
  - two filters on one column intersect;
  - an empty table filters to nothing.

  All 12 shapes measured in Desktop match.

### Fixed — `pbix_evaluate_dax_per_dimension` applies marked date tables and lists the BLANK group (issue #86)

- **Marked date tables:** the per-value fallback did not pass the model's
  marked date tables, which is the gap #79 closed in the grouped tool. By
  month, `DM_le` was 496 / 120 / (null) where Desktop gives 616 / 616 / 616.
- **The BLANK group:** the tool now lists it as `(Blank)`, as the grouped tool
  does.

### Fixed — container formatting numbers are spelled as Desktop spells them (issue #84)

- **Padding and spacing** were written as Int64 literals, and `int()` silently
  dropped a fraction: padding 7.5 was stored as `'7L'`. They are now `D`
  literals.
- **Border and drop-shadow** numbers were Python's float repr (`'1.0D'`). They
  now drop the fractional part when it is zero, as Desktop does (`'1D'`).
- **Measured on Desktop-authored files.** In the 36 such files in the local
  corpus, these properties are integral `D` literals: padding 892 of 892,
  spacing 226 of 226, border 76 of 76, drop-shadow 164 of 169. All five
  exceptions are in one file. OpenBI's 25-file census (its doc 45) found the
  same for padding.

### Changed — visible to callers

- **`pbix_evaluate_dax_grouped` and `pbix_evaluate_dax_per_dimension` can now
  return a BLANK group.** It covers stored blanks and the blank row. Its key
  is `null` in the grouped tool's JSON and `(Blank)` in the per-dimension
  text, and it is sorted last.
- **Relationships carry cardinality:** `ModelReader.relationships` and the
  DAX context's relationship dicts now include `FromCardinality` /
  `ToCardinality`.

- **Pinned by 162 tests; 111 fail on 0.9.112:**

  | test file | tests | fail on 0.9.112 |
  |---|---|---|
  | `tests/test_issue81_orphan_keys_blank_member.py` | 5 | 3 |
  | `tests/test_issue82_blank_member.py` | 133 | 86 |
  | `tests/test_issue83_relationship_cache.py` | 6 | 6 |
  | `tests/test_issue84_container_literals.py` | 3 | 3 |
  | `tests/test_issue85_treatas_filter.py` | 12 | 11 |
  | `tests/test_issue86_per_dimension_tool.py` | 3 | 2 |

  The tests that pass on 0.9.112 are controls, or cells it already had right.

## [0.9.112] - 2026-10-07

### Fixed — a date filter clears the date table's other filters only where Power BI does (issue #78; PRs #68, #72 by @allanon2)

- **The rule, measured in Power BI Desktop:** Desktop adds `ALL(<date table>)`
  to a filter on a date column only when the column is a **marked date
  table's date column**, or a **DateTime column used in a relationship —
  active or inactive**. On any other table, the dates intersect with the
  table's Year / Month / Day filters. That includes a date table related only
  by an integer key, and it is the classic "YTD equals the month until you
  mark the date table".
- The engine had that rule in pieces, each half wrong:
  - `CALCULATE` with a time-intelligence filter cleared always;
  - `TOTALYTD/QTD/MTD`, `STARTOF*` / `ENDOF*` and the balances never cleared;
  - the direct-filter rule from #73 covered marked tables only.

  All of them now use one predicate.
- **PRs #68 and #72 are merged** with the change requested in review applied
  on top:
  - `TOTALxTD` replaces the date table's filters (#68);
  - `START/ENDOF*` return dates that exist in the column (#72);
  - `CLOSINGBALANCE*` evaluates at `ENDOF*` (#72);
  - `OPENINGBALANCE*` evaluates at the last existing date before the period
    (#72).

  Their tests now mark their date table, as Corporate Spend's is.
- Verified cell by cell against Power BI Desktop (2.152, ADOMD) on a model
  with marked daily and month-grain tables, an unmarked copy, a sparse
  unmarked table, and date tables related by an active DateTime column, an
  inactive DateTime column, and an integer key: **5,947 of 5,949 cells match
  through `pbix_evaluate_dax` (2,224 fixed, 0 broken)**. The two remaining
  cells are #81, a builder bug, not the engine.

### Fixed — `pbix_evaluate_dax_grouped` applies the marked-date-table rule and ISINSCOPE (issue #79)

- The grouped tool evaluates through `evaluate_measures_batch`. That path
  received neither the model's date-table marking (#73) nor the grouping tag
  ISINSCOPE reads (#69), so both answered as before there. Now it receives
  both: **5,884 of 5,885 cells match Desktop through the grouped tool (2,904
  fixed, 0 broken)**.

### Fixed — KEEPFILTERS keeps the column in scope in every form (issue #80)

- `KEEPFILTERS` intersects, and Power BI keeps the column in scope whether the
  outer filter is the query's grouping or an iterated row after a context
  transition, and for `KEEPFILTERS(FILTER(...))` as for a plain predicate.
  The intersection is now tagged for both cases. Measured in Desktop:
  `SUMX(VALUES(S[Region]), CALCULATE(IF(ISINSCOPE(S[Region]), 1, 0),
  KEEPFILTERS(S[Region] = "West")))` is 1 per region, 4 at the total; it was 0.

- Pinned by `tests/test_issue78_date_table_rule.py`,
  `tests/test_issue79_grouped_tool.py` and
  `tests/test_issue80_keepfilters_scope.py` (40 tests; 19 fail on 0.9.111).
  The 21 that pass on 0.9.111 are the cells it already had right — mostly the
  intersecting ones — which #68 / #72 alone would have broken; they pin that
  half of the rule.

### Known issue — tracked

- **#81** — the builder joins a fact key that is missing from the dimension
  to the dimension's first row instead of the blank member.

## [0.9.111] - 2026-10-07

### Fixed — `ISINSCOPE` through a context transition (PR #77, by @allanon2)

- In DAX an iterator's row becomes a filter — and its columns come into scope
  for `ISINSCOPE` — once `CALCULATE` or a measure reference performs the
  context transition. This engine applies the row → filter step eagerly, so
  ISINSCOPE could not tell the iterated row from any other filter and answered
  FALSE. `_make_row_context` now tags the row's filter values
  (`RowContextValues`). ISINSCOPE is TRUE for them after the transition and,
  before it, answers from the enclosing context. ISINSCOPE also parses its
  argument as a column reference, so a row context cannot collapse it to the
  row's value.
- Verified against Power BI Desktop (2.152, over ADOMD) with 15
  context-transition shapes at five groupings.
  `SUMX(VALUES(c), IF(CALCULATE(ISINSCOPE(c)), 1, 0))` and the
  measure-reference form are 4 at the total, as in Desktop (were 0), and
  `SUMX(T, …)` is 6 (was 0). A row context without a transition stays out of
  scope, and `REMOVEFILTERS` / `ALL` after the transition take the column out
  of scope, as Desktop does. **130 cells fixed through `pbix_evaluate_dax`
  and 112 through `pbix_evaluate_dax_grouped`, with no cell changed among the
  5,121 compared**, including the whole 0.9.110 review set.
- The merge annotates `_make_row_context`'s filter dict so mypy stays at 135.

### Known gap — tracked

- **#80** — `KEEPFILTERS` on the iterated column after the transition still
  drops the column from scope. Desktop shows 1 per region (4 at the total);
  here it is 0, as before this release.

## [0.9.110] - 2026-10-07

### Fixed — DAX engine: four contributed fixes (PRs #69, #70, #71, #73, by @allanon2)

Each was verified against Power BI Desktop (2.152, queried over ADOMD) on a
review model with marked, unmarked, sparse and relationship-joined date
tables. Across 4,896 cells evaluated through `pbix_evaluate_dax`, the four
together fix **717 cells that differed from Desktop and change no cell that
matched**.

- **`FORMAT` rounds before it picks the sign and the section (PR #70).** A
  value that rounds to zero under the picture prints as zero — no `-`, no
  negative section, and the zero section when there is one:
  `FORMAT(-0.004, "$#,##0")` is `$0`, not `-$0`. The named `Currency` format
  parenthesises negatives in an en-US model: `FORMAT(-0.4, "Currency")` is
  `($0.40)`. All 30 FORMAT cases in the review match Desktop (12 differed).
- **A filter on a marked date table's date column clears the table's other
  filters (PR #73).** `ModelReader.date_tables` reads the marking
  (`DataCategory = 'Time'`; the key column, else the only DateTime column).
  Inside `CALCULATE`, a filter on that column drops the table's other filters,
  as Desktop does. This covers the classic running total
  `FILTER(ALL('Date'[Date]), 'Date'[Date] <= MAX('Date'[Date]))` and
  `'Date'[Date] <= DATE(…)` under a Year × Month axis. `KEEPFILTERS` still
  intersects. 615 cells fixed.
- **`ISINSCOPE` answers from the query's grouping (PR #69).**
  `pbix_evaluate_dax` takes an optional `group_by` — the `filter_context` keys
  that are the visual's grouping. ISINSCOPE is TRUE for those, and FALSE for a
  column that is only filtered, or whose filter a `CALCULATE` replaced or
  removed, matching Desktop in every case measured. A context transition
  inside an iterator is not modelled yet.
- **`ALLSELECTED` restores a slicer on the grouped column (PR #71).**
  `pbix_evaluate_dax` takes an optional `selected_filter_context` — the
  query's own filters before the grouping keys were merged in — so
  `ALLSELECTED(column)` returns the selected values' total instead of every
  value's.

Merge note: #73 and #69/#71 each added a keyword to `evaluate_measures_smart`,
resolved by keeping all three (`group_by`, `selected_filters`,
`date_tables`). Three new mypy errors from the merge were fixed, so mypy stays
at 135.

### Known gaps — tracked

- **#78** — Desktop clears a date table's other filters for a **marked date
  table or a DateTime column used in a relationship**, and nowhere else. The
  engine still clears them unconditionally in `CALCULATE` with a
  time-intelligence filter, and never in `TOTALxTD`, `STARTOF*` / `ENDOF*` or
  the balances. PRs #68 and #72 were sent back to use exactly that rule; with
  it, all 4,896 review cells match Desktop.
- **#79** — `pbix_evaluate_dax_grouped` does not yet receive the marked-table
  information (#73) or the grouping tags (#69).

## [0.9.109] - 2026-10-07

### Fixed — `report_filter_json` wrote schema-invalid bookmarks that never filtered (issue #76, from #52)

- **`pbix_add_bookmark` wrote `report_filter_json` verbatim** into
  `explorationState.filters.byExpr`. Its own documented example,
  `[{"target": {"table", "column"}, "operator": "In", "values": [...]}]`, has
  no `name` and three keys Microsoft's PBIR bookmark schema forbids
  (`FilterContainerState`: `name` required, `additionalProperties: false`),
  so every bookmark authored that way was schema-invalid — and the call
  answered success. Flagged in #52, queued, and never landed until now.
- The shorthand is now converted to the container Desktop writes —
  `{name, type: "Categorical", filter: {Version: 2, From, Where: In},
  expression, howCreated: 1}` (28 of 28 bookmark report filters in the
  corpus) — with values typed to the column (`'West'`, `2024L`, `2.5D`,
  `datetime'…'`, `true`; quotes doubled). `"NotIn"` wraps the condition in
  `Not`. A full container passes through. Anything else — no `name`, a
  disallowed key, an unknown operator, empty values, a column not in the
  model, a value that does not fit the column — is refused, and nothing is
  written.
- **A bookmark filters only through a report filter card.**
  Desktop-verified: the same bookmark filters the report when a report-level
  card on that column exists, and does nothing at all when none does. So a
  shorthand target without a card gets an unselected one ("is (All)") added —
  reported in the message and in `data.added_report_filter_cards` — and the
  bookmark's filter is named after it, so every bookmark on that column shares
  the card. In Desktop, "Only West" and "Not West" apply and switch, with and
  without a pre-existing card.
- In a PBIR report the added card uses PBIR's own `FilterContainer` shape
  (`field`, `howCreated: "User"`), and report.json plus the bookmark files
  validate against Microsoft's schemas. A report without a model (a thin
  report) cannot check the column: values are encoded by their JSON type and
  a warning says so.
- Pinned by `tests/test_issue76_bookmark_report_filters.py` (22 tests) and a
  new case in `tests/test_pbir_schema_conformance.py`: 22 of the 23 fail on
  0.9.108; the one that passes is a control (unparseable JSON was already
  refused).

## [0.9.108] - 2026-10-07

### Fixed — build warnings never reached an MCP caller (issue #75)

- **The builder reports every non-critical pre-build issue through Python's
  `warnings` module, which an MCP caller never sees.** So `pbix_create`,
  `pbix_set_table_data`, `pbix_update_table_rows`, `pbix_append_table_rows`
  and every other rebuild-path tool answered `success: true, warnings: []`
  while:
  - folding values that differ only by case onto one spelling (#43) — so
    0.9.105's "the fold is no longer silent" held only for Python-API
    callers;
  - dropping a row field that is not a column (and when it was a misspelt
    column name, leaving that column blank);
  - skipping a user hierarchy whose level column the edit had removed;
  - building a relationship between columns of different types, or with
    orphan keys.
- The builder now records each warning with its kind and the tables it
  concerns (`PBIXBuilder.build_warnings`). `pbix_create`, the rebuild path
  behind every data-changing tool, and TMDL import (`pbix_import_tmdl`,
  `pbix_open_pbip`) forward them into the response's `warnings`. Python
  callers still get the same `UserWarning`s.
- A rebuild re-checks the whole model, so it forwards only the warnings about
  tables the call touched: a standing condition elsewhere (an orphan key, an
  empty table) is not repeated on every unrelated edit. TMDL import still
  leaves out its expected empty-table warnings.
- The unknown-field warning is one line per **table** (rows affected, the
  fields, the first row) instead of one per row — a systematic typo across a
  100k-row load used to emit 100k warnings. The orphan-key warning gives a
  count and at most 10 examples instead of printing the whole set.
- Pinned by `tests/test_issue75_build_warnings_reach_mcp.py` (18 tests): 16
  fail on 0.9.107; the 2 that pass are controls (a measures-only table stays
  quiet; Python callers keep their `UserWarning`).

## [0.9.107] - 2026-10-07

### Fixed — `outline` was written where Power BI never reads it (issue #74)

Flagged while closing #67; tracked and documented in #74.

- **On the seven visuals that have an outline card, the stroke never
  applied.** The mapper wrote one selector-less entry: `outline.color` on
  actionButton, cardVisual, listSlicer, advancedSlicerVisual,
  bookmarkNavigator and pageNavigator, `outline.lineColor` on shape. Two
  things were wrong. The **name**: the report theme schema shipped inside
  Power BI Desktop (v5.71) declares the card on exactly these seven and
  names the stroke colour `lineColor` on every one; `outline.color` appears
  in no Desktop-authored file. And, for all seven including shape, the
  **place**: Desktop's visual code reads `outline.show` from the
  selector-less entry but `lineColor` / `weight` / `transparency` per
  *state*, from the entry whose selector is `{"id": "default"}`.
  Desktop-authored files follow that split (actionButton: `show`
  selector-less 231 times, `lineColor` 48 / `weight` 49 times on
  `{"id": "default"}`, never the reverse). The card now writes exactly
  that, with `weight` / `transparency` spelled as Desktop spells them
  (`"6D"`, `"20D"`) and `lineColor` accepted beside `color` (the Power BI
  name wins; the key it beat is reported).
  **Desktop-verified on all seven:** the old single entry renders the
  default grey stroke at the default width — colour *and* weight dropped,
  `show` applied — while the split renders the requested colour and width.
  So shape's `lineColor` (#47) had the right name in the wrong entry and
  never applied either.
- **Every other visual has no outline card at all** — tables, matrices,
  charts, the classic card, multiRowCard, slicer — and got one written
  anyway. It rendered nothing and answered success. Now nothing is written:
  `outline` is reported as ignored (an error when it was the only card, per
  the no-op contract of #51) with one warning naming where that visual keeps
  its outline — `grid.outlineColor` on tableEx/pivotTable,
  `card.outlineColor` on multiRowCard, `general.outlineColor` on slicer, and
  the `border` card for a border around any visual. A custom visual gets its
  own wording: it defines its own cards, so it is neither guessed at nor
  told it has none.
- **`card` and `general` gained `outlineColor` / `outlineWeight`**, so the
  multiRowCard and slicer pointers name keys that work (declared by the
  schema; `general.outlineColor` 13 and `general.outlineWeight` 10 times in
  Desktop-authored slicers). Desktop-verified on both, and on
  `grid.outlineColor` for a table.
- **basicShape** keeps its stroke on the `line` card, which Desktop's
  basicShape code reads without states (verified). Its capabilities declare
  no outline card and no line on/off, so `outline.show` — which used to land
  on a card basicShape never reads — is now reported as ignored; `weight: 0`
  hides the line (Desktop-verified).

- Pinned by `tests/test_outline_card.py` (39 tests): 35 fail on 0.9.106; the
  4 that pass are controls (shape's `lineColor` name, a `show`-only write,
  basicShape's line card, the caller's payload left untouched). The two
  tests in `test_report_editing.py` that pinned the old layout — shape's
  selector-less `lineColor`, a table's `outline.color` — now pin the
  measured one. mypy unchanged at 135 (CI baseline 140).

## [0.9.106] - 2026-10-07

### Fixed — `fontColor` on title / subtitle, and the legend colour that never applied (issue #67)

- **`title` and `subtitle` read their text colour only from `color`.** Sent
  `fontColor` — the name Power BI stores it under — they wrote nothing for
  it and still answered "Formatted visual N: title". Both now accept
  `fontColor` beside the documented `color`; the Power BI name wins when
  both are sent, and the one it beat is reported rather than dropped.
- **The legend wrote a colour property no built-in legend reads.** Found
  while fixing the above: the `legend` card wrote `fontColor`, but Power BI's
  legend text colour is `labelColor`. The report theme schema that ships
  inside Power BI Desktop (v5.71) declares a legend for 22 visual types and
  every one of them names it `labelColor` — none declares a legend
  `fontColor`. So legend text colour never applied through **any** key, the
  documented `color` included; the value round-tripped through save/reopen
  and Desktop ignored it. The legend now writes `labelColor` and accepts
  `labelColor`, `fontColor` and `color` (in that order of precedence).
  **Desktop-verified:** two identical pie charts, legend red — the one
  carrying `legend.fontColor` renders Desktop's default grey (while its
  font size *is* applied), the one carrying `legend.labelColor` renders red.
- Title/subtitle needed only the input alias: `commonCards` in the same
  schema declares `title.fontColor` and `subTitle.fontColor`, and 341 title
  cards in the local corpus carry `fontColor`, so their output was right.

### Added — keys a card does not read are reported, never dropped

- The format mapper now records which keys it actually consumes. Every key
  it did not is named **in the message** (`… (vcObjects: title; ignored:
  title.fontColour)`), **in `warnings`**, and **in
  `data.ignored = {"cards": [...], "properties": {card: [keys]}}`**. #64,
  #66 and #67 were all the same shape — a dropped key and an honoured one
  produced the same answer — and this closes the class rather than one
  spelling.
- Covers what was silent before: an unknown property inside a known card;
  an unknown card sent beside a known one (previously reported only when
  *nothing* applied); and the `text` card on any visual but `actionButton`,
  which was tested, marked handled, and dropped.
- `pbix_format_page` follows the same contract: unread keys inside a page
  card are reported in `data.ignored_properties` and `warnings`.
  `data.ignored` keeps its existing shape (unknown top-level cards) so
  capability probes built on it are unaffected.
- Verified free of false positives: none of the 38 `pbix_format_visual`
  calls the existing suite makes reports an ignored key, and a docstring
  ratchet confirms all 143 documented keys across 24 cards are read.

- Pinned by `tests/test_issue67_font_color_and_ignored_keys.py` (18 tests).
  15 fail on 0.9.105; the 3 that pass are controls that must keep passing
  (documented `color` on title, the caller's dict left untouched, the page
  tool's `ignored` shape). mypy unchanged at 135 (CI baseline 140).

## [0.9.105] - 2026-09-01

### Changed — the #43 case-fold is no longer silent

VertiPaq's string store is case-insensitive, so the encoder folds values that
differ only by case onto one dictionary entry, keeping the first spelling seen
(issue #43 — without the fold Desktop refuses the whole model: *"A duplicate
value has been detected in the Unique Value store"*). That fold is correct and
matches Power BI's own import behaviour, and it is unchanged here.

What changed is that it **rewrites caller-supplied values in silence**: three
rows of `'abc'` / `'ABC'` / `'Abc'` all read back as `'abc'`, and
`DISTINCTCOUNT` reports 1. The pre-build checks already treat the same collision
one level up (case-colliding column *names*, #53) as CRITICAL, and this
project's warning channel exists so a lossy transformation is never reported as
an unqualified success. The build now emits, per affected column:

```
PBIX pre-build: WARNING: Table 'Players' column 'Player Name' has 1 value(s)
differing only by case ('VAN DER SAR' / 'van der SAR'). VertiPaq's string store
is case-insensitive, so these are stored as ONE value using the first spelling
seen (as Power BI does on import) — distinct counts and grouping will reflect
the folded value.
```

Behaviour is otherwise unchanged. Regression:
`tests/test_case_fold_warning.py` (4 cases, incl. a no-collision negative
control and a non-string-column control).

### Audit — findings docs 1-43 and GitHub #1-#66 re-verified, both queues empty

`docs/openbi-findings-ledger.md` was stale (last audited 2026-07-30 against
0.9.59) while docs 20-43 were worked through GitHub issues. Re-audited against
0.9.104 by **execution**, not paperwork: the reported repro for every
silently-wrong-output and typing item in docs 25-43 was replayed against the
live engine — 18 of 19 checks reproduced the fixed behaviour (KEEPFILTERS
intersecting, `CALCULATE(T[Col]=v)` sugar, bare `REMOVEFILTERS()`/`ALLEXCEPT`,
Int64 typing of counts, case-colliding column names refused, Int64 past
signed-32, a data column named `RowNumber` surviving, trailing-backslash
literals, lowercase `title.alignment`, visual background image refused by name).
The 19th was the sweep's own wrong expectation about the #43 fold, which is the
change above. Ledger updated with the evidence.

## [0.9.104] - 2026-08-23

### Changed — `pbix_format_visual` says which bucket each card was written to (issue #66)

- The reported symptom was that `title` / `background` / `border` / `padding`
  "never reach the saved file". **They do** — they reach
  `singleVisual.vcObjects`, which is where Power BI keeps container-level
  formatting. Verifying against `singleVisual.objects` alone shows an empty
  dict and reads as a silent drop.
- The split is Desktop's, not this engine's. Censused across 167 corpus
  reports:

  | card | `objects` | `vcObjects` |
  |---|---|---|
  | title | 0 | 876 |
  | background | 1 | 935 |
  | border | 0 | 265 |
  | padding | 83 | 276 |
  | labels | 167 | 0 |
  | categoryLabels | 176 | 0 |
  | visualHeader | 0 | 350 |

- **The real defect was that the success claim could not be checked.** The
  message named the cards without saying where they went, so a caller had no
  way to confirm it short of knowing the bucket split in advance. The
  response now reports both:

      Formatted visual 0 on page 0: title, background, border, padding
      (vcObjects: title, background, border, padding)

  and `data` carries `{"applied": [...], "objects": [...], "vcObjects": [...]}`
  so the claim is verifiable programmatically.
- No change to placement, which was already correct, and none to which cards
  are written — this only makes the existing behaviour legible.
- Pinned by `tests/test_issue66_format_visual_reports_bucket.py` (7 tests).
  Four assert every reported card really is written across both buckets and
  **pass on 0.9.103**, which is the evidence the reported drop does not
  exist; three assert the response names the bucket and fail on 0.9.103.

## [0.9.103] - 2026-08-22

### Fixed — `title.alignment` is written lowercase (issue #65)

- **`pbix_format_visual` wrote `'Center'` / `'Left'` / `'Right'` on the
  `title` card**, a spelling Power BI never produces. Desktop does not
  recognise it and renders the title flush left. The value round-trips
  perfectly through save → reopen, so a readback test can never catch it —
  which is why it survived.
- **Power BI's casing is not uniform, and that is the trap.** Censused
  independently across the local Desktop-authored corpus:

  | card.property | casing | n |
  |---|---|---|
  | `vcObjects.title.alignment` | **lowercase** | 36 (0 counter-examples) |
  | `columnFormatting.alignment` | Capitalized | 57 |
  | `columnHeaders.alignment` | Capitalized | 13 |
  | `columnHeaders.titleAlignment` | Capitalized | 8 |
  | `rowHeaders.alignment` | Capitalized | 9 |

  A blanket rule either way breaks one side, so only the title card changed.
  The three table cards pass the caller's string through untouched, which
  was already correct.
- The caller's own casing is normalized, so `"Center"`, `"CENTER"` and
  `"  Center  "` all reach Desktop as `'center'` — a converter should not
  have to know the convention to get a centred title.
- `_ALIGNMENTS` is used by the title card only; its values are now
  lowercase, with the measurement recorded at the definition so a future
  tidy-up does not "unify" the two conventions.
- Pinned by `tests/test_issue65_title_alignment_casing.py` (16 tests),
  including one assertion holding **both** conventions side by side so a
  unifying change fails with the reason attached. Ten fail on 0.9.102; the
  six that pass are the table-card controls that must stay Capitalized.

## [0.9.102] - 2026-08-22

### Fixed — a visual background image is refused by name instead of silently dropped (issue #64)

- **`pbix_format_visual` accepted `background: {image_base64: …}` on a
  VISUAL, dropped the image, and answered "Formatted visual N on page M:
  background"** — naming the card as though it had been written. Nothing was
  registered under `RegisteredResources` and the saved card held only
  `show`, so a caller could not tell honoured from discarded. The nested
  `background: {image: {…}}` spelling behaved the same.
- **Power BI has no background image on a visual**, so the fix is to refuse,
  not implement. Verified independently of the report across 168 local
  reports: page backgrounds carry an `image` 30 times, visual backgrounds
  carry it **zero** times out of 1,507 property occurrences, and the
  vocabulary is exactly `show` / `color` / `transparency`.
- The refusal names the offending property and where to go instead:

      background.image_base64 is not a visual-level property — Power BI has
      no background image on a VISUAL (its background card is show / color /
      transparency only). Use pbix_format_page for a page background or
      wallpaper image, or place a separate `image` visual behind this one
      (z-order is add-order, later wins). Nothing was written.

  Nothing is written and no resource is registered on the refused path.
- **`show` is no longer fabricated when nothing else was recognised.** That
  auto-`show` is what made a dropped image report as success: it guaranteed
  the card was non-empty, so the card name appeared in the "formatted"
  message regardless. It is still implied by naming a colour or
  transparency, so `{"background": {"color": "#…"}}` is unchanged; an empty
  or unrecognised background now falls through to the honest "no recognised
  properties" refusal added in #51.
- Page background and wallpaper images (`pbix_format_page`) are unaffected —
  that is where the capability legitimately lives, and a test pins it.
- Pinned by `tests/test_issue64_visual_background_image.py` (11 tests: four
  image spellings refused by name, no orphan resource, nothing written to
  the saved card, the colour/`show` controls, the empty-background refusal,
  and page background still honoured). Seven fail on 0.9.101.

## [0.9.101] - 2026-08-21

### Fixed — Int64 values past signed-32 produced a file Desktop refuses to open (issue #63)

Any column whose values leave `[-2^31, 2^31-1]` shipped a dictionary that
Analysis Services could not load. **Two** independent contradictions, both
keyed on the dictionary's entry WIDTH:

- **`DictionaryStorage.IsOperatingOn32`** declares the width (1 = 4-byte
  entries, 0 = 8-byte) and was hardcoded to 1 for every integer-backed type,
  while the encoder widens to 8 bytes as soon as a value leaves signed-32
  range. The blob held 8-byte entries under a 4-byte promise. Now derived at
  both write sites from the encoder's own conversion and predicate, so the
  flag cannot drift from the bytes again.
- **`hash_information`** — the 6 x int32 block after `dictionary_type` —
  follows the entry width too, and was keyed on the logical type alone.
  Measured across a Desktop-authored model (49 numeric dictionaries):
  4-byte integers carry `(-1, 8, 64, 6, -1, -1)`, **8-byte integers carry
  `(-1, 16, 64, 3, -1, -1)` — the same block as an 8-byte float.** Wide
  integer dictionaries were getting the 4-byte parameters.

Fixing only the flag was **not** enough: Desktop still hung loading such a
file. Both had to be right.

**Decimal is affected too** — the issue listed it as an untested adjacent
risk, and it is real. Decimal is stored scaled by 10000, so `214749.0`
becomes 2,147,490,000 and needs 8-byte entries while the unscaled value
looks small. Boolean is unaffected.

**Desktop-verified** (Power BI Desktop, one file per shape): `[1, 2]`,
`2147483647`, `7000000000`, `800000000000000`, Decimal `1.5` and Decimal
`214749.0` all open. Before the fix the last three hung on "Loading data
model" indefinitely while the controls loaded in ~11s.

### Added — `pbix_doctor` checks dictionary width against its declared flag

The issue's sharpest point was that every engine-side check passed a file
Desktop refuses. The new **"Dictionary width vs IsOperatingOn32"** check
compares the width in the dictionary bytes against both the metadata flag
and the `hash_information` block, so either contradiction is caught without
Desktop. Doctor is now a 21-point diagnostic.

- Pinned by `tests/test_issue63_int64_dictionary_width.py` (18 tests: the
  flag predicate incl. nulls and non-integer types, built-file width and
  hash for six shapes, and the doctor check on a planted contradiction).
  Verified against released 0.9.100, where both fields are wrong for every
  value past signed-32.

## [0.9.100] - 2026-08-19

### Fixed — `columnWidth` spells integral widths the way Desktop does (issue #62)

- The `columnWidth` card added in 0.9.99 wrote every **integral** width with
  a fractional part — `200` became `"200.0D"`, `288` became `"288.0D"` —
  where Desktop-authored files carry `"288D"`, `"155D"`, `"168D"` and never
  a `.0`. Only half-integral values such as `258.5` happened to match.
  Byte-compatibility with Desktop saves is the whole point of the card, so
  the spelling moved every written literal off the measured form and broke
  diff-based round-trip comparisons.
- New `_pbi_double_lit` emits `int` form when the value is integral and the
  decimal form otherwise. The generic `_pbi_lit` is deliberately unchanged:
  altering it would move every font size and offset in the codebase at once,
  and none of those has been measured against Desktop the way these have.
- The page background/wallpaper `transparency` property had the same rule
  and an inline copy of the logic; it now shares the one helper.
- Verified against a Desktop-authored file in the local corpus
  (`"288D"`, `"155D"`, `"168D"`).
- Pinned by `tests/test_issue57_59_page_column_format.py` — five parametrised
  spellings plus the shared-helper check; all five fail on 0.9.99.

## [0.9.99] - 2026-08-18

### Added — `pbix_format_page`: page background and wallpaper (issue #57)

- **A report page could not be given a background colour or a wallpaper image
  at all**, so every converted sheet landed on a plain white canvas even when
  the source painted the whole sheet — one of the largest single contributors
  to a converted report reading as a layout skeleton rather than the original.
- New `pbix_format_page(alias, page_index, format_json)` — the page-level
  counterpart of `pbix_format_visual`. Two cards:
  - `background` — the canvas: `color`, `transparency`,
    `image_path`/`image_base64`, `scaling`, `name`
  - `wallpaper` — the area around the canvas, Power BI's `outspace` card,
    same keys (`outspace` is accepted as a synonym)
  A bare string is taken as the colour (`{"background": "#F2F6F6"}`), images
  are registered as report resources the way `pbix_add_image` does, and a
  second call MERGES into a card rather than replacing its sibling
  properties.
- Card names are Microsoft's own: the published PBIR page schema
  (`page/2.1.0`) defines `background` and `outspace` with the same
  `{color, image, transparency}` shape. `color` and `transparency` match a
  Desktop-authored page byte for byte, including the integral-transparency
  literal `0D` (not `0.0D`). The image sub-object reuses the
  ResourcePackageItem mechanism measured for image visuals; its inner
  nesting follows the card's structured-value convention and is **derived
  rather than measured**, as no Desktop-authored page wallpaper was
  available locally to diff against.

### Added — per-column table formatting (issue #59)

- **New `columnWidth` card**, keyed by column:
  `{"columnWidth": {"Sales.Amount": 258.5}}` writes one entry per column
  carrying `value` and a `{"metadata": "Table.Column"}` selector — the shape
  Desktop-authored files use. There was no such card at all, so a converter
  that knows every source column's width could express none of it.
- **`columnFormatting` now takes a per-column selector.** It wrote a single
  selector-less entry, so a property could not be aimed at one column. The
  per-column form `{"Sales.Amount": {"alignment": "Center"}}` emits one
  selector-bearing entry per column; the flat form still writes the legacy
  default entry, so existing callers are unaffected. Per-column `fontColor`,
  `backColor`, `fontSize`, `fontFamily`, `bold` and `italic` were added
  alongside the existing alignment/units/precision/style properties.

- Pinned by `tests/test_issue57_59_page_column_format.py`; 11 of the 12 fail
  on 0.9.98.
- Tool count 132 → 133.

## [0.9.98] - 2026-08-18

### Fixed — FORMAT() interprets quoted literals in a format picture (issue #58)

- **A VBA/DAX format picture escapes literal text by QUOTING it**, and Desktop
  emits the contents without the quote characters. The engine printed them
  verbatim, so `FORMAT(1234.5, "\"$ \"#,##0")` came back as `"$ "1,235`
  instead of `$ 1,235`. Currency pictures are exactly the shape that uses
  this, so every number format carried over from a source document rendered
  wrongly under `pbix_evaluate_dax` even though the numbers themselves were
  right.
- Quoted spans are now stripped of their quote characters on output, and —
  just as importantly — they are excluded from the placeholder search and
  from section splitting, so a digit inside a literal (`"0 of "0`) is text
  and a `;` inside one is not a section break.

### Added — FORMAT() resolves separators from the model's culture (issue #60)

- **`FORMAT()` ignored `Model.Culture` and its own third `locale` argument**,
  so there was no way to obtain non-en-US separators at all. A model whose
  culture is `pt-BR` rendered `FORMAT(1477000, "#,##0.00")` as
  `1,477,000.00` where DAX renders `1.477.000,00`. This matters because a
  `FORMAT()` result is a STRING the canvas prints verbatim — its separators
  come from the model, not from the reader's host.
- Resolution order is now: the `locale` argument if given, else
  `Model.Culture`, else en-US. An unknown culture falls back to en-US
  rather than guessing, since a wrong separator is indistinguishable from a
  right one in the output.
- **Known limit, stated rather than guessed at:** only the decimal and
  group SEPARATORS are culture-resolved. Month and day NAMES still render
  en-US.

### Added — `pbix_set_model_culture` (issue #60)

- The model's own culture is a first-class property and no longer needs raw
  SQL through `pbix_datamodel_modify_metadata`. Sets `Model.Culture` and
  keeps `SourceQueryCulture` in step, the way Desktop does, and drops the
  DAX context cache so `FORMAT()` picks it up immediately. Distinct from
  `pbix_add_culture`, which adds a TRANSLATION culture.

### Fixed — `pbix_get_cultures` reports the model's own culture (issue #60)

- It listed translation cultures only, so its answer could contradict the
  model it described: after setting the culture to `pt-BR` it still said
  `en-US — 0 translation(s)` and nothing else. It now leads with
  `Model culture: <name>` and lists the translation cultures separately,
  returning both in `data` (`model_culture`, `translation_cultures`).

- Pinned by `tests/test_issue58_60_format.py` (24 tests: the reporter's five
  currency pictures, the literal/section-splitting edge cases, the
  resolution order, a separator table, and the setter/readback through
  set → save → close → reopen). Verified failing on 0.9.97.
- Tool count 131 → 132; the doc ratchet caught the drift.

## [0.9.97] - 2026-08-18

### Fixed — a data column named `RowNumber` was silently deleted (issue #55)

- **The first calculated column added to a table DELETED any data column
  named `RowNumber`** — no error, no warning, and the file still opened.
  That is the one failure class a caller cannot catch by readback, because
  everything else about the file is valid; it surfaced only when a converted
  document happened to carry the name and a later step compared column
  counts.
- Cause: the system row-number column is `Type = 3` and carries a GUID
  suffix (`RowNumber-2662979B-…`), but thirteen places excluded it by NAME —
  `NOT LIKE 'RowNumber%'`, `startswith("RowNumber")`, `"RowNumber" in path`.
  A user column called `RowNumber` matched every one of them. All now test
  the column's `Type` (or, on the display side, `IsHidden`, which already
  excluded the system column), so the name is no longer load-bearing
  anywhere: metadata planning, the property snapshot, the VertiPaq encoder's
  system-column branch, the decoder's column-file lookup and the
  schema/doc/perf listings.
- The user's column now survives create → calculated column → save → reopen
  with its values intact.

### Fixed — a string literal ending in a backslash broke the enclosing call (issue #56)

- **A calculated table whose DAX contained a literal ending in a backslash**
  — an ordinary Windows path like `"D:\Projects\"` — **did not
  materialize.** DAX escapes a quote by DOUBLING it and treats a backslash
  as an ordinary character, but the function-argument scan applied the
  C/Python rule and read `\"` as an escaped quote. The scan then never
  found the closing paren, the call fell through to the tail case, and the
  whole expression evaluated to `None` with nothing naming the cause.
- The scan now implements DAX's doubled-quote rule (the same rule the rest
  of the engine already used). Interior backslashes, trailing single and
  trailing doubled backslashes all work, and `""` escaping is unaffected.
- This was one site — the only place in the codebase that assumed
  backslash-escaping.

### Fixed — an all-blank calculated column is accepted (issue #54)

- **A calculated column that is legitimately blank on every row was refused**
  with "the expression likely references a column or name that doesn't
  resolve in this engine" — a guess that was measurably false. An
  unresolved reference is already detected precisely, and named, by the
  check immediately above it, so anything reaching that point has resolved
  every column it names. `IF(LEFT(T[Tag],1) <> "$", T[Tag])` over rows whose
  tags all begin with `$` is blank on every row because that is the correct
  answer — the source QlikView list box shows nothing either.
- The stale heuristic is removed; such a column now materializes and
  persists. A genuinely unresolved reference is still refused, by name.

- All three pinned by `tests/test_issue54_55_56_calc.py`; six of the eleven
  fail on 0.9.96 (the rest are controls that must keep passing).

## [0.9.96] - 2026-08-17

### Fixed — names that collide only by case are refused instead of producing an unopenable file (issue #53)

- **`pbix_create` accepted two columns whose names differed only by case**
  (`DEST_WAC` and `Dest_WAC`), built successfully, gave both distinct
  internal ids, and every engine-side readback showed two columns — but
  Analysis Services treats column names as case-insensitive, so Power BI
  Desktop refused the saved file with *"The Column with the name of
  'Dest_WAC' already exists in the 'T' Table"*. Nothing engine-side could
  fault the file, so the first symptom was a customer double-clicking it.
  Pre-build validation now rejects the collision, naming **both** spellings
  so the caller can decide which to keep, and writes no file. Exact
  duplicates are caught by the same check (they previously died deep in the
  encoder with `UNIQUE constraint failed: AttributeHierarchy.ID`).
- The same rule is applied to **table** names, which produce the identically
  unopenable file.
- Because the check lives in the builder's pre-build pass, it covers every
  path that rebuilds — `pbix_create`, `pbix_set_table_data` and
  `pbix_append_table_rows` alike, so an EDIT cannot introduce a collision
  either.
- **`pbix_doctor` gained a "Table / column name collisions" check** for files
  already written this way. The issue's core complaint was that a silent
  accept could not be checked by any engine-side readback; now it can.
- Same failure class as #43 (case-colliding *values* in one column), one
  level up, and it uses the same casefold identity.
- Pinned by `tests/test_issue53_name_collisions.py` — the issue's own
  three-column repro, exact duplicates, an edit-time collision, planted
  metadata caught end-to-end by the doctor, plus negative controls
  (shared prefixes and same-named columns on *different* tables must still
  build). Six of the eight fail on 0.9.95.

### Fixed — doctor check count in the docs

- README and `docs/tool-contracts.md` advertised a "17-point diagnostic"
  while `pbix_doctor` ran 19 checks — the invariants added for #43 and #53
  never reached the prose. Both now say 20, and
  `tests/test_doc_numbers.py::test_doctor_check_count_matches_docs` ties the
  number to the code so it cannot drift again.

## [0.9.95] - 2026-08-17

### Fixed — bookmarks capture visual state, and both tools return the internal name (issue #52)

- **Every bookmark recorded `singleVisual: {}` for each visual it targeted**,
  so applying one restored nothing: a slicer selection live at capture time
  was lost, and no bookmark could clear one either. `pbix_add_bookmark` now
  captures each targeted visual's data state — the selection at
  `objects.general[].properties.filter` plus the visual's own filters —
  controlled by the new `capture_visual_state` (default on; set False for a
  display-only bookmark).
- **New `clear_selections` option** records the CLEARED state: each targeted
  selection is written with its `Where` clause dropped, an explicit "nothing
  selected" that applying the bookmark restores. An *absent* filter means
  "do not override", which is why a Clear-all bookmark could not be authored
  before.
- **The internal bookmark name is now returned.** An action button's
  bookmark action must reference it (`vcObjects.visualLink[0].properties.bookmark`),
  but `pbix_add_bookmark` returned only a message and `pbix_get_bookmarks`
  listed display names — which are not unique — forcing callers to read
  `Report/Layout` raw and match on the label. `pbix_add_bookmark` now returns
  `data.name` (plus displayName, page and index) and `pbix_get_bookmarks`
  returns a `data.bookmarks` array carrying `name` beside `displayName`,
  the active section, the targeted visuals and how many of them actually
  carry state.
- Per-visual filters are written as the PBIR bookmark schema requires — a
  `FiltersState` object keyed `byName`, with only the `FilterContainerState`
  properties that schema allows — not the container's plain array. Verified
  by the existing `tests/test_pbir_schema_conformance.py`, which rejected the
  array outright.
- Pinned by `tests/test_issue52_bookmarks.py` (the issue's own probe: save →
  close → reopen → read the layout back); all nine fail on 0.9.94.
- Visibility bookmarks are unchanged: hidden visuals still get
  `display.mode = "hidden"`, visible ones still omit `mode`.

## [0.9.94] - 2026-08-17

### Fixed — combo chart's secondary value axis is reachable from `pbix_format_visual` (issue #51)

- **The humanized `valueAxis` card handled only the PRIMARY axis.** Power BI
  keeps both of a combo chart's value axes on that one card, the second
  under `sec`-prefixed property names, and the mapper produced no `sec*`
  property and no `alignZeros` — so every such key was dropped, the props
  dict came out empty, and the whole call was refused with the visual left
  unchanged. The card now writes the measured names `secShow`,
  `secFontSize`, `secLabelColor`, `secLabelDisplayUnits`, `secAxisTitle`,
  `secShowAxisTitle`, `secStart`, `secEnd` and `alignZeros`, with friendly
  aliases `secColor` / `secDisplayUnits` / `secTitle` mapping onto those
  same measured names (`secTitle` turns the title on, mirroring the
  primary `title`; an explicit `secShowAxisTitle` still wins).
- **The "nothing applied" error named the wrong thing.** It called the card
  unrecognised *and* listed that same card among the supported keys, so a
  caller went looking for the wrong bug. It now separates a recognised card
  whose properties were all dropped (`valueAxis: no recognised properties
  in ['secShow']`) from a genuinely unknown card (`unrecognised card(s):
  ['notACard']`), names the visual type, and no longer leaks internal
  underscore-prefixed keys.
- Pinned by `tests/test_report_editing.py::TestComboSecondaryValueAxis`
  (the issue's own three repro calls plus the full property set, through
  write → save → reopen → readback) and
  `::TestNothingAppliedMessageNamesProperties`, with a
  `test_format_cards_constant_matches_mapper` ratchet keeping the new
  `_FORMAT_CARDS` list in step with the cards the mapper actually reads.

## [0.9.93] - 2026-08-17

### Fixed — CALCULATE filter-argument semantics (issues #49, #50)

- **Filter arguments now evaluate in the OUTER filter context** (issue
  #49). They were evaluated sequentially against the context as already
  modified by the preceding arguments, so
  `CALCULATE(MAX(col), ALLSELECTED(), VALUES(T[dim]))` had VALUES see the
  post-ALLSELECTED (widened) context and return every dim value — only
  133/231 grouped cells matched Desktop. Every evaluation site in the
  filter loop (table/FILTER args, TREATAS, time-intelligence, DATEADD /
  SAMEPERIODLASTYEAR, predicate value expressions) now reads the outer
  context while its result still lands on the cumulative context, so
  multiple filter arguments continue to AND together.
- **Bare `REMOVEFILTERS()` was a silent no-op** (issue #50) — the
  reference regex cannot match empty parens, so
  `CALCULATE(expr, REMOVEFILTERS())` evaluated identically to `expr`. It
  now clears every filter in the context, slicer selection included.
- **`ALLEXCEPT` as a CALCULATE argument was a silent no-op** (issue #50) —
  it starts with `ALL`, landed in the generic ALL branch, and its
  multi-argument call defeated the single-reference regex there. It now
  clears every filter on the named table — propagated filters included,
  with the same live-filter snapshot `ALL(Table)` uses — except the listed
  columns' own filters.
- Pinned by `tests/test_issue49_50_calculate.py` (grouped-evaluation
  repros from the issues; five of the six tests fail on the pre-fix
  engine).

## [0.9.92] - 2026-08-17

### Fixed — shape and actionButton formatting keys match measured Desktop visuals (issues #47, #48)

- **Shape rotation landed on the `shape` card as `rotation`** — a property
  no measured Desktop shape carries; Desktop ignored it. It now writes the
  measured card: `rotation.shapeAngle` on `shape`, `rotation.angle` on
  `basicShape` (other visual types keep the legacy spelling).
- **Outline stroke colour was written as the generic `outline.color`**,
  which matches no measured shape — the authored colour rendered as
  Desktop's default stroke (the #45 silent class). It now writes
  `lineColor`: on the `outline` card for `shape`, on the `line` card
  (with weight/transparency) for `basicShape`; non-shape visuals keep the
  generic spelling.
- **No formatting key existed for shape geometry** — `shape.tileShape` /
  `shape.geometry` now writes `shape.tileShape` (`shape`) or
  `general.shapeType` (`basicShape`), so line/rectangle/oval/arrow no
  longer needs hand-built config_json (QlikView line/arrow conversion).
- **The `action` key wrote `objects.visualLink`** — a bucket no
  Desktop-authored button reads — producing persisted-but-dead actions.
  It now writes `vcObjects.visualLink` (the measured bucket) and gained
  `show`, `webUrl` and `tooltip`; the `visualLink` key also gained
  `webUrl`, so a `type: "WebUrl"` action can carry its URL.
- **New `text` formatting key for actionButton labels** — writes
  `objects.text` with the measured default-state selector
  (`{"id": "default"}`); accepts a bare string or
  `{text, show, fontSize, fontColor}`.
- Pinned by `tests/test_report_editing.py::TestShapeAndButtonFormatting`
  (the issues' own write → save → reopen → readback protocol on all three
  visual types, wrong spellings asserted ABSENT, plus a tableEx control
  keeping the legacy outline spelling).

## [0.9.91] - 2026-08-16

### Added — streaming/append row load (issue #46). 131 tools (was 130)

- **NDJSON streaming row source**: `pbix_create`'s per-table specs accept
  `"rows_path"` — a file with one JSON row object per line — instead of
  inline `"rows"`. A caller converting a large table writes batches to the
  file and frees them as it goes, then passes the path, instead of holding
  source rows + a row-dict list + the whole serialized JSON string
  simultaneously (measured by the reporter at ~3x the data size; a 2.94M-row
  conversion peaked at 10.1 GB and had to be killed, while the encoder
  itself is linear at ~149 µs/row). Malformed lines error with their line
  number; `rows` and `rows_path` are mutually exclusive.
- **`pbix_append_table_rows`** — ADDS rows to an existing table (both prior
  row APIs were replace-only, making batching impossible). Schema inferred
  from the existing table like `pbix_update_table_rows`; missing keys store
  as NULL; accepts `rows_json` or an NDJSON `rows_path`. Documented cost:
  each call re-encodes the whole table (VertiPaq needs the full column
  view), so a single huge initial load should prefer `rows_path` on create.
- Pinned by `tests/test_streaming_rows.py` (8 tests: create-from-NDJSON,
  mutual exclusion, missing file, malformed-line line numbers, append
  accumulates + persists through save/reopen with exact COUNTROWS/SUM,
  missing keys → NULL, error paths). The doc ratchet also caught — and
  this release fixes before it ever shipped — a helper accidentally
  registered as an MCP tool in place of `pbix_create`.

## [0.9.90] - 2026-08-15

### Fixed — categoryLabels.color keyed off the visual type (issue #45)

- `pbix_format_visual(..., {"categoryLabels": {"color": ...}})` always wrote
  `categoryLabelFontColor` — the MULTI-ROW card's property — so a plain
  `card`, which renders `categoryLabels.color`, accepted the call, persisted
  the file, and rendered unchanged. The property name is now selected by the
  target's visualType: `color` for `card`, `categoryLabelFontColor` for
  everything else (multi-row card behavior unchanged).
- Pinned by `tests/test_report_editing.py::TestCategoryLabelColorByVisualType`
  using the issue's own measurement protocol — write, save, reopen from
  disk, read the exact property path back — for both visual types, asserting
  the right name present AND the wrong name absent.

## [0.9.89] - 2026-08-14

### Fixed — ModelReader.statistics reported the first column's DISTINCT count as RowCount (issue #44)

- `statistics` took `RowCount` from a data column's IDFMETA `row_count`,
  which for dictionary-encoded columns is the DICTIONARY entry count — the
  column's distinct count, not the table's row count. A 500-row table whose
  first column held 10 distinct values reported "10 rows" (11 with nulls:
  10 + the blank); the reporter's 3,495-row table read "191 rows" in the
  Fields pane, 18× smaller than reality.
- Now read from `ColumnStorage.Statistics_RowCount` (the segment record
  count Power BI itself maintains, MAX across the table's columns — the
  same source `pbix_doctor` already trusts, and as cheap as the metadata
  bundle), with the ROWNUMBER column's IDFMETA as fallback for files
  without storage statistics (RowNumber has one entry per physical row, so
  its count IS the table's — unlike the data columns the old code read).
- Pinned by `tests/test_model_reader_tempfile.py::TestStatisticsRowCount`
  (the issue's four shapes: few/all distinct, strings, nulls — all 500;
  Desktop-authored Adventure Works counts exact).

## [0.9.88] - 2026-08-14

### Fixed — case-colliding strings no longer make the file unloadable (issue #43)

- **Two strings differing only by case in one column made the whole .pbix
  UNLOADABLE** — VertiPaq's string store is case-insensitive, the encoder
  wrote `"VAN DER SAR"` and `"van der SAR"` as separate dictionary entries,
  and Desktop rejected the model ("A duplicate value has been detected in
  the Unique Value store"). Three case pairs among 37,784 rows were enough
  to keep the reporter's converted document from opening at all.
- `_val_key` — the encoder's single value-identity function — now folds
  strings with `casefold()`: one dictionary entry per folded key, the FIRST
  spelling seen stored, every case variant's rows mapped to it (Desktop's
  own import behavior). Dictionary build, row→index mapping, H$ hierarchy
  and distinct counts all fold consistently through the one chokepoint.
- **`pbix_doctor` gained a DBCC-style "String dictionary invariants"
  check**: every stored string dictionary is scanned for case-folded
  duplicates, so files written by pbix-mcp < 0.9.88 are flagged with the
  offending pairs BEFORE Desktop's load failure is the first symptom.
- **Desktop-verified**: the issue's exact three-row repro file, which
  previously raised the duplicate-value dialog, now opens in Power BI
  Desktop (title bar carries the file name, AS engine loads the model).
  Pinned by `tests/test_case_insensitive_dictionary.py` (7 tests: fold to
  first spelling, folded distinct counts, raw stored-dictionary scan,
  doctor green on new + Desktop-authored files, doctor RED on a simulated
  pre-fix file naming the colliding pair, DAX sees folded values).

## [0.9.87] - 2026-08-14

### Fixed — relationship join keys starting with a date collapsed into one bucket (issue #42)

- `_as_date()` parses a date PREFIX (`s[:len(fmt)+2]`) — right for coercing
  datetime-ish strings, wrong for deciding date-EQUALITY classes: compound
  keys like `01/01/2017-CLUSTER 1` and `01/01/2017-CLUSTER 2` both
  prefix-parsed to `2017-01-01`, and the value-index aliasing merged every
  distinct key sharing a date prefix into ONE bucket. A filter on one key
  returned every division's rows for that date, and through a relationship
  the fact table came back UNFILTERED — the grand total under every
  selection, `status: ok`, no warning. Ordinary for imported models whose
  keys are `date & '-' & code`.
- New STRICT whole-value date parse (`_as_date_strict`) now used at every
  equality/alias site: `_value_index_map` (cell aliasing + dateish
  sampling), `_indices_for_values` (filter-value aliasing),
  `make_value_matcher` (allowed-dates + cell match + relative_date), and
  `_compare`'s date branch. Coercion sites (DATEADD scans, relative-date
  anchors) stay lenient. The alias's legitimate case — the same date as a
  datetime cell on one side and an ISO string on the other — is preserved
  and pinned.
- Pinned by `tests/test_dax_engine.py::TestDatePrefixKeysStayDistinct`
  (the issue's four key-shape repros through a relationship: all 20, never
  the 40 grand total; single-key filter returns one row; the legitimate
  date alias still resolves in both the index fast path and the matcher).

## [0.9.86] - 2026-08-13

### Fixed — metadata edits no longer fail on real-world models with incompressible chunks (issue #41)

- **Every metadata-only edit failed with "Compression failed or not
  effective"** on PBIX files whose model contains already-compressed
  VertiPaq chunks — i.e. any substantial real-world file. The xpress9
  binding's encoder lands ~430 bytes OVER the input on incompressible
  2 MiB chunks (Power BI's own encoder stores them ~410 bytes UNDER), and
  the wrapper's strict guard turns that into a hard failure for the whole
  re-encode.
- `compress_datamodel` now runs a **fallback ladder**: (1) the primary
  single-threaded re-encode, byte-compatible with everything previously
  produced; (2) on incompressible-chunk failure, a **hybrid multi-threaded
  container** that reuses the original stream's unchanged session prefix
  BYTE-VERBATIM — sidestepping the encoder for exactly the chunks it cannot
  emit, and keeping Power BI's superior window-22 encoding for them — and
  re-encodes only the changed tail as single-chunk thread-groups (each
  legally its own XPress9 session; empirically pinned: XPress9 LZ77 windows
  chain within a session, so only session prefixes are reusable, and the
  multi-threaded container is the format's sanctioned way to mix sessions);
  (3) the **uncompressed ABF format** (`STREAM_STORAGE_SIGNATURE`), which
  every reader of this entry accepts, with a warning surfaced on the tool
  response. Metadata (sqlite) lives near the END of the ABF — chunk 8 of 10
  on Adventure Works — so the incompressible VertiPaq chunks sit in the
  reusable prefix and fallback (2) covers the reported repros. Hybrid
  output is itself reusable, so **repeated edits stay compressed**.
- Every hybrid container is round-trip-verified against the input before
  being returned; `original_dm` is threaded through `_modify_metadata_only`,
  `pbix_datamodel_recompress`, and `pbix_datamodel_replace_file`; the
  multi-threaded reader caps its thread pool at 16 (the declared thread
  count is a grouping, not a concurrency requirement).
- Pinned by `tests/test_incompressible_datamodel.py` (9 tests: hybrid
  round-trip on the Desktop-authored AW stream, verbatim prefix reuse,
  hybrid-of-hybrid chaining, the full ladder, unrelated errors still raise,
  informative terminal error, primary path unchanged for compressible
  input, end-to-end chained add_measure under a forced incompressible
  condition with save + reopen + evaluate).

## [0.9.85] - 2026-08-13

### Fixed — count-family results typed Int64, declared Measure.DataType flows through (issue #40, docs r30)

- **`pbix_evaluate_dax` labelled every numeric result `data_type: "Double"`**,
  including count-family values it returned as Python ints — so
  `DISTINCTCOUNT` came back `value: 5, data_type: "Double"` and consumers
  formatting by the reported type padded counts to `5.00` where Desktop
  renders the bare `5` (Analysis Services types the count family Whole
  Number). `DAXResult`'s type derivation now labels int values **`Int64`**
  and floats `Double`, matching the engine's own int/float discipline.
- **A declared `data_type="Int64"` at add_measure time was write-only** for
  evaluation. `Measure.DataType` now loads into the DAX context and a
  declared Int64 coerces an integral float result to int (the way AS casts
  to the declared type) before typing. Only the value-consistent direction
  is trusted — Measure.DataType is unreliable the other way (currency
  measures stored as Int64), so a non-integral value keeps its real Double
  typing.
- Pinned by `tests/test_issue24_type_info.py::TestCountFamilyTypesInt64`
  (the issue's ProbeTyped/ProbeUntyped repro: both now `value 5` /
  `data_type "Int64"`; Double sums stay Double; unit-level derivation).

## [0.9.84] - 2026-08-13

### Fixed — CALCULATE predicate sugar vs numeric column types (issue #39, docs r29)

- **`CALCULATE(expr, T[Year]=2024)` returned BLANK on Double-typed columns**
  while the explicit `FILTER(ALL(T[Year]), …)` form answered 350. The sugar's
  In-set matched by `str()` in both the `_indices_for_values` fast path and
  `make_value_matcher` — and `str(2024) != str(2024.0)`, so the filter
  selected ZERO rows. (The issue's inline-int repro passed; any
  Double/Decimal column — e.g. everything QlikView-converted — hit it.)
  Both paths now also match NUMERICALLY whenever both sides parse as
  numbers, in both directions (int literal vs Double cells and the mirror),
  covering `=`, `IN {…}`, `NOT(… IN …)`, and caller-supplied
  `filter_context` In-sets. Bools are excluded — `TRUE` never aliases `1`.
- **Literal-first predicates (`2024 = T[Year]`) registered NO filter** and
  silently answered the grand total. A reversed-form parse now flips the
  comparison onto the column for `=`, `<>`, `<`, `<=`, `>`, `>=`.
- Pinned by `tests/test_calculate_predicates.py::
  TestNumericTypeCoercionInFilters` (9 tests: sugar==FILTER equivalence in
  all four type pairings, IN/NOT-IN, literal-first with flipped operators,
  no-match still BLANK, strings untouched, bool/1 non-aliasing, caller
  filter-context coercion). Desktop-conformance goldens unchanged.

## [0.9.83] - 2026-08-13

### Fixed — datetime.utcnow() retired at all eight call sites (issue #38, docs r28)

- Python 3.12+ deprecates `datetime.utcnow()` (removal-scheduled); the eight
  call sites produced ~700 warnings per downstream test run and would become
  a hard `AttributeError` on removal. Each site was replaced with its
  naive/aware requirement stated, not a blind sed: the DAX engine's
  `UTCNOW()`/`UTCTODAY()` stay NAIVE (aware values would break comparisons
  against the model's naive datetimes), the three FILETIME epoch-delta sites
  (server.py ×3, builder_v2.py) stay naive because they subtract a naive
  1601 epoch, and builder_v2's db.xml timestamp formats the UTC wall-clock
  unchanged.
- **Latent bug fixed en passant**: `formats/metadata_schema.py`'s
  `_windows_filetime_now()` called `.timestamp()` on the naive `utcnow()`
  value — Python reinterprets a naive datetime as LOCAL time, so every
  FILETIME it stored was skewed by the host's UTC offset (2 h here). It now
  uses an aware UTC datetime, agreeing with builder_v2's epoch-delta helper.
- **NOW()/UTCNOW() semantics checked as asked**: they were already split
  correctly — `NOW()` returns local time, `UTCNOW()` UTC, matching DAX; now
  pinned. Suite warnings drop from ~950 to 10.
  `tests/test_dax_engine.py::TestUtcnowRetirement`: no `.utcnow()` call
  sites remain (source scan), the NOW family is deprecation-free and naive,
  `NOW()-UTCNOW()` equals the host UTC offset, and the two FILETIME helpers
  agree.

## [0.9.82] - 2026-08-12

### Fixed — column-backed field parameters refused in aggregating value roles (issue #37, docs r27)

- Residual of #36 (whose measure-parameter fix is Desktop-verified): a field
  parameter whose fields are raw numeric COLUMNS bound to `Y`/`Values`/
  `Y2`/`Size` of a chart still rendered an empty visual — a bare column in
  a value role is dropped by Desktop, and the Sum-wrap alternative breaks
  the NAMEOF correspondence (#36). Both silent-empty shapes, so
  `pbix_bind_field_parameter` now REFUSES the bind at author time with an
  actionable message (names the offending column fields, suggests
  `pbix_datamodel_add_measure` + repointing the parameter, or a grouping
  role). Columns remain valid for grouping roles (`Category`/`Axis`/`Rows`
  — the documented dimension-swap use, projected bare per #36) and for
  table/slicer wells (Chandoo oracle: covered column in tableEx Values).
  Also fixed the refusal path leaking its temp metadata `.db` on Windows
  (handle still open at unlink — the WinError 32 class). Pinned by
  `TestCoveredColumnStaysBare` (refusal + grouping-role + tableEx cases).

## [0.9.81] - 2026-08-12

### Fixed — field parameter bound over COLUMNS rendered an empty visual (issue #36, docs r26)

- `pbix_bind_field_parameter` on a well whose parameter fields are numeric
  COLUMNS produced a chart that draws its title and category axis but **no
  bars and no value axis**, in Desktop and the service — the silent-wrong-
  render class. Root cause: the implicit-aggregation pass wrapped the
  parameter-covered column into `Sum(F.Sales)`, breaking the queryRef ↔
  parameter-field correspondence (`'F'[Sales]` NAMEOF) that Desktop's query
  re-derivation resolves the well through — it drops the well entirely.
  Diffed against three Desktop-authored bindings in the corpus (Furniture
  Sales, Root Cause Analysis, Chandoo Performance): every parameter-covered
  select is exactly what the NAMEOF ref resolves to — a bare Column or
  Measure, never an Aggregation. `apply_implicit_aggregations` now skips
  projections covered by `queryFieldParametersByRole` (uncovered siblings on
  the same visual still get their implicit Sum), which also fixes the
  `columnProperties` key mismatch (`F.Sales` vs a projection renamed to
  `Sum(F.Sales)`). Measure-backed parameters were unaffected. Pinned by
  `tests/test_field_parameter_binding.py::TestCoveredColumnStaysBare` (the
  issue's own A/B repro) and `TestApplyImplicitAggregationsSkipsCovered`.

## [0.9.80] - 2026-08-11

### Added — TMDL import + PBIP open/save (issue #34, the OpenBI #14 blocker)

Model round-trip is now two-way. 130 tools (was 128).

- **`pbix_import_tmdl`** — the exporter's inverse. Parses a TMDL definition
  folder, a `<X>.SemanticModel` folder, or a single `.tmdl` document into a
  working schema-only PBIX: tables, columns (calculated included), measures,
  relationships (name/active/cross-filter/cardinality), user hierarchies,
  partitions with their real M/DAX sources, shared M expressions
  (parameters), RLS roles + table permissions, **ExtendedProperties** (field
  parameters' `ParameterMetadata`) and **LineageTags**. Round-trip contract,
  pinned by `tests/test_tmdl_import.py`: export → import → export reproduces
  the same TMDL files **byte-for-byte**. TMDL carries no row data, so tables
  are empty until Desktop refreshes them from their partition sources.
  Implemented on a new real TMDL parser
  (`formats/tmdl_reader.py`: indentation tree, quoted-name `''` escapes,
  inline + block expressions, extended properties, unknown-vocabulary
  tolerance for Desktop-authored TMDL).
- **`pbix_open_pbip`** — opens a PBIP project folder (the `.pbip` file or its
  directory) as a live session: the TMDL model half is built into a working
  model, and the report half is attached whether it is this project's
  `report.json` or a Desktop-authored PBIR `definition/` tree. Every existing
  tool then operates on it.
- **PBIP save** — `pbix_save` on a PBIP-opened session (no `output_path`)
  writes edits back into the project folder: TMDL re-exported into
  `<name>.SemanticModel/definition/`, the report half mirrored back
  (`report.json` or the PBIR tree), scaffolding (`.pbip`, `definition.pbism`,
  `definition.pbir`) created only when missing. `pbix_save` with a `.pbix`
  `output_path` converts the project to a PBIX.
- **TMDL exporter upgraded to carry what the importer preserves**:
  multi-line measure / calculated-column expressions now emit the TMDL block
  form (they used to leak raw newlines mid-document), hierarchies + levels,
  lineageTag on tables/columns/measures/hierarchies/levels,
  `extendedProperty` blocks, `sortByColumn`, non-default `summarizeBy`,
  `dataCategory`, `displayFolder`, relationship cardinality
  (`fromCardinality`/`toCardinality` when not many→one), and TMDL-correct
  quote escaping (doubled `''`, not backslashes).

### Fixed — KEEPFILTERS was a silent no-op (issue #35, docs r25)

- `CALCULATE(..., KEEPFILTERS(pred))` treated the predicate exactly like a
  plain override — under any outer filter overlapping the predicate's
  column, every KEEPFILTERS measure computed the override's number with no
  error (the silent-wrong-values class). The predicate now **intersects**
  the outer filter on the same column: `Cat="A"` outside +
  `KEEPFILTERS(Cat="B")` inside filters to the empty set and the SUM goes
  BLANK, exactly as Desktop answers; matching and absent outer filters are
  unchanged, and partial overlaps keep only the intersection
  (`tests/test_dax_engine.py::TestKeepFilters`). OpenBI can lift its
  HTML-export freeze on KEEPFILTERS measures.

## [0.9.79] - 2026-08-05

### Fixed — type information on the way out of the engine (issue #24 r22)

- **r22#1 — datetimes left `pbix_evaluate_dax` in two shapes**: midnight ones
  as ISO strings, with-time ones as bare OLE serials
  (`DATE(2026,8,2)+TIME(14,5,9)` → `46236.5869…`), indistinguishable from
  numbers. Date arithmetic now keeps its type in the engine (`DATE()+TIME()`
  and `d+7` are datetimes; `d1-d2` stays a number of days), every datetime
  serializes as an ISO-8601 string, and each result carries an explicit
  **`data_type`** field (`Double`/`String`/`DateTime`/`Boolean`) across all
  evaluate tools.
- **r22#2 — datetime measures stored as Double (8)**:
  `pbix_datamodel_add_measure` now infers **DateTime (9)** for
  datetime-valued expressions (`DATE()+TIME()`, `TODAY()`, `LASTDATE(…)`,
  `EOMONTH(…)`, VAR-bound and IF/SWITCH-branched forms; `d1-d2` and
  `DATEDIFF`/`YEAR` correctly stay numeric, `FORMAT` stays String).
- **r22#3 — `pbix_datamodel_query_metadata` masked the real SQL error with
  `WinError 32`** (temp `.db` removed while the SQLite handle was open —
  Windows-only, same class as 0.9.78's ModelReader fix). A bad query now
  surfaces the actual SQLite error; the docstring also states the metadata
  store is SQLite (not an AS `$SYSTEM` rowset) with quoting guidance.

### Fixed — ALLSELECTED semantics under grouped evaluation (issues #26 r24, #25 r23#2)

The engine now distinguishes the filters GROUPED evaluation injects (the
visual's row grouping) from the caller's slicer selection —
`DAXContext.group_keys` + the caller's base filter context threaded through
`pbix_evaluate_dax_grouped` / `pbix_evaluate_dax_per_dimension`. ALLSELECTED
removes exactly the former and restores the latter:

- **`ALLSELECTED()` and `ALLSELECTED(<table>)` as direct CALCULATE args were
  silent no-ops** (the bare form didn't even match the arg parser) — every
  percent-of-total idiom rendered flat 1.0. Both now restore the outer query
  context: 180 for every group on r24#1's model, with real percent-of-total.
- **`ALLSELECTED(<column>)` behaved exactly like `ALL(<column>)`** — under a
  `Cat IN (A,B)` slicer it answered the 180 grand total where Desktop answers
  the 60 slicer total (r24#2). It now restores the slicer's own selection on
  that column — including when the sliced column IS the grouped column, where
  the group value had overwritten the slicer's in the merged filter context.
- **Two column-scoped ALLSELECTED args no longer degrade to ALL** (r24#3): an
  outer `Reg=N` filter survives `ALLSELECTED(S[Cat]), ALLSELECTED(S[Reg])`.
- **ALLSELECTED as a FILTER/RANKX source** (r23#2): the running-total idiom
  `CALCULATE(SUM, FILTER(ALLSELECTED(S[Cat]), S[Cat] <= MAX(S[Cat])))` returns
  30/65/105/180 and `RANKX(ALLSELECTED(S[Cat]), …)` ranks 4/3/2/1 instead of
  answering 1 for every group.

### Fixed — 2023 window functions under grouped evaluation (issue #25 r23#1)

ROWNUMBER / RANK / OFFSET / INDEX / WINDOW required an iterator row context, so
in a measure under `pbix_evaluate_dax_grouped` the first two returned blank and
the last three silently left the filter context unchanged (plausible wrong
numbers, no error). They now synthesize the **visual axis**: the selected
values of the ORDERBY/PARTITIONBY columns, with the current position taken from
the group's own filter. On the A/B/C/D model: `ROWNUMBER(ORDERBY(S[Cat]))` =
1/2/3/4, `RANK(ORDERBY(S[Cat] DESC))` = 4/3/2/1, `OFFSET(-1,…)` = blank/30/35/40,
`INDEX(1,…)` = 30 everywhere, `WINDOW(1,ABS,0,REL,…)` = 30/65/105/180. Also
fixed: `_win_index_of` matched the first row for every single-column current row
(`{} == {}`), and `ORDERBY(T[C] DESC)` (space form) silently sorted ASC.

### Fixed / verified — remaining #25 items

- **r23#3** (`ALL(<table>)` as FILTER source): run-verified correct on the
  reported shapes — `CALCULATE(SUM, FILTER(ALL(S), S[V]>30))` = 150 under an
  outer slicer (table-filter replacement incl. the sliced column), SUMX form
  identical; pinned by regression.
- **r23#5** (DATEVALUE constant-folded only): does not reproduce — direct
  column, VAR-bound, and concatenated forms all materialize correct dates in
  calculated columns; pinned by regression.
- **r23#6**: `pbix_datamodel_add_calculated_column` docstring now states the
  exact supported/refused function surface (measured, not guessed).
- **r23#4**: the O(n²) `SUMX(FILTER(ALL(D), D<=_at), CALCULATE(…))`
  accumulation form is documented in limitations.md with the O(n)
  single-pass rewrite.

### Regression

`tests/test_issue25_26_dax.py` (8 scenario tests pinning every value above) +
`tests/test_issue24_type_info.py` (18, from 0.9.79's #24 work landing in the
same release train). Full fast suite: 1,775 passed; conformance goldens and the
24-report corpus parity unchanged.

## [0.9.78] - 2026-08-01

### Fixed — `ModelReader` leaked its temp SQLite handle on a query error (Windows `WinError 32`)

Found while reproducing #21. `ModelReader._query_metadata` — the shared read path
behind `pbix_get_model_measures`, `pbix_get_model_relationships`,
`pbix_get_model_power_query`, `pbix_list_data_sources`, and the schema/table
readers — closed its SQLite connection **only on the success path**. Any query
error left the handle open, so the `finally: os.unlink()` of the temp `.db` then
raised *"[WinError 32] the file is being used by another process"*, masking the
real error. It only bit Windows (POSIX allows unlinking an open file), which is
why CI never saw it. The connection is now closed on every path before the unlink,
and a failed unlink (e.g. an AV scanner briefly holding the file) is swallowed
rather than raised. Regression: `tests/test_model_reader_tempfile.py`.

## [0.9.77] - 2026-08-01

### Fixed — `pbix_set_table_data` silently dropped a column's declared type (OpenBI #21)

- **A column type passed under `dataType` (or a lowercase name like `int64`) was
  ignored, so every column defaulted to `String`.** A numeric column then shipped
  as text: `SUM()` over it returned BLANK, and Power BI Desktop rendered every
  bound visual as *"Error fetching data for this visual"* — while the tool
  reported success and `pbix_query_table` returned the (text) rows. The engine
  reproduces it: after the call, `[Val] = SUM(S[V])` went from 550 to **blank**;
  with the fix it returns the correct **3000**.
- The column type is now read from `data_type`, `dataType`, or `type`, and the
  value is normalized case-insensitively (`"int64"` → `Int64`). A column with a
  type we do not recognize is **rejected with a clear error** instead of silently
  becoming `String`; a column with no type at all still defaults to `String`.
  Centralized in `PBIXBuilder.add_table` (`normalize_column_defs`), so every
  table-building path — `pbix_set_table_data`, `pbix_create`, the rebuild path —
  gets it.
- **Clearer errors for a malformed `columns` payload.** Passing `columns` as a
  list of strings used to surface a raw `"string indices must be integers"`
  TypeError; it now says *"Column 0 … must be an object like {\"name\": …,
  \"data_type\": …}, not bare strings."* The accepted shape (and the accepted
  keys) are now documented in the tool docstring.
- Regression: `tests/test_set_table_data_typing.py` (10 cases — the reporter's
  exact camelCase/lowercase payload keeps the bound measure queryable, plus the
  malformed-shape and unrecognized-type errors).

## [0.9.76] - 2026-08-01

### Docs — cross-document consistency sweep + number ratchet

An independent adversarial re-read of the whole doc set (three skeptical readers)
found cross-document number drift that the 0.9.74/0.9.75 passes missed. All are
reconciled to ground truth measured from the code/artifacts, and the drift-prone
counts are now pinned by a test so they cannot silently go stale again.

- **Tool count → 128 everywhere.** `docs/architecture.md`, `docs/tool-contracts.md`,
  and a stale `README.md` Package Layout comment said 127; the authoritative count
  (`@mcp.tool` decorators in `server.py`) is 128. `tool-contracts.md` also gains
  the missing `pbix_set_partition_m` under Partition Management (3 → 4).
- **Test corpus → 24 reports everywhere.** `README.md` (two spots) said "4
  dashboards" / "20-file"; `docs/dax-coverage.md`, `docs/supported-dax.md`, and
  `PROGRESS.md` said "25-file". The default download is 24 reports (4 community
  MIT dashboards + 20 Microsoft public samples, both MIT). `THIRD_PARTY_NOTICES.md`
  now credits both sources. (The CryptKey experiment's separate 25-file
  byte-observation set is unchanged — it is a different corpus.)
- **DirectQuery (open existing)** — `docs/limitations.md` claimed layout/measures/
  metadata are read-only, contradicting `README.md` and `SUPPORT.md`; editing
  works (it is metadata/JSON, storage-mode-agnostic), only DAX eval and table
  reads are unavailable. Reconciled to "editing works".
- **Test tallies de-brittled.** `docs/development.md` (888) and `README.md` (1202)
  gave stale fast-suite figures; both now point at `pytest --co -q` for the exact
  number. `test_pbir_reader.py` count corrected 38 → 47.
- **Overclaim scoping.** The "No Microsoft Dependencies" line now scopes "every
  layer … that pbix-mcp's supported capabilities require" (matching the rest of
  the README); `docs/vertipaq-spec.md`'s footer no longer states a categorical
  "in accordance with applicable reverse engineering laws" conclusion — it is
  hedged and cross-references `docs/legal-and-cleanroom.md` with a not-legal-advice
  note.
- **Ratchet extended** (`tests/test_doc_numbers.py`): now also pins the tool count
  (vs `server.py`) and the corpus size (vs `scripts/download_test_corpus.py`), on
  top of the DAX value-probe / golden-backed / corpus-pinned counts. The DAX-framing
  reader found nothing — the 0.9.75 "100% of the evaluable surface" wording holds.

## [0.9.75] - 2026-08-01

### Docs — say "100% of the evaluable surface", not a bare "435 of 467"

Follow-up to 0.9.74. The short docs stated DAX coverage as a bare "435 of 467
functions", which reads as ~93% / "unfinished" when it is not. Parity is **100%
of the DAX surface Power BI Desktop can evaluate in a query**: all 435
query-evaluable functions match at 1e-9. The other 32 of the engine's 467-function
catalog are **not a coverage gap** — Desktop itself refuses to evaluate them in a
query (13 visual-calculation-only, 8 week-grain time-intelligence requiring a
custom-calendar object the engine build refuses, 4 calculation-group-only, 3
engine-internal, 3 edition/compat-blocked `INFO.*`, 1 DirectQuery-only), with
Desktop's own error text recorded in `tests/conformance/golden.json`. Matching
parity means refusing them the same way; implementing them would invent behavior
Desktop does not have. Reworded `README.md`, `docs/architecture.md`,
`docs/limitations.md`, `SUPPORT.md`, and `CONTRIBUTING.md` to lead with the
100%-of-evaluable framing. See [docs/dax-coverage.md](docs/dax-coverage.md). No
code changes.

## [0.9.74] - 2026-08-01

### Docs — clean-room / IP / security paperwork

A documentation-only release that closes the governance gaps a five-dimension
clean-room/IP/security audit surfaced. No code changes.

- **`CONTRIBUTING.md`** — added a **contributor provenance certification** (each
  contribution is original or drawn only from lawful public docs/specs, lawful
  black-box observation, or license-compatible OSS), an explicit **prohibition**
  on Microsoft proprietary / decompiled / leaked material, binaries, keys,
  credentials, and copyrighted assets, and a **Developer Certificate of Origin
  1.1** (`git commit -s` / `Signed-off-by`) requirement.
- **`THIRD_PARTY_NOTICES.md`** (new) — consolidated attribution manifest for the
  runtime dependencies (xpress9 MIT, xmhuffman MIT, mcp/FastMCP MIT, pydantic
  MIT, apsw "any-OSI"), the development-only test corpus (MIT, Sajjad Ahmadi),
  and the bundled `.pbiviz` visual + icon provenance. Records what is **not**
  bundled (no Microsoft source/binary/key/credential, no Microsoft fonts or
  theme assets, no Deneb/Vega runtime).
- **`SECURITY.md`** — extended the threat model with the previously-undocumented
  controls: binary-parser hardening (ABF/VertiPaq/XPRESS9 bound-check untrusted
  length/offset/count fields, fail closed), no silent network egress from
  report-embedded URLs, credential redaction (never logged/persisted, incl.
  `pbix_get_password`), report-text-is-data (prompt-injection resistance), no
  untrusted values in a shell, and secure per-file working/temp directories.
- **`docs/legal-and-cleanroom.md`** (new) — consolidated, conservatively-worded
  legal posture: interoperability provisions framed as a **foundation, not a
  guarantee**, subject to their conditions, with an explicit *not legal advice*
  note and a recommendation to consult counsel.
- **`README.md`** — replaced the categorical "reverse engineering … supersedes
  contractual restrictions" claim with a descriptive, attributive summary plus a
  not-legal-advice note; scoped two absolute completeness phrasings ("every
  byte" / "every layer") to the project's supported capabilities; linked
  `THIRD_PARTY_NOTICES.md` and the DCO from the License section; clarified the
  trademark line as nominative use.
- **Consistency** — reconciled the stale "best-effort evaluator (174/156
  functions)" self-description in `docs/architecture.md`, `docs/limitations.md`,
  and `SUPPORT.md` to match the README's verified-parity framing (435 of 467
  functions); corrected `SUPPORT.md`'s stale "PBIR read-only" note; refreshed
  the tool count (128) and DAX function count in `CONTRIBUTING.md`.

## [0.9.73] - 2026-07-31

### Fixed — RELATED() ignored row context inside iterators (silent wrong values)

- **`SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))` — the canonical
  line-total pattern — returned wrong values** (e.g. 839.72 instead of
  599.72). `RELATED` resolved to the *first visible row* of the related
  table regardless of which row the iterator was on, so every iterated row
  got the same related value; grouped output masked it (each group leaves
  one related row visible) while grand totals and cards were corrupted.
  `RELATED` now navigates from the **current row's** foreign key through the
  relationship (single- and multi-hop, active relationships only). A second,
  related defect is fixed too: `FILTER(T, RELATED(...) = v)` had `RELATED`
  wrongly listed in the aggregation guard, so FILTER never bound the current
  row for it — removed. Desktop-pinned probes added to the conformance
  harness (`RELATED` → 350 / 7 / 1) plus an 9-case unit matrix
  (`tests/test_related_rowcontext.py`). Reported by OpenBI's bridge suite
  (findings #20).

### Fixed — builder reports now render in Power BI Desktop (was: crash)

- **Builder-produced reports carried no `themeCollection`, so Power BI
  Desktop's renderer crashed** with *"Cannot read properties of undefined
  (reading 'customTheme')"* — the model loaded (queries answered), but the
  report never rendered. `PBIXBuilder._build_layout` now emits a
  report-level `config.themeCollection` referencing a built-in base theme
  by name (the same one `pbix_set_theme` uses; no theme asset shipped).
  **Verified by opening the built report in Desktop and screenshotting the
  rendered bar chart with data**, not just checking the model. Regression
  guard: `tests/test_report_render_theme.py`.
- **A visual config that names fields but matches no binding shape now
  warns instead of silently shipping an empty visual.** Passing, e.g.,
  `{"category": "T.C", "values": [...]}` (wrong shape) to a chart used to
  produce a container Desktop renders as an empty "drag fields here"
  placeholder with no signal; the builder now emits a `UserWarning` naming
  the expected shape.

**CryptKey.bin is now independently generated — no Microsoft key material
ships in the package.**

Earlier builds embedded a fixed 144-byte `CryptKey.bin` copied out of a
working PBIX, documented as a "Microsoft RSA key BLOB [that] requires
`rskeymgmt` to generate." A clean-room differential study of 25 lawfully
generated corpus files
([docs/reverse-engineering/experiments/cryptkey.md](docs/reverse-engineering/experiments/cryptkey.md))
established that the 144 bytes are a fixed-format container: a scaffold that
is byte-identical across every observed file, plus a variable region that
Power BI Desktop accepts filled with our own non-degenerate bytes (random and
hash-derived key regions load and serve data; only a degenerate all-zero
region is rejected). The "RSA key BLOB / rskeymgmt" description was unverified.

### Changed

- `abf_from_scratch.build_cryptkey()` composes the observed format scaffold
  with a self-authored SHA-512 keystream; the from-scratch builder
  (`builder_v2`) and the rebuild path (`abf_rebuild`) both use it. Verified:
  a builder-produced file carrying the generated key opens in Power BI
  Desktop and returns the expected measure value. The generated key is
  database-independent. Regression: `tests/test_cryptkey.py`.
- README / architecture / limitations docs restated accurately: every
  artifact is generated (including `CryptKey.bin`); dropped the "Microsoft
  RSA key BLOB / requires Microsoft's crypto infrastructure" framing and the
  "entire PBIX format" absolute.
- Added `docs/reverse-engineering/` provenance trail (methodology + the
  CryptKey experiment).

## [0.9.72] - 2026-07-31

**The OpenBI findings ledger is EMPTY — every open item across L3/L4/L5 is
now fixed, implemented, or closed with written rationale.**

### Added

- **Per-key Top-N filter spec** (ledger issues-14, half B — half A's
  predicate objects were already live). A `filter_context` value of
  `{"top_n": {"n": 5, "by": "<measure or Table.Column>", "direction":
  "desc"}}` is materialized server-side into a concrete In-set — the key's
  distinct values ranked by the aggregate under the other filters, blanks
  last, stable ties, `"asc"` for bottom-N — in all three evaluate tools.
  This is the materialization OpenBI performed client-side, moved
  server-side.

### Closed with rationale (no code)

- **Matrix / series entry point** (issues-17): covered by composition —
  `pbix_evaluate_dax_grouped` takes a composite `group_by`
  ("RowDim.Col,ColDim.Col") and returns one structured row per cell; the
  recipe is now in the tool docstring.
- **Propagation-result reuse** (issues-17): measured unnecessary — 50
  repeated calls sharing a filter set cost 6.0 ms/call (2.6 ms for a
  second measure) on a 200K-row model; existing caches already make
  repeats near-free.
- **Auto date/time synthesis** (issues-13/14): wont-do-now — hidden
  tables the user did not author, Desktop generates its own when the
  option is on, built models verify clean without them, and no downstream
  demand exists. User-authored date hierarchies remain covered by
  `add_user_hierarchy` + a calculated `CALENDAR()` table.

## [0.9.71] - 2026-07-31

**Two feature asks from the OpenBI ledger (L5, issues-12).**

### Added

- **`pbix_set_partition_m(alias, table, m_expression)`** — table-scoped raw
  Power Query M setter, the complement to `pbix_set_m_code`
  (whole-DataMashup) and `pbix_update_data_source` (structured params).
  Writes `Partition.QueryDefinition` verbatim, metadata-only: cached
  VertiPaq rows stay, Power BI runs the new M on the next Refresh.
- **`source_json` parameter on `pbix_set_table_data`** — apply connection
  parameters to the table's partition right after the rows are written
  (same format as `pbix_update_data_source`), so writing a data snapshot
  and pointing the partition at its live source is one call. A failed
  source update reports loudly that the rows ARE written.

## [0.9.70] - 2026-07-31

**L4 Desktop-fidelity cluster complete.**

### Fixed

- **`config.layouts` and `drillFilterOtherVisuals` are now written for
  every visual type** by `pbix_add_visual` — previously only image
  visuals carried them. Field-for-field audit against a Desktop-authored
  `tableEx` (GeoSales) shows structural parity; the remaining differences
  (`objects.columnFormatting`/`columnHeaders`/`grid`/`total`, `vcObjects`
  styling, `columnProperties` display names) are user/theme content
  accepted via `config_json` / `pbix_format_visual`, not defaults to
  invent.

### Documentation

- **Offline behavior of reference-only public custom visuals** recorded
  (ledger issues-8): Desktop fetches `publicCustomVisuals` GUIDs from
  AppSource at open and caches per-machine (`ExtensionCache`); offline
  with a cold cache the report still opens with an unavailable-visual
  placeholder in that container. Noted in
  `pbix_reference_public_visual`'s docstring.

### Internal

- The `config: dict` annotation in `pbix_add_visual` cleared a cluster of
  mypy noise — the checked-error count drops from 140 to **127**, the new
  ratchet baseline.

## [0.9.69] - 2026-07-31

**Desktop-fidelity fixes from the OpenBI findings ledger (L4).**

### Fixed

- **`tabOrder` is now stamped on every visual container** (ledger
  issues-3). `pbix_add_visual`, `pbix_add_html_visual`, and the builder
  write Desktop's 1000-step `z` and `tabOrder = z + 1000` on both the
  container and `config.layouts[0].position` — previously only
  `pbix_add_image` did, and `add_visual` wrote `z = 0` with no `tabOrder`
  at all. Keyboard/tab navigation order in built reports now matches
  Desktop-authored files.
- **The builder's Report/Layout carries a report-level `config`**
  (ledger issues-4): `version` (5.61, the corpus-era report schema),
  `activeSectionIndex`, `linguisticSchemaSyncVersion`,
  `defaultDrillFilterOtherVisuals`, and filter-pane settings — matching
  Desktop-authored files field-for-field (ground truth: MS_AI_Sample,
  GeoSales_Dashboard). Previously the layout had only `id` + `sections`.

Both verified live: a freshly built report opens in Power BI Desktop and
answers its measure through the workspace engine.

## [0.9.68] - 2026-07-31

**Two "silently wrong output" fixes from the OpenBI findings ledger (L3).**

### Fixed

- **Unresolvable `[Name]` references are now typed errors, not silent
  blanks** (ledger issues-7). A measure like `[Nope] + 1` used to answer
  `1` with status `"ok"` — the missing reference degraded to BLANK and the
  arithmetic kept going. The engine now raises for a bare `[Name]` that is
  neither a measure, a row/extension-column key, nor a column anywhere in
  the model (the same rule the qualified `Table[Name]` path has had since
  0.9.53), records the reason, and `pbix_evaluate_dax` reports status
  `"error"` with the message — distinguishable from a legitimate BLANK.
  Extension-column aliases (`SUMX(ADDCOLUMNS(...), [alias])`) are
  unaffected. Corpus-verified: MS_AI_Sample 22/22, MS_Life_Expectancy
  41/41, Agents_Performance 102/102 against fresh Desktop ground truth.
- **Coordinates now AVERAGE, never Sum** (ledger issues-5). A bare numeric
  column in a map's Latitude/Longitude field well — or a
  lat/long-named numeric column in any value or X role — becomes
  `Aggregation(..., Function=1)` (`Avg(...)` queryRef), matching Desktop's
  default summarization for geographic columns. Summed coordinates place
  the point on no real map. Other value-role columns keep Sum /
  CountNonNull exactly as before.

## [0.9.67] - 2026-07-31

**`PATH` + `PATHITEMREVERSE` implemented and Desktop-verified — 435 of the
live engine's 467 DAX functions now carry conformance goldens; 8 remain open
(week-grain calendar family).**

### Added

- `PATH(id, parent)` — pipe-delimited ancestor chain walked over the
  pre-transition context, integers printed Desktop-style (`1|2|4`) — and
  `PATHITEMREVERSE(path, n)`, completing the PATH family.

### Changed

- The conformance fixture's parent-child table `PC` is now a **calculated
  table** (`ADDCOLUMNS` over `DATATABLE`, root parent a true BLANK):
  Desktop recomputes and fully processes calculated tables at open, which
  makes their hierarchy support structures PATH-queryable. Import tables
  written by the builder are not — Desktop's *"internal support structures
  not processed"* — a builder issue now precisely characterized in
  [docs/dax-coverage.md](docs/dax-coverage.md) (metadata layout matches
  Desktop's; versions, `IsPrivate`, and refresh-type experiments ruled
  out; remaining suspect is a binary detail of the H$ structure files).

### Fixed

- A `BLANK()` inside a `DATATABLE` row literal materializes as 0 in
  Desktop's own evaluation — the fixture avoids the shape and documents it.

## [0.9.66] - 2026-07-31

**119 new DAX functions (314 → 433 of the live engine's 467): the window
family, the complete INFO.\* metadata family, and conformance batches 4–5.
Every function in the engine is now either implemented with Desktop-captured
goldens or proven not query-authorable by Desktop's own error message — 10
remain open (week-grain calendar family, PATH pair), each with a concrete
investigation path. See [docs/dax-coverage.md](docs/dax-coverage.md).**

### Added

- **Window functions** — `ROWNUMBER`, `RANK` (`SKIP`/`DENSE`), `INDEX`,
  `OFFSET`, `WINDOW` (`ABS`/`REL` endpoints) with `ORDERBY`, `PARTITIONBY`,
  `MATCHBY` markers. The relation is materialised against the
  pre-transition context so window functions inside iterators see every
  iterated row; all 14 Desktop goldens (including partitioned row numbers
  and relative windows) match.
- **`INFO.*` family (66 functions)** — the full model-metadata surface
  Desktop will evaluate in a query, serving the logical model plus the
  Vertipaq physical-structure counts it implies (pinned by Desktop's own
  counts: `INFO.FUNCTIONS()` = 467, 22 storage tables, 52 column storages).
  A generated `dax/function_catalog.py` carries the 467-function inventory.
- **Batch 4 (40 functions)** — dotted-name column statistics
  (`STDEV.S`/`VAR.P`/…), type predicates (`ISNUMERIC`/`ISCURRENCY`/…),
  `CONVERT`, `YEARFRAC`, `LINEST`/`LINESTX` (BLANK participates as zero,
  Desktop-verified), `TOCSV`/`TOJSON`, `SAMPLE`, `ADDMISSINGITEMS`,
  `ROLLUP`/`ROLLUPGROUP`/`ISSUBTOTAL`, and more.
- **Misc** — `NONVISUAL` (applied as the filter it wraps; Desktop-verified
  grouped-column requirement), `ROLLUPISSUBTOTAL` (working argument order
  captured), `SAMPLEAXISWITHLOCALMINMAX`, `COLUMNSTATISTICS` (its 20
  fixture rows are user columns + one internal RowNumber row per table —
  the batch-4 needs-model-feature classification is reversed).

### Changed

- `docs/dax-coverage.md` rewritten from the definitive artifacts: 433
  implemented and verified, 26 classified out with Desktop's error text
  recorded in `golden.json`, 10 open.

### Fixed

- Conformance probes for `USERCULTURE`-style environment-dependent INFO
  counts pinned by shape (`COUNTROWS(...) > 0`), not by msmdsrv-session
  internals.

## [0.9.65] - 2026-07-31

**131 new DAX functions (183 → 314), each pinned to Power BI Desktop's own
answers by a new per-function conformance harness.**

`tests/conformance/` holds a deterministic fixture model built with pbix-mcp's
own builder, per-function probe expressions, and Desktop-captured golden values
(207 probes, 1e-9 relative for floats). The conformance suite has no
"unsupported" escape hatch — an unimplemented function fails its probes — which
makes it a ratchet toward full-surface parity. The authoritative surface is the
engine's own `$SYSTEM.MDSCHEMA_FUNCTIONS` (467 DAX functions in the March 2026
build); progress is tracked in `docs/dax-coverage.md`.

### Added, all Desktop-pinned
- **Trig & math** (29): the full trig family, `COMBIN(A)`, `PERMUT`,
  `QUOTIENT`, `BIT*`, `ISO.CEILING`, `SQRTPI`, `DEGREES`, `RADIANS`
- **Statistical distributions** (24): `NORM.*`, `BETA.*`, `CHISQ.*`, `T.*`,
  `EXPON.DIST`, `POISSON.DIST`, `CONFIDENCE.*`, `PERCENTILE(X).INC/EXC`,
  `RANK.EQ` — inverse normal via Acklam + Halley polish, incomplete beta/gamma
  via Lentz continued fractions, all at 1e-9 against Desktop
- **Table machinery & dates** (18): `GROUPBY`+`CURRENTGROUP`,
  `NATURALINNERJOIN`, `NATURALLEFTOUTERJOIN`, `TOPNSKIP`, `FILTERS`,
  `CONTAINSROW`, `ALLNOBLANKROW`, `ALLCROSSFILTERED`, `SUBSTITUTEWITHINDEX`,
  `DETAILROWS`, `ISONORAFTER`, `NEXTDAY`, `PREVIOUSDAY`, `NETWORKDAYS`,
  `ISDATETIME`, `IGNORE`/`ROLLUPADDISSUBTOTAL` in `SUMMARIZECOLUMNS`
- **Financial** (51): the complete family — annuities, depreciation
  (Excel month conventions), `XIRR`/`XNPV`, and the bond family on day-count
  bases 0–4 with a coupon-schedule kernel; `RATE`/`YIELD`/`ODDFYIELD` via
  Newton
- Dotted function names (`STDEV.S`, `T.DIST.2T`) now parse at all — the name
  regex stopped at `\w`, so every dotted function silently fell through

### Classified from Desktop's own behaviour, not assumed
- `CEILING.MATH`/`FLOOR.MATH`: listed by the DMV, refused by the engine in a
  query — out of authorable scope
- The week-grain time-intelligence family requires a model *calendar
  reference* — needs-model-feature
- `ROWNUMBER`/`ORDERBY` (window family): deferred to a dedicated batch rather
  than shipped shallow

## [0.9.64] - 2026-07-31

### Fixed

- **`pbix_open` no longer strands its extraction directory.** Every open
  extracts the .pbix into a `pbix_mcp_*` directory under the system temp dir,
  and only `pbix_close` deleted it — so any caller that exits without closing
  (scripts, test runs, killed processes) leaked the whole extraction, and the
  directories accumulate until the disk fills. Two independent mechanisms now
  clean up: an `atexit` hook removes every directory the process created and
  has not closed, and a once-per-process scavenger deletes stale sibling
  directories whose owning pid (parsed from the end of the name) is dead. Live
  and unparseable names are kept unless a 7-day backstop passes, so another
  process's active extraction is never touched — and pid liveness on Windows is
  probed via `OpenProcess`, never `os.kill(pid, 0)`, which on Windows
  terminates the target.

## [0.9.63] - 2026-07-31

**Full corpus parity: every comparable cell now matches Power BI Desktop.**
Verified on the release commit — **432/432** comparable measures at the grand
total, **1,705/1,705** measure×dimension cells under a filter context, and
**397/397** calculated columns against their stored VertiPaq values. The four
cells 0.9.62 shipped as known-wrong are fixed.

### Filter propagation follows table expansion

- **A filter on a COLUMN propagates one → many only; a filter on a TABLE also
  propagates many → one, because it filters the table's EXPANDED table.**
  Desktop returns both behaviours in the same model, pinned in one query on
  `MS_Employee_Hiring`:

  ```
  MAX('Date'[PeriodNumber])                                      201612
  CALCULATE(same, FILTER(Employee, ISBLANK(Employee[TermDate])))  201412
  CALCULATE(same, Employee[FP] = "FT")                            201612
  ```

  The relationship index was symmetric, so a column filter on the many side
  wrongly restricted the one side: on `Agents_Performance`,
  `SELECTEDVALUE(DimEmployee[EmployeeKey])` under
  `DimStore[StoreType]="Catalog"` answered 213 where Desktop answers BLANK, and
  the three `[Rank Filtering *]` measures returned 1 for Desktop's 0 (with
  `[Employee Name]` returning a name for Desktop's empty marker). A purely
  directional index — attempted and reverted during 0.9.62 — broke the table
  case instead: `[Actives]` is `CALCULATE([EmpCount], FILTER(Employee, …))` and
  *needs* the many → one flow, since `[EmpCount]` anchors to
  `MAX('Date'[PeriodNumber])`, which only drops to the last period Employee
  covers because the table filter restricts Date.

  The engine now keeps a directional index as the default and records the keys
  registered by a table filter argument; only those may take the reverse
  direction. Both anchors hold at once — `[Actives]` = 32,401 and the Agents
  `SELECTEDVALUE` = BLANK — which no previous formulation achieved, and the
  regression tests discriminate all three historical states so neither wrong
  version can return unnoticed.

## [0.9.62] - 2026-07-31

**Five filter-context defects, found by widening the Desktop comparison past the
grand total.** The grand total is one cell per measure and the cell where
blank-propagation, relationship filtering and time intelligence trivially agree;
comparing `measure x dimension value` is what exposed these. Every rule below is
Desktop's answer, taken from the workspace `msmdsrv` over ADOMD.

Verified on the release commit: **431/431** comparable measures at the grand
total, **1,701/1,705** measure×dimension cells under a filter context, and
**397/397** calculated columns against their stored VertiPaq values.

The four remaining filter-context cells are one known cluster on
`Agents_Performance`, tracked in `PROGRESS.md`: a filter on a COLUMN must not
propagate from the many side of a relationship to the one side, while a filter on
a TABLE must, because it filters the expanded table. Desktop returns both in the
same model. The engine currently gets the table case right and the column case
wrong; a fix that reversed that trade was landed and reverted this cycle rather
than shipped.

### Time intelligence

- **`DATEADD` shifts each CONTIGUOUS RUN of the selection, not the overall
  min..max range.** `'Date'[Qtr] = 2` selects seven DISJOINT quarters, and
  shifting the single span from the first to the last swallowed every month in
  between, so the filter degenerated to the whole table:
  `[New Hires SPLY]` returned the GRAND TOTAL 43,120 under *every* quarter where
  Desktop returns 11,601 for Q2 and 13,840 for Q3, and each YoY measure built on
  a SPLY inherited it. Desktop's own COUNTROWS over the same shift is
  546 = 91 x 6 — six shifted QUARTERS, not six years of dates — and 644 = 92 x 7
  for a `-1 MONTH` shift. The range form is still used *within* a run, which is
  what keeps a shifted period contiguous over the date table; mapping
  date-by-date would leave holes wherever no source date lands on a target.
- **A shifted period that falls OUTSIDE the date table is BLANK, not "no
  filter".** `Ecommerce_Conversion`'s calendar starts 2025-01-01, so under
  `QuarterName=Q1` a `-1 QUARTER` shift asks for Oct–Dec 2024.
  `[Page_Views_PMTD/PQTD]` answered 14,548,763 where Desktop is BLANK, and the
  `*_%Delta` measures dividing by it came out exactly -1.0. The table form was
  already correct — `COUNTROWS` of the same `DATEADD` was blank — so only the
  CALCULATE filter path skipped the empty result.

### FORMAT

- **Date pictures are VBA-style and case-INSENSITIVE.** The token table was
  .NET-cased — `MM` month, `mm` *minutes* — but DAX pictures are neither, and
  lower-case `m` is a MONTH. `mmmm` matched `mm` twice and rendered `0000`
  instead of `July`; every `mm/dd/yyyy` came out `00/19/2021` and every
  `yyyy-mm-dd` as `2021-00-19`. `m`, `d`, `Long Date` and `Short Date` fell
  through as literal text. Desktop:
  `mmmm`→July, `mmm`→Jul, `mm`→07, `m`→7, `d`→19,
  `dddd, mmmm dd, yyyy`→Monday, July 19, 2021, `Long Date`→Monday, July 19,
  2021, `Short Date`→7/19/2021, and `m/d/yyyy` on 2021-03-05 →3/5/2021.
  `m` means minutes only when it FOLLOWS an hour token, which Desktop pins in a
  single picture: `FORMAT(<noon>, "mm hh:mm")` is `07 12:00`. The .NET
  spellings still work, since the scan is now case-insensitive.

  Worth recording how nearly this escaped: `0000` is exactly as wide as `July`,
  so the capture harness's LEN fallback — the rule that accepts a truncated
  capture when the lengths agree — was structurally blind to it, and the diff
  display truncated both sides to an identical-looking prefix.

### Filter propagation

- **`ALL(Table)` suppresses only the filters it actually cleared.** The
  suppression flagged a table for the rest of the evaluation, so a filter
  created LATER inside a nested `CALCULATE` could never propagate into it.
  Desktop keeps it: with `Cases` related to `Owners`,
  `CALCULATE(CALCULATE(AVERAGE('Cases'[CSAT]), 'Owners'[Manager]="Low, Spencer"),
  ALL('Cases'))` is 4.13796627491058 there and was the global 4.2706 here (the
  nested `COUNTROWS` was 3,914 against our 10,000). Suppression now carries a
  snapshot of the filters live when `ALL` ran, key *and* value — re-filtering a
  column `ALL` had cleared makes a NEW filter, which is why the value signature
  is needed and not just the name.
- **A multi-column table filter argument replaces propagation, like
  `ALL(Table)`.** `CALCULATE(AVERAGE('Cases'[CSAT]), FILTER(ALL('Cases'), …))`
  under a filter on the related `Owners[Manager]` averaged that manager's 3,914
  rows instead of all 10,000; the row set and the `SELECTEDVALUE` inside it were
  already right, only the filter context the row set establishes was wrong.
  `MS_AI_Sample`'s four `[CSAT Impact*]` measures are `1 - AllAvgExcept/AllAvg`
  over that shape and read ±0.03 where Desktop returns exactly 0. Scoped by the
  same snapshot: a single-column filter (`ALL(T[Col])`, `VALUES`) still lets a
  related table's filter through, and nothing is suppressed that was not live.

## [0.9.61] - 2026-07-30

**Every measure of every corpus file now returns what Power BI Desktop returns.**

The whole 24-file corpus was captured from Desktop's own engine — the workspace
`msmdsrv` instance Desktop starts for an open .pbix, queried over ADOMD — and
all 547 measures were diffed against it. Sixteen defects came out of that, every
one of them producing a plausible value rather than an error. All the rules
below are Desktop's answers, not the documentation's.

### Storage and identifier bugs that blanked whole files

- **A bare `[Measure]` reference is now resolved case-insensitively.** DAX
  identifiers are case-insensitive; the fully-qualified `Table[Measure]` path
  already knew that, the bare path did not. One `[TOTAL UNITS]` against a
  measure named `Total Units` silently blanked NINE `MS_Competitive_Marketing`
  measures — the whole SAMEPERIODLASTYEAR family (1,299,599 and 49,832 read as
  blank), their variances, and an indicator that answered 2 where Desktop
  answers 1.
- **A date stored as an Int64 serial is a date.** `IT_Support` stores
  `fact_IT_Support[Date]` as `ExplicitDataType 6`, so `DATEDIFF` returned BLANK
  on all 11,923 rows — which also made `DATEDIFF(...) <= 3` keep every row
  (`BLANK() <= 3` is TRUE) and `[% of Tickets Closed Within 3 Days]` read 1.0
  against Desktop's 0.7987.
- **Relationship join keys are matched across storage types.** The two sides
  were each stringified, so a datetime dimension never joined an Int64 fact and
  every date filter reduced the fact to zero rows.
- **A bare `[Column]` reference resolves against the model.** Power BI's own
  generated measures rely on it: `MS_Corporate_Spend`'s `[Amount]` is literally
  `TOTALYTD(SUM([Value]), 'Date'[Date])*.3`, and reading `[Value]` as a missing
  measure made all 15 of that file's measures read 0.0.
- **`ALL` / `ALLSELECTED` keep every column argument**, and `SUMMARIZE` accepts
  a table EXPRESSION, not only a table name. Both used an unanchored regex on
  the raw argument text and silently dropped everything after the first column.
- **`ALLSELECTED` restores the query context** instead of behaving like
  `VALUES`, so it now removes a filter its own `CALCULATE` applied.
- **Auto date/time hierarchy accessors** (`'T'[C].[Date]`) resolve through the
  relationship to the hidden `LocalDateTable`. `DATEADD` over one had been
  silently doing nothing.

### Semantics corrected against the live engine

| | was | Desktop |
|---|---|---|
| `BLANK() * 100`, `BLANK() / 100` | `0` | BLANK |
| `DIVIDE(BLANK(), 100, 42)` | `0.0` | BLANK (the alternate is skipped) |
| `5 / 0`, `0 / 0` | BLANK | `inf`, `nan` |
| every aggregate over no rows | `0` | BLANK |
| `DISTINCTCOUNT` over a column with blanks | excluded the blank | counts it |
| `CALCULATE(m, PREVIOUSMONTH(...))` off the table | grand total | BLANK |
| `1 == 1` | BLANK (`==` unimplemented) | TRUE, and strict about blanks |
| `"" & (1/3)` | `0.3333333333333333` | `0.333333333333333` |
| `"" & DATE(2025,7,1)` | `2025-07-01 00:00:00` | `7/1/2025` |
| `FORMAT(2297200.9, "$#,##0,.0K")` | `$2,297,200.90K` | `$2,297.2K` |
| `FORMAT(1234.5, "#,##0")` | `1,234` (banker's) | `1,235` |
| `RANKX(..., Dense)` | ranked as SKIP | 6, not 6578 |

`PREVIOUS*` also anchors on the FIRST date of its input, not the last.

### New functions

`COUNTA`, `DISTINCTCOUNTNOBLANK`, `MEDIANX`, `MROUND`, `FIRSTNONBLANK`,
`LASTNONBLANK`, `ISINSCOPE`, `ERROR`. Four of the eight had been returning a
confident wrong value rather than BLANK: `LASTNONBLANK` as a CALCULATE filter
argument applied NO filter (grand total), `ISINSCOPE` in a `SWITCH(TRUE(), ...)`
took the fallback branch every time, `MEDIANX` fed a `>=` that always won.
`ISINSCOPE` answers FALSE and that is the faithful answer, not a stub — it asks
about the query's grouping axes, which a single-cell evaluation has none of, and
Desktop answers FALSE there too (it is NOT `ISFILTERED`).

### Robustness

- Non-finite results are serialized as `"Infinity"` / `"NaN"` strings. Python
  writes the bare literals `Infinity` and `NaN`, which are not valid JSON — one
  infinite cell would have failed the ENTIRE tool response for a strict client
  parser.
- The calculated-column evaluator now honours the same wall-clock budget
  measures do. It never armed the deadline, so a per-row `CALCULATE`/`FILTER`
  over 1.7M rows ran for over two hours at 4 GB with no output. It now refuses
  with a row count and points at `PBIX_DAX_MAX_SECONDS`.

### Found by comparing UNDER A FILTER CONTEXT, not just at the grand total

The grand total is one cell per measure, and the cell least likely to expose a
bug. A second sweep compares every measure against Desktop for several values
of a real dimension column — ~1,700 cells — and found these, none of which the
totals sweep could reach:

- **`ALL(Table)` now clears a filter that reaches the table THROUGH a
  relationship**, not just the direct `Table.col` keys. `CALCULATE(MAX(COVID[Date]),
  ALL(COVID))` went BLANK under a state slice matching no COVID row, where
  Desktop returns the global maximum. `ALLSELECTED(Table)` is now distinguished
  from it: it RESTORES the query context rather than clearing it.
- **A value-encoded Decimal column read 10,000x too large.** A fixed-decimal
  column is stored as its value times 10,000 whichever way it is encoded; the
  value-encoded branch skipped the scaling the dictionary branch applies. Proved
  without Desktop by decoding the same 397-row AdventureWorks Product table out
  of two corpus files: `Product[List Price]` came out 2.29–3578.27 from one and
  22900–35782700 from the other. This is a DATA bug — it affected every read of
  such a column, not only DAX.
- **A bare `[Column]` several tables share resolves against the measure's HOME
  TABLE**, which is what DAX does. Three tables own `ProductRevenue`, so
  refusing blanked all six MS_Revenue_Opportunities measures; `Revenue` now
  returns Desktop's 1,968,250,939 exactly.
- **`FIRSTNONBLANK` / `LASTNONBLANK` accept a table expression**, not only a
  column reference — five MS_Life_Expectancy measures pass `ALL(Years[Years])`.
- **A one-row, one-column table is a scalar.** `LASTDATE('Year'[Date])` leaked
  72 characters of Python repr into the measure's value.
- **`CONCATENATE` dropped a legitimate `0`** (`str(x or '')`), the same defect
  already fixed for `&`; it now shares one renderer with the operator.
- **Datetime precision.** The serial conversion rounded twice and landed on the
  wrong double for 27% of timestamps (54,550 of 200,000 random instants), and
  .NET's 100-ns ticks do not fit Python's 1-µs datetime at all — the decoder now
  carries the original stored serial on the value. MS_Perf_Analyzer's four
  millisecond columns went from mismatching to bit-identical.

### A reference that cannot be resolved is refused, not evaluated around

The plain aggregates parsed their argument with a pure regex, so
`SUM(T[No Such Column])` produced a syntactically valid reference,
`get_column_data` returned nothing, the aggregate came back BLANK, and `+`
folded that to 0 — the surrounding arithmetic carried on and produced a
confident wrong number. MS_Life_Expectancy has five measures that sum a column
the model does not contain: Desktop refuses all five, and this engine answered
**3,104,480** for `[Health]` and **222** for `[Health Expenditure]`.

Both paths now refuse — the aggregates validate the parsed reference, and a
qualified `Table[Name]` that is neither a column nor a measure raises rather
than degrading to BLANK. A CALCULATE predicate naming a missing column is
refused for the same reason; that behaviour had been pinned by a test that
described it as "NOT desirable", to be changed deliberately.

### Verification

547 measures across 24 files, each compared against Desktop's own answer with a
1e-9 relative band for floats. Separately, **397 calculated columns across the
whole corpus recompute bit-identically to the values Desktop itself stored** —
0 mismatches — with 8 deliberate refusals (PATH / PATHITEM / RANKX and
multi-column MAX−MIN need a table scan this engine does not implement) and 2
budget refusals. Bit-identical float agreement is not achievable
in principle — VertiPaq sums a column in parallel segments — and
`MS_Corporate_Spend`'s `[Var LE1]` is the worked example: the exact decimal
answer is 14,697,755.96505, this engine returns 14,697,755.965050012 (correctly
rounded) and Desktop returns 14,697,755.9650462, which is 300× further from the
truth. Measures whose definition reaches a `RAND()` through any chain of
references are excluded by name and counted separately; they can never pad the
score.

## [0.9.60] - 2026-07-30

Closes OpenBI findings **#9 #5b**, the highest-severity item in the newly audited
findings ledger: CALCULATE silently ignored every boolean filter argument except
`Table[Col] = value`.

### CALCULATE honours the whole predicate family

`<>`, `>`, `>=`, `<`, `<=` and `IN {...}` used to fall off the end of CALCULATE's
filter loop adding NO filter and NO warning, so the measure returned the
UNFILTERED total with `unsupported_functions` empty:

```
CALCULATE(SUM(P[V]), P[S] <> "Lead")   -> 650   should be 550
CALCULATE(SUM(P[V]), P[V] > 100)       -> 650   should be 500
CALCULATE(SUM(P[V]), P[S] IN {"Lead","Proposal"}) -> 650  should be 400
```

The filter context already evaluated structured predicates natively
(`make_value_matcher`), so CALCULATE now translates into that tested mechanism
rather than growing a second evaluator.

- `NOT` is folded into the operator, and `KEEPFILTERS` is peeled. Both were
  previously swallowed by the equality regex, which matched the WRAPPER text and
  registered a filter on a column named `NOT(P` or `KEEPFILTERS(P`.
- Several predicates on the SAME column now intersect, as DAX does. Two
  comparisons cannot share a flat spec (both need the `"op"` key), so
  `make_value_matcher` gained an `{"all": [...]}` conjunction.
- A predicate on a column the OUTER context already filters still REPLACES it --
  that override is the point of CALCULATE, and it must not be confused with the
  sibling-argument case.

**Desktop-verified on the corpus.** Five `Agents_Performance` measures that
returned BLANK now return exactly what Power BI Desktop's own engine returns:
`CF Table` -> "black", the three `Rank Filtering ...` measures -> 0,
`Employee Name` -> blank. Confirmed against the live msmdsrv, and checked with
`apply_default_filters` both ways so the match is not a slicer-default artifact.
201 corpus measures compared against 0.9.59: no regressions; the only other
movement is five RAND-based GeoSales measures, which differ run to run.

### `IN` as a general operator: implemented, deliberately not enabled

`_eval_in` / `_in_set_values` are implemented and unit-tested, including DAX's
BLANK rule (`BLANK() IN {1,2}` is FALSE) and a refusal to treat an unevaluable
table expression as an empty set. CALCULATE uses them.

They are NOT wired into the expression planner yet. Enabling `IN` in arbitrary
expressions made seven `Agents_Performance` measures return a CONFIDENTLY WRONG
value where they had returned BLANK -- 1 instead of Desktop's 0, "white" instead
of "black". The fault is not in `IN`: those measures wrap it around a RANKX/TOPN
chain over a parameter-table scalar that is independently inaccurate here, and
`IN` merely stopped masking it. A blank is a visible non-answer; a wrong number
is not. It stays off until that chain matches Desktop, and the reason is recorded
at the decision point in `_analyze_expr`.

As a consequence `FILTER(t, col IN {...})` remains unsupported (returns no rows)
— unchanged from 0.9.59, not a new limitation.

### Also

A table constructor `{1,2,3}` is now ONE argument to the expression splitter.
Braces did not nest, so `IN {"Lead","Proposal"}` was split at its comma into two
arguments.

## [0.9.59] - 2026-07-30

Closes the guard rail requested in OpenBI findings **#18**: re-pointing one
`DAXContext` across groupings returned the FIRST grouping's column members for
every later grouping — silently.

### `get_column_data`'s memo now follows the filter context

0.9.55 memoized `get_column_data` per `(table, column)`, valid only for the
filter set in force when it was populated. `filter_context` is a public writable
attribute, so this caller pattern — correct through 0.9.54 — went quietly wrong:

```python
for cat in ["Books", "Clothing", "Electronics"]:
    ctx.filter_context = {"Categories.CategoryName": [cat]}   # reuse + re-point
    print(cat, sorted(set(ctx.get_column_data("Products", "ProductName"))))
# 0.9.55-0.9.58: Books' products printed three times
```

`filter_context` is now a property whose setter clears the column memo, so
assignment is a supported way to re-point a context. No exception was raised and
the parent subtotals still reconciled, which is the combination that lets this
survive review — OpenBI hit it in a two-level matrix where only the child rows
were wrong.

`_measure_cache` is deliberately NOT cleared: its key already carries a
filter-context fingerprint, so its entries stay valid and re-pointing back keeps
the fast path. That asymmetry between the two memos was the actual defect.

**Not** covered, and now documented on the property: mutating the dict in place
(`ctx.filter_context[k] = v`) cannot be observed by any memo. Assign a new dict,
or use `with_filters()` / `without_filters()`.

The issue-#6 win is intact — the setter fires once per assignment, not per call.
Measured on `Agents_Performance.pbix`: 524-539 ms per measure with the guard
rail, 540-598 ms without.

## [0.9.58] - 2026-07-29

Two defects in `pbix_bind_field_parameter` (shipped in 0.9.57), both found by
adversarial probing of the shapes the happy-path tests did not cover.

### Rebinding a sorted well left a DANGLING OrderBy

`pbix_bind_field_parameter` drops the select it replaces. If a prior
`pbix_set_visual_sort` had targeted that field, the compiled query kept ordering
by a field it no longer selected:

```
sort  Y (Sales[Total Revenue]) desc
rebind Y -> Sales[Total Units]
  Select  = [Sales.Month, Sales.Total Units]
  OrderBy = Sales.Total Revenue        <-- not in Select
```

The rebind now re-points such an OrderBy at the newly bound field, preserving
the user's intent to sort by the value axis rather than silently dropping the
sort.

**Verified in Power BI Desktop**: a from-scratch file that sorts by Revenue desc
and then rebinds the well to Units opens clean and renders sorted DESC by
**Total Units** — Feb (30), Jan (20), Mar (10). The fixture's rows are chosen so
the two orders are opposites (Revenue desc would be Mar, Jan, Feb), which makes
the sort field unambiguous on screen.

### Binding to a visual with no field wells is now refused

Binding a field parameter to a `textbox` "succeeded", leaving the textbox
carrying a `query`, `dataTransforms` and a `Y` projection — structurally
incoherent, and `pbix_doctor` does not flag it. The tool now refuses for the
built-in types that provably have no data roles (`textbox`, `image`, `shape`,
`basicShape`, `actionButton`) and names the visual type in the message. The
check is deliberately narrow: an unrecognized type (any custom visual) is
assumed to have wells, so it still binds.

## [0.9.57] - 2026-07-29

Closes issue **#8** (from OpenBI service-verification findings #19): there was no
way to bind a field parameter into a visual programmatically, and the naive
attempt failed SILENTLY.

### The trap this closes

`pbix_datamodel_add_field_parameter` authors the model half correctly (re-verified
by the findings), but putting the parameter's display column straight into a
projection (`projections.Y = [{"queryRef": "Metric.Metric"}]`) makes Desktop treat
it as ordinary text and degrade the well to an implicit "Count of Metric" — six
equal bars, no field-swapping, no error.

### `pbix_bind_field_parameter` (127 tools)

`pbix_bind_field_parameter(alias, page_index, visual_index, role, parameter_name,
initial_field="")` authors the working shape — all five pieces, each diffed
piece-by-piece against a Desktop-authored binding and confirmed IDENTICAL:

1. `projections.<role>` holds the currently-RESOLVED field, with the matching
   `prototypeQuery.Select` entry carrying `NativeReferenceName`;
2. `queryFieldParametersByRole` on `singleVisual` carries the parameter linkage
   (index/length/display-column expr);
3. `columnProperties` restates the parameter's display label;
4. the compiled `query` joins the parameter table and gains a `Where` clause
   selecting the resolved field through the hidden `"<name> Fields"` column with
   the NAMEOF-style triple-quoted literal (`'''Sales''[Total Revenue]'`);
5. the resolved field's `dataTransforms` select carries `sourceFieldParameters`
   provenance.

`initial_field` accepts a display name or a `Table[Field]` ref, defaulting to the
parameter's first field. Rebinding replaces cleanly (no accreted selects or Where
clauses); unknown parameters and fields are refused with the valid options named.

**Verified in Power BI Desktop**: a from-scratch file (pbix_create + parameter +
chart + this tool) opens clean and renders "Revenue, Units and Profit by Month"
with all three parameter fields as series — the real field-parameter expansion,
not the degraded Count.

### The silent degradation now warns

`pbix_add_visual` and `pbix_update_visual_json` warn when a projection's queryRef
resolves to a field-parameter display column without `queryFieldParametersByRole`
— including when the binding compiler has already wrapped the bare column in the
implicit `CountNonNull(...)` aggregation, which is the degradation itself in
flight. The warning names this tool as the fix.

### Docs

The binding shape is documented in docs/rich-content.md (it appeared nowhere
before); tool counts updated to 127.

## [0.9.56] - 2026-07-29

Verification and sync release. No behaviour change: the diff against 0.9.55 is
documentation plus this version bump.

### Issue #5's fix is now confirmed against Power BI Desktop

When #5 shipped, its fix was verified structurally (run lengths summing to exactly
the row count), referentially (0 orphan relationships) and semantically (plausible
ranges) — but Desktop was in use at the time, so it was never checked against
Desktop's own engine, and `PROGRESS.md` said so. Both files have now been opened in
Power BI Desktop and its workspace engine queried over ADOMD.

`MS_Corporate_Spend` `Fact` — all 8 aggregates identical, including
`SUM(Value) = 4203674047.3179` to the last digit, and DISTINCTCOUNT of Value /
Department / Cost Element ID / Scenario ID = 35,807 / 410 / 240 / 5.

`MS_Employee_Hiring` `Employee` (1,290,259 rows) — all 9 identical.

Because equal aggregates can still hide unequal data, also checked at the VALUE
level on exactly the three columns the bug corrupted: `Gender` (C 590,639 ·
D 699,620), `FP` (F 631,127 · P 659,132), the six earliest `date` values with their
exact row counts, and `Age` (SUM 50,965,422 · AVG 39.5001484198134 · COUNT
1,290,259). Every distinct value and row count matches. `Gender` is the column that
summed to 93,629,586,803 rows before the fix.

### Sync

Tag, `main` and the published package now all point at the same tree — 0.9.55's tag
sat two documentation commits behind `main`. The F: working mirror was also audited
file-by-file against every tracked path (26 were stale, from earlier partial syncs)
and is now byte-identical.

## [0.9.55] - 2026-07-29

Closes issue **#6** (compiled DAX expression plans), and fixes **two operator-precedence
bugs** and **a paren-in-column-name bug** the new corpus-wide verification exposed —
plus three `pbix_doctor` false-positive classes, an invalid `ExpressionKind` written by
`set_incremental_refresh`, and ungated release workflows found by a repo audit.

### #6 — compiled expression plans

`_eval_expr` re-ran its whole dispatch chain — comment stripping, literal regexes,
operator splitting, branch checks — on every call, and iterators call it once per row
with the SAME text. The syntactic analysis is now done once per expression and cached
as a "plan"; evaluation code is unchanged. The calculated-column evaluator stops
substituting text per row entirely: column refs resolve through the engine's row
context and each mask (aggregate / CALCULATE / RELATED / LOOKUPVALUE) binds as an
engine VARIABLE, so the same text evaluates on every row and its plan is parsed once.
`get_column_data` is also cached per context (contexts are copy-on-modify).

Measured: the issue's own benchmark (`Agents_Performance`, 199,999 rows) went
1196→504 ms for one measure and 2373→981 ms for three; `MS_Employee_Hiring`
(5 calc columns × 1,290,259 rows) went 1053s→183s; `MS_Covid_Tracking` 332s.

Verified per the issue's protocol: every measure in all 24 corpus files was captured
before and after. **520 of 544 comparable measures are byte-identical**; the 24
differences are 5 RAND-volatile measures, 2 identical-failure path artifacts, and 17
measures changed BY the precedence fixes below — each classified individually, none
unexplained.

### Operator precedence was wrong in two ways (silent wrong values)

Top-level split order IS precedence, and the old chain split arithmetic before
comparison and before `&`, and `NOT` before `&&`. Every rule is now probed against
Power BI Desktop's own engine, loosest to tightest: `||` `&&` `NOT` comparisons `&`
`+ -` `* /`.

- `a - b < 0` parsed as `a - (b < 0)`: the blank comparison became 0, the condition
  collapsed to `a` — always truthy. `Employee[TenureDays]` (the absolute-difference
  idiom `IF(a-b<0, b-a, a-b)`) materialized SIGN-FLIPPED on 1,247,139 of 1,290,259
  rows. GeoSales' conditional-formatting measures (`IF([a]-[b]<0, red, blue)`)
  answered `#D64550` where Desktop answers `#118DFF` — end-to-end verified against
  the live Desktop engine.
- `NOT ISBLANK(a) && NOT ISBLANK(b)` — the standard quick-measure template — parsed
  as `NOT (ISBLANK(a) && NOT ISBLANK(b))`. Desktop: `NOT FALSE() && FALSE()` is FALSE.
- `"a" & 1 + 2` parsed as `("a" & 1) + 2`; Desktop says `"a3"`.

### Parentheses in column names read as BLANK (silent wrong values)

The column-reference branch demanded `'(' not in expr`, so a reference to
`Indicators[People using at least basic drinking water services (% of population)]`
never resolved and read as BLANK. Every bucket column on `MS_Life_Expectancy`
mis-bucketed thousands of rows. The regex is anchored at both ends, so when it
matches, any paren is inside the quoted table name or the bracketed column name —
both legal; the guard is gone.

**Corpus ground truth after all fixes: 395 calculated columns match Desktop's stored
values exactly, 0 logic mismatches** (4 remaining differences are the documented
sub-microsecond DateTime-serial class on MS_Perf_Analyzer's trace tables), 23/24
files rebuild, 0 fidelity findings.

### Found by adversarial review of the rewrite

- **Fixed before release:** a RELATED/CALCULATE mask nested inside a LOOKUPVALUE
  search value resolved against the PREVIOUS row's scope — every row silently
  materialized the previous row's lookup result, shifted by one. The scope is now
  installed before LOOKUPVALUE resolution; the reviewer's repro now yields the
  Desktop-correct chain.
- **Documented behavior changes** (all proven improvements or neutral, none with
  corpus coverage): a datetime reaching a TEXT context now renders as Python's
  `str()` (`2024-01-15 00:00:00`, no `T` — matching how measures already rendered
  it); NaN/Infinity doubles now propagate through arithmetic instead of silently
  collapsing to BLANK; string values containing newlines are preserved verbatim
  (the old text-splicing path corrupted them to spaces); and callers that bypass
  `calc_column_unsupported_reason` and evaluate iterator expressions directly get
  real DAX row-context shadowing instead of the old substituted-constant behavior.

### set_incremental_refresh wrote an invalid ExpressionKind

TOM's `ExpressionKind` enum defines a single member, `M = 0` — and every one of the
25 Desktop/Service-authored Expression rows across the corpus is Kind=0, parameter
queries included. The writer inserted **Kind=1**, which is precisely the out-of-range
enum `PFE_TM_ENUM_VALUES_VALIDATION_FAILED` rejects. Now Kind=0.

### Three pbix_doctor false-positive classes on working files

- **Expression/DataMashup consistency** flagged 9 of 24 corpus files (37.5%): V3
  enhanced-metadata files legitimately store shared M expressions with no DataMashup
  part. The check now tests the REAL invalid condition — an out-of-range
  `Expression.Kind`.
- **Table/storage consistency** matched storage by display name, but storage folder
  names are NOT the display name: special characters are space-sanitized
  (`fct_Orders` → `fct Orders`) and a renamed table keeps its creation-time folder
  (`PositiveYOY-NegativeYOY` lives in `Table (2542).tbl`). 10 of GeoSales' 14 tables
  were flagged storage-less on a file Desktop opens fine. Matching is by table ID now.
- **DAX measures** crashed with `NoneType has no len()` on Desktop's NULL-expression
  placeholder measures (same class as the 0.9.53 builder fix).

`pbix_doctor` now reports ALL CLEAN on GeoSales_Dashboard, MS_AI_Sample,
MS_Life_Expectancy, IT_Support and Agents_Performance.

### Release/CI hardening (repo audit)

- `release.yml` published to PyPI with NO gates — a tag cut from a red commit still
  shipped. A gate job (ruff + unit tests on windows-latest) now precedes build;
  `github-release` needs `publish-pypi` so a rejected PyPI publish no longer leaves a
  public GitHub release; `fail_on_unmatched_files: true` so an empty-asset release
  fails instead of shipping hollow.
- The CI mypy gate piped through `grep -c "error:" || true`, which counted a mypy
  CRASH as zero errors and passed green. Exit status is now checked separately.
- Actions bumped (checkout v7, setup-python v7, artifacts v7/v8). Python 3.14 was
  tried and rolled back: the xmhuffman dependency ships no cp314 wheel and its
  sdist build is broken, so the package cannot install on 3.14 until upstream
  publishes wheels (reason recorded in the CI matrix comment).
- `pbix_open`'s work dir was only second-granular, so two same-alias opens within one
  second (parallel processes) extracted into the SAME directory and silently read
  each other's models. The dir now carries pid + uuid.

### Docs

Tool count corrected to 126 (14 missing tools added to the README index), DAX
function count to 174 (four stale sites), stale test totals refreshed, and seven
stale "PBIR not supported" claims corrected across README, tool-contracts,
limitations and html-visuals — PBIR has been read AND write since 0.9.35/0.9.39.

## [0.9.54] - 2026-07-29

Closes issues **#4**, **#5** and **#7**. The theme is the same throughout: a wrong
value stored in VertiPaq is invisible — the file opens and every number is quietly
wrong — so anything this engine cannot reproduce EXACTLY is refused with a reason
rather than materialized with a guess.

Everything below was verified against **the values Power BI Desktop itself stored**.
The corpus files were authored in Desktop, so a calculated column's stored VertiPaq
values *are* Desktop's answer; recomputing from the DAX and diffing them is exact
ground truth over hundreds of thousands of rows. That method found four
silent-wrong-value bugs the entire unit suite missed.

### #5 — columns that failed to decode (`primary_segment_size` is capacity, not count)

`primary_segment_size` in an IDF segment is the **allocated capacity** of the primary
array, not the number of entries in use. It is always a power of two while the used
entries are far fewer; the slots past the end are sub-segment bytes that read as
nonsensical `(data_value, repeat)` pairs. Summing those garbage run lengths overshot
the row count — `Gender` on the 1,290,259-row `Employee` table summed to
**93,629,586,803 rows**. Decoding now stops once the segment's rows are complete, and
per-segment record counts are threaded through for multi-segment columns.

Also drops the `is_row_number` skip in `read_table_from_abf` in favour of the metadata
`Type == 3`: Desktop leaves that flag 0 on ordinary columns, so **126 of 1121 user
columns across the corpus were silently discarded on read** — including
`IT_Support` `dim_Date[Date]`, the column its relationships join on.

### #4 — calculated columns that blocked a rebuild

- **Auto date/time accessor** `X.[Date]` → `DATE(YEAR(X), MONTH(X), DAY(X))`, applied in
  both the gate and the evaluator so they cannot disagree. The other variation parts
  (`.[Year]`, `.[Month]`, …) map to locale-dependent display strings and are refused.
- **`LOOKUPVALUE`** — multi-column search, alternate result, self-lookup. String
  matching is case-insensitive (Power BI's default collation). Rows matching with
  DIFFERENT values are an error in DAX, so the column is refused rather than picking one.
- **`RELATED`** — many-to-one only, across ACTIVE relationships, when exactly one path
  exists. Two paths, no path, the wrong direction, an inactive relationship, or a bare
  `RELATED([Col])` are each refused.
- **`CALCULATE(<aggregate>, FILTER(<own table>, <predicate>))`**, including `EARLIER`.
  In a calculated column CALCULATE performs context transition and a FILTER over that
  same table replaces it, so this means "aggregate every row satisfying the predicate".
  The predicate is **compiled** — equality terms to a hash index, one inequality to a
  prefix aggregate over a sorted key — so `MS_Covid_Tracking`'s **1,740,185-row** table
  costs one lookup per row instead of a table scan.
- `VAR … RETURN [Col]` was refused as *"references another table 'RETURN'"* — the bare
  word before `[` was read as a table name, rejecting the most common modern DAX idiom.

**Corpus: 23 of 24 files now rebuild** (was 11 at the start of this work). The
one refusal is `MS_Perf_Analyzer`, whose `Events[Path]` uses `PATH` — a genuine
table scan this engine does not implement, refused deliberately rather than
guessed.

### Four silent-wrong-value bugs in the DAX engine

1. **`-` and `/` were right-associative.** `10 - 3 - 2` gave 9 instead of 5;
   `20 / 4 / 5` gave 25 instead of 1. Every measure and calculated column with a
   repeated subtraction or division was affected. Found because
   `Date[Rolling Period]` disagreed with Desktop on 1096 of 6209 rows.
2. **`&` rendered every FALSY value as empty** (`str(v or '')`), so the zero-padding
   idiom `RIGHT("0" & n, 2)` lost its pad on exactly the rows where n was 0 —
   `'P-00'` became `'P-0'`.
3. **A DATE is a number in DAX** (days since 1899-12-30), but `_as_number` returned
   None for one, so `[Timestamp] * 86400000` was BLANK on every row. `_as_datetime`
   also truncated fractional seconds, rounding timestamps to the whole second.
4. **FORMAT** ignored leading-zero pictures (`FORMAT(1, "000")` → `"1"`, which changes
   sort order, not just display) and recognised only lower-case date tokens, so
   `FORMAT(d, "YYYY-MM-DD")` returned the literal `"YYYY-01-DD"`.

Plus: an unresolved column reference inside a LOOKUPVALUE search value or a FILTER
right-hand side reads as blank and matches nothing, materializing BLANK instead of
failing — `Events[ParentIndex]` was blank on 102 of 117 rows where Desktop had a value.
Both paths now refuse.

### #7 — calculation groups, translations and detail-rows survive a rebuild

No corpus file exercises any of this, so the path had never run. Authoring a
calculation group with this project's own `pbix_datamodel_add_calculation_group` and
then editing a table dropped `CalculationGroup`, `CalculationItem`,
`[Table].CalculationGroupID` and the Type=7 partition **to zero** — with
`success: true` and an empty warnings list.

All 14 tables from the issue are now carried: `QueryGroup`, `CalculationGroup`,
`CalculationItem`, `CalculationExpression`, `Set`, `PerspectiveSet`,
`ObjectTranslation`, `DetailRowsDefinition`, `AlternateOf`, `RefreshPolicy`,
`Calendar`, `TimeUnitColumnAssociation`, `CalendarColumnReference`,
`AnalyticsAIMetadata`.

A calculation group is wired from **both** ends, so the `[Table].CalculationGroupID`
back-reference and the `Type=7` partition are restored too — and that partition must
carry **no `QueryDefinition`**, or Power BI rejects the whole file on open. Every
metadata-level check passed (referential integrity clean, no dangling keys, doctor
reporting nothing new) and the file still would not load; only opening it in Desktop
surfaced it. Verified against Desktop's own engine: `INFO.CALCULATIONGROUPS()` and
`INFO.CALCULATIONITEMS()` answer 1 group and 2 items after a rebuild.

### Performance

Two changes to calculated-column evaluation, both value-identical (checked against
Desktop's stored values, 90 columns, 0 mismatches):

- ISO-date string coercion is cached — dates reach the evaluator as ISO strings, so
  a wide table re-parsed the same handful of dates millions of times (~12x on that
  path).
- A plain row expression is a pure function of the columns it names, so results are
  memoized on the tuple of those values. `IF([Age]<30,1,IF([Age]<50,2,3))` over
  1,290,259 rows has about 80 distinct ages; the evaluator had been re-parsing the
  substituted expression text every single row. Not applied when a CALCULATE,
  RELATED or LOOKUPVALUE mask is present, since those read columns that need not
  appear in the masked text.

`MS_Employee_Hiring` (5 calculated columns over 1,290,259 rows) went from over an
hour — it never completed — to **1053s**. `MS_Human_Resources` 1046s,
`MS_Life_Expectancy` 415s, `MS_Store_Sales` 222s; the rest of the corpus is seconds.
Per-row evaluation is still the bottleneck for wide tables, which is issue #6.

### Known limitation

DateTime columns decode to a Python `datetime` (microsecond resolution) while VertiPaq
stores a double serial, so scaling a timestamp to milliseconds can differ from
Desktop in the sub-microsecond digits.

## [0.9.53] - 2026-07-28

**Two ways an ordinary text value was silently destroyed**, both found by an adversarial review of the calculated-column path and both confirmed by three independent attempts to refute them.

### A double quote in the data deleted the whole value

DAX escapes a quote by **doubling** it, so `6" pipe` is written `"6"" pipe"` — four quote characters. The literal parser required exactly two, so any such value fell through the string branch and came back **BLANK**:

```
Sales[Product] = ['6" pipe', 'plain']
Label = Sales[Product] & " x"

before:  [' x',        'plain x']     <- the entire value vanished
after:   ['6" pipe x', 'plain x']
```

Nothing surfaced it: the reference resolved, so the unresolved-reference check was silent, and other rows were non-blank, so the all-blank net never fired. Any inch mark, dimension (`10" x 4"`) or quoted phrase in a text column was affected.

### `//` or `--` inside a string truncated the expression

Comments were stripped with a plain regex over the raw text, with no awareness of string literals. A URL, an ISO range, or a double dash in prose therefore deleted the rest of the line — **including whole column references**:

```
'T'[A] & "https://x" & 'T'[B]
before:  ['x', 'p']            <- the & 'T'[B] term was deleted outright
after:   ['xhttps://xy', 'phttps://xq']
```

The unresolved-reference check was structurally unable to catch this, because it only ever sees the text *after* stripping — by which point the reference is already gone. Comment removal is now literal-aware, and handles `/* */` blocks, which were not stripped at all.

### A measure with no expression failed the whole edit

Power BI Desktop writes `Expression NULL` for a measure you create and never fill in — `MS_Life_Expectancy.pbix` has two, literally named `Measure` and `Measure 2`. The pre-build validator read it with `m.get("expression", "")`, whose default applies only when the **key is absent**, so a present-but-`None` value reached `.upper()` and failed the edit with a bare:

```
'NoneType' object has no attribute 'upper'
```

No file name, no measure name, nothing to act on. The builder's INSERT also subscripted the key directly, raising `KeyError` when a caller simply omitted it. Both now tolerate a missing or null expression, and the `NULL` is written through unchanged — which is what the source model had.

Like the `MS_Employee_Hiring` multi-segment decode failure, this is a **pre-existing defect that became reachable** once the file's calculated columns stopped being refused, not a new one.

### `pip install pbix-mcp` was broken by mcp 2.0

The dependency was declared as an unbounded `mcp>=1.0.0`. **mcp 2.0.0 removed `mcp.server.fastmcp` entirely** — the server API is now `mcp.server.mcpserver` — so every fresh install after that release resolved to a version this package cannot import:

```
ModuleNotFoundError: No module named 'mcp.server.fastmcp'
```

This is what turned CI red across all eight matrix cells. Bounded to `mcp>=1.0.0,<2`, which states the truth: the code is written against the 1.x FastMCP API. Overriding the bound now raises a message naming the package and the fix rather than a bare `ModuleNotFoundError`. Porting to the 2.x API is separate work.

### The narrowed aggregate gate, continued

`MIN`/`MAX` also have a **scalar** overload, `MIN(<expr1>, <expr2>)`. It is now allowed when neither argument reads a column — that is just arithmetic over literals and VARs, and is how Power BI's own binning clamps a bin number (`MIN(__BinNumber, __Count - 1)`, which appears in three columns of `MS_Life_Expectancy.pbix`). With a column reference inside it, it stays refused: that is the form that returned 0 on every row.

## [0.9.52] - 2026-07-28

**A blank compared against a number took the wrong branch, 11% of every model's data columns were silently dropped when read, and the calculated-column gate now allows plain aggregates.** All three found by comparing our output against Power BI Desktop's own engine over the corpus.

### BLANK did not compare like DAX, so bucketing columns took the wrong branch

DAX does not propagate BLANK through a comparison — it coerces it to the **zero of the other operand's type** and compares that. We returned BLANK, so every test against a blank was neither true nor false and fell through to the ELSE branch.

On the bucketing shape that Power BI's own binning feature and countless hand-written columns use:

```
IF(T[x] < 30, 20, IF(T[x] < 45, 30, IF(T[x] < 60, 45, 80)))
```

every blank row scored **80** where Desktop scores it **20**. A plausible number, never an error, materialized into VertiPaq and inherited by every downstream answer.

Measured on `test_corpus/MS_Life_Expectancy.pbix`: `Indicators[Basic drinking water services (% of population)]` disagreed with Desktop on **70,562 of 72,645 rows**. Four columns in that file were affected. After the fix all of them match Desktop exactly.

The full rule, every line read live from Desktop's engine:

```
BLANK() < 50           TRUE     BLANK() = 0                  TRUE
BLANK() >= 50          FALSE    BLANK() = ""                 TRUE
BLANK() <> ""          FALSE    BLANK() = FALSE()            TRUE
BLANK() < "a"          TRUE     BLANK() = DATE(1899,12,30)   TRUE
BLANK() = BLANK()      TRUE     BLANK() < DATE(2024,1,1)     TRUE
```

Arithmetic is untouched — only comparisons coerce, so `BLANK() + 5` is still 5 and `BLANK() * 5` is still 0.

### A tenth of every model's columns vanished on read, with no error

`read_table_from_abf` skipped any column whose IDFMETA reported `is_row_number`. That flag is inferred from a uint64 our own encoder writes as 1 for a data column — but **Power BI Desktop leaves it 0 on plenty of ordinary columns**, so it does not mean "this is a RowNumber". A skipped column was stored as `None` and then filtered out at the end of the function, so it disappeared without an error, a warning, or a trace.

Measured across the 24-file corpus: **126 of 1121 user data columns (11.2%)** were discarded. A rebuild-path edit then wrote the table without them.

```
IT_Support.pbix   dim_Date[Date]                  <- the column its relationships JOIN on
                  dim_Clusters[Cluster_ID]
                  fact_IT_Support[Similarity_Score]
```

`IT_Support.pbix` is a file every existing check reported as clean, including `verify_rebuild_fidelity.py` at 0 findings.

The RowNumber column is now identified from the model metadata (AMO `Column.Type == 3`, or the reserved name), which is authoritative and involves no guessing. After the fix, **0 declared data columns are missing anywhere in the corpus**. Recovered values were checked against Power BI Desktop's own engine — `dim_Date` gives 521 rows, 521 distinct dates, 2024-01-01 through 2025-06-04, matching exactly.

The same flag was also consulted in `model_reader.py` to choose which column to read a table's row count from; when it misfired on every column of a table, the table reported 0 rows.

### Plain aggregates over the table's own columns are no longer refused

A calculated column has no filter context beyond its own row, so `MIN('Date'[Year])` really is the minimum of the whole column, on every row. These were refused wholesale, which blocked real files whose only obstacle was an expression of the shape:

```
Date[MonthIncrementNumber] = ([Year] - MIN([Year])) * 12 + [MonthNumber]
```

Only **`MIN`, `MAX`, `SUM` and `AVERAGE`** are allowed, only **with a single argument that is one column of the target table**, and only when no filter-context function appears anywhere in the expression — inside `CALCULATE` the same call means something else.

The narrowness is deliberate. An adversarial review of the first, broader cut found six ways it would have written wrong values, each demonstrated end to end, and each is now refused:

| shape | what it did |
|---|---|
| `MIN(T[Amount], 0)` | DAX's **two-argument scalar** overload, not an aggregate — masking hid its column reference and the engine returned 0 on every row |
| `COUNTROWS(Other)` | a bare **table** name is invisible to the cross-table check, which only sees `Name[` — returned 0 on every row |
| `MIN([Yeer])` | a typo'd column answers 0 rather than failing, so the check now requires the column to exist |
| `DISTINCTCOUNT`, `COUNT`, `COUNTA`, `COUNTBLANK` | our implementations disagree with DAX about how BLANK and `""` are counted — off by one |
| `STDEV.P`, `VAR.S`, `PERCENTILEX.INC` | DAX spells these with a **dot**; listing only the underscore spellings left the real names refused by nothing |
| `[Total Count (n)]` | a column *named* after an aggregate had its name chopped in half by the scanner |

`IF(T[D] = MIN(T[D]), 1, 0)` — the standard "earliest record" flag — also scored 0 on every row: row substitution writes a DateTime as a quoted ISO string while the aggregate returns a native datetime, so the two never compared equal. Comparisons now match a date against its ISO text.

**This required more than relaxing the gate.** The per-row evaluator substitutes every reference to the target table's columns with *that row's* literal value. Applied naively to `MIN('Date'[Year])` that yields `MIN(2015)` — the current row's year rather than the column minimum, a different wrong answer on every row, written into VertiPaq with nothing reporting it. Aggregate calls are now masked out before substitution and restored afterwards.

Verified against Power BI Desktop on `MS_Employee_Hiring.pbix`: `MIN(Year)` is 2010, so 2015-11 is 71 and 2016-11 is 83. All 13 sampled `(Year, MonthNumber)` pairs match exactly. Collapsing the aggregate to the row would have given 11 instead of 71.

The masker searches a copy of the expression with string literals blanked out, so an aggregate name **inside a string** is never mistaken for a call. Without that, `IF(T[Cat] = "MIN(", T[A], T[B])` masked from the `"MIN("` inside the literal to the end of the expression, hiding `T[A]` and `T[B]` from both the substitution and the unresolved-reference check — they then evaluated to blank, silently.

## [0.9.51] - 2026-07-28

**`INT` was off by one for every negative number, and DATEDIFF / WEEKNUM / ROUNDDOWN / ROUNDUP now exist.** Progress on issue #4.

Every value in this release was checked against Power BI Desktop's own Analysis Services engine, queried directly over ADOMD while Desktop had the file open — not against documentation and not against our own expectations.

### `INT` truncated instead of flooring

```
INT(-1.5)      Desktop -2      we returned -1
INT(-0.1)      Desktop -1      we returned  0
```

DAX `INT` rounds toward **negative infinity**; `TRUNC` and `ROUNDDOWN` round toward **zero**. They differ only below zero, and Python's `int()` does the wrong one. This never raised — it returned a plausible number one step off.

It mattered most in the binning idiom Power BI's own "New group" feature generates, `INT(x / 5) * 5`, where being off by one puts the row in the **wrong bin**:

```
INT(-1612/5)*5    Desktop -1615     we returned -1610
```

`test_corpus/MS_Regional_Sales.pbix` has exactly this shape in `Opportunities[Days Remaining In Pipeline (bins)]`, over a column that is negative for every past date.

### DATEDIFF

Counts interval **boundaries crossed**, not elapsed time — `DATEDIFF(DATE(2023,12,31), DATE(2024,1,1), YEAR)` is 1 despite the dates being a day apart. The obvious implementation (floor the elapsed difference) disagrees with Desktop on **five of ten** real rows from `Opportunities[Weeks Open]`, so it would have shipped silently wrong.

`WEEK` was the subtle one: a DAX week starts on **Sunday**. Verified against twelve rows read live from Desktop's engine. `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE` and `SECOND` are all boundary counts; reversed arguments give a negative rather than an error.

### WEEKNUM

Not ISO. The week containing January 1 is always week 1, where `date.isocalendar()` puts 2021-01-01 in week 53 of the previous year. Return types 1 (Sunday-start, the default) and 2 (Monday-start) both verified against Desktop.

### ROUNDDOWN / ROUNDUP

Both round relative to zero, so `ROUNDDOWN(-1.5, 0)` is -1 where `INT(-1.5)` is -2.

### A refusal heuristic that blocked a whole file

A calculated column whose every row evaluated to blank was refused outright, as a tell-tale of a reference that silently failed to resolve. Reasonable, but it has a false positive: a column whose expression is literally `BLANK()` is legitimately blank on every row, and one such column in `MS_Regional_Sales.pbix` blocked every edit to that file.

The real condition is now detected directly instead of guessed at. After the per-row substitution, any column reference still present in the expression belongs to another table or to nothing — the engine reads it as blank, which is exactly how a wrong value gets materialized. Those are named in the refusal:

```
before:  every row evaluated to blank — the expression likely references
         a column or name that doesn't resolve in this engine
now:     references a column this engine cannot resolve in row context: [Date]
```

The all-blank guard is kept for expressions that do read a column, where it is still the right signal.

## [0.9.50] - 2026-07-28

**Power BI Desktop refused to open a rebuilt file for four more reasons. All four are fixed and all three schema eras now open and answer queries in Desktop.**

Issue #3 asked for the rebuild path to be verified against real Power BI Desktop on Windows, because the service only returns opaque numeric codes. Doing that found four defects that no offline check caught — our own reader parsed every one of these files happily.

Verified by opening each rebuilt file in Desktop and querying the workspace Analysis Services engine it starts, so the proof is the engine returning rows rather than a window appearing:

| source | level | result |
|---|---|---|
| `MS_Blog_DataProfiling.pbix` | 1455 | 91 Customers, 830 Orders, 3 Numbers (the edit) |
| `Agents_Performance.pbix` | 1550 | 199,999 FactSales, 293 DimEmployee, 306 DimStore |
| `GeoSales_Dashboard.pbix` | 1601 | 9,994 fct_Orders, 296 Returns, 793 dim_Customer |

Each is compared against a CONTROL (open and save, no edit) rather than the original, so nothing the save path does on its own is blamed on the rebuild. The control is byte-clean on every metadata table.

### The metadata file was sized from a stale record, so AS truncated it

`splice_metadata_in_abf` updated the VirtualDirectory's size for `metadata.sqlitedb` but not the BackupLog's, and **Analysis Services sizes the file from the BackupLog**. The result was a truncated SQLite image and:

```
The database disk image is malformed. SQLite Error Code=11
```

Two separate causes: a `len(old) == len(new)` guard skipped any size whose digit count changed, and the two regions have **different encodings in the same file** — a Desktop-authored ABF stores the VirtualDirectory as UTF-8 but the BackupLog as UTF-16-LE, so searching with one encoding never found the other. The BackupLog is also identified as the last VirtualDirectory entry rather than by offset, because its UTF-16 BOM puts the recorded offset two bytes below where the tag is found.

### Legacy `Query` partitions were rewritten to inline M, orphaning the DataMashup

The builder always emits a `Type=4` (M) partition with the rows inline. A model authored by an older Desktop uses `Type=1` (Query), whose `QueryDefinition` names a query in the DataMashup — which the rebuild left byte-identical. Rewriting the partition orphaned that query, and Desktop opened the file with an **empty Data pane** and *"There are pending changes in your queries that haven't been applied"*. Affects 3 of 24 corpus files.

### Two columns claimed to be the key, and Desktop rejected the whole model

```
The table 'DateAutoTemplate' has two columns with the IsKey property set to True.
```

The builder marks every RowNumber column `IsKey`, which is correct for a table it authored but collides with a table the author keyed on a real column. Corpus ground truth across 248 RowNumber columns in 24 Desktop-authored files: `IsKey` is 0 for exactly the 14 whose table has another key column and 1 for the other 234 — never two per table. Not confined to auto-date system tables; Ecommerce `dimDate`, IT_Support `dim_Date` and three MS_Store_Sales tables are ordinary user tables. 9 of 24 corpus files were affected.

### Calculated-table columns lost their table qualifier

```
Relationship 'ed5f222c-…' points to deleted column 'Date' in table 'Date'.
```

A calculated table built on another table qualifies its columns — every per-table auto-date table reads `DateAutoTemplate[Year]` because it is a copy of that template. The stamper synthesised a bare `[Year]` instead, which does not resolve, so the engine treated the column as deleted and refused the model. The model's own `SourceColumn` is now carried through.

### Measures and hierarchies kept only their name and expression

The builder wrote defaults for everything else. On the 102-measure `Agents_Performance.pbix` that meant **89 measures losing the DisplayFolder they were organised into**, hidden helper measures such as `Date[_ShowValueForDates]` becoming visible, and 18 measures silently retyped (Boolean and Int64 both landing on Double). Hierarchies lost `IsHidden` and `HideMembers`; measures, hierarchies and levels lost the `LineageTag` that TMDL/PBIP round-trips and service lineage match on.

`DisplayFolder`, `IsHidden`, `Description`, `FormatString`, `DataType`, `DataCategory`, `IsSimpleMeasure` and `LineageTag` are now snapshotted and restored by name alongside the table and column properties that already were. `Hierarchy.State` remains the one intentional difference: the rebuild does not materialize the hierarchy index, so it declares `CalculationNeeded` rather than claiming data it does not have.

### Windows is now a first-class CI target

POSIX unlinks a file that still has an open handle; Windows raises `WinError 32`. Three unclosed SQLite handles made **every calculated-column and calculated-table edit fail on Windows** — the platform nearly every Power BI user is on — while CI stayed green because it only ran Ubuntu. Fixed at nine sites, and `windows-latest` is now in the CI matrix across all four Python versions so this class of bug cannot recur silently.

## [0.9.49] - 2026-07-28

**The rebuild replaced the model's encryption key while keeping data encrypted under the old one.**

The 0.9.47 artifact was rejected by the service with a **different** error from the three before it:

```
before:  An error occurred when loading … .db.xml     code -1055653859
now:     Failed to decrypt sensitive data. Possibly
         the encryption key does not match             code -1054474227
```

`build_abf_clean` writes a **hardcoded 144-byte `0.CryptKey.bin`**. That was harmless while a rebuild started from a blank schema, because the result contained no encrypted values. 0.9.47 began preserving the source's metadata — including `DataSource` rows whose `ConnectionString` is a ~4 KB blob encrypted under **the source's** key — so the model then carried ciphertext it no longer had the key for.

```
source CryptKey    8c6bb6ca…   (its own)
rebuilt CryptKey   3331be84…   = the builder's constant
DataSource rows    3 × ~4 KB encrypted ConnectionString, carried verbatim
```

**24 of 24 corpus files have a CryptKey different from the builder's constant**, so this affected every model with a data source, not just the one tested.

The source's `0.CryptKey.bin` is now carried through the rebuild, exactly as `db.xml` and the metadata schema already were. A newly created file still gets the generated key, since it has nothing encrypted.

### Added — eighth dimension in `scripts/verify_rebuild_fidelity.py`
Encryption-key identity. Every failure mode found becomes a standing check; this one would have been caught before the upload.

Across all 24 corpus reports: **11 edits accepted with 0 findings on all eight dimensions, 13 refused with accurate reasons.**

## [0.9.48] - 2026-07-28

### Added — `pbix_doctor` now reports what an edit would cost, before you make it
A rebuild-path edit reports what it could not carry across **afterwards**. The new **Rebuild-path cost** check says so **beforehand**, which is when the choice is still available:

```
✅ Rebuild-path eligibility: rebuild-path edits supported (6 calculated object(s) preserved)
✅ Rebuild-path cost: a rebuild-path edit re-creates by name: annotations (106),
   perspectives (6), dynamic format strings (6), shared M expressions (1), Q&A …
```

### Investigated — the `MS_Corporate_Spend.pbix` decode gap
Four `Fact` columns still fail to decode (contained since 0.9.45: a 1.3 s refusal instead of a 3.7 GB allocation). The leading hypothesis was that `primary_segment_size` is a **byte** count rather than an **entry** count — the observed values are all powers of two, and the RowNumber column's run lengths sum to exactly the row count under that reading.

Tested across **1,986 columns** in the corpus: the entry-count reading matches the declared row count for **1,881**, the byte-count reading for only **1,386**. **The hypothesis is wrong** and the current interpretation is correct; those four columns are a different gap. Recording this so the next attempt does not repeat it.

## [0.9.47] - 2026-07-28

**Root cause of the service rejecting rebuilt models: the rebuild imposed its own metadata schema on every file.**

Three files built with 0.9.45 and 0.9.46 were rejected by the Power BI service with `Failed to PublishAbf database … An error occurred when loading … .db.xml` (error `-1055653859`). Each time, a field-by-field metadata comparison had reported no differences — and each time that comparison covered a different, insufficient subset of what actually matters.

### The cause
`PBIXBuilder` created a **blank metadata database with a fixed 63-table schema** and filled it in. Measured across the corpus:

| source | metadata tables |
|---|---|
| `MS_Blog_DataProfiling.pbix` (level 1455) | **51** |
| the builder's blank schema | **63** |
| `GeoSales_Dashboard.pbix` (level 1601) | **65** |

**20 of the 24 corpus models are an older era than the builder emits.** Rebuilding one invented tables (`Calendar`, `QueryGroup`, `ChangedProperty`, …) and columns (`LineageTag`, `ExpressionContext`, `DirectLakeBehavior`, …) that its compatibility level never had, and Analysis Services refuses to load such a database.

This was masked for a long time because **every file ever verified in the service was built from `GeoSales_Dashboard.pbix`, which is *newer* than the builder** — there we dropped two tables rather than inventing any, which the service tolerates. Lifting the auto date/time ceiling in 0.9.44 is what first let an older-era file reach this code; the defect long predates it.

### The fix
A rebuild now starts from **the source model's own `metadata.sqlitedb`**, clearing the rows it rewrites and keeping the schema. Consequences:

- the schema era is preserved exactly — nothing is invented
- the ABF's `db.xml` is carried through (only the database GUID is rewritten), so `CompatibilityLevel` and `DbUniqueId` stay the source's own instead of a hardcoded `1550`
- INSERT and UPDATE statements naming columns an older era lacks are **narrowed to that era's columns** rather than failing or adding them
- `Culture.LinguisticMetadataID` and `Model.DefaultMeasureID` are re-pointed at the rows the carry-over re-creates, instead of dangling

### Added — `scripts/verify_rebuild_fidelity.py`
The check that should have existed from the start. It compares a rebuild-path edit against **the same file's own open+save control** on all seven dimensions that have actually been shown to matter: schema era, compatibility level, referential integrity, authoring properties, storage consistency, DAX binding, and auto date/time shape.

Across all 24 corpus reports: **11 edits accepted with 0 findings on every dimension, 13 refused with accurate reasons, 0 with any fidelity finding.** `MS_Blog_DataProfiling.pbix` — the file the service rejected three times — is among the clean ones.

### Not yet confirmed in the service
Every claim above is offline evidence. The rebuilt model has not been loaded by Analysis Services, because uploading is not something this toolchain can do. That confirmation is still outstanding.

## [0.9.46] - 2026-07-28

**Reverts the one change that altered the schema Analysis Services parses.**

A file built with 0.9.45 was rejected by the Power BI service on upload:

```
Failed to PublishAbf database '…': An error occurred when loading … .db.xml
Power BI Semantic Model Error Code -1055653859
```

Structural verification had passed it at 0 differences, so the defect is something a field-by-field metadata comparison cannot see. Ruled out by inspection: the ABF storage inventory (identical entry sets, same segment counts), foreign-key integrity (every reference resolves), and DAX binding (every calculated-table and calculated-column expression resolves to a real table and column). Everything the carry-over inserts binds by **name** — `LinguisticMetadata` uses `"ConceptualEntity": "fct_Orders"` — with no IDs from the old model embedded anywhere.

Building the same edit on the same file with **v0.9.41** — the last release confirmed loading in the service — and diffing showed exactly one structural change beyond added rows: 0.9.44 added `CREATE TABLE [Function]` to the builder's schema, taking the metadata database from **63 tables to 64**.

Adding rows to a table Desktop already writes is a different risk class from adding a table Desktop's own files do not contain. That change shipped in the version whose output was rejected, and its benefit was one row in one corpus file. It is reverted. User-defined DAX functions are now **reported as uncarryable** rather than carried, which is the behaviour every release before 0.9.44 had.

This is a containment measure, not a confirmed root cause — the service is the only thing that can confirm it, and this release is what makes that test possible.

## [0.9.45] - 2026-07-28

**Every rebuild-path edit was quietly reformatting the model. Found by building a verification file for 0.9.44 and checking the bytes I was about to hand over.**

0.9.44 claimed the auto date/time tables came through "identical to Desktop". That claim was true only of the fields the check happened to compare — `Type`, `ExplicitDataType`, `InferredDataType`, `SourceColumn`, `SystemFlags`, `IsAvailableInMDX`, `Expression`. It never looked at `IsHidden` or `SummarizeBy`, and both were wrong.

### Fixed — authoring properties are now carried across a rebuild
These live as **properties on rows the builder does create**, so the 0.9.43 row-level carry-over never covered them; the builder simply wrote its defaults over them.

| Property | What it cost |
|---|---|
| `Table.IsHidden`, `Column.IsHidden` | Every hidden table and column became **visible**. All four auto date/time tables appeared in the field list as `LocalDateTable_<guid>`. |
| `Column.SummarizeBy` | `Year`, `MonthNo`, `QuarterNo`, `Day` went from `None` to **`Sum`** — dragging `Year` into a visual would add the years together. |
| `Column.FormatString` | A currency, percentage or date column lost its formatting. |
| `Column.SortByColumnID` | "Month sorted by MonthNo" reverted to **alphabetical**. |
| `Column.DataCategory` | An `ImageUrl` or `WebUrl` column stopped rendering as an image or link. |
| `Column.DisplayOrdinal` | Field order in the model changed. |
| `Table.ShowAsVariationsOnly`, `Table.IsPrivate` | Auto date/time tables surfaced where Desktop hides them. |

Sort-by is carried **by name**, not by ID: the rebuild renumbers every primary key, so the stored number would have pointed at an arbitrary column. Storage and type fields (`ColumnStorageID`, `InferredDataType`, `IsAvailableInMDX`, the `*ModifiedTime` stamps) are deliberately **not** carried — they describe how the rebuilt data is physically stored and must take their new values.

### Verified
Full field-by-field diff of `[Table]` and `[Column]` between an open+save control and a rebuild-path edit: **no property is reset**, where before the fix 8 column fields and 3 table fields were. The five new tests fail 5/5 against 0.9.44.

## [0.9.44] - 2026-07-28

**The auto date/time ceiling is lifted, calculated columns can be removed, and DAX evaluation is twice as fast.**

### The auto date/time ceiling
A calculated table that *also* owns calculated columns was refused outright. Every Power BI auto date/time table is exactly that shape — a `Date` column from the partition's `CALENDAR` expression plus `Year`/`MonthNo`/`Month`/`QuarterNo`/`Quarter`/`Day` computed on top — so any report with auto date/time on could not take a rebuild-path edit. It accounted for **16 of the 18 corpus refusals**.

The cause was one stamper overwriting the other. `_apply_calculated_table_metadata` rewrote *every* non-RowNumber column to `Type = 4, ExplicitName = NULL`, which destroyed the calculated columns **and** nulled the `ExplicitName` that the calc-column stamper looks a column up by, so the second stamp then matched nothing. The table stamper is now told which columns belong to the calc-column stamp and leaves them alone, and calculated columns on a calculated table carry `SystemFlags = 2`, matching Desktop.

Verified field-for-field against each file's own open+save control, across `Type`, `ExplicitDataType`, `InferredDataType`, `SourceColumn`, `SystemFlags`, `IsAvailableInMDX` and `Expression`, plus table and partition flags.

Across the 24-report corpus: **11 edits accepted (was 6), 0 with any difference across the fields compared, 13 refused (was 18)** — but see 0.9.45: that comparison omitted `IsHidden` and `SummarizeBy`, both of which were in fact wrong — and **none** of the remaining refusals is the auto date/time shape. They now name genuinely different causes: calculated columns using `CALCULATE`, `RELATED` or `DATEDIFF`, or referencing another table, which this engine does not reproduce.

### Fixed — every auto-date `Date` column was being retyped to text
Found while verifying the above. The generating `CALENDAR` expression hands dates back as ISO **strings**, and the rebuild inferred each column's type from the regenerated values, so `Date` came back `InferredDataType = 2` (String) instead of `9` (DateTime). The table looked intact while its date semantics were gone. Column types are now taken from what the model already declares, falling back to inference only for genuinely new columns.

### Added — `pbix_datamodel_remove_calculated_column` (126 tools)
The inverse of `pbix_datamodel_add_calculated_column`, so authoring a calculated column can be undone. Drops both halves — the `Type = 2` metadata carrying the DAX and the materialized values in VertiPaq — and re-evaluates every remaining calculated column and calculated table.

It refuses, rather than corrupts, in two cases: the target is not a calculated column (naming `pbix_set_table_data` as the right tool for a data column), or another calculated column on the same table reads it (naming the dependents). Dependency detection matches all three reference forms Power BI writes — `[Col]`, `Table[Col]`, `'Table'[Col]` — case-insensitively.

### Performance — DAX evaluation is 1.92x faster on a real 200k-row model
`_split_operators`, the expression scanner, is a pure function of `(expression, operator)` but runs inside every row iteration. Profiling one `RANKX`/`FILTER` measure on `Agents_Performance.pbix` (`FactSales` = 199,999 rows) showed **95,586 calls, 48% of total runtime** — the cost was not scanning the fact table, it was re-parsing the same DAX text once per row.

Memoizing it gives a **99.9% hit rate from only 300 distinct parses**, and a controlled A/B in one process (interleaved arms, median of three) measures **1.92x — 48% less wall clock**, matching the profile exactly.

This answers the attribution question in handover 17: the cliff is **engine-side**. A single `evaluate_dax` call over 199,999 rows costs seconds with the model already decoded and cached, so no amount of client-side batching can rescue it. The remaining cost is the expression interpreter walking rows under filter context; that is a larger change and is not attempted here.

### Fixed — an unbounded VertiPaq decode
Lifting the ceiling let edits reach a column the refusal used to mask. Its RLE run length decoded as billions of rows and the decoder expanded it literally: **3.7 GB of RSS and no end after ten minutes, on a 0.8 MB file**. Every count in an `.idf` segment header comes straight off the wire, so a slightly wrong offset reads as an astronomical number. Those counts are now bounded by what the buffer can physically hold, and **raise rather than truncate** — the caller already knows how to refuse an edit whose rows it cannot decode, whereas quietly short data would rebuild the table with silently wrong rows.

The same file now refuses in **1.3 seconds**, naming the table and the columns.

### Also fixed
- **`[Function]` — user-defined DAX functions — were unrecoverable.** The table was missing from the builder's schema entirely, so a rebuilt model had nowhere to put one. Added (verbatim DDL from a Desktop-authored file) and carried across rebuilds.
- **The carry-over silently skipped any table missing from the target schema** — the exact way `[Function]` went unnoticed. It now reports instead.
- **`pbix_set_table_data` returned `'list' object has no attribute 'get'`** when handed a bare array of rows, which is the obvious guess. It now names the expected shape.
- **`pbix_datamodel_add_calculated_column` stamped column metadata before table metadata**, the same order that silently demoted calculated columns elsewhere. Table first now.

## [0.9.43] - 2026-07-27

**A rebuild-path edit was quietly deleting parts of the model it could not rebuild. All 24 corpus reports were affected.**

The from-scratch builder writes five metadata tables — `[Table]`, `[Column]`, `[Partition]`, `[Relationship]`, `[Measure]`. `metadata.sqlitedb` defines about seventy. Everything in the other sixty-five was discarded on every rebuild-path edit, and the tool returned `success: true` with an empty `warnings` list.

### Fixed — model metadata is now carried across a rebuild
Snapshotted before the rebuild with every foreign key expressed as a **name** rather than an ID (the rebuild renumbers every primary key), then re-attached afterwards:

- **perspectives** (`Perspective` / `PerspectiveTable` / `PerspectiveColumn` / `PerspectiveMeasure` / `PerspectiveHierarchy`)
- **Q&A synonyms and phrasings** (`LinguisticMetadata`) — present in **24/24** corpus reports, wiped by every edit
- **KPI definitions on measures**
- **dynamic format strings** (`FormatStringDefinition`)
- **shared M expressions and query parameters** (`Expression`)
- **declared data sources**
- **auto date/time drill-down wiring** (`Variation`)
- **column grouping** (`RelatedColumnDetails` / `GroupByColumn`)
- **annotations, extended properties, changed properties**

The `ObjectType` enum needed to re-resolve `(ObjectID, ObjectType)` owners was derived from the corpus rather than assumed: across 4,345 rows in 24 reports every value resolved to exactly one entity table — `1=Model, 3=Table, 4=Column, 7=Relationship, 8=Measure, 9=Hierarchy, 12=KPI, 41=Expression`.

### Fixed — a drill-down hierarchy dropped without a word
Hierarchy levels were read with `c.ExplicitName`. A calculated-table or auto-date column carries its name in `InferredName` and leaves `ExplicitName` NULL, so every level came back nameless and the hierarchy was skipped. `Agents_Performance.pbix` lost **both** of its date hierarchies to an edit that reported success. Now read with `COALESCE(ExplicitName, InferredName)` — the same defect class as the 0.9.42 `Type = 4` fix.

### Changed — nothing is dropped in silence
Anything that genuinely cannot be re-attached — because the object it referenced was deleted by the very edit being made — is now **reported in the response's `warnings`**, naming the count, the kind, and the reason. Removing a table that a perspective covers, for example, now says so instead of quietly shrinking the perspective.

Warnings raised deep in an operation reach the response through a shared channel rather than being threaded through each of the ~40 mutating tools. Threading is how call sites get missed, and missed call sites are precisely where this class of bug lives.

### Fail-safe by construction
A carried row whose owner cannot be resolved is **skipped and reported**, never written with a dangling foreign key. A missing annotation is recoverable; a half-attached model is not.

### Verified
Across all 24 corpus reports: **6 rebuild-path edits accepted, 18 refused, 0 with silent loss** — previously all 6 accepted edits lost metadata without a word. The six new corpus tests fail 6/6 against 0.9.42.

### Still not lifted
The auto date/time ceiling stands: a calculated table that also owns calculated columns is refused, which is 16 of the 18 refusals. That limitation is unchanged and deliberate.

## [0.9.42] - 2026-07-27

**Three DAX bugs that returned a wrong value instead of an error, a diagnostic that lied, and a tool that reported success while doing nothing.**

### Fixed — silently wrong values
These are the worst shape a bug can take: nothing surfaces them, so they propagate into materialized column data and into any answer built on it. All three were found by running the engine against real reports.

- **`MIN`/`MAX` over a date or text column returned `0`.** The implementation filtered values to `(int, float)`, so `MIN(Sales[Date])` — an extremely common pattern — produced `0` rather than the earliest date. Dates and text now order correctly; numeric behaviour is unchanged, and mixed columns never compare across types.
- **`FORMAT(<ISO date string>, "MMMM")` returned the raw timestamp** instead of `"January"`. A date reaches the formatter as a string in several ordinary paths (row context over a generated table, a `CALENDAR` result, a text column holding dates); those are now coerced when the pattern is a date pattern. A non-date string is still left alone.
- **An unqualified `[Column]` reference in a calculated column evaluated to blank.** Only `Table[Col]` and `'Table'[Col]` resolved — but `[Date]` is the idiomatic form and the one Power BI Desktop itself generates. Bare references are now qualified against the table being materialized, surgically: already-qualified refs, string literals, and names that are not columns of that table are left untouched.

### Fixed — a diagnostic that lied
- **`pbix_doctor`'s "Rebuild-path eligibility" check now runs the real predicate** as a dry run instead of approximating it. The 0.9.41 version inferred the answer from the presence of auto date/time tables and told two corpus reports "supported" when the edit then refused for an unrelated reason. Across 24 real reports the prediction now matches reality in **24/24** cases.

### Fixed — success reported for a no-op
- **`pbix_format_visual` returned `{"success": true}` when given keys it does not recognise**, having changed nothing. Passing raw Power BI object descriptors instead of the documented human-readable form was silently accepted. It now fails with the ignored keys named and the supported ones listed.

### Changed
- Calculated-table metadata is stamped **before** calculated-column metadata. A table that is both a calculated table and a calc-column owner needs two stamps on the same table, and applying them in the old order demoted six calculated columns to plain data.
- A calculated table that also defines calculated columns is now **refused with a clear message** rather than rebuilt. Power BI's auto date/time tables are the common case. This is a deliberate limitation, not an oversight: an attempt to support it silently dropped those columns, and a wrong model is worse than a declined edit.
- A calculated table's data columns (`Type = 4`, whose name lives in `InferredName`) are now included in the materialization schema.

### Verified
Across all 24 corpus reports: **6 rebuild-path edits succeed with calculated objects byte-identical, 18 are refused with an accurate reason, 0 corrupted, 0 diagnostic mismatches.**

## [0.9.41] - 2026-07-27

**Test corpus grown 4 -> 24 real reports, which immediately exposed a hang.**

### Fixed
- **`pbix_set_table_data` could hang forever.** `_rebuild_preserving_calc` (0.9.37) obtained the relationship list from `_get_dax_context`, which materializes EVERY table's rows — decoding the whole model's VertiPaq data to learn which columns join. On Microsoft's Competitive Marketing sample one column segment decodes so slowly that the call never returned. A hang is worse than an error: the caller has nothing to act on. Relationships now come from metadata SQL, and the same call answers in **0.2s instead of never**. Found only because the corpus grew.

### Added
- **The test corpus is now 24 reports (111 MB)**, adding Microsoft's MIT-licensed [powerbi-desktop-samples](https://github.com/microsoft/powerbi-desktop-samples) to the four community dashboards. These cover what four dashboards cannot: AI visuals (key influencers, decomposition tree), a 937-visual page, 18-page reports, large DAX models, embedded private custom visuals, and every built-in visual type. `--core-only` restores the original four; `--all-samples` pulls everything.
  - Verified across all 24: a no-op read/write cycle is byte-identical (**24/24**), and a matrix of 10 editing tools x 24 reports persists every change (**240/240**).
- **`pbix_doctor` now checks report-definition integrity**, one check per defect class this audit found: registered resources declared vs present, custom visual registration (across all three registration routes), page/visual naming, bookmark references, PBIR page/visual tree consistency, PBIR naming convention, classic-shape leaks, and enum fields carrying classic ints. Failures (❌) are what Power BI rejects; warnings (⚠️) are what it tolerates. Calibrated against the corpus: **0 failures, 5 warnings** across 24 real reports, all genuine stale bookmarks in Microsoft's own samples.
- **`Rebuild-path eligibility` check** tells you UP FRONT whether a model can take a rebuild-path edit. Auto date/time is ON by default in Desktop, so most real reports carry generated date tables whose rows cannot be reproduced — 16 of 24 corpus reports. Learning that from a diagnostic beats discovering it from a failed call.
- The refusal message for auto date/time tables now names the cause and both workarounds (use the surgical tools, or turn off Auto date/time and re-save) instead of saying "has no readable columns".

### Verified
- Calc-object preservation across every corpus model: **6 preserved byte-identically, 0 changed, 0 corrupted, 18 refused with a reason.** Zero silent damage — the property that matters most for a tool that writes to your work files.

## [0.9.40] - 2026-07-27

### Fixed
- **Bookmark files were rewritten on every save.** 0.9.38's bookmark support stamped its own `$schema` onto existing `.bookmark.json` files, overwriting the (newer) version the service had declared. A read/write cycle that changed nothing still rewrote all four bookmark files in `IT_Support.pbix`. The declared `$schema` is now preserved; only a NEW bookmark gets a default stamped on it — the same rule already applied to pages and visuals.
  - Neither the 125-tool sweep nor schema validation caught this: the rewritten files were still schema-valid and the bookmarks still worked. It was found by a new test that asserts a no-op read/write cycle leaves every definition file byte-identical, run against the two real service-authored reports in the public corpus.

### Added
- `tests/test_pbir_roundtrip.py` gains fidelity tests against the **real** corpus PBIR reports (50 and 22 visuals), not just the synthetic fixture: byte-faithful no-op round-trip, container formatting preserved across an unrelated edit, report-level state preserved, and no internal bookkeeping keys leaking to disk.

## [0.9.39] - 2026-07-27

**All 125 tools audited on both report formats. Thirteen were silently discarding their changes on service-authored (PBIR) reports; all are fixed and verified.**

### The audit

Every tool was tested the same way: apply it to a PBIR report, save, reopen, and check the *saved bytes* — with a negative control proving the check fails on an untouched file, plus schema validation of the output. Across 125 tools: **13 LOST_ON_PBIR, 1 SILENT_NOOP, 50 PERSISTS, 59 READONLY, 2 NOT_APPLICABLE.** All 14 broken tools are fixed and re-verified; the fixes are pinned by 23 tests that fail against the previous release.

### Fixed — container formatting and sort were dropped on PBIR
`pbix_format_visual`, `pbix_set_visual_property`, `pbix_update_visual_json`, `pbix_add_visual`, `pbix_duplicate_visual`, `pbix_duplicate_page`, `pbix_set_visual_sort`, `pbix_recolor`

- **`vcObjects` had no mapping to PBIR in either direction.** Container-level formatting — title, background, border, shadow, header — is `visual.visualContainerObjects` in PBIR and `singleVisual.vcObjects` in classic. The reader never translated it, so it read back empty; the writer's four-key whitelist never emitted it, so it was discarded on save. `pbix_format_visual` would report *"Formatted visual 0 on page 0: title, background"* and persist neither. `pbix_recolor` reported replacing colours while leaving every container colour untouched. Both directions are now mapped.
  - **Placement corrected against ground truth.** The field belongs *inside* `visual`, not as a top-level sibling: 70 of 70 visuals in the service-authored corpus put it there, and `visualContainer` sets `additionalProperties: false` without permitting it at the top level. `pbix_export_pbip` had been emitting it at the top level, producing PBIP that Power BI rejects — also fixed.
- **The writer's whitelist is gone.** It propagated only `visualType`, `objects`, `syncGroup` and `drillFilterOtherVisuals`, silently dropping `columnProperties`, `expansionStates`, `activeProjections`, `showAllRoles`, `display` and `howCreated` — and, for a *newly created* visual (no original to copy from), everything else. `pbix_update_visual_json` documents "replace the entire config JSON" and kept four keys.
- **`prototypeQuery.OrderBy` now translates to `query.sortDefinition`.** Any newly added or duplicated visual took the query-rewrite path and came out with no sort at all, so `pbix_set_visual_sort` and `pbix_add_visual(sort_by=...)` were no-ops on PBIR.
- **Numeric dot-path segments build arrays.** `pbix_set_visual_property` with `title.0.properties…` created `{"0": …}` where the schema requires a list — JSON Power BI rejects.

### Fixed — report-level state was written to a document that is never saved
`pbix_add_image`, `pbix_set_image`, `pbix_set_theme`, `pbix_add_html_visual`, `pbix_remove_custom_visual`, `pbix_set_filters` (report scope), `pbix_set_settings`

`resourcePackages`, `publicCustomVisuals`, `themeCollection`, report filters and settings live in `Report/definition/report.json` on PBIR, but these tools mutated the synthesized *layout* and called `_set_layout`, which writes only the pages tree. `_get_layout`/`_set_layout` now round-trip all of it, so the tools are fixed without individual patches.

- `pbix_add_image` wrote the PNG and the image visual but never declared the resource — Microsoft requires an entry in `report.json`, so the image never rendered.
- `pbix_remove_custom_visual` deleted the files while leaving the registration behind: a corrupting half-apply, not merely a no-op.
- `pbix_set_settings` created a legacy `Report/Settings` part alongside PBIR's authoritative `report.json` settings, leaving two conflicting documents; `pbix_get_settings` reported "no settings" for a PBIR report that had six.
- `pbix_set_theme` no longer substitutes a built-in base theme that would contradict `resourcePackages`, and a new `customTheme` now carries the `reportVersionAtImport` the schema requires.

### Changed
- mypy baseline ratcheted 145 → 140 (currently 137).

### Note on schema validation
`scripts/validate_pbir_schemas.py` is necessary but **not sufficient**: every file in this audit passed schema validation, including one declaring a custom visual that was never registered. Schema-valid and semantically correct are independent properties.

## [0.9.38] - 2026-07-26

### Fixed
- **The PBIR writer now refuses to emit a document Power BI would reject.** 0.9.35 wrote a page carrying the classic integer `displayOption` into a PBIR report. The service IMPORTED that file without complaint — both the semantic model and the report item were created — and then failed to render it with "Something went wrong / Unable to load report", because Microsoft classifies a schema violation as a *blocking* error. Nothing in the writer noticed, so the defect only surfaced on upload. `_pbir_write_json` now runs an offline structural check on every document before writing it: the fields PBIR types as string enums (`displayOption`, `visibility`, `type`, `howCreated`) must carry the enum NAME, and required fields must be present. Writing the classic int form now fails the save with a message naming the mistake, instead of producing a file that imports and then won't open. The check needs no network; `scripts/validate_pbir_schemas.py` remains the full check against Microsoft's published schemas.

## [0.9.37] - 2026-07-26

Model edits work on models that contain calculated tables and columns — three of the four corpus reports that previously refused them.

### Fixed
- **Rebuild-path edits are no longer refused on models with calculated objects.** Adding or replacing a table, adding or removing a relationship, and removing a table all reconstruct the model rather than splicing metadata. A from-scratch rebuild loses `Type=2` calculated columns and demotes calculated tables to plain data, so rather than corrupt the file these edits were refused outright on any model containing either — **three of the four reports in the public corpus**, which made adding a table impossible on most real files. They now re-materialize the calculated objects as part of the edit: calculated columns are re-evaluated from their DAX, calculated tables keep their rows and their `Type=2` partition + `QueryDefinition`, so Power BI still recomputes both on Refresh. All four corpus reports now accept the edits with their calculated objects byte-identical afterwards, measures and relationships intact.
  - The refusal remains where it is the right answer: when the engine cannot reproduce an existing calculated column or table, the edit is still refused rather than written with wrong values.
  - When a caller replaces a table's rows, that table's calculated columns are recomputed **from the new rows**, not carried over from the old data.
  - A table the caller is removing is excluded from the preservation plan, so it isn't resurrected by the very edit that deleted it.
- **Relationships onto calculated-table and auto-date columns were read as `None`.** Those columns carry no `ExplicitName` — their name is in `InferredName` — so the rebuild saw an endpoint of `None` and pre-build validation rejected the model with "column does not exist". This blocked `GeoSales_Dashboard` and `Agents_Performance` entirely.

## [0.9.36] - 2026-07-26

Report-editing primitives, sort-by-column, and PBIR output validated against Microsoft's own schemas.

### Added
- **Report-editing primitives (6 new tools).** `pbix_add_page` and `pbix_remove_page` existed with nothing in between, and visual geometry was readable (`pbix_get_visual_positions`) with no way to write it back — so renaming a page or nudging a visual meant hand-editing raw layout JSON. Now: `pbix_rename_page`, `pbix_reorder_pages`, `pbix_set_page_visibility`, `pbix_duplicate_page`, `pbix_move_visual`, `pbix_duplicate_visual`. All six behave identically on classic `Report/Layout` and on PBIR. Renaming preserves the internal `name`, so bookmarks and page navigation keep resolving; duplicating hands the copy — and every visual on it — fresh identities, because two objects sharing a `name` collide in bookmarks and navigation.
- **Sort-by-column (2 new tools).** `pbix_set_sort_by_column` / `pbix_get_sort_by_columns`. The model stores this as `Column.SortByColumnID` — an ID, not a name — so it was unreachable through `pbix_datamodel_modify_column`, which sets a property to a literal value. Without it, any non-alphabetical text column (month names, weekday names, size labels) sorted wrongly in every visual. Names are resolved to IDs (including `InferredName` columns, which carry no `ExplicitName`), and self-sorts and A↔B cycles are rejected rather than written into a model that then opens broken.
- **PBIR bookmarks are persisted.** `_set_layout_pbir` only ever wrote the pages tree, so on a service-authored report `pbix_add_bookmark` returned success and silently discarded the bookmark — the worst failure shape, because nothing surfaced the loss. Bookmarks now round-trip through `definition/bookmarks/*.bookmark.json` plus `bookmarks.json`, groups included, and a page-only edit cannot delete them.
- **`scripts/validate_pbir_schemas.py`** validates every file in a PBIR report against the JSON schema it declares on `developer.microsoft.com` — testing the writer against the format owner's contract rather than against our own reader. Where the service stamps a version newer than the public index, it falls back to the highest published minor of the same major. Also runs as `tests/test_pbir_schema_conformance.py` (marked `integration`).
- **`docs/capability-parity.md`** — a per-operation audit of pbix-mcp against Power BI's authoring surface, with the remaining gaps ranked.

### Fixed
- **PBIR pages were written with the classic integer `displayOption`.** PBIR requires the enum name (`"FitToPage"`), so every page added to a service-authored report carried a value the schema rejects — found by the schema validation above. `displayOption` and page `visibility` are now converted at both boundaries, so `_get_layout` returns one shape whichever format the file uses and callers never branch on format.
- **`pbix_add_page` used `displayOption: 0`** — the deprecated dynamic mode — where Power BI Desktop writes `1` (`FitToPage`). This was wrong in classic files too.
- **Bookmarks were emitted with a `byColumn` filter bucket that PBIR does not define, and without the required `explorationState.sections`.** Neither appears in any Desktop-authored bookmark; both are now correct in classic and PBIR alike.
- **Clearing a sort-by writes `0`, not `NULL`** — every column in a Desktop-authored model has a non-NULL `SortByColumnID`.

### Changed
- mypy baseline ratcheted 165 → 145 (currently 144) after clearing 21 implicit-`Optional` annotations.

## [0.9.35] - 2026-07-26

Service-authored (PBIR) reports are now **editable**, not just readable.

### Added
- **PBIR write support — a service-authored report can be edited.** 0.9.34 made these reports readable but refused every layout write; since every report authored in the Power BI *service* is PBIR, that left the most common kind of report analysable but not editable. Layout changes are now written back into the `Report/definition` tree: pages and visuals are added, moved, resized, renamed, hidden and removed; `pages.json` `pageOrder` / `activePageName` are kept in step; and a classic `Report/Layout` is still never planted alongside the tree.
- **Fidelity by construction: preserve-and-patch.** Rather than regenerating the tree from the (lossy) legacy view, each page/visual is patched onto **the original file it was read from**, and only fields that actually differ from that original are written. So anything this converter doesn't model — `sortDefinition`, `howCreated`, custom-visual settings — survives an edit untouched, and **a write that changes nothing changes nothing on disk**: read + write is verified semantically byte-faithful across all 96 files of three real PBIR reports (the two in the public corpus plus a service-authored sample). Field bindings are only rewritten when the bindings actually changed, and the converter's own defaults (a missing `tabOrder`, an absent `visual` block, a `$schema` on a file that had none) are never invented into the file.
- **Resource and custom-visual registration works on PBIR.** `pbix_register_resource`, `pbix_add_custom_visual`, `pbix_reference_public_visual` and `pbix_add_html_visual` all refused PBIR files. `resourcePackages` / `publicCustomVisuals` live in `Report/definition/report.json` for PBIR (the files themselves are under the same `Report/StaticResources` and `Report/CustomVisuals` paths), so the four tools now read and write whichever document the format uses — emitting PBIR's **flat** package shape with **string** item types (`Image`, `CustomTheme`, `BaseTheme`), verified against `test_corpus/{Ecommerce_Conversion,IT_Support}.pbix`, instead of the classic nested numeric form.

### Changed
- `pbix_report_format` now reports PBIR as **writable**. `tests/test_pbir_reader.py` grew to 29 tests, pinning the no-op fidelity guarantee, survival of unmodelled fields across an edit, page/visual add + delete, and that no classic `Report/Layout` is ever created.

## [0.9.34] - 2026-07-26

Service-authored (PBIR) reports are now readable through the normal tool surface.

### Fixed
- **A report in PBIR format read as "no layout" at all.** `_get_layout` — the single entry point every consumer uses — only knew the classic `Report/Layout` document and returned `None` otherwise. But **every report authored in the Power BI service downloads as PBIR**: no `Report/Layout` exists, the report is a `Report/definition/` tree of per-page and per-visual JSON files (2 of the 4 files in the public test corpus are already this shape). `_get_layout` now falls back to the PBIR reader, so pages, visuals, bindings and filters come through everywhere — `pbix_get_layout_raw`, DAX default-filter resolution, `pbix_doctor`, documentation and diff all included. The three hand-rolled fallbacks at the call sites were folded into the shared path.
- **The PBIR converter dropped nearly every semantic field**, so what it returned wasn't usable even where it was called. Two of those were hard failures: a visual carried **no name**, so it could not be addressed at all, and **no field bindings**, so it rendered empty. Now mapped: visual `name` → `config.name`; `query.queryState.<Role>.projections[]` → `singleVisual.projections` **plus a synthesized `prototypeQuery`** whose `Select` preserves the `Column` / `Measure` / `Aggregation` discriminator (entity refs rewritten to classic `From` aliases); full geometry incl. `z` and `tabOrder`; page `name`, real `displayName` (it used to emit the page GUID), `width`/`height`, `displayOption` and `type` (so a **Tooltip** page is identifiable); `isHidden`; visual and page `filterConfig`; the active page; and each visual's `mobile.json` geometry. Sync groups, already preserved, still are. Verified against the corpus: `IT_Support.pbix` reads 3 pages / 50 visuals, all named, with both Column and Measure bindings recovered.

### Added
- **`pbix_report_format`** — reports whether an open file is `classic` or `PBIR`, its page/visual counts, and explicitly whether the layout is **writable**, so a client can explain the format to a user instead of attempting an edit that cannot work. **117 tools.**

### Changed
- **PBIR reports are read-only, enforced at the choke point.** The layout a consumer holds for a PBIR file is a *synthesized* legacy view; writing it back would plant a classic `Report/Layout` inside a PBIR file and leave two conflicting definitions. Every layout mutation funnels through `_set_layout`, which now refuses with `FORMAT_UNSUPPORTED` and an explanation (the synthesized document is also tagged `__pbir__`, so it is refused even if it travels). Classic files are unaffected. New `tests/test_pbir_reader.py` (24 tests) builds a PBIR tree from scratch and pins the mapping and the refusal.

## [0.9.33] - 2026-07-26

Table functions that silently dropped their aggregates now work, and calculated column values are readable.

### Fixed
- **`SUMMARIZE` / `SUMMARIZECOLUMNS` silently DROPPED their extension columns — the aggregated value vanished.** `SUMMARIZE(Sales, Sales[Category], "Total", SUM(Sales[Amount]))` returned only the group column, so any measure built on it was quietly wrong (the same class of defect as the 0.9.30 `FILTER` fix). Extension columns are now evaluated per group, in a filter context restricted to that group's key: the example returns `HW → 150`, `EL → 500`, and `SUMX(SUMMARIZE(...), [Total])` totals correctly.
- **`SUMMARIZE` grouping by a RELATED table's column returned NOTHING.** Only group-by columns on the base table were honoured, so the canonical "group a fact by a dimension column" shape (`SUMMARIZE(Sales, Products[Category], ...)`) evaluated to an empty table. Related columns are now resolved through the engine's own relationship propagation, and combinations that don't exist in the base table are skipped — matching DAX.
- **`SELECTCOLUMNS` raised `KeyError` on a plain table.** It unconditionally copied `__column__`/`__value__` meta keys, which a multi-column row (a bare table reference or `ALL(Table)`) doesn't carry.
- **Calculated column values were unreadable.** `read_table_from_abf` filtered `Type IN (1, 3, 4)`, omitting `Type = 2` calculated columns whose values *are* stored — so after 0.9.32 `schema` reported a calculated column that `get_table` / `pbix_get_table_data` never returned. `ModelReader.get_table` now includes them by default (`include_calculated=True`); the DataModel rebuild path keeps the previous behavior, since it re-materializes calculated columns from their DAX and would otherwise duplicate the column.

### Changed
- **Calculated-table authoring accepts `SUMMARIZE`, `SUMMARIZECOLUMNS` and `SELECTCOLUMNS`**, which were refused while they were lossy. `GROUPBY` and the join/index helpers remain refused — they are still unimplemented, and the evaluator refuses anything reporting an unsupported function rather than persisting a wrong table.
- **The five reporting tools that had no test coverage now have it** — `pbix_get_model_schema`, `pbix_get_model_columns`, `pbix_performance`, `pbix_diff`, `pbix_document` (new `tests/test_tool_surfaces.py`). Note `pbix_document`'s `.docx` output requires `python-docx`; without it the tool returns the markdown and says so.
- **mypy baseline ratcheted 175 → 165** (actual: 162) after annotating twelve untyped collections, so the gate can't drift back up.

## [0.9.32] - 2026-07-25

Calculated fields now read back typed, flagged as calculated, and named.

### Fixed
- **A calculated column/table reported `DataType = "Unknown"`.** `ModelReader.schema` derived the type from `ExplicitDataType` alone, but a calculated column (and every calculated-table column) carries `ExplicitDataType = 1` "Automatic" with the real type in `InferredDataType` — Desktop's own shape — and 1 isn't a concrete type, so the lookup fell through to "Unknown". The reader now falls back to `InferredDataType` when the explicit code isn't a concrete type, so an authored `Inventory = Products[UnitPrice] * Products[StockQty]` reads `Double` and a text calc column reads `String`. Regular columns are unaffected (their two codes agree).
- **`IsCalculated` was inverted — calculated columns read `false` and the RowNumber system column read `true`.** The check was `Column.Type == 3`, but the AMO enum is **1=Data, 2=Calculated, 3=RowNumber, 4=CalculatedTableColumn**. It is now `Type in (2, 4)`, so calculated columns *and* calculated-table columns report `true` while RowNumber correctly reports `false`.
- **A calculated-table column read back with a NULL name**, which also crashed clients doing `columnName.startswith(...)`. Power BI Desktop leaves `ExplicitName` NULL on a calculated-table column and carries the name in `InferredName`, setting `ExplicitName` only when the user *renames* the column — verified across the public corpus (Agents_Performance 26/27, GeoSales 7/8 NULL; the non-NULL ones are renamed field parameters). The writer is therefore already Desktop-exact and is unchanged; the reader now coalesces the two, so `ColumnName` is never None and a `DISTINCT(Products[CategoryID])` calculated table reports its column as `CategoryID`, inherited from the source column, exactly as Desktop shows it. A side benefit: a calculated-table column can now be bound as a visual field (`CategoryList[CategoryID]`) — previously impossible, because a NULL name could never match.
- **`ModelReader.dax_columns` returned an EMPTY list for every model** — an audit of the same enum turned up two more instances. It filtered on `Type = 3`, the RowNumber system column, which never carries an `Expression`, so `pbix_get_model_columns` always reported "No DAX calculated columns found" even on a model with 27 of them. Now filters `Type IN (2, 4)` and reports the real type instead of "Unknown".
- **`ModelReader.statistics` mis-counted `ColumnCount`** — it counted `Type != 2`, which excluded calculated columns and counted the RowNumber system column, so a measure-only table reported 1 column instead of 0. Now counts `Type != 3` (data + calculated + calculated-table). Feeds `pbix_performance`, `pbix_diff` and `pbix_document`.

New `tests/test_issues15.py` (18 tests).

## [0.9.31] - 2026-07-25

Date-part DAX functions (they were missing entirely), predicate filter contexts, live Top-N, and calculated columns no longer leak into the partition query.

### Fixed
- **`YEAR`, `MONTH`, `DAY`, `QUARTER`, `HOUR`, `MINUTE`, `SECOND`, `WEEKDAY`, `DATE`, `EDATE`, `EOMONTH` were not implemented at all.** Any expression using one evaluated to BLANK and was reported as an unsupported function — so a `Year = YEAR(Sales[Date])` calculated column, a month grouping, or any date-part measure simply did not work. All eleven are now implemented with DAX semantics: `DATE()` rolls over out-of-range months/days (`DATE(2024,13,1)` → 2025-01-01), `EDATE`/`EOMONTH` clamp to the end of a short month (2024-01-31 + 1 month → 2024-02-29), and `WEEKDAY` supports return types 1/2/3. Implementing `DATE()` also made **`CALENDAR()` usable**, so a real Date dimension can now be authored as a calculated table.
- **A datetime cell was substituted into row-context DAX unquoted**, producing unparseable text (`YEAR(2024-01-15 00:00:00)`), which is why date-part calculated columns failed even once the functions existed. Dates now go in as quoted ISO literals — in both the calculated-column evaluator and `FILTER`.
- **A calculated column's values leaked into the partition's "Enter data" query.** The values are materialized into VertiPaq and *then* the column is re-stamped as calculated, so the M literal still carried them — stale data the engine ignores and that Desktop would never write. The M now embeds source columns only (`builder.add_table(..., calc_columns=[...])`); VertiPaq storage is unchanged.

### Added
- **`filter_context` accepts structured predicates, not just In-sets.** A value may now be a dict instead of a list, so a caller no longer has to enumerate every matching value of a high-cardinality column before evaluating: `{"op": ">", "value": 100}` (also `>=`, `<`, `<=`, `=`, `<>`), `{"between": [lo, hi]}`, `{"in": [...]}` / `{"not_in": [...]}`, `{"contains"|"starts_with"|"ends_with": "text"}` (case-insensitive), `{"relative_date": {"last": 7, "unit": "day", "anchor": "2024-03-10"}}`, and `{"is_blank": true}`. Several keys in one dict are ANDed, and predicates mix freely with list filters. Comparisons are numeric when both sides are numbers, date-aware when both parse as dates, and text otherwise. **List values keep their exact previous semantics.** Applied at every filter site, including relationship propagation.
- **Live Top-N on `pbix_evaluate_dax_grouped`** via `top_n`, `order_by` and `order`. Every group is still evaluated, so the ranking reflects real measure values; groups with no value sink to the bottom in both directions rather than outranking real numbers.

### Notes
- **Desktop's *automatic* date hierarchies (`LocalDateTable_<guid>` + `Variation` wiring) are still not generated** — deliberately. No Desktop-authored sample in the public corpus has auto date/time enabled, so the structure could only be guessed, and a wrong guess is exactly the class of defect that makes the Power BI service reject a model. The user-facing need is met with verified primitives instead: author a Date dimension (`CALENDAR(DATE(y,1,1), DATE(y,12,31))` as a calculated table), add date-part calculated columns, and build the drill path with `pbix_add_hierarchy` — covered end-to-end in `tests/test_issues14.py::TestDateHierarchyRecipe`.

## [0.9.30] - 2026-07-25

Calculated-table authoring, a single-call GROUP-BY evaluator, in-place relationship edits — and a FILTER fix that was silently zeroing measures.

### Fixed
- **`FILTER(Table, Table[Column] <op> value)` returned NO rows — every measure built on it was wrong.** A bare column reference inside the condition evaluated to an unresolved `('Table','Column')` marker rather than the iterated row's value, so the comparison yielded `None`, the condition was never true, and FILTER returned an empty table. `FILTER(Sales, Sales[Amount] > 90)` produced 0 rows instead of 2, and anything wrapping it — `COUNTROWS(FILTER(...))`, `CALCULATE(SUM(...), FILTER(...))` — collapsed to 0/BLANK. The condition is now evaluated against the row being iterated (its column references substituted with that row's values), so text comparisons, `&&`/`||` compounds and nested use all work. Conditions containing an aggregation (`FILTER(Sales, SUM(Sales[Amount]) > 90)`) deliberately keep the previous filter-context route, so no existing behavior shifts. Covered in `tests/test_issues14.py::TestFilterFix`.

### Added
- **`pbix_datamodel_add_calculated_table` — author a DAX calculated table.** Evaluates the table expression, materializes the resulting rows into VertiPaq, and stamps Desktop's calculated-table metadata — Table `SystemFlags=2`, partition `Type=2` + `SystemFlags=2` carrying the DAX as its `QueryDefinition`, and every data column `Type=4` with the name moved to `InferredName`, `SourceColumn` `[Name]`, `ExplicitDataType=1` (Automatic) and the real type in `InferredDataType` — verified field-for-field against Desktop-authored calc tables in `test_corpus/GeoSales_Dashboard.pbix`. So the file opens with data and Power BI recomputes the table on Refresh. **Scope is enforced, not assumed:** only shapes this engine reproduces exactly are accepted (`DATATABLE`, `GENERATESERIES`, `DISTINCT`, `VALUES`, `FILTER`, `TOPN`, `ADDCOLUMNS`, a bare table reference); `SUMMARIZE`/`SUMMARIZECOLUMNS` silently drop their extension columns and `SELECTCOLUMNS`/`GROUPBY` can't be evaluated, so all of them are REFUSED (`UNSUPPORTED_CALC_TABLE`) rather than persisted with wrong rows, as is any expression using an unsupported function or yielding an unusable shape.
- **Calculated columns and calculated tables now compose in either order.** Adding either one re-materializes the other (a rebuild would otherwise drop `Type=2` calc columns, which aren't read back from VertiPaq, and demote calc tables to plain data tables). Previously, adding a calculated column to a model that contained a calculated table was refused outright.
- **`pbix_evaluate_dax_grouped` — evaluate measures for every group key in ONE call.** The GROUP-BY entry point for chart-shaped work: instead of one evaluation per category value, the fact rows are bucketed by the propagated join key once, so 1,500 groups × 2 measures return in a single sub-second call (values verified row-for-row against brute-force aggregation). Results come back as structured per-group objects rather than a formatted table, `max_groups` defaults to 3500 (Power BI's own data-reduction window) and truncation is reported via `group_count`/`truncated`. Measures the fast path can't bucket are still evaluated exactly, per group, so values always match `pbix_evaluate_dax`. A composite `group_by` ("Table.A,Table.B") evaluates per group combination.
- **`pbix_datamodel_modify_relationship` — edit a relationship in place** instead of remove + re-add. Changing `is_active` / `cross_filter_direction` is a metadata-only splice, so it works even on models the rebuild path refuses (those with calculated tables/columns); a `cardinality` change re-runs the rebuild so the R$ join indexes match. Finds the relationship in either stored orientation, and reports `NOTHING_TO_CHANGE` / `RELATIONSHIP_NOT_FOUND` / `INVALID_ARGUMENT` explicitly. **116 tools.** New `tests/test_issues14.py` (37 tests).

## [0.9.29] - 2026-07-25

Author calculated columns, and stop inline-data tables from emptying on Refresh.

### Added
- **`pbix_datamodel_add_calculated_column` — author a DAX calculated column and materialize its values.** Writes a Desktop-shape calculated column (`Column.Type=2` + the DAX Expression, `ExplicitDataType=1` Automatic with the real type in `InferredDataType`, `SourceColumn` NULL — verified field-for-field against a Desktop-authored calc column in `test_corpus/GeoSales_Dashboard.pbix`) AND stores the evaluated values in VertiPaq, so the file opens with correct data and the Power BI service recomputes the column on Refresh. Implemented via the proven "materialize-as-data then re-stamp calc metadata" path (the same approach as field parameters); multiple calculated columns are supported (existing ones are re-evaluated on each add, including calc-columns that reference other calc-columns, in dependency order). **Safety:** only row-context expressions over the table's OWN columns are materializable (e.g. `fct[Sales] - fct[Cost]`, `IF(t[Qty] > 0, "Yes", "No")`, `ROUND(t[X] * 0.1, 2)`); aggregations (SUM/COUNT/…), CALCULATE, RELATED and other table/filter functions are computed by the service across rows and cannot be reproduced per-row, so they are REFUSED with a clear reason (`UNSUPPORTED_CALC`) rather than stored silently wrong. A calc-column name that duplicates a column, or collides with a same-table measure (which breaks the service), is also rejected. New `tests/test_calc_column.py` (24 tests); `calc_tables.calc_column_unsupported_reason` / `evaluate_row_context_column`. **113 tools.**

### Fixed
- **Tables authored from inline rows (via `pbix_set_table_data` / `pbix_create`, and any rebuild-path edit) no longer go blank on a Desktop/service Refresh.** The row data was embedded in VertiPaq (so the table rendered on open), but the partition's Power Query definition was a headers-only `#table(type table [...], {})` — a Refresh re-ran that empty query and wiped the table, blanking every visual bound to it. The partition M now embeds the rows as Power BI Desktop's own "Enter data" literal — `Table.FromRows(Json.Document(Binary.Decompress(Binary.FromText("<base64>", BinaryEncoding.Base64), Compression.Deflate)), <type table>)` followed by a `Table.TransformColumnTypes(..., "en-US")` — so a Refresh reproduces exactly the same rows (raw-DEFLATE + base64 payload, verified to round-trip). Cells are serialized as text and cast back by type (decimals preserved, `null` → JSON null, booleans → `TRUE`/`FALSE`), with an invariant `en-US` parse so a non-US model culture can't corrupt numbers. Column names with spaces/special characters are M-escaped. Payloads beyond ~12 MB base64 fall back to the old headers-only form (such a table should carry a real source). New `tests/test_enter_data_m.py` (12 tests); helpers `builder._build_enter_data_m` / `_build_m_expression(rows=...)`.

## [0.9.28] - 2026-07-24

Service-fidelity measure authoring: reject three classes of measure that our lenient DAX engine accepts but Analysis Services (the Power BI service engine) rejects — same-table measure/column name collisions, reserved-word VAR names, and data-type lies.

### Fixed
- **A VAR named after a DAX function or reserved keyword is now rejected — it made the measure's visual go permanently blank in the Power BI service.** A measure such as `VAR status = ... RETURN status & ...` evaluated fine in our engine, but Analysis Services (measures compile into an MDX-hosted model script) rejects `status` as an identifier and fails the visual with `MdxScript(Model) (1,1) Failed to resolve name 'SYNTAXERROR'` / *"Failed to move the data reader to the next result."* We now scan every measure's VAR names against the set of DAX function names + DAX/MDX reserved keywords and reject the measure (matching Power BI Desktop) with an error naming the offending VAR and suggesting a rename. The reserved set is calibrated against app.powerbi.com's own DAX engine — `status`, `value`, `level`, `count`, `name`, `date`, `filter`, `rank`, `member`, `dimension`, `parent`, `scope`, `current` (and every function name) are rejected, while `result`, `total`, `amount`, `position` are deliberately NOT flagged (not every MDX reserved word is actually rejected, so the list avoids false positives). Enforced in `pbix_datamodel_add_measure`, `pbix_datamodel_modify_measure`, and the builder's pre-build checks; exposed as `builder.find_reserved_var_names`. (Real-world repro: a `Profit sammendrag` summary measure whose `VAR status` blanked the Profitabilitet page's conclusion visual.)
- **A measure whose name collides with a same-table column is now rejected — it silently broke EVERY visual in the Power BI service.** A model authored via `pbix_create` / `pbix_datamodel_add_measure` opened and rendered perfectly in pbix-mcp's DAX engine but, once uploaded to app.powerbi.com / Fabric, showed every data visual blank with *"One or more errors were encountered in the MDX script."* Analysis Services compiles all measures into a single calculation script and rejects an ambiguous unqualified reference when a measure shares a name (case-insensitively) with a column on its own table — a collision Power BI Desktop's UI prevents, but which pbix-mcp's engine never noticed because it resolves measures and columns through separate namespaces. One such measure poisons the whole script, so even an orphan measure referenced by no visual blanks the entire report. (Real-world repro: a 7-table Norwegian financial model where measure `Entur_Regnskap[Eiendeler]` collided with column `Entur_Regnskap[eiendeler]`.) Both `pbix_datamodel_add_measure` and the builder's pre-build checks now refuse the collision with a clear error naming the offending column, instead of emitting a file that loads locally but breaks online.
- **Measures no longer hardcode `DataType = Int64 (6)` — a type-lie that truncated decimals AND hard-broke text measures in the service.** Every measure was written as Int64 regardless of what it returned. Two failure modes: (1) a measure yielding `0.153` was stored as a whole number and rendered as `0` in Analysis Services (silent wrong value); (2) a **text-returning** measure declared Int64 made its visual go dead in the Power BI service — the query fails with *"Failed to move the data reader to the next result"* / `MdxScript(Model) Failed to resolve name 'SYNTAXERROR'`, because AS builds the result reader from the declared Int64 type and cannot read a string scalar through it. (Confirmed in the field: a `Profit sammendrag` text-summary measure left one visual permanently blank until re-typed.) The data type is now **inferred from the expression** — text-returning expressions (`FORMAT`, `CONCATENATEX`, `&` concatenation, `IF`/`SWITCH` with string branches, a `RETURN` of a text `VAR`, …) get `String (2)`; everything else defaults to `Double (8)`, which stores integers and decimals without truncation. Applies to `pbix_datamodel_add_measure` and every builder `[Measure]` INSERT.

### Added
- **`data_type` parameter on `pbix_datamodel_add_measure` and `new_data_type` on `pbix_datamodel_modify_measure`.** Accepts a friendly name (`String` / `Int64` / `Double` / `Decimal` / `DateTime` / `Boolean`) or a raw AMO code to override the inferred type — e.g. to force a whole-number count to `Int64`, or to repair a measure whose stored type lies about its result. The builder's `add_measure` gained the same keyword. New `tests/test_measure_collision_datatype.py` (46 tests).

## [0.9.27] - 2026-07-24

First-class image / registered-resource authoring: `pbix_add_image`, `pbix_register_resource`, `pbix_set_image` — replacing an undocumented private hook.

### Added
- **`pbix_add_image` — register + place an image in one call, matching Desktop field-for-field.** Until now the only way to author an image was an UNDOCUMENTED branch inside `pbix_add_visual` that keyed off a private `singleVisual.objects.general[0].properties.imageUrl.sourcePath` string in `config_json`, accepted only a local file path (a caller holding bytes had to write a temp file), and produced a container missing Desktop's `howCreated`, `tabOrder`, 1000-step `z`, and `vcObjects.padding`. The new tool takes `image_path` OR `image_base64` (a full `data:` URI is accepted), registers the resource, and writes the container diffed field-for-field against Desktop-authored ground truth (`test_corpus/GeoSales_Dashboard.pbix`): `howCreated: "InsertVisualButton"`, 1000-step `z` + `tabOrder` on both the container and `layouts[0].position`, `drillFilterOtherVisuals`, the `ImageUrl` ResourcePackageItem expr (PackageType 1), `objects.imageScaling` (`'Fit'` / `'Fill'` / `'Normal'` — note scaling lives under `objects.imageScaling`, NOT `objects.general`), and `vcObjects.padding` `0D` on all four sides.
- **`pbix_register_resource` — register any file resource (image 100, shapeMap 200, customTheme 201, baseTheme 202)** across all three touchpoints Desktop uses: the bytes under `Report/StaticResources/RegisteredResources/`, a `<Default Extension="…" ContentType=""/>` in `[Content_Types].xml`, and a type-tagged `resourcePackages` item. Returns the final item name.
- **`pbix_set_image` — repoint or restyle an EXISTING image visual** with new bytes, an already-registered `item_name`, and/or a new `scaling`. Previously no engine path could register a resource for an existing visual (the private hook ran only inside `pbix_add_visual`), so a picture replacement meant reimplementing the whole registration client-side. The previously referenced resource is deliberately left in place — another visual may reference the same item.
- **Security posture on every resource write**: the file type is decided by CONTENT, never by a filename or caller claim — images by magic bytes (PNG/JPEG/GIF/WebP/BMP/TIFF/ICO, plus SVG detected past a BOM / XML declaration / DOCTYPE / comments with an `<svg>` ROOT element, so arbitrary XML that merely mentions svg is rejected), while shape maps and themes must be JSON (the form Desktop stores them in — registering a PNG as a theme is refused); 5 MB cap on both input paths; base64 input tolerates line wrapping and `data:` URIs; item names are matched CASE-INSENSITIVELY and adopt the existing casing, because on a case-insensitive filesystem (macOS/Windows, where Desktop runs) `Logo.png` and `logo.png` are one file — keeping the caller's casing would have registered a layout item and a visual reference for a part that never landed in the .pbix; item names sanitized to `[A-Za-z0-9._-]`, forced to the sniffed extension so the name always agrees with `[Content_Types].xml`, contained with `_safe_join`, and uniquified rather than overwriting a different existing resource (identical bytes under the same name reuse it). The engine never fetches remote URLs — callers holding a URL fetch it themselves and pass the bytes.

### Fixed
- **`[Content_Types].xml` no longer silently skips the image extension.** The declaration was inserted with a string replace anchored on `<Default Extension="json"`; on a document whose `[Content_Types].xml` has no json Default (the repo's own fixtures, and any minimally-authored report) the replace was a NO-OP — no error, and the saved pbix declared an image part with no registered extension. Insertion now falls back to `</Types>`, and the duplicate guard is case-insensitive and whitespace-tolerant so a Desktop-authored `ContentType="" />` is recognized. The legacy `sourcePath` hook shares the same helpers, so it inherits the fix; it also no longer PERSISTS the private `sourcePath` key (which leaked the author's absolute local path into the saved report) and fails loudly on an unreadable path instead of silently shipping an image-less visual. New `tests/test_images.py` (23 tests).

## [0.9.26] - 2026-07-24

Service-parity DAX evaluation controls (default-filter opt-out + page scoping), a literal-first arithmetic engine fix (Total Sales was 0), DataCategory ergonomics, and hyphenated marketplace GUIDs.

### Added
- **`apply_default_filters` + `page_index` on both evaluate tools — control over the implicit default-slicer context.** `pbix_evaluate_dax` silently applied EVERY page's persisted default slicer selections whenever `filter_context` was empty, with no opt-out — diverging from the service, which scopes a slicer's default to its own page (repro: ai_report's page-1 "Open Pipeline Value" card shows 273,900 in the service; the tool returned 151,200 because a page-2 slicer default leaked in) — while `pbix_evaluate_dax_per_dimension` applied none, so the same measure gave different numbers depending on the tool. Both tools now share one machinery and two parameters: `apply_default_filters` (False = the raw, truly unfiltered model; defaults keep each tool's historic behavior — True for `pbix_evaluate_dax`, False for `pbix_evaluate_dax_per_dimension` — pass explicitly for identical behavior) and `page_index` (-1 = merge all pages, historic; >= 0 = ONLY that page's slicers, matching service semantics — `page_index=0` reproduces the page-1 card's 273,900 exactly). An out-of-range `page_index` errors loudly (`Page index N out of range`) instead of silently evaluating the raw model, and PBIR-format reports honor `pages.json`'s `pageOrder` so page indices are deterministic. Contract documented in `docs/tool-contracts.md`.
- **`pbix_datamodel_set_measure_category` — set or CLEAR a measure's DataCategory, no expression required.** Clearing was previously impossible through the tools (empty string means "leave unchanged" on `pbix_datamodel_modify_measure`), forcing a raw-SQL hatch. The dedicated setter touches nothing but `Measure.DataCategory`; empty clears (SQL NULL). `pbix_datamodel_modify_measure`'s `new_expression` is now optional too — changing only a format string or DataCategory no longer forces the caller to read and re-send the current expression (a no-op call errors with `NOTHING_TO_CHANGE`).
- **`pbix_reference_public_visual` accepts hyphenated marketplace GUIDs** (legacy `PBI_CV_<GUID>` AppSource ids). The GUID is registered VERBATIM — never normalized — because the service resolves certified visuals by exact GUID. Consumers that mirrored the old letters/digits/underscores rule can widen to `[A-Za-z0-9_-]`.
- **Parameter-level sort-authoring contract** in `docs/tool-contracts.md`: accepted `sort_by` forms, direction values, and error modes for `pbix_set_visual_sort` and `pbix_add_visual`'s `sort_by` (previously only readable from the `attach_order_by` source).

### Fixed
- **Literal-first arithmetic around a bare column reference no longer evaluates to BLANK (ai_report's `Total Sales` was 0).** The engine's column-reference pattern let the "table name" swallow leading operands and operators, so `1 - Sales[Discount]` parsed as column `Discount` of a table named `"1 - Sales"` — a bogus reference that made the whole expression BLANK. Every literal-first shape was affected (`1 - T[C]`, `1 + T[C]`, `2 * T[C]`, `0 - T[C]`, and parenthesized forms like the idiomatic `SUMX(Sales, Quantity * UnitPrice * (1 - Discount))`); column-first forms (`T[C] - 1`) worked, which is why the bug hid. The table part of a reference must now be a quoted name or a plain identifier (no operators), and a unary-minus handler covers `-T[C]` / `-[Measure]` (applied only after binary splitting, so `-a + b` still parses as `(-a) + b`). ai_report's `Total Sales` now evaluates to 75,567.29 — matching the service's "$76K" card. New `TestLiteralFirstArithmetic` suite in `tests/test_dax_engine.py`.

## [0.9.25] - 2026-07-23

Rich-content authoring for service-portable reports (AppSource/Deneb references, ImageUrl measures, Desktop-complete field parameters, SVG measure codegen); bracketed measure names evaluate correctly (were silently BLANK); typed DAX errors; opt-in visual-level sort authoring.

### Added
- **`pbix_reference_public_visual` — reference a certified AppSource visual (e.g. Deneb) by GUID, zero file payload.** Registering a public visual needs only its GUID in the layout's top-level `publicCustomVisuals` array: no `Report/CustomVisuals/` folder, no `.pbiviz`, no `[Content_Types].xml` changes, `resourcePackages` untouched — the Power BI service resolves certified visuals from AppSource for report consumers (service-verified on app.powerbi.com against a certified-only tenant: a pbix-mcp-authored file with `publicCustomVisuals: ["deneb7E15AEF80B9E4D4F8E12924291ECE89A"]` + `objects.vega` string-Literal spec properties auto-loaded Deneb and drew the Vega-Lite chart). Validates the GUID (alnum/underscore, same rule as the manifest reader), dedupes, and returns the resulting array; `pbix_remove_custom_visual` de-registers reference-only entries as-is. The full Deneb authoring recipe (dataset role + `objects.vega`) is documented in `docs/rich-content.md` and covered end-to-end in `tests/test_rich_content.py`.
- **`DataCategory` authoring — and it now SURVIVES rebuilds.** `pbix_datamodel_add_measure` / `pbix_datamodel_modify_measure` gained a `data_category` / `new_data_category` parameter, and `pbix_set_table_data` accepts an optional per-column `"data_category"` key (threaded through every builder `[Column]`/`[Measure]` INSERT) — e.g. `"ImageUrl"` so table/matrix cells (and the service) render `data:image/svg+xml;utf8,...` strings as live vector images. Critically, `_rebuild_datamodel`'s collection queries now read `Column.DataCategory` and `Measure.DataCategory` back (previously EVERY rebuild-based edit — set_table_data, add_relationship, add_field_parameter, … — silently reset them to NULL, so a category set via `pbix_datamodel_modify_column` was one edit away from disappearing). Regression-tested in `tests/test_rich_content.py`.
- **`pbix_datamodel_add_field_parameter` now authors the COMPLETE Desktop field-parameter shape** (previously a plain 3-column lookup table Desktop did not treat as a field parameter — no field-swapping in visuals). Diffed field-by-field against Desktop-authored ground truth (`test_corpus/Ecommerce_Conversion.pbix`, two genuine field parameters): calculated partition (Type=2) holding the `{("Display", NAMEOF('Table'[Field]), n), …}` tuple set with full static VertiPaq storage (exactly what Desktop's own files physically contain), the `ParameterMetadata` = `{"version":3,"kind":2}` ExtendedProperty on the hidden Fields column (first `[ExtendedProperty]` writer in the engine), calc-table columns (Type=4, `ExplicitDataType` automatic + real `InferredDataType`, `SourceColumn [Value1..3]`), display column sorted by the hidden Order column (`SortByColumnID`), Order column `FormatString '0'`, `Table.SystemFlags=2`, and the display→Fields group-by wiring (`RelatedColumnDetails`/`GroupByColumn`). Field refs are normalized to the quoted `'Table'[Field]` form `NAMEOF()` evaluates to and validated against the model (a typo'd ref fails loud). **Field parameters survive rebuild-based edits**: `_rebuild_datamodel` recognizes the shape (calculated partition + parseable NAMEOF tuple set), rebuilds the static rows, and re-stamps the metadata — instead of refusing the whole edit with `MODEL_EDIT_UNSUPPORTED` as it would for genuine calculated tables (which still refuse). Multiple field parameters coexist.
- **`pbix_svg_measure` — DAX codegen for SVG data-URI image measures** (new `svg_measures.py`): `data_bar`, `bullet`, `pill`, `icon_updown`, and `sparkline` templates emit DAX evaluating to `data:image/svg+xml;utf8,<svg …>` strings — with `DataCategory='ImageUrl'` these render as live, filter-context-aware vector images in table/matrix cells everywhere (Desktop, service, PDF export, subscriptions) with zero custom visuals. Templates are hygiene-hardened: colors percent-encoded (`%23…` — a raw `#` truncates a utf8 data URI), utf8 never base64 (base64 wastes ~33% of the ~32k AS text budget), single-quoted SVG attributes (no DAX quote doubling), `FORMAT(INT(…), "0")` interpolation so comma-decimal locales can never corrupt coordinates, and XML-escaping of pill text. Turnkey mode (`alias` + `measure_name`) authors the measure directly with `DataCategory='ImageUrl'`. Every template's generated DAX evaluates correctly in pbix-mcp's own engine (asserted in `tests/test_rich_content.py`, including sparkline point normalization).

- **Visual-level sort authoring (`prototypeQuery.OrderBy`) — opt-in, matching Desktop.** No sort was ever authored, so the Power BI service fell back to category-ascending query order on every pbix-mcp visual (Desktop's usual value-descending bar/column default comes from Desktop *authoring* an OrderBy, not from the renderer). Now: `pbix_add_visual` gained `sort_by` / `sort_direction` parameters; a new `pbix_set_visual_sort` tool sets or clears the sort on an existing visual (recompiling its `query`/`dataTransforms` so the compiled query carries the same clause); the from-scratch builder accepts `"sort"` (a field name, or `{"by": ..., "direction": "asc"|"desc"}`) on a visual config; and the PBIR export translates an OrderBy into a real `sortDefinition` (previously always the empty `isDefaultSort` marker). `sort_by` accepts bare names, `[Field]`, `'Table'[Field]`, `Table[Field]`, or `Table.Field` queryRefs; sorting by a bare numeric value-role column follows the implicit-Sum rewrite into the OrderBy `Aggregation` expression, exactly as Desktop stores such sorts. An unknown sort field fails loudly, listing the visual's available fields, rather than silently dropping the requested sort. New `attach_order_by` in `report_binding.py`; tests in `tests/test_report_binding.py` and `tests/test_found_issues.py`.

### Fixed
- **Desktop calculated tables and field parameters now READ correctly.** `read_table_from_abf` excluded calc-table columns (Type=4), so Desktop-authored field parameters and calculated tables read back EMPTY (`pbix_get_table_data`, the DAX engine's context, rebuild row preservation). The column filter now includes Type=4, and `ExplicitDataType=1` ("automatic", Desktop's calc-table convention) falls back to `InferredDataType` for correct typing. Verified against the Desktop corpus: `Ecommerce_Conversion.pbix`'s `KPI_#1` / `M-W-D` field parameters decode their exact NAMEOF tuple rows. Note: on models whose DATE table is itself a calculated table (e.g. the Agents corpus dashboard), time-intelligence measures that previously evaluated fast-but-BLANK (the date table was invisible) now evaluate for real — bulk evaluation on large fact tables is correspondingly slower (each measure stays bounded by the engine's ~20s wall-clock budget, tunable via `PBIX_DAX_MAX_SECONDS`).
- **DAX-style measure references (`[Pipeline Value]`, `'SalesPipeline'[Pipeline Value]`) no longer silently evaluate to BLANK in `pbix_evaluate_dax` / `pbix_evaluate_dax_per_dimension`.** The tools split the `measures` argument on commas and kept brackets/table qualifiers verbatim, while the measure store is keyed by bare names — and the engine returns BLANK for an unknown measure by design, so the standard DAX forms returned `(null)` for every row with `success: true` and no warning. Measure references are now normalized to bare names before lookup (all of `Name`, `[Name]`, `'Table'[Name]`, `Table[Name]` resolve identically; commas inside `[...]`/`'...'` no longer split a name), and a genuinely unknown measure raises a typed `DAX_MEASURE_NOT_FOUND` error with close-match hints ("did you mean: Pipeline Value?") instead of being indistinguishable from a real BLANK. Verified against the Power BI service rendering of the report that surfaced the issue: bracketed and table-qualified forms now return the service-exact per-dimension values. New regression tests in `tests/test_found_issues.py`.
- **An invalid `dimension` argument now returns the intended parse message instead of a masked AttributeError.** `pbix_evaluate_dax_per_dimension`'s `except ValueError` handler read `e.message`/`e.code`, which plain `ValueError` lacks — the handler itself raised `AttributeError: 'ValueError' object has no attribute 'message'` and the tool reported that with a double traceback. `DimensionRef.parse` now raises a typed `DimensionParseError` (code `DIMENSION_INVALID`; both a `PBIXMCPError` and a `ValueError`, so pre-existing `except ValueError` callers keep working), and the handler falls back to `str(e)` for any untyped ValueError. This was the only one of the 103 `ToolResponse.error(e.message, e.code)` sites sitting under an untyped exception.

## [0.9.24] - 2026-07-21

DAX engine: column refs inside iterator scalar expressions (CONCATENATEX & friends) now evaluate like Desktop — the natural data-driven HTML form works.

### Fixed
- **Bare column references inside an iterator's COMPOUND scalar expression now resolve against the current row.** `CONCATENATEX(VALUES(T[C]), T[C] & ": " & FORMAT(...), " | ")` — the most idiomatic way to build data-driven HTML lists — stringified the column identifier (`('Sales', 'Region')`) instead of the row value; the same leak hit `FORMAT(T[C], ...)` and `CONCATENATE(T[C], ...)` arguments. The row binding is now visible to sub-expression evaluation for every iteration shape (full-row `SUMX` dicts, single-column `VALUES`/`ALL` dicts, and extension columns). **Desktop cross-checked 1:1**: a real card bound to the natural-form measure renders exactly the engine's output.
- **`SELECTCOLUMNS`/`ADDCOLUMNS` extension columns (`[r]`, `[lbl]`) resolve inside iterators** instead of evaluating to blank (measures still take precedence, matching DAX name resolution).
- **A plain aggregate typed directly in an iterator's scalar expression sees the OUTER filter context** — `SUMX(VALUES(T[C]), ... SUM(T[Val]) ...)` used a row-sliced sum where Desktop (correctly) uses the un-transitioned grand total; row context does not filter a plain aggregate. `CALCULATE(SUM(...))` and measure invocations still transition (row-sliced), so existing measures are unchanged — the transition now happens exactly at CALCULATE / measure boundaries, which also stops the row context leaking INTO `CALCULATE` (a column ref there resolves against the filter context, so `CALCULATE(SELECTEDVALUE(T[C]))` keeps working). Plain aggregates (`SUM`/`AVERAGE`/`MIN`/`MAX`/`COUNT`/`DISTINCTCOUNT`) now parse their column-reference argument syntactically, as DAX defines them.
- **`SUM` over an empty selection returns BLANK** (Desktop semantics) instead of `0` — `ISBLANK(...)` finally fires; use `COALESCE(x, 0)` where a zero is wanted.
- **`NOW()` / `TODAY()` / `UTCNOW()` implemented** (were unsupported → blank), with DAX date patterns (`yyyy`, `MM`, `dd`, `HH:mm:ss`, …) in `FORMAT`.
- **Scientific-notation literals (`1e6`, `2.5E-3`) parse** (were null).
- **`VALUES()` iterates in data order** (order-preserving dedup) instead of Python hash-set order, and **`CONCATENATEX` honors its `orderBy`/`ASC|DESC` arguments** (previously silently ignored — output order was nondeterministic).
  New `TestIteratorRowContext` suite in `tests/test_dax_engine.py`; the empty-`SUM` expectations in three older tests were updated to the Desktop-verified BLANK semantics.

## [0.9.23] - 2026-07-20

HTML visuals now cross-filter the rest of the report (native selection), full docs, and a pure-Python example.

### Added
- **HTML visuals can now cross-filter / cross-highlight the rest of the report — like a native visual.** Because the bundled `PBIX HTML` visual renders in its own DOM (not a sandboxed iframe), it can drive Power BI's selection manager directly. `pbix_add_html_visual` gained a `category_field` argument (`Table[Column]` / `Table.Column` / `Column`): bind a column, tag clickable HTML/SVG elements with `data-pbix-select="<category value>"`, and clicking one selects that value's identity and filters every other visual bound to the same field. Ctrl/Cmd-click multi-selects, clicking the background clears, right-click opens the report context menu, and unselected regions dim. The bundled visual was rebuilt (v1.2.0.0, same GUID) with an `ISelectionManager` + a `category` data role; the binding compiler now emits the category grouping alongside the String-typed content measure. (Filtering *into* an HTML visual already worked — the content is a live DAX measure that re-evaluates under cross-filter.) Desktop-verified end-to-end: clicking a tagged SVG region filters a native matrix + table, unselected regions dim, and clicking the visual's background (or re-clicking the selected region) clears the selection and restores the page.
- **New guide `docs/html-visuals.md`** covering the visual, all four HTML tools, DAX-authoring rules, the template library, cross-filter, and the pure-Python (no-MCP) usage path — plus a runnable `examples/html_visual_pure_python.py`. Every `@mcp.tool()` is a plain importable Python function, so the whole feature is usable from pure Python (as the OpenBI runtime uses it), not only via an MCP client.

### Fixed
- **Cartesian charts fed a plain numeric column on their value axis now render (were empty — Desktop-verified fix).** A column/bar/line/area/pie chart that binds a raw numeric column (not a measure) to `Y` / `Values` / `Y2` / `Size` rendered in Power BI Desktop as title + axes only — no bars/line. Root cause: Desktop re-derives the live data query from `config.singleVisual.prototypeQuery` + `projections`, and the prototype carried the bare column as a group-by dimension — so there was nothing to plot regardless of the compiled `query`. The fix mirrors what Desktop's own field well stores when you drop a numeric column on a value axis (ground truth: AI Sample `barChart`):
  - **Prototype rewrite** — new `apply_implicit_aggregations` (called by `compile_visual_binding`, which now mutates `singleVisual`): every bare `Column` select projected into a value role becomes an `Aggregation` (`Sum` for `Int64`/`Double`/`Decimal`, `CountNonNull` otherwise) named `Sum(Entity.Property)` per Desktop's queryRef convention, with the projection repointed — in the prototype, the compiled query, and `dataTransforms` alike. `pbix_add_visual` and the from-scratch page builder now serialize the config **after** compiling so the rewritten prototype is persisted. Explicit `Aggregation` selects are handled; flat grids (`table`/`tableEx`) and slicers show raw values; measures are untouched.
  - **Model metadata** — the builder wrote `SummarizeBy = 2` (None, "don't summarize") for **every** column; numeric columns (`Int64`/`Double`/`Decimal`) now get `SummarizeBy = 1` (Default → Sum), matching Desktop's default and the corpus.
  Verified in real Power BI Desktop: a measure chart and a raw-`Double`-column chart side by side now render identical bars. This fixes the most common OpenBI cartesian chart, which routinely binds `Table.Amount` directly. New `TestValueColumnAggregation` + `TestSummarizeByDefaults` (`tests/test_report_binding.py`).
- **CI is green again.** The 0.9.22 push failed CI at `ruff check src/ tests/` — the new test files' import blocks were unsorted (local pre-commit only linted `src/`). Fixed the imports and tightened three mypy `no-any-return` / unknown-callable warnings in the new HTML code (the `TEMPLATES` registry is now typed). No behavior change.

## [0.9.22] - 2026-07-20

Custom HTML / CSS / SVG visuals — pbix-mcp's own custom visual + turnkey tools — plus two serious data-corruption fixes in the metadata splice that affected every measure/metadata edit.

### Added
- **Create, view, and edit custom HTML / CSS / SVG (and inline JS) visuals in a report.** pbix-mcp now ships its **own** Power BI custom visual (`PBIX HTML`, `src/pbix_mcp/assets/pbix_html_visual/`, ~3.8 KB, no third-party dependency) that renders a `content` data-role string as HTML inside Power BI's sandboxed visual iframe — build KPI cards, SVG charts / gauges / maps, badges, custom tables, or anything HTML/CSS/SVG can express. New tools:
  - `pbix_add_html_visual(alias, page_index, html=|dax=|template=, x, y, width, height, measure_name=, measure_table=, css=, pbiviz_path=)` — one call embeds the visual, authors the DAX measure that produces the HTML (static literal or a data-driven `FORMAT()`/`&` expression), and places a fully data-bound container (the content measure is bound as `String`). Pass your own HTML-rendering `.pbiviz` via `pbiviz_path` if you prefer.
  - `pbix_get_html_visual(alias, page_index=-1)` — list the report's HTML visuals with position, bound measure, and decoded HTML.
  - `pbix_set_html_visual(alias, page_index, visual_index=|measure_name=, html=|dax=|css=)` — edit an existing HTML visual's content.
  - `pbix_html_template(kind, spec_json)` — render professional, HTML-escaped snippets (`kpi_card`, `bar_chart`, `gauge`, `table`, `progress`, `badge`) from `src/pbix_mcp/html_templates.py`; usable directly or via `pbix_add_html_visual(template=...)`.
  Verified in real Power BI Desktop: a showcase page of six HTML visuals (gradient KPI card with SVG sparkline, SVG bar chart, SVG radial gauge, inline SVG map, styled HTML table, and a live data-driven DAX card) all render.
- **`pbix_add_custom_visual` rewritten to embed any `.pbiviz` correctly.** It now reads the visual GUID from the package manifest (never fabricates one), extracts the `.pbiviz` verbatim into `Report/CustomVisuals/<guid>/`, and registers the GUID in the top-level `publicCustomVisuals` array — exactly how Power BI Desktop embeds a custom visual. The previous implementation registered a non-canonical `resourcePackages` type-7 entry, named the folder by an alias, and generated a random GUID, so Desktop silently dropped the visual. `pbix_remove_custom_visual` now de-registers from `publicCustomVisuals`.

### Fixed
- **Metadata splice no longer corrupts the data model on measure / metadata edits (DBCC load failure).** `splice_metadata_in_abf` shifts every embedded-file offset after the metadata by the metadata's size change. It did this with a `buf.find(old)+replace` loop over the mutating buffer: when two files' offsets differed by exactly the size delta — near-certain when a page-aligned metadata block grows (e.g. +8192 bytes) — `find` matched the value it had just written, double-shifting one entry and leaving another **stale**. The stale offset then overlapped its neighbour and Power BI Desktop failed to load the model ("Database consistency checks (DBCC) failed while checking the data segments"). The shift is now a single-pass `re.sub` over the VirtualDirectory text, which never re-matches its own replacements and also tolerates offsets that gain or lose digits. This affected `pbix_datamodel_add_measure` / `modify_measure` and any metadata edit that grew the file — a latent silent-corruption bug since 0.x.
- **Metadata splice no longer writes a truncated VirtualDirectory size.** The spliced BackupLogHeader `DataSize` was measured from the position of `<VirtualDirectory>` (which for a UTF-16 VDir sits after the 2-byte byte-order mark) while `m_cbOffsetHeader` points at the BOM, so `DataSize` came out 2 bytes short and the reader dropped the closing `>` of `</VirtualDirectory>`. Power BI tolerated it, but pbix-mcp's own reader raised `unclosed token` on the next read after an edit. `DataSize` is now sized from the header offset so it is byte-exact. New regression tests in `tests/test_abf_splice_datasize.py` (VDir close-tag reachable + no offset overlaps) and `tests/test_html_visuals.py`.

## [0.9.21] - 2026-07-20

Custom report themes now apply in Power BI Desktop (chart colors were wrong).

### Fixed
- **A custom theme set via `pbix_set_theme` now actually applies in Power BI Desktop.** Previously the report opened but charts rendered in Desktop's DEFAULT palette (teal) instead of the theme's colors. The theme was registered by overwriting `config.themeCollection.baseTheme.name` with the custom theme's name and writing the JSON into `SharedResources/BaseThemes`. But Power BI treats `baseTheme.name` as a BUILT-IN theme id (e.g. `CY24SU10`), so a custom name like "Modern Blue" fails to resolve and Desktop silently falls back to its default palette (`themeCollection: {}` renders in that same teal). `pbix_set_theme` now registers the theme the way Desktop expects: a `customTheme` OVERLAY on a valid built-in `baseTheme`, with the theme JSON in a `RegisteredResources` package (item type 201, not `BaseThemes`), and fills the report-level `config` keys Desktop expects (`version`, `activeSectionIndex`, `linguisticSchemaSyncVersion`). Power BI resolves the built-in base by name, so no base-theme file is shipped. Verified against real Power BI Desktop 2.152: a "Modern Blue" (`#2E86DE`) theme now renders its charts in that blue, matching Desktop-authored custom-themed reports (Cars Sales, Briqlab). `pbix_get_theme` now also reads themes from `RegisteredResources`.

## [0.9.20] - 2026-07-20

Correct data bindings for matrix / pivotTable and slicer visuals (report render fix).

### Fixed
- **Matrix, pivotTable, and slicer visuals now render in Power BI Desktop instead of throwing "An error occurred while rendering the report."** `compile_visual_binding` emitted a single flat `Primary` grouping (plus `isPivoted`) for every visual type. That is correct for cards / cartesian / pie / table, but wrong for a matrix — which crosses **rows on the Primary axis against columns + values on a Secondary axis** — and for a slicer, which needs `IncludeEmptyGroups` and an empty `Window` (no `Count`) DataReduction. The compiler now special-cases both: a matrix/pivotTable with a column field emits `Primary.Groupings` (one per row level) + `Secondary.Groupings` (`[columns…, values…]`) with a dual DataReduction and no `isPivoted` (a matrix with only rows collapses to a flat table grouping); a slicer emits the empty-window / `IncludeEmptyGroups` binding with active data roles. Byte-exact to Desktop-authored ground truth (Matrix Bubble Chart, Contoso IBCS, AI Sample); card / pie / table bindings are unchanged. Verified against real Power BI Desktop 2.152: a Titanic 3-pager whose matrix, three slicers, and data-bar table previously errored now render fully. New matrix/slicer tests in `tests/test_report_binding.py`.

## [0.9.19] - 2026-07-20

Reports built by pbix-mcp now load in Power BI Desktop (critical report-layer fix).

### Fixed
- **Reports no longer fail Power BI Desktop's report loader ("Failed to load the report").** Every data visual in the legacy `Report/Layout` was written with only `config` (projections + prototypeQuery) and no compiled data binding. As soon as a report carried report-level `config` / visual `objects` (as OpenBI/pbix-mcp reports do), Desktop's report loader rejected the whole report — no pages, no visuals — even though the data model opened cleanly and `pbix_doctor` was all-green (it never runs the report loader). Both visual-creation paths — the from-scratch builder (`add_page`) and `pbix_add_visual` — now compile the `projections` + `prototypeQuery` into the `query` (a `SemanticQueryDataShapeCommand` with its `Binding`) and `dataTransforms` (projectionOrdering / queryMetadata / visualElements / selects) that the loader requires, plus `filters` and `z`. Textboxes, shapes, images, and buttons (no data binding) are left untouched. Verified against real Power BI Desktop: an OpenBI Titanic report that failed to load now loads once the compiled bindings are present; the binding structure matches Desktop-authored reports (sales_demo / GeoSales) field-for-field. New `src/pbix_mcp/report_binding.py` + `tests/test_report_binding.py`.

## [0.9.18] - 2026-07-19

Decodes multi-segment columns — large import tables no longer read back truncated.

### Fixed
- **Columns spanning more than one VertiPaq segment now decode in full (data-integrity).** Power BI splits an import table into ~1,048,576-row segments; the `.idf` concatenates one block per segment and the `.idfmeta` carries one CS/SS block per segment, each with its OWN bit width and `min_data_id`. The reader decoded only the first segment, so any table beyond ~1M rows silently read back truncated to its first ~1,048,576 rows (and would be rebuilt truncated on a metadata edit). `decode_idf` now walks every segment, and a new `decode_idfmeta_segments` supplies the per-segment `(bit_width, rle_base, bitpacked_add)` so each segment is shifted onto the shared dictionary's global scale. Verified against `pbixray` on a real 12,627,608-row / 13-segment fact table: every column (dates, keys, quantities, currency) matches exactly across all segments. Single-segment columns and our own encoder's output are unaffected. New multi-segment unit tests plus a `PBIX_TEST_SAMPLES`-gated full-length corpus check.

## [0.9.17] - 2026-07-19

Correctly reads Power BI Desktop-authored column data — value encoding, RLE, and multi-group bit-packed segments — instead of returning blanks.

### Fixed
- **Value-encoded numeric columns now decode to their real values (data-integrity).** Columns Power BI stores with VALUE encoding (`DictionaryStorage.Type=2`, no external dictionary — common for integer keys/counts and dates) have no dictionary to look up, so the reader previously returned them entirely blank. They are now reconstructed as `value = (data_id + BaseId) / Magnitude`, with OLE-date semantics for DateTime (e.g. serial `45748` → `2025-04-01`). Verified byte-for-byte against `pbixray` across the whole test corpus (including a 199,999-row fact table).
- **RLE-compressed segments decode correctly.** An RLE run stores the ABSOLUTE `data_id`, whereas the bit-packed sub-segment stores values relative to the segment minimum. The reader now re-bases RLE runs onto the same scale, so single-value / low-cardinality columns (e.g. a `Returned = "Yes"` flag) no longer read as all-blank.
- **Segments with multiple bit-packed groups decode correctly.** Each successive bit-packed marker is `0xFFFFFFFF` minus the number of bit-packed values already consumed (not a fixed `0xFFFFFFFF`), and all bit-packed values are stored contiguously in the sub-segment. The reader now tracks the running offset and slices the flat bit-packed array by value (not by whole words), fixing columns that previously decoded correctly for the first ~N rows and then turned to garbage/blank.

- **Tables whose name contains `_`, `-`, or `#` no longer read back as zero columns (data-integrity).** Power BI sanitizes those characters to spaces in the internal ABF file paths (`fct_Orders` → `fct Orders (14).tbl`), but the reader matched files by the raw table name, so every column of such a table failed to match and the table silently returned no columns and no rows — and, worse, was rebuilt empty on any metadata edit. File matching now keys on the numeric `(TableID).tbl` token, which is stable across name sanitization and the generic `Table (id)` / `Parameter (id)` folders that calc / field-parameter tables use. (This also unblocked verification of the currency/scale value-encoded columns, `Magnitude` 10000 and 1e9, which live in such tables.)
- **Nullable value-encoded columns with RLE runs decode consistently.** The value-encoding reconstruction now uses the same re-based scale (`min_data_id − null_offset`) that the segment decode used, so an RLE run in a nullable value-encoded column no longer comes out off by `1/Magnitude`, and the reserved blank slot decodes to NULL.

Net effect: reading Desktop-authored models (`pbix_get_table_data`, `pbix_query_table`, CSV/PBIP export, and the data-preserving rebuild path) now returns the true values for every column encoding present in the corpus — hash (bit-packed / RLE / mixed) and value, across all 162 data columns of the 19-table test corpus — where a quarter of columns (and whole tables) previously came back blank or wrong. New `tests/test_vertipaq_decode.py` locks in the segment-decode format with crafted-byte unit tests plus a `pbixray` corpus cross-check that also fails if any table decodes to zero columns.

Known limitation (pre-existing, tracked): a column whose data spans more than one VertiPaq segment (import tables beyond ~1M rows) still decodes only its first segment. Not present in the test corpus; addressed separately.

## [0.9.16] - 2026-07-19

Hardens the reader against silent data loss on high-cardinality String columns, and fixes a `pbix_doctor` mislabel.

### Fixed
- **Reading a large/compressed String column can no longer silently vanish (data-integrity).** When a String column's character store crosses the 8192-char threshold it is stored as a Huffman-compressed dictionary (the format Power BI Desktop itself uses). The reader (`read_table_from_abf`) previously wrapped every per-column decode in a bare `except … → None`, so if a column's VertiPaq files *existed* but could not be decoded (e.g. the `xmhuffman` dependency missing, or a corrupt store), the column was silently returned blank and then dropped from the result entirely — indistinguishable from "no data". The reader now **fails loud**: it collects such columns and raises `InvalidPBIXError` naming each column, the decode stage, and the underlying reason (with an install hint when `xmhuffman` is the cause), instead of returning partial data. Legitimately data-less columns (calculated columns, RowNumber) are still excluded quietly as before. Verified that the D: compressed encoder itself is correct and Power BI Desktop reads it: a model with an 891-distinct Titanic-style `Name` column (commas/parens, latin-1) opens clean, and encode↔decode round-trips exactly across the boundary and at high cardinality (uncompressed, single-page, and multi-page compressed; ASCII, latin-1 accents, CJK, and emoji). New `tests/test_large_string_roundtrip.py` locks in the round-trip and the fail-loud behaviour.
- **A metadata edit can no longer silently rebuild a table with no rows (data-integrity).** The rebuild path (`_rebuild_datamodel`, `_modify_metadata_sqlite`) re-reads every table's VertiPaq data to carry it through a rebuild. It previously wrapped that read in `except Exception: … rows=[]`, so if any column failed to decode the *entire table* was rebuilt empty and written to disk — turning an unrelated edit (`add_relationship`, `set_table_data`, …) into whole-table data loss. It now aborts loudly (raises `InvalidPBIXError` naming the table) and leaves the file on disk unchanged, so no data is destroyed. (Found by adversarial review of the reader fix above.)
- **`pbix_doctor` no longer mislabels imported tables as calculated.** The "Calculated tables" check keyed on `Partition.Type = 4`, which is a plain M/import partition — so every imported table was counted as calculated (e.g. a 3-table import model reported "3 calculated tables"). It now keys on `Partition.Type = 2` (the actual DAX calculated-table marker, `DATATABLE`/`GENERATESERIES`/`CALENDAR`/…) and lists the table names.

## [0.9.15] - 2026-07-19

Fixes a critical DBCC failure on all-NULL columns, and a stale default-slicer-filter cache in DAX evaluation.

### Fixed
- **All-NULL columns no longer break the model on open (CRITICAL).** A column whose rows all exist but are every one blank (zero real dictionary entries — e.g. an imported column that is entirely empty) produced a segment that declared its real-value data-id range as `[3,3]`, i.e. it claimed a real value at data_id 3 that the empty dictionary never provides. Power BI Desktop's DBCC consistency check rejected such a model on open (`PFE_XM_DBCC_COLUMN_DICTIONARY_FAILED` → "Something went wrong", model won't load). The encoder now recognizes the all-NULL case and declares `min_data_id = max_data_id = 2` — the sole data-id physically stored is the blank id 2 (= dictionary `BaseId`), which also drives `DictionaryStorage.LastId = 2` to match the empty dictionary. Ordinary nullable columns (some values + some NULLs) were already consistent and are byte-for-byte unchanged. Verified end-to-end against Power BI Desktop: the pre-fix model fails to open, the fixed model loads clean, and the data round-trips (all-NULL → all blank, partial-NULL preserved). New `tests/test_nullable_hierarchy.py` locks in the segment/dictionary/attribute-hierarchy consistency invariants and fails if the fix is reverted.
- **DAX evaluation no longer applies a stale default slicer selection.** `pbix_evaluate_dax` auto-applies the report's default slicer filters when no explicit `filter_context` is given. Those come from the report layout, which can change (`pbix_set_filters`, slicer edits, `pbix_set_layout_raw`, …) without the cached DAX context being rebuilt — so a previous selection kept leaking into later evaluations. The default filters are now re-derived fresh from the current layout at evaluation time, so a slicer change is reflected on the very next evaluate. New `tests/test_default_filters_cache.py` covers both directions (stale filter ignored, fresh filter applied).

## [0.9.14] - 2026-07-18

Extends datamodel editing to models with a measures table, and closes a guard gap.

### Added
- **Measure-only container tables are preserved through a rebuild.** A table that holds only measures (no data columns — e.g. a `_Measures` table) is now re-emitted as a RowNumber-only empty table + its measures, so the rebuild-based tools (`add_relationship`, `remove_relationship`, `set_table_data`, …) work on the very common "a measures table + import tables" model shape instead of being refused. Verified: a measures-table model rebuilds (adding a relationship) with the container + measures intact and opens in Power BI Desktop with no repair.

### Fixed
- **The rebuild guard now also catches DAX calculated columns** (`Column.Type=2` on a normal table), not just calculated tables and calc-table columns. Previously a model with a calculated column but no calculated table slipped past the guard, and a rebuild would silently **drop the column** (breaking any relationship on it). Such models are now correctly refused with `MODEL_EDIT_UNSUPPORTED` naming the table.

### Note
- Full support for editing models with **calculated tables** (`DATATABLE`/`GENERATESERIES`) or **calculated columns** still needs verbatim VertiPaq preservation (copying the computed column bytes through untouched — decode-and-re-encode is not lossless for value-encoded columns); those models remain guarded. The surgical tools (`add_measure`/`modify_measure`/`remove_measure`/`modify_column`) continue to work on all models.

## [0.9.13] - 2026-07-18

Completes relationship fidelity: genuine one-to-one relationships (the last item
0.9.10 downgraded).

### Added
- **Full one-to-one relationships.** A 1:1 is now stored exactly as Power BI Desktop does — `FromCardinality=ToCardinality=1`, cross-filter forced to Both, and **two** R$ join indexes: a forward index on the From table and a reverse index on the To table (`RelationshipStorageID` + `RelationshipStorage2ID`, two `RelationshipStorage` rows sharing one GUID name, two R$ system tables). 0.9.10 could only downgrade a 1:1 to a bidirectional many-to-one because a 1:1 written with a single index fails to load (`TMProxyRelationship::GetStorage2ID`); the reverse index fixes that. Authoring (`cardinality="OneToOne"`) and preservation-through-rebuild both produce the true 1:1.

### Changed
- The R$ index construction was factored into a reusable closure so the forward and reverse indexes share one code path. The refactor was proven **byte-identical** for every non-1:1 relationship (a canonical logical hash of the metadata is unchanged), so nothing about existing relationships moved.

### Verified
- Ground truth re-confirmed against a Desktop-authored 1:1 (two R$ tables; the reverse is a faithful mirror of the forward with From/To swapped). The built 1:1 structure matches it field-for-field.
- Round-trip: a pure-1:1 model and an all-five-relationship-types model both open in Power BI Desktop with **no repair** (verified via process/window inspection). A 1:1 survives a datamodel rebuild with both indexes intact.
- Two independent adversarial code reviews of the builder diff found no correctness defects. Full fast suite: 304 passed; corpus (non-Agents): 135 passed; 0 failures; ruff clean; mypy 163.

## [0.9.12] - 2026-07-18

### Added
- **Wall-clock guard for the DAX engine.** The existing eval-*call* budget bounds runaway/expansion measures, but it misses an O(dimension × fact) measure — e.g. `RANKX(ALL(DimEmployee), …)` where each of hundreds of dimension rows filter-scans a 200K-row fact: few eval calls, but enormous wall-clock. Such a measure previously hung the tool (and the Agents cross-report test). Each outermost measure is now bounded by a wall-clock deadline (default 20s, override with `PBIX_DAX_MAX_SECONDS`); on exceed it degrades to BLANK instead of hanging. The guard lives on the engine (not the context) so it survives the per-row sub-contexts iterators create, and it is throttled to keep the clock read off the hot path. Ordinary measures are unaffected; a slow measure computes correctly when given enough budget. (No change to any public-corpus result.)

## [0.9.11] - 2026-07-18

Clears the remaining OpenBI-reported issues (#2, #5b, and the #1 coverage gap),
hardens the DAX engine, and makes DataModel edits fail safely on models the
rebuild can't reproduce.

### Fixed
- **Bookmarks no longer write `display.mode = "visible"`** (OpenBI #2). Power BI's `display.mode` enum is `hidden` | `maximize` | `spotlight` | `elevation` — there is no `"visible"`; a visible visual is expressed by *omitting* mode. A hide-some-visuals bookmark previously stamped `mode:"visible"` on the untouched visuals, which could make Desktop ignore the block or mishandle Selection-pane state. Hidden visuals now get `{"display":{"mode":"hidden"}}` and visible ones a bare `{"singleVisual":{}}`.
- **`CALCULATE` now consumes `USERELATIONSHIP` and `CROSSFILTER`** (OpenBI #5b). They previously parsed to marker tuples that `CALCULATE` never applied, so both were silent no-ops. `USERELATIONSHIP(col1, col2)` now activates that relationship for the wrapped expression (deactivating the sibling active one on the same table pair — role-playing date tables work), and `CROSSFILTER(col1, col2, None|OneWay|Both)` overrides the cross-filter direction.
- **`TOPN` / `RANKX` (and `ADDCOLUMNS`, `SELECTCOLUMNS`, `GENERATE`, `GENERATEALL`, `CONCATENATEX`) over a bare table no longer return BLANK.** They built the per-row context with a direct `__column__` lookup that `KeyError`-ed on a bare-table (multi-column `__row__`) iterator; they now use `_make_row_context` like `SUMX`/`AVERAGEX`. This clears the last "bare-table iterators" limitation.
- **`pbix_format_visual` maps the `labels` object** (OpenBI #1 coverage gap). `{"labels": {"color": ..., "fontSize": ...}}` — a Card's "Callout value" colour/size (`objects.labels.*`) — was silently dropped because only the friendly alias `dataLabels` was recognized; both now route to `objects.labels`.

### Changed
- **Date-table auto-detection is relationship-aware.** It now prefers a date dimension that sits on the one-side of a relationship over a fact table that merely has a `Date` column, disambiguating models where both do. Name heuristics remain as the fallback. (No change to any public-corpus result.)

### Added
- **A datamodel edit that requires a full rebuild now fails safely on models it can't reproduce.** The rebuild path can't recompute **calculated tables** (`DATATABLE`/`GENERATESERIES`), **calculated columns**, or **measure-only container tables** (their VertiPaq data comes from a DAX expression), so a rebuild would reopen them empty. The rebuild tools (`add_relationship`, `remove_relationship`, `remove_table`, `set_table_data`, …) now raise a clear `MODEL_EDIT_UNSUPPORTED` error naming the offending tables and pointing at the surgical tools (`add_measure` / `modify_measure` / `remove_measure` / `modify_column`) that work on all models — instead of a cryptic builder crash or a corrupt file. Full support for editing such models is a tracked follow-up.

### Verified
- New tests: bookmark display-mode, `USERELATIONSHIP`/`CROSSFILTER`, `TOPN`/`RANKX` bare-table, `labels` mapping, relationship-aware date detection, and the rebuild guard. Public-corpus DAX + cross-report re-run with no drift. Full fast suite: 292 passed, 10 skipped, 0 failures; ruff clean; mypy 164.

## [0.9.10] - 2026-07-18

Resolves the relationship-semantics data loss that 0.9.9 deferred (OpenBI #3/#4).
Every relationship trait was reverse-engineered from Power BI Desktop-authored
files and round-tripped: files written by pbix-mcp open in Desktop with **no
repair prompt** and show the correct cardinality / cross-filter / active state in
Manage relationships.

### Fixed
- **A datamodel edit no longer resets relationship semantics** (OpenBI #3, data loss). Every mutating tool (add/remove measure, modify column, add table, …) routes through `_rebuild_datamodel`, which re-read relationships as bare 4-tuples and re-created them as active / single-direction / many-to-one — so the first unrelated edit silently rewrote any **bidirectional** or **inactive** relationship (role-playing date tables, bridge tables). The rebuild now reads and preserves `IsActive`, `CrossFilteringBehavior`, `FromCardinality`, `ToCardinality`, `RelyOnReferentialIntegrity`, `SecurityFilteringBehavior` and keeps the source file's Many/One orientation verbatim.

### Added
- **`pbix_datamodel_add_relationship` can set cardinality, cross-filter direction, and active state** (OpenBI #4). New optional params: `cardinality` (`ManyToOne` default, `OneToMany`, `OneToOne`, `ManyToMany`; also accepts `*:1`/`1:*`/`1:1`/`*:*`), `cross_filter_direction` (`single`/`both`), `is_active`. Previously every relationship was hardcoded to active / single / many-to-one.
- **The builder writes the exact storage Desktop produces for each relationship type**, verified byte-for-byte against Desktop-authored files:
  - *inactive* → `IsActive=0` (storage unchanged);
  - *bidirectional* → `CrossFilteringBehavior=2` (single storage, `Storage2ID=0`);
  - *many-to-many* → `2→2` with **no** physical join index at all (`RelationshipStorageID=0`, no RelationshipStorage / RelationshipIndexStorage / R$ table — Desktop joins m2m via the column dictionaries).

### Known limitation
- **One-to-one** relationships need a *second* (reverse) R$ index — Desktop stores `RelationshipStorage2ID` plus a mirror R$ table. A 1:1 written with only a single index fails to load (`TMProxyRelationship::GetStorage2ID`). Until the reverse index is emitted, a requested/preserved 1:1 is stored as a **bidirectional many-to-one**: it loads cleanly and cross-filters both ways; only the exact 1:1 uniqueness hint is dropped, and a warning is emitted. The Desktop ground truth for full 1:1 support is captured for a follow-up.

### Verified
- Ground truth captured live from Power BI Desktop 2.152.882.0 (bidirectional, inactive, many-to-many, one-to-one authored and diffed).
- Round-trip: a from-scratch model carrying all five relationship types opens in Desktop with no repair prompt; Manage relationships shows the correct glyph for each (`*──◄►──1` bidirectional, `*──►──*` many-to-many, inactive flagged).
- Preservation verified end-to-end: after adding a measure, bidirectional / inactive / many-to-many all survive the rebuild.
- New `tests/test_relationship_semantics.py` (12 tests). Public corpus DAX + cross-report re-run with no drift. Full fast suite: 278 passed, 9 skipped, 0 failures; ruff clean; mypy 162.

## [0.9.9] - 2026-07-18

Works through several documented DAX-engine limitations plus two OpenBI-reported
issues. Each fix is verified for zero drift against the public corpus.

### Fixed
- **Operators no longer require surrounding spaces.** A real tokenizer replaces the space-delimited operator splitting, so unspaced DAX — `SUM(a)/SUM(b)`, `T[Qty]*T[Price]`, `x=y`, `a&&b` — now evaluates identically to the spaced form. This removes the "operator spacing" limitation and, with it, the old "`SUMX` with infix arithmetic returns 0" limitation (`SUMX(T, T[Qty]*T[Price])` is now correct). The tokenizer respects `()`/`[]`/quotes, treats a leading `-`/`+` as a unary sign, and keeps `--` line comments intact. Verified equivalence across 9,000 randomized spaced-vs-unspaced expressions (0 mismatches) and the public corpus (no drift).
- **Bare-table iterators `AVERAGEX`/`MINX`/`COUNTX` no longer return BLANK.** They now build the per-row context the same way `SUMX`/`MAXX` do, so iterating a bare table (`AVERAGEX(ALL(T), T[a])`) computes correctly instead of `KeyError`-ing on the multi-column row and degrading to BLANK. (`TOPN`/`RANKX` use different paths and remain noted.)
- **`pbix_format_visual` now deep-merges nested object properties** (reported by OpenBI). A partial update — e.g. changing only a border colour — previously replaced the whole object entry and dropped the unspecified siblings (`width`, `radius`). Entries are now merged by `selector`, updating only the specified properties.
- **The DAX engine carries cross-filter direction** (reported by OpenBI). `CrossFilteringBehavior` is now loaded into the evaluation context, so a bidirectional (`=2`) relationship adds the reverse multi-hop propagation edge instead of being silently treated as single-direction.

### Added
- **Evaluation budget guard**: each top-level measure is bounded to a fixed number of sub-expression evaluations, so a non-terminating / runaway-expansion measure degrades to BLANK instead of hanging the tool (defense-in-depth). A single pathologically-slow (e.g. O(n²)) measure can still be slow but no longer hangs indefinitely on runaway expansion.

### Verified
- 9,000-expression spaced-vs-unspaced tokenizer fuzz (0 mismatches); public corpus DAX + cross-report re-evaluated with no result drift.
- New regression tests for operator spacing, bare-table iterators, format deep-merge, bidirectional edge, and the eval guard.
- Full test suite: 294 collected, 266 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 162.

### Deferred (documented; not shipped here)
- **Relationship-semantics preservation on rebuild + a relationship editor** (OpenBI #3/#4): the builder still hardcodes `IsActive`/`CrossFilteringBehavior`/cardinality, so a datamodel edit resets bidirectional/inactive relationships. Held because generating correct relationship *storage* for those semantics needs live Power BI Desktop verification (risk of producing a file Desktop rejects). Highest-priority next item.
- Date-table detection via the model relationship (not a name heuristic); `USERELATIONSHIP`/`CROSSFILTER` consumption in `CALCULATE`; the bookmark `display.mode` value (needs a Desktop-authored bookmark to confirm the valid enum).

## [0.9.8] - 2026-07-18

A correctness release: five DAX/encoder bugs that silently produced wrong
numbers. Every fix is repro-first and verified against the public corpus (real
measures) for zero result drift.

### Fixed
- **Decimal/Currency columns truncated on write.** `int(float(v)*10000)` truncated (`19.99*10000 == 199899.99999999997`), so `19.99` round-tripped as `19.9899` — silent data corruption. Now uses `Decimal(str(v))` with half-up rounding.
- **`CALCULATE(m, ALL(Table[Col]))` / `REMOVEFILTERS(Table[Col])` was a silent no-op for UNQUOTED table names.** The column-reference regex captured the whole `Sales[Region]` as a table name, so the filter modifier did nothing and "% of total" returned **100% for every row**. Now `Sales[Region]` splits into table + column correctly (quoted `'Sales'[Region]` already worked).
- **Empty cross-filter selection leaked the grand total** (single-hop and date-table paths). When a filter combination selected zero dimension rows, the empty key-set was dropped and the fact table was left unfiltered, so the measure returned the grand total instead of BLANK. Now an empty selection filters the fact to zero rows (mirrors the multi-hop fix shipped in 0.9.6).
- **`&&` / `||` were evaluated as string concatenation.** `A && B` became `str(A)+str(B)`, so multi-condition `FILTER`/`IF` predicates — and standard multi-condition **RLS rules** — silently dropped conditions (a multi-condition RLS rule could report 0 visible rows). They are now proper logical AND/OR with correct precedence (`||` looser than `&&`). Single-`&` string concatenation is unchanged.
- **`DIVIDE(x, 0)` and `x / 0` returned the wrong value.** `DIVIDE(x, 0)` returned `0` instead of BLANK (breaking `ISBLANK`/visual blanking), and a spaced binary `SUM(a) / SUM(b)` with a zero denominator returned the **numerator** instead of BLANK. Both now return BLANK (or the supplied alternate). A BLANK *numerator* is still treated as 0, per DAX.

### Verified
- New repro-first regression tests for every fix, incl. a build→decode round-trip for all non-String data types, "% of total" with unquoted `ALL`, empty-selection BLANK, RLS-substituted multi-condition predicates, and DIVIDE/`/` BLANK semantics.
- Public corpus (real measures across 4 dashboards) re-evaluated with no result drift.
- Full test suite: 284 collected, 256 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 162 (CI baseline 175).

### Known limitations (unchanged; documented for the next release)
- The DAX evaluator requires **spaces around binary and comparison operators** (`a / b`, `a = b`, `a && b`), not the unspaced forms (`a/b`, `a=b`). This underlies the "SUMX with infix arithmetic" note in `docs/limitations.md`. A proper operator tokenizer is the top candidate for a future release.
- Table iterators (`AVERAGEX`/`MINX`/`COUNTX`/`TOPN`/`RANKX`) over a *bare* table can return BLANK; `SUMX`/`MAXX` are correct.

## [0.9.7] - 2026-07-18

### Security
- **Path traversal / arbitrary file write in `pbix_set_theme` (CWE-22 / CWE-73).** The `filename` argument was joined straight into the write path (`os.path.join(base_dir, filename)`) for both the `BaseThemes` and `RegisteredResources` writes, with no containment check — a value like `../../../../evil.json` escaped the per-file work directory and wrote attacker-controlled JSON anywhere the server process could write. Now every write that incorporates untrusted input goes through a new `_safe_join()` helper that resolves the path and refuses (raising `UnsafeWriteError`) anything outside the work directory. Reported by **Moshe Levi (Levinity Cyber)**; confirmed with a PoC and covered by a regression test.
- **Zip-Slip in `.pbiviz` custom-visual extraction.** Found during the review prompted by the report above: `pbix_add_custom_visual` extracted each member of the user-supplied `.pbiviz` archive via `os.path.join(cv_dir, name)` (and the manifest-derived `visual_name`) with no containment, so a crafted archive member named `../../…` could write outside the work directory. Both the member names and `visual_name` are now contained via `_safe_join()`. (The `.pbix`/`.pbit` open path was already hardened in 0.9.6; this closes the sibling `.pbiviz` path.)

### Performance
- **`pbix_evaluate_dax_per_dimension` no longer re-scans the fact table per dimension value.** It previously re-filtered the entire fact table once for every dimension value (O(values × fact_rows)) — fine at demo scale, slow on large models with many categories. For simple aggregation measures (`SUM`/`AVERAGE`/`MIN`/`MAX`/`COUNT`/`DISTINCTCOUNT` of a column, or `COUNTROWS` of a table) the fact rows are now grouped by the propagated join key **once** and each bucket is aggregated with the real engine (O(fact_rows + values)). The dimension→fact mapping reuses the engine's own relationship propagation, including the 0.9.6 multi-hop (snowflake) path, so results are byte-for-byte identical to the per-value path. The fast path is applied only where it is provably equivalent; it transparently falls back to the exact per-value evaluation for: measures that are not simple aggregations, a join key that maps a fact row to more than one dimension value (non-unique/NULL keys), and — importantly — any `filter_context` whose filter sits on the sliced dimension's own join path (dimension table or an intermediate), which must be combined conjunctively per dimension row. Base filters on the fact itself or on unrelated dimensions keep the fast path. **No returned value changes in any case.**

### Verified
- Optimized output equals an independent group-by over a 200,000-row / 50-value model and equals the previous per-value engine path exactly; multi-hop snowflake bucketing verified; complex measures confirmed to fall back.
- **Adversarial correctness sweep**: a 6-dimension multi-agent hunt (NULLs, type mismatches, relationship shapes, base-filter interactions, aggregation semantics, value-capping) initially surfaced a real divergence class — a base filter on the sliced dimension's own table combined with non-unique/NULL join keys — which is now fixed by falling back. A re-run against the fixed code exercised the fast path on 57,000+ measure-instances across 37,000+ randomized models with **zero divergences**. Permanent regression tests capture every original repro plus a 400-model fuzz.
- Perf guard test asserts total fact-row scans stay O(fact_rows), independent of `max_values` (the old path scaled ~`max_values × fact_rows`).
- Full test suite: 270 collected, 242 passed, 28 skipped (corpus-dependent), 0 failures; ruff clean; mypy 162 (CI baseline 175 — improved via `Optional` annotations on `DAXContext`). Includes new path-traversal / `_safe_join` regression tests.

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
- Briqlab original vs a client-recolored copy: correctly detected 469 removed theme colors + 10 added

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
- **Corporate brand compliance test** — airport dashboard recolored from teal to a corporate brand palette using only MCP tools (`pbix_extract_colors` → `pbix_recolor` → `pbix_set_theme`). All 531 original colors replaced. Zero non-brand colors remaining. Logo swapped. Verified in PBI Desktop March 2026.

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
