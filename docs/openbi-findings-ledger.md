# OpenBI findings ledger

Every engine issue OpenBI has reported lives in that project's
`docs/pbix-mcp-issues-<N>.md` files. This ledger is the audited status of all
of them, so an item cannot quietly sit unimplemented -- which is exactly what
happened to findings #18 (fixed in 0.9.59, after being live for four releases).

**Audited 2026-09-01 against 0.9.104. Docs 1-43 and GitHub #1-#66 are all
closed — both queues are empty.** Docs 20-43 were worked through GitHub issues
(#23-#66) across 0.9.73-0.9.104; docs 28/29/30/39/41/42/43 carry OpenBI's own
CLOSED banners, and the rest map to issues that are closed.

That paperwork was not taken on trust. A re-verification sweep ran the reported
repro for every silently-wrong-output and typing item in docs 25-43 against the
live 0.9.104 engine: **18 of 19 checks reproduced the fixed behaviour** —
KEEPFILTERS intersects (BLANK under a disjoint outer filter, not the 20 an
override gives), `CALCULATE(T[Col]=v)` sugar, bare `REMOVEFILTERS()` and
`ALLEXCEPT`, count aggregations typed Int64 (DIVIDE still Double),
case-colliding column names refused, Int64 past signed-32 round-tripping,
a data column named RowNumber surviving, trailing-backslash literals,
`title.alignment` written lowercase, and a visual background image refused by
name with its colour control still working. The 19th was the sweep's own wrong
expectation, not an engine defect — see below.

Re-run the audit before claiming the queue is clear. `gh issue list` covers only
the GitHub tracker; these docs are a separate stream.

**Updated 2026-10-07 (0.9.109): docs 1-44 and GitHub issues #1-#76 are closed;
the issue queue is empty.** Since the audit above:

- doc 44 = GitHub #67 (`fontColor` on title/subtitle/legend, unread keys
  reported) — closed in 0.9.106;
- #74 — the `outline` follow-up flagged while closing #67 — closed in 0.9.107;
- #75 — build warnings never reached an MCP caller, which completes #43's
  0.9.105 visibility fix (see the audit note below) — closed in 0.9.108;
- #76 — the `report_filter_json` follow-up queued in #52 and never landed —
  closed in 0.9.109.

From 0.9.107 on, every fix has its own GitHub issue, and the CHANGELOG entry
and release notes cite it — so a change can be followed from the CHANGELOG
alone.

**Updated 2026-10-10 (0.9.137): docs 1-60 and GitHub issues/PRs #1-#179 and
#181-#184 are closed; #180 (SEARCH / FIND / SUBSTITUTE match linguistically in
Desktop) is open.** 0.9.137 closes doc 60 = #182 (OpenBI's): a
time-intelligence function takes a table of one date column as its <dates>
-- PREVIOUSMONTH(DATESINPERIOD(...)) or of a variable holding one was empty
(Executive Sales Report's [Orders Previous month]); and #184: with both
columns of a relationship unique, the builder took the larger table for the
Many side and turned a fact -> dates relationship around, so a date filter
never reached the facts.

As of 0.9.136, docs 1-59 and GitHub issues/PRs #1-#179, #181 and #183 were
closed; #180 (SEARCH / FIND / SUBSTITUTE match linguistically in Desktop) and
#182 (OpenBI's doc 60: a time-intelligence function given a table of dates
returns nothing) were open. 0.9.136 closes doc 58 = #174 and #175 (OpenBI's):
a VAR block splits only at its own VAR / RETURN -- at depth 0, outside
strings, 'quoted names' and [names], VAR only before white space (VAR.P stays
a call) -- and /* */ comments are read; doc 59 = #181 (OpenBI's):
evaluate_per_dimension buckets the fact rows by the folded join key, so a
dimension joined on a text key answers again (a regression of 0.9.133); and
#183: the builder and pbix_datamodel_add_measure refuse two measures whose
names are equal ignoring case, which Desktop cannot open.

As of 0.9.135, docs 1-57 and GitHub issues/PRs #1-#173 and #176-#179 were
closed; #174 and #175 (OpenBI's doc 58: a VAR block cut at every VAR / RETURN
word, /* */ comments) and #180 (SEARCH / FIND / SUBSTITUTE match
linguistically in Desktop: ss = sharp s, a soft hyphen skipped, width and kana
ignored by SEARCH) were open. 0.9.135 closes #167-#173 and #176-#179, the DAX
text functions at their edges, measured against Desktop in eight batteries:
TRIM removes spaces only; counts, starts and instances are rounded half away
from zero and checked; SEARCH's wildcards and its matcher's own rules (a start
past the text, a leading *, `**`, the tildes); VALUE reads as OLE Automation
does (hex, D exponents, dates in Desktop's orders); UPPER / LOWER through
Desktop's casing table; UNICHAR's refusals; UTF-16 lengths and positions;
BLANK through the text functions; CONCATENATEX renders as `&`; and a one-row
table of any shape is its value (FILTER over a table leaked Python text).

As of 0.9.134, docs 1-57 and #1-#166 were closed; #167-#173 were open. 0.9.134
closes doc 57 = #166 (OpenBI's): a measure that reads no filter of an
iteration's table -- every read under a CALCULATE that clears all filters, or
over a table the iteration's filters cannot reach -- is evaluated once per
iteration, not once per row (Agents Performance's [Number of Employees with
Positive Change]: 137, as Desktop, in 4.5 s instead of 44.6 s).

As of 0.9.133, docs 1-56 and #1-#165 were closed. 0.9.133 closes #163: the
engine joins a relationship's text keys as Desktop does -- with their ASCII
case folded, also across the two tables, and a key the one side holds more
than once through its last row (a filter on an earlier row reaches nothing,
RELATED returns the last row), while a filter from the many side reaches every
row holding the key (build_b165.py 26 of 26, 0.9.132 10).

As of 0.9.132, docs 1-56 and #1-#162, #164 and #165 were closed; #163 was
open. 0.9.132 closes six issues found verifying #156, all about text as
Desktop's column store holds it. #159: Desktop's import
strips U+0001-U+0020 from the end of a text, and the builder now stores what it
stores (build_b159.py 35 of 35 the same as built and refreshed, 0.9.131 13).
#161: the empty text "" is a value of its own, stored as Desktop stores it -- a
zero-length record on a page flagged page_contains_nulls, the flag whose
absence made 0.9.3's "" unloadable (build_b161.py 7 of 7, 0.9.131 0). #160: a
built text column's hierarchy, relationship index and orientation guess number
the case-folded dictionary (build_b160.py: COUNTROWS(VALUES) 8, not 14). #162: a
key the one side holds more than once reaches its last row, as Desktop joins
it. #164 and #165: the engine takes "" for a value, not BLANK, and text
functions take a one-row table's value (build_b163.py 38 of 38, 0.9.131 14).
#163, the engine's own joins of repeated and case-variant keys, was open.

As of 0.9.131, docs 1-56 and #1-#158 were closed. 0.9.131 closes #157: the
engine compares and orders text in the model's collation, as Desktop does
(Windows NLS 6.1 sort weights measured per character, less the 1,321
characters Desktop's older table does not know: case-insensitive, accents
after the letters, punctuation before the digits, hyphens and apostrophes
last; 62 of 62 expression pairs, 0.9.130 35); #156, a built text column's
attribute hierarchy is written in that order, so a built model sorts as it
does after Desktop's refresh (build_b156.py 12 of 12, 0.9.130 5); and #158,
TOPN takes every order pair, orders by any value (dates and text too, BLANK
first) and keeps the ties at the n-th row (build_b158.py 13 of 13, 0.9.130 6).
All three were found verifying #136.

As of 0.9.130, docs 1-56 and #1-#155 were closed. 0.9.130 closes #136: every
column of a built model is available in MDX (IsAvailableInMDX = 1, as Desktop
writes it; Desktop's MDX slicers, member sets and schema rowsets answer as DAX
does, the same as built and after a refresh), and #155, found verifying it: a
table rewrite keeps a calculation group's source columns ('Name' / 'Ordinal'),
so a model whose group column was renamed still opens in Desktop after
pbix_set_table_data.

As of 0.9.129, docs 1-56 and #1-#135 and #137-#154 were closed; #136 was
open. 0.9.129 closes #117 (OpenBI's):
SUMMARIZE by a related table's column groups the SOURCE rows and keeps each
column's lineage -- one part per table, as a CROSSJOIN row, so the table
filters the combinations and a transition over it filters each table -- and
an extension column sees its group's source rows: the columns they carry,
and for a table's rows their expanded table; a source of columns (ALL /
ALLSELECTED of several, VALUES, SUMMARIZE) expands to nothing, and a group
column it cannot reach is Desktop's error (build_b117.py 115 of 115;
build_b115.py, build_b114.py and build_b115b.py now match in full). Found
working on it: #152, ALL / ALLSELECTED of several columns was taken for the
table's rows (as a filter it dropped the table's other filters and
relationships: 24 for Desktop's 3); #153 and #154, a table filter on several
columns, or of a table's rows, kept each column's values and not the
combinations its rows have (12 for 11); #151, a CROSSJOIN calculated table
takes each part's columns (Desktop's own tables through TOM, 8 of 8); and
#150, the measure memo key of a fact-row transition is the context's own
signature, not a sorted copy of every filter (the cost 0.9.128 added).

As of 0.9.128, docs 1-56 and #1-#116 and #118-#149 except #136 were closed;
#117 and #136 were open. 0.9.128 closes
#115 and #114, both OpenBI's: the transition of a table's row and a table
filter argument filter the EXPANDED table -- the columns of every table the
rows reach many-to-one carry the related rows' values, so ISFILTERED sees
them, REMOVEFILTERS of one leaves the others and they reach the facts that
share the dimension (build_b115b.py 111 of 112, build_b114.py 38 of 39,
build_b115.py 51 of 55; the misses are #117). Found working on them: #147
(DATEADD inside an iterator shifts the row's dates), #148 (table filters
intersect within one CALCULATE and under KEEPFILTERS) and #149 (two filters
of more than 4096 values shared a memo entry).

As of 0.9.127, docs 1-56 and #1-#113 and #116-#146 except #117 and #136
were closed; #114, #115, #117 and #136 were open. 0.9.127 closes doc 56 = #143: PBIXBuilder marks a projection
`active` as Desktop does -- a drillable role's first entry (its top level),
every entry with "expanded": true, never a measure well -- so a two-field axis
no longer opens expanded. And #146, found verifying #145: date arithmetic is
typed as Desktop types it (date - date is a DateTime; '-' keeps its left
operand's type), build_b146.py 36 of 36.

As of 0.9.126, docs 1-55 and #1-#113, #116, #118-#142, #144 and #145 were
closed; #114, #115, #117, #136, #143 and #146 were open. 0.9.126 closes #116: a row context filters nothing until
CALCULATE or a measure reference turns it into a filter -- table expressions,
nested iterators, VALUES, FILTER's aggregates and CALCULATE's filter arguments
inside an iterator read the context the rows were opened in, while
time-intelligence functions and RELATEDTABLE see the transition; EARLIER /
EARLIEST walk the row contexts (build_b116.py 39 of 40, the other is #115;
build_b118.py now 420 of 420). Found verifying it: #144 (a VAR block in a
function argument) and #145 (the one-date tables of time intelligence as
values).

As of 0.9.125, docs 1-55 and #1-#113, #118-#142 were closed; #114-#117 and
#136 were open. 0.9.125 closes #142:
the #131 shield also proves a measure that re-filters a date table through a
CALCULATE on its date column independent of the date row, so Awesome
Chocolates' calculation items answer in under a second (70-110 s before),
every cell of Desktop's by-item query matching.

As of 0.9.124, docs 1-55 and #1-#113, #118-#141 were closed; #114-#117,
#136 and #142 were open. 0.9.124 closes four
defects found verifying #118 on Awesome Chocolates, whose QOQ calculation item
now matches Desktop end to end:

- #137: a row transition on a date column (a DateTime relationship column, or
  a marked date table's date column) clears the table's other filters, as a
  CALCULATE filter on it does (#78) -- build_b137.py, 74 probes;
- #138: DATEADD / SAMEPERIODLASTYEAR over a table expression shift the
  table's own dates, whatever else filters the date table -- build_b138.py,
  60 probes;
- #139: DIVIDE and the math functions convert numeric text, TRUE / FALSE and
  dates as arithmetic does, and raise for other text -- build_b139.py, 82
  probes;
- #141: DATEADD / SAMEPERIODLASTYEAR of a date column shift nothing when no
  date is visible (the whole calendar before) -- build_b141.py, 24 probes.

As of 0.9.123, docs 1-55 and #1-#113, #118-#135 and #140 were closed;
#114-#117 and #136-#139 were open. 0.9.123 closes
#118: ALLSELECTED keeps the measure's own CALCULATE filters and puts back what
an iteration iterated (its shadow), measured on 420 cells (build_b118.py);
the Financial Sample's Arrow Chart now matches Desktop on every measure and
Awesome Chocolates at the grand total. #140, found by its own census before
release: the memo keys an iteration only for a measure that can reach
ALLSELECTED, so a measure FILTER and AVERAGEX share is evaluated once.

As of 0.9.122, docs 1-55 and #1-#113, #119-#135 were closed; #114-#118
were open. 0.9.122 also solves PR #123 by
@allanon2 on our side (#130, the modern visual header). It closes docs 52-55:

- doc 52 = #121: calculation groups are applied -- the item a filter on the
  group's item column selects replaces each outside measure reference,
  higher precedence outer, SELECTEDMEASURE & co. inside items, and the
  outermost item with a format string decides the format (format_string
  in pbix_evaluate_dax; engine.evaluate_format_strings). The definitions
  attach to the measures dict (engine.set_calculation_groups, or the
  calculation_groups= argument of the evaluate functions);
- doc 53 = #122: DATESINPERIOD around 29 February and month ends;
- doc 54 = #124 (RANKX's order argument) and #125 (RANKX with BLANK, text
  and dates);
- doc 55 = #126: a DateTime with a time of day joins by date part only
  across JoinOnDateBehavior = DatePartOnly (auto date/time), and exactly
  otherwise -- per-value and per-dimension alike;
- found verifying them: #127 (DATEADD / SAMEPERIODLASTYEAR over a table of
  dates), #128 (the builder stores DatePartOnly as Desktop does), #129
  (DATEADD keeps a single date's day number), #133 (text that is no number
  is an error in arithmetic, not a BLANK), #134 (a VAR is evaluated when
  first used, as in DAX), #135 (IFERROR / ISERROR react to errors, not to
  BLANK);
- found by the 0.9.121 census: #132 (a column named with a leading or
  trailing space, Financial Sample's ' Sales', is found).

As of 0.9.121, docs 1-51 and #1-#113, #119-#120 and #131 were closed.
0.9.121 closes docs 50 and 51, and #131, found by its own corpus census:

- doc 50 = #119: a ThemeDataColor's ColorId indexes Desktop's picker row --
  white, black, then the first 8 data colours (from desktop.min.js), so 0 and 1
  are fixed white and black, not the theme's background / foreground (O47);
- doc 51 = #120: ALLSELECTED(<table>) as a table is the table's rows under the
  selection. Desktop keeps only the selection on its own columns for the
  column form, so doc 51's expected 1 for COUNTROWS(ALLSELECTED(T[Region]))
  under a D[Zone] slicer is 2 in Desktop.
- #131: with #120's rows, a "last week with sales" measure was evaluated
  once per date row; the measure memo now leaves out filters a measure
  provably cannot see, so Awesome Chocolates' Selection max date answers
  in 0.24 s instead of running out of time.

As of 0.9.120, docs 1-49 and #1-#113 were closed. 0.9.120 closes docs 48 and
49, found by OpenBI's port of the engine:

- doc 48 = #110: ROUND rounds half away from zero, on the decimal value;
- doc 48 = #111: ISFILTERED(<table>) sees a direct filter on the table's
  columns;
- doc 49 = #112: a PBIR report's visual groups survive the classic layout, and
  the PBIR writer round-trips them;
- #113, found verifying #111: ISCROSSFILTERED sees every filter that reaches
  the table.

The Desktop batteries for #111/#113 exposed five older engine defects, filed
as open issues:

- #114: a table filter argument does not act on its expanded table;
- #115: a context transition over a fact row does not filter its dimensions;
- #116: in a row context, a table expression over the iterated table sees only
  the current row;
- #117: SUMMARIZE by a related table's column (groups and lineage);
- #118: ALLSELECTED removes the measure's own CALCULATE filters.

As of 0.9.119, docs 1-47 and #1-#109 were closed. 0.9.119 closes the three
gaps found while verifying 0.9.118:

- #107: text compares case-insensitively in expressions.
- #108: iterating a CROSSJOIN reads every table's columns.
- #109: the builder folds only ASCII letters, as the column store does.

As of 0.9.118, docs 1-47 and #1-#106 were closed. 0.9.118 solves PRs
#94-#99 by @allanon2 on our side, each with its own issue:

- #100: ALLSELECTED over the expanded table (PR #94).
- #102: filter values matched as DAX compares them (PR #95).
- #103: TREATAS onto several columns (PR #99).
- #104: builder aliases, roles and aggregations (PR #98).
- #105: unsupported functions per measure (PR #96).
- #106: one engine per thread (PR #97).
- #101: ALL / REMOVEFILTERS / ALLEXCEPT over the expanded table, found
  verifying #100.

Three further gaps were found while verifying it and were filed as #107-#109,
fixed in 0.9.119 (above).

As of 0.9.117: 0.9.116 closes doc 46 = #92 (whole-number doubles spelled
'13D', as Desktop does, for every property); 0.9.117 closes doc 47 = #93 (the
fill card's state split on shape, actionButton and the navigators).
Earlier, as of 0.9.115: 0.9.114 adds #87 (CALCULATE applies its
modifiers before its filter arguments) and #88 (a deterministic row-context
simulation); 0.9.115 merges PRs #89 (bare [Column] in the grouped tools) and
#90 (DataMashup reader) by @allanon2, and fixes #91 (DataMashup writer).

- #77 (PR, @allanon2) -- ISINSCOPE through a context transition -- 0.9.111;
- #78, #79, #80 -- the marked-date-table rule, the grouped tool, KEEPFILTERS
  scope -- 0.9.112;
- #81, #82, #83, #85, #86 -- the blank (unknown) member in the builder and the
  engine, the USERELATIONSHIP cache leak, TREATAS in CALCULATE, the
  per-dimension tool -- 0.9.113;
- doc 45 = GitHub #84 (padding written as an Int64 literal) -- 0.9.113.

## Audit note: the #43 case-fold is correct, and now visible

The sweep flagged that `'abc'`/`'ABC'`/`'Abc'` all read back as `'abc'` with
`DISTINCTCOUNT` = 1. That is **not** a regression: VertiPaq's string store is
case-insensitive, and folding onto one dictionary entry keeping the first
spelling seen is exactly what issues-32/#43 asked for and what Power BI itself
does on import — without it Desktop refuses the whole model ("A duplicate value
has been detected in the Unique Value store"). The fold stays.

What was missing is that the fold **rewrites the caller's supplied values in
silence**. The pre-build checks already treat the same collision one level up
(case-colliding column NAMES, #53) as CRITICAL, and the project's warning
channel exists precisely so a lossy transformation is never reported as an
unqualified success. So the build now emits, for each affected column, a
`PBIX pre-build: WARNING:` naming the column, how many values collided, and an
example pair, and stating that they store as one value using the first spelling.
Behaviour is unchanged; only the silence is. Regression:
`tests/test_case_fold_warning.py` (incl. a negative control and a
non-string-column control).

**Correction (2026-10-07):** that warning went to Python's `warnings` stream
only, which an MCP caller never sees — over MCP the fold stayed silent
(`success: true, warnings: []`) until **0.9.108 (#75)**, which forwards every
build warning about the call's own data into the response. The `pbix_doctor` DBCC-style validator that the same
report asked for is present (`check_string_dictionaries`,
`check_name_collisions`, `check_dictionary_widths`).

## Open: silently wrong output

These produce a plausible answer with no error, which is why they survived so long.

- [x] **issues-9** -- CALCULATE boolean filter args other than `Col = value` were silently dropped (fixed 0.9.60, Desktop-verified). The `IN` operator is
      implemented and used by CALCULATE but deliberately NOT enabled for
      general expressions.
      **CORRECTION (2026-07-30): the stated reason was wrong.** It was recorded
      here, and in `TestInMachinery`, that enabling `IN` "unmasks an inaccurate
      RANKX/TOPN chain". Measured against Desktop on Agents_Performance, the
      chain is NOT inaccurate: `[MTD Total Sales] @ StoreType=Catalog` is
      1783540.7792 in Desktop and identical here, and the non-blank-MTD employee
      count is 1 in both. What is wrong is REVERSE FILTER PROPAGATION -- our
      single-hop relationship index is symmetric, so filtering the many side
      restricts the one side, and `SELECTEDVALUE(DimEmployee[EmployeeKey])`
      answers 213 where Desktop answers BLANK. That is what makes the measure
      return 1 instead of 0. RESOLVED in 0.9.63: propagation now follows TABLE
      EXPANSION (a column filter propagates one -> many only; a table filter
      argument's keys may also ride many -> one), and BOTH anchors hold --
      Agents_Performance is 408/408 with [Actives] = 32,401. Whether `IN` can
      now be enabled generally is a SEPARATE question, still untested.
- [x] **issues-3** -- set_visual_property / update_visual_json on a CLASSIC layout now recompile query+dataTransforms (0.9.61-dev, shipped in 0.9.62), so Desktop no longer renders the old field
- [x] **issues-7** -- CLOSED (0.9.68). Two layers: unknown TOP-LEVEL names
      already raised DAXMeasureNotFoundError with close-match hints; the
      remaining hole was IN-EXPRESSION references -- `[Nope] + 1` answered 1
      with status "ok". A bare [Name] that is neither a measure, a
      row/extension-column key, nor a model column now raises (same rule as
      the qualified Table[Name] path), and pbix_evaluate_dax reports status
      "error" with the message. Corpus re-verified (22/22, 41/41, 102/102).
- [x] **issues-9** -- default-filter behaviour differs between pbix_evaluate_dax and pbix_evaluate_dax_per_dimension.
      NOT A DEFECT: the divergence is a deliberate contract (a per-dimension
      sweep is normally asked against the raw model) and is pinned by tests.
      The real gap was documentation -- `pbix_evaluate_dax_grouped` carried no
      NOTE. Added in the 0.9.61-dev cycle (shipped in 0.9.62); all three tools now say so in their own docstring.
- [x] **issues-15** -- residual COALESCE(ExplicitName, InferredName) instances (fixed in the 0.9.61-dev cycle, shipped in 0.9.62). Four sites audited and corrected: `pbix_get_hierarchies`'
      level query, `_report_type_resolver`'s Type IN (1,2,4) scan,
      `_detect_field_parameter_shape` (whose `ExplicitName NOT LIKE` also
      dropped the NULL rows outright), and the perspective column list.
- [x] **issues-5** -- CLOSED (0.9.68). Latitude/Longitude field wells and
      lat/long-named numeric columns in value/X roles now compile to
      Aggregation Function=1 (Avg), matching Desktop's geographic default
      summarization; all other value-role behavior unchanged.

- [x] **issues-9 (follow-on)** -- CLOSED. Agents_Performance matches Desktop on
      all 102 measures, `CALCULATETABLE` is implemented, and the `IN` operator is
      enabled generally. The corpus-wide Desktop diff that followed (0.9.61)
      took every other file to 1:1 as well.

## Open: Desktop fidelity

Our output loads and renders, but differs field-for-field from a Desktop-authored file.

- [x] **issues-3** -- CLOSED (0.9.69). add_visual, add_html_visual and the
      builder now stamp Desktop's 1000-step `z` + `tabOrder = z + 1000` on
      the container AND config.layouts position (add_image's verified
      pattern). Desktop-load + query verified on a freshly built file.
- [x] **issues-4** -- CLOSED (0.9.69). The builder's Report/Layout now
      carries a report-level config with `version` (5.61, the corpus-era
      schema version), `activeSectionIndex`, `linguisticSchemaSyncVersion`,
      `defaultDrillFilterOtherVisuals` and filter-pane settings, matching
      Desktop-authored files (ground truth MS_AI_Sample / GeoSales).
      Desktop-load verified.
- [x] **issues-8 (table audit)** -- CLOSED (0.9.70-dev). Field-for-field vs
      the GeoSales Desktop-authored tableEx: container keys match (incl.
      tabOrder, query, dataTransforms, filters); the two structural gaps --
      config.layouts written only for image visuals, and
      drillFilterOtherVisuals not defaulted -- are fixed in pbix_add_visual
      for every type. The remaining differences (objects.columnFormatting /
      columnHeaders / grid / total, vcObjects styling, columnProperties
      display names, hasDefaultSort) are user/theme content, accepted via
      config_json / pbix_format_visual, not defaults to invent.
- [x] **issues-8 (offline behaviour)** -- CLOSED as documentation
      (0.9.70-dev). Desktop resolves `publicCustomVisuals` GUIDs from
      AppSource at report open and keeps a per-machine cache
      (`%LOCALAPPDATA%\Microsoft\Power BI Desktop\ExtensionCache`,
      hashed entries; `CertifiedExtensions` holds connector .pqx, not
      visuals). Offline with a cold cache the report still OPENS -- the
      referenced visual's container renders Desktop's unavailable-visual
      placeholder while every other visual and the model work normally;
      once the visual has been fetched on any online open, the cache
      serves it offline. NOT verified by a live network-isolated run on
      this machine (blocking Desktop's network needs firewall changes
      out of scope for this environment); the cache-directory facts are
      verified locally, the placeholder behaviour is Power BI's
      documented/standard handling. pbix_reference_public_visual's
      docstring now carries the offline note.

## Open: feature asks

Capability requests rather than defects.

- [x] **issues-12 (partition-M setter)** -- CLOSED (0.9.71-dev).
      `pbix_set_partition_m(alias, table, m_expression)` writes
      Partition.QueryDefinition verbatim, metadata-only (cached rows
      untouched; Power BI runs the new M on next Refresh).
- [x] **issues-12 (source on set_table_data)** -- CLOSED (0.9.71-dev).
      Optional `source_json` (same format as pbix_update_data_source)
      applies the partition source right after the rows are written --
      snapshot + repoint in one call; a failed source update reports
      loudly that the rows ARE written.
- [x] **issues-14 (predicates + Top-N)** -- CLOSED (0.9.72). Half A
      (predicate objects: comparison / between / relative-date) was already
      live via make_value_matcher; half B lands now: a filter value of
      {"top_n": {"n": 5, "by": "<measure or Table.Column>",
      "direction": "desc"}} is materialized SERVER-SIDE into a concrete
      In-set (ranked under the other filters, blanks last, stable ties)
      before evaluation, in all three evaluate tools -- the same
      materialization OpenBI performed client-side, moved server-side.
- [x] **issues-17 (matrix)** -- CLOSED as covered-by-composition (0.9.72).
      pbix_evaluate_dax_grouped already takes a COMPOSITE group_by
      ("RowDim.Col,ColDim.Col") and returns one structured row per (row,
      column) combination -- a matrix or series is a client-side pivot of
      that flat result. Recipe documented in the tool docstring.
- [x] **issues-17 (propagation reuse)** -- CLOSED as measured-unnecessary
      (0.9.72). On Agents_Performance (200K fact rows), 50 repeated
      pbix_evaluate_dax calls sharing a filter set cost 6.0 ms/call
      (2.6 ms/call for a second measure under the same set): the shared
      filter-index cache plus the per-context measure memo already make
      repeats near-free. No workload evidence justifies another cache
      layer.
- [x] **issues-13 (auto date/time)** -- CLOSED as wont-do-now (0.9.72).
      Rationale: LocalDateTable_<guid> + Variations are HIDDEN tables the
      user did not author; Desktop generates its own the moment its auto
      date/time option is on (and our built models verify clean in Desktop
      and the service without them -- the whole DAX-parity program ran on
      models that carry none); no downstream user has asked for them
      (checked the downstream-usage record). Date hierarchies that users DO
      author are covered by add_user_hierarchy + a real date table
      (pbix_datamodel_add_calculated_table over CALENDAR()).
- [x] **issues-14 (auto date/time, re-ask)** -- same wont-do-now close as
      issues-13 above.

## Refuted (reported open, actually implemented)

Kept so the same items are not re-litigated:

- **pbix-mcp-issues-10.md** -- §4 Copy the bridge's security posture into the engine tools: magic-byte type sniffing (never extension/header claims), 5
  - implemented at: D:/dependency_tracker/pbix-mcp/src/pbix_mcp/server.py -- magic-byte sniffing `_sniff_image_ext` 3645-3711 + `_sniff_resource_ext` 3714-3731; 5 MB cap 
- **pbix-mcp-issues-13.md** -- Item 4: let filter_context carry structured predicates (comparison / between / relative-date) and a Top-N spec applied n
  - implemented at: Predicates (half A): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\dax\engine.py:506-564 `make_value_matcher` (+ helpers `_compare`:458, `_relative_date
- **pbix-mcp-issues-14.md** -- Item 1.2: extend calculated-COLUMN authoring beyond row context (aggregation / CALCULATE / RELATED), or keep an explicit
  - implemented at: Refusal branch (explicitly permitted by the item): D:\dependency_tracker\pbix-mcp\src\pbix_mcp\server.py:10840-10845 + docstring 10815-10820; test loc

## Recently closed

- **doc 60 (GH #182), GH #184** -- CLOSED (0.9.137). A time-intelligence
  function given a table of dates returned nothing (Desktop: build_b180.py,
  24 probes); the builder turned a relationship around when both columns
  were unique (build_b184.py). Regression:
  tests/test_issue182_dates_table_argument.py,
  tests/test_issue184_relationship_orientation.py.
- **doc 58 (GH #174, #175), doc 59 (GH #181), GH #183** -- CLOSED (0.9.136).
  A VAR block split at every VAR / RETURN word and /* */ comments were not
  read (Desktop: build_b177.py, 27 probes); evaluate_per_dimension was BLANK
  for a text-keyed dimension since 0.9.133 (build_b179.py); two measures whose
  names differ only by case made a file Desktop cannot open (build_b178.py).
  Regression: tests/test_issue174_var_block_split.py,
  tests/test_issue175_block_comments.py,
  tests/test_issue181_per_dimension_text_keys.py,
  tests/test_issue183_measure_name_collisions.py.
- **GH #167-#173, #176-#179** -- CLOSED (0.9.135). The DAX text functions at
  their edges, each measured against Power BI Desktop 2.152 (build_b166.py,
  build_b166b.py, build_b170.py, build_b174.py - build_b176.py): TRIM,
  counts, SEARCH / FIND / CONTAINSSTRING, VALUE, UPPER / LOWER, UNICHAR /
  UNICODE / SUBSTITUTE / COMBINEVALUES, PROPER (removed), UTF-16, BLANK,
  CONCATENATEX, one-row tables as values. Regression:
  tests/test_issue167_trim_spaces.py ... tests/test_issue179_one_row_tables.py.
- **doc 57 (GH #166)** -- CLOSED (0.9.134). A measure that reads no filter
  of an iteration's table -- every read under a CALCULATE that clears all
  filters, or over a table the iteration's filters cannot reach -- was
  evaluated once per row. The memo's shield now proves it and keys those
  filters by the iteration. Regression: tests/test_issue166_noread_shield.py.
- **doc 52 (GH #121)** -- CLOSED (0.9.122). Calculation groups were read as
  ordinary tables, so no item was ever applied. Now the item that the filter
  on the group's item column selects is applied, with Desktop's rules,
  measured over 549 query cells and 80 MDX format strings. Regression:
  tests/test_issue121_calculation_groups.py,
  tests/test_issue121_calc_group_rules.py.
- **doc 55 (GH #126)** -- CLOSED (0.9.122). evaluate_per_dimension matched
  join keys exactly and the per-value path by date part on every
  relationship. Desktop: DatePartOnly (auto date/time) joins by date part,
  DateAndTime exactly. The reader passes JoinOnDateBehavior and both paths
  follow it. Regression: tests/test_issue126_date_part_joins.py.
- **doc 54 (GH #124, #125)** -- CLOSED (0.9.122). RANKX read its order as
  'DESC' in the text, returned BLANK for a BLANK value and ranked numbers
  only. Now Desktop's order spellings, BLANK as 0 / "", text and dates.
  Regression: tests/test_issue124_rankx_order.py,
  tests/test_issue125_rankx_blank.py.
- **doc 53 (GH #122)** -- CLOSED (0.9.122). DATESINPERIOD's YEAR branch
  raised on 29 February; all intervals now shift as DATEADD's month-end rule.
  Regression: tests/test_issue122_datesinperiod_month_ends.py.
- **doc 51 (GH #120)** -- CLOSED (0.9.121). ALLSELECTED(<table>) as a table
  returned a CALCULATE marker, so iterators over it saw no rows (Matrix Bubble
  Chart drew no bubbles). Now the table's rows under every selection reaching
  it; the column form keeps only its own columns' selection (Desktop, 400-cell
  matrix: 390 match, the other 10 are #118). Regression:
  tests/test_issue120_allselected_table.py.
- **doc 50 (GH #119)** -- CLOSED (0.9.121). ThemeDataColor read
  dataColors[ColorId]; Desktop's picker row is [white, black] + 8 data
  colours, so pbix_extract_colors called white blue and pbix_recolor painted
  white fills the new primary. Shade = Desktop's own function (equal on 7,560
  pairs under Node). Regression: tests/test_issue119_theme_data_color.py.
- **doc 49 (GH #112)** -- CLOSED (0.9.120). The PBIR reader dropped
  `parentGroupName` and `visualGroup`, so grouped visuals read back as
  top-level at their group-relative positions. Now a child carries
  `parentGroupName` and a group carries `singleVisualGroup` (groupMode
  ScaleMode = 0, ScrollMode = 1, isHidden), as Desktop writes classic groups.
  The writer puts changes back into visual.json, and Desktop opens the edited
  template. Regression: tests/test_issue112_pbir_groups.py.
- **GH #113** -- CLOSED (0.9.120). ISCROSSFILTERED(<table>) was never TRUE,
  and the column form missed its own table's other columns and filters on
  column combinations. Both functions now share one argument parser and answer
  from the filter context outside an iterator. Desktop-verified, a 276-cell
  battery. Regression: tests/test_issue113_iscrossfiltered.py.
- **doc 48 (GH #110, #111)** -- CLOSED (0.9.120). ROUND was Python's round
  (ties to even, on the double). It now rounds half away from zero on the
  decimal value. ISFILTERED(<table>) was always FALSE; it is now TRUE under a
  direct filter on any of the table's columns. Desktop-verified, 32 of 32
  cells. Regression: tests/test_issue110_round_half_away.py,
  tests/test_issue111_isfiltered_table.py.
- **doc 47 (GH #93)** -- CLOSED (0.9.117). The fill colour was written on the
  selector-less entry, which Desktop does not read on shape / actionButton /
  pageNavigator / bookmarkNavigator: now `show` selector-less and fillColor /
  transparency on {"id": "default"}; Desktop renders the requested colour.
  Regression: tests/test_issue93_fill_states.py.
- **doc 46 (GH #92)** -- CLOSED (0.9.116). Font sizes, background
  transparency and every other float literal were written '13.0D'; Desktop
  writes all 6,204 whole-number D literals in 37 Desktop-authored reports bare
  ('13D'). The literal writer now does too. Regression:
  tests/test_issue92_whole_number_doubles.py.
- **GH #91** -- CLOSED (0.9.115). pbix_set_m_code returned success but left
  the DataMashup unparseable when its MS-QDEFF Metadata ends with a content zip
  (Microsoft's 2018 Fuzzy Matching demo, COVID-19 US Tracking template). It now
  rewrites only the PackageParts and their length; Desktop opens the edited
  sample and shows the edit. Regression: tests/test_issue91_datamashup_writer.py.
- **PR #90** -- MERGED (0.9.115, @allanon2). pbix_get_m_code read the wrong
  archive on those same files; now reads PackageParts by their length prefix.
  Regression: tests/test_datamashup_package_parts.py.
- **PR #89** -- MERGED (0.9.115, @allanon2). The grouped and per-dimension
  tools answered None for every measure with a bare [Column]; they now pass the
  measures' home tables (Desktop-verified). Regression:
  tests/test_batch_measure_home_tables.py.
- **GH #88** -- CLOSED (0.9.114). evaluate_measures_smart's row-context
  simulation tried `list(set(values))[0]`, whose order follows the hash seed:
  the answer changed between runs. Now the first value in data order.
  Regression: tests/test_issue88_simulation_determinism.py.
- **GH #87** -- CLOSED (0.9.114). CALCULATE applied ALL / REMOVEFILTERS /
  ALLEXCEPT in argument order, so one written after a filter cancelled it;
  KEEPFILTERS intersected the pre-modifier context; time intelligence rebuilt
  the context and lost ALL's snapshot. Modifiers now apply first, as in
  Desktop (38 measured shapes). Regression:
  tests/test_issue87_calculate_modifier_order.py.
- **doc 45 (GH #84)** -- CLOSED (0.9.113). Padding and spacing were written as
  Int64 literals (`'8L'`, and 7.5 truncated to `'7L'`), border and drop-shadow
  numbers as `'1.0D'`. Desktop writes integral D literals for all four cards
  (892 / 226 / 76 / 164 of 169 in 36 Desktop-authored files). Regression:
  tests/test_issue84_container_literals.py.
- **GH #86** -- CLOSED (0.9.113). `pbix_evaluate_dax_per_dimension` ignored
  marked date tables -- the gap #79 closed in the grouped tool -- and left out
  the BLANK group. Regression: tests/test_issue86_per_dimension_tool.py.
- **GH #85** -- CLOSED (0.9.113). `CALCULATE(e, TREATAS(...))` applied no
  filter (a marker-shape mismatch), so the unfiltered value came back. It now
  replaces the column's filter (KEEPFILTERS intersects; an empty table filters
  to nothing). 12 Desktop-measured shapes. Regression:
  tests/test_issue85_treatas_filter.py.
- **GH #83** -- CLOSED (0.9.113). The model-wide relationship-propagation memo
  was keyed without the relationship set, so USERELATIONSHIP / CROSSFILTER
  results and plain ones served each other -- across calls. Their context also
  dropped the grouping and the selection. Regression:
  tests/test_issue83_relationship_cache.py.
- **GH #82** -- CLOSED (0.9.113). The engine's blank (unknown) member: the
  blank row Power BI adds when a relationship holds unmatched or blank keys
  was missing from VALUES, ALL, filters that keep BLANK, grouping, inactive /
  bidirectional / 1:1 relationships and snowflakes; ISBLANK / `= BLANK()`
  CALCULATE filters applied no filter. 147 of 147 Desktop queries match (was
  49). Regression: tests/test_issue82_blank_member.py.
- **GH #81** -- CLOSED (0.9.113). The builder joined an unmatched fact key to
  the dimension's FIRST row; it now writes Desktop's encoding (R$ slot 0 and
  RelationshipIndexStorage.Flags 1), identical before and after a Desktop
  refresh on 147 queries. Regression:
  tests/test_issue81_orphan_keys_blank_member.py.
- **GH #80** -- CLOSED (0.9.112; the blank-row remainder in 0.9.113).
  KEEPFILTERS kept a column in scope only for the plain-predicate form on a
  grouped column. Regression: tests/test_issue80_keepfilters_scope.py.
- **GH #79** -- CLOSED (0.9.112). `pbix_evaluate_dax_grouped` ignored marked
  date tables and answered ISINSCOPE FALSE on its own grouped columns.
  Regression: tests/test_issue79_grouped_tool.py.
- **GH #78** -- CLOSED (0.9.112). A date filter clears the date table's other
  filters only where Power BI does: a marked date table's date column, or a
  DateTime relationship column. Regression:
  tests/test_issue78_date_table_rule.py.
- **GH #77** -- MERGED (0.9.111, PR by @allanon2). ISINSCOPE through a context
  transition. Regression: tests/test_isinscope_context_transition.py.

- **GH #76** -- CLOSED (0.9.109). `report_filter_json` was written verbatim:
  the documented `{target, operator, values}` example produced
  schema-invalid bookmarks (no `name`, three disallowed keys). Now converted
  to Desktop's filter container, typed to the column; a full container passes
  through, anything else is refused. Desktop-verified that a bookmark filters
  ONLY through a report-level filter card on that column, so a missing card is
  added (unselected) and reported. Regression:
  tests/test_issue76_bookmark_report_filters.py + a PBIR conformance case.
- **GH #75** -- CLOSED (0.9.108). Builder warnings (case-fold #43, row fields
  that are not columns, a hierarchy a rebuild dropped, relationship type
  mismatch / orphan keys) went to Python's `warnings` only; every MCP tool
  answered `warnings: []`. Now forwarded, scoped to the tables the call
  touched. Regression: tests/test_issue75_build_warnings_reach_mcp.py.
- **GH #74** -- CLOSED (0.9.107). `outline` colour and width never applied on
  any visual: `outline.color` on visuals whose card names it `lineColor`, the
  stroke on the selector-less entry where Desktop reads it only from
  `{"id": "default"}`, and a card written on visuals that have none (tables,
  charts). Desktop-verified on all seven outline visuals. Regression:
  tests/test_outline_card.py.
- **doc 44 (GH #67)** -- CLOSED (0.9.106). `fontColor` dropped on
  title/subtitle/legend; the legend wrote a property no legend reads
  (`labelColor` is right); keys a card does not read are now reported.
  Regression: tests/test_issue67_font_color_and_ignored_keys.py.
- **findings-24 (GH #26)** -- CLOSED (0.9.79). ALLSELECTED semantics under
  grouped evaluation: the engine now distinguishes group-by filters from the
  caller's slicer (`DAXContext.group_keys` + `selected_filters` threaded
  through the grouped/per-dimension tools). Bare `ALLSELECTED()` and
  `ALLSELECTED(T)` were silent no-ops (percent-of-total = flat 1.0);
  `ALLSELECTED(T[C])` behaved as `ALL` (180 where Desktop answers the 60
  slicer total, incl. the sliced-column == grouped-column overwrite case);
  two column-scoped args no longer degrade to ALL (outer Reg=N survives).
  Regression: tests/test_issue25_26_dax.py.
- **findings-23 (GH #25)** -- CLOSED (0.9.79). r23#1 window functions
  (ROWNUMBER/RANK/OFFSET/INDEX/WINDOW) now work in measures under grouped
  evaluation via a synthesized visual axis (selected ORDERBY/PARTITIONBY
  values + the group's own filter as current position); r23#2 ALLSELECTED as
  FILTER/RANKX source (running totals 30/65/105/180, RANKX 4/3/2/1); r23#3
  ALL(table)-as-FILTER-source run-verified correct + pinned; r23#5 DATEVALUE
  over columns does not reproduce + pinned; r23#4 O(n^2) accumulation form
  documented in limitations.md; r23#6 add_calculated_column docstring states
  the measured supported/refused surface.
- **findings-22 (GH #24)** -- CLOSED (0.9.79). r22#1 datetimes now leave the
  engine in ONE shape (ISO-8601 + a data_type field on every result; date
  arithmetic keeps its type); r22#2 add_measure infers DateTime (9) for
  datetime measures; r22#3 query_metadata surfaces the real SQL error instead
  of a masking WinError 32. Regression: tests/test_issue24_type_info.py.
- **findings-21 (GH #23)** -- also closed on GitHub with 0.9.77 evidence; GH
  #15 (r13), #18 (r16), #19 (r17) closed as verified-fixed/attributed on
  0.9.72-0.9.78 work.
- **findings-21** -- CLOSED (0.9.77). `pbix_set_table_data` left the report
  unqueryable in Desktop ("Error fetching data for this visual"). Root cause: a
  column type passed under `dataType` (camelCase) or a lowercase name (`int64`)
  was ignored -- only `data_type` was read -- so every column silently defaulted
  to `String`. A numeric column then shipped as text, `SUM()` over it returned
  BLANK, and every measure-bound visual errored while the tool reported success
  and `pbix_query_table` read the (text) rows. Engine-reproduced: `[Val] =
  SUM(S[V])` went 550 -> blank on the call, and returns the correct 3000 with the
  fix. `PBIXBuilder.add_table` now normalizes the type key (`data_type`/`dataType`
  /`type`) and value (case-insensitive) and refuses an unrecognized type rather
  than defaulting to String; the malformed-`columns` payloads that used to leak a
  raw `"string indices must be integers"` TypeError now give a clear shape error.
  `tests/test_set_table_data_typing.py` (10). The secondary "contradictory error
  messages" note in the report is resolved by the same clear-error path.
- **findings-20** -- CLOSED (0.9.73). `RELATED()` inside an iterator ignored
  row context: it resolved to the first *visible* row of the related table for
  every iterated row, so `SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))`
  gave 839.72 instead of 599.72 (grouped output masked it; grand totals and
  cards were wrong). `_fn_related` now navigates from the current row's FK
  through active relationships (single/multi-hop); `RELATED`/`RELATEDTABLE`
  removed from the FILTER aggregation guard so `FILTER(T, RELATED(...) = v)`
  binds the current row. Desktop-pinned probes (RELATED -> 350/7/1) +
  `tests/test_related_rowcontext.py` (9 cases). Reported by OpenBI's bridge
  suite after the 0.9.55 -> 0.9.72 upgrade.
- **issues-19** -- `pbix_bind_field_parameter` (0.9.57), plus the dangling-OrderBy
  and wells-less-visual fixes (0.9.58). All 8 pieces of the binding diffed
  IDENTICAL against the Desktop-authored artifact in that doc.
- **issues-18** -- assigning `filter_context` now clears the column memo (0.9.59).

