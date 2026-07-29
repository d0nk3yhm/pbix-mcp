# Session progress — 2026-07-28 → 07-29

Working note for picking this back up. Records what was done, what is verified
and how, what is left, and the traps already hit so they are not re-hit.

## Where things stand

| issue | state |
|---|---|
| **#3** service verification of the rebuild path | **CLOSED** — all 3 schema eras verified live in Power BI Desktop |
| **#4** calc columns blocking corpus files | groups (a) `.[Date]`, (b) LOOKUPVALUE + RELATED, (c) CALCULATE/FILTER all **DONE** |
| **#5** columns fail to decode | **root-caused, fixed, tested, committed** |
| **#6** DAX perf / compiled expression tree | partial — two memoizations landed (1h+ -> 1053s on a 1.29M-row file); the compiled tree itself is still open |
| **#7** calc groups, translations, detail-rows | **DONE** — verified live in Desktop via INFO.CALCULATIONGROUPS() |

Released 07-28: **0.9.50-0.9.53**. **0.9.54** closes #4, #5 and #7.

**Corpus: 23 of 24 rebuild** (was 11). The one refusal is MS_Perf_Analyzer's
 = , a genuine table scan this engine does not
implement -- refused deliberately, not a bug.

## Verifying against Desktop WITHOUT opening Desktop

The single most valuable tool this session. The corpus files were authored in
Power BI Desktop, so a calculated column's **stored VertiPaq values ARE
Desktop's answer**. Recomputing from the DAX and diffing them is exact ground
truth, needs no GUI, and covers hundreds of thousands of rows at once.

```
scratchpad/gt_all.py    every calc column in the corpus vs Desktop's stored values
scratchpad/lv_gt.py     the cross-table (LOOKUPVALUE / RELATED) subset
```

It found **four silent-wrong-value bugs the whole unit suite missed**. Run it
after any change to the DAX engine or the calc-column evaluator.

Two rules learned the hard way:

* **Skip volatile expressions.** A `TODAY()` column's stored values were
  computed when the file was last refreshed. `MS_AI_Sample`
  `Opportunities[Days Remaining In Pipeline]` read as a 2467-row MISMATCH that
  was purely the 973 days since authoring — every difference was exactly -973,
  and the 2467 rows were exactly the `Status="Open"` ones. Not a bug.
* **Qualify bare references first**, as `_materialize_table_calc_columns` does.
  Without it the tool reports failures the real path never has.

## Verifying against a LIVE Desktop (when the GUI is needed)

Everything below was checked against **Power BI Desktop's own Analysis Services
engine**, not against documentation and not against expectations.

```
scratchpad/verify_live.ps1   open a .pbix, wait for the window title, run DAX
scratchpad/live_dax.ps1      find the msmdsrv port, connect ADOMD, run a .dax file
scratchpad/diff_meta.py      field-level metadata diff between two .pbix (name-matched)
scratchpad/calc_ground_truth.py  our re-materialized calc columns vs Desktop's stored ones
```

Two hard-won details:

* Desktop opens some files behind a modal **"Enter your email address"** sign-in
  prompt that blocks the model load forever. `verify_live.ps1` dismisses it with
  Cancel after 25s. Never sign in.
* Screenshot pixels are **not** desktop coordinates. The virtual screen starts at
  x=-2560 on this machine, so every synthetic click must add `VirtualScreen.Left`.
  Clicks also need `AttachThreadInput` + `SetForegroundWindow` or Windows drops
  them silently. `click_shot.ps1` handles both.

**Always compare an edit against a CONTROL** (open + save, no edit), never
against the original — saving legitimately rewrites members on its own. The
control has been byte-clean on every metadata table all session.

## What was fixed

### Issue #3 (closed) — four reasons Desktop refused a rebuilt file

1. **metadata.sqlitedb sized from a stale record.** The VirtualDirectory's size
   was updated but not the BackupLog's, and **AS sizes the file from the
   BackupLog** → truncated SQLite → "The database disk image is malformed.
   SQLite Error Code=11". Two causes: a `len(old)==len(new)` guard skipped any
   size whose digit count changed, and the two regions have **different
   encodings in one file** (VirtualDirectory UTF-8, BackupLog UTF-16-LE).
2. **Legacy `Type=1` (Query) partitions rewritten to inline M**, orphaning the
   DataMashup query → empty Data pane + "pending changes in your queries".
3. **Two columns claimed to be the key.** The builder marks every RowNumber
   `IsKey`; corpus ground truth across 248 RowNumber columns says `IsKey` is 0
   for exactly the 14 whose table has another key column.
4. **Calc-table columns lost their table qualifier** (`DateAutoTemplate[Year]`
   became `[Year]`) → "Relationship points to deleted column".

Plus: measures/hierarchies kept only name and expression (89 of 102 measures
lost their DisplayFolder), and **Windows joined the CI matrix** — three unclosed
SQLite handles made every calc-column edit fail on Windows while Ubuntu-only CI
stayed green.

### Silently-wrong-value bugs (the class this project treats as unacceptable)

* **`INT` truncated instead of flooring.** `INT(-1.5)` gave -1, Desktop gives -2.
  Worst in the binning idiom Power BI's own "New group" generates:
  `INT(-1612/5)*5` → we said -1610, Desktop says -1615. **Wrong bin.**
* **BLANK did not compare like DAX.** DAX coerces BLANK to the *zero of the other
  operand's type*; we returned BLANK so every test fell to ELSE. On
  `IF(T[x]<30,20,IF(T[x]<45,30,80))` every blank row scored 80 where Desktop
  scores 20. `Indicators[Basic drinking water services]` disagreed with Desktop
  on **70,562 of 72,645 rows**.
* **A double quote in the data deleted the whole value.** DAX escapes quotes by
  doubling, so `6" pipe` has four quote chars; the parser demanded exactly two.
* **`//` or `--` inside a string truncated the expression**, deleting whole
  column references. The unresolved-reference check was structurally blind: it
  only sees text *after* comment stripping.
* **11% of every model's data columns silently dropped on read.**
  `read_table_from_abf` skipped any column whose IDFMETA said `is_row_number` —
  a flag Desktop leaves 0 on ordinary columns. 126 of 1121 user columns across
  the corpus, including `IT_Support` `dim_Date[Date]`, the column its
  relationships join on, in a file every check called clean.

### Issue #5 — root cause found (this is the important one)

`primary_segment_size` in an IDF segment is the **allocated capacity** of the
primary array, not the number of entries in use. It is always a power of two
(16, 32, 64, 256, 2048, 4096, 8192) while the used entries are far fewer. The
real entries end **exactly when their run lengths sum to the segment's row
count**; the slots after that are sub-segment bytes that read as nonsensical
`(data_value, repeat)` pairs.

Reading the full capacity summed garbage run lengths. It only surfaced when the
garbage was large, which is why it looked column-specific: on the 1,290,259-row
`Employee` table, 13 columns decoded fine while `date`, `Gender` and `FP` blew
past the sanity limit — `Gender` summed to **93,629,586,803 rows**.

Verified on all 16 columns of that table: **every one reaches exactly 1,290,259
and stops.** Same root cause as the four `Fact` columns of
`MS_Corporate_Spend.pbix` this issue was opened for.

Fix: `_decode_idf_segment_at` stops consuming entries once the segment's rows
are complete; per-segment record counts are threaded through for multi-segment
columns.

**Offline verification** (Desktop was in use, so this was not GUI-confirmed):
* structural — run lengths sum to exactly the row count on all 16 columns;
* referential — 6 of 7 `Fact` relationships resolve with **0 orphans**;
* semantic — ages 14..96, 2 genders, dates in range.

The 7th relationship (`Fact[Cost Element ID]`) orphans because the **model
itself** declares it String while `Cost Element[Cost Element ID]` is Double — a
type mismatch in the source file, not a decode fault. Confirmed from metadata.

**Still worth doing:** open `MS_Corporate_Spend.pbix` and `MS_Employee_Hiring.pbix`
in Desktop and compare `SUM(Fact[Value])`, `DISTINCTCOUNT` etc. against our
decode. Queries are ready at `scratchpad/q_cs.dax` and `scratchpad/q_eh.dax`.

## What is left

### Issue #4 — 3 groups, smallest first

1. **Auto-date variation accessor** (1 file). `MS_Revenue_Opportunities.pbix`
   has `Fact[Date] = 'Fact'[EstimatedCloseDate].[Date]` — the `.[Date]` drills
   into the hidden LocalDateTable for that column. Smallest remaining piece.
   **Semantics already VERIFIED: `X.[Date]` is the date part of X — 458/458
   exact against Desktop's own stored `Fact[Date]` values.** A `Variation` row
   links ColumnID -> RelationshipID -> the LocalDateTable, but the value needs
   no traversal at evaluation time. Only `.[Date]` is verified; refuse the other
   parts (`.[Year]`, `.[Month]`, ...) until each is checked — they map to
   auto-date template columns with locale-dependent display strings.
2. **Cross-table references** (5 files) — `RELATED`, `LOOKUPVALUE`, or
   `'Other'[Col]` directly. Needs relationship traversal in row context; the
   graph is already available via `_relationships_from_metadata`.
3. **CALCULATE / FILTER** (3 files) — needs a real filter context per row. The
   largest single piece of work left.

### The calculated-column gate is deliberately narrow

Only `MIN`, `MAX`, `SUM`, `AVERAGE`, **single argument, one column of the target
table**. An adversarial review of a broader first cut (61 agents; 19 candidates
→ 3 confirmed, 16 refuted) demonstrated six ways it would write wrong values.
Do not widen it without re-checking each:

| shape | what it did |
|---|---|
| `MIN(T[Amount], 0)` | the two-argument **scalar** overload — 0 on every row |
| `COUNTROWS(Other)` | a bare table name is invisible to the cross-table check |
| `MIN([Yeer])` | a typo'd column answers 0 rather than failing |
| `DISTINCTCOUNT`/`COUNT`/`COUNTA`/`COUNTBLANK` | disagree with DAX on BLANK/`""` |
| `STDEV.P`, `VAR.S`, `PERCENTILEX.INC` | DAX spells these with a **dot** |
| `[Total Count (n)]` | a column *named* after an aggregate got chopped |

The scalar overload IS allowed when neither argument reads a column — that is how
Power BI's own binning clamps a bin number, `MIN(__BinNumber, __Count - 1)`.

### Issues #6 and #7

Untouched. #6 is gated on baselining every corpus measure first and asserting
byte-identical results, per its own terms. #7 needs a .pbix authored in Desktop
containing a calculation group, perspective, detail rows and a translation.

## Traps already hit

* **Do not trust a green-looking CI without opening the run.** CI was red for
  0.9.50 and 0.9.51 and was reported here as green. Two real bugs were hiding in
  it: a Windows stale-cache race (a slicer edit rewrites the layout to the *same*
  byte length, so inside one mtime tick the change-stamp was identical and the
  next evaluate served the previous slicer's answer) and an unbounded
  `mcp>=1.0.0` that let **mcp 2.0.0 break `pip install pbix-mcp` outright**
  (it removed `mcp.server.fastmcp`). Now pinned `<2`.
* **The corpus sweep writes a temp dir of rebuilt .pbix per run.** Dozens of runs
  filled C: to 0.2 GB free. Clean `%LOCALAPPDATA%\Temp\tmp*` dirs containing
  `o*.pbix`, and `pytest-of-*`. (~10.8 GB of Visual Studio installer cache also
  sits there — not ours.)
* **Every new test must be confirmed to FAIL against the previous release.**
  Several "passing" tests turned out to assert behaviour that never worked.
* Run the sweep **uncontended**. With ~20 stray Python processes the slow suite
  took 2 hours instead of 13 minutes.

## Issue #6 — the bottleneck is measured, and it is not where you'd guess

Measured 2026-07-29 on this machine. The full corpus sweep went from ~25 min to
**3.3+ CPU-hours** once the Issue #5 decode fix made three 1.29M-row tables
readable for the first time. Breaking that down:

| stage | measured | @ 1,290,259 rows |
|---|---|---|
| VertiPaq decode | 14.2 s for 1.29M rows | linear, fine |
| builder encode | 3.0 s / 100k rows (flat from 10k→200k) | ~39 s, linear, fine |
| **calc-column evaluation** | **13.6 s / 100k rows** | **~2.9 min PER COLUMN** |

`MS_Employee_Hiring.Employee` has **5** calculated columns, and the sweep builds
each file twice (control + edit), so that one table alone is ~30 minutes; two
files with the same shape is an hour before anything else runs.

Decode and encode are both linear with small constants — they are not the
problem. The calc-column path costs ~136 microseconds **per row per column**
because `evaluate_row_context_column` string-substitutes the row's values into
the expression and **re-parses the whole DAX string for every row**.

That is precisely what issue #6 asks for: parse once into an expression tree,
then evaluate the tree per row. The correctness gate for that work already
exists — `scratchpad/calc_ground_truth.py` compares our re-materialized values
against Desktop's own stored values, and the corpus files whose calc columns are
already verified exact (MS_Employee_Hiring MonthIncrementNumber 13/13,
MS_Life_Expectancy binning 250/250 and its BLANK-comparison columns 400/400,
MS_Regional_Sales Weeks Open 12/12) make a ready-made byte-identical baseline.

Practical note: until this is fixed, do NOT run the full 24-file sweep as a
routine gate. Run it excluding MS_Employee_Hiring / MS_Human_Resources /
MS_Corporate_Spend for a fast regression signal, and run those three separately
when specifically testing them.


## Done 2026-07-29 (early hours)

* **#5 decode fix** implemented + 8 regression tests (all confirmed failing
  without it). `MS_Employee_Hiring`, `MS_Human_Resources` and
  `MS_Corporate_Spend` all decode now. Uncommitted, pending the sweep.
* **#4 group (a) — auto-date `.[Date]` accessor DONE.** `expand_variation_accessors`
  in `dax/calc_tables.py` rewrites `X.[Date]` to
  `DATE(YEAR(X), MONTH(X), DAY(X))` — verified primitives only. Applied in BOTH
  the gate and the evaluator so they cannot disagree.
  * **458/458 exact** against Desktop's own stored `Fact[Date]`.
  * `.[Year]`, `.[Month]`, `.[Quarter]`, `.[Day]`, `.[MonthNo]` deliberately
    NOT expanded — they map to auto-date template columns with locale-dependent
    display strings. Each is confirmed to **refuse**, never materialize a guess.
  * 8 tests; 3 fail against the previous release.
  * `MS_Revenue_Opportunities.pbix` now **OK** → **15/24**. Sweep clean:
    0 fidelity findings.
* **Ordering fix in `_materialize_table_calc_columns`** — expand the accessor
  BEFORE `_qualify_bare_column_refs`. This is a *separate latent* bug, NOT what
  unblocked the file above (I first claimed it was; wrong — see the trap below).
  When the table also has a real column named `Date`, the qualifier reads the
  accessor's own `[Date]` as a bare same-table reference and rewrites it to
  `'T'[Close].'T'[Date]`, which stops matching `_VARIATION_ACCESSOR` — expansion
  silently stops firing and the column is refused. Common shape on fact tables.

### Trap: a repro built with inputs the real path never passes

I diagnosed the above from a hand-built `col_names` that included `Date`, saw
`'Fact'[EstimatedCloseDate].'Fact'[Date]`, and concluded that was why the file
was refused. Instrumenting the **real** call showed `col_names` has no `Date`
at that point, so the qualifier leaves the accessor alone. Reverting only the
server.py edit still left the file OK — the `calc_tables.py` expansion alone
fixed it.

The first regression test I wrote passed with AND without the fix, because it
inherited the same wrong `col_names`. **A test that passes both ways proves
nothing** — always run it against the reverted fix. The kept test uses
`col_names` containing `Date` and is confirmed to fail without the ordering fix.

* Gates at this point: ruff clean, mypy 140 (baseline), **1009 unit tests pass**.

Next: release #5 + `.[Date]` together, close #5, then #4 group (b) cross-table
refs.

---

# Session 2026-07-29 — issue #4 finished, four engine bugs found

## Issue #4 is complete: all three groups

**(a) auto-date `.[Date]`** — `expand_variation_accessors` rewrites `X.[Date]`
to `DATE(YEAR(X), MONTH(X), DAY(X))`. Applied in BOTH the gate and the
evaluator so they cannot disagree, and **before** `_qualify_bare_column_refs`
(see the trap below). Only `.[Date]` is expanded; `.[Year]`, `.[Month]`,
`.[Quarter]`, `.[Day]`, `.[MonthNo]` map to auto-date template columns with
locale-dependent display strings and are confirmed to REFUSE.

**(b) cross-table refs** — LOOKUPVALUE and RELATED.

* Both are parsed, validated against the model's real tables/columns, and
  MASKED so the cross-table scan never sees them; the value sub-expressions are
  pulled back out and re-gated on their own, so masking cannot smuggle an
  unsupported expression past the gate.
* Indexes are built ONCE per column, not per row.
* LOOKUPVALUE: several matching rows are fine when they agree; genuinely
  ambiguous matches are an ERROR in DAX, so the column is refused rather than
  picking one. A miss yields the alternate result if supplied, else BLANK.
  String matching is case-INSENSITIVE (Power BI's default collation).
* RELATED walks many-to-one only (From → To in AMO) across ACTIVE
  relationships. Two paths, no path, wrong direction, a bare `RELATED([Col])`,
  or an inactive relationship are each refused, never guessed.
* A caller that supplies neither `known_tables` nor `relationships` keeps the
  old strictly-safe refusal, so nothing regressed for pure-python callers.

**(c) CALCULATE / FILTER** — the semantic that makes it tractable: in a
calculated column CALCULATE performs context transition, and a FILTER over that
SAME table replaces the transitioned filter entirely. So `FILTER(T, cond)` and
`FILTER(ALL(T), cond)` both reduce to *aggregate every row of T satisfying
cond*. Confirmed against Desktop's stored values, not assumed.

Performance mattered: `MS_Covid_Tracking`'s COVID table has **1,740,185 rows**,
so a literal per-row scan is ~3e12 operations. The predicate is COMPILED
instead:

* equality terms → a hash index, one dict lookup per row;
* one inequality → a prefix aggregate over each group sorted by that column,
  answered with a bisect;
* right-hand sides are memoised on just the columns they mention, so
  `DATEADD(COVID[Date], -1, DAY)` costs one evaluation per distinct date.

`VAR`s are inlined into the predicate so scoping is not reimplemented.

## Four silent-wrong-value bugs the unit suite never saw

All four were found by diffing against Desktop's stored values, and all four
now have tests that fail against the previous code.

1. **`-` and `/` were RIGHT-associative.** `_eval_binary` evaluated `parts[0]`
   against the REJOINED tail: `10 - 3 - 2` → 9 instead of 5, `20 / 4 / 5` → 25
   instead of 1. This hit **every measure and calculated column** containing a
   repeated subtraction or division. Found because
   `MS_Competitive_Marketing` `Date[Rolling Period]` disagreed with Desktop on
   1096 of 6209 rows. Splitting on `+` before `-` is still correct, because
   `a - b + c` groups as `(a - b) + c`; only the fold had to become left-to-right.
2. **`&` rendered every FALSY value as empty** — `str(v or '')`. The
   zero-padding idiom `RIGHT("0" & n, 2)` lost its pad on exactly the rows
   where n was 0: `'P-00'` became `'P-0'`, 93 rows.
3. **A DATE is a number in DAX** (days since 1899-12-30) but `_as_number`
   returned None for one, so `[Timestamp] * 86400000` was BLANK on every row.
   Dates reach the evaluator as ISO strings; `_as_datetime` also truncated the
   fractional seconds, leaving timestamps rounded to the whole second.
4. **FORMAT** ignored leading-zero pictures (`FORMAT(1, "000")` → `"1"`, not
   `"001"` — which changes sort order, not just display) and recognised only
   lower-case date tokens, so `FORMAT(d, "YYYY-MM-DD")` returned the literal
   `"YYYY-01-DD"`.

Plus a fifth, in the new code: an **unresolved column reference** inside a
LOOKUPVALUE search value or a FILTER right-hand side reads as blank, matches
nothing, and materialises BLANK rather than failing. `Events[ParentIndex]` was
blank on 102 of 117 rows where Desktop had a value. Both paths now check
`_unresolved_refs` after substitution and refuse.

## Traps hit this session

**A repro built with inputs the real path never passes.** I diagnosed the
`.[Date]` refusal from a hand-built `col_names` that contained `Date`, saw the
qualifier mangle the accessor, and concluded that was the cause. Instrumenting
the REAL call showed `col_names` has no `Date` there. Reverting only the
server.py edit left the file still OK — the `calc_tables.py` expansion alone
fixed it. The ordering fix is real but guards a DIFFERENT, latent case (a table
that also has a column named `Date`).

**A test that passes both ways proves nothing.** The first regression test for
that fix inherited the same wrong `col_names` and passed with AND without the
fix. Always run a new test against the reverted fix.

**The engine can answer with a non-scalar and not flag it.** `DATEADD` returns
a marker tuple `('__DATEADD__', ...)` and registers nothing in
`unsupported_functions`. Compared against index keys it simply never matched,
so the CALCULATE collapsed to its empty result on every row. Filter values are
now required to be scalars.

## Known limitation

DateTime columns decode to Python `datetime` (microsecond resolution) while
VertiPaq stores a double serial. Scaling a timestamp to milliseconds therefore
differs from Desktop in the sub-microsecond digits — e.g. `3796149882573.3325`
vs `3796149882573.333`. Visible on `MS_Perf_Analyzer`'s trace tables, which are
refused for other reasons (RANKX, PATH/PATHITEM) anyway.

## Issue #7 — calculation groups now survive a rebuild

The issue asked for a .pbix authored in Desktop. Not needed: this project's own
`pbix_datamodel_add_calculation_group` builds one, which is also the exact case
the issue calls out ("a model this project itself created can contain
calculation groups that a later rebuild-path edit would drop").

Reproduced first. Authoring a group on `GeoSales_Dashboard` and then editing a
table dropped `CalculationGroup`, `CalculationItem`, `[Table]
.CalculationGroupID` and the Type=7 partition **to zero**, with `success: true`
and an empty warnings list.

All 14 tables from the issue are now in `_CARRY_SPEC`. Two things the generic
carry could not do on its own:

* **A calculation group is wired from BOTH ends.** `[Table].CalculationGroupID`
  points back at the group, and the partition must be `Type=7`. Without them the
  group's table loads present but inert.
* **Optional self-references (`self:X?`) were declared but never worked** — the
  snapshot built its remap key from the raw kind string, so it looked for a
  table literally called `FormatStringDefinition?`. Now a calculation item
  survives losing its format string instead of vanishing with it.

### The Desktop-only failure

Every metadata check passed — referential integrity clean, no dangling keys,
all four counts restored, doctor reporting nothing the source file did not
already have — and Power BI **still refused the file**:

> Partition 'Time Intelligence' in table 'Time Intelligence' has the
> QueryDefinition property set which is not a valid field for this partition type.

The rebuild writes an Enter-data M query for every partition; setting `Type=7`
left it in place. Ground truth from an authored group: a Type=7 partition
carries NO `QueryDefinition`, and every other field already matched byte for
byte. **Only opening the file in Desktop found this** — a standing argument for
the GUI check even when every offline gate is green.

Now verified against Desktop's own engine: the report opens, and
`INFO.CALCULATIONGROUPS()` / `INFO.CALCULATIONITEMS()` answer **1 group, 2
items** — `Current` ordinal 0, `YTD` ordinal 1 — after a rebuild.

## Next

1. Release the accumulated work (#4, #5, #7) and close the three issues.
2. **#6** — compiled DAX expression tree. The CALCULATE compiler added for #4
   is the same idea applied to one construct, and is a reasonable template.
