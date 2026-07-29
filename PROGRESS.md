# Session progress — 2026-07-28

Working note for picking this back up. Records what was done, what is verified
and how, what is left, and the traps already hit so they are not re-hit.

## Where things stand

| issue | state |
|---|---|
| **#3** service verification of the rebuild path | **CLOSED** — all 3 schema eras verified live in Power BI Desktop |
| **#4** calc columns blocking corpus files | partial — 14/24 files OK (was 11); group (a) .[Date] DONE, 2 groups left |
| **#5** columns fail to decode | **root-caused, fixed, tested** — pending sweep + release |
| **#6** DAX perf / compiled expression tree | not started — but the bottleneck is now MEASURED, see below |
| **#7** calc groups, translations, detail-rows | not started |

Released today: **0.9.50, 0.9.51, 0.9.52, 0.9.53** (all on PyPI + GitHub, CI green).

## The verification method that actually works

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
* Gates at this point: ruff clean, mypy 140 (baseline), **1007 unit tests pass**.

Next: finish the 21-file regression sweep, release #5 + `.[Date]` together,
close #5, then #4 group (b) cross-table refs.
