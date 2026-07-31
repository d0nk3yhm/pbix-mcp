# Session progress — 2026-07-28 → 07-30

Working note for picking this back up. Records what was done, what is verified
and how, what is left, and the traps already hit so they are not re-hit.

## Where things stand

**0.9.62 is released** (PyPI + GitHub, 2026-07-31), verified by installing it
from PyPI into a clean venv: `pbix_mcp 0.9.62` on Python 3.13, pure-Python, and
`FORMAT(DATE(2021,7,19),"mmmm dd, yyyy")` returns `July 19, 2021` from the
published artifact.

**Note on 0.9.61: it was never published.** This file previously called it "the
last released version". There is no `v0.9.61` tag, no GitHub release, and PyPI
goes 0.9.60 -> 0.9.62. It was a version-bump commit only; its CHANGELOG section
documents work that actually shipped inside 0.9.62. The last release before
today was **0.9.60**.

A sixth fix (directional filter propagation) was landed and reverted this cycle;
the write-up below is the one to read before re-attempting it.

| verification | scale | result |
|---|---|---|
| measures at the grand total | 547 measures, 24 files | 1:1 with Desktop |
| measures UNDER A FILTER CONTEXT | ~1,700 measure x dimension-value cells | see per-file table below |
| calculated columns vs stored VertiPaq values | 397 columns, all 25 files | 0 mismatches, 8 deliberate refusals |
| unit tests | 1,336 | pass (`pytest -m "not slow"`) |
| ruff / mypy | — | clean / 140 (the standing baseline) |

Filter-context comparisons re-run after the five landed fixes: MS_AI_Sample
44/44, Ecommerce_Conversion 132/132, MS_Competitive_Marketing 88/88,
MS_Covid_Tracking 22/22, MS_Sales_Returns 116/116, GeoSales_Dashboard 152/152,
Agents_Performance 404/408 (the four in the reverted-propagation write-up).

Earlier issues #3-#7 all remain CLOSED. The OpenBI findings ledger
(`docs/openbi-findings-ledger.md`) is the authority on what is still open.

## Verifying against Desktop — the three ground truths

Ranked by cost. Use the cheapest one that can answer the question.

**1. Stored calculated-column values (no GUI, no Desktop process).**
The corpus files were authored in Desktop, so a Type=2 column's stored VertiPaq
values ARE Desktop's answer. Recomputing from the DAX and diffing is exact and
covers hundreds of thousands of rows at once.

```
scratchpad/gt_all.py [file...]     every calc column vs Desktop's stored values
```

**2. The workspace engine, at the grand total.**
Desktop starts an `msmdsrv` for an open .pbix; querying it over ADOMD is
Desktop's own evaluator, not a re-implementation.

```
scratchpad/gen_all.py              emit gt_<file>.dax (+ .names) for every measure
scratchpad/capture_all.ps1         open each file in Desktop, run the queries
scratchpad/cmp_all_files.py        diff every captured answer against ours
```

**3. The workspace engine, UNDER A FILTER CONTEXT.**
The grand total is one cell per measure, and the cell least likely to expose a
bug: relationship propagation, ALLSELECTED, time intelligence and the blank
(unknown) member only diverge once something is filtered. This widens the
comparison to `measure x a few values of a real dimension column`, and it found
bugs the totals sweep could not — `ALL(Table)` was not clearing a filter that
reached the table through a relationship.

```
scratchpad/gen_ctx.py              pick a dimension column mechanically, emit the DAX
scratchpad/capture_ctx.ps1         capture; -Prefix gt_ctxlen for the LEN companion
scratchpad/cmp_ctx.py              diff, with the same rules as the totals sweep
```

## Traps already hit — do not re-hit

- **Desktop's sign-in modal opens BEHIND the browser.** The old dismissal was a
  synthetic click at fixed screen coordinates; its process guard correctly
  refused, silently, and 7 of 18 captures timed out at 240s. `pbi_windows.ps1
  -CloseDialogs` sends WM_CLOSE to a handle already proven to belong to
  PBIDesktop — no focus, no cursor, and it cannot land on another application.
  Never sign in; closing the dialog is Cancel.
- **`powershell` is 5.1 and reads files in the ANSI codepage.** A measure named
  `△ Sales dummy` arrived as `â–³ Sales dummy` and every query for it failed, so
  7 measures silently had no ground truth. Use `pwsh` and `-Encoding UTF8`.
- **The capture prints one line per row**, so a value containing a newline
  arrives cut off. Compare Desktop's own `LEN()` when the printed length
  disagrees with it.
- **`EVALUATE ROW()` applies no slicer defaults**; `pbix_evaluate_dax` does.
  Always pass `apply_default_filters=False` when comparing.
- **A `&& echo OK` after a linter prints nothing on FAILURE.** Print the exit
  code explicitly: `ruff check src/ tests/; echo "RUFF=$?"`.
- **Bit-identical float agreement is not achievable, and chasing it is wrong.**
  VertiPaq sums a column in parallel segments. On `MS_Corporate_Spend[Var LE1]`
  the exact decimal answer is 14,697,755.96505, this engine returns
  ...965050012 (correctly rounded) and Desktop returns ...9650462 — Desktop is
  300x further from the truth. The harness uses a 1e-9 relative band and LISTS
  every measure that needed it.
  What IS fixable, and was: rounding TWICE (`total_seconds()/86400` lands on the
  wrong double for 27% of timestamps), and the .NET 100-ns tick precision
  Python's microsecond datetime cannot hold — the decoder now carries the
  original stored serial on the value.

## Filter-context findings — what the sweep found, fixed and still open

Every entry is a measure where Desktop and this engine disagreed under a FILTER
CONTEXT, recorded with its root-cause class so the next session starts from
evidence rather than from scratch. The grand-total sweep could not see any of
them.

**FIXED since this list was written.** Each Desktop-verified, each with tests
that fail on the pre-fix code.

- `New Hires SPLY` and the whole SPLY/YoY family -- `DATEADD` shifted the single
  min..max range, so seven disjoint quarters became one span and the measure
  returned the GRAND TOTAL under every quarter. Commit 8c84783.
- Ecommerce `*_PMTD/PQTD` and `*_%Delta` @ Q1 -- a shifted period outside the
  calendar applied NO filter instead of an empty one, so PMTD/PQTD answered
  14,548,763 against Desktop's BLANK and the %Delta measures came out -1.0.
  Commit 243d836.
- **`ALL(Table)` suppressed too much** -- a PRE-EXISTING defect, found while
  diagnosing the revert below and independent of it. `_no_propagate` flagged a
  table for the rest of the evaluation, so a filter created LATER inside a
  nested CALCULATE could never propagate into it. Desktop keeps it:
  `CALCULATE(CALCULATE(AVERAGE('Cases'[CSAT]), 'Owners'[Manager]="Low, Spencer"),
  ALL('Cases'))` is 4.13796627491058 there and was 4.2706 here; the nested
  COUNTROWS was 3914 against our 10000. Suppression now carries a SNAPSHOT of
  the filters live when ALL ran -- key AND value signature. The signature is
  what makes composition right: under an outer "Weiler, Anne" with an inner
  "Low, Spencer", Desktop returns Spencer's number, so re-filtering a column
  ALL had cleared makes a NEW filter. Keying on the name alone got that case
  wrong (30 where DAX says 10). Commit edd951a.
- MS_AI_Sample's four `CSAT Impact*` measures -- a multi-column table filter
  argument now replaces propagation the way `ALL(Table)` does, scoped by that
  same snapshot. Commit 3d0c2f2, after 25e3bfe did it unscoped and was reverted.

**Read this before touching filter suppression again.** 25e3bfe fixed CSAT by
suppressing propagation for every table a multi-column row set covered, and took
MS_Employee_Hiring's `[Actives]` from Desktop's 32,401 to 1,260,817 -- about
twenty dependent measures with it. `[Actives]` is
`CALCULATE([EmpCount], FILTER(Employee, ...))`, and `[EmpCount]` creates a
`Date[PeriodNumber]` filter LATER; the blanket flag blocked it from reaching
Employee. Two things went wrong beyond the code itself:

- The first root cause written here was WRONG. It blamed a single-column ALL
  being materialised as `__row__` dicts. Measured, `FILTER(ALL('Date'[PeriodNumber]),
  ...)` has `__row__`=False and zero columns, and never matched the branch at
  all. Measure the shapes before theorising about them.
- The scoping test asserted the single-column case as `ALL(Fact[v])` DIRECTLY
  and never wrapped it in FILTER, so it passed while the shape that actually
  breaks went unchecked. `test_a_later_nested_filter_still_propagates` is now
  that shape and fails on 25e3bfe with the same 30-vs-10 signature.

Any future change here must keep BOTH anchors at once:
`[Actives]` = 32,401 and
`CALCULATE(AVERAGE('Cases'[CSAT]), FILTER(ALL('Cases'),1=1))` = 4.2706.

### Directional propagation: Desktop-correct, REVERTED anyway (6a4896a -> 8a77b9d)

Our single-hop relationship index is SYMMETRIC, so a filter propagates from the
MANY side of a relationship to the ONE side. Desktop does not do that, and this
is not in doubt -- on Agents_Performance, where
`DimStore[EmployeeKey] -> DimEmployee[EmployeeKey]`:

```
COUNTROWS(DimEmployee)                                    Desktop 293
  ... under DimStore[StoreType] = "Catalog"               Desktop 293  UNCHANGED
CALCULATE(SELECTEDVALUE(DimEmployee[EmployeeKey]), same)  Desktop BLANK, ours 213
CALCULATE(COUNTROWS(DimStore), DimEmployee[EmployeeKey]=213)
                                                          Desktop 1  one->many flows
```

Making the single-hop index directional (matching `_rel_adj`, which already had
the rule) took Agents_Performance from 404/408 to **408/408** and left five other
files unchanged. It also took MS_Employee_Hiring's `[Actives]` from Desktop's
**32,401 to None**, and about thirty dependent cells with it -- the whole
Actives / TO % / Sep% / BadHire% family. Bisected precisely: 32,401 at 3d0c2f2,
None at 6a4896a. Not a timeout -- it returns None in 201s with a 1800s budget
and an empty `timed_out` set.

So the rule is right and our engine still depends on breaking it. Reverted,
because it costs ~30 cells to buy 4.

**Where to start next time. MEASURED, and it corrects what was written here
first.** An earlier draft of this note said
`CALCULATE(COUNTROWS(Employee), 'Date'[PeriodNumber] = 201612)` returning BLANK
proved the propagation was "already broken". It is not broken -- that BLANK is
CORRECT, and the data says so:

```
Employee[date]            2011-01-01 .. 2014-12-01   (month starts, 48 distinct)
Date[Date]                2010-01-01 .. 2016-12-31
MAX('Date'[PeriodNumber])                    201612
```

Employee has NO row dated in Dec 2016, so filtering to period 201612 legitimately
selects nothing. The dimension predicate is fine too -- it selects 31 Date rows.

That reframes why the directional fix blanked `[Actives]`:

```
[Actives]  = CALCULATE([EmpCount], FILTER(Employee, ISBLANK(Employee[TermDate])))
[EmpCount] = CALCULATE(COUNT([EmplID]),
               FILTER(ALL('Date'[PeriodNumber]),
                      'Date'[PeriodNumber] = MAX('Date'[PeriodNumber])))
```

`MAX('Date'[PeriodNumber])` is 201612 unless something restricts the Date table.
Our SYMMETRIC index lets the outer `FILTER(Employee, ...)` propagate MANY -> ONE
into Date, which drops the max to 201412 -- the last period Employee actually
covers -- and the count then comes out at Desktop's 32,401. Make propagation
directional and that restriction disappears, the max stays 201612, Employee has
nothing there, and the whole family blanks.

So reverse propagation is not incidental here; it is load-bearing for this
measure. **The open question is how Desktop gets 32,401 without it**, since
Employee is the many side and a single-direction relationship should not carry
that filter to Date. Answer that before re-attempting the direction rule --
possibilities worth testing against Desktop: that the `FILTER(Employee, ...)`
table argument restricts Date some other way, that `MAX` over a date column has
its own context rule here, or that the measure depends on a Desktop behaviour
this engine models differently. Do not assume, as this note previously did, that
one of our two behaviours must simply be wrong.

**ANSWERED (Desktop, MS_Employee_Hiring). The rule is TABLE EXPANSION, and both
of the "contradictory" anchors are correct.**

```
MAX('Date'[PeriodNumber])                                        201612
CALCULATE(MAX('Date'[PeriodNumber]),
          FILTER(Employee, ISBLANK(Employee[TermDate])))         201412   <-- restricts Date
CALCULATE(MAX('Date'[PeriodNumber]), Employee[FP] = "FT")        201612   <-- does NOT
[Actives]                                                         32401
MAX(Employee[date])                                          2014-12-01
```

A filter applied to a TABLE on the many side expands to that table's EXPANDED
TABLE, which includes the one-side dimensions it points at -- so
`FILTER(Employee, ...)` legitimately restricts `Date`, and `MAX(PeriodNumber)`
drops to 201412 where Employee's data actually ends. A filter on a COLUMN does
not expand: `Employee[FP] = "FT"` leaves Date at 201612. Desktop returns both,
in the same model, in the same query.

That is the narrower formulation the direction rule needed, and it reconciles
the two anchors that looked incompatible:

| shape | reaches the ONE side? | anchor |
|---|---|---|
| `CALCULATE(..., FILTER(Employee, ...))` -- table filter | YES (expansion) | `[Actives]` = 32,401 |
| `CALCULATE(..., DimStore[StoreType]="Catalog")` -- column filter | NO | Agents `SELECTEDVALUE` = BLANK |

So the fix is NOT "never propagate many -> one". It is: **a COLUMN filter does
not propagate many -> one; a TABLE filter argument does, because it filters the
expanded table.**

**IMPLEMENTED (commit 0e4c352), and both anchors hold at once for the first
time.** `_rel_dir` carries the one->many (+ bidirectional) edges and is the
default lookup; keys registered by the multi-column row-set branch of CALCULATE
go into `_expanded_keys` and only those may take the reverse direction.
Verified: Agents_Performance **408/408** (the last open cluster), MS_Employee_
Hiring 124/124, MS_AI_Sample 44/44, Ecommerce_Conversion 132/132, `[Actives]` =
32,401, `[Rank Filtering Employyees MTD]` = 0.
`TestPropagationFollowsTableExpansion` discriminates all three historical
states, so neither wrong formulation can return unnoticed.

The probe that produced this, for re-running after a change --
`verify_live.ps1 -File test_corpus/MS_Employee_Hiring.pbix`:

```
EVALUATE ROW("max_period_plain", MAX('Date'[PeriodNumber]))
EVALUATE ROW("max_period_under_employee_filter",
  CALCULATE(MAX('Date'[PeriodNumber]),
            FILTER(Employee, ISBLANK(Employee[TermDate]))))
EVALUATE ROW("empcount_plain", [EmpCount])
EVALUATE ROW("actives", [Actives])
EVALUATE ROW("employee_rows_in_201612",
  CALCULATE(COUNTROWS(Employee), 'Date'[PeriodNumber] = 201612))
```

If `max_period_under_employee_filter` is **201412**, Desktop DOES carry that
filter many -> one into Date and our symmetric index is right for this shape --
the direction rule then needs a narrower formulation than "never many -> one".
If it is **201612**, Desktop reaches 32,401 some other way and `[EmpCount]`
itself is what to model next. Either answer closes the question; guessing does
not. (Deferred only because the corpus sweep had the machine: 3.4 GB free of
31.9 GB, and Desktop wants several for this file.)

Two anchors, both required, and neither is optional:
`[Actives]` = 32,401 (MS_Employee_Hiring) and
`CALCULATE(SELECTEDVALUE(DimEmployee[EmployeeKey]), DimStore[StoreType]="Catalog")`
= BLANK (Agents_Performance).

- **`FORMAT` date pictures were .NET-cased**, so `mmmm` rendered `0000` instead
  of `July` and every `mm/dd/yyyy` came out `00/19/2021`. Commit 72c0afc. Only
  ONE corpus measure uses a real date picture (`MS_Covid_Tracking[Updated]`),
  which is why the blast radius was small -- a scan of every FORMAT call in all
  25 files confirmed it, and `Date Range Previous Period` renders its dates by
  `&` concatenation, not FORMAT, so it is a different path.

**Re-verified after the fixes, with the sweep's own rules:**

| file | filter-context cells |
|---|---|
| MS_AI_Sample | 44/44 |
| Ecommerce_Conversion | 132/132 |
| MS_Competitive_Marketing | 88/88 |
| MS_Covid_Tracking | 22/22 |
| Agents_Performance | 404/408 (the 4 below) |

**STILL OPEN.**

- **Agents_Performance `Rank Filtering *` + `Employee Name` under
  `StoreType=Catalog` — ROOT CAUSE FOUND, and it is not the RANKX chain.**
  We propagate a filter from the MANY side of a relationship to the ONE side.
  Desktop does not, and both directions are now pinned:

  ```
  relationship: DimStore[EmployeeKey] -> DimEmployee[EmployeeKey]   (many -> one)

  COUNTROWS(DimEmployee)                                    Desktop 293
  CALCULATE(COUNTROWS(DimEmployee), DimStore[StoreType]="Catalog")
                                                            Desktop 293  UNCHANGED
  CALCULATE(SELECTEDVALUE(DimEmployee[EmployeeKey]), same)   Desktop BLANK, ours 213
  CALCULATE(COUNTROWS(DimStore), DimEmployee[EmployeeKey]=213)
                                                            Desktop 1    one->many DOES flow
  ```

  The measure is `SWITCH(TRUE(), SELECTEDVALUE(DimEmployee[EmployeeKey]) IN
  _Rank_Asc, 1, ... , 0)`. With `SELECTEDVALUE` wrongly resolving to 213 instead
  of BLANK, the first branch matches and the answer is 1 where Desktop says 0.
  Desktop's `RANKX(ALL(DimEmployee[EmployeeKey]), [MTD Total Sales], , DESC)`
  under Catalog is 2 -- not `N+1`/`N+2` -- so its third branch is FALSE too.

  Two things this rules OUT, both of which were previously suspected:
  `[MTD Total Sales] @ Catalog` is 1783540.7792 in Desktop and 1783540.7792
  here, and the non-blank-MTD employee count is 1 in both. The TOPN is not
  "legitimately empty" as the older note in `TestInMachinery` guessed -- the
  filter context feeding SELECTEDVALUE is simply wrong.

  A fix has to make propagation directional (one -> many only, unless
  `CrossFilteringBehavior` is bidirectional) and is broad enough to need the
  whole corpus as its check: reverse propagation may currently be load-bearing
  for measures that match today.
- ~~MS_Employee_Hiring `AVG Tenure Days` / `AVG Tenure Months`~~ -- CLOSED,
  both against the captured Desktop values rather than a remembered range.
  `AVG Tenure Days @ Qtr=2` is 2952.93278336456, Desktop's value to every digit
  (it was None). `AVG Tenure Months` per quarter is 99.5 / 97.4 / 94.1 / 91.3
  against Desktop's 99.5 / 97.4 / 94.1 / 91.3 (it was -1).
- ~~MS_Competitive_Marketing `% Units Market Share SPLY` / `@Indicator05`~~ --
  CLOSED. Both were SPLY-family and the DATEADD run fix resolved them; the file
  is 88/88 under filter context.
- **`Employee Name @ StoreType=Catalog` is a REAL defect, not the capture
  artefact it was filed as.** Desktop's own `LEN` of that cell is 1 -- a single
  space, the same "empty" marker it returns for `StoreType=Store` -- and we
  return `'Jan Dryml'`, 9 characters. The measure is a RANKX/`IN` chain over
  `'Top-Bottom-N'[Top-Bottom-N Value]`, a PARAMETER SCALAR, so it belongs with
  the `Rank Filtering *` cluster above and with ledger item L1b, not with the
  truncated captures. Checking Desktop's LEN is what distinguished them.
- **`Date Range Previous Period @ QuarterName=Q1` now MATCHES** and is closed.
  It was a truncated capture; comparing Desktop's own `LEN` gives 13 against our
  `' - 12/31/2024'`, also 13. The v8 sweep saw LEN 20 because the period-outside
  -the-calendar defect was still feeding it a non-blank date -- the empty-period
  fix closed this one as a side effect.

## Known limits (deliberate, documented)

- `COVID[Daily cases]` — a per-row CALCULATE/FILTER over 1.74M rows evaluates at
  1.3 rows/sec (≈15 days). The calculated-column path now enforces the same
  wall-clock budget measures have and refuses with a row count instead of
  hanging; it previously ran 2+ hours at 4 GB with no output.
- MS_Perf_Analyzer's `PATH` / `PATHITEM` / `RANKX` calculated columns are
  refused deliberately: they need a table scan this engine does not implement.
  Refused, never silently wrong.
- Measures whose definition reaches a `RAND()` through any chain of references
  cannot be compared by value at all and are excluded by name, never counted as
  matches.
