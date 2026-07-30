# Session progress — 2026-07-28 → 07-30

Working note for picking this back up. Records what was done, what is verified
and how, what is left, and the traps already hit so they are not re-hit.

## Where things stand

**0.9.61 is the corpus-parity release.** Every measure of every corpus file was
compared against the value Power BI Desktop's own engine returns for it, and the
sixteen defects that came out of it are fixed. Details in CHANGELOG.md.

| verification | scale | result |
|---|---|---|
| measures at the grand total | 547 measures, 24 files | 1:1 with Desktop |
| measures UNDER A FILTER CONTEXT | ~1,700 measure x dimension-value cells | see `cmp_ctx.py` |
| calculated columns vs stored VertiPaq values | ~400 columns | 0 mismatches |
| unit tests | 1,280 | pass (`pytest -m "not slow"`) |

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

**STILL OPEN.** Every one is a Desktop disagreement under a filter context, and
every one was measured BEFORE the four fixes above -- re-run the sweep and
re-confirm each before spending time on it.

- **Agents_Performance `Rank Filtering *` under `StoreType=Catalog`.** Desktop 0,
  we return 1. Three measures: `Dynamics`, `Employyees MTD`, `Employyees MTD%`
  (the misspelling is the model's). They wrap IN around a RANKX/TOPN chain --
  see the note in `TestInMachinery` about why IN is deliberately not wired into
  the expression planner.
- **MS_Employee_Hiring `AVG Tenure Days @ Qtr=2`** Desktop 2952.93, we return
  None, and `AVG Tenure Months @ Qtr=N` returned -1 against Desktop's 91-99.5.
  `AVG Tenure Days` is `AVERAGE([TenureDays])` -- a bare column reference, so
  start with home-table resolution rather than the date logic.
- **MS_Competitive_Marketing `% Units Market Share SPLY @ MfgisVanArsdel=Yes`**
  Desktop 1, we return 0; and `@Indicator05` Desktop 2, we return 1. Both are
  SPLY-family; re-measure against the DATEADD fix before investigating.
- **Two capture artefacts, not engine bugs**: `Employee Name @ StoreType=Catalog`
  and `Date Range Previous Period @ QuarterName=Q1` compare via the LEN
  fallback because the captured value was truncated; confirm against Desktop's
  own LEN before treating either as a defect.

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
