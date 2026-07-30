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

## Still open (found by the filter-context sweep, NOT yet fixed)

Every one of these is a measure where Desktop and this engine disagree under a
FILTER CONTEXT. They are recorded with their root-cause class so the next
session starts from evidence rather than from scratch.

- **Ecommerce_Conversion `*_PMTD/PQTD` and `*_%Delta` under `QuarterName=Q1`.**
  Desktop BLANK, we return a value (and the `%Delta` measures built on them come
  out -1.0). The chain is
  `_FirstDatePrevtQTD = CALCULATE(MAX(dimDate[FirstOfMonth]), DATEADD(dimDate[Date],-1,QUARTER))`
  which is correctly BLANK in Q1 (the previous quarter is outside the calendar),
  then `_LastDatePrevQ = DATE(YEAR(_Max_Date), MONTH(_FirstDatePrevtQTD), DAY(_Max_Date))`.
  Start by checking `MONTH(BLANK())` and `DATE()` with a blank part against
  Desktop -- a blank month probably has to poison the whole DATE(), and the
  DATESBETWEEN that follows.
- **MS_AI_Sample `CSAT Impact` / `- Agent` / `- Products` / `- Subject` per
  Manager.** Desktop 0, we return +-0.03. All four share one shape:
  `VAR AllAvg = CALCULATE(AVERAGE(Cases[CSAT]), ALL(Cases))`
  `VAR AllAvgExcept = CALCULATE(AVERAGE(Cases[CSAT]),`
  `    FILTER(ALL(Cases), Cases[X] <> SELECTEDVALUE(Cases[X])))`
  `RETURN 1 - (AllAvgExcept / AllAvg)`, X = Topic/Agent/ProductSeq/Subject.
  Desktop's exact 0 means AllAvgExcept == AllAvg, i.e. the predicate drops NO
  row. Two things were checked and one hypothesis was KILLED:
  - NOT the FILTER row-substitution guard (`_AGG_CALL_RE`, engine.py:4688).
    `SELECTEDVALUE` is absent from that regex, so the obvious theory was that
    the iterated row's value gets substituted. A 4-row fixture refutes it: our
    `SELECTEDVALUE` inside `FILTER(ALL(T), ...)` returns BLANK either way.
  - It exposed a DIFFERENT bug instead. CALCULATE's filter arguments are
    evaluated in the OUTER context, so with `Topic` pinned outside,
    `SELECTEDVALUE(Cases[Topic])` must be "A" and the measure non-zero. We
    return 0.0 there too -- our `ALL(Cases)` clears the column BEFORE the
    SELECTEDVALUE in the predicate is evaluated. Fixture, hand-checkable:
    4 rows [A,5],[B,3],[A,4],[B,2]; pinned Topic=A -> 1 - 2.5/3.5 = 0.2857.
  - So the +-0.03 is most likely rows dropped by `Col <> BLANK()` where the
    column HAS blanks. Next step needs a real probe of the four VARs on the
    file. `pbix_evaluate_dax` takes MEASURE NAMES, not expressions -- probe by
    adding temp measures, or build the DAXContext the way `cmp_ctx.py` does.
- **Agents_Performance `Rank Filtering *` under `StoreType=Catalog`.** Desktop 0,
  we return 1.
- **MS_Employee_Hiring `New Hires SPLY` — the quarter filter is DROPPED, not
  mis-computed.** Desktop 11601 @ Qtr=2 and 13840 @ Qtr=3; we return **43120 for
  both**, and 43120 is the measure's GRAND TOTAL. A constant answer across
  filter values is the signature of a filter that never reached the
  evaluation, so look at the SPLY/SAMEPERIODLASTYEAR path dropping the
  incoming context, NOT at the arithmetic. `Bad Hires YoY Var @ Qtr=N`
  (Desktop 0, we return +2466 / -7104) is built on it and should follow.
  This is NOT the earlier budget-timeout cluster in the same file -- a timeout
  yields None, and these are confident numbers.
- **MS_Employee_Hiring `AVG Tenure Days @ Qtr=2`** Desktop 2952.93, we return
  None; same family as the `AVG Tenure Months @ Qtr=N` -1 above.
- **MS_Competitive_Marketing `% Units Market Share SPLY @ MfgisVanArsdel=Yes`**
  Desktop 1, we return 0; and `@Indicator05` Desktop 2, we return 1. Both are
  SPLY-family, so check them against the same root cause as New Hires SPLY.
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
