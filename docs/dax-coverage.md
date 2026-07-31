# DAX Surface Coverage

The authoritative function inventory is taken from the **live Power BI Desktop
engine itself** — `SELECT [FUNCTION_NAME],[ORIGIN] FROM
$SYSTEM.MDSCHEMA_FUNCTIONS` against the workspace `msmdsrv`, keeping ORIGIN 3
and 4 (the DAX surface; ORIGIN 1 is MDX). That is the exact list for the exact
build the corpus is verified against, immune to documentation lag. Desktop
independently confirms the count: `COUNTROWS(INFO.FUNCTIONS())` = **467** on
the conformance fixture.

| | count |
|---|---|
| DAX functions in the engine (March 2026 build) | **467** |
| implemented and verified | **433** ([supported-dax.md](supported-dax.md)) |
| classified out of authorable scope (Desktop's own error as evidence) | **26** |
| open — week-grain family, needs calendar-object investigation | **8** |
| open — PATH pair, blocked on a builder fix | **2** |

Implemented-and-verified means one of two proof levels: **257** functions have
per-function goldens captured from the live Desktop engine and replayed by
`tests/test_dax_conformance.py` (the ratchet: no "unsupported" escape hatch,
1e-9 relative tolerance), and **176** core functions predate the harness and
are pinned by full-corpus parity (every comparable cell of the 25-file corpus,
v0.9.63). An unimplemented function returns `None` with status
`"unsupported"`; it is never guessed.

## Classified out of authorable scope (26)

Each of these was probed against live Desktop and refused with an explicit
error; the error text is recorded in `tests/conformance/golden.json`. They are
not countable against query parity because Desktop itself cannot evaluate them
in a query/measure context.

- **Visual-calculation-only (13)** — Desktop: *"can only be used in the
  expression of a visual calculation"*: `COLLAPSE`, `COLLAPSEALL`, `EXPAND`,
  `EXPANDALL`, `FIRST`, `LAST`, `NEXT`, `PREVIOUS`, `RANGE`, `MOVINGAVERAGE`,
  `RUNNINGSUM`, `ISATLEVEL`, `LOOKUP`
- **Calculation-group context only (4)** — evaluable only inside a
  calculation item: `SELECTEDMEASURE`, `SELECTEDMEASURENAME`,
  `SELECTEDMEASUREFORMATSTRING`, `ISSELECTEDMEASURE`
- **Engine-internal (3)** — no user-authorable argument shape exists:
  `NATURALJOINUSAGE` (*"can only be used as a value filter for
  SUMMARIZECOLUMNS"*, yet refused in that position too),
  `LOOKUPWITHTOTALS` (rejects every column-reference shape),
  `EXTERNALMEASURE` (composite-model remote measures only)
- **Edition/compatibility-blocked (3)** — Desktop: *"unavailable in the
  current edition of the server or database compatibility level"*:
  `INFO.DATACOVERAGEDEFINITIONS`, `INFO.EXCLUDEDARTIFACTS`,
  `INFO.USERDEFINEDFUNCTIONS`
- **Storage-mode-limited (1)** — `APPROXIMATEDISTINCTCOUNT` (DirectQuery
  sources only; refused against an import model)
- **Name not resolvable (2)** — Desktop cannot resolve the name in any
  context we could author: `CEILING.MATH`, `FLOOR.MATH`

## Open items (10)

- **Week-grain time intelligence (8)**: `CLOSINGBALANCEWEEK`, `DATESWTD`,
  `ENDOFWEEK`, `NEXTWEEK`, `OPENINGBALANCEWEEK`, `PREVIOUSWEEK`,
  `STARTOFWEEK`, `TOTALWTD`. Desktop refuses them without a calendar
  reference (2025 calendar-object model feature). Under investigation:
  whether the fixture can carry a calendar object, so these can be captured
  and implemented — otherwise they finalize as needs-model-feature.
- **PATH family (2)**: `PATH`, `PATHITEMREVERSE`. Desktop refuses them on
  builder-produced models: *"Cannot query internal support structures for
  column ... because they are not processed"* — for **every** column that is
  not a relationship join column (`PATH(D[M], D[Q])` fails even though D
  participates in a relationship; `COUNTROWS(VALUES(PC[id]))` works fine).
  This is a builder gap (per-column hierarchy support structures are only
  processed for join columns), tracked as a correctness issue in its own
  right; once fixed, PATH goldens capture normally. `PATHLENGTH`,
  `PATHITEM`, `PATHCONTAINS` are already implemented and verified against
  literal path strings.

## INFO.* semantics note

The 66 implemented `INFO.*` functions serve the **logical model the engine
executes** (tables, columns, measures, relationships, dependencies) plus the
Vertipaq physical-structure counts implied by it (one hierarchy table of two
structure columns per user column, one index table per relationship, one
internal RowNumber column per table) — formulas pinned by Desktop's own counts
on the conformance fixture (22 storage tables, 52 column storages).
Feature families the context does not model (calculation groups, KPIs, roles,
perspectives, translations, …) answer honestly empty, which is also what
Desktop answers on the fixture. Five functions whose raw counts are
msmdsrv-session internals (`INFO.ANNOTATIONS`, `INFO.PROPERTIES`,
`INFO.STORAGEFILES`, `INFO.STORAGEFOLDERS`,
`INFO.STORAGETABLECOLUMNSEGMENTS`) are pinned by shape
(`COUNTROWS(...) > 0`), the same precedent as `USERCULTURE`.
