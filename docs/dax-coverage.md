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
| implemented and verified | **435** ([supported-dax.md](supported-dax.md)) |
| classified out of authorable scope (Desktop's own error as evidence) | **32** |
| open | **0** |

**Every function in the live engine's DAX surface is accounted for**: it
either matches Power BI Desktop through the per-function conformance
harness, or Desktop/the engine itself refuses it in every authorable shape,
with the refusal recorded. The exact tallies are derivable from the
committed artifacts: 260 functions carry value goldens in
`tests/conformance/golden.json`, 175 core functions predate the harness and
are pinned by full-corpus parity, and 32 carry only Desktop-error records.

Implemented-and-verified means one of two proof levels: **260** functions have
per-function goldens captured from the live Desktop engine and replayed by
`tests/test_dax_conformance.py` (the ratchet: no "unsupported" escape hatch,
1e-9 relative tolerance), and **175** core functions predate the harness and
are pinned by full-corpus parity (every comparable cell of the 24-report corpus,
v0.9.63). An unimplemented function returns `None` with status
`"unsupported"`; it is never guessed.

`CEILING.MATH` and `FLOOR.MATH` appear in some DAX documentation but are
**not in the engine's MDSCHEMA_FUNCTIONS inventory at all** — Desktop cannot
resolve the names in any context, because they are not engine functions.
They count against nothing.

## Classified out of authorable scope (32)

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
- **Calendar-feature-gated (8)** — the week-grain time-intelligence family:
  `CLOSINGBALANCEWEEK`, `DATESWTD`, `ENDOFWEEK`, `NEXTWEEK`,
  `OPENINGBALANCEWEEK`, `PREVIOUSWEEK`, `STARTOFWEEK`, `TOTALWTD`.
  Two-layer evidence, both from the live engine:
  1. Each function refuses every date-column shape with *"parameter N must
     be a calendar reference"* (recorded in `golden.json`).
  2. A calendar reference requires a calendar object (TMSCHEMA
     `Calendar`/`TimeUnitColumnAssociation`/`CalendarColumnReference`), and
     the engine **refuses to accept one**: creating a complete, well-formed
     custom calendar via TOM/XMLA (compatibility level raised to the
     required 1701, three time-unit column groups with bound primary
     columns) is rejected by msmdsrv with *"The model contains a custom
     calendar. This feature is not supported."*
  No model this project can author — by builder, metadata edit, or live
  XMLA — can carry the object these functions need, so they are not
  authorable against this engine build. The probes stay in the harness: a
  future Desktop build that enables calendars will flip them to value
  goldens on the next capture, and the family graduates to implementable.

## PATH and the builder hierarchy gap

`PATH`/`PATHITEMREVERSE` are implemented and conformance-verified: the
fixture's parent-child table is a **calculated table**, which Desktop
recomputes and fully processes at open, making its hierarchy support
structures PATH-queryable. Desktop still refuses PATH on the builder's
**import** tables (*"Cannot query internal support structures for column
... because they are not processed"*, for every non-join column even in
tables that participate in relationships — while `VALUES(...)`,
aggregations, and relationships all work). The investigation closed with
a definitive comparison: a Desktop-saved file whose calculated table had
just been processed live persists hierarchy structures **byte-identical
to the builder's output** — payloads, `.idfmeta`, versions, file names.
There is no at-rest "processed" byte for the builder to write. Import
tables become PATH-queryable only through a real engine refresh (the
corpus files that answer PATH at rest all carry high refresh
generations), which Desktop cannot run on a source-less model.
Resolution: **documented limitation with a supported workaround** —
author parent-child tables that need `PATH` as calculated tables, which
Desktop recomputes and fully processes at open; that is exactly what the
conformance fixture does. See README → Known Limitations. pbix-mcp's own
DAX engine evaluates `PATH` on any table regardless.

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
