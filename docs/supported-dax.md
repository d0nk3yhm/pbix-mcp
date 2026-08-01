# Supported DAX Functions

All 435 of the DAX functions Power BI Desktop can evaluate in a query are implemented and verified — 100% of the evaluable surface (out of the engine's 467-function catalog; the remaining 32 are proven not query-authorable by the engine's own refusals, so the whole surface is accounted for — [dax-coverage.md](dax-coverage.md)). Semantics are verified two ways: a per-function conformance harness replays Desktop-captured goldens (359 value probes, 1e-9 relative tolerance), and the 24-report corpus matches Desktop on every comparable cell — 432 grand totals, 1,705 filter-context cells, 397 calculated columns (v0.9.63). Functions outside this list return `None` with status `"unsupported"` rather than a guess; expressions using unlisted shapes may still be refused.

## Aggregation (13)
`SUM`, `AVERAGE`, `COUNT`, `COUNTA`, `COUNTROWS`, `MIN`, `MAX`, `DISTINCTCOUNT`, `DISTINCTCOUNTNOBLANK`, `PRODUCT`, `MEDIAN`, `MEDIANX`, `COUNTBLANK`

`DISTINCTCOUNT` counts BLANK as one distinct value, as Desktop does; use
`DISTINCTCOUNTNOBLANK` to exclude it.

## Iterators (11)
`SUMX`, `MAXX`, `MINX`, `AVERAGEX`, `COUNTX`, `COUNTAX`, `CONCATENATEX`, `RANKX`, `FILTER`, `GENERATE`, `GENERATEALL`

## Table (13)
`TOPN`, `ADDCOLUMNS`, `SUMMARIZE`, `SUMMARIZECOLUMNS`, `SELECTCOLUMNS`, `DISTINCT`, `UNION`, `EXCEPT`, `INTERSECT`, `CROSSJOIN`, `DATATABLE`, `ROW`, `TREATAS`

## Time Intelligence (35)
`CALCULATE`, `CALCULATETABLE`, `DATEADD`, `SAMEPERIODLASTYEAR`, `TOTALYTD`, `TOTALMTD`, `TOTALQTD`, `PREVIOUSMONTH`, `PREVIOUSQUARTER`, `PREVIOUSYEAR`, `NEXTMONTH`, `NEXTQUARTER`, `NEXTYEAR`, `PARALLELPERIOD`, `DATESYTD`, `DATESMTD`, `DATESQTD`, `STARTOFMONTH`, `STARTOFQUARTER`, `STARTOFYEAR`, `ENDOFMONTH`, `ENDOFQUARTER`, `ENDOFYEAR`, `FIRSTDATE`, `LASTDATE`, `FIRSTNONBLANK`, `LASTNONBLANK`, `DATESBETWEEN`, `DATESINPERIOD`, `CALENDAR`, `CALENDARAUTO`, `OPENINGBALANCEMONTH`, `OPENINGBALANCEQUARTER`, `OPENINGBALANCEYEAR`, `CLOSINGBALANCEMONTH`, `CLOSINGBALANCEQUARTER`, `CLOSINGBALANCEYEAR`

## Filter (12)
`REMOVEFILTERS`, `ALL`, `ALLEXCEPT`, `ALLSELECTED`, `KEEPFILTERS`, `VALUES`, `SELECTEDVALUE`, `HASONEVALUE`, `HASONEFILTER`, `ISFILTERED`, `ISINSCOPE`, `ISCROSSFILTERED`

`ISINSCOPE` answers the same question as `ISFILTERED` here: without a visual's
grouping there is no scope beyond the filter context the tool was handed.

## Logic (12)
`IF`, `SWITCH`, `AND`, `OR`, `NOT`, `ISBLANK`, `IFERROR`, `COALESCE`, `CONTAINS`, `TRUE`, `FALSE`, `ERROR`

## Math (28)
`DIVIDE`, `ABS`, `ROUND`, `ROUNDUP`, `ROUNDDOWN`, `MROUND`, `INT`, `CEILING`, `FLOOR`, `MOD`, `POWER`, `SQRT`, `LOG`, `LOG10`, `LN`, `EXP`, `SIGN`, `TRUNC`, `EVEN`, `ODD`, `FACT`, `GCD`, `LCM`, `PI`, `RAND`, `RANDBETWEEN`, `CURRENCY`, `FIXED`

## Text (22)
`CONCATENATE`, `FORMAT`, `LEFT`, `RIGHT`, `MID`, `LEN`, `UPPER`, `LOWER`, `PROPER`, `TRIM`, `SUBSTITUTE`, `REPLACE`, `REPT`, `SEARCH`, `FIND`, `CONTAINSSTRING`, `CONTAINSSTRINGEXACT`, `EXACT`, `UNICHAR`, `UNICODE`, `VALUE`, `COMBINEVALUES`

## Relationship (9)
`RELATED`, `RELATEDTABLE`, `USERELATIONSHIP`, `CROSSFILTER`, `EARLIER`, `EARLIEST`, `PATHITEM`, `PATHLENGTH`, `PATHCONTAINS`

## Date/Time (14)
`DATE`, `DATEDIFF`, `EDATE`, `EOMONTH`, `YEAR`, `QUARTER`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `WEEKDAY`, `WEEKNUM`, `NOW`, `TODAY`, `UTCNOW`

## Information (10)
`LOOKUPVALUE`, `ISNUMBER`, `ISTEXT`, `ISNONTEXT`, `ISLOGICAL`, `ISERROR`, `USERNAME`, `USERPRINCIPALNAME`, `BLANK`, `GENERATESERIES`


## Trigonometry & advanced math (29) — conformance batch 1
`ACOS`, `ACOSH`, `ACOT`, `ACOTH`, `ASIN`, `ASINH`, `ATAN`, `ATANH`, `COS`, `COSH`, `COT`, `COTH`, `SIN`, `SINH`, `TAN`, `TANH`, `DEGREES`, `RADIANS`, `SQRTPI`, `COMBIN`, `COMBINA`, `PERMUT`, `QUOTIENT`, `BITAND`, `BITOR`, `BITXOR`, `BITLSHIFT`, `BITRSHIFT`, `ISO.CEILING`

## Statistical distributions (24) — conformance batch 1
`NORM.DIST`, `NORM.INV`, `NORM.S.DIST`, `NORM.S.INV`, `EXPON.DIST`, `POISSON.DIST`, `BETA.DIST`, `BETA.INV`, `CHISQ.DIST`, `CHISQ.DIST.RT`, `CHISQ.INV`, `CHISQ.INV.RT`, `T.DIST`, `T.DIST.RT`, `T.DIST.2T`, `T.INV`, `T.INV.2T`, `CONFIDENCE.NORM`, `CONFIDENCE.T`, `PERCENTILE.INC`, `PERCENTILE.EXC`, `PERCENTILEX.INC`, `PERCENTILEX.EXC`, `RANK.EQ`

## Batch-1 additions elsewhere
`GEOMEAN`, `GEOMEANX`, `STDEVX.S`, `STDEVX.P`, `VARX.S`, `VARX.P`, `AVERAGEA`, `DATEVALUE`, `TIMEVALUE`

Every batch-1 function is pinned by Desktop-captured golden values in
`tests/conformance/golden.json` (114 probes, 1e-9 relative). `CEILING.MATH`
and `FLOOR.MATH` are listed by the engine's DMV but Desktop itself cannot
resolve them in a query — empirically out of authorable scope, not missing.


## Batch-2 additions (18)
`NETWORKDAYS`, `ISDATETIME`, `CONTAINSROW`, `ALLNOBLANKROW`, `FILTERS`, `TOPNSKIP`, `NATURALINNERJOIN`, `NATURALLEFTOUTERJOIN`, `GROUPBY`, `CURRENTGROUP`, `ISONORAFTER`, `ALLCROSSFILTERED`, `SUBSTITUTEWITHINDEX`, `DETAILROWS`, `NEXTDAY`, `PREVIOUSDAY`, `IGNORE`, `ROLLUPADDISSUBTOTAL`

The week-grain time-intelligence family (`STARTOFWEEK`, `ENDOFWEEK`, `NEXTWEEK`,
`PREVIOUSWEEK`, `DATESWTD`, `OPENINGBALANCEWEEK`, `CLOSINGBALANCEWEEK`) is
classified **needs-model-feature**: Desktop requires a model *calendar
reference* as their first argument ("parameter 1 must be a calendar
reference"), an object this engine's model layer does not yet carry.
`ROWNUMBER`/`ORDERBY` (the window family) is deliberately deferred to its own
batch rather than shipped shallow.


## Financial (51) — conformance batch 3
`PMT`, `FV`, `PV`, `NPER`, `RATE`, `IPMT`, `PPMT`, `CUMIPMT`, `CUMPRINC`, `ISPMT`, `SLN`, `SYD`, `DDB`, `DB`, `VDB`, `AMORDEGRC`, `AMORLINC`, `EFFECT`, `NOMINAL`, `RRI`, `PDURATION`, `DOLLARDE`, `DOLLARFR`, `XNPV`, `XIRR`, `ACCRINT`, `ACCRINTM`, `COUPDAYBS`, `COUPDAYS`, `COUPDAYSNC`, `COUPNCD`, `COUPNUM`, `COUPPCD`, `DISC`, `DURATION`, `MDURATION`, `INTRATE`, `PRICE`, `PRICEDISC`, `PRICEMAT`, `RECEIVED`, `TBILLEQ`, `TBILLPRICE`, `TBILLYIELD`, `YIELD`, `YIELDDISC`, `YIELDMAT`, `ODDFPRICE`, `ODDFYIELD`, `ODDLPRICE`, `ODDLYIELD`

Day-count bases 0–4 (30/360 US, actual/actual, actual/360, actual/365,
30E/360) with a coupon-schedule kernel; `RATE`/`YIELD`/`XIRR`/`ODDFYIELD` via
Newton iteration. All 51 pinned by Desktop goldens at 1e-9.


## Batch-4 additions (40)
`STDEV.S`, `STDEV.P`, `VAR.S`, `VAR.P`, `MAXA`, `MINA`, `PRODUCTX`, `ISEVEN`, `ISODD`, `ISBOOLEAN`, `ISSTRING`, `ISNUMERIC`, `ISINTEGER`, `ISINT64`, `ISDECIMAL`, `ISDOUBLE`, `ISCURRENCY`, `ISEMPTY`, `ISAFTER`, `FIRSTNONBLANKVALUE`, `LASTNONBLANKVALUE`, `CONVERT`, `TIME`, `YEARFRAC`, `IF.EAGER`, `EVALUATEANDLOG`, `NAMEOF`, `USERCULTURE`, `USEROBJECTID`, `CUSTOMDATA`, `SAMPLE`, `TOCSV`, `TOJSON`, `LINEST`, `LINESTX`, `ADDMISSINGITEMS`, `TABLEOF`, `SAMPLECARTESIANPOINTSBYCOVER`, `UTCTODAY`, plus `ROLLUP`/`ROLLUPGROUP`/`ISSUBTOTAL` inside `SUMMARIZE`

Notes pinned by Desktop: `LINEST` pairs columns row-by-row with BLANK
participating as **zero**; `CURRENCY()` carries a Fixed Decimal type marker so
`ISDECIMAL`/`ISCURRENCY` answer as Desktop does; and `COUNTROWS` over a
`ROLLUP` summarize is **non-compositional in Desktop itself** (EVALUATE prints
4 rows, COUNTROWS of the same expression says 2) — the conformance probe uses
the `SUMX` form Desktop answers consistently.

Classified out of authorable scope this batch, each from Desktop's own error:
visual-calculation-only `LOOKUP`, `COLLAPSE`, `EXPAND`, `ISATLEVEL` (and by
the same family `COLLAPSEALL`/`EXPANDALL`); calculation-group-context-only
`SELECTEDMEASURE`, `ISSELECTEDMEASURE`, `SELECTEDMEASURENAME`,
`SELECTEDMEASUREFORMATSTRING`; unresolvable `EXTERNALMEASURE`;
import-storage-unsupported `APPROXIMATEDISTINCTCOUNT`; calendar-reference
`TOTALWTD`; auto-date-table-dependent `COLUMNSTATISTICS` (reversed in
batch 5 — see below).

## Batch-5 additions (79)

**Window family (8)**: `ROWNUMBER`, `RANK`, `INDEX`, `OFFSET`, `WINDOW` with
the `ORDERBY`, `PARTITIONBY`, `MATCHBY` marker sub-expressions. The relation
is materialised against the pre-transition context, so a window function
inside `SUMX`/`ADDCOLUMNS` sees every iterated row rather than the single row
the eager row-to-filter transition narrows the context to; the current row is
located by value in the sorted partition (all columns, or the `MATCHBY`
columns). `WINDOW` supports `ABS` (1-based, negative-from-end) and `REL`
(clamped at partition edges) endpoints; `RANK` supports `SKIP` and `DENSE`
ties. All fourteen Desktop goldens — including partitioned row numbers and
relative windows — match by hand-checkable values.

**INFO.\* model-metadata family (66)**: every `INFO.*` function Desktop's
engine will evaluate in a query. They serve the logical model the engine
executes plus the Vertipaq physical-structure counts it implies; Desktop's
own counts on the fixture pin the formulas (`INFO.FUNCTIONS()` = 467,
22 storage tables, 52 column storages). See the semantics note in
[dax-coverage.md](dax-coverage.md). The three functions Desktop refuses as
edition/compat-level-unavailable (`INFO.DATACOVERAGEDEFINITIONS`,
`INFO.EXCLUDEDARTIFACTS`, `INFO.USERDEFINEDFUNCTIONS`) are classified out
with that error as evidence.

**Misc (5)**: `NONVISUAL` (valid only over grouped columns —
Desktop-verified shape; a no-op marker in plain queries, applied as the
filter it wraps), `ROLLUPISSUBTOTAL` (the working argument order is
`ROLLUPISSUBTOTAL(groupCol, [isSubtotalCol])` inside `ADDMISSINGITEMS`),
`SAMPLEAXISWITHLOCALMINMAX` (5-argument form), `COLUMNSTATISTICS`
(batch-4 classification **reversed**: its 20 fixture rows are the 15 user
columns plus one internal RowNumber row per table — `INFO.TABLES()` = 5
proved no auto date/time tables existed in the capture), and the internal
per-table RowNumber column surfaced consistently across
`COLUMNSTATISTICS`/`INFO.COLUMNS`.

Classified out this batch, each from Desktop's own error message:
visual-calculation-only `MOVINGAVERAGE`, `RUNNINGSUM`, `FIRST`, `LAST`,
`NEXT`, `PREVIOUS`, `RANGE`, `COLLAPSEALL`, `EXPANDALL` (*"can only be used
in the expression of a visual calculation"*); engine-internal
`NATURALJOINUSAGE` (*"can only be used as a value filter for
SUMMARIZECOLUMNS"*, yet refused there too) and `LOOKUPWITHTOTALS` (rejects
every authorable column-reference shape).

## Batch-6 additions (2)

`PATH` and `PATHITEMREVERSE`, completing the PATH family. The fixture's
parent-child table became a **calculated table** so Desktop processes its
hierarchy support structures at open (import tables from the builder are
not PATH-queryable in Desktop — a builder issue tracked in
[dax-coverage.md](dax-coverage.md)). `PATH` walks the id→parent mapping in
the pre-transition context and prints integers Desktop-style (`1|2|4`);
the root's parent must be a true BLANK — a `BLANK()` inside a `DATATABLE`
row literal arrives as 0, which PATH rejects, hence the
`ADDCOLUMNS`+`IF` shape in the fixture definition.

## Known Limitations

### `FORMAT` date pictures

Pictures are VBA-style and **case-insensitive**, matching Power BI:

| picture | `DATE(2021,7,19)` |
|---|---|
| `mmmm` / `mmm` / `mm` / `m` | `July` / `Jul` / `07` / `7` |
| `dddd` / `ddd` / `dd` / `d` | `Monday` / `Mon` / `19` / `19` |
| `yyyy` / `yy` | `2021` / `21` |
| `mm/dd/yyyy` | `07/19/2021` |
| `dddd, mmmm dd, yyyy` | `Monday, July 19, 2021` |
| `Long Date` / `Short Date` | `Monday, July 19, 2021` / `7/19/2021` |

`m` is a **month** except when it follows an hour token, where it is minutes —
`FORMAT(<noon>, "mm hh:mm")` is `07 12:00`. Use `nn` when you want minutes
unambiguously. Single-letter tokens do not zero-pad: `m/d/yyyy` on 2021-03-05
is `3/5/2021`. The .NET spellings (`MMMM`, `DD`, `YYYY`) also work.

### Other limitations

- **Date-table detection** uses heuristics (looks for columns named "Date" or "Calendar")
- **Unsupported functions** return `None` with status `"unsupported"` and are tracked in `unsupported_functions`
- **Circular references** raise `DAXEvaluationError` (caught by graceful degradation → returns `None`)
