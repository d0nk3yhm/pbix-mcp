# Supported DAX Functions

314 functions across 13 categories. Semantics are verified against Power BI Desktop's own engine on a 25-file corpus: every comparable cell — 432 grand totals, 1,705 filter-context cells, 397 calculated columns — matches Desktop exactly (v0.9.63). Functions outside this list return `None` with status `"unsupported"` rather than a guess; expressions using unlisted shapes may still be refused. Full parity with the entire DAX surface is the roadmap, not yet a claim.

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
