# Supported DAX Functions

183 functions across 11 categories. The engine is **best-effort** — it produces correct results for common patterns but does not aim for semantic parity with Analysis Services.

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

## Known Limitations

- **Date-table detection** uses heuristics (looks for columns named "Date" or "Calendar")
- **Unsupported functions** return `None` with status `"unsupported"` and are tracked in `unsupported_functions`
- **Circular references** raise `DAXEvaluationError` (caught by graceful degradation → returns `None`)
