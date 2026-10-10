"""Issue #182 (OpenBI's doc 60): a time-intelligence function takes a TABLE
of one date column as its <dates> -- a DATESINPERIOD / FILTER / DATEADD
result, inline or in a variable -- and keeps the column's lineage.

_get_date_column_dates read its argument only as a column reference, so
PREVIOUSMONTH, NEXTMONTH, DATESYTD, FIRSTDATE ... of such a table were empty:
Executive Sales Report's VAR _datetable = DATESINPERIOD(...) RETURN
CALCULATE([Total Sales], PREVIOUSMONTH(_datetable)) was BLANK.

Expected values: Power BI Desktop 2.152 over ADOMD, each measure under
CALCULATETABLE(..., 'Date'[Month] = 3, 'Date'[Year] = 2024) (the Desktop oracle
toolkit's build_b180.py; Sales holds two rows on days 5 and 15 of each month,
Date runs December 2023 - May 2024)."""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [dt.datetime(2023, 12, 1) + dt.timedelta(days=i) for i in range(183)]
TABLES = {
    "Date": {"columns": ["Date", "Year", "Month"], "rows": [[d, d.year, d.month] for d in DAYS]},
    "Sales": {"columns": ["SDate", "Amount"], "rows": [[d, 1] for d in DAYS if d.day in (5, 15) for _ in (0, 1)]},
}
RELS = [{"FromTable": "Sales", "FromColumn": "SDate", "ToTable": "Date", "ToColumn": "Date", "IsActive": 1}]
DESKTOP = [
    ('Total', 'SUM(Sales[Amount])', '4'),
    ('PM', "CALCULATE([Total], PREVIOUSMONTH('Date'[Date]))", '4'),
    ('PMvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN CALCULATE([Total], PREVIOUSMONTH(t))", '4'),
    ('PMinline', "CALCULATE([Total], PREVIOUSMONTH(DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH)))", '4'),
    ('PMvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(PREVIOUSMONTH(t))", '31'),
    ('NMvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN CALCULATE([Total], NEXTMONTH(t))", '4'),
    ('NMvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(NEXTMONTH(t))", '30'),
    ('PQvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(PREVIOUSQUARTER(t))", '31'),
    ('YTDvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN CALCULATE([Total], DATESYTD(t))", '12'),
    ('YTDvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(DATESYTD(t))", '75'),
    ('MTDvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(DATESMTD(t))", '15'),
    ('FDvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN FIRSTDATE(t)", '2024-02-16T00:00:00'),
    ('LDvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN LASTDATE(t)", '2024-03-15T00:00:00'),
    ('SPLYvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(SAMEPERIODLASTYEAR(t))", None),
    ('DAvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(DATEADD(t, -1, MONTH))", '31'),
    ('DAvarFirst', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN FIRSTDATE(DATEADD(t, -1, MONTH))", '2024-01-16T00:00:00'),
    ('EOMvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN ENDOFMONTH(t)", '2024-03-31T00:00:00'),
    ('SOMvar', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN STARTOFMONTH(t)", '2024-02-01T00:00:00'),
    ('PPvarRows', "VAR t = DATESINPERIOD('Date'[Date], MAX(Sales[SDate]), -1, MONTH) RETURN COUNTROWS(PARALLELPERIOD(t, -1, MONTH))", '60'),
    ('PMfilter', "CALCULATE([Total], PREVIOUSMONTH(FILTER(ALL('Date'[Date]), 'Date'[Date] >= DATE(2024,3,10) && 'Date'[Date] <= DATE(2024,3,20))))", '4'),
    ('PMfilterRows', "COUNTROWS(PREVIOUSMONTH(FILTER(ALL('Date'[Date]), 'Date'[Date] >= DATE(2024,3,10) && 'Date'[Date] <= DATE(2024,3,20))))", '29'),
    ('PMdateadd', "COUNTROWS(PREVIOUSMONTH(DATEADD('Date'[Date], -1, MONTH)))", '31'),
    ('FDempty', "VAR t = FILTER(ALL('Date'[Date]), FALSE()) RETURN IF(ISBLANK(FIRSTDATE(t)), 1, 0)", '1'),
    ('PMempty', "VAR t = FILTER(ALL('Date'[Date]), FALSE()) RETURN COUNTROWS(PREVIOUSMONTH(t)) + 0", '0'),
]
MEASURES = {k: dax for k, dax, _w in DESKTOP}


def _answers():
    got = de.evaluate_measures_batch(list(MEASURES), TABLES, dict(MEASURES),
                                     filter_context={"Date.Month": [3], "Date.Year": [2024]},
                                     date_table="Date", date_column="Date", relationships=RELS,
                                     date_tables={"Date": "Date"})
    out = {}
    for k, v in got.items():
        v = de._scalarize(v)
        if isinstance(v, dt.datetime):
            v = v.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(v, float) and v.is_integer():
            v = str(int(v))
        elif v is not None:
            v = str(v)
        out[k] = v
    return out


ANSWERS: dict = {}


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    if not ANSWERS:
        ANSWERS.update(_answers())
    assert ANSWERS[probe] == want
