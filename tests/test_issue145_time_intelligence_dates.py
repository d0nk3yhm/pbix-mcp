"""Issue #145: the one-date tables of time intelligence are dates as values.

FIRSTDATE, LASTDATE, STARTOF... / ENDOF..., LASTNONBLANK and the other
functions that return dates gave each date as 'YYYY-MM-DD' text. That worked
as a CALCULATE filter, but a one-row, one-column table used as a value is its
date in Desktop: in an iterator, a comparison, date arithmetic, YEAR, DATEDIFF,
FORMAT and &. The rows now carry the column's own cell, and those positions
read a one-row, one-column table as its value (_scalarize).

Expected values: Power BI Desktop 2.152 over ADOMD (build_b145.py), one
EVALUATE ROW per probe, generated from its output. Its date - date probes are
a separate issue and are left out.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(91)]   # Jan..Mar 2024
SALE_DAYS = [d for i, d in enumerate(DAYS) if i % 3 == 0]
TABLES = {"Dt": {"columns": ["Date", "Month"], "rows": [[d, d.month] for d in DAYS]},
          "F": {"columns": ["Date", "Amount"],
                "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}}
RELS = [{"FromTable": "F", "FromColumn": "Date", "ToTable": "Dt", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {"FS": "SUM(F[Amount])"}
TEXT = {"format_lastdate", "concat_firstdate"}   # FORMAT and & give text

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b145.py's output
    ('maxx_firstdate', 'MAXX(Dt, FIRSTDATE(Dt[Date]))', '2024-03-31T00:00:00'),
    ('minx_lastdate', 'MINX(VALUES(Dt[Month]), LASTDATE(Dt[Date]))', '2024-01-31T00:00:00'),
    ('minx_endofmonth', 'MINX(VALUES(Dt[Month]), ENDOFMONTH(Dt[Date]))', '2024-01-31T00:00:00'),
    ('maxx_startofmonth', 'MAXX(VALUES(Dt[Month]), STARTOFMONTH(Dt[Date]))', '2024-03-01T00:00:00'),
    ('filter_firstdate_eq', 'COUNTROWS(FILTER(Dt, FIRSTDATE(Dt[Date]) = Dt[Date]))', 91),
    ('firstdate_plus', 'FIRSTDATE(Dt[Date]) + 1', '2024-01-02T00:00:00'),
    ('datediff', 'DATEDIFF(FIRSTDATE(Dt[Date]), LASTDATE(Dt[Date]), DAY)', 90),
    ('year_lastdate', 'YEAR(LASTDATE(Dt[Date]))', 2024),
    ('istext_firstdate', 'IF(ISTEXT(FIRSTDATE(Dt[Date])), 1, 0)', 0),
    ('lastdate_gt', 'IF(LASTDATE(Dt[Date]) > DATE(2024, 3, 30), 1, 0)', 1),
    ('endofyear', 'ENDOFYEAR(Dt[Date])', '2024-03-31T00:00:00'),
    ('startofquarter', 'STARTOFQUARTER(Dt[Date])', '2024-01-01T00:00:00'),
    ('format_lastdate', 'FORMAT(LASTDATE(Dt[Date]), "yyyy-mm-dd")', '2024-03-31'),
    ('concat_firstdate', '"d:" & FIRSTDATE(Dt[Date])', 'd:1/1/2024'),
    ('calc_lastdate_month', 'CALCULATE(LASTDATE(Dt[Date]), Dt[Month] = 2)', '2024-02-29T00:00:00'),
    ('fs_at_lastdate', 'CALCULATE([FS], LASTDATE(Dt[Date]))', 31),
    ('lastnonblank', 'LASTNONBLANK(Dt[Date], [FS])', '2024-03-31T00:00:00'),
    ('maxx_lastnonblank', 'MAXX(VALUES(Dt[Month]), LASTNONBLANK(Dt[Date], [FS]))', '2024-03-31T00:00:00'),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES)
    m["p"] = expr
    got = de.evaluate_measures_smart(["p"], TABLES, m, {}, relationships=RELS, date_tables={"Dt": "Date"},
                                     simulate_row_context=False)["p"]
    if probe in TEXT:
        assert got == want
    elif isinstance(want, str):
        assert got is not None and hasattr(got, "isoformat") and got.isoformat()[:19] == want[:19]
    else:
        assert got == pytest.approx(want)
