"""Issue #122: DATESINPERIOD shifts its start as DATEADD does -- clamped to the
target month, a month END to a month END -- with the far boundary exclusive.

The YEAR branch built datetime(year + offset, month, day), which raised on 29
February and blanked the measure; MONTH and QUARTER clamped the day but sent a
month end to the same day number. Expected values: Power BI Desktop 2.152 over
ADOMD, on a daily calendar 2023-01-01..2025-12-31 (dates as YYYYMMDD).
"""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"Date": {"columns": ["Date"],
                   "rows": [[dt.datetime(2023, 1, 1) + dt.timedelta(days=i)] for i in range(1096)]}}

DESKTOP = [   # (start, n, interval, count, first, last); * = 0.9.121 differed
    ("DATE(2024,2,29)", -1, "YEAR", 366, 20230301, 20240229),      # * raised -> BLANK
    ("DATE(2024,2,28)", -1, "YEAR", 365, 20230301, 20240228),
    ("DATE(2024,2,29)", 1, "YEAR", 365, 20240229, 20250227),       # *
    ("DATE(2024,2,29)", -12, "MONTH", 366, 20230301, 20240229),
    ("DATE(2024,2,29)", -4, "QUARTER", 366, 20230301, 20240229),
    ("DATE(2024,3,31)", -1, "MONTH", 31, 20240301, 20240331),
    ("DATE(2024,3,31)", -1, "QUARTER", 91, 20240101, 20240331),
    ("DATE(2025,2,28)", -1, "YEAR", 365, 20240301, 20250228),      # * a month end to a month end
    ("DATE(2024,2,29)", -2, "YEAR", 425, 20230101, 20240229),      # * (calendar starts 2023)
    ("DATE(2024,2,29)", -1, "DAY", 1, 20240229, 20240229),
    ("DATE(2024,1,31)", 1, "MONTH", 29, 20240131, 20240228),
    ("DATE(2024,5,31)", -3, "MONTH", 92, 20240301, 20240531),
]


def _ymd(expr):
    return f"VAR d = {expr} RETURN YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)"


@pytest.mark.parametrize("start,n,unit,count,first,last", DESKTOP,
                         ids=[f"{s}{n:+d}{u}" for s, n, u, *_ in DESKTOP])
def test_window_matches_desktop(start, n, unit, count, first, last):
    dip = f"DATESINPERIOD('Date'[Date], {start}, {n}, {unit})"
    m = {"n": f"COUNTROWS({dip})", "f": _ymd(f"MINX({dip}, 'Date'[Date])"),
         "l": _ymd(f"MAXX({dip}, 'Date'[Date])")}
    got = de.evaluate_measures_smart(list(m), TABLES, m, {}, simulate_row_context=False)
    assert (got["n"], got["f"], got["l"]) == (count, first, last)


def test_moving_annual_total_on_29_february():
    """Desktop 2.152: 366 -- the measure was BLANK on 29 February 2024."""
    m = {"mat": "CALCULATE(CALCULATE(COUNTROWS('Date'), DATESINPERIOD('Date'[Date], MAX('Date'[Date]), "
                "-1, YEAR)), 'Date'[Date] = DATE(2024,2,29))"}
    assert de.evaluate_measures_smart(["mat"], TABLES, m, {}, simulate_row_context=False)["mat"] == 366
