"""Issue #129: DATEADD shifts a run's START by its day number (clamped), and
only the END of a run of two or more days that closes its month lands on the
target month's last day -- a single date keeps its day number.

Both ends went through the month-end rule, so 30 April alone -1 MONTH was 31
March. Expected values: Power BI Desktop 2.152 over ADOMD, daily calendar
2023-01-01..2025-12-31; each case is DATEADD(DATESBETWEEN('Date'[Date], from,
to), n, interval) as a table (count, first, last as YYYYMMDD) and as a
CALCULATE filter over the same selection (the count).
"""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"Date": {"columns": ["Date"],
                   "rows": [[dt.datetime(2023, 1, 1) + dt.timedelta(days=i)] for i in range(1096)]}}

DESKTOP = [   # (name, from, to, n, interval, count, first, last); * = 0.9.121 differed
    ("apr30_m1", "DATE(2024,4,30)", "DATE(2024,4,30)", -1, "MONTH", 1, 20240330, 20240330),          # *
    ("mar31_m1", "DATE(2024,3,31)", "DATE(2024,3,31)", -1, "MONTH", 1, 20240229, 20240229),
    ("jan31_p1", "DATE(2024,1,31)", "DATE(2024,1,31)", 1, "MONTH", 1, 20240229, 20240229),
    ("apr15_30_m1", "DATE(2024,4,15)", "DATE(2024,4,30)", -1, "MONTH", 17, 20240315, 20240331),
    ("apr_m1", "DATE(2024,4,1)", "DATE(2024,4,30)", -1, "MONTH", 31, 20240301, 20240331),
    ("mar_m1", "DATE(2024,3,1)", "DATE(2024,3,31)", -1, "MONTH", 29, 20240201, 20240229),
    ("feb29_p1", "DATE(2024,2,29)", "DATE(2024,2,29)", 1, "MONTH", 1, 20240329, 20240329),           # *
    ("feb28_23_p1", "DATE(2023,2,28)", "DATE(2023,2,28)", 1, "MONTH", 1, 20230328, 20230328),        # *
    ("feb28_24_p1", "DATE(2024,2,28)", "DATE(2024,2,28)", 1, "MONTH", 1, 20240328, 20240328),
    ("feb29_y1", "DATE(2024,2,29)", "DATE(2024,2,29)", -1, "YEAR", 1, 20230228, 20230228),
    ("feb28_23_py1", "DATE(2023,2,28)", "DATE(2023,2,28)", 1, "YEAR", 1, 20240228, 20240228),        # *
    ("nov30_q1", "DATE(2024,11,30)", "DATE(2024,11,30)", -1, "QUARTER", 1, 20240830, 20240830),      # *
    ("apr29_30_m1", "DATE(2024,4,29)", "DATE(2024,4,30)", -1, "MONTH", 3, 20240329, 20240331),
    ("may30_m1", "DATE(2024,5,30)", "DATE(2024,5,30)", -1, "MONTH", 1, 20240430, 20240430),
    ("may31_m1", "DATE(2024,5,31)", "DATE(2024,5,31)", -1, "MONTH", 1, 20240430, 20240430),
    ("jun30_p1", "DATE(2024,6,30)", "DATE(2024,6,30)", 1, "MONTH", 1, 20240730, 20240730),           # *
    ("apr30_may2_m1", "DATE(2024,4,30)", "DATE(2024,5,2)", -1, "MONTH", 4, 20240330, 20240402),      # *
    ("jun30_jul1_p1", "DATE(2024,6,30)", "DATE(2024,7,1)", 1, "MONTH", 3, 20240730, 20240801),       # *
    ("jan31_feb2_p1", "DATE(2024,1,31)", "DATE(2024,2,2)", 1, "MONTH", 3, 20240229, 20240302),
    ("mar31_apr1_m1", "DATE(2024,3,31)", "DATE(2024,4,1)", -1, "MONTH", 2, 20240229, 20240301),
    ("nov30_dec31_q1", "DATE(2024,11,30)", "DATE(2024,12,31)", -1, "QUARTER", 32, 20240830, 20240930),  # *
    ("feb28_mar1_23_py1", "DATE(2023,2,28)", "DATE(2023,3,1)", 1, "YEAR", 3, 20240228, 20240301),    # *
]


def _ymd(expr):
    return f"VAR d = {expr} RETURN YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)"


CALC_MEASURED = {c[0] for c in DESKTOP[:16]}   # the CALCULATE form was measured for these


@pytest.mark.parametrize("name,a,b,n,unit,count,first,last", DESKTOP, ids=[c[0] for c in DESKTOP])
def test_matches_desktop(name, a, b, n, unit, count, first, last):
    t = f"DATEADD(DATESBETWEEN('Date'[Date], {a}, {b}), {n}, {unit})"
    m = {"n": f"COUNTROWS({t})", "f": _ymd(f"MINX({t}, 'Date'[Date])"), "l": _ymd(f"MAXX({t}, 'Date'[Date])"),
         "c": f"CALCULATE(CALCULATE(COUNTROWS('Date'), DATEADD('Date'[Date], {n}, {unit})), "
              f"DATESBETWEEN('Date'[Date], {a}, {b}))"}
    got = de.evaluate_measures_smart(list(m), TABLES, m, {}, simulate_row_context=False)
    assert (got["n"], got["f"], got["l"]) == (count, first, last)
    if name in CALC_MEASURED:
        assert got["c"] == count
