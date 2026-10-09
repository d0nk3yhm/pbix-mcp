"""Issue #127: DATEADD / SAMEPERIODLASTYEAR over a TABLE of dates -- as a table
and as a CALCULATE filter argument.

Only a column reference was parsed: a table expression returned a marker, so
COUNTROWS / MINX over the result saw nothing, and the CALCULATE fast path
dropped the filter. Expected values: Power BI Desktop 2.152 over ADOMD, daily
calendar 2023-01-01..2025-12-31 (dates as YYYYMMDD).
"""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"Date": {"columns": ["Date"],
                   "rows": [[dt.datetime(2023, 1, 1) + dt.timedelta(days=i)] for i in range(1096)]}}


def _ymd(expr):
    return f"VAR d = {expr} RETURN YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)"


DBT = "DATESBETWEEN('Date'[Date], {a}, {b})"
DESKTOP = {   # expression -> Desktop 2.152
    "COUNTROWS(DATEADD(" + DBT.format(a="DATE(2024,3,1)", b="DATE(2024,3,5)") + ", -1, YEAR))": 5,
    _ymd("MINX(DATEADD(" + DBT.format(a="DATE(2024,2,29)", b="DATE(2024,2,29)") + ", -1, YEAR), 'Date'[Date])"):
        20230228,
    "COUNTROWS(SAMEPERIODLASTYEAR(" + DBT.format(a="DATE(2024,2,1)", b="DATE(2024,2,29)") + "))": 28,
    "COUNTROWS(DATEADD(FILTER(ALL('Date'[Date]), 'Date'[Date] >= DATE(2024,3,1) && "
    "'Date'[Date] <= DATE(2024,3,5)), -1, YEAR))": 5,
    "CALCULATE(COUNTROWS('Date'), DATEADD(" + DBT.format(a="DATE(2024,3,1)", b="DATE(2024,3,31)")
    + ", -1, MONTH))": 29,                                              # 0.9.121: 1096, the filter dropped
    "CALCULATE(COUNTROWS('Date'), SAMEPERIODLASTYEAR(" + DBT.format(a="DATE(2024,2,1)", b="DATE(2024,2,29)")
    + "))": 28,
    "CALCULATE(COUNTROWS('Date'), DATEADD(" + DBT.format(a="DATE(2030,1,1)", b="DATE(2030,1,5)")
    + ", -1, YEAR))": None,                                             # nothing to shift
    _ymd("MINX(DATEADD(" + DBT.format(a="DATE(2024,4,30)", b="DATE(2024,4,30)") + ", -1, MONTH), 'Date'[Date])"):
        20240330,                                                       # #129: a single date keeps its day
}


@pytest.mark.parametrize("expr,want", list(DESKTOP.items()), ids=[f"p{i}" for i in range(len(DESKTOP))])
def test_matches_desktop(expr, want):
    got = de.evaluate_measures_smart(["m"], TABLES, {"m": expr}, {}, simulate_row_context=False)["m"]
    assert got == want
