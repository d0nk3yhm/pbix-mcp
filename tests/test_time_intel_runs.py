"""DATEADD shifts each CONTIGUOUS RUN of the selection, not the min..max range.

Verified against Power BI Desktop's own engine on MS_Employee_Hiring, whose
'Date' table is marked as a date table (DataCategory = "Time") and holds 2,557
rows spanning seven years:

    CALCULATE(COUNTROWS('Date'), DATEADD('Date'[Date],-1,MONTH))  @ Qtr=2 = 644
    CALCULATE(COUNTROWS('Date'), DATEADD('Date'[Date],-1,YEAR))   @ Qtr=2 = 546
    CALCULATE([New Hires SPLY], 'Date'[Qtr]=2)                          = 11601

644 is 92 x 7 -- Mar+Apr+May of each of the seven years -- and 546 is 91 x 6:
six shifted QUARTERS, not six years of dates.

``'Date'[Qtr] = 2`` selects seven DISJOINT quarters, so shifting the single
min..max range spanned everything between the first and the last quarter and
the filter degenerated to the whole table: [New Hires SPLY] returned the GRAND
TOTAL 43120 under every quarter, where Desktop returns 11601 for Q2 and 13840
for Q3, and every YoY measure built on it inherited the error.

Desktop returns the SAME numbers when the other date-column filter is dropped
explicitly -- REMOVEFILTERS('Date'[Qtr]) leaves 644 at 644 and 546 at 546 --
so the shift is computed UNDER the current filter and the date table's other
filters are removed only afterwards. Both halves matter: computing the shift
with the quarter already gone would have scanned all 2,557 dates.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from pbix_mcp.dax import engine as de


def _ev(tables, expr, filters):
    ctx = de.DAXContext(tables, {"M": expr}, None, None, filters, [])
    return de.DAXEngine().evaluate_measure("M", ctx)


class TestDisjointSelectionShiftsPerRun:
    """The miniature of the corpus defect, small enough to check by hand."""

    TABLES = {
        "Date": {
            "columns": ["Date", "Qtr", "v"],
            "rows": [
                [datetime(2023, 1, 15), 1, 5],
                [datetime(2023, 4, 15), 2, 10],
                [datetime(2024, 1, 15), 1, 7],
                [datetime(2024, 4, 15), 2, 20],
            ],
        }
    }
    Q2 = {"Date.Qtr": [2]}

    def test_sameperiodlastyear_shifts_each_quarter_separately(self):
        """Q2 selects 2023-04-15 and 2024-04-15 -- two runs. Shifted back a
        year they are 2022-04-15 (absent) and 2023-04-15 (v=10)."""
        got = _ev(self.TABLES,
                  "CALCULATE(SUM('Date'[v]), SAMEPERIODLASTYEAR('Date'[Date]))",
                  self.Q2)
        assert got == 10

    def test_dateadd_agrees_with_sameperiodlastyear(self):
        got = _ev(self.TABLES,
                  "CALCULATE(SUM('Date'[v]), DATEADD('Date'[Date], -1, YEAR))",
                  self.Q2)
        assert got == 10

    def test_the_whole_table_is_not_the_answer(self):
        """43,120-shaped failure: the shifted range must not span the gap
        between the two quarters and swallow the January rows."""
        got = _ev(self.TABLES,
                  "CALCULATE(SUM('Date'[v]), SAMEPERIODLASTYEAR('Date'[Date]))",
                  self.Q2)
        assert got != 42          # grand total
        assert got != 15          # 2023 entire (the old min..max range)


def _calendar(y1, m1, y2, m2):
    """Every day from the 1st of (y1,m1) to the end of (y2,m2)."""
    rows, d = [], datetime(y1, m1, 1)
    end = datetime(y2 + (m2 == 12), (m2 % 12) + 1, 1)
    while d < end:
        last = (datetime(d.year + (d.month == 12), (d.month % 12) + 1, 1)
                - timedelta(days=1)).day
        rows.append([d, d.month, 1 if d.day == last else 0])
        d += timedelta(days=1)
    return {"D": {"columns": ["Date", "M", "eom"], "rows": rows}}


class TestContiguousSelectionIsUnchanged:
    """The behaviour the run-splitting must NOT break: one block still shifts
    as a PERIOD and is refilled over the date table, so no day is dropped."""

    TABLES = _calendar(2024, 1, 2024, 2)      # Jan (31) + Feb (29), leap year

    def test_a_full_month_shifts_to_a_full_month(self):
        """February is contiguous. Shifted back a month it must be all 31 days
        of January -- not 29. Mapping date-by-date drops Jan 30 and 31 because
        no February day lands there; that hole cost 11.9M on PM Total Sales."""
        got = _ev(self.TABLES,
                  "CALCULATE(COUNTROWS('D'), DATEADD('D'[Date], -1, MONTH))",
                  {"D.M": [2]})
        assert got == 31

    def test_month_end_maps_to_month_end(self):
        got = _ev(self.TABLES,
                  "CALCULATE(MAX('D'[Date]), DATEADD('D'[Date], -1, MONTH))",
                  {"D.M": [2]})
        assert got == datetime(2024, 1, 31)


class TestTwoIsolatedDaysDoNotSpanTheGap:
    """The sharpest discriminator: two single-day runs far apart.

    Jan 31 shifts to Dec 31 2023, which the table does not contain, so it
    contributes nothing; Feb 29 is a month end and shifts to Jan 31. The answer
    is one row. Shifting min..max instead gives Dec 31..Jan 31, i.e. the whole
    of January -- 31 rows.
    """

    TABLES = _calendar(2024, 1, 2024, 2)

    def test_only_the_surviving_run_counts(self):
        got = _ev(self.TABLES,
                  "CALCULATE(COUNTROWS('D'), DATEADD('D'[Date], -1, MONTH))",
                  {"D.eom": [1]})
        assert got == 1
