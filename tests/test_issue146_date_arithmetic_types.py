"""Issue #146: date arithmetic is typed as Desktop types it.

In Power BI Desktop 2.152, '-' gives a DateTime when its LEFT operand is one
(date - date included: 90 days is 1900-03-30), '+' when either is, '*' and '/'
a number; ISNUMBER of a date is TRUE; SUMX and MAXX of dates are dates and
AVERAGEX a number; IF with a date beside a number literal is a number. The
engine made date - date a number of days (0.9.79, a choice that was never
measured), and its ISNUMBER, SUMX and AVERAGEX skipped dates.

Expected values: Desktop over ADOMD (build_b146.py, and the date - date probes
of build_b145.py), one EVALUATE ROW per probe, generated from its output.
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
# probes whose answer is text (& and FORMAT)
TEXT = {"dd_concat", "d_concat", "t_minus_t_concat", "sumx_dd_concat", "maxx_dd_concat",
        "averagex_d_concat", "max_minus_min_concat", "if_mixed_concat", "dd_format0",
        "date_minus_date_concat"}

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b146.py's output
    ('d_minus_d', 'DATE(2024, 3, 31) - DATE(2024, 1, 1)', '1900-03-30T00:00:00'),
    ('d_minus_d_neg', 'DATE(2024, 1, 1) - DATE(2024, 3, 31)', '1899-10-01T00:00:00'),
    ('d_minus_n', 'DATE(2024, 3, 31) - 1', '2024-03-30T00:00:00'),
    ('n_minus_d', '50000 - DATE(2024, 3, 31)', 4618),
    ('d_plus_n', 'DATE(2024, 3, 31) + 1', '2024-04-01T00:00:00'),
    ('n_plus_d', '1 + DATE(2024, 3, 31)', '2024-04-01T00:00:00'),
    ('d_plus_d', 'DATE(2024, 3, 31) + DATE(2024, 1, 1)', '2148-04-02T00:00:00'),
    ('d_times_n', 'DATE(2024, 3, 31) * 1', 45382),
    ('d_div_n', 'DATE(2024, 3, 31) / 1', 45382),
    ('neg_d', '-DATE(2024, 3, 31)', -45382),
    ('dd_plus_0', '(DATE(2024, 3, 31) - DATE(2024, 1, 1)) + 0', '1900-03-30T00:00:00'),
    ('dd_times_1', '(DATE(2024, 3, 31) - DATE(2024, 1, 1)) * 1', 90),
    ('dd_div_1', '(DATE(2024, 3, 31) - DATE(2024, 1, 1)) / 1', 90),
    ('dd_int', 'INT(DATE(2024, 3, 31) - DATE(2024, 1, 1))', 90),
    ('dd_concat', '"n:" & (DATE(2024, 3, 31) - DATE(2024, 1, 1))', 'n:3/30/1900'),
    ('d_concat', '"d:" & DATE(2024, 3, 31)', 'd:3/31/2024'),
    ('dd_format0', 'FORMAT(DATE(2024, 3, 31) - DATE(2024, 1, 1), "0")', '90'),
    ('dd_istext', 'IF(ISTEXT(DATE(2024, 3, 31) - DATE(2024, 1, 1)), 1, 0)', 0),
    ('dd_isnumber', 'IF(ISNUMBER(DATE(2024, 3, 31) - DATE(2024, 1, 1)), 1, 0)', 1),
    ('d_isnumber', 'IF(ISNUMBER(DATE(2024, 3, 31)), 1, 0)', 1),
    ('t_minus_t', 'TIME(18, 0, 0) - TIME(6, 0, 0)', '1899-12-30T12:00:00'),
    ('t_minus_t_concat', '"t:" & (TIME(18, 0, 0) - TIME(6, 0, 0))', 't:12:00:00 PM'),
    ('d_minus_t', 'DATE(2024, 3, 31) - TIME(6, 0, 0)', '2024-03-30T18:00:00'),
    ('dd_blank', 'DATE(2024, 3, 31) - BLANK()', '2024-03-31T00:00:00'),
    ('blank_minus_d', 'BLANK() - DATE(2024, 3, 31)', -45382),
    ('sumx_dd', 'SUMX(Dt, Dt[Date] - DATE(2024, 1, 1))', '1911-03-18T00:00:00'),
    ('sumx_dd_concat', '"s:" & SUMX(Dt, Dt[Date] - DATE(2024, 1, 1))', 's:3/18/1911'),
    ('maxx_dd', 'MAXX(Dt, Dt[Date] - DATE(2024, 1, 1))', '1900-03-30T00:00:00'),
    ('maxx_dd_concat', '"m:" & MAXX(Dt, Dt[Date] - DATE(2024, 1, 1))', 'm:3/30/1900'),
    ('averagex_d', 'AVERAGEX(Dt, Dt[Date])', 45337),
    ('averagex_d_concat', '"a:" & AVERAGEX(Dt, Dt[Date])', 'a:45337'),
    ('max_minus_min', 'MAX(Dt[Date]) - MIN(Dt[Date])', '1900-03-30T00:00:00'),
    ('max_minus_min_concat', '"r:" & (MAX(Dt[Date]) - MIN(Dt[Date]))', 'r:3/30/1900'),
    ('if_mixed', 'IF(TRUE(), DATE(2024, 3, 31) - DATE(2024, 1, 1), 0)', 90),
    ('if_mixed_concat', '"i:" & IF(TRUE(), DATE(2024, 3, 31) - DATE(2024, 1, 1), 0)', 'i:90'),
    ('datediff', 'DATEDIFF(DATE(2024, 1, 1), DATE(2024, 3, 31), DAY)', 90),
    # build_b145.py's date - date probes
    ('lastdate_minus_first', 'LASTDATE(Dt[Date]) - FIRSTDATE(Dt[Date])', '1900-03-30T00:00:00'),
    ('date_minus_date', 'DATE(2024, 3, 31) - DATE(2024, 1, 1)', '1900-03-30T00:00:00'),
    ('date_minus_date_isnumber', 'IF(ISNUMBER(DATE(2024, 3, 31) - DATE(2024, 1, 1)), 1, 0)', 1),
    ('date_minus_date_int', 'INT(DATE(2024, 3, 31) - DATE(2024, 1, 1))', 90),
    ('date_minus_date_times1', '(DATE(2024, 3, 31) - DATE(2024, 1, 1)) * 1', 90),
    ('date_minus_date_concat', '"n:" & (DATE(2024, 3, 31) - DATE(2024, 1, 1))', 'n:3/30/1900'),
    ('date_minus_number', 'DATE(2024, 3, 31) - 1', '2024-03-30T00:00:00'),
    ('col_minus_col', 'MAXX(Dt, Dt[Date] - DATE(2024, 1, 1))', '1900-03-30T00:00:00'),
    ('col_minus_col_isnumber', 'IF(ISNUMBER(MAX(Dt[Date]) - MIN(Dt[Date])), 1, 0)', 1),
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
        assert not hasattr(got, "isoformat"), got   # a number, not a date
        assert got == pytest.approx(want)
