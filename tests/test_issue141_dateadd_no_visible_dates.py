"""Issue #141: DATEADD / SAMEPERIODLASTYEAR of a date column shift nothing when
the context leaves no date visible.

The engine fell back to the whole calendar: CALCULATE(COUNTROWS(DATEADD(
D[Date], 1, MONTH)), D[Month] = 199001) counted 516, and a sales total over
that shift read 13,962, where Power BI Desktop answers BLANK. A filter on the
fact, which does not reach the calendar, still leaves every date visible.

Expected values: Desktop over ADOMD (build_b141.py: the calendar, facts and
marking of build_b138.py), one EVALUATE ROW per probe. Generated from
Desktop's output.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2023, 1, 1) + timedelta(days=i) for i in range(547)]
SALE_DAYS = [d for i, d in enumerate(DAYS) if i % 3 == 0 and d <= datetime(2024, 5, 15)]


def _calendar():
    return {"columns": ["Date", "Month", "Quarter", "Weekday"],
            "rows": [[d, d.year * 100 + d.month, d.year * 10 + (d.month - 1) // 3 + 1, d.isoweekday()]
                     for d in DAYS]}


def _fact():
    return {"columns": ["Date", "Amount"],
            "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}


TABLES = {"Dt": _calendar(), "Du": _calendar(), "F": _fact(), "G": _fact()}
RELS = [{"FromTable": "F", "FromColumn": "Date", "ToTable": "Dt", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1},
        {"FromTable": "G", "FromColumn": "Date", "ToTable": "Du", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {"FS": "SUM(F[Amount])", "GS": "SUM(G[Amount])"}

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b141.py's output
    ('none_month_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), Dt[Month] = 199001)', None),
    ('none_date_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), Dt[Date] = DATE(1990, 1, 1))', None),
    ('none_filter_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), FILTER(ALL(Dt[Date]), FALSE()))', None),
    ('none_weekday_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), Dt[Weekday] = 9))', None),
    ('none_sply_Dt', 'CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(Dt[Date])), Dt[Month] = 199001)', None),
    ('none_min_Dt', 'CALCULATE(MINX(DATEADD(Dt[Date], 1, MONTH), Dt[Date]), Dt[Month] = 199001)', None),
    ('none_calc_Dt', 'CALCULATE(CALCULATE([FS], DATEADD(Dt[Date], 1, MONTH)), Dt[Month] = 199001)', None),
    ('none_calctable_Dt', 'CALCULATE(COUNTROWS(CALCULATETABLE(DATEADD(Dt[Date], 1, MONTH))), Dt[Month] = 199001)', None),
    ('none_back_q_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], -1, QUARTER)), Dt[Weekday] = 9)', None),
    ('fact_filter_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), F[Amount] = -1)', 516),
    ('all_Dt', 'COUNTROWS(DATEADD(Dt[Date], 1, MONTH))', 516),
    ('last_month_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), Dt[Month] = 202406)', None),
    ('none_month_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), Du[Month] = 199001)', None),
    ('none_date_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), Du[Date] = DATE(1990, 1, 1))', None),
    ('none_filter_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), FILTER(ALL(Du[Date]), FALSE()))', None),
    ('none_weekday_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), Du[Weekday] = 9))', None),
    ('none_sply_Du', 'CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(Du[Date])), Du[Month] = 199001)', None),
    ('none_min_Du', 'CALCULATE(MINX(DATEADD(Du[Date], 1, MONTH), Du[Date]), Du[Month] = 199001)', None),
    ('none_calc_Du', 'CALCULATE(CALCULATE([GS], DATEADD(Du[Date], 1, MONTH)), Du[Month] = 199001)', None),
    ('none_calctable_Du', 'CALCULATE(COUNTROWS(CALCULATETABLE(DATEADD(Du[Date], 1, MONTH))), Du[Month] = 199001)', None),
    ('none_back_q_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], -1, QUARTER)), Du[Weekday] = 9)', None),
    ('fact_filter_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), G[Amount] = -1)', 516),
    ('all_Du', 'COUNTROWS(DATEADD(Du[Date], 1, MONTH))', 516),
    ('last_month_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), Du[Month] = 202406)', None),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES)
    m["p"] = expr
    got = de.evaluate_measures_smart(["p"], TABLES, m, {}, relationships=RELS,
                                     date_tables={"Dt": "Date"}, simulate_row_context=False)["p"]
    if want is None:
        assert got is None
    else:
        assert got == pytest.approx(want)
