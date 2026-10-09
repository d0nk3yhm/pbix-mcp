"""Issue #147: DATEADD / SAMEPERIODLASTYEAR as a CALCULATE filter inside an
iterator shift the row's dates.

CALCULATE's fast path for them read the dates of the context the rows were
opened in, so every month of SUMX(VALUES(Dt[Month]), CALCULATE([FS],
DATEADD(Dt[Date], -1, MONTH))) shifted the whole selection (630, Desktop 210).
Desktop reads the <dates> column as CALCULATETABLE(DISTINCT(<dates>)), which
transitions the row, as PREVIOUSMONTH and a measure reference already did.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b147.py), on a marked
(Dt) and an unmarked (Du) calendar."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit


def _rel(many, many_col, one, one_col):
    return {"FromTable": many, "FromColumn": many_col, "ToTable": one, "ToColumn": one_col,
            "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


def _eval(expr, tables, measures, rels, date_tables=None):
    m = dict(measures)
    m["p"] = expr
    return de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels,
                                      date_tables=date_tables or {}, simulate_row_context=False)["p"]


def _check(got, want):
    if want is None:
        assert got is None
    elif isinstance(want, bool):
        assert got is want
    elif isinstance(want, str):
        assert got == want
    else:
        assert got == pytest.approx(want)


DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(91)]   # Jan..Mar 2024
SALE_DAYS = [d for i, d in enumerate(DAYS) if i % 3 == 0]


def _calendar():
    return {"columns": ["Date", "Month"], "rows": [[d, d.month] for d in DAYS]}


def _fact():
    # two rows per sale day, as in build_b137.py
    return {"columns": ["Date", "Amount"],
            "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}


def _date_rel(fact, cal):
    return {"FromTable": fact, "FromColumn": "Date", "ToTable": cal, "ToColumn": "Date", "IsActive": True,
            "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


# build_b147.py: two calendars, Dt marked as a date table
CALS = {"Dt": _calendar(), "Du": _calendar(), "F": _fact(), "G": _fact()}
CALS_RELS = [_date_rel("F", "Dt"), _date_rel("G", "Du")]
CALS_MEASURES = {"FS": "SUM(F[Amount])", "GS": "SUM(G[Amount])",
                 "PM_Dt": "CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH))",
                 "PM_Du": "CALCULATE([GS], DATEADD(Du[Date], -1, MONTH))"}

B147 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b147.py
    ('prev_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH)))', 210),
    ('next_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], 1, MONTH)))', 430),
    ('prev_measure_Dt', 'SUMX(VALUES(Dt[Month]), [PM_Dt])', 210),
    ('prev_nested_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH))))', 210),
    ('prev_day_Dt', 'SUMX(Dt, CALCULATE([FS], DATEADD(Dt[Date], -1, DAY)))', 465),
    ('prev_week_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -7, DAY)))', 406),
    ('sply_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], SAMEPERIODLASTYEAR(Dt[Date])))', None),
    ('prev_max_Dt', 'MAXX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH)))', 144),
    ('prev_rows_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(Dt), DATEADD(Dt[Date], -1, MONTH)))', 60),
    ('prev_kept_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH), Dt[Month] = 2))', 144),
    ('filter_prev_Dt', 'COUNTROWS(FILTER(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH)) > 0))', 2),
    ('pp_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], PARALLELPERIOD(Dt[Date], -1, MONTH)))', 210),
    ('prev_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], -1, MONTH)))', 210),
    ('next_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], 1, MONTH)))', 430),
    ('prev_measure_Du', 'SUMX(VALUES(Du[Month]), [PM_Du])', 210),
    ('prev_nested_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(CALCULATE([GS], DATEADD(Du[Date], -1, MONTH))))', 210),
    ('prev_day_Du', 'SUMX(Du, CALCULATE([GS], DATEADD(Du[Date], -1, DAY)))', 465),
    ('prev_week_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], -7, DAY)))', 406),
    ('sply_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], SAMEPERIODLASTYEAR(Du[Date])))', None),
    ('prev_max_Du', 'MAXX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], -1, MONTH)))', 144),
    ('prev_rows_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(Du), DATEADD(Du[Date], -1, MONTH)))', 60),
    ('prev_kept_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], -1, MONTH), Du[Month] = 2))', 144),
    ('filter_prev_Du', 'COUNTROWS(FILTER(VALUES(Du[Month]), CALCULATE([GS], DATEADD(Du[Date], -1, MONTH)) > 0))', 2),
    ('pp_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], PARALLELPERIOD(Du[Date], -1, MONTH)))', 210),
]


@pytest.mark.parametrize("probe,expr,want", B147, ids=[p for p, _e, _w in B147])
def test_b147(probe, expr, want):
    _check(_eval(expr, CALS, CALS_MEASURES, CALS_RELS, {"Dt": "Date"}), want)
