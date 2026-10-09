"""Issue #138: DATEADD / SAMEPERIODLASTYEAR over a TABLE expression shift the
table's own dates, whatever else filters the date table.

The engine read the table's dates back under the current context with only
the date column's filter replaced, so an outer row's month, quarter or weekday
cut the table down before the shift, and when nothing was left it shifted
every date of the calendar. Awesome Chocolates' [Max Previous Quarter Not
Blank], CALCULATE([Max Quarter Not Blank], DATEADD(FILTER(ALLSELECTED(
'dim-Date'[Date]), NOT ISBLANK([Sales Actual])), -1, QUARTER)), answered the
quarter before each quarter; Desktop answers 20234 for every quarter.

Expected values: Desktop over ADOMD (build_b138.py), one EVALUATE ROW per probe,
over a marked calendar (Dt) and an unmarked one (Du), 2023-01-01 to 2024-06-30,
with a sale every third day through 2024-05-15. They answer alike. Generated
from Desktop's output.
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
    # two rows per sale day: the fact is the MANY side, as in build_b138.py
    return {"columns": ["Date", "Amount"],
            "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}


TABLES = {"Dt": _calendar(), "Du": _calendar(), "F": _fact(), "G": _fact()}
RELS = [{"FromTable": "F", "FromColumn": "Date", "ToTable": "Dt", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1},
        {"FromTable": "G", "FromColumn": "Date", "ToTable": "Du", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {"FS": "SUM(F[Amount])", "GS": "SUM(G[Amount])"}
for _D, _M in (("Dt", "FS"), ("Du", "GS")):
    MEASURES[f"MaxQ_{_D}"] = f"MAXX(FILTER(ALLSELECTED({_D}), NOT ISBLANK([{_M}])), {_D}[Quarter])"
    MEASURES[f"MaxPrevQ_{_D}"] = (f"CALCULATE([MaxQ_{_D}], DATEADD(FILTER(ALLSELECTED({_D}[Date]), "
                                  f"NOT ISBLANK([{_M}])), -1, QUARTER))")

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b138.py's output
    ('feb_month_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 558),
    ('feb_quarter_iter_Dt', 'SUMX(VALUES(Dt[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 186),
    ('feb_weekday_iter_Dt', 'SUMX(VALUES(Dt[Weekday]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 217),
    ('jan10_row_iter_Dt', 'SUMX(FILTER(Dt, Dt[Month] = 202403), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 1, 1) && Dt[Date] <= DATE(2024, 1, 10)), 1, MONTH))))', 310),
    ('one_month_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] = DATE(2024, 2, 10)), 1, MONTH))))', 18),
    ('one_min_Dt', 'MINX(VALUES(Dt[Month]), CALCULATE(MINX(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] = DATE(2024, 2, 10)), 1, MONTH), Dt[Date])))', '2024-03-10T00:00:00'),
    ('feb_explicit_month_Dt', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH)), Dt[Month] = 202305)', 31),
    ('feb_explicit_weekday_Dt', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH)), Dt[Weekday] = 3)', 31),
    ('one_explicit_quarter_Dt', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] = DATE(2024, 2, 10)), 1, MONTH)), Dt[Quarter] = 20231)', 1),
    ('feb_top_Dt', 'COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH))', 31),
    ('feb_back_q_top_Dt', 'COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), -1, QUARTER))', 30),
    ('feb_back_q_iter_Dt', 'SUMX(VALUES(Dt[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), -1, QUARTER))))', 180),
    ('sply_month_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29))))))', 504),
    ('sply_explicit_Dt', 'CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)))), Dt[Quarter] = 20242)', 28),
    ('feb_calc_sum_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH)))', 26550),
    ('feb_calc_sum_top_Dt', 'CALCULATE([FS], DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] >= DATE(2024, 2, 1) && Dt[Date] <= DATE(2024, 2, 29)), 1, MONTH))', 1475),
    ('one_calc_rows_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(Dt), DATEADD(FILTER(ALL(Dt[Date]), Dt[Date] = DATE(2024, 2, 10)), 1, MONTH)))', 18),
    ('sales_q_iter_Dt', 'SUMX(VALUES(Dt[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), NOT ISBLANK([FS])), -1, QUARTER))))', 816),
    ('sales_q_top_Dt', 'COUNTROWS(DATEADD(FILTER(ALL(Dt[Date]), NOT ISBLANK([FS])), -1, QUARTER))', 136),
    ('sales_sel_q_min_Dt', 'MINX(VALUES(Dt[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), -1, QUARTER))))', 136),
    ('sales_sel_q_max_Dt', 'MAXX(VALUES(Dt[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), -1, QUARTER))))', 136),
    ('mpq_top_Dt', '[MaxPrevQ_Dt]', 20241),
    ('mpq_q_min_Dt', 'MINX(VALUES(Dt[Quarter]), [MaxPrevQ_Dt])', 20241),
    ('mpq_q_max_Dt', 'MAXX(VALUES(Dt[Quarter]), [MaxPrevQ_Dt])', 20241),
    ('mpq_row_min_Dt', 'MINX(Dt, [MaxPrevQ_Dt])', 20241),
    ('mpq_row_max_Dt', 'MAXX(Dt, [MaxPrevQ_Dt])', 20241),
    ('mpq_rows_Dt', 'COUNTROWS(FILTER(Dt, Dt[Quarter] = [MaxPrevQ_Dt]))', 91),
    ('mpq_pq_sum_Dt', 'CALCULATE([FS], FILTER(Dt, Dt[Quarter] = [MaxPrevQ_Dt]))', 4125),
    ('col_month_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH))))', 516),
    ('col_explicit_Dt', 'CALCULATE(COUNTROWS(DATEADD(Dt[Date], 1, MONTH)), Dt[Month] = 202402)', 31),
    ('feb_month_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 558),
    ('feb_quarter_iter_Du', 'SUMX(VALUES(Du[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 186),
    ('feb_weekday_iter_Du', 'SUMX(VALUES(Du[Weekday]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH))))', 217),
    ('jan10_row_iter_Du', 'SUMX(FILTER(Du, Du[Month] = 202403), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 1, 1) && Du[Date] <= DATE(2024, 1, 10)), 1, MONTH))))', 310),
    ('one_month_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] = DATE(2024, 2, 10)), 1, MONTH))))', 18),
    ('one_min_Du', 'MINX(VALUES(Du[Month]), CALCULATE(MINX(DATEADD(FILTER(ALL(Du[Date]), Du[Date] = DATE(2024, 2, 10)), 1, MONTH), Du[Date])))', '2024-03-10T00:00:00'),
    ('feb_explicit_month_Du', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH)), Du[Month] = 202305)', 31),
    ('feb_explicit_weekday_Du', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH)), Du[Weekday] = 3)', 31),
    ('one_explicit_quarter_Du', 'CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] = DATE(2024, 2, 10)), 1, MONTH)), Du[Quarter] = 20231)', 1),
    ('feb_top_Du', 'COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH))', 31),
    ('feb_back_q_top_Du', 'COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), -1, QUARTER))', 30),
    ('feb_back_q_iter_Du', 'SUMX(VALUES(Du[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), -1, QUARTER))))', 180),
    ('sply_month_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29))))))', 504),
    ('sply_explicit_Du', 'CALCULATE(COUNTROWS(SAMEPERIODLASTYEAR(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)))), Du[Quarter] = 20242)', 28),
    ('feb_calc_sum_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH)))', 26550),
    ('feb_calc_sum_top_Du', 'CALCULATE([GS], DATEADD(FILTER(ALL(Du[Date]), Du[Date] >= DATE(2024, 2, 1) && Du[Date] <= DATE(2024, 2, 29)), 1, MONTH))', 1475),
    ('one_calc_rows_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(Du), DATEADD(FILTER(ALL(Du[Date]), Du[Date] = DATE(2024, 2, 10)), 1, MONTH)))', 18),
    ('sales_q_iter_Du', 'SUMX(VALUES(Du[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), NOT ISBLANK([GS])), -1, QUARTER))))', 816),
    ('sales_q_top_Du', 'COUNTROWS(DATEADD(FILTER(ALL(Du[Date]), NOT ISBLANK([GS])), -1, QUARTER))', 136),
    ('sales_sel_q_min_Du', 'MINX(VALUES(Du[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, QUARTER))))', 136),
    ('sales_sel_q_max_Du', 'MAXX(VALUES(Du[Quarter]), CALCULATE(COUNTROWS(DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, QUARTER))))', 136),
    ('mpq_top_Du', '[MaxPrevQ_Du]', 20241),
    ('mpq_q_min_Du', 'MINX(VALUES(Du[Quarter]), [MaxPrevQ_Du])', 20241),
    ('mpq_q_max_Du', 'MAXX(VALUES(Du[Quarter]), [MaxPrevQ_Du])', 20241),
    ('mpq_row_min_Du', 'MINX(Du, [MaxPrevQ_Du])', 20241),
    ('mpq_row_max_Du', 'MAXX(Du, [MaxPrevQ_Du])', 20241),
    ('mpq_rows_Du', 'COUNTROWS(FILTER(Du, Du[Quarter] = [MaxPrevQ_Du]))', 91),
    ('mpq_pq_sum_Du', 'CALCULATE([GS], FILTER(Du, Du[Quarter] = [MaxPrevQ_Du]))', 4125),
    ('col_month_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH))))', 516),
    ('col_explicit_Du', 'CALCULATE(COUNTROWS(DATEADD(Du[Date], 1, MONTH)), Du[Month] = 202402)', 31),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES)
    m["p"] = expr
    got = de.evaluate_measures_smart(["p"], TABLES, m, {}, relationships=RELS,
                                     date_tables={"Dt": "Date"}, simulate_row_context=False)["p"]
    if isinstance(want, str):
        assert got is not None and got.isoformat()[:19] == want[:19]
    elif want is None:
        assert got is None
    else:
        assert got == pytest.approx(want)
