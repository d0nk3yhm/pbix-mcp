"""Issue #137: a row transition on a date column clears the date table's
other filters, as an explicit CALCULATE filter on it does.

A filter on a DateTime column that joins a relationship, or on a marked date
table's date column, clears that table's other filters (#78). The engine
applied it only when CALCULATE wrote the filter. Power BI Desktop 2.152 applies
it to a ROW TRANSITION as well. In SUMX(VALUES(D[Month]), CALCULATE(COUNTROWS(
FILTER(ALL(D[Date]), CALCULATE(COUNTROWS(D)) > 0)))) every date passes in every
month: 273, where the engine counted 91. Awesome Chocolates' QOQ calculation
item filters its 731 dates by [Max Previous Quarter Not Blank], which Desktop
answers 20234 on every row for this reason.

Expected values: Desktop over ADOMD (build_b137.py), one EVALUATE ROW per probe,
over a marked calendar (Dt) and an unmarked one (Du) joined on their date, which
answer alike. Generated from Desktop's output.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(91)]
SALE_DAYS = [d for i, d in enumerate(DAYS) if i % 3 == 0]
TABLES = {
    "Dt": {"columns": ["Date", "Month"], "rows": [[d, d.month] for d in DAYS]},
    "Du": {"columns": ["Date", "Month"], "rows": [[d, d.month] for d in DAYS]},
    "F": {"columns": ["Date", "Amount"],
          "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]},
    "G": {"columns": ["Date", "Amount"],
          "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]},
}
RELS = [{"FromTable": "F", "FromColumn": "Date", "ToTable": "Dt", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1},
        {"FromTable": "G", "FromColumn": "Date", "ToTable": "Du", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {"FS": "SUM(F[Amount])", "GS": "SUM(G[Amount])", "Rows_Dt": "COUNTROWS(Dt)", "Rows_Du": "COUNTROWS(Du)",
            "PrevMonth_Dt": "CALCULATE(MAXX(FILTER(ALLSELECTED(Dt), NOT ISBLANK([FS])), Dt[Month]), "
                            "DATEADD(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), -1, MONTH))",
            "PrevMonth_Du": "CALCULATE(MAXX(FILTER(ALLSELECTED(Du), NOT ISBLANK([GS])), Du[Month]), "
                            "DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, MONTH))"}

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b137.py's output
    ('rows_count_Dt', 'SUMX(FILTER(Dt, Dt[Month] = 2), CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])))))', 261),
    ('rows_maxdate_Dt', 'MAXX(Dt, CALCULATE(MAXX(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), Dt[Date])))', '2024-03-31T00:00:00'),
    ('rows_mindate_Dt', 'MINX(Dt, CALCULATE(MAXX(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), Dt[Date])))', '2024-03-31T00:00:00'),
    ('prev_month_rows_Dt', 'COUNTROWS(FILTER(Dt, Dt[Month] = [PrevMonth_Dt]))', None),
    ('prev_month_top_Dt', '[PrevMonth_Dt]', None),
    ('explicit_date_in_month_iter_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(Dt), Dt[Date] = DATE(2024, 1, 15)))', 3),
    ('month_transition_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(Dt)))', 91),
    ('row_transition_Dt', 'SUMX(FILTER(Dt, Dt[Month] = 2), CALCULATE(COUNTROWS(Dt)))', 29),
    ('date_values_in_month_Dt', 'SUMX(FILTER(Dt, Dt[Month] = 2), CALCULATE(COUNTROWS(VALUES(Dt[Month]))))', 29),
    ('date_iter_month_count_Dt', 'SUMX(FILTER(ALL(Dt[Date]), Dt[Date] <= DATE(2024, 1, 3)), CALCULATE(COUNTROWS(VALUES(Dt[Month]))))', 3),
    ('c_allsel_Dt', 'COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])))', 31),
    ('c_values_Dt', 'COUNTROWS(FILTER(VALUES(Dt[Date]), NOT ISBLANK([FS])))', 31),
    ('c_all_Dt', 'COUNTROWS(FILTER(ALL(Dt[Date]), NOT ISBLANK([FS])))', 31),
    ('c_allsel_tab_Dt', 'COUNTROWS(FILTER(ALLSELECTED(Dt), NOT ISBLANK([FS])))', 31),
    ('c_calc_Dt', 'COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK(CALCULATE([FS]))))', 31),
    ('c_sumx_Dt', 'SUMX(ALLSELECTED(Dt[Date]), IF(ISBLANK([FS]), 0, 1))', 31),
    ('shift_cnt_Dt', 'COUNTROWS(DATEADD(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), -1, MONTH))', 20),
    ('shift_sum_Dt', 'CALCULATE([FS], DATEADD(FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS])), -1, MONTH))', None),
    ('n_values_Dt', 'MINX(Dt, CALCULATE(MAXX(FILTER(VALUES(Dt[Date]), NOT ISBLANK([FS])), Dt[Date])))', '2024-01-01T00:00:00'),
    ('n_all_Dt', 'MINX(Dt, CALCULATE(MAXX(FILTER(ALL(Dt[Date]), NOT ISBLANK([FS])), Dt[Date])))', '2024-03-31T00:00:00'),
    ('n_isf_allsel_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), CALCULATE(ISFILTERED(Dt[Month]))))))', None),
    ('n_isf_all_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALL(Dt[Date]), CALCULATE(ISFILTERED(Dt[Month]))))))', None),
    ('n_isf_nocalc_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), ISFILTERED(Dt[Month])))))', 91),
    ('n_cnt_allsel_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(ALLSELECTED(Dt[Date]))))', 91),
    ('n_mod_isf_Dt', 'MINX(Dt, CALCULATE(CALCULATE(IF(ISFILTERED(Dt[Month]), 1, 0), ALLSELECTED(Dt[Date]))))', 1),
    ('n_mod_cnt_Dt', 'MINX(Dt, CALCULATE(CALCULATE(COUNTROWS(Dt), ALLSELECTED(Dt[Date]))))', 29),
    ('n_month_in_pred_Dt', 'MINX(Dt, CALCULATE(MAXX(FILTER(ALLSELECTED(Dt[Date]), CALCULATE(MAX(Dt[Month])) = 3), Dt[Date])))', '2024-03-31T00:00:00'),
    ('n_values_month_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Dt[Date]), CALCULATE(COUNTROWS(VALUES(Dt[Month]))) = 1))))', 91),
    ('n_sumx_rows_Dt', 'MINX(Dt, CALCULATE(SUMX(ALLSELECTED(Dt[Date]), CALCULATE(COUNTROWS(Dt)))))', 91),
    ('n_values_tab_Dt', 'MINX(Dt, CALCULATE(MAXX(FILTER(ALLSELECTED(Dt), NOT ISBLANK([FS])), Dt[Date])))', '2024-03-31T00:00:00'),
    ('r3_explicit_Dt', 'MINX(Dt, CALCULATE(CALCULATE(COUNTROWS(Dt), Dt[Date] = DATE(2024, 3, 1))))', 1),
    ('r3_explicit_isf_Dt', 'MINX(Dt, CALCULATE(CALCULATE(IF(ISFILTERED(Dt[Month]), 1, 0), Dt[Date] = DATE(2024, 3, 1))))', 0),
    ('r5_month_outer_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(FILTER(ALL(Dt[Date]), CALCULATE(COUNTROWS(Dt)) > 0))))', 273),
    ('r5_month_outer_vals_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(FILTER(VALUES(Dt[Date]), CALCULATE(COUNTROWS(Dt)) > 0))))', 91),
    ('r6_rows_inner_rows_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALL(Dt), CALCULATE(COUNTROWS(Dt)) > 0))))', 91),
    ('r7_explicit_month_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALL(Dt[Date]), CALCULATE(COUNTROWS(Dt)) > 0)), Dt[Month] = 1))', 91),
    ('r8_measure_ref_Dt', 'MINX(Dt, CALCULATE(COUNTROWS(FILTER(ALL(Dt[Date]), NOT ISBLANK([Rows_Dt])))))', 91),
    ('rows_count_Du', 'SUMX(FILTER(Du, Du[Month] = 2), CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])))))', 261),
    ('rows_maxdate_Du', 'MAXX(Du, CALCULATE(MAXX(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), Du[Date])))', '2024-03-31T00:00:00'),
    ('rows_mindate_Du', 'MINX(Du, CALCULATE(MAXX(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), Du[Date])))', '2024-03-31T00:00:00'),
    ('prev_month_rows_Du', 'COUNTROWS(FILTER(Du, Du[Month] = [PrevMonth_Du]))', None),
    ('prev_month_top_Du', '[PrevMonth_Du]', None),
    ('explicit_date_in_month_iter_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(Du), Du[Date] = DATE(2024, 1, 15)))', 3),
    ('month_transition_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(Du)))', 91),
    ('row_transition_Du', 'SUMX(FILTER(Du, Du[Month] = 2), CALCULATE(COUNTROWS(Du)))', 29),
    ('date_values_in_month_Du', 'SUMX(FILTER(Du, Du[Month] = 2), CALCULATE(COUNTROWS(VALUES(Du[Month]))))', 29),
    ('date_iter_month_count_Du', 'SUMX(FILTER(ALL(Du[Date]), Du[Date] <= DATE(2024, 1, 3)), CALCULATE(COUNTROWS(VALUES(Du[Month]))))', 3),
    ('c_allsel_Du', 'COUNTROWS(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])))', 31),
    ('c_values_Du', 'COUNTROWS(FILTER(VALUES(Du[Date]), NOT ISBLANK([GS])))', 31),
    ('c_all_Du', 'COUNTROWS(FILTER(ALL(Du[Date]), NOT ISBLANK([GS])))', 31),
    ('c_allsel_tab_Du', 'COUNTROWS(FILTER(ALLSELECTED(Du), NOT ISBLANK([GS])))', 31),
    ('c_calc_Du', 'COUNTROWS(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK(CALCULATE([GS]))))', 31),
    ('c_sumx_Du', 'SUMX(ALLSELECTED(Du[Date]), IF(ISBLANK([GS]), 0, 1))', 31),
    ('shift_cnt_Du', 'COUNTROWS(DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, MONTH))', 20),
    ('shift_sum_Du', 'CALCULATE([GS], DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, MONTH))', None),
    ('n_values_Du', 'MINX(Du, CALCULATE(MAXX(FILTER(VALUES(Du[Date]), NOT ISBLANK([GS])), Du[Date])))', '2024-01-01T00:00:00'),
    ('n_all_Du', 'MINX(Du, CALCULATE(MAXX(FILTER(ALL(Du[Date]), NOT ISBLANK([GS])), Du[Date])))', '2024-03-31T00:00:00'),
    ('n_isf_allsel_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Du[Date]), CALCULATE(ISFILTERED(Du[Month]))))))', None),
    ('n_isf_all_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALL(Du[Date]), CALCULATE(ISFILTERED(Du[Month]))))))', None),
    ('n_isf_nocalc_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Du[Date]), ISFILTERED(Du[Month])))))', 91),
    ('n_cnt_allsel_Du', 'MINX(Du, CALCULATE(COUNTROWS(ALLSELECTED(Du[Date]))))', 91),
    ('n_mod_isf_Du', 'MINX(Du, CALCULATE(CALCULATE(IF(ISFILTERED(Du[Month]), 1, 0), ALLSELECTED(Du[Date]))))', 1),
    ('n_mod_cnt_Du', 'MINX(Du, CALCULATE(CALCULATE(COUNTROWS(Du), ALLSELECTED(Du[Date]))))', 29),
    ('n_month_in_pred_Du', 'MINX(Du, CALCULATE(MAXX(FILTER(ALLSELECTED(Du[Date]), CALCULATE(MAX(Du[Month])) = 3), Du[Date])))', '2024-03-31T00:00:00'),
    ('n_values_month_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALLSELECTED(Du[Date]), CALCULATE(COUNTROWS(VALUES(Du[Month]))) = 1))))', 91),
    ('n_sumx_rows_Du', 'MINX(Du, CALCULATE(SUMX(ALLSELECTED(Du[Date]), CALCULATE(COUNTROWS(Du)))))', 91),
    ('n_values_tab_Du', 'MINX(Du, CALCULATE(MAXX(FILTER(ALLSELECTED(Du), NOT ISBLANK([GS])), Du[Date])))', '2024-03-31T00:00:00'),
    ('r3_explicit_Du', 'MINX(Du, CALCULATE(CALCULATE(COUNTROWS(Du), Du[Date] = DATE(2024, 3, 1))))', 1),
    ('r3_explicit_isf_Du', 'MINX(Du, CALCULATE(CALCULATE(IF(ISFILTERED(Du[Month]), 1, 0), Du[Date] = DATE(2024, 3, 1))))', 0),
    ('r5_month_outer_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(FILTER(ALL(Du[Date]), CALCULATE(COUNTROWS(Du)) > 0))))', 273),
    ('r5_month_outer_vals_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(FILTER(VALUES(Du[Date]), CALCULATE(COUNTROWS(Du)) > 0))))', 91),
    ('r6_rows_inner_rows_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALL(Du), CALCULATE(COUNTROWS(Du)) > 0))))', 91),
    ('r7_explicit_month_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALL(Du[Date]), CALCULATE(COUNTROWS(Du)) > 0)), Du[Month] = 1))', 91),
    ('r8_measure_ref_Du', 'MINX(Du, CALCULATE(COUNTROWS(FILTER(ALL(Du[Date]), NOT ISBLANK([Rows_Du])))))', 91),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES)
    m["p"] = expr
    got = de.evaluate_measures_smart(["p"], TABLES, m, {}, relationships=RELS,
                                     date_tables={"Dt": "Date"}, simulate_row_context=False)["p"]
    if isinstance(want, str) and want[:4] == "2024":
        assert got is not None and got.isoformat()[:19] == want[:19]
    elif want is None:
        assert got is None
    else:
        assert got == pytest.approx(want)
