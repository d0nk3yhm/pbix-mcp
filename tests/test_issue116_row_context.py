"""Issue #116: a row context filters nothing until CALCULATE or a measure
reference turns it into a filter.

The engine applied an iterator's row to the filter context eagerly (for
CALCULATE and measure references), so everything else inside the iterator saw
only the current row: a table expression over the iterated table, a nested
iterator, VALUES, an aggregate in FILTER's condition, CALCULATE's filter
arguments. Now a row context's data reads come from the context the rows were
opened in (DAXContext._row_root). Time-intelligence functions over a date
column, and RELATEDTABLE, still see the transition: Desktop reads their column
as CALCULATETABLE(DISTINCT(<dates>)). Also:

- EARLIER / EARLIEST walk the row contexts; EARLIER returned the column,
  which compared the row with itself;
- a column of another table reads the innermost row over that table;
- a function that takes a column (VALUES, DISTINCT, COUNT, SELECTEDVALUE, ...)
  takes the column, not the current row's value (_column_arg).

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe,
generated from its output:

- build_b116.py: Orders / Dim. r16, RELATEDTABLE(Dim) over a fact row, is
  #115's (test_issue115_transition_expanded.py);
- build_b143.py: CALCULATE's filter arguments inside an iterator, over a marked
  (Dt) and an unmarked (Du) calendar;
- build_b144.py: the row-context probes.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

ORDERS = {"Dim": {"columns": ["Region", "Zone"], "rows": [["N", "z1"], ["S", "z1"], ["W", "z2"]]},
          "Orders": {"columns": ["Region", "Revenue"], "rows": [["N", 50.0], ["S", 150.0], ["W", 250.0]]}}
ORDERS_RELS = [{"FromTable": "Orders", "FromColumn": "Region", "ToTable": "Dim", "ToColumn": "Region",
                "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(91)]   # Jan..Mar 2024
SALE_DAYS = [d for i, d in enumerate(DAYS) if i % 3 == 0]


def _calendar():
    return {"columns": ["Date", "Month"], "rows": [[d, d.month] for d in DAYS]}


def _fact():
    # two rows per sale day: the fact is the many side, as in the batteries
    return {"columns": ["Date", "Amount"],
            "rows": [[d, float(i + 1) / 2] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}


def _rel(fact, cal):
    return {"FromTable": fact, "FromColumn": "Date", "ToTable": cal, "ToColumn": "Date", "IsActive": True,
            "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


# build_b143.py: two calendars, Dt marked
CALS = {"Dt": _calendar(), "Du": _calendar(), "F": _fact(), "G": _fact()}
CALS_RELS = [_rel("F", "Dt"), _rel("G", "Du")]
CALS_MEASURES = {"FS": "SUM(F[Amount])", "GS": "SUM(G[Amount])",
                 "YTD_Dt": "CALCULATE([FS], DATESYTD(Dt[Date]))", "YTD_Du": "CALCULATE([GS], DATESYTD(Du[Date]))"}
# build_b144.py: Orders / Dim and one marked calendar
BOTH = dict(ORDERS, Dt=_calendar(), F=_fact())
BOTH_RELS = ORDERS_RELS + [_rel("F", "Dt")]
BOTH_MEASURES = {"FS": "SUM(F[Amount])"}

B116 = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b116.py's output
    ('r01', 'SUMX(Orders, COUNTROWS(Orders))', 9),
    ('r02', 'SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Revenue] > 100)))', 6),
    ('r03', 'SUMX(Orders, COUNTROWS(VALUES(Orders[Region])))', 9),
    ('r04', 'SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Revenue] < EARLIER(Orders[Revenue]))))', 3),
    ('r05', 'SUMX(Orders, COUNTROWS(FILTER(ALL(Orders), Orders[Revenue] <= EARLIER(Orders[Revenue]))))', 6),
    ('r06', 'SUMX(Orders, SUMX(Orders, 1))', 9),
    ('r07', 'SUMX(Orders, SUMX(Dim, SUM(Orders[Revenue])))', 4050),
    ('r08', 'SUMX(Orders, SUMX(Dim, CALCULATE(SUM(Orders[Revenue]))))', 450),
    ('r09', 'SUMX(Orders, CALCULATE(SUMX(Orders, 1)))', 3),
    ('r10', 'SUMX(Orders, COUNTROWS(DISTINCT(Orders[Region])))', 9),
    ('r11', 'SUMX(Orders, COUNTROWS(ALL(Orders)))', 9),
    ('r12', 'SUMX(Orders, MAXX(Orders, Orders[Revenue]))', 750),
    ('r13', 'SUMX(Orders, COUNTROWS(TOPN(2, Orders, Orders[Revenue])))', 6),
    ('r14', 'SUMX(Orders, COUNTROWS(ADDCOLUMNS(Orders, "x", 1)))', 9),
    ('r15', 'SUMX(Orders, COUNTROWS(CALCULATETABLE(Orders)))', 3),
    ('r17', 'SUMX(Dim, COUNTROWS(RELATEDTABLE(Orders)))', 3),
    ('r18', 'SUMX(Dim, COUNTROWS(Orders))', 9),
    ('r19', 'SUMX(Dim, COUNTROWS(FILTER(Orders, Orders[Region] = Dim[Region])))', 3),
    ('r20', 'COUNTROWS(FILTER(Orders, COUNTROWS(Orders) = 3))', 3),
    ('r21', 'COUNTROWS(FILTER(Orders, CALCULATE(COUNTROWS(Orders)) = 1))', 3),
    ('r22', 'SUMX(VALUES(Orders[Region]), COUNTROWS(Orders))', 9),
    ('r23', 'SUMX(VALUES(Orders[Region]), CALCULATE(COUNTROWS(Orders)))', 3),
    ('r24', 'SUMX(Orders, IF(HASONEVALUE(Orders[Region]), 1, 0))', 0),
    ('r25', 'SUMX(Orders, CALCULATE(IF(HASONEVALUE(Orders[Region]), 1, 0)))', 3),
    ('r26', 'SUMX(Orders, IF(ISBLANK(SELECTEDVALUE(Orders[Region])), 1, 0))', 3),
    ('r27', 'SUMX(Orders, CALCULATE(IF(ISBLANK(SELECTEDVALUE(Orders[Region])), 1, 0)))', 0),
    ('r28', 'SUMX(Orders, COUNTROWS(FILTER(Dim, Dim[Region] = Orders[Region])))', 3),
    ('r29', 'CALCULATE(SUMX(Orders, COUNTROWS(Orders)), Orders[Revenue] > 100)', 4),
    ('r30', 'SUMX(Orders, VAR t = FILTER(Orders, Orders[Revenue] > 100) RETURN COUNTROWS(t))', 6),
    ('r31', 'AVERAGEX(Orders, COUNTROWS(Orders))', 3),
    ('r32', 'SUMX(Orders, DIVIDE(Orders[Revenue], SUMX(Orders, Orders[Revenue])))', 1),
    ('r33', 'SUMX(Orders, Orders[Revenue] / CALCULATE(SUM(Orders[Revenue]), ALL(Orders)))', 1),
    ('r34', 'SUMX(Orders, RANKX(Orders, Orders[Revenue]))', 6),
    ('r35', 'SUMX(Orders, RANKX(ALL(Orders), Orders[Revenue]))', 6),
    ('r36', 'SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Region] = EARLIER(Orders[Region]))))', 3),
    ('r37', 'SUMX(Orders, SUMX(FILTER(Orders, Orders[Revenue] > EARLIER(Orders[Revenue])), Orders[Revenue]))', 650),
    ('r38', 'SUMX(Orders, COUNTX(Orders, Orders[Region]))', 9),
    ('r39', 'SUMX(Orders, MINX(VALUES(Orders[Region]), CALCULATE(SUM(Orders[Revenue]))))', 450),
    ('r40', 'SUMX(Dim, SUMX(Orders, CALCULATE(COUNTROWS(Dim))))', 9),
]
B143 = [   # generated from build_b143.py's output
    ('ytd_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATESYTD(Dt[Date])))', 772),
    ('ytd_measure_Dt', 'SUMX(VALUES(Dt[Month]), [YTD_Dt])', 772),
    ('ytd_nested_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(CALCULATE([FS], DATESYTD(Dt[Date]))))', 772),
    ('mtd_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], DATESMTD(Dt[Date])))', 496),
    ('prev_month_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], PREVIOUSMONTH(Dt[Date])))', 210),
    ('running_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], FILTER(ALL(Dt[Date]), Dt[Date] <= MAX(Dt[Date]))))', 1488),
    ('bool_max_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], Dt[Date] <= MAX(Dt[Date])))', 1488),
    ('values_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], VALUES(Dt[Date])))', 1488),
    ('filter_values_arg_Dt', 'SUMX(VALUES(Dt[Month]), CALCULATE(COUNTROWS(Dt), FILTER(VALUES(Dt[Date]), TRUE())))', 273),
    ('rows_arg_Dt', 'SUMX(Dt, CALCULATE(COUNTROWS(Dt), FILTER(ALL(Dt[Date]), Dt[Date] <= MAX(Dt[Date]))))', 8281),
    ('month_max_Dt', 'MAXX(VALUES(Dt[Month]), CALCULATE(MAX(Dt[Date]), DATESYTD(Dt[Date])))', '2024-03-31T00:00:00'),
    ('month_min_Dt', 'MINX(VALUES(Dt[Month]), CALCULATE(MAX(Dt[Date]), DATESYTD(Dt[Date])))', '2024-01-31T00:00:00'),
    ('ytd_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATESYTD(Du[Date])))', 772),
    ('ytd_measure_Du', 'SUMX(VALUES(Du[Month]), [YTD_Du])', 772),
    ('ytd_nested_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(CALCULATE([GS], DATESYTD(Du[Date]))))', 772),
    ('mtd_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], DATESMTD(Du[Date])))', 496),
    ('prev_month_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], PREVIOUSMONTH(Du[Date])))', 210),
    ('running_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], FILTER(ALL(Du[Date]), Du[Date] <= MAX(Du[Date]))))', 1488),
    ('bool_max_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], Du[Date] <= MAX(Du[Date])))', 1488),
    ('values_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE([GS], VALUES(Du[Date])))', 1488),
    ('filter_values_arg_Du', 'SUMX(VALUES(Du[Month]), CALCULATE(COUNTROWS(Du), FILTER(VALUES(Du[Date]), TRUE())))', 273),
    ('rows_arg_Du', 'SUMX(Du, CALCULATE(COUNTROWS(Du), FILTER(ALL(Du[Date]), Du[Date] <= MAX(Du[Date]))))', 8281),
    ('month_max_Du', 'MAXX(VALUES(Du[Month]), CALCULATE(MAX(Du[Date]), DATESYTD(Du[Date])))', '2024-03-31T00:00:00'),
    ('month_min_Du', 'MINX(VALUES(Du[Month]), CALCULATE(MAX(Du[Date]), DATESYTD(Du[Date])))', '2024-01-31T00:00:00'),
]
B144 = [   # generated from build_b144.py's output (its row-context probes)
    ('filter_sum_cond', 'COUNTROWS(FILTER(Orders, SUM(Orders[Revenue]) > 90))', 3),
    ('filter_calc_sum_cond', 'COUNTROWS(FILTER(Orders, CALCULATE(SUM(Orders[Revenue])) > 90))', 2),
    ('filter_max_cond', 'COUNTROWS(FILTER(Orders, Orders[Revenue] = MAX(Orders[Revenue])))', 1),
    ('earliest', 'SUMX(Orders, SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Revenue] <= EARLIEST(Orders[Revenue])))))', 18),
    ('earlier_2', 'SUMX(Orders, SUMX(Orders, COUNTROWS(FILTER(Orders, Orders[Revenue] <= EARLIER(Orders[Revenue], 2)))))', 18),
    ('earlier_rank', 'SUMX(Orders, COUNTROWS(FILTER(ALL(Orders), Orders[Revenue] > EARLIER(Orders[Revenue]))) + 1)', 6),
    ('outer_col_sumx', 'SUMX(Dim, SUMX(Orders, IF(Orders[Region] = Dim[Region], Orders[Revenue], 0)))', 450),
    ('outer_col_related', 'SUMX(Dim, SUMX(Orders, IF(RELATED(Dim[Zone]) = Dim[Zone], 1, 0)))', 5),
    ('rowctx_selectedvalue', 'SUMX(Orders, IF(ISBLANK(SELECTEDVALUE(Dim[Zone])), 1, 0))', 3),
    ('rowctx_distinctcount', 'SUMX(Orders, DISTINCTCOUNT(Orders[Region]))', 9),
    ('rowctx_countrows_values_dim', 'SUMX(Orders, COUNTROWS(VALUES(Dim[Zone])))', 6),
    ('rowctx_concatenatex', 'COUNTROWS(FILTER(Orders, LEN(CONCATENATEX(Orders, Orders[Region], "")) = 3))', 3),
    ('calc_transition_values', 'SUMX(Orders, CALCULATE(COUNTROWS(VALUES(Orders[Region]))))', 3),
    ('calc_body_aggregate', 'SUMX(Dim, CALCULATE(SUM(Orders[Revenue])))', 450),
    ('calc_filter_arg_values', 'SUMX(Orders, CALCULATE(SUM(Orders[Revenue]), VALUES(Orders[Region])))', 450),
    ('calc_filter_arg_all', 'SUMX(Orders, CALCULATE(SUM(Orders[Revenue]), ALL(Orders[Revenue])))', 450),
    ('ti_rows_ytd', 'SUMX(Dt, CALCULATE([FS], DATESYTD(Dt[Date])))', 15376),
    ('ti_rows_lastdate', 'SUMX(Dt, CALCULATE(COUNTROWS(Dt), LASTDATE(Dt[Date])))', 91),
    ('ti_rows_nextday', 'SUMX(Dt, CALCULATE(COUNTROWS(Dt), NEXTDAY(Dt[Date])))', 90),
    ('ti_values_month_lastdate', 'SUMX(VALUES(Dt[Month]), CALCULATE([FS], LASTDATE(Dt[Date])))', 42),
    ('ti_rows_firstdate_value', 'MAXX(Dt, FIRSTDATE(Dt[Date]))', '2024-03-31T00:00:00'),
    ('ti_month_endofmonth', 'MINX(VALUES(Dt[Month]), ENDOFMONTH(Dt[Date]))', '2024-01-31T00:00:00'),
]


def _check(got, want):
    if isinstance(want, str):
        assert got is not None and hasattr(got, "isoformat") and got.isoformat()[:19] == want[:19]
    elif want is None:
        assert got is None
    else:
        assert got == pytest.approx(want)


def _eval(expr, tables, measures, rels, date_tables=None):
    m = dict(measures)
    m["p"] = expr
    return de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels,
                                      date_tables=date_tables or {}, simulate_row_context=False)["p"]


@pytest.mark.parametrize("probe,expr,want", B116, ids=[p for p, _e, _w in B116])
def test_b116(probe, expr, want):
    _check(_eval(expr, ORDERS, {}, ORDERS_RELS), want)


@pytest.mark.parametrize("probe,expr,want", B143, ids=[p for p, _e, _w in B143])
def test_b143_filter_arguments(probe, expr, want):
    _check(_eval(expr, CALS, CALS_MEASURES, CALS_RELS, {"Dt": "Date"}), want)


@pytest.mark.parametrize("probe,expr,want", B144, ids=[p for p, _e, _w in B144])
def test_b144(probe, expr, want):
    _check(_eval(expr, BOTH, BOTH_MEASURES, BOTH_RELS, {"Dt": "Date"}), want)
