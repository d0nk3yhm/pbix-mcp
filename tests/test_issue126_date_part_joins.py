"""Issue #126: a fact DateTime with a time of day joins a date-only calendar
by its DATE PART across a JoinOnDateBehavior = DatePartOnly relationship (what
Power BI writes for auto date/time), and by its exact value otherwise -- in
the per-value path and in evaluate_per_dimension alike.

The per-dimension path bucketed by the exact value (a 12:30 departure found
no row: BLANK for that month), while the per-value path matched date parts on
EVERY relationship. Expected values: Power BI Desktop 2.152 over ADOMD:
  * DateAndTime: only the midnight departure joins (February 1), the other
    five go to the blank member;
  * DatePartOnly, set on the same relationship by Desktop's own TOM and
    recalculated: January 3, February 2, the blank member 1 (a departure past
    the calendar), 12 January 2.
On the Desktop-authored Briqlab file (auto date/time) all 300 departures,
every one with a time of day, land on their months.
"""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DEPS = [(1, dt.datetime(2024, 1, 12, 12, 30)), (2, dt.datetime(2024, 1, 15, 11, 15)),
        (3, dt.datetime(2024, 2, 3, 0, 0)), (4, dt.datetime(2024, 2, 9, 18, 45)),
        (5, dt.datetime(2024, 1, 12, 23, 59, 59)), (6, dt.datetime(2024, 3, 9, 8, 0))]
CAL = [dt.datetime(2024, 1, 1) + dt.timedelta(days=i) for i in range(60)]


def _model(cal_name, jod):
    tables = {cal_name: {"columns": ["Date", "Month"], "rows": [[d, d.strftime("%B")] for d in CAL]},
              "Flights": {"columns": ["Flight_ID", "Dep"], "rows": [[i, d] for i, d in DEPS]}}
    rel = {"FromTable": "Flights", "FromColumn": "Dep", "ToTable": cal_name, "ToColumn": "Date",
           "IsActive": 1, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}
    if jod is not None:
        rel["JoinOnDateBehavior"] = jod
    measures = {"n": "COUNT(Flights[Flight_ID])",
                "n_jan12": f"CALCULATE([n], {cal_name}[Date] = DATE(2024,1,12))",
                "n_blank": f"CALCULATE([n], ISBLANK({cal_name}[Date]))"}
    return tables, [rel], measures


CASES = [   # (calendar, JoinOnDateBehavior, Desktop: January, February, 12 Jan, blank member)
    ("Cal", 1, None, 1, None, 5),                         # exact; 0.9.121: January 3 (date part)
    ("Cal", 2, 3, 2, 2, 1),                               # date part; 0.9.121 per-dimension: BLANK
    ("LocalDateTable_x", None, 3, 2, 2, 1),               # auto date/time, behaviour not given
]


@pytest.mark.parametrize("cal,jod,jan,feb,jan12,blank", CASES, ids=["DateAndTime", "DatePartOnly", "auto"])
def test_per_value_path(cal, jod, jan, feb, jan12, blank):
    tables, rels, measures = _model(cal, jod)
    by_month = {m: de.evaluate_measures_smart(["n"], tables, measures, {f"{cal}.Month": [m]}, relationships=rels,
                                              simulate_row_context=False, group_by={f"{cal}.Month"},
                                              selected_filters={})["n"] for m in ("January", "February")}
    totals = de.evaluate_measures_smart(["n_jan12", "n_blank"], tables, measures, {}, relationships=rels,
                                        simulate_row_context=False)
    assert (by_month["January"], by_month["February"], totals["n_jan12"], totals["n_blank"]) == \
        (jan, feb, jan12, blank)


@pytest.mark.parametrize("cal,jod,jan,feb,jan12,blank", CASES, ids=["DateAndTime", "DatePartOnly", "auto"])
def test_per_dimension_path_agrees(cal, jod, jan, feb, jan12, blank):
    tables, rels, measures = _model(cal, jod)
    got = de.evaluate_per_dimension(["n"], tables, measures, None, f"{cal}.Month", cal, "Month",
                                    ["January", "February"], relationships=rels)
    assert (got["n"]["January"], got["n"]["February"]) == (jan, feb)


def test_openbi_repro_auto_date_time():
    """OpenBI doc 55's repro: a LocalDateTable relationship given without its
    JoinOnDateBehavior -- Desktop writes DatePartOnly on every one."""
    days = [dt.datetime(2024, 1, 1) + dt.timedelta(days=i) for i in range(60)]
    tables = {"Flights": {"columns": ["Flight_ID", "Scheduled_Dep"],
                          "rows": [[1, dt.datetime(2024, 1, 12, 12, 30)], [2, dt.datetime(2024, 1, 15, 11, 15)],
                                   [3, dt.datetime(2024, 2, 3, 0, 0)], [4, dt.datetime(2024, 2, 9, 18, 45)]]},
              "LocalDateTable_x": {"columns": ["Date", "Month"], "rows": [[d, d.strftime("%B")] for d in days]}}
    rels = [{"FromTable": "Flights", "FromColumn": "Scheduled_Dep", "ToTable": "LocalDateTable_x",
             "ToColumn": "Date", "IsActive": 1, "CrossFilteringBehavior": 1}]
    measures = {"Total Flights": "COUNT(Flights[Flight_ID])"}
    got = de.evaluate_per_dimension(["Total Flights"], tables, measures, None, "LocalDateTable_x.Month",
                                    "LocalDateTable_x", "Month", ["January", "February"], relationships=rels)
    assert got == {"Total Flights": {"January": 2, "February": 2}}     # 0.9.121: January BLANK
