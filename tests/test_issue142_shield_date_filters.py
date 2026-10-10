"""Issue #142: a measure that re-filters a date table through a CALCULATE on its
date column is evaluated once, not once per row of that table.

Awesome Chocolates' [Max Previous Quarter Not Blank] is CALCULATE([Max Quarter
Not Blank], DATEADD(FILTER(ALLSELECTED('dim-Date'[Date]), NOT ISBLANK([Sales
Actual])), -1, QUARTER)), and its QOQ calculation item filters the 731 dates by
it, one row at a time. Its value cannot depend on the row:

- ALLSELECTED(D[Date]) restores the iteration's dates (#118);
- each date's transition clears the date table's other filters (#137);
- DATEADD of a table shifts the table's own dates (#138, #141);
- the CALCULATE filter on the date column clears the rest (#78).

The #131 shield proved this only for ALL / ALLSELECTED of a whole table, so the
memo missed on every row: under QOQ each of Sales Previous, Sales Change, Sales
CF and Sales color took 70-110 s, where Desktop answers at once.

The shield's proof now also accepts a CALCULATE whose one filter is dates of
such a column: FILTER over ALL / ALLSELECTED(D[Date]) with a condition that
reads the table only through the row, or DATEADD / SAMEPERIODLASTYEAR of that.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2023, 10, 1) + timedelta(days=i) for i in range(183)]   # 2023-10-01 .. 2024-03-31
# a sale every day through 2024-02-15, so a quarter back lands on sale days too
SALE_DAYS = [d for d in DAYS if d <= datetime(2024, 2, 15)]


def _calendar():
    return {"columns": ["Date", "Month", "Quarter"],
            "rows": [[d, d.year * 100 + d.month, d.year * 10 + (d.month - 1) // 3 + 1] for d in DAYS]}


def _fact():
    return {"columns": ["Date", "Amount"],
            "rows": [[d, float(i + 1)] for i, d in enumerate(SALE_DAYS) for _half in (0, 1)]}


# Dt: marked. Du: unmarked, joined on its DateTime column. Dn: unmarked and
# unrelated, so a filter on its date column clears nothing (#78).
TABLES = {"Dt": _calendar(), "Du": _calendar(), "Dn": _calendar(), "F": _fact(), "G": _fact()}
RELS = [{"FromTable": "F", "FromColumn": "Date", "ToTable": "Dt", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1},
        {"FromTable": "G", "FromColumn": "Date", "ToTable": "Du", "ToColumn": "Date", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
DATE_TABLES = {"Dt": "Date"}
SALES = "FILTER(ALLSELECTED(Dt[Date]), NOT ISBLANK([FS]))"
MEASURES = {
    "FS": "SUM(F[Amount])",
    "GS": "SUM(G[Amount])",
    "MaxQ": "MAXX(FILTER(ALLSELECTED(Dt), NOT ISBLANK([FS])), Dt[Quarter])",
    "MaxPrevQ": f"CALCULATE([MaxQ], DATEADD({SALES}, -1, QUARTER))",
    "MaxPrevM": f"CALCULATE([MaxQ], DATEADD({SALES}, -1, MONTH))",
    "SalesLY": f"CALCULATE([FS], SAMEPERIODLASTYEAR({SALES}))",
    "SalesDays": "CALCULATE([FS], FILTER(ALL(Dt[Date]), NOT ISBLANK([FS])))",
    "MaxPrevQ_Du": "CALCULATE(MAXX(FILTER(ALLSELECTED(Du), NOT ISBLANK([GS])), Du[Quarter]), "
                   "DATEADD(FILTER(ALLSELECTED(Du[Date]), NOT ISBLANK([GS])), -1, QUARTER))",
    # each of these reads a filter on the table, so none may be shielded
    "Modifier": "CALCULATE([FS], ALL(Dt[Date]))",
    "ColumnShift": "CALCULATE([FS], DATEADD(Dt[Date], -1, MONTH))",
    "RunningTotal": "CALCULATE([FS], FILTER(ALL(Dt[Date]), Dt[Date] <= MAX(Dt[Date])))",
    "OtherColumn": "CALCULATE([FS], FILTER(ALL(Dt[Date]), Dt[Month] = 202401))",
    "Keep": "CALCULATE([FS], KEEPFILTERS(FILTER(ALL(Dt[Date]), NOT ISBLANK([FS]))))",
    "TwoFilters": f"CALCULATE([MaxQ], DATEADD({SALES}, -1, QUARTER), Dt[Month] = 202401)",
    "Unrelated": "CALCULATE(COUNTROWS(Dn), DATEADD(FILTER(ALLSELECTED(Dn[Date]), Dn[Date] > DATE(2024, 1, 1)), "
                 "-1, MONTH))",
}
SHIELDED = {"MaxQ": {"Dt"}, "MaxPrevQ": {"Dt"}, "MaxPrevM": {"Dt"}, "SalesLY": {"Dt"}, "SalesDays": {"Dt"},
            "MaxPrevQ_Du": {"Du"},
            # issue #166: an aggregation reads no filter on a table that cannot
            # reach its own -- F's sum sees nothing of Dn, Du or G (Dt, the
            # date table, is never claimed: its filters reach a table without
            # a relationship through its date column)
            "FS": {"Dn", "Du", "G"}, "GS": {"Dn", "F"}}
# per date row: the shapes the QOQ item uses
PROBES = {
    "rows_q": "COUNTROWS(FILTER(Dt, Dt[Quarter] = [MaxPrevQ]))",
    "min_q": "MINX(Dt, [MaxPrevQ])",
    "max_m": "MAXX(Dt, [MaxPrevM])",
    "sum_pq": "CALCULATE([FS], FILTER(Dt, Dt[Quarter] = [MaxPrevQ]))",
    "sum_ly": "SUMX(Dt, [SalesLY])",
    "sum_days": "SUMX(VALUES(Dt[Month]), [SalesDays])",
    "rows_q_du": "COUNTROWS(FILTER(Du, Du[Quarter] = [MaxPrevQ_Du]))",
    "min_keep": "MINX(Dt, [Keep])",
    "sum_two": "SUMX(VALUES(Dt[Month]), [TwoFilters])",
}


def _eval(names, fc=None, shield=True, monkeypatch=None):
    measures = dict(MEASURES, **PROBES)
    if not shield:
        monkeypatch.setattr(de.DAXEngine, "_shielded_keys", lambda self, n, c: {})
    try:
        return de.evaluate_measures_smart(list(names), TABLES, measures, fc or {}, relationships=RELS,
                                          date_tables=DATE_TABLES, simulate_row_context=False)
    finally:
        if not shield:
            monkeypatch.undo()


@pytest.mark.parametrize("name", sorted(MEASURES))
def test_shielded_only_where_provable(name, monkeypatch):
    eng = de._engine._get()      # the thread's engine behind the module proxy
    monkeypatch.setattr(eng, "_date_tables", dict(DATE_TABLES), raising=False)
    monkeypatch.setattr(eng, "_clearing_cache", None, raising=False)
    got = eng._shielded_tables(name, de.DAXContext(TABLES, MEASURES, relationships=RELS))
    assert got == frozenset(SHIELDED.get(name, set()))


SLICERS = [None, {"Dt.Month": [202401]}, {"Dt.Quarter": [20241]}, {"F.Amount": [1.0, 4.0, 9.0]}]


@pytest.mark.parametrize("fc", SLICERS, ids=["none", "month", "quarter", "amount"])
def test_values_unchanged_by_the_shield(fc, monkeypatch):
    names = sorted(PROBES) + sorted(MEASURES)
    assert _eval(names, fc) == _eval(names, fc, shield=False, monkeypatch=monkeypatch)


def test_previous_quarter_values():
    got = _eval(["MaxPrevQ", "rows_q", "min_q"])
    # sales run through 2024-02-15 (20241); a quarter back, the last quarter with sales is 20234
    assert got["MaxPrevQ"] == 20234
    assert got["min_q"] == 20234
    assert got["rows_q"] == 92          # the days of 2023 Q4


def test_evaluated_once_not_once_per_row(monkeypatch):
    calls = {"n": 0}
    real = de.DAXEngine._eval_expr
    body = MEASURES["MaxPrevQ"]

    def counting(self, expr, ctx, var_scope=None):
        if expr == body:           # evaluate_measure running [MaxPrevQ]'s body
            calls["n"] += 1
        return real(self, expr, ctx, var_scope)

    monkeypatch.setattr(de.DAXEngine, "_eval_expr", counting)
    assert _eval(["rows_q"])["rows_q"] == 92
    shielded = calls["n"]
    calls["n"] = 0
    monkeypatch.setattr(de.DAXEngine, "_shielded_keys", lambda self, n, c: {})
    assert _eval(["rows_q"])["rows_q"] == 92
    # once, instead of once per row of the FILTER over Dt
    assert shielded == 1
    assert calls["n"] == len(DAYS)


CHOC = os.path.join(os.path.dirname(__file__), "..", "test_samples", "temp_dl", "Full Dashboards",
                    "Performance Dashboard (Using Calculation Groups) - Awesome Chocolates Chandoo.org",
                    "Awesome Chocolates - Performance Dashboard - Chandoo.org.pbix")
CHOC_DESKTOP = {   # Power BI Desktop 2.152 over ADOMD, SUMMARIZECOLUMNS('Time Intelligence'[Time], ...)
    "WOW": {"Sales Previous": 671262.75, "Sales Change": -0.183814385648541, "Sales CF": "#ff383f",
            "Sales color": "#D60007", "Max Previous Quarter Not Blank": 20234},
    "MOM": {"Sales Previous": 2833089.75, "Sales Change": -0.107646342654694, "Sales CF": "#ff383f",
            "Sales color": "#D60007", "Max Previous Quarter Not Blank": 20234},
    "QOQ": {"Sales Previous": 8063111.25, "Sales Change": -0.335094409121541, "Sales CF": "#ff383f",
            "Sales color": "#D60007", "Max Previous Quarter Not Blank": 20233},
}


@pytest.mark.skipif(not os.path.exists(CHOC), reason="community corpus file not present")
@pytest.mark.parametrize("item", sorted(CHOC_DESKTOP))
def test_awesome_chocolates_items(item):
    import json
    import warnings

    from pbix_mcp import server as S
    warnings.simplefilter("ignore")
    alias = "choc142" + item
    assert json.loads(S.pbix_open(CHOC, alias))["success"]
    try:
        ctx = S._get_dax_context(alias)
        want = CHOC_DESKTOP[item]
        got = de.evaluate_measures_smart(
            list(want), ctx["tables"], ctx["measure_defs"], {"Time Intelligence.Time": [item]},
            ctx["date_table"], ctx["date_column"], ctx["relationships"], simulate_row_context=False,
            measure_tables=ctx.get("measure_tables"), model_columns=ctx.get("model_columns"),
            date_tables=ctx.get("date_tables"))
    finally:
        S.pbix_close(alias)
    for m, w in want.items():
        assert got[m] == (w if isinstance(w, str) else pytest.approx(w)), m
