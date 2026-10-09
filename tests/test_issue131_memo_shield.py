"""Issue #131: a measure that re-filters a table by whole rows is evaluated once,
not once per row of that table.

"The last week with sales", MAXX(FILTER(ALLSELECTED('Date'), NOT
ISBLANK([Sales])), 'Date'[Week]), cannot see any filter on 'Date':
ALLSELECTED restores the selection, and every [Sales] inside runs under the
transition of a whole 'Date' row. Reports call it once per date, as in
FILTER(ALLSELECTED('Date'), 'Date'[Week] = [Max Week]). The measure memo,
keyed on the whole filter context, missed on every row. Once ALLSELECTED(<table>)
returned its rows (#120), Awesome Chocolates' "Selection max date" (731 dates)
ran out of time; Power BI Desktop 2.152 answers 2024-02-26 at once.

The memo key now leaves out the filters on such a table. The proof is
syntactic and conservative: a top-level aggregate, a column read outside the
row, VALUES, or anything else it does not recognise keeps the full key.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(100)]
TABLES = {
    "Date": {"columns": ["Date", "Week", "Month"],
             "rows": [[d, i // 7 + 1, d.month] for i, d in enumerate(DAYS)]},
    "Sales": {"columns": ["Date", "Amount"],
              "rows": [[d, float(10 + i)] for i, d in enumerate(DAYS) if i % 3 == 0 and i <= 66]},
}
RELS = [{"FromTable": "Sales", "FromColumn": "Date", "ToTable": "Date", "ToColumn": "Date",
         "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {
    "Sales": "SUM(Sales[Amount])",
    "Max Week": "MAXX(FILTER(ALLSELECTED('Date'), NOT ISBLANK([Sales])), 'Date'[Week])",
    "Max Week V": "VAR w = MAXX(FILTER(ALLSELECTED('Date'), NOT ISBLANK([Sales])), 'Date'[Week]) RETURN w",
    "Max Week All": "MAXX(FILTER(ALL('Date'), [Sales] > 0), 'Date'[Week])",
    "Days In Max Week": "COUNTROWS(FILTER(ALLSELECTED('Date'), 'Date'[Week] = [Max Week]))",
    "Last Day": "CALCULATE(MAX('Date'[Date]), FILTER(ALLSELECTED('Date'), "
                "NOT ISBLANK([Sales]) && 'Date'[Week] = [Max Week]))",
    # each of these reads the 'Date' filters, so none may be shielded
    "Plus Rows": "[Max Week] + COUNTROWS('Date')",
    "Agg In Pred": "MAXX(FILTER(ALLSELECTED('Date'), [Sales] > SUM(Sales[Amount]) / 10), 'Date'[Week])",
    "Visible": "MAXX(FILTER(VALUES('Date'[Week]), NOT ISBLANK([Sales])), 'Date'[Week])",
    "Col Outside": "MAXX(FILTER(ALLSELECTED('Date'), NOT ISBLANK([Sales])), 'Date'[Week]) + MAX('Date'[Month])",
    # evaluated once per date row
    "Per Day": "SUMX(VALUES('Date'[Date]), [Max Week])",
    "Per Day Last": "MAXX(VALUES('Date'[Date]), [Last Day])",
    "Per Day Plus": "SUMX(VALUES('Date'[Date]), [Plus Rows])",
    "Per Day Agg": "SUMX(VALUES('Date'[Date]), [Agg In Pred])",
}
SHIELDED = {"Max Week", "Max Week V", "Max Week All", "Days In Max Week"}
LAST_SALE = max(r[0] for r in TABLES["Sales"]["rows"])
MAX_WEEK = (LAST_SALE - DAYS[0]).days // 7 + 1


def _ctx():
    c = de.DAXContext(TABLES, MEASURES, relationships=RELS)
    return c


def _eval(names, fc=None, shield=True, monkeypatch=None):
    if not shield:
        monkeypatch.setattr(de.DAXEngine, "_shielded_keys", lambda self, n, c: frozenset())
    try:
        return de.evaluate_measures_batch(list(names), TABLES, MEASURES, fc or {}, relationships=RELS)
    finally:
        if not shield:
            monkeypatch.undo()


@pytest.mark.parametrize("name", sorted(MEASURES))
def test_shielded_only_where_provable(name):
    got = de._engine._shielded_tables(name, _ctx())
    assert got == (frozenset({"Date"}) if name in SHIELDED else frozenset())


SLICERS = [None, {"Date.Month": [2]}, {"Date.Week": [3, 4]}, {"Sales.Amount": [10.0, 13.0]}]


@pytest.mark.parametrize("fc", SLICERS, ids=["none", "month", "weeks", "amount"])
def test_values_unchanged_by_the_shield(fc, monkeypatch):
    with_shield = _eval(MEASURES, fc)
    without = _eval(MEASURES, fc, shield=False, monkeypatch=monkeypatch)
    assert with_shield == without


def test_last_week_values():
    got = _eval(["Max Week", "Last Day", "Days In Max Week", "Per Day"])
    assert got["Max Week"] == MAX_WEEK
    assert got["Last Day"] == LAST_SALE
    assert got["Days In Max Week"] == 7
    assert got["Per Day"] == MAX_WEEK * len(DAYS)


def test_evaluated_once_not_once_per_row(monkeypatch):
    calls = {"n": 0}
    real = de.DAXEngine._eval_expr
    body = MEASURES["Max Week"]

    def counting(self, expr, ctx, var_scope=None):
        if expr == body:           # evaluate_measure running [Max Week]'s body
            calls["n"] += 1
        return real(self, expr, ctx, var_scope)

    monkeypatch.setattr(de.DAXEngine, "_eval_expr", counting)
    assert _eval(["Last Day"]) == {"Last Day": LAST_SALE}
    shielded = calls["n"]
    calls["n"] = 0
    monkeypatch.setattr(de.DAXEngine, "_shielded_keys", lambda self, n, c: frozenset())
    assert _eval(["Last Day"]) == {"Last Day": LAST_SALE}
    # once, instead of once per row of [Last Day]'s FILTER
    assert shielded == 1
    assert calls["n"] == len(DAYS)


CHOC = os.path.join(os.path.dirname(__file__), "..", "test_samples", "temp_dl", "Full Dashboards",
                    "Performance Dashboard (Using Calculation Groups) - Awesome Chocolates Chandoo.org",
                    "Awesome Chocolates - Performance Dashboard - Chandoo.org.pbix")
CHOC_DESKTOP = {   # Power BI Desktop 2.152 over ADOMD, EVALUATE ROW("v", [m])
    "Selection max date": datetime(2024, 2, 26),
    "Selection Sales Calculation": 34042511.25,
    "Max Week Not Blank": 202409,
    "Max Month Not Blank": 202402,
    "Max Quarter Not Blank": 20241,
}


@pytest.mark.skipif(not os.path.exists(CHOC), reason="community corpus file not present")
def test_awesome_chocolates_selection_measures():
    import json
    import warnings

    from pbix_mcp import server as S
    warnings.simplefilter("ignore")
    assert json.loads(S.pbix_open(CHOC, "choc131"))["success"]
    try:
        ctx = S._get_dax_context("choc131")
        got = de.evaluate_measures_smart(
            list(CHOC_DESKTOP), ctx["tables"], ctx["measure_defs"], {}, ctx["date_table"],
            ctx["date_column"], ctx["relationships"], simulate_row_context=False,
            measure_tables=ctx.get("measure_tables"), model_columns=ctx.get("model_columns"),
            date_tables=ctx.get("date_tables"))
    finally:
        S.pbix_close("choc131")
    for m, want in CHOC_DESKTOP.items():
        assert got[m] == want, m
