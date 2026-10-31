"""A filter on the date table reaches a table two hops away.

Date -> Fact -> Lines, where Lines has no relationship of its own to Date and the
Fact-Lines relationship filters both ways. Reading the rows by hand: 2018 keeps
fact rows 1 and 2, so lines a and b; the answers below are counted from TABLES.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de


def _rel(frm, fc, to, tc, both=False, active=1):
    return {"FromTable": frm, "FromColumn": fc, "ToTable": to, "ToColumn": tc, "IsActive": active,
            "CrossFilteringBehavior": 2 if both else 1, "FromCardinality": 2, "ToCardinality": 1}


TABLES = {
    "Cal": {"columns": ["Key", "Date", "Year"], "rows": [[1, "2018-01-01", 2018], [2, "2019-01-01", 2019]]},
    "Fact": {"columns": ["Line", "CalKey", "Amt"], "rows": [["a", 1, 10.0], ["b", 1, 20.0], ["c", 2, 40.0]]},
    "Lines": {"columns": ["Line", "Order"], "rows": [["a", "o1"], ["b", "o2"], ["c", "o3"]]},
    "Other": {"columns": ["Date", "Amt"], "rows": [["2018-01-01", 1.0], ["2019-01-01", 2.0]]},
}
RELS = [_rel("Fact", "CalKey", "Cal", "Key"), _rel("Lines", "Line", "Fact", "Line", both=True)]
MEASURES = {"Orders": "DISTINCTCOUNT(Lines[Order])", "Amt": "SUM(Fact[Amt])", "Rows": "COUNTROWS(Lines)",
            "OtherAmt": "SUM(Other[Amt])",
            "AllOrders": "CALCULATE(DISTINCTCOUNT(Lines[Order]), ALL(Lines))"}


def _eval(measure, fc):
    de._engine.eval_errors.clear()
    return de.evaluate_measures_batch([measure], TABLES, dict(MEASURES), fc, relationships=RELS,
                                      date_table="Cal", date_column="Date")[measure]


@pytest.mark.parametrize("measure,fc,want", [
    ("Amt", {"Cal.Year": [2018]}, 30.0),         # one hop: the route that always worked
    ("Orders", {}, 3),
    ("Orders", {"Cal.Year": [2018]}, 2),         # two hops, the bug: answered 3
    ("Orders", {"Cal.Year": [2019]}, 1),
    ("Rows", {"Cal.Year": [2018]}, 2),
    ("Orders", {"Cal.Year": [1999]}, None),      # an empty selection is empty, not the grand total
])
def test_the_date_table_reaches_two_hops(measure, fc, want):
    assert _eval(measure, fc) == want


def test_an_unrelated_table_keeps_the_date_column_fallback():
    # No path from Cal to Other: the old route by column name still answers, unchanged.
    assert _eval("OtherAmt", {"Cal.Year": [2018]}) == 1.0


def test_a_column_filter_on_the_two_hop_table_survives_with_the_date_filter():
    assert _eval("Orders", {"Cal.Year": [2018], "Lines.Order": ["o2"]}) == 1
    assert _eval("Orders", {"Cal.Year": [2018], "Lines.Order": ["o3"]}) is None
