"""Issue #369: a filter on the date table reaches a table two hops from it.

Shape of Microsoft's 2026 AdventureWorks Sales sample: Cal -> Sales ->
Orders, where Orders has no relationship (and no date column) of its own and
the Orders-Sales relationship filters both ways. Before the fix the date
table's by-column-name route ran first, found nothing on Orders, and the
measure answered its grand total.

Expected values are read off the rows below: 2020 keeps sales lines L1, L2
(orders A, A -> one order) and L3 (order B), so two orders and three lines.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de


def _rel(frm, fc, to, tc, both=False):
    return {"FromTable": frm, "FromColumn": fc, "ToTable": to, "ToColumn": tc, "IsActive": 1,
            "CrossFilteringBehavior": 2 if both else 1, "FromCardinality": 2, "ToCardinality": 1}


TABLES = {
    "Cal": {"columns": ["DateKey", "Date", "Year"],
            "rows": [[1, "2020-03-01", 2020], [2, "2020-09-01", 2020], [3, "2021-03-01", 2021]]},
    "Sales": {"columns": ["LineKey", "DateKey", "Amount"],
              "rows": [["L1", 1, 5.0], ["L2", 2, 7.0], ["L3", 2, 11.0], ["L4", 3, 13.0]]},
    "Orders": {"columns": ["LineKey", "Order"],
               "rows": [["L1", "A"], ["L2", "A"], ["L3", "B"], ["L4", "C"]]},
    # No relationship to anything: only the date table's by-name route reaches it.
    "Loose": {"columns": ["Date", "Amount"], "rows": [["2020-03-01", 1.0], ["2021-03-01", 2.0]]},
}
RELS = [_rel("Sales", "DateKey", "Cal", "DateKey"),
        _rel("Orders", "LineKey", "Sales", "LineKey", both=True)]
MEASURES = {
    "Order Count": "DISTINCTCOUNT(Orders[Order])",
    "Order Lines": "COUNTROWS(Orders)",
    "Amount": "SUM(Sales[Amount])",
    "Loose Amount": "SUM(Loose[Amount])",
}


def _ev(name, fc):
    de._engine.eval_errors.clear()
    return de.evaluate_measures_batch([name], TABLES, dict(MEASURES), fc, relationships=RELS,
                                      date_table="Cal", date_column="Date")[name]


@pytest.mark.parametrize("name,fc,want", [
    ("Amount", {"Cal.Year": [2020]}, 23.0),          # one hop, unchanged
    ("Order Count", {}, 3),
    ("Order Count", {"Cal.Year": [2020]}, 2),        # the bug: answered 3
    ("Order Count", {"Cal.Year": [2021]}, 1),
    ("Order Lines", {"Cal.Year": [2020]}, 3),
    ("Order Count", {"Cal.Year": [1990]}, None),     # empty, not the grand total
])
def test_date_filter_reaches_two_hops(name, fc, want):
    assert _ev(name, fc) == want


def test_with_a_filter_on_the_far_table_too():
    assert _ev("Order Count", {"Cal.Year": [2020], "Orders.Order": ["B"]}) == 1
    assert _ev("Order Count", {"Cal.Year": [2020], "Orders.Order": ["C"]}) is None


def test_a_table_no_path_reaches_keeps_the_date_column_route():
    assert _ev("Loose Amount", {"Cal.Year": [2020]}) == 1.0
