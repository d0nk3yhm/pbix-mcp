"""RELATED() inside an iterator is a row-context navigation, not a
filter-context lookup (pbix-mcp findings #20).

Regression: ``SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))`` returned
839.72 (= SUM(Qty) x the FIRST Products row's price) instead of 599.72,
because RELATED resolved to the first *visible* related row regardless of
which row the iterator was on. The probe matrix below is arithmetic ground
truth over a 3-row model; P6 is the decisive case (a single-row iteration
must yield that row's related value, not the first table row).
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

_TABLES = {
    "Sales": {"columns": ["OrderID", "ProductID", "Qty"],
              "rows": [[1001, 1, 5], [1002, 2, 3], [1003, 3, 20]]},
    "Products": {"columns": ["ProductID", "ProductName", "UnitPrice"],
                 "rows": [[1, "Widget A", 29.99], [2, "Widget B", 49.99],
                          [3, "Gadget X", 14.99]]},
}
_RELS = [{"FromTable": "Sales", "FromColumn": "ProductID",
          "ToTable": "Products", "ToColumn": "ProductID", "IsActive": 1}]


def _ev(expr: str):
    ctx = de.DAXContext(
        {k: {"columns": v["columns"], "rows": [list(r) for r in v["rows"]]}
         for k, v in _TABLES.items()},
        {"__p__": expr}, None, None, None, [dict(r) for r in _RELS])
    return de.DAXEngine().evaluate_measure("__p__", ctx)


@pytest.mark.parametrize("expr,expected", [
    ("SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))", 599.72),
    ("SUMX(Sales, Sales[Qty] * Sales[Qty])", 434),
    ("SUMX(Sales, RELATED(Products[UnitPrice]))", 94.97),
    ("SUMX(Sales, 1)", 3),
    ("AVERAGEX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))", 199.906667),
    ("SUMX(FILTER(Sales, Sales[OrderID] = 1003), RELATED(Products[UnitPrice]))",
     14.99),
    ("SUMX(FILTER(Sales, Sales[OrderID] = 1002), RELATED(Products[UnitPrice]))",
     49.99),
    ("SUMX(FILTER(Sales, Sales[OrderID] >= 1002), "
     "RELATED(Products[UnitPrice]))", 64.98),
])
def test_related_probe_matrix(expr, expected):
    got = _ev(expr)
    assert isinstance(got, (int, float)) and abs(got - expected) < 1e-4, \
        f"{expr}: got {got!r}, expected {expected}"


def test_multihop_related():
    """RELATED follows a chain (Sales -> Products -> Category)."""
    tables = {
        "Sales": {"columns": ["ProductID"], "rows": [[1], [2]]},
        "Products": {"columns": ["ProductID", "CategoryID"],
                     "rows": [[1, 10], [2, 20]]},
        "Category": {"columns": ["CategoryID", "Name"],
                     "rows": [[10, "Bikes"], [20, "Parts"]]},
    }
    rels = [
        {"FromTable": "Sales", "FromColumn": "ProductID",
         "ToTable": "Products", "ToColumn": "ProductID", "IsActive": 1},
        {"FromTable": "Products", "FromColumn": "CategoryID",
         "ToTable": "Category", "ToColumn": "CategoryID", "IsActive": 1},
    ]
    ctx = de.DAXContext(
        {k: {"columns": v["columns"], "rows": [list(r) for r in v["rows"]]}
         for k, v in tables.items()},
        {"__p__": 'COUNTROWS(FILTER(Sales, RELATED(Category[Name]) = "Bikes"))'},
        None, None, None, rels)
    assert de.DAXEngine().evaluate_measure("__p__", ctx) == 1
