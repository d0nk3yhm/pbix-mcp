"""Issue #132: a column whose name begins or ends with a space is found.

Microsoft's Financial Sample has a column called ' Sales' (leading space), and
its reports read it as financials[ Sales]. Every parse of a reference strips
the bracketed name, and the column lookup then matched 'Sales' against
' Sales' exactly, so it found nothing. SUM(financials[ Sales]) was BLANK, and so
was everything built on it. Two community reports in the corpus were affected:
Arrow Chart had 3 of 49 measures matching Desktop, and Target Line Bar Charts
2 of 11. Expected values: Power BI Desktop 2.152 over ADOMD.
"""
from __future__ import annotations

import os

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"financials": {"columns": ["Product", " Sales", "Units "],
                         "rows": [["A", 10.0, 1], ["B", 20.0, 2], ["A", 5.0, 3]]}}
MEASURES = {
    "Sales": "SUM( financials[ Sales] )",
    "Sales q": "SUM('financials'[ Sales])",
    "Units": "SUM(financials[Units ])",
    "Max": "MAX(financials[ Sales])",
    "Avg": "AVERAGE(financials[ Sales])",
    "Count": "COUNT(financials[ Sales])",
    "Distinct": "DISTINCTCOUNT(financials[ Sales])",
    "Sumx": "SUMX(financials, financials[ Sales] * financials[Units ])",
    "Filtered": "CALCULATE(SUM(financials[ Sales]), financials[ Sales] > 6)",
    "Filter rows": "COUNTROWS(FILTER(financials, financials[ Sales] > 6))",
    "Values": "COUNTROWS(VALUES(financials[ Sales]))",
    "A only": 'CALCULATE([Sales], financials[Product] = "A")',
}
EXPECTED = {"Sales": 35.0, "Sales q": 35.0, "Units": 6, "Max": 20.0, "Avg": 35.0 / 3, "Count": 3,
            "Distinct": 3, "Sumx": 10.0 + 40.0 + 15.0, "Filtered": 30.0, "Filter rows": 2, "Values": 3,
            "A only": 15.0}


@pytest.mark.parametrize("name", sorted(MEASURES))
def test_space_named_columns(name):
    got = de.evaluate_measures_batch([name], TABLES, MEASURES, {},
                                     model_columns={"financials": TABLES["financials"]["columns"]})
    assert got[name] == pytest.approx(EXPECTED[name])


def test_a_slicer_on_a_space_named_column():
    got = de.evaluate_measures_batch(["Sales"], TABLES, MEASURES, {"financials. Sales": [20.0]})
    assert got["Sales"] == 20.0


ARROW = os.path.join(os.path.dirname(__file__), "..", "test_samples", "temp_dl", "Native Visuals",
                     "Visualizing Change with Arrow Charts", "Arrow Chart - Microsot Financial Dataset.pbix")
ARROW_DESKTOP = {   # Power BI Desktop 2.152 over ADOMD, EVALUATE ROW("v", [m])
    "Sales": 118726350.26, "Current Year": 2014, "Current Month": 12, "PY Sales": 26415255.51,
    "MTD Sales": 11998787.9, "YTD Sales": 92311094.7500001, "YOY % Sales": 3.49461297904364,
    "QOQ Sales": 29758822.02, "MOM % Sales": 0.112424453765066,
}


@pytest.mark.skipif(not os.path.exists(ARROW), reason="community corpus file not present")
def test_arrow_chart_financial_sample():
    import json
    import warnings

    from pbix_mcp import server as S
    warnings.simplefilter("ignore")
    assert json.loads(S.pbix_open(ARROW, "arrow132"))["success"]
    try:
        ctx = S._get_dax_context("arrow132")
        got = de.evaluate_measures_smart(
            list(ARROW_DESKTOP), ctx["tables"], ctx["measure_defs"], {}, ctx["date_table"],
            ctx["date_column"], ctx["relationships"], simulate_row_context=False,
            measure_tables=ctx.get("measure_tables"), model_columns=ctx.get("model_columns"),
            date_tables=ctx.get("date_tables"))
    finally:
        S.pbix_close("arrow132")
    for m, want in ARROW_DESKTOP.items():
        assert got[m] == pytest.approx(want, rel=1e-9), m
