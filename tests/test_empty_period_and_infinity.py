"""Two Desktop-verified rules that made MS_Sales_Returns silently wrong.

Both were found by comparing all 58 of that file's measures against the
workspace engine Power BI Desktop itself starts (msmdsrv over ADOMD), not
against the documentation.

1. A time-intelligence period that lands OUTSIDE the date table is an EMPTY
   filter table, and CALCULATE over an empty filter table is BLANK. We skipped
   the empty filter entirely, which applied NO filter and returned the GRAND
   TOTAL -- so [Net Sales PM] read 1,248,013 (the full total) where Desktop
   shows blank, and the ten Variance / Indicator / "Last 2 Months" measures
   built on it inherited that number.

2. The bare `/` operator does not blank a divide-by-zero (that is what DIVIDE()
   is for): Desktop returns an IEEE special. Those cannot be written into JSON
   as the bare `Infinity` / `NaN` literals Python emits, because that is not
   valid JSON and a strict client parser would reject the whole response.
"""
from __future__ import annotations

import json
import math

import pytest

from pbix_mcp.dax import engine as de
from pbix_mcp.models.responses import DAXResult, json_safe_number

# A calendar that stops short on BOTH ends, so the previous month of the first
# month and the next month of the last month are both off the table -- exactly
# the shape of MS_Sales_Returns' Calendar (2019-01-01 .. 2019-06-30).
TABLES = {
    "Calendar": {"columns": ["Date"],
                 "rows": [["2019-01-15"], ["2019-02-15"], ["2019-03-15"]]},
    "Sales": {"columns": ["Date", "Amount"],
              "rows": [["2019-01-15", 10], ["2019-02-15", 20],
                       ["2019-03-15", 30]]},
}
RELS = [{"fromTable": "Sales", "fromColumn": "Date",
         "toTable": "Calendar", "toColumn": "Date"}]
TOTAL = 60


def ev(expr, extra=None):
    defs = {"M": expr}
    defs.update(extra or {})
    ctx = de.DAXContext(TABLES, defs, "Calendar", "Date", None, RELS)
    return de.DAXEngine().evaluate_measure("M", ctx)


class TestEmptyPeriodIsBlankNotTheGrandTotal:
    @pytest.mark.parametrize("fn", [
        "PREVIOUSMONTH", "PREVIOUSQUARTER", "PREVIOUSYEAR",
    ])
    def test_period_before_the_table_is_blank(self, fn):
        """PREVIOUS* anchors on the FIRST date, so all three fall off the front."""
        got = ev(f"CALCULATE(SUM(Sales[Amount]), {fn}('Calendar'[Date]))")
        assert got is None, f"{fn} leaked a value: {got}"
        assert got != TOTAL, f"{fn} returned the GRAND TOTAL"

    @pytest.mark.parametrize("fn", ["NEXTMONTH", "NEXTQUARTER", "NEXTYEAR"])
    def test_period_after_the_table_is_blank(self, fn):
        """NEXT* anchors on the LAST date, so all three fall off the back."""
        got = ev(f"CALCULATE(SUM(Sales[Amount]), {fn}('Calendar'[Date]))")
        assert got is None, f"{fn} leaked a value: {got}"
        assert got != TOTAL, f"{fn} returned the GRAND TOTAL"

    def test_a_period_inside_the_table_still_filters(self):
        """The guard must not blank a period that DOES exist."""
        got = ev("CALCULATE(SUM(Sales[Amount]), "
                 "DATESBETWEEN('Calendar'[Date], DATE(2019,2,1), DATE(2019,2,28)))")
        assert got == 20

    def test_the_downstream_measures_inherit_blank_correctly(self):
        """The exact MS_Sales_Returns chain, on the same blank."""
        pm = "CALCULATE(SUM(Sales[Amount]), PREVIOUSMONTH('Calendar'[Date]))"
        extra = {"Base": "SUM(Sales[Amount])", "PM": pm}
        # Desktop: total - BLANK = total  (subtraction folds blank to 0)
        assert ev("[Base]-[PM]", extra) == TOTAL
        # Desktop: DIVIDE(total, BLANK, 0) - 1 = 0 - 1 = -1
        assert ev("DIVIDE([Base],[PM],0)-1", extra) == -1
        # Desktop: total + BLANK = total, NOT double the total
        assert ev("[Base]+[PM]", extra) == TOTAL


class TestDivideByZeroIsAnIEEESpecial:
    def test_operator_returns_infinity(self):
        assert ev("SUM(Sales[Amount]) / 0") == float("inf")
        assert ev("-SUM(Sales[Amount]) / 0") == float("-inf")
        assert math.isnan(ev("(SUM(Sales[Amount]) - 60) / 0"))

    def test_blank_numerator_still_wins(self):
        assert ev("BLANK() / 0") is None

    def test_divide_function_is_still_the_safe_one(self):
        assert ev("DIVIDE(SUM(Sales[Amount]), 0)") is None
        assert ev("DIVIDE(SUM(Sales[Amount]), 0, -1)") == -1


class TestNonFiniteValuesStayJsonLegal:
    """`Infinity` and `NaN` are Python-only JSON extensions.

    json.dumps writes them as bare literals, which JSON.parse, encoding/json and
    serde_json all reject -- one infinite cell would fail the ENTIRE tool
    response, which is worse than the rare value being a string.
    """

    @pytest.mark.parametrize("value,want", [
        (float("inf"), "Infinity"),
        (float("-inf"), "-Infinity"),
        (float("nan"), "NaN"),
        (1.5, 1.5),
        (0.0, 0.0),
        (None, None),
        ("text", "text"),
    ])
    def test_json_safe_number(self, value, want):
        assert json_safe_number(value) == want

    def test_dax_result_sanitizes_on_construction(self):
        r = DAXResult(name="m", value=float("inf"))
        assert r.value == "Infinity"

    def test_the_serialized_payload_parses_as_strict_json(self):
        r = DAXResult(name="m", value=float("nan"))
        text = json.dumps(r.model_dump(), allow_nan=False)   # would raise on nan
        assert json.loads(text)["value"] == "NaN"
