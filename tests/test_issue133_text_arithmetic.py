"""Issue #133: text that is no number is an error in arithmetic, not a BLANK.

Power BI Desktop 2.152 over ADOMD (build_b133.py, an en-US model), one
EVALUATE ROW per probe. "x" + 1 raises "Cannot convert value 'x' of type Text
to type Number"; the engine answered BLANK, which then folded into confident
numbers -- a calculation item adding 100 over Double of a text measure read
100 (#121). Numeric and date text still converts, as in Desktop.

Not covered here, and still different from Desktop: comparisons between text
and a number or a Boolean ("x" = 1, TRUE() = 1 raise in Desktop), and SUMX
over text. DIVIDE and the math functions convert text as arithmetic does
since #139 (test_issue139_numeric_text_arguments.py).
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["c", "v"], "rows": [["a", 10.0], ["b", 20.0]]}}
ERROR = object()
DESKTOP = {   # expression -> Desktop's answer
    '"x" + 1': ERROR,
    '"x" * 2': ERROR,
    '-"x"': ERROR,
    '"x" / 2': ERROR,
    '2 / "x"': ERROR,
    '"x" - 1': ERROR,
    '"3" + 1': 4,
    '" 3 " + 1': 4,
    '"3.5" + 0': 3.5,
    '"3,5" + 0': 35,
    '"1e3" + 0': 1000,
    '"$3" + 0': 3,
    '"" + 1': ERROR,
    '"2024-01-15" + 1': 45307,
    '"01/02/2024" + 1': 45294,
    "TRUE() + 1": 2,
    'IFERROR("x" * 2, 5)': 5,
    'IF(ISERROR("x" * 2), 1, 0)': 1,
    '"x" & 1': "x1",
    'IF(FALSE(), "x" * 2, 7)': 7,
    'BLANK() + "x"': ERROR,
    '"x" * BLANK()': None,
    "[Txt] * 2": ERROR,
    "[NumTxt] + 1": 4,
    'SUM(T[v]) + "x"': ERROR,
    "-BLANK()": None,
    '-"3"': -3,
    "-TRUE()": -1,
    "-DATE(2024, 1, 1)": -45292,
    'IF("x" < BLANK(), 1, 0)': 0,
}


@pytest.mark.parametrize("expr", list(DESKTOP))
def test_matches_desktop(expr):
    measures = {"Txt": '"x"', "NumTxt": '"3"', "p": expr}
    de._engine.eval_errors.clear()
    got = de.evaluate_measures_batch(["p"], TABLES, measures, {})["p"]
    want = DESKTOP[expr]
    if want is ERROR:
        assert got is None
        assert "Cannot convert value" in de._engine.eval_errors.get("p", "")
    elif isinstance(want, (int, float)) and not isinstance(want, bool):
        assert got == pytest.approx(want)
    else:
        assert got == want


def test_a_culture_reads_its_own_separators():
    # nb-NO: decimal comma, space groups -- "3,5" is 3.5 there
    measures = {"p": '"3,5" + 0'}
    ctx_got = de.evaluate_measures_smart(["p"], TABLES, measures, {}, culture="nb-NO",
                                         simulate_row_context=False)
    assert ctx_got["p"] == pytest.approx(3.5)
