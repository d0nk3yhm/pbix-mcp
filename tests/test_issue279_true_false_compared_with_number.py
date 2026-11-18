"""#279: TRUE / FALSE compared with a number was compared as 1
or 0; Power BI Desktop fails the query.

_eval_comparison and IN compared a Python bool with a number, so TRUE was 1:
IF(2 = 2 = 2, 1, 0) -- (2 = 2) = 2, TRUE against 2 -- answered 0, and
IF(1 = 1 IN {TRUE()}, 1, 0) -- 1 = (1 IN {TRUE()}) -- answered 1. Desktop
refuses both comparisons: "DAX comparison operations do not support comparing
values of type True/False with values of type Integer. Consider using the
VALUE or FORMAT function to convert one of the values." (d5) and "Function
CONTAINSROW does not support comparing values of type True/False with values
of type Integer. ..." (d11) -- its Norwegian, in English.

Expected values: Power BI Desktop 2.152 over ADOMD, F:/_desktop_oracle/
build_b255d.py d5 and d11 (errors); the other cases compare TRUE / FALSE with
TRUE / FALSE or BLANK, by exact arithmetic (Microsoft Learn: BLANK takes the
other side's zero, FALSE for TRUE / FALSE), and are unchanged.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"TM": {"columns": ["n", "d", "z"], "rows": [[7, 2, 1], [7, 0, 0], [-7, 2, 1], [7, None, 0]]}}


def _engine(dax):
    eng = de.DAXEngine()
    got = eng.evaluate_measure("p", de.DAXContext(TABLES, {"p": dax}))
    return ("ERROR", str(eng.eval_errors["p"])) if "p" in eng.eval_errors else (got, None)


def test_d5_true_against_an_integer_fails():
    got, err = _engine("IF(2 = 2 = 2, 1, 0)")
    assert got == "ERROR"
    assert "DAX comparison operations do not support comparing values of type True/False" in err
    assert "Integer" in err


def test_d11_in_against_true_fails():
    got, err = _engine("IF(1 = 1 IN {TRUE()}, 1, 0)")
    assert got == "ERROR"
    # 2.152's text was taken from a Norwegian Desktop; 2.157's English (2026-11-18, TRUE() IN {1}) quotes the name
    # and puts the table's type first: here the table holds TRUE, the value 1 (1 = (1 IN {TRUE()}))
    assert "Function 'CONTAINSROW' does not support comparing values of type True/False with values of type " \
           "Integer" in err


@pytest.mark.parametrize("expr", [
    "IF(TRUE() = 1, 1, 0)",
    "IF(1 < TRUE(), 1, 0)",
    "IF(FALSE() <> 2.5, 1, 0)",
    "IF(2 IN {TRUE(), FALSE()}, 1, 0)",
    "IF(TRUE() IN {1, 2}, 1, 0)",
    "COUNTROWS(FILTER(TM, (TM[n] = 7) = TM[z]))",
])
def test_every_operator_and_in_refuse_it(expr):
    """The same clash through other operators, IN in either direction and a
    row's column. Not Desktop answers: extrapolated from the rule d5 / d11
    measured (= and IN with an Integer); of these only TRUE() = 1 has an
    earlier Desktop note (build_b133.py true_eq_1, an error)."""
    got, err = _engine(expr)
    assert got == "ERROR"
    assert "True/False" in err


@pytest.mark.parametrize("expr,want", [
    ("IF(TRUE() = (1 = 1), 1, 0)", 1),
    ("IF(FALSE() = BLANK(), 1, 0)", 1),          # BLANK is FALSE here
    ("IF(TRUE() IN {TRUE()}, 1, 0)", 1),
    ("IF(BLANK() = 0, 1, 0)", 1),
    ("IF(\"a\" = \"A\" = TRUE(), 1, 0)", 1),      # build_b255d.py d6
    ("COUNTROWS(FILTER(TM, TM[d] = 2 = TRUE()))", 2),  # d4
])
def test_true_false_with_true_false_or_blank_is_unchanged(expr, want):
    assert _engine(expr) == (want, None)
