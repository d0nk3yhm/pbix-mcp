"""Issues #134 and #135: VARs are lazy, and IFERROR / ISERROR react to errors.

Power BI Desktop 2.152 over ADOMD (build_b134.py), one EVALUATE ROW per probe:

#134 -- a VAR is evaluated the first time it is used, in the filter context
where it is defined, and never if it is not: VAR x = ERROR("boom") RETURN 5 is
5. The engine evaluated every VAR up front, so an unused failing VAR failed the
measure (with #133 "a" * 1 is such a failure), and a calculation item computed
each period's VAR for every measure it applied to. A variable's error is not
caught where the variable is used: VAR x = ERROR("boom") RETURN IFERROR(x, 9)
fails in Desktop.

#135 -- IFERROR / ISERROR answered a BLANK as an error: IFERROR(BLANK(), 5) is
BLANK in Desktop and was 5; ISERROR(BLANK()) is FALSE and was TRUE. A division
by zero IS an error there (IFERROR(1 / 0, 5) is 5), and so is a referenced
measure that fails (IFERROR([Bad], 13) is 13).

Not covered: VAR x = [Bad] RETURN x fails in Desktop; the engine still degrades
a referenced measure's error to BLANK outside IFERROR / ISERROR.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["c", "v"], "rows": [["a", 10.0], ["b", 20.0], ["c", 30.0]]}}
ERROR = object()
DESKTOP = {   # expression -> Desktop's answer
    'VAR x = "a" * 1 RETURN 5': 5,
    'VAR x = ERROR("boom") RETURN 5': 5,
    'VAR x = ERROR("boom") RETURN IF(FALSE(), x, 6)': 6,
    'VAR x = ERROR("boom") RETURN IF(TRUE(), x, 6)': ERROR,
    'VAR x = ERROR("boom") VAR y = 7 RETURN SWITCH(TRUE(), FALSE(), x, y)': 7,
    'VAR x = ERROR("boom") VAR y = x + 1 RETURN 8': 8,
    'VAR x = ERROR("boom") VAR y = x + 1 RETURN y': ERROR,
    'VAR x = SUM(T[v]) RETURN CALCULATE(x, T[c] = "a")': 60,
    'VAR x = SUM(T[v]) RETURN SUMX(VALUES(T[c]), x)': 180,
    'VAR x = SUM(T[v]) RETURN x + x': 120,
    'VAR x = ERROR("boom") RETURN IFERROR(x, 9)': ERROR,
    'VAR x = [Bad] RETURN 10': 10,
    'VAR x = [Bad] RETURN IF(ISBLANK(BLANK()), 11, x)': 11,
    'VAR x = ERROR("boom") RETURN IF(ISERROR(x), 1, 0)': ERROR,
    'VAR x = IFERROR(ERROR("boom"), 1) RETURN x': 1,
    "IFERROR(BLANK(), 5)": None,
    "IF(ISERROR(BLANK()), 1, 0)": 0,
    "IFERROR(1 / 0, 5)": 5,
    "IFERROR(DIVIDE(1, BLANK()), 5)": None,
    'IFERROR(ERROR("boom"), 12)': 12,
    'IF(ISERROR(ERROR("boom")), 1, 0)': 1,
    "IFERROR([Bad], 13)": 13,
    "IF(ISERROR([Bad]), 1, 0)": 1,
    "IFERROR([Blank], 14)": None,
}


@pytest.mark.parametrize("expr", list(DESKTOP))
def test_matches_desktop(expr):
    measures = {"Bad": '"x" * 2', "Blank": "BLANK()", "p": expr}
    de._engine.eval_errors.clear()
    got = de.evaluate_measures_batch(["p"], TABLES, measures, {})["p"]
    want = DESKTOP[expr]
    if want is ERROR:
        assert got is None
        assert "p" in de._engine.eval_errors
    else:
        assert got == want


def test_a_variable_is_evaluated_once():
    calls = {"n": 0}
    real = de.DAXEngine._fn_sum

    def counting(self, args_str, ctx):
        calls["n"] += 1
        return real(self, args_str, ctx)

    eng = de.DAXEngine()
    eng._func_map["SUM"] = counting.__get__(eng)
    ctx = de.DAXContext(TABLES, {"p": "VAR x = SUM(T[v]) RETURN x + x + x"})
    assert eng.evaluate_measure("p", ctx) == 180
    assert calls["n"] == 1
