"""Issue #140: a measure that two iterations evaluate over the same rows is
evaluated once per row, unless it can reach ALLSELECTED.

#118 keyed the measure memo by a row transition's iteration, for every
measure, because ALLSELECTED restores the iteration's rows. FILTER's pass and
AVERAGEX's pass over the same employees then evaluated [MTD Total Sales]
twice. Agents Performance's "Employees Avg MTD Sales - Adjusted" took 42.7 s
instead of 21.7 s, and the corpus census (30 s a measure) read BLANK. Only
ALLSELECTED reads an iteration's rows, so the iteration is part of the key
only for a measure that can reach it: in its own text, in a measure it
references, or in a calculation item.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["K", "V"], "rows": [[k, float(k)] for k in range(1, 11)]}}
MEASURES = {
    "S": "SUM(T[V])",
    "Avg Adjusted": "AVERAGEX(FILTER(VALUES(T[K]), NOT ISBLANK([S]) && T[K] <= 6), [S])",
    "MA": "CALCULATE(COUNTROWS(ALLSELECTED(T[K])))",
    "Via": "[MA]",
    # FILTER's pass sees 10 rows, SUMX's pass the 2 it kept: [MA] is 10, then 2
    "Sum MA": "SUMX(FILTER(VALUES(T[K]), [MA] > 0 && T[K] <= 2), [MA])",
    "Sum Via": "SUMX(FILTER(VALUES(T[K]), [Via] > 0 && T[K] <= 2), [Via])",
}


def _eval(names, measures=None):
    return de.evaluate_measures_batch(list(names), TABLES, measures or MEASURES, {})


def test_two_passes_share_a_measure_without_allselected(monkeypatch):
    calls = {"n": 0}
    real = de.DAXEngine._eval_expr
    body = MEASURES["S"]

    def counting(self, expr, ctx, var_scope=None):
        if expr == body:           # evaluate_measure running [S]'s body
            calls["n"] += 1
        return real(self, expr, ctx, var_scope)

    monkeypatch.setattr(de.DAXEngine, "_eval_expr", counting)
    assert _eval(["Avg Adjusted"])["Avg Adjusted"] == pytest.approx(3.5)
    # once per key in FILTER's pass; AVERAGEX's pass over 6 of them reuses it
    assert calls["n"] == 10


@pytest.mark.parametrize("name", ["Sum MA", "Sum Via"])
def test_a_measure_reaching_allselected_keeps_each_iteration(name):
    # ALLSELECTED restores each pass's own rows: 2 + 2, not FILTER's 10 + 10
    assert _eval([name])[name] == 4


@pytest.mark.parametrize("name,want", [("S", False), ("Avg Adjusted", False), ("MA", True),
                                       ("Via", True), ("Sum MA", True)])
def test_reads_shadow(name, want):
    assert de._engine._reads_shadow(name, de.DAXContext(TABLES, MEASURES)) is want


def test_a_calculation_item_reaching_allselected_counts_for_every_measure():
    measures = dict(MEASURES)
    de.set_calculation_groups(measures, [{"table": "CG", "column": "Name", "items": [
        {"name": "Share", "expression": "DIVIDE(SELECTEDMEASURE(), [Via])"}]}])
    try:
        assert de._engine._reads_shadow("S", de.DAXContext(TABLES, measures)) is True
    finally:
        de.set_calculation_groups(measures, None)
