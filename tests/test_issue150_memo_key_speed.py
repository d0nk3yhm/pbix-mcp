"""Issue #150: the measure memo key of a fact-row transition costs what the
row changed, not what the whole context holds.

0.9.128 (#115) gave a fact-row transition one filter per dimension column.
evaluate_measure copied every filter's values into its memo key and sorted the
key for every row, so SUMX(Fact, [S]) took twice as long as the same iteration
written with CALCULATE (5.0 s against 2.5 s on 20,000 rows). The key is now
the context's own signature: each filter signed once (carried on the value,
the dimension filters' taken from the expansion's cache), a derived context's
derived from its parent's.
"""
from __future__ import annotations

import random
import time

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit


def _model(n=3000):
    rnd = random.Random(7)
    dims, rels = {}, []
    for d, size in (("Prod", 200), ("Cust", 400), ("Store", 30)):
        cols = [f"{d}Key"] + [f"{d}A{i}" for i in range(15)]
        dims[d] = {"columns": cols, "rows": [[k] + [f"{d}{k}_{i % 5}" for i in range(15)] for k in range(size)]}
        rels.append({"FromTable": "Fact", "FromColumn": f"{d}Key", "ToTable": d, "ToColumn": f"{d}Key",
                     "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1})
    fact = {"columns": ["ProdKey", "CustKey", "StoreKey", "Amt"],
            "rows": [[rnd.randrange(200), rnd.randrange(400), rnd.randrange(30), float(rnd.randrange(1, 100))]
                     for _ in range(n)]}
    return dict(dims, Fact=fact), rels


def _timed(expr, tables, rels):
    m = {"S": "SUM(Fact[Amt])", "p": expr}
    de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels, simulate_row_context=False)
    best, value = None, None
    for _ in range(3):
        t0 = time.perf_counter()
        value = de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels,
                                           simulate_row_context=False)["p"]
        dt = time.perf_counter() - t0
        best = dt if best is None else min(best, dt)
    return best, value


def test_measure_reference_costs_about_what_calculate_costs():
    tables, rels = _model()
    t_calc, v_calc = _timed("SUMX(Fact, CALCULATE(SUM(Fact[Amt])))", tables, rels)
    t_meas, v_meas = _timed("SUMX(Fact, [S])", tables, rels)
    assert v_meas == pytest.approx(v_calc)
    # 0.9.128: 1.8x; 0.9.127 1.1x; with the context's signature as the key 1.0x
    assert t_meas < 1.45 * t_calc, (t_meas, t_calc)


def test_memo_key_is_the_context_signature():
    tables, rels = _model(200)
    ctx = de.DAXContext(tables, {"S": "SUM(Fact[Amt])"}, None, None, {}, rels)
    eng = de.DAXEngine()
    rows = eng._eval_expr("Fact", ctx)
    row_ctx = eng._make_row_context(rows[0], ctx, shadow=rows)
    eng.evaluate_measure("S", row_ctx)
    keys = [k for k in row_ctx._measure_cache if k[0] == "S"]
    assert keys and all(isinstance(k[1], frozenset) for k in keys)
    # the key names the row's dimension filters too (the expanded table, #115)
    assert any(any(str(part[0]).startswith("Prod.") for part in k[1]) for k in keys)
