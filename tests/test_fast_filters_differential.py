"""The fast filters (_FAST_FILTERS: a one -> many hop through per-row buckets,
an inequality through a sorted index, a source table's filters memoized by
themselves) change no answer: every measure under every filter context gives
exactly what the plain route gives, over random models built to hit the edges
-- keys a dimension holds twice, fact keys matching no row, BLANK keys and
cells, text and NaN among numbers, dates with times, a two-hop date table.

A semantic rewrite is proven by a differential run, not by hand cases alone
(a downstream fuzz of #200's pushdown found three wrong answers its hand cases missed).
"""
from __future__ import annotations

import math
import random
from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

SEEDS = range(40)
CONTEXTS = 25
MEASURES = {
    "Total": "SUM(F[Amt])",
    "Rows": "COUNTROWS(F)",
    "Least": "MIN(F[Amt])",
    "Running": "CALCULATE([Total], Cal[Date] <= MAX(Cal[Date]))",
    "NotA": 'CALCULATE([Total], D[Cat] <> "a")',
    "Big": "CALCULATE([Total], F[Amt] > 50)",
    "All": "CALCULATE([Total], REMOVEFILTERS())",
    "LastDate": "MAX(Cal[Date])",
    "Window": "CALCULATE([Rows], Cal[Date] >= MIN(Cal[Date]) - 3, Cal[Date] <= MAX(Cal[Date]))",
}
RELS = [{"FromTable": "F", "FromColumn": "DKey", "ToTable": "D", "ToColumn": "Key", "IsActive": 1},
        {"FromTable": "F", "FromColumn": "When", "ToTable": "Cal", "ToColumn": "Date", "IsActive": 1},
        {"FromTable": "Cal", "FromColumn": "Date", "ToTable": "Y", "ToColumn": "Date", "IsActive": 1}]


def model(seed: int) -> dict:
    r = random.Random(seed)
    first = datetime(2024, 1, 1)
    days = [first + timedelta(days=i) for i in range(r.randint(20, 60))]
    keys = list(range(1, r.randint(4, 9)))
    if r.random() < 0.3:
        keys.append(keys[0])                     # a key the dimension holds twice (#162)
    d_rows = [[k, r.choice("abc"), r.choice([1.5, 2, 3, None, "x"] if r.random() < 0.3 else [1.5, 2, 3])]
              for k in keys]
    when = days + ([datetime(2024, 1, 3, 12, 30)] if r.random() < 0.3 else [])
    f_rows = []
    for _ in range(r.randint(30, 120)):
        dkey = r.choice(keys + ([99, None] if r.random() < 0.4 else []))   # unmatched and BLANK keys
        amt = r.choice([round(r.uniform(-100, 100), 2), r.randint(0, 80)])
        if r.random() < 0.08:
            amt = None
        f_rows.append([dkey, r.choice(when), amt])
    if r.random() < 0.15:
        f_rows.append([keys[0], days[0], math.nan])
    return {
        "D": {"columns": ["Key", "Cat", "Num"], "rows": d_rows},
        "Cal": {"columns": ["Date"], "rows": [[d] for d in days]},
        "Y": {"columns": ["Date", "Year", "Week"], "rows": [[d, d.year, d.isocalendar()[1]] for d in days]},
        "F": {"columns": ["DKey", "When", "Amt"], "rows": f_rows},
    }


def copy(tables: dict) -> dict:
    return {k: {"columns": list(v["columns"]), "rows": [list(x) for x in v["rows"]]} for k, v in tables.items()}


def contexts(seed: int, tables: dict) -> list:
    r = random.Random(seed * 7919)
    days = [x[0] for x in tables["Cal"]["rows"]]
    out = []
    for _ in range(CONTEXTS):
        fc: dict = {}
        if r.random() < 0.5:
            fc["Cal.Date"] = [r.choice(days)]
        elif r.random() < 0.5:
            fc["Cal.Date"] = {"op": r.choice(["<", "<=", ">", ">="]),
                              "value": r.choice([r.choice(days), None, 45300.0, "2024-01-10"])}
        if r.random() < 0.4:
            fc["D.Cat"] = r.sample("abc", r.randint(1, 2))
        if r.random() < 0.3:
            fc["D.Num"] = {"op": r.choice(["<", ">="]), "value": r.choice([2, None, "2"])}
        if r.random() < 0.3:
            fc["F.Amt"] = {"all": [{"op": ">", "value": -20}, {"op": "<=", "value": r.choice([10, 60])}]}
        if r.random() < 0.3:
            fc["Y.Week"] = {"not_in": [r.choice([1, 2, 3])]}
        if r.random() < 0.2:
            fc["D.Key"] = [r.choice([1, 2, 99, None])]
        out.append(fc)
    return out


def answers(tables: dict, fcs: list, fast: bool, monkeypatch) -> list:
    monkeypatch.setattr(de, "_FAST_FILTERS", fast)
    eng = de.DAXEngine()
    got = []
    for fc in fcs:
        row = []
        for name in MEASURES:
            ctx = de.DAXContext(tables, MEASURES, "Cal", "Date", dict(fc), RELS)
            try:
                v = eng.evaluate_measure(name, ctx)
            except Exception as e:   # the same error both ways is the same answer
                v = ("error", type(e).__name__)
            row.append(v)
        got.append(row)
    return got


def same(a, b) -> bool:
    """Equal answers: exactly, or numbers within 1e-6 absolute or 1e-9 relative -- a running total from
    per-date subtotals adds floats in another order than row by row (#155), as Desktop's own engine does;
    the Desktop suites compare so. NaN equals NaN; a type change is a difference."""
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) or math.isnan(b):
            return math.isnan(a) and math.isnan(b)
        return abs(a - b) <= 1e-6 or math.isclose(a, b, rel_tol=1e-9)
    return type(a) is type(b) and a == b


@pytest.mark.parametrize("seed", SEEDS)
def test_fast_filters_answer_as_the_plain_route(seed, monkeypatch):
    base = model(seed)
    fcs = contexts(seed, base)
    plain = answers(copy(base), fcs, False, monkeypatch)
    fast = answers(copy(base), fcs, True, monkeypatch)
    for fc, p, f in zip(fcs, plain, fast):
        assert all(same(x, y) for x, y in zip(p, f)), (fc, [(m, x, y) for m, x, y in zip(MEASURES, p, f) if not same(x, y)])


def test_the_fuzz_reaches_the_fast_paths(monkeypatch):
    """Teeth: the random contexts do take the bucketed hop and the sorted index."""
    calls = {"hop": 0, "sorted": 0}
    real_hop, real_sorted = de.DAXContext._hop_row_set, de.DAXContext._indices_for_column_predicate

    def hop(self, *a, **k):
        out = real_hop(self, *a, **k)
        calls["hop"] += out is not None
        return out

    def srt(self, *a, **k):
        out = real_sorted(self, *a, **k)
        calls["sorted"] += out is not None
        return out
    monkeypatch.setattr(de.DAXContext, "_hop_row_set", hop)
    monkeypatch.setattr(de.DAXContext, "_indices_for_column_predicate", srt)
    for seed in range(5):
        base = model(seed)
        answers(copy(base), contexts(seed, base), True, monkeypatch)
    assert calls["hop"] > 50 and calls["sorted"] > 20, calls
