"""One model's filter-index cache is bounded: past _FILTER_CACHE_MAX_WEIGHT
elements it starts again, and no answer changes.

Its entries are keyed by filter context. A running total by date, asked once
per date under that date's context, added each date's cross-table key sets,
and nothing ever removed them: 4,000 dates of a 1 MB model held 16 GB until
the operating system ran out of memory. Every entry is a pure function of its
key, so emptying the cache can only cost time.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(60)]
ACCOUNTS = ["a", "b", "c"]
TABLES = {
    "dim": {"columns": ["D"], "rows": [[d] for d in DAYS]},
    "fact": {"columns": ["D", "Account", "Amt"],
             "rows": [[d, acc, i * 10 + j] for i, d in enumerate(DAYS) for j, acc in enumerate(ACCOUNTS)]},
}
RELS = [{"FromTable": "fact", "FromColumn": "D", "ToTable": "dim", "ToColumn": "D", "IsActive": 1}]
MEASURES = {"Running": "CALCULATE(SUM(fact[Amt]), FILTER(ALL(dim[D]), dim[D] <= MAX(dim[D])))"}


def fresh_tables():
    # A new tables mapping is a new model: its own filter cache (_shared_filter_cache).
    return {k: {"columns": list(v["columns"]), "rows": [list(r) for r in v["rows"]]} for k, v in TABLES.items()}


def expected(day):
    return sum(i * 10 + j for i, d in enumerate(DAYS) for j, acc in enumerate(ACCOUNTS)
               if d <= day and acc in ("a", "b"))


def running_totals(tables):
    """The running total for each day, under the day and two accounts, with the
    cache's weight after each."""
    eng = de.DAXEngine()
    got, weights = {}, []
    for day in DAYS:
        ctx = de.DAXContext(tables, MEASURES, "dim", "D", {"dim.D": [day], "fact.Account": ["a", "b"]}, RELS)
        got[day] = eng.evaluate_measure("Running", ctx)
        weights.append(de._shared_filter_cache(tables).weight)
    return got, weights


def test_a_running_total_fills_the_cache_one_context_per_date():
    got, weights = running_totals(fresh_tables())
    assert got == {d: expected(d) for d in DAYS}
    assert weights[-1] > weights[9] > 0, "each date's context adds to the cache"


def test_past_its_bound_the_cache_starts_again_and_every_answer_stands(monkeypatch):
    _, unbounded = running_totals(fresh_tables())
    bound = unbounded[-1] // 4
    monkeypatch.setattr(de, "_FILTER_CACHE_MAX_WEIGHT", bound)
    tables = fresh_tables()
    got, weights = running_totals(tables)
    assert got == {d: expected(d) for d in DAYS}
    assert max(weights) <= bound
    assert any(b < a for a, b in zip(weights, weights[1:])), "the cache never started again"
    assert de._shared_filter_cache(tables)["__tables_ref__"] is tables


def test_an_entry_larger_than_the_bound_is_still_kept(monkeypatch):
    monkeypatch.setattr(de, "_FILTER_CACHE_MAX_WEIGHT", 10)
    tables = fresh_tables()
    cache = de._shared_filter_cache(tables)
    cache["small"] = frozenset(range(5))
    cache["large"] = [(frozenset(range(50)), 1)]
    assert "small" not in cache and cache["large"] == [(frozenset(range(50)), 1)]
    assert cache.weight == de._cache_weight([(frozenset(range(50)), 1)]) and cache["__tables_ref__"] is tables


def test_facts_about_the_model_outlive_a_reset(monkeypatch):
    """Value maps and blank-row tables don't depend on a filter: dropped with the filter-named
    entries, they were rebuilt in the middle of a query, whose filter-named entries then filled
    the cache again."""
    monkeypatch.setattr(de, "_FILTER_CACHE_MAX_WEIGHT", 10)
    tables = fresh_tables()
    cache = de._shared_filter_cache(tables)
    cache[("blank-row-tables", 1)] = frozenset({"dim"})
    cache[(id(tables["fact"]), "vmap", 0)] = {"a": frozenset(range(5))}
    cache[(id(tables["fact"]), "xtf", 1)] = [(frozenset(range(8)), 0)]
    cache[(id(tables["fact"]), "x", 2)] = frozenset(range(7))       # past the bound: the filter-named go
    assert (id(tables["fact"]), "xtf", 1) not in cache
    assert cache[("blank-row-tables", 1)] == frozenset({"dim"}) and (id(tables["fact"]), "vmap", 0) in cache
    assert (id(tables["fact"]), "x", 2) in cache and cache["__tables_ref__"] is tables
