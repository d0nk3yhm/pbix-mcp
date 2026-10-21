"""A filter of several values on a column takes the union of their row sets;
the union is memoized per model by the values it unites (issue #202; PR #199,
by @allanon2). A year-to-date measure over a matrix of countries by month asks
for the same dates' rows for every country of a month: on Microsoft's
Corporate Spend sample most of the query's time went to those unions. The
union depends on the values alone (the filters in force apply after it), so
the memo must never change an answer."""
from __future__ import annotations

from datetime import datetime

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, d) for d in range(1, 11)]
COUNTRIES = ["A", "B", "C"]
RELS = [{"FromTable": "F", "FromColumn": "D", "ToTable": "Cal", "ToColumn": "D", "IsActive": 1}]


def model():
    # a new tables mapping is a new model, with its own filter cache
    return {"Cal": {"columns": ["D"], "rows": [[d] for d in DAYS]},
            "F": {"columns": ["D", "Country", "Amt", "Flag"],
                  "rows": [[d, c, i * 10 + j, j % 2 == 0] for i, d in enumerate(DAYS) for j, c in enumerate(COUNTRIES)]}}


def test_a_union_is_the_rows_of_its_values_and_is_answered_again():
    ctx = de.DAXContext(model(), {}, "Cal", "D", None, RELS)
    tbl = ctx.tables["F"]
    ac = ctx._indices_for_values(tbl, 1, ["a", "c"])          # join spellings: text folded
    assert ac == frozenset(i for i, r in enumerate(tbl["rows"]) if r[1] in ("A", "C"))
    assert ctx._indices_for_values(tbl, 1, ["c", "a"]) is ac   # in any order, from the memo
    assert ctx._indices_for_values(tbl, 1, ["b"]) == frozenset(range(1, 30, 3))
    assert ctx._indices_for_values(tbl, 1, ["x", "y"]) == frozenset()


def test_the_memo_tells_columns_and_tables_apart():
    tables = model()
    tables["G"] = {"columns": ["Country"], "rows": [["A"], ["C"], ["A"]]}
    ctx = de.DAXContext(tables, {}, "Cal", "D", None, RELS)
    f, g = ctx.tables["F"], ctx.tables["G"]
    assert len(ctx._indices_for_values(f, 1, ["a", "c"])) == 20
    assert ctx._indices_for_values(g, 0, ["a", "c"]) == frozenset({0, 1, 2})


def test_values_python_calls_equal_are_not_confused():
    ctx = de.DAXContext(model(), {}, "Cal", "D", None, RELS)
    tbl = ctx.tables["F"]
    assert ctx._indices_for_values(tbl, 3, [True, False]) == frozenset(range(30))
    assert ctx._indices_for_values(tbl, 3, [1, 0]) == frozenset()     # True is not 1 (#39)


def test_year_to_date_by_country_and_day_through_the_memo(monkeypatch):
    # The memo serves a hop by keys; with the fast filters on, this direct hop goes
    # through per-row buckets instead (_hop_row_set, test_fast_filters_differential.py).
    monkeypatch.setattr(de, "_FAST_FILTERS", False)
    tables = model()
    eng = de.DAXEngine()
    for day in DAYS:
        for country in COUNTRIES:
            ctx = de.DAXContext(tables, {"YTD": "TOTALYTD(SUM(F[Amt]), Cal[D])"}, "Cal", "D",
                                {"Cal.D": [day], "F.Country": [country]}, RELS)
            want = sum(i * 10 + j for i, d in enumerate(DAYS) for j, c in enumerate(COUNTRIES)
                       if d <= day and c == country)
            assert eng.evaluate_measure("YTD", ctx) == want
    assert de._shared_filter_cache(tables).get("__index_unions__")


def test_the_memo_starts_again_past_its_bound(monkeypatch):
    monkeypatch.setattr(de.DAXContext, "_UNION_MEMO_MAX_ROWS", 25)
    ctx = de.DAXContext(model(), {}, "Cal", "D", None, RELS)
    tbl = ctx.tables["F"]
    ab = ctx._indices_for_values(tbl, 1, ["a", "b"])                   # 20 rows held
    memo = ctx._filter_idx_cache["__index_unions__"]
    assert memo["__rows__"] == 20
    bc = ctx._indices_for_values(tbl, 1, ["b", "c"])                   # 40 > 25: starts again
    assert memo["__rows__"] == 20 and len(memo) == 2
    assert ctx._indices_for_values(tbl, 1, ["a", "b"]) == ab != bc
