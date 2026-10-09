"""Issue #149: two filters of more than 4096 values never share a memo entry.

The cross-filter memo signed such a filter as None, so CALCULATE over the
dates before 2015 was served the result for the dates before 2014 -- in the
same measure or another one, as the memo is model-wide -- and [b] - [a] was 0
(Desktop 365). The ALL() snapshot compared signatures the same way.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b149.py). The probes
run in one call, as a visual asks for several measures at once."""
from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit


def _rel(many, many_col, one, one_col):
    return {"FromTable": many, "FromColumn": many_col, "ToTable": one, "ToColumn": one_col,
            "IsActive": True, "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}


def _eval(expr, tables, measures, rels, date_tables=None):
    m = dict(measures)
    m["p"] = expr
    return de.evaluate_measures_smart(["p"], tables, m, {}, relationships=rels,
                                      date_tables=date_tables or {}, simulate_row_context=False)["p"]


def _check(got, want):
    if want is None:
        assert got is None
    elif isinstance(want, bool):
        assert got is want
    elif isinstance(want, str):
        assert got == want
    else:
        assert got == pytest.approx(want)


DAYS = [datetime(2000, 1, 1) + timedelta(days=i) for i in range(6000)]
KEYS = [f"k{i:05d}" for i in range(5000)]
# build_b149.py: a 6000-day calendar and a 5000-key dimension, one fact row per member
BIG = {"D": {"columns": ["Date", "Year"], "rows": [[d, d.year] for d in DAYS]},
       "F": {"columns": ["Date", "Amt"], "rows": [[d, 1.0] for d in DAYS]},
       "K": {"columns": ["Key"], "rows": [[k] for k in KEYS]},
       "G": {"columns": ["Key", "Amt"], "rows": [[k, 1.0] for k in KEYS]}}
BIG_RELS = [_rel("F", "Date", "D", "Date"), _rel("G", "Key", "K", "Key")]
BIG_MEASURES = {"a": "CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2014, 1, 1)))",
                "b": "CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2015, 1, 1)))",
                "ka": 'CALCULATE(SUM(G[Amt]), FILTER(ALL(K[Key]), K[Key] < "k04500"))',
                "kb": 'CALCULATE(SUM(G[Amt]), FILTER(ALL(K[Key]), K[Key] < "k04900"))'}

B149 = [   # (probe, expression, Power BI Desktop 2.152) -- build_b149.py
    ('a', 'CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2014, 1, 1)))', 5114),
    ('b', 'CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2015, 1, 1)))', 5479),
    ('d', '[b] - [a]', 365),
    ('d2', 'CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2015, 1, 1))) - CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2014, 1, 1)))', 365),
    ('ka', 'CALCULATE(SUM(G[Amt]), FILTER(ALL(K[Key]), K[Key] < "k04500"))', 4500),
    ('kb', 'CALCULATE(SUM(G[Amt]), FILTER(ALL(K[Key]), K[Key] < "k04900"))', 4900),
    ('kd', '[kb] - [ka]', 400),
    ('small', 'CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2001, 1, 1))) + CALCULATE(SUM(F[Amt]), FILTER(ALL(D[Date]), D[Date] < DATE(2002, 1, 1)))', 1097),
]


def test_b149_in_one_call():
    names = [k for k, _e, _w in B149]
    measures = dict(BIG_MEASURES, **{f"p_{k}": e for k, e, _w in B149})
    got = de.evaluate_measures_smart([f"p_{k}" for k in names], BIG, measures, {},
                                     relationships=BIG_RELS, simulate_row_context=False)
    for k, _e, want in B149:
        _check(got[f"p_{k}"], want)


@pytest.mark.parametrize("probe,expr,want", B149, ids=[p for p, _e, _w in B149])
def test_b149_each(probe, expr, want):
    _check(_eval(expr, BIG, BIG_MEASURES, BIG_RELS), want)
