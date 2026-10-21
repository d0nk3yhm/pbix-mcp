"""pbix_evaluate_dax_batch answers each filter context exactly as pbix_evaluate_dax
does -- values, statuses, error messages and warnings -- though its contexts share
one measure memo. A caller asking one context per row of a visual (thousands for
a running total by date) paid the per-call lookups, parsing and an empty memo for
each."""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit

DAYS = [datetime(2024, 1, 1) + timedelta(days=i) for i in range(40)]
MEASURES = {
    "Total": "SUM(Fact[Amt])",
    "Running": "CALCULATE([Total], 'Cal'[Date] <= MAX('Cal'[Date]))",
    "Ever": "CALCULATE(MAX(Fact[When]), REMOVEFILTERS())",
    "Scope": "IF(ISINSCOPE('Cal'[Date]), 1, 0)",
    "Share": "DIVIDE([Total], CALCULATE([Total], ALLSELECTED('Cal'[Date])))",
    "Broken": "SUM(Fact[NoSuchColumn])",
    "Unknown": "NOSUCHFUNCTION(1)",
}


def _model(path: str) -> None:
    r = random.Random(156)
    b = PBIXBuilder("BatchModel")
    b.add_table("Cal", [{"name": "Date", "data_type": "DateTime"}], rows=[{"Date": d} for d in DAYS])
    b.add_table("Dim", [{"name": "Key", "data_type": "Int64"}, {"name": "Cat", "data_type": "String"}],
                rows=[{"Key": k, "Cat": "ab"[k % 2]} for k in range(1, 6)])
    b.add_table("Fact", [{"name": "DKey", "data_type": "Int64"}, {"name": "When", "data_type": "DateTime"},
                         {"name": "Amt", "data_type": "Double"}],
                rows=[{"DKey": r.randint(1, 6), "When": r.choice(DAYS), "Amt": round(r.uniform(-50, 100), 2)}
                      for _ in range(200)])
    b.add_relationship("Fact", "When", "Cal", "Date")
    b.add_relationship("Fact", "DKey", "Dim", "Key")
    for name, expr in MEASURES.items():
        b.add_measure("Fact", name, expr)
    b.add_page("Page 1")
    b.save(path)


def _contexts() -> list[dict]:
    r = random.Random(7)
    out = []
    for _ in range(60):
        day = r.choice(DAYS).isoformat()
        fc: dict = {"Cal.Date": [day]}
        base: dict = {}
        if r.random() < 0.5:
            base["Dim.Cat"] = [r.choice("ab")]
        fc.update(base)
        item: dict = {"filter_context": fc}
        if r.random() < 0.7:
            item["group_by"] = ["Cal.Date"]
            if r.random() < 0.7:   # the default (the filters but the grouping), or none: ALLSELECTED differs
                # the default, or a slicer on the axis column (some dates selected): ALLSELECTED differs
                item["selected_filter_context"] = base if r.random() < 0.5 else {
                    **base, "Cal.Date": sorted({day} | {x.isoformat() for x in r.sample(DAYS, 5)})}
        out.append(item)
    out += out[:15]                 # repeated contexts: answers from the shared memo
    out.append({"filter_context": {}})
    return out


def test_each_context_answers_as_pbix_evaluate_dax(tmp_path):
    path = str(tmp_path / "batch.pbix")
    _model(path)
    alias = "batch_eq"
    server.pbix_open(path, alias)
    try:
        names = ",".join(MEASURES)
        items = _contexts()
        batch = json.loads(server.pbix_evaluate_dax_batch(alias, names, json.dumps(items)))
        assert batch["success"] and len(batch["results"]) == len(items)
        statuses = set()
        for item, got in zip(items, batch["results"]):
            extra = {}
            if item.get("group_by"):
                extra["group_by"] = json.dumps(item["group_by"])
                if "selected_filter_context" in item:
                    extra["selected_filter_context"] = json.dumps(item["selected_filter_context"])
            fc = item["filter_context"]
            want = json.loads(server.pbix_evaluate_dax(alias, names, filter_context=json.dumps(fc) if fc else "",
                                                       apply_default_filters=False, **extra))
            assert got == want, item
            statuses |= {x["status"] for x in want["results"]}
        assert {"ok", "error"} <= statuses, statuses     # the broken measures report, not BLANK
    finally:
        server.pbix_close(alias, force=True)


def test_a_malformed_batch_is_refused(tmp_path):
    path = str(tmp_path / "batch.pbix")
    _model(path)
    server.pbix_open(path, "batch_bad")
    try:
        for bad in ('{"filter_context": {}}', "[1]", '[{"group_by": ["Cal.Date"], "filter_context": {}}]'):
            r = json.loads(server.pbix_evaluate_dax_batch("batch_bad", "Total", bad))
            assert not r["success"], bad
    finally:
        server.pbix_close("batch_bad", force=True)
