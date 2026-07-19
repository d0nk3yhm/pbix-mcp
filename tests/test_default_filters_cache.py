"""Regression (OpenBI #7): the per-alias DAX default_filters cache must not go
stale. pbix_evaluate_dax auto-applies the report's default slicer selections when
no explicit filter_context is given. Those selections live in the report layout
and can change (pbix_set_filters, slicer edits, set_layout_raw, ...) WITHOUT the
DAX context being rebuilt. Before the fix the context cached default_filters once
and reused it forever, leaking a previous slicer selection into later evaluations.

The fix re-derives default_filters fresh from the current layout at evaluation
time, so:
  * a stale cached filter is ignored once the slicer it came from is gone, and
  * a filter added to the layout after the context was cached IS applied.
"""

import json

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit


def _make_model(path):
    b = PBIXBuilder("FilterModel")
    b.add_table(
        "Fact",
        [
            {"name": "Cat", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
        ],
        rows=[
            {"Cat": "A", "Amount": 10.0},
            {"Cat": "B", "Amount": 20.0},
            {"Cat": "A", "Amount": 30.0},
        ],
    )
    b.add_measure("Fact", "Total", "SUM(Fact[Amount])")
    b.add_page("Page 1")
    b.save(path)


def _total(alias):
    """Evaluate the Total measure and return its numeric value."""
    raw = server.pbix_evaluate_dax(alias, "Total")
    env = json.loads(raw)
    assert env.get("success"), env
    results = env.get("results") or []
    match = next((r for r in results if r.get("name") == "Total"), None)
    assert match is not None, f"no Total result in: {env!r}"
    return float(match["value"])


def test_removed_slicer_stops_applying_its_default_filter(tmp_path):
    """The real staleness scenario: a slicer selection (Cat=A) is cached, then the
    slicer is removed from the layout. The previously-cached filter must NOT keep
    being applied — the change-stamp differs, so defaults re-derive to empty."""
    path = str(tmp_path / "m.pbix")
    _make_model(path)
    alias = "df_stale"
    server.pbix_open(path, alias)
    try:
        # A slicer selects Cat=A -> the cache legitimately holds {"Fact.Cat":["A"]}.
        _write_slicer(alias, "A")
        assert _total(alias) == pytest.approx(40.0)

        # Remove the slicer (layout now has no default selection).
        info = server._ensure_open(alias)
        layout = server._get_layout(info["work_dir"])
        layout["sections"][0]["visualContainers"] = []
        server._set_layout(info["work_dir"], layout)

        # The previously-applied Cat=A filter must be dropped -> back to 60, not 40.
        assert _total(alias) == pytest.approx(60.0)
    finally:
        server.pbix_close(alias, force=True)


def test_fresh_layout_default_filter_is_applied(tmp_path, monkeypatch):
    path = str(tmp_path / "m.pbix")
    _make_model(path)
    alias = "df_fresh"
    server.pbix_open(path, alias)
    try:
        # Prime the context; its cached default_filters is empty (no slicer yet).
        ctx = server._get_dax_context(alias)
        assert not ctx.get("default_filters")

        # Simulate a slicer selection now present in the layout by making the
        # fresh derivation return Cat=A. If evaluation trusted the (empty) cache
        # the result would be 60; re-deriving fresh applies the filter -> 40.
        monkeypatch.setattr(server, "_get_all_default_filters", lambda wd: {"Fact.Cat": ["A"]})
        # Invalidate the change-stamp so the mtime cache re-derives.
        ctx.pop("_default_filters_stamp", None)
        assert _total(alias) == pytest.approx(40.0)
    finally:
        server.pbix_close(alias, force=True)


def _slicer_container(entity, prop, value):
    """A minimal visualContainer whose slicer selects `entity.prop == value`,
    in the exact shape _extract_default_filters_dict parses (In-type filter)."""
    filt = {
        "singleVisual": {
            "visualType": "slicer",
            "objects": {
                "general": [
                    {
                        "properties": {
                            "filter": {
                                "filter": {
                                    "From": [{"Name": "s", "Entity": entity}],
                                    "Where": [
                                        {
                                            "Condition": {
                                                "In": {
                                                    "Expressions": [
                                                        {
                                                            "Column": {
                                                                "Expression": {"SourceRef": {"Source": "s"}},
                                                                "Property": prop,
                                                            }
                                                        }
                                                    ],
                                                    "Values": [[{"Literal": {"Value": f"'{value}'"}}]],
                                                }
                                            }
                                        }
                                    ],
                                }
                            }
                        }
                    }
                ]
            },
        }
    }
    return {"config": json.dumps(filt)}


def _write_slicer(alias, value):
    """Inject a slicer selecting Fact.Cat == value into page 0 of the layout."""
    info = server._ensure_open(alias)
    layout = server._get_layout(info["work_dir"])
    sections = layout.setdefault("sections", [{}])
    sections[0]["visualContainers"] = [_slicer_container("Fact", "Cat", value)]
    server._set_layout(info["work_dir"], layout)


def test_end_to_end_slicer_edit_is_reflected_on_next_evaluate(tmp_path):
    """Real end-to-end: a slicer written into the on-disk layout is picked up by
    the very next evaluate, and CHANGING it is reflected (no stale cache),
    exercising the actual layout parser (not a monkeypatch)."""
    path = str(tmp_path / "m.pbix")
    _make_model(path)
    alias = "df_e2e"
    server.pbix_open(path, alias)
    try:
        # No slicer yet -> full total.
        assert _total(alias) == pytest.approx(60.0)

        # Slicer selects Cat=A -> only the two A rows (10 + 30).
        _write_slicer(alias, "A")
        assert _total(alias) == pytest.approx(40.0)

        # Change the slicer to Cat=B -> the single B row (20). The layout file
        # changed on disk, so the change-stamp differs and defaults re-derive;
        # a stale cache would still report 40.
        _write_slicer(alias, "B")
        assert _total(alias) == pytest.approx(20.0)
    finally:
        server.pbix_close(alias, force=True)


def test_unchanged_layout_reuses_cached_default_filters(tmp_path, monkeypatch):
    """Steady state: with the layout unchanged between evaluations, the expensive
    layout re-derivation runs at most once (the rest are served from the
    change-stamp cache) so the DAX hot path stays cheap."""
    path = str(tmp_path / "m.pbix")
    _make_model(path)
    alias = "df_reuse"
    server.pbix_open(path, alias)
    try:
        server.pbix_evaluate_dax(alias, "Total")  # prime
        calls = {"n": 0}
        real = server._get_all_default_filters

        def counting(wd):
            calls["n"] += 1
            return real(wd)

        monkeypatch.setattr(server, "_get_all_default_filters", counting)
        for _ in range(5):
            server.pbix_evaluate_dax(alias, "Total")
        # Layout never changed -> no full re-derivation across the 5 evaluations.
        assert calls["n"] == 0, f"expected cached reuse, re-derived {calls['n']} times"
    finally:
        server.pbix_close(alias, force=True)
