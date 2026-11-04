"""#366: every tool says what it does, and the read-only ones are read-only.

Anthropic's Software Directory Policy (5.E) requires MCP tools to carry their
annotations, in particular readOnlyHint, destructiveHint and title. A client can
act on readOnlyHint without asking anyone, so a tool wrongly marked read-only is
worse than one not marked: each is run here against an open file, and neither
the file on disk, nor its working copy, nor its unsaved-changes flag may move.

#368: the server does not take a trademarked name; its title and the names it
reports are ours.

Expected values: the MCP specification's meaning of the hints, and by
construction (a digest of the same files before and after).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os

import pytest

from pbix_mcp import server

pytestmark = pytest.mark.unit


def _tools() -> list:
    return asyncio.run(server.mcp.list_tools())


def _digest(root: str) -> dict:
    found = {}
    for folder, _, names in os.walk(root):
        for name in names:
            path = os.path.join(folder, name)
            with open(path, "rb") as f:
                found[os.path.relpath(path, root)] = hashlib.sha256(f.read()).hexdigest()
    return found


@pytest.fixture
def opened(tmp_path):
    tables = [{"name": "T", "columns": [{"name": "K", "data_type": "String"}, {"name": "A", "data_type": "Int64"}],
               "rows": [{"K": "x", "A": 1}, {"K": "y", "A": 2}, {"K": "x", "A": 4}]}]
    measures = [{"table": "T", "name": "Total", "expression": "SUM(T[A])"}]
    paths = {}
    for alias in ("a", "b"):
        paths[alias] = str(tmp_path / f"{alias}.pbix")
        created = json.loads(server.pbix_create(paths[alias], alias, json.dumps(tables), json.dumps(measures)))
        assert created["success"], created
        server.pbix_add_page(alias, "One")
        assert json.loads(server.pbix_add_visual(alias, 0, "card", 0, 0, 200, 100))["success"]
        assert json.loads(server.pbix_set_rls_role(alias, "R", "T", "T[A] > 1"))["success"]
        assert json.loads(server.pbix_save(alias, overwrite=True))["success"]
    yield paths
    for alias in list(server._open_files):
        server.pbix_close(alias, force=True)


def _read_only_calls(tmp_path, paths) -> dict:
    """One real call of each read-only tool, on the file the fixture opened."""
    a = {"alias": "a"}
    return {
        "pbix_report_format": a,
        "pbix_list_open": {},
        "pbix_get_pages": a,
        "pbix_get_page_visuals": {**a, "page_index": 0},
        "pbix_get_visual_detail": {**a, "page_index": 0, "visual_index": 0},
        "pbix_get_layout_raw": a,
        "pbix_get_filters": a,
        "pbix_get_settings": a,
        "pbix_get_bookmarks": a,
        "pbix_get_metadata": a,
        "pbix_list_resources": a,
        "pbix_get_html_visual": a,
        "pbix_html_template": {"kind": "kpi_card", "spec_json": json.dumps({"title": "t", "value": "1"})},
        "pbix_get_theme": a,
        "pbix_extract_colors": a,
        "pbix_get_linguistic_schema": a,
        "pbix_get_m_code": a,
        "pbix_get_model_schema": a,
        "pbix_get_model_measures": a,
        "pbix_get_model_relationships": a,
        "pbix_get_model_power_query": a,
        "pbix_list_data_sources": a,
        "pbix_get_model_columns": a,
        "pbix_get_table_data": {**a, "table_name": "T"},
        "pbix_find_value": {**a, "search_value": "x"},
        "pbix_query_table": {**a, "table_name": "T"},
        "pbix_table_stats": {**a, "table_name": "T"},
        "pbix_data_diff": {"alias_a": "a", "alias_b": "b", "table_name": "T", "key_columns": "K"},
        "pbix_list_tables": a,
        "pbix_datamodel_query_metadata": {**a, "sql_query": "SELECT Name FROM [Table]"},
        "pbix_get_sort_by_columns": a,
        "pbix_datamodel_list_abf_files": a,
        "pbix_evaluate_dax": {**a, "measures": "Total"},
        "pbix_evaluate_dax_batch": {**a, "measures": "Total", "contexts": json.dumps([{}])},
        "pbix_evaluate_dax_per_dimension": {**a, "measures": "Total", "dimension": "T.K"},
        "pbix_evaluate_dax_grouped": {**a, "measures": "Total", "group_by": "T.K"},
        "pbix_get_default_filters": {**a, "page_index": 0},
        "pbix_get_visual_positions": {**a, "page_index": 0},
        "pbix_get_rls_roles": a,
        "pbix_evaluate_rls": {**a, "role_name": "R", "table_name": "T"},
        "pbix_get_perspectives": a,
        "pbix_get_hierarchies": a,
        "pbix_get_cultures": a,
        "pbix_get_partitions": a,
        "pbix_get_password": a,
        "pbix_performance": a,
        "pbix_diff": {"alias_a": "a", "alias_b": "b"},
        "pbix_doctor": a,
        "pbix_get_incremental_refresh": a,
    }


def test_every_tool_declares_all_it_is_asked_to():
    tools = _tools()
    assert len(tools) >= 134
    for tool in tools:
        hints = tool.annotations
        assert hints is not None, tool.name
        assert hints.title, tool.name
        assert isinstance(hints.readOnlyHint, bool), tool.name
        assert isinstance(hints.destructiveHint, bool), tool.name
        assert isinstance(hints.idempotentHint, bool), tool.name
        assert hints.openWorldHint is False, tool.name  # the package makes no network call
        assert not (hints.readOnlyHint and hints.destructiveHint), tool.name
        assert len(tool.name) <= 64


def test_titles_tell_tools_apart_and_use_no_trademarked_name():
    titles = [tool.annotations.title for tool in _tools()]
    assert len(set(titles)) == len(titles)
    for title in titles:
        assert not any(word in title.lower() for word in ("power bi", "powerbi", "microsoft", "excel"))


def test_the_server_does_not_call_itself_by_a_trademarked_name():
    for text in (server.mcp.name, server.mcp.instructions):
        assert "powerbi" not in text.lower().replace(" ", "")
        assert "microsoft" not in text.lower()


@pytest.mark.parametrize("name", [
    "pbix_remove_page", "pbix_save", "pbix_close", "pbix_create", "pbix_export_pbip", "pbix_export_table_csv",
    "pbix_datamodel_remove_measure", "pbix_datamodel_modify_metadata", "pbix_set_table_data",
])
def test_what_can_lose_something_is_marked_destructive(name):
    # pbix_close discards unsaved edits with force=True; pbix_create and the
    # exports replace a file already at the path, and pbix_export_pbip a folder
    hints = next(t for t in _tools() if t.name == name).annotations
    assert hints.readOnlyHint is False and hints.destructiveHint is True


def test_every_read_only_tool_leaves_the_file_and_its_working_copy_alone(opened, tmp_path):
    calls = _read_only_calls(tmp_path, opened)
    # opening is read-only too, and has a test of its own below
    read_only = {t.name for t in _tools() if t.annotations.readOnlyHint}
    assert read_only - {"pbix_open", "pbix_open_pbip"} == set(calls)

    info = server._open_files["a"]
    source = _digest(os.path.dirname(opened["a"]))
    working = _digest(info["work_dir"])
    assert info.get("modified") is False
    for name, kwargs in calls.items():
        answer = getattr(server, name)(**kwargs)
        assert '"success":false' not in answer.replace(" ", ""), (name, answer[:300])
        assert _digest(os.path.dirname(opened["a"])) == source, name
        assert _digest(info["work_dir"]) == working, name
        assert info.get("modified") is False, name


def test_opening_changes_neither_a_file_nor_a_project_folder(opened, tmp_path):
    before = _digest(os.path.dirname(opened["a"]))
    assert json.loads(server.pbix_open(opened["a"], "again"))["success"]
    assert _digest(os.path.dirname(opened["a"])) == before

    project = tmp_path / "project"
    assert json.loads(server.pbix_export_pbip("a", str(project)))["success"]
    before = _digest(str(project))
    assert json.loads(server.pbix_open_pbip(str(project), "folder"))["success"]
    assert _digest(str(project)) == before
    assert server._open_files["folder"].get("modified") is False
