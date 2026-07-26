"""Sort-by-column: "Month Name" ordered by "Month Number".

The model stores this as `Column.SortByColumnID` — an ID, not a name — so it
was unreachable through `pbix_datamodel_modify_column`, which sets a property
to a literal value. Without it, any text column that is not alphabetical
(month names, weekday names, size labels) sorts wrongly in every visual.
"""
import json
import os
import shutil
import uuid

import pytest

from pbix_mcp import server

CORPUS = os.environ.get("PBIX_TEST_SAMPLES", "test_corpus")
SAMPLE = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")

pytestmark = pytest.mark.skipif(
    not os.path.exists(SAMPLE),
    reason="needs the public test corpus (scripts/download_test_corpus.py)")


@pytest.fixture
def model(tmp_path):
    path = str(tmp_path / "m.pbix")
    shutil.copy(SAMPLE, path)
    alias = "sb_" + uuid.uuid4().hex[:8]
    server.pbix_open(path, alias)
    yield alias, path, tmp_path
    try:
        server.pbix_close(alias, force=True)
    except Exception:
        pass


def _ok(raw):
    d = json.loads(raw)
    assert d["success"], d
    return d


def _sort_map(alias):
    d = _ok(server.pbix_get_sort_by_columns(alias))
    return {(r["table"], r["column"]): r["sort_by"]
            for r in d["data"]["sort_by"]}


def _two_columns(alias):
    """Find a table with at least two plain data columns.

    Discovered rather than hardcoded — the corpus files have different
    schemas, and a test that names one file's tables silently stops covering
    anything when the fixture changes.
    """
    d = _ok(server.pbix_datamodel_query_metadata(
        alias,
        "SELECT t.Name, COALESCE(c.ExplicitName, c.InferredName) "
        "FROM [Column] c JOIN [Table] t ON c.TableID = t.ID "
        # 0, not NULL, is how a Desktop model stores "no sort-by".
        "WHERE c.Type = 1 AND COALESCE(c.SortByColumnID, 0) = 0 "
        "ORDER BY t.Name"))
    by_table: dict = {}
    for line in d["message"].splitlines()[2:]:
        if "|" not in line:
            continue
        table, col = (p.strip() for p in line.split("|", 1))
        by_table.setdefault(table, []).append(col)
        if len(by_table[table]) >= 2:
            return table, by_table[table][0], by_table[table][1]
    pytest.skip("no table in this corpus file has two plain data columns")


class TestReadSortBy:
    def test_reads_desktop_authored_sort_by(self, model):
        """The corpus files carry Desktop-authored sort-by relationships."""
        alias, _p, _t = model
        found = _sort_map(alias)
        assert found, "expected at least one Desktop-authored sort-by"
        for (table, col), by in found.items():
            assert table and col and by

    def test_names_resolve_for_inferred_name_columns(self, model):
        """Auto date-hierarchy columns have no ExplicitName; a naive query
        reports them all as None."""
        alias, _p, _t = model
        for (table, col), by in _sort_map(alias).items():
            assert col is not None and by is not None, (table, col, by)


class TestSetSortBy:
    def test_set_then_read_back(self, model):
        alias, _p, _t = model
        table, col, by = _two_columns(alias)
        _ok(server.pbix_set_sort_by_column(alias, table, col, by))
        assert _sort_map(alias)[(table, col)] == by

    def test_survives_save_and_reopen(self, model):
        alias, _p, tmp_path = model
        table, col, by = _two_columns(alias)
        _ok(server.pbix_set_sort_by_column(alias, table, col, by))
        out = str(tmp_path / "saved.pbix")
        server.pbix_save(alias, out, overwrite=True, backup=False)

        alias2 = "sb2_" + uuid.uuid4().hex[:8]
        server.pbix_open(out, alias2)
        try:
            assert _sort_map(alias2)[(table, col)] == by
        finally:
            server.pbix_close(alias2, force=True)

    def test_clear_sort_by(self, model):
        alias, _p, _t = model
        existing = list(_sort_map(alias))
        assert existing, "fixture has no sort-by to clear"
        table, col = existing[0]
        _ok(server.pbix_set_sort_by_column(alias, table, col, ""))
        assert (table, col) not in _sort_map(alias)

    def test_self_sort_rejected(self, model):
        alias, _p, _t = model
        table, col, _by = _two_columns(alias)
        d = json.loads(server.pbix_set_sort_by_column(alias, table, col, col))
        assert not d["success"]
        assert "itself" in d["message"].lower()

    def test_cycle_rejected(self, model):
        """A sorts by B while B sorts by A has no defined order; Power BI
        refuses it and so must we, or the model opens broken."""
        alias, _p, _t = model
        table, col, by = _two_columns(alias)
        _ok(server.pbix_set_sort_by_column(alias, table, col, by))
        d = json.loads(server.pbix_set_sort_by_column(alias, table, by, col))
        assert not d["success"]
        assert "cycle" in d["message"].lower()

    def test_unknown_column_rejected(self, model):
        alias, _p, _t = model
        table, _col, by = _two_columns(alias)
        d = json.loads(server.pbix_set_sort_by_column(
            alias, table, "NoSuchColumn", by))
        assert not d["success"]

    def test_unknown_sort_by_column_rejected(self, model):
        alias, _p, _t = model
        table, col, _by = _two_columns(alias)
        d = json.loads(server.pbix_set_sort_by_column(
            alias, table, col, "NoSuchColumn"))
        assert not d["success"]

    def test_failed_call_leaves_the_model_untouched(self, model):
        alias, _p, _t = model
        before = _sort_map(alias)
        table, col, _by = _two_columns(alias)
        json.loads(server.pbix_set_sort_by_column(
            alias, table, col, "NoSuchColumn"))
        assert _sort_map(alias) == before
