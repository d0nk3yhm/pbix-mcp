"""Rebuild-path edits on models that contain calculated tables/columns.

A from-scratch rebuild reconstructs the model from data, which drops Type=2
calculated columns and demotes calculated tables to plain data. Rather than
corrupt the file, every rebuild-path edit used to be REFUSED on such a model —
which is three of the four reports in the public corpus. Adding a table,
adding a relationship or removing a table was therefore impossible on most
real files.

`_rebuild_preserving_calc` now plans the re-materialization first, so the edit
lands with the calc objects intact. These tests pin BOTH halves of that: the
edit succeeds, AND the calculated objects come out the other side unchanged.
The second half is the one that matters — an edit that "succeeds" by quietly
dropping a calculated table is worse than the refusal it replaced.
"""
import json
import os
import shutil
import uuid

import pytest

from pbix_mcp import server

CORPUS = os.environ.get("PBIX_TEST_SAMPLES", "test_corpus")
# GeoSales is the interesting fixture: it has a calculated COLUMN
# (fct_Orders[Discount Group]) *and* five calculated TABLES.
SAMPLE = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not os.path.exists(SAMPLE),
        reason="needs the public test corpus (scripts/download_test_corpus.py)"),
]

CALC_COLUMNS_SQL = (
    "SELECT t.Name || '[' || COALESCE(c.ExplicitName, c.InferredName) || ']=' "
    "|| COALESCE(c.Expression, '') "
    "FROM [Column] c JOIN [Table] t ON c.TableID = t.ID "
    "WHERE c.Type = 2 AND c.ExplicitName NOT LIKE 'RowNumber%' ORDER BY 1")
CALC_TABLES_SQL = (
    "SELECT t.Name || '::' || COALESCE(p.QueryDefinition, '') "
    "FROM [Partition] p JOIN [Table] t ON p.TableID = t.ID "
    "WHERE p.Type = 2 ORDER BY 1")


def _ok(raw):
    d = json.loads(raw)
    assert d["success"], d
    return d


def _calc_state(alias):
    return (
        _ok(server.pbix_datamodel_query_metadata(alias, CALC_COLUMNS_SQL))["message"],
        _ok(server.pbix_datamodel_query_metadata(alias, CALC_TABLES_SQL))["message"],
    )


@pytest.fixture
def model(tmp_path):
    path = str(tmp_path / "m.pbix")
    shutil.copy(SAMPLE, path)
    alias = "cp_" + uuid.uuid4().hex[:8]
    server.pbix_open(path, alias)
    yield alias, tmp_path
    try:
        server.pbix_close(alias, force=True)
    except Exception:
        pass


def _reopen(alias, tmp_path):
    out = str(tmp_path / f"o_{uuid.uuid4().hex[:6]}.pbix")
    server.pbix_save(alias, out, overwrite=True, backup=False)
    server.pbix_close(alias, force=True)
    alias2 = "cp2_" + uuid.uuid4().hex[:8]
    server.pbix_open(out, alias2)
    return alias2


NEW_TABLE = json.dumps({
    "columns": [{"name": "Region", "data_type": "String"}],
    "rows": [{"Region": "North"}, {"Region": "South"}],
})


class TestFixtureIsInteresting:
    def test_sample_really_has_calc_objects(self, model):
        """Guards the guard: if the fixture had no calculated objects, every
        preservation assertion below would pass vacuously."""
        alias, _ = model
        cols, tables = _calc_state(alias)
        assert "Discount Group" in cols
        assert "topProductsSelection" in tables


class TestAddTable:
    def test_add_table_now_succeeds(self, model):
        alias, _ = model
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))

    def test_calc_objects_survive_unchanged(self, model):
        alias, tmp_path = model
        before = _calc_state(alias)
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            assert _calc_state(alias2) == before
        finally:
            server.pbix_close(alias2, force=True)

    def test_new_table_is_readable_after_reopen(self, model):
        alias, tmp_path = model
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            d = _ok(server.pbix_get_table_data(alias2, "BrandNewTable"))
            assert "North" in d["message"] and "South" in d["message"]
        finally:
            server.pbix_close(alias2, force=True)

    def test_relationships_are_not_lost(self, model):
        """A relationship onto a calc-table column has no ExplicitName; reading
        only that gave an endpoint of None and the rebuild rejected it."""
        alias, tmp_path = model
        before = _ok(server.pbix_get_model_relationships(alias))["message"]
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            after = _ok(server.pbix_get_model_relationships(alias2))["message"]
            assert after.count("\n") == before.count("\n")
        finally:
            server.pbix_close(alias2, force=True)

    def test_measures_are_not_lost(self, model):
        alias, tmp_path = model
        before = _ok(server.pbix_get_model_measures(alias))["message"]
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            after = _ok(server.pbix_get_model_measures(alias2))["message"]
            assert after.count("\n") == before.count("\n")
        finally:
            server.pbix_close(alias2, force=True)


class TestCalcColumnValues:
    def test_values_match_the_dax(self, model):
        """The calc column is re-evaluated from its DAX during preservation.
        Its expression buckets fct_Orders[Discount] into four bands, so a
        wrong evaluation shows up as the wrong number of distinct values."""
        alias, tmp_path = model
        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            stats = _ok(server.pbix_table_stats(alias2, "fct_Orders"))["message"]
            lines = stats.split("\n")
            i = next(n for n, ln in enumerate(lines)
                     if ln.startswith("## Discount Group"))
            block = "\n".join(lines[i:i + 5])
            assert "distinct=4" in block, block
            for band in ("0% - 15%", "15% - 30%", "30% - 50%"):
                assert band in block, block
            # Every row gets a value — a partial materialization would show
            # nulls or a short count.
            assert "nulls=0" in block, block
        finally:
            server.pbix_close(alias2, force=True)

    def test_source_column_is_untouched(self, model):
        alias, tmp_path = model
        before = _ok(server.pbix_table_stats(alias, "fct_Orders"))["message"]
        i = before.split("\n").index(
            next(ln for ln in before.split("\n") if ln.startswith("## Discount (")))
        src_before = "\n".join(before.split("\n")[i:i + 4])

        _ok(server.pbix_set_table_data(alias, "BrandNewTable", NEW_TABLE))
        alias2 = _reopen(alias, tmp_path)
        try:
            after = _ok(server.pbix_table_stats(alias2, "fct_Orders"))["message"]
            j = after.split("\n").index(
                next(ln for ln in after.split("\n") if ln.startswith("## Discount (")))
            assert "\n".join(after.split("\n")[j:j + 4]) == src_before
        finally:
            server.pbix_close(alias2, force=True)


class TestRemoveTable:
    def test_removing_a_calc_table_still_works(self, model):
        alias, tmp_path = model
        _ok(server.pbix_datamodel_remove_table(alias, "topProductsSelection"))
        alias2 = _reopen(alias, tmp_path)
        try:
            _cols, tables = _calc_state(alias2)
            assert "topProductsSelection" not in tables
            # The other calculated tables are untouched.
            assert "DiscountGroup" in tables and "Metrics" in tables
        finally:
            server.pbix_close(alias2, force=True)

    def test_removed_table_is_not_resurrected_by_the_plan(self, model):
        """The preservation plan re-materializes calc tables; without a skip
        list it would put back the very table the caller just deleted."""
        alias, tmp_path = model
        _ok(server.pbix_datamodel_remove_table(alias, "Metrics"))
        alias2 = _reopen(alias, tmp_path)
        try:
            listed = _ok(server.pbix_list_tables(alias2))["message"]
            assert "Metrics" not in listed.split()
        finally:
            server.pbix_close(alias2, force=True)


class TestStillRefusesWhenUnsafe:
    def test_unreproducible_calc_column_is_still_refused(self, model):
        """Preservation re-evaluates calculated columns. When the engine cannot
        reproduce one, the edit must still be refused — succeeding would write
        wrong values into VertiPaq, which is worse than not editing at all."""
        alias, _ = model
        _ok(server.pbix_datamodel_modify_metadata(
            alias,
            "UPDATE [Column] SET Expression = "
            "'CALCULATE(SUM(fct_Orders[Sales]), ALL(fct_Orders))' "
            "WHERE Type = 2 AND ExplicitName = 'Discount Group'"))
        d = json.loads(server.pbix_set_table_data(alias, "Nope", NEW_TABLE))
        assert d["success"] is False, d
