"""Issue #53: names that collide case-insensitively build a file Desktop
refuses to open.

`pbix_create` accepted a table with columns DEST_WAC, Dest_WAC and n. It
succeeded, both columns got distinct internal ids, and every engine-side
readback (pbix_get_model_schema included) showed two columns — but Power BI
Desktop refused the saved file:

    The Column with the name of 'Dest_WAC' already exists in the 'T' Table.

Analysis Services treats table and column names as case-INSENSITIVE. Same
failure class as #43 (case-colliding VALUES in one column), one level up,
and worse in one respect: #43 failed loudly at a later engine stage, while
this produced a file nothing engine-side could fault, so the first symptom
was a customer double-clicking the .pbix.

QlikView field names ARE case-sensitive, so this is a real corpus shape:
DEST_WAC (a code) and Dest_WAC (its label) are two legitimate fields.
"""
import json
import os

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder

pytestmark = pytest.mark.unit


#: The issue's own repro: three columns, two rows.
_ISSUE_TABLE = {
    "name": "T",
    "columns": [{"name": "DEST_WAC", "data_type": "String"},
                {"name": "Dest_WAC", "data_type": "String"},
                {"name": "n", "data_type": "Int64"}],
    "rows": [{"DEST_WAC": "1", "Dest_WAC": "US", "n": 1},
             {"DEST_WAC": "2", "Dest_WAC": "CA", "n": 2}],
}


class TestCreateRejectsCollidingColumnNames:
    def test_issue_repro_is_refused_and_no_file_is_written(self, tmp_path):
        p = str(tmp_path / "i53.pbix")
        r = json.loads(server.pbix_create(
            p, "i53a", json.dumps([_ISSUE_TABLE])))
        assert not r.get("success"), r
        msg = r["message"]
        # both spellings named, so the caller can decide which to keep
        assert "'DEST_WAC'" in msg and "'Dest_WAC'" in msg
        assert "case" in msg.lower()
        # a refused build must not leave a half-written file behind
        assert not os.path.exists(p)

    def test_exact_duplicate_column_name_is_refused_too(self, tmp_path):
        r = json.loads(server.pbix_create(
            str(tmp_path / "dup.pbix"), "i53b", json.dumps([{
                "name": "T",
                "columns": [{"name": "A", "data_type": "String"},
                            {"name": "A", "data_type": "String"}],
                "rows": [{"A": "x"}]}])))
        assert not r.get("success")
        assert "duplicate" in r["message"].lower()

    def test_distinct_names_still_build(self, tmp_path):
        """The check must not fire on names that merely share a prefix or
        differ by more than case."""
        p = str(tmp_path / "ok.pbix")
        r = json.loads(server.pbix_create(p, "i53c", json.dumps([{
            "name": "T",
            "columns": [{"name": "DEST_WAC", "data_type": "String"},
                        {"name": "DEST_WAC_LABEL", "data_type": "String"},
                        {"name": "n", "data_type": "Int64"}],
            "rows": [{"DEST_WAC": "1", "DEST_WAC_LABEL": "US", "n": 1}]}])))
        try:
            assert r.get("success"), r
            assert os.path.exists(p)
        finally:
            server._open_files.pop("i53c", None)
            server._dax_cache.pop("i53c", None)

    def test_collision_across_tables_is_fine(self, tmp_path):
        """The rule is per-table: two DIFFERENT tables may each have a
        column called Region, and case differences between them are not a
        collision either."""
        p = str(tmp_path / "two.pbix")
        r = json.loads(server.pbix_create(p, "i53d", json.dumps([
            {"name": "A", "columns": [{"name": "Region",
                                       "data_type": "String"}],
             "rows": [{"Region": "W"}]},
            {"name": "B", "columns": [{"name": "REGION",
                                       "data_type": "String"}],
             "rows": [{"REGION": "E"}]}])))
        try:
            assert r.get("success"), r
        finally:
            server._open_files.pop("i53d", None)
            server._dax_cache.pop("i53d", None)


class TestBuilderRejectsCollidingTableNames:
    def test_table_names_differing_only_by_case_are_refused(self):
        b = PBIXBuilder("M")
        b.add_table("Sales", [{"name": "A", "data_type": "String"}],
                    rows=[{"A": "x"}])
        b.add_table("SALES", [{"name": "B", "data_type": "String"}],
                    rows=[{"B": "y"}])
        with pytest.raises(ValueError) as e:
            b.build()
        assert "'Sales'" in str(e.value) and "'SALES'" in str(e.value)


class TestSetTableDataRejectsCollision:
    """pbix_set_table_data rebuilds through the same builder, so the same
    invariant must stop a collision introduced by an EDIT — not just at
    create time."""

    @pytest.fixture()
    def opened(self, tmp_path):
        alias = "i53e"
        p = str(tmp_path / "base.pbix")
        assert json.loads(server.pbix_create(p, alias, json.dumps([{
            "name": "T",
            "columns": [{"name": "Region", "data_type": "String"}],
            "rows": [{"Region": "W"}]}])))["success"]
        yield alias
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)

    def test_edit_that_introduces_a_collision_is_refused(self, opened):
        r = json.loads(server.pbix_set_table_data(opened, "T", json.dumps({
            "columns": [{"name": "Region", "data_type": "String"},
                        {"name": "REGION", "data_type": "String"}],
            "rows": [{"Region": "W", "REGION": "West"}]})))
        assert not r.get("success"), r
        assert "'Region'" in r["message"] and "'REGION'" in r["message"]


class TestDoctorFlagsExistingCollisions:
    """A file written by an older version cannot be checked any other way —
    that is exactly how this survived until a Desktop pass caught it."""

    def test_collision_planted_in_metadata_is_reported(self, tmp_path):
        """Plant the collision the way an older pbix-mcp would have left it —
        directly in the model metadata — and run the real doctor over it."""
        p = str(tmp_path / "planted.pbix")
        alias = "i53g"
        assert json.loads(server.pbix_create(p, alias, json.dumps([{
            "name": "T",
            "columns": [{"name": "Region", "data_type": "String"},
                        {"name": "Amount", "data_type": "Double"}],
            "rows": [{"Region": "W", "Amount": 1.0}]}])))["success"]
        try:
            r = json.loads(server.pbix_datamodel_modify_metadata(
                alias, "UPDATE [Column] SET ExplicitName='REGION' "
                       "WHERE ExplicitName='Amount'"))
            assert r["success"], r
            out = json.loads(server.pbix_doctor(alias))["message"]
            line = next(ln for ln in out.splitlines()
                        if "Table / column name collisions" in ln)
            assert not line.strip().startswith("✅"), line
            assert "Region" in line and "REGION" in line
            assert "refuse to open" in line
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_healthy_model_passes_the_check(self, tmp_path):
        p = str(tmp_path / "clean.pbix")
        alias = "i53f"
        assert json.loads(server.pbix_create(p, alias, json.dumps([{
            "name": "T",
            "columns": [{"name": "Region", "data_type": "String"}],
            "rows": [{"Region": "W"}]}])))["success"]
        try:
            assert json.loads(server.pbix_save(
                alias, output_path=p, overwrite=True))["success"]
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)
        alias2 = "i53f_r"
        assert json.loads(server.pbix_open(p, alias2))["success"]
        try:
            out = json.loads(server.pbix_doctor(alias2))["message"]
            line = next(ln for ln in out.splitlines()
                        if "Table / column name collisions" in ln)
            assert line.strip().startswith("✅"), line
        finally:
            server._open_files.pop(alias2, None)
            server._dax_cache.pop(alias2, None)
