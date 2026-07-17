"""Regression tests for issues found while building OpenBI (found_issues.md).

- #4  MAXID invariant: MAXID >= highest object id after a build.
- #5  pbix_save must NOT clear the modified flag on a copy-export.
- #6  pbix_get_default_filters must return a JSON envelope (not a bare string).
"""
import json
import os
import sqlite3
import tempfile
import zipfile

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


def _build_minimal_pbix(path):
    b = PBIXBuilder("T")
    b.add_table("Items", [
        {"name": "ID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "Price", "data_type": "Double"},
    ], rows=[{"ID": 1, "Name": "A", "Price": 9.99}])
    b.save(path)


def _read_maxid_and_max_object_id(pbix_path):
    """Return (declared MAXID, actual highest object id across metadata tables)."""
    with zipfile.ZipFile(pbix_path) as zf:
        abf = decompress_datamodel(zf.read("DataModel"))
    db = read_metadata_sqlite(abf)
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, db)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT Value FROM DBPROPERTIES WHERE Name='MAXID'")
        row = cur.fetchone()
        maxid = int(row[0]) if row and row[0] is not None else None
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = [r[0] for r in cur.fetchall()]
        actual_max = 0
        for n in names:
            try:
                cur.execute(f"SELECT MAX(ID) FROM [{n}]")
                m = cur.fetchone()[0]
                if m is not None:
                    actual_max = max(actual_max, int(m))
            except sqlite3.Error:
                continue
        return maxid, actual_max
    finally:
        conn.close()
        try:
            os.remove(tmp)
        except OSError:
            pass


class TestMaxIdInvariant:
    def test_builder_maxid_ge_max_object_id(self, tmp_path):
        """Issue #4: a freshly built file must satisfy MAXID >= highest id."""
        p = str(tmp_path / "maxid.pbix")
        _build_minimal_pbix(p)
        maxid, actual_max = _read_maxid_and_max_object_id(p)
        if maxid is None:
            import pytest
            pytest.skip("could not read metadata db from built pbix")
        assert maxid >= actual_max, (
            f"MAXID={maxid} < highest object id={actual_max} "
            "(Power BI would crash on TMCCollectionObject::Add)"
        )


class TestSaveModifiedFlag:
    def test_copy_export_keeps_modified(self, tmp_path):
        """Issue #5: saving to a DIFFERENT path must not mark the session clean."""
        p = str(tmp_path / "orig.pbix")
        _build_minimal_pbix(p)
        alias = "issue5"
        try:
            server.pbix_open(p, alias)
            # make an edit (sets modified=True)
            server.pbix_add_page(alias, "Page X")
            assert server._open_files[alias]["modified"] is True

            # export a COPY to a different path
            copy_path = str(tmp_path / "copy.pbix")
            server.pbix_save(alias, output_path=copy_path)
            # the ORIGINAL still has unsaved edits -> must stay modified
            assert server._open_files[alias]["modified"] is True, (
                "copy-export wrongly cleared the modified flag (data-loss risk)"
            )

            # saving back to the original clears it
            server.pbix_save(alias)
            assert server._open_files[alias]["modified"] is False
        finally:
            server._open_files.pop(alias, None)

    def test_close_after_copy_export_refuses(self, tmp_path):
        """A copy-export must not let pbix_close silently discard work."""
        p = str(tmp_path / "orig2.pbix")
        _build_minimal_pbix(p)
        alias = "issue5b"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "Page Y")
            server.pbix_save(alias, output_path=str(tmp_path / "copy2.pbix"))
            # close without force must refuse (still modified)
            result = server.pbix_close(alias, force=False)
            assert "unsaved" in result.lower() or "modified" in result.lower()
            assert alias in server._open_files  # not closed
        finally:
            server._open_files.pop(alias, None)


class TestGroupedVisualWriteCoords:
    def test_child_stored_group_relative(self, tmp_path):
        """Issue #8: adding a visual under a singleVisualGroup stores group-relative x/y."""
        p = str(tmp_path / "grp.pbix")
        _build_minimal_pbix(p)
        alias = "issue8"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]

            # Inject a singleVisualGroup container at absolute (200, 100).
            layout = server._get_layout(wd)
            sections = layout.get("sections") or layout.get("pages") or []
            page = sections[0]
            group_cfg = {"name": "grp1", "singleVisualGroup": {"displayName": "G"}}
            page.setdefault("visualContainers", []).append({
                "x": 200, "y": 100, "z": 0, "width": 400, "height": 300,
                "config": json.dumps(group_cfg),
            })
            server._set_layout(wd, layout)

            # Add a child visual at ABSOLUTE (250, 150) -> expect stored (50, 50).
            child_cfg = json.dumps({"parentGroupName": "grp1"})
            server.pbix_add_visual(alias, 0, "card", x=250, y=150,
                                   width=100, height=80, config_json=child_cfg)

            layout2 = server._get_layout(wd)
            page2 = (layout2.get("sections") or layout2.get("pages"))[0]
            child = None
            for vc in page2["visualContainers"]:
                cfg = json.loads(vc.get("config", "{}"))
                if cfg.get("parentGroupName") == "grp1":
                    child = vc
                    break
            assert child is not None, "child visual not found"
            assert child["x"] == 50 and child["y"] == 50, (
                f"expected group-relative (50,50), got ({child['x']},{child['y']})"
            )
        finally:
            server._open_files.pop(alias, None)

    def test_top_level_visual_still_absolute(self, tmp_path):
        """A visual with no parent group keeps absolute page coords (regression)."""
        p = str(tmp_path / "top.pbix")
        _build_minimal_pbix(p)
        alias = "issue8b"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]
            server.pbix_add_visual(alias, 0, "card", x=120, y=90,
                                   width=100, height=80)
            layout = server._get_layout(wd)
            page = (layout.get("sections") or layout.get("pages"))[0]
            vc = page["visualContainers"][-1]
            assert vc["x"] == 120 and vc["y"] == 90
        finally:
            server._open_files.pop(alias, None)


class TestDefaultFiltersEnvelope:
    def test_returns_json_envelope(self, tmp_path):
        """Issue #6: success path must be valid JSON like every other tool."""
        p = str(tmp_path / "df.pbix")
        _build_minimal_pbix(p)
        alias = "issue6"
        try:
            server.pbix_open(p, alias)
            result = server.pbix_get_default_filters(alias, 0)
            parsed = json.loads(result)  # must not raise
            assert parsed["success"] is True
            assert "data" in parsed
            assert "filters" in parsed["data"]
        finally:
            server._open_files.pop(alias, None)
