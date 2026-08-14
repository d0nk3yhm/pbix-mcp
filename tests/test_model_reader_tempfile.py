"""Regression: ModelReader._query_metadata must not leak its temp SQLite handle.

The old code closed the connection only on the success path. On Windows, a query
error then left the handle open, and the `finally: os.unlink(...)` raised
"[WinError 32] the file is being used by another process" — masking the real
error (seen in the field as pbix_get_model_measures failing with WinError 32).
POSIX allows unlinking an open file, so CI (ubuntu) never surfaced it. These
tests pin: a query error surfaces the REAL error, and the happy path still works.
"""
import json
import sqlite3

import pytest

from pbix_mcp import server as S
from pbix_mcp.formats.model_reader import ModelReader


@pytest.fixture()
def model_path(tmp_path):
    path = str(tmp_path / "m.pbix")
    tables = [{"name": "T",
               "columns": [{"name": "A", "data_type": "Int64"}],
               "rows": [{"A": 1}, {"A": 2}]}]
    measures = [{"table": "T", "name": "M", "expression": "SUM(T[A])"}]
    S.pbix_create(path, "mrt", tables_json=json.dumps(tables),
                  measures_json=json.dumps(measures))
    S.pbix_close("mrt")
    return path


def test_query_error_surfaces_real_error_not_winerror(model_path):
    mr = ModelReader(model_path)
    # A bad query raises the SQLite error. Before the fix, the leaked handle made
    # the temp-file unlink raise OSError (WinError 32) on Windows, masking this.
    with pytest.raises(sqlite3.OperationalError):
        mr._query_metadata("SELECT * FROM __definitely_not_a_table__")
    # The reader is still usable afterwards — no lingering lock.
    assert any(m.get("Name") == "M" for m in mr.dax_measures)


def test_get_model_measures_tool_works(model_path):
    S.pbix_open(model_path, "mrt2")
    try:
        resp = json.loads(S.pbix_get_model_measures("mrt2"))
        assert resp["success"], resp
        assert "M" in resp["message"]
    finally:
        S.pbix_close("mrt2")


class TestStatisticsRowCount:
    """Issue #44: ModelReader.statistics took RowCount from a data column's
    IDFMETA, which for dictionary-encoded columns is the DICTIONARY entry
    count — the column's distinct count, not the table's row count. A
    500-row table whose first column held 10 distinct values reported
    '10 rows' (11 with nulls: 10 + the blank). Now read from
    ColumnStorage.Statistics_RowCount (the segment record count Power BI
    itself maintains), with the ROWNUMBER column's IDFMETA as fallback."""

    @pytest.mark.parametrize("label,rows,dtype", [
        ("few_distinct_doubles",
         [{"V": float(i % 10)} for i in range(500)], "Double"),
        ("all_distinct_doubles",
         [{"V": float(i)} for i in range(500)], "Double"),
        ("few_distinct_strings",
         [{"V": f"s{i % 10}"} for i in range(500)], "String"),
        ("few_distinct_with_nulls",
         [{"V": (None if i % 50 == 0 else float(i % 10))}
          for i in range(500)], "Double"),
    ])
    def test_row_count_is_rows_not_distincts(self, tmp_path, label, rows, dtype):
        import json as _json

        from pbix_mcp import server
        from pbix_mcp.formats.model_reader import ModelReader
        p = str(tmp_path / f"{label}.pbix")
        alias = f"st_{label}"
        r = _json.loads(server.pbix_create(p, alias, _json.dumps(
            [{"name": "T", "columns": [{"name": "V", "data_type": dtype}],
              "rows": rows}])))
        assert r.get("success"), r
        server._open_files.pop(alias, None)
        st = ModelReader(p).statistics
        assert st[0]["TableName"] == "T"
        assert st[0]["RowCount"] == 500, st

    def test_desktop_authored_counts(self):
        import os as _os

        from pbix_mcp.formats.model_reader import ModelReader
        aw = _os.path.join(_os.path.dirname(__file__), "..", "test_samples",
                           "Adventure Works DW 2020.pbix")
        if not _os.path.exists(aw):
            pytest.skip("Adventure Works sample not available")
        by_name = {t["TableName"]: t["RowCount"]
                   for t in ModelReader(aw).statistics}
        assert by_name["Sales"] == 121253
        assert by_name["Sales Territory"] == 11
        assert by_name["Currency"] == 105
