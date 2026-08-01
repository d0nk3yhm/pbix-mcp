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
