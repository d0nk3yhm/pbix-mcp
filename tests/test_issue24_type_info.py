"""Issue #24 (r22): type information lost/wrong on the way out of the engine.

r22#1 — datetimes left pbix_evaluate_dax in two shapes: midnight ones as ISO
strings, with-time ones as bare OLE serials (DATE(2026,8,2)+TIME(14,5,9) ->
46236.58...), indistinguishable from numbers; and no type field existed.
r22#2 — add_measure inferred Double (8) for datetime measures instead of 9.
r22#3 — a bad SQL in pbix_datamodel_query_metadata was masked by WinError 32
(temp .db removed while the sqlite handle was open — Windows-only).
"""
import json
from datetime import datetime

import pytest

from pbix_mcp import server as S
from pbix_mcp.builder import (
    MEASURE_DT_DATETIME,
    MEASURE_DT_DOUBLE,
    MEASURE_DT_STRING,
    infer_measure_data_type,
)
from pbix_mcp.models.responses import DAXResult

# ---- r22#2: DateTime inference (unit) ----

@pytest.mark.parametrize("expr", [
    "DATE(2026,8,2)+TIME(14,5,9)",
    "TODAY()",
    "NOW()",
    "EOMONTH(TODAY(), 0)",
    "LASTDATE('Date'[Date])",
    "STARTOFMONTH('Date'[Date])",
    "TODAY() + 7",
    "IF(TRUE(), TODAY(), BLANK())",
    "VAR d = DATE(2026,1,1) RETURN d + 1",
])
def test_datetime_expressions_infer_datetime(expr):
    assert infer_measure_data_type(expr) == MEASURE_DT_DATETIME


@pytest.mark.parametrize("expr,want", [
    ("SUM(S[V])", MEASURE_DT_DOUBLE),
    ("TODAY() - DATE(2026,1,1)", MEASURE_DT_DOUBLE),   # dt - dt = days
    ("DATEDIFF(DATE(2026,1,1), TODAY(), DAY)", MEASURE_DT_DOUBLE),
    ("YEAR(TODAY())", MEASURE_DT_DOUBLE),
    ('FORMAT(TODAY(), "yyyy-MM")', MEASURE_DT_STRING),
])
def test_non_datetime_expressions_stay_correct(expr, want):
    assert infer_measure_data_type(expr) == want


# ---- r22#1: DAXResult data_type derivation (unit) ----

def test_daxresult_derives_types():
    assert DAXResult(name="a", value=1.5).data_type == "Double"
    assert DAXResult(name="b", value="x").data_type == "String"
    assert DAXResult(name="c", value=True).data_type == "Boolean"
    assert DAXResult(name="d", value=datetime(2026, 8, 2)).data_type == "DateTime"
    assert DAXResult(name="e", value=None).data_type is None
    assert DAXResult(name="f", value=float("inf")).data_type == "Double"


# ---- integration: the reporter's exact scenario ----

@pytest.fixture()
def model(tmp_path):
    p = str(tmp_path / "r22.pbix")
    S.pbix_create(p, "r22t", tables_json=json.dumps(
        [{"name": "S", "columns": [{"name": "A", "data_type": "Int64"}],
          "rows": [{"A": 1}]}]))
    yield "r22t"
    S.pbix_close("r22t")


def test_datetimes_return_one_shape_with_type(model):
    S.pbix_datamodel_add_measure(model, "S", "q_nodate", "DATE(2026,8,2)")
    S.pbix_datamodel_add_measure(model, "S", "z_dt_full",
                                 "DATE(2026,8,2)+TIME(14,5,9)")
    r = json.loads(S.pbix_evaluate_dax(model, "q_nodate,z_dt_full"))
    by = {x["name"]: x for x in r["results"]}
    assert by["q_nodate"]["value"] == "2026-08-02T00:00:00"
    # The with-time case was the bug: bare serial 46236.586909722224.
    assert by["z_dt_full"]["value"] == "2026-08-02T14:05:09"
    assert by["q_nodate"]["data_type"] == "DateTime"
    assert by["z_dt_full"]["data_type"] == "DateTime"


def test_add_measure_stores_datetime_datatype(model):
    out = json.loads(S.pbix_datamodel_add_measure(
        model, "S", "z_dt", "DATE(2026,8,2)+TIME(14,5,9)"))
    assert "DateTime (inferred)" in out["message"]
    meta = json.loads(S.pbix_datamodel_query_metadata(
        model, "SELECT DataType FROM Measure WHERE Name = 'z_dt'"))
    assert "9" in meta["message"]


def test_query_metadata_bad_sql_shows_real_error(model):
    r = json.loads(S.pbix_datamodel_query_metadata(
        model, "SELECT * FROM __nope__"))
    assert not r["success"]
    assert "no such table" in r["message"]
    assert "WinError 32" not in r["message"]
