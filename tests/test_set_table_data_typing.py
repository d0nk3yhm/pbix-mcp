"""OpenBI #21: pbix_set_table_data must not silently drop a column's declared type.

A caller passing the type under "dataType" (or a lowercase name like "int64")
used to have it ignored: every column defaulted to String, so a numeric column
shipped as text, a measure aggregating it returned BLANK, and Power BI Desktop
rendered every bound visual as "Error fetching data for this visual" while the
tool reported success. The engine itself reproduces it: SUM over the text column
is BLANK. These tests pin the fix (accept dataType/type + case-insensitive names,
refuse unrecognized types) at both the unit and the tool level.
"""
import json

import pytest

from pbix_mcp import server as S
from pbix_mcp.builder import normalize_column_defs

# ---- unit: normalize_column_defs ----

def test_accepts_camelcase_key_and_lowercase_value():
    out = normalize_column_defs([{"name": "V", "dataType": "int64"}])
    assert out[0]["data_type"] == "Int64"


def test_accepts_type_key():
    out = normalize_column_defs([{"name": "C", "type": "string"}])
    assert out[0]["data_type"] == "String"


def test_canonical_names_pass_through():
    for t in ("String", "Int64", "Double", "DateTime", "Decimal", "Boolean"):
        assert normalize_column_defs([{"name": "X", "data_type": t}])[0]["data_type"] == t


def test_missing_type_defaults_to_string():
    assert normalize_column_defs([{"name": "X"}])[0]["data_type"] == "String"


def test_unrecognized_type_raises_clearly():
    with pytest.raises(ValueError, match="unrecognized data type"):
        normalize_column_defs([{"name": "V", "data_type": "integer64"}])


def test_bare_string_column_raises_clearly():
    # The payload that used to surface "string indices must be integers".
    with pytest.raises(TypeError, match="must be an object"):
        normalize_column_defs(["Cat", "V"])


def test_other_keys_preserved():
    out = normalize_column_defs(
        [{"name": "Img", "dataType": "string", "data_category": "ImageUrl"}])
    assert out[0]["data_category"] == "ImageUrl"
    assert out[0]["data_type"] == "String"


# ---- integration: the tool + the DAX engine (the reporter's scenario) ----

@pytest.fixture()
def model_with_measure(tmp_path):
    """A file with table S (Cat, V, Goal) and measure Val = SUM(S[V])."""
    path = str(tmp_path / "m.pbix")
    tables = [{"name": "S",
               "columns": [{"name": "Cat", "data_type": "String"},
                           {"name": "V", "data_type": "Int64"},
                           {"name": "Goal", "data_type": "Int64"}],
               "rows": [{"Cat": "A", "V": 100, "Goal": 80},
                        {"Cat": "B", "V": 150, "Goal": 80}]}]
    measures = [{"table": "S", "name": "Val", "expression": "SUM(S[V])"}]
    S.pbix_create(path, "m21", tables_json=json.dumps(tables),
                  measures_json=json.dumps(measures))
    S.pbix_close("m21")
    return path


def _val(alias):
    r = json.loads(S.pbix_evaluate_dax(alias, "[Val]"))
    return r["results"][0]


def test_camelcase_payload_keeps_measure_queryable(model_with_measure):
    S.pbix_open(model_with_measure, "a21")
    try:
        # Reporter's EXACT shape: camelCase "dataType", lowercase names.
        data = {"columns": [{"name": "Cat", "dataType": "string"},
                            {"name": "V", "dataType": "int64"},
                            {"name": "Goal", "dataType": "int64"}],
                "rows": [{"Cat": f"C{i}", "V": v, "Goal": 80}
                         for i, v in enumerate([200, 400, 600, 800, 1000])]}
        resp = json.loads(S.pbix_set_table_data("a21", "S", json.dumps(data)))
        assert resp["success"], resp
        # The measure must evaluate to the real sum, not BLANK.
        res = _val("a21")
        assert res.get("status") == "ok", res
        assert res.get("value") == 3000, res
    finally:
        S.pbix_close("a21")


def test_columns_as_strings_gives_clear_error(model_with_measure):
    S.pbix_open(model_with_measure, "b21")
    try:
        bad = {"columns": ["Cat", "V"], "rows": [{"Cat": "A", "V": 1}]}
        resp = json.loads(S.pbix_set_table_data("b21", "S", json.dumps(bad)))
        assert not resp["success"]
        assert "must be an object" in resp["message"]
        assert "string indices" not in resp["message"]
    finally:
        S.pbix_close("b21")


def test_unrecognized_type_rejected_not_silently_string(model_with_measure):
    S.pbix_open(model_with_measure, "c21")
    try:
        bad = {"columns": [{"name": "V", "data_type": "integer64"}],
               "rows": [{"V": 1}]}
        resp = json.loads(S.pbix_set_table_data("c21", "S", json.dumps(bad)))
        assert not resp["success"]
        assert "unrecognized data type" in resp["message"]
    finally:
        S.pbix_close("c21")
