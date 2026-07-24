"""Regression tests for issues-11: measure/column name collisions and
measure DataType inference.

Two model-generation defects surfaced when a pbix-mcp-authored model was
uploaded to the Power BI service:

1. A measure whose name matches a column on the SAME table (case-insensitively)
   makes Analysis Services fail to process the model — every visual goes blank
   with "One or more errors were encountered in the MDX script". The lenient
   local DAX engine never noticed, so pbix-mcp happily emitted the broken file.
   Both the builder (pbix_create) and pbix_datamodel_add_measure must reject it.

2. Every measure was hardcoded to DataType Int64 (6). Decimal/percentage
   measures were therefore truncated in the service (0.153 -> 0). The type is
   now inferred (text -> String, otherwise Double) and overridable.
"""
import json
import os
import sqlite3
import tempfile

import pytest

from pbix_mcp.builder import (
    MEASURE_DT_DOUBLE,
    MEASURE_DT_INT64,
    MEASURE_DT_STRING,
    PBIXBuilder,
    find_reserved_var_names,
    infer_measure_data_type,
    normalize_measure_data_type,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------
class TestInferMeasureDataType:
    @pytest.mark.parametrize("expr", [
        "SUM(Sales[Amount])",
        "COUNTROWS(Sales)",
        "DIVIDE([a], [b])",
        "CALCULATE(SUM(Sales[Amt]), Sales[Y] = 2024)",
        "IF([a] > 0, [b], BLANK())",
        "VAR n = SUM(T[x]) RETURN IF([c] > 0, n, BLANK())",
        # numeric results despite string match values in SWITCH
        'SWITCH([grade], "A", 4, "B", 3, 0)',
        # the idareport string-parsing measure returns a number
        ('VAR s = SELECTEDVALUE(T[c]) VAR k = "x" VAR p = SEARCH(k, s, 1, 0) '
         'RETURN IF(p > 0, VALUE(MID(s, p, 3)), BLANK())'),
    ])
    def test_numeric_expressions_infer_double(self, expr):
        assert infer_measure_data_type(expr) == MEASURE_DT_DOUBLE

    @pytest.mark.parametrize("expr", [
        'IF([a] > 0, "BRA", "DRIT")',
        '"Total: " & FORMAT([x], "#,0")',
        'FORMAT([x], "0.0%")',
        'CONCATENATEX(T, T[n], ", ")',
        'UPPER(SELECTEDVALUE(T[name]))',
        'SWITCH(TRUE(), [a] > 0, "hi", [a] < 0, "lo", "mid")',
        'VAR r = "res: " & [m] RETURN r',
        'LEFT([txt], 5)',
    ])
    def test_text_expressions_infer_string(self, expr):
        assert infer_measure_data_type(expr) == MEASURE_DT_STRING

    def test_empty_expression_defaults_double(self):
        assert infer_measure_data_type("") == MEASURE_DT_DOUBLE


class TestNormalizeMeasureDataType:
    @pytest.mark.parametrize("value,expected", [
        ("String", MEASURE_DT_STRING),
        ("string", MEASURE_DT_STRING),
        ("Text", MEASURE_DT_STRING),
        ("Double", MEASURE_DT_DOUBLE),
        ("Int64", MEASURE_DT_INT64),
        ("integer", MEASURE_DT_INT64),
        ("Decimal", 10),
        ("DateTime", 9),
        ("Boolean", 11),
        (8, MEASURE_DT_DOUBLE),
        ("2", MEASURE_DT_STRING),
        ("", None),
        ("   ", None),
        (None, None),
    ])
    def test_valid(self, value, expected):
        assert normalize_measure_data_type(value) == expected

    @pytest.mark.parametrize("value", ["banana", 99, True, 3.5])
    def test_invalid_raises(self, value):
        with pytest.raises(ValueError):
            normalize_measure_data_type(value)


class TestFindReservedVarNames:
    # Empirically confirmed against app.powerbi.com's DAX engine as rejected.
    @pytest.mark.parametrize("name", [
        "status", "value", "level", "count", "scope", "name", "date",
        "filter", "rank", "member", "dimension", "parent", "current",
    ])
    def test_reserved_flagged(self, name):
        expr = f"VAR {name} = 1 RETURN {name}"
        assert find_reserved_var_names(expr) == [name]

    # Empirically confirmed accepted by the service — must NOT be flagged.
    @pytest.mark.parametrize("name", [
        "result", "total", "amount", "position", "selected", "temp",
        "res", "margin", "omsetning", "kost", "ansatte", "lonn", "x", "n",
    ])
    def test_safe_not_flagged(self, name):
        expr = f"VAR {name} = 1 RETURN {name}"
        assert find_reserved_var_names(expr) == []

    def test_case_insensitive(self):
        assert find_reserved_var_names("VAR Status = 1 RETURN Status") == ["Status"]

    def test_measure_reference_not_flagged(self):
        # [Profit status] is a quoted measure ref, not a VAR — leave it alone.
        expr = 'VAR x = [Profit status] RETURN x & "!"'
        assert find_reserved_var_names(expr) == []

    def test_multiple_in_order(self):
        expr = "VAR total = 1 VAR value = 2 VAR count = 3 RETURN total+value+count"
        assert find_reserved_var_names(expr) == ["value", "count"]

    def test_deduplicated(self):
        expr = "VAR count = 1 RETURN count + count"
        assert find_reserved_var_names(expr) == ["count"]

    def test_the_real_idareport_measure(self):
        # The exact shape that broke idareport's Profitabilitet visual.
        expr = (
            "VAR status = [Profit status]\n"
            "VAR res = [Driftsresultat beregnet MNOK]\n"
            "RETURN status & \": \" & FORMAT(res, \"#,0.0\")"
        )
        assert find_reserved_var_names(expr) == ["status"]


# ---------------------------------------------------------------------------
# Builder path (pbix_create-style construction)
# ---------------------------------------------------------------------------
def _read_measure_datatypes(pbix_path):
    """Return {measure_name: DataType} read straight from the built file."""
    import zipfile

    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with zipfile.ZipFile(pbix_path) as z:
        dm = z.read("DataModel")
    db = read_metadata_sqlite(decompress_datamodel(dm))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, db)
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        rows = conn.execute("SELECT Name, DataType FROM Measure").fetchall()
        conn.close()
        return dict(rows)
    finally:
        os.unlink(tmp)


class TestBuilderCollision:
    def test_same_table_collision_rejected(self, tmp_path):
        b = PBIXBuilder("Collide")
        b.add_table("Entur_Regnskap",
                    [{"name": "eiendeler", "data_type": "String"}],
                    rows=[{"eiendeler": "x"}])
        # measure "Eiendeler" collides with column "eiendeler" (case-insensitive)
        b.add_measure("Entur_Regnskap", "Eiendeler",
                      "SELECTEDVALUE(Entur_Regnskap[eiendeler])")
        b.add_page("P")
        with pytest.raises(ValueError) as ei:
            b.save(str(tmp_path / "collide.pbix"))
        msg = str(ei.value)
        assert "Eiendeler" in msg
        assert "collides with column" in msg

    def test_cross_table_name_reuse_allowed(self, tmp_path):
        # A measure may share a name with a column on a DIFFERENT table, as
        # long as its OWN table has no such column.
        b = PBIXBuilder("CrossOK")
        b.add_table("Sales", [{"name": "SalesKey", "data_type": "Int64"}],
                    rows=[{"SalesKey": 5}])
        b.add_table("Budget", [{"name": "Amount", "data_type": "Int64"}],
                    rows=[{"Amount": 9}])
        # measure "Amount" lives on Sales (no "Amount" column) -> allowed
        b.add_measure("Sales", "Amount", "SUM(Budget[Amount])")
        b.add_page("P")
        # must not raise
        b.save(str(tmp_path / "crossok.pbix"))

    def test_exact_same_name_collision_rejected(self, tmp_path):
        b = PBIXBuilder("Exact")
        b.add_table("T", [{"name": "Total", "data_type": "Int64"}],
                    rows=[{"Total": 1}])
        b.add_measure("T", "Total", "SUM(T[Total])")
        b.add_page("P")
        with pytest.raises(ValueError):
            b.save(str(tmp_path / "exact.pbix"))


class TestBuilderDataType:
    def test_inferred_datatypes(self, tmp_path):
        b = PBIXBuilder("Types")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_measure("S", "NumM", "SUM(S[A])")
        b.add_measure("S", "PctM", "DIVIDE(SUM(S[A]), 100)",
                      format_string="0.0%")
        b.add_measure("S", "TextM", 'IF(SUM(S[A]) > 0, "yes", "no")')
        b.add_page("P")
        p = str(tmp_path / "types.pbix")
        b.save(p)
        dt = _read_measure_datatypes(p)
        assert dt["NumM"] == MEASURE_DT_DOUBLE
        assert dt["PctM"] == MEASURE_DT_DOUBLE  # not truncated to Int64
        assert dt["TextM"] == MEASURE_DT_STRING

    def test_explicit_datatype_override(self, tmp_path):
        b = PBIXBuilder("Override")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_measure("S", "Count", "COUNTROWS(S)", data_type="Int64")
        b.add_page("P")
        p = str(tmp_path / "override.pbix")
        b.save(p)
        dt = _read_measure_datatypes(p)
        assert dt["Count"] == MEASURE_DT_INT64

    def test_invalid_datatype_rejected(self, tmp_path):
        b = PBIXBuilder("Bad")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        with pytest.raises(ValueError):
            b.add_measure("S", "M", "SUM(S[A])", data_type="banana")


class TestBuilderReservedVar:
    def test_reserved_var_rejected(self, tmp_path):
        b = PBIXBuilder("Resv")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_measure("S", "Summary",
                      'VAR status = SUM(S[A]) RETURN status & " done"')
        b.add_page("P")
        with pytest.raises(ValueError) as ei:
            b.save(str(tmp_path / "resv.pbix"))
        msg = str(ei.value)
        assert "status" in msg and "reserved" in msg.lower()

    def test_renamed_var_ok(self, tmp_path):
        b = PBIXBuilder("ResvOK")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_measure("S", "Summary",
                      'VAR vStatus = SUM(S[A]) RETURN vStatus & " done"')
        b.add_page("P")
        b.save(str(tmp_path / "resvok.pbix"))  # must not raise


# ---------------------------------------------------------------------------
# Server tool path (pbix_datamodel_add_measure / modify_measure)
# ---------------------------------------------------------------------------
def _server_measure_datatype(alias, measure_name):
    from pbix_mcp import server
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    info = server._open_files[alias]
    with open(os.path.join(info["work_dir"], "DataModel"), "rb") as f:
        db = read_metadata_sqlite(decompress_datamodel(f.read()))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, db)
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        row = conn.execute(
            "SELECT DataType FROM Measure WHERE Name = ?", (measure_name,)
        ).fetchone()
        conn.close()
        return row[0] if row else None
    finally:
        os.unlink(tmp)


class TestServerAddMeasure:
    def _open(self, tmp_path, alias):
        from pbix_mcp import server
        b = PBIXBuilder("Srv")
        b.add_table("Entur_Regnskap",
                    [{"name": "eiendeler", "data_type": "String"}],
                    rows=[{"eiendeler": "x"}])
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_page("P")
        p = str(tmp_path / f"{alias}.pbix")
        b.save(p)
        server.pbix_open(p, alias)
        return server

    def test_collision_rejected(self, tmp_path):
        server = self._open(tmp_path, "srvcol")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvcol", "Entur_Regnskap", "Eiendeler",
                "SELECTEDVALUE(Entur_Regnskap[eiendeler])"))
            assert out["success"] is False, out
            assert "collides with column" in out["message"]
        finally:
            server.pbix_close("srvcol")

    def test_infers_double_for_numeric(self, tmp_path):
        server = self._open(tmp_path, "srvnum")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvnum", "S", "Pct", "DIVIDE(SUM(S[A]), 100)"))
            assert out["success"], out
            assert _server_measure_datatype("srvnum", "Pct") == MEASURE_DT_DOUBLE
        finally:
            server.pbix_close("srvnum")

    def test_infers_string_for_text(self, tmp_path):
        server = self._open(tmp_path, "srvtxt")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvtxt", "S", "Label", 'IF(SUM(S[A]) > 0, "hi", "lo")'))
            assert out["success"], out
            assert _server_measure_datatype("srvtxt", "Label") == MEASURE_DT_STRING
        finally:
            server.pbix_close("srvtxt")

    def test_explicit_datatype(self, tmp_path):
        server = self._open(tmp_path, "srvexp")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvexp", "S", "Cnt", "COUNTROWS(S)", data_type="Int64"))
            assert out["success"], out
            assert _server_measure_datatype("srvexp", "Cnt") == MEASURE_DT_INT64
        finally:
            server.pbix_close("srvexp")

    def test_modify_datatype(self, tmp_path):
        server = self._open(tmp_path, "srvmod")
        try:
            server.pbix_datamodel_add_measure(
                "srvmod", "S", "Val", "SUM(S[A])", data_type="Int64")
            assert _server_measure_datatype("srvmod", "Val") == MEASURE_DT_INT64
            out = json.loads(server.pbix_datamodel_modify_measure(
                "srvmod", "Val", new_data_type="Double"))
            assert out["success"], out
            assert _server_measure_datatype("srvmod", "Val") == MEASURE_DT_DOUBLE
        finally:
            server.pbix_close("srvmod")

    def test_reserved_var_rejected(self, tmp_path):
        server = self._open(tmp_path, "srvresv")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvresv", "S", "Summary",
                'VAR status = SUM(S[A]) RETURN status & " x"'))
            assert out["success"] is False, out
            assert out["error_code"] == "RESERVED_VAR_NAME"
            assert "status" in out["message"]
        finally:
            server.pbix_close("srvresv")

    def test_reserved_var_renamed_ok(self, tmp_path):
        server = self._open(tmp_path, "srvresvok")
        try:
            out = json.loads(server.pbix_datamodel_add_measure(
                "srvresvok", "S", "Summary",
                'VAR vStatus = SUM(S[A]) RETURN vStatus & " x"'))
            assert out["success"], out
        finally:
            server.pbix_close("srvresvok")

    def test_modify_reserved_var_rejected(self, tmp_path):
        server = self._open(tmp_path, "srvmodresv")
        try:
            server.pbix_datamodel_add_measure(
                "srvmodresv", "S", "Val", "SUM(S[A])")
            out = json.loads(server.pbix_datamodel_modify_measure(
                "srvmodresv", "Val",
                new_expression='VAR count = SUM(S[A]) RETURN count'))
            assert out["success"] is False, out
            assert out["error_code"] == "RESERVED_VAR_NAME"
        finally:
            server.pbix_close("srvmodresv")
