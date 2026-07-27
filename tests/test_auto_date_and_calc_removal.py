"""The auto date/time ceiling, calculated-column removal, and the scan cache.

Three separate pieces of 0.9.44, sharing a fixture set:

  * a calculated table that ALSO owns calculated columns is no longer refused —
    every Power BI auto date/time table is that shape, and it accounted for 16
    of the 18 corpus refusals
  * pbix_datamodel_remove_calculated_column, the inverse of the add tool
  * memoizing the expression scan, which halved DAX evaluation wall clock

The corpus cases are slow; the rest run everywhere.
"""
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.dax import engine as dax_engine

CORPUS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "test_corpus")


def _auto_date_shape(pbix_path):
    """{table: {"partition": (type, sysflags), "cols": {name: (…)}}} for the
    auto date/time tables, with the fields Desktop's own shape is defined by."""
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with zipfile.ZipFile(pbix_path) as z:
        raw = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, raw)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    conn.row_factory = sqlite3.Row
    try:
        out = {}
        for r in conn.execute(
            "SELECT t.ID AS tid, t.Name AS nm, t.SystemFlags AS tsf, "
            "p.Type AS pt, p.SystemFlags AS psf FROM [Table] t "
            "JOIN [Partition] p ON p.TableID = t.ID WHERE t.ModelID = 1"
        ):
            if not server._is_auto_date_table(r["nm"]):
                continue
            cols = {}
            for c in conn.execute(
                "SELECT COALESCE(ExplicitName, InferredName) AS nm, Type, "
                "ExplicitDataType, InferredDataType, SourceColumn, "
                "SystemFlags, IsAvailableInMDX, Expression "
                "FROM [Column] WHERE TableID = ? ORDER BY ID", (r["tid"],)
            ):
                cols[c["nm"]] = tuple(c[k] for k in c.keys() if k != "nm")
            out[r["nm"]] = {"tsf": r["tsf"], "pt": r["pt"], "psf": r["psf"],
                            "cols": cols}
        return out
    finally:
        conn.close()
        os.unlink(tmp)


_AMO = {2: "String", 6: "Int64", 8: "Float64", 9: "DateTime",
        10: "Decimal", 11: "Boolean"}


def _small_table(alias):
    _i, conn, tmp = server._read_metadata_db(alias)
    try:
        r = conn.execute(
            "SELECT t.Name AS nm, COUNT(c.ID) AS nc FROM [Table] t "
            "JOIN [Column] c ON c.TableID = t.ID WHERE t.ModelID = 1 "
            "AND c.Type IN (1, 4) AND COALESCE(c.ExplicitName, c.InferredName) "
            "NOT LIKE 'RowNumber%' GROUP BY t.ID HAVING nc BETWEEN 1 AND 6 "
            "ORDER BY nc LIMIT 1").fetchone()
        return r["nm"] if r else None
    finally:
        conn.close()
        os.unlink(tmp)


def _columns(alias, table):
    _i, conn, tmp = server._read_metadata_db(alias)
    try:
        out = []
        for r in conn.execute(
            "SELECT COALESCE(c.ExplicitName, c.InferredName) AS nm, "
            "c.ExplicitDataType AS edt, c.InferredDataType AS idt "
            "FROM [Column] c JOIN [Table] t ON c.TableID = t.ID "
            "WHERE t.Name = ? AND c.Type IN (1, 4) "
            "AND COALESCE(c.ExplicitName, c.InferredName) NOT LIKE 'RowNumber%' "
            "ORDER BY c.ID", (table,)
        ):
            amo = r["edt"] if r["edt"] in _AMO else r["idt"]
            out.append({"name": r["nm"], "data_type": _AMO.get(amo, "String")})
        return out
    finally:
        conn.close()
        os.unlink(tmp)


def _value(dt, i):
    return {"String": f"v{i}", "Int64": i, "Float64": float(i),
            "Decimal": float(i), "Boolean": bool(i % 2),
            "DateTime": f"2020-01-0{i + 1}T00:00:00"}.get(dt, f"v{i}")


def _edit_and_save(src, tmpdir, table=None):
    work = os.path.join(tmpdir, f"w{uuid.uuid4().hex[:8]}.pbix")
    shutil.copy(src, work)
    out = os.path.join(tmpdir, f"o{uuid.uuid4().hex[:8]}.pbix")
    alias = "a" + uuid.uuid4().hex[:8]
    server.pbix_open(work, alias)
    try:
        if table:
            cols = _columns(alias, table)
            rows = [{c["name"]: _value(c["data_type"], i) for c in cols}
                    for i in range(3)]
            res = json.loads(server.pbix_set_table_data(
                alias, table, json.dumps({"columns": cols, "rows": rows})))
            if not res.get("success"):
                return None, str(res.get("message"))
        server.pbix_save(alias, out, overwrite=True, backup=False)
        return out, ""
    finally:
        try:
            server.pbix_close(alias, force=True)
        except Exception:
            pass


class TestExpressionScanCache:
    """Memoizing the scan must not change a single split."""

    CASES = [
        ("a+b*c", "+", ["a", "b*c"]),
        ("1e-5+2", "+", ["1e-5", "2"]),          # exponent, not an operator
        ("-a+b", "+", ["-a", "b"]),              # unary sign
        ("x<=y", "<", ["x<=y"]),                 # part of <=
        ("a<>b", "<", ["a<>b"]),                 # part of <>
        ('"a+b"+c', "+", ['"a+b"', "c"]),        # inside a string literal
        ("'T'[a+b]+c", "+", ["'T'[a+b]", "c"]),  # inside a column reference
        ("f(a+b)+c", "+", ["f(a+b)", "c"]),      # inside parentheses
    ]

    @pytest.mark.parametrize("expr,op,expected", CASES)
    def test_split_is_unchanged(self, expr, op, expected):
        assert dax_engine.DAXEngine()._split_operators(expr, op) == expected

    def test_a_caller_mutating_the_result_cannot_poison_the_cache(self):
        """The cached value is a tuple and each call gets a fresh list — a
        caller appending to what it got back must not affect the next caller."""
        eng = dax_engine.DAXEngine()
        first = eng._split_operators("a+b", "+")
        first.append("MUTATED")
        assert eng._split_operators("a+b", "+") == ["a", "b"]

    def test_repeated_calls_hit_the_cache(self):
        scan = dax_engine.DAXEngine._split_operators_scan
        scan.cache_clear()
        eng = dax_engine.DAXEngine()
        for _ in range(50):
            eng._split_operators("SUM(T[A]) + SUM(T[B])", "+")
        info = scan.cache_info()
        assert info.misses == 1 and info.hits == 49

    def test_the_cache_key_excludes_self(self):
        """Two engines must share the cache; keying on the instance would make
        it useless, since evaluation builds a fresh engine."""
        scan = dax_engine.DAXEngine._split_operators_scan
        scan.cache_clear()
        dax_engine.DAXEngine()._split_operators("q+r", "+")
        dax_engine.DAXEngine()._split_operators("q+r", "+")
        assert scan.cache_info().hits == 1


class TestRemoveCalculatedColumnGuards:
    """The refusals, which need no corpus file."""

    def test_removing_a_missing_column_says_so(self, tmp_path):
        src = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("corpus not downloaded")
        work = str(tmp_path / "w.pbix")
        shutil.copy(src, work)
        alias = "r" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            res = json.loads(server.pbix_datamodel_remove_calculated_column(
                alias, "fct_Orders", "NoSuchColumn"))
            assert not res["success"]
            assert res["error_code"] == "COLUMN_NOT_FOUND"
        finally:
            server.pbix_close(alias, force=True)

    def test_a_plain_data_column_is_refused_with_the_alternative(self, tmp_path):
        src = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("corpus not downloaded")
        work = str(tmp_path / "w.pbix")
        shutil.copy(src, work)
        alias = "r" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            cols = _columns(alias, "People")
            res = json.loads(server.pbix_datamodel_remove_calculated_column(
                alias, "People", cols[0]["name"]))
            assert not res["success"]
            assert res["error_code"] == "NOT_A_CALCULATED_COLUMN"
            assert "pbix_set_table_data" in res["message"]
        finally:
            server.pbix_close(alias, force=True)


class TestCalcColumnDependencyDetection:
    """`_calc_column_dependents` decides whether a removal is safe."""

    class _FakeConn:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, _sql, _params):
            return list(self._rows)

    def _dependents(self, rows, table="T", column="Base"):
        return server._calc_column_dependents(
            self._FakeConn(rows), 1, table, column)

    def test_finds_all_three_reference_forms(self):
        rows = [("A", "YEAR([Base])"), ("B", "T[Base] * 2"),
                ("C", "'T'[Base] + 1"), ("D", "1 + 1")]
        assert self._dependents(rows) == ["A", "B", "C"]

    def test_is_case_insensitive_because_dax_is(self):
        assert self._dependents([("A", "year([base])")]) == ["A"]

    def test_a_column_does_not_depend_on_itself(self):
        assert self._dependents([("Base", "YEAR([Base])")]) == []

    def test_a_similarly_named_column_is_not_a_dependent(self):
        assert self._dependents([("A", "[BaseRate] + 1")]) == []


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestAutoDateCeiling:
    """Files that used to be refused outright must now edit, faithfully."""

    # Each was refused by 0.9.43 with "is a calculated table that also defines
    # N calculated column(s)".
    FILES = ["MS_Supply_Chain.pbix", "MS_Blog_FuzzyMatching.pbix",
             "MS_Blog_DataProfiling.pbix"]

    @pytest.mark.parametrize("fname", FILES)
    def test_edit_is_accepted_and_auto_date_tables_keep_desktop_shape(
            self, fname, tmp_path):
        src = os.path.join(CORPUS, fname)
        if not os.path.exists(src):
            pytest.skip(f"{fname} not in corpus")
        control, err = _edit_and_save(src, str(tmp_path))
        assert control, f"control save failed: {err}"
        before = _auto_date_shape(control)
        assert before, f"{fname} has no auto date/time tables — bad fixture"

        alias = "p" + uuid.uuid4().hex[:8]
        work = str(tmp_path / "probe.pbix")
        shutil.copy(src, work)
        server.pbix_open(work, alias)
        try:
            table = _small_table(alias)
        finally:
            server.pbix_close(alias, force=True)

        out, err = _edit_and_save(src, str(tmp_path), table)
        assert out, f"{fname}: edit refused — {err[:160]}"
        after = _auto_date_shape(out)

        for name, want in before.items():
            assert name in after, f"{name} disappeared"
            got = after[name]
            assert (got["pt"], got["psf"], got["tsf"]) == \
                   (want["pt"], want["psf"], want["tsf"]), \
                   f"{name}: partition/table flags changed"
            for cname, cval in want["cols"].items():
                assert cname in got["cols"], f"{name}[{cname}] disappeared"
                assert got["cols"][cname] == cval, \
                    f"{name}[{cname}] changed: {cval} -> {got['cols'][cname]}"

    def test_the_date_column_stays_a_datetime(self, tmp_path):
        """Regression: the CALENDAR expression hands dates back as ISO strings,
        so inferring the type from the regenerated values retyped every Date
        column from DateTime (9) to String (2) while the table looked intact."""
        src = os.path.join(CORPUS, "MS_Blog_DataProfiling.pbix")
        if not os.path.exists(src):
            pytest.skip("MS_Blog_DataProfiling.pbix not in corpus")
        alias = "d" + uuid.uuid4().hex[:8]
        work = str(tmp_path / "probe.pbix")
        shutil.copy(src, work)
        server.pbix_open(work, alias)
        try:
            table = _small_table(alias)
        finally:
            server.pbix_close(alias, force=True)
        out, err = _edit_and_save(src, str(tmp_path), table)
        assert out, f"edit refused — {err[:160]}"
        for name, t in _auto_date_shape(out).items():
            if "Date" in t["cols"]:
                # (Type, ExplicitDataType, InferredDataType, …)
                assert t["cols"]["Date"][2] == 9, \
                    f"{name}[Date] InferredDataType is {t['cols']['Date'][2]}, " \
                    f"expected 9 (DateTime)"


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestRemoveCalculatedColumnRoundTrip:
    """Add a calculated column, remove it, and land back where we started."""

    def test_add_then_remove_restores_the_schema(self, tmp_path):
        src = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("GeoSales_Dashboard.pbix not in corpus")
        work = str(tmp_path / "w.pbix")
        shutil.copy(src, work)
        alias = "rt" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            def calc_columns():
                _i, conn, tmp = server._read_metadata_db(alias)
                try:
                    return {(r[0], r[1]) for r in conn.execute(
                        "SELECT t.Name, c.ExplicitName FROM [Column] c "
                        "JOIN [Table] t ON c.TableID = t.ID WHERE c.Type = 2 "
                        "AND t.ModelID = 1")}
                finally:
                    conn.close()
                    os.unlink(tmp)

            before = calc_columns()
            add = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Returns", "ProbeCol", "1"))
            assert add["success"], add.get("message")
            assert ("Returns", "ProbeCol") in calc_columns()

            rm = json.loads(server.pbix_datamodel_remove_calculated_column(
                alias, "Returns", "ProbeCol"))
            assert rm["success"], rm.get("message")
            after = calc_columns()
            assert ("Returns", "ProbeCol") not in after
            assert after == before, \
                f"other calculated columns changed: {before ^ after}"
        finally:
            server.pbix_close(alias, force=True)

    def test_a_dependent_calculated_column_blocks_removal(self, tmp_path):
        src = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("GeoSales_Dashboard.pbix not in corpus")
        work = str(tmp_path / "w.pbix")
        shutil.copy(src, work)
        alias = "dp" + uuid.uuid4().hex[:8]
        server.pbix_open(work, alias)
        try:
            a = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Returns", "BaseProbe", "1"))
            assert a["success"], a.get("message")
            b = json.loads(server.pbix_datamodel_add_calculated_column(
                alias, "Returns", "DerivedProbe", "[BaseProbe] + 1"))
            assert b["success"], b.get("message")

            res = json.loads(server.pbix_datamodel_remove_calculated_column(
                alias, "Returns", "BaseProbe"))
            assert not res["success"]
            assert res["error_code"] == "CALC_COLUMN_HAS_DEPENDENTS"
            assert "DerivedProbe" in res["message"]
        finally:
            server.pbix_close(alias, force=True)
