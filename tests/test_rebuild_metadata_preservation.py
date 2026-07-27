"""Model metadata a rebuild-path edit must not silently destroy.

A from-scratch rebuild writes only [Table], [Column], [Partition],
[Relationship] and [Measure]. metadata.sqlitedb defines roughly seventy tables.
Everything else — perspectives, Q&A phrasings, KPIs, dynamic format strings,
shared M expressions, auto date/time variations, annotations — used to be
discarded, and the tool reported success with an empty warnings list.

Measured across the 24-report corpus before the fix: 24/24 reports carried
metadata the rebuild could not reproduce, and `Agents_Performance.pbix` lost
both of its date hierarchies to an edit that was accepted.

The corpus tests are marked slow; the rest run everywhere.
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

CORPUS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                      "test_corpus")
_AMO = {2: "String", 6: "Int64", 8: "Float64", 9: "DateTime",
        10: "Decimal", 11: "Boolean"}

# Tables whose rows a user would miss. Storage/bookkeeping tables are excluded:
# the rebuild legitimately regenerates those.
WATCH = ["Hierarchy", "Level", "Variation", "LinguisticMetadata", "Perspective",
         "PerspectiveTable", "PerspectiveColumn", "KPI",
         "FormatStringDefinition", "Expression", "ExtendedProperty",
         "RelatedColumnDetails", "GroupByColumn", "DataSource"]


def _counts(pbix_path, tables=WATCH):
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with zipfile.ZipFile(pbix_path) as z:
        raw = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, raw)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    try:
        out = {}
        for t in tables:
            try:
                out[t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except sqlite3.Error:
                out[t] = 0
        return out
    finally:
        conn.close()
        os.unlink(tmp)


def _hierarchies(pbix_path):
    from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
    from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

    with zipfile.ZipFile(pbix_path) as z:
        raw = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, raw)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    try:
        return {(r[0], r[1]) for r in conn.execute(
            "SELECT t.Name, h.Name FROM [Hierarchy] h "
            "JOIN [Table] t ON h.TableID = t.ID WHERE t.ModelID = 1")}
    except sqlite3.Error:
        return set()
    finally:
        conn.close()
        os.unlink(tmp)


def _data_columns(alias, table):
    _info, conn, tmp = server._read_metadata_db(alias)
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


def _sample(dt, i):
    return {"String": f"v{i}", "Int64": i, "Float64": float(i),
            "Decimal": float(i), "Boolean": bool(i % 2),
            "DateTime": f"2020-01-0{i + 1}T00:00:00"}.get(dt, f"v{i}")


def _save_after(src, tmpdir, table=None):
    """Open src, optionally rebuild `table`, save. Returns the output path.

    With table=None this is the CONTROL: opening and saving rewrites members on
    its own, so comparing an edit against the ORIGINAL file would blame the save
    for damage it did not do.
    """
    work = os.path.join(tmpdir, f"w{uuid.uuid4().hex[:8]}.pbix")
    shutil.copy(src, work)
    out = os.path.join(tmpdir, f"o{uuid.uuid4().hex[:8]}.pbix")
    alias = "t" + uuid.uuid4().hex[:8]
    server.pbix_open(work, alias)
    try:
        if table:
            cols = _data_columns(alias, table)
            rows = [{c["name"]: _sample(c["data_type"], i) for c in cols}
                    for i in range(3)]
            res = json.loads(server.pbix_set_table_data(
                alias, table, json.dumps({"columns": cols, "rows": rows})))
            if not res.get("success"):
                pytest.skip(f"edit refused: {str(res.get('message'))[:100]}")
        server.pbix_save(alias, out, overwrite=True, backup=False)
        return out
    finally:
        try:
            server.pbix_close(alias, force=True)
        except Exception:
            pass


class TestCarrySpecShape:
    """The spec is data; these guard the invariants the restore relies on."""

    def test_object_type_enum_is_the_corpus_derived_one(self):
        assert server._AMO_OBJECT_TYPE == {
            1: "Model", 3: "Table", 4: "Column", 7: "Relationship",
            8: "Measure", 9: "Hierarchy", 12: "KPI", 41: "Expression"}

    def test_parents_precede_their_children(self):
        """A `self:` reference can only resolve if its parent ran first."""
        seen = []
        for tname, fks, _identity in server._CARRY_SPEC:
            for kind in fks.values():
                if kind.startswith("self:"):
                    assert kind[5:] in seen, (
                        f"{tname} references {kind[5:]} before it is carried")
            seen.append(tname)

    def test_every_carried_table_has_a_user_facing_meaning(self):
        for tname, _fks, _identity in server._CARRY_SPEC:
            assert tname in server._CARRY_MEANING, (
                f"{tname} would be reported to a user by its raw table name")

    def test_identity_columns_are_declared_for_every_table(self):
        for tname, _fks, identity in server._CARRY_SPEC:
            assert identity, f"{tname} has no identity — it would duplicate"


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestCorpusPreservation:
    """The real thing: metadata must survive an edit on real Power BI files."""

    CASES = [
        ("GeoSales_Dashboard.pbix", "topProductsSelection"),
        ("Ecommerce_Conversion.pbix", "MTD-QTD Selection"),
        ("MS_AdventureWorks_DW.pbix", "Sales Reason Bridge"),
        ("MS_Blog_2020_Nov.pbix", "Table"),
    ]

    @pytest.mark.parametrize("fname,table", CASES)
    def test_no_watched_metadata_is_lost(self, fname, table, tmp_path):
        src = os.path.join(CORPUS, fname)
        if not os.path.exists(src):
            pytest.skip(f"{fname} not in corpus")
        control = _counts(_save_after(src, str(tmp_path)))
        edited = _counts(_save_after(src, str(tmp_path), table))
        lost = {k: (control[k], edited[k])
                for k in WATCH if edited[k] < control[k]}
        assert not lost, f"{fname}: rebuild lost {lost}"

    def test_agents_performance_keeps_both_date_hierarchies(self, tmp_path):
        """The regression that proves the COALESCE fix.

        Both hierarchies here have levels on calculated-table columns, whose
        name lives in InferredName. Reading only ExplicitName gave every level
        a null column and the hierarchy was dropped without a word.
        """
        src = os.path.join(CORPUS, "Agents_Performance.pbix")
        if not os.path.exists(src):
            pytest.skip("Agents_Performance.pbix not in corpus")
        control = _hierarchies(_save_after(src, str(tmp_path)))
        edited = _hierarchies(_save_after(src, str(tmp_path), "Top-Bottom-N"))
        assert ("Date", "Calendar") in control, "fixture no longer representative"
        assert control - edited == set(), f"hierarchies lost: {control - edited}"

    def test_geosales_keeps_its_perspective_and_format_strings(self, tmp_path):
        """Named rather than counted, so the assertion says what it protects."""
        src = os.path.join(CORPUS, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("GeoSales_Dashboard.pbix not in corpus")
        control = _counts(_save_after(src, str(tmp_path)))
        edited = _counts(_save_after(src, str(tmp_path), "topProductsSelection"))
        assert control["Perspective"] >= 1, "fixture no longer representative"
        assert edited["Perspective"] == control["Perspective"]
        assert edited["PerspectiveColumn"] == control["PerspectiveColumn"]
        assert edited["FormatStringDefinition"] == control["FormatStringDefinition"]
        assert edited["LinguisticMetadata"] == control["LinguisticMetadata"]
