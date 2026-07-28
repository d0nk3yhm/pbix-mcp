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


class TestSchemaEraPreservation:
    """A rebuild must keep the source model's metadata SCHEMA, not impose one.

    The builder's blank schema is a fixed 63 tables. 20 of the 24 corpus models
    are an OLDER compatibility level than that — one has 51 tables — so
    rebuilding them from a blank schema invented tables and columns their level
    never had. The Power BI service rejected the result outright with "Failed to
    PublishAbf database ... An error occurred when loading ... .db.xml", three
    times, while a field-by-field metadata comparison reported no differences.
    """

    def test_only_database_singletons_survive_the_clear(self):
        """Anything else left behind would point at a reassigned primary key."""
        from pbix_mcp import builder
        assert builder._PRESERVED_TABLES == {"Model", "Culture", "DBPROPERTIES"}

    def test_the_builder_starts_from_source_metadata_when_given_it(self):
        from pbix_mcp.builder import PBIXBuilder
        b = PBIXBuilder()
        assert b._source_metadata is None, "a NEW file must use the blank schema"
        assert b._source_db_xml is None

    def test_insert_of_an_unknown_column_is_narrowed_not_rejected(self):
        """The 1455 era has no Column.ExpressionContext; naming it must not
        fail the rebuild, and must not add the column either."""
        import sqlite3

        from pbix_mcp.builder import _SchemaAwareCursor
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE [T] (ID INTEGER, Name TEXT)")
        c = _SchemaAwareCursor(conn.cursor())
        c.execute("INSERT INTO T (ID, Name, ExpressionContext) VALUES (?, ?, ?)",
                  (1, "a", 99))
        assert conn.execute("SELECT ID, Name FROM T").fetchall() == [(1, "a")]
        assert "ExpressionContext" not in {
            r[1] for r in conn.execute("PRAGMA table_info([T])")}

    def test_insert_keeps_literals_aligned_with_placeholders(self):
        """Several builder inserts mix literals into VALUES; dropping a column
        has to drop the right parameter, not shift them all."""
        import sqlite3

        from pbix_mcp.builder import _SchemaAwareCursor
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE [T] (A INTEGER, C INTEGER, D TEXT)")
        c = _SchemaAwareCursor(conn.cursor())
        c.execute("INSERT INTO T (A, B, C, D) VALUES (?, ?, 42, ?)",
                  (1, 999, "keep"))
        assert conn.execute("SELECT A, C, D FROM T").fetchall() == [(1, 42, "keep")]

    def test_update_of_an_unknown_column_is_narrowed(self):
        """DictionaryStorage.Size does not exist at 1455 and is UPDATEd."""
        import sqlite3

        from pbix_mcp.builder import _SchemaAwareCursor
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE [T] (ID INTEGER, Keep TEXT)")
        conn.execute("INSERT INTO T VALUES (1, 'before')")
        c = _SchemaAwareCursor(conn.cursor())
        c.execute("UPDATE T SET Size = ?, Keep = ? WHERE ID = ?", (5, "after", 1))
        assert conn.execute("SELECT Keep FROM T").fetchone()[0] == "after"

    def test_update_with_no_surviving_columns_is_a_no_op(self):
        import sqlite3

        from pbix_mcp.builder import _SchemaAwareCursor
        conn = sqlite3.connect(":memory:")
        conn.execute("CREATE TABLE [T] (ID INTEGER)")
        conn.execute("INSERT INTO T VALUES (1)")
        c = _SchemaAwareCursor(conn.cursor())
        c.execute("UPDATE T SET Size = ? WHERE ID = ?", (5, 1))
        assert conn.execute("SELECT COUNT(*) FROM T").fetchone()[0] == 1


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestSchemaEraOnRealFiles:
    """The regression, on the file the service actually rejected."""

    @staticmethod
    def _schema(pbix):
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with zipfile.ZipFile(pbix) as z:
            raw = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.write(fd, raw)
        os.close(fd)
        conn = sqlite3.connect(tmp)
        try:
            tabs = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            cols = {}
            for n in tabs:
                cols[n] = {r[1] for r in conn.execute(f"PRAGMA table_info([{n}])")}
            return tabs, cols
        finally:
            conn.close()
            os.unlink(tmp)

    @pytest.mark.parametrize("fname,table", [
        ("MS_Blog_DataProfiling.pbix", "Numbers"),   # 1455 — the rejected one
        ("GeoSales_Dashboard.pbix", "People"),       # 1601 — always worked
    ])
    def test_rebuild_invents_no_table_or_column(self, fname, table, tmp_path):
        src = os.path.join(CORPUS, fname)
        if not os.path.exists(src):
            pytest.skip(f"{fname} not in corpus")
        control, err = _save_after(src, str(tmp_path)), ""
        assert control, err
        edited = _save_after(src, str(tmp_path), table)
        assert edited, "edit was refused"
        ct, cc = self._schema(control)
        et, ec = self._schema(edited)
        assert not et - ct, f"invented tables: {sorted(et - ct)}"
        invented = {n: sorted(ec[n] - cc[n]) for n in cc
                    if n in ec and ec[n] - cc[n]}
        assert not invented, f"invented columns: {invented}"


class TestObjectPropertyLists:
    """The property allow-lists decide what survives a rebuild."""

    def test_storage_and_type_fields_are_never_carried(self):
        """Those describe how the REBUILT data is physically stored; carrying
        them would re-attach stale storage to freshly written columns."""
        forbidden = {"ColumnStorageID", "TableStorageID", "AttributeHierarchyID",
                     "InferredDataType", "ExplicitDataType", "IsAvailableInMDX",
                     "ModifiedTime", "StructureModifiedTime", "RefreshedTime",
                     "Type", "TableID", "ID", "SystemFlags"}
        assert not forbidden & set(server._COLUMN_PROPERTIES)
        assert not forbidden & set(server._TABLE_PROPERTIES)

    def test_the_fields_that_were_actually_being_lost_are_covered(self):
        """Measured on the corpus before the fix: a rebuild reset these."""
        for f in ("IsHidden", "FormatString", "SummarizeBy", "DataCategory",
                  "DisplayOrdinal"):
            assert f in server._COLUMN_PROPERTIES, f
        for f in ("IsHidden", "IsPrivate", "ShowAsVariationsOnly"):
            assert f in server._TABLE_PROPERTIES, f

    def test_sort_by_is_carried_by_name_not_id(self):
        """SortByColumnID is a foreign key; the rebuild renumbers every ID, so
        carrying the number would point it at an arbitrary column."""
        assert "SortByColumnID" not in server._COLUMN_PROPERTIES


@pytest.mark.slow
@pytest.mark.skipif(not os.path.isdir(CORPUS), reason="corpus not downloaded")
class TestObjectPropertiesSurviveARebuild:
    """Hidden-ness, formatting, summarize-by and sort-by are authoring choices.

    They live as PROPERTIES on rows the builder does create, so the row-level
    carry-over never covered them and every rebuild-path edit reset them to
    defaults. What that cost: hidden tables became visible, format strings
    vanished, "Month sorted by MonthNo" reverted to alphabetical, and a numeric
    column like Year got SummarizeBy=Sum — so dragging it into a visual added
    the years together.
    """

    WATCHED = ("IsHidden", "FormatString", "SummarizeBy", "DataCategory",
               "DisplayOrdinal")

    @staticmethod
    def _props(pbix_path):
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
            names = {r["ID"]: r["nm"] for r in conn.execute(
                "SELECT c.ID, COALESCE(c.ExplicitName, c.InferredName) AS nm "
                "FROM [Column] c JOIN [Table] t ON c.TableID = t.ID "
                "WHERE t.ModelID = 1")}
            tables = {r["Name"]: r["IsHidden"] for r in conn.execute(
                "SELECT Name, IsHidden FROM [Table] WHERE ModelID = 1")}
            cols = {}
            for r in conn.execute(
                "SELECT c.*, t.Name AS _t FROM [Column] c "
                "JOIN [Table] t ON c.TableID = t.ID WHERE t.ModelID = 1"
            ):
                nm = r["ExplicitName"] or r["InferredName"]
                if not nm or nm.startswith("RowNumber"):
                    continue
                d = {k: r[k] for k in TestObjectPropertiesSurviveARebuild.WATCHED}
                d["SortBy"] = names.get(r["SortByColumnID"]) \
                    if r["SortByColumnID"] else None
                cols[(r["_t"], nm)] = d
            return tables, cols
        finally:
            conn.close()
            os.unlink(tmp)

    def test_no_property_is_reset_by_an_edit(self, tmp_path):
        src = os.path.join(CORPUS, "MS_Blog_DataProfiling.pbix")
        if not os.path.exists(src):
            pytest.skip("MS_Blog_DataProfiling.pbix not in corpus")
        bt, bc = self._props(_save_after(src, str(tmp_path)))
        at, ac = self._props(_save_after(src, str(tmp_path), "Numbers"))

        assert any(v for v in bt.values()), "fixture has no hidden table"
        changed_t = {k: (v, at[k]) for k, v in bt.items()
                     if k in at and at[k] != v}
        assert not changed_t, f"table properties reset: {changed_t}"

        changed_c = {k: {f: (v[f], ac[k][f]) for f in v if ac[k][f] != v[f]}
                     for k, v in bc.items() if k in ac and ac[k] != v}
        assert not changed_c, f"column properties reset: {changed_c}"

    def test_auto_date_tables_stay_hidden_and_non_summarizing(self, tmp_path):
        """The two most visible consequences, asserted by name."""
        src = os.path.join(CORPUS, "MS_Blog_DataProfiling.pbix")
        if not os.path.exists(src):
            pytest.skip("MS_Blog_DataProfiling.pbix not in corpus")
        at, ac = self._props(_save_after(src, str(tmp_path), "Numbers"))
        auto = [t for t in at if server._is_auto_date_table(t)]
        assert auto, "fixture has no auto date/time tables"
        for t in auto:
            assert at[t] == 1, f"{t} is visible in the field list"
        for (tbl, col), d in ac.items():
            if server._is_auto_date_table(tbl):
                assert d["IsHidden"] == 1, f"{tbl}[{col}] is visible"
                assert d["SummarizeBy"] == 2, \
                    f"{tbl}[{col}] would aggregate by default"


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
