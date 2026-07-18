"""Relationship-semantics tests (OpenBI pbix-mcp-issues #3 and #4).

Every non-default relationship trait must (a) be writable by the builder with the
exact [Relationship] / RelationshipStorage encoding Power BI Desktop produces, and
(b) survive a datamodel rebuild instead of silently resetting to active /
single-direction / many-to-one.

Ground truth was captured from Power BI Desktop 2.152.882.0:
  - inactive        -> IsActive=0, storage identical to active
  - bidirectional   -> CrossFilteringBehavior=2, single storage (Storage2ID=0)
  - many-to-many    -> FromCardinality=ToCardinality=2, NO storage (StorageID=0,
                       no RelationshipStorage / RelationshipIndexStorage / R$ table)
  - one-to-one      -> needs a reverse index (RelationshipStorage2ID); the builder
                       downgrades it to a bidirectional many-to-one (loads clean).

All of these round-tripped through Desktop (opened with no repair prompt, correct
glyphs in Manage relationships).
"""
import io
import os
import sqlite3
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _star_builder():
    """A small star schema exercising every relationship type."""
    b = PBIXBuilder("Rel")
    b.add_table("Sales", [
        {"name": "SaleID", "data_type": "Int64"},
        {"name": "CustID", "data_type": "String"},
        {"name": "ProdID", "data_type": "String"},
        {"name": "Chan", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
    ], rows=[
        {"SaleID": 1, "CustID": "C1", "ProdID": "P1", "Chan": "Web", "Amount": 10.0},
        {"SaleID": 2, "CustID": "C2", "ProdID": "P2", "Chan": "Store", "Amount": 20.0},
        {"SaleID": 3, "CustID": "C1", "ProdID": "P2", "Chan": "Web", "Amount": 30.0},
    ])
    b.add_table("Customer", [
        {"name": "CustID", "data_type": "String"}, {"name": "CName", "data_type": "String"}],
        rows=[{"CustID": "C1", "CName": "Alice"}, {"CustID": "C2", "CName": "Bob"}])
    b.add_table("Product", [
        {"name": "ProdID", "data_type": "String"}, {"name": "PName", "data_type": "String"}],
        rows=[{"ProdID": "P1", "PName": "Widget"}, {"ProdID": "P2", "PName": "Gadget"}])
    b.add_table("Channel", [
        {"name": "Chan", "data_type": "String"}, {"name": "ChName", "data_type": "String"}],
        rows=[{"Chan": "Web", "ChName": "Website"}, {"Chan": "Store", "ChName": "Retail"}])
    b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
    return b


def _dump(pbix_bytes):
    """Return (rels_by_key, n_storage_rows, n_index_rows, n_rtables)."""
    dm = zipfile.ZipFile(io.BytesIO(pbix_bytes)).read("DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(dm))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        conn.row_factory = sqlite3.Row
        tn = {r["ID"]: r["Name"] for r in conn.execute("SELECT ID, Name FROM [Table]")}
        cn = {r["ID"]: r["ExplicitName"] for r in conn.execute(
            "SELECT ID, ExplicitName FROM [Column]")}
        rels = {}
        for r in conn.execute("SELECT * FROM [Relationship]"):
            key = "%s[%s]->%s[%s]" % (
                tn.get(r["FromTableID"], "?"), cn.get(r["FromColumnID"], "?"),
                tn.get(r["ToTableID"], "?"), cn.get(r["ToColumnID"], "?"))
            rels[key] = dict(r)
        nrs = conn.execute("SELECT COUNT(*) FROM RelationshipStorage").fetchone()[0]
        nris = conn.execute("SELECT COUNT(*) FROM RelationshipIndexStorage").fetchone()[0]
        nrt = conn.execute(
            "SELECT COUNT(*) FROM [Table] WHERE Name LIKE 'R$%'").fetchone()[0]
        conn.close()
        return rels, nrs, nris, nrt
    finally:
        os.unlink(tmp)


# --------------------------------------------------------------------------- #
# #4 — the builder can author every relationship type
# --------------------------------------------------------------------------- #
class TestBuilderRelationshipSemantics:
    def _build_all_types(self):
        b = _star_builder()
        b.add_relationship("Sales", "CustID", "Customer", "CustID")  # default *:1
        b.add_relationship("Sales", "ProdID", "Product", "ProdID",
                           cross_filter_behavior=2, auto_orient=False)  # bidirectional
        # inactive on a *separate* pair so it stays a distinct row
        b.add_relationship("Sales", "Chan", "Channel", "Chan",
                           is_active=False, auto_orient=False)
        return _dump(b.build())

    def test_default_is_active_single_many_to_one(self):
        rels, _, _, _ = self._build_all_types()
        r = rels["Sales[CustID]->Customer[CustID]"]
        assert r["IsActive"] == 1
        assert r["CrossFilteringBehavior"] == 1
        assert r["FromCardinality"] == 2 and r["ToCardinality"] == 1
        assert r["RelationshipStorageID"] != 0

    def test_bidirectional_sets_crossfilter_2(self):
        rels, _, _, _ = self._build_all_types()
        r = rels["Sales[ProdID]->Product[ProdID]"]
        assert r["CrossFilteringBehavior"] == 2
        assert r["IsActive"] == 1
        assert r["RelationshipStorageID"] != 0  # bidir keeps single storage

    def test_inactive_sets_isactive_0(self):
        rels, _, _, _ = self._build_all_types()
        r = rels["Sales[Chan]->Channel[Chan]"]
        assert r["IsActive"] == 0
        assert r["CrossFilteringBehavior"] == 1
        assert r["RelationshipStorageID"] != 0  # inactive keeps single storage

    def test_many_to_many_has_no_storage(self):
        b = _star_builder()
        b.add_relationship("Sales", "CustID", "Customer", "CustID")   # normal
        b.add_relationship("Sales", "Chan", "Channel", "Chan",
                           from_cardinality=2, to_cardinality=2, auto_orient=False)
        rels, nrs, nris, nrt = _dump(b.build())
        m = rels["Sales[Chan]->Channel[Chan]"]
        assert m["FromCardinality"] == 2 and m["ToCardinality"] == 2
        assert m["RelationshipStorageID"] == 0
        assert m["RelationshipStorage2ID"] == 0
        # exactly one storage/index/R$ for the normal relationship; m2m omitted
        assert nrs == 1 and nris == 1 and nrt == 1

    def test_one_to_many_normalizes_to_many_to_one(self):
        # A (one side, unique keys) -> B (many side, duplicate keys), authored
        # as OneToMany. It must be stored canonically as B(many) -> A(one) so the
        # single R$ index sits on the true many side.
        b = PBIXBuilder("OM")
        b.add_table("A", [{"name": "K", "data_type": "String"}, {"name": "V", "data_type": "String"}],
                    rows=[{"K": "1", "V": "a"}, {"K": "2", "V": "b"}])
        b.add_table("B", [{"name": "AK", "data_type": "String"}, {"name": "W", "data_type": "String"}],
                    rows=[{"AK": "1", "W": "x"}, {"AK": "1", "W": "y"}, {"AK": "2", "W": "z"}])
        b.add_measure("A", "Cnt", "COUNTROWS(A)")
        b.add_relationship("A", "K", "B", "AK",
                           from_cardinality=1, to_cardinality=2, auto_orient=False)
        rels, nrs, _, nrt = _dump(b.build())
        assert "B[AK]->A[K]" in rels  # canonicalized many -> one
        r = rels["B[AK]->A[K]"]
        assert r["FromCardinality"] == 2 and r["ToCardinality"] == 1
        assert r["RelationshipStorageID"] != 0
        assert nrs == 1 and nrt == 1

    def test_one_to_many_default_auto_orient_not_double_swapped(self):
        # Regression: a 1:* is normalized to *:1, and the uniqueness heuristic
        # must NOT then re-swap it. Data is crafted so the naive heuristic would
        # fire (the normalized From side is unique in-sample and the To side has
        # more rows) — the result must still be B(many) -> A(one).
        b = PBIXBuilder("OM2")
        b.add_table("A", [{"name": "K", "data_type": "String"}, {"name": "V", "data_type": "String"}],
                    rows=[{"K": "1", "V": "a"}, {"K": "2", "V": "b"}, {"K": "3", "V": "c"}])
        b.add_table("B", [{"name": "AK", "data_type": "String"}, {"name": "W", "data_type": "String"}],
                    rows=[{"AK": "1", "W": "x"}, {"AK": "2", "W": "y"}])
        b.add_measure("A", "Cnt", "COUNTROWS(A)")
        # auto_orient left at its True default on purpose
        b.add_relationship("A", "K", "B", "AK", from_cardinality=1, to_cardinality=2)
        rels, _, _, _ = _dump(b.build())
        assert "B[AK]->A[K]" in rels          # canonical many -> one, not re-swapped
        assert "A[K]->B[AK]" not in rels
        assert rels["B[AK]->A[K]"]["FromCardinality"] == 2

    def test_one_to_one_builds_forward_and_reverse_index(self):
        # Full 1:1 (verified byte-for-byte against a Desktop-authored file and
        # round-tripped: opens in Desktop with no repair). A 1:1 keeps
        # FromCardinality=ToCardinality=1, forces CrossFilteringBehavior=2, and
        # emits TWO R$ join indexes (RelationshipStorageID + Storage2ID) — a 1:1
        # with only the single forward index fails to load.
        b = PBIXBuilder("O")
        b.add_table("A", [{"name": "K", "data_type": "String"}, {"name": "V", "data_type": "String"}],
                    rows=[{"K": "1", "V": "a"}, {"K": "2", "V": "b"}])
        b.add_table("B", [{"name": "K", "data_type": "String"}, {"name": "W", "data_type": "String"}],
                    rows=[{"K": "1", "W": "x"}, {"K": "2", "W": "y"}])
        b.add_measure("A", "Cnt", "COUNTROWS(A)")
        b.add_relationship("A", "K", "B", "K",
                           from_cardinality=1, to_cardinality=1,
                           cross_filter_behavior=2, auto_orient=False)
        rels, nrs, nris, nrt = _dump(b.build())
        r = rels["A[K]->B[K]"]
        assert r["FromCardinality"] == 1 and r["ToCardinality"] == 1   # true 1:1
        assert r["CrossFilteringBehavior"] == 2                        # Both
        assert r["RelationshipStorageID"] != 0
        assert r["RelationshipStorage2ID"] != 0                        # reverse index
        assert r["RelationshipStorageID"] != r["RelationshipStorage2ID"]
        # two RelationshipStorage / IndexStorage rows and two R$ tables for the 1:1
        assert nrs == 2 and nris == 2 and nrt == 2


# --------------------------------------------------------------------------- #
# #3 — a datamodel edit must preserve existing relationship semantics
# --------------------------------------------------------------------------- #
class TestRebuildPreservesRelationships:
    def _built_with_all_types(self):
        b = _star_builder()
        b.add_relationship("Sales", "CustID", "Customer", "CustID",
                           cross_filter_behavior=2, auto_orient=False)  # bidirectional
        b.add_relationship("Sales", "ProdID", "Product", "ProdID",
                           is_active=False, auto_orient=False)          # inactive
        b.add_relationship("Sales", "Chan", "Channel", "Chan",
                           from_cardinality=2, to_cardinality=2, auto_orient=False)  # m2m
        return b.build()

    def test_rebuild_keeps_bidir_inactive_m2m(self, tmp_path):
        work = tmp_path / "work"
        work.mkdir()
        pbix = self._built_with_all_types()
        with zipfile.ZipFile(io.BytesIO(pbix)) as z:
            (work / "DataModel").write_bytes(z.read("DataModel"))

        info = {"work_dir": str(work)}
        server._rebuild_datamodel(info, extra_measures=[
            {"table": "Sales", "name": "_probe", "expression": "1",
             "format_string": None}])

        rels, nrs, nris, nrt = _dump(_zip_dir_to_pbix_bytes(work))
        # bidirectional preserved
        assert rels["Sales[CustID]->Customer[CustID]"]["CrossFilteringBehavior"] == 2
        # inactive preserved
        assert rels["Sales[ProdID]->Product[ProdID]"]["IsActive"] == 0
        # many-to-many preserved (still no storage)
        m = rels["Sales[Chan]->Channel[Chan]"]
        assert (m["FromCardinality"], m["ToCardinality"]) == (2, 2)
        assert m["RelationshipStorageID"] == 0
        # 3 relationships total (bidir + inactive + m2m); the m2m omits storage,
        # so 2 storage/index/R$ rows survive the rebuild.
        assert nrs == 2 and nris == 2 and nrt == 2


def _zip_dir_to_pbix_bytes(work_dir):
    """Wrap just the rebuilt DataModel into a minimal in-memory zip for _dump."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.write(os.path.join(str(work_dir), "DataModel"), "DataModel")
    return buf.getvalue()


# --------------------------------------------------------------------------- #
# #4 — the public tool parses the friendly cardinality / direction values
# --------------------------------------------------------------------------- #
class TestRebuildGuardsSpecialTables:
    """A datamodel edit that rebuilds must refuse (clearly) on models with
    calculated / measure-only tables rather than crash or corrupt the file."""

    _SAMPLES = os.environ.get("PBIX_TEST_SAMPLES", "")

    @pytest.mark.skipif(not _SAMPLES, reason="needs PBIX_TEST_SAMPLES corpus")
    def test_rebuild_refuses_calc_table_model(self, tmp_path):
        from pbix_mcp.errors import UnsupportedModelEditError
        src = os.path.join(self._SAMPLES, "GeoSales_Dashboard.pbix")
        if not os.path.exists(src):
            pytest.skip("GeoSales_Dashboard.pbix not in corpus")
        work = tmp_path / "w"
        work.mkdir()
        with zipfile.ZipFile(src) as z:
            dm = next(n for n in z.namelist() if n.lower() == "datamodel")
            (work / "DataModel").write_bytes(z.read(dm))
        with pytest.raises(UnsupportedModelEditError) as ei:
            server._rebuild_datamodel(
                {"work_dir": str(work)},
                extra_measures=[{"table": "fct_Orders", "name": "_probe",
                                 "expression": "1", "format_string": None}])
        assert ei.value.code == "MODEL_EDIT_UNSUPPORTED"
        # names the offending table(s) and points at the surgical tools
        assert "# Measures" in ei.value.message or "calculated table" in ei.value.message
        assert "pbix_datamodel_add_measure" in ei.value.message

    def test_normal_model_still_rebuilds(self, tmp_path):
        # a plain built model (no calc/measure-only tables) must NOT trip the guard
        b = _star_builder()
        b.add_relationship("Sales", "CustID", "Customer", "CustID")
        work = tmp_path / "w"
        work.mkdir()
        with zipfile.ZipFile(io.BytesIO(b.build())) as z:
            (work / "DataModel").write_bytes(z.read("DataModel"))
        old, new = server._rebuild_datamodel(
            {"work_dir": str(work)},
            extra_measures=[{"table": "Sales", "name": "_p2",
                             "expression": "1", "format_string": None}])
        assert new > 0


class TestAddRelationshipToolParsing:
    @pytest.mark.parametrize("value", ["nonsense", "1:2:3", "one"])
    def test_invalid_cardinality_rejected(self, value):
        out = server.pbix_datamodel_add_relationship(
            "no_such_alias", "A", "K", "B", "K", cardinality=value)
        assert "Invalid cardinality" in out or "INVALID_ARGUMENT" in out

    @pytest.mark.parametrize("value", ["diagonal", "3", ""])
    def test_invalid_direction_rejected(self, value):
        out = server.pbix_datamodel_add_relationship(
            "no_such_alias", "A", "K", "B", "K", cross_filter_direction=value)
        assert "cross_filter_direction" in out or "INVALID_ARGUMENT" in out
