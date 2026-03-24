"""End-to-end tests for PBIX builder — validates full pipeline."""
import json
import os
import sqlite3
import tempfile
import zipfile

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import _ABFStructure, read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "..", "test_data")


def _verify_pbix(path):
    """Verify internal structure. Returns (table_names, col_counts, rel_count, errors)."""
    errors = []
    tables = []
    col_counts = {}
    rel_count = 0

    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        for req in ["Version", "DataModel", "Report/Layout", "[Content_Types].xml", "Settings", "Metadata"]:
            if req not in names:
                errors.append(f"Missing ZIP entry: {req}")

        dm = zf.read("DataModel")
        abf = decompress_datamodel(dm)
        abf_struct = _ABFStructure(abf)
        if len(abf_struct.data_entries) == 0:
            errors.append("ABF has no data entries")

        db = read_metadata_sqlite(abf)
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.write(fd, db)
        os.close(fd)
        conn = sqlite3.connect(tmp)
        c = conn.cursor()

        c.execute("SELECT Name FROM [Table] WHERE Name NOT LIKE 'H$%' AND Name NOT LIKE 'R$%' ORDER BY ID")
        tables = [r[0] for r in c.fetchall()]

        for tname in tables:
            c.execute(
                "SELECT COUNT(*) FROM [Column] col JOIN [Table] t ON col.TableID = t.ID WHERE t.Name = ? AND col.Type = 1",
                (tname,),
            )
            col_counts[tname] = c.fetchone()[0]

        c.execute("SELECT COUNT(*) FROM Relationship")
        rel_count = c.fetchone()[0]

        # Required annotations
        c.execute("SELECT COUNT(*) FROM Annotation WHERE Name = 'PBI_ResultType'")
        if c.fetchone()[0] < len(tables):
            errors.append("Missing PBI_ResultType annotations")

        c.execute("SELECT COUNT(*) FROM Annotation WHERE Name = '__PBI_TimeIntelligenceEnabled'")
        if c.fetchone()[0] == 0:
            errors.append("Missing __PBI_TimeIntelligenceEnabled")

        c.execute("SELECT COUNT(*) FROM Annotation WHERE Name = 'PBI_QueryOrder'")
        if c.fetchone()[0] == 0:
            errors.append("Missing PBI_QueryOrder")

        # DATASOURCEVERSION
        c.execute("SELECT Value FROM DBPROPERTIES WHERE Name = 'DATASOURCEVERSION'")
        dsv = c.fetchone()
        if not dsv or str(dsv[0]) != "2":
            errors.append(f"DATASOURCEVERSION wrong: {dsv}")

        # RowNumber GUID
        c.execute("SELECT ExplicitName FROM [Column] WHERE Type = 3")
        for (rn,) in c.fetchall():
            if "RowNumber" in rn and "2662979B" not in rn:
                errors.append(f"Bad RowNumber GUID: {rn}")

        # Valid layout JSON
        layout = zf.read("Report/Layout")
        json.loads(layout.decode("utf-16-le"))

        conn.close()
        os.unlink(tmp)

    return tables, col_counts, rel_count, errors


# ── Test cases ────────────────────────────────────────────────────────


class TestSingleTable:
    def test_1_table_3_cols(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("Items", [
            {"name": "ID", "data_type": "Int64"},
            {"name": "Name", "data_type": "String"},
            {"name": "Price", "data_type": "Double"},
        ], rows=[{"ID": 1, "Name": "A", "Price": 9.99}])
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert tables == ["Items"]
        assert cols["Items"] == 3

    def test_1_table_6_cols(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("Big", [
            {"name": "A", "data_type": "Int64"},
            {"name": "B", "data_type": "String"},
            {"name": "C", "data_type": "String"},
            {"name": "D", "data_type": "Double"},
            {"name": "E", "data_type": "Int64"},
            {"name": "F", "data_type": "String"},
        ], rows=[{"A": 1, "B": "x", "C": "y", "D": 1.0, "E": 2, "F": "z"}])
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert cols["Big"] == 6

    def test_all_data_types(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("Types", [
            {"name": "IntCol", "data_type": "Int64"},
            {"name": "StrCol", "data_type": "String"},
            {"name": "DblCol", "data_type": "Double"},
            {"name": "BoolCol", "data_type": "Boolean"},
        ], rows=[
            {"IntCol": 42, "StrCol": "Hello", "DblCol": 3.14, "BoolCol": True},
            {"IntCol": -7, "StrCol": "World", "DblCol": 2.718, "BoolCol": False},
        ])
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert cols["Types"] == 4

    def test_100_rows(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("Data", [
            {"name": "ID", "data_type": "Int64"},
            {"name": "Val", "data_type": "Double"},
            {"name": "Label", "data_type": "String"},
        ], rows=[{"ID": i, "Val": i * 1.5, "Label": f"Item_{i}"} for i in range(100)])
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs


class TestMultiTable:
    def test_2_tables_with_relationship(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("Dim", [
            {"name": "DID", "data_type": "Int64"},
            {"name": "Label", "data_type": "String"},
        ], rows=[{"DID": 1, "Label": "X"}, {"DID": 2, "Label": "Y"}])
        b.add_table("Fact", [
            {"name": "FID", "data_type": "Int64"},
            {"name": "DID", "data_type": "Int64"},
            {"name": "Amt", "data_type": "Double"},
        ], rows=[{"FID": 1, "DID": 1, "Amt": 100.0}])
        b.add_relationship("Dim", "DID", "Fact", "DID")
        b.add_measure("Fact", "Total", "SUM(Fact[Amt])")
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert len(tables) == 2
        assert rels == 1

    def test_3_tables_star_schema(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("DimA", [{"name": "AID", "data_type": "Int64"}, {"name": "AN", "data_type": "String"}],
                     rows=[{"AID": 1, "AN": "C1"}])
        b.add_table("DimB", [{"name": "BID", "data_type": "Int64"}, {"name": "BN", "data_type": "String"}],
                     rows=[{"BID": 1, "BN": "R1"}])
        b.add_table("Fact", [
            {"name": "FID", "data_type": "Int64"}, {"name": "AID", "data_type": "Int64"},
            {"name": "BID", "data_type": "Int64"}, {"name": "V", "data_type": "Double"},
        ], rows=[{"FID": 1, "AID": 1, "BID": 1, "V": 42.0}])
        b.add_relationship("DimA", "AID", "Fact", "AID")
        b.add_relationship("DimB", "BID", "Fact", "BID")
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert len(tables) == 3
        assert rels == 2

    def test_4_tables_8_col_fact_3_rels(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("R", [{"name": "RID", "data_type": "Int64"}, {"name": "Reg", "data_type": "String"}],
                     rows=[{"RID": 1, "Reg": "N"}])
        b.add_table("C", [
            {"name": "CID", "data_type": "Int64"}, {"name": "RID", "data_type": "Int64"},
            {"name": "Nm", "data_type": "String"}, {"name": "Em", "data_type": "String"},
        ], rows=[{"CID": 1, "RID": 1, "Nm": "A", "Em": "a@b"}])
        b.add_table("P", [
            {"name": "PID", "data_type": "Int64"}, {"name": "PN", "data_type": "String"},
            {"name": "Pr", "data_type": "Double"},
        ], rows=[{"PID": 1, "PN": "W", "Pr": 9.99}])
        b.add_table("O", [
            {"name": "OID", "data_type": "Int64"}, {"name": "CID", "data_type": "Int64"},
            {"name": "PID", "data_type": "Int64"}, {"name": "Qty", "data_type": "Int64"},
            {"name": "Tot", "data_type": "Double"}, {"name": "Dt", "data_type": "String"},
            {"name": "St", "data_type": "String"}, {"name": "Nt", "data_type": "String"},
        ], rows=[{"OID": 1, "CID": 1, "PID": 1, "Qty": 3, "Tot": 29.97, "Dt": "2025-01", "St": "S", "Nt": "R"}])
        b.add_relationship("R", "RID", "C", "RID")
        b.add_relationship("C", "CID", "O", "CID")
        b.add_relationship("P", "PID", "O", "PID")
        b.add_measure("O", "Rev", "SUM(O[Tot])")
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert len(tables) == 4
        assert cols["O"] == 8
        assert rels == 3


class TestDirectQuery:
    def test_dq_postgresql(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("DQ")
        b.add_table("Products", [
            {"name": "product_id", "data_type": "Int64"},
            {"name": "product", "data_type": "String"},
            {"name": "category", "data_type": "String"},
        ], rows=[{"product_id": 1, "product": "X", "category": "Y"}],
        source_db={"type": "postgresql", "server": "localhost", "database": "pbi_test",
                    "table": "demo_products", "port": 5432, "schema": "public"}, mode="directquery")
        b.add_table("Sales", [
            {"name": "sale_id", "data_type": "Int64"}, {"name": "product_id", "data_type": "Int64"},
            {"name": "year", "data_type": "Int64"}, {"name": "amount", "data_type": "Double"},
        ], rows=[{"sale_id": 1, "product_id": 1, "year": 2025, "amount": 1.0}],
        source_db={"type": "postgresql", "server": "localhost", "database": "pbi_test",
                    "table": "demo_sales", "port": 5432, "schema": "public"}, mode="directquery")
        b.add_relationship("Products", "product_id", "Sales", "product_id")
        b.add_measure("Sales", "Rev", "SUM(Sales[amount])")
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        assert len(tables) == 2
        assert rels == 1

    def test_dq_mysql(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("DQ")
        b.add_table("Customers", [
            {"name": "CustomerID", "data_type": "Int64"},
            {"name": "Name", "data_type": "String"},
            {"name": "City", "data_type": "String"},
        ], rows=[{"CustomerID": 1, "Name": "X", "City": "Y"}],
        source_db={"type": "mysql", "server": "localhost", "database": "testdb",
                    "table": "customers", "port": 3306}, mode="directquery")
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs


class TestVisuals:
    def test_dashboard_with_visuals(self, tmp_path):
        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("V")
        b.add_table("P", [
            {"name": "PID", "data_type": "Int64"},
            {"name": "Name", "data_type": "String"},
            {"name": "Cat", "data_type": "String"},
        ], rows=[{"PID": 1, "Name": "Laptop", "Cat": "Elec"}, {"PID": 2, "Name": "Chair", "Cat": "Furn"}])
        b.add_table("S", [
            {"name": "SID", "data_type": "Int64"}, {"name": "PID", "data_type": "Int64"},
            {"name": "Year", "data_type": "Int64"}, {"name": "Amt", "data_type": "Double"},
        ], rows=[{"SID": 1, "PID": 1, "Year": 2025, "Amt": 1200.0}])
        b.add_relationship("P", "PID", "S", "PID")
        b.add_measure("S", "Rev", "SUM(S[Amt])")
        b.add_page("Dashboard", [
            {"name": "slicer", "type": "slicer", "x": 20, "y": 20, "width": 200, "height": 80,
             "config": {"column": {"table": "S", "column": "Year"}}},
            {"name": "card", "type": "card", "x": 250, "y": 20, "width": 200, "height": 80,
             "config": {"measure": "Rev"}},
            {"name": "table", "type": "table", "x": 20, "y": 120, "width": 500, "height": 300,
             "config": {"columns": [{"table": "P", "column": "Name"}, {"measure": "Rev"}]}},
            {"name": "pie", "type": "pieChart", "x": 550, "y": 120, "width": 400, "height": 300,
             "config": {"category": {"table": "P", "column": "Cat"}, "measure": "Rev"}},
        ])
        b.save(p)
        tables, cols, rels, errs = _verify_pbix(p)
        assert not errs, errs
        # Verify layout has visuals
        with zipfile.ZipFile(p) as zf:
            layout = json.loads(zf.read("Report/Layout").decode("utf-16-le"))
            sections = layout.get("sections", [])
            assert len(sections) >= 1
            containers = sections[0].get("visualContainers", [])
            assert len(containers) == 4, f"Expected 4 visuals, got {len(containers)}"
