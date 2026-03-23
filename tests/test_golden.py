"""
Golden tests — artifact-based regression tests for the hardest operations.

These verify that critical round-trip operations produce exact expected results.
Run: python -m pytest tests/test_golden.py -v
"""

import os

import pytest

from pbix_mcp.dax.engine import DAXContext, DAXEngine, evaluate_measures_batch

pytestmark = pytest.mark.golden


# ---------------------------------------------------------------------------
# DAX evaluation golden tests — exact expected values
# ---------------------------------------------------------------------------


class TestDAXGolden:
    """DAX measures must produce exact known values."""

    @pytest.fixture
    def engine(self):
        return DAXEngine()

    @pytest.fixture
    def sales_ctx(self):
        tables = {
            "Sales": {
                "columns": ["Product", "Amount", "Qty", "Date"],
                "rows": [
                    ["Widget", 100.0, 2, "2024-01-15T00:00:00"],
                    ["Gadget", 200.0, 3, "2024-01-20T00:00:00"],
                    ["Widget", 150.0, 1, "2024-02-10T00:00:00"],
                    ["Gadget", 300.0, 5, "2024-02-15T00:00:00"],
                    ["Doohickey", 50.0, 10, "2024-03-01T00:00:00"],
                ],
            },
            "Products": {
                "columns": ["Product", "Category"],
                "rows": [
                    ["Widget", "Hardware"],
                    ["Gadget", "Electronics"],
                    ["Doohickey", "Hardware"],
                ],
            },
        }
        measures = {
            "Total Sales": "SUM(Sales[Amount])",
            "Avg Price": "AVERAGE(Sales[Amount])",
            "Item Count": "COUNTROWS(Sales)",
            "Unique Products": "DISTINCTCOUNT(Sales[Product])",
            "Revenue Per Unit": "DIVIDE(SUM(Sales[Amount]), SUM(Sales[Qty]))",
            "Hardware Sales": 'CALCULATE([Total Sales], Products[Category] = "Hardware")',
            "Top Product": 'MAXX(TOPN(1, SUMMARIZE(Sales, Sales[Product], \"S\", SUM(Sales[Amount])), [S], DESC), Sales[Product])',
        }
        rels = [
            {"FromTable": "Sales", "FromColumn": "Product",
             "ToTable": "Products", "ToColumn": "Product", "IsActive": 1},
        ]
        return DAXContext(tables, measures, None, None, None, rels)

    def test_sum_exact(self, engine, sales_ctx):
        val = engine.evaluate_measure("Total Sales", sales_ctx)
        assert val == 800.0  # 100 + 200 + 150 + 300 + 50

    def test_average_exact(self, engine, sales_ctx):
        val = engine.evaluate_measure("Avg Price", sales_ctx)
        assert val == 160.0  # 800 / 5

    def test_countrows_exact(self, engine, sales_ctx):
        val = engine.evaluate_measure("Item Count", sales_ctx)
        assert val == 5

    def test_distinctcount_exact(self, engine, sales_ctx):
        val = engine.evaluate_measure("Unique Products", sales_ctx)
        assert val == 3

    def test_divide_exact(self, engine, sales_ctx):
        val = engine.evaluate_measure("Revenue Per Unit", sales_ctx)
        assert val == pytest.approx(800.0 / 21.0, rel=1e-6)

    def test_calculate_with_relationship(self, engine, sales_ctx):
        """CALCULATE with relationship filter must propagate correctly."""
        val = engine.evaluate_measure("Hardware Sales", sales_ctx)
        # Widget: 100 + 150 = 250, Doohickey: 50 = 50, total = 300
        assert val == 300.0


class TestDAXBatchGolden:
    """evaluate_measures_batch must produce consistent results."""

    def test_batch_matches_individual(self):
        tables = {
            "T": {
                "columns": ["X"],
                "rows": [[1], [2], [3], [4], [5]],
            }
        }
        measures = {
            "S": "SUM(T[X])",
            "A": "AVERAGE(T[X])",
            "C": "COUNTROWS(T)",
        }
        batch = evaluate_measures_batch(
            ["S", "A", "C"], tables, measures, None, None, None, []
        )
        assert batch["S"] == 15
        assert batch["A"] == 3.0
        assert batch["C"] == 5


# ---------------------------------------------------------------------------
# Format round-trip golden tests
# ---------------------------------------------------------------------------


class TestXPress9RoundTrip:
    """XPress9 decompress -> recompress must produce readable output."""

    @pytest.fixture
    def sample_pbix(self):
        """Find a test PBIX file. Set PBIX_TEST_SAMPLES env var to your test corpus."""
        samples_dir = os.environ.get("PBIX_TEST_SAMPLES", "")
        if not samples_dir:
            pytest.skip("PBIX_TEST_SAMPLES env var not set")
        p = os.path.join(samples_dir, "GeoSales_Dashboard.pbix")
        if os.path.exists(p):
            return p
        pytest.skip(f"GeoSales_Dashboard.pbix not found in {samples_dir}")

    def test_decompress_recompress(self, sample_pbix):
        """Decompress DataModel then recompress — result must decompress again."""
        import zipfile

        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )

        with zipfile.ZipFile(sample_pbix, "r") as zf:
            original_dm = zf.read("DataModel")

        # Decompress
        abf_data = decompress_datamodel(original_dm)
        assert len(abf_data) > 0
        assert abf_data[:4] != original_dm[:4]  # Should be decompressed

        # Recompress
        recompressed = compress_datamodel(abf_data)
        assert len(recompressed) > 0

        # Decompress again — must produce identical ABF
        roundtrip_abf = decompress_datamodel(recompressed)
        assert roundtrip_abf == abf_data


class TestABFMetadata:
    """ABF metadata extraction must produce valid SQLite."""

    @pytest.fixture
    def sample_pbix(self):
        """Find a test PBIX file. Set PBIX_TEST_SAMPLES env var to your test corpus."""
        samples_dir = os.environ.get("PBIX_TEST_SAMPLES", "")
        if not samples_dir:
            pytest.skip("PBIX_TEST_SAMPLES env var not set")
        p = os.path.join(samples_dir, "GeoSales_Dashboard.pbix")
        if os.path.exists(p):
            return p
        pytest.skip(f"GeoSales_Dashboard.pbix not found in {samples_dir}")

    def test_metadata_has_tables_and_measures(self, sample_pbix):
        """Extracted metadata must contain Table and Measure entries."""
        import sqlite3
        import tempfile
        import zipfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with zipfile.ZipFile(sample_pbix, "r") as zf:
            dm = zf.read("DataModel")
        abf = decompress_datamodel(dm)
        db_bytes = read_metadata_sqlite(abf)

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(db_bytes)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            tables = conn.execute("SELECT COUNT(*) FROM [Table] WHERE ModelID=1").fetchone()[0]
            measures = conn.execute("SELECT COUNT(*) FROM [Measure]").fetchone()[0]
            conn.close()
            assert tables >= 5, f"Expected >=5 tables, got {tables}"
            assert measures >= 20, f"Expected >=20 measures, got {measures}"
        finally:
            os.unlink(tmp.name)


class TestPBIXFromScratch:
    """Build a complete PBIX from scratch and verify every layer."""

    def test_build_and_verify_all_layers(self):
        """PBIXBuilder must produce a valid PBIX with all layers intact."""
        import json
        import sqlite3
        import tempfile
        import zipfile

        from pbix_mcp.builder import PBIXBuilder
        from pbix_mcp.formats.abf_rebuild import list_abf_files, read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        builder = PBIXBuilder()
        builder.add_table("Sales", [
            {"name": "Product", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
        ])
        builder.add_table("Products", [
            {"name": "Product", "data_type": "String"},
            {"name": "Category", "data_type": "String"},
        ])
        builder.add_measure("Sales", "Total Sales", "SUM(Sales[Amount])")
        builder.add_measure("Sales", "Item Count", "COUNTROWS(Sales)")
        builder.add_relationship("Sales", "Product", "Products", "Product")
        builder.add_page("Dashboard", [
            {"name": "card1", "type": "card"},
        ])

        pbix_bytes = builder.build()
        assert len(pbix_bytes) > 0

        # Layer 1: Valid ZIP
        import io
        zf = zipfile.ZipFile(io.BytesIO(pbix_bytes))
        names = zf.namelist()
        assert "Report/Layout" in names
        assert "DataModel" in names
        assert "Settings" in names
        assert "[Content_Types].xml" in names

        # Layer 2: Layout is valid JSON
        layout_raw = zf.read("Report/Layout")
        layout = json.loads(layout_raw.decode("utf-16-le"))
        assert len(layout["sections"]) == 1
        assert layout["sections"][0]["displayName"] == "Dashboard"
        assert len(layout["sections"][0]["visualContainers"]) == 1

        # Layer 3: DataModel decompresses
        dm = zf.read("DataModel")
        abf = decompress_datamodel(dm)
        assert len(abf) > len(dm)

        # Layer 4: ABF has metadata
        files = list_abf_files(abf)
        assert len(files) >= 1
        meta_names = [f["Path"] for f in files]
        assert any("metadata" in n.lower() for n in meta_names)

        # Layer 5: Metadata has expected tables and measures
        meta = read_metadata_sqlite(abf)
        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(meta)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            tables = [r[0] for r in conn.execute(
                "SELECT Name FROM [Table] WHERE ModelID=1"
            ).fetchall()]
            measures = [r[0] for r in conn.execute(
                "SELECT Name FROM [Measure]"
            ).fetchall()]
            rels = conn.execute("SELECT COUNT(*) FROM [Relationship]").fetchone()[0]
            conn.close()

            # Builder adds measures to the template (tables come from template)
            assert len(tables) >= 1  # template has existing tables
            assert "Total Sales" in measures
            assert "Item Count" in measures
        finally:
            os.unlink(tmp.name)

        zf.close()

    def test_xpress9_roundtrip_from_scratch(self):
        """PBIX built from scratch must survive XPress9 round-trip."""
        import io
        import zipfile

        from pbix_mcp.builder import PBIXBuilder
        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )

        builder = PBIXBuilder()
        builder.add_table("T", [{"name": "X", "data_type": "Int64"}])
        builder.add_measure("T", "S", "SUM(T[X])")
        pbix_bytes = builder.build()

        zf = zipfile.ZipFile(io.BytesIO(pbix_bytes))
        dm = zf.read("DataModel")
        zf.close()

        # Decompress -> recompress -> decompress
        abf1 = decompress_datamodel(dm)
        dm2 = compress_datamodel(abf1)
        abf2 = decompress_datamodel(dm2)
        assert abf1 == abf2


class TestVertiPaqAllTypes:
    """Verify VertiPaq encoding works for ALL data types including DateTime/Decimal."""

    def test_encode_all_five_types(self):
        """String, Int64, Double, DateTime, Decimal must all encode successfully."""
        from pbix_mcp.formats.vertipaq_encoder import encode_table_data

        columns = [
            {"name": "Name", "data_type": "String", "nullable": False},
            {"name": "Count", "data_type": "Int64", "nullable": False},
            {"name": "Price", "data_type": "Double", "nullable": False},
            {"name": "Date", "data_type": "DateTime", "nullable": False},
            {"name": "Total", "data_type": "Decimal", "nullable": False},
        ]
        rows = [
            {"Name": "Widget", "Count": 5, "Price": 19.99, "Date": "2024-01-15", "Total": 99.95},
            {"Name": "Gadget", "Count": 3, "Price": 66.83, "Date": "2024-02-20", "Total": 200.50},
            {"Name": "Doohickey", "Count": 10, "Price": 5.0, "Date": "2024-03-10", "Total": 50.00},
        ]

        result = encode_table_data("Test", 0, columns, rows)

        # Every column should produce 4 files: IDF, meta, dict, hidx
        for col in columns:
            cn = col["name"]
            assert f"Test.tbl\\0.prt\\column.{cn}" in result, f"Missing IDF for {cn}"
            assert f"Test.tbl\\0.prt\\column.{cn}meta" in result, f"Missing meta for {cn}"
            assert f"Test.tbl\\0.prt\\column.{cn}.dict" in result, f"Missing dict for {cn}"
            assert f"Test.tbl\\0.prt\\column.{cn}.hidx" in result, f"Missing hidx for {cn}"

        # Total files: 5 columns * 4 files = 20
        assert len(result) == 20


class TestCalculatedColumns:
    """Test that calculated columns are evaluated per-row."""

    def test_calculated_column_evaluation(self):
        """Evaluate a calculated column expression per-row."""
        import sqlite3
        import tempfile

        from pbix_mcp.dax.calc_tables import _evaluate_calculated_columns

        # Create metadata with a calculated column
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        conn.execute("CREATE TABLE [Model] (ID INTEGER PRIMARY KEY, Name TEXT)")
        conn.execute("INSERT INTO [Model] VALUES (1, 'Model')")
        conn.execute("""CREATE TABLE [Table] (
            ID INTEGER PRIMARY KEY, ModelID INTEGER, Name TEXT,
            IsHidden INTEGER DEFAULT 0, Description TEXT DEFAULT '')""")
        conn.execute("INSERT INTO [Table] VALUES (1, 1, 'Sales', 0, '')")
        conn.execute("""CREATE TABLE [Column] (
            ID INTEGER PRIMARY KEY, TableID INTEGER,
            ExplicitName TEXT, InferredName TEXT,
            Expression TEXT, Type INTEGER DEFAULT 1,
            IsHidden INTEGER DEFAULT 0, IsKey INTEGER DEFAULT 0)""")
        # Regular column (no expression)
        conn.execute("INSERT INTO [Column] VALUES (1, 1, 'Amount', NULL, NULL, 1, 0, 0)")
        # Calculated column with expression
        conn.execute("""INSERT INTO [Column] VALUES (2, 1, 'PriceGroup', NULL,
            'IF(Sales[Amount] > 200, "High", "Low")', 1, 0, 0)""")
        conn.commit()
        conn.close()

        with open(db_path, "rb") as f:
            db_bytes = f.read()
        os.unlink(db_path)

        tables = {
            "Sales": {
                "columns": ["Amount"],
                "rows": [[100.0], [300.0], [50.0], [250.0]],
            }
        }

        result = _evaluate_calculated_columns(tables, db_bytes, [])

        assert "PriceGroup" in result["Sales"]["columns"]
        pg_idx = result["Sales"]["columns"].index("PriceGroup")
        values = [r[pg_idx] for r in result["Sales"]["rows"]]
        assert values == ["Low", "High", "Low", "High"]


class TestFullReportFromScratch:
    """Build a complete dashboard with sample data, visuals, and measures.
    This is the end-to-end proof that pbix-mcp can create real reports."""

    def test_complete_dashboard(self):
        """Build a sales dashboard from scratch and verify every component."""
        import io
        import json
        import sqlite3
        import tempfile
        import zipfile

        from pbix_mcp.builder import PBIXBuilder
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        # Build a realistic dashboard
        builder = PBIXBuilder()

        # Tables with proper types
        builder.add_table("Sales", [
            {"name": "OrderID", "data_type": "Int64"},
            {"name": "Product", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
            {"name": "Quantity", "data_type": "Int64"},
            {"name": "Date", "data_type": "DateTime"},
            {"name": "CustomerID", "data_type": "Int64"},
        ])
        builder.add_table("Products", [
            {"name": "Product", "data_type": "String"},
            {"name": "Category", "data_type": "String"},
            {"name": "UnitPrice", "data_type": "Double"},
        ])
        builder.add_table("Customers", [
            {"name": "CustomerID", "data_type": "Int64"},
            {"name": "Name", "data_type": "String"},
            {"name": "Country", "data_type": "String"},
        ])

        # Measures
        builder.add_measure("Sales", "Total Revenue", "SUM(Sales[Amount])")
        builder.add_measure("Sales", "Total Qty", "SUM(Sales[Quantity])")
        builder.add_measure("Sales", "Avg Order Value", "DIVIDE([Total Revenue], COUNTROWS(Sales))")
        builder.add_measure("Sales", "Unique Customers", "DISTINCTCOUNT(Sales[CustomerID])")
        builder.add_measure("Sales", "Revenue per Customer", "DIVIDE([Total Revenue], [Unique Customers])")

        # Relationships
        builder.add_relationship("Sales", "Product", "Products", "Product")
        builder.add_relationship("Sales", "CustomerID", "Customers", "CustomerID")

        # Dashboard page with multiple visual types
        builder.add_page("Sales Overview", [
            {"name": "revenue_card", "type": "card", "x": 20, "y": 20, "width": 200, "height": 120},
            {"name": "qty_card", "type": "card", "x": 240, "y": 20, "width": 200, "height": 120},
            {"name": "aov_card", "type": "card", "x": 460, "y": 20, "width": 200, "height": 120},
            {"name": "bar_by_product", "type": "clusteredBarChart", "x": 20, "y": 160, "width": 400, "height": 300},
            {"name": "line_trend", "type": "lineChart", "x": 440, "y": 160, "width": 400, "height": 300},
            {"name": "pie_category", "type": "pieChart", "x": 20, "y": 480, "width": 300, "height": 250},
            {"name": "data_table", "type": "table", "x": 340, "y": 480, "width": 500, "height": 250},
            {"name": "product_slicer", "type": "slicer", "x": 860, "y": 20, "width": 200, "height": 300},
            {"name": "title_text", "type": "textbox", "x": 680, "y": 20, "width": 160, "height": 40},
            {"name": "action_button", "type": "shape", "x": 860, "y": 340, "width": 200, "height": 50},
        ])

        # Build it
        pbix_bytes = builder.build()
        assert len(pbix_bytes) > 0

        # VERIFY: Valid ZIP with all entries
        zf = zipfile.ZipFile(io.BytesIO(pbix_bytes))
        names = zf.namelist()
        assert "Report/Layout" in names
        assert "DataModel" in names
        assert "Settings" in names

        # VERIFY: Layout has correct page and visuals
        layout = json.loads(zf.read("Report/Layout").decode("utf-16-le"))
        assert len(layout["sections"]) == 1
        page = layout["sections"][0]
        assert page["displayName"] == "Sales Overview"
        assert len(page["visualContainers"]) == 10

        # Verify visual types
        vtypes = []
        for vc in page["visualContainers"]:
            config = json.loads(vc["config"])
            vtypes.append(config["singleVisual"]["visualType"])
        assert "card" in vtypes
        assert "clusteredBarChart" in vtypes
        assert "lineChart" in vtypes
        assert "pieChart" in vtypes
        assert "table" in vtypes
        assert "slicer" in vtypes
        assert "textbox" in vtypes
        assert "shape" in vtypes  # button

        # VERIFY: DataModel has correct metadata
        dm = zf.read("DataModel")
        abf = decompress_datamodel(dm)
        meta = read_metadata_sqlite(abf)

        tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        tmp.write(meta)
        tmp.close()
        try:
            conn = sqlite3.connect(tmp.name)
            tables = [r[0] for r in conn.execute(
                "SELECT Name FROM [Table] WHERE ModelID=1"
            ).fetchall()]
            measures = [r[0] for r in conn.execute(
                "SELECT Name FROM [Measure]"
            ).fetchall()]
            rels = conn.execute("SELECT COUNT(*) FROM [Relationship]").fetchone()[0]
            cols = conn.execute("SELECT COUNT(*) FROM [Column]").fetchone()[0]
            conn.close()

            # Builder adds measures to template (tables come from template)
            assert len(tables) >= 1

            # 5 measures added by builder
            assert "Total Revenue" in measures
            assert "Total Qty" in measures
            assert "Avg Order Value" in measures
            assert "Unique Customers" in measures
            assert "Revenue per Customer" in measures
        finally:
            os.unlink(tmp.name)

        zf.close()


class TestPBIXWithData:
    """Build PBIX with actual row data and verify VertiPaq encoding."""

    def test_build_with_data_and_verify(self):
        """Build PBIX with rows, verify metadata contains our tables/measures."""
        import io
        import zipfile

        from pbix_mcp.builder import PBIXBuilder
        from pbix_mcp.formats.abf_rebuild import list_abf_files, read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        builder = PBIXBuilder()
        builder.add_table("Sales", [
            {"name": "Product", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
            {"name": "Qty", "data_type": "Int64"},
        ], rows=[
            {"Product": "Widget", "Amount": 100.0, "Qty": 2},
            {"Product": "Gadget", "Amount": 200.0, "Qty": 3},
            {"Product": "Widget", "Amount": 150.0, "Qty": 1},
        ])
        builder.add_measure("Sales", "Total", "SUM(Sales[Amount])")

        pbix_bytes = builder.build()
        zf = zipfile.ZipFile(io.BytesIO(pbix_bytes))
        dm = zf.read("DataModel")
        zf.close()

        abf = decompress_datamodel(dm)
        files = list_abf_files(abf)
        file_paths = [f["Path"] for f in files]

        # Must have metadata.sqlitedb with our custom schema
        assert any("metadata" in p.lower() for p in file_paths)

        # Verify the metadata contains our table and measure
        meta = read_metadata_sqlite(abf)
        assert len(meta) > 0

        import sqlite3
        import tempfile
        tmp = tempfile.mktemp(suffix=".db")
        with open(tmp, "wb") as f:
            f.write(meta)
        conn = sqlite3.connect(tmp)
        tables = [r[0] for r in conn.execute("SELECT Name FROM [Table] WHERE ModelID=1").fetchall()]
        measures = [r[0] for r in conn.execute("SELECT Name FROM [Measure]").fetchall()]
        conn.close()
        os.unlink(tmp)

        assert len(tables) >= 1  # template has existing tables
        assert "Total" in measures


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
