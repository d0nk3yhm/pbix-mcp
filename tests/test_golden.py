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
        """Find a test PBIX file."""
        candidates = [
            os.path.join(os.path.dirname(os.path.dirname(__file__)),
                         "test_samples", "..", "..", "OpenBI", "test_samples",
                         "GeoSales_Dashboard.pbix"),
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        pytest.skip("No test PBIX file available")

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
        candidates = [
            os.path.join(os.path.dirname(os.path.dirname(__file__)),
                         "test_samples", "..", "..", "OpenBI", "test_samples",
                         "GeoSales_Dashboard.pbix"),
        ]
        for p in candidates:
            if os.path.exists(p):
                return p
        pytest.skip("No test PBIX file available")

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


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
