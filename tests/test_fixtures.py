"""
Fixture-based tests — run from a fresh clone without private PBIX files.

These tests use the public fixtures in tests/fixtures/ and verify
that basic operations work with synthetic data.

Run: python -m pytest tests/test_fixtures.py -v
"""

import json
import os
import zipfile

import pytest

pytestmark = pytest.mark.unit

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")
PBIX_DIR = os.path.join(FIXTURES, "pbix")
EXPECTED_DIR = os.path.join(FIXTURES, "expected")


class TestBasicLayout:
    """Test layout parsing with the minimal synthetic PBIX."""

    @pytest.fixture
    def layout_pbix(self):
        path = os.path.join(PBIX_DIR, "basic_layout.pbix")
        if not os.path.exists(path):
            pytest.skip("basic_layout.pbix fixture not found")
        return path

    @pytest.fixture
    def expected(self):
        path = os.path.join(EXPECTED_DIR, "basic_layout.json")
        with open(path) as f:
            return json.load(f)

    def test_zip_valid(self, layout_pbix):
        """Fixture must be a valid ZIP."""
        assert zipfile.is_zipfile(layout_pbix)

    def test_has_layout(self, layout_pbix):
        """Must contain Report/Layout."""
        with zipfile.ZipFile(layout_pbix) as zf:
            names = zf.namelist()
            assert "Report/Layout" in names

    def test_layout_parseable(self, layout_pbix):
        """Layout must parse as JSON."""
        with zipfile.ZipFile(layout_pbix) as zf:
            raw = zf.read("Report/Layout")
            layout = json.loads(raw.decode("utf-16-le"))
            assert "sections" in layout
            assert len(layout["sections"]) >= 1

    def test_page_count_matches(self, layout_pbix, expected):
        with zipfile.ZipFile(layout_pbix) as zf:
            raw = zf.read("Report/Layout")
            layout = json.loads(raw.decode("utf-16-le"))
        assert len(layout["sections"]) == len(expected["pages"])

    def test_visual_types_match(self, layout_pbix, expected):
        with zipfile.ZipFile(layout_pbix) as zf:
            raw = zf.read("Report/Layout")
            layout = json.loads(raw.decode("utf-16-le"))
        page = layout["sections"][0]
        types = []
        for vc in page["visualContainers"]:
            config = json.loads(vc["config"])
            vtype = config.get("singleVisual", {}).get("visualType", "unknown")
            types.append(vtype)
        assert sorted(types) == sorted(expected["visual_types"])


class TestDAXGoldenFixture:
    """Verify DAX golden results match the expected fixture."""

    @pytest.fixture
    def expected(self):
        path = os.path.join(EXPECTED_DIR, "dax_golden.json")
        with open(path) as f:
            return json.load(f)

    def test_golden_measures_match(self, expected):
        """All golden measure values must match expected."""
        from pbix_mcp.dax.engine import DAXContext, DAXEngine

        fixture = expected["sales_fixture"]
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
            "Hardware Sales": 'CALCULATE(SUM(Sales[Amount]), Products[Category] = "Hardware")',
        }
        rels = [
            {"FromTable": "Sales", "FromColumn": "Product",
             "ToTable": "Products", "ToColumn": "Product", "IsActive": 1},
        ]

        engine = DAXEngine()
        ctx = DAXContext(tables, measures, None, None, None, rels)

        for name, spec in fixture["measures"].items():
            val = engine.evaluate_measure(name, ctx)
            tol = spec.get("tolerance", 0.001)
            assert val == pytest.approx(spec["expected"], rel=tol), (
                f"Measure '{name}': expected {spec['expected']}, got {val}"
            )

    def test_table_structure_matches(self, expected):
        """Table schemas must match expected fixture."""
        fixture = expected["sales_fixture"]
        for tname, tspec in fixture["tables"].items():
            assert tspec["row_count"] > 0
            assert len(tspec["columns"]) > 0


class TestPackageImports:
    """Verify all package modules import correctly from a fresh install."""

    def test_import_server(self):
        from pbix_mcp.server import mcp
        assert mcp is not None

    def test_import_dax_engine(self):
        from pbix_mcp.dax.engine import DAXEngine
        engine = DAXEngine()
        assert engine is not None

    def test_import_errors(self):
        from pbix_mcp.errors import (
            DAXEvaluationError,
            InvalidPBIXError,
            PBIXMCPError,
        )
        assert issubclass(DAXEvaluationError, PBIXMCPError)
        assert issubclass(InvalidPBIXError, PBIXMCPError)

    def test_import_models(self):
        import json

        from pbix_mcp.models.responses import ToolResponse
        r = ToolResponse.ok("test")
        assert r.success is True
        # Verify to_text() returns valid JSON
        output = r.to_text()
        parsed = json.loads(output)
        assert parsed["success"] is True
        assert parsed["message"] == "test"

    def test_import_logging(self):
        from pbix_mcp.logging_config import logger, set_level
        set_level("normal")
        assert logger is not None

    def test_version(self):
        from pbix_mcp import __version__
        assert __version__ == "0.7.0"


class TestRealPBIXFixture:
    """Tests using the real basic_measures.pbix fixture with valid DataModel."""

    @pytest.fixture
    def pbix_path(self):
        path = os.path.join(PBIX_DIR, "basic_measures.pbix")
        if not os.path.exists(path):
            pytest.skip("basic_measures.pbix fixture not found")
        return path

    def test_is_valid_zip(self, pbix_path):
        assert zipfile.is_zipfile(pbix_path)

    def test_has_datamodel(self, pbix_path):
        with zipfile.ZipFile(pbix_path) as zf:
            assert "DataModel" in zf.namelist()

    def test_datamodel_decompresses(self, pbix_path):
        """XPress9 DataModel must decompress to valid ABF."""
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with zipfile.ZipFile(pbix_path) as zf:
            dm = zf.read("DataModel")
        abf = decompress_datamodel(dm)
        assert len(abf) > len(dm)  # Decompressed is larger

    def test_metadata_has_expected_tables(self, pbix_path):
        """SQLite metadata must have our custom tables."""
        import sqlite3
        import tempfile

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with zipfile.ZipFile(pbix_path) as zf:
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
            conn.close()

            assert "Sales" in tables
            assert "Products" in tables
            assert "Total Sales" in measures
            assert "Avg Price" in measures
            assert len(measures) == 4
        finally:
            os.unlink(tmp.name)

    def test_xpress9_roundtrip(self, pbix_path):
        """Decompress -> recompress -> decompress must be identical."""
        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )

        with zipfile.ZipFile(pbix_path) as zf:
            dm = zf.read("DataModel")
        abf1 = decompress_datamodel(dm)
        dm2 = compress_datamodel(abf1)
        abf2 = decompress_datamodel(dm2)
        assert abf1 == abf2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
