"""
Tests for typed exceptions, Pydantic response models, and request models.
Verifies the contract layer that every MCP tool relies on.
"""
import json

import pytest

from pbix_mcp.errors import (
    ERROR_CODES,
    ABFRebuildError,
    DataModelCompressionError,
    DAXEvaluationError,
    DAXParseError,
    DAXUnsupportedError,
    FileAlreadyOpenError,
    FileNotOpenError,
    InvalidPBIXError,
    LayoutParseError,
    MetadataSQLError,
    PBIXMCPError,
    SessionError,
    UnsafeWriteError,
    UnsupportedFormatError,
)
from pbix_mcp.models.requests import DimensionRef, FilterContext
from pbix_mcp.models.responses import DAXEvalResponse, DAXResult, ToolResponse

pytestmark = [pytest.mark.unit]


# ===========================================================================
# Error hierarchy
# ===========================================================================

class TestErrorHierarchy:
    """All exceptions inherit from PBIXMCPError and have unique codes."""

    def test_all_inherit_from_base(self):
        exceptions = [
            InvalidPBIXError, UnsupportedFormatError, LayoutParseError,
            DataModelCompressionError, ABFRebuildError, MetadataSQLError,
            DAXEvaluationError, DAXUnsupportedError, DAXParseError,
            UnsafeWriteError, SessionError, FileNotOpenError, FileAlreadyOpenError,
        ]
        for exc_cls in exceptions:
            assert issubclass(exc_cls, PBIXMCPError), f"{exc_cls.__name__} must inherit PBIXMCPError"

    def test_unique_codes(self):
        exceptions = [
            InvalidPBIXError, UnsupportedFormatError, LayoutParseError,
            DataModelCompressionError, ABFRebuildError, MetadataSQLError,
            DAXEvaluationError, DAXUnsupportedError, DAXParseError,
            UnsafeWriteError, SessionError, FileNotOpenError, FileAlreadyOpenError,
        ]
        codes = [exc.code for exc in exceptions]
        assert len(codes) == len(set(codes)), f"Duplicate error codes: {codes}"

    def test_error_code_registry_complete(self):
        """Every exception class's code is in the registry."""
        exceptions = [
            InvalidPBIXError, UnsupportedFormatError, LayoutParseError,
            DataModelCompressionError, ABFRebuildError, MetadataSQLError,
            DAXEvaluationError, DAXUnsupportedError, DAXParseError,
            UnsafeWriteError, SessionError, FileNotOpenError, FileAlreadyOpenError,
        ]
        for exc_cls in exceptions:
            assert exc_cls.code in ERROR_CODES, f"{exc_cls.__name__}.code '{exc_cls.code}' not in ERROR_CODES"

    def test_exception_message_format(self):
        """Exception str includes [CODE] prefix."""
        e = InvalidPBIXError("test file broken")
        assert "[PBIX_INVALID]" in str(e)
        assert "test file broken" in str(e)

    def test_exception_custom_code(self):
        """Can override code at instantiation."""
        e = PBIXMCPError("custom", code="CUSTOM_CODE")
        assert e.code == "CUSTOM_CODE"
        assert "[CUSTOM_CODE]" in str(e)

    def test_dax_unsupported_function_name(self):
        """DAXUnsupportedError stores function name."""
        e = DAXUnsupportedError("MYFUNCTION")
        assert e.function_name == "MYFUNCTION"
        assert "MYFUNCTION" in str(e)

    def test_exception_attributes(self):
        e = FileNotOpenError("no such alias")
        assert e.message == "no such alias"
        assert e.code == "FILE_NOT_OPEN"


# ===========================================================================
# ToolResponse
# ===========================================================================

class TestToolResponse:
    """ToolResponse is the contract for all MCP tool returns."""

    def test_ok_returns_json(self):
        r = ToolResponse.ok("done")
        text = r.to_text()
        parsed = json.loads(text)
        assert parsed["success"] is True
        assert parsed["message"] == "done"
        assert "error_code" not in parsed or parsed["error_code"] is None

    def test_error_returns_json(self):
        r = ToolResponse.error("broken", "PBIX_INVALID")
        text = r.to_text()
        parsed = json.loads(text)
        assert parsed["success"] is False
        assert parsed["error_code"] == "PBIX_INVALID"
        assert "broken" in parsed["message"]

    def test_ok_with_data(self):
        r = ToolResponse.ok("found 3 tables", data={"count": 3, "tables": ["A", "B", "C"]})
        parsed = json.loads(r.to_text())
        assert parsed["data"]["count"] == 3
        assert len(parsed["data"]["tables"]) == 3

    def test_ok_with_warnings(self):
        r = ToolResponse.ok("done", warnings=["large file", "slow query"])
        parsed = json.loads(r.to_text())
        assert len(parsed["warnings"]) == 2

    def test_error_has_error_code(self):
        r = ToolResponse.error("fail", "SESSION_ERROR")
        parsed = json.loads(r.to_text())
        assert parsed["error_code"] == "SESSION_ERROR"
        assert parsed["success"] is False


# ===========================================================================
# DAXResult and DAXEvalResponse
# ===========================================================================

class TestDAXModels:
    def test_dax_result_ok(self):
        r = DAXResult(name="Total Sales", expression="SUM(Sales[Amount])", value=42.0, status="ok")
        assert r.status == "ok"
        assert r.value == 42.0

    def test_dax_result_unsupported(self):
        r = DAXResult(name="M", value=None, status="unsupported",
                      error_message="USEROBJECTID not supported")
        assert r.status == "unsupported"
        assert "USEROBJECTID" in r.error_message

    def test_dax_eval_response(self):
        results = [
            DAXResult(name="A", value=2, status="ok"),
            DAXResult(name="B", value=None, status="unsupported"),
        ]
        resp = DAXEvalResponse(results=results)
        assert len(resp.results) == 2
        assert resp.results[0].status == "ok"
        assert resp.results[1].status == "unsupported"


# ===========================================================================
# Request models
# ===========================================================================

class TestRequestModels:
    def test_filter_context(self):
        fc = FilterContext(filters={"Products.Category": ["Hardware", "Software"]})
        assert len(fc.filters["Products.Category"]) == 2

    def test_filter_context_empty(self):
        fc = FilterContext()
        assert fc.filters == {}

    def test_dimension_ref(self):
        d = DimensionRef(table="Products", column="Category")
        assert d.table == "Products"
        assert d.column == "Category"

    def test_dimension_ref_parse(self):
        d = DimensionRef.parse("Sales.Amount")
        assert d.table == "Sales"
        assert d.column == "Amount"


# ===========================================================================
# Builder
# ===========================================================================

class TestBuilder:
    def test_builder_creates_valid_zip(self, tmp_path):
        from pbix_mcp.builder import PBIXBuilder
        builder = PBIXBuilder()
        builder.add_table("Products", [
            {"name": "Name", "data_type": "String"},
            {"name": "Price", "data_type": "Int64"},
        ], [
            {"Name": "Widget", "Price": 10},
            {"Name": "Gadget", "Price": 20},
        ])
        builder.add_measure("Products", "Total", "SUM(Products[Price])")

        out = tmp_path / "test.pbix"
        builder.save(str(out))
        assert out.exists()
        assert out.stat().st_size > 0

        import zipfile
        with zipfile.ZipFile(out) as zf:
            names = zf.namelist()
            assert "DataModel" in names
            assert "[Content_Types].xml" in names

    def test_builder_with_relationships(self, tmp_path):
        from pbix_mcp.builder import PBIXBuilder
        builder = PBIXBuilder()
        builder.add_table("Sales", [
            {"name": "ProductID", "data_type": "String"},
            {"name": "Amount", "data_type": "Int64"},
        ], [{"ProductID": "P1", "Amount": 100}])
        builder.add_table("Products", [
            {"name": "ProductID", "data_type": "String"},
            {"name": "Name", "data_type": "String"},
        ], [{"ProductID": "P1", "Name": "Widget"}])
        builder.add_relationship("Sales", "ProductID", "Products", "ProductID")
        builder.add_measure("Sales", "Total", "SUM(Sales[Amount])")

        out = tmp_path / "test_rel.pbix"
        builder.save(str(out))
        assert out.exists()


# ===========================================================================
# Logging config
# ===========================================================================

class TestLogging:
    def test_logger_exists(self):
        from pbix_mcp.logging_config import logger
        assert logger.name == "pbix_mcp"

    def test_set_level(self):
        import logging

        from pbix_mcp.logging_config import logger, set_level
        set_level("debug")
        assert logger.level == logging.DEBUG
        set_level("normal")  # Reset
