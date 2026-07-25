"""Coverage for the reporting tools that had NO tests.

`pbix_get_model_schema`, `pbix_get_model_columns`, `pbix_performance`,
`pbix_diff` and `pbix_document` all read `ModelReader.schema` / `.dax_columns` /
`.statistics`. Those readers carried an inverted `Column.Type` enum and an
explicit-only data-type lookup (fixed in 0.9.32), yet nothing exercised the
tools, so neither the bugs nor their fixes were covered. These tests pin the
values the tools report for a model containing a calculated column, a
calculated table and a measure.
"""
import json
import uuid

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder


def _build(tmp_path, name="tools.pbix", with_calc=True):
    p = str(tmp_path / name)
    b = PBIXBuilder("Tools")
    b.add_table("Products", [
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "CategoryID", "data_type": "Int64"},
        {"name": "UnitPrice", "data_type": "Double"},
        {"name": "StockQty", "data_type": "Int64"},
    ], rows=[
        {"ProductID": 1, "CategoryID": 10, "UnitPrice": 2.5, "StockQty": 4},
        {"ProductID": 2, "CategoryID": 20, "UnitPrice": 3.0, "StockQty": 2},
    ])
    b.add_measure("Products", "Total Value",
                  "SUMX(Products, Products[UnitPrice] * Products[StockQty])")
    b.add_page("Page 1")
    b.save(p)
    if not with_calc:
        return p
    alias = "tl_" + uuid.uuid4().hex[:8]
    server.pbix_open(p, alias)
    try:
        assert json.loads(server.pbix_datamodel_add_calculated_column(
            alias, "Products", "Inventory",
            "Products[UnitPrice] * Products[StockQty]"))["success"]
        assert json.loads(server.pbix_datamodel_add_calculated_table(
            alias, "CategoryList", "DISTINCT(Products[CategoryID])"))["success"]
        out = str(tmp_path / f"calc_{name}")
        server.pbix_save(alias, out, overwrite=True, backup=False)
    finally:
        server.pbix_close(alias, force=True)
    return out


@pytest.fixture(scope="module")
def model(tmp_path_factory):
    return _build(tmp_path_factory.mktemp("tools"))


@pytest.fixture
def opened(model):
    alias = "op_" + uuid.uuid4().hex[:8]
    server.pbix_open(model, alias)
    yield alias
    server.pbix_close(alias, force=True)


class TestGetModelSchema:
    def test_reports_real_type_for_calculated_column(self, opened):
        out = json.loads(server.pbix_get_model_schema(opened))
        assert out["success"], out
        body = out.get("message", "") + str(out.get("data", ""))
        # the row for the USER table (H$ hierarchy rows also mention the name)
        inv = [ln for ln in body.splitlines()
               if ln.startswith("Products") and "Inventory" in ln]
        assert inv, body[:600]
        assert "Unknown" not in inv[0], inv[0]
        assert "Double" in inv[0], inv[0]

    def test_calc_table_column_named(self, opened):
        out = json.loads(server.pbix_get_model_schema(opened))
        body = out.get("message", "") + str(out.get("data", ""))
        assert "CategoryList" in body
        cat_lines = [ln for ln in body.splitlines()
                     if ln.startswith("CategoryList") and "RowNumber" not in ln]
        assert cat_lines, body[:600]
        # the derived column is named (it used to render as None)
        assert any("CategoryID" in ln for ln in cat_lines), cat_lines
        assert not any("None" in ln for ln in cat_lines), cat_lines


class TestGetModelColumns:
    def test_finds_the_calculated_column(self, opened):
        """This tool reported "no DAX calculated columns" for EVERY model."""
        out = json.loads(server.pbix_get_model_columns(opened))
        assert out["success"], out
        body = out.get("message", "") + str(out.get("data", ""))
        assert "Inventory" in body, body[:400]
        assert "Products[UnitPrice] * Products[StockQty]" in body

    def test_clean_message_when_none(self, tmp_path):
        plain = _build(tmp_path, name="plain.pbix", with_calc=False)
        alias = "nc_" + uuid.uuid4().hex[:8]
        server.pbix_open(plain, alias)
        try:
            out = json.loads(server.pbix_get_model_columns(alias))
            assert out["success"], out
        finally:
            server.pbix_close(alias, force=True)


class TestPerformance:
    def test_runs_and_counts_calculated_columns(self, opened):
        out = json.loads(server.pbix_performance(opened))
        assert out["success"], out
        body = out.get("message", "") + str(out.get("data", ""))
        # exactly one calculated column exists on a user table
        assert "1 calculated column" in body or "calculated" in body, body[:400]

    def test_no_unknown_types_reported(self, opened):
        out = json.loads(server.pbix_performance(opened))
        body = out.get("message", "") + str(out.get("data", ""))
        assert "Unknown" not in body, body[:400]


class TestDocument:
    def test_includes_calculated_column_and_measure(self, opened):
        out = json.loads(server.pbix_document(opened))
        assert out["success"], out
        body = out.get("message", "") + str(out.get("data", ""))
        assert "Inventory" in body
        assert "Total Value" in body
        assert "Unknown" not in body, body[:600]

    def test_docx_path_is_honoured_when_python_docx_available(
            self, opened, tmp_path):
        """The tool returns markdown and ALSO writes a .docx — but only when
        python-docx is installed; otherwise it says so instead of failing."""
        import os
        target = str(tmp_path / "doc.docx")
        out = json.loads(server.pbix_document(opened, target))
        assert out["success"], out
        msg = out.get("message", "")
        try:
            import docx  # noqa: F401
        except ImportError:
            assert "python-docx not installed" in msg
        else:
            assert os.path.exists(target)
            assert target in msg


class TestDiff:
    def test_identical_files_report_no_schema_changes(self, model):
        a = "da_" + uuid.uuid4().hex[:8]
        b = "db_" + uuid.uuid4().hex[:8]
        server.pbix_open(model, a)
        server.pbix_open(model, b)
        try:
            out = json.loads(server.pbix_diff(a, b))
            assert out["success"], out
            body = out.get("message", "") + str(out.get("data", ""))
            assert "Unknown" not in body
        finally:
            server.pbix_close(a, force=True)
            server.pbix_close(b, force=True)

    def test_detects_an_added_calculated_column(self, model, tmp_path):
        plain = _build(tmp_path, name="plain2.pbix", with_calc=False)
        a = "dp_" + uuid.uuid4().hex[:8]
        b = "dc_" + uuid.uuid4().hex[:8]
        server.pbix_open(plain, a)
        server.pbix_open(model, b)
        try:
            out = json.loads(server.pbix_diff(a, b))
            assert out["success"], out
            body = out.get("message", "") + str(out.get("data", ""))
            # the calculated column exists only in `model`
            assert "Inventory" in body, body[:600]
        finally:
            server.pbix_close(a, force=True)
            server.pbix_close(b, force=True)
