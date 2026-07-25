"""Regression tests for issues-15 — how calculated column/table metadata reads back.

Three reader-side defects made an authored calculated field look untyped,
non-calculated, or nameless:

1. ``DataType`` came only from ``ExplicitDataType``. A calculated column carries
   ``ExplicitDataType = 1`` (Automatic) with the real type in
   ``InferredDataType``, so it reported "Unknown".
2. ``IsCalculated`` tested ``Column.Type == 3`` — an inverted enum. The real AMO
   values are 1=Data, 2=Calculated, 3=RowNumber, 4=CalculatedTableColumn, so
   calculated columns read False and the RowNumber system column read True.
3. A calculated-TABLE column reported a NULL name. Desktop leaves
   ``ExplicitName`` NULL on such columns and carries the name in
   ``InferredName`` (it sets ExplicitName only when a user renames the column —
   verified across the public corpus), so the reader must coalesce the two.
"""
import json
import uuid

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.model_reader import ModelReader


@pytest.fixture(scope="module")
def authored(tmp_path_factory):
    """A model with BOTH an authored calculated column and calculated table."""
    tmp_path = tmp_path_factory.mktemp("i15")
    p = str(tmp_path / "i15.pbix")
    b = PBIXBuilder("I15")
    b.add_table("Products", [
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "CategoryID", "data_type": "Int64"},
        {"name": "UnitPrice", "data_type": "Double"},
        {"name": "StockQty", "data_type": "Int64"},
        {"name": "Label", "data_type": "String"},
    ], rows=[
        {"ProductID": 1, "CategoryID": 10, "UnitPrice": 2.5, "StockQty": 4,
         "Label": "a"},
        {"ProductID": 2, "CategoryID": 20, "UnitPrice": 3.0, "StockQty": 2,
         "Label": "b"},
    ])
    b.add_page("P")
    b.save(p)

    alias = "i15_" + uuid.uuid4().hex[:8]
    server.pbix_open(p, alias)
    try:
        assert json.loads(server.pbix_datamodel_add_calculated_column(
            alias, "Products", "Inventory",
            "Products[UnitPrice] * Products[StockQty]"))["success"]
        assert json.loads(server.pbix_datamodel_add_calculated_column(
            alias, "Products", "Tag",
            'Products[Label] & "!"'))["success"]
        assert json.loads(server.pbix_datamodel_add_calculated_table(
            alias, "CategoryList", "DISTINCT(Products[CategoryID])"))["success"]
        out = str(tmp_path / "i15_out.pbix")
        server.pbix_save(alias, out, overwrite=True, backup=False)
    finally:
        server.pbix_close(alias, force=True)
    return out


def _col(schema, table, column):
    for r in schema:
        if r["TableName"] == table and r["ColumnName"] == column:
            return r
    raise AssertionError(f"{table}[{column}] not found in schema")


class TestCalculatedColumnDataType:
    def test_numeric_calc_column_reports_real_type(self, authored):
        # was "Unknown" — ExplicitDataType is 1 (Automatic) on a calc column
        assert _col(ModelReader(authored).schema,
                    "Products", "Inventory")["DataType"] == "Double"

    def test_text_calc_column_reports_real_type(self, authored):
        assert _col(ModelReader(authored).schema,
                    "Products", "Tag")["DataType"] == "String"

    def test_calc_table_column_reports_real_type(self, authored):
        assert _col(ModelReader(authored).schema,
                    "CategoryList", "CategoryID")["DataType"] == "Int64"

    def test_regular_columns_unchanged(self, authored):
        schema = ModelReader(authored).schema
        assert _col(schema, "Products", "UnitPrice")["DataType"] == "Double"
        assert _col(schema, "Products", "ProductID")["DataType"] == "Int64"
        assert _col(schema, "Products", "Label")["DataType"] == "String"


class TestIsCalculatedFlag:
    def test_calculated_column_is_flagged(self, authored):
        assert _col(ModelReader(authored).schema,
                    "Products", "Inventory")["IsCalculated"] is True

    def test_calculated_table_column_is_flagged(self, authored):
        # Type=4 CalculatedTableColumn also counts as calculated
        assert _col(ModelReader(authored).schema,
                    "CategoryList", "CategoryID")["IsCalculated"] is True

    def test_regular_column_is_not_flagged(self, authored):
        assert _col(ModelReader(authored).schema,
                    "Products", "UnitPrice")["IsCalculated"] is False

    def test_rownumber_is_not_flagged(self, authored):
        """Type=3 RowNumber used to read as calculated — a false positive."""
        rn = [r for r in ModelReader(authored).schema
              if (r["ColumnName"] or "").startswith("RowNumber")]
        assert rn, "expected a RowNumber system column"
        assert all(r["IsCalculated"] is False for r in rn)


class TestColumnNameNeverNull:
    def test_calc_table_column_has_a_name(self, authored):
        """Desktop leaves ExplicitName NULL and carries the name in
        InferredName; the reader coalesces so the name is never None."""
        names = [r["ColumnName"] for r in ModelReader(authored).schema
                 if r["TableName"] == "CategoryList"]
        assert None not in names
        assert "CategoryID" in names  # inherited from the source column

    def test_no_null_names_anywhere(self, authored):
        assert all(r["ColumnName"] is not None
                   for r in ModelReader(authored).schema)

    def test_names_are_safe_for_string_ops(self, authored):
        """A client doing columnName.startswith(...) must not hit None."""
        for r in ModelReader(authored).schema:
            assert not r["ColumnName"].startswith("\x00")


class TestDaxColumnsProperty:
    """`dax_columns` filtered on Type=3 (RowNumber, which never carries an
    Expression), so it returned an EMPTY list for every model — making
    pbix_get_model_columns always report "no DAX calculated columns"."""

    def test_finds_authored_calc_column(self, authored):
        dc = ModelReader(authored).dax_columns
        by_name = {r["ColumnName"]: r for r in dc}
        assert "Inventory" in by_name
        assert by_name["Inventory"]["TableName"] == "Products"
        assert by_name["Inventory"]["Expression"] == \
            "Products[UnitPrice] * Products[StockQty]"

    def test_reports_real_type_not_unknown(self, authored):
        by_name = {r["ColumnName"]: r for r in ModelReader(authored).dax_columns}
        assert by_name["Inventory"]["DataType"] == "Double"
        assert by_name["Tag"]["DataType"] == "String"

    def test_excludes_plain_data_columns(self, authored):
        names = {r["ColumnName"] for r in ModelReader(authored).dax_columns}
        assert "UnitPrice" not in names
        assert not any(n.startswith("RowNumber") for n in names)


class TestStatisticsColumnCount:
    """ColumnCount counted `Type != 2`, which EXCLUDED calculated columns and
    INCLUDED the RowNumber system column."""

    def test_matches_real_user_columns(self, authored):
        mr = ModelReader(authored)
        schema = mr.schema
        for st in mr.statistics:
            tn = st["TableName"]
            if tn.startswith(("H$", "R$", "U$")):
                continue
            real = [r for r in schema
                    if r["TableName"] == tn
                    and not (r["ColumnName"] or "").startswith("RowNumber")]
            assert st["ColumnCount"] == len(real), tn

    def test_counts_the_calculated_column(self, authored):
        mr = ModelReader(authored)
        products = next(s for s in mr.statistics
                        if s["TableName"] == "Products")
        # ProductID, CategoryID, UnitPrice, StockQty, Label + Inventory + Tag
        assert products["ColumnCount"] == 7


class TestFieldResolution:
    """A calculated-table column had a NULL name and so could never be bound;
    it now resolves when qualified."""

    def test_calc_table_column_resolves_when_qualified(self, authored, tmp_path):
        alias = "rf_" + uuid.uuid4().hex[:8]
        server.pbix_open(authored, alias)
        try:
            info = server._open_files[alias]
            assert server._resolve_model_field(
                info, "CategoryList[CategoryID]") == ("CategoryList", "CategoryID")
            assert server._resolve_model_field(
                info, "Products[Inventory]") == ("Products", "Inventory")
            # a unique bare name still resolves
            assert server._resolve_model_field(
                info, "UnitPrice") == ("Products", "UnitPrice")
        finally:
            server.pbix_close(alias, force=True)

    def test_ambiguous_bare_name_errors_clearly(self, authored):
        from pbix_mcp.errors import LayoutParseError
        alias = "rf_" + uuid.uuid4().hex[:8]
        server.pbix_open(authored, alias)
        try:
            info = server._open_files[alias]
            with pytest.raises(LayoutParseError) as ei:
                server._resolve_model_field(info, "CategoryID")
            msg = str(ei.value)
            assert "ambiguous" in msg
            assert "CategoryList" in msg and "Products" in msg
        finally:
            server.pbix_close(alias, force=True)
