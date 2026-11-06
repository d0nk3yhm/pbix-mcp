"""Issue #370: ModelReader.calculation_groups finds the item column when the
file leaves its SourceColumn empty.

Microsoft's 2026 AdventureWorks Sales sample: 'Date Role' and 'Time
Intelligence' each hold one data column (Type 1) whose SourceColumn is NULL,
and the groups were skipped.
"""
from __future__ import annotations

import pytest

from pbix_mcp.formats.model_reader import ModelReader


class _Metadata(ModelReader):
    """calculation_groups over fixed metadata rows, without a file."""

    def __init__(self, columns):  # noqa: D107 - skips ModelReader's file loading
        self._metadata_cache = {}
        self._columns = columns

    def _query_metadata(self, sql):
        if "[CalculationGroup]" in sql:
            return [{"ID": 3, "Precedence": 1, "TableName": "Date Role", "TableID": 7}]
        if "FROM [Column]" in sql:
            return self._columns
        return [{"Name": "By Due Date", "Expression": "SELECTEDMEASURE()", "Ordinal": 1,
                 "FormatString": None}]


@pytest.mark.parametrize("columns,want", [
    # The usual Desktop form: SourceColumn "Name" picks the column.
    ([{"ColumnName": "Date Role", "SourceColumn": "Name"},
      {"ColumnName": "Ordinal", "SourceColumn": "Ordinal"}], "Date Role"),
    # The 2026 sample: one column, no SourceColumn.
    ([{"ColumnName": "Date Role", "SourceColumn": None}], "Date Role"),
    # No SourceColumn on the item column, beside an Ordinal column.
    ([{"ColumnName": "Date Role", "SourceColumn": ""},
      {"ColumnName": "Ordinal", "SourceColumn": "Ordinal"}], "Date Role"),
])
def test_item_column_found(columns, want):
    groups = _Metadata(columns).calculation_groups
    assert [g["column"] for g in groups] == [want]
    assert [i["name"] for i in groups[0]["items"]] == ["By Due Date"]


def test_two_unsourced_columns_stay_undecided():
    cols = [{"ColumnName": "A", "SourceColumn": None}, {"ColumnName": "B", "SourceColumn": None}]
    assert _Metadata(cols).calculation_groups == []
