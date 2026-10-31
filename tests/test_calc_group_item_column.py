"""ModelReader.calculation_groups finds a group's item column when the file leaves SourceColumn empty."""
from __future__ import annotations

import pytest

from pbix_mcp.formats.model_reader import ModelReader


class _Reader(ModelReader):
    """calculation_groups over given metadata rows: no file to read."""

    def __init__(self, columns):
        self._metadata_cache = {}
        self._cols = columns

    def _query_metadata(self, sql):
        if "FROM [CalculationGroup]" in sql:
            return [{"ID": 1, "Precedence": 5, "TableName": "Role", "TableID": 9}]
        if "FROM [Column]" in sql:
            return self._cols
        return [{"Name": "By Due", "Expression": "SELECTEDMEASURE()", "Ordinal": 0, "FormatString": None}]


@pytest.mark.parametrize("cols,want", [
    ([{"ColumnName": "Name", "SourceColumn": "Name"}, {"ColumnName": "Ordinal", "SourceColumn": "Ordinal"}], "Name"),
    ([{"ColumnName": "Date Role", "SourceColumn": None}], "Date Role"),   # measured: AdventureWorks Sales 2026
    ([{"ColumnName": "Role", "SourceColumn": None}, {"ColumnName": "Ordinal", "SourceColumn": "Ordinal"}], "Role"),
])
def test_the_item_column_when_the_file_names_no_source_column(cols, want):
    groups = _Reader(cols).calculation_groups
    assert [g["column"] for g in groups] == [want]
    assert groups[0]["items"][0]["name"] == "By Due"


def test_two_unnamed_columns_stay_ambiguous():
    assert _Reader([{"ColumnName": "A", "SourceColumn": None}, {"ColumnName": "B", "SourceColumn": None}]).calculation_groups == []
