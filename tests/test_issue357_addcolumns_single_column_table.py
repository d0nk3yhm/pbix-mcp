"""#357 (reported with PR #352 by @allanon2): a calculated table written
ADDCOLUMNS over a table of one column keeps that column.

What was wrong (0.9.149): a row of one column (VALUES / DISTINCT / ALL of a
column, CALENDAR) holds its cell under ``__column__`` / ``__value__``, and
ADDCOLUMNS puts its columns beside them. The calculated table treated both
keys as bookkeeping once the row had a column of its own, so
ADDCOLUMNS(VALUES(T[C]), "N", ..) was the table N alone and
ADDCOLUMNS(CALENDAR(..), "Month", MONTH([Date])) lost Date.

Expected values:
- Microsoft's ADDCOLUMNS: "Returns a table with all its original columns and
  the added ones"; VALUES(T[C]) is the one column C, CALENDAR the one column
  Date.
- The engine reads such a row the same way (DAXEngine._row_cells, #234):
  COUNTROWS(DISTINCT(ADDCOLUMNS(VALUES(M[a]), "z", 0))) counts M[a]'s values.
- Power BI Desktop 2.152, build_b358.py: a_values_csv heads
  'T'[Region],[N] and a_cal_csv [Date],[M]; a_values_concat "E=Ex,N=Nx,W=Wx",
  a_values_distinct 3.
"""
from __future__ import annotations

from datetime import datetime

import pytest

from pbix_mcp.dax import engine as de
from pbix_mcp.dax.calc_tables import evaluate_calc_table_expression as build_table

pytestmark = pytest.mark.unit

SALES = {"Sales": {"columns": ["Region", "Qty"], "rows": [["West", 5], ["East", 7], ["West", 1]]}}


def test_addcolumns_over_values_keeps_the_column():
    got, err = build_table('ADDCOLUMNS(VALUES(Sales[Region]), "One", 1)', SALES)
    assert err is None, err
    assert got == {"columns": ["Region", "One"], "rows": [["West", 1], ["East", 1]]}


def test_addcolumns_over_values_reads_the_column_per_row():
    got, err = build_table('ADDCOLUMNS(VALUES(Sales[Region]), "Code", UPPER(LEFT(Sales[Region], 2)), '
                          '"Total", CALCULATE(SUM(Sales[Qty])))', SALES)
    assert err is None, err
    assert got == {"columns": ["Region", "Code", "Total"], "rows": [["West", "WE", 6], ["East", "EA", 7]]}


def test_addcolumns_over_distinct_and_all_of_a_column():
    for src in ("DISTINCT(Sales[Region])", "ALL(Sales[Region])"):
        got, err = build_table(f'ADDCOLUMNS({src}, "Len", LEN(Sales[Region]))', SALES)
        assert err is None, (src, err)
        assert got == {"columns": ["Region", "Len"], "rows": [["West", 4], ["East", 4]]}, src


def test_addcolumns_over_calendar_keeps_date():
    got, err = build_table('ADDCOLUMNS(CALENDAR(DATE(2023, 12, 31), DATE(2024, 1, 1)), "Yr", YEAR([Date]))', {})
    assert err is None, err
    assert got == {"columns": ["Date", "Yr"],
                   "rows": [[datetime(2023, 12, 31), 2023], [datetime(2024, 1, 1), 2024]]}


def test_a_name_the_column_already_has_is_refused():
    got, err = build_table('ADDCOLUMNS(VALUES(Sales[Region]), "REGION", 0)', SALES)
    # the engine refuses it first, with Power BI Desktop 2.157's text
    # (build_b371.py addcol_existing_case, #375)
    assert got is None and "Function ADDCOLUMNS cannot add column [REGION] since it already exists." in err


def test_columns_named_like_the_engines_keys_stay_columns():
    # only GENERATE's own rows hold _inner_ keys, only CROSSJOIN's _2_ ones
    got, err = build_table('ADDCOLUMNS(VALUES(Sales[Region]), "_inner_a", 1, "_2_b", 2)', SALES)
    assert err is None and got == {"columns": ["Region", "_inner_a", "_2_b"],
                                   "rows": [["West", 1, 2], ["East", 1, 2]]}


def test_one_column_tables_alone_and_selectcolumns_read_as_before():
    assert build_table("VALUES(Sales[Region])", SALES)[0] == {"columns": ["Region"], "rows": [["West"], ["East"]]}
    # SELECTCOLUMNS(VALUES(..)) keeps its source's column as lineage only (#225)
    got, err = build_table('SELECTCOLUMNS(VALUES(Sales[Region]), "r", Sales[Region] & "*")', SALES)
    assert err is None and got == {"columns": ["r"], "rows": [["West*"], ["East*"]]}
    got, err = build_table('ADDCOLUMNS(SELECTCOLUMNS(VALUES(Sales[Region]), "r", Sales[Region]), "s", 2)', SALES)
    assert err is None and got == {"columns": ["r", "s"], "rows": [["West", 2], ["East", 2]]}
    # ROW / GENERATESERIES hold their column as a key of its own
    assert build_table('ADDCOLUMNS(GENERATESERIES(3, 4), "sq", [Value] * [Value])', {})[0] == \
        {"columns": ["Value", "sq"], "rows": [[3, 9], [4, 16]]}


def test_the_engine_reads_the_column_of_such_rows():
    ctx = de.DAXContext(SALES, {}, None, None, None, [])
    ev = de.DAXEngine()._eval_expr
    assert ev('CONCATENATEX(ADDCOLUMNS(VALUES(Sales[Region]), "Code", LEFT(Sales[Region], 1)), '
              'Sales[Region] & ":" & [Code], " ", Sales[Region], ASC)', ctx) == "East:E West:W"
    assert ev('COUNTROWS(DISTINCT(ADDCOLUMNS(VALUES(Sales[Region]), "k", 0)))', ctx) == 2
