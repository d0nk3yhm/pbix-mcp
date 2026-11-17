"""Answers that were silently wrong, made Power BI Desktop 2.157's.

Expected values: Power BI Desktop 2.157, each query run against its engine
over its loopback connection (black-box), on Microsoft's MIT sample
"2026 Power BI Samples Revamp/Revenue Opportunities.pbix" (2026-11-12) and on
a synthetic model (2026-11-17) of the tables PROBE below copies, with _M a
table of no rows holding the measure Total. The tables here are synthetic
copies of those shapes.

Where Desktop errors the engine answered a value: ROW's missing or repeated
arguments, ADDCOLUMNS over a column the table has, MEDIAN / MEDIANX over no
number, a measure qualified by another table, a tuple IN of another width
or comparing text with a number. Where Desktop answers, the engine answered
otherwise: a tuple before IN, a VAR block as a VAR's value, a measure on a
table with no rows, a table with no rows, the headings of the columns
ADDCOLUMNS and SUMMARIZE add, a constructor's text column.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

STAGE = {"columns": ["Probability", "Sales Stage", "Sales Stage ID"],
         "rows": [[0.1, "Lead", 1], [0.2, "Qualify", 2], [0.4, "Solution", 3]]}
MODEL = {"Sales stage": STAGE, "N": {"columns": ["v"], "rows": [[1], [2], [4]]}}


def measure(expr: str):
    """(value, error) of ``expr`` evaluated as a measure."""
    eng = de.DAXEngine()
    ctx = de.DAXContext(MODEL, {"m": expr}, None, None, None, [])
    value = eng.evaluate_measure("m", ctx)
    return value, eng.eval_errors.get("m")


def error_of(expr: str) -> str:
    value, err = measure(expr)
    assert err is not None, f"{expr} answered {value!r}, no error"
    return err


def test_row_missing_argument():
    assert "Argument '2' in ROW function is required." in error_of('COUNTROWS(ROW("x", ))')


def test_row_too_few_arguments():
    assert ("Too few arguments were passed to the ROW function. The minimum argument count for the "
            "function is 2.") in error_of('COUNTROWS(ROW("x"))')


@pytest.mark.parametrize("first, second", [("AMPM", "ampm"), ("a", "A")])
def test_row_names_compare_without_case(first, second):
    assert (f"Function 'ROW' cannot add column [{second}], as it has the same name with an "
            "existing column.") in error_of(f'COUNTROWS(ROW("{first}", 1, "{second}", 2))')


def test_row_with_distinct_names_still_answers():
    assert measure('COUNTROWS(ROW("a", 1, "b", 2))') == (1, None)


@pytest.mark.parametrize("name", ["Probability", "probability"])
def test_addcolumns_over_a_column_the_table_has(name):
    assert (f"Function 'ADDCOLUMNS' cannot add column [{name}] since it already exists."
            in error_of(f"SUMX(ADDCOLUMNS('Sales stage', \"{name}\", 1), 'Sales stage'[Probability])"))


def test_addcolumns_over_a_one_column_table():
    assert "cannot add column [v] since it already exists." in error_of(
        'COUNTROWS(ADDCOLUMNS(VALUES(N[v]), "v", 1))')


def test_addcolumns_two_new_columns_of_one_name():
    assert "Function 'ADDCOLUMNS' cannot add column [X], as it has the same name with an existing column." in \
        error_of("COUNTROWS(ADDCOLUMNS('Sales stage', \"x\", 1, \"X\", 2))")


def test_addcolumns_new_names_still_answer():
    # a name only like an existing one is a new column
    assert measure("SUMX(ADDCOLUMNS('Sales stage', \"Probability2\", 1), [Probability2])") == (3, None)
    assert measure('SUMX(ADDCOLUMNS({1, 2}, "Value2", [Value] * 10), [Value2])') == (30, None)


def test_median_over_text():
    assert "MEDIAN does not support expressions of type string/Boolean/date." in error_of(
        "MEDIAN('Sales stage'[Sales Stage])")


def test_median_over_numbers_still_answers():
    assert measure("MEDIAN(N[v])") == (2, None)
    assert measure("MEDIAN('Sales stage'[Probability])") == (0.2, None)


def test_constructor_of_number_and_text_is_text():
    assert measure('CONCATENATEX({1, "a"}, [Value], ",")') == ("1,a", None)
    # text, so "1" is the value and the number 1 is no longer one
    assert measure('COUNTROWS(FILTER({1, "a"}, [Value] = "1"))') == (1, None)
    assert measure('COUNTROWS(FILTER({1, "a"}, ISTEXT([Value])))') == (2, None)


def test_constructor_of_numbers_stays_numbers():
    assert measure('COUNTROWS(FILTER({1, 2}, ISNUMBER([Value])))') == (2, None)
    assert measure('SUMX({1, 2.5}, [Value])') == (3.5, None)


def test_row_constructor_column_of_number_and_text_is_text():
    # Value1 holds 1 beside "a": a Text column; Value2 stays numbers
    assert measure('COUNTROWS(FILTER({(1, 1), ("a", 2)}, ISTEXT([Value1])))') == (2, None)
    assert measure('COUNTROWS(FILTER({(1, 1), ("a", 2)}, ISNUMBER([Value2])))') == (2, None)


# ---------------------------------------------------------------------------
# Measured 2026-11-17 on a synthetic model (the same Desktop, the same route):
# Opp (Rank Int64, Name String, Flag Boolean, When DateTime) and _M, a table
# with no rows, home of the measure Total = SUM(Opp[Rank]).
# ---------------------------------------------------------------------------

D = __import__("datetime").datetime
OPP = {"columns": ["Rank", "Name", "Flag", "When"],
       "rows": [[1, "a", True, D(2026, 1, 5)], [2, "b", False, D(2026, 1, 6)], [3, "a", True, D(2026, 1, 9)]]}
M_EMPTY = {"columns": ["Column1"], "rows": []}
PROBE = {"Sales stage": STAGE, "Opp": OPP, "_M": M_EMPTY}   # _M as the server loads a table with no rows
PROBE_COLUMNS = {"Sales stage": set(STAGE["columns"]), "Opp": set(OPP["columns"]), "_M": {"Column1"}}
PROBE_MEASURES = {"Total": "SUM(Opp[Rank])"}


def probe(expr: str, tables: dict | None = None):
    """(value, error) of ``expr`` as a measure of the probe model."""
    eng = de.DAXEngine()
    ctx = de.DAXContext(PROBE if tables is None else tables, {**PROBE_MEASURES, "m": expr}, None, None, None, [])
    ctx.measure_tables = {"Total": "_M", "m": "Opp"}
    ctx.model_columns = PROBE_COLUMNS
    value = eng.evaluate_measure("m", ctx)
    return value, eng.eval_errors.get("m")


def probe_error(expr: str) -> str:
    value, err = probe(expr)
    assert err is not None, f"{expr} answered {value!r}, no error"
    return err


# A row constructor before IN: CONTAINSROW, as Microsoft Learn documents IN

@pytest.mark.parametrize("expr, value", [
    ("COUNTROWS(FILTER('Opp', ('Opp'[Rank], 'Opp'[Name]) IN {(1, \"a\"), (3, \"a\")}))", 2),
    ("COUNTROWS(FILTER('Opp', ('Opp'[Rank], 'Opp'[Name]) IN {(1, \"a\")}))", 1),
    ('(1, "a") IN {(1, "a"), (2, "b")}', True),
    ('(1, "b") IN {(1, "a"), (2, "b")}', False),
    ('(BLANK(), "a") IN {(0, "a")}', False),
    ('(BLANK(), "a") IN {(BLANK(), "a")}', True),
    ('(1, "A") IN {(1, "a")}', True),
    ("(2, \"b\") IN SELECTCOLUMNS('Opp', \"r\", 'Opp'[Rank], \"n\", 'Opp'[Name])", True),
    ('NOT ((1, "a") IN {(1, "a")})', False),
])
def test_row_constructor_in(expr, value):
    got, err = probe(expr)
    assert err is None and got == value and type(got) is type(value), (got, err)


def test_row_constructor_in_text_against_number():
    assert ("Function 'CONTAINSROW' does not support comparing values of type Integer with values of "
            "type Text. Consider using the VALUE or FORMAT function to convert one of the values.") in \
        probe_error('("1", "a") IN {(1, "a")}')


def test_row_constructor_in_needs_a_value_per_column():
    assert ("The number of arguments is invalid. Function CONTAINSROW must have a value for each column "
            "in the table expression.") in probe_error("(1, 2) IN {1, 2}")


def test_containsrow_reads_alike():
    assert probe("CONTAINSROW({(1, \"a\")}, 1, \"A\")") == (True, None)
    assert "Function CONTAINSROW must have a value for each column" in probe_error("CONTAINSROW({1, 2}, 1, 2)")


# A VAR block as a VAR's value, or after RETURN

@pytest.mark.parametrize("expr, value", [
    ("VAR a = VAR b = 1 RETURN b + 1 RETURN a", 2),
    ("VAR a = 1 VAR b = VAR c = 2 RETURN c RETURN a + b", 3),
    ("VAR a = VAR b = VAR c = 1 RETURN c + 1 RETURN b * 10 RETURN a + 100", 120),
    ("VAR a = 1 RETURN VAR b = a + 1 RETURN b * 2", 4),
    ("VAR a = VAR b = 2 RETURN b * 3 VAR c = a + 1 RETURN c", 7),
])
def test_nested_var_blocks(expr, value):
    assert probe(expr) == (value, None)


# A measure on a table with no rows, and a measure qualified by its own table

def test_a_measure_on_a_table_with_no_rows():
    assert probe("'_M'[Total]") == (6, None)
    assert probe("[Total]") == (6, None)


def test_a_measure_qualified_by_another_table_is_no_column_of_it():
    assert "Column 'Total' in table 'Opp' cannot be found or may not be used in this expression" in \
        probe_error("'Opp'[Total]")


def test_a_table_with_no_rows_is_an_empty_table():
    # measured: COUNTROWS BLANK, EVALUATE '_M' no row; ISEMPTY follows (not measured)
    assert probe("COUNTROWS('_M')") == (None, None)
    assert probe("ISEMPTY('_M')") == (True, None)


def test_a_table_missing_from_the_tables_is_not_claimed_empty():
    # a table whose decode failed stays out of the tables; nothing says it holds no rows
    without = {k: v for k, v in PROBE.items() if k != "Sales stage"}
    assert probe("ISEMPTY('Sales stage')", without)[0] is not True


def test_a_measure_on_a_table_left_out_of_the_tables_is_still_its_measure():
    without = {k: v for k, v in PROBE.items() if k != "_M"}
    assert probe("'_M'[Total]", without) == (6, None)


@pytest.mark.parametrize("expr", [
    # reasoned, not measured: each row is in its table
    "CONTAINSROW(ADDCOLUMNS(VALUES(Opp[Rank]), \"x\", 1), 2, 1)",
    "CONTAINSROW(CROSSJOIN(VALUES(Opp[Name]), VALUES(Opp[Rank])), \"a\", 2)",
    "(\"a\", 2) IN CROSSJOIN(VALUES(Opp[Name]), VALUES(Opp[Rank]))",
    "(1, \"a\", TRUE()) IN SELECTCOLUMNS('Opp', \"r\", 'Opp'[Rank], \"n\", 'Opp'[Name], \"f\", 'Opp'[Flag])",
])
def test_containsrow_reads_every_column_of_a_row(expr):
    assert probe(expr) == (True, None)


def test_the_server_keeps_a_table_with_no_rows(tmp_path):
    """End to end: a built file with an empty table, as Desktop opened it."""
    import json
    import uuid

    from pbix_mcp import server
    alias = "empty_" + uuid.uuid4().hex[:8]
    tables = [{"name": "Opp", "columns": [{"name": "Rank", "data_type": "Int64"}],
               "rows": [{"Rank": 1}, {"Rank": 2}, {"Rank": 3}]},
              {"name": "_M", "columns": [{"name": "Column1", "data_type": "String"}], "rows": []}]
    measures = [{"table": "_M", "name": "Total", "expression": "SUM(Opp[Rank])"},
                {"table": "Opp", "name": "Qualified", "expression": "'_M'[Total] + 0"},
                {"table": "Opp", "name": "M is empty", "expression": "ISEMPTY('_M')"}]
    r = json.loads(server.pbix_create(str(tmp_path / "e.pbix"), alias, json.dumps(tables), json.dumps(measures)))
    assert r.get("success"), r
    try:
        out = json.loads(server.pbix_evaluate_dax(alias, "Qualified", apply_default_filters=False))
        assert out["results"][0]["value"] == 6, out
        # the table itself: an empty table, not a name the engine can't read
        out = json.loads(server.pbix_evaluate_dax(alias, "M is empty", apply_default_filters=False))
        assert out["results"][0]["value"] is True, out
    finally:
        server._open_files.pop(alias, None)
        server._dax_cache.pop(alias, None)


# The columns ADDCOLUMNS and SUMMARIZE add are their own, whatever their names

@pytest.mark.parametrize("table", [
    "ADDCOLUMNS(SUMMARIZE('Opp', 'Opp'[Name]), \"Rank\", 1)",
    "ADDCOLUMNS(GROUPBY('Opp', 'Opp'[Name]), \"Rank\", 1)",
    "ADDCOLUMNS(SUMMARIZECOLUMNS('Opp'[Name]), \"Rank\", 1)",
    "SUMMARIZE('Opp', 'Opp'[Name], \"Rank\", 1)",
])
def test_an_added_column_is_headed_by_its_name(table):
    assert probe(f"TOCSV({table})") == ("'Opp'[Name],[Rank]\na,1\nb,1", None)


def test_addcolumns_over_summarize_of_a_column_it_has():
    assert "Function 'ADDCOLUMNS' cannot add column [Name] since it already exists." in \
        probe_error("COUNTROWS(ADDCOLUMNS(SUMMARIZE('Opp', 'Opp'[Name]), \"Name\", 1))")


# MEDIAN and MEDIANX over text, TRUE / FALSE and dates

@pytest.mark.parametrize("expr, fn", [
    ("MEDIAN('Opp'[Flag])", "MEDIAN"), ("MEDIAN('Opp'[When])", "MEDIAN"),
    ("MEDIANX('Opp', 'Opp'[Name])", "MEDIANX"),
])
def test_median_over_no_number(expr, fn):
    assert f"{fn} does not support expressions of type string/Boolean/date." in probe_error(expr)


# A table constructor's column of text beside other values

@pytest.mark.parametrize("ctor, text", [
    ('{2.5, "a"}', "2.5,a"), ('{DATE(2026, 1, 6), "a"}', "1/6/2026,a"), ('{TRUE(), "a"}', "TRUE,a"),
    ('{CURRENCY(1.5), "a"}', "1.5,a"), ('{BLANK(), "a"}', ",a"),
])
def test_constructor_values_beside_text_are_text(ctor, text):
    assert probe(f'CONCATENATEX({ctor}, [Value], ",")') == (text, None)
    blank = ctor.startswith("{BLANK()")
    assert probe(f"COUNTROWS(FILTER({ctor}, ISTEXT([Value])))") == (1 if blank else 2, None)



def test_an_added_column_marker_is_no_calculated_table_column():
    """A calculated table read from ADDCOLUMNS / SUMMARIZE rows keeps their
    columns only: the rows' marker of the columns they added is none."""
    from pbix_mcp.dax.calc_tables import _convert_dax_result

    # a SUMMARIZE row of Opp's Name with the column Rank added
    rows = [{"__table__": "Opp", "Name": "a", "Rank": 1, "__added__": frozenset({"Rank"})}]
    out = _convert_dax_result(rows, {})
    assert out["columns"] == ["Name", "Rank"] and out["rows"] == [["a", 1]], out
