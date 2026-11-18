"""IN with one value, GROUPBY's own columns, CALCULATE's filters comparing
types, and 'T'[Measure] under a table in no ``tables``, against Power BI
Desktop 2.157's answers.

Every expected value was measured on Desktop 2.157 (2026-11-18, engine
17.0.83.18): the query sent over its loopback connection as EVALUATE, on a
synthetic model of this shape (Opp: Rank Int64, Name String, Flag Boolean, When
DateTime; _M, no rows, home of Total = SUM(Opp[Rank])). Asked here as
measures: ``EVALUATE ROW("x", e)`` is the measure ``e``; a table is read
through TOCSV or COUNTROWS. Behaviour, not code: what Desktop returned,
nothing read from it.

- ``<value> IN <table>`` compares as CONTAINSROW: a value of another type than
  the table's is "Function 'CONTAINSROW' does not support comparing values of
  type <the table's> with values of type <the value's>. ...", whether or not a
  row would match. A constructor's or UNION's text beside other values makes
  a Text column: ``1 IN {1, "a"}`` errors, ``"a" IN {1, "a"}`` is TRUE.
- In a CALCULATE filter, ``T[c] IN <set>`` of another type is that error too,
  and IFERROR around the CALCULATE does not catch it; ``T[c] = <value>`` of
  text against a number is "DAX comparison operations do not support
  comparing values of type Text with values of type Integer. ..."; a date
  column against numbers compares them as date serials.
- GROUPBY's own columns are headed ``[Name]`` whatever their name.
"""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax.engine import DAXContext, DAXEngine

pytestmark = pytest.mark.unit

TABLES = {
    "Opp": {"columns": ["Rank", "Name", "Flag", "When"],
            "rows": [[1, "a", True, dt.datetime(2026, 1, 5)], [2, "b", False, dt.datetime(2026, 1, 6)],
                     [3, "a", True, dt.datetime(2026, 1, 9)]]},
    "Sales stage": {"columns": ["Probability", "Sales Stage", "Sales Stage ID"],
                    "rows": [[0.1, "Lead", 1], [0.2, "Qualify", 2]]},
}
MEASURES = {"Total": "SUM(Opp[Rank])"}
HOMES = {"Total": "_M"}


def ev(expr: str, tables: dict | None = None) -> tuple:
    """(value, error) of ``expr`` as a measure."""
    c = DAXContext(tables or TABLES, dict(MEASURES, Probe=expr))
    c.measure_tables = dict(HOMES, Probe="_M")
    e = DAXEngine()
    v = e.evaluate_measure("Probe", c)
    return v, e.eval_errors.get("Probe"), e


def value(expr: str):
    v, err, _e = ev(expr)
    assert err is None, err
    return v


def error(expr: str) -> str:
    v, err, _e = ev(expr)
    assert err is not None, f"answered {v!r}"
    return err


def _clash(first: str, second: str) -> str:
    return (f"Function 'CONTAINSROW' does not support comparing values of type {first} with values of type "
            f"{second}. Consider using the VALUE or FORMAT function to convert one of the values.")


# ---------------------------------------------------------------------------
# One value IN a table
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("expr, want", [
    ('"a" IN {1, "a"}', True),
    ('"A" IN {1, "a"}', True),
    ('BLANK() IN {1, "a"}', False),
    ('"a" IN UNION({1}, {"a"})', True),
    ('"a" IN UNION(VALUES(Opp[Rank]), VALUES(Opp[Name]))', True),
    ('DATE(2026,1,5) IN {46027}', True),
    ('46027 IN {DATE(2026,1,5)}', True),
    ("COUNTROWS(FILTER('Opp', 'Opp'[When] IN {46027}))", 1),
    ('CURRENCY(1) IN {1, 2}', True),
])
def test_what_compares(expr, want):
    assert value(expr) == want


@pytest.mark.parametrize("expr, first, second", [
    ('1 IN {1, "a"}', "Text", "Integer"),
    ('2 IN {1, "a"}', "Text", "Integer"),
    ('NOT (2 IN {1, "a"})', "Text", "Integer"),
    ('"1" IN {1}', "Integer", "Text"),
    ('1 IN {"1"}', "Text", "Integer"),
    ('"a" IN {1, 2}', "Integer", "Text"),
    ('TRUE() IN {1}', "Integer", "True/False"),
    ('TRUE() IN {"a"}', "Text", "True/False"),
    ('"a" IN {TRUE()}', "True/False", "Text"),
    ('DATE(2026,1,5) IN {"1/5/2026"}', "Text", "Date"),
    ("COUNTROWS(FILTER('Opp', 'Opp'[Rank] IN {1, \"a\"}))", "Text", "Integer"),
    ("COUNTROWS(FILTER('Opp', 'Opp'[Name] IN {1}))", "Integer", "Text"),
    ('CONTAINSROW({1, "a"}, 1)', "Text", "Integer"),
    ('CONTAINSROW({1}, "1")', "Integer", "Text"),
    ('1 IN UNION({1}, {"a"})', "Text", "Integer"),
    ('1 IN UNION(VALUES(Opp[Rank]), VALUES(Opp[Name]))', "Text", "Integer"),
])
def test_a_value_of_another_type_is_containsrows_error(expr, first, second):
    assert error(expr).endswith(_clash(first, second))


def test_an_expressions_mixed_column_compares_as_before():
    # Desktop's reading of a Variant expression column is not measured: only constructors and UNION are Text
    assert value('2 IN SELECTCOLUMNS(Opp, "v", IF(Opp[Rank] = 1, "x", Opp[Rank]))') is True


# ---------------------------------------------------------------------------
# CALCULATE's filters
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("expr, first, second", [
    ('CALCULATE([Total], Opp[Rank] IN {1, "a"})', "Text", "Integer"),
    ('CALCULATE([Total], Opp[Name] IN {1})', "Integer", "Text"),
    ('CALCULATE([Total], Opp[Rank] IN {"1"})', "Text", "Integer"),
    ('CALCULATE([Total], Opp[When] IN {DATE(2026,1,5), "x"})', "Text", "Date"),
    ('CALCULATE([Total], Opp[Flag] IN {1})', "Integer", "True/False"),
    ('CALCULATE([Total], Opp[Flag] IN {"a"})', "Text", "True/False"),
    ('CALCULATE([Total], KEEPFILTERS(Opp[Rank] IN {1, "a"}))', "Text", "Integer"),
    ('CALCULATE([Total], NOT Opp[Rank] IN {1, "a"})', "Text", "Integer"),
    ('CALCULATE([Total], FILTER(ALL(Opp[Rank]), Opp[Rank] IN {1, "a"}))', "Text", "Integer"),
    ('COUNTROWS(CALCULATETABLE(VALUES(Opp[Name]), Opp[Rank] IN {1, "a"}))', "Text", "Integer"),
    # IFERROR around the CALCULATE does not catch it
    ('IFERROR(CALCULATE([Total], Opp[Rank] IN {1, "a"}), -1)', "Text", "Integer"),
])
def test_a_calculate_filter_of_another_type_is_containsrows_error(expr, first, second):
    assert error(expr).endswith(_clash(first, second))


def test_a_calculate_equality_of_text_with_a_number():
    msg = ("DAX comparison operations do not support comparing values of type Text with values of type Integer. "
           "Consider using the VALUE or FORMAT function to convert one of the values.")
    assert error('CALCULATE([Total], Opp[Name] = 1)').endswith(msg)
    assert error('IFERROR(CALCULATE([Total], Opp[Name] = 1), -1)').endswith(msg)


@pytest.mark.parametrize("expr, want", [
    ('CALCULATE([Total], Opp[Rank] IN {1, 3})', 4),
    ('CALCULATE([Total], Opp[Name] IN {"A"})', 4),
    ('CALCULATE([Total], Opp[When] IN {46027})', 1),      # a date serial
])
def test_a_calculate_filter_of_one_type(expr, want):
    assert value(expr) == want


@pytest.mark.parametrize("serial", ["3000000", "-700000", "1e10"])
def test_a_serial_past_any_date_matches_nothing(serial):
    # as FILTER's route answers it (not measured on Desktop)
    assert value(f'CALCULATE([Total], Opp[When] IN {{{serial}}})') is None


def _model_dates() -> dict:
    """Opp with its When column as a decoded model holds it (DAXDateTime, a datetime subclass)."""
    from pbix_mcp.formats.vertipaq_decoder import _oa_date_to_python
    t = {k: {"columns": list(v["columns"]), "rows": [list(r) for r in v["rows"]]} for k, v in TABLES.items()}
    for r in t["Opp"]["rows"]:
        r[3] = _oa_date_to_python(float((r[3] - dt.datetime(1899, 12, 30)).days))
    assert type(t["Opp"]["rows"][0][3]) is not dt.datetime
    return t


@pytest.mark.parametrize("expr", [
    'CALCULATE([Total], Opp[When] IN {DATE(2026,1,5), "x"})',
    'CALCULATE([Total], Opp[When] IN {"x"})',
    'COUNTROWS(FILTER(Opp, Opp[When] IN {"x"}))',
])
def test_a_models_own_date_type_is_checked(expr):
    v, err, _e = ev(expr, _model_dates())
    assert v is None and err.endswith(_clash("Text", "Date"))


# ---------------------------------------------------------------------------
# GROUPBY's own columns
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", ["Rank", "Probability", "n"])
def test_groupby_heads_its_columns_by_name_alone(name):
    csv = value(f"TOCSV(GROUPBY('Opp', 'Opp'[Name], \"{name}\", SUMX(CURRENTGROUP(), 1)))")
    assert csv.splitlines()[0] == f"'Opp'[Name],[{name}]"


def test_groupby_with_no_group_column():
    csv = value("TOCSV(GROUPBY('Opp', \"Rank\", SUMX(CURRENTGROUP(), 'Opp'[Rank])))")
    assert csv.splitlines() == ["[Rank]", "6"]


def test_groupby_two_columns_of_one_name():
    assert error("COUNTROWS(GROUPBY('Opp', 'Opp'[Name], \"x\", SUMX(CURRENTGROUP(), 1), \"X\", "
                 "SUMX(CURRENTGROUP(), 2)))").endswith(
        "Function 'GROUPBY' cannot add column [X], as it has the same name with an existing column.")


def test_groupby_a_column_named_exactly_as_a_group_column_is_refused():
    # Desktop shows Opp[Name] and [Name] side by side; a row here holds a column by its name alone, so the
    # engine says it can't, never overwriting the group column
    _v, _err, e = ev("COUNTROWS(GROUPBY('Opp', 'Opp'[Name], \"Name\", SUMX(CURRENTGROUP(), 1)))")
    assert "GROUPBY" in e.unsupported_functions


def test_groupby_a_column_named_as_a_group_column_in_another_case_fits():
    csv = value("TOCSV(GROUPBY('Opp', 'Opp'[Name], \"name\", SUMX(CURRENTGROUP(), 1)))")
    assert csv.splitlines()[0] == "'Opp'[Name],[name]"


# ---------------------------------------------------------------------------
# 'T'[Measure] under a table in no ``tables``
# ---------------------------------------------------------------------------

def _measure_of(homes: dict, name: str, model_columns: dict | None = None, extra: dict | None = None):
    t = {"Opp": {"columns": ["Rank"], "rows": [[1], [2], [3]]}}
    m = dict({"Total": "SUM(Opp[Rank])", "Uses": "'_M'[Total] + 1"}, **(extra or {}))
    c = DAXContext(t, m)
    c.measure_tables = homes
    c.model_columns = model_columns or {}
    e = DAXEngine()
    return e.evaluate_measure(name, c), e.eval_errors.get(name)


def test_a_measure_whose_home_is_unknown_is_taken_under_any_table():
    # it leaked its (table, name) marker: a BLANK, and no error recorded
    assert _measure_of({}, "Uses") == (7, None)
    assert _measure_of({}, "Uses", {"Opp": ["Rank"], "_M": ["Column1"]}) == (7, None)


def test_under_a_table_the_model_does_not_name_or_not_its_home_it_is_an_error():
    for homes, model_columns in (({}, {"Opp": ["Rank"]}), ({"Total": "Opp"}, {})):
        v, err = _measure_of(homes, "Uses", model_columns)
        assert v is None
        assert err.endswith("Column 'Total' in table '_M' cannot be found or may not be used in this expression")


def test_a_column_of_an_empty_table_sharing_a_measures_name_stays_the_column():
    for homes in ({"Amount": "Opp"}, {}):
        v, err = _measure_of(dict(homes), "Probe", {"Opp": ["Rank"], "E": ["Amount"]},
                             {"Amount": "SUM(Opp[Rank]) * 100", "Probe": "SELECTEDVALUE('E'[Amount], -1)"})
        assert (v, err) == (-1, None)
