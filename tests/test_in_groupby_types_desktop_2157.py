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


# ---------------------------------------------------------------------------
# Comparisons by declared type, SUMMARIZECOLUMNS' filter tables, UNION's columns, INTERSECT
# (Power BI Desktop 2.157, 2026-11-18)
# ---------------------------------------------------------------------------

def _cmp(first: str, second: str) -> str:
    return (f"DAX comparison operations do not support comparing values of type {first} with values of type "
            f"{second}. Consider using the VALUE or FORMAT function to convert one of the values.")


@pytest.mark.parametrize("expr, first, second", [
    ('IF("a" = 1, 1, 0)', "Text", "Integer"),
    ('IF("1" = 1, 1, 0)', "Text", "Integer"),
    ('IF("a" < 1, 1, 0)', "Text", "Integer"),
    ('IF(1 == "a", 1, 0)', "Integer", "Text"),
    ('IF(DATE(2026,1,5) = "2026-01-05", 1, 0)', "Date", "Text"),
    ("COUNTROWS(FILTER(Opp, Opp[Name] = 1))", "Text", "Integer"),
    ("COUNTROWS(FILTER(Opp, Opp[When] = \"1/5/2026\"))", "Date", "Text"),
    ("IFERROR(COUNTROWS(FILTER(Opp, Opp[Name] = 1)), -1)", "Text", "Integer"),
    ('CALCULATE([Total], Opp[Name] = 1 || Opp[Rank] = 1)', "Text", "Integer"),
    ('CALCULATE([Total], Opp[Name] <> 1)', "Text", "Integer"),
    ('CALCULATE([Total], Opp[Name] > 1)', "Text", "Integer"),
    ('CALCULATE([Total], 1 = Opp[Name])', "Integer", "Text"),
    ('CALCULATE([Total], Opp[Name] = TRUE())', "Text", "True/False"),
    ('CALCULATE([Total], Opp[Flag] = "a")', "True/False", "Text"),
    ('CALCULATE([Total], Opp[Flag] = 1)', "True/False", "Integer"),
    ('CALCULATE([Total], Opp[When] = "1/5/2026")', "Date", "Text"),
])
def test_a_comparison_of_declared_types_that_clash(expr, first, second):
    assert error(expr).endswith(_cmp(first, second))


@pytest.mark.parametrize("expr, want", [
    ('CALCULATE(IF(SELECTEDVALUE(Opp[Name], 0) == 0, "none", "one"), Opp[Name] = "a")', "one"),
    ('CALCULATE(IF(SELECTEDVALUE(Opp[Name], 0) = 0, "none", "one"), Opp[Name] = "a")', "one"),
    ('IF(SELECTEDVALUE(Opp[Rank], "none") = 1, 1, 0)', 0),
    ('IF(IF(1 = 1, "x", 1) = 1, 1, 0)', 0),
    ('IF(COALESCE(MAX(Opp[Name]), 0) = 0, 1, 2)', 2),
    ('SUMX(Opp, IF(IF(Opp[Rank] = 1, "x", Opp[Rank]) = 2, 1, 0))', 1),
    ('SWITCH(TRUE(), IF(1 = 1, "x", 1) = 1, 1, 0)', 0),
    ('CALCULATE([Total], Opp[When] = 46027)', 1),                  # a date serial
    ('CALCULATE([Total], TREATAS({46027}, Opp[When]))', 1),
    ('CALCULATE([Total], TREATAS({"1"}, Opp[Rank]))', 1),
    ('CONVERT(-1.25, DATETIME)', dt.datetime(1899, 12, 29, 6, 0)),
])
def test_what_compares_by_declared_type(expr, want):
    assert value(expr) == want


@pytest.mark.parametrize("filters, rows", [
    ("FILTER(ALL(Opp[Rank]), Opp[Rank] = 1)", {"a": 1}),
    ("TREATAS({1}, Opp[Rank])", {"a": 1}),
    ("KEEPFILTERS(FILTER(ALL(Opp[Rank]), Opp[Rank] = 1))", {"a": 1}),
    ("FILTER(ALL(Opp[Flag]), Opp[Flag] = TRUE())", {"a": 4}),
    ("TREATAS({\"a\"}, Opp[Name])", {"a": 4}),
    ("TREATAS({1, 2, 3}, Opp[Rank]), FILTER(ALL(Opp[Flag]), Opp[Flag] = TRUE())", {"a": 4}),
    ("TREATAS({\"Lead\"}, 'Sales stage'[Sales Stage])", {"a": 4, "b": 2}),
    ("TREATAS({99}, Opp[Rank])", {}),
    ("FILTER(VALUES(Opp[Rank]), Opp[Rank] >= 2)", {"a": 3, "b": 2}),
    ("FILTER(Opp, Opp[Rank] = 3)", {"a": 3}),
])
def test_summarizecolumns_filter_tables_apply(filters, rows):
    sc = f"SUMMARIZECOLUMNS('Opp'[Name], {filters}, \"t\", [Total])"
    assert value(f"COUNTROWS({sc}) + 0") == len(rows)
    for name, total in rows.items():
        assert value(f"SUMX(FILTER({sc}, [Name] = \"{name}\"), [t])") == total


def test_summarizecolumns_filter_with_no_expression_keeps_the_groups_it_filters():
    assert value("CONCATENATEX(SUMMARIZECOLUMNS('Opp'[Name], TREATAS({1}, Opp[Rank])), [Name], \",\")") == "a"


@pytest.mark.parametrize("union, cells", [
    ('UNION({DATE(2026,1,6)}, {"a"})', ["1/6/2026", "a"]),
    ('UNION({TRUE()}, {"a"})', ["TRUE", "a"]),
    ('UNION({2.5}, {"a"})', ["2.5", "a"]),
    ('UNION({"a"}, {1})', ["a", "1"]),
    ('UNION({DATE(2026,1,6)}, {1})', [46028.0, 1]),
    ('UNION({TRUE()}, {1})', [1, 1]),
])
def test_union_unifies_each_column(union, cells):
    _v, err, e = ev("COUNTROWS(" + union + ")")
    assert err is None
    c = DAXContext(TABLES, dict(MEASURES))
    c.measure_tables = dict(HOMES)
    rows = DAXEngine()._eval_expr(union, c)
    assert [DAXEngine._row_cells(r)[0] for r in rows] == cells


def test_intersect_of_text_with_numbers():
    assert error('COUNTROWS(INTERSECT(UNION({1}, {"a"}), VALUES(Opp[Rank])))').endswith(
        "Function 'INTERSECT' does not support joining a column of type Text with a column of type Integer.")


# ---------------------------------------------------------------------------
# Refused before anything runs, by declared types (Power BI Desktop 2.157, 2026-11-18)
# ---------------------------------------------------------------------------

def _ev_static(expr: str, extra: dict | None = None, home: str = "_M", types: dict | None = None):
    t = dict(TABLES, _M={"columns": ["Column1"], "rows": []}, **(extra or {}))
    c = DAXContext(t, dict(MEASURES, Probe=expr))
    c.measure_tables = dict(HOMES, Probe=home)
    c.column_types = dict(types or {})
    e = DAXEngine()
    return e.evaluate_measure("Probe", c), e.eval_errors.get("Probe")


def test_an_if_branch_never_taken_is_checked_too():
    v, err = _ev_static('IF(FALSE(), CALCULATE([Total], Opp[Rank] IN {"a"}), 7)')
    assert v is None and err.endswith(_clash("Text", "Integer"))


def test_operands_are_typed_before_their_operator():
    # 1 = (1 IN {TRUE()}): the IN's error, not the comparison's
    v, err = _ev_static("IF(1 = 1 IN {TRUE()}, 1, 0)")
    assert err.endswith(_clash("True/False", "Integer"))


def test_a_column_with_no_value_is_typed_by_its_declared_type():
    v, err = _ev_static("IF(MAX('_M'[Column1]) = 1, 1, 0)", types={("_M", "Column1"): "String"})
    assert v is None and err.endswith(_cmp("Text", "Integer"))
    assert _ev_static("IF(MAX('_M'[Column1]) = 1, 1, 0)") == (0, None)      # no declared type: nothing to check by


ATTR = {"Attr": {"columns": ["Value"], "rows": [["x"], ["y"]]}}
AB = {"A": {"columns": ["Key", "W"], "rows": [[1, 5]]}, "B": {"columns": ["Key", "W"], "rows": [["x", 1], ["y", 2]]}}


@pytest.mark.parametrize("expr, extra, home, want", [
    # not measured on Desktop: row-context and arithmetic rules; the check refuses no valid DAX
    ("COUNTROWS(FILTER({1, 2, 3}, [Value] >= 2))", ATTR, "_M", 2),
    ("IF([Total] > 100, COUNTROWS(FILTER({1, 2, 3}, [Value] >= 2)), -1)", ATTR, "_M", -1),
    ("SUMX({1, 2, 3}, IF([Value] > 1, [Value], 0))", ATTR, "_M", 5),
    ('COUNTROWS(FILTER(B, [Key] = "x"))', AB, "A", 1),
    ('SUMX(B, IF([Key] = "x", [W], 0))', AB, "A", 1),
    ('IF("3" + BLANK() = 3, 1, 0)', None, "_M", 1),
    ('IF(BLANK() + "3" = 3, 1, 0)', None, "_M", 1),
    ('IF(BLANK() - TRUE() = -1, 1, 0)', None, "_M", 1),
    ('IF(FALSE(), IF("3" + BLANK() = 3, 1, 0), 5)', None, "_M", 5),
])
def test_valid_dax_is_not_refused(expr, extra, home, want):
    assert _ev_static(expr, extra, home) == (want, None)


def test_the_batch_memo_keeps_declared_types_apart():
    from pbix_mcp.dax.engine import evaluate_measures_batch
    t = dict(TABLES, _M={"columns": ["Column1"], "rows": []})
    ms = {"Probe": "IF(MAX('_M'[Column1]) = 1, 1, 0)", "Total": "SUM(Opp[Rank])"}
    homes = {"Probe": "_M", "Total": "_M"}
    cache: dict = {}
    typed = evaluate_measures_batch(["Probe"], t, ms, measure_tables=homes, cache=cache,
                                    column_types={("_M", "Column1"): "String"})
    plain = evaluate_measures_batch(["Probe"], t, ms, measure_tables=homes, cache=cache)
    assert typed == {"Probe": None} and plain == {"Probe": 0}
