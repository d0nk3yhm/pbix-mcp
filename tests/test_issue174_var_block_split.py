"""Issue #174 (OpenBI's doc 58): a VAR block is split only at its own VAR /
RETURN keywords -- at depth 0, outside "strings", 'quoted names' and [names],
and VAR only when white space follows it.

_eval_var_return took every word VAR / RETURN in the text, so a block nested
in a declaration or in RETURN, a [Sales Var] reference, a [Return Qty] column
or a "Return rate" literal cut the outer block and the measure was BLANK. The
memo's shield split the same way, and the builder's reserved-name check read
COUNTROWS('Var Table') as a variable named Table.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe
(the Desktop oracle toolkit's build_b177.py)."""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as dax

pytestmark = pytest.mark.unit

TABLES = {
    "Emp": {"columns": ["k", "Return Qty", "Profit -- Net"], "rows": [[1, 4, 10], [2, 5, 20], [3, 6, 30]]},
    "Var Table": {"columns": ["v"], "rows": [[1], [2]]},
}
MEASURES = {
    "Sales Var": "SUM(Emp[k])",
    "argOnly": "SUMX(VALUES(Emp[k]), VAR b = Emp[k] RETURN b * 2)",
    "nestedInVar": "VAR a = SUMX(VALUES(Emp[k]), VAR b = Emp[k] RETURN b * 2) RETURN a",
    "nestedInReturn": "VAR m = 10 RETURN SUMX(VALUES(Emp[k]), VAR b = Emp[k] RETURN b + m)",
    "bracketWord": "VAR x = [Sales Var] RETURN x",
    "columnWord": "VAR x = SUM(Emp[Return Qty]) RETURN x",
    "stringWord": 'VAR t = "Return rate" RETURN t',
    "quotedTableWord": "VAR x = COUNTROWS('Var Table') RETURN x",
    "varpCall": "VAR x = VAR.P(Emp[k]) RETURN x",
    "lowerKeywords": "var x = 2 return x * 3",
    "escapedQuote": 'VAR t = "say ""RETURN"" now" RETURN LEN(t)',
    "varInReturnParen": "VAR a = 1 RETURN (VAR b = 2 RETURN a + b)",
    "concatxLoop": 'VAR p = 2 VAR bars = CONCATENATEX(VALUES(Emp[k]), VAR i = Emp[k] - 1 VAR x = p + i '
                   'RETURN "<" & x & ">", "") RETURN bars',
}
DESKTOP = {
    "argOnly": 12, "nestedInVar": 12, "nestedInReturn": 36, "bracketWord": 6, "columnWord": 15,
    "stringWord": "Return rate", "quotedTableWord": 2, "varpCall": 2 / 3, "lowerKeywords": 6,
    "escapedQuote": 16, "varInReturnParen": 3, "concatxLoop": "<2><3><4>",
}


def _results():
    names = [k for k in MEASURES if k != "Sales Var"]
    return dax.evaluate_measures_batch(names, TABLES, dict(MEASURES), filter_context={}, relationships=[])


@pytest.mark.parametrize("name", sorted(DESKTOP))
def test_matches_desktop(name):
    got = _results()[name]
    want = DESKTOP[name]
    assert got == (pytest.approx(want) if isinstance(want, float) else want)


@pytest.mark.parametrize("text,decls,ret", [
    ("VAR a = SUMX(T, VAR b = 1 RETURN b) RETURN a", [("a", "SUMX(T, VAR b = 1 RETURN b)")], "a"),
    ("VAR x = [Sales Var] RETURN x", [("x", "[Sales Var]")], "x"),
    ("VAR x = COUNTROWS('Var Table') RETURN x", [("x", "COUNTROWS('Var Table')")], "x"),
    ("VAR x = [a]]VAR b] RETURN x", [("x", "[a]]VAR b]")], "x"),
    ("VAR x = VAR.S(T[v]) RETURN x", [("x", "VAR.S(T[v])")], "x"),
    ("VAR a = {1, VAR b = 2 RETURN b} RETURN a", [("a", "{1, VAR b = 2 RETURN b}")], "a"),
])
def test_the_block_splits_at_its_own_keywords(text, decls, ret):
    assert dax._var_block_parts(text) == (decls, ret)


def test_the_builder_reads_no_variable_in_a_quoted_name():
    from pbix_mcp.builder import find_reserved_var_names

    assert find_reserved_var_names("VAR x = COUNTROWS('Var Table') RETURN x") == []
    assert find_reserved_var_names('VAR t = "VAR Table" RETURN t') == []
    assert find_reserved_var_names("VAR x = 1 // VAR Table\nRETURN x") == []
    assert find_reserved_var_names("VAR Table = 1 RETURN Table") == ["Table"]
    assert find_reserved_var_names("SUMX(T, VAR Filter = 1 RETURN Filter)") == ["Filter"]
