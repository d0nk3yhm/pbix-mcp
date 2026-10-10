"""Issue #175 (OpenBI's doc 58): /* */ comments are read, and a comment is not
looked for inside "strings", [names] or 'quoted names'.

_strip_line_comments removed // and -- comments only, so a /* */ comment
anywhere made a measure BLANK -- before a VAR block, TRUE -- and a -- inside a
column name was taken for a comment.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe
(the Desktop oracle toolkit's build_b177.py)."""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as dax

pytestmark = pytest.mark.unit

TABLES = {
    "Emp": {"columns": ["k", "Profit -- Net"], "rows": [[1, 10], [2, 20], [3, 30]]},
    "A//B": {"columns": ["v"], "rows": [[1]]},
}
MEASURES = {
    "lead": "/* note */ SUM(Emp[k])",
    "tail": "SUM(Emp[k]) /* note */",
    "inside": "SUM(/* note */ Emp[k])",
    "betweenVarAndName": "VAR /* note */ x = 5 RETURN x",
    "beforeVar": "/* note */ VAR x = 5 RETURN x",
    "multiLine": "VAR\n    /*** Chart Constants ***/\n    W = 20\nVAR H = 3\nRETURN W * H",
    "inString": 'VAR t = "/* not a comment */" RETURN t',
    "lineCommentControl": "SUM(Emp[k]) // note",
    "dashInName": "SUM(Emp[Profit -- Net])",
    "slashInQuoted": "COUNTROWS('A//B')",
    "quoteInComment": "SUM(Emp[k]) // don't",
    "blockQuote": "SUM(/* it's */ Emp[k])",
    "blockInDivide": "DIVIDE(SUM(Emp[k]) /* x */, 2)",
    "svgString": 'VAR s = "<!-- Data -->" RETURN LEN(s)',
}
DESKTOP = {
    "lead": 6, "tail": 6, "inside": 6, "betweenVarAndName": 5, "beforeVar": 5, "multiLine": 60,
    "inString": "/* not a comment */", "lineCommentControl": 6, "dashInName": 60, "slashInQuoted": 1,
    "quoteInComment": 6, "blockQuote": 6, "blockInDivide": 3, "svgString": 13,
}


@pytest.mark.parametrize("name", sorted(DESKTOP))
def test_matches_desktop(name):
    got = dax.evaluate_measures_batch([name], TABLES, dict(MEASURES), filter_context={}, relationships=[])[name]
    assert got == DESKTOP[name]


@pytest.mark.parametrize("text,want", [
    ("/* a */ SUM(T[x])", "SUM(T[x])"),
    ("SUM(T[x]) /* open", "SUM(T[x])"),
    ('"/* kept */"', '"/* kept */"'),
    ("T[a -- b] + 1", "T[a -- b] + 1"),
    ("'a // b'[c]", "'a // b'[c]"),
    ("T[a]]--b] -- gone", "T[a]]--b]"),
    ('"<!-- svg -->\n  x"', '"<!-- svg -->\n  x"'),
])
def test_the_stripper(text, want):
    assert dax._strip_line_comments(text) == want
