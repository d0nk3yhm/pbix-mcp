"""Issue #165: a text function takes a one-row table's value.

LEN, UPPER, LOWER, LEFT, RIGHT, MID, TRIM, SUBSTITUTE, REPLACE, REPT, SEARCH,
FIND, CONTAINSSTRING, EXACT, UNICODE, VALUE and COMBINEVALUES applied str() to
their argument, so LEN(FIRSTNONBLANK(T[c], 1)) measured the internal row
list's Python text (56) and UPPER(TOPN(1, VALUES(T[c]), T[c])) returned it.
They now convert as DAX does (and as `&` already did): a one-row table is its
value, a date its text (LEN(LASTDATE(T[d])) is 8, "4/2/2024"), 0 is "0".

Expected values: Power BI Desktop 2.152 over ADOMD (build_b163.py)."""
from __future__ import annotations

from datetime import date

import pytest

pytestmark = pytest.mark.unit

MODEL = {'T': {'columns': ['c', 'i', 'd', 'n'], 'rows': [['', 1, date(2024, 1, 5), '10'], ['b', 2, date(2024, 3, 9), '20'], [None, 3, date(2024, 2, 1), '30'], ['a', 4, date(2024, 1, 20), '40'], ['', 5, date(2024, 4, 2), '50']]}}

def _engine(dax):
    from pbix_mcp.dax import engine as de

    got = de.evaluate_measures_smart(["p"], MODEL, {"p": dax}, {}, simulate_row_context=False)["p"]
    if got is None:
        return None
    if isinstance(got, bool):
        return "True" if got else "False"
    if isinstance(got, float) and got.is_integer():
        return str(int(got))
    return str(got)

DESKTOP = [('fnb_one', 'LEN(FIRSTNONBLANK(T[c], 1))', '0'), ('fnb_upper', 'UPPER(FIRSTNONBLANK(T[c], 1))', ''), ('lnb_len', 'LEN(LASTNONBLANK(T[c], 1))', '1'), ('lnb_left', 'LEFT(LASTNONBLANK(T[c], 1), 1)', 'b'), ('lastdate_len', 'LEN(LASTDATE(T[d]))', '8'), ('lastdate_year', 'YEAR(LASTDATE(T[d]))', '2024'), ('firstdate_fmt', 'FORMAT(FIRSTDATE(T[d]), "yyyy-mm-dd")', '2024-01-05'), ('topn_upper', 'UPPER(TOPN(1, VALUES(T[c]), T[c], DESC))', 'B'), ('values_len', 'CALCULATE(LEN(VALUES(T[c])), T[c] = "b")', '1'), ('trim_fnb', 'LEN(TRIM(FIRSTNONBLANK(T[c], 1)) & "z")', '1'), ('search_lnb', 'SEARCH("b", LASTNONBLANK(T[c], 1), 1, -1)', '1'), ('substitute_lnb', 'SUBSTITUTE(LASTNONBLANK(T[c], 1), "b", "B")', 'B'), ('concat_fnb', 'CONCATENATE(LASTNONBLANK(T[c], 1), "!")', 'b!'), ('value_fnb', 'VALUE(FIRSTNONBLANK(T[n], 1))', '10'), ('unicode_lnb', 'UNICODE(LASTNONBLANK(T[c], 1))', '98'), ('exact_lnb', 'IF(EXACT(LASTNONBLANK(T[c], 1), "b"), 1, 0)', '1')]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _engine(dax) == want
