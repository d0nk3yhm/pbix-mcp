"""Issue #164: the engine treats the empty text "" as a value, not BLANK.

ISBLANK("") was TRUE, COUNTX / COUNTAX skipped "", MIN of a column and
COALESCE passed over it, and FIRSTNONBLANK(T[c], T[c]) skipped a "" value.
Desktop: ISBLANK("") and ISBLANK("" & BLANK()) are FALSE, a column's "" rows
are not blank, COUNTX(T, T[c]) counts them, MIN(T[c]) is "" (LEN 0),
COALESCE("", "x") is "". COUNTBLANK counts "" as blank and = "" matches
BLANK, as before. Models built by pbix-mcp hold "" from 0.9.132 (#161);
Desktop's own files always could.

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

DESKTOP = [('isb_lit', 'IF(ISBLANK(""), 1, 0)', '0'), ('isb_blank', 'IF(ISBLANK(BLANK()), 1, 0)', '1'), ('isb_concat', 'IF(ISBLANK("" & BLANK()), 1, 0)', '0'), ('isb_rows', 'COUNTROWS(FILTER(T, ISBLANK(T[c])))', '1'), ('isb_not', 'COUNTROWS(FILTER(T, NOT ISBLANK(T[c])))', '4'), ('countx', 'COUNTX(T, T[c])', '4'), ('countax', 'COUNTAX(T, T[c])', '4'), ('counta', 'COUNTA(T[c])', '4'), ('countblank', 'COUNTBLANK(T[c])', '3'), ('min_len', 'LEN(MIN(T[c]))', '0'), ('min_isb', 'IF(ISBLANK(MIN(T[c])), 1, 0)', '0'), ('max', 'MAX(T[c])', 'b'), ('minx_len', 'LEN(MINX(T, T[c]))', '0'), ('minx_isb', 'IF(ISBLANK(MINX(T, T[c])), 1, 0)', '0'), ('maxx', 'MAXX(T, T[c])', 'b'), ('min2', 'LEN(MIN("", "a"))', '0'), ('coalesce_lit', 'LEN(COALESCE("", "x"))', '0'), ('coalesce_blank', 'COALESCE(BLANK(), "x")', 'x'), ('coalesce_col', 'CONCATENATEX(T, "[" & COALESCE(T[c], "x") & "]", ",", T[i], ASC)', '[],[b],[x],[a],[]'), ('fnb_text', 'LEN(FIRSTNONBLANK(T[c], T[c]))', '0'), ('fnb_text_isb', 'IF(ISBLANK(FIRSTNONBLANK(T[c], T[c])), 1, 0)', '0'), ('lnb_text', 'LASTNONBLANK(T[c], T[c])', 'b')]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _engine(dax) == want
