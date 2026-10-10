"""Issue #169: SEARCH and CONTAINSSTRING take the wildcards ? (any one
character) and * (any run), and a character right after a ~ is literal. SEARCH
and FIND raise an error when the text isn't found and no NotFoundValue is
given, and return the NotFoundValue when one is.

Desktop's matcher has its own rules at the edges, measured here: a start
past the text finds nothing (SEARCH("", "") fails; FIND and
CONTAINSSTRINGEXACT do find "" in ""); a pattern that begins with * is found
at position 1 whatever the start; two or more * alone are never found; a ~
gives nothing unless a ~ precedes it ("~" at the end is nothing, "~~*" is the
literal "~*").

The engine compared literally (SEARCH("b*", "xabc") was -1 for 3), returned
-1 for text it didn't find, and FIND ignored its fourth argument. SEARCH's
linguistic matching beyond ASCII (ß = ss, width, ignorable characters) is a
separate issue and not pinned here.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b166.py, build_b166b.py, build_b174.py - build_b175.py)."""
from __future__ import annotations

import datetime as dt

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

MODEL = {
    "T": {"columns": ["c"], "rows": [["x"]]},
    # build_b175.py's N
    "N": {"columns": ["k", "d", "dt", "b", "i"],
          "rows": [["a", 1.0, dt.datetime(2024, 1, 2), True, 1],
                   ["b", 2.5, dt.datetime(2024, 3, 4, 15, 30), False, 2]]},
}


def _engine(dax):
    """The engine's answer as ADOMD prints it; "ERROR" for an error."""
    eng = de.DAXEngine()
    try:
        got = eng._eval_expr(dax, de.DAXContext(MODEL, {}))
    except Exception:
        return "ERROR"
    if got is None:
        return None
    if isinstance(got, bool):
        return "True" if got else "False"
    if isinstance(got, float) and got.is_integer():
        return str(int(got))
    return str(got)


def _want(want):
    """A date of the current year (measured in 2026) as its serial now."""
    if isinstance(want, str) and want.startswith("THIS_YEAR "):
        _, m, d = want.split()
        return str((dt.date(dt.date.today().year, int(m), int(d)) - dt.date(1899, 12, 30)).days)
    return want


def _same(got, want):
    if got == want:
        return True
    try:
        return abs(float(got) - float(want)) <= 1e-9 * max(1.0, abs(float(want)))
    except (TypeError, ValueError):
        return False

DESKTOP = [
    ('b166:search_case', 'SEARCH("B", "abc")', '2'),
    ('b166:search_star', 'SEARCH("b*", "xabc")', '3'),
    ('b166:search_q', 'SEARCH("?c", "abc")', '2'),
    ('b166:search_tilde', 'SEARCH("~*", "a*b")', '2'),
    ('b166:search_nf', 'SEARCH("z", "abc", 1, -1)', '-1'),
    ('b166:search_err', 'IFERROR(SEARCH("z", "abc"), "err")', 'err'),
    ('b166:search_start', 'SEARCH("a", "abca", 2)', '4'),
    ('b166:find_case', 'IFERROR(FIND("B", "abc"), "err")', 'err'),
    ('b166:find_start', 'FIND("b", "abcb", 3)', '4'),
    ('b166:find_nf', 'FIND("z", "abc", 1, 0)', '0'),
    ('b166:contains_case', 'IF(CONTAINSSTRING("abc", "B"), 1, 0)', '1'),
    ('b166:contains_wild', 'IF(CONTAINSSTRING("abc", "a?c"), 1, 0)', '1'),
    ('b166:contains_star', 'IF(CONTAINSSTRING("abc", "a*"), 1, 0)', '1'),
    ('b166:containsx_case', 'IF(CONTAINSSTRINGEXACT("abc", "B"), 1, 0)', '0'),
    ('b166b:search_star_only', 'SEARCH("*", "abc")', '1'),
    ('b166b:search_q_only', 'SEARCH("?", "abc")', '1'),
    ('b166b:search_empty', 'SEARCH("", "abc")', '1'),
    ('b166b:search_tilde_q', 'SEARCH("~?", "a?b")', '2'),
    ('b166b:search_star_mid', 'SEARCH("a*c", "xxabbbc")', '3'),
    ('b166b:contains_tilde', 'IF(CONTAINSSTRING("a*b", "~*"), 1, 0)', '1'),
    ('b166b:contains_empty', 'IF(CONTAINSSTRING("abc", ""), 1, 0)', '1'),
    ('b166b:find_empty', 'FIND("", "abc")', '1'),
    ('b174:s_start_past', 'IFERROR(SEARCH("a", "abc", 5), "err")', 'err'),
    ('b174:s_start_past_nf', 'IFERROR(SEARCH("a", "abc", 5, -1), "err")', '-1'),
    ('b174:s_start_end', 'IFERROR(SEARCH("c", "abc", 3), "err")', '3'),
    ('b174:s_empty_end', 'IFERROR(SEARCH("", "abc", 4), "err")', 'err'),
    ('b174:s_empty_empty', 'IFERROR(SEARCH("", ""), "err")', 'err'),
    ('b174:s_blank_start', 'IFERROR(SEARCH("c", "abc", BLANK()), "err")', 'err'),
    ('b174:s_blank_within', 'IFERROR(SEARCH("", BLANK()), "err")', 'err'),
    ('b174:s_nf_blank', 'IF(ISBLANK(SEARCH("z", "abc", 1, BLANK())), 1, 0)', '1'),
    ('b174:f_start_past_nf', 'IFERROR(FIND("a", "abc", 5, -1), "err")', '-1'),
    ('b174:f_empty_end', 'IFERROR(FIND("", "abc", 4), "err")', 'err'),
    ('b174:f_wild_q', 'IFERROR(FIND("a?c", "abc"), "err")', 'err'),
    ('b174:f_wild_lit', 'FIND("?", "a?c")', '2'),
    ('b174:f_wild_star', 'IFERROR(FIND("a*", "abc"), "err")', 'err'),
    ('b174:s_tilde_alone', 'IFERROR(SEARCH("~", "a~b"), "err")', '1'),
    ('b174:s_tilde_tilde', 'IFERROR(SEARCH("~~", "a~b"), "err")', '2'),
    ('b174:s_tilde_letter', 'IFERROR(SEARCH("~b", "a~b"), "err")', '3'),
    ('b174:s_tilde_end', 'IFERROR(SEARCH("a~", "a~b"), "err")', '1'),
    ('b174:s_star_end', 'SEARCH("b*", "ab")', '2'),
    ('b174:s_q_past', 'IFERROR(SEARCH("c?", "abc"), "err")', 'err'),
    ('b174:s_q_newline', 'IFERROR(SEARCH("a?b", "a" & UNICHAR(10) & "b"), "err")', '1'),
    ('b174:s_star_newline', 'IFERROR(SEARCH("a*b", "a" & UNICHAR(10) & "b"), "err")', '1'),
    ('b174:s_star_star', 'SEARCH("**", "abc")', 'ERROR'),
    ('b174:s_q_star', 'IFERROR(SEARCH("?*?", "a"), "err")', 'err'),
    ('b174:cx_wild_q', 'IF(CONTAINSSTRINGEXACT("abc", "a?c"), 1, 0)', '0'),
    ('b174:cx_wild_star', 'IF(CONTAINSSTRINGEXACT("abc", "a*"), 1, 0)', '0'),
    ('b174:cx_tilde', 'IF(CONTAINSSTRINGEXACT("a*b", "~*"), 1, 0)', '0'),
    ('b174:c_blank_empty', 'IF(CONTAINSSTRING(BLANK(), ""), 1, 0)', '0'),
    ('b174:c_star_only', 'IF(CONTAINSSTRING("", "*"), 1, 0)', '0'),
    ('b174:s_e_acute_case', 'IFERROR(SEARCH(UNICHAR(233), "x" & UNICHAR(201)), "err")', '2'),
    ('b174:s_accent', 'IFERROR(SEARCH("e", "x" & UNICHAR(233)), "err")', 'err'),
    ('b174:s_kelvin', 'IFERROR(SEARCH("k", "x" & UNICHAR(8490)), "err")', 'err'),
    ('b174:s_dotless_i', 'IFERROR(SEARCH("i", "x" & UNICHAR(305)), "err")', 'err'),
    ('b174:s_cyrillic', 'IFERROR(SEARCH(UNICHAR(1076), "x" & UNICHAR(1044)), "err")', '2'),
    ('b174:s_hyphen', 'IFERROR(SEARCH("coop", "co-op"), "err")', 'err'),
    ('b174:s_pos_after_sz', 'IFERROR(SEARCH("b", UNICHAR(223) & "b"), "err")', '2'),
    ('b174:f_e_acute_case', 'IFERROR(FIND(UNICHAR(233), "x" & UNICHAR(201)), "err")', 'err'),
    ('b174:c_accent', 'IF(CONTAINSSTRING("x" & UNICHAR(233), "e"), 1, 0)', '0'),
    ('b174:c_case_beyond', 'IF(CONTAINSSTRING("x" & UNICHAR(201), UNICHAR(233)), 1, 0)', '1'),
    ('b174b:w_star_star', 'IFERROR(SEARCH("**", "abc"), "err")', 'err'),
    ('b174b:w_star_star_lit', 'IFERROR(SEARCH("**", "a*c"), "err")', 'err'),
    ('b174b:w_star_q', 'IFERROR(SEARCH("*?", "abc"), "err")', '1'),
    ('b174b:w_star_q_lit', 'IFERROR(SEARCH("*?", "a?c"), "err")', '1'),
    ('b174b:w_q_star', 'IFERROR(SEARCH("?*", "abc"), "err")', '1'),
    ('b174b:w_a_star_star_c', 'IFERROR(SEARCH("a**c", "abc"), "err")', '1'),
    ('b174b:w_a_star_star_c_lit', 'IFERROR(SEARCH("a**c", "a*c"), "err")', '1'),
    ('b174b:w_star_tilde_star', 'IFERROR(SEARCH("*~*", "ab*c"), "err")', '1'),
    ('b174b:w_star_a', 'IFERROR(SEARCH("*c", "abc"), "err")', '1'),
    ('b174b:w_a_star_b_empty', 'IFERROR(SEARCH("a*b", "ab"), "err")', '1'),
    ('b174b:w_a_star_alone', 'IFERROR(SEARCH("a*", "a"), "err")', '1'),
    ('b174b:w_star_b_star', 'IFERROR(SEARCH("*b*", "abc"), "err")', '1'),
    ('b174b:w_a_star_b_star_c', 'IFERROR(SEARCH("a*b*c", "axbyc"), "err")', '1'),
    ('b174b:w_a_star_c_first', 'IFERROR(SEARCH("a*c", "abcac"), "err")', '1'),
    ('b174b:w_backtrack', 'IFERROR(SEARCH("a*bc", "abxbc"), "err")', '1'),
    ('b174b:w_backtrack2', 'IFERROR(SEARCH("a*b?d", "abcbxd"), "err")', '1'),
    ('b174b:w_q_q', 'IFERROR(SEARCH("??", "abc"), "err")', '1'),
    ('b174b:w_q_end', 'IFERROR(SEARCH("b?", "ab"), "err")', 'err'),
    ('b174b:w_q_past', 'IFERROR(SEARCH("a?", "xa"), "err")', 'err'),
    ('b174b:w_tilde_q_star', 'IFERROR(SEARCH("~?*", "a?b"), "err")', '2'),
    ('b174b:w_tilde_tilde_star', 'IFERROR(SEARCH("~~*", "a~b"), "err")', 'err'),
    ('b174b:w_tilde_mid', 'IFERROR(SEARCH("a~b", "xab"), "err")', '2'),
    ('b174b:w_tilde_only2', 'IFERROR(SEARCH("~", "abc"), "err")', '1'),
    ('b174b:w_tilde_tail', 'IFERROR(SEARCH("b~", "abc"), "err")', '2'),
    ('b174b:w_start_wild', 'IFERROR(SEARCH("*", "abc", 2), "err")', '1'),
    ('b174b:w_start_end_wild', 'IFERROR(SEARCH("*", "abc", 3), "err")', '1'),
    ('b174b:w_star_empty_within', 'IFERROR(SEARCH("*", ""), "err")', 'err'),
    ('b174b:c_star_star', 'IF(CONTAINSSTRING("abc", "**"), 1, 0)', '0'),
    ('b174b:c_star_q', 'IF(CONTAINSSTRING("abc", "*?"), 1, 0)', '1'),
    ('b174b:c_tilde', 'IF(CONTAINSSTRING("abc", "~"), 1, 0)', '1'),
    ('b174b:c_start', 'IF(CONTAINSSTRING("abc", "c"), 1, 0)', '1'),
    ('b174c:w_lead_star_start', 'IFERROR(SEARCH("*a", "aba", 2), "err")', '1'),
    ('b174c:w_lead_star_start3', 'IFERROR(SEARCH("*a", "aba", 3), "err")', '1'),
    ('b174c:w_lead_star_c_start3', 'IFERROR(SEARCH("*c", "abc", 3), "err")', '1'),
    ('b174c:w_lead_star_none', 'IFERROR(SEARCH("*a", "abc", 2), "err")', 'err'),
    ('b174c:w_lead_q_start', 'IFERROR(SEARCH("?*", "abc", 2), "err")', '2'),
    ('b174c:w_lead_qa_start', 'IFERROR(SEARCH("?a", "xaba", 2), "err")', '3'),
    ('b174c:w_b_star_start', 'IFERROR(SEARCH("b*", "abcb", 3), "err")', '4'),
    ('b174c:w_star_star_star', 'IFERROR(SEARCH("**", "*"), "err")', 'err'),
    ('b174c:w_star_star_2', 'IFERROR(SEARCH("**", "**"), "err")', 'err'),
    ('b174c:w_star_star_a', 'IFERROR(SEARCH("**a", "ba"), "err")', '1'),
    ('b174c:w_a_star_star', 'IFERROR(SEARCH("a**", "ab"), "err")', '1'),
    ('b174c:w_a_star_star_end', 'IFERROR(SEARCH("a**", "a"), "err")', '1'),
    ('b174c:w_three_stars', 'IFERROR(SEARCH("***", "abc"), "err")', 'err'),
    ('b174c:w_a_three_stars_c', 'IFERROR(SEARCH("a***c", "abc"), "err")', '1'),
    ('b174c:w_star_q_star', 'IFERROR(SEARCH("*?*", "a"), "err")', '1'),
    ('b174c:w_star_tilde_star_star', 'IFERROR(SEARCH("*~**", "a*b"), "err")', '1'),
    ('b174c:w_tilde_star_star', 'IFERROR(SEARCH("~**", "*a"), "err")', '1'),
    ('b174c:w_tilde_star_star2', 'IFERROR(SEARCH("~**", "a*"), "err")', '2'),
    ('b174c:w_star_a_star', 'IFERROR(SEARCH("*a*", "bab"), "err")', '1'),
    ('b174c:w_q_star_lead', 'IFERROR(SEARCH("?*", "a"), "err")', '1'),
    ('b174c:w_star_q_lead', 'IFERROR(SEARCH("*?", "a"), "err")', '1'),
    ('b174c:w_star_q_q', 'IFERROR(SEARCH("*??", "a"), "err")', 'err'),
    ('b174c:w_tt_star_lit', 'IFERROR(SEARCH("~~*", "a~*b"), "err")', '2'),
    ('b174c:w_tt_star_star', 'IFERROR(SEARCH("~~*", "a*b"), "err")', 'err'),
    ('b174c:w_tt_star_tilde', 'IFERROR(SEARCH("~~*", "~"), "err")', 'err'),
    ('b174c:w_tt_a', 'IFERROR(SEARCH("~~a", "x~a"), "err")', '2'),
    ('b174c:w_tt_q', 'IFERROR(SEARCH("~~?", "x~a"), "err")', 'err'),
    ('b174c:w_tt_q_lit', 'IFERROR(SEARCH("~~?", "x?"), "err")', 'err'),
    ('b174c:w_ttt', 'IFERROR(SEARCH("~~~", "x~~"), "err")', '2'),
    ('b174c:w_ttt_star', 'IFERROR(SEARCH("~~~*", "x~*"), "err")', 'err'),
    ('b174c:w_tq_tt', 'IFERROR(SEARCH("~?~~", "x?~"), "err")', '2'),
    ('b174c:w_x_tt_star', 'IFERROR(SEARCH("x~~*", "x~"), "err")', 'err'),
    ('b174c:w_tilde_q_lit_tail', 'IFERROR(SEARCH("a~?", "a?"), "err")', '1'),
    ('b174c:w_tilde_star_lit_mid', 'IFERROR(SEARCH("a~*b", "xa*b"), "err")', '2'),
    ('b174c:w_tilde_tilde_tail', 'IFERROR(SEARCH("a~~", "xa~"), "err")', '2'),
    ('b174c:w_lone_tilde_empty', 'IFERROR(SEARCH("~", ""), "err")', 'err'),
    ('b174c:w_star_empty', 'IFERROR(SEARCH("*", "a", 2), "err")', 'err'),
    ('b174c:w_q_start_end', 'IFERROR(SEARCH("?", "abc", 3), "err")', '3'),
    ('b174c:c_lead_star', 'IF(CONTAINSSTRING("abc", "*c"), 1, 0)', '1'),
    ('b174c:c_tt_star', 'IF(CONTAINSSTRING("a~b", "~~*"), 1, 0)', '0'),
    ('b174c:c_star_star_lit', 'IF(CONTAINSSTRING("**", "**"), 1, 0)', '0'),
    ('b175:cx_empty_empty', 'IF(CONTAINSSTRINGEXACT("", ""), 1, 0)', '1'),
    ('b175:cx_blank_empty', 'IF(CONTAINSSTRINGEXACT(BLANK(), ""), 1, 0)', '1'),
    ('b175:cx_abc_empty', 'IF(CONTAINSSTRINGEXACT("abc", ""), 1, 0)', '1'),
    ('b175:cx_empty_a', 'IF(CONTAINSSTRINGEXACT("", "a"), 1, 0)', '0'),
    ('b175:f_empty_empty', 'IFERROR(FIND("", ""), "err")', '1'),
    ('b175:f_empty_blank_nf', 'IFERROR(FIND("", BLANK(), 1, -1), "err")', '1'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))
