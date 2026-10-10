"""Issue #168: LEFT, RIGHT, MID, REPT, REPLACE, UNICHAR, SUBSTITUTE's instance and
SEARCH / FIND's start round half away from zero; a count that rounds below 0,
or a start or an instance below 1, is an error; BLANK is 0; RIGHT(x, 0) is "".

The engine truncated (int()): LEFT("abcd", 2.5) was "ab" where Desktop gives
"abc", MID("abcdef", 1.5, 1) "a" for "b", REPLACE("abcdef", 2.5, 1, "X")
"aXcdef" for "abXdef". A negative count returned a value (LEFT("abc", -1) was
"ab"), RIGHT(x, 0) the whole text (s[-0:]), and REPLACE("abc", 1, BLANK(), "X")
failed in Python (int(None)).

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b166.py, build_b166b.py, build_b174.py)."""
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
    ('b166:right0', 'LEN(RIGHT("abc", 0))', '0'),
    ('b166:left0', 'LEN(LEFT("abc", 0))', '0'),
    ('b166:right5', 'RIGHT("abc", 5)', 'abc'),
    ('b166:left_frac', 'LEFT("abcd", 2.9)', 'abc'),
    ('b166:right_frac', 'RIGHT("abcd", 2.9)', 'bcd'),
    ('b166:mid_past', 'LEN(MID("abc", 5, 2))', '0'),
    ('b166:mid_frac', 'MID("abcdef", 2.7, 2.2)', 'cd'),
    ('b166:mid_zero', 'IFERROR(MID("abc", 0, 2), "err")', 'err'),
    ('b166:left_neg', 'IFERROR(LEFT("abc", -1), "err")', 'err'),
    ('b166:left_default', 'LEFT("abc")', 'a'),
    ('b166:rept0', 'LEN(REPT("ab", 0))', '0'),
    ('b166:rept_frac', 'REPT("ab", 2.9)', 'ababab'),
    ('b166:subst_inst', 'SUBSTITUTE("aaa", "a", "b", 2)', 'aba'),
    ('b166:replace_mid', 'REPLACE("abcdef", 2, 3, "X")', 'aXef'),
    ('b166b:left_25', 'LEFT("abcd", 2.5)', 'abc'),
    ('b166b:left_15', 'LEFT("abcd", 1.5)', 'ab'),
    ('b166b:left_35', 'LEFT("abcdef", 3.5)', 'abcd'),
    ('b166b:left_neg04', 'IFERROR(LEN(LEFT("abc", -0.4)), "err")', '0'),
    ('b166b:left_neg06', 'IFERROR(LEN(LEFT("abc", -0.6)), "err")', 'err'),
    ('b166b:right_25', 'RIGHT("abcd", 2.5)', 'bcd'),
    ('b166b:mid_25', 'MID("abcdef", 2.5, 1)', 'c'),
    ('b166b:mid_n25', 'MID("abcdef", 1, 2.5)', 'abc'),
    ('b166b:mid_start15', 'MID("abcdef", 1.5, 1)', 'b'),
    ('b166b:mid_start04', 'IFERROR(MID("abcdef", 0.4, 1), "err")', 'err'),
    ('b166b:rept_25', 'REPT("x", 2.5)', 'xxx'),
    ('b166b:rept_35', 'REPT("x", 3.5)', 'xxxx'),
    ('b166b:rept_neg', 'IFERROR(REPT("x", -1), "err")', 'err'),
    ('b166b:unichar_frac', 'UNICODE(UNICHAR(65.7))', '66'),
    ('b174:left_blank', 'LEN(LEFT("abc", BLANK()))', '0'),
    ('b174:right_blank', 'LEN(RIGHT("abc", BLANK()))', '0'),
    ('b174:mid_blank_start', 'IFERROR(MID("abc", BLANK(), 1), "err")', 'err'),
    ('b174:mid_blank_n', 'LEN(MID("abc", 1, BLANK()))', '0'),
    ('b174:mid_neg_n', 'IFERROR(MID("abc", 1, -1), "err")', 'err'),
    ('b174:rept_blank', 'LEN(REPT("x", BLANK()))', '0'),
    ('b174:left_text_n', 'LEFT("abc", "2")', 'ab'),
    ('b174:left_bool_n', 'LEFT("abc", TRUE())', 'a'),
    ('b174:left_bad_text_n', 'IFERROR(LEFT("abc", "x"), "err")', 'err'),
    ('b174:left_number', 'LEFT(12345, 2)', '12'),
    ('b174:replace_start_25', 'REPLACE("abcdef", 2.5, 1, "X")', 'abXdef'),
    ('b174:replace_n_15', 'REPLACE("abcdef", 2, 1.5, "X")', 'aXdef'),
    ('b174:replace_start0', 'IFERROR(REPLACE("abc", 0, 1, "X"), "err")', 'err'),
    ('b174:replace_neg_n', 'IFERROR(REPLACE("abc", 1, -1, "X"), "err")', 'err'),
    ('b174:replace_past', 'REPLACE("abc", 5, 1, "X")', 'abcX'),
    ('b174:replace_blank_n', 'REPLACE("abc", 1, BLANK(), "X")', 'Xabc'),
    ('b174:replace_blank_start', 'IFERROR(REPLACE("abc", BLANK(), 1, "X"), "err")', 'err'),
    ('b174:subst_inst0', 'IFERROR(SUBSTITUTE("aaa", "a", "b", 0), "err")', 'err'),
    ('b174:subst_inst_neg', 'IFERROR(SUBSTITUTE("aaa", "a", "b", -1), "err")', 'err'),
    ('b174:subst_inst_15', 'SUBSTITUTE("aaa", "a", "b", 1.5)', 'aba'),
    ('b174:subst_inst_14', 'SUBSTITUTE("aaa", "a", "b", 1.4)', 'baa'),
    ('b174:subst_inst_blank', 'IFERROR(SUBSTITUTE("aaa", "a", "b", BLANK()), "err")', 'err'),
    ('b174:subst_overlap', 'SUBSTITUTE("aaaa", "aa", "b")', 'bb'),
    ('b174:subst_overlap_inst', 'SUBSTITUTE("aaa", "aa", "b", 2)', 'ab'),
    ('b174:subst_case_inst', 'SUBSTITUTE("aAa", "a", "b", 2)', 'aAb'),
    ('b174:s_start0', 'IFERROR(SEARCH("a", "abc", 0), "err")', 'err'),
    ('b174:s_start0_nf', 'IFERROR(SEARCH("a", "abc", 0, -1), "err")', 'err'),
    ('b174:s_start_15', 'SEARCH("a", "abca", 1.5)', '4'),
    ('b174:s_start_14', 'SEARCH("a", "abca", 1.4)', '1'),
    ('b174:f_start0', 'IFERROR(FIND("a", "abc", 0), "err")', 'err'),
    ('b174:f_start_15', 'FIND("a", "abca", 1.5)', '4'),
    ('b174:unichar_half', 'UNICODE(UNICHAR(65.5))', '66'),
    ('b174:unichar_text', 'IFERROR(UNICHAR("65"), "err")', 'A'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))
