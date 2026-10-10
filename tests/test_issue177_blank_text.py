"""Issue #177: a BLANK text gives BLANK in LEFT, RIGHT, MID, LEN, UPPER, LOWER,
TRIM, SUBSTITUTE, REPT, VALUE, UNICODE and UNICHAR(BLANK()), and CONCATENATE
of two BLANKs and CONCATENATEX over no rows are BLANK; an empty table is BLANK
and a table of several rows an error.

The engine read BLANK as "" in every text function (LEN(BLANK()) was 0,
ISBLANK(UPPER(BLANK())) FALSE), and a table of two rows as its internal
Python text (LEN 114). REPLACE, EXACT, COMBINEVALUES, SEARCH and FIND read
BLANK as "" in Desktop too. LEFT and RIGHT read their count first, so
LEFT(BLANK(), -1) fails, where MID(BLANK(), 0, 1) is BLANK.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b174.py, build_b174b.py, build_b174c.py, build_b175.py)."""
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
    ('b174:blank_upper', 'IF(ISBLANK(UPPER(BLANK())), 1, 0)', '1'),
    ('b174:blank_lower', 'IF(ISBLANK(LOWER(BLANK())), 1, 0)', '1'),
    ('b174:blank_trim', 'IF(ISBLANK(TRIM(BLANK())), 1, 0)', '1'),
    ('b174:blank_left', 'IF(ISBLANK(LEFT(BLANK(), 1)), 1, 0)', '1'),
    ('b174:blank_right', 'IF(ISBLANK(RIGHT(BLANK(), 1)), 1, 0)', '1'),
    ('b174:blank_mid', 'IF(ISBLANK(MID(BLANK(), 1, 1)), 1, 0)', '1'),
    ('b174:blank_subst', 'IF(ISBLANK(SUBSTITUTE(BLANK(), "a", "b")), 1, 0)', '1'),
    ('b174:blank_replace', 'IF(ISBLANK(REPLACE(BLANK(), 1, 1, "x")), 1, 0)', '0'),
    ('b174:blank_rept', 'IF(ISBLANK(REPT(BLANK(), 2)), 1, 0)', '1'),
    ('b174:blank_concat', 'IF(ISBLANK(CONCATENATE(BLANK(), BLANK())), 1, 0)', '1'),
    ('b174:blank_amp', 'IF(ISBLANK(BLANK() & BLANK()), 1, 0)', '1'),
    ('b174:blank_len', 'LEN(BLANK())', None),
    ('b174:blank_exact', 'IF(EXACT(BLANK(), ""), 1, 0)', '1'),
    ('b174:blank_left0', 'IF(ISBLANK(LEFT("abc", 0)), 1, 0)', '0'),
    ('b174:blank_combine', 'IF(ISBLANK(COMBINEVALUES(",", BLANK(), BLANK())), 1, 0)', '0'),
    ('b174:v_blank_arg', 'IFERROR(VALUE(BLANK()), "err")', None),
    ('b174:v_blank_isblank', 'IF(ISBLANK(IFERROR(VALUE(BLANK()), 99)), 1, 0)', '1'),
    ('b174:unichar_blank_len', 'IFERROR(LEN(UNICHAR(BLANK())), -1)', None),
    ('b174:unichar_blank_isblank', 'IFERROR(IF(ISBLANK(UNICHAR(BLANK())), 1, 0), -1)', '1'),
    ('b174b:b_subst_old', 'SUBSTITUTE("abc", BLANK(), "x")', 'abc'),
    ('b174b:b_subst_new', 'SUBSTITUTE("abc", "b", BLANK())', 'ac'),
    ('b174b:b_replace_new', 'REPLACE("abc", 1, 1, BLANK())', 'bc'),
    ('b174b:b_left0', 'IF(ISBLANK(LEFT(BLANK(), 0)), 1, 0)', '1'),
    ('b174b:b_mid_past', 'IF(ISBLANK(MID(BLANK(), 5, 1)), 1, 0)', '1'),
    ('b174b:b_upper_empty', 'IF(ISBLANK(UPPER("")), 1, 0)', '0'),
    ('b174b:b_concat_a', 'CONCATENATE(BLANK(), "a")', 'a'),
    ('b174b:b_concat_empty', 'IF(ISBLANK(CONCATENATE("", BLANK())), 1, 0)', '0'),
    ('b174b:b_concat_empty2', 'IF(ISBLANK(CONCATENATE(BLANK(), "")), 1, 0)', '0'),
    ('b174b:b_trim_empty', 'IF(ISBLANK(TRIM("")), 1, 0)', '0'),
    ('b174b:b_rept_empty', 'IF(ISBLANK(REPT("", 2)), 1, 0)', '0'),
    ('b174b:b_rept0', 'IF(ISBLANK(REPT(BLANK(), 0)), 1, 0)', '1'),
    ('b174b:b_rept_text0', 'IF(ISBLANK(REPT("x", 0)), 1, 0)', '0'),
    ('b174b:b_exact_blanks', 'IF(EXACT(BLANK(), BLANK()), 1, 0)', '1'),
    ('b174b:b_contains', 'IF(CONTAINSSTRING(BLANK(), "a"), 1, 0)', '0'),
    ('b174b:b_search_nf', 'IFERROR(SEARCH("a", BLANK(), 1, -1), "err")', '-1'),
    ('b174b:b_search_blank_find', 'IFERROR(SEARCH(BLANK(), "abc"), "err")', '1'),
    ('b174b:b_find_blank_find', 'IFERROR(FIND(BLANK(), "abc"), "err")', '1'),
    ('b174b:b_len_empty', 'IF(ISBLANK(LEN("")), 1, 0)', '0'),
    ('b174b:b_len_concat', 'IF(ISBLANK(LEN(BLANK() & "")), 1, 0)', '0'),
    ('b174b:b_lower_number', 'LOWER(BLANK() + 1)', '1'),
    ('b174b:b_left_n_blank_text', 'IF(ISBLANK(LEFT(BLANK())), 1, 0)', '1'),
    ('b174b:b_right_blank_n', 'IF(ISBLANK(RIGHT("abc", BLANK())), 1, 0)', '0'),
    ('b174b:b_subst_inst_blank_text', 'IF(ISBLANK(SUBSTITUTE(BLANK(), "a", "b", 1)), 1, 0)', '1'),
    ('b174b:b_format', 'IF(ISBLANK(FORMAT(BLANK(), "0")), 1, 0)', '0'),
    ('b174b:b_fixed', 'IF(ISBLANK(FIXED(BLANK())), 1, 0)', '0'),
    ('b174b:b_concatx', 'IF(ISBLANK(CONCATENATEX(FILTER(T, FALSE()), T[c])), 1, 0)', '1'),
    ('b174c:cx_empty_len', 'IFERROR(LEN(CONCATENATEX(FILTER(T, FALSE()), T[c])), -1)', None),
    ('b174c:cx_blank_rows', 'IF(ISBLANK(CONCATENATEX(T, BLANK())), 1, 0)', '0'),
    ('b174c:cx_empty_rows', 'IF(ISBLANK(CONCATENATEX(T, "")), 1, 0)', '0'),
    ('b175:bl_left_neg', 'IFERROR(IF(ISBLANK(LEFT(BLANK(), -1)), "blank", "text"), "err")', 'err'),
    ('b175:bl_right_neg', 'IFERROR(IF(ISBLANK(RIGHT(BLANK(), -1)), "blank", "text"), "err")', 'err'),
    ('b175:bl_mid_start0', 'IFERROR(IF(ISBLANK(MID(BLANK(), 0, 1)), "blank", "text"), "err")', 'blank'),
    ('b175:bl_mid_neg', 'IFERROR(IF(ISBLANK(MID(BLANK(), 1, -1)), "blank", "text"), "err")', 'blank'),
    ('b175:bl_rept_neg', 'IFERROR(IF(ISBLANK(REPT(BLANK(), -1)), "blank", "text"), "err")', 'blank'),
    ('b175:bl_subst_inst0', 'IFERROR(IF(ISBLANK(SUBSTITUTE(BLANK(), "a", "b", 0)), "blank", "text"), "err")', 'blank'),
    ('b175:bl_left_text_n', 'IFERROR(IF(ISBLANK(LEFT(BLANK(), "x")), "blank", "text"), "err")', 'err'),
    ('b175:bl_replace_start0', 'IFERROR(REPLACE(BLANK(), 0, 1, "x"), "err")', 'err'),
    ('b175:bl_unichar_text', 'IFERROR(IF(ISBLANK(UNICHAR(BLANK())), "blank", "text"), "err")', 'blank'),
    ('b175:tb_len_empty', 'IFERROR(IF(ISBLANK(LEN(FILTER(VALUES(T[c]), FALSE()))), "blank", "text"), "err")', 'blank'),
    ('b175:tb_upper_empty', 'IFERROR(IF(ISBLANK(UPPER(FILTER(VALUES(T[c]), FALSE()))), "blank", "text"), "err")', 'blank'),
    ('b175:tb_len_two', 'IFERROR(LEN(VALUES(N[k])), "err")', 'err'),
    ('b175:tb_upper_two', 'IFERROR(UPPER(VALUES(N[k])), "err")', 'err'),
    ('b175:tb_exact_empty', 'IFERROR(IF(EXACT(FILTER(VALUES(T[c]), FALSE()), ""), 1, 0), "err")', '1'),
    ('b175:tb_concat_empty', 'IFERROR(IF(ISBLANK(FILTER(VALUES(T[c]), FALSE()) & ""), "blank", "text"), "err")', 'text'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))
