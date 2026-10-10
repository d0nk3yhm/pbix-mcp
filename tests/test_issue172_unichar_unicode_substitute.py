"""Issue #172: UNICHAR, UNICODE, SUBSTITUTE and COMBINEVALUES at their edges.

- UNICHAR is an error for 0 and the code points XML 1.0 forbids (the C0
  controls but tab, line feed and carriage return, the surrogates, U+FFFE and
  U+FFFF: "The function UNICHAR does not return invalid XML characters") and
  for the noncharacters U+FDD0 to U+FDEF; the engine returned a character
  for every code point.
- UNICODE("") and UNICODE(BLANK()) are BLANK; the engine gave 0.
- SUBSTITUTE with an empty old text returns the text unchanged; the engine put
  the new text between every character.
- COMBINEVALUES takes two values or more; COMBINEVALUES(",", BLANK()) is
  refused, where the engine gave ",".

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b166.py, build_b166b.py, build_b174.py, build_b174b.py), and build_b170.py
over every BMP code point."""
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
    ('b166:unichar_a', 'UNICHAR(65)', 'A'),
    ('b166:unicode_empty', 'IFERROR(UNICODE(""), "err")', None),
    ('b166:subst_all', 'SUBSTITUTE("aAa", "a", "b")', 'bAb'),
    ('b166:subst_empty', 'SUBSTITUTE("abc", "", "x")', 'abc'),
    ('b166:combine_blank', 'COMBINEVALUES(",", "a", BLANK(), "c")', 'a,,c'),
    ('b166b:unicode_blank', 'IFERROR(UNICODE(BLANK()), "err")', None),
    ('b166b:unichar_0', 'IFERROR(UNICHAR(0), "err")', 'err'),
    ('b166b:subst_inst3', 'SUBSTITUTE("abab", "ab", "x", 3)', 'abab'),
    ('b174:subst_empty_inst', 'SUBSTITUTE("abc", "", "x", 1)', 'abc'),
    ('b174:unichar_past', 'IFERROR(UNICODE(UNICHAR(1114112)), -1)', '-1'),
    ('b174:unichar_neg', 'IFERROR(UNICODE(UNICHAR(-1)), -1)', '-1'),
    ('b174:unicode_number', 'UNICODE(5)', '53'),
    ('b174:unicode_bool', 'IFERROR(UNICODE(TRUE()), -1)', '84'),
    ('b174b:b_combine_one', 'IF(ISBLANK(COMBINEVALUES(",", BLANK())), 1, 0)', 'ERROR'),
    ('b174b:b_unicode_blank', 'IF(ISBLANK(UNICODE(BLANK())), 1, 0)', '1'),
    ('b174c:cv_two', 'IF(ISBLANK(COMBINEVALUES(",", BLANK(), BLANK())), 1, 0)', '0'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))


# build_b170.py: the BMP code points whose UNICHAR is an error (with 0:
# IFERROR(UNICHAR(0), "err") is "err" in build_b166b.py)
REFUSED = [(1, 8), (11, 12), (14, 31), (55296, 57343), (64976, 65007), (65534, 65535)]


def _refused(cp):
    return cp == 0 or any(a <= cp <= b for a, b in REFUSED)


EDGES = sorted({c for a, b in REFUSED for c in (a - 1, a, b, b + 1)} | {0, 9, 10, 13, 32, 0xFFFD})


@pytest.mark.parametrize("cp", EDGES)
def test_unichar_refuses_what_desktop_refuses(cp):
    got = _engine(f"IFERROR(UNICODE(UNICHAR({cp})), -1)")
    assert got == ("-1" if _refused(cp) else str(cp))


def test_every_bmp_code_point():
    assert [c for c in range(0x10000) if de._unichar(c) is None] == [c for c in range(0x10000) if _refused(c)]
