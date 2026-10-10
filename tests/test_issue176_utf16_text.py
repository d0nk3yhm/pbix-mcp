"""Issue #176: Desktop's text is UTF-16, and its text functions count code
units: a character beyond the BMP is 2.

LEN of an emoji is 2; LEFT / RIGHT / MID / REPLACE / SEARCH / FIND /
SUBSTITUTE positions and the ? wildcard count units, so LEFT of an emoji is
its high surrogate, and joining the two halves back (`&`, CONCATENATE) gives
the emoji again. UNICODE of a lone low surrogate is the unit itself, of a
high surrogate with no low one after it an error. UNICHAR beyond plane 1
keeps the code point's low 16 bits (UNICHAR(0x20000) is U+10000) and
refuses the last two code points of every plane. The engine counted Python
characters (code points).

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b174.py, build_b174b.py, build_b174c.py)."""
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
    ('b174:len_supp', 'LEN(UNICHAR(128512))', '2'),
    ('b174:unicode_supp', 'UNICODE(UNICHAR(128512))', '128512'),
    ('b174:left_supp_len', 'LEN(LEFT(UNICHAR(128512) & "a", 1))', '1'),
    ('b174:left_supp_code', 'UNICODE(LEFT(UNICHAR(128512) & "a", 1))', 'ERROR'),
    ('b174:mid_supp_code', 'IFERROR(UNICODE(MID("a" & UNICHAR(128512), 3, 1)), -1)', '56832'),
    ('b174:right_supp_code', 'IFERROR(UNICODE(RIGHT("a" & UNICHAR(128512), 1)), -1)', '56832'),
    ('b174:search_supp', 'SEARCH("a", UNICHAR(128512) & "a")', '3'),
    ('b174:find_supp', 'FIND("a", UNICHAR(128512) & "a")', '3'),
    ('b174:rept_supp', 'LEN(REPT(UNICHAR(128512), 2))', '4'),
    ('b174:trim_supp', 'LEN(TRIM(" " & UNICHAR(128512) & " "))', '2'),
    ('b174:s_q_supp', 'IFERROR(SEARCH("a?b", "a" & UNICHAR(128512) & "b"), "err")', 'err'),
    ('b174:s_qq_supp', 'IFERROR(SEARCH("a??b", "a" & UNICHAR(128512) & "b"), "err")', '1'),
    ('b174:unichar_max', 'IFERROR(UNICODE(UNICHAR(1114111)), -1)', '-1'),
    ('b174b:uc_plane2_eq', 'IF(EXACT(UNICHAR(131072), UNICHAR(65536)), 1, 0)', '1'),
    ('b174b:uc_plane2_low', 'UNICODE(MID(UNICHAR(131072), 2, 1))', '56320'),
    ('b174b:uc_plane2_len', 'LEN(UNICHAR(131072))', '2'),
    ('b174b:uc_plane16', 'IFERROR(UNICODE(UNICHAR(1048576)), -1)', '65536'),
    ('b174b:uc_plane16_eq', 'IF(EXACT(UNICHAR(1048576), UNICHAR(65536)), 1, 0)', '1'),
    ('b174b:uc_plane1_max', 'IFERROR(UNICODE(UNICHAR(131069)), -1)', '131069'),
    ('b174b:uc_e0001', 'IFERROR(UNICODE(UNICHAR(917505)), -1)', '65537'),
    ('b174b:s_pair_rejoin', 'IF(EXACT(LEFT(UNICHAR(128512), 1) & RIGHT(UNICHAR(128512), 1), UNICHAR(128512)), 1, 0)', '1'),
    ('b174b:s_pair_unicode', 'UNICODE(LEFT(UNICHAR(128512), 1) & RIGHT(UNICHAR(128512), 1))', '128512'),
    ('b174b:s_high_then_a', 'IFERROR(UNICODE(LEFT(UNICHAR(128512), 1) & "a"), -1)', '-1'),
    ('b174b:s_low_alone_len', 'LEN(RIGHT(UNICHAR(128512), 1))', '1'),
    ('b174b:s_high_len', 'LEN(LEFT(UNICHAR(128512), 1))', '1'),
    ('b174b:s_upper_lone', 'UNICODE(UPPER(RIGHT(UNICHAR(128512), 1)))', '56832'),
    ('b174b:s_q_emoji', 'IFERROR(SEARCH("?", UNICHAR(128512)), "err")', '1'),
    ('b174b:s_qq_emoji', 'IFERROR(SEARCH("??", UNICHAR(128512)), "err")', '1'),
    ('b174b:s_find_low', 'IFERROR(FIND(RIGHT(UNICHAR(128512), 1), "a" & UNICHAR(128512)), "err")', '3'),
    ('b174b:s_search_low', 'IFERROR(SEARCH(RIGHT(UNICHAR(128512), 1), "a" & UNICHAR(128512)), "err")', '3'),
    ('b174b:s_subst_half', 'LEN(SUBSTITUTE(UNICHAR(128512), RIGHT(UNICHAR(128512), 1), "x"))', '2'),
    ('b174b:s_replace_half', 'LEN(REPLACE(UNICHAR(128512) & "a", 2, 1, "x"))', '3'),
    ('b174b:s_mid_start2', 'UNICODE(MID(UNICHAR(128512) & "a", 3, 1))', '97'),
    ('b174b:s_trim_lone', 'LEN(TRIM(" " & LEFT(UNICHAR(128512), 1) & " "))', '1'),
    ('b174b:s_lower_emoji', 'UNICODE(LOWER(UNICHAR(128512)))', '128512'),
    ('b174b:s_concat_len', 'LEN(CONCATENATE(UNICHAR(128512), UNICHAR(128512)))', '4'),
    ('b174b:s_exact_halves', 'IF(EXACT(LEFT(UNICHAR(128512), 1), LEFT(UNICHAR(128513), 1)), 1, 0)', '1'),
    ('b174c:u_high_end', 'IFERROR(UNICODE(LEFT(UNICHAR(128512), 1)), -1)', '-1'),
    ('b174c:u_high_high', 'IFERROR(UNICODE(LEFT(UNICHAR(128512), 1) & LEFT(UNICHAR(128512), 1)), -1)', '-1'),
    ('b174c:u_low_then', 'IFERROR(UNICODE(RIGHT(UNICHAR(128512), 1) & "a"), -1)', '56832'),
    ('b174c:u_len_pair_of_highs', 'LEN(LEFT(UNICHAR(128512), 1) & LEFT(UNICHAR(128512), 1))', '2'),
    ('b174c:u_upper_high', 'IFERROR(LEN(UPPER(LEFT(UNICHAR(128512), 1))), -1)', '1'),
    ('b174c:u_exact_lone', 'IF(EXACT(RIGHT(UNICHAR(128512), 1), RIGHT(UNICHAR(128513), 1)), 1, 0)', '0'),
    ('b174c:u_eq_lone', 'IF(RIGHT(UNICHAR(128512), 1) = RIGHT(UNICHAR(128513), 1), 1, 0)', '0'),
    ('b174c:u_search_high', 'IFERROR(SEARCH(LEFT(UNICHAR(128512), 1), "a" & UNICHAR(128512)), "err")', '2'),
    ('b174c:u_find_q_pair', 'IFERROR(SEARCH("a??b", "a" & UNICHAR(128512) & "b"), "err")', '1'),
    ('b174c:u_trim_emoji_inner', 'LEN(TRIM(UNICHAR(128512) & "  " & UNICHAR(128512)))', '5'),
    ('b174c:u_rept_lone', 'LEN(REPT(RIGHT(UNICHAR(128512), 1), 3))', '3'),
    ('b174c:u_subst_emoji', 'LEN(SUBSTITUTE("a" & UNICHAR(128512) & "b", UNICHAR(128512), "x"))', '3'),
    ('b174c:u_unichar_d800_pair', 'IFERROR(UNICODE(UNICHAR(55357) & UNICHAR(56832)), -1)', '-1'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))


# build_b174.py: UNICHAR over 87 code points of planes 1 - 16 -- {cp: (UNICODE of
# it, LEN of it)}, (-1, -1) where UNICHAR fails
SUPPLEMENTARY_UNICHAR = {65536: (65536, 2), 65537: (65537, 2), 119070: (119070, 2), 128512: (128512, 2), 131069: (131069, 2), 131070: (-1, -1), 131071: (-1, -1), 131072: (65536, 2), 131073: (65537, 2), 173791: (108255, 2), 196605: (131069, 2), 196606: (-1, -1), 196607: (-1, -1), 196608: (65536, 2), 196609: (65537, 2), 201546: (70474, 2), 262141: (131069, 2), 262142: (-1, -1), 262143: (-1, -1), 262144: (65536, 2), 262145: (65537, 2), 327677: (131069, 2), 327678: (-1, -1), 327679: (-1, -1), 327680: (65536, 2), 327681: (65537, 2), 393213: (131069, 2), 393214: (-1, -1), 393215: (-1, -1), 393216: (65536, 2), 393217: (65537, 2), 458749: (131069, 2), 458750: (-1, -1), 458751: (-1, -1), 458752: (65536, 2), 458753: (65537, 2), 524285: (131069, 2), 524286: (-1, -1), 524287: (-1, -1), 524288: (65536, 2), 524289: (65537, 2), 589821: (131069, 2), 589822: (-1, -1), 589823: (-1, -1), 589824: (65536, 2), 589825: (65537, 2), 655357: (131069, 2), 655358: (-1, -1), 655359: (-1, -1), 655360: (65536, 2), 655361: (65537, 2), 720893: (131069, 2), 720894: (-1, -1), 720895: (-1, -1), 720896: (65536, 2), 720897: (65537, 2), 786429: (131069, 2), 786430: (-1, -1), 786431: (-1, -1), 786432: (65536, 2), 786433: (65537, 2), 851965: (131069, 2), 851966: (-1, -1), 851967: (-1, -1), 851968: (65536, 2), 851969: (65537, 2), 917501: (131069, 2), 917502: (-1, -1), 917503: (-1, -1), 917504: (65536, 2), 917505: (65537, 2), 917631: (65663, 2), 917760: (65792, 2), 917999: (66031, 2), 983037: (131069, 2), 983038: (-1, -1), 983039: (-1, -1), 983040: (65536, 2), 983041: (65537, 2), 1048573: (131069, 2), 1048574: (-1, -1), 1048575: (-1, -1), 1048576: (65536, 2), 1048577: (65537, 2), 1114109: (131069, 2), 1114110: (-1, -1), 1114111: (-1, -1)}


@pytest.mark.parametrize("cp", sorted(SUPPLEMENTARY_UNICHAR))
def test_unichar_beyond_the_bmp(cp):
    code, length = SUPPLEMENTARY_UNICHAR[cp]
    assert _engine(f"IFERROR(UNICODE(UNICHAR({cp})), -1)") == str(code)
    assert _engine(f"IFERROR(LEN(UNICHAR({cp})), -1)") == str(length)
