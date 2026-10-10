"""Issue #167: TRIM removes the space (U+0020) only, from both ends, and
collapses each run of spaces between words to one.

The engine stripped every kind of whitespace from the ends (str.strip()) and
left the spaces between words: TRIM("  a   b  ") was "a   b" where Desktop
gives "a b", and a tab or a no-break space at the ends was taken off where
Desktop keeps it.

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
    ('b166:trim_inner', 'LEN(TRIM("  a   b  "))', '3'),
    ('b166:trim_text', '"[" & TRIM("  a   b  ") & "]"', '[a b]'),
    ('b166:trim_tab', 'LEN(TRIM(UNICHAR(9) & "a" & UNICHAR(9)))', '3'),
    ('b166:trim_nbsp', 'LEN(TRIM(UNICHAR(160) & "a" & UNICHAR(160)))', '3'),
    ('b166:trim_ideo', 'LEN(TRIM(UNICHAR(12288) & "a"))', '2'),
    ('b166b:trim_inner_tab', 'LEN(TRIM("a" & UNICHAR(9) & UNICHAR(9) & "b"))', '4'),
    ('b166b:trim_mixed', 'LEN(TRIM(" a " & UNICHAR(9) & " b "))', '5'),
    ('b166b:trim_only', 'LEN(TRIM("     "))', '0'),
    ('b174:trim_newline', 'LEN(TRIM("a" & UNICHAR(10) & "  b"))', '4'),
    ('b174:trim_crlf_ends', 'LEN(TRIM(UNICHAR(13) & UNICHAR(10) & " a "))', '4'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))
