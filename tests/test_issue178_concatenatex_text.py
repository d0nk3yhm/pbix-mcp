"""Issue #178: CONCATENATEX renders each value -- and the delimiter -- as `&`
does.

The engine used Python's str(): CONCATENATEX of a Double column gave "1.0" for
1, of a DateTime column "2024-01-02 00:00:00" for "1/2/2024", of a Boolean
column "True" for "TRUE", and a delimiter of 0 was dropped (`str(x or '')`).

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe (the Desktop oracle toolkit's build_b175.py)."""
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
    ('b175:cx_doubles', 'CONCATENATEX(N, N[d], "|", N[k])', '1|2.5'),
    ('b175:cx_dates', 'CONCATENATEX(N, N[dt], "|", N[k])', '1/2/2024|3/4/2024 3:30:00 PM'),
    ('b175:cx_bools', 'CONCATENATEX(N, N[b], "|", N[k])', 'TRUE|FALSE'),
    ('b175:cx_ints', 'CONCATENATEX(N, N[i], "|", N[k])', '1|2'),
    ('b175:cx_expr_float', 'CONCATENATEX(N, N[i] / 2, "|", N[k])', '0.5|1'),
    ('b175:cx_expr_bool', 'CONCATENATEX(N, N[i] > 1, "|", N[k])', 'FALSE|TRUE'),
    ('b175:cx_delim_number', 'CONCATENATEX(N, N[k], 0, N[k])', 'a0b'),
    ('b175:cx_blank_values', 'CONCATENATEX(N, BLANK(), "|")', '|'),
    ('b175:cx_one_blank_row_len', 'LEN(CONCATENATEX(FILTER(N, N[k] = "a"), BLANK(), "|"))', '0'),
    ('b175:amp_double', 'MAX(N[d]) & ""', '2.5'),
    ('b175:amp_date', 'MAX(N[dt]) & ""', '3/4/2024 3:30:00 PM'),
    ('b175:combine_double', 'COMBINEVALUES("|", MAX(N[d]), MAX(N[dt]))', '2.5|3/4/2024 3:30:00 PM'),
    ('b175:combine_nums', 'COMBINEVALUES(",", 1, 2.5)', '1,2.5'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))
