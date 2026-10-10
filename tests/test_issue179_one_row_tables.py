"""Issue #179: a one-row, one-column table is its value wherever a value is
needed, whatever made it, and a table with no rows is BLANK.

FILTER / TOPN / SELECTCOLUMNS over a table give rows that carry the table's
columns rather than one value, and the engine read only the latter as a
value: FILTER(T2, T2[c] = "x") & "!" was the rows' Python text and "!",
LEN of it 48, FILTER(...) = "x" FALSE, FILTER(N2, N2[n] = 5) + 1 BLANK, a
measure defined as such a FILTER returned the rows, and ISBLANK of an empty
FILTER was FALSE. A table of several rows used as a value is an error that
IFERROR catches; one of several columns is refused outright ("The expression
refers to multiple columns. Multiple columns cannot be converted to a scalar
value"), which IFERROR does not catch.

Expected values: Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per probe
(the Desktop oracle toolkit's build_b176.py)."""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

MODEL = {
    "T": {"columns": ["c"], "rows": [["x"]]},
    "T2": {"columns": ["c"], "rows": [["x"], ["y"]]},
    "U": {"columns": ["c", "d"], "rows": [["x", 1]]},
    "N2": {"columns": ["n"], "rows": [[5], [7]]},
}
MEASURES = {"t_measure": 'FILTER(T2, T2[c] = "x")'}


def _engine(dax):
    """The engine's answer as ADOMD prints it; "ERROR" for an error."""
    eng = de.DAXEngine()
    try:
        # every probe is a measure in Desktop: its result is a value
        got = de._scalarize(eng._eval_expr(dax, de.DAXContext(MODEL, dict(MEASURES))))
    except Exception:
        return "ERROR"
    if got is None:
        return None
    if isinstance(got, bool):
        return "True" if got else "False"
    if isinstance(got, float) and got.is_integer():
        return str(int(got))
    return str(got)


DESKTOP = [
    ('t_filter_len', 'LEN(FILTER(T2, T2[c] = "x"))', '1'),
    ('t_filter_upper', 'UPPER(FILTER(T2, T2[c] = "x"))', 'X'),
    ('t_topn_upper', 'UPPER(TOPN(1, T2, T2[c]))', 'Y'),
    ('t_two_cols', 'IFERROR(UPPER(FILTER(U, TRUE())), "err")', 'ERROR'),
    ('t_rowctor', 'UPPER({"q"})', 'Q'),
    ('t_rowctor_two', 'IFERROR(UPPER({("q", 1)}), "err")', 'ERROR'),
    ('t_selectcols', 'UPPER(SELECTCOLUMNS(FILTER(T2, T2[c] = "y"), "z", T2[c]))', 'Y'),
    ('t_amp', 'FILTER(T2, T2[c] = "x") & "!"', 'x!'),
    ('t_concat', 'CONCATENATE(FILTER(T2, T2[c] = "x"), "!")', 'x!'),
    ('t_eq', 'IF(FILTER(T2, T2[c] = "x") = "x", 1, 0)', '1'),
    ('t_arith', 'FILTER(N2, N2[n] = 5) + 1', '6'),
    ('t_arith_values', 'CALCULATE(VALUES(N2[n]), N2[n] = 7) * 2', '14'),
    ('t_isblank', 'IF(ISBLANK(FILTER(T2, T2[c] = "x")), 1, 0)', '0'),
    ('t_isblank_empty', 'IF(ISBLANK(FILTER(T2, FALSE())), 1, 0)', '1'),
    ('t_coalesce', 'COALESCE(FILTER(T2, FALSE()), "d")', 'd'),
    ('t_coalesce_one', 'COALESCE(FILTER(T2, T2[c] = "y"), "d")', 'y'),
    ('t_format', 'FORMAT(FILTER(N2, N2[n] = 5), "0.0")', '5.0'),
    ('t_measure', 'FILTER(T2, T2[c] = "x")', 'x'),
    ('t_measure_ref', '[t_measure] & "?"', 'x?'),
    ('t_two_rows_amp', 'IFERROR(T2 & "", "err")', 'err'),
    ('t_two_rows_len', 'IFERROR(LEN(T2), "err")', 'err'),
    ('t_crossjoin_one', 'IFERROR(UPPER(CROSSJOIN(FILTER(T2, T2[c] = "x"), FILTER(N2, N2[n] = 5))), "err")', 'ERROR'),
    ('t_max_of', 'MAXX(T2, UPPER(FILTER(T2, T2[c] = "y")))', 'Y'),
    ('t_search_in', 'SEARCH("x", FILTER(T2, T2[c] = "x"))', '1'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _engine(dax) == want


def test_a_measure_that_is_a_one_row_table_is_its_value():
    got = de.evaluate_measures_batch(["t_measure"], MODEL, dict(MEASURES))
    assert got["t_measure"] == "x"
