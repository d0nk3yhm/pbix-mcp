"""Issue #139: DIVIDE and the math functions convert their arguments as
arithmetic does.

Numeric text is its number (in the model's culture, as arithmetic reads it,
#133), TRUE / FALSE are 1 / 0 and a date is its serial; text that is no number
is an error. The engine returned BLANK for all of these (CEILING / FLOOR
returned the text), so Awesome Chocolates' QOQ calculation item, DIVIDE(_CQ -
_PQ, _PQ) over CONVERT([Sales Actual], STRING), coloured a 33.5 % fall as a
rise. A text ALTERNATE result stays text, as in Desktop.

Expected values: Desktop over ADOMD (build_b139.py, an en-US model), one
EVALUATE ROW per probe. Generated from Desktop's output.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["Key", "Amount"], "rows": [[1, 2.5], [2, 2.5]]}}
MEASURES = {"Amt": "SUM(T[Amount])", "TextAmount": "CONVERT([Amt], STRING)"}
ERROR = "ERROR"

DESKTOP = [   # (probe, expression, Power BI Desktop 2.152) -- generated from build_b139.py's output
    ('div_num_text', 'DIVIDE(3, "3")', 1),
    ('div_text_num', 'DIVIDE("6", 3)', 2),
    ('div_text_text', 'DIVIDE("6", "3")', 2),
    ('div_text_zero', 'DIVIDE(3, "0")', None),
    ('div_text_zero_alt', 'DIVIDE(3, "0", 9)', 9),
    ('div_alt_text', 'DIVIDE(3, 0, "7")', '7'),
    ('div_alt_text_plus', 'DIVIDE(3, 0, "7") + 1', 8),
    ('div_nonnum', 'DIVIDE("abc", 3)', ERROR),
    ('div_nonnum_den', 'DIVIDE(3, "abc")', ERROR),
    ('div_empty', 'DIVIDE("", 3)', ERROR),
    ('div_empty_den', 'DIVIDE(3, "")', ERROR),
    ('div_sub', 'DIVIDE("6" - "3", "3")', 1),
    ('div_lt', 'IF(DIVIDE("5361207.75" - "8063111.25", "8063111.25") < 0, 1, 0)', 1),
    ('div_conv', 'DIVIDE(CONVERT(5, STRING), 2)', 2.5),
    ('div_conv_measure', 'DIVIDE([TextAmount], 2)', 2.5),
    ('div_bool', 'DIVIDE(TRUE(), 2)', 0.5),
    ('div_date', 'DIVIDE(DATE(2024, 1, 2), 1)', 45293),
    ('div_blank_text', 'DIVIDE(BLANK(), "2")', None),
    ('div_spaces', 'DIVIDE(" 6 ", 3)', 2),
    ('div_exp', 'DIVIDE("1E3", 10)', 100),
    ('abs_text', 'ABS("-3")', 3),
    ('round_text', 'ROUND("2.567", 2)', 2.57),
    ('round_digits_text', 'ROUND(2.567, "2")', 2.57),
    ('int_text', 'INT("3.7")', 3),
    ('trunc_text', 'TRUNC("3.7")', 3),
    ('mod_text', 'MOD("7", 3)', 1),
    ('power_text', 'POWER("2", 3)', 8),
    ('sqrt_text', 'SQRT("16")', 4),
    ('sign_text', 'SIGN("-2")', -1),
    ('ceiling_text', 'CEILING("2.1", 1)', 3),
    ('floor_text', 'FLOOR("2.9", 1)', 2),
    ('roundup_text', 'ROUNDUP("2.1", 0)', 3),
    ('rounddown_text', 'ROUNDDOWN("2.9", 0)', 2),
    ('exp_text', 'EXP("0")', 1),
    ('ln_text', 'LN("1")', 0),
    ('log10_text', 'LOG10("100")', 2),
    ('quotient_text', 'QUOTIENT("7", "2")', 3),
    ('mround_text', 'MROUND("7", "2")', 8),
    ('abs_nonnum', 'ABS("abc")', ERROR),
    ('round_nonnum', 'ROUND("abc", 0)', ERROR),
    ('ceiling_sig_text', 'CEILING(2.1, "1")', 3),
    ('floor_sig_text', 'FLOOR(2.9, "1")', 2),
    ('log_text', 'LOG("100", "10")', 2),
    ('even_text', 'EVEN("3")', 4),
    ('odd_text', 'ODD("2")', 3),
    ('fact_text', 'FACT("4")', 24),
    ('gcd_text', 'GCD("12", 8)', 4),
    ('lcm_text', 'LCM("4", "6")', 12),
    ('trunc_digits_text', 'TRUNC(3.789, "1")', 3.7),
    ('power_exp_text', 'POWER(2, "3")', 8),
    ('mod_div_text', 'MOD(7, "3")', 1),
    ('sin_text', 'SIN("0")', 0),
    ('cos_text', 'COS("0")', 1),
    ('degrees_text', 'DEGREES("0")', 0),
    ('combin_text', 'COMBIN("4", 2)', 6),
    ('permut_text', 'PERMUT("4", "2")', 12),
    ('bitand_text', 'BITAND("6", 3)', 2),
    ('iseven_text', 'IF(ISEVEN("4"), 1, 0)', 1),
    ('norm_text', 'NORM.S.DIST("0", TRUE())', 0.5),
    ('sin_nonnum', 'SIN("x")', ERROR),
    ('combin_nonnum', 'COMBIN("x", 1)', ERROR),
    ('log_nonnum', 'LOG("x")', ERROR),
    ('abs_bool', 'ABS(TRUE())', 1),
    ('int_bool', 'INT(TRUE())', 1),
    ('round_bool', 'ROUND(TRUE(), 0)', 1),
    ('sqrt_bool', 'SQRT(TRUE())', 1),
    ('sin_bool', 'SIN(FALSE())', 0),
    ('exp_bool', 'EXP(FALSE())', 1),
    ('mod_bool', 'MOD(TRUE(), 2)', 1),
    ('abs_date', 'ABS(DATE(2024, 1, 2))', 45293),
    ('int_date', 'INT(DATE(2024, 1, 2) + 0.75)', 45293),
    ('round_date', 'ROUND(DATE(2024, 1, 2), 0)', 45293),
    ('sin_date', 'SIN(DATE(1899, 12, 30))', 0),
    ('div_group', 'DIVIDE("3,5", 1)', 35),
    ('abs_currency', 'ABS("$3")', 3),
    ('div_dmy', 'DIVIDE("01/02/2024", 1)', 45293),
    ('alt_istext', 'IF(ISTEXT(DIVIDE(3, 0, "7")), 1, 0)', 1),
    ('alt_isnumber', 'IF(ISNUMBER(DIVIDE(3, 0, "7")), 1, 0)', 0),
    ('alt_concat', 'DIVIDE(3, 0, "07") & ""', '07'),
    ('div_isnumber', 'IF(ISNUMBER(DIVIDE("6", 3)), 1, 0)', 1),
    ('abs_isnumber', 'IF(ISNUMBER(ABS("-3")), 1, 0)', 1),
    ('ceiling_isnumber', 'IF(ISNUMBER(CEILING("2.1", 1)), 1, 0)', 1),
]


@pytest.mark.parametrize("probe,expr,want", DESKTOP, ids=[p for p, _e, _w in DESKTOP])
def test_matches_desktop(probe, expr, want):
    m = dict(MEASURES)
    m["p"] = expr
    de._engine.eval_errors.clear()
    got = de.evaluate_measures_smart(["p"], TABLES, m, {}, simulate_row_context=False)["p"]
    if want == ERROR:
        assert got is None
        assert "Cannot convert value" in de._engine.eval_errors.get("p", "")
    elif isinstance(want, str):
        assert got == want
    elif want is None:
        assert got is None
    else:
        assert not isinstance(got, str)
        assert got == pytest.approx(want)


def test_a_culture_reads_its_own_separators():
    # The model's culture decides the separators, as for "3,5" + 0 (#133).
    got = de.evaluate_measures_smart(["p"], TABLES, {"p": 'DIVIDE("3,5", 1)'}, {},
                                     simulate_row_context=False, culture="de-DE")["p"]
    assert got == pytest.approx(3.5)
