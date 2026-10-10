"""Issue #170: VALUE reads text as Desktop does in an en-US model -- as OLE
Automation's VarR8FromStr, then VarDateFromStr -- and text that is no number
is an error.

A number: Unicode space around it, one sign leading or trailing, parentheses
for minus (not with a sign), the currency sign before and / or after, ASCII
digits, a group separator anywhere after the first digit, an exponent with e /
E / d / D, &H / &O / & hex and octal (eight hex digits are a signed 32-bit
number). Otherwise a date or a time: numbers with / - , or space, an English
month or a 3-letter-or-longer prefix of one, h:m[:s] with AM / PM, in any
order; two numbers tried as month-day (this year), month-year, day-month,
year-month, three as month-day-year, year-month-day, day-month-year; a year
below 100 is 1950 - 2049. No percent, no weekday names, no ISO "T". BLANK is
BLANK.

The engine returned 0 for anything it couldn't parse -- VALUE("1e3"),
VALUE("(5)"), VALUE("2024-01-02"), VALUE("&H10") were 0 -- and 0 again where
Desktop raises (VALUE("abc"), VALUE("TRUE")); VALUE("50%") was 50.

A probe whose answer is a day of the measuring year is pinned as that day of
the current year.

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
    ('b166:value_thousands', 'VALUE("1,234.5")', '1234.5'),
    ('b166:value_dollar', 'VALUE("$1,000")', '1000'),
    ('b166:value_space', 'VALUE(" 12 ")', '12'),
    ('b166:value_exp', 'VALUE("1e3")', '1000'),
    ('b166:value_err', 'IFERROR(VALUE("abc"), "err")', 'err'),
    ('b166:value_neg', 'VALUE("(5)")', '-5'),
    ('b166b:value_plus', 'VALUE("+5")', '5'),
    ('b166b:value_minus_trail', 'IFERROR(VALUE("5-"), "err")', '-5'),
    ('b166b:value_comma_dec', 'IFERROR(VALUE("1,5"), "err")', '15'),
    ('b166b:value_date', 'IFERROR(VALUE("2024-01-02"), "err")', '45293'),
    ('b166b:value_time', 'IFERROR(VALUE("12:00"), "err")', '0.5'),
    ('b166b:value_true', 'IFERROR(VALUE("TRUE"), "err")', 'err'),
    ('b166b:value_blank', 'IFERROR(VALUE(""), "err")', 'err'),
    ('b166b:value_curr_neg', 'VALUE("-$1,000.50")', '-1000.5'),
    ('b174:v_hex', 'IFERROR(VALUE("&H10"), "err")', '16'),
    ('b174:v_d_exp', 'IFERROR(VALUE("1d3"), "err")', '1000'),
    ('b174:v_space_cur', 'IFERROR(VALUE("$ 5"), "err")', '5'),
    ('b174:v_space_inner', 'IFERROR(VALUE("1 000"), "err")', '36526'),
    ('b174:v_sign_space', 'IFERROR(VALUE("- 5"), "err")', '-5'),
    ('b174:v_trail_space', 'IFERROR(VALUE("5 -"), "err")', '-5'),
    ('b174:v_cur_after', 'IFERROR(VALUE("5$"), "err")', '5'),
    ('b174:v_euro', 'IFERROR(VALUE(UNICHAR(8364) & "5"), "err")', 'err'),
    ('b174:v_pound', 'IFERROR(VALUE(UNICHAR(163) & "5"), "err")', 'err'),
    ('b174:v_dot_end', 'IFERROR(VALUE("1."), "err")', '1'),
    ('b174:v_dot_start', 'IFERROR(VALUE(".5"), "err")', '0.5'),
    ('b174:v_comma_start', 'IFERROR(VALUE(",5"), "err")', 'err'),
    ('b174:v_comma_frac', 'IFERROR(VALUE("1.2,3"), "err")', '1.23'),
    ('b174:v_double_comma', 'IFERROR(VALUE("1,,2"), "err")', '12'),
    ('b174:v_comma_end', 'IFERROR(VALUE("5,"), "err")', '5'),
    ('b174:v_paren_neg', 'IFERROR(VALUE("(-5)"), "err")', 'err'),
    ('b174:v_paren_cur', 'IFERROR(VALUE("($5)"), "err")', '-5'),
    ('b174:v_cur_neg', 'IFERROR(VALUE("$-5"), "err")', '-5'),
    ('b174:v_plus_paren', 'IFERROR(VALUE("+(5)"), "err")', 'err'),
    ('b174:v_two_signs', 'IFERROR(VALUE("-5-"), "err")', 'err'),
    ('b174:v_tab', 'IFERROR(VALUE(UNICHAR(9) & "5"), "err")', '5'),
    ('b174:v_nbsp', 'IFERROR(VALUE(UNICHAR(160) & "5"), "err")', '5'),
    ('b174:v_exp_neg', 'IFERROR(VALUE("2.5E-1"), "err")', '0.25'),
    ('b174:v_exp_plus', 'IFERROR(VALUE("1e+3"), "err")', '1000'),
    ('b174:v_inf', 'IFERROR(VALUE("Infinity"), "err")', 'err'),
    ('b174:v_nan', 'IFERROR(VALUE("NaN"), "err")', 'err'),
    ('b174:v_big', 'VALUE("12345678901234567890")', '1.23456789012346E+19'),
    ('b174:v_leading_zero', 'VALUE("007")', '7'),
    ('b174:v_year', 'IFERROR(VALUE("2024"), "err")', '2024'),
    ('b174:v_datetime', 'IFERROR(VALUE("2024-01-02 12:00"), "err")', '45293.5'),
    ('b174:v_iso_t', 'IFERROR(VALUE("2024-01-02T12:00:00"), "err")', 'err'),
    ('b174:v_us_date', 'IFERROR(VALUE("1/2/2024"), "err")', '45293'),
    ('b174:v_us_date_short', 'IFERROR(VALUE("1/2/24"), "err")', '45293'),
    ('b174:v_month_name', 'IFERROR(VALUE("Jan 2, 2024"), "err")', '45293'),
    ('b174:v_month_name2', 'IFERROR(VALUE("2 January 2024"), "err")', '45293'),
    ('b174:v_ampm', 'IFERROR(VALUE("1:30 PM"), "err")', '0.5625'),
    ('b174:v_secs', 'IFERROR(VALUE("12:00:30"), "err")', '0.500347222222222'),
    ('b174:v_pct_space', 'IFERROR(VALUE("50 %"), "err")', 'err'),
    ('b174:v_num_arg', 'VALUE(5)', '5'),
    ('b174:v_bool_arg', 'IFERROR(VALUE(TRUE()), "err")', 'err'),
    ('b174:v_date_arg', 'IFERROR(VALUE(DATE(2024, 1, 2)), "err")', '45293'),
    ('b174:v_blank_arg', 'IFERROR(VALUE(BLANK()), "err")', None),
    ('b174:v_blank_isblank', 'IF(ISBLANK(IFERROR(VALUE(BLANK()), 99)), 1, 0)', '1'),
    ('b174b:b_value_empty', 'IFERROR(VALUE(""), "err")', 'err'),
    ('b174b:v_oct', 'IFERROR(VALUE("&O17"), "err")', '15'),
    ('b174b:v_amp_digits', 'IFERROR(VALUE("&17"), "err")', '15'),
    ('b174b:v_hex_ffff', 'IFERROR(VALUE("&HFFFF"), "err")', '65535'),
    ('b174b:v_hex_big', 'IFERROR(VALUE("&H10000"), "err")', '65536'),
    ('b174b:v_hex_ffffffff', 'IFERROR(VALUE("&HFFFFFFFF"), "err")', '-1'),
    ('b174b:v_hex_lower', 'IFERROR(VALUE("&hff"), "err")', '255'),
    ('b174b:v_hex_neg', 'IFERROR(VALUE("-&H10"), "err")', 'err'),
    ('b174b:v_e_only', 'IFERROR(VALUE("1e"), "err")', 'err'),
    ('b174b:v_e_lead', 'IFERROR(VALUE("e3"), "err")', 'err'),
    ('b174b:v_d_neg', 'IFERROR(VALUE("2D-1"), "err")', '0.2'),
    ('b174b:v_comma_after_dot2', 'IFERROR(VALUE("1.5,5,5"), "err")', '1.555'),
    ('b174b:v_comma_dot_comma', 'IFERROR(VALUE("1,2.3,4"), "err")', '12.34'),
    ('b174b:v_space_date2', 'IFERROR(VALUE("1 2"), "err")', 'THIS_YEAR 1 2'),
    ('b174b:v_space_date3', 'IFERROR(VALUE("1 2 2024"), "err")', '45293'),
    ('b174b:v_space_big', 'IFERROR(VALUE("12 2024"), "err")', '45627'),
    ('b174b:v_space_13', 'IFERROR(VALUE("13 2024"), "err")', 'err'),
    ('b174b:v_dash_date', 'IFERROR(VALUE("1-2-2024"), "err")', '45293'),
    ('b174b:v_dot_date', 'IFERROR(VALUE("2.1.2024"), "err")', 'err'),
    ('b174b:v_ymd_slash', 'IFERROR(VALUE("2024/01/02"), "err")', '45293'),
    ('b174b:v_dmy_invalid_m', 'IFERROR(VALUE("13/1/2024"), "err")', '45304'),
    ('b174b:v_month_year', 'IFERROR(VALUE("January 2024"), "err")', '45292'),
    ('b174b:v_mon_year_dash', 'IFERROR(VALUE("Jan-2024"), "err")', '45292'),
    ('b174b:v_mon_dash_day_year', 'IFERROR(VALUE("2-Jan-2024"), "err")', '45293'),
    ('b174b:v_weekday', 'IFERROR(VALUE("Tuesday, January 2, 2024"), "err")', 'err'),
    ('b174b:v_month_lower', 'IFERROR(VALUE("jan 2 2024"), "err")', '45293'),
    ('b174b:v_sept', 'IFERROR(VALUE("Sept 2, 2024"), "err")', '45537'),
    ('b174b:v_time_am', 'IFERROR(VALUE("12:00 AM"), "err")', '0'),
    ('b174b:v_time_25', 'IFERROR(VALUE("25:00"), "err")', 'err'),
    ('b174b:v_time_hms', 'IFERROR(VALUE("1:2:3"), "err")', '0.0430902777777778'),
    ('b174b:v_time_pm_only', 'IFERROR(VALUE("3 PM"), "err")', '0.625'),
    ('b174b:v_time_frac', 'IFERROR(VALUE("12:30:15.5"), "err")', 'err'),
    ('b174b:v_date_time_pm', 'IFERROR(VALUE("1/2/2024 3:30 PM"), "err")', '45293.6458333333'),
    ('b174b:v_two_digit_30', 'IFERROR(VALUE("1/2/30"), "err")', '47485'),
    ('b174b:v_two_digit_29', 'IFERROR(VALUE("1/2/29"), "err")', '47120'),
    ('b174b:v_year_first_dash', 'IFERROR(VALUE("2024-1-2"), "err")', '45293'),
    ('b174b:v_iso_space_secs', 'IFERROR(VALUE("2024-01-02 12:00:30"), "err")', '45293.5003472222'),
    ('b174b:v_nbsp_inner', 'IFERROR(VALUE("1" & UNICHAR(160) & "000"), "err")', '36526'),
    ('b174b:v_lf', 'IFERROR(VALUE("5" & UNICHAR(10)), "err")', '5'),
    ('b174b:v_cr_lead', 'IFERROR(VALUE(UNICHAR(13) & "5"), "err")', '5'),
    ('b174b:v_ideo_space', 'IFERROR(VALUE(UNICHAR(12288) & "5"), "err")', '5'),
    ('b174b:v_fullwidth_digit', 'IFERROR(VALUE(UNICHAR(65301)), "err")', 'err'),
    ('b174b:v_arabic_digit', 'IFERROR(VALUE(UNICHAR(1637)), "err")', 'err'),
    ('b174b:v_plus_dollar', 'IFERROR(VALUE("+$5"), "err")', '5'),
    ('b174b:v_dollar_paren', 'IFERROR(VALUE("$(5)"), "err")', '-5'),
    ('b174b:v_trail_plus', 'IFERROR(VALUE("5+"), "err")', '5'),
    ('b174b:v_dot_only', 'IFERROR(VALUE("."), "err")', 'err'),
    ('b174b:v_dollar_only', 'IFERROR(VALUE("$"), "err")', 'err'),
    ('b174b:v_minus_only', 'IFERROR(VALUE("-"), "err")', 'err'),
    ('b174b:v_exp_space', 'IFERROR(VALUE("1 e3"), "err")', 'err'),
    ('b174b:v_huge_exp', 'IFERROR(VALUE("1e400"), "err")', 'err'),
    ('b174b:v_tiny_exp', 'IFERROR(VALUE("1e-400"), "err")', '0'),
    ('b174b:v_many_digits', 'VALUE("0.1234567890123456789")', '0.123456789012346'),
    ('b174b:v_type_int', 'IF(ISBLANK(VALUE("5") / 2), 1, VALUE("5") / 2)', '2.5'),
    ('b174c:v_y49', 'IFERROR(VALUE("1/2/49"), "err")', '54425'),
    ('b174c:v_y50', 'IFERROR(VALUE("1/2/50"), "err")', '18265'),
    ('b174c:v_y68', 'IFERROR(VALUE("1/2/68"), "err")', '24839'),
    ('b174c:v_y69', 'IFERROR(VALUE("1/2/69"), "err")', '25205'),
    ('b174c:v_y99', 'IFERROR(VALUE("1/2/99"), "err")', '36162'),
    ('b174c:v_y00', 'IFERROR(VALUE("1/2/00"), "err")', '36527'),
    ('b174c:v_y3digit', 'IFERROR(VALUE("1/2/100"), "err")', '-657433'),
    ('b174c:v_y_1899', 'IFERROR(VALUE("12/30/1899"), "err")', '0'),
    ('b174c:v_y_1900', 'IFERROR(VALUE("1/1/1900"), "err")', '2'),
    ('b174c:v_y_1800', 'IFERROR(VALUE("1/1/1800"), "err")', '-36522'),
    ('b174c:v_y_99999', 'IFERROR(VALUE("1/1/9999"), "err")', '2958101'),
    ('b174c:v_feb29', 'IFERROR(VALUE("2/29/2023"), "err")', 'err'),
    ('b174c:v_feb29_ok', 'IFERROR(VALUE("2/29/2024"), "err")', '45351'),
    ('b174c:v_day32', 'IFERROR(VALUE("1/32/2024"), "err")', 'err'),
    ('b174c:v_dmy_32', 'IFERROR(VALUE("32/1/2024"), "err")', 'err'),
    ('b174c:v_ymd_dash_2digit', 'IFERROR(VALUE("24-1-2"), "err")', '45293'),
    ('b174c:v_md_dash', 'IFERROR(VALUE("1-2"), "err")', 'THIS_YEAR 1 2'),
    ('b174c:v_month_only', 'IFERROR(VALUE("January"), "err")', 'err'),
    ('b174c:v_month_day', 'IFERROR(VALUE("January 2"), "err")', 'THIS_YEAR 1 2'),
    ('b174c:v_day_month', 'IFERROR(VALUE("2 Jan"), "err")', 'THIS_YEAR 1 2'),
    ('b174c:v_year_month', 'IFERROR(VALUE("2024 January"), "err")', '45292'),
    ('b174c:v_month_2digit', 'IFERROR(VALUE("Jan 24"), "err")', 'THIS_YEAR 1 24'),
    ('b174c:v_month_31', 'IFERROR(VALUE("Jan 31"), "err")', 'THIS_YEAR 1 31'),
    ('b174c:v_month_32', 'IFERROR(VALUE("Jan 32"), "err")', '48214'),
    ('b174c:v_month_dot', 'IFERROR(VALUE("Jan. 2, 2024"), "err")', '45293'),
    ('b174c:v_month_comma_year', 'IFERROR(VALUE("January, 2024"), "err")', '45292'),
    ('b174c:v_iso_slash_time', 'IFERROR(VALUE("2024/01/02 13:45"), "err")', '45293.5729166667'),
    ('b174c:v_time_first', 'IFERROR(VALUE("13:45 1/2/2024"), "err")', '45293.5729166667'),
    ('b174c:v_time_hm_pm', 'IFERROR(VALUE("1:30PM"), "err")', '0.5625'),
    ('b174c:v_time_a', 'IFERROR(VALUE("1:30 A"), "err")', '0.0625'),
    ('b174c:v_time_p', 'IFERROR(VALUE("1:30 p"), "err")', '0.5625'),
    ('b174c:v_time_13pm', 'IFERROR(VALUE("13:00 PM"), "err")', '0.541666666666667'),
    ('b174c:v_time_0am', 'IFERROR(VALUE("0:30 AM"), "err")', '0.0208333333333333'),
    ('b174c:v_time_secs60', 'IFERROR(VALUE("1:00:60"), "err")', 'err'),
    ('b174c:v_time_min60', 'IFERROR(VALUE("1:60"), "err")', 'err'),
    ('b174c:v_time_hour_only', 'IFERROR(VALUE("13:"), "err")', 'err'),
    ('b174c:v_time_24', 'IFERROR(VALUE("24:00"), "err")', 'err'),
    ('b174c:v_space_three', 'IFERROR(VALUE("1 2 3"), "err")', '37623'),
    ('b174c:v_space_year_first', 'IFERROR(VALUE("2024 1 2"), "err")', '45293'),
    ('b174c:v_space_two_31', 'IFERROR(VALUE("1 31"), "err")', 'THIS_YEAR 1 31'),
    ('b174c:v_space_two_32', 'IFERROR(VALUE("1 32"), "err")', '48214'),
    ('b174c:v_space_13_1', 'IFERROR(VALUE("13 1"), "err")', 'THIS_YEAR 1 13'),
    ('b174c:v_space_0_5', 'IFERROR(VALUE("0 5"), "err")', '36647'),
    ('b174c:v_comma_date', 'IFERROR(VALUE("1,2,2024"), "err")', '122024'),
    ('b174c:v_hex_8digits', 'IFERROR(VALUE("&H7FFFFFFF"), "err")', '2147483647'),
    ('b174c:v_hex_80000000', 'IFERROR(VALUE("&H80000000"), "err")', '-2147483648'),
    ('b174c:v_hex_9digits', 'IFERROR(VALUE("&H100000000"), "err")', '4294967296'),
    ('b174c:v_hex_empty', 'IFERROR(VALUE("&H"), "err")', 'err'),
    ('b174c:v_hex_space', 'IFERROR(VALUE(" &H10 "), "err")', '16'),
    ('b174c:v_hex_dot', 'IFERROR(VALUE("&H1.5"), "err")', 'err'),
    ('b174c:v_oct_8', 'IFERROR(VALUE("&O8"), "err")', 'err'),
    ('b174c:v_oct_big', 'IFERROR(VALUE("&O37777777777"), "err")', '-1'),
    ('b174c:v_hex_paren', 'IFERROR(VALUE("(&H10)"), "err")', 'err'),
    ('b174c:v_hex_dollar', 'IFERROR(VALUE("$&H10"), "err")', 'err'),
    ('b174c:v_paren_space', 'IFERROR(VALUE("( 5 )"), "err")', '-5'),
    ('b174c:v_paren_trail_minus', 'IFERROR(VALUE("(5)-"), "err")', 'err'),
    ('b174c:v_dollar_trail_minus', 'IFERROR(VALUE("$5-"), "err")', '-5'),
    ('b174c:v_minus_dollar_trail', 'IFERROR(VALUE("5$-"), "err")', '-5'),
    ('b174c:v_two_dollars', 'IFERROR(VALUE("$5$"), "err")', '5'),
    ('b174c:v_comma_after_e', 'IFERROR(VALUE("1e1,0"), "err")', 'err'),
    ('b174c:v_dot_after_e', 'IFERROR(VALUE("1e1.5"), "err")', 'err'),
    ('b174c:v_comma_lead_zero', 'IFERROR(VALUE("0,5"), "err")', '5'),
    ('b174c:v_lead_zeros_comma', 'IFERROR(VALUE("00,1"), "err")', '1'),
    ('b174c:v_dot_comma', 'IFERROR(VALUE(".,5"), "err")', 'err'),
    ('b174c:v_neg_dot', 'IFERROR(VALUE("-.5"), "err")', '-0.5'),
    ('b174c:v_space_inside_number', 'IFERROR(VALUE("1 5.5"), "err")', 'err'),
    ('b175:v_feb30', 'IFERROR(VALUE("Feb 30"), "err")', '47515'),
    ('b175:v_2_30', 'IFERROR(VALUE("2 30"), "err")', '47515'),
    ('b175:v_jan05', 'IFERROR(VALUE("Jan 05"), "err")', 'THIS_YEAR 1 5'),
    ('b175:v_jan00', 'IFERROR(VALUE("Jan 00"), "err")', '36526'),
    ('b175:v_jan_2024_2', 'IFERROR(VALUE("Jan 2024 2"), "err")', '45293'),
    ('b175:v_2024_jan_2', 'IFERROR(VALUE("2024 Jan 2"), "err")', '45293'),
    ('b175:v_2_2024_jan', 'IFERROR(VALUE("2 2024 Jan"), "err")', '45293'),
    ('b175:v_31_12_2024', 'IFERROR(VALUE("31/12/2024"), "err")', '45657'),
    ('b175:v_y049', 'IFERROR(VALUE("1/2/049"), "err")', '54425'),
    ('b175:v_y0049', 'IFERROR(VALUE("1/2/0049"), "err")', '54425'),
    ('b175:v_mixed_seps', 'IFERROR(VALUE("1/2-2024"), "err")', '45293'),
    ('b175:v_comma_seps', 'IFERROR(VALUE("1, 2, 2024"), "err")', '45293'),
    ('b175:v_ma', 'IFERROR(VALUE("Ma 2, 2024"), "err")', 'err'),
    ('b175:v_mar', 'IFERROR(VALUE("Mar 2, 2024"), "err")', '45353'),
    ('b175:v_month_full_wrong', 'IFERROR(VALUE("Janu 2, 2024"), "err")', '45293'),
    ('b175:v_month_overlong', 'IFERROR(VALUE("Januaryx 2, 2024"), "err")', 'err'),
    ('b175:v_two_months', 'IFERROR(VALUE("Jan Feb 2"), "err")', 'err'),
    ('b175:v_time_then_ampm_sep', 'IFERROR(VALUE("1:30 P.M."), "err")', 'err'),
    ('b175:v_noon', 'IFERROR(VALUE("12 PM"), "err")', '0.5'),
    ('b175:v_midnight', 'IFERROR(VALUE("12 AM"), "err")', '0'),
    ('b175:v_hour_ampm_13', 'IFERROR(VALUE("13 PM"), "err")', '0.541666666666667'),
    ('b175:v_date_time_no_space', 'IFERROR(VALUE("1/2/2024 13:00"), "err")', '45293.5416666667'),
    ('b175:v_two_times', 'IFERROR(VALUE("1:00 2:00"), "err")', 'err'),
    ('b175:v_date_ampm_only', 'IFERROR(VALUE("1/2/2024 PM"), "err")', 'err'),
    ('b175:v_hex_long', 'IFERROR(VALUE("&HFFFFFFFFFFFFFFFF"), "err")', '-1'),
    ('b175:v_hex_17', 'IFERROR(VALUE("&H10000000000000000"), "err")', 'err'),
    ('b175:v_curr_space_neg', 'IFERROR(VALUE("$ -5"), "err")', '-5'),
    ('b175:v_exp_dollar', 'IFERROR(VALUE("1e3$"), "err")', '1000'),
]


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(probe, dax, want):
    assert _same(_engine(dax), _want(want))


def test_a_percent_is_an_error():
    # Desktop: VALUE("50%") fails ("Cannot convert value '50%' of type Text to type Number")
    assert _engine('IFERROR(VALUE("50%"), "err")') == "err"
