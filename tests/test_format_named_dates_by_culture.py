"""FORMAT on dates: the AMPM token, "General Date"'s zero parts, and the named
date and time formats by culture.

**Measured** on Power BI Desktop 2.157 (an en-US model, EVALUATE over Desktop's
own engine; the connection's locale changed none of the answers):

- EN_US / EN_AU: ``FORMAT(v, fmt)`` on the en-US model and
  ``FORMAT(v, fmt, "en-AU")``, four named formats and four pictures.
- MEASURED_C and NOON_MIDNIGHT: ``FORMAT(v, fmt, c)`` for en-US, en-AU, en-GB,
  en-NZ, en-IE and en-CA, all seven named formats and ``AMPM``.

Readings: the ``AMPM`` token prints the culture's own designators, case
included (``AM``/``PM`` in en-US, en-AU, en-GB, en-CA; ``am``/``pm`` in en-NZ
and en-IE; ``ampm`` is the same token). "General Date" drops a zero part:
midnight prints the date alone, a value on 1899-12-30 the time alone. Short
Date, Long Date, Long Time and General Date follow the culture; Medium Date
(``dd-mmm-yy``), Medium Time (``hh:nn AMPM``) and Short Time (``HH:nn``) are
the same in all six but for the designators. A picture ignores the culture's
date and time separators: ``d/MM/yyyy`` gives ``6/01/2026`` in en-US too.

**Not measured**, each marked where it is tested: a culture as the MODEL's
culture rather than the third argument (presumed the same: the model's culture
decides on an en-US model); the third argument beating another model culture;
culture names and named-format names in other cases; the spellings ``AmPm`` and
``aMpM``; the case rules of the older ``am/pm`` and ``a/p`` tokens; a date held
as ISO text; day zero at midnight. The refusals in TestCulturesWithoutPatterns
are decisions: Desktop formats an unknown culture name ("zz-ZZ", "xx") with an
unidentified fallback culture (PM designator "carra") and de-DE as German
(Short Date ``06.01.2026``, measured), which the fork does not port.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax.engine import DAXContext, DAXEngine

pytestmark = pytest.mark.unit

AFTERNOON = "DATE(2026,1,6) + TIME(13,30,5)"
MIDNIGHT = "DATE(2026,1,6)"
DAY_ZERO = "DATE(1899,12,30) + TIME(9,5,0)"
NEW_YEAR = "DATE(2011,1,1)"

NAMED = ("Short Date", "General Date", "Long Time", "Long Date")
PICTURES = ("d/MM/yyyy h:mm:ss AMPM", "M/d/yyyy h:mm:ss AMPM", "AMPM", "ampm")

# value -> the eight measured cells, in NAMED + PICTURES order
EN_US = {
    AFTERNOON: ("1/6/2026", "1/6/2026 1:30:05 PM", "1:30:05 PM", "Tuesday, January 6, 2026",
                "6/01/2026 1:30:05 PM", "1/6/2026 1:30:05 PM", "PM", "PM"),
    MIDNIGHT: ("1/6/2026", "1/6/2026", "12:00:00 AM", "Tuesday, January 6, 2026",
               "6/01/2026 12:00:00 AM", "1/6/2026 12:00:00 AM", "AM", "AM"),
    DAY_ZERO: ("12/30/1899", "9:05:00 AM", "9:05:00 AM", "Saturday, December 30, 1899",
               "30/12/1899 9:05:00 AM", "12/30/1899 9:05:00 AM", "AM", "AM"),
    NEW_YEAR: ("1/1/2011", "1/1/2011", "12:00:00 AM", "Saturday, January 1, 2011",
               "1/01/2011 12:00:00 AM", "1/1/2011 12:00:00 AM", "AM", "AM"),
}
EN_AU = {
    AFTERNOON: ("6/01/2026", "6/01/2026 1:30:05 PM", "1:30:05 PM", "Tuesday, 6 January 2026",
                "6/01/2026 1:30:05 PM", "1/6/2026 1:30:05 PM", "PM", "PM"),
    MIDNIGHT: ("6/01/2026", "6/01/2026", "12:00:00 AM", "Tuesday, 6 January 2026",
               "6/01/2026 12:00:00 AM", "1/6/2026 12:00:00 AM", "AM", "AM"),
    DAY_ZERO: ("30/12/1899", "9:05:00 AM", "9:05:00 AM", "Saturday, 30 December 1899",
               "30/12/1899 9:05:00 AM", "12/30/1899 9:05:00 AM", "AM", "AM"),
    NEW_YEAR: ("1/01/2011", "1/01/2011", "12:00:00 AM", "Saturday, 1 January 2011",
               "1/01/2011 12:00:00 AM", "1/1/2011 12:00:00 AM", "AM", "AM"),
}



C_FORMATS = ("Short Date", "General Date", "Long Time", "Long Date", "Medium Date", "Medium Time",
             "Short Time", "AMPM")

# culture -> value -> the eight cells in C_FORMATS order, FORMAT(v, fmt, culture)
MEASURED_C = {
    "en-US": {
        AFTERNOON: ('1/6/2026', '1/6/2026 1:30:05 PM', '1:30:05 PM', 'Tuesday, January 6, 2026', '06-Jan-26', '01:30 PM', '13:30', 'PM'),
        MIDNIGHT: ('1/6/2026', '1/6/2026', '12:00:00 AM', 'Tuesday, January 6, 2026', '06-Jan-26', '12:00 AM', '00:00', 'AM'),
        DAY_ZERO: ('12/30/1899', '9:05:00 AM', '9:05:00 AM', 'Saturday, December 30, 1899', '30-Dec-99', '09:05 AM', '09:05', 'AM'),
        NEW_YEAR: ('1/1/2011', '1/1/2011', '12:00:00 AM', 'Saturday, January 1, 2011', '01-Jan-11', '12:00 AM', '00:00', 'AM'),
    },
    "en-AU": {
        AFTERNOON: ('6/01/2026', '6/01/2026 1:30:05 PM', '1:30:05 PM', 'Tuesday, 6 January 2026', '06-Jan-26', '01:30 PM', '13:30', 'PM'),
        MIDNIGHT: ('6/01/2026', '6/01/2026', '12:00:00 AM', 'Tuesday, 6 January 2026', '06-Jan-26', '12:00 AM', '00:00', 'AM'),
        DAY_ZERO: ('30/12/1899', '9:05:00 AM', '9:05:00 AM', 'Saturday, 30 December 1899', '30-Dec-99', '09:05 AM', '09:05', 'AM'),
        NEW_YEAR: ('1/01/2011', '1/01/2011', '12:00:00 AM', 'Saturday, 1 January 2011', '01-Jan-11', '12:00 AM', '00:00', 'AM'),
    },
    "en-GB": {
        AFTERNOON: ('06/01/2026', '06/01/2026 13:30:05', '13:30:05', '06 January 2026', '06-Jan-26', '01:30 PM', '13:30', 'PM'),
        MIDNIGHT: ('06/01/2026', '06/01/2026', '00:00:00', '06 January 2026', '06-Jan-26', '12:00 AM', '00:00', 'AM'),
        DAY_ZERO: ('30/12/1899', '09:05:00', '09:05:00', '30 December 1899', '30-Dec-99', '09:05 AM', '09:05', 'AM'),
        NEW_YEAR: ('01/01/2011', '01/01/2011', '00:00:00', '01 January 2011', '01-Jan-11', '12:00 AM', '00:00', 'AM'),
    },
    "en-NZ": {
        AFTERNOON: ('6/01/2026', '6/01/2026 1:30:05 pm', '1:30:05 pm', 'Tuesday, 6 January 2026', '06-Jan-26', '01:30 pm', '13:30', 'pm'),
        MIDNIGHT: ('6/01/2026', '6/01/2026', '12:00:00 am', 'Tuesday, 6 January 2026', '06-Jan-26', '12:00 am', '00:00', 'am'),
        DAY_ZERO: ('30/12/1899', '9:05:00 am', '9:05:00 am', 'Saturday, 30 December 1899', '30-Dec-99', '09:05 am', '09:05', 'am'),
        NEW_YEAR: ('1/01/2011', '1/01/2011', '12:00:00 am', 'Saturday, 1 January 2011', '01-Jan-11', '12:00 am', '00:00', 'am'),
    },
    "en-IE": {
        AFTERNOON: ('06/01/2026', '06/01/2026 13:30:05', '13:30:05', 'Tuesday 6 January 2026', '06-Jan-26', '01:30 pm', '13:30', 'pm'),
        MIDNIGHT: ('06/01/2026', '06/01/2026', '00:00:00', 'Tuesday 6 January 2026', '06-Jan-26', '12:00 am', '00:00', 'am'),
        DAY_ZERO: ('30/12/1899', '09:05:00', '09:05:00', 'Saturday 30 December 1899', '30-Dec-99', '09:05 am', '09:05', 'am'),
        NEW_YEAR: ('01/01/2011', '01/01/2011', '00:00:00', 'Saturday 1 January 2011', '01-Jan-11', '12:00 am', '00:00', 'am'),
    },
    "en-CA": {
        AFTERNOON: ('2026-01-06', '2026-01-06 1:30:05 PM', '1:30:05 PM', 'January 6, 2026', '06-Jan-26', '01:30 PM', '13:30', 'PM'),
        MIDNIGHT: ('2026-01-06', '2026-01-06', '12:00:00 AM', 'January 6, 2026', '06-Jan-26', '12:00 AM', '00:00', 'AM'),
        DAY_ZERO: ('1899-12-30', '9:05:00 AM', '9:05:00 AM', 'December 30, 1899', '30-Dec-99', '09:05 AM', '09:05', 'AM'),
        NEW_YEAR: ('2011-01-01', '2011-01-01', '12:00:00 AM', 'January 1, 2011', '01-Jan-11', '12:00 AM', '00:00', 'AM'),
    },
}
NOON = "DATE(2026,1,6) + TIME(12,0,0)"
# culture -> (AMPM at noon, h:nn AMPM at noon, AMPM at midnight, h:nn AMPM at midnight)
NOON_MIDNIGHT = {
    "en-US": ('PM', '12:00 PM', 'AM', '12:00 AM'),
    "en-AU": ('PM', '12:00 PM', 'AM', '12:00 AM'),
    "en-GB": ('PM', '12:00 PM', 'AM', '12:00 AM'),
    "en-NZ": ('pm', '12:00 pm', 'am', '12:00 am'),
    "en-IE": ('pm', '12:00 pm', 'am', '12:00 am'),
    "en-CA": ('PM', '12:00 PM', 'AM', '12:00 AM'),
}
OTHER_CULTURES = [c for c in MEASURED_C if c not in ("en-US", "en-AU")]

def _ctx(culture=None):
    c = DAXContext({"T": {"columns": ["A"], "rows": [[1.0]]}}, {}, None, None, None, [])
    c.culture = culture
    return c


def _cells(table):
    return [(v, f, want) for v, row in table.items() for f, want in zip(NAMED + PICTURES, row)]


def _c_cells():
    return [(c, v, f, want) for c, by_value in MEASURED_C.items() for v, row in by_value.items()
            for f, want in zip(C_FORMATS, row)]


def _format(value, fmt, locale=None, model=None):
    e = DAXEngine()
    third = f', "{locale}"' if locale is not None else ""
    got = e._eval_expr(f'FORMAT({value}, "{fmt}"{third})', _ctx(model))
    return got, e


class TestMeasuredEnUs:
    @pytest.mark.parametrize("value,fmt,want", _cells(EN_US))
    def test_the_model_culture(self, value, fmt, want):
        assert _format(value, fmt, model="en-US")[0] == want

    @pytest.mark.parametrize("value,fmt,want", _cells(EN_US))
    def test_the_third_argument(self, value, fmt, want):
        # identical to the model's en-US in every measured cell
        assert _format(value, fmt, locale="en-US", model="en-US")[0] == want

    @pytest.mark.parametrize("value,fmt,want", _cells(EN_US))
    def test_no_culture_is_en_us(self, value, fmt, want):
        assert _format(value, fmt)[0] == want


class TestMeasuredEnAu:
    @pytest.mark.parametrize("value,fmt,want", _cells(EN_AU))
    def test_the_third_argument(self, value, fmt, want):
        assert _format(value, fmt, locale="en-AU", model="en-US")[0] == want

    @pytest.mark.parametrize("value,fmt,want", _cells(EN_AU))
    def test_the_model_culture(self, value, fmt, want):
        # NOT measured: no en-AU model was opened. The model's culture decides
        # when no third argument is given (measured on an en-US model), so an
        # en-AU model is presumed to print what the third argument does.
        assert _format(value, fmt, model="en-AU")[0] == want

    def test_the_third_argument_beats_the_model(self):
        # NOT measured on an en-AU model (the third argument's precedence is
        # the fork's existing rule for separators)
        assert _format(AFTERNOON, "Short Date", locale="en-US", model="en-AU")[0] == "1/6/2026"

    def test_the_culture_name_is_case_insensitive(self):
        # NOT measured: a decision, as for separators
        assert _format(AFTERNOON, "Short Date", locale="EN-au")[0] == "6/01/2026"


class TestMeasuredSixCultures:
    @pytest.mark.parametrize("culture,value,fmt,want", _c_cells())
    def test_the_third_argument(self, culture, value, fmt, want):
        assert _format(value, fmt, locale=culture, model="en-US")[0] == want

    @pytest.mark.parametrize("culture,value,fmt,want", _c_cells())
    def test_the_model_culture(self, culture, value, fmt, want):
        # NOT measured as a model's culture (presumed the same, as above)
        assert _format(value, fmt, model=culture)[0] == want

    @pytest.mark.parametrize("culture", sorted(NOON_MIDNIGHT))
    def test_noon_and_midnight(self, culture):
        got = (_format(NOON, "AMPM", locale=culture)[0], _format(NOON, "h:nn AMPM", locale=culture)[0],
               _format(MIDNIGHT, "AMPM", locale=culture)[0], _format(MIDNIGHT, "h:nn AMPM", locale=culture)[0])
        assert got == NOON_MIDNIGHT[culture]


class TestAmPmToken:
    # "AMPM" and "ampm" measured; "AmPm" and "aMpM" NOT measured (presumed alike)
    @pytest.mark.parametrize("token", ["AMPM", "ampm", "AmPm", "aMpM"])
    def test_any_case_prints_the_cultures_designators(self, token):
        assert _format(AFTERNOON, token)[0] == "PM"
        assert _format(MIDNIGHT, token)[0] == "AM"
        assert _format(AFTERNOON, token, locale="en-NZ")[0] == "pm"
        assert _format(MIDNIGHT, token, locale="en-IE")[0] == "am"

    def test_the_older_tokens_keep_their_case(self):
        # NOT measured here: the fork's existing behaviour, kept
        assert _format(AFTERNOON, "am/pm")[0] == "pm"
        assert _format(AFTERNOON, "AM/PM")[0] == "PM"
        assert _format(AFTERNOON, "a/p")[0] == "p"
        assert _format(AFTERNOON, "AM/PM", locale="en-NZ")[0] == "PM"

    def test_an_ampm_string_value_is_read_as_a_date(self):
        # NOT measured: text holding a date reaches FORMAT in row context over
        # generated tables, and the fork reads it as the date
        assert _format('"2026-01-06T13:30:05"', "AMPM")[0] == "PM"


class TestGeneralDateZeroParts:
    def test_a_time_on_day_zero_prints_the_time_alone(self):
        # TIME(9,5,0) is the measured DATE(1899,12,30) + TIME(9,5,0)
        assert _format("TIME(9,5,0)", "General Date")[0] == "9:05:00 AM"

    def test_the_named_format_is_case_insensitive(self):
        # NOT measured in lower case: the fork's existing rule, kept
        assert _format(MIDNIGHT, "general date")[0] == "1/6/2026"

    def test_day_zero_at_midnight(self):
        # NOT measured: both parts zero; the time alone is printed
        assert _format("DATE(1899,12,30)", "General Date")[0] == "12:00:00 AM"


class TestCulturesWithoutPatterns:
    """Decisions, not measurements: a named date format, or the AMPM token, in
    a culture whose data the fork does not hold is reported unsupported, never
    printed in en-US's. (Desktop prints de-DE as German and an unknown name in
    an unidentified fallback culture; neither is ported.)"""

    @pytest.mark.parametrize("culture", ["de-DE", "fr-FR", "pt-BR", "zz-ZZ", "xx", "ar-SA"])
    @pytest.mark.parametrize("fmt", C_FORMATS)
    def test_a_culture_without_patterns_is_refused(self, culture, fmt):
        # de-DE, fr-FR, pt-BR have separators in the fork; zz-ZZ, xx, ar-SA do not
        got, e = _format(AFTERNOON, fmt, locale=culture)
        assert got is None
        assert any("FORMAT" in f and culture in f for f in e.unsupported_functions)

    @pytest.mark.parametrize("culture", ["zz-ZZ", "ar-SA"])
    def test_a_model_culture_without_patterns_is_refused(self, culture):
        got, e = _format(AFTERNOON, "Short Date", model=culture)
        assert got is None
        assert e.unsupported_functions

    def test_a_picture_with_ampm_is_refused_there(self):
        got, e = _format(AFTERNOON, "h:nn AMPM", locale="de-DE")
        assert got is None
        assert e.unsupported_functions

    def test_a_picture_without_ampm_is_not(self):
        # NOT measured for de-DE: the fork's existing picture rendering, kept
        got, e = _format(AFTERNOON, "yyyy-mm-dd", locale="de-DE")
        assert got == "2026-01-06"
        assert not e.unsupported_functions

    def test_an_empty_third_argument_is_the_models_culture(self):
        # Desktop prints the invariant culture for "" (01/06/2026 13:30:05,
        # measured); not ported: the fork reads "" as no third argument
        assert _format(AFTERNOON, "Short Date", locale="", model="en-AU")[0] == "6/01/2026"

    def test_numbers_keep_the_separators_fallback(self):
        # FORMAT's separators still fall back to en-US for an unknown culture
        got, e = _format("1234.5", "#,##0.00", locale="zz-ZZ")
        assert got == "1,234.50"
        assert not e.unsupported_functions
