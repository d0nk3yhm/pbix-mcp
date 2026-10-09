"""
pbix-mcp — DAX Engine
=====================
Evaluates DAX measure expressions against VertiPaq data.

Supports 150+ DAX functions:
- Aggregation: SUM, AVERAGE, COUNT, COUNTROWS, MIN, MAX, DISTINCTCOUNT, PRODUCT, MEDIAN
- Iteration: SUMX, MAXX, MINX, AVERAGEX, COUNTX, COUNTAX, COUNTBLANK
- Table: TOPN, ADDCOLUMNS, SUMMARIZE, SUMMARIZECOLUMNS, SELECTCOLUMNS, DISTINCT,
         UNION, EXCEPT, INTERSECT, CROSSJOIN, DATATABLE, ROW, TREATAS, GENERATE, GENERATEALL, GENERATESERIES
- Filter: CALCULATE, REMOVEFILTERS, ALL, ALLEXCEPT, ALLSELECTED, KEEPFILTERS,
          VALUES, FILTER, HASONEVALUE, HASONEFILTER, ISFILTERED, ISCROSSFILTERED,
          USERELATIONSHIP, EARLIER, EARLIEST
- Time Intelligence: DATEADD, SAMEPERIODLASTYEAR, DATESYTD/MTD/QTD, TOTALYTD/MTD/QTD,
                     PREVIOUSMONTH/QUARTER/YEAR, NEXTMONTH/QUARTER/YEAR, PARALLELPERIOD,
                     STARTOFMONTH/QUARTER/YEAR, ENDOFMONTH/QUARTER/YEAR,
                     OPENINGBALANCEMONTH/QUARTER/YEAR, CLOSINGBALANCEMONTH/QUARTER/YEAR,
                     FIRSTDATE, LASTDATE, DATESBETWEEN, DATESINPERIOD, CALENDAR, CALENDARAUTO
- Math: DIVIDE, ABS, ROUND, INT, CEILING, FLOOR, MOD, POWER, SQRT, LOG, LOG10, LN, EXP,
        SIGN, TRUNC, EVEN, ODD, FACT, GCD, LCM, RAND, RANDBETWEEN, PI, CURRENCY, FIXED
- Text: CONCATENATE, FORMAT, SELECTEDVALUE, LEFT, RIGHT, MID, LEN, UPPER, LOWER, PROPER,
        TRIM, SUBSTITUTE, REPLACE, REPT, SEARCH, FIND, CONTAINSSTRING, CONTAINSSTRINGEXACT,
        EXACT, UNICHAR, UNICODE, VALUE, COMBINEVALUES, PATHCONTAINS, PATHITEM, PATHLENGTH
- Logic: IF, SWITCH, AND, OR, NOT, ISBLANK, BLANK, TRUE, FALSE, IFERROR, COALESCE, CONTAINS
- Info: ISNUMBER, ISTEXT, ISNONTEXT, ISLOGICAL, ISERROR, USERNAME, USERPRINCIPALNAME,
        LOOKUPVALUE
- Relationship: RELATED, RELATEDTABLE, CROSSFILTER
- Table references: table[column] syntax
- Measure references: [MeasureName] syntax
- VAR / RETURN: variable declarations with expression evaluation
- String concatenation with &
"""

import bisect
import calendar
import decimal
import json
import math
import os
import random
import re
import statistics
import threading
import time
from calendar import monthrange
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from typing import AbstractSet, Any, Optional

from pbix_mcp.errors import DAXEvaluationError as _DAXEvaluationError

# Sentinel returned by _eval_binary to mean "this expression is NOT a binary
# expression" — distinct from a genuine BLANK (None) result of an operation such
# as x / 0. It lets _eval_expr return BLANK for a real division-by-zero instead
# of falling through and mis-parsing the expression as a bare function call
# (which would return just the numerator).
_NOT_APPLICABLE = object()

# Cache-miss sentinel, distinct from a cached None (which is a real answer:
# "this bare column name resolves to nothing").
_MISSING = object()

# An aggregation call in a FILTER condition aggregates over the context rather
# than the iterated row, so its column references must NOT be substituted with
# the row's values (see _fn_filter).

class _Currency(float):
    """Marker for DAX's Fixed Decimal (Currency) type. CURRENCY() returns it;
    ISCURRENCY / ISDECIMAL test for it -- Desktop treats the two names as the
    same underlying type (ISDECIMAL(CURRENCY(1)) is TRUE, ISDECIMAL(1.5) is
    FALSE)."""
    __slots__ = ()


# 1-arg math scalars for the conformance batch; domain errors return BLANK via
# the ValueError guard in _fn_math1.
_MATH1 = {
    'ACOS': math.acos, 'ASIN': math.asin, 'ATAN': math.atan,
    'ACOSH': math.acosh, 'ASINH': math.asinh, 'ATANH': math.atanh,
    'COS': math.cos, 'SIN': math.sin, 'TAN': math.tan,
    'COSH': math.cosh, 'SINH': math.sinh, 'TANH': math.tanh,
    'COT': lambda x: math.cos(x) / math.sin(x),
    'COTH': lambda x: math.cosh(x) / math.sinh(x),
    'ACOT': lambda x: math.pi / 2 - math.atan(x),
    'ACOTH': lambda x: math.atanh(1.0 / x),
    'DEGREES': math.degrees, 'RADIANS': math.radians,
    'SQRTPI': lambda x: math.sqrt(x * math.pi),
}

# Calls that AGGREGATE over the context inside a FILTER predicate -- these keep
# the filter-context path and must NOT be row-substituted. RELATED /
# RELATEDTABLE are deliberately EXCLUDED: they are row-context NAVIGATIONS (the
# opposite of an aggregation), so a FILTER predicate using them needs the
# current row bound, not the filter-context fallback (findings #20 -- otherwise
# FILTER(Sales, RELATED(Products[X]) = v) resolves RELATED to the first visible
# related row for every row).
_AGG_CALL_RE = re.compile(
    r"\b(SUM|SUMX|AVERAGE|AVERAGEX|MIN|MINX|MAX|MAXX|COUNT|COUNTX|COUNTA|"
    r"COUNTROWS|COUNTBLANK|DISTINCTCOUNT|MEDIAN|MEDIANX|PRODUCT|PRODUCTX|"
    r"STDEV\.[SP]|VAR\.[SP]|RANKX|CALCULATE)\s*\(",
    re.IGNORECASE,
)


# Only an ISO-ish date shape may be read as a number by _as_number; a plain
# word must stay non-numeric.
# Fractional seconds included: the row-context evaluator substitutes dates
# with datetime.isoformat(), which keeps microseconds. Omitting them made
# every microsecond-precision timestamp non-numeric, so `[end] * 86400000`
# came out BLANK on all 117 rows of a real trace table.
_ISO_DATEISH = re.compile(
    r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?$")


_DAX_EPOCH_DT = datetime(1899, 12, 30)


def _dax_serial(dt: datetime) -> float:
    """A datetime as its DAX serial, CORRECTLY ROUNDED.

    The obvious `(dt - epoch).total_seconds() / 86400.0` rounds TWICE -- once
    building the seconds float, once dividing -- and lands on the wrong double
    for 27% of microsecond-precision timestamps (54,550 of 200,000 random
    instants). Integer microseconds divided by an integer is rounded once, and
    Python's int/int is correctly rounded, so this is the nearest double to the
    true value -- which is the one Desktop stores.

    It matters because the serial is routinely scaled back up:
    MS_Perf_Analyzer's `[start] * 86400000` turned a 1-ULP serial error into
    0.0005 ms, and `([end] - [start]) * 86400000` cancelled the two into
    99.99945759773254 against Desktop's 100.00008624047041 -- a visible,
    wrong duration.
    """
    exact = getattr(dt, 'oa_serial', None)
    if isinstance(exact, float):
        # The decoder kept the ORIGINAL stored double (see
        # vertipaq_decoder.DAXDateTime): .NET ticks are 100 ns and Python's
        # datetime resolves to 1 us, so reconstructing the serial from the
        # datetime cannot round-trip a sub-microsecond timestamp at all.
        return exact
    d = dt - _DAX_EPOCH_DT
    us = d.days * 86_400_000_000 + d.seconds * 1_000_000 + d.microseconds
    return us / 86_400_000_000


def _as_number(v):
    """Best-effort numeric coercion; None when the value isn't numeric.

    A DATE is a number in DAX -- days since 1899-12-30, with the time of day as
    the fraction -- which is what makes the common `[Timestamp] * 86400000`
    milliseconds idiom work. Without this, dates coerced to nothing and every
    such column silently materialised BLANK or 0 instead of a value.

    Dates also arrive here as ISO STRINGS: the calculated-column evaluator
    substitutes a row's date as a quoted literal, since bare
    `2024-01-15 00:00:00` is not parseable DAX.
    """
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, datetime):
        return _dax_serial(v)
    if isinstance(v, date):
        return float((v - date(1899, 12, 30)).days)
    if isinstance(v, str):
        s = v.strip()
        try:
            return float(s)
        except ValueError:
            pass
        # Only ISO-ish shapes, so an ordinary word is still "not a number".
        return _iso_serial(s)
    return None


@lru_cache(maxsize=100_000)
def _iso_serial(s: str):
    """OA serial for an ISO-ish date string, or None. Cached.

    The row-context evaluator substitutes every date as an ISO string, so a
    single expression over a wide table re-parses the same handful of dates
    millions of times: `[date] - [HireDate]` on a 1.29M-row table is ~2.6M
    strptime calls for a few thousand distinct dates. Caching turns almost all
    of them into a dict hit. Pure function of the string, so the cache is safe.
    """
    if not _ISO_DATEISH.match(s):
        return None
    got = _as_datetime(s)
    if got is None:
        return None
    return _dax_serial(got)


# A whole expression that is exactly one DAX string literal. Interior quotes
# must be DOUBLED, which is how DAX escapes them.
_FULL_STRING_LITERAL = re.compile(r'^"(?:[^"]|"")*"$')
_STRING_LIT_RE = re.compile(r'"(?:[^"]|"")*"')

# `'Online Sales'[Purchase date].[Date]` -- the auto date/time hierarchy
# accessor. Resolved through the relationship to the hidden LocalDateTable, see
# DAXEngine._expand_variation_refs.
_VARIATION_REF_RE = re.compile(
    r"(?:'([^']+)'|\b([A-Za-z_][\w ]*))\s*\[([^\]]+)\]\s*\.\s*\[([^\]]+)\]")


_DAX_NUM_CTX = decimal.Context(prec=15, rounding=decimal.ROUND_HALF_EVEN)


def _dax_number_str(v: float) -> str:
    """Render a float the way DAX's implicit string conversion does.

    Python's ``str`` prints the shortest round-trippable form (up to 17
    significant digits); DAX prints at most 15 and then decides between fixed
    and scientific notation by WIDTH, not by exponent alone. Both halves were
    read off the live Desktop engine:

        1/3          -> 0.333333333333333     (15 sig digits, fixed)
        1561.09*1    -> 1561.09               (not 1561.0900000000001)
        1/30         -> 3.33333333333333E-02  (16 decimals needed -> scientific)
        1.234567E-9  -> 0.000000001234567     (15 decimals -> still fixed)
        1.2345678E-9 -> 1.2345678E-09         (16 -> scientific)
        5E-15        -> 0.000000000000005     boundary, fixed
        5E-16        -> 5E-16                 boundary, scientific
        123456789012345 -> 123456789012345    (15 integer digits, fixed)
        1E+15        -> 1E+15                 (16 -> scientific)
        4.0          -> 4                     0*-1.0 -> -0

    So: round to 15 significant digits, then use fixed notation iff it needs at
    most 15 decimal places AND at most 15 integer digits. This matters well
    beyond cosmetics -- the corpus's SVG and HTML measures paste numbers into
    markup, and Python's extra digits made four of them differ from Desktop by
    exactly the surplus character count.
    """
    if math.isnan(v):
        return '-nan(ind)'               # what Desktop's `&` produces
    if math.isinf(v):
        return 'inf' if v > 0 else '-inf'
    if v == 0:
        return '-0' if math.copysign(1.0, v) < 0 else '0'
    d = _DAX_NUM_CTX.create_decimal(repr(v)).normalize(_DAX_NUM_CTX)
    nsig = len(d.as_tuple().digits)
    adj = d.adjusted()                   # floor(log10(|value|))
    decimals = max(0, nsig - 1 - adj)
    int_digits = adj + 1 if adj >= 0 else 1
    if decimals <= 15 and int_digits <= 15:
        return format(d, 'f')
    mantissa = format(d.scaleb(-adj, _DAX_NUM_CTX), 'f')
    return f"{mantissa}E{'+' if adj >= 0 else '-'}{abs(adj):02d}"


_DAX_EPOCH = date(1899, 12, 30)          # DAX serial 0


def _dax_time_str(t) -> str:
    """A time as DAX renders it: 12-hour, no leading hour zero, AM/PM."""
    if t is None:
        return '12:00:00 AM'
    hour12 = t.hour % 12 or 12
    return f"{hour12}:{t.minute:02d}:{t.second:02d} " \
           f"{'AM' if t.hour < 12 else 'PM'}"


def _dax_datetime_str(v) -> str:
    """Render a date/datetime the way DAX's `&` operator does.

    Verified against Desktop:
        DATE(2025,7,1)          -> 7/1/2025             (no leading zeros)
        DATE(2025,12,25)        -> 12/25/2025
        DATE(2025,7,1) + 0.5    -> 7/1/2025 12:00:00 PM
        DATE(2025,7,1) + 0.25   -> 7/1/2025 6:00:00 AM
        DATE(1899,12,30)        -> 12:00:00 AM          (serial 0 is a TIME)
        TIME(13,5,9)            -> 1:05:09 PM

    Python's str() gives "2025-07-01 00:00:00", which is why
    Ecommerce_Conversion's [Date Range Selected Period] read
    "2025-10-01 00:00:00 - 2025-10-04 00:00:00" where Desktop shows
    "10/1/2025 - 10/4/2025".
    """
    if isinstance(v, datetime):
        dpart, tpart = v.date(), v.time()
    else:
        dpart, tpart = v, None
    if dpart == _DAX_EPOCH:
        # A serial below 1 carries no date, so Desktop prints the time alone.
        return _dax_time_str(tpart)
    out = f"{dpart.month}/{dpart.day}/{dpart.year}"
    if tpart and (tpart.hour or tpart.minute or tpart.second):
        out += ' ' + _dax_time_str(tpart)
    return out


_FMT_RUN_RE = re.compile(r'[#0][#0,.]*')

# FORMAT's named formats, as the ones Power BI itself generates.
_NAMED_FORMATS = {
    'general number': '',
    # en-US, as every entry here: Desktop 2.157 renders FORMAT(-0.4, "Currency") as
    # "($0.40)" in an en-US model, so the negative section is parenthesised.
    'currency': '$#,##0.00;($#,##0.00)',
    'fixed': '0.00',
    'standard': '#,##0.00',
    'percent': '0.00%',
    'scientific': '0.00E+00',
}


#: Decimal/group separators by culture, for FORMAT() (issue #60). Power BI
#: renders a FORMAT() result as a STRING the canvas prints verbatim, so its
#: separators come from the MODEL's culture, not the reader's host.
#:
#: Keyed by the language part, with a few region overrides in front. Only the
#: separators are modelled: month and day NAMES still render en-US, which is
#: recorded as a known limit rather than guessed at.
_CULTURE_SEPS_BY_REGION = {
    # Spanish-speaking regions that use the en-US convention
    "es-mx": (",", "."), "es-pa": (",", "."), "es-pr": (",", "."),
    "es-do": (",", "."), "es-ni": (",", "."), "es-gt": (",", "."),
    "es-hn": (",", "."), "es-sv": (",", "."), "es-pe": (",", "."),
    # French Switzerland groups with an apostrophe
    "fr-ch": ("'", "."), "de-ch": ("'", "."), "it-ch": ("'", "."),
    "en-za": (" ", ","), "en-in": (",", "."),
}

_CULTURE_SEPS_BY_LANG = {
    "en": (",", "."), "ja": (",", "."), "zh": (",", "."), "ko": (",", "."),
    "he": (",", "."), "th": (",", "."), "ga": (",", "."), "ms": (",", "."),
    "pt": (".", ","), "es": (".", ","), "it": (".", ","), "de": (".", ","),
    "nl": (".", ","), "da": (".", ","), "id": (".", ","), "tr": (".", ","),
    "vi": (".", ","), "el": (".", ","), "ro": (".", ","), "ca": (".", ","),
    "sl": (".", ","), "hr": (".", ","), "sr": (".", ","), "bs": (".", ","),
    "mk": (".", ","), "sq": (".", ","), "is": (".", ","),
    "fr": (" ", ","), "ru": (" ", ","), "uk": (" ", ","), "pl": (" ", ","),
    "cs": (" ", ","), "sk": (" ", ","), "hu": (" ", ","), "sv": (" ", ","),
    "nb": (" ", ","), "no": (" ", ","), "nn": (" ", ","), "fi": (" ", ","),
    "et": (" ", ","), "lv": (" ", ","), "lt": (" ", ","), "bg": (" ", ","),
    "af": (" ", ","), "kk": (" ", ","),
}


def _culture_seps(culture):
    """``(group_sep, decimal_sep)`` for a culture name; en-US when unknown.

    Unknown cultures deliberately fall back to en-US rather than guessing:
    a wrong separator is indistinguishable from a right one in the output.
    """
    if not culture:
        return (",", ".")
    key = str(culture).strip().lower().replace("_", "-")
    if key in _CULTURE_SEPS_BY_REGION:
        return _CULTURE_SEPS_BY_REGION[key]
    return _CULTURE_SEPS_BY_LANG.get(key.split("-")[0], (",", "."))


_CURRENCY_CHARS = "$€£¥"


def _arith_text_number(s: str, culture=None):
    """Text in arithmetic as Desktop's formula engine converts it, where
    _as_number gives up: the culture's group separator and a currency symbol
    are accepted, and a non-ISO date follows the culture's day/month order.
    Desktop 2.152 (en-US model): "3,5" + 0 is 35, "$3" + 0 is 3, and
    "01/02/2024" + 1 is 45294 (2 January). None for text that is no number
    (issue #133)."""
    t = s.strip()
    if not t:
        return None
    group, dec = _culture_seps(culture)
    u = t
    if u[:1] in _CURRENCY_CHARS:
        u = u[1:].strip()
    elif u[-1:] in _CURRENCY_CHARS:
        u = u[:-1].strip()
    u = u.replace(group, '').replace(' ', '') if group else u
    if dec != '.':
        u = u.replace(dec, '.')
    try:
        return float(u)
    except ValueError:
        pass
    m = re.fullmatch(r'(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{4})', t)
    if m:
        a, b, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        key = str(culture or 'en-US').lower().replace('_', '-')
        month, day = (a, b) if key in ('en-us', 'en') else (b, a)
        try:
            return float((date(y, month, day) - date(1899, 12, 30)).days)
        except ValueError:
            return None
    return None


def _fmt_split_sections(fmt: str) -> list:
    """Split a format picture on `;` that are OUTSIDE quoted literals."""
    out, buf, in_q = [], [], False
    i, n = 0, len(fmt)
    while i < n:
        ch = fmt[i]
        if ch == '"':
            in_q = not in_q
            buf.append(ch)
        elif ch == ";" and not in_q:
            out.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    out.append("".join(buf))
    return out


def _fmt_mask_quoted(section: str) -> str:
    """`section` with every quoted literal blanked to a neutral filler.

    The digit-run search must not see placeholders that live INSIDE a quoted
    literal: `"0 items"#,##0` has a `0` in its prefix text. Masking to a
    character that is not a placeholder keeps every index aligned with the
    original string, so prefix/suffix can be sliced back out of it.
    """
    out, in_q = [], False
    for ch in section:
        if ch == '"':
            in_q = not in_q
            out.append("\x00")
            continue
        out.append("\x00" if in_q else ch)
    return "".join(out)


def _fmt_unquote(text: str) -> str:
    """Drop the quote CHARACTERS of a format picture's quoted literals.

    VBA/DAX format pictures escape literal text by quoting it, and Desktop
    emits the contents without the quotes: `FORMAT(1234.5, "\"$ \"#,##0")` is
    `$ 1,235`. Printing the quotes verbatim (issue #58) is what made every
    carried-over currency picture render wrongly.
    """
    return text.replace('"', "")


def _format_number(val: float, fmt: str, group_sep: str = ",",
                   dec_sep: str = "."):
    """FORMAT() for a numeric custom format string. None = not understood.

    Implements the parts of the VB/Excel numeric format that Power BI's own
    generated measures actually use:

      * `;`-separated sections -- positive;negative;zero. The negative section
        carries its own sign, so the value is formatted from its magnitude.
      * a `,` BETWEEN digit placeholders turns on thousands grouping;
        a `,` immediately before the decimal point (or at the end of the digit
        run) SCALES the value down by 1000 for each such comma. This is the one
        that mattered: `FORMAT(2297200.9, "$#,##0,.0K")` is "$2,297.2K" in
        Desktop, and reading the comma as grouping produced "$2,297,200.90K" --
        which is how two GeoSales_Dashboard SVG measures came out longer than
        Desktop's by exactly the surplus digits.
      * `%` scales by 100 and stays in the output.
      * `0` is a required digit (FORMAT(1,"000") is "001"), `#` an optional one,
        so trailing `#` decimals drop rather than pad.
      * anything outside the digit run is a literal prefix/suffix.
    """
    named = _NAMED_FORMATS.get(fmt.strip().lower())
    if named is not None:
        if named == '':
            return _dax_number_str(val)
        fmt = named
    # `;` inside a quoted literal is text, not a section break.
    sections = _fmt_split_sections(fmt)
    if val < 0 and len(sections) > 1 and sections[1]:
        section, explicit_sign = sections[1], True
        val = abs(val)
    elif val == 0 and len(sections) > 2 and sections[2]:
        section, explicit_sign = sections[2], True
    else:
        section, explicit_sign = sections[0], False
    # Search the MASKED section so a digit inside a quoted literal is not
    # mistaken for a placeholder, then slice the ORIGINAL by the same indices.
    m = _FMT_RUN_RE.search(_fmt_mask_quoted(section))
    if not m:
        # A section with no digit placeholder is a pure literal, which is how
        # the zero section is normally written: Desktop renders
        # FORMAT(0, "0.0;(0.0);zero") as "zero".
        return _fmt_unquote(section) if explicit_sign else None
    prefix, run = section[:m.start()], section[m.start():m.end()]
    suffix = section[m.end():]
    if '%' in prefix or '%' in suffix:
        val *= 100
    int_pat, _dot, dec_pat = run.partition('.')
    # Trailing commas on the integer pattern are scaling, not grouping.
    scale = 0
    while int_pat.endswith(','):
        int_pat = int_pat[:-1]
        scale += 1
    if scale:
        val /= 1000 ** scale
    grouping = ',' in int_pat
    int_pat = int_pat.replace(',', '')
    max_dec = len(dec_pat)
    min_dec = dec_pat.count('0')
    min_int = int_pat.count('0')

    neg = val < 0
    # HALF AWAY FROM ZERO, not Python's banker's rounding. Desktop:
    #   FORMAT(1234.5, "#,##0") -> 1,235   (banker's would give 1,234)
    #   FORMAT(0.125,  "0.00")  -> 0.13    (banker's would give 0.12)
    rounded = decimal.Decimal(repr(abs(val))).quantize(
        decimal.Decimal(1).scaleb(-max_dec), rounding=decimal.ROUND_HALF_UP)
    if not rounded and val:
        # A value that ROUNDS to zero is formatted as zero: no sign, no negative
        # section, and the zero section when there is one. Desktop 2.157:
        #   FORMAT(-0.004, "$#,##0")                  -> "$0"   (not "-$0")
        #   FORMAT(-0.004, "$#,##0;($#,##0)")         -> "$0"   (not "($0)")
        #   FORMAT(-0.004, "$#,##0;($#,##0);\"zero\"")  -> "zero"
        #   FORMAT( 0.004, "$#,##0;($#,##0);\"zero\"")  -> "zero"
        #   FORMAT(-0.004, "0.0%")                    -> "-0.4%" (does not round to zero)
        return _format_number(0.0, fmt, group_sep, dec_sep)
    body = f"{rounded:,.{max_dec}f}" if grouping else f"{rounded:.{max_dec}f}"
    int_part, _d, dec_part = body.partition('.')
    # `#` decimals are optional: drop trailing zeros down to the required count.
    while len(dec_part) > min_dec and dec_part.endswith('0'):
        dec_part = dec_part[:-1]
    digits = int_part.replace(',', '')
    if len(digits) < min_int:
        digits = '0' * (min_int - len(digits)) + digits
        int_part = f"{int(digits):,}" if grouping else digits
    elif not min_int and digits == '0':
        # "#.##" shows ".5", not "0.5"; "0.##" keeps the leading zero.
        int_part = ''
    # A format that HAS a decimal section keeps its separator even when every
    # optional decimal dropped -- Desktop renders FORMAT(2, "0.##") as "2.".
    tail = ('.' + dec_part) if (max_dec and (dec_part or _dot)) else ''
    # Re-separate for the model's culture (issue #60). The body was built with
    # the en-US pair, so swap through a placeholder to avoid clobbering.
    if group_sep != ',' or dec_sep != '.':
        int_part = int_part.replace(',', '\x00')
        tail = tail.replace('.', dec_sep, 1) if tail else tail
        int_part = int_part.replace('\x00', group_sep)
    out = _fmt_unquote(prefix) + int_part + tail + _fmt_unquote(suffix)
    if neg and not explicit_sign:
        out = '-' + out
    return out


def _scalarize(v):
    """A ONE-ROW, ONE-COLUMN table is implicitly a scalar in DAX.

    `last year = LASTDATE('Year'[Date])` is a measure, so it must evaluate to a
    value; Desktop renders it as the date (8 characters). This engine returned
    the internal row-dict list, whose str() is 72 characters of Python repr --
    the kind of leak that shows up in a report as literal
    "[{'__table__': ...}]" text.
    """
    if (isinstance(v, list) and len(v) == 1 and isinstance(v[0], dict)
            and '__value__' in v[0]):
        return v[0]['__value__']
    return v


def _concat_str(v):
    """Render a value for the DAX `&` operator.

    `str(v or '')` dropped every FALSY value, so `"0" & 0` produced "0" instead
    of "00" -- and the zero-padding idiom RIGHT("0" & n, 2) silently lost its
    pad on exactly the rows where n was 0. Only BLANK renders as empty.
    Numbers go through _dax_number_str, which reproduces Desktop's 15-digit
    fixed/scientific choice instead of Python's 17-digit repr.
    """
    v = _scalarize(v)
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, float):
        return _dax_number_str(v)
    if isinstance(v, (datetime, date)):
        return _dax_datetime_str(v)
    return str(v)


# ---------------------------------------------------------------------------
# Compiled expression plans (issue #6)
# ---------------------------------------------------------------------------
# _eval_expr used to re-run its whole dispatch chain -- comment stripping,
# literal regexes, operator splitting, branch checks -- on every call, and
# iterators call it once per row WITH THE SAME TEXT. Profiling one
# RANKX/FILTER measure showed the interpreter's string analysis dominating the
# runtime. The analysis is a pure function of the expression text, so it is
# done ONCE here and cached as a "plan": an ordered tuple of steps, almost
# always a single terminal one. Evaluation code is unchanged -- the plan only
# records WHICH branch of the old chain applies, plus its precomputed splits.
#
# Two branches are decided at runtime, not analysis, and stay conditional:
#   * a bare identifier may be a VAR in the current scope, else it falls
#     through to the bare-table tail;
#   * a syntactically-matched comparison can evaluate to None (a blank side),
#     and the old chain then FELL THROUGH to concat/minus/function -- so the
#     plan keeps a fallthrough step after the comparison.
_P_NONE = 0       # empty expression -> BLANK
_P_VARRET = 1     # VAR ... RETURN block
_P_PAREN = 2      # ( inner )
_P_CONST = 3      # string/number/bool literal, value precomputed
_P_MAYBEVAR = 4   # bare identifier: var_scope hit or fall through
_P_BRACKET1 = 5   # [Name] with exactly one bracket pair
_P_TCOL = 6       # Table[Column]
_P_BRACKET2 = 7   # [Name], the permissive late variant
_P_NOT = 8        # NOT <inner>
_P_LOGICAL = 9    # || / && with precomputed parts
_P_BINARY = 10    # + - * / with precomputed parts
_P_CMP = 11       # comparison chain; may return None -> fall through
_P_CONCAT = 12    # & with precomputed parts
_P_NEG = 13       # unary minus
_P_FUNC = 14      # FUNC(args), name + args text precomputed
_P_TAIL = 15      # bare table name / final BLANK
_P_IN = 16        # <scalar> IN <set>; may return None -> fall through
_P_TABLECTOR = 17  # { a, b, c } -> one-column table, column named Value

_PLAN_CACHE: dict = {}

_SCI_NUM_RE = re.compile(r'^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$')
_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_BRACKET2_RE = re.compile(r'^\[[^\]]+\]$')
_NOT_PREFIX_RE = re.compile(r'(?i)^not\s+(.+)$')
# Dots are legal inside DAX function names (STDEV.S, T.DIST.2T,
# NORM.S.INV) -- the old \w* pattern matched only the prefix, so every
# dotted function silently fell through to the unsupported path.
_FUNC_CALL_RE = re.compile(r'([A-Za-z_][\w.]*)\s*\(')
_TCOL_RE = re.compile(r"(?:'([^'\[\]]+)'|([^\W\d][\w .]*))\s*\[([^\]]+)\]$")
# The same shape, anchored at BOTH ends. ALL/ALLSELECTED matched their argument
# with an unanchored pattern, which quietly accepted only the first column of a
# multi-column call and only the base column of `T[C].[Part]`.
_WHOLE_TCOL_RE = re.compile(
    r"^\s*(?:'([^'\[\]]+)'|([^\W\d][\w .]*))\s*\[([^\]]+)\]\s*$")
# CALCULATE's modifier arguments (issue #87), and the subset that REMOVES filters.
_CALC_MODIFIER_RE = re.compile(
    r"(?is)^\s*(?:ALL|ALLSELECTED|ALLEXCEPT|ALLNOBLANKROW|ALLCROSSFILTERED|"
    r"REMOVEFILTERS|USERELATIONSHIP|CROSSFILTER)\s*\(")
_CALC_REMOVE_RE = re.compile(
    r"(?is)^\s*(?:ALL|ALLSELECTED|ALLEXCEPT|ALLNOBLANKROW|ALLCROSSFILTERED|"
    r"REMOVEFILTERS)\s*\(")
_CALC_PRED_RE = re.compile(
    r"^'?([^'\[\]]+?)'?\s*\[([^\]]+)\]\s*(<>|>=|<=|>|<|=)\s*(.+)$", re.S)
# Literal-first CALCULATE predicate: ``2024 = T[Year]`` (issue #39). The left
# side may not contain brackets, so a column-vs-column or measure comparison
# never matches; the column ref is anchored at the end.
_CALC_PRED_REV_RE = re.compile(
    r"^([^\[\]]+?)\s*(<>|>=|<=|>|<|=)\s*'?([^'\[\]]+?)'?\s*\[([^\]]+)\]$", re.S)
_VAR_KW_RE = re.compile(r'\bVAR\b', re.IGNORECASE)
_RETURN_KW_RE = re.compile(r'\bRETURN\b', re.IGNORECASE)


def _strip_line_comments(expr):
    """Strip // and -- comments, respecting string literals.

    The string state is tracked across the WHOLE expression, not per line. It
    used to reset on every newline, so a multi-line string literal lost its
    quoting and any ``--`` inside it was taken for a comment: an SVG measure
    containing ``<!-- Data -->`` came back as ``<!`` with the rest of the line
    eaten. That is the shape every measure in docs/rich-content.md's SVG rail
    has, and Desktop of course returns the comment intact.

    Newlines INSIDE a string literal are preserved for the same reason -- they
    are part of the value. Outside a string they become spaces, as before, since
    DAX is whitespace-insensitive there.
    """
    # Segments alternate between outside-string and inside-string text so the
    # whitespace collapse can be applied to the former ONLY. Collapsing runs of
    # spaces everywhere flattened the indentation of an SVG literal and made the
    # value 32 characters shorter than Desktop's.
    segs: list = []          # (text, is_string)
    buf: list = []
    in_str = False
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if in_str:
            if ch == '"':
                # "" is an escaped quote inside a DAX string, not a terminator.
                if i + 1 < n and expr[i + 1] == '"':
                    buf.append('""')
                    i += 2
                    continue
                buf.append(ch)
                segs.append((''.join(buf), True))
                buf = []
                in_str = False
                i += 1
                continue
            buf.append(ch)
            i += 1
            continue
        if ch == '"':
            segs.append((''.join(buf), False))
            buf = [ch]
            in_str = True
            i += 1
            continue
        if expr[i:i + 2] in ('//', '--'):
            j = expr.find('\n', i)
            if j < 0:
                break
            i = j          # leave the newline; it becomes the line separator
            continue
        buf.append(' ' if ch == '\n' else ch)
        i += 1
    segs.append((''.join(buf), in_str))
    parts = [t if is_s else re.sub(r'[ \t]+', ' ', t) for t, is_s in segs]
    return ''.join(parts).strip()


def _collapse_ws_outside_strings(expr: str) -> str:
    r"""Collapse runs of whitespace to one space, but NEVER inside a string
    literal. A blanket re.sub(r'\s+', ' ') flattened the newlines and
    indentation of an SVG literal, so a measure that Desktop returns as 665
    characters came back as 633 with every line joined.
    """
    out = []
    in_str = False
    i = 0
    n = len(expr)
    while i < n:
        ch = expr[i]
        if in_str:
            if ch == '"':
                if i + 1 < n and expr[i + 1] == '"':
                    out.append('""')
                    i += 2
                    continue
                in_str = False
            out.append(ch)
            i += 1
            continue
        if ch == '"':
            in_str = True
            out.append(ch)
            i += 1
            continue
        if ch.isspace():
            if out and out[-1] != ' ':
                out.append(' ')
            i += 1
            continue
        out.append(ch)
        i += 1
    return ''.join(out).strip()


def _analyze_expr(raw):
    """Pure syntactic analysis of one expression -> a plan (tuple of steps).

    Mirrors the old _eval_expr dispatch chain exactly, in the same order; every
    check here depends only on the text, never on the context. Runtime-dependent
    decisions become CONDITIONAL steps followed by their fallthrough.
    """
    expr = raw.strip()
    if not expr:
        return ((_P_NONE, None),)
    expr = _strip_line_comments(expr)
    if not expr:
        return ((_P_NONE, None),)

    # ''[Value] -- an EMPTY table qualifier. Power BI's own auto-generated
    # measures use it to read the implicit [Value] column of a table constructor
    # (see _ShowValueForDates: MAXX({ MAX(T[C]) }, ''[Value])). An empty table
    # name matches nothing, so strip the qualifier and let the bare-bracket path
    # read the column out of the current row.
    if "''[" in expr:
        expr = expr.replace("''[", "[")

    if _VAR_KW_RE.search(expr) and _RETURN_KW_RE.search(expr):
        return ((_P_VARRET, expr),)

    # A table constructor: { expr, expr, ... } -> a one-column table whose column
    # is called Value, which is the name DAX gives it.
    if expr.startswith('{') and expr.endswith('}'):
        depth = 0
        wraps_all = True
        for i, ch in enumerate(expr):
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
            if depth == 0 and i < len(expr) - 1:
                wraps_all = False
                break
        if wraps_all:
            return ((_P_TABLECTOR, expr[1:-1].strip()),)

    if expr.startswith('(') and expr.endswith(')'):
        depth = 0
        wraps_all = True
        for i, ch in enumerate(expr):
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
            if depth == 0 and i < len(expr) - 1:
                wraps_all = False
                break
        if wraps_all:
            return ((_P_PAREN, expr[1:-1].strip()),)

    if _FULL_STRING_LITERAL.match(expr):
        return ((_P_CONST, expr[1:-1].replace('""', '"')),)

    try:
        if _SCI_NUM_RE.match(expr):
            return ((_P_CONST, float(expr)),)
        if '.' in expr:
            return ((_P_CONST, float(expr)),)
        return ((_P_CONST, int(expr)),)
    except ValueError:
        pass

    if expr.upper() == 'TRUE':
        return ((_P_CONST, True),)
    if expr.upper() == 'FALSE':
        return ((_P_CONST, False),)

    if _IDENT_RE.match(expr):
        # A bare identifier: only a var-scope hit or the bare-table tail can
        # resolve it -- no later branch of the chain matches an identifier.
        return ((_P_MAYBEVAR, expr), (_P_TAIL, expr))

    if (expr.startswith('[') and expr.endswith(']')
            and expr.count('[') == 1 and expr.count(']') == 1):
        return ((_P_BRACKET1, expr[1:-1]),)

    # NOTE the old chain also demanded `'(' not in expr` here. The regex is
    # anchored at both ends, so when it matches, ANY paren is inside the quoted
    # table name or the bracketed column name -- both legal. The guard rejected
    # every reference to a column like [People using ... (% of population)],
    # which then fell through to the bare-table tail and read as BLANK: a
    # silently wrong value on real Microsoft sample files.
    col_match = _TCOL_RE.match(expr)
    if col_match and col_match.end() == len(expr):
        return ((_P_TCOL, ((col_match.group(1) or col_match.group(2)).strip(),
                           col_match.group(3).strip())),)

    if _BRACKET2_RE.match(expr):
        return ((_P_BRACKET2, expr[1:-1]),)

    # Top-level split order IS operator precedence: split at the LOOSEST
    # operator first, so it becomes the root of the parse. The old chain split
    # arithmetic before comparison and before `&`, which mis-parsed
    # `a - b < 0` as `a - (b < 0)` -- the blank comparison then became 0, the
    # whole condition collapsed to `a`, always truthy, and Employee[TenureDays]
    # materialized SIGN-FLIPPED on 1.25M rows. Verified against Power BI
    # Desktop's own engine, loosest to tightest:
    #     ||   &&   NOT   comparisons   &   + -   * /
    #     (10 - 3 < 0 is FALSE;  "a" & 1 + 2 is "a3";  "x" & 2 < 1 errors,
    #      so & binds tighter than a comparison;  NOT FALSE() && FALSE() is
    #      FALSE, so NOT binds tighter than &&;  NOT 1 = 2 is TRUE, so NOT
    #      binds looser than a comparison.)
    for lop in ('||', '&&'):
        parts = DAXEngine._split_operators_scan(expr, lop)
        if len(parts) > 1:
            return ((_P_LOGICAL, (lop, tuple(p.strip() for p in parts))),)

    m = _NOT_PREFIX_RE.match(expr)
    if m:
        return ((_P_NOT, m.group(1).strip()),)

    steps = []
    # IN is a comparison-class operator and, like the others, is conditional: an
    # unparseable set falls through rather than inventing an answer. Checked
    # before the symbolic comparisons because `x IN {1,2}` contains none of them,
    # while `NOT x IN {...}` has already been peeled by the NOT branch above
    # (DAX: NOT binds looser than a comparison).
    #
    # This was held back for one release. Enabling it while RANKX still returned
    # dead-last for a non-member value turned seven Agents_Performance measures
    # from BLANK into confidently WRONG values. It is on now because the chain
    # underneath -- DATEADD's month shift, RANKX's competition ranking, date-aware
    # filter matching -- matches Desktop measure for measure.
    in_parts = DAXEngine._split_in_scan(expr)
    if in_parts:
        steps.append((_P_IN, in_parts))

    for op in ('==', '<>', '>=', '<=', '>', '<', '='):
        if len(DAXEngine._split_operators_scan(expr, op)) == 2:
            # Conditional: a comparison with a non-comparable side evaluates
            # to None at runtime and falls through to the next-tighter level.
            steps.append((_P_CMP, expr))
            break

    if '&' in expr:
        parts = DAXEngine._split_toplevel_scan(expr, '&')
        if len(parts) > 1:
            steps.append((_P_CONCAT, tuple(p.strip() for p in parts)))
            return tuple(steps)

    for op in ('+', '-'):
        parts = DAXEngine._split_operators_scan(expr, op)
        if len(parts) > 1:
            steps.append((_P_BINARY, (op, tuple(p.strip() for p in parts))))
            return tuple(steps)
    for op in ('*', '/'):
        parts = DAXEngine._split_operators_scan(expr, op)
        if len(parts) > 1:
            steps.append((_P_BINARY, (op, tuple(p.strip() for p in parts))))
            return tuple(steps)

    if expr.startswith('-'):
        steps.append((_P_NEG, expr[1:].strip()))
        return tuple(steps)

    fm = _FUNC_CALL_RE.match(expr)
    if fm:
        args_str = DAXEngine._extract_args_scan(expr[fm.end() - 1:])
        if args_str is not None:
            steps.append((_P_FUNC, (fm.group(1).upper(), args_str[1:-1])))
            return tuple(steps)

    steps.append((_P_TAIL, expr))
    return tuple(steps)


@lru_cache(maxsize=100_000)
def _as_date_str(s: str):
    """The strptime half of _as_date, memoized.

    Date filters carry the same few hundred date strings over and over -- a
    DATEADD range is ~1000 values and every filter application re-parsed all of
    them. strptime dominated the per-row profile of an iterator over a dimension
    (2455 calls, the single largest cost after the index union).
    """
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s[:len(fmt) + 2].strip(), fmt).date()
        except ValueError:
            continue
    return None


def _as_date(v):
    """Best-effort date coercion from a value or ISO-ish string."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return _as_date_str(v.strip())
    return None


@lru_cache(maxsize=100_000)
def _as_date_str_strict(s: str):
    """Whole-string date parse: None unless the ENTIRE value is a date.

    _as_date_str parses a PREFIX (s[:len(fmt)+2]) — right for coercing
    datetime-ish strings, wrong for deciding date-EQUALITY classes: compound
    keys like '01/01/2017-CLUSTER 1' and '01/01/2017-CLUSTER 2' both
    prefix-parse to 2017-01-01, so the value-index aliasing merged every
    distinct key sharing a date prefix into ONE bucket and a filtered fact
    table came back UNFILTERED (issue #42)."""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _as_date_strict(v):
    """Date coercion for EQUALITY/ALIAS use: a string counts only when the
    whole value is a date (see _as_date_str_strict)."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        return _as_date_str_strict(v.strip())
    return None


def _as_datetime(v):
    """Best-effort datetime coercion from a value or ISO-ish string."""
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if isinstance(v, str):
        s = v.strip()
        # Fractional seconds FIRST, and matched against the whole string: the
        # `s[:len(fmt) + 2]` truncation below silently discarded microseconds,
        # so a timestamp came back rounded down to the second. On a trace table
        # that turned `[end] * 86400000` into an answer 403 ms adrift of
        # Desktop's on every row -- close enough to look right, and wrong.
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                    "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s[:len(fmt) + 2].strip(), fmt)
            except ValueError:
                continue
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        # A DATE IS A NUMBER in DAX -- the serial day count from 1899-12-30 --
        # and a model can store one as Int64. IT_Support does exactly that:
        # fact_IT_Support[Date] is ExplicitDataType 6, so the engine sees 45292
        # where the date dimension holds a datetime. With no numeric branch here
        # DATEDIFF returned BLANK on all 11,923 rows, which made five measures
        # blank and turned `DATEDIFF(...) <= 3` into a filter that kept every
        # row (BLANK <= 3 is TRUE), so [% Closed Within 3 Days] read 1.0 against
        # Desktop's 0.7987. The fraction is kept, unlike _parse_date's int(),
        # because a serial carries the time of day.
        try:
            return datetime(1899, 12, 30) + timedelta(days=float(v))
        except (ValueError, OverflowError, OSError):
            return None
    return None


def _midnight_date(v):
    """v's date when v is a date, or a date-time AT MIDNIGHT (a string
    included), else None. An exact join may equate a midnight date-time with
    its bare date -- the same moment -- but never another time of that day:
    Power BI Desktop 2.152 joins a 12:30 departure to no row of a date-only
    calendar through a DateAndTime relationship (issue #126)."""
    if isinstance(v, datetime):
        return None if (v.hour or v.minute or v.second or v.microsecond) else v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        m = _as_moment_str(v.strip())
        if m is None or m.hour or m.minute or m.second or m.microsecond:
            return None
        return m.date()
    return None


_DP_PREFIX = 'dp:'


class _DatePartKeys(frozenset):
    """A cross filter's keys across a DatePartOnly relationship: date parts
    ('dp:YYYY-MM-DD', plus 'None' for the blank row). See _date_part_key."""


def _date_part_key(v):
    """A join value's DATE PART, as a JoinOnDateBehavior = DatePartOnly
    relationship compares it -- the one Power BI writes for auto date/time --
    or None when v is no date. Desktop 2.152 joins every time of a day to that
    day's row through it (issue #126)."""
    d = _as_date(v)
    if d is None and isinstance(v, (int, float)) and not isinstance(v, bool):
        dt = _as_datetime(v)
        d = dt.date() if dt is not None else None
    return _DP_PREFIX + d.isoformat() if d is not None else None


def _key_in(v, allowed) -> bool:
    """Does a join cell match a cross filter's key set? By date part across a
    DatePartOnly relationship, by its str() spelling otherwise."""
    if isinstance(allowed, _DatePartKeys):
        k = _date_part_key(v) if v is not None else 'None'
        return (k or str(v)) in allowed
    return str(v) in allowed


def _join_key_aliases(v) -> set:
    """Every spelling a relationship join key can legitimately arrive in.

    A model is free to store the SAME key with different storage types on the
    two sides of a relationship. IT_Support does: dim_Date[Date] holds real
    datetimes while fact_IT_Support[Date] is an Int64 Excel serial, so
    str(dim value) is "2024-01-01 00:00:00" and str(fact value) is "45292" --
    a per-side str() never matched, every dim_Date filter reduced the fact to
    ZERO rows, and measures that should read 11,923 tickets read blank.

    Aliases are only ever expanded on the DIMENSION side, which has few rows;
    the fact side keeps its single str() lookup.
    """
    out = {str(v)}
    if isinstance(v, (datetime, date)) and not isinstance(v, bool):
        dt = v if isinstance(v, datetime) else datetime(v.year, v.month, v.day)
        out.add(dt.isoformat())
        if _midnight_date(dt) is not None:      # never another time of the day (#126)
            out.add(dt.date().isoformat())
        serial = _dax_serial(dt)
        if serial == int(serial):
            out.add(str(int(serial)))
            out.add(str(float(serial)))
    elif isinstance(v, (int, float)) and not isinstance(v, bool):
        dt = _as_datetime(v)
        if dt is not None:
            out.add(str(dt))
            out.add(dt.isoformat())
            if _midnight_date(dt) is not None:
                out.add(dt.date().isoformat())
        if isinstance(v, float) and v == int(v):
            out.add(str(int(v)))
        elif isinstance(v, int):
            out.add(str(float(v)))
    return out


def _blank_zero_of(other):
    """The value BLANK takes on when compared against `other`.

    DAX does not propagate BLANK through a comparison — it coerces it to the
    ZERO of the other operand's type and compares that. Verified against Power
    BI Desktop's engine:

        BLANK() < 50            TRUE     BLANK() = 0                  TRUE
        BLANK() >= 50           FALSE    BLANK() = ""                 TRUE
        BLANK() = FALSE()       TRUE     BLANK() = DATE(1899,12,30)   TRUE
        BLANK() <> ""           FALSE    BLANK() < DATE(2024,1,1)     TRUE

    Returning None instead made every comparison against a blank fall to the
    ELSE branch, so a bucketing expression of the shape
    ``IF(T[x] < 30, 20, IF(T[x] < 45, 30, 80))`` scored every blank row 80
    where Desktop scores it 20 — a plausible number, never an error.
    """
    if isinstance(other, bool):
        return False
    if isinstance(other, datetime):
        return datetime(1899, 12, 30)
    if isinstance(other, date):
        return date(1899, 12, 30)
    if isinstance(other, str):
        return ""
    if isinstance(other, (int, float)):
        return 0
    return None


def _coerce_blanks_for_compare(left, right):
    """Make two operands comparable the way DAX compares them.

    BLANK takes the zero of the other side's type (see _blank_zero_of); two
    blanks compare as equal (0 vs 0), which is what Desktop reports for ``=``,
    ``<=`` and ``>=`` and the negation of for ``<>`` and ``<``.

    A date is also matched against its ISO text. That is not cosmetic: the
    calculated-column evaluator substitutes a row's DateTime as a QUOTED ISO
    string (a bare timestamp is not parseable DAX), while an aggregate over the
    same column is evaluated against the table snapshot and yields a native
    datetime. Comparing the two as str-vs-datetime is never equal, so the
    standard "earliest record" flag ``IF(T[D] = MIN(T[D]), 1, 0)`` scored 0 on
    every row instead of marking the earliest.
    """
    if left is None and right is None:
        return 0, 0
    if left is None:
        return _blank_zero_of(right), right
    if right is None:
        return left, _blank_zero_of(left)
    if isinstance(left, str) != isinstance(right, str):
        text, other = (left, right) if isinstance(left, str) else (right, left)
        if isinstance(other, (datetime, date)):
            as_dt = _as_datetime(text)
            if as_dt is not None:
                other_dt = other if isinstance(other, datetime) else \
                    datetime(other.year, other.month, other.day)
                return (as_dt, other_dt) if isinstance(left, str) \
                    else (other_dt, as_dt)
    # A NUMBER against a DATE: a date IS a number in DAX (the serial day count
    # from 1899-12-30), so a model that stores a date column as Int64 still
    # compares equal to a DATE() literal. IT_Support stores
    # fact_IT_Support[Date] as Int64, and `fact[Date] = DATE(2024,1,1)` matched
    # nothing at all until this. Compared as serials, so the arithmetic stays
    # exact.
    l_isdt = isinstance(left, (datetime, date)) and not isinstance(left, bool)
    r_isdt = isinstance(right, (datetime, date)) and not isinstance(right, bool)
    l_isnum = isinstance(left, (int, float)) and not isinstance(left, bool)
    r_isnum = isinstance(right, (int, float)) and not isinstance(right, bool)
    if l_isdt and r_isnum:
        return _to_serial(left), float(right)
    if r_isdt and l_isnum:
        return float(left), _to_serial(right)
    return left, right


def _to_serial(v) -> float:
    """A date/datetime as its DAX serial (days since 1899-12-30)."""
    dt = v if isinstance(v, datetime) else datetime(v.year, v.month, v.day)
    return _dax_serial(dt)


# ---- Filter values as DAX compares them -------------------------------------
_BLANK_KEY = ('blank',)


@lru_cache(maxsize=100_000)
def _as_moment_str(s: str):
    """A string that is WHOLLY an ISO date or date-time (or one of the strict
    date spellings, see _as_date_str_strict) as a naive datetime, else None."""
    if not s[:1].isdigit():
        return None
    try:
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except ValueError:
        d = _as_date_str_strict(s)
        return datetime(d.year, d.month, d.day) if d is not None else None


def _as_moment(v):
    """A date or date-time as a naive datetime -- a date is midnight -- for
    EQUALITY: two times of one day are two values."""
    if isinstance(v, datetime):
        return v.replace(tzinfo=None)
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if isinstance(v, str):
        return _as_moment_str(v.strip())
    return None


_ASCII_LOWER = str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')


def _column_text_key(s: str) -> str:
    """Text as the STORAGE engine compares a column's values: ASCII letters
    case-insensitively, every other character as it is. Desktop 2.152's own
    import keeps 'Äpfel' / 'äpfel', 'Øl' / 'øl', 'Éa' / 'éa', 'Σίγμα' /
    'σίγμα', 'Дом' / 'дом' and 'Straße' / 'Strasse' as distinct values and
    folds only 'Apple' / 'apple'; a filter on "ÄPFEL" selects 'Äpfel' alone.
    (Expressions compare differently: there "Ä" = "ä" and "ß" = "ss".)"""
    return s.translate(_ASCII_LOWER)


def _value_keys(v, dateish: bool = True) -> tuple:
    """The keys a CELL matches a filter value under, as DAX compares values.

    Text is case-insensitive for ASCII letters -- VertiPaq's collation:
    TREATAS({"north"}, T[Region]) selects "North" (see _column_text_key) --
    and it stays TEXT: '01' and '1' are two values, not one number. Numbers
    compare by value whatever their storage (int / float, issue #39). Dates
    and date-times are MOMENTS: 09:00 and 17:00 on one day are two values, and
    a date is midnight. With ``dateish`` (a column that keeps dates as text) a
    string that is wholly an ISO date or date-time is also its moment.
    """
    if v is None:
        return (_BLANK_KEY,)
    if isinstance(v, bool):
        return (('bool', v),)
    if isinstance(v, (int, float, decimal.Decimal)):
        return (('num', float(v)),)
    if isinstance(v, (datetime, date)):
        return (('moment', _as_moment(v)),)
    if isinstance(v, str):
        k = ('text', _column_text_key(v))
        m = _as_moment_str(v.strip()) if dateish else None
        return (k, ('moment', m)) if m is not None else (k,)
    return (('text', _column_text_key(str(v))),)


def _filter_value_keys(v) -> frozenset:
    """The cell keys (see _value_keys) one FILTER value selects.

    A caller spells a value either way -- a Double column filtered by '2024',
    a text code by 7 -- so a number also selects its text spelling, a numeric
    string its number, and 'true' / 'false' the boolean. 'None' is the
    engine's own spelling of BLANK in internal filter sets. Never another time
    of the same day, and never '01' for '1'.
    """
    keys = set(_value_keys(v))
    if isinstance(v, str):
        s = v.strip()
        if v == 'None':
            keys.add(_BLANK_KEY)
        low = s.lower()
        if low in ('true', 'false'):
            keys.add(('bool', low == 'true'))
        try:
            keys.add(('num', float(s)))
        except ValueError:
            pass
    elif isinstance(v, bool):
        keys.add(('text', 'true' if v else 'false'))
    elif isinstance(v, (int, float, decimal.Decimal)):
        fv = float(v)
        if math.isfinite(fv):
            keys.add(('text', str(int(fv)) if fv.is_integer() else repr(fv)))
    return frozenset(keys)


# ---- Filters on column COMBINATIONS (TREATAS onto several columns) ----------
_TUPLE_FILTER_PREFIX = '__tuples__#'


def _tuple_filter_key(cols) -> str:
    """The filter_context key of a filter on column combinations. It has no
    'Table.Column' shape, so code reading per-column filters passes it by."""
    return _TUPLE_FILTER_PREFIX + json.dumps(
        [[t, c] for t, c in cols], ensure_ascii=False, separators=(',', ':'))


def _tuple_key(row) -> tuple:
    """One combination as DAX compares it (see _value_keys)."""
    return tuple(_value_keys(v, dateish=False)[0] for v in row)


def _project_tuple_filter(spec: dict, keep: list) -> dict:
    """The filter on the combinations of the columns at positions ``keep``."""
    cols = spec.get('tuple_columns') or []
    rows = dict.fromkeys(tuple(r[i] for i in keep) for r in spec.get('rows') or [])
    return {'tuple_columns': [list(cols[i]) for i in keep],
            'rows': [list(r) for r in rows]}


def _intersect_tuple_filters(a: dict, b: dict) -> dict:
    """Both filters on the same column combinations at once."""
    keep = {_tuple_key(r) for r in b.get('rows') or []}
    return {'tuple_columns': a.get('tuple_columns'),
            'rows': [r for r in a.get('rows') or [] if _tuple_key(r) in keep]}


def _text_key(s: str) -> str:
    """Text as DAX compares it: case-insensitively (the model's collation;
    issue #107). Accents still count."""
    return s.casefold()


def _dax_equal(a, b) -> bool:
    """Equality as DAX's lookups and SWITCH see it: text case-insensitively,
    numbers by value whatever their storage, dates as moments (_value_keys)."""
    ka = _value_keys(a, False)
    return any(k in ka for k in _filter_value_keys(b))


def _compare(cell, op: str, target) -> bool:
    """Compare a cell against a target — numerically when both are numbers, by
    date when both parse as dates, else as text, case-insensitively (#107). A
    BLANK side compares as the other side's zero (see _blank_zero_of): BLANK = 0
    and BLANK < 1 are TRUE."""
    if target is None and cell is not None:
        target = _blank_zero_of(cell)
    elif cell is None and target is not None:
        cell = _blank_zero_of(target)
    a_num, b_num = _as_number(cell), _as_number(target)
    if a_num is not None and b_num is not None:
        a, b = a_num, b_num
    else:
        a_dt, b_dt = _as_date_strict(cell), _as_date_strict(target)
        if a_dt is not None and b_dt is not None:
            a, b = a_dt, b_dt
        else:
            a = _text_key("" if cell is None else str(cell))
            b = _text_key("" if target is None else str(target))
    if op == ">":
        return bool(a > b)
    if op in (">=", "=>"):
        return bool(a >= b)
    if op == "<":
        return bool(a < b)
    if op in ("<=", "=<"):
        return bool(a <= b)
    if op in ("=", "==", "eq"):
        return bool(a == b)
    if op in ("<>", "!=", "ne"):
        return bool(a != b)
    raise ValueError(f"unsupported filter operator {op!r}")


def _relative_date_bounds(spec: dict):
    """(lo, hi) date bounds for a relative-date spec.

    ``{"last": 7, "unit": "day"}`` covers the last 7 days up to and including
    the anchor; ``{"next": 3, "unit": "month"}`` looks forward. ``unit`` is
    day/week/month/year. ``anchor`` (an ISO date) defaults to today and lets a
    caller pin the window deterministically.
    """
    unit = str(spec.get("unit", "day")).lower().rstrip("s")
    anchor = _as_date(spec.get("anchor")) or date.today()
    n = int(spec.get("last", spec.get("next", 0)) or 0)
    per = {"day": 1, "week": 7, "month": 30, "year": 365}.get(unit)
    if per is None:
        raise ValueError(f"unsupported relative_date unit {unit!r}")
    span = timedelta(days=per * n)
    if "next" in spec:
        return anchor, anchor + span
    return anchor - span, anchor


_SHARED_FILTER_CACHES: dict = {}
_SHARED_FILTER_CACHE_MAX = 4

# Guards the insert / evict / id-assign steps of the module-level caches below,
# which evaluations on different threads (see _PerThreadEngine) may run at
# once. Reads stay lock-free: an entry is a pure function of its key. Without
# it two relationship sets interned together could get the SAME id, and the
# cross-filter memo keyed by it would serve one set's answer for the other.
_CACHE_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Calculation groups (issue #121)
# ---------------------------------------------------------------------------
# A model's calculation groups travel with its MEASURES dict: every context
# over the model -- including the ones a dozen construction sites derive
# without copying attributes -- carries that same dict, so registering the
# groups against its identity reaches all of them (as _shared_filter_cache
# does for the filter caches).
class _CalcGroup:
    """One calculation group as the engine applies it."""
    __slots__ = ('table', 'column', 'precedence', 'items', 'by_name', 'key')

    def __init__(self, spec: dict):
        self.table = str(spec['table'])
        self.column = str(spec.get('column') or 'Name')
        self.precedence = int(spec.get('precedence') or 0)
        self.items = [dict(i) for i in (spec.get('items') or [])
                      if i.get('name') is not None]
        self.by_name = {str(i['name']).lower(): i for i in self.items}
        self.key = self.table.lower()


_CALC_GROUPS: dict = {}       # id(measures) -> (measures, groups, measure formats)
_CALC_GROUPS_MAX = 64


def set_calculation_groups(measures: dict, groups, measure_formats: dict | None = None) -> None:
    """Attach a model's calculation groups (ModelReader.calculation_groups)
    and its measures' format strings (ModelReader.measure_format_strings) to
    its measures dict. Evaluations over that dict then apply the selected
    calculation items; ``groups`` empty or None detaches them."""
    with _CACHE_LOCK:
        _CALC_GROUPS.pop(id(measures), None)
        if groups or measure_formats:
            if len(_CALC_GROUPS) >= _CALC_GROUPS_MAX:
                _CALC_GROUPS.pop(next(iter(_CALC_GROUPS)), None)
            gs = sorted((g if isinstance(g, _CalcGroup) else _CalcGroup(g)
                         for g in (groups or [])), key=lambda g: -g.precedence)
            _CALC_GROUPS[id(measures)] = (measures, tuple(gs), dict(measure_formats or {}))


def _calc_registry(measures) -> tuple:
    hit = _CALC_GROUPS.get(id(measures))
    if hit is not None and hit[0] is measures:
        return hit[1], hit[2]
    return (), {}


# Relationship sets seen so far -> a small id (see DAXContext._rels_sig).
_RELS_SIG_IDS: dict = {}


_REL_INDEX_CACHE: dict = {}
_REL_INDEX_CACHE_MAX = 16


def _relationship_indexes(relationships: list) -> tuple:
    """The relationship lookups a DAXContext propagates with, built once per
    relationship LIST rather than once per context.

    A context is created per iterated row -- a FILTER over 40,000 rows builds
    ~50,000 -- and rebuilding these for each one was the largest single cost
    of such a measure (issue #82 added two more of them). Nothing mutates them
    after construction, and the list itself is held so its id cannot be
    reused. USERELATIONSHIP / CROSSFILTER build a NEW list, so they get their
    own entry.

    Returns (rel_index, rel_dir, rel_adj, one_side_of, rels_sig, expand_adj,
    date_part_joins).
    """
    hit = _REL_INDEX_CACHE.get(id(relationships))
    if hit is not None and hit[0] is relationships:
        cached: tuple = hit[1]
        return cached
    # { (fromTable, toTable): { fromCol, toCol } }, both orientations
    rel_index: dict = {}
    rel_dir: dict = {}
    for rel in relationships:
        if rel.get('IsActive'):
            ft = rel.get('FromTable', '')
            tt = rel.get('ToTable', '')
            fc = rel.get('FromColumn', '')
            tc = rel.get('ToColumn', '')
            if ft and tt and fc and tc:
                rel_index[(ft, tt)] = {'from_col': fc, 'to_col': tc}
                rel_index[(tt, ft)] = {'from_col': tc, 'to_col': fc}
                # Directional copy: a filter flows ONE -> MANY (ToTable ->
                # FromTable) by default; the reverse edge exists only for a
                # bidirectional relationship. The symmetric index above
                # stays, but propagation may only take the reverse
                # direction for EXPANDED keys (see _expanded_keys).
                rel_dir[(ft, tt)] = {'from_col': fc, 'to_col': tc}
                if rel.get('CrossFilteringBehavior') == 2:
                    rel_dir[(tt, ft)] = {'from_col': tc, 'to_col': fc}
    # Directed adjacency for MULTI-HOP (snowflake) filter propagation.
    # A filter propagates along the default cross-filter direction: from the
    # "one" side (ToTable) to the "many" side (FromTable). Each edge carries
    # the join columns on both endpoints so a filter can be chained hop by
    # hop. Bidirectional relationships (CrossFilteringBehavior == 2) also add
    # the reverse edge. Only used when no DIRECT relationship exists, so the
    # existing single-hop behaviour is untouched.
    #   rel_adj[a] = [ (b, col_on_a, col_on_b), ... ]
    #   -> allowed values of a[col_on_a] restrict b[col_on_b]
    rel_adj: dict = {}
    for rel in relationships:
        if not rel.get('IsActive'):
            continue
        ft = rel.get('FromTable', '')
        tt = rel.get('ToTable', '')
        fc = rel.get('FromColumn', '')
        tc = rel.get('ToColumn', '')
        if not (ft and tt and fc and tc):
            continue
        # one -> many (dim -> fact): ToTable -> FromTable
        rel_adj.setdefault(tt, []).append((ft, tc, fc))
        if rel.get('CrossFilteringBehavior') == 2:
            # bidirectional: also many -> one
            rel_adj.setdefault(ft, []).append((tt, fc, tc))
    # (many table, one table) for every active relationship, so a hop knows
    # which end is the dimension; a 1:1 is a "one" side both ways.
    one_side_of: set = set()
    for rel in relationships:
        if rel.get('IsActive'):
            ft, tt = rel.get('FromTable', ''), rel.get('ToTable', '')
            one_side_of.add((ft, tt))
            if rel.get('FromCardinality') == 1 and rel.get('ToCardinality') == 1:
                one_side_of.add((tt, ft))
    # table -> the tables its EXPANDED table takes in directly: the one side of
    # each active many-to-one relationship it is the many side of, and the
    # other end of a one-to-one. A many-to-many relationship expands nothing.
    # (See DAXContext.expanded_tables.)
    expand_adj: dict = {}
    for rel in relationships:
        if not rel.get('IsActive'):
            continue
        ft, tt = rel.get('FromTable', ''), rel.get('ToTable', '')
        if not (ft and tt) or ft == tt:
            continue
        fcard, tcard = rel.get('FromCardinality'), rel.get('ToCardinality')
        if fcard == 2 and tcard == 2:
            continue
        if fcard == 1 and tcard == 2:
            expand_adj.setdefault(tt, set()).add(ft)
            continue
        expand_adj.setdefault(ft, set()).add(tt)
        if fcard == 1 and tcard == 1:
            expand_adj.setdefault(tt, set()).add(ft)
    # The relationship set itself, interned to a small int. It is part of the
    # cross-filter memo key: that memo lives on the model-wide cache, which
    # USERELATIONSHIP / CROSSFILTER contexts share with a DIFFERENT set
    # (issue #83).
    rels_sig: int | None
    try:
        sig = tuple(
            (r.get('FromTable'), r.get('FromColumn'), r.get('ToTable'),
             r.get('ToColumn'), r.get('IsActive'),
             r.get('CrossFilteringBehavior'), r.get('FromCardinality'),
             r.get('ToCardinality'), r.get('JoinOnDateBehavior'))
            for r in relationships)
        with _CACHE_LOCK:
            rels_sig = _RELS_SIG_IDS.setdefault(sig, len(_RELS_SIG_IDS))
    except (AttributeError, TypeError):
        rels_sig = None
    # The joins that compare DATE PARTS (JoinOnDateBehavior = 2, DatePartOnly),
    # both orientations, as (table, column, other table, other column) --
    # Power BI writes it for auto date/time (issue #126).
    date_part: set = set()
    for rel in relationships:
        jod = rel.get('JoinOnDateBehavior')
        if jod is None and any(str(rel.get(k) or '').startswith('LocalDateTable_')
                               for k in ('ToTable', 'FromTable')):
            jod = 2     # not given: Desktop writes DatePartOnly on every auto date/time join
        if rel.get('IsActive') and jod == 2:
            ft, fc = rel.get('FromTable', ''), rel.get('FromColumn', '')
            tt, tc = rel.get('ToTable', ''), rel.get('ToColumn', '')
            date_part.add((ft, fc, tt, tc))
            date_part.add((tt, tc, ft, fc))
    out = (rel_index, rel_dir, rel_adj, one_side_of, rels_sig,
           {t: frozenset(v) for t, v in expand_adj.items()}, frozenset(date_part))
    with _CACHE_LOCK:
        if len(_REL_INDEX_CACHE) >= _REL_INDEX_CACHE_MAX:
            _REL_INDEX_CACHE.pop(next(iter(_REL_INDEX_CACHE)), None)
        _REL_INDEX_CACHE[id(relationships)] = (relationships, out)
    return out


def _shared_filter_cache(tables) -> dict:
    """One filter-index cache per model, shared by EVERY context over it.

    Assigning the cache at each derived-context construction site does not hold:
    there are a dozen of them across engine.py and calc_tables.py, and the two
    that were missed silently created a private cache per row, so an iterator
    rebuilt 200k-row index maps for every row it visited. Keying on the identity
    of the tables mapping makes sharing automatic and future construction sites
    correct by default.

    The row COUNTS are part of the key so a rewritten table cannot be served
    stale indices; a fresh tables mapping (which is what happens when the model
    is edited and the DAX context is rebuilt) gets a fresh identity anyway. A
    handful of models are retained, FIFO, to bound memory.
    """
    try:
        fp = (id(tables), tuple(sorted(
            (k, len(v.get('rows') or ())) for k, v in tables.items()
            if isinstance(v, dict))))
    except (AttributeError, TypeError):
        return {}
    hit = _SHARED_FILTER_CACHES.get(fp)
    if hit is None:
        with _CACHE_LOCK:
            hit = _SHARED_FILTER_CACHES.get(fp)
            if hit is None:
                if len(_SHARED_FILTER_CACHES) >= _SHARED_FILTER_CACHE_MAX:
                    _SHARED_FILTER_CACHES.pop(next(iter(_SHARED_FILTER_CACHES)), None)
                hit = _SHARED_FILTER_CACHES[fp] = {'__tables_ref__': tables}
    return hit


def _extremum(cur, cand, want_max: bool):
    """Running MAX/MIN across the types DAX's MAXX/MINX accept.

    The old test was ``isinstance(result, (int, float))``, so a DATE-valued
    iteration collapsed to the 0 fallback: Power BI's own _ShowValueForDates
    guard does ``MAXX({ MAX('FactSales'[DateKey]) }, ''[Value])``, got 0, and the
    measure it guards silently returned BLANK where Desktop shows $19,260,877.
    Text is accepted too (DAX's MAXX over strings returns the last one
    alphabetically). A candidate that cannot be compared with what is already
    held is skipped rather than raising.
    """
    if cand is None or isinstance(cand, bool):
        return cur
    if not isinstance(cand, (int, float, str, datetime, date)):
        return cur
    if cur is None:
        return cand
    a, b = cand, cur
    if isinstance(a, str) and isinstance(b, str):
        # Text orders case-insensitively (Desktop: MAXX({"a", "B"}, [Value])
        # is "B"; issue #107).
        a, b = _text_key(a), _text_key(b)
    try:
        return cand if ((a > b) if want_max else (a < b)) else cur
    except TypeError:
        return cur


def make_value_matcher(spec):
    """Build a predicate ``f(cell) -> bool`` for one filter_context entry.

    A LIST is an In-set: membership as DAX compares values (_value_keys).
    A DICT is a structured predicate, so a caller no longer has to enumerate a
    column's matching values before evaluating:

      ``{"op": ">", "value": 100}``   comparison (>, >=, <, <=, =, <>)
      ``{"between": [lo, hi]}``       inclusive range
      ``{"in": [...]}`` / ``{"not_in": [...]}``
      ``{"contains"|"starts_with"|"ends_with": "text"}``  (case-insensitive)
      ``{"relative_date": {"last": 7, "unit": "day"}}``
      ``{"is_blank": true|false}``

    Several keys in one dict are ANDed.
    """
    if not isinstance(spec, dict):
        values = spec if isinstance(spec, (list, tuple, set, frozenset)) else [spec]
        # Values match as DAX compares them (see _value_keys / _filter_value_keys).
        # A date reaches us in more than one representation -- a Date table
        # holds datetime objects, DATESMTD/DATESYTD and caller filters produce
        # ISO strings -- and str() of two equal dates need not match: that made
        # CALCULATE([Sales], DATESMTD(...)) select ZERO rows where Desktop
        # returns $19,260,877. Numbers likewise ('2024.0' vs '2024', issue #39).
        # Both still match; but the old str()/float()/calendar-day aliasing
        # also made '01' select '1', 09:00 select 17:00 of the same day, and
        # "north" select nothing, where Desktop 2.152 answers 1, 1 and 5.
        allowed: set = set()
        for v in values:
            allowed |= _filter_value_keys(v)
        dateish = any(k[0] == 'moment' for k in allowed)

        def _match(cell):
            return any(k in allowed for k in _value_keys(cell, dateish))

        return _match

    tests = []
    if "op" in spec:
        op, target = spec["op"], spec.get("value")
        tests.append(lambda c: _compare(c, op, target))
    if "between" in spec:
        lo, hi = spec["between"]
        tests.append(lambda c: _compare(c, ">=", lo) and _compare(c, "<=", hi))
    if "in" in spec:
        # T[C] IN {...}: the same membership as a list filter (Desktop 2.152:
        # F2[Region] IN {"NORTH", "east"} selects North and East).
        tests.append(make_value_matcher(list(spec["in"])))
    if "not_in" in spec:
        _denied = make_value_matcher(list(spec["not_in"]))
        tests.append(lambda c: not _denied(c))
    if "contains" in spec:
        needle = str(spec["contains"]).lower()
        tests.append(lambda c: needle in str("" if c is None else c).lower())
    if "starts_with" in spec:
        pre = str(spec["starts_with"]).lower()
        tests.append(
            lambda c: str("" if c is None else c).lower().startswith(pre))
    if "ends_with" in spec:
        suf = str(spec["ends_with"]).lower()
        tests.append(
            lambda c: str("" if c is None else c).lower().endswith(suf))
    if "relative_date" in spec:
        _lo, _hi = _relative_date_bounds(spec["relative_date"])

        def _rd(c, lo=_lo, hi=_hi):
            d = _as_date_strict(c)
            return d is not None and lo <= d <= hi

        tests.append(_rd)
    if "is_blank" in spec:
        want = bool(spec["is_blank"])
        tests.append(lambda c: (c is None or str(c) == "") is want)
    if "all" in spec:
        # Conjunction of whole specs, which the flat keys cannot express: two
        # comparisons on one column ({"op": ">"} AND {"op": "<"}) would collide
        # on the "op" key. CALCULATE builds this when it is given several
        # predicates for the same column, since DAX intersects them.
        subs = [make_value_matcher(s) for s in spec["all"]]
        tests.append(lambda c: all(m(c) for m in subs))
    if not tests:
        raise ValueError(f"filter predicate {spec!r} has no recognized keys")
    return lambda cell: all(t(cell) for t in tests)


def _substitute_row_refs(expr: str, table_name: str, row_item: dict) -> str:
    """Replace ``Table[Col]`` references with the row's literal values.

    Used by FILTER to evaluate a condition against the row being iterated: a
    bare column reference otherwise evaluates to an unresolved
    ('Table','Column') marker and every comparison against it returns None.
    """
    out = expr
    for col_name, val in row_item.items():
        if col_name.startswith("__"):
            continue
        if isinstance(val, str):
            literal = '"' + val.replace('"', '""') + '"'
        elif val is None:
            literal = "BLANK()"
        elif isinstance(val, bool):
            literal = "TRUE()" if val else "FALSE()"
        elif isinstance(val, (datetime, date)):
            literal = '"' + val.isoformat() + '"'
        else:
            literal = str(val)
        for pat in (f"'{table_name}'[{col_name}]", f"{table_name}[{col_name}]"):
            if pat in out:
                out = out.replace(pat, literal)
    return out


class _VarEvalError(_DAXEvaluationError):
    """A variable's expression failed. Desktop does not let IFERROR / ISERROR
    at the use site catch it: VAR x = ERROR("boom") RETURN IFERROR(x, 9) fails
    (build_b134.py, issue #134). Carries the original error."""

    def __init__(self, original: BaseException):
        super().__init__(getattr(original, 'message', None) or str(original))
        self.original = original


class _LazyVar:
    """A VAR, evaluated the first time it is used, in the filter context and
    with the variables it was defined with. Desktop 2.152 (build_b134.py,
    issue #134): VAR x = ERROR("boom") RETURN 5 is 5, IF(FALSE(), x, 6) is 6,
    and VAR x = SUM(T[v]) RETURN CALCULATE(x, T[c] = "a") is the whole 60."""
    __slots__ = ('expr', 'ctx', 'scope', 'state', 'value')

    def __init__(self, expr: str, ctx, scope: dict):
        self.expr, self.ctx, self.scope = expr, ctx, scope
        self.state = 0           # 0 pending, 1 running, 2 done, 3 failed
        self.value: Any = None

    def force(self, engine) -> Any:
        if self.state == 2:
            return self.value
        if self.state == 3:
            raise self.value
        if self.state == 1:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(f"Variable expression '{self.expr[:60]}' refers to itself")
        self.state = 1
        prev = engine._current_var_scope
        engine._current_var_scope = self.scope
        try:
            self.value = engine._eval_expr(self.expr, self.ctx, self.scope)
            self.state = 2
            return self.value
        except Exception as exc:
            if getattr(exc, "_pbix_deadline", False):
                self.state = 0
                raise
            err = exc if isinstance(exc, _VarEvalError) else _VarEvalError(exc)
            self.state, self.value = 3, err
            raise err from exc
        finally:
            engine._current_var_scope = prev


class GroupByValues(list):
    """A filter_context In-set that came from the QUERY'S GROUPING (a row of a
    SUMMARIZECOLUMNS / visual axis), not from a slicer or a CALCULATE filter.

    ISINSCOPE asks exactly that question, so the provenance travels with the
    values: CALCULATE builds fresh plain lists when it replaces or removes a
    column's filter, which drops the tag, and leaves an untouched column's list
    as it was. Verified against Power BI Desktop 2.157: ISINSCOPE is TRUE for a
    grouped column, FALSE for one that is only filtered (TREATAS) or whose
    filter a CALCULATE replaced (``IT Area = "BU Support"``, even on that very
    row) or removed (REMOVEFILTERS / ALL / ALLSELECTED()), and stays TRUE when
    another column's filter is removed.
    """


class GroupByPredicate(dict):
    """The GroupByValues tag for a predicate-form filter (e.g. the BLANK group's
    ``{"is_blank": True}``)."""


class RowContextValues(list):
    """A filter_context In-set that an ITERATION's row put there.

    This engine applies the row->filter transition eagerly, so an iterator's row
    is already in filter_context while its row context is still only a row
    context (``_outer_ctx`` is set). In DAX the row becomes a filter -- and its
    columns come into scope for ISINSCOPE -- only through CALCULATE or a measure
    reference, which clear ``_outer_ctx``. Desktop 2.157 (Microsoft's MIT
    Corporate Spend sample):

        SUMX(VALUES(T[c]), IF(CALCULATE(ISINSCOPE(T[c])), 1, 0))   -- 5 at the total
        SUMX(VALUES(T[c]), [ISINSCOPE(T[c]) measure] + 0)          -- 5
        SUMX(T, IF(CALCULATE(ISINSCOPE(T[c])), 1, 0))              -- 40, one per row
        SUMX(VALUES(T[c]), IF(ISINSCOPE(T[c]), 1, 0))              -- 0 (no transition)

    ``shadow`` is the iteration's rows (its "shadow filter context"): what
    ALLSELECTED puts back for this column (issue #118). None when the
    iterator did not say.
    """
    shadow: Optional[list] = None


class RowContextPredicate(dict):
    """The RowContextValues tag for a predicate-form filter: KEEPFILTERS on an
    iterated column INTERSECTS the row's filter, and the column stays in scope
    for ISINSCOPE (issue #80). Desktop 2.152: SUMX(VALUES(S[Region]),
    CALCULATE(IF(ISINSCOPE(S[Region]), 1, 0), KEEPFILTERS(S[Region] = "West")))
    is 1 for every region, 4 at the total."""
    shadow: Optional[list] = None


def _row_value(vals: list, shadow: Optional[list]) -> RowContextValues:
    """A row transition's filter value, carrying the iteration's rows."""
    tagged = RowContextValues(vals)
    tagged.shadow = shadow
    return tagged


def _iteration_tagged(v) -> bool:
    """A filter an iteration put there: a grouping or a row transition."""
    return isinstance(v, (GroupByValues, GroupByPredicate, RowContextValues, RowContextPredicate))


def _keep_scope_tag(outer, value):
    """KEEPFILTERS intersects the outer filter; the intersection keeps the
    outer filter's scope tag (grouping, or an iterated row), so ISINSCOPE
    answers as Desktop does (issues #69, #80)."""
    if isinstance(outer, (GroupByValues, GroupByPredicate)):
        return GroupByPredicate(value)
    if isinstance(outer, (RowContextValues, RowContextPredicate)):
        tagged = RowContextPredicate(value)
        tagged.shadow = outer.shadow
        return tagged
    return value


class DAXContext:
    """Execution context for DAX evaluation — holds table data and filter state."""

    def __init__(self, tables: dict, measures: dict, date_table: Optional[str] = None,
                 date_column: Optional[str] = None, filter_context: Optional[dict] = None,
                 relationships: Optional[list] = None):
        """
        tables: { 'tableName': { 'columns': [...], 'rows': [[...], ...] } }
        measures: { 'MeasureName': 'DAX expression string' }
        date_table: name of the date dimension table
        date_column: name of the date column in the date table
        filter_context: { 'tableName.columnName': [allowed_values] }
        relationships: [ { FromTable, FromColumn, ToTable, ToColumn, IsActive } ]
        """
        self.tables = tables
        self.measures = measures
        self.date_column = date_column or 'Date'
        self.filter_context = filter_context or {}
        self.relationships = relationships or []
        # Filtered column values, cached per (table, column) -- NOT per filter
        # set, so the memo is only valid for the filter context in force when it
        # was populated. Re-ASSIGNING filter_context clears it (see the property
        # below); mutating that dict in place cannot be detected. No caller
        # mutates the returned list either (audited: they wrap it in list(),
        # sorted() or a comprehension). This is the issue-#6 _minmax_column
        # win: a measure like MIN(T[Col]) under one outer context rebuilt the
        # same filtered list on every one of thousands of per-row calls.
        self._column_data_cache: dict = {}
        # Per-filter row-index sets. Keyed on the identity of the tables
        # mapping, so EVERY context over the same model shares one cache without
        # each construction site having to remember to pass it along.
        self._filter_idx_cache: dict = _shared_filter_cache(tables)
        # __init__ assigned filter_context BEFORE this cache existed, so the
        # property setter could not clear it. Nothing is cached yet, so there is
        # nothing to lose -- but keep this after the cache is created.
        # Auto-detect date table if not provided (relationship-aware).
        if date_table:
            self.date_table = date_table
        else:
            self.date_table = self._auto_detect_date_table(tables, self.relationships)
        # Set during row iteration (SUMX, AVERAGEX, etc.)
        self._current_row: Optional[dict] = None
        # measure name -> the table it is DEFINED ON. DAX uses it to resolve an
        # unqualified [Column] inside that measure, which is the only thing that
        # can disambiguate a column name several tables share.
        self.measure_tables: dict = {}
        # {table: {column names}} straight from the model metadata. EMPTY means
        # "unknown", and an unresolvable-reference check must then refuse
        # nothing rather than everything.
        self.model_columns: dict = {}
        # Filter-context KEYS ("Table.Column") that came from GROUPED
        # evaluation (the visual's row/column grouping), as opposed to the
        # caller's explicit slicer selection. ALLSELECTED removes exactly
        # these and keeps the rest — without the distinction, every
        # ALLSELECTED form under pbix_evaluate_dax_grouped was a no-op or
        # degraded to ALL (issues #25 r23#2 / #26 r24#1-3).
        self.group_keys: set = set()
        # The caller's TRUE slicer state (the base filter_context BEFORE the
        # group key was merged in). Needed because grouping a column that is
        # ALSO sliced OVERWRITES the slicer's values in filter_context —
        # `Cat IN (A,B)` grouped by Cat becomes `Cat=[A]`, and the slicer
        # selection is unrecoverable from the merged dict (r24#2's 180-vs-60).
        self.selected_filters: dict | None = None
        # Tables MARKED as date tables, {table: date column} (PR #73): read
        # once per evaluation into DAXEngine._date_tables.
        self.date_tables: dict = {}
        # Tables an enclosing ALL(Table)/REMOVEFILTERS(Table) made immune to
        # cross-table filter propagation (see _get_cross_table_filters).
        self._no_propagate: set = set()
        # table -> the filter_context KEYS that were live when ALL(table)
        # was applied. Only those are suppressed; a filter created later
        # inside a nested CALCULATE still propagates, as Desktop does.
        self._no_prop_keys: dict = {}
        # The MODEL's own culture (Model.Culture, e.g. "pt-BR"). FORMAT()
        # returns a STRING the canvas prints verbatim, so its separators come
        # from the model rather than from the reader's host (issue #60).
        # None = the model states nothing, which means en-US.
        self.culture: Optional[str] = None
        # Rows of the group being evaluated by GROUPBY's extension columns;
        # CURRENTGROUP() reads it.
        self._current_group: Optional[list] = None
        # Filter keys registered by a TABLE filter argument (FILTER(T,...),
        # ALL(T) row sets). Desktop's rule, pinned on MS_Employee_Hiring:
        # a filter on a TABLE filters its EXPANDED table, so it reaches the
        # one-side dimensions the table points at (MAX('Date'[PeriodNumber])
        # drops 201612 -> 201412 under FILTER(Employee, ...)); a filter on a
        # COLUMN does not (Employee[FP]="FT" leaves it at 201612). Keys in
        # this set may propagate MANY -> ONE; every other filter is one->many.
        self._expanded_keys: set = set()
        # Pre-transition (outer) context, set by _make_row_context. In real DAX a
        # row context does NOT filter — only CALCULATE / a measure invocation
        # performs the row->filter transition. This engine applies the transition
        # eagerly (row filters are baked into the iteration context), so plain
        # column aggregates typed directly in an iterator's scalar expression
        # (SUM(T[Col]) as a grand-total denominator) evaluate against _outer_ctx
        # to see the un-transitioned filter context, exactly like Desktop.
        # CALCULATE and evaluate_measure clear it (they ARE the transition).
        self._outer_ctx: Optional['DAXContext'] = None
        self._measure_cache: dict = {}
        # measure-cache key -> the unsupported functions that evaluation hit
        self._measure_cache_unsupported: dict = {}
        # measure -> the tables whose filters its memo key leaves out
        # (DAXEngine._shielded_tables), shared like the memo itself.
        self._shield_cache: dict = {}
        self._eval_stack: set = set()  # Prevent circular refs
        # Bound total sub-expression evaluations per top-level measure so a
        # pathological/non-terminating measure degrades to BLANK instead of
        # hanging the whole tool. Reset per outermost measure in evaluate_measure.
        self._eval_calls = 0
        self._max_eval_calls = 3_000_000
        # Relationship lookups (see _relationship_indexes), shared by every
        # context over the same relationship list.
        (self._rel_index, self._rel_dir, self._rel_adj, self._one_side_of,
         self._rels_sig, self._expand_adj,
         self._date_part_joins) = _relationship_indexes(self.relationships)

    # ---- DAX's blank (unknown) row -------------------------------------------
    # When a relationship joins a row whose key matches no row of the one side
    # (or whose key is blank), Power BI gives the one-side table a BLANK ROW and
    # attributes those rows to it (issue #82). Measured on Desktop 2.152 over
    # ADOMD: the row exists for active AND inactive relationships, for both
    # sides of a 1:1, never for many-to-many, and it carries one level up a
    # snowflake (its own keys are blank). VALUES, ALL, ALLSELECTED, FILTERS and
    # column predicates see it; DISTINCT, ALLNOBLANKROW, a bare table reference,
    # COUNTROWS(T), DISTINCTCOUNT, COUNTA and MIN/MAX do not.

    def blank_row_tables(self) -> frozenset:
        """The tables that carry a blank row in this model.

        Memoized on the model-wide cache per relationship set: finding them
        scans each relationship's many side once.
        """
        cache = self._filter_idx_cache
        key = ('blank-row-tables', self._rels_sig)
        if self._rels_sig is not None:
            hit: frozenset | None = cache.get(key)
            if hit is not None:
                return hit
        edges = []
        for rel in self.relationships:
            ft, fc = rel.get('FromTable'), rel.get('FromColumn')
            tt, tc = rel.get('ToTable'), rel.get('ToColumn')
            if not (ft and fc and tt and tc):
                continue
            fcard = rel.get('FromCardinality') or 2
            tcard = rel.get('ToCardinality') or 1
            if fcard == 2 and tcard == 2:
                continue                      # many-to-many: no blank row
            edges.append((ft, fc, tt, tc))
            if fcard == 1 and tcard == 1:
                edges.append((tt, tc, ft, fc))
        blank: set = set()
        unmatched: dict = {}
        changed = True
        while changed:
            changed = False
            for edge in edges:
                ft, fc, tt, tc = edge
                if tt in blank:
                    continue
                if edge not in unmatched:
                    unmatched[edge] = self._has_unmatched_keys(ft, fc, tt, tc)
                # A blank row of the many side has a blank key, which matches
                # no row here unless this table holds a blank key itself.
                if unmatched[edge] or (
                        ft in blank and 'None' not in self._key_aliases(tt, tc)):
                    blank.add(tt)
                    changed = True
        out = frozenset(blank)
        if self._rels_sig is not None:
            cache[key] = out
        return out

    def _key_aliases(self, table_name: str, col_name: str) -> frozenset:
        """Every spelling of every key in table_name[col_name] (see
        _join_key_aliases), memoized on the model-wide cache."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return frozenset()
        idx = self._find_col_idx(tbl['columns'], col_name)
        if idx < 0:
            return frozenset()
        key = (id(tbl), 'key-aliases', idx)
        hit = self._filter_idx_cache.get(key)
        if hit is None:
            acc: set = set()
            for r in tbl['rows']:
                acc |= _join_key_aliases(r[idx])
            hit = self._filter_idx_cache[key] = frozenset(acc)
        return hit

    def _date_part_key_set(self, table_name: str, col_name: str) -> frozenset:
        """Every DATE PART in table_name[col_name] ('dp:...', 'None' for a
        blank), memoized: the keys a DatePartOnly join matches (issue #126)."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return frozenset()
        idx = self._find_col_idx(tbl['columns'], col_name)
        if idx < 0:
            return frozenset()
        key = (id(tbl), 'dp-keys', idx)
        hit = self._filter_idx_cache.get(key)
        if hit is None:
            hit = self._filter_idx_cache[key] = frozenset(
                (_date_part_key(r[idx]) or str(r[idx])) if r[idx] is not None else 'None'
                for r in tbl['rows'])
        return hit

    def _has_unmatched_keys(self, many_table, many_col, one_table, one_col) -> bool:
        mtbl = self.tables.get(many_table)
        if not mtbl or not self.tables.get(one_table):
            return False
        mi = self._find_col_idx(mtbl['columns'], many_col)
        if mi < 0:
            return False
        if (many_table, many_col, one_table, one_col) in self._date_part_joins:
            dp_keys = self._date_part_key_set(one_table, one_col)
            return any(not _key_in(r[mi], _DatePartKeys(dp_keys)) for r in mtbl['rows'])
        keys = self._key_aliases(one_table, one_col)
        return any(str(r[mi]) not in keys for r in mtbl['rows'])

    def _unmatched_key_strs(self, many_table: str, many_idx: int,
                            one_table: str, one_col: str) -> frozenset:
        """The many side's keys that reach one_table's blank row, plus 'None'
        (a blank key), as the str() spellings a cross filter holds -- date
        parts across a DatePartOnly join."""
        mtbl = self.tables.get(many_table)
        if not mtbl:
            return frozenset({'None'})
        key = (id(mtbl), 'unmatched', many_idx, one_table, one_col)
        hit = self._filter_idx_cache.get(key)
        if hit is None:
            many_col = mtbl['columns'][many_idx] if many_idx < len(mtbl['columns']) else None
            if (many_table, many_col, one_table, one_col) in self._date_part_joins:
                keys = self._date_part_key_set(one_table, one_col)
                out = {(_date_part_key(r[many_idx]) or str(r[many_idx]))
                       if r[many_idx] is not None else 'None' for r in mtbl['rows']}
            else:
                keys = self._key_aliases(one_table, one_col)
                out = {str(r[many_idx]) for r in mtbl['rows']}
            hit = self._filter_idx_cache[key] = frozenset(
                {k for k in out if k not in keys} | {'None'})
        return hit

    def _filters_admit_blank(self, col_filters) -> bool:
        """Do ALL these (column, values) filters keep a row whose every value
        is BLANK? That is whether they keep the table's blank row."""
        for _col, values in col_filters:
            try:
                if not make_value_matcher(values)(None):
                    return False
            except (ValueError, TypeError):
                return False
        return True

    def blank_row_visible(self, table_name: str) -> bool:
        """Is table_name's blank row in the current filter context?

        It is when the table has one, every direct filter on the table keeps a
        BLANK value, and every filter reaching the table through a relationship
        admits it ('None' in the allowed keys -- see _hop_keys).
        """
        if table_name not in self.blank_row_tables():
            return False
        tbl = self.tables.get(table_name)
        if not tbl:
            return False
        direct = []
        for fk, values in self.filter_context.items():
            t, _, c = fk.partition('.')
            if t == table_name and self._find_col_idx(tbl['columns'], c) >= 0:
                direct.append((c, values))
        if not self._filters_admit_blank(direct):
            return False
        for fk, spec in self.filter_context.items():
            # A filter on column combinations keeps the blank row only through
            # a combination that is BLANK on every column the table sees.
            if fk.startswith(_TUPLE_FILTER_PREFIX):
                pos = [i for i, (t, c) in enumerate(spec.get('tuple_columns') or [])
                       if self._tuple_column_getter(table_name, t, c) is not None]
                if pos and not any(all(r[i] is None for i in pos)
                                   for r in spec.get('rows') or []):
                    return False
        return all('None' in allowed
                   for allowed, _idx in self._get_cross_table_filters(table_name))

    def blank_row_dict(self, table_name: str) -> dict:
        """The blank row as a full-row dict (every column BLANK)."""
        row: dict = {'__table__': table_name, '__row__': True, '__blank_row__': True}
        for c in (self.tables.get(table_name) or {}).get('columns', []):
            row[c] = None
        return row

    def _hop_keys(self, cur_name: str, cur_rows: list, cur_blank: bool,
                  cur_idx: int, nxt_name: str, nxt_idx: int) -> AbstractSet[str]:
        """The keys of nxt_name[nxt_idx] that the filtered rows of cur_name
        select across one relationship, blank row included.

        Across a one -> many hop, a visible blank row of the one side selects
        the many-side rows whose key matches nothing (and blank keys). Across a
        many -> one hop (bidirectional or an expanded table filter), the one
        side's blank row is selected when a surviving row's key matches
        nothing. Either way the blank row travels as 'None'.
        """
        cur_tbl = self.tables.get(cur_name) or {}
        nxt_tbl = self.tables.get(nxt_name) or {}
        cur_col = (cur_tbl.get('columns') or [None])[cur_idx] if cur_tbl else None
        nxt_col = (nxt_tbl.get('columns') or [None])[nxt_idx] if nxt_tbl else None
        date_part = (cur_name, cur_col, nxt_name, nxt_col) in self._date_part_joins
        allowed: set = set()
        for r in cur_rows:
            if date_part:
                # A DatePartOnly join (auto date/time): the next table's rows
                # of the same DAY, whatever their time (issue #126).
                v = r[cur_idx]
                allowed.add((_date_part_key(v) or str(v)) if v is not None else 'None')
            else:
                allowed |= _join_key_aliases(r[cur_idx])
        if (nxt_name, cur_name) in self._one_side_of:
            if cur_blank and cur_col is not None:
                allowed |= self._unmatched_key_strs(nxt_name, nxt_idx, cur_name, cur_col)
        elif nxt_col is not None and nxt_name in self.blank_row_tables():
            if date_part:
                dp_keys = _DatePartKeys(self._date_part_key_set(nxt_name, nxt_col))
                missing = any(not _key_in(r[cur_idx], dp_keys) for r in cur_rows)
            else:
                keys = self._key_aliases(nxt_name, nxt_col)
                missing = any(str(r[cur_idx]) not in keys for r in cur_rows)
            if cur_blank or missing:
                allowed.add('None')
        return _DatePartKeys(allowed) if date_part else allowed

    @staticmethod
    def _auto_detect_date_table(tables: dict, relationships: list | None = None) -> str:
        """Auto-detect the date/calendar dimension table from available tables.

        A relationship-aware pass runs first: a table that is the ONE side
        (ToTable) of a relationship AND carries a Date column plus calendar
        columns (Year/Month) — or a date-y name — is almost certainly the date
        dimension, which is more reliable than matching on the table name alone
        (a fact table can also have a "Date" column). Name heuristics follow as
        a fallback for models without an explicit relationship.
        """
        relationships = relationships or []

        # Pass 0 (relationship-aware): prefer a date dimension on the one-side of
        # a relationship.
        one_side = [r.get('ToTable') for r in relationships if r.get('ToTable')]
        # de-dup preserving order
        seen: set = set()
        for tname in one_side:
            if tname in seen:
                continue
            seen.add(tname)
            tdata = tables.get(tname)
            if not tdata:
                continue
            cols_lower = [c.lower() for c in tdata.get('columns', [])]
            if 'date' in cols_lower and (
                'year' in cols_lower or 'month' in cols_lower
                or 'date' in tname.lower() or 'calendar' in tname.lower()
            ):
                return str(tname)

        # Pass 1: table name contains 'date' and has a 'Date' column
        for tname, tdata in tables.items():
            if 'date' in tname.lower() and 'Date' in tdata.get('columns', []):
                return str(tname)
        # Pass 2: common date-table prefixes (dimDate, DimDate, Calendar, etc.)
        for tname, tdata in tables.items():
            tlow = tname.lower().replace(' ', '').replace('-', '').replace('_', '')
            if tlow in ('dimdate', 'datetable', 'calendar', 'datekey', 'dates'):
                for cname in tdata.get('columns', []):
                    if cname.lower() == 'date':
                        return str(tname)
        # Pass 3: any table with Date + Year/Month columns (likely a date dimension)
        for tname, tdata in tables.items():
            cols_lower = [c.lower() for c in tdata.get('columns', [])]
            if 'date' in cols_lower and ('year' in cols_lower or 'month' in cols_lower):
                return str(tname)
        return 'dim-Date'  # fallback default

    def _find_col_idx(self, cols: list, col_name: str) -> int:
        """Find column index by name, with fuzzy matching."""
        for i, c in enumerate(cols):
            if c == col_name:
                return i
        # Fuzzy: try case-insensitive and hyphen/underscore normalization
        norm = col_name.lower().replace('-', '_').replace(' ', '_')
        for i, c in enumerate(cols):
            if c.lower().replace('-', '_').replace(' ', '_') == norm:
                return i
        # A model column may begin or end with a space -- Microsoft's Financial
        # Sample has ' Sales', read as financials[ Sales] -- while every parse
        # of a reference strips the bracketed name. Matching exactly found no
        # column, so SUM(financials[ Sales]) and all 47 measures built on it
        # were BLANK where Desktop answers 118,726,350.26 (issue #132).
        bare = col_name.strip().lower()
        for i, c in enumerate(cols):
            if c.strip().lower() == bare:
                return i
        return -1

    def _filter_snapshot(self, table: str) -> dict:
        """Signature of every filter NOT on `table`, as ALL(table) sees them.

        Keyed by filter key -> value signature so that re-filtering the same
        column later is recognised as a different filter.
        """
        snap = {}
        for k, v in self.filter_context.items():
            if k.startswith(f"{table}."):
                continue
            try:
                snap[k] = self._filter_sig(v)
            except TypeError:
                snap[k] = None
        return snap

    def model_table(self, name: str) -> str:
        """``name`` as the model spells it (DAX table names are case-insensitive)."""
        if name in self.tables:
            return name
        low = name.lower()
        return next((t for t in self.tables if t.lower() == low), name)

    def expanded_tables(self, table: str) -> frozenset:
        """``table`` and the tables of its EXPANDED table.

        DAX's ALL / REMOVEFILTERS / ALLEXCEPT / ALLSELECTED on a table act on
        every column of its expanded table: the one side of each active
        many-to-one relationship, followed transitively (a snowflake), and both
        ends of a one-to-one. A many-to-many relationship expands nothing.
        Measured on Power BI Desktop 2.152 over ADOMD, Fact -> Dim -> Cat with
        Dim <-> Twin one-to-one and Fact <-> Tags many-to-many: under a visual
        grouped by Cat[CatName], CALCULATE(MIN(Fact[v]), ALLSELECTED(Twin)) is
        the all-rows 5 (Twin's expanded table reaches Cat through Dim), and
        under Tags[TagName] CALCULATE(SUM(Fact[v]), ALL(Fact)) is the tag's
        27 / 18, not 45.
        """
        out, todo = {table}, [table]
        adj = self._expand_adj
        while todo:
            for nxt in adj.get(todo.pop(), ()):
                if nxt not in out:
                    out.add(nxt)
                    todo.append(nxt)
        return frozenset(out)

    def is_directly_filtered(self, table: str, column: str | None = None) -> bool:
        """ISFILTERED's test: a DIRECT filter on ``table[column]``, or on any
        column of ``table`` when ``column`` is None -- a filter on the column
        itself, or a filter on column combinations (TREATAS onto several
        columns) that names it. A filter that only reaches ``table`` through
        a relationship is a cross filter: ISFILTERED(Orders) stays FALSE under
        Dim[Zone] = "z1" (Power BI Desktop 2.152 over ADOMD).

        Not modelled yet: a TABLE filter argument filters every column of its
        EXPANDED table directly -- Desktop says ISFILTERED(Dim[Zone]) is TRUE
        under FILTER(Orders, ...) -- because this engine keeps a table filter
        as filters on the table's own columns (see _expanded_keys).

        A key naming a column the table does not have filters nothing
        (_surviving_indices skips it), so it is no filter here either."""
        tl, cl = table.lower(), (column.lower() if column is not None else None)
        tbl = self.tables.get(table)
        for k, v in self.filter_context.items():
            if k.startswith(_TUPLE_FILTER_PREFIX):
                if any(str(t).lower() == tl and (cl is None or str(c).lower() == cl)
                       for t, c in v.get('tuple_columns') or []):
                    return True
                continue
            t, sep, c = k.partition('.')
            if (sep and t.lower() == tl and (cl is None or c.lower() == cl)
                    and (tbl is None or self._find_col_idx(tbl['columns'], c) >= 0)):
                return True
        return False

    def is_cross_filtered(self, table: str) -> bool:
        """ISCROSSFILTERED's test: anything filters ``table`` -- a direct
        filter on any of its columns (is_directly_filtered), a filter on
        column combinations that reaches it through its expanded table, or a
        filter on another table that a relationship carries to it (one to
        many, both ways, or a table filter's many to one). Desktop 2.152
        answers the column form exactly as the table form: a filter on
        Orders[Region] cross-filters Orders[Revenue] (issue #113)."""
        if self.is_directly_filtered(table):
            return True
        tbl = self.tables.get(table)
        if not tbl:
            return False
        for k, v in self.filter_context.items():
            if (k.startswith(_TUPLE_FILTER_PREFIX)
                    and self._tuple_filter_indices(table, tbl, v) is not None):
                return True
        return bool(self._get_cross_table_filters(table))

    def without_columns(self, drop, keep_keys=()) -> 'DAXContext':
        """This context (always a NEW one) without the filters on the columns
        ``drop(table, column)`` selects; filters keyed in ``keep_keys`` stay.

        A plain column filter goes. A filter on column COMBINATIONS (TREATAS
        onto several columns) loses those columns and keeps its projection
        onto the others, as DAX does; it goes when none is left.
        """
        keys: list = []
        projected: dict = {}
        for k, v in self.filter_context.items():
            if k in keep_keys:
                continue
            if k.startswith(_TUPLE_FILTER_PREFIX):
                cols = v.get('tuple_columns') or []
                keep = [i for i, (t, c) in enumerate(cols) if not drop(t, c)]
                if len(keep) < len(cols):
                    keys.append(k)
                    if keep:
                        spec = _project_tuple_filter(v, keep)
                        pk = _tuple_filter_key(spec['tuple_columns'])
                        projected[pk] = (_intersect_tuple_filters(projected[pk], spec)
                                         if pk in projected else spec)
                continue
            t, sep, c = k.partition('.')
            if sep and drop(t, c):
                keys.append(k)
        ctx = self.without_filters(keys)
        if projected:
            for pk, spec in list(projected.items()):
                live = ctx.filter_context.get(pk)
                if live is not None:
                    projected[pk] = _intersect_tuple_filters(live, spec)
            ctx = ctx.with_filters(projected)
        return ctx

    def _all_snapshot(self, table: str, expanded: frozenset,
                      keep: frozenset | set = frozenset()) -> dict:
        """The live filters ALL(table) must still keep away from ``table``
        after the filters on its expanded table are gone (see _no_prop_keys).

        Two kinds only. A TABLE filter argument on a many-side table
        (FILTER(Employee, ...), which may restrict ``table`` many -> one):
        ALL takes ``table``'s columns out of it. And a filter on the date
        table where no relationship joins it to ``table`` (the
        _get_date_cross_filter path). Every other filter stays in force: one
        on a table outside the expanded table still reaches ``table`` through
        a many-to-many or a bidirectional relationship -- Desktop 2.152 keeps
        the Tags[TagName] grouping under ALL(Fact) (27 / 18, not 45), and
        COUNTROWS(Dim) under ALL(Dim) still sees the Fact2 filter that crosses
        a both-ways Fact2 -> Dim relationship (2, not 3).
        """
        snap = {}
        for k, v in self.filter_context.items():
            if k in keep:
                continue
            t, sep, _c = k.partition('.')
            if not sep or t in expanded:
                continue
            if k in self._expanded_keys or (
                    t == self.date_table and not self._rel_dir.get((table, t))):
                try:
                    snap[k] = self._filter_sig(v)
                except TypeError:
                    snap[k] = None
        return snap

    def _get_cross_table_filters(self, table_name: str) -> list:
        """
        Get ALL cross-table filters that apply to a target table.
        Uses model relationships to propagate dimension filters to fact tables.
        Returns list of (allowed_values_set, fact_col_idx) tuples.

        Memoized on the shared cache: this walks the relationship graph and reads
        intermediate tables, and every get_column_data / get_filtered_rows call
        asked for it again. One measure evaluation asks ~7 times, so an iterator
        over a dimension paid for the same propagation hundreds of times.
        """
        if not self.filter_context:
            return []
        # ALL(Table) / REMOVEFILTERS(Table) inside CALCULATE. Dropping only the
        # DIRECT `Table.col` keys is not enough: a filter on a related dimension
        # reaches this table through the relationship, and that propagated
        # filter IS a filter on this table's columns, so ALL has to stop it too.
        # MS_Covid_Tracking's `CALCULATE(MAX('COVID'[Date]), ALL('COVID'))`
        # returned BLANK under a StateDim slice that matches no COVID row, where
        # Desktop returns the global max.
        #
        # But it stops only the filters that were LIVE when ALL ran. Blanking
        # the table for the rest of the evaluation also blocked filters created
        # LATER, inside a nested CALCULATE, which Desktop keeps:
        #   CALCULATE(CALCULATE(AVERAGE('Cases'[CSAT]),
        #                       'Owners'[Manager]="Low, Spencer"), ALL('Cases'))
        #   Desktop 4.13796627491058, we returned the global 4.2706; the nested
        #   COUNTROWS was 3914 in Desktop and 10000 here.
        # `_no_prop_keys` carries that snapshot, and the loop below skips
        # exactly those source keys.
        if (table_name in self._no_propagate
                and not self._no_prop_keys.get(table_name)):
            # ALL applied with nothing live to suppress: later filters still
            # propagate, so fall through rather than blanket-blocking.
            pass

        tbl = self.tables.get(table_name)
        if not tbl:
            return []

        ck = None
        try:
            if self._rels_sig is None:
                raise TypeError
            ck = (id(tbl), 'xtf', tuple(sorted(
                (k, self._filter_sig(v)) for k, v in self.filter_context.items())),
                tuple(sorted((self._no_prop_keys.get(table_name) or {}).items())),
                tuple(sorted(k for k in self._expanded_keys
                             if k in self.filter_context)),
                self._rels_sig)
        except TypeError:
            ck = None
        if ck is not None:
            hit = self._filter_idx_cache.get(ck)
            if hit is not None:
                return list(hit)
            out = self._get_cross_table_filters_uncached(table_name, tbl)
            self._filter_idx_cache[ck] = out
            return out
        return self._get_cross_table_filters_uncached(table_name, tbl)

    def _get_cross_table_filters_uncached(self, table_name: str, tbl: dict) -> list:

        result_filters = []

        # Group filter context entries by source table
        table_filters: dict = {}
        # Filters this table's ALL() was clearing. Skipping them here (rather
        # than blanking the whole table) is what lets a filter created LATER in
        # a nested CALCULATE still reach this table, which is what Desktop does.
        _suppressed = self._no_prop_keys.get(table_name) or {}
        for fk, values in self.filter_context.items():
            # Suppress only if this key still holds the SAME filter ALL cleared.
            # Re-filtering the column inside a nested CALCULATE makes a NEW
            # filter, and Desktop lets it through: under an outer
            # Owners[Manager]="Weiler, Anne", an inner "Low, Spencer" inside
            # ALL('Cases') gives Spencer's 4.13796627491058 / 3914 rows, not
            # Anne's and not the global.
            if fk in _suppressed and _suppressed[fk] == self._filter_sig(values):
                continue
            parts = fk.split('.', 1)
            if len(parts) == 2:
                src_table, src_col = parts
                if src_table != table_name:  # Only cross-table filters
                    if src_table not in table_filters:
                        table_filters[src_table] = []
                    table_filters[src_table].append((src_col, values))

        for src_table, col_filters in table_filters.items():
            src_tbl = self.tables.get(src_table)
            if not src_tbl:
                continue

            # Find relationship between source dim table and target table.
            # Directional first (one -> many, plus bidirectional). The reverse
            # direction -- the MANY side restricting the ONE side -- is DAX's
            # expanded-table behaviour and is taken only for keys a TABLE
            # filter argument registered: FILTER(Employee, ...) restricts Date
            # ([Actives] = 32,401 needs exactly that), while a column filter
            # like DimStore[StoreType]="Catalog" must NOT reach DimEmployee
            # (Desktop: COUNTROWS(DimEmployee) stays 293, SELECTEDVALUE BLANK).
            rel = self._rel_dir.get((table_name, src_table))
            if not rel:
                expanded = [cf for cf in col_filters
                            if f"{src_table}.{cf[0]}" in self._expanded_keys]
                if expanded:
                    rel = self._rel_index.get((table_name, src_table))
                    if rel:
                        col_filters = expanded
            if not rel:
                # Try via date table special handling (for Year/Month filters on date dim)
                if src_table == self.date_table:
                    result_filters.extend(self._get_date_cross_filter(table_name, src_tbl, col_filters))
                    continue
                # No direct relationship: try a multi-hop (snowflake) path so a
                # filter on a dimension 2+ hops from the fact is not silently
                # dropped (e.g. Regions -> Customers -> Orders).
                path = self._find_rel_path(src_table, table_name)
                if path:
                    res = self._propagate_filter_path(src_tbl, col_filters, path, table_name)
                    # res is None only on a structural error; an (empty) set is a
                    # real result and MUST be applied so the fact filters to zero
                    # rows rather than falling back to the grand total.
                    if res is not None:
                        result_filters.append(res)
                continue

            # Direct relationship exists: filter source dim table, get join key values
            fact_join_col = rel['from_col']  # Column in target (fact) table
            dim_join_col = rel['to_col']     # Column in source (dim) table

            fact_join_idx = self._find_col_idx(tbl['columns'], fact_join_col)
            dim_join_idx = self._find_col_idx(src_tbl['columns'], dim_join_col)
            if fact_join_idx < 0 or dim_join_idx < 0:
                continue

            # Filter dim table rows by all filters on that table
            filtered_dim_rows, applied = self._rows_where(src_tbl, col_filters)
            # The source's blank row survives when every filter on it keeps
            # BLANK; it then selects the target rows no source row matches.
            src_blank = (src_table in self.blank_row_tables()
                         and self._filters_admit_blank(applied))

            # Get allowed join key values. Append even an EMPTY set: an empty
            # dimension selection must filter the fact to zero rows (BLANK), not
            # be dropped (which would leave the fact unfiltered -> grand total).
            # Mirrors the multi-hop path's empty-set handling.
            allowed_keys = self._hop_keys(src_table, filtered_dim_rows, src_blank,
                                          dim_join_idx, table_name, fact_join_idx)
            result_filters.append((allowed_keys, fact_join_idx))

        return result_filters

    def _find_rel_path(self, src_table: str, target_table: str) -> Optional[list]:
        """BFS the directed relationship graph for a filter-propagation path.

        Returns the shortest list of hops [(cur, nxt, col_on_cur, col_on_nxt), ...]
        from src_table to target_table, or None if none exists. Direction is
        enforced by _rel_adj (one->many by default), so a filter cannot leak
        across a shared fact to a sibling dimension.
        """
        if src_table == target_table:
            return None
        visited = {src_table}
        # queue of (table, path_so_far); small graphs, list-as-queue is fine
        queue: list = [(src_table, [])]
        while queue:
            cur, path = queue.pop(0)
            for (nxt, col_cur, col_nxt) in self._rel_adj.get(cur, []):
                if nxt in visited:
                    continue
                new_path = path + [(cur, nxt, col_cur, col_nxt)]
                if nxt == target_table:
                    return new_path
                visited.add(nxt)
                queue.append((nxt, new_path))
        return None

    def _propagate_filter_path(self, src_tbl, col_filters, path, target_table):
        """Propagate a dimension filter along a multi-hop path to the fact table.

        Returns (allowed_key_set, target_col_idx) to apply to target_table, or
        None on a structural error (missing column/table). The returned set may
        be empty — that is a legitimate result (no matching rows) and the caller
        must apply it rather than dropping the filter.
        """
        # Start from the source dimension rows filtered by its own column filters.
        frontier_rows, applied = self._rows_where(src_tbl, col_filters)
        # The blank row travels hop by hop (see _hop_keys): the source's survives
        # when its filters keep BLANK, and the next table's when the hop's keys
        # admit it. A fact row two hops down an unmatched key therefore lands on
        # the blank row of every table above it (issue #82).
        cur_name = path[0][0] if path else ''
        cur_blank = (cur_name in self.blank_row_tables()
                     and self._filters_admit_blank(applied))
        frontier_tbl = src_tbl
        for (cur_name, nxt_name, col_cur, col_nxt) in path:
            cur_idx = self._find_col_idx(frontier_tbl['columns'], col_cur)
            if cur_idx < 0:
                return None
            nxt_tbl = self.tables.get(nxt_name)
            if not nxt_tbl:
                return None
            nxt_idx = self._find_col_idx(nxt_tbl['columns'], col_nxt)
            if nxt_idx < 0:
                return None
            allowed_keys = self._hop_keys(cur_name, frontier_rows, cur_blank,
                                          cur_idx, nxt_name, nxt_idx)
            if nxt_name == target_table:
                # Final hop: emit the filter for the fact table (empty set is OK).
                return (allowed_keys, nxt_idx)
            # Intermediate hop: restrict the next table's rows and continue.
            frontier_rows = [r for r in nxt_tbl['rows'] if _key_in(r[nxt_idx], allowed_keys)]
            cur_blank = (nxt_name in self.blank_row_tables()
                         and 'None' in allowed_keys)
            frontier_tbl = nxt_tbl
        return None

    def _get_date_cross_filter(self, table_name, date_tbl, col_filters):
        """Handle date dimension filters (Year, Month etc.) that need Date column resolution."""
        date_cols = date_tbl['columns']
        date_col_idx = self._find_col_idx(date_cols, self.date_column)
        if date_col_idx < 0:
            return []

        # Filter date rows
        filtered_rows, _applied = self._rows_where(date_tbl, col_filters)

        # An empty allowed_dates set is a legitimate empty selection (filter the
        # fact to zero rows -> BLANK), NOT a reason to drop the filter and leak
        # the grand total; it flows through to the returns below unchanged.
        allowed_dates = set(str(r[date_col_idx]) for r in filtered_rows)

        # Find date column in fact table via relationship or heuristic
        tbl = self.tables.get(table_name)
        if not tbl:
            return []

        # Try relationship first
        rel = self._rel_index.get((table_name, self.date_table))
        if rel:
            fact_date_idx = self._find_col_idx(tbl['columns'], rel['from_col'])
            if fact_date_idx >= 0:
                return [(allowed_dates, fact_date_idx)]

        # Fallback: heuristic date column names
        for dcn in ['Order Date', 'Date', 'OrderDate', 'Transaction Date']:
            fact_date_idx = self._find_col_idx(tbl['columns'], dcn)
            if fact_date_idx >= 0:
                return [(allowed_dates, fact_date_idx)]

        return []

    @property
    def filter_context(self) -> dict:
        """The filter set this context evaluates under.

        ASSIGNING this is supported and clears the per-context column memo, so
        reusing one context across groupings works:

            ctx.filter_context = {"Categories.CategoryName": ["Books"]}

        MUTATING the dict in place (``ctx.filter_context[k] = v``) is NOT
        supported -- no memo can observe that, and stale values are served.
        Assign a new dict, or use :meth:`with_filters` / :meth:`without_filters`
        to derive a fresh context.
        """
        return self._filter_context

    @filter_context.setter
    def filter_context(self, value: Optional[dict]) -> None:
        self._filter_context = value or {}
        # Everything memoized under the previous filter set is now invalid.
        # OpenBI findings #18: a caller re-pointed ONE context per grouping, and
        # every grouping after the first got the FIRST grouping's members back --
        # silently, with the parent subtotals still reconciling, which is exactly
        # the combination that survives review. _measure_cache is deliberately
        # NOT cleared: its key already carries a filter-context fingerprint, so
        # its entries stay valid (and re-pointing back keeps the fast path).
        # __init__ assigns before the cache exists, hence the guard.
        cache = getattr(self, "_column_data_cache", None)
        if cache:
            cache.clear()

    def get_column_data(self, table_name: str, column_name: str) -> list:
        """Get all values for a column, respecting current filter context.

        Memoized per (table, column) for the CURRENT filter set -- assigning
        :attr:`filter_context` invalidates it. Do NOT mutate the returned list.
        """
        _ck = (table_name, column_name)
        _hit: list | None = self._column_data_cache.get(_ck)
        if _hit is not None:
            return _hit
        out = self._get_column_data_uncached(table_name, column_name)
        self._column_data_cache[_ck] = out
        return out

    @staticmethod
    def _filter_sig(allowed):
        """A hashable signature for one filter's value set, or None when it is
        too large to be worth hashing (the scan is then done uncached)."""
        if isinstance(allowed, dict):
            try:
                return ('d', json.dumps(allowed, sort_keys=True, default=str))
            except (TypeError, ValueError):
                return None
        if isinstance(allowed, (list, tuple, set, frozenset)):
            if len(allowed) > 4096:
                return None
            try:
                return ('l', tuple(sorted(str(v) for v in allowed)))
            except TypeError:
                return None
        return ('s', str(allowed))

    def _value_index_map(self, tbl: dict, col_idx: int) -> dict:
        """``{value-key: frozenset(row indices)}`` for one column, built once.

        Keyed by ``str(value)`` and, for a date-ish value, additionally by its ISO
        date, so the date-aware In-set semantics survive the index lookup: a
        filter carrying '2009-12-01' still finds cells holding
        ``datetime(2009, 12, 1)``, whose str() is '2009-12-01 00:00:00'.
        """
        key = (id(tbl), 'vmap', col_idx)
        vmap = self._filter_idx_cache.get(key)
        if vmap is None:
            rows = tbl['rows']
            # Only pay for the ISO alias on a column that actually holds dates.
            # _as_date on a plain string tries five strptime formats, so doing it
            # per cell of a 200k-row text column costs more than the scan it is
            # meant to replace. Sample first.
            dateish = False
            for row in rows[:64]:
                v = row[col_idx]
                if v is None:
                    continue
                if isinstance(v, (datetime, date)):
                    dateish = True
                elif isinstance(v, str):
                    dateish = _as_date_str_strict(v.strip()) is not None
                break
            acc: dict = {}
            if dateish:
                for i, row in enumerate(rows):
                    v = row[col_idx]
                    sv = str(v)
                    acc.setdefault(sv, []).append(i)
                    # STRICT parse (issue #42): a compound key with a date
                    # PREFIX must not alias to the bare date, or every key
                    # sharing that prefix merges into one bucket. And only a
                    # MIDNIGHT value is its bare date: a 12:30 departure is not
                    # that day's key through an exact join (issue #126; a
                    # DatePartOnly join uses _date_part_index_map instead).
                    dv = _midnight_date(v)
                    if dv is not None:
                        iso = dv.isoformat()
                        if iso != sv:
                            acc.setdefault(iso, []).append(i)
            else:
                for i, row in enumerate(rows):
                    acc.setdefault(str(row[col_idx]), []).append(i)
            vmap = {k: frozenset(v) for k, v in acc.items()}
            self._filter_idx_cache[key] = vmap
        return vmap

    def _date_part_index_map(self, tbl: dict, col_idx: int) -> dict:
        """``{date part key: frozenset(row indices)}`` for one column -- the
        lookup across a DatePartOnly join, where every time of a day is that
        day ('dp:YYYY-MM-DD'; a blank cell is 'None'). Issue #126."""
        key = (id(tbl), 'dpmap', col_idx)
        dmap = self._filter_idx_cache.get(key)
        if dmap is None:
            acc: dict = {}
            for i, row in enumerate(tbl['rows']):
                v = row[col_idx]
                k = (_date_part_key(v) or str(v)) if v is not None else 'None'
                acc.setdefault(k, []).append(i)
            dmap = {k: frozenset(v) for k, v in acc.items()}
            self._filter_idx_cache[key] = dmap
        return dmap

    def _indices_for_values(self, tbl: dict, col_idx: int, allowed):
        """Row indices matching an In-SET filter, via the column's value map --
        dict lookups instead of a scan. None when the filter is not a plain set
        (a structured predicate), so the caller falls back to the matcher.

        This is what makes an iterator over a dimension affordable: RANKX walking
        261 employees used to trigger 261 separate 200k-row scans of the fact
        table, one per employee's propagated key set. Now the fact table is
        traversed ONCE per join column and each employee costs a few lookups.
        """
        if isinstance(allowed, dict):
            return None
        if isinstance(allowed, _DatePartKeys):
            # Across a DatePartOnly join the keys ARE date parts (issue #126).
            dmap = self._date_part_index_map(tbl, col_idx)
            found = [dmap[k] for k in allowed if k in dmap]
            if not found:
                return frozenset()
            return found[0] if len(found) == 1 else frozenset().union(*found)
        values = allowed if isinstance(allowed, (list, tuple, set, frozenset)) \
            else [allowed]
        vmap = self._value_index_map(tbl, col_idx)
        keys = set()
        for v in values:
            keys.add(str(v))
            # strict: no date-prefix aliasing (#42); midnight only (#126)
            dv = _midnight_date(v)
            if dv is not None:
                keys.add(dv.isoformat())
            # Numeric alternates (issue #39): the map is keyed str(cell), and
            # str(2024) != str(2024.0), so an int literal missed every Double
            # cell (and vice versa) — CALCULATE(expr, T[Year]=2024) selected
            # zero rows and went BLANK. Add both spellings; bools excluded so
            # True never aliases 1.
            if not isinstance(v, bool):
                fv = None
                if isinstance(v, (int, float)):
                    fv = float(v)
                elif isinstance(v, str):
                    try:
                        fv = float(v)
                    except ValueError:
                        pass
                if fv is not None:
                    keys.add(str(fv))
                    if fv.is_integer():
                        keys.add(str(int(fv)))
        found = [vmap[k] for k in keys if k in vmap]
        if not found:
            return frozenset()
        if len(found) == 1:
            return found[0]
        # Accumulate into ONE mutable set. frozenset().union(*found) allocates a
        # new frozenset per operand, which is the dominant cost when a date
        # filter carries ~1000 single-value index sets.
        acc: set = set()
        for f in found:
            acc |= f
        return frozenset(acc)

    def _typed_index_map(self, tbl: dict, col_idx: int) -> dict:
        """``{value key: frozenset(row indices)}`` for one column, keyed as DAX
        compares values (_value_keys), built once. It serves a filter on the
        column ITSELF; relationship joins keep the str()-keyed map above,
        whose spellings the join machinery produces."""
        key = (id(tbl), 'tmap', col_idx)
        tmap = self._filter_idx_cache.get(key)
        if tmap is None:
            rows = tbl['rows']
            # A text column's cells are dates only when the column keeps dates
            # as text; sampling saves a date parse per cell of a code column.
            dateish = False
            for row in rows[:64]:
                v = row[col_idx]
                if v is None:
                    continue
                dateish = isinstance(v, (datetime, date)) or (
                    isinstance(v, str) and _as_moment_str(v.strip()) is not None)
                break
            acc: dict = {}
            for i, row in enumerate(rows):
                for k in _value_keys(row[col_idx], dateish):
                    acc.setdefault(k, []).append(i)
            tmap = {k: frozenset(v) for k, v in acc.items()}
            self._filter_idx_cache[key] = tmap
        return tmap

    def _indices_for_column_filter(self, tbl: dict, col_idx: int, allowed):
        """Row indices an In-SET filter on the column itself selects, as DAX
        compares values (see _filter_value_keys); None for a structured
        predicate (the caller uses make_value_matcher)."""
        if isinstance(allowed, dict):
            return None
        values = allowed if isinstance(allowed, (list, tuple, set, frozenset)) \
            else [allowed]
        tmap = self._typed_index_map(tbl, col_idx)
        keys: set = set()
        for v in values:
            keys |= _filter_value_keys(v)
        found = [tmap[k] for k in keys if k in tmap]
        if not found:
            return frozenset()
        if len(found) == 1:
            return found[0]
        acc: set = set()
        for f in found:
            acc |= f
        return frozenset(acc)

    def _rows_where(self, tbl: dict, col_filters) -> tuple:
        """``(rows, applied)``: the rows of ``tbl`` that every ``(column,
        values)`` filter keeps, in row order, and the filters that applied.

        An In-set goes through the column's typed index (one lookup per
        value), a predicate through make_value_matcher (one cached scan). The
        dimension side of a relationship used to be filtered row by row with
        the matcher, once per propagation: a TOPN over 1,850 products made
        3.4 million matcher calls."""
        rows = tbl['rows']
        keep = None
        applied = []
        for col, values in col_filters:
            idx = self._find_col_idx(tbl['columns'], col)
            if idx < 0:
                continue
            hit = self._indices_for_column_filter(tbl, idx, values)
            if hit is None:
                sig = self._filter_sig(values)
                ck = (id(tbl), 'd', idx, sig) if sig is not None else None
                hit = self._filter_idx_cache.get(ck) if ck is not None else None
                if hit is None:
                    _m = make_value_matcher(values)
                    hit = frozenset(i for i, r in enumerate(rows) if _m(r[idx]))
                    if ck is not None:
                        self._filter_idx_cache[ck] = hit
            keep = hit if keep is None else keep & hit
            applied.append((col, values))
        if keep is None:
            return rows, applied
        return [rows[i] for i in sorted(keep)], applied

    def _tuple_column_getter(self, table_name: str, t: str, c: str):
        """``row of table_name -> its value of t[c]``, when t[c] is in
        table_name's EXPANDED table (its own column, or one reached through
        the relationships the expanded table follows, by key); None if not.
        A key matching no row reads BLANK (DAX's blank row)."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return None
        if table_name == t:
            idx = self._find_col_idx(tbl['columns'], c)
            return (lambda row, i=idx: row[i]) if idx >= 0 else None
        # Breadth-first over the expansion edges: the shortest path to t.
        prev: dict[str, str | None] = {table_name: None}
        todo = [table_name]
        while todo and t not in prev:
            nxt_todo = []
            for cur in todo:
                for nxt in self._expand_adj.get(cur, ()):
                    if nxt not in prev:
                        prev[nxt] = cur
                        nxt_todo.append(nxt)
            todo = nxt_todo
        if t not in prev:
            return None
        path = [t]
        back = prev[t]
        while back is not None:
            path.append(back)
            back = prev[back]
        path.reverse()
        ttbl = self.tables.get(t)
        cidx = self._find_col_idx(ttbl['columns'], c) if ttbl else -1
        if cidx < 0:
            return None
        hops = []
        for a, b in zip(path, path[1:]):
            rel = self._rel_index.get((a, b))
            atbl, btbl = self.tables.get(a), self.tables.get(b)
            if not rel or not atbl or not btbl:
                return None
            ai = self._find_col_idx(atbl['columns'], rel['from_col'])
            bi = self._find_col_idx(btbl['columns'], rel['to_col'])
            if ai < 0 or bi < 0:
                return None
            by_key: dict = {}
            for r in btbl['rows']:
                by_key.setdefault(_value_keys(r[bi], False)[0], r)
            hops.append((ai, by_key))

        def _get(row):
            for ai, by_key in hops:
                row = by_key.get(_value_keys(row[ai], False)[0])
                if row is None:
                    return None
            return row[cidx]
        return _get

    def _tuple_filter_indices(self, table_name: str, tbl: dict, spec: dict):
        """Rows of ``tbl`` a filter on column combinations keeps, or None when
        it does not reach this table.

        A table sees the combination's columns that are in its expanded table
        and keeps a row when those values match some combination's values for
        the same columns -- the projection. Desktop 2.152, TREATAS({("N", "A"),
        ("W", "B")}, Region[Region], Prod[Cat]): Sales keeps the N-A and W-B
        rows (9), COUNTROWS(Region) is 2 and COUNTROWS(Prod) is 2."""
        cols = [tuple(c) for c in spec.get('tuple_columns') or []]
        sig = self._filter_sig(spec)
        ck = ((id(tbl), 'tuple', table_name, self._rels_sig, sig)
              if sig is not None and self._rels_sig is not None else None)
        if ck is not None:
            hit = self._filter_idx_cache.get(ck, _MISSING)
            if hit is not _MISSING:
                return hit
        getters, positions = [], []
        for i, (t, c) in enumerate(cols):
            g = self._tuple_column_getter(table_name, t, c)
            if g is not None:
                getters.append(g)
                positions.append(i)
        out = None
        if getters:
            keep = {tuple(_value_keys(r[i], False)[0] for i in positions)
                    for r in spec.get('rows') or []}
            out = frozenset(
                i for i, row in enumerate(tbl['rows'])
                if tuple(_value_keys(g(row), False)[0] for g in getters) in keep)
        if ck is not None:
            self._filter_idx_cache[ck] = out
        return out

    def _surviving_indices(self, table_name: str, tbl: dict):
        """Indices of ``tbl['rows']`` that satisfy the filter context, or None
        when nothing filters this table.

        Each individual filter's index set is cached and the sets are
        INTERSECTED. That matters because sibling contexts usually differ in ONE
        filter: RANKX iterating 261 employees re-applied the identical 31-date
        set to a 200k-row fact table 261 times, ~31 ms each. Now the date set is
        scanned once and reused, and only the employee predicate is new.
        The cache is shared down the derivation tree (see with_filters).
        """
        cols = tbl['columns']
        rows = tbl['rows']
        cache = self._filter_idx_cache
        sets = []
        for fk, allowed in self.filter_context.items():
            if fk.startswith(_TUPLE_FILTER_PREFIX):
                hit = self._tuple_filter_indices(table_name, tbl, allowed)
                if hit is not None:
                    sets.append(hit)
                continue
            parts = fk.split('.', 1)
            if len(parts) != 2 or parts[0] != table_name:
                continue
            filt_idx = self._find_col_idx(cols, parts[1])
            if filt_idx < 0:
                continue
            hit = self._indices_for_column_filter(tbl, filt_idx, allowed)
            if hit is None:
                sig = self._filter_sig(allowed)
                key = (id(tbl), 'd', filt_idx, sig) if sig is not None else None
                hit = cache.get(key) if key is not None else None
                if hit is None:
                    _m = make_value_matcher(allowed)
                    hit = frozenset(i for i, row in enumerate(rows)
                                    if _m(row[filt_idx]))
                    if key is not None:
                        cache[key] = hit
            sets.append(hit)
        for allowed_vals, join_idx in self._get_cross_table_filters(table_name):
            hit = self._indices_for_values(tbl, join_idx, allowed_vals)
            if hit is None:
                sig = self._filter_sig(allowed_vals)
                key = (id(tbl), 'x', join_idx, sig) if sig is not None else None
                hit = cache.get(key) if key is not None else None
                if hit is None:
                    hit = frozenset(i for i, row in enumerate(rows)
                                    if str(row[join_idx]) in allowed_vals)
                    if key is not None:
                        cache[key] = hit
            sets.append(hit)
        if not sets:
            return None
        sets.sort(key=len)
        keep = sets[0]
        for s in sets[1:]:
            keep &= s
            if not keep:
                break
        return keep

    def _get_column_data_uncached(self, table_name: str, column_name: str) -> list:
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        cols = tbl['columns']
        col_idx = self._find_col_idx(cols, column_name)
        if col_idx < 0:
            return []

        rows = tbl['rows']
        keep = self._surviving_indices(table_name, tbl)
        if keep is None:
            return [row[col_idx] for row in rows]
        return [rows[i][col_idx] for i in sorted(keep)]

    def get_filtered_rows(self, table_name: str) -> list:
        """Get rows of a table after applying filter context."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        rows = tbl['rows']
        keep = self._surviving_indices(table_name, tbl)
        if keep is None:
            return rows
        return [rows[i] for i in sorted(keep)]

    def with_filters(self, extra_filters: dict) -> 'DAXContext':
        """Create a new context with additional filters applied."""
        new_filters = dict(self.filter_context)
        new_filters.update(extra_filters)
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        ctx._filter_idx_cache = self._filter_idx_cache
        ctx._no_propagate = set(self._no_propagate)
        ctx._no_prop_keys = dict(self._no_prop_keys)
        ctx._expanded_keys = set(self._expanded_keys)
        ctx.group_keys = set(self.group_keys)
        ctx.culture = self.culture
        ctx.selected_filters = (None if self.selected_filters is None
                                else dict(self.selected_filters))
        ctx.measure_tables = self.measure_tables
        ctx.model_columns = self.model_columns
        # Share the measure memo by REFERENCE across the derivation family. Its
        # key already carries a filter-context fingerprint, so entries are
        # scoped to the context that produced them and cannot leak between
        # siblings. It used to be reset to {} here, which meant RANKX evaluating
        # the same measure for the same 261 employees in two different VARs paid
        # for all of it twice.
        ctx._measure_cache = self._measure_cache
        ctx._measure_cache_unsupported = self._measure_cache_unsupported
        ctx._shield_cache = self._shield_cache
        return ctx

    def with_relationships(self, relationships: list) -> 'DAXContext':
        """The same evaluation over a different relationship set
        (USERELATIONSHIP, CROSSFILTER).

        It used to be a bare DAXContext, which dropped the grouping, the marked
        date tables, the slicer selection, the culture, the measure home
        tables and the ALL snapshots, so ISINSCOPE, ALLSELECTED and friends
        answered for a different query (issue #83). The measure memo is NOT
        shared: its key does not carry the relationships.
        """
        ctx = self.with_filters({})
        fresh = DAXContext(self.tables, self.measures, self.date_table,
                           self.date_column, self.filter_context, relationships)
        for attr in ('relationships', '_rel_index', '_rel_dir', '_rel_adj',
                     '_one_side_of', '_rels_sig', '_expand_adj'):
            setattr(ctx, attr, getattr(fresh, attr))
        ctx.date_tables = dict(self.date_tables)
        ctx._measure_cache = {}
        ctx._measure_cache_unsupported = {}
        return ctx

    def without_filters(self, keys: list) -> 'DAXContext':
        """Create a new context with specified filters removed."""
        new_filters = {k: v for k, v in self.filter_context.items() if k not in keys}
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        ctx._filter_idx_cache = self._filter_idx_cache
        ctx._no_propagate = set(self._no_propagate)
        ctx._no_prop_keys = dict(self._no_prop_keys)
        ctx._expanded_keys = set(self._expanded_keys)
        ctx.group_keys = set(self.group_keys)
        ctx.culture = self.culture
        ctx.selected_filters = (None if self.selected_filters is None
                                else dict(self.selected_filters))
        ctx.measure_tables = self.measure_tables
        ctx.model_columns = self.model_columns
        return ctx


class DAXEngine:
    """Evaluates DAX expressions."""

    def __init__(self):
        self._current_var_scope = None  # Active variable scope during VAR/RETURN eval
        self.unsupported_functions: set[str] = set()  # Track unsupported DAX functions hit
        # The same, per MEASURE (PR #96): a function hit while evaluating M is
        # recorded for M and for every measure whose evaluation reached M, so
        # a caller evaluating several measures at once can tell which result
        # depended on one. _measure_path is the chain being evaluated, each
        # with the set its own evaluation hit (kept with its measure-cache
        # entry, so a value served from the cache still carries it).
        self.unsupported_by_measure: dict[str, set[str]] = {}
        self._measure_path: list[tuple[str, set]] = []
        # Measures abandoned on the wall-clock budget. They return BLANK like
        # any other failure, and BLANK is ALSO what a legitimately empty measure
        # returns -- so without this the caller cannot tell "no value" from "we
        # gave up", which is the one distinction that matters when the number is
        # going into a report.
        self.timed_out: set[str] = set()
        # Measures whose evaluation RAISED (unresolvable reference, circular
        # definition, ...). They also surface as BLANK values, and without
        # this record the caller cannot tell "genuinely blank" from "could
        # not be evaluated at all" (ledger issues-7).
        self.eval_errors: dict[str, str] = {}
        # Wall-clock budget per outermost measure, enforced on the ENGINE (not
        # the context) because iterators spawn a fresh sub-context per row —
        # a context-local timer/counter would reset every row and never fire.
        # _eval_depth tracks true measure nesting so the deadline is set once at
        # the top and shared across every sub-context; _time_counter is a global
        # throttle so the time check runs regardless of which context is active.
        try:
            # A HANG guard, not a performance target. At 20s legitimate measures
            # were being cut off mid-flight and, worse, the partial result
            # surfaced as a NUMBER: Agents_Performance
            # "Employees Avg MTD % change PM" returned 0.2808 where Desktop
            # gives 0.2438, and "Number of Employees with Positive change PM"
            # returned 0.0 for 0.3754. Desktop answers these instantly; this
            # engine needs ~20-25s, so the cap has to clear that comfortably.
            self._max_eval_seconds = float(
                os.environ.get("PBIX_DAX_MAX_SECONDS", "300"))
        except (TypeError, ValueError):
            self._max_eval_seconds = 20.0
        self._deadline = None
        self._eval_depth = 0
        self._time_counter = 0
        # Calculation groups (issue #121): group -> the selection whose item
        # was applied on the current evaluation path, and the measures the
        # items being evaluated apply to (SELECTEDMEASURE's measure).
        self._cg_applied: dict = {}
        self._cg_frames: list[str] = []
        # Measure errors degraded to BLANK so far: IFERROR / ISERROR compare it
        # before and after their argument (issue #135).
        self._nested_errors = 0
        # (id(rows), table, column) -> (rows, distinct values): an iteration's
        # shadow per column, for ALLSELECTED and the measure memo (#118).
        self._shadow_cache: dict = {}
        self._shadow_tokens: dict = {}       # shadow fingerprint -> number
        self._shadow_next = 0
        # Bare `[Column]` -> owning table, memoized per model (see
        # _resolve_bare_column): the lookup scans every table's column list and
        # runs inside per-row iteration.
        self._bare_col_cache: dict = {}
        # `T[C].[Part]` -> `'LocalDateTable_x'[Part]`, memoized per expression.
        self._variation_cache: dict = {}
        # Filter context of the OUTERMOST measure -- the query/slicer selection
        # that ALLSELECTED restores (see _selected_ctx).
        self._query_filters: dict | None = None
        # The subset of that snapshot injected by GROUPED evaluation (the
        # visual-row grouping). ALLSELECTED drops these, keeps the rest.
        self._group_keys: set = set()
        # The caller's true slicer state, when grouped evaluation passes it.
        self._selected_base: dict | None = None
        # Home tables of the measures currently on the evaluation stack; the
        # innermost one disambiguates a bare [Column] inside that measure.
        self._home_tables: list = []
        self._func_map = {
            # --- Aggregation ---
            'SUM': self._fn_sum,
            'AVERAGE': self._fn_average,
            'COUNT': self._fn_count,
            'COUNTROWS': self._fn_countrows,
            'MIN': self._fn_min,
            'MAX': self._fn_max,
            'DISTINCTCOUNT': self._fn_distinctcount,
            'DISTINCTCOUNTNOBLANK': self._fn_distinctcountnoblank,
            'COUNTA': self._fn_counta,
            'PRODUCT': self._fn_product,
            'MEDIAN': self._fn_median,
            'MEDIANX': self._fn_medianx,
            # --- Iteration ---
            'SUMX': self._fn_sumx,
            'MAXX': self._fn_maxx,
            'MINX': self._fn_minx,
            'AVERAGEX': self._fn_averagex,
            'COUNTX': self._fn_countx,
            'COUNTAX': self._fn_countax,
            'COUNTBLANK': self._fn_countblank,
            # --- Math ---
            'DIVIDE': self._fn_divide,
            'ABS': self._fn_abs,
            'ROUND': self._fn_round,
            'MROUND': self._fn_mround,
            'ROUNDDOWN': self._fn_rounddown,
            'ROUNDUP': self._fn_roundup,
            'INT': self._fn_int,
            'CEILING': self._fn_ceiling,
            'FLOOR': self._fn_floor,
            'MOD': self._fn_mod,
            'POWER': self._fn_power,
            'SQRT': self._fn_sqrt,
            'LOG': self._fn_log,
            'LOG10': self._fn_log10,
            'LN': self._fn_ln,
            'EXP': self._fn_exp,
            'SIGN': self._fn_sign,
            'TRUNC': self._fn_trunc,
            'EVEN': self._fn_even,
            'ODD': self._fn_odd,
            'FACT': self._fn_fact,
            'GCD': self._fn_gcd,
            'LCM': self._fn_lcm,
            'RAND': self._fn_rand,
            'RANDBETWEEN': self._fn_randbetween,
            'PI': self._fn_pi,
            'CURRENCY': self._fn_currency,
            'FIXED': self._fn_fixed,
            # --- conformance batch 1: math scalars ---
            'ACOS': lambda a, c: self._fn_math1('ACOS', a, c),
            'ACOSH': lambda a, c: self._fn_math1('ACOSH', a, c),
            'ACOT': lambda a, c: self._fn_math1('ACOT', a, c),
            'ACOTH': lambda a, c: self._fn_math1('ACOTH', a, c),
            'ASIN': lambda a, c: self._fn_math1('ASIN', a, c),
            'ASINH': lambda a, c: self._fn_math1('ASINH', a, c),
            'ATAN': lambda a, c: self._fn_math1('ATAN', a, c),
            'ATANH': lambda a, c: self._fn_math1('ATANH', a, c),
            'COS': lambda a, c: self._fn_math1('COS', a, c),
            'COSH': lambda a, c: self._fn_math1('COSH', a, c),
            'COT': lambda a, c: self._fn_math1('COT', a, c),
            'COTH': lambda a, c: self._fn_math1('COTH', a, c),
            'SIN': lambda a, c: self._fn_math1('SIN', a, c),
            'SINH': lambda a, c: self._fn_math1('SINH', a, c),
            'TAN': lambda a, c: self._fn_math1('TAN', a, c),
            'TANH': lambda a, c: self._fn_math1('TANH', a, c),
            'DEGREES': lambda a, c: self._fn_math1('DEGREES', a, c),
            'RADIANS': lambda a, c: self._fn_math1('RADIANS', a, c),
            'SQRTPI': lambda a, c: self._fn_math1('SQRTPI', a, c),
            'COMBIN': self._fn_combin,
            'COMBINA': self._fn_combina,
            'PERMUT': self._fn_permut,
            'QUOTIENT': self._fn_quotient,
            'BITAND': lambda a, c: self._fn_bitop('BITAND', a, c),
            'BITOR': lambda a, c: self._fn_bitop('BITOR', a, c),
            'BITXOR': lambda a, c: self._fn_bitop('BITXOR', a, c),
            'BITLSHIFT': lambda a, c: self._fn_bitop('BITLSHIFT', a, c),
            'BITRSHIFT': lambda a, c: self._fn_bitop('BITRSHIFT', a, c),
            # --- conformance batch 1: distributions ---
            'NORM.DIST': self._fn_norm_dist,
            'NORM.INV': self._fn_norm_inv,
            'NORM.S.DIST': self._fn_norm_s_dist,
            'NORM.S.INV': self._fn_norm_s_inv,
            'EXPON.DIST': self._fn_expon_dist,
            'POISSON.DIST': self._fn_poisson_dist,
            'BETA.DIST': self._fn_beta_dist,
            'BETA.INV': self._fn_beta_inv,
            'CHISQ.DIST': self._fn_chisq_dist,
            'CHISQ.DIST.RT': self._fn_chisq_dist_rt,
            'CHISQ.INV': self._fn_chisq_inv,
            'CHISQ.INV.RT': self._fn_chisq_inv_rt,
            'T.DIST': self._fn_t_dist,
            'T.DIST.RT': self._fn_t_dist_rt,
            'T.DIST.2T': self._fn_t_dist_2t,
            'T.INV': self._fn_t_inv,
            'T.INV.2T': self._fn_t_inv_2t,
            'CONFIDENCE.NORM': self._fn_confidence_norm,
            'CONFIDENCE.T': self._fn_confidence_t,
            # --- conformance batch 1: column/iterator statistics ---
            'PERCENTILE.INC': lambda a, c: self._fn_percentile('PERCENTILE.INC', a, c),
            'PERCENTILE.EXC': lambda a, c: self._fn_percentile('PERCENTILE.EXC', a, c),
            'PERCENTILEX.INC': lambda a, c: self._fn_percentilex('PERCENTILEX.INC', a, c),
            'PERCENTILEX.EXC': lambda a, c: self._fn_percentilex('PERCENTILEX.EXC', a, c),
            'GEOMEAN': self._fn_geomean,
            'GEOMEANX': self._fn_geomeanx,
            'STDEVX.S': lambda a, c: self._fn_stdevx('STDEVX.S', a, c),
            'STDEVX.P': lambda a, c: self._fn_stdevx('STDEVX.P', a, c),
            'VARX.S': lambda a, c: self._fn_stdevx('VARX.S', a, c),
            'VARX.P': lambda a, c: self._fn_stdevx('VARX.P', a, c),
            'RANK.EQ': self._fn_rank_eq,
            'AVERAGEA': self._fn_averagea,
            'DATEVALUE': self._fn_datevalue,
            'TIMEVALUE': self._fn_timevalue,
            'ISO.CEILING': self._fn_iso_ceiling,
            # --- conformance batch 2 ---
            'NETWORKDAYS': self._fn_networkdays,
            'ISDATETIME': self._fn_isdatetime,
            'CONTAINSROW': self._fn_containsrow,
            'ALLNOBLANKROW': self._fn_allnoblankrow,
            'FILTERS': self._fn_filters,
            'TOPNSKIP': self._fn_topnskip,
            'NATURALINNERJOIN': lambda a, c: self._fn_naturaljoin('NATURALINNERJOIN', a, c),
            'NATURALLEFTOUTERJOIN': lambda a, c: self._fn_naturaljoin('NATURALLEFTOUTERJOIN', a, c),
            'GROUPBY': self._fn_groupby,
            'CURRENTGROUP': self._fn_currentgroup,
            'ISONORAFTER': self._fn_isonorafter,
            'ALLCROSSFILTERED': self._fn_allcrossfiltered,
            'SUBSTITUTEWITHINDEX': self._fn_substitutewithindex,
            'DETAILROWS': self._fn_detailrows,
            'NEXTDAY': self._fn_nextday,
            'PREVIOUSDAY': self._fn_previousday,
            # --- conformance batch 4 ---
            'STDEV.S': lambda a, c: self._fn_column_stat('STDEV.S', a, c),
            'STDEV.P': lambda a, c: self._fn_column_stat('STDEV.P', a, c),
            'VAR.S': lambda a, c: self._fn_column_stat('VAR.S', a, c),
            'VAR.P': lambda a, c: self._fn_column_stat('VAR.P', a, c),
            'MAXA': lambda a, c: self._fn_maxa_mina('MAXA', a, c),
            'MINA': lambda a, c: self._fn_maxa_mina('MINA', a, c),
            'PRODUCTX': self._fn_productx,
            'ISEVEN': lambda a, c: self._fn_iseven_odd('ISEVEN', a, c),
            'ISODD': lambda a, c: self._fn_iseven_odd('ISODD', a, c),
            'ISBOOLEAN': lambda a, c: self._fn_type_pred('ISBOOLEAN', a, c),
            'ISSTRING': lambda a, c: self._fn_type_pred('ISSTRING', a, c),
            'ISNUMERIC': lambda a, c: self._fn_type_pred('ISNUMERIC', a, c),
            'ISINTEGER': lambda a, c: self._fn_type_pred('ISINTEGER', a, c),
            'ISINT64': lambda a, c: self._fn_type_pred('ISINT64', a, c),
            'ISDECIMAL': lambda a, c: self._fn_type_pred('ISDECIMAL', a, c),
            'ISDOUBLE': lambda a, c: self._fn_type_pred('ISDOUBLE', a, c),
            'ISCURRENCY': lambda a, c: self._fn_type_pred('ISCURRENCY', a, c),
            'ISEMPTY': self._fn_isempty,
            'ISAFTER': self._fn_isafter,
            'FIRSTNONBLANKVALUE': lambda a, c: self._fn_nonblankvalue('FIRSTNONBLANKVALUE', a, c),
            'LASTNONBLANKVALUE': lambda a, c: self._fn_nonblankvalue('LASTNONBLANKVALUE', a, c),
            'CONVERT': self._fn_convert,
            'TIME': self._fn_time,
            'YEARFRAC': self._fn_yearfrac,
            'IF.EAGER': self._fn_if_eager,
            'EVALUATEANDLOG': self._fn_evaluateandlog,
            'NAMEOF': self._fn_nameof,
            'USERCULTURE': self._fn_userculture,
            'USEROBJECTID': self._fn_userobjectid,
            'CUSTOMDATA': self._fn_customdata,
            'SAMPLE': self._fn_sample,
            'TOCSV': self._fn_tocsv,
            'TOJSON': self._fn_tojson,
            'LINEST': self._fn_linest,
            'LINESTX': lambda a, c: self._fn_linest(a, c, iterator=True),
            'ADDMISSINGITEMS': self._fn_addmissingitems,
            'TABLEOF': self._fn_tableof,
            'SAMPLECARTESIANPOINTSBYCOVER': self._fn_samplecartesian,
            'UTCTODAY': self._fn_utctoday,
            'ROWNUMBER': self._fn_rownumber_win,
            'RANK': self._fn_rank_win,
            'INDEX': self._fn_index_win,
            'OFFSET': self._fn_offset_win,
            'WINDOW': self._fn_window_win,
            'COLUMNSTATISTICS': self._fn_columnstatistics,
            'SAMPLEAXISWITHLOCALMINMAX': self._fn_sampleaxis,
            'NONVISUAL': self._fn_nonvisual,
            'INFO.TABLES': lambda a, c, _k='tables': self._info_rows(_k, c),
            'INFO.VIEW.TABLES': lambda a, c, _k='tables': self._info_rows(_k, c),
            'INFO.COLUMNS': lambda a, c, _k='columns': self._info_rows(_k, c),
            'INFO.VIEW.COLUMNS': lambda a, c, _k='columns': self._info_rows(_k, c),
            'INFO.ATTRIBUTEHIERARCHIES': lambda a, c, _k='columns': self._info_rows(_k, c),
            'INFO.ATTRIBUTEHIERARCHYSTORAGES': lambda a, c, _k='columns': self._info_rows(_k, c),
            'INFO.MEASURES': lambda a, c, _k='measures': self._info_rows(_k, c),
            'INFO.VIEW.MEASURES': lambda a, c, _k='measures': self._info_rows(_k, c),
            'INFO.RELATIONSHIPS': lambda a, c, _k='relationships': self._info_rows(_k, c),
            'INFO.VIEW.RELATIONSHIPS': lambda a, c, _k='relationships': self._info_rows(_k, c),
            'INFO.RELATIONSHIPSTORAGES': lambda a, c, _k='rel_storage': self._info_rows(_k, c),
            'INFO.RELATIONSHIPINDEXSTORAGES': lambda a, c, _k='rel_storage': self._info_rows(_k, c),
            'INFO.PARTITIONS': lambda a, c, _k='partitions': self._info_rows(_k, c),
            'INFO.MODEL': lambda a, c, _k='one': self._info_rows(_k, c),
            'INFO.CATALOGS': lambda a, c, _k='one': self._info_rows(_k, c),
            'INFO.CULTURES': lambda a, c, _k='one': self._info_rows(_k, c),
            'INFO.CSDLMETADATA': lambda a, c, _k='one': self._info_rows(_k, c),
            'INFO.FUNCTIONS': lambda a, c, _k='functions': self._info_rows(_k, c),
            'INFO.DEPENDENCIES': lambda a, c, _k='dependencies': self._info_rows(_k, c),
            'INFO.CALCDEPENDENCY': lambda a, c, _k='dependencies': self._info_rows(_k, c),
            'INFO.STORAGETABLES': lambda a, c, _k='storage_tables': self._info_rows(_k, c),
            'INFO.TABLESTORAGES': lambda a, c, _k='storage_tables': self._info_rows(_k, c),
            'INFO.PARTITIONSTORAGES': lambda a, c, _k='storage_tables': self._info_rows(_k, c),
            'INFO.SEGMENTMAPSTORAGES': lambda a, c, _k='storage_tables': self._info_rows(_k, c),
            'INFO.COLUMNSTORAGES': lambda a, c, _k='column_storages': self._info_rows(_k, c),
            'INFO.DICTIONARYSTORAGES': lambda a, c, _k='column_storages': self._info_rows(_k, c),
            'INFO.SEGMENTSTORAGES': lambda a, c, _k='column_storages': self._info_rows(_k, c),
            'INFO.STORAGETABLECOLUMNS': lambda a, c, _k='column_storages': self._info_rows(_k, c),
            'INFO.COLUMNPARTITIONSTORAGES': lambda a, c, _k='column_storages': self._info_rows(_k, c),
            'INFO.ANNOTATIONS': lambda a, c, _k='annotations': self._info_rows(_k, c),
            'INFO.PROPERTIES': lambda a, c, _k='properties': self._info_rows(_k, c),
            'INFO.STORAGEFILES': lambda a, c, _k='storage_files': self._info_rows(_k, c),
            'INFO.STORAGEFOLDERS': lambda a, c, _k='storage_folders': self._info_rows(_k, c),
            'INFO.STORAGETABLECOLUMNSEGMENTS': lambda a, c, _k='segments': self._info_rows(_k, c),
            'INFO.ALTERNATEOFDEFINITIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.CALCULATIONGROUPS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.CALCULATIONITEMS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.CHANGEDPROPERTIES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.COLUMNPERMISSIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.DATASOURCES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.DELTATABLEMETADATASTORAGES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.DETAILROWSDEFINITIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.EXPRESSIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.EXTENDEDPROPERTIES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.FORMATSTRINGDEFINITIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.GENERALSEGMENTMAPSEGMENTMETADATASTORAGES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.GROUPBYCOLUMNS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.HIERARCHIES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.HIERARCHYSTORAGES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.KPIS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.LEVELS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.LINGUISTICMETADATA': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.OBJECTTRANSLATIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PARQUETFILESTORAGES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PERSPECTIVECOLUMNS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PERSPECTIVEHIERARCHIES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PERSPECTIVEMEASURES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PERSPECTIVES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.PERSPECTIVETABLES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.QUERYGROUPS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.REFRESHPOLICIES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.RELATEDCOLUMNDETAILS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.ROLEMEMBERSHIPS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.ROLES': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.TABLEPERMISSIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            'INFO.VARIATIONS': lambda a, c, _k='empty': self._info_rows(_k, c),
            # --- conformance batch 3: financial ---
            'PMT': self._fn_pmt, 'FV': self._fn_fv, 'PV': self._fn_pv,
            'NPER': self._fn_nper, 'RATE': self._fn_rate,
            'IPMT': self._fn_ipmt, 'PPMT': self._fn_ppmt,
            'CUMIPMT': self._fn_cumipmt, 'CUMPRINC': self._fn_cumprinc,
            'ISPMT': self._fn_ispmt,
            'SLN': self._fn_sln, 'SYD': self._fn_syd, 'DDB': self._fn_ddb,
            'DB': self._fn_db, 'VDB': self._fn_vdb,
            'AMORDEGRC': self._fn_amordegrc, 'AMORLINC': self._fn_amorlinc,
            'EFFECT': self._fn_effect, 'NOMINAL': self._fn_nominal,
            'RRI': self._fn_rri, 'PDURATION': self._fn_pduration,
            'DOLLARDE': self._fn_dollarde, 'DOLLARFR': self._fn_dollarfr,
            'XNPV': self._fn_xnpv, 'XIRR': self._fn_xirr,
            'ACCRINT': self._fn_accrint, 'ACCRINTM': self._fn_accrintm,
            'COUPDAYBS': self._fn_coupdaybs, 'COUPDAYS': self._fn_coupdays,
            'COUPDAYSNC': self._fn_coupdaysnc, 'COUPNCD': self._fn_coupncd,
            'COUPNUM': self._fn_coupnum, 'COUPPCD': self._fn_couppcd,
            'DISC': self._fn_disc, 'INTRATE': self._fn_intrate,
            'RECEIVED': self._fn_received,
            'PRICE': self._fn_price, 'PRICEDISC': self._fn_pricedisc,
            'PRICEMAT': self._fn_pricemat,
            'YIELD': self._fn_yield, 'YIELDDISC': self._fn_yielddisc,
            'YIELDMAT': self._fn_yieldmat,
            'TBILLEQ': self._fn_tbilleq, 'TBILLPRICE': self._fn_tbillprice,
            'TBILLYIELD': self._fn_tbillyield,
            'DURATION': self._fn_duration, 'MDURATION': self._fn_mduration,
            'ODDLPRICE': self._fn_oddlprice, 'ODDLYIELD': self._fn_oddlyield,
            'ODDFPRICE': self._fn_oddfprice, 'ODDFYIELD': self._fn_oddfyield,
            # --- Logic ---
            'IF': self._fn_if,
            'SWITCH': self._fn_switch,
            'AND': self._fn_and,
            'OR': self._fn_or,
            'NOT': self._fn_not,
            'ISBLANK': self._fn_isblank,
            'BLANK': self._fn_blank,
            'TRUE': self._fn_true,
            'FALSE': self._fn_false,
            'IFERROR': self._fn_iferror,
            'COALESCE': self._fn_coalesce,
            'CONTAINS': self._fn_contains,
            # --- Filter ---
            'CALCULATE': self._fn_calculate,
            # CALCULATETABLE has the same contract as CALCULATE -- apply the
            # filter arguments, perform the row->filter transition, evaluate
            # argument 1 -- and differs only in that argument 1 is a table
            # expression, which _eval_expr already returns as row dicts. Sharing
            # the implementation means the filter-argument handling (boolean
            # predicates, NOT/KEEPFILTERS peeling, ALL/REMOVEFILTERS, DATEADD,
            # USERELATIONSHIP) cannot drift between the two.
            'CALCULATETABLE': self._fn_calculate,
            'REMOVEFILTERS': self._fn_removefilters,
            'ALL': self._fn_all,
            'ALLEXCEPT': self._fn_allexcept,
            'ALLSELECTED': self._fn_allselected,
            'KEEPFILTERS': self._fn_keepfilters,
            'VALUES': self._fn_values,
            'SELECTEDVALUE': self._fn_selectedvalue,
            'FILTER': self._fn_filter,
            'HASONEVALUE': self._fn_hasonevalue,
            'HASONEFILTER': self._fn_hasonefilter,
            'ISFILTERED': self._fn_isfiltered,
            'ISINSCOPE': self._fn_isinscope,
            'ERROR': self._fn_error,
            'FIRSTNONBLANK': self._fn_firstnonblank,
            'LASTNONBLANK': self._fn_lastnonblank,
            'ISCROSSFILTERED': self._fn_iscrossfiltered,
            'USERELATIONSHIP': self._fn_userelationship,
            'EARLIER': self._fn_earlier,
            'EARLIEST': self._fn_earliest,
            # --- Table ---
            'TOPN': self._fn_topn,
            'ADDCOLUMNS': self._fn_addcolumns,
            'SUMMARIZE': lambda a, c: (self._fn_summarize_rollup(a, c) if 'ROLLUP' in a.upper() else self._fn_summarize(a, c)),
            'SUMMARIZECOLUMNS': self._fn_summarizecolumns,
            'SELECTCOLUMNS': self._fn_selectcolumns,
            'DISTINCT': self._fn_distinct,
            'UNION': self._fn_union,
            'EXCEPT': self._fn_except,
            'INTERSECT': self._fn_intersect,
            'CROSSJOIN': self._fn_crossjoin,
            'DATATABLE': self._fn_datatable,
            'ROW': self._fn_row,
            'TREATAS': self._fn_treatas,
            'GENERATE': self._fn_generate,
            'GENERATEALL': self._fn_generateall,
            'GENERATESERIES': self._fn_generateseries,
            # --- Text ---
            'FORMAT': self._fn_format,
            'NOW': self._fn_now,
            'TODAY': self._fn_today,
            'YEAR': self._fn_year,
            'MONTH': self._fn_month,
            'DAY': self._fn_day,
            'QUARTER': self._fn_quarter,
            'HOUR': self._fn_hour,
            'MINUTE': self._fn_minute,
            'SECOND': self._fn_second,
            'WEEKDAY': self._fn_weekday,
            'WEEKNUM': self._fn_weeknum,
            'DATE': self._fn_date,
            'EDATE': self._fn_edate,
            'EOMONTH': self._fn_eomonth,
            'DATEDIFF': self._fn_datediff,
            'UTCNOW': self._fn_utcnow,
            'CONCATENATE': self._fn_concatenate,
            'LEFT': self._fn_left,
            'RIGHT': self._fn_right,
            'MID': self._fn_mid,
            'LEN': self._fn_len,
            'UPPER': self._fn_upper,
            'LOWER': self._fn_lower,
            'PROPER': self._fn_proper,
            'TRIM': self._fn_trim,
            'SUBSTITUTE': self._fn_substitute,
            'REPLACE': self._fn_replace,
            'REPT': self._fn_rept,
            'SEARCH': self._fn_search,
            'FIND': self._fn_find,
            'CONTAINSSTRING': self._fn_containsstring,
            'CONTAINSSTRINGEXACT': self._fn_containsstringexact,
            'EXACT': self._fn_exact,
            'UNICHAR': self._fn_unichar,
            'UNICODE': self._fn_unicode,
            'VALUE': self._fn_value,
            'COMBINEVALUES': self._fn_combinevalues,
            'CONCATENATEX': self._fn_concatenatex,
            'RANKX': self._fn_rankx,
            'PATH': self._fn_path,
            'PATHITEMREVERSE': self._fn_pathitemreverse,
            'PATHCONTAINS': self._fn_pathcontains,
            'PATHITEM': self._fn_pathitem,
            'PATHLENGTH': self._fn_pathlength,
            # --- Time Intelligence ---
            'DATEADD': self._fn_dateadd,
            'SAMEPERIODLASTYEAR': self._fn_sameperiodlastyear,
            'DATESYTD': self._fn_datesytd,
            'DATESMTD': self._fn_datesmtd,
            'DATESQTD': self._fn_datesqtd,
            'TOTALYTD': self._fn_totalytd,
            'TOTALMTD': self._fn_totalmtd,
            'TOTALQTD': self._fn_totalqtd,
            'PREVIOUSMONTH': self._fn_previousmonth,
            'PREVIOUSQUARTER': self._fn_previousquarter,
            'PREVIOUSYEAR': self._fn_previousyear,
            'NEXTMONTH': self._fn_nextmonth,
            'NEXTQUARTER': self._fn_nextquarter,
            'NEXTYEAR': self._fn_nextyear,
            'PARALLELPERIOD': self._fn_parallelperiod,
            'STARTOFMONTH': self._fn_startofmonth,
            'ENDOFMONTH': self._fn_endofmonth,
            'STARTOFQUARTER': self._fn_startofquarter,
            'ENDOFQUARTER': self._fn_endofquarter,
            'STARTOFYEAR': self._fn_startofyear,
            'ENDOFYEAR': self._fn_endofyear,
            'OPENINGBALANCEMONTH': self._fn_openingbalancemonth,
            'CLOSINGBALANCEMONTH': self._fn_closingbalancemonth,
            'OPENINGBALANCEQUARTER': self._fn_openingbalancequarter,
            'CLOSINGBALANCEQUARTER': self._fn_closingbalancequarter,
            'OPENINGBALANCEYEAR': self._fn_openingbalanceyear,
            'CLOSINGBALANCEYEAR': self._fn_closingbalanceyear,
            'FIRSTDATE': self._fn_firstdate,
            'LASTDATE': self._fn_lastdate,
            'DATESBETWEEN': self._fn_datesbetween,
            'DATESINPERIOD': self._fn_datesinperiod,
            'CALENDAR': self._fn_calendar,
            'CALENDARAUTO': self._fn_calendarauto,
            # --- Information ---
            'ISNUMBER': self._fn_isnumber,
            'ISTEXT': self._fn_istext,
            'ISNONTEXT': self._fn_isnontext,
            'ISLOGICAL': self._fn_islogical,
            'ISERROR': self._fn_iserror,
            'USERNAME': self._fn_username,
            'USERPRINCIPALNAME': self._fn_userprincipalname,
            'LOOKUPVALUE': self._fn_lookupvalue,
            # --- Relationship ---
            'RELATED': self._fn_related,
            'RELATEDTABLE': self._fn_relatedtable,
            'CROSSFILTER': self._fn_crossfilter,
            # --- Calculation groups (issue #121) ---
            'SELECTEDMEASURE': self._fn_selectedmeasure,
            'SELECTEDMEASURENAME': self._fn_selectedmeasurename,
            'ISSELECTEDMEASURE': self._fn_isselectedmeasure,
            'SELECTEDMEASUREFORMATSTRING': self._fn_selectedmeasureformatstring,
        }

    def evaluate_measure(self, measure_name: str, ctx: DAXContext,
                         propagate: bool = False) -> Any:
        """Evaluate a named measure in the given context. An error inside a
        nested measure degrades it to BLANK, unless ``propagate``."""
        # DAX identifiers are CASE-INSENSITIVE. Canonicalize to the model's own
        # spelling before anything else, so the cache key, the circular-reference
        # stack and the definition lookup all agree.
        #
        # Without this, `[TOTAL UNITS]` against a measure named `Total Units`
        # passed the case-insensitive existence check and then missed the exact
        # dict lookup below, returning a SILENT BLANK. That single misspelling in
        # MS_Competitive_Marketing blanked nine measures: the whole
        # SAMEPERIODLASTYEAR family (1,299,599 and 49,832 read as blank), the
        # variance measures built on them, and @Indicator03, which answered 2
        # where Desktop answers 1. The fully-qualified `Table[Measure]` path
        # already had this fallback; the bare `[Measure]` path did not.
        if measure_name not in ctx.measures:
            lowered = measure_name.lower()
            for _name in ctx.measures:
                if _name.lower() == lowered:
                    measure_name = _name
                    break
        # Calculation groups (issue #121): the selected item of highest
        # precedence that this evaluation path has not applied yet replaces
        # the reference with its own expression (see _cg_chain).
        _cg = None
        _cg_sig: tuple = ()
        _groups = _calc_registry(ctx.measures)[0]
        if _groups and measure_name in ctx.measures:
            _chain = self._cg_chain(_groups, ctx)
            if _chain:
                _cg = _chain[0]
                _cg_sig = tuple((g.key, str(i['name']).lower()) for g, i, _s in _chain)
        # Check cache
        try:
            # A filter value is a list (In-set) or a dict (structured
            # predicate). Dicts are unhashable, so serialize them
            # deterministically instead of letting the cache key blow up —
            # an exception here used to surface as a null measure result.
            # A tagged value (GroupByValues & co.) answers ISINSCOPE differently
            # from a plain list with the same members, so the tag is part of
            # the key -- without it, a measure cached under a plain filter was
            # returned for the grouped row with the same value.
            # A row transition's value also carries its iteration's rows, which
            # ALLSELECTED restores (#118): the same row under two iterations
            # can have two values, so the iteration is part of the key -- for
            # a measure that can reach ALLSELECTED (_reads_shadow).
            _shadow_on = bool(ctx.filter_context) and self._reads_shadow(measure_name, ctx)

            def _fc_part(k, v):
                tag = type(v).__name__ if type(v) not in (list, dict) else ''
                if isinstance(v, list):
                    if tag:
                        shadow = self._shadow_token(k, v) if _shadow_on else None
                        return (tag, tuple(v)) if shadow is None else (tag, tuple(v), shadow)
                    return tuple(v)
                if isinstance(v, dict):
                    return ("__pred__" + tag, json.dumps(v, sort_keys=True,
                                                         default=str),
                            self._shadow_token(k, v) if _shadow_on else None)
                return v
            # Filters the measure provably cannot see are keyed by what it
            # does see of them (_shielded_keys): "the last week with sales"
            # called once per date row is then evaluated once.
            _repl = (self._shielded_keys(measure_name, ctx)
                     if ctx.filter_context and not _cg_sig else {})
            fc_key = tuple(sorted(
                (k, _repl[k] if k in _repl else _fc_part(k, v))
                for k, v in ctx.filter_context.items()
            )) if ctx.filter_context else ()
            # Under the same filters, a reference whose item applies and one
            # whose item is already applied (inside the item, or nested in the
            # measure) have different values: the items are part of the key.
            cache_key = (measure_name, fc_key) + ((_cg_sig,) if _cg_sig else ())
        except Exception:
            cache_key = None
        if cache_key and cache_key in ctx._measure_cache:
            # The value is reused, and so is what it depended on: a measure
            # reaching this one through the cache still reports the
            # unsupported functions the cached evaluation hit.
            _fns = ctx._measure_cache_unsupported.get(cache_key)
            if _fns:
                self._note_unsupported(*_fns, measure=measure_name)
            return ctx._measure_cache[cache_key]

        # Prevent circular references. An applied item evaluates the same
        # measure again through SELECTEDMEASURE(), so it has a token of its own.
        _token: Any = measure_name if _cg is None else (measure_name, _cg[0].key)
        if _token in ctx._eval_stack:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(f"Circular reference detected: '{measure_name}' references itself")
        if not ctx._eval_stack:
            ctx._eval_calls = 0   # reset budget for each outermost measure
        ctx._eval_stack.add(_token)

        expr = ctx.measures.get(measure_name)
        if expr is None:
            ctx._eval_stack.discard(_token)
            return None
        if _cg is not None:
            # The item's expression stands in for the measure; its
            # SELECTEDMEASURE() is this measure with the remaining items.
            expr = _cg[1].get('expression') or 'SELECTEDMEASURE()'
        # Resolve auto date/time hierarchy accessors once, here, so every
        # downstream consumer -- including the ones that regex the raw argument
        # text (_fn_dateadd, _fn_all) rather than evaluating it -- sees a plain
        # `'Table'[Column]` reference.
        expr = self._expand_variation_refs(expr, ctx)

        # Engine-level wall-clock deadline, set once at the true outermost
        # measure and shared across every sub-context iterators create.
        self._eval_depth += 1
        if self._eval_depth == 1:
            # Snapshot what ALLSELECTED must restore, before any CALCULATE in
            # this measure has had a chance to modify the context. The GROUP
            # keys (from grouped evaluation — the visual's row grouping) are
            # snapshotted separately: ALLSELECTED removes those and keeps the
            # slicer selection, which is the whole difference between it and
            # ALL under a grouped query (issues #25 r23#2 / #26 r24).
            self._query_filters = dict(ctx.filter_context)
            self._group_keys = set(getattr(ctx, 'group_keys', ()) or ())
            self._date_tables = dict(getattr(ctx, 'date_tables', None) or {})
            self._clearing_cache: tuple | None = None
            self._selected_base = getattr(ctx, 'selected_filters', None)
        if self._eval_depth == 1:
            self._deadline = time.monotonic() + self._max_eval_seconds
        # A measure invocation IS the row->filter context transition (implicit
        # CALCULATE): inside the body, the iteration row's filters are real
        # filters, so plain aggregates must NOT step back to the outer context.
        _prev_outer = getattr(ctx, '_outer_ctx', None)
        ctx._outer_ctx = None
        self._home_tables.append(ctx.measure_tables.get(measure_name))
        _hit: set = set()
        self._measure_path.append((measure_name, _hit))
        _cg_prev = None
        if _cg is not None:
            _cg_prev = self._cg_applied.get(_cg[0].key)
            self._cg_applied[_cg[0].key] = _cg[2]
            self._cg_frames.append(measure_name)
        try:
            result = _scalarize(self._eval_expr(expr.strip(), ctx))
            if cache_key:
                ctx._measure_cache[cache_key] = result
                if _hit:
                    ctx._measure_cache_unsupported[cache_key] = frozenset(_hit)
            return result
        except Exception as _exc:
            # A DEADLINE abort must not be swallowed here. Returning None for a
            # nested measure let the caller keep computing and hand back a
            # plausible WRONG number (0.0, 0.2808) for a measure that had simply
            # been cut off. Propagate until the outermost measure, which reports
            # BLANK -- a visible non-answer -- instead.
            if getattr(_exc, "_pbix_deadline", False):
                if self._eval_depth > 1:
                    raise
                self.timed_out.add(measure_name)
            elif propagate and self._eval_depth > 1:
                # SELECTEDMEASURE(): the item's own measure reference, so its
                # error is the item's error (Desktop: Add100 over Double of
                # "x" fails; as a BLANK it read 100).
                raise
            else:
                # Record WHY before degrading, so the tool layer can report
                # status "error" instead of a blank indistinguishable from a
                # legitimate BLANK (ledger issues-7).
                self.eval_errors.setdefault(measure_name, str(_exc))
                self._nested_errors += 1
            # Graceful degradation
            return None
        finally:
            ctx._outer_ctx = _prev_outer
            self._home_tables.pop()
            self._measure_path.pop()
            if _cg is not None:
                self._cg_frames.pop()
                if _cg_prev is None:
                    self._cg_applied.pop(_cg[0].key, None)
                else:
                    self._cg_applied[_cg[0].key] = _cg_prev
            ctx._eval_stack.discard(_token)
            self._eval_depth -= 1
            if self._eval_depth == 0:
                self._deadline = None

    def _note_unsupported(self, *funcs: str, measure: str | None = None) -> None:
        """Record unsupported DAX functions: call-wide, and for every measure
        on the evaluation path (plus ``measure``, one served from the cache)."""
        self.unsupported_functions.update(funcs)
        for m, hit in self._measure_path:
            hit.update(funcs)
            self.unsupported_by_measure.setdefault(m, set()).update(funcs)
        if measure is not None:
            self.unsupported_by_measure.setdefault(measure, set()).update(funcs)

    # ---- calculation groups (issue #121) ------------------------------------
    # Power BI Desktop 2.152 over ADOMD, one cell per query (build_b121.py,
    # build_b121b.py; T(c, v) = a 10, b 20, c 30, S = SUM(T[v])):
    #   * an item applies when the filter context selects exactly ONE item of
    #     its group: by grouping, a TREATAS slicer or a CALCULATE filter.
    #     Several items, or none, apply nothing;
    #   * it applies once per measure reference made outside it. S2 = [S] is
    #     Double(S2) = 120, not 240, and Plus1 of SS = [S] + [S] is 121;
    #   * the item's own measure references are plain: RefS = [S] * 10 is 600
    #     for every measure;
    #   * an inline aggregation is no measure reference: SUM(T[v]) in the query
    #     stays 60, while CALCULATE([S]) and SUMX(VALUES(T[c]), [S]) are
    #     affected (Plus1: 61, and 63 = 11 + 21 + 31);
    #   * the group of higher precedence is the outer one: Double (0) with
    #     Add100 (10) is Add100(Double(S)) = 220, and Plus1 with Neg is -61.
    # build_b121b.py adds:
    #   * only a filter on the ITEM column selects: CG[Ordinal] = 2 leaves
    #     every measure plain, although SELECTEDVALUE(CG[Name]) is "Plus1";
    #   * an item stays applied while the same item is selected: a measure
    #     whose body is CALCULATE([S], CG[Name] = "Double") is 120 under
    #     Double, while CALCULATE([S], CG[Name] = "Plus1") is 122 -- a
    #     CALCULATE that selects ANOTHER item applies it to the references
    #     inside, and so does sideways recursion (CALCULATE(SELECTEDMEASURE(),
    #     CG[Name] = "Double") as an item is Double);
    #   * a group's item applies to the measure references in the expression
    #     of a higher-precedence group's item: Ref10 = [S] * 10 (precedence
    #     10) with Double (0) is 1200;
    #   * a one-item group applies nothing without a filter.

    def _cg_selection(self, g: '_CalcGroup', ctx: DAXContext):
        """The item of group ``g`` that ``ctx`` selects: the filter on its
        item column lets exactly one item through. A filter on another column
        of the group's table plays no part (Desktop: CALCULATE([S], CG[Name]
        = "Double") under CG[Ordinal] = 2 is Double's 120, and CG[Ordinal] =
        1 under CG[Name] IN {"Double", "Plus1"} applies nothing). None
        otherwise."""
        col_key = f"{g.key}.{g.column.lower()}"
        specs = [v for k, v in ctx.filter_context.items() if k.lower() == col_key]
        if not specs:
            return None
        names = [str(i['name']) for i in g.items]
        for spec in specs:
            try:
                match = make_value_matcher(spec)
            except ValueError:
                return None
            names = [n for n in names if match(n)]
        if len(names) != 1:
            return None
        return g.by_name.get(names[0].lower())

    def _cg_chain(self, groups: tuple, ctx: DAXContext) -> list:
        """The selected items not yet applied on this path, outermost (highest
        precedence) first: [(group, item, item's lower-cased name)]."""
        out = []
        for g in groups:
            item = self._cg_selection(g, ctx)
            if item is None:
                continue
            name = str(item['name']).lower()
            if self._cg_applied.get(g.key) == name:
                continue
            out.append((g, item, name))
        return out

    def _cg_measure(self, fn: str) -> str:
        if not self._cg_frames:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(
                f"There is no measure reference in the current context that {fn} can use")
        return self._cg_frames[-1]

    def _fn_selectedmeasure(self, args_str: str, ctx: DAXContext) -> Any:
        """SELECTEDMEASURE(): the measure the item applies to, with the items
        of lower precedence still to apply."""
        return self.evaluate_measure(self._cg_measure('SELECTEDMEASURE'), ctx, propagate=True)

    def _fn_selectedmeasurename(self, args_str: str, ctx: DAXContext) -> Any:
        return self._cg_measure('SELECTEDMEASURENAME')

    def _fn_isselectedmeasure(self, args_str: str, ctx: DAXContext) -> Any:
        """ISSELECTEDMEASURE(<measure>, ...): is the item applied to one of them."""
        cur = self._cg_measure('ISSELECTEDMEASURE').lower()
        for a in self._split_args(args_str):
            a = a.strip()
            name = a[a.rfind('[') + 1:-1] if a.endswith(']') and '[' in a else a
            if name.strip().lower() == cur:
                return True
        return False

    def _fn_selectedmeasureformatstring(self, args_str: str, ctx: DAXContext) -> Any:
        """SELECTEDMEASUREFORMATSTRING(): the format string of the measure the
        item applies to -- its dynamic format string evaluated, else its static
        one, else BLANK (Desktop: BLANK for a measure without one)."""
        name = self._cg_measure('SELECTEDMEASUREFORMATSTRING')
        return self._measure_own_format(name, ctx)

    def _measure_own_format(self, name: str, ctx: DAXContext) -> Any:
        fmt = _calc_registry(ctx.measures)[1].get(name) or {}
        if fmt.get('expression'):
            self._cg_frames.append(name)
            try:
                return _scalarize(self._eval_expr(str(fmt['expression']).strip(), ctx))
            finally:
                self._cg_frames.pop()
        return fmt.get('format_string') or None

    _NO_ITEM_FORMAT = object()

    def item_format_string(self, measure_name: str, ctx: DAXContext) -> Any:
        """The format string an applied calculation item gives measure
        ``measure_name`` in ``ctx``: the format-string expression of the
        outermost selected item that has one, evaluated with the measure as
        SELECTEDMEASURE. _NO_ITEM_FORMAT when no such item applies -- the
        measure keeps its own format string."""
        if measure_name not in ctx.measures:
            low = measure_name.lower()
            measure_name = next((m for m in ctx.measures if m.lower() == low), measure_name)
        # Desktop 2.152 over MDX (build_b121c.py, cell FORMAT_STRING): the
        # outermost item WITH a format string decides it -- Add100's over
        # Double's, Double's when the outer item has none -- and in that
        # expression SELECTEDMEASURE() is the value being formatted, the item
        # applied: IF(SELECTEDMEASURE() > 100, "#,0", "0.00") on Triple of 60
        # is "#,0".
        groups = _calc_registry(ctx.measures)[0]
        for g, item, _name in (self._cg_chain(groups, ctx) if groups else []):
            if item.get('format_string'):
                self._cg_frames.append(measure_name)
                try:
                    return _scalarize(self._eval_expr(str(item['format_string']).strip(), ctx))
                finally:
                    self._cg_frames.pop()
        return self._NO_ITEM_FORMAT

    # ---- measure memo: filters a measure provably cannot see ----------------
    # "The last week with sales" is MAXX(FILTER(ALLSELECTED('Date'),
    # NOT ISBLANK([Sales])), 'Date'[Week]). Its value does not depend on any
    # filter on 'Date': ALLSELECTED restores the selection, and every [Sales]
    # inside runs under the transition of a whole 'Date' row, which replaces
    # whatever 'Date' filters the caller had. Report measures call it once
    # per date row (FILTER(ALLSELECTED('Date'), 'Date'[Week] = [Max Week])),
    # and a memo keyed on the full filter context missed on every row: 731
    # dates x 731 inner rows, so Awesome Chocolates' "Selection max date" ran
    # out of time where Desktop answers at once. The memo key now leaves out
    # the filters on such a table. The proof is syntactic and conservative:
    # anything it does not recognise keeps the full key.
    #
    # Functions whose value is a function of their arguments alone.
    _SHIELD_PURE_FNS = frozenset({
        'NOT', 'ISBLANK', 'AND', 'OR', 'IF', 'SWITCH', 'TRUE', 'FALSE',
        'BLANK', 'COALESCE', 'IFERROR', 'ISERROR', 'ISNUMBER', 'ISTEXT',
        'ISNONTEXT', 'ISLOGICAL', 'ABS', 'SIGN', 'ROUND', 'ROUNDUP',
        'ROUNDDOWN', 'INT', 'TRUNC', 'MOD', 'POWER', 'SQRT', 'EXP', 'LN',
        'LOG', 'LOG10', 'CEILING', 'FLOOR', 'DIVIDE', 'YEAR', 'MONTH', 'DAY',
        'QUARTER', 'WEEKDAY', 'WEEKNUM', 'HOUR', 'MINUTE', 'SECOND', 'DATE',
        'TIME', 'EOMONTH', 'EDATE', 'DATEDIFF', 'LEN', 'LEFT', 'RIGHT', 'MID',
        'UPPER', 'LOWER', 'TRIM', 'CONCATENATE', 'SUBSTITUTE', 'REPT',
        'SEARCH', 'FIND', 'CONTAINSSTRING', 'CONTAINSSTRINGEXACT', 'VALUE',
        'FORMAT', 'CONVERT', 'UNICHAR', 'EXACT'})
    # Iterators (table, expression) whose value is a function of the rows and
    # of the expression evaluated in each row.
    _SHIELD_ITER_FNS = frozenset({
        'MAXX', 'MINX', 'SUMX', 'AVERAGEX', 'COUNTX', 'COUNTAX', 'PRODUCTX'})
    _SHIELD_CAND_RE = re.compile(
        r"(?i)\bALL(?:SELECTED)?\s*\(\s*('(?:[^']|'')+'|[A-Za-z_][\w \-]*?)\s*\)")

    def _shielded_keys(self, name: str, ctx: DAXContext) -> dict:
        """For measure ``name``: its memo key's entry for each filter on a
        shielded table that the measure sees only through ALLSELECTED's
        restore (see the note above). Since #118 ALLSELECTED keeps an explicit
        filter, so only an ITERATION's filter qualifies: a row transition is
        keyed by its iteration's rows (the same for every row of one
        iteration), a grouping by nothing (it restores the query's selection).
        An explicit filter keeps its own value in the key."""
        tables = self._shielded_tables(name, ctx)
        if not tables or not ctx.filter_context:
            return {}
        shadow_on = self._reads_shadow(name, ctx)
        out: dict = {}
        for t in tables:
            tbl = ctx.tables.get(t)
            if tbl is None:
                continue
            # A table filter's expanded propagation is not replaced by a row
            # transition, so a table with one keeps its full key.
            prefix = t + '.'
            if any(k.startswith(prefix) for k in ctx._expanded_keys):
                continue
            for c in tbl['columns']:
                k = prefix + c
                v = ctx.filter_context.get(k)
                if isinstance(v, (RowContextValues, RowContextPredicate)):
                    out[k] = ('shadow', self._shadow_token(k, v) if shadow_on else None)
                elif isinstance(v, (GroupByValues, GroupByPredicate)):
                    out[k] = ('grouping',)
        return out

    _ALLSELECTED_RE = re.compile(r"(?i)\bALLSELECTED\s*\(")
    _BRACKET_REF_RE = re.compile(r"\[((?:[^\]]|\]\])+)\]")

    def _reads_shadow(self, name: str, ctx: DAXContext) -> bool:
        """Can evaluating measure ``name`` reach ALLSELECTED, the one reader of
        an iteration's rows (_restore_selected)? Through its own text, a
        measure it references, or any calculation item. Conservative: a
        column reference spelled like a measure counts as the measure.

        Without ALLSELECTED one row has one value under every iteration, so
        the memo key leaves the iteration out (issue #140). #118 had keyed it
        for every measure: the passes of FILTER and AVERAGEX over the same
        employees no longer shared [MTD Total Sales], and Agents Performance's
        "Employees Avg MTD Sales - Adjusted" took twice as long."""
        cache = ctx._shield_cache
        key = ('reads-shadow', name)
        hit = cache.get(key)
        if hit is not None:
            return bool(hit)
        if ('reads-shadow-items',) not in cache:
            # An applied item stands in for any measure, and its own measure
            # references count (Awesome Chocolates' QOQ item reaches
            # ALLSELECTED through [Max Previous Quarter Not Blank]).
            groups = _calc_registry(ctx.measures)[0]
            cache[('reads-shadow-items',)] = self._reaches_allselected(
                [str(item.get('expression') or '') for g in groups for item in g.items], ctx)
        found = bool(cache[('reads-shadow-items',)]) or self._reaches_allselected(
            [str(ctx.measures.get(name) or '')], ctx, {name})
        cache[key] = found
        return found

    def _reaches_allselected(self, texts: list, ctx: DAXContext, seen: set | None = None) -> bool:
        """Does one of the DAX ``texts``, or a measure they reference
        (transitively), call ALLSELECTED?"""
        lower = ctx._shield_cache.get(('measure-names',))
        if lower is None:
            lower = ctx._shield_cache[('measure-names',)] = {m.lower(): m for m in ctx.measures}
        seen = set(seen or ())
        todo = list(texts)
        while todo:
            expr = todo.pop()
            if self._ALLSELECTED_RE.search(expr):
                return True
            for ref in self._BRACKET_REF_RE.findall(expr):
                m = lower.get(ref.replace(']]', ']').strip().lower())
                if m is not None and m not in seen:
                    seen.add(m)
                    todo.append(str(ctx.measures.get(m) or ''))
        return False

    def _shadow_token(self, key: str, value) -> Optional[int]:
        """A small number standing for the distinct values of ``key``'s column
        among ``value``'s iteration rows (None without them). Numbers are
        never reused, so two different shadows never share one."""
        shadow = getattr(value, 'shadow', None)
        if shadow is None:
            return None
        table, _, col = key.partition('.')
        ck = ('token', id(shadow), table, col)
        hit = self._shadow_cache.get(ck)
        if hit is not None and hit[0] is shadow:
            tok: int = hit[1]
            return tok
        vals = self._shadow_values(shadow, table, col)
        fp = tuple(sorted((type(x).__name__, repr(x)) for x in vals))
        known: Optional[int] = self._shadow_tokens.get(fp)
        if known is None:
            if len(self._shadow_tokens) > 50_000:
                self._shadow_tokens.clear()
            self._shadow_next += 1
            known = self._shadow_tokens[fp] = self._shadow_next
        self._shadow_cache[ck] = (shadow, known)
        return known

    def _shielded_tables(self, name: str, ctx: DAXContext) -> frozenset:
        """Tables (model spelling) whose filters cannot change the value of
        measure ``name``: a whole-row transition replaces every one of them
        before anything in the measure reads the context."""
        cache = ctx._shield_cache
        hit: frozenset | None = cache.get(name)
        if hit is not None:
            return hit
        cache[name] = frozenset()               # a cycle proves nothing
        expr = ctx.measures.get(name) or ''
        out = set()
        for m in self._SHIELD_CAND_RE.finditer(expr):
            raw = m.group(1).strip()
            if raw.startswith("'"):
                raw = raw[1:-1].replace("''", "'")
            t = ctx.model_table(raw)
            if t in out or t not in ctx.tables or not self._every_cell_valued(t, ctx):
                continue
            try:
                if self._shield_ind(expr, t, ctx, frozenset(), {name}):
                    out.add(t)
            except RecursionError:
                pass
        res = frozenset(out)
        cache[name] = res
        return res

    def _every_cell_valued(self, table: str, ctx: DAXContext) -> bool:
        """No BLANK cell anywhere in ``table``. A row transition skips a
        BLANK cell's column (_row_filters), which would leave the caller's
        filter on that column in force."""
        rows = ctx.tables[table].get('rows') or []
        key = ('every-cell-valued', table, id(rows), len(rows))
        cache = ctx._filter_idx_cache
        hit = cache.get(key)
        if hit is None:
            hit = cache[key] = all(v is not None for row in rows for v in row)
        return hit

    def _shield_steps(self, expr: str):
        plan = _PLAN_CACHE.get(expr)
        if plan is None:
            plan = _analyze_expr(expr)
            _PLAN_CACHE[expr] = plan
        return plan

    def _shield_cmp_sides(self, expr: str):
        for op in ('==', '<>', '>=', '<=', '>', '<', '='):
            parts = self._split_operators(expr, op)
            if len(parts) == 2:
                return [p.strip() for p in parts]
        return None

    def _shield_scalar(self, expr: str, table: str, ctx: DAXContext, vars_: frozenset,
                       visiting: set, row: bool) -> bool:
        """``expr`` reads no filter on ``table``: at the measure's top level
        (``row`` False), or in a row context over whole rows of ``table``
        (``row`` True), where a column of the row is read from the row and a
        measure reference runs under the row's transition."""
        steps = self._shield_steps(expr)
        for kind, data in steps:
            if kind in (_P_NONE, _P_CONST):
                return True
            if kind in (_P_PAREN, _P_NOT, _P_NEG):
                return self._shield_scalar(data, table, ctx, vars_, visiting, row)
            if kind in (_P_LOGICAL, _P_BINARY):
                return all(self._shield_scalar(p, table, ctx, vars_, visiting, row)
                           for p in data[1])
            if kind == _P_CONCAT:
                return all(self._shield_scalar(p, table, ctx, vars_, visiting, row)
                           for p in data)
            if kind == _P_CMP:
                sides = self._shield_cmp_sides(data)
                if not sides or not all(self._shield_scalar(p, table, ctx, vars_, visiting, row)
                                        for p in sides):
                    return False
                continue                     # a blank side falls through
            if kind == _P_IN:
                lhs, rhs = data
                rs = rhs.strip()
                if not (rs.startswith('{') and rs.endswith('}')):
                    return False
                if not self._shield_scalar(lhs, table, ctx, vars_, visiting, row):
                    return False
                if not all(self._shield_scalar(e, table, ctx, vars_, visiting, row)
                           for e in self._split_top_level(rs[1:-1], ',') if e.strip()):
                    return False
                continue
            if kind == _P_MAYBEVAR:
                return data.lower() in vars_
            if kind in (_P_BRACKET1, _P_BRACKET2):
                if self._measure_exists(data, ctx):
                    return row or self._shield_measure(data, table, ctx, visiting)
                return row and ctx._find_col_idx(ctx.tables[table]['columns'], data) >= 0
            if kind == _P_TCOL:
                tname, col = data
                if ctx.model_table(tname) == table and \
                        ctx._find_col_idx(ctx.tables[table]['columns'], col) >= 0:
                    return row
                if self._measure_exists(col, ctx):   # Table[Measure]
                    return row or self._shield_measure(col, table, ctx, visiting)
                return False
            if kind == _P_FUNC:
                fname, args_text = data
                args = [a.strip() for a in self._split_args(args_text)] if args_text.strip() else []
                if fname in self._SHIELD_PURE_FNS:
                    return all(self._shield_scalar(a, table, ctx, vars_, visiting, row)
                               for a in args)
                if fname in self._SHIELD_ITER_FNS and len(args) == 2:
                    return (self._shield_rows(args[0], table, ctx, vars_, visiting)
                            and self._shield_scalar(args[1], table, ctx, vars_, visiting, True))
                if fname == 'COUNTROWS' and len(args) == 1:
                    return self._shield_rows(args[0], table, ctx, vars_, visiting)
                return False
            if kind == _P_VARRET and not row:
                return self._shield_var_block(data, table, ctx, vars_, visiting)
            if kind == _P_TAIL:
                # The fallthrough after a comparison: BLANK unless the text
                # names a table (_eval_tail), whose rows depend on the filters.
                return ctx.tables.get(data.strip().strip("'")) is None
            return False
        return False

    def _shield_rows(self, expr: str, table: str, ctx: DAXContext, vars_: frozenset,
                     visiting: set) -> bool:
        """``expr`` is whole rows of ``table`` that no filter on it can change:
        ALL(table), ALLSELECTED(table), or FILTER over those."""
        steps = self._shield_steps(expr)
        if len(steps) != 1:
            return False
        kind, data = steps[0]
        if kind == _P_PAREN:
            return self._shield_rows(data, table, ctx, vars_, visiting)
        if kind != _P_FUNC:
            return False
        fname, args_text = data
        args = [a.strip() for a in self._split_args(args_text)]
        if fname in ('ALL', 'ALLSELECTED') and len(args) == 1:
            raw = args[0]
            if raw.startswith("'") and raw.endswith("'"):
                raw = raw[1:-1].replace("''", "'")
            return '[' not in raw and ctx.model_table(raw.strip()) == table
        if fname == 'FILTER' and len(args) == 2:
            return (self._shield_rows(args[0], table, ctx, vars_, visiting)
                    and self._shield_scalar(args[1], table, ctx, vars_, visiting, True))
        return False

    def _shield_measure(self, name: str, table: str, ctx: DAXContext, visiting: set) -> bool:
        """A measure referenced at the top level, outside any row transition."""
        canon = next((m for m in ctx.measures if m.lower() == name.lower()), name)
        if canon in visiting:
            return False
        expr = ctx.measures.get(canon)
        if not expr:
            return False
        return self._shield_ind(expr, table, ctx, frozenset(), visiting | {canon})

    def _shield_ind(self, expr: str, table: str, ctx: DAXContext, vars_: frozenset,
                    visiting: set) -> bool:
        return self._shield_scalar(expr, table, ctx, vars_, visiting, False)

    def _shield_var_block(self, expr: str, table: str, ctx: DAXContext, vars_: frozenset,
                          visiting: set) -> bool:
        """VAR ... RETURN at the top level, split as _eval_var_return splits it:
        each variable and the result must read no filter on ``table``."""
        text = _collapse_ws_outside_strings(expr)
        kws = [(m.start(), m.group().upper(), m.end())
               for m in re.finditer(r'\b(VAR|RETURN)\b', text, re.IGNORECASE)]
        scope = set(vars_)
        seen_return = False
        for i, (_pos, kw, end) in enumerate(kws):
            block = text[end:kws[i + 1][0] if i + 1 < len(kws) else len(text)].strip()
            if kw == 'VAR':
                vm = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)', block, re.DOTALL)
                if not vm or not self._shield_scalar(vm.group(2).strip(), table, ctx,
                                                     frozenset(scope), visiting, False):
                    return False
                scope.add(vm.group(1).lower())
            else:
                if not self._shield_scalar(block, table, ctx, frozenset(scope), visiting, False):
                    return False
                seen_return = True
        return seen_return

    def _eval_expr(self, expr: str, ctx: DAXContext, var_scope: dict | None = None) -> Any:
        """Evaluate a DAX expression string.

        var_scope: dict of variable names (e.g. '_max') to their evaluated values.
        Used internally for VAR/RETURN support.
        """
        # Bound runaway/non-terminating evaluation -> BLANK instead of a hang.
        ctx._eval_calls += 1
        if ctx._eval_calls > ctx._max_eval_calls:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(
                "DAX evaluation budget exceeded (possible non-terminating measure)"
            )
        # Wall-clock guard (engine-level so it survives the per-row sub-contexts
        # iterators create; throttled to keep time.monotonic() off the hot path).
        # Catches O(dim x fact) measures that scan a large fact per iteration —
        # low call count, huge wall-clock — which the eval-CALL guard misses.
        self._time_counter += 1
        if (self._deadline is not None and (self._time_counter & 0x3F) == 0
                and time.monotonic() > self._deadline):
            from pbix_mcp.errors import DAXEvaluationError
            _err = DAXEvaluationError(
                "DAX evaluation time budget exceeded (measure too slow to "
                "evaluate — e.g. a rank/iterator scanning a large fact table)"
            )
            # setattr, not attribute assignment: DAXEvaluationError declares no
            # such field and mypy rejects the direct form.
            setattr(_err, "_pbix_deadline", True)  # never swallowed by a nested measure
            raise _err

        # Merge explicit var_scope with instance-level scope (from VAR/RETURN blocks).
        # Explicit var_scope takes priority; instance scope provides fallback so
        # that function handlers calling _eval_expr without var_scope still see vars.
        if var_scope is None and self._current_var_scope:
            var_scope = self._current_var_scope
        elif var_scope and self._current_var_scope:
            merged = dict(self._current_var_scope)
            merged.update(var_scope)
            var_scope = merged

        # Compiled plan fast path (issue #6): the whole syntactic dispatch --
        # comment stripping, literal regexes, operator splits, branch checks --
        # is a pure function of the text, analyzed once and cached. Iterators
        # re-evaluate the SAME text once per row, so per row this is one dict
        # hit plus the branch body that would have run anyway.
        plan = _PLAN_CACHE.get(expr)
        if plan is None:
            if len(_PLAN_CACHE) > 200_000:
                _PLAN_CACHE.clear()
            plan = _analyze_expr(expr)
            _PLAN_CACHE[expr] = plan
        return self._exec_plan(plan, ctx, var_scope)

    def _exec_plan(self, plan, ctx: DAXContext, var_scope: dict | None):
        """Run an analyzed plan. Each branch body is the old dispatch code,
        verbatim, minus the syntactic re-analysis the plan already did."""
        for kind, data in plan:
            if kind == _P_CONST:
                return data
            if kind == _P_FUNC:
                func_name, args_text = data
                fn = self._func_map.get(func_name)
                if fn:
                    return fn(args_text, ctx)
                self._note_unsupported(func_name)
                import logging
                logging.getLogger("pbix_mcp.dax").debug(
                    "Unsupported DAX function: %s", func_name)
                return None
            if kind == _P_BINARY:
                op, parts = data
                return self._fold_arith(parts, op, ctx, var_scope)
            if kind == _P_TABLECTOR:
                # One row per element, carrying the value under BOTH the
                # single-column convention (__column__/__value__, which
                # _make_row_context filters on) and a plain "Value" key, so
                # ''[Value] / [Value] resolves through the bare-bracket path.
                rows = []
                for elem in (self._split_top_level(data, ',') if data else []):
                    elem = elem.strip()
                    if not elem:
                        continue
                    parts = (self._split_top_level(elem[1:-1], ',')
                             if elem.startswith('(') and elem.endswith(')')
                             and self._wraps_whole(elem) else [])
                    if len(parts) > 1:
                        # A row constructor ("N", 1): ONE row whose columns DAX
                        # names Value1, Value2, ... It evaluated as BLANK, so
                        # TREATAS({("N", "A")}, R[Region], P[Cat]) had nothing
                        # to filter by (PR #99). __tuple__ keeps the order.
                        tvals = tuple(self._eval_expr(p.strip(), ctx, var_scope)
                                      for p in parts)
                        row = {'__table__': '', '__tuple__': tvals}
                        row.update({f'Value{i}': v for i, v in enumerate(tvals, 1)})
                        rows.append(row)
                        continue
                    v = self._eval_expr(elem, ctx, var_scope)
                    rows.append({'__table__': '', '__column__': 'Value',
                                 '__value__': v, 'Value': v})
                return rows
            if kind == _P_IN:
                result = self._eval_in(data[0], data[1], ctx, var_scope)
                if result is not None:
                    return result
                continue  # unparseable set: fall through, do not guess
            if kind == _P_CMP:
                result = self._eval_comparison(data, ctx, var_scope)
                if result is not None:
                    return result
                continue  # a blank side: the old chain fell through
            if kind == _P_TCOL:
                table_name, col_name = data
                _cur = ctx._current_row
                if _cur and '__parts__' in _cur:
                    # A CROSSJOIN row: the part from that table (#108).
                    _cur = next((p for p in _cur['__parts__']
                                 if p.get('__table__') == table_name), _cur)
                if _cur and _cur.get('__table__') == table_name:
                    if col_name in _cur:
                        return _cur[col_name]
                    if _cur.get('__column__') == col_name:
                        return _cur.get('__value__')
                    # The row is keyed by the model's spelling, which may keep a
                    # space the parsed reference lost (' Sales', issue #132).
                    _tt = ctx.tables.get(table_name)
                    if _tt is not None:
                        _ci = ctx._find_col_idx(_tt['columns'], col_name)
                        if _ci >= 0 and _tt['columns'][_ci] in _cur:
                            return _cur[_tt['columns'][_ci]]
                # A reference to a column that does NOT exist must be BLANK, not
                # a (table, column) marker. Agents_Performance's TopN/BottomN
                # measures read 'Top-Bottom-N'[Top-Bottom-N Value] while that
                # table only has [Top-Bottom-N]; Desktop blanks the whole measure,
                # whereas the marker leaked into the arithmetic and those five
                # measures returned confident numbers (19,260,876.5611 and
                # friends) where Desktop shows nothing.
                _t = ctx.tables.get(table_name)
                if _t is not None and ctx._find_col_idx(_t['columns'], col_name) < 0:
                    # `Table[Name]` where Name is not a column is DAX's
                    # fully-qualified MEASURE reference. Agents_Performance reads
                    # 'Top-Bottom-N'[Top-Bottom-N Value] and that is the measure
                    # (= 10), not a column: Desktop resolves it, so its
                    # TOPN(10, ...) returns 10 rows and the middle-bar test is
                    # `1 IN {11, 12}` -> FALSE -> 0. Treating it as BLANK made the
                    # test `1 IN {1, 2}` -> TRUE and the measure returned 1.
                    if col_name in ctx.measures:
                        return self.evaluate_measure(col_name, ctx)
                    for _mn in ctx.measures:
                        if _mn.lower() == col_name.lower():
                            return self.evaluate_measure(_mn, ctx)
                    # Neither a column of that table nor a measure: the
                    # reference cannot be resolved AT ALL, and Desktop refuses
                    # the whole expression rather than evaluating around it
                    # ("Column 'X' in table 'Y' cannot be found or may not be
                    # used in this expression"). Degrading it to BLANK let the
                    # rest of the arithmetic proceed and produce a CONFIDENT
                    # WRONG NUMBER -- MS_Life_Expectancy's [Health] returned
                    # 3,104,480 and [Health Expenditure] 222 for measures
                    # Desktop cannot evaluate at all.
                    from pbix_mcp.errors import DAXEvaluationError
                    raise DAXEvaluationError(
                        f"Column '{col_name}' in table '{table_name}' cannot be "
                        f"found or may not be used in this expression")
                return (table_name, col_name)
            if kind == _P_BRACKET1:
                if (data not in ctx.measures and ctx._current_row
                        and data in ctx._current_row):
                    return ctx._current_row[data]
                if not self._measure_exists(data, ctx):
                    # Not a measure: DAX resolves a bare [Column] against the
                    # model. Hand back the same (table, column) marker a
                    # qualified reference produces, so the plain aggregates
                    # (SUM/MIN/MAX/...) pick it up through their existing
                    # fallback instead of aggregating a missing measure to 0.
                    col = self._resolve_bare_column(data, ctx)
                    if col is not None:
                        return col
                    # Neither a measure, a row/extension-column key, nor a
                    # column anywhere in the model: Desktop refuses the whole
                    # expression rather than evaluating around it. Degrading
                    # to BLANK let `[Nope] + 1` answer 1 with status "ok" --
                    # indistinguishable from a genuine blank (ledger
                    # issues-7). Same rule as the qualified Table[Name] path.
                    from pbix_mcp.errors import DAXEvaluationError
                    raise DAXEvaluationError(
                        f"Measure or column '[{data}]' cannot be found or "
                        f"may not be used in this expression")
                return self.evaluate_measure(data, ctx)
            if kind == _P_MAYBEVAR:
                if var_scope:
                    if data in var_scope:
                        v = var_scope[data]
                        return v.force(self) if isinstance(v, _LazyVar) else v
                    for k, v in var_scope.items():
                        if k.lower() == data.lower():
                            return v.force(self) if isinstance(v, _LazyVar) else v
                continue  # not a var in this scope: fall through to the tail
            if kind == _P_PAREN:
                return self._eval_expr(data, ctx, var_scope)
            if kind == _P_LOGICAL:
                lop, parts = data
                vals = [self._dax_truthy(self._eval_expr(p, ctx, var_scope))
                        for p in parts]
                return any(vals) if lop == '||' else all(vals)
            if kind == _P_CONCAT:
                # `&` renders a blank as "", but an ALL-BLANK concatenation is
                # BLANK, not the empty string -- Desktop: BLANK() & BLANK() is
                # blank, while BLANK() & "x" is "x".
                vals = [self._eval_expr(p, ctx, var_scope) for p in data]
                if all(v is None for v in vals):
                    return None
                return ''.join(_concat_str(v) for v in vals)
            if kind == _P_NEG:
                inner_val = self._eval_expr(data, ctx, var_scope)
                if isinstance(inner_val, (int, float)) and not isinstance(inner_val, bool):
                    return -inner_val
                if inner_val is None or isinstance(inner_val, (list, tuple)):
                    return None
                # Unary minus converts as arithmetic does (Desktop 2.152:
                # -"3" is -3, -TRUE() is -1, -DATE(2024, 1, 1) is -45292) and
                # refuses other text (-"x" raises; issue #133).
                _n = float(inner_val) if isinstance(inner_val, bool) else _as_number(inner_val)
                if _n is None and isinstance(inner_val, str):
                    _n = _arith_text_number(inner_val, getattr(ctx, 'culture', None))
                if _n is None:
                    if isinstance(inner_val, str):
                        from pbix_mcp.errors import DAXEvaluationError
                        raise DAXEvaluationError(
                            f"Cannot convert value '{inner_val}' of type Text to type Number")
                    return None
                return -int(_n) if _n.is_integer() else -_n
            if kind == _P_NOT:
                inner_val = self._eval_expr(data, ctx, var_scope)
                if inner_val is None or inner_val == 0 or inner_val == '' or inner_val is False:
                    return True
                return False
            if kind == _P_VARRET:
                return self._eval_var_return(data, ctx, var_scope)
            if kind == _P_BRACKET2:
                if (data not in ctx.measures and ctx._current_row
                        and data in ctx._current_row):
                    return ctx._current_row[data]
                if not self._measure_exists(data, ctx):
                    col = self._resolve_bare_column(data, ctx)
                    if col is not None:
                        return col
                    # Same rule as _P_BRACKET1: an unresolvable bare [Name]
                    # raises, mirroring the qualified Table[Name] path
                    # (ledger issues-7 -- silent BLANK made `[Nope] + 1`
                    # answer 1 with status "ok").
                    from pbix_mcp.errors import DAXEvaluationError
                    raise DAXEvaluationError(
                        f"Measure or column '[{data}]' cannot be found or "
                        f"may not be used in this expression")
                return self.evaluate_measure(data, ctx)
            if kind == _P_NONE:
                return None
            if kind == _P_TAIL:
                return self._eval_tail(data, ctx)
        return None

    def _eval_tail(self, expr: str, ctx: DAXContext):
        """The old chain's final branch: a bare table name resolves to its
        filtered rows for iteration; anything else is BLANK."""
        bare_name = expr.strip().strip("'")
        tbl = ctx.tables.get(bare_name)
        if tbl and tbl.get('rows'):
            cols = tbl['columns']
            filtered_rows = ctx.get_filtered_rows(bare_name)
            return [
                {**{'__table__': bare_name, '__row__': True},
                 **{cols[i]: row[i] for i in range(min(len(cols), len(row)))}}
                for row in filtered_rows
            ]
        return None

    def _eval_var_return(self, expr: str, ctx: DAXContext, var_scope: dict | None = None) -> Any:
        """Parse and evaluate a VAR ... RETURN block.

        Extracts all VAR declarations, evaluates them in order (each can
        reference previously declared variables), then evaluates the
        RETURN expression with the full variable scope.
        """
        scope = dict(var_scope) if var_scope else {}

        # We need to split the expression into VAR declarations and a RETURN part.
        # Strategy: use a regex to find top-level VAR and RETURN keywords.
        # We work on the joined, comment-stripped expression.

        # Tokenize into VAR blocks and the RETURN expression.
        # Split on VAR keyword (case-insensitive) that appears as a word boundary.
        # First, find all VAR ... = ... segments and the RETURN segment.

        # Build a list of tokens: [ ('VAR', '_name', 'expression'), ..., ('RETURN', 'expression') ]
        # We'll use a simple state-machine approach scanning word by word.

        # Normalise whitespace
        text = _collapse_ws_outside_strings(expr)

        var_decls = []
        return_expr = None

        # Find all VAR declarations and RETURN using regex on the normalized text.
        # Pattern: VAR <name> = <expression> (terminated by next VAR or RETURN)
        # We find positions of all top-level VAR and RETURN keywords.
        keyword_positions = []
        for m in re.finditer(r'\b(VAR|RETURN)\b', text, re.IGNORECASE):
            keyword_positions.append((m.start(), m.group().upper(), m.end()))

        for idx, (pos, kw, end_pos) in enumerate(keyword_positions):
            # Determine where this block ends (next keyword position or end of string)
            if idx + 1 < len(keyword_positions):
                block_end = keyword_positions[idx + 1][0]
            else:
                block_end = len(text)

            block_text = text[end_pos:block_end].strip()

            if kw == 'VAR':
                # Parse: _name = expression
                var_match = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)', block_text, re.DOTALL)
                if var_match:
                    var_name = var_match.group(1)
                    var_expr = var_match.group(2).strip()
                    var_decls.append((var_name, var_expr))
            elif kw == 'RETURN':
                return_expr = block_text

        # Evaluate each VAR declaration in order.
        # Set _current_var_scope so that function handlers (which don't receive
        # var_scope directly) can still resolve variable references.
        prev_scope = self._current_var_scope
        try:
            for var_name, var_expr in var_decls:
                # LAZY, as in DAX (issue #134): a variable is evaluated the
                # first time it is used -- in this filter context, with the
                # variables declared before it -- and never if it is not.
                # Evaluated eagerly, an unused VAR x = "a" * 1 failed a measure
                # Desktop answers, and a calculation item computed every
                # period's VAR for each measure it applied to.
                scope[var_name] = _LazyVar(var_expr, ctx, dict(scope))

            # Evaluate RETURN expression
            self._current_var_scope = scope
            if return_expr:
                return self._eval_expr(return_expr, ctx, scope)

            return None
        finally:
            self._current_var_scope = prev_scope

    def _extract_args(self, expr: str) -> Optional[str]:
        """Extract balanced parentheses from expression starting with '('."""
        return self._extract_args_scan(expr)

    @staticmethod
    @lru_cache(maxsize=200_000)
    def _extract_args_scan(expr: str) -> Optional[str]:
        """Character scan behind _extract_args, cached by input."""
        if not expr.startswith('('):
            return None
        depth = 0
        in_string = False
        i = 0
        n = len(expr)
        while i < n:
            ch = expr[i]
            # DAX escapes a quote by DOUBLING it; a backslash is an ORDINARY
            # character. Treating a backslash before a quote as an escape (the
            # C/Python rule) left this scan inside the string forever for any
            # literal ending in a backslash -- a Windows path such as
            # "D:\Projects\" -- so the closing paren was never found, the call
            # fell through to the tail case, and the whole expression
            # evaluated to None. A calculated table built on one simply did
            # not materialize, with nothing naming the cause (issue #56).
            if in_string:
                if ch == '"':
                    if i + 1 < n and expr[i + 1] == '"':
                        i += 2
                        continue
                    in_string = False
                i += 1
                continue
            if ch == '"':
                in_string = True
                i += 1
                continue
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return expr[:i + 1]
            i += 1
        return None

    def _split_args(self, args_str: str) -> list:
        """Split function arguments at top-level commas."""
        return self._split_top_level(args_str, ',')

    @staticmethod
    @lru_cache(maxsize=100_000)
    def _split_in_scan(expr: str) -> tuple:
        """Find a TOP-LEVEL ``IN`` word operator: ``(left, right)`` or ``()``.

        A word operator cannot be found by substring scanning -- "MIN(", "IN"
        inside an identifier and the text of a string literal all contain it --
        so this requires non-word characters on both sides and skips anything
        nested in (), [], {} or quotes. Braces count as nesting so the SET on the
        right of an IN is never mistaken for a second operator.
        """
        depth = 0
        in_string = False
        quote = ''
        i = 0
        n = len(expr)
        while i < n:
            ch = expr[i]
            if in_string:
                if ch == quote:
                    in_string = False
                i += 1
                continue
            if ch in ('"', "'"):
                in_string, quote = True, ch
                i += 1
                continue
            if ch in '([{':
                depth += 1
            elif ch in ')]}':
                depth -= 1
            elif (depth == 0 and (ch == 'I' or ch == 'i')
                    and i + 1 < n and expr[i + 1] in ('N', 'n')):
                before = expr[i - 1] if i > 0 else ' '
                after = expr[i + 2] if i + 2 < n else ' '
                if (not (before.isalnum() or before in '_.')
                        and not (after.isalnum() or after in '_.')):
                    left, right = expr[:i].strip(), expr[i + 2:].strip()
                    if left and right:
                        return (left, right)
            i += 1
        return ()

    @staticmethod
    def _wraps_whole(s: str) -> bool:
        """True when ``s`` opens with '(' whose matching ')' is its LAST
        character: ``("a", 1)``, not ``(1) + (2)``."""
        if not s.startswith('('):
            return False
        depth, in_str = 0, False
        for i, ch in enumerate(s):
            if ch == '"':
                in_str = not in_str
            elif in_str:
                continue
            elif ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return i == len(s) - 1
        return False

    def _split_top_level(self, expr: str, delimiter: str) -> list:
        """Split expression at top-level delimiter, respecting parens and strings.

        Memoized via the static scan below -- same rationale as
        _split_operators: pure function of the text, called per row."""
        return list(self._split_toplevel_scan(expr, delimiter))

    @staticmethod
    @lru_cache(maxsize=200_000)
    def _split_toplevel_scan(expr: str, delimiter: str) -> tuple:
        """Character scan behind _split_top_level, cached by input."""
        parts = []
        current = []
        depth = 0
        in_string = False
        i = 0
        while i < len(expr):
            ch = expr[i]
            if ch == '"':
                in_string = not in_string
            if in_string:
                current.append(ch)
                i += 1
                continue
            # Braces nest too: a table constructor `{1,2,3}` is ONE argument, and
            # splitting inside it turned `IN {"Lead","Proposal"}` into two
            # arguments, which is why IN over a literal set silently matched
            # nothing.
            if ch == '(' or ch == '{':
                depth += 1
            elif ch == ')' or ch == '}':
                depth -= 1

            if depth == 0 and expr[i:i+len(delimiter)] == delimiter:
                parts.append(''.join(current))
                current = []
                i += len(delimiter)
                continue
            current.append(ch)
            i += 1
        parts.append(''.join(current))
        return tuple(parts)

    def _split_operators(self, expr: str, op: str) -> list:
        """Split expr at top-level `op`, WITHOUT requiring surrounding spaces.

        Superset of _split_top_level for a single operator: respects (), [],
        and '...'/"..." nesting; for `+`/`-` it skips a unary sign and a
        scientific-notation exponent (1e-5); for `=`/`<`/`>` it skips the char
        when it is part of a two-char operator (`<=`, `>=`, `<>`). Spaced
        expressions split exactly as before (the space just lands in the part
        and is stripped by the caller); the new behaviour is that UNSPACED
        operators (`a*b`, `a=b`) now split too.

        Memoized. This is a pure function of (expr, op), but it runs inside
        every row iteration, so an iterator over a few hundred rows re-parses
        the same expression text thousands of times — profiling one
        RANKX/FILTER measure showed 95,586 calls here, 48% of the whole
        evaluation. A model contains few distinct (expr, op) pairs, so caching
        turns nearly all of those into a dict lookup. A fresh list is handed
        back because callers treat the result as their own.
        """
        return list(self._split_operators_scan(expr, op))

    @staticmethod
    @lru_cache(maxsize=200_000)
    def _split_operators_scan(expr: str, op: str) -> tuple:
        """Character scan behind ``_split_operators``, cached by input.

        A staticmethod so the cache key is (expr, op) alone. Caching a bound
        method would put ``self`` in the key, and a fresh engine per evaluation
        would defeat the cache completely.
        """
        parts: list = []
        cur: list = []
        dp = db = dbr = 0
        insq = indq = False
        i, n = 0, len(expr)
        while i < n:
            ch = expr[i]
            if insq:
                cur.append(ch); insq = (ch != "'"); i += 1; continue
            if indq:
                cur.append(ch); indq = (ch != '"'); i += 1; continue
            if ch == "'":
                insq = True; cur.append(ch); i += 1; continue
            if ch == '"':
                indq = True; cur.append(ch); i += 1; continue
            if ch == '(':
                dp += 1; cur.append(ch); i += 1; continue
            if ch == ')':
                dp -= 1; cur.append(ch); i += 1; continue
            if ch == '[':
                db += 1; cur.append(ch); i += 1; continue
            if ch == ']':
                db -= 1; cur.append(ch); i += 1; continue
            # Braces nest too. Without this a table constructor's contents were
            # scanned as top level, so `x IN { _A + 1, _A + 2 }` also produced a
            # bogus `+` split. It was harmless only because the IN step is tried
            # first; the moment IN falls through (an unparseable set) the
            # fall-back computed arithmetic straight across the IN operator.
            # _split_toplevel_scan and _split_in_scan already track them.
            if ch == '{':
                dbr += 1; cur.append(ch); i += 1; continue
            if ch == '}':
                dbr -= 1; cur.append(ch); i += 1; continue
            if dp == 0 and db == 0 and dbr == 0 and expr[i:i + len(op)] == op:
                prev = ''.join(cur)
                nxt = expr[i + len(op):i + len(op) + 1]
                skip = False
                if op in ('+', '-'):
                    p = prev.rstrip()
                    if p == '' or p[-1] in '+-*/(<>=&|,':
                        skip = True                       # unary sign
                    elif len(p) >= 2 and p[-1] in 'eE' and p[-2].isdigit():
                        skip = True                       # exponent 1e-5
                elif op == '=' and (prev[-1:] in ('<', '>', '=') or nxt == '='):
                    # part of <= / >= / <> ... or one half of the STRICT
                    # equality operator ==, which is its own operator and must
                    # not be shredded into two single '=' splits.
                    skip = True
                elif op == '<' and nxt in ('=', '>'):
                    skip = True                           # part of <= / <>
                elif op == '>' and nxt == '=':
                    skip = True                           # part of >=
                if not skip:
                    parts.append(prev); cur = []; i += len(op); continue
            cur.append(ch); i += 1
        parts.append(''.join(cur))
        return tuple(parts)

    def _transitioned_ctx(self, ctx: 'DAXContext', written: dict) -> 'DAXContext':
        """``ctx`` with a row transition's filters ``written``. A row value on
        a DateTime column that joins a relationship, or on a marked date
        table's date column, clears that table's other filters, as an explicit
        CALCULATE filter on it does (#78). Power BI Desktop 2.152
        (build_b137.py, issue #137): SUMX(VALUES(D[Month]), CALCULATE(
        COUNTROWS(FILTER(ALL(D[Date]), CALCULATE(COUNTROWS(D)) > 0)))) is 273,
        every date in every month, and MINX(D, CALCULATE(MAXX(FILTER(
        ALLSELECTED(D[Date]), NOT ISBLANK([M])), D[Date]))) is the last sale
        date for every row."""
        new_ctx = ctx.with_filters(written)
        clearing = self._clearing_date_columns(ctx)
        if clearing:
            drop = set()
            for table, col in clearing:
                if f"{table}.{col}" in written:
                    drop |= {k for k in ctx.filter_context
                             if k not in written and k.partition('.')[0] == table}
            if drop:
                new_ctx.filter_context = {k: v for k, v in new_ctx.filter_context.items()
                                          if k not in drop}
        return new_ctx

    def _make_row_context(self, row_item: dict, ctx: 'DAXContext',
                          shadow: Optional[list] = None) -> 'DAXContext':
        """Create a filter context from a row dict, filtering on ALL columns of the row.
        This implements the row context → filter context transition."""
        parts = row_item.get('__parts__')
        if parts:
            # A CROSSJOIN row: the transition filters every source table's
            # columns, and the row context reads each of them (issue #108).
            filters_all: dict = {}
            for part in parts:
                filters_all.update(self._row_filters(part, shadow))
            new_ctx = self._transitioned_ctx(ctx, filters_all)
            new_ctx._current_row = row_item
            new_ctx._outer_ctx = ctx
            return new_ctx
        new_ctx = self._transitioned_ctx(ctx, self._row_filters(row_item, shadow))
        # Bind the current row for ALL iteration shapes (full-row SUMX dicts,
        # single-column VALUES/ALL dicts, ADDCOLUMNS/SELECTCOLUMNS extension
        # columns) so column references resolve against the row even inside
        # compound scalar expressions (T[C] & "...", FORMAT(T[C], ...)) — not
        # only when the column ref is the entire expression.
        new_ctx._current_row = row_item
        # Keep the pre-transition context reachable for plain column aggregates
        # (see DAXContext._outer_ctx).
        new_ctx._outer_ctx = ctx
        return new_ctx

    @staticmethod
    def _row_filters(row_item: dict, shadow: Optional[list] = None) -> dict:
        """The filters one row's context transition applies: every column of
        the row, as an iterated (in-scope) value. ``shadow`` is the iteration's
        rows, which ALLSELECTED restores (issue #118)."""
        meta_keys = {'__table__', '__column__', '__value__', '__row__',
                     '__blank_row__', '__parts__', '__tuple__'}
        table_name = row_item.get('__table__', '')
        filters: dict[str, list] = {}

        if row_item.get('__blank_row__'):
            # The blank row of ALL(T) / VALUES(T): every column BLANK, which
            # the propagation resolves to the rows that match no row of T.
            for k in row_item:
                if k not in meta_keys:
                    filters[f"{table_name}.{k}"] = _row_value([None], shadow)
        for k, v in row_item.items():
            if k in meta_keys or v is None:
                continue
            filters[f"{table_name}.{k}"] = _row_value([v], shadow)
        # Also add the primary column filter
        col = row_item.get('__column__', '')
        val = row_item.get('__value__')
        if col and val is not None:
            filters[f"{table_name}.{col}"] = _row_value([val], shadow)
        elif col and '__value__' in row_item:
            # The BLANK (unknown) member. It must still emit a filter -- skipping
            # it left the context UNFILTERED, so iterating a dimension that has an
            # unknown member evaluated the measure once over the whole model and
            # AVERAGEX came out double. The propagation resolves [None] to the
            # rows whose key matches no dimension row. It is the iterated row,
            # so it stays in scope like any other (Desktop: ISINSCOPE TRUE on
            # the blank row after a transition, #80 / #82).
            filters[f"{table_name}.{col}"] = _row_value([None], shadow)
        return filters

    def _resolve_row_result(self, result, row_item, row_ctx):
        """Resolve a column reference result in a row iteration context.
        If result is a (table, column) tuple, resolve it to a concrete value."""
        if isinstance(result, tuple) and len(result) == 2:
            if isinstance(row_item, dict) and '__parts__' in row_item:
                # A CROSSJOIN row: the part from that table (#108).
                row_item = next((p for p in row_item['__parts__']
                                 if p.get('__table__') == result[0]), row_item)
            if isinstance(row_item, dict) and '__table__' in row_item:
                # Full-row dict (from bare table iteration): look up column directly
                if row_item.get('__row__') and result[0] == row_item['__table__']:
                    col_name = result[1]
                    if col_name in row_item:
                        return row_item[col_name]
                # Single-column dict (from ALL/VALUES iteration)
                if result[0] == row_item.get('__table__') and result[1] == row_item.get('__column__'):
                    return row_item.get('__value__')
            # Different column — get single value from filtered context
            vals = list(set(row_ctx.get_column_data(result[0], result[1])))
            if len(vals) == 1:
                return vals[0]
            return None
        return result

    @staticmethod
    def _dax_truthy(val: Any) -> bool:
        """DAX truthiness: BLANK / 0 / '' / False are falsy; anything else truthy."""
        return not (val is None or val == 0 or val == '' or val is False)

    def _eval_binary(self, expr: str, ctx: DAXContext, var_scope: dict | None = None) -> Any:
        """Evaluate binary arithmetic: +, -, *, /
        In DAX, BLANK is treated as 0 in arithmetic operations."""
        # `+`/`-` and `*`/`/` are LEFT-associative. Evaluating parts[0] against
        # the REJOINED tail made them right-associative, so `10 - 3 - 2` gave 9
        # instead of 5 and `20 / 4 / 5` gave 25 instead of 1 -- wrong on every
        # repeated subtraction or division, silently.
        #
        # Splitting on `+` before `-` (and `*` before `/`) is still correct:
        # a - b + c groups as (a - b) + c, so each `-` chain folds on its own.
        for op in ['+', '-']:
            parts = self._split_operators(expr, op)
            if len(parts) > 1:
                return self._fold_arith(parts, op, ctx, var_scope)

        for op in ['*', '/']:
            parts = self._split_operators(expr, op)
            if len(parts) > 1:
                return self._fold_arith(parts, op, ctx, var_scope)

        return _NOT_APPLICABLE

    def _fold_arith(self, parts, op, ctx, var_scope):
        """Fold `parts` left-to-right with `op`, DAX-style."""
        acc = self._eval_expr(parts[0].strip(), ctx, var_scope)
        # A blank folds to 0 for + and - ONLY IF SOMETHING ELSE HAS A VALUE.
        # When every operand is blank the whole expression is blank:
        #   BLANK()+BLANK() -> BLANK      BLANK()+BLANK()+5 -> 5
        #   BLANK()-BLANK() -> BLANK      BLANK()-5         -> -5
        # Folding unconditionally made MS_Corporate_Spend's [Var LE2] read 0
        # under a Scenario slice where both operands are blank and Desktop
        # shows nothing -- a measured zero where there is no measurement.
        any_value = acc is not None
        # Date arithmetic keeps its type in DAX: DATE()+TIME() and d+7 are
        # datetimes, while d1-d2 is a number of days. The serial coercion
        # below erased that -- DATE(2026,8,2)+TIME(14,5,9) came back as the
        # bare serial 46236.586..., indistinguishable from a numeric measure
        # (issue #24 r22#1). Track datetime-ness across the fold and convert
        # the final serial back. `*`/`/` results are plain numbers.
        result_is_dt = isinstance(acc, (datetime, date)) and op in ('+', '-')
        for p in parts[1:]:
            rhs = self._eval_expr(p.strip(), ctx, var_scope)
            any_value = any_value or rhs is not None
            if op == '+':
                result_is_dt = result_is_dt or isinstance(rhs, (datetime, date))
            elif op == '-':
                # datetime - datetime = days (number); datetime - number = datetime
                result_is_dt = result_is_dt and not isinstance(rhs, (datetime, date))
            # BLANK acts as 0 for + and -, but not for * and /. Every rule
            # below was read off the live Desktop engine (msmdsrv), not the
            # docs:
            #   BLANK()+100  -> 100        BLANK()-100  -> -100
            #   BLANK()*100  -> BLANK      100*BLANK()  -> BLANK
            #   BLANK()*0    -> BLANK      BLANK()*BLANK() -> BLANK
            #   BLANK()/100  -> BLANK      BLANK()/BLANK() -> BLANK
            #   100/BLANK()  -> inf        -100/BLANK() -> -inf
            #   5/0          -> inf        0/0 and 0/BLANK() -> nan
            # We folded a blank to 0 for * and /, so MS_Sales_Returns'
            # [% Return Rate Value] (SELECTEDVALUE(...)/100 over a blank) read
            # 0.0 where Desktop is blank, as did the WIF measures.
            if op == '*' and (acc is None or rhs is None):
                return None
            if op == '/' and acc is None:
                # A blank NUMERATOR wins over everything, including a blank
                # denominator -- BLANK()/BLANK() is BLANK, not nan.
                return None
            left = 0 if acc is None else acc
            right = 0 if rhs is None else rhs
            if not (isinstance(left, (int, float))
                    and isinstance(right, (int, float))):
                # A DATE is a number in DAX, and the row-context evaluator
                # hands dates over as ISO strings, so coerce before giving up.
                # Returning None here made `[end] - [start]` blank on every row
                # of a datetime column.
                ln, rn = _as_number(left), _as_number(right)
                _cul = getattr(ctx, 'culture', None)
                if ln is None and isinstance(left, str):
                    ln = _arith_text_number(left, _cul)
                if rn is None and isinstance(right, str):
                    rn = _arith_text_number(right, _cul)
                if ln is None or rn is None:
                    # Text that is no number is an ERROR, not a blank (Desktop
                    # 2.152: "x" + 1, "x" * 2, BLANK() + "x" and "" + 1 all
                    # raise; IFERROR("x" * 2, 5) is 5). As a blank, a
                    # calculation item over a text measure -- Double of "x" --
                    # turned into a confident number further up (issue #133).
                    _txt = left if ln is None else right
                    if isinstance(_txt, str):
                        from pbix_mcp.errors import DAXEvaluationError
                        raise DAXEvaluationError(
                            f"Cannot convert value '{_txt}' of type Text to type Number")
                    return None
                left, right = ln, rn
            if op == '+':
                acc = left + right
            elif op == '-':
                acc = left - right
            elif op == '*':
                acc = left * right
            else:
                if right == 0:
                    # Desktop does NOT blank a divide-by-zero on the bare `/`
                    # operator (that is what DIVIDE() is for) -- it returns an
                    # IEEE special, and ISBLANK() on it is FALSE:
                    #   5/0 -> inf   -100/BLANK() -> -inf   0/0 -> nan
                    # Returning None here made `[x] / [y]` blank where Desktop
                    # shows infinity, which also flipped every downstream
                    # comparison against it.
                    if left == 0:
                        return float('nan')
                    return float('inf') if left > 0 else float('-inf')
                acc = left / right
        if not any_value:
            return None
        if result_is_dt and isinstance(acc, (int, float)):
            # Convert the folded serial back to the datetime DAX would return.
            try:
                return datetime(1899, 12, 30) + timedelta(days=float(acc))
            except (OverflowError, ValueError):
                return acc
        return acc

    def _in_set_values(self, right: str, ctx: DAXContext,
                       var_scope: dict | None = None) -> list | None:
        """The right-hand side of an IN, as a flat list of scalars.

        Two shapes, both of which Desktop accepts:
          ``{"Lead", "Proposal"}``   table constructor -- each element evaluated
          ``VALUES(T[C])``          any single-column table expression

        Returns None when the shape is not understood, so the caller can fall
        through instead of silently answering FALSE (which is what an empty set
        would mean) -- a wrong FALSE here is exactly the class of silent error
        this whole path is fixing.
        """
        right = right.strip()
        if right.startswith('{') and right.endswith('}'):
            inner = right[1:-1].strip()
            if not inner:
                return []
            out = []
            for elem in self._split_top_level(inner, ','):
                elem = elem.strip()
                # A row constructor -- {(1,"a"),(2,"b")} -- is multi-column and
                # only meaningful for a multi-column IN, which this does not
                # implement. Refuse the whole set rather than compare against
                # the tuple text.
                if elem.startswith('('):
                    return None
                out.append(self._eval_expr(elem, ctx, var_scope))
            return out
        # A table expression: reuse the engine's row-dict convention.
        result = self._eval_expr(right, ctx, var_scope)
        if isinstance(result, list):
            if not result:
                # DAX: membership in an empty table is FALSE. Reporting "unknown"
                # here (to hedge against a table function this engine cannot
                # evaluate in the current scope) was worse: the IN step fell
                # through to the next plan step, which could answer with
                # something truthy. Rank Filtering Employyees MTD then returned 1
                # where Desktop returns 0, because TOPN(BLANK, ...) is legitimately
                # empty and both its IN tests should simply be FALSE.
                return []
            vals = []
            for row in result:
                if isinstance(row, dict):
                    if '__value__' in row:
                        vals.append(row['__value__'])
                        continue
                    cols = [k for k in row if not k.startswith('__')]
                    if len(cols) != 1:
                        return None
                    vals.append(row[cols[0]])
                else:
                    vals.append(row)
            return vals
        return None

    def _eval_in(self, left: str, right: str, ctx: DAXContext,
                 var_scope: dict | None = None) -> Any:
        """``<scalar> IN <set>`` -- TRUE when the left value is in the set.

        Membership uses the engine's usual value comparison (numeric when both
        sides are numbers, date-aware when both parse as dates, else text), so
        ``1 IN {1}`` and ``"1" IN {1}`` both hold, matching Desktop.
        """
        values = self._in_set_values(right, ctx, var_scope)
        if values is None:
            return None
        lval = self._eval_expr(left.strip(), ctx, var_scope)
        return any(_compare(lval, '=', v) for v in values)

    def _eval_comparison(self, expr: str, ctx: DAXContext, var_scope: dict | None = None) -> Any:
        """Evaluate comparison operators.

        `==` is checked FIRST and is STRICT: it is the one comparison that does
        NOT coerce a blank. Desktop, side by side:

            BLANK() =  0   TRUE        BLANK() ==  0        FALSE
            BLANK() =  ""  TRUE        BLANK() ==  ""       FALSE
                                       BLANK() ==  BLANK()  TRUE

        It was not implemented at all, so `1==1` evaluated to BLANK and
        MS_Covid_Tracking's [Drill-through button text] --
        `IF(SELECTEDVALUE(StateDim[State],0)==0, ...)` -- took the wrong branch
        and then concatenated a 57-row table into the string.
        """
        for op_str, op_fn in [('==', lambda a, b: a == b),
                              ('<>', lambda a, b: a != b), ('>=', lambda a, b: a >= b),
                              ('<=', lambda a, b: a <= b), ('>', lambda a, b: a > b),
                              ('<', lambda a, b: a < b), ('=', lambda a, b: a == b)]:
            parts = self._split_operators(expr, op_str)
            if len(parts) == 2:
                left = self._eval_expr(parts[0].strip(), ctx, var_scope)
                right = self._eval_expr(parts[1].strip(), ctx, var_scope)
                if op_str == '==':
                    # Strict: a blank equals only a blank.
                    if left is None or right is None:
                        return left is None and right is None
                else:
                    # BLANK does not propagate through a comparison in DAX; it
                    # takes the zero of the other operand's type. See
                    # _blank_zero_of for the Desktop-verified table.
                    left, right = _coerce_blanks_for_compare(left, right)
                if isinstance(left, str) and isinstance(right, str):
                    # Text compares case-insensitively ("North" = "north",
                    # "a" < "B"; issue #107).
                    left, right = _text_key(left), _text_key(right)
                if left is not None and right is not None:
                    try:
                        return op_fn(left, right)
                    except TypeError:
                        return None
        return None

    # =========================================================================
    # DAX Functions
    # =========================================================================

    @staticmethod
    def _parse_column_ref(s: str):
        """Syntactically parse a bare ``Table[Column]`` reference -> (table, col).

        Plain aggregates (SUM/AVERAGE/MIN/...) take a column REFERENCE, not an
        expression — parsing it here keeps the ref out of the row-context scalar
        resolution in _eval_expr (which would collapse it to the current row's
        value and break the aggregation)."""
        m = re.match(r"^\s*'?([^'\[\]()]+)'?\s*\[([^\]]+)\]\s*$", s or "")
        if m:
            return m.group(1).strip(), m.group(2).strip()
        return None

    def _expand_variation_refs(self, expr: str, ctx: DAXContext) -> str:
        """Rewrite `'T'[C].[Part]` to the auto-date table column it names.

        The auto date/time hierarchy stores its columns in a hidden
        `LocalDateTable_<guid>` joined to `T[C]`, and `.[Date]` / `.[Year]` /
        `.[Month]` read THAT table. Nothing here understood the accessor, so:

          * `_DATEADD_ARGS_RE` did not match, `_fn_dateadd` returned its
            unresolved marker, and the shift silently did nothing --
            MS_Blog_2020_Sep's [Revenue YoY%] read 0.0 against Desktop's 0.6253;
          * `_fn_all`'s unanchored pattern read `ALL('Calendar'[Date].[Month])`
            as `ALL('Calendar'[Date])`, removing filters from the wrong column.

        The mapping is not guessed: it is the ACTIVE relationship from `T[C]` to
        a date table that actually has a column called `Part`. With no such
        relationship the text is left alone, so an unknown accessor stays
        visibly unresolved instead of resolving to the wrong column.
        """
        if '].[' not in expr.replace(' ', ''):
            return expr
        key = (expr, id(ctx.relationships))
        hit = self._variation_cache.get(key, _MISSING)
        if hit is not _MISSING:
            return hit

        def _one(seg: str) -> str:
            def _sub(m):
                tname = (m.group(1) or m.group(2) or '').strip()
                col, part = m.group(3).strip(), m.group(4).strip()
                for rel in (ctx.relationships or []):
                    if (str(rel.get('FromTable')) == tname
                            and str(rel.get('FromColumn')) == col
                            and rel.get('IsActive', 1)):
                        to_t = str(rel.get('ToTable'))
                        tbl = ctx.tables.get(to_t)
                        if tbl and ctx._find_col_idx(
                                tbl.get('columns') or [], part) >= 0:
                            return f"'{to_t}'[{part}]"
                return m.group(0)
            return _VARIATION_REF_RE.sub(_sub, seg)

        # Only outside string literals: an SVG measure can carry "].[" in text.
        out, last = [], 0
        for m in _STRING_LIT_RE.finditer(expr):
            out.append(_one(expr[last:m.start()]))
            out.append(m.group(0))
            last = m.end()
        out.append(_one(expr[last:]))
        result = ''.join(out)
        self._variation_cache[key] = result
        return result

    def _selected_filters(self) -> dict:
        """The SLICER-only filter set ALLSELECTED restores.

        Preferred source: the caller's base filter_context, passed through
        untouched by grouped evaluation (``DAXContext.selected_filters``) —
        this survives a column that is BOTH sliced and grouped, where the
        group value overwrote the slicer's in the merged dict. Fallback: the
        outermost query snapshot minus the group keys. Under a plain
        evaluate call all three collapse to the query snapshot."""
        base = getattr(self, '_selected_base', None)
        if base is not None:
            return dict(base)
        outer = self._query_filters or {}
        gk = getattr(self, '_group_keys', None) or set()
        if not gk:
            return dict(outer)
        return {k: v for k, v in outer.items() if k not in gk}

    # ---- what ALLSELECTED restores (issue #118) -----------------------------
    # Power BI Desktop 2.152 over ADOMD (build_b118.py):
    #   * an EXPLICIT CALCULATE filter stays, in every form:
    #     CALCULATE(CALCULATE([Sales], ALLSELECTED(T)), T[Cat] = "A") is A's
    #     40, CALCULATE(COUNTROWS(ALLSELECTED(T[Cat])), T[Cat] = "A") is 1, and
    #     a DATEADD filter survives an inner ALLSELECTED('Date');
    #   * a filter an ITERATION put there goes: a grouping goes back to the
    #     query's selection, a row transition to the iteration's ROWS (its
    #     shadow): SUMX(FILTER(ALL(T[Cat]), T[Cat] = "A"),
    #     CALCULATE(COUNTROWS(ALLSELECTED(T)))) is 2, not 4;
    #   * the column form keeps its own columns only, explicit filters on the
    #     others included: CALCULATE(COUNTROWS(ALLSELECTED(Dt[Date])),
    #     Dt[Month] = 2) is all 91 dates.

    def _shadow_values(self, shadow: list, table: str, col: str) -> list:
        """The distinct values of ``table.col`` among an iteration's rows."""
        key = (id(shadow), table, col)
        hit = self._shadow_cache.get(key)
        if hit is not None and hit[0] is shadow:
            cached: list = hit[1]
            return cached
        if len(self._shadow_cache) > 20_000:
            self._shadow_cache.clear()
        vals: list = []
        seen: set = set()
        for r in shadow:
            if not isinstance(r, dict):
                continue
            for part in (r.get('__parts__') or (r,)):
                if part.get('__table__') != table:
                    continue
                if part.get('__column__') == col:
                    v = part.get('__value__')
                elif col in part:
                    v = part[col]
                else:
                    continue
                marker = (type(v).__name__, repr(v))
                if marker not in seen:
                    seen.add(marker)
                    vals.append(v)
        self._shadow_cache[key] = (shadow, vals)
        return vals

    def _restore_selected(self, key: str, value):
        """What ALLSELECTED puts back for ``key``, whose filter ``value`` an
        iteration put there: the values among the iteration's rows, else the
        query's selection, else None (no filter)."""
        shadow = getattr(value, 'shadow', None)
        if shadow is not None:
            table, _, col = key.partition('.')
            return list(self._shadow_values(shadow, table, col))
        return self._selected_filters().get(key)

    def _allselected_ctx(self, ctx: DAXContext, in_scope) -> DAXContext:
        """``ctx`` as ALLSELECTED leaves it for the keys ``in_scope(key)``
        accepts: an iteration's filter replaced by what it restores, an
        explicit filter kept, and the query's selection put back where the
        context no longer has one."""
        removes: list = []
        restores: dict = {}
        for k, v in ctx.filter_context.items():
            if k.startswith(_TUPLE_FILTER_PREFIX) or not in_scope(k) or not _iteration_tagged(v):
                continue
            back = self._restore_selected(k, v)
            if back is None:
                removes.append(k)
            else:
                restores[k] = back
        for k, v in self._selected_filters().items():
            if not k.startswith(_TUPLE_FILTER_PREFIX) and in_scope(k) and k not in ctx.filter_context:
                restores[k] = v
        out = ctx.without_filters(removes) if removes else ctx
        return out.with_filters(restores) if restores else out

    def _selected_ctx(self, ctx: DAXContext) -> DAXContext:
        """The filter context ALLSELECTED restores.

        ALLSELECTED keeps the filters that came from OUTSIDE the measure --
        the query/slicer selection -- and drops the ones CALCULATE applied
        inside it AND the group filter a grouped evaluation applied (the
        visual's row grouping; see ``DAXContext.group_keys``). Approximating
        ALLSELECTED as VALUES (which is what it did) meant it never removed a
        filter on its own column, so
        `CALCULATE(COUNTROWS(ALLSELECTED(T[Queue])), T[Queue]="IT Support")`
        answered 1 where Desktop answers 10.
        """
        outer = self._selected_filters()
        if outer == ctx.filter_context:
            return ctx
        sel = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                         ctx.date_column, dict(outer), ctx.relationships)
        sel.measure_tables = ctx.measure_tables
        sel.model_columns = ctx.model_columns
        return sel

    def _selected_on_columns(self, ctx: DAXContext, table: str, columns) -> DAXContext:
        """The selection ALLSELECTED's COLUMN form keeps: the query/slicer
        filters on those columns of ``table`` and nothing else (a filter on
        column combinations is projected onto them).

        Power BI Desktop 2.152 (issue #120), T(Region, Cat, Amount) many-to-one
        D(Region, Zone), one slicer at a time: COUNTROWS(ALLSELECTED(T[Region]))
        is 1 only under a T[Region] slicer, and stays 2 under slicers on
        T[Amount], T[Cat], D[Zone] and D[Region] that leave one region. The
        TABLE form keeps every selection reaching the table (_allselected_ctx).

        On those columns an explicit CALCULATE filter stays and an iteration's
        filter comes back as the iteration's rows (issue #118): Desktop's
        CALCULATE(COUNTROWS(ALLSELECTED(T[Cat])), T[Cat] = "A") is 1. An
        explicit filter on ANOTHER column plays no part:
        CALCULATE(COUNTROWS(ALLSELECTED(Dt[Date])), Dt[Month] = 2) is 91."""
        tl = table.lower()
        cl = {str(c).lower() for c in columns}

        def own(k: str) -> bool:
            t, sep, c = k.partition('.')
            return bool(sep) and t.lower() == tl and c.lower() in cl

        keep: dict = {}
        for k, v in ctx.filter_context.items():
            if k.startswith(_TUPLE_FILTER_PREFIX):
                pos = [i for i, (t, c) in enumerate(v.get('tuple_columns') or [])
                       if str(t).lower() == tl and str(c).lower() in cl]
                if pos:
                    spec = _project_tuple_filter(v, pos)
                    keep[_tuple_filter_key(spec['tuple_columns'])] = spec
                continue
            if not own(k):
                continue
            if _iteration_tagged(v):
                back = self._restore_selected(k, v)
                if back is not None:
                    keep[k] = back
            else:
                keep[k] = v
        for k, v in self._selected_filters().items():
            if not k.startswith(_TUPLE_FILTER_PREFIX) and own(k) and k not in ctx.filter_context:
                keep[k] = v
        sel = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                         ctx.date_column, keep, ctx.relationships)
        sel.measure_tables = ctx.measure_tables
        sel.model_columns = ctx.model_columns
        return sel

    def _multi_column_all(self, ref: str, ctx: DAXContext, selected: bool,
                          no_blank_row: bool = False):
        """ALL/ALLSELECTED over SEVERAL columns -> their distinct combinations.

        Both used an UNANCHORED regex on the raw argument text, so every column
        after the first was silently dropped:
        `ALLSELECTED(fact[Cluster_ID], fact[Queue])` returned the 8 Cluster_IDs
        instead of the 74 real pairs, and a SUMMARIZE over it then found no
        [Queue] column and produced no rows at all.

        Returns None when this is not the multi-column shape, so the callers
        fall through to their single-column and table-level paths.
        """
        args = self._split_args(ref)
        if len(args) < 2:
            return None
        cols = []
        for a in args:
            m = _WHOLE_TCOL_RE.match(a.strip())
            if not m:
                return None
            cols.append(((m.group(1) or m.group(2) or '').strip(),
                         m.group(3).strip()))
        table_name = cols[0][0]
        if any(t != table_name for t, _c in cols):
            return None            # cross-table combinations are not modelled
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return None
        idxs = [ctx._find_col_idx(tbl['columns'], c) for _t, c in cols]
        if any(i < 0 for i in idxs):
            return None
        sel = (self._selected_on_columns(ctx, table_name, [c for _t, c in cols])
               if selected else None)       # its own columns' selection only (#120)
        rows = tbl['rows'] if sel is None else sel.get_filtered_rows(table_name)
        seen, out = set(), []
        for row in rows:
            key = tuple(row[i] for i in idxs)
            if key in seen:
                continue
            seen.add(key)
            rd = {'__table__': table_name, '__row__': True}
            for (_t, c), v in zip(cols, key):
                rd[c] = v
            out.append(rd)
        # The blank row's combination: every column BLANK (issue #82).
        blank_key = tuple(None for _ in idxs)
        if blank_key not in seen and not no_blank_row and (
                sel.blank_row_visible(table_name) if sel is not None
                else table_name in ctx.blank_row_tables()):
            rd = {'__table__': table_name, '__row__': True}
            for _t, c in cols:
                rd[c] = None
            out.append(rd)
        return out

    @staticmethod
    def _measure_exists(name: str, ctx: DAXContext) -> bool:
        """Is `name` a measure in this model? Case-insensitively, like DAX.

        Gates the bare-[Column] fallback: a measure ALWAYS wins over a column of
        the same name, so a model that has both keeps its old behaviour.
        """
        if name in ctx.measures:
            return True
        lowered = name.lower()
        return any(m.lower() == lowered for m in ctx.measures)

    def _resolve_bare_column(self, name: str, ctx: DAXContext):
        """A bare ``[Column]`` reference -> a (table, column) marker.

        DAX lets a measure reference a column without the table qualifier, and
        Power BI's own generated measures rely on it. MS_Corporate_Spend's
        [Amount] is literally ``TOTALYTD(SUM([Value]), 'Date'[Date])*.3``:
        reading [Value] as a MISSING MEASURE made SUM return 0, so all 15
        measures in that file read 0.0 against Desktop's real totals
        (Amount alone is 1,261,102,214.20). MS_Perf_Analyzer's
        ``FILTER('Events', [component] = "...")`` failed the same way.

        The current row's own table wins, then a model-wide lookup. Ambiguity is
        NOT guessed at: a name owned by more than one table resolves to nothing,
        which is also what Desktop does (it refuses the expression rather than
        picking a table).
        """
        row = ctx._current_row
        if row:
            rt = row.get('__table__')
            rtbl = ctx.tables.get(rt) if rt else None
            if rtbl is not None and ctx._find_col_idx(rtbl.get('columns') or [],
                                                      name) >= 0:
                return (rt, name)
        # Memoized per model: this runs inside per-row iteration, and scanning
        # every table's column list on each row was measurable.
        key = (id(ctx.tables), name)
        owners = self._bare_col_cache.get(key, _MISSING)
        if owners is _MISSING:
            owners = [tn for tn, t in ctx.tables.items()
                      if ctx._find_col_idx(t.get('columns') or [], name) >= 0]
            self._bare_col_cache[key] = owners
        if len(owners) == 1:
            return (owners[0], name)
        # Several tables own the name. DAX does NOT refuse -- it resolves the
        # reference against the table the MEASURE IS DEFINED ON.
        # MS_Revenue_Opportunities' `Revenue = SUM([ProductRevenue])` lives on
        # Fact, and Fact / Fact A / Fact B all have a ProductRevenue column;
        # refusing returned BLANK for all six measures on that table where
        # Desktop returns 1,968,250,939.
        home = self._home_tables[-1] if self._home_tables else None
        if home and home in owners:
            return (home, name)
        return None

    def _require_real_column(self, col, ctx: DAXContext) -> None:
        """Refuse a (table, column) reference whose column does not exist.

        The plain aggregates parse their argument with a pure REGEX, so
        `SUM(T[No Such Column])` produced a syntactically fine reference,
        get_column_data returned nothing, and the aggregate came back BLANK --
        which `+` then folds to 0, so the surrounding arithmetic carried on and
        produced a CONFIDENT WRONG NUMBER. MS_Life_Expectancy's [Health] sums
        eight columns of which one does not exist: Desktop refuses the whole
        measure ("Column 'Deaths due to HIV/AIDS (per 100 000 population)' in
        table 'Indicators' cannot be found") and we answered 3,104,480.

        Only refuses when the TABLE exists -- an unknown table is a different
        shape (a table expression, a variable) that other branches handle.
        """
        if not col:
            return
        tbl = ctx.tables.get(col[0])
        if tbl is None:
            return
        if ctx._find_col_idx(tbl.get('columns') or [], col[1]) >= 0:
            return
        if self._measure_exists(col[1], ctx):
            return          # Table[Measure] is a legal qualified reference
        # `ctx.tables` holds only what was MATERIALIZED. A column the model has
        # but this run did not load is NOT an unresolvable reference, and
        # refusing it blanked thirteen MS_Employee_Hiring / MS_Human_Resources
        # measures that Desktop evaluates perfectly well. Refuse only on the
        # model's own schema, and only when that schema is actually known.
        known = (ctx.model_columns or {}).get(col[0])
        if not known:
            return
        lowered = col[1].strip().lower()
        if any(c.strip().lower() == lowered for c in known):   # ' Sales' (#132)
            return
        from pbix_mcp.errors import DAXEvaluationError
        raise DAXEvaluationError(
            f"Column '{col[1]}' in table '{col[0]}' cannot be found or may not "
            f"be used in this expression")

    def _charge_eval(self, ctx: DAXContext) -> None:
        """Charge one unit of the eval budget (same accounting _eval_expr does).

        The syntactic column-ref fast path in the plain aggregates skips
        _eval_expr for the argument, so it must charge the budget itself or a
        runaway measure could dodge the guard."""
        ctx._eval_calls += 1
        if ctx._eval_calls > ctx._max_eval_calls:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(
                "DAX evaluation budget exceeded (possible non-terminating measure)"
            )

    @staticmethod
    def _agg_ctx(ctx: DAXContext) -> DAXContext:
        """Context a PLAIN column aggregate evaluates against.

        In real DAX a row context does not filter — SUM(T[Col]) typed directly
        in an iterator's scalar expression sees the OUTER filter context (the
        grand total), unless CALCULATE / a measure invocation performed the
        row->filter transition. The engine applies the transition eagerly, so
        step back to the stashed pre-transition context when present (CALCULATE
        and evaluate_measure clear it)."""
        outer = getattr(ctx, '_outer_ctx', None)
        return outer if outer is not None else ctx

    def _fn_sum(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        values = self._agg_ctx(ctx).get_column_data(*col)
        nums = [v for v in values if isinstance(v, (int, float))]
        # SUM over no rows is BLANK in DAX (so ISBLANK fires), not 0.
        return sum(nums) if nums else None

    def _fn_average(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        values = [v for v in self._agg_ctx(ctx).get_column_data(*col)
                  if isinstance(v, (int, float))]
        # Every aggregate is BLANK over an empty set, never 0. Verified on
        # Desktop with CALCULATE(<agg>, FILTER(ALL(T), FALSE())): COUNT,
        # COUNTA, COUNTROWS, COUNTBLANK, DISTINCTCOUNT, MIN, COUNTX,
        # AVERAGEX, SUMX, MINX and MEDIANX all came back BLANK. A 0 is the
        # worst kind of wrong here: it reads as a measured zero, and it is
        # what ISBLANK() is testing for.
        return sum(values) / len(values) if values else None

    def _fn_count(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        n = len([v for v in self._agg_ctx(ctx).get_column_data(*col)
                 if v is not None])
        return n or None

    def _fn_countrows(self, args_str: str, ctx: DAXContext) -> Any:
        # Try evaluating as an expression first (handles TOPN, FILTER, etc.)
        result = self._eval_expr(args_str.strip(), ctx)
        if isinstance(result, list):
            return len(result) or None
        # Fall back to table name lookup
        table_name = args_str.strip().strip("'")
        rows = ctx.get_filtered_rows(table_name)
        return len(rows) or None

    @staticmethod
    def _comparable_values(data: Any) -> list:
        """Values MIN/MAX can order, as one type group.

        DAX MIN/MAX are not numeric-only — they order dates and text as well.
        Filtering to (int, float) made `MIN(Sales[Date])` return 0 instead of
        the earliest date: a silently wrong NUMBER, which is worse than an
        error because nothing surfaces it. Numbers are preferred so existing
        numeric behaviour is unchanged; dates and text are only used when the
        column holds no numbers, so mixed columns never compare across types.
        """
        nums = [v for v in data
                if isinstance(v, (int, float)) and not isinstance(v, bool)]
        if nums:
            return nums
        dts = [v for v in data if isinstance(v, (datetime, date))]
        if dts:
            return dts
        return [v for v in data if isinstance(v, str) and v != ""]

    def _minmax_column(self, args, ctx, pick):
        col = self._parse_column_ref(args[0])
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args[0].strip(), ctx)
            col = ref if isinstance(ref, tuple) and len(ref) == 2 else None
        if not col:
            return None
        values = self._comparable_values(
            self._agg_ctx(ctx).get_column_data(*col))
        # MIN/MAX over no rows is BLANK, like every other aggregate
        # (Desktop: CALCULATE(MIN(T[c]), FILTER(ALL(T), FALSE())) is BLANK).
        return pick(values) if values else None

    @staticmethod
    def _minmax_pair(a, b, pick):
        """Two-argument form. Compares dates and text as well as numbers, but
        never across incompatible types."""
        for kinds in ((int, float), (datetime, date), (str,)):
            if isinstance(a, kinds) and isinstance(b, kinds) \
                    and not isinstance(a, bool) and not isinstance(b, bool):
                return pick(a, b)
        return None

    def _fn_min(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) == 1:
            got = self._minmax_column(args, ctx, min)
            if got is not None:
                return got
        elif len(args) == 2:
            return self._minmax_pair(
                self._eval_expr(args[0].strip(), ctx),
                self._eval_expr(args[1].strip(), ctx), min)
        return None

    def _fn_max(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) == 1:
            got = self._minmax_column(args, ctx, max)
            if got is not None:
                return got
        elif len(args) == 2:
            return self._minmax_pair(
                self._eval_expr(args[0].strip(), ctx),
                self._eval_expr(args[1].strip(), ctx), max)
        return None

    def _fn_distinctcount(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        values = self._agg_ctx(ctx).get_column_data(*col)
        # DAX COUNTS THE BLANK as one distinct value -- that is exactly what
        # DISTINCTCOUNTNOBLANK exists to avoid. MS_Blog_2020_Sep's
        # DISTINCTCOUNT('Online Sales'[Customer]) is 119387 in Desktop over a
        # column with 119386 distinct customers and 8 blank rows; we reported
        # 119386 by dropping the blank.
        distinct = {str(v) for v in values if v is not None}
        n = len(distinct) + (1 if any(v is None for v in values) else 0)
        return n or None

    def _fn_distinctcountnoblank(self, args_str: str, ctx: DAXContext) -> Any:
        """DISTINCTCOUNTNOBLANK(column) — distinct values, blank NOT counted."""
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        values = self._agg_ctx(ctx).get_column_data(*col)
        return len({str(v) for v in values if v is not None}) or None

    def _fn_counta(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTA(column) — count of non-blank values, ANY type.

        COUNT is numeric-oriented; COUNTA counts text and logicals too. Both
        end up as "rows where the column is not blank" here.
        """
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
            self._require_real_column(col, ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return None
            col = ref
        return len([v for v in self._agg_ctx(ctx).get_column_data(*col)
                    if v is not None]) or None

    def _num_operand(self, v, ctx: DAXContext):
        """A numeric argument as Desktop's formula engine converts it: BLANK
        stays None, TRUE / FALSE are 1 / 0, a date is its serial, and text is
        read as arithmetic reads it (the culture's separators, a currency
        symbol, a culture-ordered date; #133). Text that is no number raises,
        as in arithmetic. A table is no number: None.

        Power BI Desktop 2.152 (build_b139.py, issue #139): DIVIDE(3, "3") is
        1, ABS("-3") 3, SIN("0") 0, COMBIN("4", 2) 6, ROUND(TRUE(), 0) 1,
        ABS(DATE(2024, 1, 2)) 45293, DIVIDE("3,5", 1) 35 in an en-US model;
        DIVIDE("abc", 3), DIVIDE("", 3) and SIN("x") raise. Every one of
        these was BLANK, so Awesome Chocolates' QOQ item, which divides two
        CONVERT(..., STRING) values, coloured a fall as a rise."""
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            return v
        if not isinstance(v, (str, datetime, date)):
            return None
        n = _as_number(v)
        if n is None and isinstance(v, str):
            n = _arith_text_number(v, getattr(ctx, 'culture', None))
            if n is None:
                from pbix_mcp.errors import DAXEvaluationError
                raise DAXEvaluationError(
                    f"Cannot convert value '{v}' of type Text to type Number")
        return n

    def _num_arg(self, expr: str, ctx: DAXContext):
        """``expr`` evaluated as a numeric argument (``_num_operand``)."""
        return self._num_operand(self._eval_expr(expr.strip(), ctx), ctx)

    def _fn_divide(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        numerator = self._eval_expr(args[0].strip(), ctx)
        denominator = self._eval_expr(args[1].strip(), ctx)
        # DAX: the alternate result defaults to BLANK (None), not 0.
        alt = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else None
        # A BLANK NUMERATOR makes the whole division BLANK, and it does NOT
        # fall back to the alternate result. Verified against Desktop:
        #   DIVIDE(BLANK(), 100)     -> BLANK
        #   DIVIDE(BLANK(), 100, 42) -> BLANK   (not 42)
        #   DIVIDE(BLANK(), BLANK()) -> BLANK
        # Coercing the numerator to 0 returned 0.0 and made MS_Sales_Returns'
        # blank-driven ratios read as a real zero.
        if numerator is None:
            return None
        # Text, TRUE / FALSE and dates are numbers here (issue #139).
        numerator = self._num_operand(numerator, ctx)
        denominator = self._num_operand(denominator, ctx)
        if isinstance(numerator, (int, float)) and isinstance(denominator, (int, float)):
            if denominator == 0:
                return alt
            return numerator / denominator
        return alt

    def _fn_abs(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._num_arg(args_str, ctx)
        return abs(val) if isinstance(val, (int, float)) else None

    def _fn_round(self, args_str: str, ctx: DAXContext) -> Any:
        """ROUND(number, digits) -- half AWAY from zero, on the decimal value
        (issue #110). Python's round() takes a tie to the even neighbour and
        works on the binary double: ROUND(2.5, 0) was 2, ROUND(1250, -2) 1200
        and ROUND(1.005, 2) 1.0, where DAX gives 3, 1300 and 1.01. Negative
        digits round to tens, hundreds, ... The arithmetic is FORMAT's
        (_format_number, pinned off Desktop)."""
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        d = self._num_arg(args[1], ctx) if len(args) > 1 else 0
        digits = int(d) if d is not None else 0
        if not isinstance(val, (int, float)):
            return None
        try:
            q = decimal.Decimal(repr(val)).quantize(
                decimal.Decimal(1).scaleb(-digits), rounding=decimal.ROUND_HALF_UP,
                context=decimal.Context(prec=60))
        except (decimal.InvalidOperation, ValueError):
            return val          # NaN / infinity: nothing to round
        return int(q) if isinstance(val, int) and digits <= 0 else float(q)

    def _fn_rounddown(self, args_str: str, ctx: DAXContext) -> Any:
        """ROUNDDOWN(number, digits) — toward ZERO, not toward -infinity.

        ROUNDDOWN(-1.5, 0) is -1, where INT(-1.5) is -2. Using floor here would
        be wrong for every negative value, and calendar tables are full of them
        (a "days relative to today" column is negative for the whole past).
        """
        return self._round_directed(args_str, ctx, up=False)

    def _fn_roundup(self, args_str: str, ctx: DAXContext) -> Any:
        """ROUNDUP(number, digits) — away from zero."""
        return self._round_directed(args_str, ctx, up=True)

    def _round_directed(self, args_str: str, ctx: DAXContext, up: bool) -> Any:
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        if val is None:
            return None
        digits = 0
        if len(args) > 1:
            n = self._num_arg(args[1], ctx)
            digits = int(n) if n is not None else 0
        scale = 10.0 ** digits
        scaled = val * scale
        # math.floor/ceil on the ABSOLUTE value keeps the direction relative to
        # zero rather than to the number line.
        mag = math.ceil(abs(scaled) - 1e-9) if up else math.floor(abs(scaled) + 1e-9)
        out = math.copysign(mag, scaled) / scale
        return int(out) if digits <= 0 and float(out).is_integer() else out

    def _fn_weeknum(self, args_str: str, ctx: DAXContext) -> Any:
        """WEEKNUM(date, [return_type]) — week of year, 1-based.

        Return type 1 (the DAX default) starts the week on SUNDAY and puts
        January 1 in week 1; type 2 starts it on Monday. Both count the week
        containing Jan 1 as week 1, which is NOT the ISO rule, so
        ``date.isocalendar()`` cannot be used.
        """
        args = self._split_args(args_str)
        d = _as_datetime(self._eval_expr(args[0].strip(), ctx))
        if not d:
            return None
        rtype = 1
        if len(args) > 1:
            n = _as_number(self._eval_expr(args[1].strip(), ctx))
            rtype = int(n) if n is not None else 1
        jan1 = date(d.year, 1, 1)
        # How far into its own week Jan 1 falls, so the partial first week is
        # counted as week 1. Sunday-start (type 1) vs Monday-start (type 2).
        if rtype == 2:
            first_offset = jan1.weekday()            # Mon=0 .. Sun=6
        else:
            first_offset = (jan1.weekday() + 1) % 7  # Sun=0 .. Sat=6
        return ((d.date() - jan1).days + first_offset) // 7 + 1

    def _fn_int(self, args_str: str, ctx: DAXContext) -> Any:
        """INT(number) — rounds toward NEGATIVE INFINITY, not toward zero.

        Power BI Desktop gives INT(-1.5) = -2, where TRUNC(-1.5) and
        ROUNDDOWN(-1.5, 0) both give -1. Python's int() truncates, so this was
        off by one for every negative value and returned a plausible number
        rather than an error. It is not a corner case: binning expressions of
        the form INT(x / 5) * 5 are common, and any negative x -- a "days
        relative to today" column is negative for the whole past -- landed in
        the wrong bin.
        """
        val = self._num_arg(args_str, ctx)
        if not isinstance(val, (int, float)):
            return None
        return math.floor(val)

    def _fn_if(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        condition = self._eval_expr(args[0].strip(), ctx)
        if condition:
            return self._eval_expr(args[1].strip(), ctx)
        elif len(args) > 2:
            return self._eval_expr(args[2].strip(), ctx)
        return None

    def _fn_switch(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 3:
            return None
        test_val = self._eval_expr(args[0].strip(), ctx)
        i = 1
        while i < len(args) - 1:
            case_val = self._eval_expr(args[i].strip(), ctx)
            # = semantics: text case-insensitively (#107). A BLANK side keeps
            # the plain identity test.
            if (test_val == case_val if test_val is None or case_val is None
                    else _compare(test_val, '=', case_val)):
                return self._eval_expr(args[i + 1].strip(), ctx)
            i += 2
        # Default (odd number of remaining args)
        if len(args) % 2 == 0:
            return self._eval_expr(args[-1].strip(), ctx)
        return None

    def _fn_and(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        return all(self._eval_expr(a.strip(), ctx) for a in args)

    def _fn_or(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        return any(self._eval_expr(a.strip(), ctx) for a in args)

    def _fn_not(self, args_str: str, ctx: DAXContext) -> Any:
        return not self._eval_expr(args_str.strip(), ctx)

    def _fn_isblank(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._eval_expr(args_str.strip(), ctx)
        return val is None or val == ''

    def _fn_blank(self, args_str: str, ctx: DAXContext) -> Any:
        return None

    def _fn_calculate(self, args_str: str, ctx: DAXContext) -> Any:
        """CALCULATE(expression, filter1, filter2, ...)"""
        args = self._split_args(args_str)
        if not args:
            return None

        base_expr = args[0].strip()
        new_ctx = ctx
        # Keys this CALL has filtered, so two predicates on the SAME column
        # INTERSECT (DAX ANDs multiple filter arguments) while a predicate on a
        # column the OUTER context already filtered still REPLACES it -- that
        # override is the entire point of CALCULATE, so the two cases must not be
        # confused by reading them both out of filter_context.
        applied_here: dict = {}

        # Process filter arguments
        # Keys written by KEEPFILTERS(...) arguments: they intersect, so they do not trigger
        # the marked-date-table rule below (Desktop keeps the table's other filters there).
        keep_keys: set = set()
        _kf_before = None
        # DAX applies the MODIFIERS -- ALL*, REMOVEFILTERS, USERELATIONSHIP,
        # CROSSFILTER -- before the filter arguments, wherever they are written.
        # Taken in written order, `CALCULATE(e, T[c] = "x", ALL(T))` removed
        # the filter it had just applied and returned the all-T total (Desktop:
        # the "x" value, issue #87). Each group keeps its own order.
        _rest = [a.strip() for a in args[1:]]
        ordered = ([a for a in _rest if _CALC_MODIFIER_RE.match(a)]
                   + [a for a in _rest if not _CALC_MODIFIER_RE.match(a)])
        for filter_arg in ordered:
            if _kf_before is not None:
                keep_keys |= self._written_keys(_kf_before, new_ctx)
                _kf_before = None
            if filter_arg.upper().startswith('KEEPFILTERS'):
                _kf_before = new_ctx

            # REMOVEFILTERS / ALL
            if _CALC_REMOVE_RE.match(filter_arg):
                selected = filter_arg.upper().startswith('ALLSELECTED')
                # Bare ALLSELECTED() — no argument at all. The reference regex
                # below cannot match empty parens, so this form fell straight
                # through as a SILENT NO-OP and percent-of-total measures
                # rendered flat 1.0 (issue #26 r24#1). Desktop semantics:
                # restore the whole outer query context — drop the filters
                # grouping/CALCULATE added, put back the slicer selection.
                if selected and re.match(r"(?is)^ALLSELECTED\s*\(\s*\)\s*$",
                                         filter_arg.strip()):
                    # Every iteration's filter goes back to its rows or the
                    # selection; an explicit CALCULATE filter stays (#118:
                    # Desktop's CALCULATE(CALCULATE([Sales], ALLSELECTED()),
                    # T[Cat] = "A") is A's 40, not the 75 total).
                    _as_before = new_ctx
                    new_ctx = self._allselected_ctx(new_ctx, lambda k: True)
                    # What ALLSELECTED puts back is no new date filter: it does
                    # not clear the date table's other filters (#78) -- Desktop's
                    # CALCULATE(COUNTROWS(D), ALLSELECTED(D[Date])) inside an
                    # iteration over D keeps the row's month (build_b137.py).
                    keep_keys |= self._written_keys(_as_before, new_ctx)
                    continue
                # Bare REMOVEFILTERS() / ALL() — same empty-paren blind spot as
                # bare ALLSELECTED() above: the reference regex cannot match
                # empty parens, so CALCULATE(expr, REMOVEFILTERS()) evaluated
                # identically to expr (issue #50 — only the 33/231 grouped
                # cells whose own value happened to equal the all-cleared
                # value looked right). Desktop clears EVERY filter in the
                # context, whatever table it sits on.
                if not selected and re.match(
                        r"(?is)^(?:REMOVEFILTERS|ALL)\s*\(\s*\)\s*$",
                        filter_arg.strip()):
                    new_ctx = new_ctx.without_filters(
                        list(new_ctx.filter_context.keys()))
                    continue
                # ALLEXCEPT('T', T[keep1], ...) — starts with "ALL" so it
                # landed here, but the single-reference regex below cannot
                # parse a multi-argument call, so it fell through as a SILENT
                # NO-OP (issue #50; the standalone parser's __ALLEXCEPT__
                # marker had no consumer either). Desktop semantics: ALL(T),
                # then put back the filters on the listed columns — i.e. clear
                # every filter on T's EXPANDED table except the kept columns'
                # own. A kept column may sit on a related dimension: Desktop
                # 2.152 answers CALCULATE(SUM(Fact[v]), ALLEXCEPT(Fact,
                # Dim[Name])) under Dim[Name] with that member's 16 / 20 / 9,
                # where clearing every filter reaching Fact gave the 45 total.
                if filter_arg.upper().startswith('ALLEXCEPT'):
                    _inner = filter_arg[filter_arg.find('(') + 1:
                                        filter_arg.rfind(')')]
                    _parts = self._split_args(_inner)
                    if _parts:
                        _tbl = new_ctx.model_table(_parts[0].strip().strip("'"))
                        _kept = set()
                        for _p in _parts[1:]:
                            _km = re.match(
                                r"^\s*'?([^'\[\]]+?)'?\s*\[([^\]]+)\]\s*$",
                                _p.strip())
                            if _km:
                                _kept.add(f"{_km.group(1).strip()}."
                                          f"{_km.group(2).strip()}")
                        _exp = new_ctx.expanded_tables(_tbl)
                        new_ctx = new_ctx.without_columns(
                            lambda t, c: t in _exp and f"{t}.{c}" not in _kept)
                        new_ctx._no_propagate = new_ctx._no_propagate | {_tbl}
                        new_ctx._no_prop_keys = {
                            **new_ctx._no_prop_keys,
                            _tbl: new_ctx._all_snapshot(_tbl, _exp, _kept),
                        }
                    continue
                # Extract the column/table reference. Exclude [ ] from the table
                # capture so an UNQUOTED Sales[Region] splits into table=Sales +
                # col=Region (else the whole "Sales[Region]" was captured as a
                # table name and ALL/REMOVEFILTERS silently did nothing).
                inner_match = re.search(r"\(\s*'?([^'\)\[\]]+)'?\s*(?:\[([^\]]+)\])?\s*\)", filter_arg)
                if inner_match:
                    table = inner_match.group(1).strip()
                    col = inner_match.group(2)
                    if col and selected:
                        # ALLSELECTED(T[C]) restores the SLICER selection on
                        # that column: the grouping filter goes, the outer
                        # selection stays. Removing the filter outright made
                        # it behave exactly like ALL(T[C]) — under a slicer
                        # Cat IN (A,B) it answered the 180 grand total where
                        # Desktop answers 60 (issue #26 r24#2; two of these
                        # together stay per-column, fixing r24#3).
                        # An explicit filter on the column stays (#118: Desktop's
                        # CALCULATE(CALCULATE([Sales], ALLSELECTED(T[Cat])),
                        # T[Cat] = "A") is 40 at the total and 10 / 30 by zone).
                        key = f"{new_ctx.model_table(table)}.{col}".lower()
                        _as_before = new_ctx
                        new_ctx = self._allselected_ctx(new_ctx, lambda k: k.lower() == key)
                        keep_keys |= self._written_keys(_as_before, new_ctx)
                    elif col:
                        new_ctx = new_ctx.without_columns(
                            lambda t, c: t == table and c == col)
                    elif selected:
                        # ALLSELECTED(Table) RESTORES the query/slicer context
                        # on the table's EXPANDED table; it does not clear it.
                        # Drop what a CALCULATE inside this measure OR the
                        # grouped evaluation added, restore the slicer's own
                        # values. The expanded table takes in the one-side
                        # dimensions: under a visual grouped by Dim[Name],
                        # DIVIDE(SUM(Fact[v]), CALCULATE(SUM(Fact[v]),
                        # ALLSELECTED(Fact))) is each member's share (Desktop
                        # 2.152: 0.356 / 0.444 / 0.2), and dropping only Fact's
                        # own columns kept the Dim grouping and answered 1 on
                        # every row (PR #94; MS_Perf_Analyzer's Timeline reads
                        # 440 on its second edge, not 0). A selection on those
                        # dimensions is an outer filter and stays, so the
                        # scalar `CALCULATE(MIN(EventEdges[timestampMs]),
                        # ALLSELECTED(EventEdges))` under a component slicer
                        # still reads 0.
                        # An explicit filter inside the measure stays (#118:
                        # CALCULATE(CALCULATE([Sales], ALLSELECTED(T)),
                        # T[Cat] = "A") is A's 40 in Desktop).
                        exp = new_ctx.expanded_tables(new_ctx.model_table(table))
                        _as_before = new_ctx
                        new_ctx = self._allselected_ctx(
                            new_ctx, lambda k: k.partition('.')[0] in exp)
                        keep_keys |= self._written_keys(_as_before, new_ctx)
                    else:
                        # ALL(Table) / REMOVEFILTERS(Table) clear every column
                        # of the table's EXPANDED table: its own and those of
                        # the one-side dimensions it reaches (Desktop 2.152:
                        # CALCULATE(COUNTROWS(Dim), ALL(Fact)) under Dim[Name]
                        # is 3, not 1). A filter on any other table stays and
                        # still reaches this one through a many-to-many or a
                        # bidirectional relationship (_all_snapshot says which
                        # few it must still stop); ALLCROSSFILTERED removes
                        # those cross filters too.
                        table = new_ctx.model_table(table)
                        exp = new_ctx.expanded_tables(table)
                        new_ctx = new_ctx.without_columns(lambda t, c: t in exp)
                        new_ctx._no_propagate = new_ctx._no_propagate | {table}
                        new_ctx._no_prop_keys = {
                            **new_ctx._no_prop_keys,
                            table: (new_ctx._filter_snapshot(table)
                                    if filter_arg.upper().startswith('ALLCROSSFILTERED')
                                    else new_ctx._all_snapshot(table, exp)),
                        }
                continue

            # DATEADD — the shift is computed from the OUTER context (issue
            # #49: Desktop evaluates every filter argument there), then its
            # delta lands on the cumulative new_ctx.
            if filter_arg.upper().startswith('DATEADD'):
                _shifted_ctx = self._apply_dateadd_filter(filter_arg, ctx)
                if _shifted_ctx is None:
                    return None          # period outside the date table -> BLANK
                new_ctx = self._rebase_filter_delta(ctx, _shifted_ctx, new_ctx)
                continue

            # SAMEPERIODLASTYEAR
            if filter_arg.upper().startswith('SAMEPERIODLASTYEAR'):
                _shifted_ctx = self._apply_dateadd_filter(
                    f"DATEADD({filter_arg[19:-1].strip()}, -1, YEAR)", ctx)
                if _shifted_ctx is None:
                    return None          # period outside the date table -> BLANK
                new_ctx = self._rebase_filter_delta(ctx, _shifted_ctx, new_ctx)
                continue

            # USERELATIONSHIP(col1, col2) — activate a specific (usually inactive)
            # relationship for the wrapped expression, overriding the active one
            # on the same table pair. Rebuilds the propagation graph.
            if filter_arg.upper().startswith('USERELATIONSHIP'):
                new_ctx = self._apply_userelationship(filter_arg, new_ctx)
                continue

            # CROSSFILTER(col1, col2, direction) — override a relationship's
            # cross-filter direction (None / OneWay / Both) for the wrapped
            # expression.
            if filter_arg.upper().startswith('CROSSFILTER'):
                new_ctx = self._apply_crossfilter(filter_arg, new_ctx)
                continue

            # TREATAS — evaluated in the OUTER context (issue #49), applied
            # to the cumulative new_ctx. _fn_treatas returns a
            # ('__TREATAS__', (table, column), values) marker, which this branch
            # never recognised (it looked for a dict), so EVERY
            # CALCULATE(e, TREATAS(...)) applied no filter and returned the
            # unfiltered total (issue #85). The values replace the column's
            # filter like any filter argument -- KEEPFILTERS intersects -- and
            # an empty table filters to nothing.
            _tkf, _tinner = self._peel_call(filter_arg, ('KEEPFILTERS',))
            if _tinner.upper().startswith('TREATAS'):
                result = self._eval_expr(_tinner, ctx)
                if (isinstance(result, tuple) and len(result) == 3
                        and result[0] == '__TREATAS__'):
                    (_tt, _tc), _tvals = result[1], result[2]
                    key = f"{_tt}.{_tc}"
                    value: Any = list(dict.fromkeys(_tvals))
                    if _tkf and key not in applied_here:
                        # the filter left after the modifiers (#87)
                        outer = new_ctx.filter_context.get(key)
                        if outer is not None:
                            value = _keep_scope_tag(outer, {"all": [outer, value]})
                    if key in applied_here:
                        value = {"all": [applied_here[key], value]}
                    applied_here[key] = value
                    new_ctx = new_ctx.with_filters({key: value})
                elif (isinstance(result, tuple) and len(result) == 3
                        and result[0] == '__TREATAS_TUPLES__'):
                    # A filter on column COMBINATIONS (PR #99). Plain TREATAS
                    # replaces the outer filters on its columns -- a column
                    # filter, or that column's part of another combination
                    # filter -- and KEEPFILTERS intersects. Desktop 2.152,
                    # TREATAS({("N", "A"), ("W", "B")}, Region[Region],
                    # Prod[Cat]) under Region = S: 9 plain, BLANK kept.
                    cols = [(new_ctx.model_table(t), c) for t, c in result[1]]
                    colset = set(cols)
                    key = _tuple_filter_key(cols)
                    tspec = {'tuple_columns': [list(c) for c in cols],
                             'rows': [list(r) for r in dict.fromkeys(result[2])]}
                    if _tkf:
                        prev = new_ctx.filter_context.get(key)
                        if prev is not None:
                            tspec = _intersect_tuple_filters(prev, tspec)
                    else:
                        new_ctx = new_ctx.without_columns(
                            lambda t, c: (t, c) in colset,
                            keep_keys=applied_here)
                    if key in applied_here:
                        tspec = _intersect_tuple_filters(applied_here[key], tspec)
                    applied_here[key] = tspec
                    new_ctx = new_ctx.with_filters({key: tspec})
                elif isinstance(result, list) and not result:
                    return None
                elif isinstance(result, dict) and '__treatas__' in result:
                    extra = {}
                    for fk, fv in result.items():
                        if fk != '__treatas__':
                            extra[fk] = fv
                    if extra:
                        new_ctx = new_ctx.with_filters(extra)
                continue

            # Time-intelligence table filters (DATESYTD, DATESMTD, DATESQTD,
            # TOTALYTD, TOTALMTD, TOTALQTD, PREVIOUSMONTH, PREVIOUSQUARTER,
            # PREVIOUSYEAR, PARALLELPERIOD, etc.)
            ti_prefixes = ('DATESYTD', 'DATESMTD', 'DATESQTD', 'PREVIOUSMONTH',
                           'PREVIOUSQUARTER', 'PREVIOUSYEAR', 'NEXTMONTH',
                           'NEXTQUARTER', 'NEXTYEAR', 'PARALLELPERIOD',
                           'DATESBETWEEN', 'DATESINPERIOD',
                           # A single date is a date filter like any other: Desktop
                           # 2.157 gives CALCULATE(e, STARTOFYEAR(d)) the January
                           # value under every Period of the year.
                           'STARTOFMONTH', 'STARTOFQUARTER', 'STARTOFYEAR',
                           'ENDOFMONTH', 'ENDOFQUARTER', 'ENDOFYEAR')
            fa_upper = filter_arg.upper().split('(')[0].strip()
            if fa_upper in ti_prefixes:
                # Evaluated in the OUTER context (issue #49); the date-key
                # replacement below still applies to the cumulative new_ctx.
                result = self._eval_expr(filter_arg, ctx)
                if isinstance(result, list) and not result:
                    # A period that falls OUTSIDE the date table is an empty
                    # filter table, and an empty filter table means BLANK -- the
                    # same rule the generic FILTER branch below documents.
                    # Skipping it applied no filter at all and returned the
                    # GRAND TOTAL: MS_Sales_Returns' Calendar spans
                    # 2019-01-01..06-30, so PREVIOUSMONTH of the first month has
                    # no rows and Desktop shows [Net Sales PM] blank, while we
                    # reported the full 1,248,013 -- and every Variance /
                    # Indicator / "Last 2 Months" measure built on it inherited
                    # that wrong number.
                    return None
                if isinstance(result, list) and result:
                    # Time-intelligence returns list of date-row dicts
                    first = result[0]
                    if isinstance(first, dict) and '__table__' in first:
                        tbl_name = first['__table__']
                        col_name = first['__column__']
                        date_vals = [r['__value__'] for r in result]
                        # Remove the date table's other filters -- only where
                        # Power BI does (issue #78); elsewhere they intersect.
                        keys_to_remove = ([k for k in new_ctx.filter_context
                                           if k.startswith(f"{tbl_name}.")]
                                          if (tbl_name, col_name) in self._clearing_date_columns(ctx)
                                          else [])
                        # Derived, not rebuilt: a fresh DAXContext dropped the
                        # ALL(T) snapshot (and the grouping, the selection, ...),
                        # so a filter ALL had removed came back (#87).
                        new_ctx = new_ctx.without_filters(keys_to_remove).with_filters(
                            {f"{tbl_name}.{col_name}": date_vals})
                continue

            # FILTER(table, condition) or other table-returning expressions
            # Evaluate the filter arg — if it returns a list of row dicts,
            # extract filter values grouped by table.column.
            #
            # Evaluated against the OUTER ctx, not new_ctx: Desktop evaluates
            # CALCULATE's filter arguments independently in the outer filter
            # context and only THEN intersects/applies them (issue #49).
            # Sequential evaluation made VALUES(T[dim]) see the context
            # ALLSELECTED() had already widened, so
            # CALCULATE(MAX(col), ALLSELECTED(), VALUES(T[dim])) restored
            # only 133/231 grouped cells.
            result = self._eval_expr(filter_arg, ctx)
            if isinstance(result, list) and not result:
                # An EMPTY filter table removes every row, so the expression is
                # BLANK. Falling through here treated it as "no filter at all"
                # and returned the GRAND TOTAL: Agents_Performance's five
                # TopN/BottomN measures each reported a confident number
                # (19,260,876.5611 and friends) where Desktop shows nothing,
                # because their FILTER matched no rows. Same principle the
                # multi-hop propagation already documents -- an empty result is a
                # real result and must be applied.
                return None
            if isinstance(result, list) and result:
                first = result[0]
                if isinstance(first, dict) and first.get('__parts__'):
                    # A table of COMBINATIONS (a FILTER over a CROSSJOIN): one
                    # filter on the combinations of its columns, as TREATAS
                    # onto several columns makes (#103). Taking it as the left
                    # table's rows dropped the right table (issue #108).
                    cj_cols: list = []
                    for part in first['__parts__']:
                        pt = new_ctx.model_table(part.get('__table__', ''))
                        plain = self._row_cols(part)
                        cj_cols += ([(pt, c) for c in plain] if plain
                                    else [(pt, part.get('__column__', ''))])
                    cj_rows = [tuple(self._row_values(r)) for r in result
                               if isinstance(r, dict)]
                    cj_key = _tuple_filter_key(cj_cols)
                    cj_spec = {'tuple_columns': [list(c) for c in cj_cols],
                               'rows': [list(r) for r in dict.fromkeys(cj_rows)]}
                    if filter_arg.upper().startswith('KEEPFILTERS'):
                        prev = new_ctx.filter_context.get(cj_key)
                        if prev is not None:
                            cj_spec = _intersect_tuple_filters(prev, cj_spec)
                    else:
                        cj_set = set(cj_cols)
                        new_ctx = new_ctx.without_columns(
                            lambda t, c: (t, c) in cj_set, keep_keys=applied_here)
                    if cj_key in applied_here:
                        cj_spec = _intersect_tuple_filters(applied_here[cj_key], cj_spec)
                    applied_here[cj_key] = cj_spec
                    new_ctx = new_ctx.with_filters({cj_key: cj_spec})
                    continue
                if isinstance(first, dict) and '__table__' in first:
                    groups: dict = {}
                    if '__row__' in first:
                        # Multi-column row dict from ALL(Table) + FILTER
                        for row_item in result:
                            tbl_name = row_item['__table__']
                            for col_name, val in row_item.items():
                                if col_name.startswith('__'):
                                    continue
                                key = f"{tbl_name}.{col_name}"
                                if key not in groups:
                                    groups[key] = []
                                groups[key].append(val)
                    else:
                        # Single-column row dict from ALL(Table[Col]) or VALUES
                        for row_item in result:
                            key = f"{row_item['__table__']}.{row_item['__column__']}"
                            if key not in groups:
                                groups[key] = []
                            groups[key].append(row_item['__value__'])
                    if groups and '__row__' in first:
                        # A multi-column row set REPLACES the filter context of
                        # the tables it covers, propagation included -- the same
                        # thing a bare ALL(Table) does. Only adding the row
                        # values left a related dimension's filter in force on
                        # top of them: under a filter on Owners[Manager],
                        # `CALCULATE(AVERAGE('Cases'[CSAT]),
                        #  FILTER(ALL('Cases'),1=1))` averaged that manager's
                        # 3,914 rows where Desktop returns the global 4.2706.
                        #
                        # This is scoped by the SAME snapshot ALL uses, and that
                        # is what makes it safe. An earlier attempt suppressed
                        # the table outright and took [Actives] from Desktop's
                        # 32,401 to 1,260,817: it is
                        # `CALCULATE([EmpCount], FILTER(Employee, ...))`, and the
                        # blanket flag also blocked the Date[PeriodNumber] filter
                        # that [EmpCount] creates LATER from reaching Employee.
                        # With a snapshot there is nothing live to suppress at
                        # the grand total, so that filter still propagates.
                        tbls = {r['__table__'] for r in result
                                if isinstance(r, dict) and '__table__' in r}
                        new_ctx = new_ctx.without_filters(
                            [k for k in new_ctx.filter_context
                             if any(k.startswith(f"{t}.") for t in tbls)])
                        snaps = {t: new_ctx._filter_snapshot(t) for t in tbls}
                        new_ctx = new_ctx.with_filters(groups)
                        # These keys came from a TABLE filter argument, so they
                        # filter the EXPANDED table and may ride the reverse
                        # (many -> one) direction in _get_cross_table_filters.
                        new_ctx._expanded_keys = (
                            new_ctx._expanded_keys | set(groups))
                        new_ctx._no_propagate = new_ctx._no_propagate | tbls
                        new_ctx._no_prop_keys = {**new_ctx._no_prop_keys, **snaps}
                    elif groups:
                        # Single-column row set (ALL(T[Col]), VALUES): replaces
                        # the filter on that ONE column, and a filter reaching
                        # the table through a relationship still applies.
                        if filter_arg.upper().startswith('KEEPFILTERS'):
                            # KEEPFILTERS(FILTER(...)) INTERSECTS the outer filter on
                            # the column, as it does for a predicate. Desktop 2.157,
                            # grouped by the date itself: CALCULATE(COUNTROWS(F),
                            # KEEPFILTERS(FILTER(ALL(D[Date]), D[Date] <= MAX(D[Date]))))
                            # is that day's count, not the running total.
                            for key, vals in list(groups.items()):
                                # the filter left after the modifiers (#87)
                                outer = new_ctx.filter_context.get(key)
                                if outer is not None:
                                    groups[key] = _keep_scope_tag(outer, {"all": [outer, vals]})
                        new_ctx = new_ctx.with_filters(groups)
                continue

            # Any boolean predicate: =, <>, >, >=, <, <=, IN {...}, with NOT and
            # KEEPFILTERS peeled. Handled BEFORE the legacy equality regex below,
            # which would otherwise match the wrapper text of NOT(T[C] = v) and
            # register a filter on a column named "NOT(T".
            spec = self._calculate_filter_spec(filter_arg, ctx)
            if spec:
                key, value, keep = spec
                if keep and key not in applied_here:
                    # KEEPFILTERS: the predicate INTERSECTS the outer filter on
                    # the same column instead of overriding it. An empty
                    # intersection filters to zero rows, so SUM correctly goes
                    # BLANK (issue #35 — the passthrough made the override
                    # indistinguishable from not writing KEEPFILTERS at all).
                    # It intersects the filter left AFTER the modifiers: with
                    # ALL(T) earlier in the call there is nothing to intersect
                    # (Desktop: CALCULATE(e, ALL(T), KEEPFILTERS(T[c] = v)) is
                    # the v value, not BLANK -- #87).
                    outer = new_ctx.filter_context.get(key)
                    if outer is not None:
                        # Intersecting keeps the grouping -- or the iterated
                        # row -- in scope: Desktop answers CALCULATE(ISINSCOPE(c),
                        # KEEPFILTERS(c = v)) TRUE on every grouped row and every
                        # transitioned row (replacing it: FALSE). #69, #80.
                        value = _keep_scope_tag(outer, {"all": [outer, value]})
                if key in applied_here:
                    value = {"all": [applied_here[key], value]}
                applied_here[key] = value
                new_ctx = new_ctx.with_filters({key: value})
                continue

            # Simple column = value filter: Table[Col] = value
            eq_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]\s*=\s*(.*)", filter_arg)
            if eq_match:
                tbl_name = eq_match.group(1).strip()
                col_name = eq_match.group(2).strip()
                val = self._eval_expr(eq_match.group(3).strip(), ctx)
                if val is not None:
                    new_ctx = new_ctx.with_filters({f"{tbl_name}.{col_name}": [val]})
                continue

        if _kf_before is not None:
            keep_keys |= self._written_keys(_kf_before, new_ctx)
        if any(k.startswith(_TUPLE_FILTER_PREFIX) for k in new_ctx.filter_context):
            # A column this CALCULATE overwrote leaves an OUTER filter on
            # column combinations: DAX keeps its projection onto the others.
            over = {k for k in self._written_keys(ctx, new_ctx) - keep_keys
                    if not k.startswith(_TUPLE_FILTER_PREFIX)}
            if over:
                new_ctx = new_ctx.without_columns(
                    lambda t, c: f"{t}.{c}" in over,
                    keep_keys={k for k in new_ctx.filter_context
                               if not k.startswith(_TUPLE_FILTER_PREFIX)
                               or k in applied_here})
        new_ctx = self._drop_marked_date_table_filters(ctx, new_ctx, keep_keys)

        # CALCULATE performs the row->filter context transition: inside its
        # expression the iteration row's filters are real filters, plain
        # aggregates must NOT step back to the pre-transition outer context, and
        # the ROW CONTEXT ITSELF is consumed — a column ref inside CALCULATE
        # resolves against the (single-value) filter context, not the row (so
        # SELECTEDVALUE(T[C]) sees a column reference, exactly like Desktop).
        _prev_outer = getattr(new_ctx, '_outer_ctx', None)
        _prev_row = new_ctx._current_row
        new_ctx._outer_ctx = None
        new_ctx._current_row = None
        try:
            return self._eval_expr(base_expr, new_ctx)
        finally:
            new_ctx._outer_ctx = _prev_outer
            new_ctx._current_row = _prev_row

    @staticmethod
    def _written_keys(before: DAXContext, after: DAXContext) -> set:
        """filter_context keys `after` set that `before` did not have as the same object."""
        b, a = before.filter_context, after.filter_context
        return {k for k, v in a.items() if k not in b or b[k] is not v}

    def _clearing_date_columns(self, ctx: DAXContext) -> frozenset:
        """The (table, column) pairs whose filter makes Power BI clear the rest
        of that table's filters (issue #78).

        Desktop adds ALL(<table>) to a date-column filter only when the column
        is a MARKED date table's date column, or a DateTime column used in a
        relationship. On any other table the dates INTERSECT with the table's
        other filters (the classic "YTD equals the month until you mark the
        date table"). Measured on Power BI Desktop 2.152 over ADOMD with a
        marked daily table, a marked month-grain table, an unmarked copy, a
        sparse unmarked table and a table related on its DateTime column:
        applied at every place that clears date-table filters, all 4,896 cells
        compared through pbix_evaluate_dax match Desktop.

        Computed once per evaluation (relationships only change identity under
        USERELATIONSHIP, which builds a new list).
        """
        rels = ctx.relationships or []
        key = (id(ctx.tables), id(rels))
        cache = getattr(self, '_clearing_cache', None)
        if cache is not None and cache[0] == key:
            cached: frozenset = cache[1]
            return cached
        out = {(t, c) for t, c in (getattr(self, '_date_tables', None) or {}).items() if c}
        for rel in rels:
            for t, c in ((rel.get('FromTable'), rel.get('FromColumn')),
                         (rel.get('ToTable'), rel.get('ToColumn'))):
                if (t, c) not in out and self._column_is_datetime(ctx, t, c):
                    out.add((t, c))
        result = frozenset(out)
        self._clearing_cache = (key, result)
        return result

    @staticmethod
    def _column_is_datetime(ctx: DAXContext, table, column) -> bool:
        tbl = (ctx.tables or {}).get(table) or {}
        cols = tbl.get('columns') or []
        if column not in cols:
            return False
        i = cols.index(column)
        v = next((r[i] for r in (tbl.get('rows') or []) if r[i] is not None), None)
        return isinstance(v, (datetime, date))

    def _drop_marked_date_table_filters(self, outer: DAXContext, new_ctx: DAXContext,
                                        keep_keys: set | frozenset = frozenset()) -> DAXContext:
        """A filter on a MARKED date table's date column removes the table's
        other filters.

        When a table is marked as a date table, Desktop adds ALL(table) to a
        CALCULATE that filters its date column. Measured on Power BI Desktop
        2.157 (Microsoft's MIT Corporate Spend sample, 'Date' marked): the
        running total CALCULATE(SUM(..), FILTER(ALL('Date'[Date]), 'Date'[Date]
        <= MAX('Date'[Date]))) under Year x Period is the cumulative value, not
        the period's; CALCULATE(SUM(..), 'Date'[Date] <= DATE(2014, 6, 30)) under
        Year is the same for every year. Filters this CALCULATE wrote itself are
        kept. A date filter inside KEEPFILTERS does not trigger it: Desktop gives
        CALCULATE(e, KEEPFILTERS('Date'[Date] <= x)) under Year x Period the
        period's own value (the Period filter stays), measured on the same
        sample and on a daily marked table (Revenue Opportunities).

        "Written here" is identity: with_filters copies the outer dict, so a
        filter this CALCULATE did not touch is the same object as outside.
        """
        clearing = self._clearing_date_columns(outer)
        if not clearing:
            return new_ctx
        before, after = outer.filter_context, new_ctx.filter_context
        written = {k for k, v in after.items() if k not in before or before[k] is not v}
        drop = set()
        triggers = written - set(keep_keys)
        # Marked date columns and DateTime relationship columns alike (#78).
        for table, date_col in sorted(clearing):
            key = f"{table}.{date_col}"
            value = after.get(key)
            if key not in triggers or (isinstance(value, dict) and 'all' in value):
                continue
            drop |= {k for k in after if k.split('.', 1)[0] == table and k not in written}
        return new_ctx.without_filters(sorted(drop)) if drop else new_ctx

    @staticmethod
    def _peel_call(arg: str, names: tuple) -> tuple:
        """``NOT(x)`` -> ``('NOT', 'x')``; ``NOT x`` -> ``('NOT', 'x')``.

        Returns ``('', arg)`` when no wrapper applies. The closing paren must be
        the LAST character, i.e. the call has to wrap the whole argument -- a
        substring match would peel ``NOT(a) && b`` down to ``a``.
        """
        s = arg.strip()
        for name in names:
            if not s[:len(name)].upper() == name:
                continue
            rest = s[len(name):]
            if rest.startswith('('):
                depth, in_str = 0, False
                for i, ch in enumerate(rest):
                    if ch == '"':
                        in_str = not in_str
                    elif in_str:
                        continue
                    elif ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                        if depth == 0:
                            return (name, rest[1:i].strip()) if i == len(rest) - 1 \
                                else ('', s)
            elif rest[:1].isspace():
                return (name, rest.strip())
        return ('', s)

    def _calculate_filter_spec(self, filter_arg: str, ctx: DAXContext,
                               var_scope: dict | None = None) -> tuple:
        """A CALCULATE boolean filter argument as ``(key, spec, keep)``, or ``()``.

        Only ``Table[Col] = value`` used to be honoured; every other predicate
        fell off the end of the filter loop adding NO filter and NO warning, so
        CALCULATE quietly returned the UNFILTERED total (OpenBI findings #9 #5b).
        The filter context already evaluates structured predicates natively via
        :func:`make_value_matcher`, so this only has to translate into that form.

        ``NOT`` is folded into the operator rather than wrapped, and
        ``KEEPFILTERS`` is peeled: both used to be swallowed by the equality
        regex, which matched the WRAPPER text and registered a filter on a column
        named e.g. ``KEEPFILTERS(T``. ``keep`` is True when a KEEPFILTERS
        wrapper was peeled — the caller must then INTERSECT with the outer
        filter on the same column instead of replacing it (issue #35).
        """
        kf, inner = self._peel_call(filter_arg, ('KEEPFILTERS',))
        keep = bool(kf)
        negate = False
        while True:
            wrapper, peeled = self._peel_call(inner, ('NOT',))
            if not wrapper:
                break
            negate = not negate
            inner = peeled

        # ISBLANK(T[C]) / NOT ISBLANK(T[C]): applied no filter at all, so the
        # unfiltered total came back (issue #82).
        isb, isb_inner = self._peel_call(inner, ('ISBLANK',))
        if isb:
            cm = _WHOLE_TCOL_RE.match(isb_inner)
            if cm:
                key = f"{(cm.group(1) or cm.group(2) or '').strip()}.{cm.group(3).strip()}"
                return (key, {"is_blank": not negate}, keep)
            return ()

        m = _CALC_PRED_RE.match(inner)
        rev = None if m else _CALC_PRED_REV_RE.match(inner)
        if rev:
            # Literal-first form (issue #39): ``2024 = T[Year]``. Flip the
            # comparison so the column stays on the left; = and <> are
            # symmetric. Without this the argument fell off the end of the
            # filter loop, applied NOTHING, and returned the grand total.
            val_text, op, tbl, col = (rev.group(1).strip(), rev.group(2),
                                      rev.group(3).strip(),
                                      rev.group(4).strip())
            op = {'=': '=', '<>': '<>', '>': '<', '<': '>',
                  '>=': '<=', '<=': '>='}[op]
        if m or rev:
            if m:
                tbl, col, op, val_text = (m.group(1).strip(),
                                          m.group(2).strip(),
                                          m.group(3), m.group(4).strip())
            val = self._eval_expr(val_text, ctx, var_scope)
            if negate:
                op = {'=': '<>', '<>': '=', '>': '<=', '<=': '>',
                      '>=': '<', '<': '>='}[op]
            key = f"{tbl}.{col}"
            if val is None:
                # T[C] = BLANK() is a real filter: BLANK, and the zero of the
                # column's type, since BLANK = 0 and BLANK = "" are TRUE. It
                # used to apply no filter (issue #82); _compare does the
                # coercion.
                return (key, {"op": op, "value": None}, keep)
            # Plain equality keeps the historical In-set list form: it is what
            # every existing caller and test expects, and make_value_matcher
            # treats a list as string membership.
            if op == '=':
                if _blank_zero_of(val) == val:
                    # T[C] = 0 (or "", FALSE) keeps BLANK too: FILTER(ALL(T[C]),
                    # T[C] = 0) tests BLANK = 0, which is TRUE (Desktop: a
                    # numeric key's blank row is in ZD[Key] = 0).
                    return (key, [val, None], keep)
                return (key, [val], keep)
            return (key, {"op": op, "value": val}, keep)

        in_parts = self._split_in_scan(inner)
        if in_parts:
            left = in_parts[0].strip()
            lm = re.match(r"^'?([^'\[\]]+?)'?\s*\[([^\]]+)\]$", left)
            if lm:
                vals = self._in_set_values(in_parts[1], ctx, var_scope)
                if vals is None:
                    return ()
                key = f"{lm.group(1).strip()}.{lm.group(2).strip()}"
                return (key, {"not_in" if negate else "in": vals}, keep)
        return ()

    @staticmethod
    def _two_col_refs(filter_arg: str) -> list:
        """Extract the (table, column) pairs from a function's argument list,
        e.g. USERELATIONSHIP('Sales'[Ship Date], 'Date'[Date])."""
        pairs = re.findall(r"'?([^'\[\],(]+)'?\s*\[([^\]]+)\]", filter_arg)
        return [(t.strip(), c.strip()) for t, c in pairs]

    def _apply_userelationship(self, filter_arg: str, ctx: DAXContext) -> DAXContext:
        """USERELATIONSHIP(col1, col2): make the relationship joining col1 and
        col2 active for the wrapped expression, deactivating any other active
        relationship between the same two tables (only one may be active per
        pair). No-op (graceful) if no matching relationship exists."""
        refs = self._two_col_refs(filter_arg)
        if len(refs) < 2:
            return ctx
        (t1, c1), (t2, c2) = refs[0], refs[1]
        target = {(t1, c1), (t2, c2)}
        pair = {t1, t2}
        new_rels, changed = [], False
        for rel in ctx.relationships:
            r = dict(rel)
            rk = {(r.get('FromTable'), r.get('FromColumn')),
                  (r.get('ToTable'), r.get('ToColumn'))}
            rpair = {r.get('FromTable'), r.get('ToTable')}
            if rk == target:
                r['IsActive'] = 1
                changed = True
            elif rpair == pair and r.get('IsActive'):
                r['IsActive'] = 0  # deactivate the sibling active relationship
            new_rels.append(r)
        if not changed:
            return ctx
        return ctx.with_relationships(new_rels)

    def _apply_crossfilter(self, filter_arg: str, ctx: DAXContext) -> DAXContext:
        """CROSSFILTER(col1, col2, direction): override the cross-filter
        direction of the col1<->col2 relationship for the wrapped expression.
        direction: None -> inactive-for-filtering (0), OneWay -> single (1),
        Both -> bidirectional (2)."""
        refs = self._two_col_refs(filter_arg)
        if len(refs) < 2:
            return ctx
        (t1, c1), (t2, c2) = refs[0], refs[1]
        # third argument = direction keyword
        inner = filter_arg[filter_arg.find('(') + 1:filter_arg.rfind(')')]
        args = self._split_args(inner)
        direction = args[2].strip().upper().strip("'\"") if len(args) >= 3 else 'BOTH'
        xf = {'NONE': 0, 'ONEWAY': 1, 'BOTH': 2}.get(direction, 2)
        target = {(t1, c1), (t2, c2)}
        new_rels, changed = [], False
        for rel in ctx.relationships:
            r = dict(rel)
            rk = {(r.get('FromTable'), r.get('FromColumn')),
                  (r.get('ToTable'), r.get('ToColumn'))}
            if rk == target:
                r['CrossFilteringBehavior'] = xf
                # direction None makes the relationship stop propagating filters
                if xf == 0:
                    r['IsActive'] = 0
                changed = True
            new_rels.append(r)
        if not changed:
            return ctx
        return ctx.with_relationships(new_rels)

    @staticmethod
    def _rebase_filter_delta(base: DAXContext, shifted: DAXContext,
                             target: DAXContext) -> DAXContext:
        """Apply the filter delta between ``base`` and ``shifted`` to ``target``.

        CALCULATE evaluates every filter argument in the OUTER filter context
        (issue #49), so a time-intel shift is computed from ``base`` (the outer
        ctx) — but its result must land on ``target``, the context the
        preceding filter arguments already built, or those arguments would be
        silently discarded.
        """
        removed = [k for k in base.filter_context
                   if k not in shifted.filter_context
                   and k in target.filter_context]
        changed = {}
        for k, v in shifted.filter_context.items():
            old = base.filter_context.get(k)
            if old is v:
                continue
            try:
                if old == v:
                    continue
            except (TypeError, ValueError):
                pass
            changed[k] = v
        out = target.without_filters(removed) if removed else target
        if changed:
            out = out.with_filters(changed)
        return out

    def _apply_dateadd_filter(self, expr: str,
                              ctx: DAXContext) -> Optional[DAXContext]:
        """Apply DATEADD as a filter context modification.

        Returns None when the shifted period falls OUTSIDE the date table --
        an empty filter, which makes the whole CALCULATE BLANK. That is a real
        answer, not a missing one, and the caller must not treat it as "no
        filter".
        """
        # Parse DATEADD(column, offset, interval)
        match = re.search(r"DATEADD\s*\(\s*'?([^'\[]+)'?\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*(\w+)\s*\)", expr, re.IGNORECASE)
        if not match:
            # DATEADD over a TABLE of dates (DATESBETWEEN(...), FILTER(...)):
            # its shifted dates are the filter (issue #127). This returned the
            # context unchanged, so the filter argument was silently dropped.
            rows = self._eval_expr(expr.strip(), ctx)
            if not (isinstance(rows, list)
                    and all(isinstance(r, dict) and '__column__' in r for r in rows)):
                return ctx
            if not rows:
                return None
            date_table, date_col = rows[0]['__table__'], rows[0]['__column__']
            new_filters = {k: v for k, v in ctx.filter_context.items()
                           if not k.startswith(f"{date_table}.")}
            new_filters[f"{date_table}.{date_col}"] = [r['__value__'] for r in rows]
            _tc = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                             ctx.date_column, new_filters, ctx.relationships)
            _tc._filter_idx_cache = ctx._filter_idx_cache
            return _tc

        date_table = match.group(1).strip()
        date_col = match.group(2).strip()
        offset = int(match.group(3))
        interval = match.group(4).upper()

        # Every interval, via the same shift the table-expression form uses.
        # Only YEAR was implemented below, and only when the date table happened
        # to have a "Year" column, so DATEADD(..., -1, MONTH) was a silent no-op:
        # PMTD came back equal to MTD, and month-over-month measures built on it
        # collapsed to 0.
        shifted = self._dateadd_dates(date_table, date_col, offset, interval, ctx)
        if shifted:
            new_filters = {k: v for k, v in ctx.filter_context.items()
                           if not k.startswith(f"{date_table}.")}
            new_filters[f"{date_table}.{date_col}"] = shifted
            _nc = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                              ctx.date_column, new_filters, ctx.relationships)
            _nc._filter_idx_cache = ctx._filter_idx_cache
            return _nc

        # Get all date values from the date table
        tbl = ctx.tables.get(date_table)
        if not tbl:
            return ctx

        cols = tbl['columns']
        date_col_idx = None
        for i, c in enumerate(cols):
            if c == date_col:
                date_col_idx = i
                break
        if date_col_idx is None:
            return ctx

        # Get year column if available
        year_col_idx = None
        for i, c in enumerate(cols):
            if c.lower() == 'year':
                year_col_idx = i
                break

        if interval == 'YEAR' and year_col_idx is not None:
            # Shift years: get current year range and shift by offset
            current_years = set()
            for row in ctx.get_filtered_rows(date_table):
                yr = row[year_col_idx]
                if yr is not None:
                    current_years.add(yr)

            if not current_years:
                # No filter — use all years
                for row in tbl['rows']:
                    yr = row[year_col_idx]
                    if yr is not None:
                        current_years.add(yr)

            shifted_years = {yr + offset for yr in current_years}

            # Find dates that fall in the shifted year range
            shifted_dates = []
            for row in tbl['rows']:
                yr = row[year_col_idx]
                if yr in shifted_years:
                    shifted_dates.append(row[date_col_idx])

            if shifted_dates:
                new_filters = dict(ctx.filter_context)
                # Replace ALL date table filters with the shifted dates
                # Remove any existing date table filters (Year, Date, etc.)
                keys_to_remove = [k for k in new_filters if k.startswith(f"{date_table}.")]
                for k in keys_to_remove:
                    del new_filters[k]
                # Set the shifted date filter
                new_filters[f"{date_table}.{date_col}"] = shifted_dates
                _nc = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                                  ctx.date_column, new_filters, ctx.relationships)
                _nc._filter_idx_cache = ctx._filter_idx_cache
                return _nc

        # Nothing above produced a shifted set, and the date table and column
        # both resolve -- so the period genuinely falls OUTSIDE the calendar.
        # That is an EMPTY filter, and an empty filter means BLANK, never "no
        # filter". Returning ctx unchanged applied NO filter at all:
        # Ecommerce_Conversion's calendar starts 2025-01-01, so under
        # QuarterName=Q1 `DATEADD(dimDate[Date],-1,QUARTER)` asks for Oct-Dec
        # 2024, and [Page_Views_PMTD/PQTD] answered 14,548,763 where Desktop is
        # BLANK. The three *_%Delta measures divide by it and came out exactly
        # -1.0 -- DIVIDE(BLANK - P, P) -- against Desktop's BLANK.
        #
        # The TABLE form was already correct (COUNTROWS of the same DATEADD is
        # blank); only this filter path was wrong. Same rule the table-valued
        # time-intelligence branch of CALCULATE already documents.
        #
        # This must stay AFTER the year-column fallback above. A SPARSE date
        # table (the eight isolated days in tests/test_dax_engine.py) shifts to
        # dates that do not exist, so the per-run shift is legitimately empty
        # while the year fallback still resolves 2023 -> 2022 = 290. Deciding
        # BLANK before that ran turned three passing tests into None.
        if interval in ('DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR'):
            return None
        return ctx

    def _fn_removefilters(self, args_str: str, ctx: DAXContext) -> Any:
        """REMOVEFILTERS — returns a marker for CALCULATE to process."""
        return ('__REMOVEFILTERS__', args_str.strip())

    def _fn_all(self, args_str: str, ctx: DAXContext,
                no_blank_row: bool = False) -> Any:
        """ALL — when used inside CALCULATE returns a marker; when used as a
        table expression (e.g. ALL('table'[column])) returns all distinct
        values of that column ignoring any active filters. The table's blank
        row is included unless ``no_blank_row`` (ALLNOBLANKROW)."""
        ref = args_str.strip()
        multi = self._multi_column_all(ref, ctx, selected=False,
                                       no_blank_row=no_blank_row)
        if multi is not None:
            return multi
        # Try to parse as a column reference: 'table'[column]
        col_match = _WHOLE_TCOL_RE.match(ref)
        if col_match:
            # groups: 'quoted name' | bare name | column
            table_name = (col_match.group(1) or col_match.group(2) or '').strip()
            col_name = col_match.group(3).strip()
            # Return all values ignoring filters — use raw table data
            tbl = ctx.tables.get(table_name)
            if tbl:
                col_idx = ctx._find_col_idx(tbl['columns'], col_name)
                if col_idx >= 0:
                    # Every distinct value, a stored BLANK included (Desktop:
                    # ALLNOBLANKROW(RD[Name]) keeps the real blank, so ALL
                    # must), plus the blank row's BLANK (issue #82).
                    all_values = list(dict.fromkeys(row[col_idx] for row in tbl['rows']))
                    if (None not in all_values and not no_blank_row
                            and table_name in ctx.blank_row_tables()):
                        all_values.append(None)
                    return [{'__table__': table_name, '__column__': col_name,
                             '__value__': v} for v in all_values]

        # Table-level ALL: ALL('TableName') — return all rows as multi-column row dicts
        table_name = ref.strip("'").strip()
        tbl = ctx.tables.get(table_name)
        if tbl and tbl.get('rows'):
            # Return full row dicts with all columns for FILTER to iterate
            result = []
            cols = tbl['columns']
            for row in tbl['rows']:
                row_dict = {'__table__': table_name, '__row__': True}
                for ci, col_name in enumerate(cols):
                    row_dict[col_name] = row[ci] if ci < len(row) else None
                result.append(row_dict)
            if not no_blank_row and table_name in ctx.blank_row_tables():
                result.append(ctx.blank_row_dict(table_name))
            return result

        # Fallback: marker for CALCULATE
        return ('__ALL__', ref)

    @staticmethod
    def _shift_date(d, offset: int, interval: str, month_end: bool = True):
        """One DATEADD step. Month-family arithmetic is calendar-correct and
        clamps to the last day of the target month, as DAX does when the source
        day does not exist there (31 Mar -1 MONTH -> 28/29 Feb). With
        ``month_end`` a month's last day lands on the target month's last day
        (DATESINPERIOD's far end, the END of a DATEADD run); without it the day
        number is kept and clamped (a DATEADD run's start, a single date)."""
        if interval == 'DAY':
            return d + timedelta(days=offset)
        if interval == 'WEEK':
            return d + timedelta(weeks=offset)
        months = {'MONTH': 1, 'QUARTER': 3, 'YEAR': 12}.get(interval)
        if months is None:
            return None
        total = (d.month - 1) + months * offset
        year, month = d.year + total // 12, total % 12 + 1
        last_target = calendar.monthrange(year, month)[1]
        # A month END shifts to a month END. Clamping the day alone sent
        # 30-Nov -> 30-Oct, so a window ending at November's last day produced an
        # October window of 1..30: DATESMTD then dropped 31-Oct entirely and
        # per-employee PMTD came up short (employee 84: 19,839.8 vs Desktop's
        # 35,439.8 -- exactly the 15,600 booked on 31-Oct). This is also what
        # EDATE and Desktop's own DATEADD do.
        if month_end and d.day == calendar.monthrange(d.year, d.month)[1]:
            day = last_target
        else:
            day = min(d.day, last_target)
        return datetime(year, month, day)

    def _dateadd_dates(self, date_table: str, date_col: str, offset: int,
                       interval: str, ctx: DAXContext, source: list | None = None) -> list:
        """The date values DATEADD yields: every VISIBLE date shifted by
        offset x interval, kept only where the shifted date exists in the date
        table -- DAX drops shifts that fall outside it. ``source``: the dates
        of a table expression, shifted as they are (``_shift_date_table``).

        Shared by the table-expression form and CALCULATE's filter fast path so
        the two can never disagree.
        """
        tbl = ctx.tables.get(date_table)
        if not tbl:
            return []
        idx = ctx._find_col_idx(tbl['columns'], date_col)
        if idx < 0:
            return []
        universe: dict = {}
        for row in tbl['rows']:
            dv = _as_date(row[idx])
            if dv is not None:
                universe.setdefault(dv, row[idx])
        # No visible date shifts to no date (issue #141). This fell back to the
        # whole calendar: Power BI Desktop 2.152 (build_b141.py) gives BLANK for
        # CALCULATE(COUNTROWS(DATEADD(D[Date], 1, MONTH)), D[Month] = 199001),
        # where the engine counted 516.
        visible = source if source is not None else ctx.get_column_data(date_table, date_col)
        seen_src = [d for d in (_as_datetime(v) for v in visible) if d is not None]
        if not seen_src:
            return []
        # Shift each CONTIGUOUS RUN of the selection -- not each date
        # independently, and not the single min..max range.
        #
        # Mapping date-by-date leaves holes wherever no source date lands on a
        # target: nothing shifts onto Jan 29-31 (there is no "Feb 31"), nor onto
        # the 31st of any month whose successor has 30 days. Those holes
        # silently dropped a day of sales each -- PM Total Sales came out 11.9M
        # short of Desktop, and every PM/PQ/PY/MAT/YOY measure built on it
        # inherited the error. So a run is shifted as a PERIOD and refilled
        # contiguously over the date table.
        #
        # But one min..max range is only correct when the selection IS one
        # block. `'Date'[Qtr] = 2` selects seven DISJOINT quarters, and min..max
        # spans everything between the first and the last, so the filter
        # degenerated to the whole table: [New Hires SPLY] returned the grand
        # total 43120 under every quarter, where Desktop returns 11601 for Q2
        # and 13840 for Q3. Desktop's own COUNTROWS over the same shift is
        # 546 = 91 x 6 -- six shifted QUARTERS, not six years of dates -- and
        # 644 = 92 x 7 for a -1 MONTH shift, i.e. Mar+Apr+May in each of the
        # seven years. Per-run shifting reproduces both; a single range cannot.
        uni_sorted = sorted(universe)
        pos = {k: i for i, k in enumerate(uni_sorted)}
        runs: list = []
        for _d in sorted({(x.date() if isinstance(x, datetime) else x)
                          for x in seen_src}):
            i = pos.get(_d)
            if i is None:
                continue
            if runs and i == runs[-1][1] + 1:
                runs[-1][1] = i
            else:
                runs.append([i, i])
        # A run's START keeps its day number (clamped); only the END of a run
        # of two or more days that closes its month lands on the target
        # month's end (issue #129). Power BI Desktop 2.152: 30 Apr alone -1
        # MONTH is 30 Mar, 29..30 Apr is 29..31 Mar, 30 Apr..2 May is 30 Mar..
        # 2 Apr, 30 Nov..31 Dec -1 QUARTER is 30 Aug..30 Sep (32 days).
        out: dict = {}
        for a, b in runs:
            ka, kb = uni_sorted[a], uni_sorted[b]
            lo = self._shift_date(datetime(ka.year, ka.month, ka.day),
                                  offset, interval, month_end=False)
            hi = self._shift_date(datetime(kb.year, kb.month, kb.day),
                                  offset, interval, month_end=a != b)
            if lo is None or hi is None:
                continue
            lo_d = lo.date() if isinstance(lo, datetime) else lo
            hi_d = hi.date() if isinstance(hi, datetime) else hi
            if lo_d > hi_d:
                lo_d, hi_d = hi_d, lo_d
            for k in uni_sorted[bisect.bisect_left(uni_sorted, lo_d):
                                bisect.bisect_right(uni_sorted, hi_d)]:
                out[k] = universe[k]
        return [out[k] for k in sorted(out)]

    _DATEADD_ARGS_RE = re.compile(
        r"'?([^'\[]+)'?\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*(\w+)", re.IGNORECASE)

    def _fn_dateadd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATEADD(<dates>, offset, interval) as a real TABLE.

        It used to always return a ('__DATEADD__', text) marker, interpreted only
        when it sat DIRECTLY as a CALCULATE filter argument. Anywhere else -- and
        Power BI's own generated measures do
        ``CALCULATE([MTD], CALCULATETABLE(DATEADD('Date'[Date], -1, MONTH), ...))``
        -- the marker leaked: COUNTROWS(DATEADD(...)) was 0 and the shift silently
        did nothing, so PMTD equalled MTD and every month-over-month measure built
        on it collapsed.
        """
        m = self._DATEADD_ARGS_RE.match(args_str.strip())
        if not m:
            args = self._split_args(args_str)
            if len(args) == 3:
                shifted = self._shift_date_table(args[0], args[1], args[2].strip().upper(), ctx)
                if shifted is not None:
                    return shifted
            return ('__DATEADD__', args_str.strip())
        dt, dc = m.group(1).strip(), m.group(2).strip()
        dates = self._dateadd_dates(dt, dc, int(m.group(3)),
                                    m.group(4).upper(), ctx)
        return [{'__table__': dt, '__column__': dc, '__value__': v}
                for v in dates]

    def _shift_date_table(self, table_arg: str, offset_arg, interval: str,
                          ctx: DAXContext):
        """DATEADD / SAMEPERIODLASTYEAR over a TABLE expression of dates
        (DATESBETWEEN(...), FILTER(ALL('Date'[Date]), ...)): its dates are the
        ones shifted, as a column reference's visible dates are. Only a
        column reference was parsed, so these returned a marker and
        COUNTROWS / MINX over them saw nothing (issue #127). Power BI Desktop
        2.152: DATEADD(DATESBETWEEN('Date'[Date], DATE(2024,2,29),
        DATE(2024,2,29)), -1, YEAR) is 2023-02-28, and SAMEPERIODLASTYEAR of
        February 2024 is February 2023's 28 days. None when the table is not
        one column of dates."""
        rows = self._eval_expr(table_arg.strip(), ctx)
        if not isinstance(rows, list):
            return None
        lineage: tuple[str, str] | None = None
        values: list = []
        for r in rows:
            if not (isinstance(r, dict) and '__column__' in r and '__row__' not in r):
                return None
            key = (str(r.get('__table__')), str(r.get('__column__')))
            if lineage is None:
                lineage = key
            elif key != lineage:
                return None
            values.append(r.get('__value__'))
        if lineage is None:
            return []
        offset = offset_arg if isinstance(offset_arg, int) else self._eval_expr(str(offset_arg).strip(), ctx)
        if not isinstance(offset, (int, float)) or isinstance(offset, bool):
            return None
        dt, dc = lineage
        tbl = ctx.tables.get(dt)
        if not tbl:
            return None
        idx = ctx._find_col_idx(tbl['columns'], dc)
        if idx < 0:
            return None
        # The table's dates in the column's own spelling, so the filter keys
        # match the cells (DATESBETWEEN hands back 'YYYY-MM-DD' text).
        wanted = {d for d in (_as_date(v) for v in values) if d is not None}
        cells = list(dict.fromkeys(row[idx] for row in tbl['rows'] if _as_date(row[idx]) in wanted))
        # Exactly these dates, whatever else filters the date table (issue
        # #138). They were read back under the context with only the date
        # filter replaced, so an outer row's other columns cut the table down
        # to its own quarter, and to every date when none were left. Awesome
        # Chocolates' [Max Previous Quarter Not Blank] then answered the
        # quarter before each quarter. Desktop answers 20234 for every quarter.
        return [{'__table__': dt, '__column__': dc, '__value__': v}
                for v in self._dateadd_dates(dt, dc, int(offset), interval.rstrip('S'), ctx,
                                             source=cells)]

    def _fn_sameperiodlastyear(self, args_str: str, ctx: DAXContext) -> Any:
        """SAMEPERIODLASTYEAR(<dates>) == DATEADD(<dates>, -1, YEAR)."""
        ref = args_str.strip()
        m = re.match(r"^'?([^'\[]+)'?\s*\[([^\]]+)\]$", ref)
        if not m:
            shifted = self._shift_date_table(ref, -1, 'YEAR', ctx)
            return shifted if shifted is not None else ('__DATEADD__', ref)
        dt, dc = m.group(1).strip(), m.group(2).strip()
        return [{'__table__': dt, '__column__': dc, '__value__': v}
                for v in self._dateadd_dates(dt, dc, -1, 'YEAR', ctx)]

    def _fn_values(self, args_str: str, ctx: DAXContext) -> Any:
        arg = args_str.strip()
        tname = arg.strip("'")
        if tname in ctx.tables and not _WHOLE_TCOL_RE.match(arg):
            # VALUES(<table>): the visible rows -- duplicates kept, unlike
            # DISTINCT -- plus the blank row when it is visible (Desktop:
            # VALUES(BD) 4 rows, DISTINCT(BD) 3). It used to return nothing.
            rows = self._eval_tail(arg, ctx) or []
            if ctx.blank_row_visible(tname):
                rows = rows + [ctx.blank_row_dict(tname)]
            return rows
        ref = self._eval_expr(arg, ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            # Order-preserving dedup: Desktop iterates VALUES in data order, and
            # hash-set order made CONCATENATEX output nondeterministic.
            values = self._values_with_blank(ref[0], ref[1], ctx)
            # Return as row-dict list so CONCATENATEX / FILTER / iterators work
            return [{'__table__': ref[0], '__column__': ref[1], '__value__': v} for v in values]
        return []

    @staticmethod
    def _values_with_blank(table_name: str, col_name: str, ctx: DAXContext) -> list:
        """VALUES(T[C]) as a list: the visible distinct values, plus BLANK
        when T's blank row is visible (issue #82). Also what HASONEVALUE and
        SELECTEDVALUE count."""
        values = list(dict.fromkeys(ctx.get_column_data(table_name, col_name)))
        if None not in values and ctx.blank_row_visible(table_name):
            values.append(None)
        return values

    def _fn_selectedvalue(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        ref = self._eval_expr(args[0].strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            # IF(HASONEVALUE(c), VALUES(c), alt): the one value may be the
            # blank row's, and then the answer is BLANK, not alt (issue #82).
            values = self._values_with_blank(ref[0], ref[1], ctx)
            if len(values) == 1:
                return values[0]
        default = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else None
        return default

    def _fn_now(self, args_str: str, ctx: DAXContext) -> Any:
        """NOW() — current date and time."""
        return datetime.now()

    def _fn_today(self, args_str: str, ctx: DAXContext) -> Any:
        """TODAY() — current date at midnight."""
        now = datetime.now()
        return datetime(now.year, now.month, now.day)

    def _fn_utcnow(self, args_str: str, ctx: DAXContext) -> Any:
        """UTCNOW() — current UTC date and time. NAIVE like every other
        datetime this engine produces (utcnow() is deprecated; an aware value
        would break comparisons against the model's naive datetimes)."""
        return datetime.now(timezone.utc).replace(tzinfo=None)

    # --- date-part functions -------------------------------------------------
    # YEAR/MONTH/DAY/QUARTER and friends were not implemented at all, so any
    # expression using one (a Year calc column, a month grouping, a date-based
    # measure) evaluated to BLANK and was reported as an unsupported function.

    def _date_arg(self, args_str: str, ctx: DAXContext):
        """Evaluate a date-part function's single argument to a datetime."""
        return _as_datetime(self._eval_expr(args_str.strip(), ctx))

    def _fn_year(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.year if d else None

    def _fn_month(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.month if d else None

    def _fn_day(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.day if d else None

    def _fn_quarter(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return (d.month - 1) // 3 + 1 if d else None

    def _fn_hour(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.hour if d else None

    def _fn_minute(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.minute if d else None

    def _fn_second(self, args_str: str, ctx: DAXContext) -> Any:
        d = self._date_arg(args_str, ctx)
        return d.second if d else None

    def _fn_weekday(self, args_str: str, ctx: DAXContext) -> Any:
        """WEEKDAY(date, [type]) — 1=Sunday..7=Saturday by default (type 1)."""
        args = self._split_args(args_str)
        d = _as_datetime(self._eval_expr(args[0].strip(), ctx))
        if not d:
            return None
        wtype = 1
        if len(args) > 1:
            n = _as_number(self._eval_expr(args[1].strip(), ctx))
            wtype = int(n) if n is not None else 1
        iso = d.isoweekday()            # Mon=1 .. Sun=7
        if wtype == 2:
            return iso                  # Mon=1 .. Sun=7
        if wtype == 3:
            return iso - 1              # Mon=0 .. Sun=6
        return iso % 7 + 1              # Sun=1 .. Sat=7

    def _fn_date(self, args_str: str, ctx: DAXContext) -> Any:
        """DATE(year, month, day) — out-of-range month/day rolls over, as DAX does."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return None
        raw = [self._eval_expr(a.strip(), ctx) for a in args[:3]]
        parts = [_as_number(v) for v in raw]
        # A BLANK part counts as 0 and rolls over, exactly like an out-of-range
        # one -- only an ALL-BLANK call is blank. Verified against Desktop:
        #   DATE(2025, BLANK(), 4)          -> 12/4/2024   (month 0 of 2025)
        #   DATE(BLANK(), BLANK(), BLANK()) -> BLANK
        # Returning BLANK for any blank part broke the quick-measure idiom
        # `DATE(YEAR(x), MONTH(<blank>), DAY(x))`, which Ecommerce_Conversion's
        # PMTD/PQTD measures use to build the previous period's end date.
        if all(p is None for p in parts):
            return None
        if any(v is not None and p is None for v, p in zip(raw, parts)):
            return None          # a non-blank, non-numeric part is still an error
        y, m, d = (int(p) if p is not None else 0 for p in parts)
        month_zero = (m - 1)
        year = y + month_zero // 12
        month = month_zero % 12 + 1
        try:
            return datetime(year, month, 1) + timedelta(days=d - 1)
        except (ValueError, OverflowError):
            # Out of representable range -- a blank YEAR lands in year 0. Blank
            # rather than crash the whole measure.
            return None

    def _shift_months(self, d: datetime, n: int) -> datetime:
        month_zero = (d.month - 1) + n
        year = d.year + month_zero // 12
        month = month_zero % 12 + 1
        return datetime(year, month, 1)

    def _fn_edate(self, args_str: str, ctx: DAXContext) -> Any:
        """EDATE(date, months) — the same day-of-month N months away."""
        args = self._split_args(args_str)
        d = _as_datetime(self._eval_expr(args[0].strip(), ctx))
        n = _as_number(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        if not d or n is None:
            return None
        shifted = self._shift_months(d, int(n))
        last = monthrange(shifted.year, shifted.month)[1]
        return shifted.replace(day=min(d.day, last))

    def _fn_eomonth(self, args_str: str, ctx: DAXContext) -> Any:
        """EOMONTH(date, months) — last day of the month N months away."""
        args = self._split_args(args_str)
        d = _as_datetime(self._eval_expr(args[0].strip(), ctx))
        n = _as_number(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        if not d or n is None:
            return None
        shifted = self._shift_months(d, int(n))
        return shifted.replace(day=monthrange(shifted.year, shifted.month)[1])

    def _fn_datediff(self, args_str: str, ctx: DAXContext) -> Any:
        """DATEDIFF(start, end, interval) — interval BOUNDARIES crossed.

        Not elapsed time. DATEDIFF(DATE(2023,12,31), DATE(2024,1,1), YEAR) is
        1, because one year boundary is crossed, even though the dates are one
        day apart. Computing it as a truncated elapsed difference would give 0
        and be silently wrong on every year/quarter/month boundary.

        WEEK counts week boundaries, and a DAX week starts on SUNDAY. Python's
        ordinal 1 is a Monday, so Sundays are exactly the ordinals divisible by
        7 and ``ordinal // 7`` is the Sunday-started week index.

        The interval is a bare keyword (``WEEK``, not ``"WEEK"``), so it is read
        as a literal token rather than evaluated — evaluating it would hit the
        DAY/MONTH/YEAR/QUARTER/HOUR/MINUTE/SECOND functions of the same names.
        """
        args = self._split_args(args_str)
        if len(args) < 3:
            return None
        start = _as_datetime(self._eval_expr(args[0].strip(), ctx))
        end = _as_datetime(self._eval_expr(args[1].strip(), ctx))
        if start is None or end is None:
            return None
        interval = args[2].strip().strip('"\'').upper()

        if interval == 'YEAR':
            return end.year - start.year
        if interval == 'QUARTER':
            return ((end.year * 4 + (end.month - 1) // 3)
                    - (start.year * 4 + (start.month - 1) // 3))
        if interval == 'MONTH':
            return (end.year * 12 + end.month) - (start.year * 12 + start.month)
        if interval == 'WEEK':
            return end.date().toordinal() // 7 - start.date().toordinal() // 7
        if interval == 'DAY':
            return (end.date() - start.date()).days
        if interval in ('HOUR', 'MINUTE', 'SECOND'):
            unit = {'HOUR': 3600, 'MINUTE': 60, 'SECOND': 1}[interval]
            # Floor BOTH ends to the unit before subtracting, so this stays a
            # boundary count: 10:59:59 -> 11:00:01 is one HOUR, not zero. The
            # epoch is the DAX/Excel zero date and lands on midnight, so hour,
            # minute and second boundaries all align to it.
            epoch = datetime(1899, 12, 30)
            e = int((end - epoch).total_seconds())
            s = int((start - epoch).total_seconds())
            return e // unit - s // unit
        self._note_unsupported(f"DATEDIFF interval {interval}")
        return None

    # DAX date format tokens -> strftime. Ordered longest-first so "MMMM" isn't
    # eaten by "MM" (dict preserves insertion order).
    # Named pictures, matched case-insensitively before the token scan.
    # Desktop: FORMAT(DATE(2021,7,19), "Long Date")  -> "Monday, July 19, 2021"
    #          FORMAT(DATE(2021,7,19), "Short Date") -> "7/19/2021"
    _NAMED_DATE_FMTS = {
        'long date': 'dddd, mmmm d, yyyy',
        'medium date': 'dd-mmm-yy',
        'short date': 'm/d/yyyy',
        'general date': 'm/d/yyyy h:nn:ss AM/PM',
        'long time': 'h:nn:ss AM/PM',
        'medium time': 'h:nn AM/PM',
        'short time': 'HH:nn',
    }

    def _format_datetime_pattern(self, val, fmt_str: str) -> str:
        """Render a DAX/VBA date picture.

        The old table was .NET-cased -- `MM` month, `mm` MINUTES -- but DAX
        pictures are VBA-style and case-INSENSITIVE, where lower-case `m` is a
        MONTH. `mmmm` therefore matched `mm` twice and rendered "0000" instead
        of "July", and every `mm/dd/yyyy` came out "00/19/2021". Desktop:

            mmmm -> July     mmm -> Jul     mm -> 07     m -> 7
            d -> 19          m/d/yyyy on 2021-03-05 -> 3/5/2021

        `m` means minutes only when it FOLLOWS an hour token, which is what
        Desktop's "mm hh:mm" -> "07 12:00" pins: the first is a month, the one
        after `hh:` is minutes. `nn` is always minutes.
        """
        named = self._NAMED_DATE_FMTS.get(fmt_str.strip().lower())
        if named:
            fmt_str = named
        if isinstance(val, datetime):
            hh24, mi, ss = val.hour, val.minute, val.second
        else:
            hh24 = mi = ss = 0
        h12 = hh24 % 12 or 12
        out: list = []
        prev = ''          # last TOKEN emitted, separators ignored
        i, n = 0, len(fmt_str)
        while i < n:
            low = fmt_str[i:].lower()
            if low.startswith('am/pm') or low.startswith('a/p'):
                tok = 'am/pm' if low.startswith('am/pm') else 'a/p'
                src = fmt_str[i:i + len(tok)]
                ampm = 'AM' if hh24 < 12 else 'PM'
                if tok == 'a/p':
                    ampm = ampm[0]
                out.append(ampm if src[0].isupper() else ampm.lower())
                i += len(tok); prev = 'ampm'; continue
            ch = low[0]
            run = 0
            while i + run < n and fmt_str[i + run].lower() == ch:
                run += 1
            if ch == 'y':
                out.append(f"{val.year % 100:02d}" if run <= 2
                           else f"{val.year:04d}")
            elif ch == 'd':
                out.append({1: str(val.day), 2: f"{val.day:02d}"}.get(
                    run, calendar.day_abbr[val.weekday()] if run == 3
                    else calendar.day_name[val.weekday()]))
            elif ch == 'n':
                out.append(f"{mi:02d}" if run >= 2 else str(mi))
            elif ch == 'h':
                use24 = fmt_str[i].isupper()
                v = hh24 if use24 else h12
                out.append(f"{v:02d}" if run >= 2 else str(v))
            elif ch == 's':
                out.append(f"{ss:02d}" if run >= 2 else str(ss))
            elif ch == 'm':
                if prev == 'h':                      # minutes only after hours
                    out.append(f"{mi:02d}" if run >= 2 else str(mi))
                    ch = 'n'
                elif run == 1:
                    out.append(str(val.month))
                elif run == 2:
                    out.append(f"{val.month:02d}")
                elif run == 3:
                    out.append(calendar.month_abbr[val.month])
                else:
                    out.append(calendar.month_name[val.month])
            else:
                out.append(fmt_str[i:i + run])
                i += run
                continue                              # separators keep `prev`
            i += run
            prev = ch
        return ''.join(out)

    def _fn_format(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        fmt = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else None
        # FORMAT(value, format, [locale]) -- DAX's third argument names the
        # culture to render in. It was ignored entirely, so there was no way
        # to ask for non-en-US separators at all (issue #60). The argument
        # wins over the model's own culture; the model's culture is the
        # fallback; en-US when neither states one.
        culture = None
        if len(args) > 2:
            loc = self._eval_expr(args[2].strip(), ctx)
            if isinstance(loc, str) and loc.strip():
                culture = loc.strip()
        if culture is None:
            culture = getattr(ctx, 'culture', None)
        if val is None:
            return ''
        # A date can reach here as an ISO STRING rather than a datetime (row
        # context over a generated table, a CALENDAR result, a text column
        # holding dates). Without this, FORMAT([Date], "MMMM") returned the raw
        # "2015-01-01T00:00:00" instead of "January" — a silently wrong value,
        # which is worse than an error because nothing surfaces it.
        if fmt and isinstance(val, str) and not isinstance(val, (datetime, date)):
            _f = str(fmt).strip().lower()
            if (_f in self._NAMED_DATE_FMTS
                    or any(t in _f for t in ('yy', 'mm', 'dd', 'hh', 'nn',
                                             'ss', 'am/pm'))):
                coerced = _as_datetime(val)
                if coerced is not None:
                    val = coerced
        # Datetime formatting (NOW()/TODAY()/date columns).
        if isinstance(val, (datetime, date)) and fmt:
            return self._format_datetime_pattern(val, str(fmt))
        if fmt and isinstance(val, (int, float)) and not isinstance(val, bool):
            group_sep, dec_sep = _culture_seps(culture)
            out = _format_number(float(val), str(fmt), group_sep, dec_sep)
            if out is not None:
                return out
        return _concat_str(val)

    def _fn_concatenate(self, args_str: str, ctx: DAXContext) -> Any:
        """CONCATENATE(a, b) -- the function form of `&`, and it renders its
        arguments the same way.

        `str(x or '')` dropped every FALSY value, so a legitimate 0 vanished:
        MS_Regional_Sales' [Fcst adj slicer alt text] read
        "...current value is " where Desktop reads "...current value is 0".
        _concat_str is the one renderer for both forms, so a number also comes
        out with DAX's 15-digit formatting rather than Python's repr.
        """
        args = self._split_args(args_str)
        return ''.join(_concat_str(self._eval_expr(a.strip(), ctx))
                       for a in args)

    def _fn_sumx(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMX(table_expression, expression) — iterate over table rows, sum expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            total: float = 0
            seen = False
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        total += result
                        seen = True
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        total += result
                        seen = True
            # SUMX over nothing is BLANK, matching SUM and Desktop
            # (SUMX(FILTER({1,2,3}, FALSE()), 1) is BLANK, not 0).
            return total if seen else None
        return None

    def _fn_maxx(self, args_str: str, ctx: DAXContext) -> Any:
        """MAXX(table_expression, expression) — iterate over table rows, return max."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            max_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    max_val = _extremum(max_val, result, True)
                else:
                    result = self._eval_expr(row_expr, ctx)
                    max_val = _extremum(max_val, result, True)
            return max_val
        # Fallback: if table_ref is a column ref, get max of column
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return max(values) if values else None
        return None

    def _fn_minx(self, args_str: str, ctx: DAXContext) -> Any:
        """MINX(table_expression, expression) — iterate over table rows, return min."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            min_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # _make_row_context handles both single-column (ALL(T[c]))
                    # and multi-column (bare-table ALL(T)/FILTER(T)) row dicts;
                    # the old __column__/__value__ access KeyError'd on the latter.
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    min_val = _extremum(min_val, result, False)
                else:
                    result = self._eval_expr(row_expr, ctx)
                    min_val = _extremum(min_val, result, False)
            return min_val
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return min(values) if values else None
        return None

    def _fn_averagex(self, args_str: str, ctx: DAXContext) -> Any:
        """AVERAGEX(table_expression, expression) — iterate over table rows, average expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        values = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
        return sum(values) / len(values) if values else None

    def _fn_countx(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTX(table_expression, expression) — count non-blank numeric results per row."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        count = 0
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                else:
                    result = self._eval_expr(row_expr, ctx)
                if result is not None and result != '':
                    count += 1
        return count or None

    def _fn_countax(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTAX(table_expression, expression) — count non-blank results (like COUNTX but counts text too)."""
        # In DAX, COUNTAX counts non-blank values of any type; functionally same as COUNTX here
        return self._fn_countx(args_str, ctx)

    def _fn_countblank(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTBLANK(column) — count blank values in a column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            return sum(1 for v in values if v is None or v == '') or None
        return None

    def _fn_product(self, args_str: str, ctx: DAXContext) -> Any:
        """PRODUCT(column) — multiply all values in column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = [v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float))]
            if not values:
                return None
            result = 1
            for v in values:
                result *= v
            return result
        return None

    def _fn_median(self, args_str: str, ctx: DAXContext) -> Any:
        """MEDIAN(column) — return median value."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = sorted(v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float)))
            if not values:
                return None
            return statistics.median(values)
        return None

    def _fn_medianx(self, args_str: str, ctx: DAXContext) -> Any:
        """MEDIANX(table, expression) — median of the per-row expression.

        Missing entirely before: IT_Support's "2- Median Color Coding" feeds it
        into `>=` comparisons, so a BLANK made every branch fall through to the
        same colour.
        """
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if not isinstance(table_ref, list):
            return None
        values = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                result = self._eval_expr(row_expr, row_ctx)
                result = self._resolve_row_result(result, row_item, row_ctx)
            else:
                result = self._eval_expr(row_expr, ctx)
            if isinstance(result, (int, float)) and not isinstance(result, bool):
                values.append(result)
        # MEDIANX over no rows is BLANK, so ISBLANK fires.
        return statistics.median(sorted(values)) if values else None

    def _fn_mround(self, args_str: str, ctx: DAXContext) -> Any:
        """MROUND(number, multiple) — round to the nearest multiple of `multiple`.

        DAX rounds HALF AWAY FROM ZERO (MROUND(2.5, 1) = 3), unlike Python's
        banker's rounding, and MROUND(x, 0) is 0.
        """
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        num = self._num_arg(args[0], ctx)
        mult = self._num_arg(args[1], ctx)
        if not isinstance(num, (int, float)) or not isinstance(mult, (int, float)):
            return None
        if mult == 0:
            return 0                     # Desktop: MROUND(7, 0) is 0
        if num != 0 and (num > 0) != (mult > 0):
            # Desktop REFUSES a sign mismatch ("an argument to MROUND has the
            # wrong data type, or the result is too large or too small"), so
            # returning a number here would be inventing an answer.
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(
                f"MROUND({num}, {mult}): number and multiple must have the "
                "same sign")
        q = num / mult
        rounded = math.floor(q + 0.5) if q >= 0 else math.ceil(q - 0.5)
        return rounded * mult

    def _fn_firstnonblank(self, args_str: str, ctx: DAXContext) -> Any:
        """FIRSTNONBLANK(column, expression) — first value whose expr is non-blank.

        Returns a ONE-ROW TABLE, like Desktop: it is normally used as a
        CALCULATE filter or wrapped in a scalar conversion.
        """
        return self._first_last_nonblank(args_str, ctx, last=False)

    def _fn_lastnonblank(self, args_str: str, ctx: DAXContext) -> Any:
        """LASTNONBLANK(column, expression) — last value whose expr is non-blank."""
        return self._first_last_nonblank(args_str, ctx, last=True)

    def _first_last_nonblank(self, args_str: str, ctx: DAXContext, last: bool) -> Any:
        """Shared body for FIRST/LASTNONBLANK.

        Both were unimplemented, which is worse than it sounds: MS_Life_Expectancy
        uses LASTNONBLANK as a CALCULATE filter argument, and a BLANK filter
        argument applied NO filter at all, so six measures returned the grand
        total instead of the last populated year.
        """
        args = self._split_args(args_str)
        if not args:
            return []
        col = self._parse_column_ref(args[0]) or self._as_column_ref(args[0], ctx)
        universe = None
        if col is None:
            # The first argument is a TABLE EXPRESSION, not a column reference.
            # MS_Life_Expectancy writes `LASTNONBLANK(ALL(Years[Years]), [...])`
            # in five measures; requiring a bare column reference made every one
            # of them BLANK.
            src = self._eval_expr(args[0].strip(), ctx)
            if not isinstance(src, list) or not src:
                return []
            first = src[0]
            if not isinstance(first, dict) or '__column__' not in first:
                return []
            table_name = first['__table__']
            col_name = first['__column__']
            universe = [r.get('__value__') for r in src
                        if isinstance(r, dict)]
        else:
            table_name, col_name = col
        expr = args[1].strip() if len(args) > 1 else None
        values = []
        seen = set()
        for v in (universe if universe is not None
                  else ctx.get_column_data(table_name, col_name)):
            key = str(v)
            if key in seen:
                continue
            seen.add(key)
            values.append(v)
        values = [v for v in values if v is not None]
        try:
            values.sort(key=lambda v: (v is None, v))
        except TypeError:
            values.sort(key=lambda v: (v is None, str(v)))
        if last:
            values.reverse()
        for v in values:
            if expr is None:
                return self._one_row_table(table_name, col_name, v)
            row_item = {'__table__': table_name, '__column__': col_name,
                        '__value__': v}
            row_ctx = self._make_row_context(row_item, ctx)
            probe = self._eval_expr(expr, row_ctx)
            probe = self._resolve_row_result(probe, row_item, row_ctx)
            if probe is not None and probe != '':
                return self._one_row_table(table_name, col_name, v)
        return []

    @staticmethod
    def _one_row_table(table_name: str, col_name: str, value: Any) -> list:
        return [{'__table__': table_name, '__column__': col_name,
                 '__value__': value}]

    def _as_column_ref(self, text: str, ctx: DAXContext):
        """`text` as a (table, column) pair, however it is written."""
        ref = self._eval_expr(text.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            return ref
        return None

    def _fn_isinscope(self, args_str: str, ctx: DAXContext) -> Any:
        """ISINSCOPE(column) — is the column a GROUPING level of this query?

        TRUE when the column's current filter is the query's own grouping
        (GroupByValues / GroupByPredicate, see there): the caller says which
        filter_context keys are group-by axes (``group_by`` on
        pbix_evaluate_dax / evaluate_measures_smart). A single-cell evaluation
        with no grouping answers FALSE, as Desktop's grand total does.

        It is NOT the same question as ISFILTERED -- Desktop, over the same model,
        answers

            CALCULATE(ISINSCOPE('Risk'[Location]), 'Risk'[Location] = "x")  FALSE
            CALCULATE(ISFILTERED('Risk'[Location]), 'Risk'[Location] = "x")  TRUE

        and a column the query filters without grouping it is ISFILTERED but not
        ISINSCOPE. KEEPFILTERS on a grouped column keeps it in scope. A context
        transition also brings the iterated row's columns into
        scope (RowContextValues, see there); without one, a row context leaves
        ISINSCOPE to the enclosing filter context.
        """
        # ISINSCOPE takes a column REFERENCE: parse it like the plain aggregates do,
        # so a row context cannot collapse it to the current row's value.
        ref = self._parse_column_ref(args_str) or self._eval_expr(args_str.strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return False
        return self._in_scope(f"{ref[0]}.{ref[1]}", ctx)

    def _in_scope(self, key: str, ctx: DAXContext) -> bool:
        value = ctx.filter_context.get(key)
        if isinstance(value, (GroupByValues, GroupByPredicate)):
            return True
        outer = getattr(ctx, '_outer_ctx', None)
        if isinstance(value, (RowContextValues, RowContextPredicate)):
            # The iteration's row: in scope once CALCULATE / a measure reference
            # made it a filter; before that, only what was in scope outside.
            return True if outer is None else self._in_scope(key, outer)
        return False

    def _fn_error(self, args_str: str, ctx: DAXContext) -> Any:
        """ERROR("message") — raise a DAX error.

        Quick measures generate `IF(<misuse>, ERROR("..."), <real body>)` guard
        clauses. Reaching it means the guard tripped, which Desktop surfaces as
        an error rather than a value.
        """
        from pbix_mcp.errors import DAXEvaluationError
        msg = self._eval_expr(args_str.strip(), ctx) if args_str.strip() else ''
        raise DAXEvaluationError(f"DAX ERROR(): {msg}")

    def _fn_filter(self, args_str: str, ctx: DAXContext) -> Any:
        """FILTER(table, condition) — returns filtered table rows."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if isinstance(table_ref, list):
            filtered = []
            cond_expr = args[1].strip()
            # A BARE column reference (`Sales[Amount] > 90`) evaluates to an
            # unresolved ('Table','Column') marker, so the comparison yielded
            # None and FILTER dropped every row. In a row context a bare
            # reference IS that row's value, so substitute the row's values
            # before evaluating. Conditions containing an aggregation
            # (`SUM(Sales[Amount]) > 90`) must NOT be substituted — those
            # aggregate over the context, so they keep the filter-context path.
            substitute_row_values = not _AGG_CALL_RE.search(cond_expr)
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    if '__row__' in row_item:
                        # Multi-column row dict from ALL(Table)
                        table_name = row_item['__table__']
                        extra_filters = {}
                        for col_name, val in row_item.items():
                            if col_name.startswith('__'):
                                continue
                            # A row transition like any iterator's: in scope
                            # after CALCULATE (Desktop: COUNTROWS(FILTER(T,
                            # CALCULATE(IF(ISINSCOPE(T[Cat]), 1, 0)) = 1)) is
                            # 4), and ALLSELECTED puts back FILTER's rows
                            # (issue #118). Plain lists read as explicit
                            # CALCULATE filters.
                            extra_filters[f"{table_name}.{col_name}"] = _row_value([val], table_ref)
                        row_ctx = self._transitioned_ctx(ctx, extra_filters)
                        if substitute_row_values:
                            row_cond = _substitute_row_refs(
                                cond_expr, table_name, row_item)
                            # Text substitution only rewrites the QUALIFIED
                            # form. Bind the row as well so an UNQUALIFIED
                            # `[Col]` resolves to this row's value: Desktop
                            # accepts both, and MS_Perf_Analyzer's
                            # FILTER('Events', [component] = "Change Detection")
                            # counted 0 rows against Desktop's 3 because the
                            # bare reference read as a missing measure.
                            # Only under the same no-aggregation guard --
                            # binding a row while the condition contains
                            # SUM(T[c]) would collapse the aggregate to the
                            # row's own value.
                            row_ctx._current_row = row_item
                            row_ctx._outer_ctx = ctx
                        else:
                            row_cond = cond_expr
                        cond = self._eval_expr(row_cond, row_ctx)
                        if cond:
                            filtered.append(row_item)
                    else:
                        # Single-column row dict from ALL(Table[Column]) or VALUES
                        table_name = row_item['__table__']
                        col_name = row_item['__column__']
                        # _make_row_context, not a bare with_filters: it also
                        # BINDS the row, which is what lets a column reference in
                        # the condition resolve to this row's value. Without the
                        # binding, `DimEmployee[EmployeeKey]` fell through to an
                        # unresolved ('Table','Column') marker, so ISBLANK said
                        # False and the BLANK (unknown) member survived
                        # `NOT ISBLANK(DimEmployee[EmployeeKey])` -- FILTER
                        # returned 262 rows where Desktop has 261, which pushed
                        # Rank MTD Asc to 128 against Desktop's 127. Every other
                        # iterator (MAXX/MINX/RANKX) already used this helper.
                        row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                        cond = self._eval_expr(args[1].strip(), row_ctx)
                        if cond:
                            filtered.append(row_item)
            return filtered
        return []

    # =========================================================================
    # Table functions
    # =========================================================================

    def _fn_topn(self, args_str: str, ctx: DAXContext) -> Any:
        """TOPN(n, table, orderBy, order) — return top N rows."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        n = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(n, (int, float)):
            return []
        n = int(n)
        table_ref = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(table_ref, list):
            return []

        order_expr = args[2].strip() if len(args) > 2 else None
        # order: 1 or ASC = ascending, 0 or DESC = descending (default DESC)
        descending = True
        if len(args) > 3:
            order_val = args[3].strip().upper()
            if order_val in ('1', 'ASC'):
                descending = False

        if order_expr:
            # Evaluate order expression for each row and sort
            scored = []
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # _make_row_context handles BOTH single-column
                    # (__column__/__value__) and bare-table (__row__) iterators;
                    # the old direct __column__ lookup KeyError'd on a bare table.
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    score = self._eval_expr(order_expr, row_ctx)
                else:
                    score = self._eval_expr(order_expr, ctx)
                scored.append((row_item, score if isinstance(score, (int, float)) else 0))
            scored.sort(key=lambda x: x[1], reverse=descending)
            return [item for item, _ in scored[:n]]
        else:
            return table_ref[:n]

    def _fn_addcolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """ADDCOLUMNS(table, name, expression, ...) — add computed columns to table."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return table_ref

        # Parse name/expression pairs
        extended = []
        for row_item in table_ref:
            new_item = dict(row_item) if isinstance(row_item, dict) else row_item
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
            else:
                row_ctx = ctx
            # Process name/expression pairs
            i = 1
            while i + 1 < len(args):
                col_name = self._eval_expr(args[i].strip(), ctx)
                col_val = self._eval_expr(args[i + 1].strip(), row_ctx)
                if isinstance(new_item, dict):
                    new_item[str(col_name)] = col_val
                i += 2
            extended.append(new_item)
        return extended

    def _fn_summarize_rollup(self, args_str: str, ctx: DAXContext):
        """SUMMARIZE with ROLLUP(...)/ROLLUPGROUP(...) group columns: group as
        usual, then append one subtotal row per rollup level; ISSUBTOTAL
        extension columns answer True on those rows."""
        parts = self._split_args(args_str)
        rollup_cols = []
        flat = []
        for a in parts:
            st = a.strip()
            up = st.upper()
            if up.startswith("ROLLUP") and not up.startswith("ROLLUPGROUP"):
                inner = self._split_args(st[st.index("(") + 1:-1])
                for x in inner:
                    xs = x.strip()
                    if xs.upper().startswith("ROLLUPGROUP"):
                        for y in self._split_args(xs[xs.index("(") + 1:-1]):
                            flat.append(y)
                            rollup_cols.append(y.strip())
                    else:
                        flat.append(xs)
                        rollup_cols.append(xs)
            else:
                flat.append(st)
        issub_pairs = []          # (name, colref_text)
        pruned = []
        j = 0
        while j < len(flat):
            nxt = flat[j + 1].strip().upper() if j + 1 < len(flat) else ""
            if flat[j].strip().startswith('"') and nxt.startswith("ISSUBTOTAL"):
                inner = flat[j + 1].strip()
                colref = inner[inner.index("(") + 1:-1].strip()
                nm = self._eval_expr(flat[j].strip(), ctx)
                issub_pairs.append((str(nm), colref))
                j += 2
                continue
            pruned.append(flat[j])
            j += 1
        base = self._fn_summarize(", ".join(pruned), ctx)
        if not isinstance(base, list):
            return base
        for r in base:
            for nm, _cref in issub_pairs:
                if isinstance(r, dict):
                    r[nm] = False
        if rollup_cols and base:
            tname = base[0].get("__table__") if isinstance(base[0], dict) else None
            sub = {"__table__": tname, "__row__": True}
            for rc in rollup_cols:
                m = _TCOL_RE.match(rc)
                if m:
                    sub[m.group(3)] = None
            for nm, _cref in issub_pairs:
                sub[nm] = True
            base = base + [sub]
        return base

    def _fn_summarize(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMMARIZE(table, groupBy..., [name, expression]...) — group + aggregate.

        The trailing ``"Name", <expression>`` EXTENSION columns used to be
        skipped entirely, so ``SUMMARIZE(Sales, Sales[Cat], "Total",
        SUM(Sales[Amount]))`` silently returned only the group column and the
        aggregated value vanished. Each extension is now evaluated per group,
        in a filter context restricted to that group's key.
        """
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_name = args[0].strip().strip("'")
        rows = ctx.get_filtered_rows(table_name)
        tbl = ctx.tables.get(table_name)
        if tbl is None:
            # A table EXPRESSION, not a table name. Only a bare name was ever
            # accepted, so `SUMMARIZE(ALLSELECTED(f[a], f[b]), f[a], f[b], ...)`
            # -- the shape a quick measure generates -- produced NO ROWS at all
            # and every VAR built on it went blank.
            src = self._eval_expr(args[0].strip(), ctx)
            if not isinstance(src, list) or not src:
                return []
            first = src[0]
            if not isinstance(first, dict) or '__table__' not in first:
                return []
            table_name = first['__table__']
            tbl = ctx.tables.get(table_name)
            if tbl is None:
                return []
            cols = tbl['columns']
            rows = []
            for rd in src:
                if not isinstance(rd, dict):
                    continue
                if '__row__' in rd or '__column__' not in rd:
                    rows.append([rd.get(c) for c in cols])
                else:
                    # Single-column shape: place the value in its own column.
                    row = [None] * len(cols)
                    ci = ctx._find_col_idx(cols, rd.get('__column__', ''))
                    if ci >= 0:
                        row[ci] = rd.get('__value__')
                    rows.append(row)
        if not tbl or not rows:
            return []

        # Split the tail into group-by column refs and (name, expression) pairs.
        # A quoted string literal starts the extension-column section.
        group_cols: list = []
        remote_cols: list = []
        extensions: list = []
        i = 1
        while i < len(args):
            arg = args[i].strip()
            if arg.startswith('"'):
                break
            ref = self._eval_expr(arg, ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                ref_table, ref_col = ref
                if ref_table == table_name or ref_table not in ctx.tables:
                    col_idx = ctx._find_col_idx(tbl['columns'], ref_col)
                    if col_idx >= 0:
                        group_cols.append((ref_col, col_idx))
                else:
                    # Grouping the base table by a RELATED table's column — the
                    # canonical "fact by dimension" shape. This used to be
                    # dropped, so the whole result came back empty.
                    remote_cols.append((ref_table, ref_col))
            i += 1
        while i + 1 < len(args):
            name = args[i].strip().strip('"')
            extensions.append((name, args[i + 1].strip()))
            i += 2

        if remote_cols:
            return self._summarize_with_related(
                table_name, tbl, group_cols, remote_cols, extensions, ctx)
        if not group_cols:
            return []

        seen = set()
        result = []
        for row in rows:
            key = tuple(row[idx] for _, idx in group_cols)
            if key in seen:
                continue
            seen.add(key)
            row_dict = {'__table__': table_name}
            for col_name, col_idx in group_cols:
                row_dict[col_name] = row[col_idx]
            # Use first group col as the iteration column
            row_dict['__column__'] = group_cols[0][0]
            row_dict['__value__'] = row[group_cols[0][1]]
            result.append(row_dict)
        if extensions:
            # Each group is an ITERATION's row: ALLSELECTED inside an
            # extension puts back the groups, not the group (#118). As plain
            # lists they read as explicit filters, and IT Support's "Time
            # Wasting Patterns" counted 1 alert where Desktop counts 4.
            group_names = [c for c, _i in group_cols]
            for row_dict in result:
                group_ctx = self._transitioned_ctx(ctx, {
                    f"{table_name}.{c}": _row_value([row_dict[c]], result)
                    for c in group_names})
                for ext_name, ext_expr in extensions:
                    row_dict[ext_name] = self._eval_expr(ext_expr, group_ctx)
        return result

    def _summarize_with_related(self, table_name, tbl, group_cols, remote_cols,
                                extensions, ctx: DAXContext) -> Any:
        """SUMMARIZE where at least one group-by column lives on a RELATED table.

        Each candidate group is expressed as a filter context and handed to the
        engine's own relationship propagation, so the base rows for the group
        are resolved exactly the way every other filtered evaluation resolves
        them. Combinations with no base rows are skipped, matching DAX (which
        only returns combinations present in the table).
        """
        import itertools

        axes = []
        for col_name, col_idx in group_cols:
            vals = list(dict.fromkeys(
                r[col_idx] for r in ctx.get_filtered_rows(table_name)))
            axes.append([(f"{table_name}.{col_name}", col_name, v)
                         for v in vals])
        for rt, rc in remote_cols:
            rtbl = ctx.tables.get(rt) or {}
            ridx = ctx._find_col_idx(rtbl.get('columns', []), rc)
            if ridx < 0:
                return []
            # The base rows whose key matches no row of rt group under BLANK
            # (Desktop: COUNTROWS(SUMMARIZE(BF, BD[Name])) = 4, issue #82).
            vals = self._values_with_blank(rt, rc, ctx)
            axes.append([(f"{rt}.{rc}", rc, v) for v in vals])
        if not axes:
            return []

        # Guard against a combinatorial blow-up on wide group-by sets.
        total = 1
        for a in axes:
            total *= max(len(a), 1)
        if total > 100_000:
            return []

        result = []
        combos = []
        for combo in itertools.product(*axes):
            filters = {key: [val] for key, _disp, val in combo}
            if not ctx.with_filters(filters).get_filtered_rows(table_name):
                continue  # combination doesn't exist in the base table
            row_dict = {'__table__': table_name}
            for _key, disp, val in combo:
                row_dict[disp] = val
            first_disp, first_val = combo[0][1], combo[0][2]
            row_dict['__column__'] = first_disp
            row_dict['__value__'] = first_val
            result.append(row_dict)
            combos.append(combo)
        if extensions and combos:
            # The groups are an iteration's rows; ALLSELECTED in an extension
            # puts back the groups, per column (#118).
            shadows: dict = {}
            for combo in combos:
                for key, _disp, val in combo:
                    t, _, c = key.partition('.')
                    shadows.setdefault(key, []).append(
                        {'__table__': t, '__column__': c, '__value__': val})
            for row_dict, combo in zip(result, combos):
                group_ctx = self._transitioned_ctx(ctx, {key: _row_value([val], shadows[key])
                                                         for key, _disp, val in combo})
                for ext_name, ext_expr in extensions:
                    row_dict[ext_name] = self._eval_expr(ext_expr, group_ctx)
        return result

    def _fn_summarizecolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMMARIZECOLUMNS(groupBy1, ..., name, expression) — summarize with measures.
        Simplified: treats it like SUMMARIZE for the group-by columns."""
        args = self._split_args(args_str)
        if not args:
            return []
        # IGNORE(expr) marks a measure excluded from the result AND from
        # blank-group filtering; with the pair dropped, auto-exist over the
        # group columns is exactly what remains (Desktop golden: 3 groups).
        pruned = []
        skip_next_of_ignore = False
        for j, a in enumerate(args):
            if skip_next_of_ignore:
                skip_next_of_ignore = False
                continue
            nxt = args[j + 1].strip().upper() if j + 1 < len(args) else ""
            if (a.strip().startswith('"') and nxt.startswith('IGNORE')):
                skip_next_of_ignore = True
                continue
            pruned.append(a)
        args = pruned
        # NONVISUAL(filter): the marker only affects visual-total
        # behaviour, invisible in a plain query -- unwrap and apply the
        # inner table as a filter (Desktop: TREATAS({"X"}, K[grp]) under
        # NONVISUAL narrows the result to the X group, 350).
        kept = []
        for a in args:
            st = a.strip()
            if st.upper().startswith('NONVISUAL') and st.endswith(')'):
                inner_f = st[st.index('(') + 1:-1].strip()
                fres = self._eval_expr(inner_f, ctx)
                if (isinstance(fres, tuple) and len(fres) == 3
                        and fres[0] == '__TREATAS__'):
                    (_t, _c), _vals = fres[1], fres[2]
                    ctx = ctx.with_filters({f"{_t}.{_c}": list(_vals)})
                    continue
                frows = ([r for r in fres
                          if isinstance(r, dict) and '__table__' in r]
                         if isinstance(fres, list) else None)
                if frows:
                    filters: dict = {}
                    for r in frows:
                        t = r.get('__table__')
                        if r.get('__column__'):
                            filters.setdefault(
                                f"{t}.{r['__column__']}", []).append(
                                r.get('__value__'))
                        else:
                            for k, v in self._row_cols(r).items():
                                filters.setdefault(f"{t}.{k}", []).append(v)
                    ctx = ctx.with_filters(
                        {k: list(dict.fromkeys(v))
                         for k, v in filters.items()})
                continue
            kept.append(a)
        args = kept
        # ROLLUPADDISSUBTOTAL(col, "name"): group by col, then append one
        # subtotal row (Desktop golden: 3 groups + 1 subtotal = 4).
        rollup_subtotal = False
        expanded = []
        for a in args:
            st = a.strip()
            if st.upper().startswith('ROLLUPADDISSUBTOTAL'):
                _ra_args = self._split_args(st[st.index('(') + 1:-1])
                if _ra_args:
                    expanded.append(_ra_args[0])
                rollup_subtotal = True
            else:
                expanded.append(a)
        args = expanded
        # Find group-by columns (column refs) vs name/expression pairs (string, expression)
        group_refs = []
        for arg in args:
            ref = self._eval_expr(arg.strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                group_refs.append(ref)
            else:
                break  # Rest are name/expression pairs
        if not group_refs:
            return []
        # Use first table as base. Forward the trailing "Name", <expression>
        # extension columns too — they were dropped, so the aggregated value
        # silently disappeared from the result.
        table_name = group_refs[0][0]
        inner = f"'{table_name}', " + ", ".join(
            f"'{t}'[{c}]" for t, c in group_refs)
        tail = args[len(group_refs):]
        if tail:
            inner += ", " + ", ".join(a.strip() for a in tail)
        result = self._fn_summarize(inner, ctx)
        if isinstance(result, list):
            result = self._summarizecolumns_blank_group(
                result, group_refs, tail, ctx)
        if rollup_subtotal and isinstance(result, list):
            sub = {'__table__': group_refs[0][0], '__row__': True}
            for _t, _c in group_refs:
                sub[_c] = None
            result = result + [sub]
        return result

    def _summarizecolumns_blank_group(self, result: list, group_refs: list,
                                      tail: list, ctx: DAXContext) -> list:
        """SUMMARIZECOLUMNS' (Blank) group: the group-by table's blank row,
        kept, like any group, only when an expression is non-blank on it
        (Desktop: BD[Name] grouped gives (Blank) = 20, issue #82)."""
        tables = {t for t, _c in group_refs}
        if len(tables) != 1:
            return result
        table_name = next(iter(tables))
        if not ctx.blank_row_visible(table_name):
            return result
        if any(isinstance(r, dict) and all(r.get(c) is None for _t, c in group_refs)
               for r in result):
            return result
        exts = []
        rest = [a.strip() for a in tail]
        while rest and not rest[0].startswith('"'):
            rest.pop(0)
        for i in range(0, len(rest) - 1, 2):
            exts.append((rest[i].strip('"'), rest[i + 1]))
        bctx = ctx.with_filters({f"{table_name}.{c}": [None] for _t, c in group_refs})
        vals = {name: self._eval_expr(expr, bctx) for name, expr in exts}
        if exts and all(v is None for v in vals.values()):
            return result
        row: dict = {'__table__': table_name}
        for _t, c in group_refs:
            row[c] = None
        row.update(vals)
        row['__column__'] = group_refs[0][1]
        row['__value__'] = None
        return result + [row]

    def _fn_selectcolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """SELECTCOLUMNS(table, name, expression, ...) — select/rename columns."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
            else:
                row_ctx = ctx
            new_row = {}
            i = 1
            first_name = None
            first_val = None
            while i + 1 < len(args):
                col_name = self._eval_expr(args[i].strip(), ctx)
                col_val = self._eval_expr(args[i + 1].strip(), row_ctx)
                col_name_str = str(col_name) if col_name else f"col_{i}"
                new_row[col_name_str] = col_val
                if first_name is None:
                    first_name = col_name_str
                    first_val = col_val
                i += 2
            if isinstance(row_item, dict) and '__table__' in row_item:
                # A multi-column row (bare table ref / ALL(Table)) carries
                # __row__ and has NO __column__/__value__; indexing them
                # unconditionally raised KeyError, so SELECTCOLUMNS over a
                # plain table crashed.
                new_row['__table__'] = row_item['__table__']
                for meta in ('__column__', '__value__', '__row__'):
                    if meta in row_item:
                        new_row[meta] = row_item[meta]
            result.append(new_row)
        return result

    def _fn_distinct(self, args_str: str, ctx: DAXContext) -> Any:
        """DISTINCT(column_or_table) — distinct values, respecting filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            seen = set()
            result = []
            for v in values:
                key = str(v)
                if key not in seen:
                    seen.add(key)
                    result.append({'__table__': ref[0], '__column__': ref[1], '__value__': v})
            return result
        if isinstance(ref, list):
            # Deduplicate table rows as DAX compares their values: text
            # case-insensitively (Desktop: COUNTROWS(DISTINCT({"a", "A", "b"}))
            # is 2; issue #107), numbers by value; the first spelling stays.
            seen_rows: set = set()
            result = []
            for item in ref:
                vals = self._row_values(item) if isinstance(item, dict) else [item]
                row_key = tuple(('t', _text_key(v)) if isinstance(v, str)
                                else _value_keys(v, False)[0] for v in vals)
                if row_key not in seen_rows:
                    seen_rows.add(row_key)
                    result.append(item)
            return result
        return []

    def _fn_union(self, args_str: str, ctx: DAXContext) -> Any:
        """UNION(table1, table2) — combine two tables."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        result = []
        if isinstance(t1, list):
            result.extend(t1)
        if isinstance(t2, list):
            result.extend(t2)
        return result

    def _fn_except(self, args_str: str, ctx: DAXContext) -> Any:
        """EXCEPT(table1, table2) — rows in table1 not in table2."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list):
            return []
        if not isinstance(t2, list):
            return t1
        t2_keys = set()
        for item in t2:
            if isinstance(item, dict) and '__value__' in item:
                t2_keys.add(str(item['__value__']))
            else:
                t2_keys.add(str(item))
        result = []
        for item in t1:
            if isinstance(item, dict) and '__value__' in item:
                key = str(item['__value__'])
            else:
                key = str(item)
            if key not in t2_keys:
                result.append(item)
        return result

    def _fn_intersect(self, args_str: str, ctx: DAXContext) -> Any:
        """INTERSECT(table1, table2) — rows in both tables."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list) or not isinstance(t2, list):
            return []
        t2_keys = set()
        for item in t2:
            if isinstance(item, dict) and '__value__' in item:
                t2_keys.add(str(item['__value__']))
            else:
                t2_keys.add(str(item))
        result = []
        for item in t1:
            if isinstance(item, dict) and '__value__' in item:
                key = str(item['__value__'])
            else:
                key = str(item)
            if key in t2_keys:
                result.append(item)
        return result

    def _fn_crossjoin(self, args_str: str, ctx: DAXContext) -> Any:
        """CROSSJOIN(table1, table2, ...) — cartesian product.

        Each row keeps its PARTS, one row per source table (``__parts__``), so
        an iterator's row context reads every table's columns and a context
        transition filters all of them. The merged keys (left as is, right
        prefixed ``_2_``) stay for the callers that read them; on their own
        they hid the right table: FILTER(CROSSJOIN(VALUES(R[Region]),
        VALUES(P[Cat])), P[Cat] = "B") counted nothing (issue #108)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        tables = [self._eval_expr(a.strip(), ctx) for a in args]
        if not all(isinstance(t, list) for t in tables):
            return []

        def _parts(item) -> tuple:
            if not isinstance(item, dict):
                return ()
            return tuple(item.get('__parts__') or (item,))

        result = tables[0]
        for t2 in tables[1:]:
            joined = []
            for item1 in result:
                for item2 in t2:
                    merged = {}
                    if isinstance(item1, dict):
                        merged.update(item1)
                    if isinstance(item2, dict):
                        merged.update({f"_2_{k}": v for k, v in item2.items()
                                       if k != '__parts__'})
                    merged['__parts__'] = _parts(item1) + _parts(item2)
                    joined.append(merged)
            result = joined
        return result

    def _fn_datatable(self, args_str: str, ctx: DAXContext) -> Any:
        """DATATABLE(name, type, ..., {{val1, val2}, {val3, val4}}) — create inline table."""
        # Split column definitions from data block
        # Find the outermost { which starts the data rows
        brace_pos = args_str.find('{{')
        if brace_pos < 0:
            brace_pos = args_str.find('{')

        if brace_pos < 0:
            return []

        col_defs_str = args_str[:brace_pos].rstrip().rstrip(',')
        data_block = args_str[brace_pos:]

        # Parse column definitions: "name", TYPE pairs
        col_args = self._split_args(col_defs_str)
        col_names = []
        col_types = []
        i = 0
        while i + 1 < len(col_args):
            name_val = col_args[i].strip().strip('"\'')
            type_val = col_args[i + 1].strip().upper()
            if type_val in ('INTEGER', 'STRING', 'BOOLEAN', 'DOUBLE', 'CURRENCY', 'DATETIME'):
                col_names.append(name_val)
                col_types.append(type_val)
                i += 2
            else:
                break

        if not col_names:
            return []

        # Parse data rows from {{ ... }, { ... }} blocks
        rows = []
        # Extract individual row blocks {val1, val2}
        row_pattern = re.findall(r'\{([^{}]+)\}', data_block)
        for row_str in row_pattern:
            values = [v.strip().strip('"\'') for v in row_str.split(',')]
            if len(values) >= len(col_names):
                row = {}
                for j, cn in enumerate(col_names):
                    raw = values[j]
                    if col_types[j] == 'INTEGER':
                        try: row[cn] = int(float(raw))
                        except: row[cn] = 0
                    elif col_types[j] in ('DOUBLE', 'CURRENCY'):
                        try: row[cn] = float(raw)
                        except: row[cn] = 0.0
                    elif col_types[j] == 'BOOLEAN':
                        row[cn] = raw.upper() in ('TRUE', '1')
                    else:
                        row[cn] = raw
                row['__table__'] = '__datatable__'
                row['__column__'] = col_names[0]
                row['__value__'] = row.get(col_names[0])
                rows.append(row)

        return rows

    def _fn_row(self, args_str: str, ctx: DAXContext) -> Any:
        """ROW(name, expression, ...) — single row table."""
        args = self._split_args(args_str)
        row = {'__table__': '__row__'}
        i = 0
        first_name = None
        while i + 1 < len(args):
            col_name = self._eval_expr(args[i].strip(), ctx)
            col_val = self._eval_expr(args[i + 1].strip(), ctx)
            col_name_str = str(col_name) if col_name else f"col_{i}"
            row[col_name_str] = col_val
            if first_name is None:
                first_name = col_name_str
                row['__column__'] = col_name_str
                row['__value__'] = col_val
            i += 2
        return [row]

    def _fn_treatas(self, args_str: str, ctx: DAXContext) -> Any:
        """TREATAS(table, column1, column2, ...) — apply table values as filter.
        Returns a marker for CALCULATE to process."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        # Extract target column references
        target_cols = []
        for i in range(1, len(args)):
            ref = self._eval_expr(args[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                target_cols.append(ref)
        if isinstance(table_ref, list) and len(target_cols) > 1:
            # Onto SEVERAL columns: a filter on their combinations (the filter
            # Desktop writes for a visual's two-column Include filter). Only
            # the first column was kept, and a row constructor ("N", "A") was
            # BLANK, so the filter selected nothing (PR #99). Each row must
            # carry one value per column, or DAX raises an error.
            rows = []
            for item in table_ref:
                vals = self._row_values(item) if isinstance(item, dict) else [item]
                if len(vals) != len(target_cols):
                    from pbix_mcp.errors import DAXEvaluationError
                    raise DAXEvaluationError(
                        f"TREATAS: the table has {len(vals)} column(s) for "
                        f"{len(target_cols)} target column(s)")
                rows.append(tuple(vals))
            return ('__TREATAS_TUPLES__', target_cols, rows)
        if isinstance(table_ref, list) and target_cols:
            # Extract values and return as filter marker
            values = []
            for item in table_ref:
                if isinstance(item, dict) and '__value__' in item:
                    values.append(item['__value__'])
            if values and target_cols:
                return ('__TREATAS__', target_cols[0], values)
        return table_ref

    def _fn_generate(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATE(table1, table2_expr) — like CROSS APPLY (inner join behavior)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
            else:
                row_ctx = ctx
            inner = self._eval_expr(args[1].strip(), row_ctx)
            if isinstance(inner, list) and inner:
                for inner_item in inner:
                    merged = {}
                    if isinstance(row_item, dict):
                        merged.update(row_item)
                    if isinstance(inner_item, dict):
                        merged.update({f"_inner_{k}": v for k, v in inner_item.items()})
                    result.append(merged)
        return result

    def _fn_generateall(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATEALL(table1, table2_expr) — like CROSS APPLY (includes empty inner)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
            else:
                row_ctx = ctx
            inner = self._eval_expr(args[1].strip(), row_ctx)
            if isinstance(inner, list) and inner:
                for inner_item in inner:
                    merged = {}
                    if isinstance(row_item, dict):
                        merged.update(row_item)
                    if isinstance(inner_item, dict):
                        merged.update({f"_inner_{k}": v for k, v in inner_item.items()})
                    result.append(merged)
            else:
                # GENERATEALL keeps rows even when inner is empty
                result.append(row_item if isinstance(row_item, dict) else {'__value__': row_item})
        return result

    def _fn_generateseries(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATESERIES(start, end, step) — generate a single-column table [Value]."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        start_val = self._eval_expr(args[0].strip(), ctx)
        end_val = self._eval_expr(args[1].strip(), ctx)
        step_val = self._eval_expr(args[2].strip(), ctx) if len(args) >= 3 else 1
        try:
            start_val = float(start_val) if start_val is not None else 0
            end_val = float(end_val) if end_val is not None else 0
            step_val = float(step_val) if step_val is not None else 1
        except (TypeError, ValueError):
            return []
        if step_val == 0:
            return []
        # Use int if all values are whole numbers
        use_int = (start_val == int(start_val) and end_val == int(end_val)
                   and step_val == int(step_val))
        rows: list = []
        current = start_val
        max_rows = 1000000  # safety limit
        if step_val > 0:
            while current <= end_val + 1e-9 and len(rows) < max_rows:
                val = int(current) if use_int else current
                rows.append({'Value': val, '__table__': '__generateseries__',
                             '__column__': 'Value', '__value__': val})
                current += step_val
        elif step_val < 0:
            while current >= end_val - 1e-9 and len(rows) < max_rows:
                val = int(current) if use_int else current
                rows.append({'Value': val, '__table__': '__generateseries__',
                             '__column__': 'Value', '__value__': val})
                current += step_val
        return rows

    # =========================================================================
    # Filter functions
    # =========================================================================

    def _fn_allexcept(self, args_str: str, ctx: DAXContext) -> Any:
        """ALLEXCEPT(table, column1, column2, ...) — remove all filters except on specified columns."""
        args = self._split_args(args_str)
        if not args:
            return ('__ALLEXCEPT__', args_str.strip())
        table_name = args[0].strip().strip("'")
        # Columns to keep
        keep_cols = set()
        for i in range(1, len(args)):
            ref = self._eval_expr(args[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                keep_cols.add(f"{ref[0]}.{ref[1]}")

        # When used as table expression, return all rows of table ignoring non-keep filters
        tbl = ctx.tables.get(table_name)
        if tbl:
            # Build a context that only retains filters on the keep columns
            new_filters = {k: v for k, v in ctx.filter_context.items()
                          if k in keep_cols or not k.startswith(f"{table_name}.")}
            new_ctx = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                                ctx.date_column, new_filters, ctx.relationships)
            new_ctx._filter_idx_cache = ctx._filter_idx_cache
            rows = new_ctx.get_filtered_rows(table_name)
            cols = tbl['columns']
            if rows and len(cols) > 0:
                result = []
                seen = set()
                for row in rows:
                    key = tuple(str(row[i]) for i in range(len(cols)))
                    if key not in seen:
                        seen.add(key)
                        result.append({'__table__': table_name, '__column__': cols[0], '__value__': row[0]})
                return result
        return ('__ALLEXCEPT__', args_str.strip())

    def _fn_allselected(self, args_str: str, ctx: DAXContext) -> Any:
        """ALLSELECTED(column_or_table, ...) as a TABLE expression. (As a
        CALCULATE filter argument it is a modifier, handled there by its text.)

        Power BI Desktop 2.152, one slicer at a time (issue #120):
          * the column form keeps only the selection on its OWN columns
            (_selected_on_columns), BLANK included when that keeps the blank
            row (ALLSELECTED(BD[Name]) = 4, #82);
          * the table form is the table's rows under every selection that
            reaches it (_selected_ctx), as full-row dicts like ALL(T)'s, so an
            iterator transitions over each row. It answered no rows at all:
            MINX(ALLSELECTED('Date'), [Sales]) was BLANK where Desktop gives
            the selected days' minimum.
        A grouping is never part of the selection."""
        ref = args_str.strip()
        multi = self._multi_column_all(ref, ctx, selected=True)
        if multi is not None:
            return multi
        col_match = _WHOLE_TCOL_RE.match(ref)
        if col_match:
            # groups: 'quoted name' | bare name | column
            table_name = (col_match.group(1) or col_match.group(2) or '').strip()
            col_name = col_match.group(3).strip()
            values = self._values_with_blank(
                table_name, col_name,
                self._selected_on_columns(ctx, table_name, [col_name]))
            return [{'__table__': table_name, '__column__': col_name,
                     '__value__': v} for v in values]
        name = (ref[1:-1].replace("''", "'")
                if len(ref) > 1 and ref[0] == ref[-1] == "'" else ref).strip()
        table_name = ctx.model_table(name) if name else name
        tbl = ctx.tables.get(table_name) if name else None
        if tbl is not None:
            # The iterations' filters on the table's expanded table come back
            # as their rows or the selection; explicit filters stay (#118).
            exp = ctx.expanded_tables(table_name)
            sel = self._allselected_ctx(ctx, lambda k: k.partition('.')[0] in exp)
            cols = tbl['columns']
            out = []
            for row in sel.get_filtered_rows(table_name):
                rd = {'__table__': table_name, '__row__': True}
                for ci, col in enumerate(cols):
                    rd[col] = row[ci] if ci < len(row) else None
                out.append(rd)
            if sel.blank_row_visible(table_name):
                out.append(sel.blank_row_dict(table_name))
            return out
        return ('__ALLSELECTED__', ref)

    def _fn_keepfilters(self, args_str: str, ctx: DAXContext) -> Any:
        """KEEPFILTERS(expression) — intersect rather than replace filter context.
        Approximation: just evaluate the expression (KEEPFILTERS modifies CALCULATE behavior)."""
        # NOTE: True KEEPFILTERS changes how CALCULATE applies filters (intersection vs replacement).
        # In this simplified engine, we just evaluate the inner expression.
        return self._eval_expr(args_str.strip(), ctx)

    def _fn_hasonevalue(self, args_str: str, ctx: DAXContext) -> Any:
        """HASONEVALUE(column) — check if exactly one distinct value in filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            return len(self._values_with_blank(ref[0], ref[1], ctx)) == 1
        return False

    def _fn_hasonefilter(self, args_str: str, ctx: DAXContext) -> Any:
        """HASONEFILTER(column) — check if exactly one direct filter on column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            filter_key = f"{ref[0]}.{ref[1]}"
            if filter_key in ctx.filter_context:
                return len(ctx.filter_context[filter_key]) == 1
        return False

    def _filter_test_target(self, args_str: str, ctx: DAXContext):
        """ISFILTERED / ISCROSSFILTERED's argument -> (table, column or None),
        or None when it names neither.

        A TABLE is recognised by its NAME before anything is evaluated: a bare
        table name evaluates to its rows, so the table form never matched
        (issues #111, #113). A column is parsed, not evaluated, so a row
        context cannot collapse it to the current row's value."""
        arg = args_str.strip()
        name = (arg[1:-1].replace("''", "'")
                if len(arg) > 1 and arg[0] == arg[-1] == "'" else arg)
        table = ctx.model_table(name)
        if '[' not in arg and table in ctx.tables:
            return table, None
        ref = self._parse_column_ref(arg)
        if ref is None:
            ref = self._eval_expr(arg, ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            return ctx.model_table(str(ref[0])), str(ref[1])
        if isinstance(ref, str) and ref:
            return ctx.model_table(ref.strip("'\"")), None   # a table without loaded rows
        return None

    @staticmethod
    def _transitioned(ctx: DAXContext) -> DAXContext:
        """The filter context as the last CALCULATE / measure reference left
        it. An iterator's row is in filter_context before any transition
        (RowContextValues), but a row context alone filters nothing: Desktop
        2.152, SUMX(Orders, IF(ISFILTERED(Orders[Revenue]), 1, 0)) is 0 and
        SUMX(Orders, CALCULATE(IF(ISFILTERED(Orders[Revenue]), 1, 0))) is 3."""
        outer = getattr(ctx, '_outer_ctx', None)
        while outer is not None:
            ctx, outer = outer, getattr(outer, '_outer_ctx', None)
        return ctx

    def _fn_isfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISFILTERED(column_or_table) -- a DIRECT filter on the column, or on
        any column of the table (DAXContext.is_directly_filtered).

        CALCULATE(ISFILTERED(Orders), Orders[Revenue] > 100) was FALSE where
        DAX says TRUE (issue #111)."""
        target = self._filter_test_target(args_str, ctx)
        return self._transitioned(ctx).is_directly_filtered(*target) if target else False

    def _fn_iscrossfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISCROSSFILTERED(column_or_table) -- anything filters the table:
        directly, or through a relationship (DAXContext.is_cross_filtered).
        The column form is the table form, as in Desktop. The table form was
        never TRUE, and the column form missed a filter on another column of
        its own table and a filter on column combinations (issue #113)."""
        target = self._filter_test_target(args_str, ctx)
        return self._transitioned(ctx).is_cross_filtered(target[0]) if target else False

    def _fn_userelationship(self, args_str: str, ctx: DAXContext) -> Any:
        """USERELATIONSHIP(column1, column2) — activate an inactive relationship.
        Returns a marker for CALCULATE to process."""
        # NOTE: Full implementation would require modifying the relationship index.
        # This returns a marker that CALCULATE can interpret.
        return ('__USERELATIONSHIP__', args_str.strip())

    def _fn_earlier(self, args_str: str, ctx: DAXContext) -> Any:
        """EARLIER(column, n) — row context from n levels up.
        Limitation: This engine does not maintain a row context stack.
        Returns the current column value as an approximation."""
        args = self._split_args(args_str)
        ref = self._eval_expr(args[0].strip(), ctx)
        # NOTE: EARLIER requires a row context stack which this engine doesn't maintain.
        # We return the column reference so it can be used in comparisons.
        return ref

    def _fn_earliest(self, args_str: str, ctx: DAXContext) -> Any:
        """EARLIEST(column) — outermost row context.
        Limitation: Same as EARLIER — no row context stack."""
        return self._fn_earlier(args_str, ctx)

    # =========================================================================
    # Math functions
    # =========================================================================

    def _fn_ceiling(self, args_str: str, ctx: DAXContext) -> Any:
        """CEILING(number, significance) — round up to nearest multiple of significance."""
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        sig = self._num_arg(args[1], ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.ceil(val / sig) * sig
        return val

    def _fn_floor(self, args_str: str, ctx: DAXContext) -> Any:
        """FLOOR(number, significance) — round down to nearest multiple of significance."""
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        sig = self._num_arg(args[1], ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.floor(val / sig) * sig
        return val

    def _fn_mod(self, args_str: str, ctx: DAXContext) -> Any:
        """MOD(number, divisor) — modulo."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        val = self._num_arg(args[0], ctx)
        divisor = self._num_arg(args[1], ctx)
        if isinstance(val, (int, float)) and isinstance(divisor, (int, float)) and divisor != 0:
            return val % divisor
        return None

    def _fn_power(self, args_str: str, ctx: DAXContext) -> Any:
        """POWER(base, exponent) — exponentiation."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        base = self._num_arg(args[0], ctx)
        exp = self._num_arg(args[1], ctx)
        if isinstance(base, (int, float)) and isinstance(exp, (int, float)):
            return math.pow(base, exp)
        return None


    # ------------------------------------------------------------------
    # Conformance batch 1: trig/math scalars, bit ops, statistical
    # distributions, and their iterator variants. Every function here has
    # Desktop-captured golden values in tests/conformance/golden.json; the
    # numerics (inverse normal, incomplete beta/gamma) are implemented to
    # double precision so the goldens match at 1e-9 relative, not merely
    # "close". CEILING.MATH / FLOOR.MATH are deliberately absent: Desktop
    # itself cannot resolve those names in a query, DMV listing or not.
    # ------------------------------------------------------------------

    def _num1(self, args_str: str, ctx: DAXContext):
        # Text, TRUE / FALSE and dates convert (_num_operand, issue #139).
        v = self._num_arg(args_str, ctx)
        if not isinstance(v, (int, float)):
            return None
        return float(v)

    def _num_args(self, args_str: str, ctx: DAXContext, n: int):
        parts = self._split_args(args_str)
        if len(parts) < n:
            return None
        out = []
        for prt in parts[:n]:
            v = self._num_arg(prt, ctx)
            if not isinstance(v, (int, float)):
                return None
            out.append(float(v))
        return out

    def _fn_math1(self, name: str, args_str: str, ctx: DAXContext):
        x = self._num1(args_str, ctx)
        if x is None:
            return None
        try:
            return _MATH1[name](x)
        except (ValueError, ZeroDivisionError, OverflowError):
            return None

    def _fn_combin(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None:
            return None
        n, k = int(a[0]), int(a[1])
        if k < 0 or n < 0 or k > n:
            return None
        return float(math.comb(n, k))

    def _fn_combina(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None:
            return None
        n, k = int(a[0]), int(a[1])
        if n < 0 or k < 0 or (n == 0 and k > 0):
            return None
        return float(math.comb(n + k - 1, k))

    def _fn_permut(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None:
            return None
        n, k = int(a[0]), int(a[1])
        if k < 0 or n < 0 or k > n:
            return None
        return float(math.perm(n, k))

    def _fn_quotient(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or a[1] == 0:
            return None
        return float(math.trunc(a[0] / a[1]))

    def _fn_bitop(self, name: str, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None:
            return None
        x, y = int(a[0]), int(a[1])
        if name in ('BITAND', 'BITOR', 'BITXOR'):
            if x < 0 or y < 0:
                return None
            return float({'BITAND': x & y, 'BITOR': x | y,
                          'BITXOR': x ^ y}[name])
        if name == 'BITRSHIFT':
            y = -y
        if x < 0 or abs(y) > 53:
            return None
        return float(x << y if y >= 0 else x >> -y)

    # ----------------------------------------------------- special functions

    @staticmethod
    def _norm_cdf(z: float) -> float:
        return 0.5 * math.erfc(-z / math.sqrt(2.0))

    @staticmethod
    def _norm_pdf(z: float) -> float:
        return math.exp(-0.5 * z * z) / math.sqrt(2.0 * math.pi)

    @classmethod
    def _norm_inv(cls, p: float) -> float:
        """Acklam's rational approximation polished with two Halley steps."""
        if not (0.0 < p < 1.0):
            raise ValueError("p out of range")
        a = (-3.969683028665376e+01, 2.209460984245205e+02,
             -2.759285104469687e+02, 1.383577518672690e+02,
             -3.066479806614716e+01, 2.506628277459239e+00)
        b = (-5.447609879822406e+01, 1.615858368580409e+02,
             -1.556989798598866e+02, 6.680131188771972e+01,
             -1.328068155288572e+01)
        c = (-7.784894002430293e-03, -3.223964580411365e-01,
             -2.400758277161838e+00, -2.549732539343734e+00,
             4.374664141464968e+00, 2.938163982698783e+00)
        d = (7.784695709041462e-03, 3.224671290700398e-01,
             2.445134137142996e+00, 3.754408661907416e+00)
        plow, phigh = 0.02425, 1 - 0.02425
        if p < plow:
            q = math.sqrt(-2 * math.log(p))
            x = ((((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
                 / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1))
        elif p <= phigh:
            q = p - 0.5
            r = q * q
            x = ((((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
                 / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1))
        else:
            q = math.sqrt(-2 * math.log(1 - p))
            x = -((((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
                  / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1))
        for _ in range(2):
            e = cls._norm_cdf(x) - p
            u = e / cls._norm_pdf(x)
            x = x - u / (1 + x * u / 2)
        return x

    @staticmethod
    def _betacf(a: float, b: float, x: float) -> float:
        """Continued fraction for the incomplete beta (Lentz)."""
        MAXIT, EPS, FPMIN = 300, 3e-16, 1e-300
        qab, qap, qam = a + b, a + 1.0, a - 1.0
        c = 1.0
        d = 1.0 - qab * x / qap
        if abs(d) < FPMIN:
            d = FPMIN
        d = 1.0 / d
        h = d
        for m in range(1, MAXIT + 1):
            m2 = 2 * m
            aa = m * (b - m) * x / ((qam + m2) * (a + m2))
            d = 1.0 + aa * d
            if abs(d) < FPMIN:
                d = FPMIN
            c = 1.0 + aa / c
            if abs(c) < FPMIN:
                c = FPMIN
            d = 1.0 / d
            h *= d * c
            aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
            d = 1.0 + aa * d
            if abs(d) < FPMIN:
                d = FPMIN
            c = 1.0 + aa / c
            if abs(c) < FPMIN:
                c = FPMIN
            d = 1.0 / d
            de = d * c
            h *= de
            if abs(de - 1.0) < EPS:
                break
        return h

    @classmethod
    def _betainc(cls, a: float, b: float, x: float) -> float:
        if x <= 0.0:
            return 0.0
        if x >= 1.0:
            return 1.0
        ln_bt = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                 + a * math.log(x) + b * math.log1p(-x))
        bt = math.exp(ln_bt)
        if x < (a + 1.0) / (a + b + 2.0):
            return bt * cls._betacf(a, b, x) / a
        return 1.0 - bt * cls._betacf(b, a, 1.0 - x) / b

    @classmethod
    def _betainc_inv(cls, a: float, b: float, p: float) -> float:
        if p <= 0.0:
            return 0.0
        if p >= 1.0:
            return 1.0
        x = a / (a + b)
        for _ in range(100):
            f = cls._betainc(a, b, x) - p
            ln_pdf = (math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
                      + (a - 1) * math.log(x) + (b - 1) * math.log1p(-x))
            df = math.exp(ln_pdf)
            if df == 0:
                break
            nx = x - f / df
            if nx <= 0:
                nx = x / 2
            elif nx >= 1:
                nx = (x + 1) / 2
            if abs(nx - x) < 1e-15:
                x = nx
                break
            x = nx
        return x

    @staticmethod
    def _gammainc_lower(s: float, x: float) -> float:
        if x < 0 or s <= 0:
            raise ValueError
        if x == 0:
            return 0.0
        if x < s + 1.0:
            term = 1.0 / s
            total = term
            n = s
            for _ in range(500):
                n += 1.0
                term *= x / n
                total += term
                if abs(term) < abs(total) * 3e-16:
                    break
            return total * math.exp(-x + s * math.log(x) - math.lgamma(s))
        FPMIN = 1e-300
        b0 = x + 1.0 - s
        c = 1.0 / FPMIN
        d = 1.0 / b0
        h = d
        for i in range(1, 500):
            an = -i * (i - s)
            b0 += 2.0
            d = an * d + b0
            if abs(d) < FPMIN:
                d = FPMIN
            c = b0 + an / c
            if abs(c) < FPMIN:
                c = FPMIN
            d = 1.0 / d
            de = d * c
            h *= de
            if abs(de - 1.0) < 3e-16:
                break
        q = math.exp(-x + s * math.log(x) - math.lgamma(s)) * h
        return 1.0 - q

    @classmethod
    def _gammainc_inv(cls, s: float, p: float) -> float:
        if p <= 0.0:
            return 0.0
        lo, hi = 0.0, max(s, 1.0)
        while cls._gammainc_lower(s, hi) < p:
            hi *= 2
            if hi > 1e10:
                break
        for _ in range(200):
            mid = (lo + hi) / 2
            if cls._gammainc_lower(s, mid) < p:
                lo = mid
            else:
                hi = mid
        return (lo + hi) / 2

    @classmethod
    def _t_cdf(cls, t: float, df: float) -> float:
        x = df / (df + t * t)
        p = 0.5 * cls._betainc(df / 2.0, 0.5, x)
        return 1.0 - p if t > 0 else p

    # ------------------------------------------------------ dist functions

    def _fn_norm_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 4)
        if a is None or a[2] <= 0:
            return None
        z = (a[0] - a[1]) / a[2]
        if a[3]:
            return self._norm_cdf(z)
        return self._norm_pdf(z) / a[2]

    def _fn_norm_inv(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or not (0 < a[0] < 1) or a[2] <= 0:
            return None
        return a[1] + a[2] * self._norm_inv(a[0])

    def _fn_norm_s_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None:
            return None
        return self._norm_cdf(a[0]) if a[1] else self._norm_pdf(a[0])

    def _fn_norm_s_inv(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 1)
        if a is None or not (0 < a[0] < 1):
            return None
        return self._norm_inv(a[0])

    def _fn_expon_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or a[0] < 0 or a[1] <= 0:
            return None
        if a[2]:
            return 1.0 - math.exp(-a[1] * a[0])
        return a[1] * math.exp(-a[1] * a[0])

    def _fn_poisson_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or a[0] < 0 or a[1] < 0:
            return None
        k, lam = int(a[0]), a[1]

        def pmf(i):
            return math.exp(-lam + i * math.log(lam) - math.lgamma(i + 1))

        if a[2]:
            return sum(pmf(i) for i in range(k + 1))
        return pmf(k)

    def _fn_beta_dist(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        a = self._num_args(args_str, ctx, 4)
        if a is None or a[1] <= 0 or a[2] <= 0:
            return None
        x, al, be, cum = a[0], a[1], a[2], a[3]
        lo, hi = 0.0, 1.0
        if len(parts) >= 6:
            more = self._num_args(",".join(parts[4:6]), ctx, 2)
            if more:
                lo, hi = more
        if hi <= lo:
            return None
        xx = (x - lo) / (hi - lo)
        if cum:
            return self._betainc(al, be, xx)
        if not (0 <= xx <= 1):
            return 0.0
        ln = (math.lgamma(al + be) - math.lgamma(al) - math.lgamma(be)
              + (al - 1) * math.log(xx) + (be - 1) * math.log1p(-xx))
        return math.exp(ln) / (hi - lo)

    def _fn_beta_inv(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        a = self._num_args(args_str, ctx, 3)
        if a is None or not (0 < a[0] < 1) or a[1] <= 0 or a[2] <= 0:
            return None
        lo, hi = 0.0, 1.0
        if len(parts) >= 5:
            more = self._num_args(",".join(parts[3:5]), ctx, 2)
            if more:
                lo, hi = more
        return lo + (hi - lo) * self._betainc_inv(a[1], a[2], a[0])

    def _fn_chisq_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or a[0] < 0 or a[1] < 1:
            return None
        x, df = a[0], a[1]
        if a[2]:
            return self._gammainc_lower(df / 2.0, x / 2.0)
        return (x ** (df / 2.0 - 1) * math.exp(-x / 2.0)
                / (2 ** (df / 2.0) * math.exp(math.lgamma(df / 2.0))))

    def _fn_chisq_dist_rt(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or a[0] < 0 or a[1] < 1:
            return None
        return 1.0 - self._gammainc_lower(a[1] / 2.0, a[0] / 2.0)

    def _fn_chisq_inv(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or not (0 <= a[0] < 1) or a[1] < 1:
            return None
        return 2.0 * self._gammainc_inv(a[1] / 2.0, a[0])

    def _fn_chisq_inv_rt(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or not (0 < a[0] <= 1) or a[1] < 1:
            return None
        return 2.0 * self._gammainc_inv(a[1] / 2.0, 1.0 - a[0])

    def _fn_t_dist(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or a[1] < 1:
            return None
        t, df = a[0], a[1]
        if a[2]:
            return self._t_cdf(t, df)
        ln = (math.lgamma((df + 1) / 2.0) - math.lgamma(df / 2.0)
              - 0.5 * math.log(df * math.pi)
              - (df + 1) / 2.0 * math.log1p(t * t / df))
        return math.exp(ln)

    def _fn_t_dist_rt(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or a[1] < 1:
            return None
        return 1.0 - self._t_cdf(a[0], a[1])

    def _fn_t_dist_2t(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or a[0] < 0 or a[1] < 1:
            return None
        return 2.0 * (1.0 - self._t_cdf(a[0], a[1]))

    def _t_inv_left(self, p: float, df: float) -> float:
        x = self._betainc_inv(df / 2.0, 0.5, 2.0 * min(p, 1.0 - p))
        t = math.sqrt(df * (1.0 - x) / x) if x > 0 else float("inf")
        return -t if p < 0.5 else t

    def _fn_t_inv(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or not (0 < a[0] < 1) or a[1] < 1:
            return None
        return self._t_inv_left(a[0], a[1])

    def _fn_t_inv_2t(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 2)
        if a is None or not (0 < a[0] <= 1) or a[1] < 1:
            return None
        return self._t_inv_left(1.0 - a[0] / 2.0, a[1])

    def _fn_confidence_norm(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or not (0 < a[0] < 1) or a[1] <= 0 or a[2] < 1:
            return None
        return self._norm_inv(1.0 - a[0] / 2.0) * a[1] / math.sqrt(int(a[2]))

    def _fn_confidence_t(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None or not (0 < a[0] < 1) or a[1] <= 0 or a[2] < 2:
            return None
        return (self._t_inv_left(1.0 - a[0] / 2.0, int(a[2]) - 1)
                * a[1] / math.sqrt(int(a[2])))

    # ------------------------------------------ column / iterator statistics

    def _column_numbers(self, args_str: str, ctx: DAXContext):
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            return [float(v) for v in ctx.get_column_data(ref[0], ref[1])
                    if isinstance(v, (int, float)) and not isinstance(v, bool)]
        return None

    def _iter_numbers(self, args_str: str, ctx: DAXContext):
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if not isinstance(table_ref, list):
            return None
        values = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                result = self._eval_expr(row_expr, row_ctx)
                result = self._resolve_row_result(result, row_item, row_ctx)
            else:
                result = self._eval_expr(row_expr, ctx)
            if isinstance(result, (int, float)) and not isinstance(result, bool):
                values.append(float(result))
        return values

    @staticmethod
    def _percentile_inc(values, k):
        if not values or not (0.0 <= k <= 1.0):
            return None
        v = sorted(values)
        pos = k * (len(v) - 1)
        lo = int(math.floor(pos))
        hi = min(lo + 1, len(v) - 1)
        return v[lo] + (v[hi] - v[lo]) * (pos - lo)

    @staticmethod
    def _percentile_exc(values, k):
        n = len(values)
        if not values or not (1.0 / (n + 1) <= k <= n / (n + 1.0)):
            return None
        v = sorted(values)
        pos = k * (n + 1) - 1
        lo = int(math.floor(pos))
        hi = min(lo + 1, n - 1)
        return v[lo] + (v[hi] - v[lo]) * (pos - lo)

    def _fn_percentile(self, name: str, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) != 2:
            return None
        vals = self._column_numbers(parts[0], ctx)
        k = self._num1(parts[1], ctx)
        if vals is None or k is None:
            return None
        f = self._percentile_inc if name.endswith("INC") else self._percentile_exc
        return f(vals, k)

    def _fn_percentilex(self, name: str, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) != 3:
            return None
        vals = self._iter_numbers(",".join(parts[:2]), ctx)
        k = self._num1(parts[2], ctx)
        if vals is None or k is None:
            return None
        f = self._percentile_inc if name.endswith("INC") else self._percentile_exc
        return f(vals, k)

    def _fn_geomean(self, args_str: str, ctx: DAXContext):
        vals = self._column_numbers(args_str, ctx)
        if not vals or any(v <= 0 for v in vals):
            return None
        return math.exp(sum(math.log(v) for v in vals) / len(vals))

    def _fn_geomeanx(self, args_str: str, ctx: DAXContext):
        vals = self._iter_numbers(args_str, ctx)
        if not vals or any(v <= 0 for v in vals):
            return None
        return math.exp(sum(math.log(v) for v in vals) / len(vals))

    def _fn_stdevx(self, name: str, args_str: str, ctx: DAXContext):
        vals = self._iter_numbers(args_str, ctx)
        if vals is None:
            return None
        sample = name.endswith(".S")
        if len(vals) < (2 if sample else 1):
            return None
        var = (statistics.variance(vals) if sample
               else statistics.pvariance(vals))
        return math.sqrt(var) if "STDEV" in name else var

    def _fn_rank_eq(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        x = self._num1(parts[0], ctx)
        vals = self._column_numbers(parts[1], ctx)
        if x is None or not vals:
            return None
        order = None
        if len(parts) >= 3:
            order = self._num1(parts[2], ctx)
        asc = bool(order)
        v = sorted(vals, reverse=not asc)
        for i, val in enumerate(v):
            if val == x:
                return float(i + 1)
        return None

    def _fn_averagea(self, args_str: str, ctx: DAXContext):
        ref = self._eval_expr(args_str.strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        vals = []
        for v in ctx.get_column_data(ref[0], ref[1]):
            if v is None:
                continue
            if isinstance(v, bool):
                vals.append(1.0 if v else 0.0)
            elif isinstance(v, (int, float)):
                vals.append(float(v))
            else:
                vals.append(0.0)
        return sum(vals) / len(vals) if vals else None

    def _fn_iso_ceiling(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        x = self._num1(parts[0], ctx)
        if x is None:
            return None
        sig = 1.0
        if len(parts) > 1:
            s2 = self._num1(parts[1], ctx)
            if s2 is None or s2 == 0:
                return None
            sig = abs(s2)
        return math.ceil(x / sig) * sig

    def _fn_datevalue(self, args_str: str, ctx: DAXContext):
        v = self._eval_expr(args_str.strip(), ctx)
        if isinstance(v, datetime):
            return datetime(v.year, v.month, v.day)
        if not isinstance(v, str):
            return None
        d = None
        # Model culture is en-US regardless of the machine locale: Desktop's
        # golden reads "1/8/2009" as January 8. Month-first BEFORE the generic
        # parser, which is day-first for ambiguous slash dates.
        for fmt in ("%m/%d/%Y", "%m/%d/%y", "%d/%m/%Y"):
            try:
                d = datetime.strptime(v.strip(), fmt)
                break
            except ValueError:
                continue
        if d is None:
            d = _as_datetime(v)
        if d is None:
            return None
        return datetime(d.year, d.month, d.day)

    def _fn_timevalue(self, args_str: str, ctx: DAXContext):
        v = self._eval_expr(args_str.strip(), ctx)
        if not isinstance(v, str):
            return None
        for fmt in ("%H:%M:%S", "%H:%M", "%I:%M:%S %p", "%I:%M %p"):
            try:
                t = datetime.strptime(v.strip(), fmt)
                return datetime(1899, 12, 30, t.hour, t.minute, t.second)
            except ValueError:
                continue
        return None


    # ------------------------------------------------------------------
    # Conformance batch 2: table machinery, date navigation, NETWORKDAYS.
    # Golden-pinned in tests/conformance/golden.json. The week-grain
    # time-intel family (STARTOFWEEK, DATESWTD, ...) is NOT here: Desktop
    # requires a model CALENDAR reference for those ("parameter 1 must be a
    # calendar reference"), a model feature the fixture does not carry --
    # classified needs-model-feature, not silently faked.
    # ------------------------------------------------------------------

    def _table_rows(self, expr: str, ctx: DAXContext):
        """Evaluate an expression expected to yield a table: a list of row
        dicts carrying __table__. Returns None when it does not."""
        res = self._eval_expr(expr.strip(), ctx)
        if isinstance(res, list):
            return [r for r in res if isinstance(r, dict) and '__table__' in r]
        return None

    @staticmethod
    def _row_cols(row):
        return {k: v for k, v in row.items() if not k.startswith('__')}

    @classmethod
    def _row_values(cls, row: dict) -> list:
        """One row of a table expression as its column values, in order: a
        row constructor's values, a single-column row's value, a multi-column
        row's columns, a CROSSJOIN row's left part then its right ('_2_')."""
        if '__tuple__' in row:
            return list(row['__tuple__'])
        if row.get('__parts__'):
            out: list = []
            for part in row['__parts__']:
                out += cls._row_values(part)
            return out
        left = {k: v for k, v in row.items() if not k.startswith('_2_')}
        right = {k[3:]: v for k, v in row.items() if k.startswith('_2_')}
        plain = cls._row_cols(left)
        out = list(plain.values()) if plain else [left.get('__value__')]
        if right:
            out += cls._row_values(right)
        return out

    def _fn_networkdays(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        d1 = _as_datetime(self._eval_expr(parts[0].strip(), ctx))
        d2 = _as_datetime(self._eval_expr(parts[1].strip(), ctx))
        if d1 is None or d2 is None:
            return None
        # weekend parameter: 1 = Sat/Sun (default). Other codes exist; only
        # implement what the golden pins and refuse the rest.
        weekend = {5, 6}                       # Python weekday(): Sat=5, Sun=6
        if len(parts) >= 3:
            w = self._eval_expr(parts[2].strip(), ctx)
            if isinstance(w, (int, float)) and int(w) != 1:
                return None
        sign = 1
        if d2 < d1:
            d1, d2 = d2, d1
            sign = -1
        days = 0
        cur = datetime(d1.year, d1.month, d1.day)
        end = datetime(d2.year, d2.month, d2.day)
        one = timedelta(days=1)
        while cur <= end:
            if cur.weekday() not in weekend:
                days += 1
            cur += one
        return float(sign * days)

    def _fn_isdatetime(self, args_str: str, ctx: DAXContext):
        v = self._eval_expr(args_str.strip(), ctx)
        return isinstance(v, datetime)

    def _fn_containsrow(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        rows = self._eval_expr(parts[0].strip(), ctx)
        if not isinstance(rows, list):
            return None
        needle = tuple(self._eval_expr(x.strip(), ctx) for x in parts[1:])
        for r in rows:
            if isinstance(r, dict) and '__value__' in r:
                hay = (r['__value__'],)
            elif isinstance(r, dict):
                hay = tuple(self._row_cols(r).values())
            else:
                hay = (r,)
            if len(hay) == len(needle) and all(
                    a == b if a is None or b is None else _dax_equal(a, b)
                    for a, b in zip(hay, needle)):
                return True
        return False

    def _fn_allnoblankrow(self, args_str: str, ctx: DAXContext):
        # ALL over the stored rows: the blank row is left out, a stored BLANK
        # value is not (Desktop: ALLNOBLANKROW(RD[Name]) = 3 with one real
        # blank; ALLNOBLANKROW(BD) = 3 next to ALL(BD) = 4).
        return self._fn_all(args_str, ctx, no_blank_row=True)

    def _fn_filters(self, args_str: str, ctx: DAXContext):
        """FILTERS(column) -- the directly-filtered values of the column, or
        every value when the column carries no direct filter."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        t, c = ref
        key = f"{t}.{c}"
        direct = ctx.filter_context.get(key) if ctx.filter_context else None
        if direct is not None and isinstance(direct, list):
            vals = list(dict.fromkeys(direct))
        else:
            tbl = ctx.tables.get(t)
            if not tbl:
                return None
            idx = ctx._find_col_idx(tbl['columns'], c)
            if idx < 0:
                return None
            vals = list(dict.fromkeys(row[idx] for row in tbl['rows']))
            # every value of the column, the blank row's included (Desktop:
            # COUNTROWS(FILTERS(BD[Name])) = 4 with no filter, issue #82)
            if None not in vals and t in ctx.blank_row_tables():
                vals.append(None)
        return [{'__table__': t, '__column__': c, '__value__': v} for v in vals]

    def _fn_topnskip(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 4:
            return None
        n = self._num1(parts[0], ctx)
        skip = self._num1(parts[1], ctx)
        rows = self._table_rows(parts[2], ctx)
        if n is None or skip is None or rows is None:
            return None
        order_expr = parts[3].strip()
        keyed = []
        for r in rows:
            row_ctx = self._make_row_context(r, ctx, shadow=rows)
            k = self._eval_expr(order_expr, row_ctx)
            k = self._resolve_row_result(k, r, row_ctx)
            keyed.append((k if isinstance(k, (int, float)) else float('-inf'), r))
        keyed.sort(key=lambda t2: t2[0], reverse=True)
        lo, hi = int(skip), int(skip) + int(n)
        return [r for _, r in keyed[lo:hi]]

    def _fn_naturaljoin(self, name: str, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) != 2:
            return None
        left = self._table_rows(parts[0], ctx)
        right = self._table_rows(parts[1], ctx)
        if left is None or right is None:
            return None
        if not left:
            return []
        lcols = set(self._row_cols(left[0]).keys()) if left else set()
        rcols = set(self._row_cols(right[0]).keys()) if right else set()
        common = lcols & rcols
        out = []
        for lr in left:
            lvals = self._row_cols(lr)
            matches = [rr for rr in right
                       if all(self._row_cols(rr).get(c) == lvals.get(c)
                              for c in common)]
            if matches:
                for rr in matches:
                    merged = dict(lr)
                    for k, v in self._row_cols(rr).items():
                        merged.setdefault(k, v)
                    out.append(merged)
            elif name == 'NATURALLEFTOUTERJOIN':
                out.append(dict(lr))
        return out

    def _fn_groupby(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        rows = self._table_rows(parts[0], ctx)
        if rows is None:
            return None
        group_cols = []
        i = 1
        while i < len(parts):
            ref = self._eval_expr(parts[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                group_cols.append(ref[1])
                i += 1
            else:
                break
        ext = parts[i:]
        groups: dict = {}
        for r in rows:
            key = tuple(r.get(c) for c in group_cols)
            groups.setdefault(key, []).append(r)
        out = []
        for key, grp_rows in groups.items():
            new_row = {'__table__': rows[0]['__table__'], '__row__': True}
            for c, v in zip(group_cols, key):
                new_row[c] = v
            j = 0
            while j + 1 < len(ext):
                cname = self._eval_expr(ext[j].strip(), ctx)
                prev_grp = getattr(ctx, '_current_group', None)
                ctx._current_group = grp_rows
                try:
                    val = self._eval_expr(ext[j + 1].strip(), ctx)
                finally:
                    ctx._current_group = prev_grp
                new_row[str(cname)] = val
                j += 2
            out.append(new_row)
        return out

    def _fn_currentgroup(self, args_str: str, ctx: DAXContext):
        return getattr(ctx, '_current_group', None) or []

    def _fn_isonorafter(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        i = 0
        while i < len(parts):
            v1 = self._eval_expr(parts[i].strip(), ctx)
            if i + 1 >= len(parts):
                return None
            v2 = self._eval_expr(parts[i + 1].strip(), ctx)
            order = 'ASC'
            if i + 2 < len(parts) and parts[i + 2].strip().upper() in ('ASC', 'DESC'):
                order = parts[i + 2].strip().upper()
                i += 3
            else:
                i += 2
            try:
                if v1 == v2:
                    continue
                ok = (v1 >= v2) if order == 'ASC' else (v1 <= v2)
                return bool(ok)
            except TypeError:
                return None
        return True

    def _fn_allcrossfiltered(self, args_str: str, ctx: DAXContext):
        # Clears every filter that reaches the table, directly or through
        # relationships -- the CALCULATE branch treats it like ALL(Table).
        return self._fn_all(args_str, ctx)

    def _fn_substitutewithindex(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 5:
            return None
        left = self._table_rows(parts[0], ctx)
        name = self._eval_expr(parts[1].strip(), ctx)
        right = self._table_rows(parts[2], ctx)
        if left is None or right is None:
            return None
        order_ref = self._eval_expr(parts[3].strip(), ctx)
        if not (isinstance(order_ref, tuple) and len(order_ref) == 2):
            return None
        ocol = order_ref[1]
        rsorted = sorted(right, key=lambda r: (r.get(ocol) is None, r.get(ocol)))
        common = None
        out = []
        for lr in left:
            lvals = self._row_cols(lr)
            if common is None and rsorted:
                common = set(lvals) & set(self._row_cols(rsorted[0]))
            idx = None
            for j, rr in enumerate(rsorted):
                rv = self._row_cols(rr)
                if all(rv.get(c) == lvals.get(c) for c in (common or set())):
                    idx = j
                    break
            new_row = {k: v for k, v in lr.items()
                       if k.startswith('__') or k not in (common or set())}
            new_row[str(name)] = idx
            out.append(new_row)
        return out

    def _fn_detailrows(self, args_str: str, ctx: DAXContext):
        """DETAILROWS(measure) -- no detail-rows expression support in the
        model layer yet, so this returns the measure's home-table rows under
        the current context, which is Desktop's default behaviour."""
        m = args_str.strip()
        if m.startswith('[') and m.endswith(']'):
            m = m[1:-1]
        home = (ctx.measure_tables or {}).get(m)
        if not home:
            for cand, tbl in (ctx.measure_tables or {}).items():
                if cand.lower() == m.lower():
                    home = tbl
                    break
        if not home:
            return None
        tbl = ctx.tables.get(home)
        if not tbl:
            return None
        rows = ctx.get_filtered_rows(home)
        cols = tbl['columns']
        return [dict({'__table__': home, '__row__': True},
                     **dict(zip(cols, r))) for r in rows]

    def _fn_nextday(self, args_str: str, ctx: DAXContext):
        return self._day_shift(args_str, ctx, +1)

    def _fn_previousday(self, args_str: str, ctx: DAXContext):
        return self._day_shift(args_str, ctx, -1)

    def _day_shift(self, args_str: str, ctx: DAXContext, direction: int):
        """NEXTDAY / PREVIOUSDAY: the single day after the last (before the
        first) date in the current selection -- empty when the calendar does
        not contain it, and an empty set means BLANK downstream."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        t, c = ref
        visible = [d for d in (_as_datetime(v)
                               for v in ctx.get_column_data(t, c))
                   if d is not None]
        if not visible:
            return []
        anchor = max(visible) if direction > 0 else min(visible)
        target = anchor + timedelta(days=direction)
        tbl = ctx.tables.get(t)
        if not tbl:
            return []
        idx = ctx._find_col_idx(tbl['columns'], c)
        if idx < 0:
            return []
        for row in tbl['rows']:
            d = _as_datetime(row[idx])
            if d is not None and d.date() == target.date():
                return [{'__table__': t, '__column__': c,
                         '__value__': row[idx]}]
        return []


    # ------------------------------------------------------------------
    # Conformance batch 3: the financial family. Excel-compatible formulas,
    # golden-pinned against Desktop at 1e-9. Day-count bases: 0=30/360 US,
    # 1=actual/actual, 2=actual/360, 3=actual/365, 4=30E/360.
    # ------------------------------------------------------------------

    def _fin_args(self, args_str: str, ctx: DAXContext):
        return [self._eval_expr(a.strip(), ctx)
                for a in self._split_args(args_str)]

    @staticmethod
    def _fin_num(v, default=None):
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return float(v)
        return default

    # ---- annuity family --------------------------------------------------

    @staticmethod
    def _annuity(rate, nper, pmt, pv, fv, typ):
        """0 = pv*(1+r)^n + pmt*(1+r*typ)*((1+r)^n - 1)/r + fv"""
        if rate == 0:
            return pv + pmt * nper + fv
        f = (1 + rate) ** nper
        return pv * f + pmt * (1 + rate * typ) * (f - 1) / rate + fv

    def _fn_pmt(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]):
            return None
        rate, nper, pv = a[0], a[1], a[2]
        fv = a[3] if len(a) > 3 and a[3] is not None else 0.0
        typ = a[4] if len(a) > 4 and a[4] is not None else 0.0
        if rate == 0:
            return -(pv + fv) / nper
        f = (1 + rate) ** nper
        return -(pv * f + fv) * rate / ((1 + rate * typ) * (f - 1))

    def _fn_fv(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]):
            return None
        rate, nper, pmt = a[0], a[1], a[2]
        pv = a[3] if len(a) > 3 and a[3] is not None else 0.0
        typ = a[4] if len(a) > 4 and a[4] is not None else 0.0
        return -self._annuity(rate, nper, pmt, pv, 0.0, typ)

    def _fn_pv(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]):
            return None
        rate, nper, pmt = a[0], a[1], a[2]
        fv = a[3] if len(a) > 3 and a[3] is not None else 0.0
        typ = a[4] if len(a) > 4 and a[4] is not None else 0.0
        if rate == 0:
            return -(fv + pmt * nper)
        f = (1 + rate) ** nper
        return -(fv + pmt * (1 + rate * typ) * (f - 1) / rate) / f

    def _fn_nper(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]):
            return None
        rate, pmt, pv = a[0], a[1], a[2]
        fv = a[3] if len(a) > 3 and a[3] is not None else 0.0
        typ = a[4] if len(a) > 4 and a[4] is not None else 0.0
        if rate == 0:
            if pmt == 0:
                return None
            return -(pv + fv) / pmt
        adj = pmt * (1 + rate * typ) / rate
        num = adj - fv
        den = pv + adj
        if num <= 0 or den == 0 or num / den <= 0:
            # log of a non-positive: fall through to the general identity
            try:
                return math.log(num / den) / math.log(1 + rate)
            except ValueError:
                return None
        return math.log(num / den) / math.log(1 + rate)

    def _fn_rate(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]):
            return None
        nper, pmt, pv = a[0], a[1], a[2]
        fv = a[3] if len(a) > 3 and a[3] is not None else 0.0
        typ = a[4] if len(a) > 4 and a[4] is not None else 0.0
        guess = a[5] if len(a) > 5 and a[5] is not None else 0.1
        r = guess
        for _ in range(200):
            f0 = self._annuity(r, nper, pmt, pv, fv, typ)
            h = max(abs(r), 1e-5) * 1e-6
            f1 = self._annuity(r + h, nper, pmt, pv, fv, typ)
            d = (f1 - f0) / h
            if d == 0:
                break
            rn = r - f0 / d
            if rn <= -1:
                rn = (r - 1) / 2
            if abs(rn - r) < 1e-14:
                return rn
            r = rn
        return r

    def _fn_ipmt(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        rate, per, nper, pv = a[0], a[1], a[2], a[3]
        fv = a[4] if len(a) > 4 and a[4] is not None else 0.0
        typ = a[5] if len(a) > 5 and a[5] is not None else 0.0
        pmt = self._pmt_val(rate, nper, pv, fv, typ)
        if pmt is None:
            return None
        # balance still owed after per-1 payments; the interest on it is PAID,
        # i.e. an outflow, hence the negation (Desktop: IPMT(...) = -66.67).
        bal = self._fv_val(rate, per - 1, pmt, pv, typ)
        ip = -bal * rate
        if typ == 1:
            if per == 1:
                return 0.0
            ip = ip / (1 + rate)
        return ip

    def _fn_ppmt(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        rate, per, nper, pv = a[0], a[1], a[2], a[3]
        fv = a[4] if len(a) > 4 and a[4] is not None else 0.0
        typ = a[5] if len(a) > 5 and a[5] is not None else 0.0
        pmt = self._pmt_val(rate, nper, pv, fv, typ)
        if pmt is None:
            return None
        bal = self._fv_val(rate, per - 1, pmt, pv, typ)
        ip = -bal * rate
        if typ == 1:
            ip = 0.0 if per == 1 else ip / (1 + rate)
        return pmt - ip

    @staticmethod
    def _pmt_val(rate, nper, pv, fv, typ):
        if rate == 0:
            return -(pv + fv) / nper if nper else None
        f = (1 + rate) ** nper
        return -(pv * f + fv) * rate / ((1 + rate * typ) * (f - 1))

    @staticmethod
    def _fv_val(rate, nper, pmt, pv, typ):
        """Balance (negated FV) after nper payments."""
        if rate == 0:
            return pv + pmt * nper
        f = (1 + rate) ** nper
        return pv * f + pmt * (1 + rate * typ) * (f - 1) / rate

    def _fn_cumipmt(self, args_str: str, ctx: DAXContext):
        return self._cum_i_p(args_str, ctx, interest=True)

    def _fn_cumprinc(self, args_str: str, ctx: DAXContext):
        return self._cum_i_p(args_str, ctx, interest=False)

    def _cum_i_p(self, args_str: str, ctx: DAXContext, interest: bool):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 6 or any(v is None for v in a[:6]):
            return None
        rate, nper, pv, start, end, typ = a[:6]
        if rate <= 0 or nper <= 0 or pv <= 0 or start < 1 or end < start:
            return None
        pmt = self._pmt_val(rate, nper, pv, 0.0, typ)
        total = 0.0
        for per in range(int(start), int(end) + 1):
            bal = self._fv_val(rate, per - 1, pmt, pv, typ)
            ip = -bal * rate
            if typ == 1:
                ip = 0.0 if per == 1 else ip / (1 + rate)
            total += ip if interest else (pmt - ip)
        return total

    def _fn_ispmt(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        rate, per, nper, pv = a[:4]
        return pv * rate * (per / nper - 1)

    # ---- depreciation ----------------------------------------------------

    def _fn_sln(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]) or a[2] == 0:
            return None
        return (a[0] - a[1]) / a[2]

    def _fn_syd(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        cost, salvage, life, per = a[:4]
        if life <= 0 or per < 1 or per > life:
            return None
        return (cost - salvage) * (life - per + 1) * 2 / (life * (life + 1))

    def _fn_ddb(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        cost, salvage, life, per = a[:4]
        factor = a[4] if len(a) > 4 and a[4] is not None else 2.0
        if life <= 0 or per < 1:
            return None
        rate = factor / life
        book = cost
        dep = 0.0
        for _ in range(int(per)):
            dep = min(book * rate, max(book - salvage, 0.0))
            book -= dep
        return dep

    def _fn_db(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 4 or any(v is None for v in a[:4]):
            return None
        cost, salvage, life, per = a[:4]
        month = a[4] if len(a) > 4 and a[4] is not None else 12.0
        if cost <= 0 or life <= 0:
            return None
        rate = round(1 - (salvage / cost) ** (1.0 / life), 3)
        dep_first = cost * rate * month / 12.0
        if per == 1:
            return dep_first
        book = cost - dep_first
        dep = dep_first
        for _p in range(2, int(per) + 1):
            if _p == int(life) + 1:
                dep = book * rate * (12 - month) / 12.0
            else:
                dep = book * rate
            book -= dep
        return dep

    def _fn_vdb(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 5 or any(v is None for v in a[:5]):
            return None
        cost, salvage, life, start, end = a[:5]
        factor = a[5] if len(a) > 5 and a[5] is not None else 2.0
        no_switch = bool(a[6]) if len(a) > 6 and a[6] is not None else False
        rate = factor / life
        book = cost
        total = 0.0
        per = 0
        while per < end:
            ddb = book * rate
            remaining = life - per
            sl = (book - salvage) / remaining if remaining > 0 else 0.0
            dep = ddb if (no_switch or ddb >= sl) else sl
            dep = min(dep, max(book - salvage, 0.0))
            frac = min(1.0, end - per) - max(0.0, start - per)
            if frac > 0:
                total += dep * frac
            book -= dep
            per += 1
        return total

    def _fn_amordegrc(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 7:
            return None
        cost = self._fin_num(vals[0])
        purch = _as_datetime(vals[1])
        first = _as_datetime(vals[2])
        salvage = self._fin_num(vals[3])
        period = self._fin_num(vals[4])
        rate = self._fin_num(vals[5])
        basis = int(self._fin_num(vals[6], 0.0) or 0)
        if None in (cost, purch, first, salvage, period, rate):
            return None
        life = 1.0 / rate
        if life < 3:
            coeff = 1.0
        elif life < 5:
            coeff = 1.5
        elif life <= 6:
            coeff = 2.0
        else:
            coeff = 2.5
        drate = rate * coeff
        frac = self._daycount_frac(purch, first, basis)
        dep = cost * drate * frac
        book = cost - dep
        p = 0
        while p < int(period):
            dep = book * drate
            rem = book - dep
            if rem < salvage:
                # the two special closing periods
                if p == int(1.0 / drate) - 1:
                    dep = book * 0.5
                else:
                    dep = 0.0
            book -= dep
            p += 1
        return float(round(dep))

    def _fn_amorlinc(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 7:
            return None
        cost = self._fin_num(vals[0])
        purch = _as_datetime(vals[1])
        first = _as_datetime(vals[2])
        salvage = self._fin_num(vals[3])
        period = self._fin_num(vals[4])
        rate = self._fin_num(vals[5])
        basis = int(self._fin_num(vals[6], 0.0) or 0)
        if None in (cost, purch, first, salvage, period, rate):
            return None
        frac = self._daycount_frac(purch, first, basis)
        dep0 = cost * rate * frac
        full = cost * rate
        total_periods = (cost - salvage - dep0) / full
        if period == 0:
            return dep0
        if period <= total_periods:
            return full
        if period <= total_periods + 1:
            return cost - salvage - dep0 - full * math.floor(total_periods)
        return 0.0

    # ---- rates & dollars -------------------------------------------------

    def _fn_effect(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 2 or any(v is None for v in a[:2]) or a[0] <= 0 or a[1] < 1:
            return None
        npery = int(a[1])
        return (1 + a[0] / npery) ** npery - 1

    def _fn_nominal(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 2 or any(v is None for v in a[:2]) or a[0] <= 0 or a[1] < 1:
            return None
        npery = int(a[1])
        return ((1 + a[0]) ** (1.0 / npery) - 1) * npery

    def _fn_rri(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]) or a[0] <= 0 or a[1] == 0:
            return None
        return (a[2] / a[1]) ** (1.0 / a[0]) - 1

    def _fn_pduration(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 3 or any(v is None for v in a[:3]) or a[0] <= 0 or a[1] <= 0 or a[2] <= 0:
            return None
        return (math.log(a[2]) - math.log(a[1])) / math.log(1 + a[0])

    def _fn_dollarde(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 2 or any(v is None for v in a[:2]) or int(a[1]) <= 0:
            return None
        frac = int(a[1])
        digits = math.ceil(math.log10(frac)) if frac > 1 else 0
        whole = math.trunc(a[0])
        return whole + (a[0] - whole) * (10 ** digits) / frac

    def _fn_dollarfr(self, args_str: str, ctx: DAXContext):
        a = [self._fin_num(v) for v in self._fin_args(args_str, ctx)]
        if len(a) < 2 or any(v is None for v in a[:2]) or int(a[1]) <= 0:
            return None
        frac = int(a[1])
        digits = math.ceil(math.log10(frac)) if frac > 1 else 0
        whole = math.trunc(a[0])
        return whole + (a[0] - whole) * frac / (10 ** digits)

    # ---- cash flows ------------------------------------------------------

    def _xcashflows(self, args_str: str, ctx: DAXContext, has_rate: bool):
        parts = self._split_args(args_str)
        need = 4 if has_rate else 3
        if len(parts) < need:
            return None
        rows = self._table_rows(parts[0], ctx)
        if rows is None:
            return None
        vals, dates = [], []
        for r in rows:
            row_ctx = self._make_row_context(r, ctx, shadow=rows)
            v = self._eval_expr(parts[1].strip(), row_ctx)
            v = self._resolve_row_result(v, r, row_ctx)
            d = self._eval_expr(parts[2].strip(), row_ctx)
            d = self._resolve_row_result(d, r, row_ctx)
            d = _as_datetime(d)
            v = self._fin_num(v)
            if v is None or d is None:
                continue
            vals.append(v)
            dates.append(d)
        if not vals:
            return None
        rate = None
        if has_rate:
            rate = self._fin_num(self._eval_expr(parts[3].strip(), ctx))
            if rate is None:
                return None
        return vals, dates, rate

    @staticmethod
    def _xnpv_val(rate, vals, dates):
        d0 = min(dates)
        return sum(v / (1 + rate) ** ((d - d0).days / 365.0)
                   for v, d in zip(vals, dates))

    def _fn_xnpv(self, args_str: str, ctx: DAXContext):
        got = self._xcashflows(args_str, ctx, has_rate=True)
        if got is None:
            return None
        vals, dates, rate = got
        return self._xnpv_val(rate, vals, dates)

    def _fn_xirr(self, args_str: str, ctx: DAXContext):
        got = self._xcashflows(args_str, ctx, has_rate=False)
        if got is None:
            return None
        vals, dates, _ = got
        if not (any(v > 0 for v in vals) and any(v < 0 for v in vals)):
            return None
        r = 0.1
        for _ in range(100):
            f0 = self._xnpv_val(r, vals, dates)
            h = 1e-7
            d = (self._xnpv_val(r + h, vals, dates) - f0) / h
            if d == 0:
                break
            rn = r - f0 / d
            if rn <= -0.999999:
                rn = (r - 0.999999) / 2
            if abs(rn - r) < 1e-12:
                return rn
            r = rn
        return r

    # ---- day-count / coupon kernel --------------------------------------

    @staticmethod
    def _days360(d1: datetime, d2: datetime, european: bool) -> int:
        y1, m1, dd1 = d1.year, d1.month, d1.day
        y2, m2, dd2 = d2.year, d2.month, d2.day
        if european:
            dd1 = min(dd1, 30)
            dd2 = min(dd2, 30)
        else:
            last1 = calendar.monthrange(y1, m1)[1]
            if dd1 == 31 or (m1 == 2 and dd1 == last1):
                dd1 = 30
            if dd2 == 31 and dd1 == 30:
                dd2 = 30
        return (y2 - y1) * 360 + (m2 - m1) * 30 + (dd2 - dd1)

    @classmethod
    def _daycount_frac(cls, d1: datetime, d2: datetime, basis: int) -> float:
        if d2 < d1:
            return -cls._daycount_frac(d2, d1, basis)
        if basis == 0:
            return cls._days360(d1, d2, european=False) / 360.0
        if basis == 4:
            return cls._days360(d1, d2, european=True) / 360.0
        days = (d2 - d1).days
        if basis == 2:
            return days / 360.0
        if basis == 3:
            return days / 365.0
        # basis 1: actual/actual -- year length from the anniversary span
        # Excel uses average year length across the span for multi-year
        span_years = d2.year - d1.year + 1
        total = sum(366 if calendar.isleap(y) else 365
                    for y in range(d1.year, d2.year + 1))
        return days / (total / span_years)

    @staticmethod
    def _edate_months(d, months):
        m0 = d.month - 1 + months
        y = d.year + m0 // 12
        m = m0 % 12 + 1
        day = min(d.day, calendar.monthrange(y, m)[1])
        # coupon schedules stick to month-end when maturity is month-end
        return datetime(y, m, day)

    @classmethod
    def _coup_pcd_ncd(cls, settl, mat, freq):
        """Previous and next coupon dates around settlement, stepping back
        from maturity by 12/freq months."""
        step = -int(12 / freq)
        ncd = mat
        while True:
            pcd = cls._edate_months(ncd, step)
            if pcd <= settl:
                return pcd, ncd
            ncd = pcd

    @classmethod
    def _coupdays_between(cls, d1, d2, basis):
        if basis in (0, 4):
            return cls._days360(d1, d2, european=(basis == 4))
        return (d2 - d1).days

    def _bond_args(self, args_str: str, ctx: DAXContext, n_dates: int, n_nums: int):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < n_dates + n_nums:
            return None
        dates = [_as_datetime(v) for v in vals[:n_dates]]
        nums = [self._fin_num(v) for v in vals[n_dates:n_dates + n_nums]]
        rest = [self._fin_num(v) for v in vals[n_dates + n_nums:]]
        if any(d is None for d in dates) or any(n is None for n in nums):
            return None
        return dates, nums, rest

    def _fn_coupdaybs(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        pcd, _ = self._coup_pcd_ncd(settl, mat, int(freq))
        return float(self._coupdays_between(pcd, settl, basis))

    def _fn_coupdays(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        freq = int(freq)
        pcd, ncd = self._coup_pcd_ncd(settl, mat, freq)
        if basis == 1:
            return float((ncd - pcd).days)
        if basis == 3:
            return 365.0 / freq
        return 360.0 / freq

    def _fn_coupdaysnc(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        pcd, ncd = self._coup_pcd_ncd(settl, mat, int(freq))
        if basis in (0, 4):
            # 30/360: coupon days minus accrued
            return float(self._fn_coupdays_raw(settl, mat, int(freq), basis)
                         - self._coupdays_between(pcd, settl, basis))
        return float((ncd - settl).days)

    def _fn_coupdays_raw(self, settl, mat, freq, basis):
        pcd, ncd = self._coup_pcd_ncd(settl, mat, freq)
        if basis == 1:
            return (ncd - pcd).days
        if basis == 3:
            return 365.0 / freq
        return 360.0 / freq

    def _fn_coupncd(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), _rest = got
        _, ncd = self._coup_pcd_ncd(settl, mat, int(freq))
        return ncd

    def _fn_couppcd(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), _rest = got
        pcd, _ = self._coup_pcd_ncd(settl, mat, int(freq))
        return pcd

    def _fn_coupnum(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (freq,), _rest = got
        freq = int(freq)
        n = 0
        _, ncd = self._coup_pcd_ncd(settl, mat, freq)
        cur = ncd
        while cur <= mat:
            n += 1
            if cur == mat:
                break
            cur = self._edate_months(cur, int(12 / freq))
        return float(n)

    def _fn_accrint(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 6:
            return None
        issue = _as_datetime(vals[0])
        settl = _as_datetime(vals[2])
        rate = self._fin_num(vals[3])
        par = self._fin_num(vals[4])
        basis = int(self._fin_num(vals[6], 0.0) or 0) if len(vals) > 6 else 0
        if None in (issue, settl, rate, par):
            return None
        return par * rate * self._daycount_frac(issue, settl, basis)

    def _fn_accrintm(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 4:
            return None
        issue = _as_datetime(vals[0])
        settl = _as_datetime(vals[1])
        rate = self._fin_num(vals[2])
        par = self._fin_num(vals[3])
        basis = int(self._fin_num(vals[4], 0.0) or 0) if len(vals) > 4 else 0
        if None in (issue, settl, rate, par):
            return None
        return par * rate * self._daycount_frac(issue, settl, basis)

    def _fn_disc(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 2)
        if got is None:
            return None
        (settl, mat), (pr, redemption), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        frac = self._daycount_frac(settl, mat, basis)
        if frac == 0:
            return None
        return (redemption - pr) / redemption / frac

    def _fn_intrate(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 2)
        if got is None:
            return None
        (settl, mat), (investment, redemption), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        frac = self._daycount_frac(settl, mat, basis)
        if frac == 0 or investment == 0:
            return None
        return (redemption - investment) / investment / frac

    def _fn_received(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 2)
        if got is None:
            return None
        (settl, mat), (investment, disc), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        frac = self._daycount_frac(settl, mat, basis)
        den = 1 - disc * frac
        if den == 0:
            return None
        return investment / den

    def _fn_pricedisc(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 2)
        if got is None:
            return None
        (settl, mat), (disc, redemption), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        frac = self._daycount_frac(settl, mat, basis)
        return redemption - disc * redemption * frac

    def _fn_yielddisc(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 2)
        if got is None:
            return None
        (settl, mat), (pr, redemption), rest = got
        basis = int(rest[0]) if rest and rest[0] is not None else 0
        frac = self._daycount_frac(settl, mat, basis)
        if frac == 0 or pr == 0:
            return None
        return (redemption - pr) / pr / frac

    def _fn_pricemat(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 5:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        issue = _as_datetime(vals[2])
        rate = self._fin_num(vals[3])
        yld = self._fin_num(vals[4])
        basis = int(self._fin_num(vals[5], 0.0) or 0) if len(vals) > 5 else 0
        if None in (settl, mat, issue, rate, yld):
            return None
        fim = self._daycount_frac(issue, mat, basis)
        fis = self._daycount_frac(issue, settl, basis)
        fsm = self._daycount_frac(settl, mat, basis)
        return ((1 + fim * rate) / (1 + fsm * yld) - fis * rate) * 100

    def _fn_yieldmat(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 5:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        issue = _as_datetime(vals[2])
        rate = self._fin_num(vals[3])
        pr = self._fin_num(vals[4])
        basis = int(self._fin_num(vals[5], 0.0) or 0) if len(vals) > 5 else 0
        if None in (settl, mat, issue, rate, pr):
            return None
        fim = self._daycount_frac(issue, mat, basis)
        fis = self._daycount_frac(issue, settl, basis)
        fsm = self._daycount_frac(settl, mat, basis)
        if fsm == 0:
            return None
        num = (1 + fim * rate) - (pr / 100.0 + fis * rate)
        den = (pr / 100.0 + fis * rate)
        return num / den / fsm

    def _fn_tbilleq(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (disc,), _rest = got
        dsm = (mat - settl).days
        den = 360 - disc * dsm
        if den == 0:
            return None
        return 365 * disc / den

    def _fn_tbillprice(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (disc,), _rest = got
        dsm = (mat - settl).days
        return 100 * (1 - disc * dsm / 360.0)

    def _fn_tbillyield(self, args_str: str, ctx: DAXContext):
        got = self._bond_args(args_str, ctx, 2, 1)
        if got is None:
            return None
        (settl, mat), (pr,), _rest = got
        dsm = (mat - settl).days
        if pr == 0 or dsm == 0:
            return None
        return (100 - pr) / pr * 360.0 / dsm

    def _price_val(self, settl, mat, rate, yld, redemption, freq, basis):
        pcd, ncd = self._coup_pcd_ncd(settl, mat, freq)
        e = self._fn_coupdays_raw(settl, mat, freq, basis)
        if basis in (0, 4):
            a = self._coupdays_between(pcd, settl, basis)
            dsc = e - a
        else:
            a = (settl - pcd).days
            dsc = (ncd - settl).days
        n = 0
        cur = ncd
        while cur <= mat:
            n += 1
            if cur == mat:
                break
            cur = self._edate_months(cur, int(12 / freq))
        coupon = 100.0 * rate / freq
        y = yld / freq
        if n == 1:
            t = dsc / e
            return ((redemption + coupon) / (1 + t * y)) - a / e * coupon
        total = redemption / (1 + y) ** (n - 1 + dsc / e)
        for k in range(1, n + 1):
            total += coupon / (1 + y) ** (k - 1 + dsc / e)
        total -= a / e * coupon
        return total

    def _fn_price(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 6:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        rate = self._fin_num(vals[2])
        yld = self._fin_num(vals[3])
        redemption = self._fin_num(vals[4])
        freq = int(self._fin_num(vals[5], 2.0) or 2)
        basis = int(self._fin_num(vals[6], 0.0) or 0) if len(vals) > 6 else 0
        if None in (settl, mat, rate, yld, redemption):
            return None
        return self._price_val(settl, mat, rate, yld, redemption, freq, basis)

    def _fn_yield(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 6:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        rate = self._fin_num(vals[2])
        pr = self._fin_num(vals[3])
        redemption = self._fin_num(vals[4])
        freq = int(self._fin_num(vals[5], 2.0) or 2)
        basis = int(self._fin_num(vals[6], 0.0) or 0) if len(vals) > 6 else 0
        if None in (settl, mat, rate, pr, redemption):
            return None
        y = rate if rate > 0 else 0.05
        for _ in range(200):
            f0 = self._price_val(settl, mat, rate, y, redemption, freq, basis) - pr
            h = 1e-7
            f1 = self._price_val(settl, mat, rate, y + h, redemption, freq, basis) - pr
            d = (f1 - f0) / h
            if d == 0:
                break
            yn = y - f0 / d
            if abs(yn - y) < 1e-13:
                return yn
            y = yn
        return y


    def _quasi_periods(self, start, end, freq, forward):
        """Quasi-coupon boundaries stepping 12/freq months from start."""
        step = int(12 / freq) * (1 if forward else -1)
        out = [start]
        cur = start
        guard = 0
        while (cur < end if forward else cur > end) and guard < 500:
            cur = self._edate_months(cur, step)
            out.append(cur)
            guard += 1
        return out

    def _quasi_len(self, b0, b1, freq, basis):
        if basis in (0, 1, 4):
            return self._coupdays_between(b0, b1, basis)
        return 360.0 / freq if basis == 2 else 365.0 / freq

    def _fn_oddlprice(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 7:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        last = _as_datetime(vals[2])
        rate = self._fin_num(vals[3])
        yld = self._fin_num(vals[4])
        red = self._fin_num(vals[5])
        freq = int(self._fin_num(vals[6], 2.0) or 2)
        basis = int(self._fin_num(vals[7], 0.0) or 0) if len(vals) > 7 else 0
        if None in (settl, mat, last, rate, yld, red):
            return None
        bounds = self._quasi_periods(last, mat, freq, forward=True)
        DCi, Ai = 0.0, 0.0
        for b0, b1 in zip(bounds, bounds[1:]):
            e = self._quasi_len(b0, b1, freq, basis)
            s0, s1 = max(b0, last), min(b1, mat)
            dci = self._coupdays_between(s0, s1, basis) if s1 > s0 else 0
            a0, a1 = max(b0, last), min(b1, settl)
            aa = self._coupdays_between(a0, a1, basis) if a1 > a0 else 0
            DCi += dci / e
            Ai += aa / e
        x = 100.0 * rate / freq
        dsm = self._daycount_frac(settl, mat, basis) * freq
        return (red + DCi * x) / (1 + dsm * yld / freq) - Ai * x

    def _fn_oddlyield(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 7:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        last = _as_datetime(vals[2])
        rate = self._fin_num(vals[3])
        pr = self._fin_num(vals[4])
        red = self._fin_num(vals[5])
        freq = int(self._fin_num(vals[6], 2.0) or 2)
        basis = int(self._fin_num(vals[7], 0.0) or 0) if len(vals) > 7 else 0
        if None in (settl, mat, last, rate, pr, red):
            return None
        bounds = self._quasi_periods(last, mat, freq, forward=True)
        DCi, Ai = 0.0, 0.0
        for b0, b1 in zip(bounds, bounds[1:]):
            e = self._quasi_len(b0, b1, freq, basis)
            s0, s1 = max(b0, last), min(b1, mat)
            dci = self._coupdays_between(s0, s1, basis) if s1 > s0 else 0
            a0, a1 = max(b0, last), min(b1, settl)
            aa = self._coupdays_between(a0, a1, basis) if a1 > a0 else 0
            DCi += dci / e
            Ai += aa / e
        x = 100.0 * rate / freq
        dsm = self._daycount_frac(settl, mat, basis) * freq
        den = (pr + Ai * x) * dsm
        if den == 0:
            return None
        return (red + DCi * x - (pr + Ai * x)) / den * freq

    def _oddfprice_val(self, settl, mat, issue, first, rate, yld, red, freq, basis):
        y = yld / freq
        x = 100.0 * rate / freq
        bounds = list(reversed(self._quasi_periods(first, issue, freq,
                                                   forward=False)))
        DFCsum, Asum = 0.0, 0.0
        for b0, b1 in zip(bounds, bounds[1:]):
            e = self._quasi_len(b0, b1, freq, basis)
            s0, s1 = max(b0, issue), min(b1, first)
            dfc = self._coupdays_between(s0, s1, basis) if s1 > s0 else 0
            a0, a1 = max(b0, issue), min(b1, settl)
            aa = self._coupdays_between(a0, a1, basis) if a1 > a0 else 0
            DFCsum += dfc / e
            Asum += aa / e
        n = 0
        cur = first
        while cur < mat:
            cur = self._edate_months(cur, int(12 / freq))
            n += 1
        eq = self._quasi_len(self._edate_months(first, -int(12 / freq)),
                             first, freq, basis)
        dsc = self._coupdays_between(settl, first, basis)
        t0 = dsc / eq
        total = red / (1 + y) ** (n + t0)
        total += x * DFCsum / (1 + y) ** t0
        for k in range(1, n + 1):
            total += x / (1 + y) ** (k + t0)
        total -= x * Asum
        return total

    def _fn_oddfprice(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 8:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        issue = _as_datetime(vals[2])
        first = _as_datetime(vals[3])
        rate = self._fin_num(vals[4])
        yld = self._fin_num(vals[5])
        red = self._fin_num(vals[6])
        freq = int(self._fin_num(vals[7], 2.0) or 2)
        basis = int(self._fin_num(vals[8], 0.0) or 0) if len(vals) > 8 else 0
        if None in (settl, mat, issue, first, rate, yld, red):
            return None
        return self._oddfprice_val(settl, mat, issue, first, rate, yld, red,
                                   freq, basis)

    def _fn_oddfyield(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 8:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        issue = _as_datetime(vals[2])
        first = _as_datetime(vals[3])
        rate = self._fin_num(vals[4])
        pr = self._fin_num(vals[5])
        red = self._fin_num(vals[6])
        freq = int(self._fin_num(vals[7], 2.0) or 2)
        basis = int(self._fin_num(vals[8], 0.0) or 0) if len(vals) > 8 else 0
        if None in (settl, mat, issue, first, rate, pr, red):
            return None
        y = max(rate, 0.05)
        for _ in range(200):
            f0 = self._oddfprice_val(settl, mat, issue, first, rate, y, red,
                                     freq, basis) - pr
            h = 1e-7
            f1 = self._oddfprice_val(settl, mat, issue, first, rate, y + h,
                                     red, freq, basis) - pr
            d = (f1 - f0) / h
            if d == 0:
                break
            yn = y - f0 / d
            if abs(yn - y) < 1e-13:
                return yn
            y = yn
        return y

    def _fn_duration(self, args_str: str, ctx: DAXContext, modified=False):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 5:
            return None
        settl = _as_datetime(vals[0])
        mat = _as_datetime(vals[1])
        coupon = self._fin_num(vals[2])
        yld = self._fin_num(vals[3])
        freq = int(self._fin_num(vals[4], 2.0) or 2)
        basis = int(self._fin_num(vals[5], 0.0) or 0) if len(vals) > 5 else 0
        if None in (settl, mat, coupon, yld):
            return None
        pcd, ncd = self._coup_pcd_ncd(settl, mat, freq)
        e = self._fn_coupdays_raw(settl, mat, freq, basis)
        if basis in (0, 4):
            dsc = e - self._coupdays_between(pcd, settl, basis)
        else:
            dsc = (ncd - settl).days
        n = 0
        cur = ncd
        while cur <= mat:
            n += 1
            if cur == mat:
                break
            cur = self._edate_months(cur, int(12 / freq))
        y = yld / freq
        c = coupon / freq
        t0 = dsc / e
        pv_total = 0.0
        wpv_total = 0.0
        for k in range(1, n + 1):
            t = k - 1 + t0
            cf = c + (1.0 if k == n else 0.0)
            pv = cf / (1 + y) ** t
            pv_total += pv
            wpv_total += t * pv
        dur = (wpv_total / pv_total) / freq
        if modified:
            dur = dur / (1 + y)
        return dur

    def _fn_mduration(self, args_str: str, ctx: DAXContext):
        return self._fn_duration(args_str, ctx, modified=True)


    # ------------------------------------------------------------------
    # Conformance batch 4: remaining scalars, type predicates, ROLLUP
    # machinery, LINEST, TOCSV/TOJSON. Golden-pinned. The visual-calc family
    # (LOOKUP, COLLAPSE, EXPAND, ISATLEVEL) and the calculation-group family
    # (SELECTEDMEASURE*) are NOT here: Desktop refuses them outside their
    # contexts ("can only be used in a visual calculation" / "no measure
    # reference in the current context"), so they are classified, not faked.
    # ------------------------------------------------------------------

    def _fn_column_stat(self, name: str, args_str: str, ctx: DAXContext):
        vals = self._column_numbers(args_str, ctx)
        if vals is None:
            return None
        sample = name.endswith(".S")
        if len(vals) < (2 if sample else 1):
            return None
        var = (statistics.variance(vals) if sample
               else statistics.pvariance(vals))
        return math.sqrt(var) if name.startswith("STDEV") else var

    def _fn_maxa_mina(self, name: str, args_str: str, ctx: DAXContext):
        ref = self._eval_expr(args_str.strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        vals = []
        for v in ctx.get_column_data(ref[0], ref[1]):
            if v is None:
                continue
            if isinstance(v, bool):
                vals.append(1.0 if v else 0.0)
            elif isinstance(v, (int, float)):
                vals.append(float(v))
            elif isinstance(v, datetime):
                vals.append(_dax_serial(v))
            else:
                vals.append(0.0)
        if not vals:
            return None
        return max(vals) if name == "MAXA" else min(vals)

    def _fn_productx(self, args_str: str, ctx: DAXContext):
        vals = self._iter_numbers(args_str, ctx)
        if not vals:
            return None
        out = 1.0
        for v in vals:
            out *= v
        return out

    # ---- type predicates -------------------------------------------------

    def _fn_type_pred(self, name: str, args_str: str, ctx: DAXContext):
        v = self._eval_expr(args_str.strip(), ctx)
        if name == "ISBOOLEAN":
            return isinstance(v, bool)
        if name == "ISSTRING":
            return isinstance(v, str)
        if name == "ISNUMERIC":
            return isinstance(v, (int, float)) and not isinstance(v, bool)
        if name in ("ISINTEGER", "ISINT64"):
            return (isinstance(v, int) and not isinstance(v, bool))
        if name in ("ISCURRENCY", "ISDECIMAL"):
            return isinstance(v, _Currency)
        if name == "ISDOUBLE":
            return (isinstance(v, float) and not isinstance(v, _Currency))
        return None

    def _fn_iseven_odd(self, name: str, args_str: str, ctx: DAXContext):
        x = self._num1(args_str, ctx)
        if x is None:
            return None
        even = int(math.trunc(x)) % 2 == 0
        return even if name == "ISEVEN" else not even

    def _fn_isempty(self, args_str: str, ctx: DAXContext):
        res = self._eval_expr(args_str.strip(), ctx)
        if isinstance(res, list):
            return len(res) == 0
        return None

    def _fn_isafter(self, args_str: str, ctx: DAXContext):
        # Strict version of ISONORAFTER: equality on ALL pairs is FALSE.
        parts = self._split_args(args_str)
        i = 0
        while i < len(parts):
            v1 = self._eval_expr(parts[i].strip(), ctx)
            if i + 1 >= len(parts):
                return None
            v2 = self._eval_expr(parts[i + 1].strip(), ctx)
            order = "ASC"
            if i + 2 < len(parts) and parts[i + 2].strip().upper() in ("ASC", "DESC"):
                order = parts[i + 2].strip().upper()
                i += 3
            else:
                i += 2
            try:
                if v1 == v2:
                    continue
                return bool((v1 > v2) if order == "ASC" else (v1 < v2))
            except TypeError:
                return None
        return False

    # ---- non-blank value navigation --------------------------------------

    def _fn_nonblankvalue(self, name: str, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) != 2:
            return None
        ref = self._eval_expr(parts[0].strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        t, c = ref
        vals = sorted(set(v for v in ctx.get_column_data(t, c)
                          if v is not None), key=lambda x: (str(type(x)), x))
        seq = vals if name == "FIRSTNONBLANKVALUE" else list(reversed(vals))
        # an iteration over the column's values: ALLSELECTED inside restores
        # them all (#118)
        shadow = [{'__table__': t, '__column__': c, '__value__': v} for v in vals]
        for v in seq:
            sub = self._transitioned_ctx(ctx, {f"{t}.{c}": _row_value([v], shadow)})
            res = self._eval_expr(parts[1].strip(), sub)
            if res is not None:
                return res
        return None

    # ---- misc scalars ----------------------------------------------------

    def _fn_convert(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) != 2:
            return None
        v = self._eval_expr(parts[0].strip(), ctx)
        target = parts[1].strip().upper()
        try:
            if target in ("INTEGER", "INT64"):
                if isinstance(v, str):
                    v = float(v)
                if isinstance(v, (int, float)):
                    return int(round(float(v)))
                return None
            if target == "DOUBLE":
                if isinstance(v, str):
                    return float(v)
                if isinstance(v, (int, float)):
                    return float(v)
                return None
            if target in ("CURRENCY", "DECIMAL"):
                if isinstance(v, str):
                    v = float(v)
                if isinstance(v, (int, float)):
                    return _Currency(v)
                return None
            if target in ("STRING", "TEXT"):
                return _concat_str(v)
            if target == "BOOLEAN":
                if isinstance(v, (int, float)):
                    return bool(v)
                return None
            if target == "DATETIME":
                return _as_datetime(v)
        except (ValueError, OverflowError):
            return None
        return None

    def _fn_time(self, args_str: str, ctx: DAXContext):
        a = self._num_args(args_str, ctx, 3)
        if a is None:
            return None
        total = int(a[0]) * 3600 + int(a[1]) * 60 + int(a[2])
        total %= 86400
        return datetime(1899, 12, 30, total // 3600, (total % 3600) // 60,
                        total % 60)

    def _fn_yearfrac(self, args_str: str, ctx: DAXContext):
        vals = self._fin_args(args_str, ctx)
        if len(vals) < 2:
            return None
        d1 = _as_datetime(vals[0])
        d2 = _as_datetime(vals[1])
        basis = int(self._fin_num(vals[2], 0.0) or 0) if len(vals) > 2 else 0
        if d1 is None or d2 is None:
            return None
        return abs(self._daycount_frac(d1, d2, basis))

    def _fn_if_eager(self, args_str: str, ctx: DAXContext):
        # Same result as IF; the eager evaluation strategy is unobservable in
        # a scalar engine.
        return self._fn_if(args_str, ctx)

    def _fn_evaluateandlog(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if not parts:
            return None
        return self._eval_expr(parts[0].strip(), ctx)

    def _fn_nameof(self, args_str: str, ctx: DAXContext):
        m = _TCOL_RE.match(args_str.strip())
        if m:
            t = m.group(1) or m.group(2)
            return f"'{t}'[{m.group(3)}]"
        a = args_str.strip()
        if a.startswith("[") and a.endswith("]"):
            name = a[1:-1]
            home = (ctx.measure_tables or {}).get(name)
            if home is None:
                for cand, tbl in (ctx.measure_tables or {}).items():
                    if cand.lower() == name.lower():
                        home = tbl
                        break
            if home:
                return f"'{home}'[{name}]"
            return f"[{name}]"
        return None

    def _fn_userculture(self, args_str: str, ctx: DAXContext):
        import locale
        loc = locale.getlocale()[0] or "en-US"
        return loc.replace("_", "-")

    def _fn_userobjectid(self, args_str: str, ctx: DAXContext):
        import getpass
        try:
            return getpass.getuser()
        except Exception:
            return "user"

    def _fn_customdata(self, args_str: str, ctx: DAXContext):
        return None

    def _fn_sample(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 3:
            return None
        n = self._num1(parts[0], ctx)
        rows = self._table_rows(parts[1], ctx)
        if n is None or rows is None:
            return None
        order_expr = parts[2].strip()
        desc = True
        if len(parts) >= 4 and parts[3].strip().upper() in ("ASC", "DESC"):
            desc = parts[3].strip().upper() == "DESC"
        keyed = []
        for r in rows:
            row_ctx = self._make_row_context(r, ctx, shadow=rows)
            k = self._eval_expr(order_expr, row_ctx)
            k = self._resolve_row_result(k, r, row_ctx)
            keyed.append((k if isinstance(k, (int, float)) else float("-inf"), r))
        keyed.sort(key=lambda t2: t2[0], reverse=desc)
        n = int(n)
        if n >= len(keyed):
            return [r for _, r in keyed]
        if n <= 1:
            return [keyed[0][1]] if keyed else []
        # evenly spaced across the ordered set, endpoints included
        step = (len(keyed) - 1) / (n - 1)
        picked = [keyed[round(i * step)][1] for i in range(n)]
        return picked

    def _fn_tocsv(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        rows = self._table_rows(parts[0], ctx)
        if rows is None:
            return None
        max_rows = None
        if len(parts) > 1:
            mr = self._num1(parts[1], ctx)
            if mr is not None:
                max_rows = int(mr)
        if not rows:
            return ""
        t = rows[0]["__table__"]
        cols = [c for c in rows[0] if not c.startswith("__")]
        # keep the model's column order when available
        tbl = ctx.tables.get(t)
        if tbl:
            cols = [c for c in tbl["columns"] if c in cols]
        header = ",".join(f"'{t}'[{c}]" for c in cols)
        out = [header]
        body = rows if max_rows is None else rows[:max_rows]
        for r in body:
            out.append(",".join(_concat_str(r.get(c)) for c in cols))
        return "\n".join(out)

    def _fn_tojson(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        rows = self._table_rows(parts[0], ctx)
        if rows is None:
            return None
        max_rows = None
        if len(parts) > 1:
            mr = self._num1(parts[1], ctx)
            if mr is not None:
                max_rows = int(mr)
        if not rows:
            return '{}'
        t = rows[0]["__table__"]
        cols = [c for c in rows[0] if not c.startswith("__")]
        tbl = ctx.tables.get(t)
        if tbl:
            cols = [c for c in tbl["columns"] if c in cols]
        header = ", ".join(f'"\'{t}\'[{c}]"' for c in cols)
        body = rows if max_rows is None else rows[:max_rows]
        data_lines = []
        for r in body:
            cells = []
            for c in cols:
                v = r.get(c)
                if isinstance(v, str):
                    cells.append(f'"{v}"')
                elif v is None:
                    cells.append("null")
                elif isinstance(v, bool):
                    cells.append("true" if v else "false")
                else:
                    cells.append(_concat_str(v))
            data_lines.append("\t\t[" + ", ".join(cells) + "]")
        return ("{\n\t\"header\": [" + header + "],\n\t\"rowCount\": "
                + str(len(rows)) + ",\n\t\"data\": [\n"
                + ",\n".join(data_lines) + "\n\t]\n}")

    def _fn_linest(self, args_str: str, ctx: DAXContext, iterator=False):
        parts = self._split_args(args_str)
        if iterator:
            if len(parts) < 3:
                return None
            rows = self._table_rows(parts[0], ctx)
            if rows is None:
                return None
            ys, xs = [], []
            for r in rows:
                row_ctx = self._make_row_context(r, ctx, shadow=rows)
                yv = self._resolve_row_result(
                    self._eval_expr(parts[1].strip(), row_ctx), r, row_ctx)
                xv = self._resolve_row_result(
                    self._eval_expr(parts[2].strip(), row_ctx), r, row_ctx)
                ys.append(float(yv) if isinstance(yv, (int, float))
                          and not isinstance(yv, bool) else 0.0)
                xs.append(float(xv) if isinstance(xv, (int, float))
                          and not isinstance(xv, bool) else 0.0)
        else:
            if len(parts) < 2:
                return None
            yref = self._eval_expr(parts[0].strip(), ctx)
            xref = self._eval_expr(parts[1].strip(), ctx)
            if not (isinstance(yref, tuple) and isinstance(xref, tuple)):
                return None
            yvals = ctx.get_column_data(yref[0], yref[1])
            xvals = ctx.get_column_data(xref[0], xref[1])
            # Desktop pairs the columns ROW BY ROW and a BLANK participates as
            # zero -- the fixture golden slope is -0.01, which only reproduces
            # with the blank row included as 0, not skipped.
            ys = [float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0
                  for v in yvals]
            xs = [float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0
                  for v in xvals]
        if not ys or not xs or len(ys) != len(xs) or len(ys) < 2:
            return None
        n = len(ys)
        mx = sum(xs) / n
        my = sum(ys) / n
        sxx = sum((x - mx) ** 2 for x in xs)
        if sxx == 0:
            return None
        slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
        intercept = my - slope * mx
        return [{"__table__": "__linest__", "__row__": True,
                 "Slope1": slope, "Intercept": intercept}]

    def _fn_tableof(self, args_str: str, ctx: DAXContext):
        """TABLEOF(measure) -- the measure's home-table rows under the current
        context (same default as DETAILROWS)."""
        return self._fn_detailrows(args_str, ctx)

    def _fn_samplecartesian(self, args_str: str, ctx: DAXContext):
        """SAMPLECARTESIANPOINTSBYCOVER(n, table, x, y) -- point sampling for
        charts; with n >= COUNTROWS it is the table itself, which is what the
        fixture golden pins. Larger tables reuse SAMPLE's even spacing."""
        parts = self._split_args(args_str)
        if len(parts) < 3:
            return None
        n = self._num1(parts[0], ctx)
        rows = self._table_rows(parts[1], ctx)
        if n is None or rows is None:
            return None
        if int(n) >= len(rows):
            return rows
        return self._fn_sample(
            ",".join([parts[0], parts[1], parts[2]]), ctx)

    def _fn_utctoday(self, args_str: str, ctx: DAXContext):
        now = datetime.now(timezone.utc)
        return datetime(now.year, now.month, now.day)

    # ---- window function family (ROWNUMBER / RANK / INDEX / OFFSET /
    # WINDOW).  ORDERBY / PARTITIONBY / MATCHBY are marker sub-expressions
    # parsed here, never evaluated standalone.  The relation is materialised
    # against the PRE-transition context (ctx._outer_ctx): a window function
    # inside SUMX must see every iterated row, not the one row the eager
    # row-to-filter transition narrowed the context to.

    def _win_base_ctx(self, ctx: DAXContext) -> DAXContext:
        return getattr(ctx, '_outer_ctx', None) or ctx

    def _win_parse(self, parts: list, ctx: DAXContext, skip: int = 0):
        """Split window args into (rows, orderby, partitionby, matchby).
        orderby is [(expr, 'ASC'|'DESC')]."""
        relation_expr = None
        orderby: list = []
        partitionby: list = []
        matchby: list = []
        for raw in parts[skip:]:
            ps = raw.strip()
            up = ps.upper()
            if up.startswith('ORDERBY') and ps.endswith(')'):
                items = self._split_args(ps[ps.index('(') + 1:-1])
                i = 0
                while i < len(items):
                    expr = items[i].strip()
                    direction = 'ASC'
                    if (i + 1 < len(items)
                            and items[i + 1].strip().upper() in ('ASC', 'DESC')):
                        direction = items[i + 1].strip().upper()
                        i += 1
                    else:
                        # Tolerate the space form `ORDERBY(T[C] DESC)` too —
                        # silently keeping ASC would sort the wrong way.
                        tail = expr.rsplit(None, 1)
                        if len(tail) == 2 and tail[1].upper() in ('ASC', 'DESC'):
                            expr, direction = tail[0].strip(), tail[1].upper()
                    if expr:
                        orderby.append((expr, direction))
                    i += 1
            elif up.startswith('PARTITIONBY') and ps.endswith(')'):
                partitionby = [x.strip() for x in
                               self._split_args(ps[ps.index('(') + 1:-1])]
            elif up.startswith('MATCHBY') and ps.endswith(')'):
                matchby = [x.strip() for x in
                           self._split_args(ps[ps.index('(') + 1:-1])]
            elif up in ('KEEP', 'DEFAULT', 'KEEPBLANKS', 'IGNOREBLANKS'):
                continue
            elif relation_expr is None:
                relation_expr = ps
        if relation_expr is None:
            # default relation: the table of the first ORDERBY/PARTITIONBY
            # column reference
            src = None
            if orderby:
                src = orderby[0][0]
            elif partitionby:
                src = partitionby[0]
            if not src or '[' not in src:
                return None, orderby, partitionby, matchby
            t = src[:src.index('[')].strip().strip("'")
            relation_expr = f"'{t}'" if ' ' in t else t
        rows = self._table_rows(relation_expr, self._win_base_ctx(ctx))
        return rows, orderby, partitionby, matchby

    def _win_val(self, expr: str, row: dict, base_ctx: DAXContext):
        rc = self._make_row_context(row, base_ctx)
        return self._resolve_row_result(self._eval_expr(expr.strip(), rc),
                                        row, rc)

    def _win_key(self, row: dict, orderby: list, base_ctx: DAXContext) -> tuple:
        return tuple(self._win_val(e, row, base_ctx) for e, _ in orderby)

    @staticmethod
    def _win_ord(v):
        """One orderable key: blanks first, then numbers/dates, then text."""
        if v is None:
            return (0, 0.0, '')
        if isinstance(v, bool):
            return (1, float(v), '')
        if isinstance(v, (int, float)):
            return (1, float(v), '')
        if isinstance(v, datetime):
            return (1, v.toordinal() + (v.hour * 3600 + v.minute * 60
                                        + v.second) / 86400.0, '')
        return (2, 0.0, str(v).casefold())

    def _win_sort(self, rows: list, orderby: list, base_ctx: DAXContext) -> list:
        if not orderby:
            return list(rows)
        keyed = [(self._win_key(r, orderby, base_ctx), r) for r in rows]
        for i in range(len(orderby) - 1, -1, -1):
            rev = orderby[i][1] == 'DESC'
            # sort executes inside the iteration, so capturing `i` is safe
            keyed.sort(key=lambda kr: self._win_ord(kr[0][i]), reverse=rev)
        return [r for _, r in keyed]

    def _win_partition(self, rows: list, partitionby: list,
                       cur: Optional[dict],
                       base_ctx: DAXContext) -> list:
        if not partitionby or cur is None:
            return list(rows)
        ck = tuple(self._win_val(c, cur, base_ctx) for c in partitionby)
        return [r for r in rows
                if tuple(self._win_val(c, r, base_ctx)
                         for c in partitionby) == ck]

    def _win_index_of(self, srt: list, cur: dict, matchby: list,
                      base_ctx: DAXContext):
        if cur is None:
            return None
        if matchby:
            ck = tuple(self._win_val(c, cur, base_ctx) for c in matchby)
            for i, r in enumerate(srt):
                if tuple(self._win_val(c, r, base_ctx) for c in matchby) == ck:
                    return i
            return None
        cur_cols = self._row_cols(cur)
        # Single-column rows (__column__/__value__) have NO named columns, so
        # cur_cols is {} and `{} == {}` matched the FIRST row for every
        # group — ROWNUMBER answered 1 everywhere and OFFSET stepped from
        # index 0 regardless of the current group (issue #25 r23#1). Compare
        # named columns only when there ARE any; else fall through to the
        # __column__/__value__ comparison.
        if cur_cols:
            for i, r in enumerate(srt):
                if self._row_cols(r) == cur_cols:
                    return i
        for i, r in enumerate(srt):
            if (r.get('__column__') == cur.get('__column__')
                    and r.get('__value__') == cur.get('__value__')):
                return i
        return None

    def _win_visual_axis(self, orderby: list, partitionby: list,
                         ctx: DAXContext):
        """Visual-context window axis (issue #25 r23#1).

        A window function in a MEASURE under grouped evaluation has no row
        context — ``ROWNUMBER(ORDERBY(S[Cat]))`` returned blank and
        ``OFFSET(-1,,ORDERBY(S[Cat]))`` silently left the filter context
        unchanged. Desktop's semantics there: the axis is the SELECTED values
        of the ORDERBY/PARTITIONBY columns (the visible groups) and the
        current position is the group's own filter value.

        Returns ``(axis_rows, current_row)`` — single-column row dicts when
        the axis is one column (so a CALCULATE filter replaces just that
        column), full-row dicts otherwise — or ``(None, None)`` when the
        shape is not the plain column-reference case or the current group
        cannot be identified (never guessed).
        """
        cols: list = []
        for e, _d in (orderby or []):
            m = _WHOLE_TCOL_RE.match(e.strip())
            if not m:
                return None, None
            cols.append(((m.group(1) or m.group(2) or '').strip(),
                         m.group(3).strip()))
        for e in (partitionby or []):
            m = _WHOLE_TCOL_RE.match(e.strip())
            if not m:
                return None, None
            cols.append(((m.group(1) or m.group(2) or '').strip(),
                         m.group(3).strip()))
        if not cols or len({t for t, _ in cols}) != 1:
            return None, None
        # The current group: every axis column must carry a single-valued
        # filter in the live context (the group key the grouped evaluation
        # injected).
        cur_vals = {}
        for t, c in cols:
            v = ctx.filter_context.get(f"{t}.{c}")
            if not isinstance(v, list) or len(v) != 1:
                return None, None
            cur_vals[(t, c)] = v[0]
        table = cols[0][0]
        sel = self._selected_ctx(ctx)
        if len(cols) == 1:
            t, c = cols[0]
            vals = list(dict.fromkeys(sel.get_column_data(t, c)))
            axis = [{'__table__': t, '__column__': c, '__value__': v}
                    for v in vals]
            cur = {'__table__': t, '__column__': c,
                   '__value__': cur_vals[(t, c)]}
            return axis, cur
        idxs = []
        tbl = sel.tables.get(table)
        if not tbl:
            return None, None
        for _t, c in cols:
            ci = sel._find_col_idx(tbl['columns'], c)
            if ci < 0:
                return None, None
            idxs.append((c, ci))
        seen = set()
        axis = []
        for r in sel.get_filtered_rows(table):
            combo = tuple(r[ci] for _c, ci in idxs)
            if combo in seen:
                continue
            seen.add(combo)
            d = {'__table__': table, '__row__': True}
            for (c, _ci), v in zip(idxs, combo):
                d[c] = v
            axis.append(d)
        cur = {'__table__': table, '__row__': True}
        for t, c in cols:
            cur[c] = cur_vals[(t, c)]
        return axis, cur

    def _fn_rownumber_win(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str) if args_str.strip() else []
        rows, orderby, partitionby, matchby = self._win_parse(parts, ctx)
        cur = getattr(ctx, '_current_row', None)
        if cur is None:
            rows, cur = self._win_visual_axis(orderby, partitionby, ctx)
        if rows is None or cur is None:
            return None
        base = self._win_base_ctx(ctx)
        srt = self._win_sort(self._win_partition(rows, partitionby, cur, base),
                             orderby, base)
        idx = self._win_index_of(srt, cur, matchby, base)
        return None if idx is None else float(idx + 1)

    def _fn_rank_win(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str) if args_str.strip() else []
        ties = 'SKIP'
        skip = 0
        if parts and parts[0].strip().upper() in ('SKIP', 'DENSE'):
            ties = parts[0].strip().upper()
            skip = 1
        rows, orderby, partitionby, matchby = self._win_parse(parts, ctx, skip)
        cur = getattr(ctx, '_current_row', None)
        if cur is None:
            rows, cur = self._win_visual_axis(orderby, partitionby, ctx)
        if rows is None or cur is None:
            return None
        base = self._win_base_ctx(ctx)
        srt = self._win_sort(self._win_partition(rows, partitionby, cur, base),
                             orderby, base)
        cur_k = self._win_key(cur, orderby, base)
        prev_k: object = object()
        dense = 0
        group_start = 0
        for i, r in enumerate(srt):
            k = self._win_key(r, orderby, base)
            if k != prev_k:
                dense += 1
                group_start = i
                prev_k = k
            if k == cur_k:
                return float(group_start + 1 if ties == 'SKIP' else dense)
        return None

    def _fn_index_win(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if not parts:
            return None
        pos = self._num1(parts[0], ctx)
        if pos is None:
            return None
        rows, orderby, partitionby, matchby = self._win_parse(parts, ctx, 1)
        cur = getattr(ctx, '_current_row', None)
        if cur is None:
            vrows, vcur = self._win_visual_axis(orderby, partitionby, ctx)
            if vrows is not None:
                rows, cur = vrows, vcur
        if rows is None:
            return None
        base = self._win_base_ctx(ctx)
        srt = self._win_sort(self._win_partition(rows, partitionby, cur, base),
                             orderby, base)
        n = len(srt)
        i = int(pos) - 1 if pos > 0 else n + int(pos)
        if 0 <= i < n:
            return [srt[i]]
        return []

    def _fn_offset_win(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if not parts:
            return None
        delta = self._num1(parts[0], ctx)
        rows, orderby, partitionby, matchby = self._win_parse(parts, ctx, 1)
        cur = getattr(ctx, '_current_row', None)
        if cur is None:
            vrows, vcur = self._win_visual_axis(orderby, partitionby, ctx)
            if vrows is not None:
                rows, cur = vrows, vcur
        if delta is None or rows is None or cur is None:
            return None
        base = self._win_base_ctx(ctx)
        srt = self._win_sort(self._win_partition(rows, partitionby, cur, base),
                             orderby, base)
        idx = self._win_index_of(srt, cur, matchby, base)
        if idx is None:
            return []
        i = idx + int(delta)
        if 0 <= i < len(srt):
            return [srt[i]]
        return []

    def _fn_window_win(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        frm = self._num1(parts[0], ctx)
        i = 1
        frm_type = 'REL'
        if i < len(parts) and parts[i].strip().upper() in ('ABS', 'REL'):
            frm_type = parts[i].strip().upper()
            i += 1
        if i >= len(parts):
            return None
        to = self._num1(parts[i], ctx)
        i += 1
        to_type = 'REL'
        if i < len(parts) and parts[i].strip().upper() in ('ABS', 'REL'):
            to_type = parts[i].strip().upper()
            i += 1
        if frm is None or to is None:
            return None
        rows, orderby, partitionby, matchby = self._win_parse(parts, ctx, i)
        cur = getattr(ctx, '_current_row', None)
        if cur is None:
            vrows, vcur = self._win_visual_axis(orderby, partitionby, ctx)
            if vrows is not None:
                rows, cur = vrows, vcur
        if rows is None:
            return None
        base = self._win_base_ctx(ctx)
        srt = self._win_sort(self._win_partition(rows, partitionby, cur, base),
                             orderby, base)
        n = len(srt)
        cur_idx = self._win_index_of(srt, cur, matchby, base) if cur else None
        def pos(v, typ):
            v = int(v)
            if typ == 'ABS':
                return v - 1 if v > 0 else n + v
            return (cur_idx if cur_idx is not None else 0) + v
        lo = max(0, pos(frm, frm_type))
        hi = min(n - 1, pos(to, to_type))
        if lo > hi:
            return []
        return srt[lo:hi + 1]

    # ---- INFO.* model-metadata family.  Serves the LOGICAL model the
    # context executes (tables/columns/measures/relationships) plus the
    # Vertipaq physical-structure counts implied by it: per user column one
    # hierarchy (H$) table of two structure columns, per relationship one
    # index (R$) table of one column, per table one internal RowNumber
    # column -- the formulas Desktop's own counts on the conformance fixture
    # pin (22 storage tables, 52 column storages).  Feature families the
    # context does not model (calc groups, KPIs, roles, perspectives, ...)
    # are honestly empty, which is also what Desktop answers on the fixture.

    _ROWNUMBER_COL = 'RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61'

    def _info_rows(self, kind: str, ctx: DAXContext):
        T = {'__table__': 'INFO', '__row__': True}
        tabs = list(ctx.tables.keys())
        ucols = [(t, c) for t in tabs
                 for c in ctx.tables[t].get('columns', [])]
        rels = [r for r in (ctx.relationships or [])]
        if kind == 'tables':
            return [dict(T, ID=i + 1, Name=t) for i, t in enumerate(tabs)]
        if kind == 'columns':
            out = []
            for i, t in enumerate(tabs):
                out.append(dict(T, TableID=i + 1, TableName=t,
                                ExplicitName=self._ROWNUMBER_COL))
                out += [dict(T, TableID=i + 1, TableName=t, ExplicitName=c)
                        for tt, c in ucols if tt == t]
            return out
        if kind == 'measures':
            # names wrapped in double underscores are engine-internal (the
            # conformance harness evaluates each probe AS such a measure);
            # they are not model objects.
            return [dict(T, Name=m, Expression=str(e),
                         TableName=ctx.measure_tables.get(m, ''))
                    for m, e in ctx.measures.items()
                    if not (m.startswith('__') and m.endswith('__'))]
        if kind == 'relationships':
            return [dict(T, ID=i + 1, FromTable=r.get('FromTable'),
                         FromColumn=r.get('FromColumn'),
                         ToTable=r.get('ToTable'), ToColumn=r.get('ToColumn'),
                         IsActive=bool(r.get('IsActive', 1)))
                    for i, r in enumerate(rels)]
        if kind == 'rel_storage':
            return [dict(T, ID=i + 1,
                         Name=f"R${r.get('FromTable')}-{r.get('ToTable')}")
                    for i, r in enumerate(rels)]
        if kind == 'partitions':
            return [dict(T, ID=i + 1, TableID=i + 1, Name=f'{t}-partition')
                    for i, t in enumerate(tabs)]
        if kind == 'one':
            return [dict(T, ID=1, Name='Model')]
        if kind == 'functions':
            from .function_catalog import FUNCTION_CATALOG
            return [dict(T, FUNCTION_NAME=n, INTERFACE_NAME=i)
                    for n, i in FUNCTION_CATALOG]
        if kind == 'dependencies':
            out = []
            for m, e in ctx.measures.items():
                if m.startswith('__') and m.endswith('__'):
                    continue
                expr = e if isinstance(e, str) else str(e)
                reft, refc = [], []
                for t in tabs:
                    for mm in re.finditer(re.escape(t) + r"\s*\[([^\]]+)\]",
                                          expr):
                        if t not in reft:
                            reft.append(t)
                        if (t, mm.group(1)) not in refc:
                            refc.append((t, mm.group(1)))
                out += [dict(T, OBJECT_TYPE='MEASURE', OBJECT=m,
                             REFERENCED_OBJECT_TYPE='TABLE',
                             REFERENCED_OBJECT=t) for t in reft]
                out += [dict(T, OBJECT_TYPE='MEASURE', OBJECT=m,
                             REFERENCED_OBJECT_TYPE='COLUMN',
                             REFERENCED_OBJECT=c) for _t, c in refc]
            for i, r in enumerate(rels):
                if not r.get('IsActive', 1):
                    continue
                out.append(dict(T, OBJECT_TYPE='ACTIVE_RELATIONSHIP',
                                OBJECT=str(i + 1),
                                REFERENCED_OBJECT_TYPE='COLUMN',
                                REFERENCED_OBJECT=r.get('ToColumn')))
                out.append(dict(T, OBJECT_TYPE='ACTIVE_RELATIONSHIP',
                                OBJECT=str(i + 1),
                                REFERENCED_OBJECT_TYPE='COLUMN',
                                REFERENCED_OBJECT=r.get('FromColumn')))
            return out
        if kind == 'storage_tables':
            out = [dict(T, Name=t) for t in tabs]
            out += [dict(T, Name=f'H${t}${c}') for t, c in ucols]
            out += [dict(T, Name=f"R${r.get('FromTable')}-{r.get('ToTable')}")
                    for r in rels]
            return out
        if kind == 'column_storages':
            out = []
            for t in tabs:
                out.append(dict(T, Table=t, Name=self._ROWNUMBER_COL))
                out += [dict(T, Table=t, Name=c)
                        for tt, c in ucols if tt == t]
            for t, c in ucols:
                out.append(dict(T, Table=f'H${t}${c}', Name='POS_TO_ID'))
                out.append(dict(T, Table=f'H${t}${c}', Name='ID_TO_POS'))
            out += [dict(T, Table=f"R${r.get('FromTable')}-{r.get('ToTable')}",
                         Name='ID_TO_POS') for r in rels]
            return out
        if kind == 'annotations':
            out = [dict(T, ObjectType=4, Name='SummarizationSetBy', Value='')
                   for _ in ucols]
            out += [dict(T, ObjectType=3, Name='PBI_ResultType', Value='')
                    for _ in ctx.measures]
            return out
        if kind == 'properties':
            return [dict(T, Name=n, Value='')
                    for n in ('Name', 'Culture', 'DataAccessOptions',
                              'DefaultPowerBIDataSourceVersion',
                              'SourceQueryCulture', 'Version')]
        if kind == 'storage_files':
            files = []
            for cs in self._info_rows('column_storages', ctx):
                files.append(dict(T, Name=f"{cs['Table']}.{cs['Name']}.idf"))
                files.append(dict(T, Name=f"{cs['Table']}.{cs['Name']}.dictionary"))
            files += [dict(T, Name=f"{t}.tbl.xml") for t in tabs]
            return files
        if kind == 'storage_folders':
            return [dict(T, Name=st['Name'])
                    for st in self._info_rows('storage_tables', ctx)]
        if kind == 'segments':
            return [dict(T, Table=cs['Table'], Column=cs['Name'], Segment=1)
                    for cs in self._info_rows('column_storages', ctx)]
        return []

    def _fn_columnstatistics(self, args_str: str, ctx: DAXContext):
        """COLUMNSTATISTICS(): one row per column INCLUDING the internal
        per-table RowNumber column (Desktop: 20 rows on the 15-user-column
        fixture)."""
        out = []
        for t, tbl in ctx.tables.items():
            data = tbl.get('rows', [])
            out.append({'__table__': 'COLUMNSTATISTICS', '__row__': True,
                        'Table Name': t, 'Column Name': self._ROWNUMBER_COL,
                        'Min': None, 'Max': None, 'Cardinality': len(data),
                        'Max Length': None})
            for ci, c in enumerate(tbl.get('columns', [])):
                vals = [r[ci] for r in data if len(r) > ci]
                nonnull = [v for v in vals if v is not None]
                card = len(set(vals))
                mn = mx = None
                maxlen = None
                if nonnull:
                    if all(isinstance(v, str) for v in nonnull):
                        mn = min(nonnull, key=str.casefold)
                        mx = max(nonnull, key=str.casefold)
                        maxlen = max(len(v) for v in nonnull)
                    else:
                        try:
                            mn = min(nonnull)
                            mx = max(nonnull)
                        except TypeError:
                            pass
                out.append({'__table__': 'COLUMNSTATISTICS', '__row__': True,
                            'Table Name': t, 'Column Name': c, 'Min': mn,
                            'Max': mx, 'Cardinality': card,
                            'Max Length': maxlen})
        return out

    def _fn_sampleaxis(self, args_str: str, ctx: DAXContext):
        """SAMPLEAXISWITHLOCALMINMAX(n, table, value, axis, flag) -- chart
        point sampling; with n >= COUNTROWS it is the table itself, which is
        what the fixture golden pins.  Larger tables reuse SAMPLE spacing."""
        parts = self._split_args(args_str)
        if len(parts) < 5:
            return None
        n = self._num1(parts[0], ctx)
        rows = self._table_rows(parts[1], ctx)
        if n is None or rows is None:
            return None
        if int(n) >= len(rows):
            return rows
        return self._fn_sample(",".join(parts[:3]), ctx)

    def _fn_nonvisual(self, args_str: str, ctx: DAXContext):
        """NONVISUAL(filter) -- marks a SUMMARIZECOLUMNS value filter as not
        affecting visual totals; no observable effect in a plain query."""
        return self._eval_expr(args_str.strip(), ctx)

    def _fn_addmissingitems(self, args_str: str, ctx: DAXContext):
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        # AddMissingItems(showAll_col, table, groupBy_col): union the summary
        # rows with the group values the summary filtered away.
        ref = self._eval_expr(parts[0].strip(), ctx)
        summary = self._eval_expr(parts[1].strip(), ctx)
        if not (isinstance(ref, tuple) and len(ref) == 2):
            return None
        if not isinstance(summary, list):
            return None
        t, c = ref
        have = set()
        for r in summary:
            if isinstance(r, dict):
                have.add(r.get(c))
        out = list(summary)
        for v in ctx.get_column_data(t, c):
            pass
        tbl = ctx.tables.get(t)
        if tbl:
            idx = ctx._find_col_idx(tbl["columns"], c)
            if idx >= 0:
                for v in dict.fromkeys(row[idx] for row in tbl["rows"]):
                    if v not in have:
                        out.append({"__table__": t, "__row__": True, c: v})
                        have.add(v)
        return out

    def _fn_sqrt(self, args_str: str, ctx: DAXContext) -> Any:
        """SQRT(number) — square root."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.sqrt(val)
        return None

    def _fn_log(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG(number, base) — logarithm with specified base (default 10)."""
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        base = self._num_arg(args[1], ctx) if len(args) > 1 else 10
        if isinstance(val, (int, float)) and val > 0 and isinstance(base, (int, float)) and base > 0:
            return math.log(val, base)
        return None

    def _fn_log10(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG10(number) — base-10 logarithm."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log10(val)
        return None

    def _fn_ln(self, args_str: str, ctx: DAXContext) -> Any:
        """LN(number) — natural logarithm."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log(val)
        return None

    def _fn_exp(self, args_str: str, ctx: DAXContext) -> Any:
        """EXP(number) — e^x."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)):
            return math.exp(val)
        return None

    def _fn_sign(self, args_str: str, ctx: DAXContext) -> Any:
        """SIGN(number) — returns -1, 0, or 1."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)):
            if val > 0:
                return 1
            elif val < 0:
                return -1
            return 0
        return None

    def _fn_trunc(self, args_str: str, ctx: DAXContext) -> Any:
        """TRUNC(number, digits) — truncate to specified decimal places."""
        args = self._split_args(args_str)
        val = self._num_arg(args[0], ctx)
        d = self._num_arg(args[1], ctx) if len(args) > 1 else 0
        digits = int(d) if d is not None else 0
        if isinstance(val, (int, float)):
            multiplier = 10 ** digits
            return int(val * multiplier) / multiplier
        return None

    def _fn_even(self, args_str: str, ctx: DAXContext) -> Any:
        """EVEN(number) — round up to nearest even integer."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 != 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_odd(self, args_str: str, ctx: DAXContext) -> Any:
        """ODD(number) — round up to nearest odd integer."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 == 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_fact(self, args_str: str, ctx: DAXContext) -> Any:
        """FACT(number) — factorial."""
        val = self._num_arg(args_str, ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.factorial(int(val))
        return None

    def _fn_gcd(self, args_str: str, ctx: DAXContext) -> Any:
        """GCD(a, b) — greatest common divisor."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._num_arg(args[0], ctx)
        b = self._num_arg(args[1], ctx)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return math.gcd(int(a), int(b))
        return None

    def _fn_lcm(self, args_str: str, ctx: DAXContext) -> Any:
        """LCM(a, b) — least common multiple."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._num_arg(args[0], ctx)
        b = self._num_arg(args[1], ctx)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            a_int, b_int = int(a), int(b)
            if a_int == 0 or b_int == 0:
                return 0
            return abs(a_int * b_int) // math.gcd(a_int, b_int)
        return None

    def _fn_rand(self, args_str: str, ctx: DAXContext) -> Any:
        """RAND() — random number between 0 and 1."""
        return random.random()

    def _fn_randbetween(self, args_str: str, ctx: DAXContext) -> Any:
        """RANDBETWEEN(min, max) — random integer between min and max."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        lo = self._eval_expr(args[0].strip(), ctx)
        hi = self._eval_expr(args[1].strip(), ctx)
        if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
            return random.randint(int(lo), int(hi))
        return 0

    def _fn_pi(self, args_str: str, ctx: DAXContext) -> Any:
        """PI() — returns 3.14159..."""
        return math.pi

    def _fn_currency(self, args_str: str, ctx: DAXContext) -> Any:
        """CURRENCY(value) — convert to currency (fixed-point decimal, 4 decimal places)."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            return _Currency(round(val, 4))
        return 0

    def _fn_fixed(self, args_str: str, ctx: DAXContext) -> Any:
        """FIXED(number, decimals, no_commas) — format number as text with fixed decimals."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        decimals = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 2
        no_commas = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else False
        if isinstance(val, (int, float)):
            if decimals < 0:
                # FIXED(1234.567, -2) rounds to the nearest 100 and shows no
                # decimal places -- Desktop: "1200".
                val = round(val / (10 ** -decimals)) * (10 ** -decimals)
                decimals = 0
            if no_commas:
                return f"{val:.{decimals}f}"
            return f"{val:,.{decimals}f}"
        return str(val)

    # =========================================================================
    # Text functions
    # =========================================================================

    def _fn_left(self, args_str: str, ctx: DAXContext) -> Any:
        """LEFT(text, n) — leftmost n characters."""
        args = self._split_args(args_str)
        text = self._eval_expr(args[0].strip(), ctx)
        n = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 1
        if text is not None:
            return str(text)[:n]
        return ''

    def _fn_right(self, args_str: str, ctx: DAXContext) -> Any:
        """RIGHT(text, n) — rightmost n characters."""
        args = self._split_args(args_str)
        text = self._eval_expr(args[0].strip(), ctx)
        n = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 1
        if text is not None:
            s = str(text)
            return s[-n:] if n <= len(s) else s
        return ''

    def _fn_mid(self, args_str: str, ctx: DAXContext) -> Any:
        """MID(text, start, n) — substring from start position (1-based) for n characters."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return ''
        text = self._eval_expr(args[0].strip(), ctx)
        start = int(self._eval_expr(args[1].strip(), ctx))
        n = int(self._eval_expr(args[2].strip(), ctx))
        if text is not None:
            s = str(text)
            return s[start - 1:start - 1 + n]  # DAX uses 1-based indexing
        return ''

    def _fn_len(self, args_str: str, ctx: DAXContext) -> Any:
        """LEN(text) — length of text."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            return len(str(val))
        return 0

    def _fn_upper(self, args_str: str, ctx: DAXContext) -> Any:
        """UPPER(text) — convert to uppercase."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).upper() if val is not None else ''

    def _fn_lower(self, args_str: str, ctx: DAXContext) -> Any:
        """LOWER(text) — convert to lowercase."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).lower() if val is not None else ''

    def _fn_proper(self, args_str: str, ctx: DAXContext) -> Any:
        """PROPER(text) — capitalize first letter of each word."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).title() if val is not None else ''

    def _fn_trim(self, args_str: str, ctx: DAXContext) -> Any:
        """TRIM(text) — remove leading/trailing spaces."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).strip() if val is not None else ''

    def _fn_substitute(self, args_str: str, ctx: DAXContext) -> Any:
        """SUBSTITUTE(text, old, new, instance) — replace text occurrences."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        old = str(self._eval_expr(args[1].strip(), ctx) or '')
        new = str(self._eval_expr(args[2].strip(), ctx) or '')
        if len(args) > 3:
            instance = int(self._eval_expr(args[3].strip(), ctx) or 1)
            # Replace only the nth occurrence
            count = 0
            result = []
            i = 0
            while i < len(text):
                if text[i:i + len(old)] == old:
                    count += 1
                    if count == instance:
                        result.append(new)
                        i += len(old)
                        continue
                result.append(text[i])
                i += 1
            return ''.join(result)
        return text.replace(old, new)

    def _fn_replace(self, args_str: str, ctx: DAXContext) -> Any:
        """REPLACE(text, start, n, new) — replace by position."""
        args = self._split_args(args_str)
        if len(args) < 4:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        start = int(self._eval_expr(args[1].strip(), ctx)) - 1  # DAX is 1-based
        n = int(self._eval_expr(args[2].strip(), ctx))
        new = str(self._eval_expr(args[3].strip(), ctx) or '')
        return text[:start] + new + text[start + n:]

    def _fn_rept(self, args_str: str, ctx: DAXContext) -> Any:
        """REPT(text, n) — repeat text n times."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        n = int(self._eval_expr(args[1].strip(), ctx) or 0)
        return text * max(0, n)

    def _fn_search(self, args_str: str, ctx: DAXContext) -> Any:
        """SEARCH(find, within, start) — find position (case-insensitive, 1-based). Returns -1 if not found."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return -1
        find_text = str(self._eval_expr(args[0].strip(), ctx) or '').lower()
        within_text = str(self._eval_expr(args[1].strip(), ctx) or '').lower()
        start = int(self._eval_expr(args[2].strip(), ctx)) - 1 if len(args) > 2 else 0
        pos = within_text.find(find_text, start)
        return pos + 1 if pos >= 0 else -1  # DAX returns 1-based

    def _fn_find(self, args_str: str, ctx: DAXContext) -> Any:
        """FIND(find, within, start) — find position (case-sensitive, 1-based). Returns -1 if not found."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return -1
        find_text = str(self._eval_expr(args[0].strip(), ctx) or '')
        within_text = str(self._eval_expr(args[1].strip(), ctx) or '')
        start = int(self._eval_expr(args[2].strip(), ctx)) - 1 if len(args) > 2 else 0
        pos = within_text.find(find_text, start)
        return pos + 1 if pos >= 0 else -1

    def _fn_containsstring(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINSSTRING(within, find) — case-insensitive contains check."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        within = str(self._eval_expr(args[0].strip(), ctx) or '').lower()
        find = str(self._eval_expr(args[1].strip(), ctx) or '').lower()
        return find in within

    def _fn_containsstringexact(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINSSTRINGEXACT(within, find) — case-sensitive contains check."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        within = str(self._eval_expr(args[0].strip(), ctx) or '')
        find = str(self._eval_expr(args[1].strip(), ctx) or '')
        return find in within

    def _fn_exact(self, args_str: str, ctx: DAXContext) -> Any:
        """EXACT(text1, text2) — case-sensitive string comparison."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        t1 = str(self._eval_expr(args[0].strip(), ctx) or '')
        t2 = str(self._eval_expr(args[1].strip(), ctx) or '')
        return t1 == t2

    def _fn_unichar(self, args_str: str, ctx: DAXContext) -> Any:
        """UNICHAR(number) — return unicode character for code point."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            try:
                return chr(int(val))
            except (ValueError, OverflowError):
                return ''
        return ''

    def _fn_unicode(self, args_str: str, ctx: DAXContext) -> Any:
        """UNICODE(text) — return unicode code point of first character."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            s = str(val)
            if s:
                return ord(s[0])
        return 0

    def _fn_value(self, args_str: str, ctx: DAXContext) -> Any:
        """VALUE(text) — convert text to number."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is None:
            return 0
        try:
            s = str(val).replace(',', '').replace('$', '').replace('%', '').strip()
            if '.' in s:
                return float(s)
            return int(s)
        except (ValueError, TypeError):
            return 0

    def _fn_combinevalues(self, args_str: str, ctx: DAXContext) -> Any:
        """COMBINEVALUES(delimiter, value1, value2, ...) — join values with delimiter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        delimiter = str(self._eval_expr(args[0].strip(), ctx) or '')
        parts = [str(self._eval_expr(a.strip(), ctx) or '') for a in args[1:]]
        return delimiter.join(parts)

    def _fn_concatenatex(self, args_str: str, ctx: DAXContext) -> Any:
        """CONCATENATEX(table, expression, delimiter[, orderBy_expr[, order]]) —
        iterate table, evaluate expression per row, join with delimiter,
        optionally sorted by orderBy_expr (ASC default, DESC supported)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        delimiter = str(self._eval_expr(args[2].strip(), ctx) or '') if len(args) > 2 else ''
        order_expr = args[3].strip() if len(args) > 3 else None
        descending = len(args) > 4 and 'DESC' in args[4].strip().upper()

        parts: list[Any] = []   # (sort_key, text) when sorting, else just text
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                else:
                    row_ctx = ctx
                    result = self._eval_expr(row_expr, ctx)
                if result is None:
                    # BLANK joins as "" -- the delimiter stays (Desktop:
                    # CONCATENATEX(VALUES(BD[Name]), BD[Name], ",") = "X,Y,Z,").
                    result = ''
                if order_expr is not None:
                    key = self._eval_expr(order_expr, row_ctx)
                    parts.append((key, str(result)))
                else:
                    parts.append(str(result))
        if order_expr is not None:
            # Stable sort; BLANK keys sort first (as smallest).
            def _key(item):
                k = item[0]
                return (k is None, k if isinstance(k, (int, float, str)) else str(k))
            parts.sort(key=_key, reverse=descending)
            return delimiter.join(text for _, text in parts)
        return delimiter.join(parts)

    def _rankx_order_desc(self, order: str, ctx: DAXContext) -> bool:
        """RANKX's order argument: 0 / FALSE / DESC -- and an omitted or empty
        argument -- rank high to low; 1 / TRUE / ASC low to high (issue #124:
        'DESC' in the text was tested, so 0, FALSE and an omitted order all
        ranked ascending). Desktop 2.152 answers every spelling that way."""
        ou = order.strip().upper()
        if ou in ('', 'DESC', '0', 'FALSE', 'FALSE()'):
            return True
        if ou in ('ASC', '1', 'TRUE', 'TRUE()'):
            return False
        return not bool(self._eval_expr(order.strip(), ctx))

    @staticmethod
    def _rankx_keys(values: list):
        """RANKX's values as one comparable kind, BLANK included (issue #125).

        "If expression or value evaluates to BLANK it is treated as a 0 (zero)
        for all expressions that result in a number, or as an empty text for
        all text expressions" (Microsoft's RANKX reference); Desktop 2.152
        ranks a member with no data 5th of 6, above a -5. Dates are numbers.
        Text compares as the formula engine does (case-insensitively). A value
        of any other kind maps to None and is left out."""
        text = any(isinstance(v, str) for v in values)
        out: list[str | float | None] = []
        for v in values:
            if text:
                out.append('' if v is None else (_text_key(v) if isinstance(v, str) else None))
            elif v is None:
                out.append(0.0)
            elif isinstance(v, (int, float)):
                out.append(float(v))
            elif isinstance(v, (datetime, date)):
                out.append(_dax_serial(v if isinstance(v, datetime)
                                       else datetime(v.year, v.month, v.day)))
            else:
                out.append(None)
        return out

    def _fn_rankx(self, args_str: str, ctx: DAXContext) -> Any:
        """RANKX(table, expression, value, order, ties) — rank a value within a table's evaluated expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        rank_expr = args[1].strip()
        # Value to rank (optional — defaults to the expression evaluated in current context)
        value_expr = args[2].strip() if len(args) > 2 else None
        is_desc = self._rankx_order_desc(args[3] if len(args) > 3 else '', ctx)

        # Get the value to rank
        if value_expr:
            current_raw = self._eval_expr(value_expr, ctx)
        else:
            current_raw = self._eval_expr(rank_expr, ctx)

        # Evaluate expression for all rows in the table
        row_raw = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # _make_row_context handles bare-table (__row__) iterators
                    # too; the old direct __column__ lookup KeyError'd on them.
                    row_ctx = self._make_row_context(row_item, ctx, shadow=table_ref)
                    row_raw.append(self._eval_expr(rank_expr, row_ctx))

        keys = self._rankx_keys([current_raw] + row_raw)
        current_val = keys[0]
        if current_val is None:
            return None
        all_vals = [k for k in keys[1:] if k is not None]
        if not all_vals:
            return 1

        # DAX RANKX is COMPETITION ranking (ties='SKIP' by default): the rank is
        # one plus the number of values that beat it, so equal values share a rank
        # and the next distinct value skips ahead. It also ranks a value that is
        # not a MEMBER of the set, which is the normal case -- the value being
        # ranked is the expression evaluated in the CURRENT context, and here that
        # is the grand total, not any single row.
        #
        # The old code did dense ranking over the DISTINCT values and, when the
        # value was absent, returned len(unique) + 1 -- i.e. dead last. On
        # Agents_Performance that made every RANKX return 262 (261 rows + 1)
        # regardless of the ranking expression, where Desktop returns 136.
        #
        # The fifth argument selects between the two. It was PARSED BUT NEVER
        # READ, so `...,DESC,Dense)` still ranked by SKIP: IT_Support's
        # [2- Ranking Subjects] came out 6578 (10,808 subjects, 6,577 of them
        # above the ranked value) where Desktop answers 6 -- there are only 10
        # DISTINCT values and 5 of them beat it.
        ties = (args[4].strip().strip('"\'').upper() if len(args) > 4 else '') or 'SKIP'
        if ties == 'DENSE':
            beat = len({v for v in all_vals
                        if (v > current_val if is_desc else v < current_val)})
        elif is_desc:
            beat = sum(1 for v in all_vals if v > current_val)
        else:
            beat = sum(1 for v in all_vals if v < current_val)
        return beat + 1

    @staticmethod
    def _path_item_str(v) -> str:
        """A path element as Desktop prints it: integers without decimals."""
        if isinstance(v, float) and v.is_integer():
            return str(int(v))
        return str(v)

    def _fn_path(self, args_str: str, ctx: DAXContext) -> Any:
        """PATH(id_column, parent_column) -- pipe-delimited ancestor chain,
        root first. Evaluated per row; the id->parent mapping comes from the
        PRE-transition context (the row transition narrows the table to the
        current row, which would leave nothing to walk)."""
        parts = self._split_args(args_str)
        if len(parts) < 2:
            return None
        id_ref = parts[0].strip()
        par_ref = parts[1].strip()
        if '[' not in id_ref or '[' not in par_ref:
            return None
        tname = id_ref[:id_ref.index('[')].strip().strip("'")
        id_col = id_ref[id_ref.index('[') + 1:-1]
        par_col = par_ref[par_ref.index('[') + 1:-1]
        cur = self._eval_expr(id_ref, ctx)
        cur = self._resolve_row_result(cur, getattr(ctx, '_current_row', None),
                                       ctx)
        if cur is None or isinstance(cur, tuple):
            return None
        base = getattr(ctx, '_outer_ctx', None) or ctx
        rows = self._table_rows(f"'{tname}'" if ' ' in tname else tname, base)
        if rows is None:
            return None
        parent_of = {}
        for r in rows:
            if id_col in r:
                parent_of[r.get(id_col)] = r.get(par_col)
        chain = []
        node = cur
        for _ in range(len(parent_of) + 1):
            chain.append(node)
            nxt = parent_of.get(node)
            if nxt is None:
                break
            node = nxt
        else:
            return None                       # cycle guard
        return "|".join(self._path_item_str(v) for v in reversed(chain))

    def _fn_pathitemreverse(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHITEMREVERSE(path, position, type) -- item counted from the END
        of the pipe-delimited path (1-based)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        path = str(self._eval_expr(args[0].strip(), ctx) or '')
        pos = int(self._eval_expr(args[1].strip(), ctx) or 1)
        items = path.split('|') if path else []
        if 1 <= pos <= len(items):
            return items[len(items) - pos]
        return ''

    def _fn_pathcontains(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHCONTAINS(path, item) — check if pipe-delimited path contains item."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        path = str(self._eval_expr(args[0].strip(), ctx) or '')
        item = str(self._eval_expr(args[1].strip(), ctx) or '')
        return item in path.split('|')

    def _fn_pathitem(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHITEM(path, position, type) — get item at position in pipe-delimited path (1-based)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        path = str(self._eval_expr(args[0].strip(), ctx) or '')
        pos = int(self._eval_expr(args[1].strip(), ctx) or 1)
        parts = path.split('|')
        if 1 <= pos <= len(parts):
            return parts[pos - 1]
        return ''

    def _fn_pathlength(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHLENGTH(path) — count items in pipe-delimited path."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            path = str(val)
            if path:
                return len(path.split('|'))
        return 0

    # =========================================================================
    # Logical functions
    # =========================================================================

    def _fn_true(self, args_str: str, ctx: DAXContext) -> Any:
        """TRUE() — boolean true."""
        return True

    def _fn_false(self, args_str: str, ctx: DAXContext) -> Any:
        """FALSE() — boolean false."""
        return False

    def _eval_catching(self, expr: str, ctx: DAXContext):
        """(failed, value) of ``expr`` for IFERROR / ISERROR. Power BI Desktop
        2.152 (build_b134.py, issue #135):
          * a BLANK is no error: IFERROR(BLANK(), 5) is BLANK, ISERROR(BLANK())
            FALSE -- the engine read every BLANK as an error;
          * a division by zero is one here: IFERROR(1 / 0, 5) is 5;
          * so is a referenced measure that fails: IFERROR([Bad], 13) is 13
            (the engine degrades a nested measure's error to BLANK, so this
            counts the degradations instead);
          * a VARIABLE's error is not caught where the variable is used:
            VAR x = ERROR("boom") RETURN IFERROR(x, 9) fails (_VarEvalError)."""
        before = self._nested_errors
        try:
            value = self._eval_expr(expr.strip(), ctx)
        except _VarEvalError:
            raise
        except Exception as exc:
            if getattr(exc, "_pbix_deadline", False):
                raise
            return True, None
        if self._nested_errors != before:
            return True, None
        if isinstance(value, float) and (math.isinf(value) or math.isnan(value)):
            return True, None
        return False, value

    def _fn_iferror(self, args_str: str, ctx: DAXContext) -> Any:
        """IFERROR(expression, fallback) — fallback when expression is an ERROR
        (not when it is BLANK; see _eval_catching)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        failed, result = self._eval_catching(args[0], ctx)
        return self._eval_expr(args[1].strip(), ctx) if failed else result

    def _fn_coalesce(self, args_str: str, ctx: DAXContext) -> Any:
        """COALESCE(value1, value2, ...) — return first non-blank value."""
        args = self._split_args(args_str)
        for arg in args:
            val = self._eval_expr(arg.strip(), ctx)
            if val is not None and val != '':
                return val
        return None

    def _fn_contains(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINS(table, column, value, ...) — check if table contains a row with specified values."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return False
        table_name = args[0].strip().strip("'")
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return False

        # Parse column/value pairs
        criteria = []
        i = 1
        while i + 1 < len(args):
            ref = self._eval_expr(args[i].strip(), ctx)
            value = self._eval_expr(args[i + 1].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                col_idx = ctx._find_col_idx(tbl['columns'], ref[1])
                if col_idx >= 0:
                    criteria.append((col_idx, value))
            i += 2

        if not criteria:
            return False

        rows = ctx.get_filtered_rows(table_name)
        # Values match as DAX compares them: text case-insensitively, numbers
        # by value (str() missed 1 against 1.0 and "north" against "North").
        wanted = [(col_idx, _filter_value_keys(value)) for col_idx, value in criteria]
        for row in rows:
            if all(any(k in keys for k in _value_keys(row[col_idx], False))
                   for col_idx, keys in wanted):
                return True
        return False

    # =========================================================================
    # Information functions
    # =========================================================================

    def _fn_isnumber(self, args_str: str, ctx: DAXContext) -> Any:
        """ISNUMBER(value) — check if value is numeric."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, (int, float))

    def _fn_istext(self, args_str: str, ctx: DAXContext) -> Any:
        """ISTEXT(value) — check if value is text."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, str)

    def _fn_isnontext(self, args_str: str, ctx: DAXContext) -> Any:
        """ISNONTEXT(value) — check if value is not text."""
        val = self._eval_expr(args_str.strip(), ctx)
        return not isinstance(val, str)

    def _fn_islogical(self, args_str: str, ctx: DAXContext) -> Any:
        """ISLOGICAL(value) — check if value is boolean."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, bool)

    def _fn_iserror(self, args_str: str, ctx: DAXContext) -> Any:
        """ISERROR(value) — is the expression an ERROR (a BLANK is not; see
        _eval_catching)."""
        return self._eval_catching(args_str, ctx)[0]

    def _fn_username(self, args_str: str, ctx: DAXContext) -> Any:
        """USERNAME() — returns empty string (server-side function)."""
        return ''

    def _fn_userprincipalname(self, args_str: str, ctx: DAXContext) -> Any:
        """USERPRINCIPALNAME() — returns empty string (server-side function)."""
        return ''

    def _fn_lookupvalue(self, args_str: str, ctx: DAXContext) -> Any:
        """LOOKUPVALUE(result_column, search_column, search_value, ...) — vlookup equivalent."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return None

        result_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(result_ref, tuple) or len(result_ref) != 2:
            return None

        table_name, result_col = result_ref
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return None

        result_col_idx = ctx._find_col_idx(tbl['columns'], result_col)
        if result_col_idx < 0:
            return None

        # Build search criteria: pairs of (column_ref, value)
        criteria = []
        i = 1
        while i + 1 < len(args):
            search_ref = self._eval_expr(args[i].strip(), ctx)
            search_val = self._eval_expr(args[i + 1].strip(), ctx)
            if isinstance(search_ref, tuple) and len(search_ref) == 2:
                col_idx = ctx._find_col_idx(tbl['columns'], search_ref[1])
                if col_idx >= 0:
                    criteria.append((col_idx, search_val))
            i += 2

        if not criteria:
            return None

        # Search through all rows (ignoring filter context for lookup), through
        # the columns' typed index: text case-insensitively, numbers by value
        # (#107; str() missed 1 against 1.0 and "north" against "North").
        hit = None
        for col_idx, search_val in criteria:
            idx = ctx._indices_for_column_filter(tbl, col_idx, [search_val])
            hit = idx if hit is None else hit & idx
            if not hit:
                break
        if hit:
            return tbl['rows'][min(hit)][result_col_idx]

        # Return alternate value if provided
        if len(args) > 1 + len(criteria) * 2:
            return self._eval_expr(args[-1].strip(), ctx)
        return None

    # =========================================================================
    # Relationship functions
    # =========================================================================

    def _related_row_index(self, ctx: DAXContext, to_table: str,
                           to_col: str) -> dict:
        """{ key value -> related row as a column dict } for to_table[to_col],
        built once per model and memoized in the shared per-model cache
        (_filter_idx_cache) so an iterator does not rebuild it per row. First
        matching row wins, matching relationship key semantics (the one side
        is unique)."""
        cache = ctx._filter_idx_cache
        ck = ('__related_idx__', to_table, to_col)
        cached: Optional[dict] = cache.get(ck)
        if cached is not None:
            return cached
        idx: dict = {}
        ttbl = ctx.tables.get(to_table)
        if ttbl:
            cols = ttbl['columns']
            ti = ctx._find_col_idx(cols, to_col)
            if ti >= 0:
                for r in ttbl['rows']:
                    if len(r) > ti and r[ti] not in idx:
                        mrow = {'__table__': to_table, '__row__': True}
                        for i, c in enumerate(cols):
                            if i < len(r):
                                mrow[c] = r[i]
                        idx[r[ti]] = mrow
        cache[ck] = idx
        return idx

    def _related_lookup(self, cur: dict, target_table: str,
                        ctx: DAXContext, _depth: int = 0):
        """Navigate from the current (many-side) row to its single related row
        in target_table, following active From->To relationship edges (the FK
        on the many side matches the key on the one side). Multi-hop capable.
        Returns the target row as a column dict, or None."""
        if _depth > 16 or not isinstance(cur, dict):
            return None
        src_table = cur.get('__table__')
        if src_table == target_table:
            return cur
        for rel in ctx.relationships:
            if rel.get('FromTable') != src_table:
                continue
            if not rel.get('IsActive', 1):
                continue          # RELATED follows only active relationships
            from_col = rel.get('FromColumn')
            to_table = rel.get('ToTable')
            to_col = rel.get('ToColumn')
            if from_col not in cur:
                continue          # this row dict does not carry the FK
            fk = cur[from_col]
            # O(1) lookup via a per-model FK index (built once, shared across
            # every row context through _filter_idx_cache) -- a linear scan
            # here made SUMX(fact, RELATED(...)) O(fact x dim) on 200k-row
            # models.
            idx = self._related_row_index(ctx, to_table, to_col)
            mrow = idx.get(fk)
            if mrow is None:
                continue          # no related row (orphan FK) -> BLANK
            if to_table == target_table:
                return mrow
            deeper = self._related_lookup(mrow, target_table, ctx, _depth + 1)
            if deeper is not None:
                return deeper
        return None

    def _fn_related(self, args_str: str, ctx: DAXContext) -> Any:
        """RELATED(column) -- from the CURRENT ROW on the many side, follow the
        relationship to the one side and return the column's value. This is a
        row-context navigation (the FK of the iterated row selects the related
        row), NOT a filter-context lookup -- so it must use ctx._current_row,
        never 'the first visible row of the related table' (which returns the
        same value for every iterated row)."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if not isinstance(ref, tuple) or len(ref) != 2:
            return None
        target_table, target_col = ref
        tbl = ctx.tables.get(target_table)
        if not tbl:
            return None
        target_col_idx = ctx._find_col_idx(tbl['columns'], target_col)
        if target_col_idx < 0:
            return None
        cur = getattr(ctx, '_current_row', None)
        if isinstance(cur, dict) and cur.get('__table__'):
            related = self._related_lookup(cur, target_table, ctx)
            if related is not None:
                return related.get(target_col)
            # A row context exists but no related row was reachable: the FK is
            # orphaned or the current row already IS the target table with the
            # column present. Fall through only for the same-table case.
            if cur.get('__table__') == target_table and target_col in cur:
                return cur[target_col]
            return None
        # No row context (degenerate RELATED call): the previous
        # filter-context behavior, so a single-visible-row context still
        # resolves rather than crashing.
        rows = ctx.get_filtered_rows(target_table)
        if rows:
            return rows[0][target_col_idx]
        return None

    def _fn_relatedtable(self, args_str: str, ctx: DAXContext) -> Any:
        """RELATEDTABLE(table) — follow relationship to get filtered related table rows."""
        table_name = args_str.strip().strip("'")
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return []
        rows = ctx.get_filtered_rows(table_name)
        cols = tbl['columns']
        result = []
        for row in rows:
            row_dict = {'__table__': table_name, '__column__': cols[0] if cols else '', '__value__': row[0] if row else None}
            for i, col in enumerate(cols):
                row_dict[col] = row[i]
            result.append(row_dict)
        return result

    def _fn_crossfilter(self, args_str: str, ctx: DAXContext) -> Any:
        """CROSSFILTER(column1, column2, direction) — modify cross-filter direction.
        Returns a marker for CALCULATE to process."""
        # NOTE: Full implementation would modify relationship filter propagation direction.
        return ('__CROSSFILTER__', args_str.strip())

    # =========================================================================
    # Time Intelligence functions — helpers
    # =========================================================================

    def _parse_date(self, val) -> Optional[datetime]:
        """Try to parse a value as a date."""
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime(val.year, val.month, val.day)
        if isinstance(val, str):
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S'):
                try:
                    return datetime.strptime(val, fmt)
                except ValueError:
                    continue
        if isinstance(val, (int, float)):
            # Excel serial date: days since 1899-12-30
            try:
                return datetime(1899, 12, 30) + timedelta(days=int(val))
            except (ValueError, OverflowError):
                pass
        return None

    def _get_date_column_dates(self, args_str: str, ctx: DAXContext) -> tuple:
        """Parse a date column reference and return (table_name, col_name, list_of_dates).
        Returns (table_name, col_name, dates) where dates are datetime objects."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            table_name, col_name = ref
            raw_values = ctx.get_column_data(table_name, col_name)
            dates = []
            for v in raw_values:
                d = self._parse_date(v)
                if d:
                    dates.append(d)
            return table_name, col_name, dates
        return None, None, []

    def _make_date_table_result(self, table_name: str, col_name: str, dates: list) -> list:
        """Convert a list of datetime objects into the standard row-dict format."""
        result = []
        for d in dates:
            result.append({
                '__table__': table_name,
                '__column__': col_name,
                '__value__': d.strftime('%Y-%m-%d')
            })
        return result

    def _get_all_date_table_dates(self, table_name: str, col_name: str, ctx: DAXContext) -> list:
        """Get ALL dates from the date table (ignoring filter context)."""
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return []
        col_idx = ctx._find_col_idx(tbl['columns'], col_name)
        if col_idx < 0:
            return []
        dates = []
        for row in tbl['rows']:
            d = self._parse_date(row[col_idx])
            if d:
                dates.append(d)
        return dates

    # =========================================================================
    # Time Intelligence functions — Date Sets
    # =========================================================================

    def _fn_datesytd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESYTD(dates, yearEndDate) — year to date dates."""
        args = self._split_args(args_str)
        table_name, col_name, dates = self._get_date_column_dates(args[0].strip(), ctx)
        if not dates:
            return []
        # Year end date (default Dec 31)
        year_end_month = 12
        year_end_day = 31
        if len(args) > 1:
            ye = self._eval_expr(args[1].strip(), ctx)
            if isinstance(ye, str):
                try:
                    parts = ye.split('/')
                    if len(parts) == 2:
                        year_end_month = int(parts[0])
                        year_end_day = int(parts[1])
                except ValueError:
                    pass

        max_date = max(dates)
        # YTD: from start of fiscal year to max_date
        if year_end_month == 12 and year_end_day == 31:
            year_start = datetime(max_date.year, 1, 1)
        else:
            # Fiscal year
            if max_date.month > year_end_month or (max_date.month == year_end_month and max_date.day > year_end_day):
                year_start = datetime(max_date.year, year_end_month, year_end_day) + timedelta(days=1)
            else:
                year_start = datetime(max_date.year - 1, year_end_month, year_end_day) + timedelta(days=1)

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        ytd = [d for d in all_dates if year_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, ytd)

    def _fn_datesmtd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESMTD(dates) — month to date dates."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        month_start = datetime(max_date.year, max_date.month, 1)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        mtd = [d for d in all_dates if month_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, mtd)

    def _fn_datesqtd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESQTD(dates) — quarter to date dates."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        quarter_start_month = ((max_date.month - 1) // 3) * 3 + 1
        quarter_start = datetime(max_date.year, quarter_start_month, 1)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        qtd = [d for d in all_dates if quarter_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, qtd)

    def _total_over_dates(self, expr: str, dates: Any, ctx: DAXContext) -> Any:
        """Evaluate ``expr`` with ``dates`` (a DATESYTD/QTD/MTD result) as the
        filter on its date column.

        TOTALYTD(e, d) is CALCULATE(e, DATESYTD(d)) (Microsoft's DAX
        reference), so the dates REPLACE every existing filter on the date
        table, exactly as the CALCULATE branch does for a time-intelligence
        filter argument. Merging them instead (ctx.with_filters) kept a
        Year/Month/Period filter alive, and TOTALYTD under a Year x Period
        axis returned the period's own value: on Corporate Spend
        (``[Amount] := TOTALYTD(SUM([Value]), 'Date'[Date]) * .3``, 'Date'
        marked as a date table) Desktop 2.157 shows 1,042,148,723.37 for
        2014 / Period 10 and this returned 109,053,968.15, the October value.
        Verified against Desktop: TOTALYTD and TOTALQTD now equal
        CALCULATE(e, DATESYTD/DATESQTD(d)) in every cell grouped by year,
        year x period and a non-date column x year.
        """
        if dates and isinstance(dates, list) and isinstance(dates[0], dict) and '__table__' in dates[0]:
            tbl_name, col_name = dates[0]['__table__'], dates[0]['__column__']
            if (tbl_name, col_name) not in self._clearing_date_columns(ctx):
                # Not a marked date column nor a DateTime relationship column:
                # Power BI intersects the dates with the table's filters (#78).
                return self._eval_expr(expr, ctx.with_filters(
                    {f"{tbl_name}.{col_name}": [item['__value__'] for item in dates]}))
            # Derived from ctx, so ALL snapshots and the grouping survive (#87).
            new_ctx = ctx.without_filters(
                [k for k in ctx.filter_context if k.startswith(f"{tbl_name}.")]
            ).with_filters({f"{tbl_name}.{col_name}": [item['__value__'] for item in dates]})
            return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    def _fn_totalytd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALYTD(expression, dates, filter, yearEndDate) — year to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        ytd_dates = self._fn_datesytd(', '.join(args[1:]), ctx)
        return self._total_over_dates(expr, ytd_dates, ctx)

    def _fn_totalmtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALMTD(expression, dates) — month to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        return self._total_over_dates(args[0].strip(), self._fn_datesmtd(args[1].strip(), ctx), ctx)

    def _fn_totalqtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALQTD(expression, dates) — quarter to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        return self._total_over_dates(args[0].strip(), self._fn_datesqtd(args[1].strip(), ctx), ctx)

    # =========================================================================
    # Time Intelligence — Period Navigation
    # =========================================================================

    def _fn_previousmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSMONTH(dates) — dates from the previous month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        # DAX anchors PREVIOUS* on the FIRST date of the input, not the
        # last (NEXT* uses the last). Confirmed against Desktop on a
        # Calendar spanning 2019-01-01..06-30: PREVIOUSQUARTER is BLANK
        # there, which is only possible from the first date (Q4-2018,
        # outside the table) -- anchored on the last it would have
        # returned Q1-2019, which exists. Using max() made [Net Sales PM]
        # and friends return numbers where Desktop returns BLANK.
        max_date = min(dates)
        # Previous month
        if max_date.month == 1:
            prev_year, prev_month = max_date.year - 1, 12
        else:
            prev_year, prev_month = max_date.year, max_date.month - 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if d.year == prev_year and d.month == prev_month]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_previousquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSQUARTER(dates) — dates from the previous quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        # DAX anchors PREVIOUS* on the FIRST date of the input, not the
        # last (NEXT* uses the last). Confirmed against Desktop on a
        # Calendar spanning 2019-01-01..06-30: PREVIOUSQUARTER is BLANK
        # there, which is only possible from the first date (Q4-2018,
        # outside the table) -- anchored on the last it would have
        # returned Q1-2019, which exists. Using max() made [Net Sales PM]
        # and friends return numbers where Desktop returns BLANK.
        max_date = min(dates)
        current_q = (max_date.month - 1) // 3 + 1
        if current_q == 1:
            prev_q_start = datetime(max_date.year - 1, 10, 1)
            prev_q_end = datetime(max_date.year - 1, 12, 31)
        else:
            prev_q_start_month = (current_q - 2) * 3 + 1
            prev_q_end_month = (current_q - 1) * 3
            prev_q_start = datetime(max_date.year, prev_q_start_month, 1)
            _, last_day = monthrange(max_date.year, prev_q_end_month)
            prev_q_end = datetime(max_date.year, prev_q_end_month, last_day)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if prev_q_start <= d <= prev_q_end]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_previousyear(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSYEAR(dates) — dates from the previous year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        # DAX anchors PREVIOUS* on the FIRST date of the input, not the
        # last (NEXT* uses the last). Confirmed against Desktop on a
        # Calendar spanning 2019-01-01..06-30: PREVIOUSQUARTER is BLANK
        # there, which is only possible from the first date (Q4-2018,
        # outside the table) -- anchored on the last it would have
        # returned Q1-2019, which exists. Using max() made [Net Sales PM]
        # and friends return numbers where Desktop returns BLANK.
        max_date = min(dates)
        prev_year = max_date.year - 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if d.year == prev_year]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_nextmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTMONTH(dates) — dates from the next month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        if max_date.month == 12:
            next_year, next_month = max_date.year + 1, 1
        else:
            next_year, next_month = max_date.year, max_date.month + 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if d.year == next_year and d.month == next_month]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_nextquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTQUARTER(dates) — dates from the next quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        current_q = (max_date.month - 1) // 3 + 1
        if current_q == 4:
            nq_start = datetime(max_date.year + 1, 1, 1)
            nq_end = datetime(max_date.year + 1, 3, 31)
        else:
            nq_start_month = current_q * 3 + 1
            nq_end_month = (current_q + 1) * 3
            nq_start = datetime(max_date.year, nq_start_month, 1)
            _, last_day = monthrange(max_date.year, nq_end_month)
            nq_end = datetime(max_date.year, nq_end_month, last_day)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if nq_start <= d <= nq_end]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_nextyear(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTYEAR(dates) — dates from the next year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        next_year = max_date.year + 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if d.year == next_year]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_parallelperiod(self, args_str: str, ctx: DAXContext) -> Any:
        """PARALLELPERIOD(dates, offset, interval) — shift dates by offset intervals."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_name, col_name, dates = self._get_date_column_dates(args[0].strip(), ctx)
        if not dates:
            return []
        offset = self._eval_expr(args[1].strip(), ctx)
        interval = args[2].strip().upper()
        if not isinstance(offset, (int, float)):
            return []
        offset = int(offset)

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        min_date = min(dates)
        max_date = max(dates)

        if interval in ('YEAR', 'YEARS'):
            shifted = [d for d in all_dates
                       if datetime(min_date.year + offset, min_date.month, 1) <= d <=
                          datetime(max_date.year + offset, max_date.month,
                                   monthrange(max_date.year + offset, max_date.month)[1])]
        elif interval in ('QUARTER', 'QUARTERS'):
            def shift_quarter(d, off):
                new_month = d.month + off * 3
                new_year = d.year + (new_month - 1) // 12
                new_month = ((new_month - 1) % 12) + 1
                return datetime(new_year, new_month, 1)
            # Span the shifted MIN quarter through the shifted MAX quarter. This
            # used to shift only min_date's quarter and return that ONE quarter,
            # so a multi-quarter selection collapsed: over a three-year range the
            # single shifted quarter fell outside the date table, the result was
            # empty, no filter was applied at all, and PQC Total Sales returned
            # the GRAND TOTAL instead of the previous quarter's.
            q_start = shift_quarter(
                datetime(min_date.year, ((min_date.month - 1) // 3) * 3 + 1, 1), offset)
            q_last = shift_quarter(
                datetime(max_date.year, ((max_date.month - 1) // 3) * 3 + 1, 1), offset)
            q_end_month, q_end_year = q_last.month + 2, q_last.year
            if q_end_month > 12:
                q_end_month -= 12
                q_end_year += 1
            _, last_day = monthrange(q_end_year, q_end_month)
            q_end = datetime(q_end_year, q_end_month, last_day)
            shifted = [d for d in all_dates if q_start <= d <= q_end]
        elif interval in ('MONTH', 'MONTHS'):
            def shift_month(d, off):
                new_month = d.month + off
                new_year = d.year + (new_month - 1) // 12
                new_month = ((new_month - 1) % 12) + 1
                return new_year, new_month
            sy, sm = shift_month(min_date, offset)
            ey, em = shift_month(max_date, offset)
            start = datetime(sy, sm, 1)
            _, last_day = monthrange(ey, em)
            end = datetime(ey, em, last_day)
            shifted = [d for d in all_dates if start <= d <= end]
        elif interval in ('DAY', 'DAYS'):
            delta = timedelta(days=offset)
            start = min_date + delta
            end = max_date + delta
            shifted = [d for d in all_dates if start <= d <= end]
        else:
            shifted = []

        return self._make_date_table_result(table_name, col_name, shifted)

    # =========================================================================
    # Time Intelligence — Start/End of Period
    # =========================================================================

    # Every time-intelligence function returns dates that EXIST in the dates
    # column (Microsoft's DAX reference: the result is a table of the column's
    # values). Building calendar dates instead -- the 31st of a month, Dec 31 --
    # broke every date table that is not daily: Desktop 2.157, on Microsoft's
    # MIT Corporate Spend sample (one row per month, dated the 1st), returns the
    # 2014-12-01 row for ENDOFYEAR / ENDOFMONTH, where the calendar end
    # 2014-12-31 matched no row and the measure went BLANK at every level.

    @staticmethod
    def _period_bounds(d: datetime, period: str) -> tuple:
        """(first, last) calendar day of the month / quarter / year containing d."""
        if period == 'MONTH':
            first_month, last_month = d.month, d.month
        elif period == 'QUARTER':
            first_month = ((d.month - 1) // 3) * 3 + 1
            last_month = first_month + 2
        else:
            first_month, last_month = 1, 12
        return (datetime(d.year, first_month, 1),
                datetime(d.year, last_month, monthrange(d.year, last_month)[1], 23, 59, 59))

    def _start_of(self, args_str: str, ctx: DAXContext, period: str) -> Any:
        """STARTOFMONTH/QUARTER/YEAR: the first date of the dates column in the
        period that contains the earliest date in context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        lo, hi = self._period_bounds(min(dates), period)
        inside = [d for d in self._get_all_date_table_dates(table_name, col_name, ctx) if lo <= d <= hi]
        return self._make_date_table_result(table_name, col_name, [min(inside)] if inside else [])

    def _end_of(self, args_str: str, ctx: DAXContext, period: str) -> Any:
        """ENDOFMONTH/QUARTER/YEAR: the last date of the dates column in the
        period that contains the latest date in context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        lo, hi = self._period_bounds(max(dates), period)
        inside = [d for d in self._get_all_date_table_dates(table_name, col_name, ctx) if lo <= d <= hi]
        return self._make_date_table_result(table_name, col_name, [max(inside)] if inside else [])

    def _fn_startofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFMONTH(dates) — first date of the month."""
        return self._start_of(args_str, ctx, 'MONTH')

    def _fn_endofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFMONTH(dates) — last date of the month."""
        return self._end_of(args_str, ctx, 'MONTH')

    def _fn_startofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFQUARTER(dates) — first date of the quarter."""
        return self._start_of(args_str, ctx, 'QUARTER')

    def _fn_endofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFQUARTER(dates) — last date of the quarter."""
        return self._end_of(args_str, ctx, 'QUARTER')

    def _fn_startofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFYEAR(dates) — first date of the year."""
        return self._start_of(args_str, ctx, 'YEAR')

    def _fn_endofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFYEAR(dates) — last date of the year."""
        return self._end_of(args_str, ctx, 'YEAR')

    # =========================================================================
    # Time Intelligence — Opening/Closing Balance
    # =========================================================================
    # CLOSINGBALANCEx(e, d) evaluates e at ENDOFx(d); OPENINGBALANCEx(e, d) at
    # the last date of the column BEFORE the period (BLANK when there is none).
    # Both REPLACE the date table's filters, as CALCULATE with a date filter
    # does: Desktop's OPENINGBALANCEMONTH for 2014 / Period 3 is Period 2's
    # value, which a kept Period filter would have made BLANK.

    def _balance_at(self, args_str: str, ctx: DAXContext, period: str, opening: bool) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        if opening:
            lo, _hi = self._period_bounds(min(dates), period)
            before = [d for d in self._get_all_date_table_dates(table_name, col_name, ctx) if d < lo]
            if not before:
                return None
            at = self._make_date_table_result(table_name, col_name, [max(before)])
        else:
            at = self._end_of(args[1], ctx, period)
            if not at:
                return None
        return self._total_over_dates(expr, at, ctx)

    def _fn_openingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEMONTH(expression, dates, filter) — at the last date before the month."""
        return self._balance_at(args_str, ctx, 'MONTH', opening=True)

    def _fn_closingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEMONTH(expression, dates, filter) — at the last date of the month."""
        return self._balance_at(args_str, ctx, 'MONTH', opening=False)

    def _fn_openingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEQUARTER(expression, dates, filter) — at the last date before the quarter."""
        return self._balance_at(args_str, ctx, 'QUARTER', opening=True)

    def _fn_closingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEQUARTER(expression, dates, filter) — at the last date of the quarter."""
        return self._balance_at(args_str, ctx, 'QUARTER', opening=False)

    def _fn_openingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEYEAR(expression, dates, filter) — at the last date before the year."""
        return self._balance_at(args_str, ctx, 'YEAR', opening=True)

    def _fn_closingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEYEAR(expression, dates, filter) — at the last date of the year."""
        return self._balance_at(args_str, ctx, 'YEAR', opening=False)

    # =========================================================================
    # Time Intelligence — Date Range Functions
    # =========================================================================

    def _fn_firstdate(self, args_str: str, ctx: DAXContext) -> Any:
        """FIRSTDATE(dates) — earliest date in filter context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        earliest = min(dates)
        return self._make_date_table_result(table_name, col_name, [earliest])

    def _fn_lastdate(self, args_str: str, ctx: DAXContext) -> Any:
        """LASTDATE(dates) — latest date in filter context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        latest = max(dates)
        return self._make_date_table_result(table_name, col_name, [latest])

    def _fn_datesbetween(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESBETWEEN(dates, start, end) — dates between start and end."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_name, col_name, _ = self._get_date_column_dates(args[0].strip(), ctx)
        if not table_name:
            return []
        start_val = self._eval_expr(args[1].strip(), ctx)
        end_val = self._eval_expr(args[2].strip(), ctx)
        start_date = self._parse_date(start_val)
        end_date = self._parse_date(end_val)
        if not start_date or not end_date:
            return []
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        between = [d for d in all_dates if start_date <= d <= end_date]
        return self._make_date_table_result(table_name, col_name, between)

    def _fn_datesinperiod(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESINPERIOD(dates, start, offset, interval) — dates in a period from start."""
        args = self._split_args(args_str)
        if len(args) < 4:
            return []
        table_name, col_name, _ = self._get_date_column_dates(args[0].strip(), ctx)
        if not table_name:
            return []
        start_val = self._eval_expr(args[1].strip(), ctx)
        offset = self._eval_expr(args[2].strip(), ctx)
        interval = args[3].strip().upper()
        start_date = self._parse_date(start_val)
        if not start_date or not isinstance(offset, (int, float)):
            return []
        offset = int(offset)

        if interval in ('DAY', 'DAYS'):
            if offset >= 0:
                end_date = start_date + timedelta(days=offset - 1)
            else:
                end_date = start_date
                start_date = start_date + timedelta(days=offset + 1)
        elif interval in ('MONTH', 'MONTHS', 'QUARTER', 'QUARTERS', 'YEAR', 'YEARS'):
            # The far end is the start shifted as DATEADD shifts it: clamped to
            # the target month, and a month END to a month END (issue #122).
            # The YEAR branch built datetime(year + offset, month, day), which
            # raised on 29 February and blanked the measure; MONTH and QUARTER
            # clamped but sent 30 April to 30 March. Power BI Desktop 2.152:
            # one YEAR back from 2024-02-29 is 2023-03-01..2024-02-29 (366
            # days), from 2025-02-28 it is 2024-03-01..2025-02-28 (365), and
            # one YEAR forward from 2024-02-29 ends 2025-02-27.
            other = self._shift_date(start_date, offset, interval.rstrip('S'))
            if offset >= 0:
                end_date = other
            else:
                end_date = start_date
                start_date = other
        else:
            return []

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        # The far boundary is EXCLUSIVE: DATESINPERIOD(..., -1, YEAR) from
        # 2009-12-31 is 2009-01-01..2009-12-31, a 365-day window, NOT 366 starting
        # at 2008-12-31. The DAY branch already applied this (offset +/- 1); the
        # MONTH/QUARTER/YEAR branches did not, so each returned one day too many
        # and MAT Total Sales came out 745,963 above Desktop.
        if interval not in ('DAY', 'DAYS'):
            if offset >= 0:
                end_date = end_date - timedelta(days=1)
            else:
                start_date = start_date + timedelta(days=1)
            if start_date > end_date:
                return []

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        in_period = [d for d in all_dates if start_date <= d <= end_date]
        return self._make_date_table_result(table_name, col_name, in_period)

    def _fn_calendar(self, args_str: str, ctx: DAXContext) -> Any:
        """CALENDAR(start, end) — generate a date table between start and end."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        start_val = self._eval_expr(args[0].strip(), ctx)
        end_val = self._eval_expr(args[1].strip(), ctx)
        start_date = self._parse_date(start_val)
        end_date = self._parse_date(end_val)
        if not start_date or not end_date:
            return []
        result = []
        current = start_date
        while current <= end_date:
            result.append({
                '__table__': '__calendar__',
                '__column__': 'Date',
                '__value__': current.strftime('%Y-%m-%d')
            })
            current += timedelta(days=1)
        return result

    def _fn_calendarauto(self, args_str: str, ctx: DAXContext) -> Any:
        """CALENDARAUTO() — generate a date table spanning all dates in the model.
        Scans all date-like columns to find min/max range."""
        min_date = None
        max_date = None
        for tbl_name, tbl in ctx.tables.items():
            for i, col in enumerate(tbl['columns']):
                for row in tbl['rows']:
                    d = self._parse_date(row[i])
                    if d:
                        if min_date is None or d < min_date:
                            min_date = d
                        if max_date is None or d > max_date:
                            max_date = d
                        break  # Just check first row of each column for perf
        if not min_date or not max_date:
            return []
        # Extend to full calendar years
        start = datetime(min_date.year, 1, 1)
        end = datetime(max_date.year, 12, 31)
        result = []
        current = start
        while current <= end:
            result.append({
                '__table__': '__calendar__',
                '__column__': 'Date',
                '__value__': current.strftime('%Y-%m-%d')
            })
            current += timedelta(days=1)
        return result


# =========================================================================
# API for the backend
# =========================================================================

class _PerThreadEngine:
    """The module's engine: one DAXEngine per THREAD (PR #97).

    A DAXEngine carries per-evaluation state -- the deadline and depth, the
    ALLSELECTED snapshots, the unsupported functions, the errors -- that the
    tools read back after each call. One shared instance let two evaluations
    on different threads overwrite each other's: under 8 threads, 264 of 336
    pbix_evaluate_dax answers differed from the same calls made one at a time
    (an ALLSELECTED share of 1.0001 for 1.0), so a host had to serialise every
    call. Each thread now gets its own engine on first use; the module-level
    caches they share lock their writes (_CACHE_LOCK)."""

    __slots__ = ('_local',)

    def __init__(self) -> None:
        object.__setattr__(self, '_local', threading.local())

    def _get(self) -> 'DAXEngine':
        eng = getattr(self._local, 'engine', None)
        if eng is None:
            eng = self._local.engine = DAXEngine()
        return eng

    def __getattr__(self, name: str) -> Any:
        return getattr(self._get(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(self._get(), name, value)


_engine: DAXEngine = _PerThreadEngine()  # type: ignore[assignment]


def evaluate_measure(measure_name: str, tables: dict, measures: dict,
                     filter_context: dict | None = None,
                     date_table: str | None = None, date_column: str | None = None,
                     relationships: list | None = None,
                     calculation_groups: list | None = None,
                     measure_formats: dict | None = None) -> Any:
    """Evaluate a single DAX measure. ``calculation_groups`` (and
    ``measure_formats``), when given, are attached to ``measures`` as
    set_calculation_groups does."""
    if calculation_groups is not None or measure_formats is not None:
        set_calculation_groups(measures, calculation_groups, measure_formats)
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    return _engine.evaluate_measure(measure_name, ctx)


def evaluate_measures_batch(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict | None = None,
                            date_table: str | None = None, date_column: str | None = None,
                            relationships: list | None = None,
                            group_keys: set | None = None,
                            selected_filters: dict | None = None,
                            culture: str | None = None,
                            date_tables: dict | None = None,
                            measure_tables: dict | None = None,
                            model_columns: dict | None = None,
                            calculation_groups: list | None = None,
                            measure_formats: dict | None = None) -> dict:
    """Evaluate multiple measures, returning { name: value }.

    ``calculation_groups`` is ModelReader.calculation_groups and
    ``measure_formats`` ModelReader.measure_format_strings (issue #121). When
    given they are attached to ``measures`` (set_calculation_groups), so this
    and every later evaluation over that dict applies the selected
    calculation items; omitted, whatever is attached already stays.

    ``measure_tables`` (each measure's home table) and ``model_columns`` are what
    resolve a bare ``[Column]`` in a measure, as in evaluate_measures_smart;
    without them ``AVERAGE([ProductRevenue])`` evaluated to BLANK here.

    ``group_keys`` names the filter_context keys that came from GROUPED
    evaluation (the visual's row grouping) rather than the caller's slicer
    selection, and ``selected_filters`` is that slicer selection itself (the
    base filter_context BEFORE the group key was merged in) — together they
    are what ALLSELECTED restores (see DAXContext.group_keys).
    """
    if calculation_groups is not None or measure_formats is not None:
        set_calculation_groups(measures, calculation_groups, measure_formats)
    if group_keys:
        # The grouping values carry the ISINSCOPE tag, as in
        # evaluate_measures_smart's group_by (issue #79).
        filter_context = {k: (_tag_group_by(v) if k in group_keys else v)
                          for k, v in (filter_context or {}).items()}
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    # Marked date tables (issue #79): the grouped tool applies the same
    # date-table rule as pbix_evaluate_dax.
    ctx.date_tables = date_tables or {}
    if group_keys:
        ctx.group_keys = set(group_keys)
    if selected_filters is not None:
        ctx.selected_filters = dict(selected_filters)
    ctx.culture = culture
    ctx.measure_tables = measure_tables or {}
    ctx.model_columns = model_columns or {}
    results = {}
    for name in measure_names:
        results[name] = _engine.evaluate_measure(name, ctx)
    return results


def evaluate_format_strings(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict | None = None,
                            date_table: str | None = None, date_column: str | None = None,
                            relationships: list | None = None,
                            group_keys: set | None = None,
                            selected_filters: dict | None = None,
                            culture: str | None = None,
                            date_tables: dict | None = None,
                            measure_tables: dict | None = None,
                            model_columns: dict | None = None) -> dict:
    """``{name: format string}`` for the measures whose format a selected
    calculation item decides, through its format-string expression (issue
    #121). A measure no such item applies to is left out: it keeps its own
    format string. Arguments as in evaluate_measures_batch."""
    if not _calc_registry(measures)[0]:
        return {}
    if group_keys:
        filter_context = {k: (_tag_group_by(v) if k in group_keys else v)
                          for k, v in (filter_context or {}).items()}
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    ctx.date_tables = date_tables or {}
    if group_keys:
        ctx.group_keys = set(group_keys)
    if selected_filters is not None:
        ctx.selected_filters = dict(selected_filters)
    ctx.culture = culture
    ctx.measure_tables = measure_tables or {}
    ctx.model_columns = model_columns or {}
    out = {}
    for name in measure_names:
        try:
            fmt = _engine.item_format_string(name, ctx)
        except Exception:      # noqa: BLE001 -- a broken expression formats nothing
            continue
        if fmt is not DAXEngine._NO_ITEM_FORMAT:
            out[name] = None if fmt is None else str(fmt)
    return out


# Simple single-column aggregations that can be bucketed once per fact table
# instead of re-scanning the fact table once per dimension value.
_SIMPLE_AGG_RE = re.compile(
    r"^(SUM|AVERAGE|MIN|MAX|COUNT|DISTINCTCOUNT)\s*\(\s*"
    r"(?:'([^']+)'|([A-Za-z0-9_ .\-]+?))\s*\[\s*([^\]]+?)\s*\]\s*\)$",
    re.IGNORECASE,
)
_COUNTROWS_RE = re.compile(
    r"^COUNTROWS\s*\(\s*(?:'([^']+)'|([A-Za-z0-9_ .\-]+?))\s*\)$",
    re.IGNORECASE,
)


def _parse_simple_agg(expr: str):
    """Parse a measure expression that is a single simple aggregation.

    Returns (func_upper, table_name, column_name_or_None) or None if the
    expression is anything more complex (arithmetic, CALCULATE, nested calls,
    iterators, multiple columns, …) — those are NOT eligible for bucketing.
    """
    if expr is None:
        return None
    e = expr.strip()
    m = _SIMPLE_AGG_RE.match(e)
    if m:
        func = m.group(1).upper()
        table = (m.group(2) or m.group(3) or "").strip()
        col = m.group(4).strip()
        return (func, table, col)
    m = _COUNTROWS_RE.match(e)
    if m:
        table = (m.group(1) or m.group(2) or "").strip()
        return ("COUNTROWS", table, None)
    return None


def evaluate_per_dimension(measure_names: list, tables: dict, measures: dict,
                           base_fc: Optional[dict], dimension: str, dim_table: str,
                           dim_col: str, unique_vals: list,
                           date_table: Optional[str] = None,
                           date_column: Optional[str] = None,
                           relationships: Optional[list] = None) -> dict:
    """Fast per-dimension evaluation for simple aggregation measures.

    Instead of re-filtering the whole fact table once per dimension value
    (O(values × fact_rows)), the fact rows are grouped by the propagated join
    key ONCE and each bucket is aggregated with the real engine (O(fact_rows +
    values)). The dimension->fact mapping reuses the engine's own relationship
    propagation (``_get_cross_table_filters`` / multi-hop ``_find_rel_path``),
    so results are identical to the per-value path.

    Returns ``{measure_name: {dim_value: result}}`` covering ONLY the measures
    it could safely bucket. Measures that are not simple aggregations, or whose
    join key maps a fact row to more than one dimension value (ambiguous), are
    omitted so the caller can evaluate them the slow-but-exact way.
    """
    # A selected calculation item applies to the measure in its own filter
    # context, which the bucketed evaluation below (an empty context over
    # pre-filtered rows) does not carry: leave such evaluations to the
    # per-value path (issue #121).
    _cg_tables = {g.key for g in _calc_registry(measures)[0]}
    if _cg_tables and (dim_table.lower() in _cg_tables or any(
            k.split('.', 1)[0].lower() in _cg_tables for k in (base_fc or {}))):
        return {}

    # Which requested measures are eligible? name -> (func, table, col)
    specs = {}
    for name in measure_names:
        parsed = _parse_simple_agg(measures.get(name, ""))
        if parsed is None:
            continue
        _func, tname, _cname = parsed
        if tname not in tables:
            continue
        specs[name] = tname
    if not specs:
        return {}

    # Buckets and results are keyed by the dimension value object, so values
    # that are == and hash-equal must not appear twice (1 vs 1.0 vs True would
    # collapse into one key and cross-contaminate). The production caller already
    # dedups via set(); collapse here too so the function is self-consistent for
    # any caller (keeps first occurrence, matching set()/str() comparison).
    unique_vals = list(dict.fromkeys(unique_vals))

    # base filter context WITHOUT the iterated dimension (the per-value path
    # overwrites fc[dimension] per value, so the dimension must not be part of
    # the constant base filter).
    base_fc_no_dim = {k: v for k, v in (base_fc or {}).items() if k != dimension}

    # group eligible measures by their fact table (shared bucketing)
    by_table: dict = {}
    for name, tname in specs.items():
        by_table.setdefault(tname, []).append(name)

    col_helper = DAXContext(tables, measures, date_table, date_column, {}, relationships)
    fast: dict = {}

    for ftbl_name, ms in by_table.items():
        ftbl = tables[ftbl_name]

        # --- filter the fact table by the base context ONCE ---
        base_ctx = DAXContext(tables, measures, date_table, date_column,
                              base_fc_no_dim, relationships)
        base_rows = base_ctx.get_filtered_rows(ftbl_name)

        # --- bucket base-filtered rows by dimension value ---
        # buckets are keyed by str(val) for the direct case (dimension column on
        # the aggregated table) and by the value object for the cross-table
        # case; `key_is_str` records which so the aggregation loop resolves the
        # right key.
        buckets: dict = {}
        if ftbl_name == dim_table:
            dcol_idx = col_helper._find_col_idx(ftbl["columns"], dim_col)
            if dcol_idx < 0:
                continue
            wanted = set(str(v) for v in unique_vals)
            for row in base_rows:
                key = str(row[dcol_idx])
                if key in wanted:
                    buckets.setdefault(key, []).append(row)
            key_is_str = True
        else:
            # The cross-table bucketing applies base_fc (via base_rows) and the
            # dimension filter (via key_to_value) INDEPENDENTLY, then intersects
            # at the fact-key level. That is only valid when no base filter sits
            # on the dimension's own join path: a base filter on the dimension
            # table (or an intermediate on the path to the fact) must be combined
            # CONJUNCTIVELY per dim ROW — which this fast path does not do, and
            # which misattributes rows when the join key is non-unique/NULL. So
            # fall back (skip → caller evaluates per-value) whenever a base filter
            # targets a table on that path. Base filters on the fact itself or on
            # an unrelated dimension are orthogonal and stay on the fast path.
            path_tables = {dim_table}
            _p = col_helper._find_rel_path(dim_table, ftbl_name)
            if _p:
                for _hop in _p:
                    path_tables.add(_hop[0])
                    path_tables.add(_hop[1])
            path_tables.discard(ftbl_name)
            if any(k.split(".", 1)[0] in path_tables for k in base_fc_no_dim):
                continue
            # cross-table: reuse the engine's propagation to map each fact join
            # key to a single dimension value (fall back on ambiguity).
            fact_col_idx = None
            key_to_value: dict = {}
            date_part = False
            ok = True
            for val in unique_vals:
                probe = DAXContext(tables, measures, date_table, date_column,
                                   {dimension: [val]}, relationships)
                cross = probe._get_cross_table_filters(ftbl_name)
                if len(cross) != 1:
                    ok = False
                    break
                allowed, idx = cross[0]
                # Across a DatePartOnly join (auto date/time) the keys are
                # date parts, and the fact rows bucket by theirs (issue #126).
                date_part = date_part or isinstance(allowed, _DatePartKeys)
                if fact_col_idx is None:
                    fact_col_idx = idx
                elif fact_col_idx != idx:
                    ok = False
                    break
                for k in allowed:
                    if k in key_to_value and key_to_value[k] != val:
                        ok = False  # one fact key maps to two dimension values
                        break
                    key_to_value[k] = val
                if not ok:
                    break
            if not ok or fact_col_idx is None:
                continue
            for row in base_rows:
                cell = row[fact_col_idx]
                if date_part:
                    k = (_date_part_key(cell) or str(cell)) if cell is not None else 'None'
                else:
                    k = str(cell)
                if k in key_to_value:
                    # BLANK is a group of its own: the rows that reach the
                    # dimension's blank row (issue #82)
                    buckets.setdefault(key_to_value[k], []).append(row)
            key_is_str = False

        # --- aggregate each bucket with the real engine (empty filter) ---
        for val in unique_vals:
            bkey = str(val) if key_is_str else val
            brows = buckets.get(bkey, [])
            tables_copy = dict(tables)
            tables_copy[ftbl_name] = {**ftbl, "rows": brows}
            res = evaluate_measures_batch(list(ms), tables_copy, measures, {},
                                          date_table, date_column, relationships)
            for name in ms:
                fast.setdefault(name, {})[val] = res.get(name)

    return fast


def _find_selectedvalue_targets(expr: str) -> list:
    """Find all SELECTEDVALUE('Table'[Column]) references in a DAX expression.

    Returns list of (table_name, column_name) tuples.
    """
    targets = []
    # SELECTEDVALUE('Table'[Column], ...) or SELECTEDVALUE(Table[Column], ...)
    for m in re.finditer(r"SELECTEDVALUE\s*\(\s*'?([^'(\[]+)'?\s*\[([^\]]+)\]", expr, re.IGNORECASE):
        targets.append((m.group(1).strip(), m.group(2).strip()))
    return targets


def _tag_group_by(value):
    """Tag one filter_context value as the query's grouping (GroupByValues)."""
    if isinstance(value, dict):
        return GroupByPredicate(value)
    if isinstance(value, (list, tuple, set)):
        return GroupByValues(value)
    return GroupByValues([value])


def evaluate_measures_smart(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict | None = None,
                            date_table: str | None = None, date_column: str | None = None,
                            relationships: list | None = None,
                            simulate_row_context: bool = True,
                            measure_tables: dict | None = None,
                            model_columns: dict | None = None,
                            culture: str | None = None,
                            group_by: set | None = None,
                            selected_filters: dict | None = None,
                            date_tables: dict | None = None,
                            calculation_groups: list | None = None,
                            measure_formats: dict | None = None) -> dict:
    """Evaluate measures with smart fallback for SELECTEDVALUE-dependent measures.

    ``calculation_groups`` / ``measure_formats``: as in evaluate_measures_batch
    (issue #121).

    ``group_by`` names the filter_context keys that are the query's GROUPING
    (one SUMMARIZECOLUMNS / visual row), as opposed to slicer filters: their
    values are tagged GroupByValues for ISINSCOPE, and they become the context's
    group_keys (with the rest as selected_filters) as in grouped evaluation.
    ``selected_filters`` overrides that "rest": the query's own filters BEFORE the
    grouping keys were merged in. It matters when a grouped column is also
    sliced -- ``IT Area IN {A, B}`` grouped by IT Area becomes ``IT Area = [A]``
    in filter_context, and ALLSELECTED must restore {A, B}, not drop the slicer.

    ``date_tables`` maps each table MARKED as a date table to its date column
    (ModelReader.date_tables): a CALCULATE filter on that column removes the
    table's other filters, as Desktop does.

    When a measure returns BLANK and its expression uses SELECTEDVALUE on a
    parameter table, this tries evaluating with each possible value to find
    a non-BLANK result. This simulates what Power BI does when a visual
    provides row context for a parameter table.

    That simulation makes the answer DIFFER FROM DESKTOP at the grand total,
    where the honest result is BLANK. Agents_Performance's five TopN/BottomN
    measures end in SWITCH(TRUE(), SELECTEDVALUE('Clustered Employees'[Order]) =
    1, ...) with NO default: with no selection Desktop returns BLANK, while the
    fallback picked Order = 1 and reported 385,096.472. Pass
    simulate_row_context=False for Desktop-identical evaluation; pbix_evaluate_dax
    does so unless the caller opts in.
    """
    if calculation_groups is not None or measure_formats is not None:
        set_calculation_groups(measures, calculation_groups, measure_formats)
    if group_by:
        filter_context = {key: (_tag_group_by(v) if key in group_by else v)
                          for key, v in (filter_context or {}).items()}
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    ctx.date_tables = date_tables or {}
    if group_by:
        ctx.group_keys = set(group_by)
        ctx.selected_filters = (dict(selected_filters) if selected_filters is not None else
                                {key: v for key, v in (filter_context or {}).items() if key not in group_by})
    # A measure's home table is the only thing that can disambiguate an
    # unqualified [Column] several tables share -- see _resolve_bare_column.
    ctx.measure_tables = measure_tables or {}
    ctx.model_columns = model_columns or {}
    ctx.culture = culture
    results = {}

    for name in measure_names:
        val = _engine.evaluate_measure(name, ctx)
        if val is not None or not simulate_row_context:
            results[name] = val
            continue

        # Value is BLANK — check if measure uses SELECTEDVALUE on a parameter table
        expr = measures.get(name, '')

        targets = _find_selectedvalue_targets(expr)

        # Also check for ISFILTERED('Table'[Column]) patterns
        isfiltered_targets = set()
        for m in re.finditer(r"ISFILTERED\s*\(\s*'?([^'(\[]+)'?\s*\[([^\]]+)\]", expr, re.IGNORECASE):
            tgt = (m.group(1).strip(), m.group(2).strip())
            isfiltered_targets.add(tgt)
            if tgt not in targets:
                targets.append(tgt)

        # Also check for implicit column references like 'Table'[Column] in scalar context
        for m in re.finditer(r"'([^']+)'\s*\[([^\]]+)\]", expr):
            tbl_name, col_name = m.group(1), m.group(2)
            if (tbl_name, col_name) not in targets:
                tbl = tables.get(tbl_name)
                if tbl and len(tbl['rows']) < 50:
                    targets.append((tbl_name, col_name))

        if not targets:
            results[name] = val
            continue

        # Check if measure is expensive (RANKX chain) — if so, only try
        # small parameter tables (< 10 unique values) to keep retries fast
        def _is_expensive(measure_name, visited=None):
            if visited is None:
                visited = set()
            if measure_name in visited:
                return False
            visited.add(measure_name)
            mexp = measures.get(measure_name, '').upper()
            if 'RANKX' in mexp:
                return True
            for ref_match in re.finditer(r'\[([^\]]+)\]', mexp):
                ref_name = ref_match.group(1)
                if ref_name in measures and _is_expensive(ref_name, visited):
                    return True
            return False

        expensive = _is_expensive(name)

        # Try evaluating with each possible value from the target tables
        resolved = False
        for tbl_name, col_name in targets:
            tbl = tables.get(tbl_name)
            if not tbl:
                continue
            col_idx = next((i for i, c in enumerate(tbl['columns']) if c == col_name), -1)
            if col_idx < 0:
                continue

            # Data order, not set order: set iteration of strings follows the
            # per-process hash seed, so the value tried first -- and the answer
            # -- changed from one run to the next (issue #88).
            unique_vals = list(dict.fromkeys(
                row[col_idx] for row in tbl['rows'] if row[col_idx] is not None))
            if not unique_vals:
                continue

            # For expensive measures, only try tiny tables (< 10 values)
            if expensive and len(unique_vals) > 10:
                continue

            # For ISFILTERED targets, any single value works (just need the filter active)
            # For SELECTEDVALUE targets, try the first value
            for try_val in unique_vals[:1]:
                extra_fc = dict(filter_context or {})
                extra_fc[f"{tbl_name}.{col_name}"] = [try_val]
                try_ctx = DAXContext(tables, measures, date_table, date_column, extra_fc, relationships)
                try_val_result = _engine.evaluate_measure(name, try_ctx)
                if try_val_result is not None:
                    results[name] = try_val_result
                    resolved = True
                    break
            if resolved:
                break

        if not resolved:
            results[name] = val

    return results
