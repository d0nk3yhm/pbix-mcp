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

import json
import math
import os
import random
import re
import statistics
import time
from calendar import monthrange
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Any, Optional

# Sentinel returned by _eval_binary to mean "this expression is NOT a binary
# expression" — distinct from a genuine BLANK (None) result of an operation such
# as x / 0. It lets _eval_expr return BLANK for a real division-by-zero instead
# of falling through and mis-parsing the expression as a bare function call
# (which would return just the numerator).
_NOT_APPLICABLE = object()

# An aggregation call in a FILTER condition aggregates over the context rather
# than the iterated row, so its column references must NOT be substituted with
# the row's values (see _fn_filter).
_AGG_CALL_RE = re.compile(
    r"\b(SUM|SUMX|AVERAGE|AVERAGEX|MIN|MINX|MAX|MAXX|COUNT|COUNTX|COUNTA|"
    r"COUNTROWS|COUNTBLANK|DISTINCTCOUNT|MEDIAN|MEDIANX|PRODUCT|PRODUCTX|"
    r"STDEV\.[SP]|VAR\.[SP]|RANKX|CALCULATE|RELATED|RELATEDTABLE)\s*\(",
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
        return (v - datetime(1899, 12, 30)).total_seconds() / 86400.0
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
    return (got - datetime(1899, 12, 30)).total_seconds() / 86400.0


# A whole expression that is exactly one DAX string literal. Interior quotes
# must be DOUBLED, which is how DAX escapes them.
_FULL_STRING_LITERAL = re.compile(r'^"(?:[^"]|"")*"$')


def _concat_str(v):
    """Render a value for the DAX `&` operator.

    `str(v or '')` dropped every FALSY value, so `"0" & 0` produced "0" instead
    of "00" -- and the zero-padding idiom RIGHT("0" & n, 2) silently lost its
    pad on exactly the rows where n was 0. Only BLANK renders as empty.
    A whole float renders without the ".0" tail, as Power BI does.
    """
    if v is None:
        return ''
    if isinstance(v, bool):
        return 'TRUE' if v else 'FALSE'
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
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

_PLAN_CACHE: dict = {}

_SCI_NUM_RE = re.compile(r'^[+-]?\d+(?:\.\d+)?[eE][+-]?\d+$')
_IDENT_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_BRACKET2_RE = re.compile(r'^\[[^\]]+\]$')
_NOT_PREFIX_RE = re.compile(r'(?i)^not\s+(.+)$')
_FUNC_CALL_RE = re.compile(r'([A-Za-z_]\w*)\s*\(')
_TCOL_RE = re.compile(r"(?:'([^'\[\]]+)'|([^\W\d][\w .]*))\s*\[([^\]]+)\]$")
_CALC_PRED_RE = re.compile(
    r"^'?([^'\[\]]+?)'?\s*\[([^\]]+)\]\s*(<>|>=|<=|>|<|=)\s*(.+)$", re.S)
_VAR_KW_RE = re.compile(r'\bVAR\b', re.IGNORECASE)
_RETURN_KW_RE = re.compile(r'\bRETURN\b', re.IGNORECASE)


def _strip_line_comments(expr):
    """Strip // and -- comments, respecting string literals. Verbatim from the
    old _eval_expr body -- the plan cache hoists it out of the per-call path."""
    lines = expr.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('//') or stripped.startswith('--'):
            continue
        in_str = False
        result_chars = []
        i = 0
        while i < len(stripped):
            ch = stripped[i]
            if ch == '"':
                in_str = not in_str
            if not in_str:
                if stripped[i:i+2] == '//' or stripped[i:i+2] == '--':
                    break
            result_chars.append(ch)
            i += 1
        stripped = ''.join(result_chars).rstrip()
        if stripped:
            clean_lines.append(stripped)
    return ' '.join(clean_lines).strip()


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

    if _VAR_KW_RE.search(expr) and _RETURN_KW_RE.search(expr):
        return ((_P_VARRET, expr),)

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
    # NOTE: `IN` as a general expression operator is deliberately NOT registered
    # here yet, though _eval_in below is implemented and unit-tested (including
    # DAX's BLANK semantics: BLANK() IN {1,2} is FALSE).
    #
    # Enabling it made seven Agents_Performance corpus measures go from BLANK to
    # a CONFIDENTLY WRONG value: Desktop returns 0 / "black" / $19,260,877 where
    # this engine then returned 1 / "white" / a placeholder. Verified against
    # Desktop's own msmdsrv, and NOT a slicer-default artifact -- the mismatch
    # holds with apply_default_filters False, which is the same empty context
    # Desktop's EVALUATE ROW(...) uses.
    #
    # The fault is not in IN. Those measures wrap it around RANKX / TOPN over a
    # parameter-table scalar, and that chain is independently inaccurate here;
    # IN merely stopped masking it. A blank is a visible non-answer, a wrong
    # number is not, so this stays off until the RANKX chain matches Desktop.
    # CALCULATE's own IN support does not depend on this step -- see
    # _calculate_filter_spec, which calls _split_in_scan directly.

    for op in ('<>', '>=', '<=', '>', '<', '='):
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


def _as_date(v):
    """Best-effort date coercion from a value or ISO-ish string."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        s = v.strip()
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d",
                    "%d/%m/%Y", "%m/%d/%Y"):
            try:
                return datetime.strptime(s[:len(fmt) + 2].strip(), fmt).date()
            except ValueError:
                continue
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
    return None


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
    return left, right


def _compare(cell, op: str, target) -> bool:
    """Compare a cell against a target — numerically when both are numbers, by
    date when both parse as dates, else as text."""
    a_num, b_num = _as_number(cell), _as_number(target)
    if a_num is not None and b_num is not None:
        a, b = a_num, b_num
    else:
        a_dt, b_dt = _as_date(cell), _as_date(target)
        if a_dt is not None and b_dt is not None:
            a, b = a_dt, b_dt
        else:
            a = "" if cell is None else str(cell)
            b = "" if target is None else str(target)
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


def make_value_matcher(spec):
    """Build a predicate ``f(cell) -> bool`` for one filter_context entry.

    A LIST keeps the historical In-set semantics EXACTLY (string membership).
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
        values = spec if isinstance(spec, (list, tuple, set)) else [spec]
        allowed = {str(v) for v in values}
        return lambda cell: str(cell) in allowed

    tests = []
    if "op" in spec:
        op, target = spec["op"], spec.get("value")
        tests.append(lambda c: _compare(c, op, target))
    if "between" in spec:
        lo, hi = spec["between"]
        tests.append(lambda c: _compare(c, ">=", lo) and _compare(c, "<=", hi))
    if "in" in spec:
        allowed_in = {str(v) for v in spec["in"]}
        tests.append(lambda c: str(c) in allowed_in)
    if "not_in" in spec:
        denied = {str(v) for v in spec["not_in"]}
        tests.append(lambda c: str(c) not in denied)
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
            d = _as_date(c)
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
        self._eval_stack: set = set()  # Prevent circular refs
        # Bound total sub-expression evaluations per top-level measure so a
        # pathological/non-terminating measure degrades to BLANK instead of
        # hanging the whole tool. Reset per outermost measure in evaluate_measure.
        self._eval_calls = 0
        self._max_eval_calls = 3_000_000
        # Build relationship index: { (fromTable, toTable): { fromCol, toCol } }
        self._rel_index = {}
        for rel in self.relationships:
            if rel.get('IsActive'):
                ft = rel.get('FromTable', '')
                tt = rel.get('ToTable', '')
                fc = rel.get('FromColumn', '')
                tc = rel.get('ToColumn', '')
                if ft and tt and fc and tc:
                    self._rel_index[(ft, tt)] = {'from_col': fc, 'to_col': tc}
                    self._rel_index[(tt, ft)] = {'from_col': tc, 'to_col': fc}
        # Directed adjacency for MULTI-HOP (snowflake) filter propagation.
        # A filter propagates along the default cross-filter direction: from the
        # "one" side (ToTable) to the "many" side (FromTable). Each edge carries
        # the join columns on both endpoints so a filter can be chained hop by
        # hop. Bidirectional relationships (CrossFilteringBehavior == 2) also add
        # the reverse edge. Only used when no DIRECT relationship exists, so the
        # existing single-hop behaviour is untouched.
        #   _rel_adj[a] = [ (b, col_on_a, col_on_b), ... ]
        #   -> allowed values of a[col_on_a] restrict b[col_on_b]
        self._rel_adj: dict = {}
        for rel in self.relationships:
            if not rel.get('IsActive'):
                continue
            ft = rel.get('FromTable', '')
            tt = rel.get('ToTable', '')
            fc = rel.get('FromColumn', '')
            tc = rel.get('ToColumn', '')
            if not (ft and tt and fc and tc):
                continue
            # one -> many (dim -> fact): ToTable -> FromTable
            self._rel_adj.setdefault(tt, []).append((ft, tc, fc))
            if rel.get('CrossFilteringBehavior') == 2:
                # bidirectional: also many -> one
                self._rel_adj.setdefault(ft, []).append((tt, fc, tc))

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
        return -1

    def _get_cross_table_filters(self, table_name: str) -> list:
        """
        Get ALL cross-table filters that apply to a target table.
        Uses model relationships to propagate dimension filters to fact tables.
        Returns list of (allowed_values_set, fact_col_idx) tuples.
        """
        if not self.filter_context:
            return []

        tbl = self.tables.get(table_name)
        if not tbl:
            return []

        result_filters = []

        # Group filter context entries by source table
        table_filters: dict = {}
        for fk, values in self.filter_context.items():
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

            # Find relationship between source dim table and target table
            rel = self._rel_index.get((table_name, src_table))
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
            filtered_dim_rows = src_tbl['rows']
            for src_col, values in col_filters:
                col_idx = self._find_col_idx(src_tbl['columns'], src_col)
                if col_idx >= 0:
                    _m = make_value_matcher(values)
                    filtered_dim_rows = [r for r in filtered_dim_rows if _m(r[col_idx])]

            # Get allowed join key values. Append even an EMPTY set: an empty
            # dimension selection must filter the fact to zero rows (BLANK), not
            # be dropped (which would leave the fact unfiltered -> grand total).
            # Mirrors the multi-hop path's empty-set handling.
            allowed_keys = set(str(r[dim_join_idx]) for r in filtered_dim_rows)
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
        frontier_rows = src_tbl['rows']
        for src_col, values in col_filters:
            idx = self._find_col_idx(src_tbl['columns'], src_col)
            if idx >= 0:
                _m = make_value_matcher(values)
                frontier_rows = [r for r in frontier_rows if _m(r[idx])]
        frontier_tbl = src_tbl
        for (cur_name, nxt_name, col_cur, col_nxt) in path:
            cur_idx = self._find_col_idx(frontier_tbl['columns'], col_cur)
            if cur_idx < 0:
                return None
            allowed_keys = set(str(r[cur_idx]) for r in frontier_rows)
            nxt_tbl = self.tables.get(nxt_name)
            if not nxt_tbl:
                return None
            nxt_idx = self._find_col_idx(nxt_tbl['columns'], col_nxt)
            if nxt_idx < 0:
                return None
            if nxt_name == target_table:
                # Final hop: emit the filter for the fact table (empty set is OK).
                return (allowed_keys, nxt_idx)
            # Intermediate hop: restrict the next table's rows and continue.
            frontier_rows = [r for r in nxt_tbl['rows'] if str(r[nxt_idx]) in allowed_keys]
            frontier_tbl = nxt_tbl
        return None

    def _get_date_cross_filter(self, table_name, date_tbl, col_filters):
        """Handle date dimension filters (Year, Month etc.) that need Date column resolution."""
        date_cols = date_tbl['columns']
        date_col_idx = self._find_col_idx(date_cols, self.date_column)
        if date_col_idx < 0:
            return []

        # Filter date rows
        filtered_rows = date_tbl['rows']
        for col_name, values in col_filters:
            col_idx = self._find_col_idx(date_cols, col_name)
            if col_idx >= 0:
                _m = make_value_matcher(values)
                filtered_rows = [r for r in filtered_rows if _m(r[col_idx])]

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

    def _get_column_data_uncached(self, table_name: str, column_name: str) -> list:
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        cols = tbl['columns']
        col_idx = self._find_col_idx(cols, column_name)
        if col_idx < 0:
            return []

        rows = tbl['rows']

        # Apply ALL direct filters for this table (not just the target column)
        for fk, allowed_values in self.filter_context.items():
            parts = fk.split('.', 1)
            if parts[0] == table_name and len(parts) == 2:
                filt_col = parts[1]
                filt_idx = self._find_col_idx(cols, filt_col)
                if filt_idx >= 0:
                    _m = make_value_matcher(allowed_values)
                    rows = [row for row in rows if _m(row[filt_idx])]

        # Apply ALL cross-table filters (star-schema propagation via relationships)
        for allowed_vals, join_idx in self._get_cross_table_filters(table_name):
            rows = [row for row in rows if str(row[join_idx]) in allowed_vals]

        return [row[col_idx] for row in rows]

    def get_filtered_rows(self, table_name: str) -> list:
        """Get rows of a table after applying filter context."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        rows = tbl['rows']
        cols = tbl['columns']

        # Apply all direct filters for this table
        filtered = rows
        for fk, allowed_values in self.filter_context.items():
            parts = fk.split('.', 1)
            if parts[0] == table_name and len(parts) == 2:
                col_name = parts[1]
                col_idx = self._find_col_idx(cols, col_name)
                if col_idx >= 0:
                    _m = make_value_matcher(allowed_values)
                    filtered = [r for r in filtered if _m(r[col_idx])]

        # Apply ALL cross-table filters via relationships
        for allowed_vals, join_idx in self._get_cross_table_filters(table_name):
            filtered = [row for row in filtered if str(row[join_idx]) in allowed_vals]

        return filtered

    def with_filters(self, extra_filters: dict) -> 'DAXContext':
        """Create a new context with additional filters applied."""
        new_filters = dict(self.filter_context)
        new_filters.update(extra_filters)
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        ctx._measure_cache = {}
        return ctx

    def without_filters(self, keys: list) -> 'DAXContext':
        """Create a new context with specified filters removed."""
        new_filters = {k: v for k, v in self.filter_context.items() if k not in keys}
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        return ctx


class DAXEngine:
    """Evaluates DAX expressions."""

    def __init__(self):
        self._current_var_scope = None  # Active variable scope during VAR/RETURN eval
        self.unsupported_functions: set[str] = set()  # Track unsupported DAX functions hit
        # Wall-clock budget per outermost measure, enforced on the ENGINE (not
        # the context) because iterators spawn a fresh sub-context per row —
        # a context-local timer/counter would reset every row and never fire.
        # _eval_depth tracks true measure nesting so the deadline is set once at
        # the top and shared across every sub-context; _time_counter is a global
        # throttle so the time check runs regardless of which context is active.
        try:
            self._max_eval_seconds = float(os.environ.get("PBIX_DAX_MAX_SECONDS", "20"))
        except (TypeError, ValueError):
            self._max_eval_seconds = 20.0
        self._deadline = None
        self._eval_depth = 0
        self._time_counter = 0
        self._func_map = {
            # --- Aggregation ---
            'SUM': self._fn_sum,
            'AVERAGE': self._fn_average,
            'COUNT': self._fn_count,
            'COUNTROWS': self._fn_countrows,
            'MIN': self._fn_min,
            'MAX': self._fn_max,
            'DISTINCTCOUNT': self._fn_distinctcount,
            'PRODUCT': self._fn_product,
            'MEDIAN': self._fn_median,
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
            'ISCROSSFILTERED': self._fn_iscrossfiltered,
            'USERELATIONSHIP': self._fn_userelationship,
            'EARLIER': self._fn_earlier,
            'EARLIEST': self._fn_earliest,
            # --- Table ---
            'TOPN': self._fn_topn,
            'ADDCOLUMNS': self._fn_addcolumns,
            'SUMMARIZE': self._fn_summarize,
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
        }

    def evaluate_measure(self, measure_name: str, ctx: DAXContext) -> Any:
        """Evaluate a named measure in the given context."""
        # Check cache
        try:
            # A filter value is a list (In-set) or a dict (structured
            # predicate). Dicts are unhashable, so serialize them
            # deterministically instead of letting the cache key blow up —
            # an exception here used to surface as a null measure result.
            def _fc_part(v):
                if isinstance(v, list):
                    return tuple(v)
                if isinstance(v, dict):
                    return ("__pred__", json.dumps(v, sort_keys=True,
                                                   default=str))
                return v
            fc_key = tuple(sorted(
                (k, _fc_part(v)) for k, v in ctx.filter_context.items()
            )) if ctx.filter_context else ()
            cache_key = (measure_name, fc_key)
        except Exception:
            cache_key = None
        if cache_key and cache_key in ctx._measure_cache:
            return ctx._measure_cache[cache_key]

        # Prevent circular references
        if measure_name in ctx._eval_stack:
            from pbix_mcp.errors import DAXEvaluationError
            raise DAXEvaluationError(f"Circular reference detected: '{measure_name}' references itself")
        if not ctx._eval_stack:
            ctx._eval_calls = 0   # reset budget for each outermost measure
        ctx._eval_stack.add(measure_name)

        expr = ctx.measures.get(measure_name)
        if expr is None:
            ctx._eval_stack.discard(measure_name)
            return None

        # Engine-level wall-clock deadline, set once at the true outermost
        # measure and shared across every sub-context iterators create.
        self._eval_depth += 1
        if self._eval_depth == 1:
            self._deadline = time.monotonic() + self._max_eval_seconds
        # A measure invocation IS the row->filter context transition (implicit
        # CALCULATE): inside the body, the iteration row's filters are real
        # filters, so plain aggregates must NOT step back to the outer context.
        _prev_outer = getattr(ctx, '_outer_ctx', None)
        ctx._outer_ctx = None
        try:
            result = self._eval_expr(expr.strip(), ctx)
            if cache_key:
                ctx._measure_cache[cache_key] = result
            return result
        except Exception:
            # Graceful degradation
            return None
        finally:
            ctx._outer_ctx = _prev_outer
            ctx._eval_stack.discard(measure_name)
            self._eval_depth -= 1
            if self._eval_depth == 0:
                self._deadline = None

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
            raise DAXEvaluationError(
                "DAX evaluation time budget exceeded (measure too slow to "
                "evaluate — e.g. a rank/iterator scanning a large fact table)"
            )

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
                self.unsupported_functions.add(func_name)
                import logging
                logging.getLogger("pbix_mcp.dax").debug(
                    "Unsupported DAX function: %s", func_name)
                return None
            if kind == _P_BINARY:
                op, parts = data
                return self._fold_arith(parts, op, ctx, var_scope)
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
                if ctx._current_row and ctx._current_row.get('__table__') == table_name:
                    if col_name in ctx._current_row:
                        return ctx._current_row[col_name]
                    if ctx._current_row.get('__column__') == col_name:
                        return ctx._current_row.get('__value__')
                return (table_name, col_name)
            if kind == _P_BRACKET1:
                if (data not in ctx.measures and ctx._current_row
                        and data in ctx._current_row):
                    return ctx._current_row[data]
                return self.evaluate_measure(data, ctx)
            if kind == _P_MAYBEVAR:
                if var_scope:
                    if data in var_scope:
                        return var_scope[data]
                    for k, v in var_scope.items():
                        if k.lower() == data.lower():
                            return v
                continue  # not a var in this scope: fall through to the tail
            if kind == _P_PAREN:
                return self._eval_expr(data, ctx, var_scope)
            if kind == _P_LOGICAL:
                lop, parts = data
                vals = [self._dax_truthy(self._eval_expr(p, ctx, var_scope))
                        for p in parts]
                return any(vals) if lop == '||' else all(vals)
            if kind == _P_CONCAT:
                return ''.join(_concat_str(self._eval_expr(p, ctx, var_scope))
                               for p in data)
            if kind == _P_NEG:
                inner_val = self._eval_expr(data, ctx, var_scope)
                if isinstance(inner_val, (int, float)) and not isinstance(inner_val, bool):
                    return -inner_val
                return None
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
        text = re.sub(r'\s+', ' ', expr).strip()

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
                self._current_var_scope = scope
                val = self._eval_expr(var_expr, ctx, scope)
                scope[var_name] = val

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
        for i, ch in enumerate(expr):
            if ch == '"' and (i == 0 or expr[i-1] != '\\'):
                in_string = not in_string
            if in_string:
                continue
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return expr[:i+1]
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
        dp = db = 0
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
            if dp == 0 and db == 0 and expr[i:i + len(op)] == op:
                prev = ''.join(cur)
                nxt = expr[i + len(op):i + len(op) + 1]
                skip = False
                if op in ('+', '-'):
                    p = prev.rstrip()
                    if p == '' or p[-1] in '+-*/(<>=&|,':
                        skip = True                       # unary sign
                    elif len(p) >= 2 and p[-1] in 'eE' and p[-2].isdigit():
                        skip = True                       # exponent 1e-5
                elif op == '=' and prev[-1:] in ('<', '>'):
                    skip = True                           # part of <= / >=
                elif op == '<' and nxt in ('=', '>'):
                    skip = True                           # part of <= / <>
                elif op == '>' and nxt == '=':
                    skip = True                           # part of >=
                if not skip:
                    parts.append(prev); cur = []; i += len(op); continue
            cur.append(ch); i += 1
        parts.append(''.join(cur))
        return tuple(parts)

    def _make_row_context(self, row_item: dict, ctx: 'DAXContext') -> 'DAXContext':
        """Create a filter context from a row dict, filtering on ALL columns of the row.
        This implements the row context → filter context transition."""
        meta_keys = {'__table__', '__column__', '__value__', '__row__'}
        table_name = row_item.get('__table__', '')
        filters = {}
        for k, v in row_item.items():
            if k in meta_keys or v is None:
                continue
            filters[f"{table_name}.{k}"] = [v]
        # Also add the primary column filter
        col = row_item.get('__column__', '')
        val = row_item.get('__value__')
        if col and val is not None:
            filters[f"{table_name}.{col}"] = [val]
        new_ctx = ctx.with_filters(filters)
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

    def _resolve_row_result(self, result, row_item, row_ctx):
        """Resolve a column reference result in a row iteration context.
        If result is a (table, column) tuple, resolve it to a concrete value."""
        if isinstance(result, tuple) and len(result) == 2:
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
        for p in parts[1:]:
            rhs = self._eval_expr(p.strip(), ctx, var_scope)
            # DAX: BLANK is 0 in arithmetic.
            left = 0 if acc is None else acc
            right = 0 if rhs is None else rhs
            if not (isinstance(left, (int, float))
                    and isinstance(right, (int, float))):
                # A DATE is a number in DAX, and the row-context evaluator
                # hands dates over as ISO strings, so coerce before giving up.
                # Returning None here made `[end] - [start]` blank on every row
                # of a datetime column.
                left, right = _as_number(left), _as_number(right)
                if left is None or right is None:
                    return None
            if op == '+':
                acc = left + right
            elif op == '-':
                acc = left - right
            elif op == '*':
                acc = left * right
            else:
                if right == 0:
                    return None
                acc = left / right
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
                # An EMPTY table expression is ambiguous here. Real DAX would say
                # FALSE, but this engine also returns [] for a table function it
                # cannot evaluate in the current scope -- VALUES(T[C]) yields []
                # inside a row context, for instance. Answering FALSE would turn
                # that limitation into a confident wrong answer, so report
                # "unknown" and let the caller fall through to BLANK. A literal
                # `{}` is handled above and DOES mean the empty set.
                return None
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
        """Evaluate comparison operators."""
        for op_str, op_fn in [('<>', lambda a, b: a != b), ('>=', lambda a, b: a >= b),
                               ('<=', lambda a, b: a <= b), ('>', lambda a, b: a > b),
                               ('<', lambda a, b: a < b), ('=', lambda a, b: a == b)]:
            parts = self._split_operators(expr, op_str)
            if len(parts) == 2:
                left = self._eval_expr(parts[0].strip(), ctx, var_scope)
                right = self._eval_expr(parts[1].strip(), ctx, var_scope)
                # BLANK does not propagate through a comparison in DAX; it
                # takes the zero of the other operand's type. See
                # _blank_zero_of for the Desktop-verified table.
                left, right = _coerce_blanks_for_compare(left, right)
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
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return 0
            col = ref
        values = self._agg_ctx(ctx).get_column_data(*col)
        nums = [v for v in values if isinstance(v, (int, float))]
        # SUM over no rows is BLANK in DAX (so ISBLANK fires), not 0.
        return sum(nums) if nums else None

    def _fn_average(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return 0
            col = ref
        values = [v for v in self._agg_ctx(ctx).get_column_data(*col)
                  if isinstance(v, (int, float))]
        return sum(values) / len(values) if values else 0

    def _fn_count(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return 0
            col = ref
        return len([v for v in self._agg_ctx(ctx).get_column_data(*col)
                    if v is not None])

    def _fn_countrows(self, args_str: str, ctx: DAXContext) -> Any:
        # Try evaluating as an expression first (handles TOPN, FILTER, etc.)
        result = self._eval_expr(args_str.strip(), ctx)
        if isinstance(result, list):
            return len(result)
        # Fall back to table name lookup
        table_name = args_str.strip().strip("'")
        rows = ctx.get_filtered_rows(table_name)
        return len(rows)

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
        if col is None:
            ref = self._eval_expr(args[0].strip(), ctx)
            col = ref if isinstance(ref, tuple) and len(ref) == 2 else None
        if not col:
            return None
        values = self._comparable_values(
            self._agg_ctx(ctx).get_column_data(*col))
        return pick(values) if values else 0

    @staticmethod
    def _minmax_pair(a, b, pick):
        """Two-argument form. Compares dates and text as well as numbers, but
        never across incompatible types."""
        for kinds in ((int, float), (datetime, date), (str,)):
            if isinstance(a, kinds) and isinstance(b, kinds) \
                    and not isinstance(a, bool) and not isinstance(b, bool):
                return pick(a, b)
        return 0

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
        return 0

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
        return 0

    def _fn_distinctcount(self, args_str: str, ctx: DAXContext) -> Any:
        col = self._parse_column_ref(args_str)
        if col is not None:
            self._charge_eval(ctx)
        if col is None:
            ref = self._eval_expr(args_str.strip(), ctx)
            if not (isinstance(ref, tuple) and len(ref) == 2):
                return 0
            col = ref
        values = self._agg_ctx(ctx).get_column_data(*col)
        return len(set(str(v) for v in values if v is not None))

    def _fn_divide(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        numerator = self._eval_expr(args[0].strip(), ctx)
        denominator = self._eval_expr(args[1].strip(), ctx)
        # DAX: the alternate result defaults to BLANK (None), not 0.
        alt = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else None
        # DAX treats a BLANK numerator as 0; a BLANK/zero denominator yields the
        # alternate (BLANK by default) — NOT the numerator.
        if numerator is None:
            numerator = 0
        if isinstance(numerator, (int, float)) and isinstance(denominator, (int, float)):
            if denominator == 0:
                return alt
            return numerator / denominator
        return alt

    def _fn_abs(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._eval_expr(args_str.strip(), ctx)
        return abs(val) if isinstance(val, (int, float)) else None

    def _fn_round(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        digits = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        return round(val, digits) if isinstance(val, (int, float)) else None

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
        val = _as_number(self._eval_expr(args[0].strip(), ctx))
        if val is None:
            return None
        digits = 0
        if len(args) > 1:
            n = _as_number(self._eval_expr(args[1].strip(), ctx))
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
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, bool) or not isinstance(val, (int, float)):
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
            if test_val == case_val:
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
        for i in range(1, len(args)):
            filter_arg = args[i].strip()

            # REMOVEFILTERS / ALL
            if filter_arg.upper().startswith('REMOVEFILTERS') or filter_arg.upper().startswith('ALL'):
                # Extract the column/table reference. Exclude [ ] from the table
                # capture so an UNQUOTED Sales[Region] splits into table=Sales +
                # col=Region (else the whole "Sales[Region]" was captured as a
                # table name and ALL/REMOVEFILTERS silently did nothing).
                inner_match = re.search(r"\(\s*'?([^'\)\[\]]+)'?\s*(?:\[([^\]]+)\])?\s*\)", filter_arg)
                if inner_match:
                    table = inner_match.group(1).strip()
                    col = inner_match.group(2)
                    if col:
                        new_ctx = new_ctx.without_filters([f"{table}.{col}"])
                    else:
                        # Remove all filters for this table
                        keys_to_remove = [k for k in new_ctx.filter_context if k.startswith(f"{table}.")]
                        new_ctx = new_ctx.without_filters(keys_to_remove)
                continue

            # DATEADD
            if filter_arg.upper().startswith('DATEADD'):
                new_ctx = self._apply_dateadd_filter(filter_arg, new_ctx)
                continue

            # SAMEPERIODLASTYEAR
            if filter_arg.upper().startswith('SAMEPERIODLASTYEAR'):
                new_ctx = self._apply_dateadd_filter(
                    f"DATEADD({filter_arg[19:-1].strip()}, -1, YEAR)", new_ctx)
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

            # TREATAS
            if filter_arg.upper().startswith('TREATAS'):
                result = self._eval_expr(filter_arg, new_ctx)
                if isinstance(result, dict) and '__treatas__' in result:
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
                           'DATESBETWEEN', 'DATESINPERIOD')
            fa_upper = filter_arg.upper().split('(')[0].strip()
            if fa_upper in ti_prefixes:
                result = self._eval_expr(filter_arg, new_ctx)
                if isinstance(result, list) and result:
                    # Time-intelligence returns list of date-row dicts
                    first = result[0]
                    if isinstance(first, dict) and '__table__' in first:
                        tbl_name = first['__table__']
                        col_name = first['__column__']
                        date_vals = [r['__value__'] for r in result]
                        new_filters = dict(new_ctx.filter_context)
                        # Remove existing date table filters
                        keys_to_remove = [k for k in new_filters if k.startswith(f"{tbl_name}.")]
                        for k in keys_to_remove:
                            del new_filters[k]
                        new_filters[f"{tbl_name}.{col_name}"] = date_vals
                        new_ctx = DAXContext(new_ctx.tables, new_ctx.measures, new_ctx.date_table,
                                             new_ctx.date_column, new_filters, new_ctx.relationships)
                continue

            # FILTER(table, condition) or other table-returning expressions
            # Evaluate the filter arg — if it returns a list of row dicts,
            # extract filter values grouped by table.column
            result = self._eval_expr(filter_arg, new_ctx)
            if isinstance(result, list) and result:
                first = result[0]
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
                    if groups:
                        new_ctx = new_ctx.with_filters(groups)
                continue

            # Any boolean predicate: =, <>, >, >=, <, <=, IN {...}, with NOT and
            # KEEPFILTERS peeled. Handled BEFORE the legacy equality regex below,
            # which would otherwise match the wrapper text of NOT(T[C] = v) and
            # register a filter on a column named "NOT(T".
            spec = self._calculate_filter_spec(filter_arg, new_ctx)
            if spec:
                key, value = spec
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
                val = self._eval_expr(eq_match.group(3).strip(), new_ctx)
                if val is not None:
                    new_ctx = new_ctx.with_filters({f"{tbl_name}.{col_name}": [val]})
                continue

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
        """A CALCULATE boolean filter argument as ``(key, spec)``, or ``()``.

        Only ``Table[Col] = value`` used to be honoured; every other predicate
        fell off the end of the filter loop adding NO filter and NO warning, so
        CALCULATE quietly returned the UNFILTERED total (OpenBI findings #9 #5b).
        The filter context already evaluates structured predicates natively via
        :func:`make_value_matcher`, so this only has to translate into that form.

        ``NOT`` is folded into the operator rather than wrapped, and
        ``KEEPFILTERS`` is peeled: both used to be swallowed by the equality
        regex, which matched the WRAPPER text and registered a filter on a column
        named e.g. ``KEEPFILTERS(T``.
        """
        wrapper, inner = self._peel_call(filter_arg, ('KEEPFILTERS',))
        negate = False
        while True:
            wrapper, peeled = self._peel_call(inner, ('NOT',))
            if not wrapper:
                break
            negate = not negate
            inner = peeled

        m = _CALC_PRED_RE.match(inner)
        if m:
            tbl, col, op, val_text = (m.group(1).strip(), m.group(2).strip(),
                                      m.group(3), m.group(4).strip())
            val = self._eval_expr(val_text, ctx, var_scope)
            if val is None:
                return ()
            if negate:
                op = {'=': '<>', '<>': '=', '>': '<=', '<=': '>',
                      '>=': '<', '<': '>='}[op]
            key = f"{tbl}.{col}"
            # Plain equality keeps the historical In-set list form: it is what
            # every existing caller and test expects, and make_value_matcher
            # treats a list as string membership.
            if op == '=':
                return (key, [val])
            return (key, {"op": op, "value": val})

        in_parts = self._split_in_scan(inner)
        if in_parts:
            left = in_parts[0].strip()
            lm = re.match(r"^'?([^'\[\]]+?)'?\s*\[([^\]]+)\]$", left)
            if lm:
                vals = self._in_set_values(in_parts[1], ctx, var_scope)
                if vals is None:
                    return ()
                key = f"{lm.group(1).strip()}.{lm.group(2).strip()}"
                return (key, {"not_in" if negate else "in": vals})
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
        return DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                          ctx.date_column, ctx.filter_context, new_rels)

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
        return DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                          ctx.date_column, ctx.filter_context, new_rels)

    def _apply_dateadd_filter(self, expr: str, ctx: DAXContext) -> DAXContext:
        """Apply DATEADD as a filter context modification."""
        # Parse DATEADD(column, offset, interval)
        match = re.search(r"DATEADD\s*\(\s*'?([^'\[]+)'?\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*(\w+)\s*\)", expr, re.IGNORECASE)
        if not match:
            return ctx

        date_table = match.group(1).strip()
        date_col = match.group(2).strip()
        offset = int(match.group(3))
        interval = match.group(4).upper()

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
                return DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                                  ctx.date_column, new_filters, ctx.relationships)

        return ctx

    def _fn_removefilters(self, args_str: str, ctx: DAXContext) -> Any:
        """REMOVEFILTERS — returns a marker for CALCULATE to process."""
        return ('__REMOVEFILTERS__', args_str.strip())

    def _fn_all(self, args_str: str, ctx: DAXContext) -> Any:
        """ALL — when used inside CALCULATE returns a marker; when used as a
        table expression (e.g. ALL('table'[column])) returns all distinct
        values of that column ignoring any active filters."""
        ref = args_str.strip()
        # Try to parse as a column reference: 'table'[column]
        col_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]", ref)
        if col_match:
            table_name = col_match.group(1).strip()
            col_name = col_match.group(2).strip()
            # Return all values ignoring filters — use raw table data
            tbl = ctx.tables.get(table_name)
            if tbl:
                col_idx = ctx._find_col_idx(tbl['columns'], col_name)
                if col_idx >= 0:
                    # Return list of {column: value} dicts for iteration
                    all_values = list(set(row[col_idx] for row in tbl['rows'] if row[col_idx] is not None))
                    return [{'__table__': table_name, '__column__': col_name, '__value__': v} for v in all_values]

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
            return result

        # Fallback: marker for CALCULATE
        return ('__ALL__', ref)

    def _fn_dateadd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATEADD — returns a marker for CALCULATE to process."""
        return ('__DATEADD__', args_str.strip())

    def _fn_sameperiodlastyear(self, args_str: str, ctx: DAXContext) -> Any:
        return ('__DATEADD__', args_str.strip())

    def _fn_values(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            # Order-preserving dedup: Desktop iterates VALUES in data order, and
            # hash-set order made CONCATENATEX output nondeterministic.
            values = list(dict.fromkeys(ctx.get_column_data(ref[0], ref[1])))
            # Return as row-dict list so CONCATENATEX / FILTER / iterators work
            return [{'__table__': ref[0], '__column__': ref[1], '__value__': v} for v in values]
        return []

    def _fn_selectedvalue(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        ref = self._eval_expr(args[0].strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = list(set(ctx.get_column_data(ref[0], ref[1])))
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
        """UTCNOW() — current UTC date and time."""
        return datetime.utcnow()

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
        parts = [_as_number(self._eval_expr(a.strip(), ctx)) for a in args[:3]]
        if any(p is None for p in parts):
            return None
        y, m, d = (int(p) for p in parts)  # type: ignore[arg-type]
        month_zero = (m - 1)
        year = y + month_zero // 12
        month = month_zero % 12 + 1
        return datetime(year, month, 1) + timedelta(days=d - 1)

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
        self.unsupported_functions.add(f"DATEDIFF interval {interval}")
        return None

    # DAX date format tokens -> strftime. Ordered longest-first so "MMMM" isn't
    # eaten by "MM" (dict preserves insertion order).
    _DATE_FMT_TOKENS = (
        ('yyyy', '%Y'), ('yy', '%y'), ('MMMM', '%B'), ('MMM', '%b'), ('MM', '%m'),
        ('dddd', '%A'), ('ddd', '%a'), ('dd', '%d'), ('HH', '%H'), ('hh', '%I'),
        ('mm', '%M'), ('ss', '%S'), ('AM/PM', '%p'), ('am/pm', '%p'),
        # Power BI accepts the upper-case spellings too. Without them
        # FORMAT(d, "YYYY-MM-DD") returned the literal "YYYY-01-DD": only MM
        # matched, and the rest of the picture was copied through verbatim.
        ('YYYY', '%Y'), ('YY', '%y'), ('DDDD', '%A'), ('DDD', '%a'),
        ('DD', '%d'), ('SS', '%S'),
    )

    def _fn_format(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        fmt = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else None
        if val is None:
            return ''
        # A date can reach here as an ISO STRING rather than a datetime (row
        # context over a generated table, a CALENDAR result, a text column
        # holding dates). Without this, FORMAT([Date], "MMMM") returned the raw
        # "2015-01-01T00:00:00" instead of "January" — a silently wrong value,
        # which is worse than an error because nothing surfaces it.
        if fmt and isinstance(val, str) and not isinstance(val, (datetime, date)):
            if any(tok in str(fmt) for tok, _strf in self._DATE_FMT_TOKENS):
                coerced = _as_datetime(val)
                if coerced is not None:
                    val = coerced
        # Datetime formatting (NOW()/TODAY()/date columns): translate the DAX
        # date pattern to strftime token by token.
        if isinstance(val, (datetime, date)) and fmt:
            out = ''
            fmt_str = str(fmt)
            i = 0
            while i < len(fmt_str):
                for tok, strf in self._DATE_FMT_TOKENS:
                    if fmt_str.startswith(tok, i):
                        out += val.strftime(strf)
                        i += len(tok)
                        break
                else:
                    out += fmt_str[i]
                    i += 1
            return out
        if fmt and isinstance(val, (int, float)):
            # Handle common DAX format strings
            fmt_str = str(fmt)
            # "0" or "0.0" or "#,##0" style — count decimal places
            if re.match(r'^[#0,.]+$', fmt_str):
                # Count digits after decimal point
                if '.' in fmt_str:
                    decimals = len(fmt_str.split('.')[-1])
                else:
                    decimals = 0
                use_comma = ',' in fmt_str
                # Leading zeros: each '0' LEFT of the decimal point is a digit
                # that must always be shown. FORMAT(1, "000") is "001", not
                # "1" -- the padded form is how Power BI's own "New group"
                # and sort-key columns are built, so getting it wrong changes
                # sort order as well as display.
                min_int = fmt_str.split('.')[0].count('0')
                neg = val < 0
                body = (f"{abs(val):,.{decimals}f}" if use_comma
                        else f"{abs(val):.{decimals}f}")
                int_part, _dot, dec_part = body.partition('.')
                digits = int_part.replace(',', '')
                if len(digits) < min_int:
                    digits = '0' * (min_int - len(digits)) + digits
                    int_part = (f"{int(digits):,}" if use_comma else digits)
                formatted = int_part + ('.' + dec_part if dec_part else '')
                return ('-' + formatted) if neg else formatted
            # "0.0%" style
            if fmt_str.endswith('%'):
                inner_fmt = fmt_str[:-1]
                if '.' in inner_fmt:
                    decimals = len(inner_fmt.split('.')[-1])
                else:
                    decimals = 0
                return f"{val * 100:.{decimals}f}%"
            # "$#,##0" or "$#,##0.00" style
            if fmt_str.startswith('$'):
                inner = fmt_str[1:]
                if '.' in inner:
                    decimals = len(inner.split('.')[-1])
                else:
                    decimals = 0
                return f"${val:,.{decimals}f}"
        return str(val)

    def _fn_concatenate(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        parts = [str(self._eval_expr(a.strip(), ctx) or '') for a in args]
        return ''.join(parts)

    def _fn_sumx(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMX(table_expression, expression) — iterate over table rows, sum expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            total = 0
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        total += result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        total += result
            return total
        return 0

    def _fn_maxx(self, args_str: str, ctx: DAXContext) -> Any:
        """MAXX(table_expression, expression) — iterate over table rows, return max."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            max_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
            return max_val if max_val is not None else 0
        # Fallback: if table_ref is a column ref, get max of column
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return max(values) if values else 0
        return 0

    def _fn_minx(self, args_str: str, ctx: DAXContext) -> Any:
        """MINX(table_expression, expression) — iterate over table rows, return min."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            min_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # _make_row_context handles both single-column (ALL(T[c]))
                    # and multi-column (bare-table ALL(T)/FILTER(T)) row dicts;
                    # the old __column__/__value__ access KeyError'd on the latter.
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        if min_val is None or result < min_val:
                            min_val = result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        if min_val is None or result < min_val:
                            min_val = result
            return min_val if min_val is not None else 0
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return min(values) if values else 0
        return 0

    def _fn_averagex(self, args_str: str, ctx: DAXContext) -> Any:
        """AVERAGEX(table_expression, expression) — iterate over table rows, average expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        values = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
        return sum(values) / len(values) if values else 0

    def _fn_countx(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTX(table_expression, expression) — count non-blank numeric results per row."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        count = 0
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                else:
                    result = self._eval_expr(row_expr, ctx)
                if result is not None and result != '':
                    count += 1
        return count

    def _fn_countax(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTAX(table_expression, expression) — count non-blank results (like COUNTX but counts text too)."""
        # In DAX, COUNTAX counts non-blank values of any type; functionally same as COUNTX here
        return self._fn_countx(args_str, ctx)

    def _fn_countblank(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTBLANK(column) — count blank values in a column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            return sum(1 for v in values if v is None or v == '')
        return 0

    def _fn_product(self, args_str: str, ctx: DAXContext) -> Any:
        """PRODUCT(column) — multiply all values in column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = [v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float))]
            if not values:
                return 0
            result = 1
            for v in values:
                result *= v
            return result
        return 0

    def _fn_median(self, args_str: str, ctx: DAXContext) -> Any:
        """MEDIAN(column) — return median value."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = sorted(v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float)))
            if not values:
                return 0
            return statistics.median(values)
        return 0

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
                            extra_filters[f"{table_name}.{col_name}"] = [val]
                        row_ctx = ctx.with_filters(extra_filters)
                        if substitute_row_values:
                            row_cond = _substitute_row_refs(
                                cond_expr, table_name, row_item)
                        else:
                            row_cond = cond_expr
                        cond = self._eval_expr(row_cond, row_ctx)
                        if cond:
                            filtered.append(row_item)
                    else:
                        # Single-column row dict from ALL(Table[Column]) or VALUES
                        table_name = row_item['__table__']
                        col_name = row_item['__column__']
                        val = row_item['__value__']
                        row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
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
                    row_ctx = self._make_row_context(row_item, ctx)
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
                row_ctx = self._make_row_context(row_item, ctx)
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
            if extensions:
                group_ctx = ctx.with_filters({
                    f"{table_name}.{col_name}": [row[col_idx]]
                    for col_name, col_idx in group_cols
                })
                for ext_name, ext_expr in extensions:
                    row_dict[ext_name] = self._eval_expr(ext_expr, group_ctx)
            # Use first group col as the iteration column
            row_dict['__column__'] = group_cols[0][0]
            row_dict['__value__'] = row[group_cols[0][1]]
            result.append(row_dict)
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
            vals = list(dict.fromkeys(
                r[ridx] for r in ctx.get_filtered_rows(rt)))
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
        for combo in itertools.product(*axes):
            filters = {key: [val] for key, _disp, val in combo}
            group_ctx = ctx.with_filters(filters)
            if not group_ctx.get_filtered_rows(table_name):
                continue  # combination doesn't exist in the base table
            row_dict = {'__table__': table_name}
            for _key, disp, val in combo:
                row_dict[disp] = val
            for ext_name, ext_expr in extensions:
                row_dict[ext_name] = self._eval_expr(ext_expr, group_ctx)
            first_disp, first_val = combo[0][1], combo[0][2]
            row_dict['__column__'] = first_disp
            row_dict['__value__'] = first_val
            result.append(row_dict)
        return result

    def _fn_summarizecolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMMARIZECOLUMNS(groupBy1, ..., name, expression) — summarize with measures.
        Simplified: treats it like SUMMARIZE for the group-by columns."""
        args = self._split_args(args_str)
        if not args:
            return []
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
        return self._fn_summarize(inner, ctx)

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
                row_ctx = self._make_row_context(row_item, ctx)
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
            # Deduplicate table rows
            seen = set()
            result = []
            for item in ref:
                if isinstance(item, dict) and '__value__' in item:
                    key = str(item['__value__'])
                else:
                    key = str(item)
                if key not in seen:
                    seen.add(key)
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
        """CROSSJOIN(table1, table2) — cartesian product."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list) or not isinstance(t2, list):
            return []
        result = []
        for item1 in t1:
            for item2 in t2:
                merged = {}
                if isinstance(item1, dict):
                    merged.update(item1)
                if isinstance(item2, dict):
                    merged.update({f"_2_{k}": v for k, v in item2.items()})
                result.append(merged)
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
                row_ctx = self._make_row_context(row_item, ctx)
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
                row_ctx = self._make_row_context(row_item, ctx)
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
        """ALLSELECTED(column_or_table) — respect only external (slicer) filters.
        Approximation: returns all distinct values from filtered context (same as VALUES)."""
        # NOTE: True ALLSELECTED requires distinguishing external vs internal filters,
        # which is not tracked in this simplified engine. We approximate with VALUES behavior.
        ref = args_str.strip()
        col_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]", ref)
        if col_match:
            table_name = col_match.group(1).strip()
            col_name = col_match.group(2).strip()
            values = ctx.get_column_data(table_name, col_name)
            unique = list(set(values))
            return [{'__table__': table_name, '__column__': col_name, '__value__': v} for v in unique]
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
            values = list(set(ctx.get_column_data(ref[0], ref[1])))
            return len(values) == 1
        return False

    def _fn_hasonefilter(self, args_str: str, ctx: DAXContext) -> Any:
        """HASONEFILTER(column) — check if exactly one direct filter on column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            filter_key = f"{ref[0]}.{ref[1]}"
            if filter_key in ctx.filter_context:
                return len(ctx.filter_context[filter_key]) == 1
        return False

    def _fn_isfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISFILTERED(column_or_table) — check if column/table has any direct filter."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            filter_key = f"{ref[0]}.{ref[1]}"
            return filter_key in ctx.filter_context
        # Table-only reference (string) — check if any filter matches the table
        if isinstance(ref, str):
            table_name = ref.strip("'\"")
            return any(k.split('.')[0] == table_name for k in ctx.filter_context)
        return False

    def _fn_iscrossfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISCROSSFILTERED(column) — check if column is cross-filtered via relationships."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            # Check direct filter
            filter_key = f"{ref[0]}.{ref[1]}"
            if filter_key in ctx.filter_context:
                return True
            # Check cross-table filters
            cross_filters = ctx._get_cross_table_filters(ref[0])
            return len(cross_filters) > 0
        return False

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
        val = self._eval_expr(args[0].strip(), ctx)
        sig = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.ceil(val / sig) * sig
        return val

    def _fn_floor(self, args_str: str, ctx: DAXContext) -> Any:
        """FLOOR(number, significance) — round down to nearest multiple of significance."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        sig = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.floor(val / sig) * sig
        return val

    def _fn_mod(self, args_str: str, ctx: DAXContext) -> Any:
        """MOD(number, divisor) — modulo."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        val = self._eval_expr(args[0].strip(), ctx)
        divisor = self._eval_expr(args[1].strip(), ctx)
        if isinstance(val, (int, float)) and isinstance(divisor, (int, float)) and divisor != 0:
            return val % divisor
        return None

    def _fn_power(self, args_str: str, ctx: DAXContext) -> Any:
        """POWER(base, exponent) — exponentiation."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        base = self._eval_expr(args[0].strip(), ctx)
        exp = self._eval_expr(args[1].strip(), ctx)
        if isinstance(base, (int, float)) and isinstance(exp, (int, float)):
            return math.pow(base, exp)
        return None

    def _fn_sqrt(self, args_str: str, ctx: DAXContext) -> Any:
        """SQRT(number) — square root."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.sqrt(val)
        return None

    def _fn_log(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG(number, base) — logarithm with specified base (default 10)."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        base = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 10
        if isinstance(val, (int, float)) and val > 0 and isinstance(base, (int, float)) and base > 0:
            return math.log(val, base)
        return None

    def _fn_log10(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG10(number) — base-10 logarithm."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log10(val)
        return None

    def _fn_ln(self, args_str: str, ctx: DAXContext) -> Any:
        """LN(number) — natural logarithm."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log(val)
        return None

    def _fn_exp(self, args_str: str, ctx: DAXContext) -> Any:
        """EXP(number) — e^x."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            return math.exp(val)
        return None

    def _fn_sign(self, args_str: str, ctx: DAXContext) -> Any:
        """SIGN(number) — returns -1, 0, or 1."""
        val = self._eval_expr(args_str.strip(), ctx)
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
        val = self._eval_expr(args[0].strip(), ctx)
        digits = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        if isinstance(val, (int, float)):
            multiplier = 10 ** digits
            return int(val * multiplier) / multiplier
        return None

    def _fn_even(self, args_str: str, ctx: DAXContext) -> Any:
        """EVEN(number) — round up to nearest even integer."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 != 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_odd(self, args_str: str, ctx: DAXContext) -> Any:
        """ODD(number) — round up to nearest odd integer."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 == 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_fact(self, args_str: str, ctx: DAXContext) -> Any:
        """FACT(number) — factorial."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.factorial(int(val))
        return None

    def _fn_gcd(self, args_str: str, ctx: DAXContext) -> Any:
        """GCD(a, b) — greatest common divisor."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._eval_expr(args[0].strip(), ctx)
        b = self._eval_expr(args[1].strip(), ctx)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return math.gcd(int(a), int(b))
        return None

    def _fn_lcm(self, args_str: str, ctx: DAXContext) -> Any:
        """LCM(a, b) — least common multiple."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._eval_expr(args[0].strip(), ctx)
        b = self._eval_expr(args[1].strip(), ctx)
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
            return round(val, 4)
        return 0

    def _fn_fixed(self, args_str: str, ctx: DAXContext) -> Any:
        """FIXED(number, decimals, no_commas) — format number as text with fixed decimals."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        decimals = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 2
        no_commas = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else False
        if isinstance(val, (int, float)):
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
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                else:
                    row_ctx = ctx
                    result = self._eval_expr(row_expr, ctx)
                if result is None:
                    continue
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

    def _fn_rankx(self, args_str: str, ctx: DAXContext) -> Any:
        """RANKX(table, expression, value, order, ties) — rank a value within a table's evaluated expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        rank_expr = args[1].strip()
        # Value to rank (optional — defaults to the expression evaluated in current context)
        value_expr = args[2].strip() if len(args) > 2 else None
        order_str = args[3].strip().upper() if len(args) > 3 else 'DESC'
        is_desc = 'DESC' in order_str

        # Get the value to rank
        if value_expr:
            current_val = self._eval_expr(value_expr, ctx)
        else:
            current_val = self._eval_expr(rank_expr, ctx)

        if not isinstance(current_val, (int, float)):
            return None

        # Evaluate expression for all rows in the table
        all_vals = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # _make_row_context handles bare-table (__row__) iterators
                    # too; the old direct __column__ lookup KeyError'd on them.
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(rank_expr, row_ctx)
                    if isinstance(result, (int, float)):
                        all_vals.append(result)

        if not all_vals:
            return 1

        # Sort and find rank
        if is_desc:
            all_vals.sort(reverse=True)
        else:
            all_vals.sort()

        # Dense ranking
        unique_sorted = sorted(set(all_vals), reverse=is_desc)
        for i, v in enumerate(unique_sorted):
            if abs(v - current_val) < 0.0001:
                return i + 1
        return len(unique_sorted) + 1

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

    def _fn_iferror(self, args_str: str, ctx: DAXContext) -> Any:
        """IFERROR(expression, fallback) — return fallback if expression errors."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        try:
            result = self._eval_expr(args[0].strip(), ctx)
            if result is None:
                return self._eval_expr(args[1].strip(), ctx)
            return result
        except Exception:
            return self._eval_expr(args[1].strip(), ctx)

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
        for row in rows:
            match = True
            for col_idx, value in criteria:
                if str(row[col_idx]) != str(value):
                    match = False
                    break
            if match:
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
        """ISERROR(value) — check if expression results in error."""
        try:
            val = self._eval_expr(args_str.strip(), ctx)
            return val is None
        except Exception:
            return True

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

        # Search through all rows (ignoring filter context for lookup)
        for row in tbl['rows']:
            match = True
            for col_idx, search_val in criteria:
                if str(row[col_idx]) != str(search_val):
                    match = False
                    break
            if match:
                return row[result_col_idx]

        # Return alternate value if provided
        if len(args) > 1 + len(criteria) * 2:
            return self._eval_expr(args[-1].strip(), ctx)
        return None

    # =========================================================================
    # Relationship functions
    # =========================================================================

    def _fn_related(self, args_str: str, ctx: DAXContext) -> Any:
        """RELATED(column) — follow relationship to get a value from a related table.
        Approximation: looks up value via relationship index and current filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if not isinstance(ref, tuple) or len(ref) != 2:
            return None
        target_table, target_col = ref
        # Try to find a related value via relationships
        tbl = ctx.tables.get(target_table)
        if not tbl:
            return None
        target_col_idx = ctx._find_col_idx(tbl['columns'], target_col)
        if target_col_idx < 0:
            return None
        # Get filtered rows from the target table
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

    def _fn_totalytd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALYTD(expression, dates, filter, yearEndDate) — year to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        # Get YTD dates
        ytd_dates = self._fn_datesytd(', '.join(args[1:]), ctx)
        if not ytd_dates:
            return self._eval_expr(expr, ctx)
        # Apply date filter
        if ytd_dates and isinstance(ytd_dates, list) and ytd_dates:
            first = ytd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in ytd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    def _fn_totalmtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALMTD(expression, dates) — month to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        mtd_dates = self._fn_datesmtd(args[1].strip(), ctx)
        if mtd_dates and isinstance(mtd_dates, list) and mtd_dates:
            first = mtd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in mtd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    def _fn_totalqtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALQTD(expression, dates) — quarter to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        qtd_dates = self._fn_datesqtd(args[1].strip(), ctx)
        if qtd_dates and isinstance(qtd_dates, list) and qtd_dates:
            first = qtd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in qtd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    # =========================================================================
    # Time Intelligence — Period Navigation
    # =========================================================================

    def _fn_previousmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSMONTH(dates) — dates from the previous month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
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
        max_date = max(dates)
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
        max_date = max(dates)
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
            q_start = shift_quarter(datetime(min_date.year, ((min_date.month - 1) // 3) * 3 + 1, 1), offset)
            q_end_month = q_start.month + 2
            q_end_year = q_start.year
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

    def _fn_startofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFMONTH(dates) — first date of the month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        start = datetime(min_date.year, min_date.month, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFMONTH(dates) — last date of the month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        _, last_day = monthrange(max_date.year, max_date.month)
        end = datetime(max_date.year, max_date.month, last_day)
        return self._make_date_table_result(table_name, col_name, [end])

    def _fn_startofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFQUARTER(dates) — first date of the quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        q_start_month = ((min_date.month - 1) // 3) * 3 + 1
        start = datetime(min_date.year, q_start_month, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFQUARTER(dates) — last date of the quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        q_end_month = ((max_date.month - 1) // 3 + 1) * 3
        _, last_day = monthrange(max_date.year, q_end_month)
        end = datetime(max_date.year, q_end_month, last_day)
        return self._make_date_table_result(table_name, col_name, [end])

    def _fn_startofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFYEAR(dates) — first date of the year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        start = datetime(min_date.year, 1, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFYEAR(dates) — last date of the year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        end = datetime(max_date.year, 12, 31)
        return self._make_date_table_result(table_name, col_name, [end])

    # =========================================================================
    # Time Intelligence — Opening/Closing Balance
    # =========================================================================

    def _fn_openingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEMONTH(expression, dates, filter) — evaluate at last date of previous month."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        # End of previous month
        eop = datetime(min_date.year, min_date.month, 1) - timedelta(days=1)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eop.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEMONTH(expression, dates, filter) — evaluate at last date of current month."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        _, last_day = monthrange(max_date.year, max_date.month)
        eom = datetime(max_date.year, max_date.month, last_day)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eom.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_openingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEQUARTER(expression, dates, filter) — evaluate at last date before quarter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        q_start_month = ((min_date.month - 1) // 3) * 3 + 1
        eoq = datetime(min_date.year, q_start_month, 1) - timedelta(days=1)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoq.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEQUARTER(expression, dates, filter) — evaluate at last date of quarter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        q_end_month = ((max_date.month - 1) // 3 + 1) * 3
        _, last_day = monthrange(max_date.year, q_end_month)
        eoq = datetime(max_date.year, q_end_month, last_day)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoq.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_openingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEYEAR(expression, dates, filter) — evaluate at last date of previous year."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        eoy = datetime(min_date.year - 1, 12, 31)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoy.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEYEAR(expression, dates, filter) — evaluate at last date of year."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        eoy = datetime(max_date.year, 12, 31)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoy.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

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
        elif interval in ('MONTH', 'MONTHS'):
            new_month = start_date.month + offset
            new_year = start_date.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            if offset >= 0:
                _, last_day = monthrange(new_year, new_month)
                end_date = datetime(new_year, new_month, min(start_date.day, last_day))
            else:
                end_date = start_date
                _, last_day = monthrange(new_year, new_month)
                start_date = datetime(new_year, new_month, min(start_date.day, last_day))
        elif interval in ('QUARTER', 'QUARTERS'):
            new_month = start_date.month + offset * 3
            new_year = start_date.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            if offset >= 0:
                _, last_day = monthrange(new_year, new_month)
                end_date = datetime(new_year, new_month, min(start_date.day, last_day))
            else:
                end_date = start_date
                _, last_day = monthrange(new_year, new_month)
                start_date = datetime(new_year, new_month, min(start_date.day, last_day))
        elif interval in ('YEAR', 'YEARS'):
            if offset >= 0:
                end_date = datetime(start_date.year + offset, start_date.month, start_date.day)
            else:
                end_date = start_date
                start_date = datetime(start_date.year + offset, start_date.month, start_date.day)
        else:
            return []

        if start_date > end_date:
            start_date, end_date = end_date, start_date

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

_engine = DAXEngine()


def evaluate_measure(measure_name: str, tables: dict, measures: dict,
                     filter_context: dict | None = None,
                     date_table: str | None = None, date_column: str | None = None,
                     relationships: list | None = None) -> Any:
    """Evaluate a single DAX measure."""
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    return _engine.evaluate_measure(measure_name, ctx)


def evaluate_measures_batch(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict | None = None,
                            date_table: str | None = None, date_column: str | None = None,
                            relationships: list | None = None) -> dict:
    """Evaluate multiple measures, returning { name: value }."""
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    results = {}
    for name in measure_names:
        results[name] = _engine.evaluate_measure(name, ctx)
    return results


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
            ok = True
            for val in unique_vals:
                probe = DAXContext(tables, measures, date_table, date_column,
                                   {dimension: [val]}, relationships)
                cross = probe._get_cross_table_filters(ftbl_name)
                if len(cross) != 1:
                    ok = False
                    break
                allowed, idx = cross[0]
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
                v = key_to_value.get(str(row[fact_col_idx]))
                if v is not None:
                    buckets.setdefault(v, []).append(row)
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


def evaluate_measures_smart(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict | None = None,
                            date_table: str | None = None, date_column: str | None = None,
                            relationships: list | None = None) -> dict:
    """Evaluate measures with smart fallback for SELECTEDVALUE-dependent measures.

    When a measure returns BLANK and its expression uses SELECTEDVALUE on a
    parameter table, this tries evaluating with each possible value to find
    a non-BLANK result. This simulates what Power BI does when a visual
    provides row context for a parameter table.
    """
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    results = {}

    for name in measure_names:
        val = _engine.evaluate_measure(name, ctx)
        if val is not None:
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

            unique_vals = list(set(row[col_idx] for row in tbl['rows'] if row[col_idx] is not None))
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
