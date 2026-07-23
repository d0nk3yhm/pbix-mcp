"""DAX codegen for SVG data-URI image measures (Rail-A rich content).

Each template emits a DAX expression that evaluates to a
``data:image/svg+xml;utf8,<svg …>`` string. Marked ``DataCategory =
'ImageUrl'``, such a measure renders as a live vector image in table/matrix
cells — in Power BI Desktop AND the service (service-verified), PDF export,
and subscriptions — with zero custom visuals. Because the SVG is computed by
DAX it stays live under any filter context.

Authoring rules baked into every template:

- ``utf8`` encoding, never base64 (base64 wastes ~33% of the ~32k
  Analysis-Services text budget — see ``_DAX_STRING_MAX`` in server.py).
- ``#`` is a URI fragment delimiter, so colors are percent-encoded
  (``#2E86DE`` → ``%232E86DE``).
- SVG attributes use single quotes — inside a DAX string literal that avoids
  doubling every double-quote.
- Numeric interpolation goes through ``FORMAT(INT(…), "0")`` so a
  comma-decimal locale can never inject ``,`` into coordinates.
- Value expressions are clamped with DIVIDE/IF (no two-scalar MIN/MAX), so
  the output is valid for Desktop, the service, and pbix-mcp's own engine.

Templates take DAX sub-expressions (e.g. ``[Total Revenue]`` or
``DIVIDE([X],[Y])``) for the dynamic parts and Python values for the static
styling. ``render(kind, spec)`` returns the DAX string.
"""

from __future__ import annotations

import re
from typing import Callable

# Palette matching html_templates.py
ACCENT = "#2E86DE"
GOOD = "#188038"
BAD = "#D93025"
MUTED = "#5F6368"
TRACK = "#E8ECF2"
INK = "#202124"
FONT = "Segoe UI,Arial,sans-serif"

_COLOR_RE = re.compile(r"^(#[0-9A-Fa-f]{3,8}|[A-Za-z]{3,20})$")


def _c(color: str) -> str:
    """Validate a color and percent-encode '#' for a utf8 data URI."""
    if not _COLOR_RE.match(color or ""):
        raise ValueError(f"Invalid color {color!r} — use #RRGGBB or a name")
    return color.replace("#", "%23")


def _i(v, name: str, lo: int = 1, hi: int = 2000) -> int:
    try:
        iv = int(v)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer, got {v!r}")
    if not lo <= iv <= hi:
        raise ValueError(f"{name} must be {lo}..{hi}, got {iv}")
    return iv


def _dax(expr, name: str) -> str:
    """A DAX sub-expression parameter: non-empty, brace/quote sane."""
    s = str(expr or "").strip()
    if not s:
        raise ValueError(f"{name} requires a DAX expression (e.g. \"[Total Revenue]\")")
    return s


def _px(expr: str) -> str:
    """Locale-proof integer-pixel interpolation of a DAX numeric expression."""
    return f'FORMAT(INT({expr}), "0")'


def _clamp01(value: str, max_value: str) -> str:
    """DAX for DIVIDE(value,max) clamped to 0..1 (BLANK -> 0)."""
    return (f"VAR _r0 = DIVIDE({value}, {max_value})\n"
            f"VAR _r = IF(ISBLANK(_r0), 0, IF(_r0 < 0, 0, IF(_r0 > 1, 1, _r0)))")


# Runtime text escaping: URI hygiene first ('%' before '#' — a raw '#' starts
# the URI fragment and truncates the image; '%' would corrupt the escapes),
# then XML entities.
_XMLESC = ('SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE(SUBSTITUTE({t}, '
           '"%", "%25"), "#", "%23"), "&", "&amp;"), '
           '"<", "&lt;"), ">", "&gt;")')


def data_bar(value, max_value, fill: str = ACCENT, track: str = TRACK,
             width=120, height=16, radius=4) -> str:
    """Proportional horizontal bar: value/max_value of the width filled."""
    v, m = _dax(value, "value"), _dax(max_value, "max_value")
    w, h = _i(width, "width"), _i(height, "height")
    r = _i(radius, "radius", 0, h)
    return (
        f"{_clamp01(v, m)}\n"
        f"RETURN \"data:image/svg+xml;utf8,\"\n"
        f"    & \"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' "
        f"height='{h}' viewBox='0 0 {w} {h}'>\"\n"
        f"    & \"<rect width='{w}' height='{h}' rx='{r}' "
        f"fill='{_c(track)}'/>\"\n"
        f"    & \"<rect width='\" & {_px(f'_r * {w}')} & \"' height='{h}' "
        f"rx='{r}' fill='{_c(fill)}'/>\"\n"
        f"    & \"</svg>\""
    )


def bullet(value, target, max_value, fill: str = ACCENT, track: str = TRACK,
           marker: str = INK, width=120, height=16) -> str:
    """Bullet chart: proportional bar plus a target tick line."""
    v = _dax(value, "value")
    t = _dax(target, "target")
    m = _dax(max_value, "max_value")
    w, h = _i(width, "width"), _i(height, "height")
    return (
        f"{_clamp01(v, m)}\n"
        f"VAR _t0 = DIVIDE({t}, {m})\n"
        f"VAR _t = IF(ISBLANK(_t0), 0, IF(_t0 < 0, 0, IF(_t0 > 1, 1, _t0)))\n"
        f"RETURN \"data:image/svg+xml;utf8,\"\n"
        f"    & \"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' "
        f"height='{h}' viewBox='0 0 {w} {h}'>\"\n"
        f"    & \"<rect width='{w}' height='{h}' rx='2' fill='{_c(track)}'/>\"\n"
        f"    & \"<rect width='\" & {_px(f'_r * {w}')} & \"' height='{h}' "
        f"rx='2' fill='{_c(fill)}'/>\"\n"
        f"    & \"<rect x='\" & {_px(f'_t * {w - 2}')} & \"' width='2' "
        f"height='{h}' fill='{_c(marker)}'/>\"\n"
        f"    & \"</svg>\""
    )


def pill(text, fill: str = ACCENT, color: str = "#FFFFFF",
         width=90, height=22, font_size=11) -> str:
    """Rounded badge with centered text (text = a DAX string expression,
    e.g. FORMAT([Growth], "+0.0%;-0.0%")). The text is XML-escaped."""
    t = _dax(text, "text")
    w, h = _i(width, "width"), _i(height, "height")
    fs = _i(font_size, "font_size", 4, h)
    esc = _XMLESC.format(t=f"({t})")
    return (
        f"\"data:image/svg+xml;utf8,\"\n"
        f"    & \"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' "
        f"height='{h}' viewBox='0 0 {w} {h}'>\"\n"
        f"    & \"<rect width='{w}' height='{h}' rx='{h // 2}' "
        f"fill='{_c(fill)}'/>\"\n"
        f"    & \"<text x='{w // 2}' y='{(h + fs) // 2 - 1}' "
        f"text-anchor='middle' font-family='{FONT}' font-size='{fs}' "
        f"fill='{_c(color)}'>\" & {esc} & \"</text>\"\n"
        f"    & \"</svg>\""
    )


def icon_updown(value, up_color: str = GOOD, down_color: str = BAD,
                flat_color: str = MUTED, size=16) -> str:
    """Up/down/flat arrow by the sign of ``value``."""
    v = _dax(value, "value")
    s = _i(size, "size")
    m = s // 2
    up = f"<path d='M{m} 2 L{s - 2} {s - 4} L2 {s - 4} Z' fill='{_c(up_color)}'/>"
    dn = f"<path d='M{m} {s - 2} L{s - 2} 4 L2 4 Z' fill='{_c(down_color)}'/>"
    fl = (f"<rect x='2' y='{m - 1}' width='{s - 4}' height='3' rx='1' "
          f"fill='{_c(flat_color)}'/>")
    return (
        f"VAR _v = {v}\n"
        f"VAR _shape = IF(ISBLANK(_v) || _v = 0, \"{fl}\",\n"
        f"    IF(_v > 0, \"{up}\", \"{dn}\"))\n"
        f"RETURN \"data:image/svg+xml;utf8,\"\n"
        f"    & \"<svg xmlns='http://www.w3.org/2000/svg' width='{s}' "
        f"height='{s}' viewBox='0 0 {s} {s}'>\" & _shape & \"</svg>\""
    )


def sparkline(table, category, value, stroke: str = ACCENT,
              width=120, height=28, stroke_width=2) -> str:
    """Polyline sparkline of ``value`` per ``category`` (ordered ascending).

    table/category name a model table+column (bare names, e.g. table="Sales",
    category="Month Index" — the category should sort naturally, so prefer a
    numeric index or date column); value is a DAX expression evaluated in each
    category's context (a measure like [Total Revenue]).
    """
    tbl = _dax(table, "table")
    if tbl.startswith("'") and tbl.endswith("'") and len(tbl) > 1:
        tbl = tbl[1:-1].replace("''", "'")
    cat = _dax(category, "category")
    val = _dax(value, "value")
    w, h = _i(width, "width"), _i(height, "height")
    sw = _i(stroke_width, "stroke_width", 1, 10)
    pad = sw + 1
    # DAX-escape an apostrophe in the table name ('O''Brien'[Idx])
    col = f"'{tbl.replace(chr(39), chr(39) * 2)}'[{cat}]"
    return (
        f"VAR _pts = ADDCOLUMNS(VALUES({col}), \"@v\", CALCULATE({val}))\n"
        f"VAR _n = COUNTROWS(_pts)\n"
        f"VAR _min = MINX(_pts, [@v])\n"
        f"VAR _max = MAXX(_pts, [@v])\n"
        f"VAR _rng = IF(_max = _min, 1, _max - _min)\n"
        f"VAR _line = CONCATENATEX(_pts,\n"
        f"    {_px(f'DIVIDE(RANKX(_pts, {col}, {col}, ASC, DENSE) - 1, IF(_n <= 1, 1, _n - 1)) * {w - 2 * pad} + {pad}')}\n"
        f"    & \",\" & {_px(f'{h - pad} - DIVIDE([@v] - _min, _rng) * {h - 2 * pad}')},\n"
        f"    \" \", {col}, ASC)\n"
        f"RETURN \"data:image/svg+xml;utf8,\"\n"
        f"    & \"<svg xmlns='http://www.w3.org/2000/svg' width='{w}' "
        f"height='{h}' viewBox='0 0 {w} {h}'>\"\n"
        f"    & \"<polyline points='\" & _line & \"' fill='none' "
        f"stroke='{_c(stroke)}' stroke-width='{sw}' "
        f"stroke-linecap='round' stroke-linejoin='round'/>\"\n"
        f"    & \"</svg>\""
    )


# kind -> (builder, allowed spec keys)
TEMPLATES: dict[str, tuple[Callable[..., str], list[str]]] = {
    "data_bar": (data_bar, ["value", "max_value", "fill", "track",
                            "width", "height", "radius"]),
    "bullet": (bullet, ["value", "target", "max_value", "fill", "track",
                        "marker", "width", "height"]),
    "pill": (pill, ["text", "fill", "color", "width", "height", "font_size"]),
    "icon_updown": (icon_updown, ["value", "up_color", "down_color",
                                  "flat_color", "size"]),
    "sparkline": (sparkline, ["table", "category", "value", "stroke",
                              "width", "height", "stroke_width"]),
}


def render(kind: str, spec: dict) -> str:
    """Render a template kind with a spec dict → DAX expression string."""
    if kind not in TEMPLATES:
        raise ValueError(
            f"Unknown template '{kind}'. Available: {', '.join(sorted(TEMPLATES))}")
    fn, allowed = TEMPLATES[kind]
    unknown = set(spec) - set(allowed)
    if unknown:
        raise ValueError(
            f"Template '{kind}' accepts {allowed}; unknown: {sorted(unknown)}")
    return fn(**spec)
