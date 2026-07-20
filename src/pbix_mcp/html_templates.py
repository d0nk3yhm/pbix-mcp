"""Professional HTML / CSS / SVG snippet builders for the PBIX HTML custom visual.

These produce self-contained HTML strings (inline ``<style>`` / inline ``<svg>``,
no external URLs — the visual sandbox blocks them) suitable for
``pbix_add_html_visual(html=...)``. Every value that can carry user text is
HTML-escaped, so injecting a caption like ``A & B <x>`` can never break the
markup or the surrounding DAX string literal.

Design goals: look like a real Power BI report (Segoe UI, restrained palette,
rounded cards), be fully responsive (``%``/viewBox units, ``max-width:100%``),
and stay well under the ~32 KB text-cell limit.
"""
from __future__ import annotations

import html as _html

# A calm, professional default palette (Power BI "Modern" blue family).
ACCENT = "#2E86DE"
ACCENT_DARK = "#1B4F8A"
INK = "#1B2A3A"
MUTED = "#6B7A8D"
TRACK = "#E3ECF5"
FONT = "Segoe UI,Arial,sans-serif"


def esc(value) -> str:
    """HTML-escape a value for safe interpolation into markup (quotes included)."""
    return _html.escape("" if value is None else str(value), quote=True)


def _num(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _clamp_pct(x) -> float:
    return max(0.0, min(100.0, _num(x)))


def kpi_card(title: str, value, subtitle: str = "", accent: str = ACCENT,
             spark: list | None = None) -> str:
    """A gradient KPI card with a big value and an optional SVG sparkline.

    ``spark`` is an optional list of numbers rendered as a trend line.
    """
    accent = esc(accent)
    spark_svg = ""
    pts = [_num(v) for v in (spark or [])]
    if len(pts) >= 2:
        lo, hi = min(pts), max(pts)
        rng = (hi - lo) or 1.0
        n = len(pts)
        coords = " ".join(
            f"{i * (200 / (n - 1)):.1f},{34 - (v - lo) / rng * 28:.1f}"
            for i, v in enumerate(pts))
        spark_svg = (
            "<svg viewBox='0 0 200 40' preserveAspectRatio='none' "
            "style='width:100%;height:38px;margin-top:10px;'>"
            f"<polyline fill='none' stroke='rgba(255,255,255,.85)' "
            f"stroke-width='3' points='{coords}'/></svg>")
    sub = (f"<div style='font-size:12px;opacity:.85;margin-top:6px;'>{esc(subtitle)}</div>"
           if subtitle else "")
    return (
        f"<div style=\"font-family:{FONT};height:100%;box-sizing:border-box;"
        f"background:linear-gradient(135deg,{accent},{esc(ACCENT_DARK)});color:#fff;"
        "border-radius:14px;padding:18px 22px;display:flex;flex-direction:column;"
        "justify-content:center;overflow:hidden;\">"
        f"<div style='font-size:13px;letter-spacing:.14em;opacity:.85;'>{esc(title)}</div>"
        f"<div style='font-size:clamp(28px,7vw,52px);font-weight:800;line-height:1.05;'>{esc(value)}</div>"
        f"{sub}{spark_svg}</div>")


def badge(text: str, color: str = ACCENT, filled: bool = True) -> str:
    """A small status pill."""
    color = esc(color)
    style = (f"background:{color};color:#fff;" if filled
             else f"background:transparent;color:{color};border:1.5px solid {color};")
    return (f"<span style=\"font-family:{FONT};display:inline-block;padding:3px 12px;"
            f"border-radius:999px;font-size:12px;font-weight:700;{style}\">{esc(text)}</span>")


def svg_bar_chart(title: str, items: list, accent: str = ACCENT,
                  value_suffix: str = "") -> str:
    """Horizontal SVG bar chart. ``items`` = list of ``(label, value)`` or
    ``(label, value, max)``; bars scale to the largest value (or the given max)."""
    accent = esc(accent)
    rows = []
    parsed = []
    for it in items:
        label = it[0]
        val = _num(it[1])
        mx = _num(it[2]) if len(it) > 2 else None
        parsed.append((label, val, mx))
    peak = max([p[2] or p[1] for p in parsed] + [1.0])
    row_h = 34
    height = max(row_h * len(parsed) + 10, 40)
    for i, (label, val, mx) in enumerate(parsed):
        base = mx or peak
        frac = 0.0 if base <= 0 else max(0.0, min(1.0, val / base))
        y = i * row_h + 8
        bar_w = 210 * frac
        rows.append(
            f"<text x='0' y='{y + 15}' font-size='12' fill='{esc(INK)}'>{esc(label)}</text>"
            f"<rect x='46' y='{y + 3}' width='210' height='18' rx='4' fill='{esc(TRACK)}'/>"
            f"<rect x='46' y='{y + 3}' width='{bar_w:.1f}' height='18' rx='4' fill='{accent}'/>"
            f"<text x='262' y='{y + 15}' font-size='12' fill='{esc(MUTED)}'>"
            f"{esc(_fmt(val) + value_suffix)}</text>")
    return (
        f"<div style=\"font-family:{FONT};padding:14px 16px;height:100%;box-sizing:border-box;\">"
        f"<div style='font-weight:700;font-size:15px;margin-bottom:8px;color:{esc(ACCENT_DARK)};'>{esc(title)}</div>"
        f"<svg viewBox='0 0 300 {height}' style='width:100%;height:calc(100% - 30px);'>"
        f"<g font-family='{FONT}'>{''.join(rows)}</g></svg></div>")


def svg_donut_gauge(title: str, percent, accent: str = ACCENT,
                    center_label: str = "") -> str:
    """A 180° SVG gauge showing ``percent`` (0–100)."""
    accent = esc(accent)
    pct = _clamp_pct(percent)
    # semicircle arc from (10,60) to (110,60), radius 50; sweep proportional to pct
    import math
    ang = math.pi * (1 - pct / 100.0)
    ex = 60 + 50 * math.cos(ang)
    ey = 60 - 50 * math.sin(ang)
    large = 0  # always <=180deg
    label = esc(center_label) if center_label else f"{pct:.0f}%"
    return (
        f"<div style=\"font-family:{FONT};text-align:center;padding:10px;height:100%;box-sizing:border-box;\">"
        f"<div style='font-weight:700;color:{esc(ACCENT_DARK)};margin-bottom:4px;'>{esc(title)}</div>"
        "<svg viewBox='0 0 120 72' style='width:100%;height:calc(100% - 24px);'>"
        f"<path d='M10,60 A50,50 0 0,1 110,60' fill='none' stroke='{esc(TRACK)}' stroke-width='14'/>"
        f"<path d='M10,60 A50,50 0 {large},1 {ex:.2f},{ey:.2f}' fill='none' stroke='{accent}' "
        "stroke-width='14' stroke-linecap='round'/>"
        f"<text x='60' y='54' text-anchor='middle' font-size='22' font-weight='800' "
        f"fill='{esc(ACCENT_DARK)}'>{label}</text></svg></div>")


def data_table(headers: list, rows: list, accent: str = ACCENT_DARK,
               align_right_from: int = 1) -> str:
    """A styled HTML table. Columns from ``align_right_from`` onward are right-aligned."""
    accent = esc(accent)
    ths = "".join(
        f"<th style='text-align:{'right' if i >= align_right_from else 'left'};"
        f"padding:7px 10px;'>{esc(h)}</th>" for i, h in enumerate(headers))
    trs = []
    for r, row in enumerate(rows):
        bg = "#F6FAFE" if r % 2 else "#fff"
        tds = "".join(
            f"<td style='text-align:{'right' if i >= align_right_from else 'left'};"
            f"padding:6px 10px;border-bottom:1px solid {esc(TRACK)};'>{esc(c)}</td>"
            for i, c in enumerate(row))
        trs.append(f"<tr style='background:{bg};'>{tds}</tr>")
    return (
        f"<div style=\"font-family:{FONT};padding:10px;height:100%;box-sizing:border-box;overflow:auto;\">"
        "<table style='width:100%;border-collapse:collapse;font-size:13px;'>"
        f"<thead><tr style='background:{accent};color:#fff;'>{ths}</tr></thead>"
        f"<tbody>{''.join(trs)}</tbody></table></div>")


def progress_list(title: str, items: list, accent: str = ACCENT) -> str:
    """A titled list of labelled progress bars. ``items`` = ``(label, percent)``."""
    accent = esc(accent)
    blocks = []
    for label, pct in items:
        p = _clamp_pct(pct)
        blocks.append(
            f"<div style='margin:8px 0;'>"
            f"<div style='display:flex;justify-content:space-between;font-size:12px;"
            f"color:{esc(INK)};margin-bottom:3px;'><span>{esc(label)}</span>"
            f"<span style='color:{esc(MUTED)};'>{p:.0f}%</span></div>"
            f"<div style='background:{esc(TRACK)};border-radius:999px;height:9px;'>"
            f"<div style='width:{p:.1f}%;background:{accent};height:9px;border-radius:999px;'></div>"
            "</div></div>")
    return (f"<div style=\"font-family:{FONT};padding:14px 16px;height:100%;box-sizing:border-box;\">"
            f"<div style='font-weight:700;font-size:15px;margin-bottom:6px;color:{esc(ACCENT_DARK)};'>{esc(title)}</div>"
            f"{''.join(blocks)}</div>")


def _fmt(v) -> str:
    """Compact number formatting for chart value labels."""
    n = _num(v)
    if n == int(n):
        return f"{int(n):,}"
    return f"{n:,.1f}"


# Registry for the pbix_html_template MCP tool. Each entry maps a kind name to a
# builder + the spec keys it accepts (for docs/validation).
TEMPLATES = {
    "kpi_card": (kpi_card, ["title", "value", "subtitle", "accent", "spark"]),
    "badge": (badge, ["text", "color", "filled"]),
    "bar_chart": (svg_bar_chart, ["title", "items", "accent", "value_suffix"]),
    "gauge": (svg_donut_gauge, ["title", "percent", "accent", "center_label"]),
    "table": (data_table, ["headers", "rows", "accent", "align_right_from"]),
    "progress": (progress_list, ["title", "items", "accent"]),
}


def render(kind: str, spec: dict) -> str:
    """Render a named template from a spec dict. Raises ValueError on unknown kind
    or on a spec key the template doesn't accept."""
    if kind not in TEMPLATES:
        raise ValueError(
            f"Unknown template '{kind}'. Available: {', '.join(sorted(TEMPLATES))}")
    fn, allowed = TEMPLATES[kind]
    unknown = set(spec) - set(allowed)
    if unknown:
        raise ValueError(
            f"Template '{kind}' got unexpected keys {sorted(unknown)}; "
            f"accepts {allowed}")
    return fn(**spec)
