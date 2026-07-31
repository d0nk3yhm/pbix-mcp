"""Emit the capture .dax for every probe, and parse a capture back to golden.

  python tools/dax_conformance/gen_capture.py emit <out.dax>
  python tools/dax_conformance/gen_capture.py parse <capture.txt> <golden.json>

The capture file is verify_live.ps1 output: for each query a ``Q:`` line, then
either column-header + value lines or an ``ERROR:`` line. A Desktop ERROR is a
real result -- it marks the probe (and, if every probe of a function errors,
the function) as not authorable in this context.
"""
from __future__ import annotations

import io
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from tests.conformance.probes import PROBES  # noqa: E402


def emit(out_path: str) -> None:
    lines = []
    for func, exprs in PROBES.items():
        for i, expr in enumerate(exprs):
            name = f"{func}__{i}"
            lines.append(f'EVALUATE ROW("{name}", {expr})')
    io.open(out_path, "w", encoding="utf-8", newline="\n").write("\n".join(lines) + "\n")
    print(f"emitted {len(lines)} probes for {len(PROBES)} functions -> {out_path}")


_Q_RE = re.compile(r'^\s*Q: EVALUATE ROW\("([A-Z0-9._]+)__(\d+)", (.*)\)\s*$')


def parse(capture_path: str, golden_path: str) -> None:
    golden: dict = {}
    cur = None            # (func, idx, expr)
    state = None          # None | 'header' | 'value'
    for raw in io.open(capture_path, encoding="utf-8", errors="replace"):
        line = raw.rstrip("\n")
        m = _Q_RE.match(line)
        if m:
            cur = (m.group(1), int(m.group(2)), m.group(3))
            state = "await"
            continue
        if cur is None:
            continue
        s = line.strip()
        if not s:
            continue
        func, idx, expr = cur
        rec = golden.setdefault(func, [])
        if s.startswith("ERROR:"):
            rec.append({"expr": expr, "error": s[len("ERROR:"):].strip()[:300]})
            cur = None
        elif state == "await" and s.startswith("["):
            state = "value"          # the column-header line
        elif state == "value":
            rec.append({"expr": expr, "value": s})
            cur = None
        # A query whose ROW returns BLANK prints header then nothing; the next
        # Q: line resets cur, and the probe is recorded on flush below.
    # flush headers-without-value as BLANK
    seen = {(f, r["expr"]) for f, rows in golden.items() for r in rows}
    for func, exprs in PROBES.items():
        for expr in exprs:
            if (func, expr) not in seen:
                golden.setdefault(func, []).append({"expr": expr, "value": None})
    n_err = sum(1 for rows in golden.values() for r in rows if "error" in r)
    n_val = sum(1 for rows in golden.values() for r in rows if "error" not in r)
    io.open(golden_path, "w", encoding="utf-8", newline="\n").write(
        json.dumps(golden, indent=1, sort_keys=True))
    print(f"golden: {len(golden)} functions, {n_val} values, {n_err} desktop-errors -> {golden_path}")


if __name__ == "__main__":
    if sys.argv[1] == "emit":
        emit(sys.argv[2])
    elif sys.argv[1] == "parse":
        parse(sys.argv[2], sys.argv[3])
