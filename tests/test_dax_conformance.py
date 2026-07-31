"""Per-function conformance against Power BI Desktop.

``tests/conformance/golden.json`` holds, for every probe expression, exactly
what live Desktop returned for it against the fixture model (see
``tools/dax_conformance``). This test evaluates the SAME expressions in our
engine over the SAME rows and requires a match.

The contract is strict on purpose: a probe with a golden VALUE must produce
that value (1e-9 relative for floats); a probe Desktop itself errored on is
skipped (out of authorable scope); and there is no "unsupported" escape hatch
-- an unimplemented function fails its probes, which is what makes this file
the ratchet toward full-surface parity.
"""
from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ""))
from tools.dax_conformance.fixture_def import (  # noqa: E402
    FIXTURE_RELATIONSHIPS,
    FIXTURE_TABLES,
)

from pbix_mcp.dax import engine as de  # noqa: E402

GOLDEN = os.path.join(os.path.dirname(__file__), "conformance", "golden.json")

with open(GOLDEN, encoding="utf-8") as fh:
    _golden = json.load(fh)

CASES = []
for func, rows in sorted(_golden.items()):
    for i, rec in enumerate(rows):
        CASES.append(pytest.param(
            func, rec, id=f"{func}-{i}",
            marks=[] if "error" not in rec else [pytest.mark.skip(
                reason="Desktop itself refuses this probe")]))


def _ev(expr):
    ctx = de.DAXContext(
        {k: {"columns": v["columns"], "rows": [list(r) for r in v["rows"]]}
         for k, v in FIXTURE_TABLES.items()},
        {"__probe__": expr}, None, None, None,
        [dict(r) for r in FIXTURE_RELATIONSHIPS])
    return de.DAXEngine().evaluate_measure("__probe__", ctx)


_NUM_RE = re.compile(r"^-?\d+(\.\d+)?([eE][-+]?\d+)?$")
# Desktop prints dates in the capture as M/d/yyyy h:mm:ss (locale-shaped) or
# ISO; accept both.
_DATE_FMTS = ("%m/%d/%Y %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
              "%Y-%m-%d %H:%M:%S")


def _parse_golden(text):
    if text is None:
        return None
    t = text.strip()
    if _NUM_RE.match(t):
        return float(t)
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(t, fmt)
        except ValueError:
            continue
    if t in ("True", "False"):
        return t == "True"
    return t


@pytest.mark.parametrize("func,rec", CASES)
def test_probe_matches_desktop(func, rec):
    want = _parse_golden(rec.get("value"))
    got = _ev(rec["expr"])
    if want is None:
        assert got is None, f"{rec['expr']}: Desktop BLANK, ours {got!r}"
        return
    if isinstance(want, float):
        # The capture prints values without type: FIXED() returns TEXT "1200",
        # which the golden parser reads as 1200.0. A numeric-looking string
        # from our engine is compared by value, not rejected by type.
        if isinstance(got, str) and _NUM_RE.match(got.replace(",", "").strip()):
            got = float(got.replace(",", ""))
        assert isinstance(got, (int, float)) and not isinstance(got, bool), (
            f"{rec['expr']}: Desktop {want!r}, ours {got!r}")
        if want == 0:
            assert abs(float(got)) < 1e-12, f"{rec['expr']}: {got!r} != 0"
        else:
            rel = abs(float(got) - want) / abs(want)
            assert rel < 1e-9, (
                f"{rec['expr']}: Desktop {want!r}, ours {got!r} (rel {rel:.2e})")
        return
    if isinstance(want, datetime):
        assert isinstance(got, datetime), f"{rec['expr']}: {got!r}"
        assert (got.year, got.month, got.day, got.hour, got.minute,
                got.second) == (want.year, want.month, want.day, want.hour,
                                want.minute, want.second), (
            f"{rec['expr']}: Desktop {want}, ours {got}")
        return
    if isinstance(want, bool):
        assert got is want or got == want, f"{rec['expr']}: {want!r} vs {got!r}"
        return
    assert str(got) == str(want), f"{rec['expr']}: {want!r} vs {got!r}"
