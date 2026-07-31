"""The conformance fixture model, defined ONCE.

``build_fixture.py`` turns this into ``test_corpus/conformance_fixture.pbix``
(built with pbix-mcp's own builder — the file Desktop opens IS a product of the
code under test), and ``tests/test_dax_conformance.py`` feeds the same rows to
the engine directly. One definition, so the two sides cannot drift.

Everything is deterministic and tiny: goldens captured from Desktop against
this model stay valid until this file changes, and a probe over five rows is
checkable by hand when a mismatch needs debugging.
"""
from __future__ import annotations

from datetime import datetime

# ---------------------------------------------------------------------------
# Tables. Values chosen to exercise sign, zero, blanks and non-integers, and
# to stay INSIDE every function's domain when used by the standard probes
# (e.g. x in (-0.9, 0.9) works for ASIN/ATANH; positive i works for LOG/SQRT).
# ---------------------------------------------------------------------------

N_ROWS = [
    # x,      i,  b,      s
    [0.25,    1,  True,   "alpha"],
    [0.5,     2,  False,  "Beta"],
    [-0.75,   3,  True,   "gamma"],
    [0.9,     4,  False,  "delta"],
    [None,    5,  True,   "Epsilon"],
]

D_ROWS = [
    # Date,                    M, Q
    [datetime(2024, 1, 15), 1, 1],
    [datetime(2024, 2, 15), 2, 1],
    [datetime(2024, 3, 15), 3, 1],
    [datetime(2024, 4, 15), 4, 2],
    [datetime(2024, 5, 15), 5, 2],
    [datetime(2024, 6, 15), 6, 2],
]

F_ROWS = [
    # k, v,      d
    [1, 100.0, datetime(2024, 1, 15)],
    [2, 200.0, datetime(2024, 2, 15)],
    [1, 50.0,  datetime(2024, 4, 15)],
    [3, 300.0, datetime(2024, 5, 15)],
]

K_ROWS = [
    # k, grp
    [1, "X"],
    [2, "X"],
    [3, "Y"],
    [4, "Z"],          # no fact rows -> tests blank-member behaviour
]

FIXTURE_TABLES = {
    "N": {"columns": ["x", "i", "b", "s"], "rows": N_ROWS},
    "D": {"columns": ["Date", "M", "Q"], "rows": D_ROWS},
    "F": {"columns": ["k", "v", "d"], "rows": F_ROWS},
    "K": {"columns": ["k", "grp"], "rows": K_ROWS},
}

FIXTURE_RELATIONSHIPS = [
    {"FromTable": "F", "FromColumn": "k", "ToTable": "K", "ToColumn": "k",
     "IsActive": 1},
    {"FromTable": "F", "FromColumn": "d", "ToTable": "D", "ToColumn": "Date",
     "IsActive": 1},
]

# data types for the builder (engine-side dicts carry python values directly)
BUILDER_TABLES = [
    ("N", [{"name": "x", "data_type": "Double"},
           {"name": "i", "data_type": "Int64"},
           {"name": "b", "data_type": "Boolean"},
           {"name": "s", "data_type": "String"}], N_ROWS),
    ("D", [{"name": "Date", "data_type": "DateTime"},
           {"name": "M", "data_type": "Int64"},
           {"name": "Q", "data_type": "Int64"}], D_ROWS),
    ("F", [{"name": "k", "data_type": "Int64"},
           {"name": "v", "data_type": "Double"},
           {"name": "d", "data_type": "DateTime"}], F_ROWS),
    ("K", [{"name": "k", "data_type": "Int64"},
           {"name": "grp", "data_type": "String"}], K_ROWS),
]

FIXTURE_PBIX = "test_corpus/conformance_fixture.pbix"
