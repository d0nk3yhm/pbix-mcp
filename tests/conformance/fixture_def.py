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

# PC is a CALCULATED table in the built fixture. Desktop recomputes and fully
# processes calculated tables at open, so their hierarchy support structures
# are PATH-queryable. Builder-written IMPORT tables lack processed H$ support
# structures (PATH says "not processed"; every other query works) — a real
# builder gap tracked separately. Column order (id, label, parent) is what
# ADDCOLUMNS produces. The root's parent must be a true BLANK: a BLANK()
# inside a DATATABLE row literal arrives as 0, which PATH rejects.
PC_ROWS = [
    # id, label,     parent
    [1, "root", None],
    [2, "child-a", 1],
    [3, "child-b", 1],
    [4, "grand", 2],
]
PC_CALC_DAX = (
    'ADDCOLUMNS(DATATABLE("id", INTEGER, "label", STRING, '
    '{{1, "root"}, {2, "child-a"}, {3, "child-b"}, {4, "grand"}}), '
    '"parent", IF([id] = 1, BLANK(), IF([id] = 4, 2, 1)))'
)

FIXTURE_TABLES = {
    "N": {"columns": ["x", "i", "b", "s"], "rows": N_ROWS},
    "D": {"columns": ["Date", "M", "Q"], "rows": D_ROWS},
    "F": {"columns": ["k", "v", "d"], "rows": F_ROWS},
    "K": {"columns": ["k", "grp"], "rows": K_ROWS},
    "PC": {"columns": ["id", "label", "parent"], "rows": PC_ROWS},
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
    # PC is NOT in BUILDER_TABLES: build_fixture.py adds it as a calculated
    # table (PC_CALC_DAX) after the import build — see the note above PC_ROWS.
]

FIXTURE_MEASURES = {"Total V": ("F", "SUM(F[v])")}

FIXTURE_PBIX = "test_corpus/conformance_fixture.pbix"
