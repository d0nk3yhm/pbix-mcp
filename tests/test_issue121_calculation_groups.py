"""Issue #121: calculation groups are applied.

A filter that selects exactly one item of a calculation group replaces each
measure reference with the item's expression, SELECTEDMEASURE() being the
measure. Before, the group's table was read as an ordinary table and every
measure evaluated as if the group did not exist: Awesome Chocolates' Sales
Change read 34,042,511.25 where Desktop shows the QOQ change.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b121.py), one
EVALUATE ROW per cell, generated from its output. The rules they show:
  * one item selected applies it; several, or none, apply nothing;
  * an item applies once per reference made outside it: S2 = [S] under Double
    is 120, and Plus1 of SS = [S] + [S] is 121 (not 122);
  * the item's own references are plain: RefS = [S] * 10 is 600 everywhere;
  * the group of higher precedence is the outer one: Double with Add100 is
    Add100(Double(S)) = 220, Plus1 with Neg is -61;
  * SELECTEDMEASURENAME, ISSELECTEDMEASURE, SELECTEDMEASUREFORMATSTRING (BLANK
    for a measure without a format string).
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

CG_ITEMS = [   # precedence 0
    ("Base", "SELECTEDMEASURE()"),
    ("Double", "SELECTEDMEASURE() * 2"),
    ("Name", "SELECTEDMEASURENAME()"),
    ("IsS", "IF(ISSELECTEDMEASURE([S]), 1, 0)"),
    ("OnlyA", 'CALCULATE(SELECTEDMEASURE(), T[c] = "a")'),
    ("Plus1", "SELECTEDMEASURE() + 1"),
    ("RefS", "[S] * 10"),
    ("Fmt", "SELECTEDMEASUREFORMATSTRING()"),
]
CG2_ITEMS = [   # precedence 10
    ("Add100", "SELECTEDMEASURE() + 100"),
    ("Neg", "-SELECTEDMEASURE()"),
]
TABLES = {
    "T": {"columns": ["c", "v"], "rows": [["a", 10.0], ["b", 20.0], ["c", 30.0]]},
    "CG": {"columns": ["Name", "Ordinal"], "rows": [[n, i] for i, (n, _e) in enumerate(CG_ITEMS)]},
    "CG2": {"columns": ["Name", "Ordinal"], "rows": [[n, i] for i, (n, _e) in enumerate(CG2_ITEMS)]},
}
MEASURES = {
    "S": "SUM(T[v])",
    "S2": "[S]",
    "S3": "[S2] + 0",
    "Sb": 'CALCULATE([S], T[c] = "b")',
    "Cnt": "COUNTROWS(T)",
    "Txt": '"x"',
    "SS": "[S] + [S]",
}
GROUPS = [
    {"table": "CG", "column": "Name", "precedence": 0,
     "items": [{"name": n, "expression": e, "ordinal": i} for i, (n, e) in enumerate(CG_ITEMS)]},
    {"table": "CG2", "column": "Name", "precedence": 10,
     "items": [{"name": n, "expression": e, "ordinal": i} for i, (n, e) in enumerate(CG2_ITEMS)]},
]

DESKTOP = {   # Power BI Desktop 2.152 over ADOMD, EVALUATE ROW("v", CALCULATE([m], <selection>)) -- generated
    'none|S': 60, 'Base|S': 60, 'Base+Add100|S': 160, 'Base+Neg|S': -60,
    'Double|S': 120, 'Double+Add100|S': 220, 'Double+Neg|S': -120, 'Name|S': 'S',
    'Name+Add100|S': 'ERROR', 'Name+Neg|S': 'ERROR', 'IsS|S': 1, 'IsS+Add100|S': 101,
    'IsS+Neg|S': -1, 'OnlyA|S': 10, 'OnlyA+Add100|S': 110, 'OnlyA+Neg|S': -10,
    'Plus1|S': 61, 'Plus1+Add100|S': 161, 'Plus1+Neg|S': -61, 'RefS|S': 600,
    'RefS+Add100|S': 700, 'RefS+Neg|S': -600, 'Fmt|S': None, 'Fmt+Add100|S': 100,
    'Fmt+Neg|S': None, 'Add100|S': 160, 'Neg|S': -60, 'none|S2': 60,
    'Base|S2': 60, 'Base+Add100|S2': 160, 'Base+Neg|S2': -60, 'Double|S2': 120,
    'Double+Add100|S2': 220, 'Double+Neg|S2': -120, 'Name|S2': 'S2', 'Name+Add100|S2': 'ERROR',
    'Name+Neg|S2': 'ERROR', 'IsS|S2': 0, 'IsS+Add100|S2': 100, 'IsS+Neg|S2': 0,
    'OnlyA|S2': 10, 'OnlyA+Add100|S2': 110, 'OnlyA+Neg|S2': -10, 'Plus1|S2': 61,
    'Plus1+Add100|S2': 161, 'Plus1+Neg|S2': -61, 'RefS|S2': 600, 'RefS+Add100|S2': 700,
    'RefS+Neg|S2': -600, 'Fmt|S2': None, 'Fmt+Add100|S2': 100, 'Fmt+Neg|S2': None,
    'Add100|S2': 160, 'Neg|S2': -60, 'none|S3': 60, 'Base|S3': 60,
    'Base+Add100|S3': 160, 'Base+Neg|S3': -60, 'Double|S3': 120, 'Double+Add100|S3': 220,
    'Double+Neg|S3': -120, 'Name|S3': 'S3', 'Name+Add100|S3': 'ERROR', 'Name+Neg|S3': 'ERROR',
    'IsS|S3': 0, 'IsS+Add100|S3': 100, 'IsS+Neg|S3': 0, 'OnlyA|S3': 10,
    'OnlyA+Add100|S3': 110, 'OnlyA+Neg|S3': -10, 'Plus1|S3': 61, 'Plus1+Add100|S3': 161,
    'Plus1+Neg|S3': -61, 'RefS|S3': 600, 'RefS+Add100|S3': 700, 'RefS+Neg|S3': -600,
    'Fmt|S3': None, 'Fmt+Add100|S3': 100, 'Fmt+Neg|S3': None, 'Add100|S3': 160,
    'Neg|S3': -60, 'none|Sb': 20, 'Base|Sb': 20, 'Base+Add100|Sb': 120,
    'Base+Neg|Sb': -20, 'Double|Sb': 40, 'Double+Add100|Sb': 140, 'Double+Neg|Sb': -40,
    'Name|Sb': 'Sb', 'Name+Add100|Sb': 'ERROR', 'Name+Neg|Sb': 'ERROR', 'IsS|Sb': 0,
    'IsS+Add100|Sb': 100, 'IsS+Neg|Sb': 0, 'OnlyA|Sb': 20, 'OnlyA+Add100|Sb': 120,
    'OnlyA+Neg|Sb': -20, 'Plus1|Sb': 21, 'Plus1+Add100|Sb': 121, 'Plus1+Neg|Sb': -21,
    'RefS|Sb': 600, 'RefS+Add100|Sb': 700, 'RefS+Neg|Sb': -600, 'Fmt|Sb': None,
    'Fmt+Add100|Sb': 100, 'Fmt+Neg|Sb': None, 'Add100|Sb': 120, 'Neg|Sb': -20,
    'none|Cnt': 3, 'Base|Cnt': 3, 'Base+Add100|Cnt': 103, 'Base+Neg|Cnt': -3,
    'Double|Cnt': 6, 'Double+Add100|Cnt': 106, 'Double+Neg|Cnt': -6, 'Name|Cnt': 'Cnt',
    'Name+Add100|Cnt': 'ERROR', 'Name+Neg|Cnt': 'ERROR', 'IsS|Cnt': 0, 'IsS+Add100|Cnt': 100,
    'IsS+Neg|Cnt': 0, 'OnlyA|Cnt': 1, 'OnlyA+Add100|Cnt': 101, 'OnlyA+Neg|Cnt': -1,
    'Plus1|Cnt': 4, 'Plus1+Add100|Cnt': 104, 'Plus1+Neg|Cnt': -4, 'RefS|Cnt': 600,
    'RefS+Add100|Cnt': 700, 'RefS+Neg|Cnt': -600, 'Fmt|Cnt': None, 'Fmt+Add100|Cnt': 100,
    'Fmt+Neg|Cnt': None, 'Add100|Cnt': 103, 'Neg|Cnt': -3, 'none|Txt': 'x',
    'Base|Txt': 'x', 'Base+Add100|Txt': 'ERROR', 'Base+Neg|Txt': 'ERROR', 'Double|Txt': 'ERROR',
    'Double+Add100|Txt': 'ERROR', 'Double+Neg|Txt': 'ERROR', 'Name|Txt': 'Txt', 'Name+Add100|Txt': 'ERROR',
    'Name+Neg|Txt': 'ERROR', 'IsS|Txt': 0, 'IsS+Add100|Txt': 100, 'IsS+Neg|Txt': 0,
    'OnlyA|Txt': 'x', 'OnlyA+Add100|Txt': 'ERROR', 'OnlyA+Neg|Txt': 'ERROR', 'Plus1|Txt': 'ERROR',
    'Plus1+Add100|Txt': 'ERROR', 'Plus1+Neg|Txt': 'ERROR', 'RefS|Txt': 600, 'RefS+Add100|Txt': 700,
    'RefS+Neg|Txt': -600, 'Fmt|Txt': None, 'Fmt+Add100|Txt': 100, 'Fmt+Neg|Txt': None,
    'Add100|Txt': 'ERROR', 'Neg|Txt': 'ERROR', 'none|SS': 120, 'Base|SS': 120,
    'Base+Add100|SS': 220, 'Base+Neg|SS': -120, 'Double|SS': 240, 'Double+Add100|SS': 340,
    'Double+Neg|SS': -240, 'Name|SS': 'SS', 'Name+Add100|SS': 'ERROR', 'Name+Neg|SS': 'ERROR',
    'IsS|SS': 0, 'IsS+Add100|SS': 100, 'IsS+Neg|SS': 0, 'OnlyA|SS': 20,
    'OnlyA+Add100|SS': 120, 'OnlyA+Neg|SS': -20, 'Plus1|SS': 121, 'Plus1+Add100|SS': 221,
    'Plus1+Neg|SS': -121, 'RefS|SS': 600, 'RefS+Add100|SS': 700, 'RefS+Neg|SS': -600,
    'Fmt|SS': None, 'Fmt+Add100|SS': 100, 'Fmt+Neg|SS': None, 'Add100|SS': 220,
    'Neg|SS': -120,
}


def _measures():
    m = dict(MEASURES)
    de.set_calculation_groups(m, GROUPS)
    return m


def _fc(selection: str) -> dict:
    if selection == "none":
        return {}
    fc = {}
    for part in selection.split("+"):
        table = "CG2" if part in {n for n, _e in CG2_ITEMS} else "CG"
        fc[f"{table}.Name"] = [part]
    return fc


def _eval(measure, fc, measures=None):
    m = measures if measures is not None else _measures()
    de._engine.eval_errors.clear()
    return de.evaluate_measures_batch([measure], TABLES, m, fc)[measure]


VALUE_CELLS = sorted(k for k, v in DESKTOP.items() if v != "ERROR")
ERROR_CELLS = sorted(k for k, v in DESKTOP.items() if v == "ERROR")


@pytest.mark.parametrize("cell", VALUE_CELLS)
def test_matches_desktop(cell):
    selection, measure = cell.split("|")
    want = DESKTOP[cell]
    got = _eval(measure, _fc(selection))
    if isinstance(want, (int, float)):
        assert got == pytest.approx(want)
    else:
        assert got == want


@pytest.mark.parametrize("cell", ERROR_CELLS)
def test_text_arithmetic_is_an_error_not_a_value(cell):
    # Desktop refuses these cells (text in arithmetic: "x" * 2, "S" + 100):
    # no confident number may come back.
    selection, measure = cell.split("|")
    got = _eval(measure, _fc(selection))
    assert got is None
    assert measure in de._engine.eval_errors


def test_two_items_selected_apply_nothing():
    assert _eval("S", {"CG.Name": ["Double", "Plus1"]}) == 60


def test_grouped_by_another_column_under_a_treatas_item():
    # Desktop: SUMMARIZECOLUMNS(T[c], TREATAS({"Double"}, CG[Name]), "S", [S],
    # "S2", [S2], "Sb", [Sb]) -> a 20/20/40, b 40/40/40, c 60/60/40
    m = _measures()
    for c, want in (("a", (20, 20, 40)), ("b", (40, 40, 40)), ("c", (60, 60, 40))):
        fc = {"CG.Name": ["Double"], "T.c": [c]}
        got = de.evaluate_measures_batch(["S", "S2", "Sb"], TABLES, m, fc)
        assert (got["S"], got["S2"], got["Sb"]) == want


def test_without_definitions_the_group_is_an_ordinary_table():
    plain = dict(MEASURES)          # nothing attached
    assert _eval("S", {"CG.Name": ["Double"]}, measures=plain) == 60


def test_detaching_restores_plain_evaluation():
    m = _measures()
    assert _eval("S", {"CG.Name": ["Double"]}, measures=m) == 120
    de.set_calculation_groups(m, None)
    assert _eval("S", {"CG.Name": ["Double"]}, measures=m) == 60


def test_the_api_argument_attaches_the_groups():
    m = dict(MEASURES)
    got = de.evaluate_measures_batch(["S"], TABLES, m, {"CG.Name": ["Plus1"]}, calculation_groups=GROUPS)
    assert got["S"] == 61


@pytest.mark.parametrize("fn", ["SELECTEDMEASURE()", "SELECTEDMEASURENAME()",
                                "ISSELECTEDMEASURE([S])", "SELECTEDMEASUREFORMATSTRING()"])
def test_outside_an_item_the_family_is_an_error(fn):
    # Desktop: "There is no measure reference in the current context ..."
    m = dict(MEASURES)
    m["Bad"] = fn
    de._engine.eval_errors.clear()
    assert de.evaluate_measures_batch(["Bad"], TABLES, m, {})["Bad"] is None
    assert "no measure reference" in de._engine.eval_errors["Bad"]


def test_selectedmeasureformatstring_reads_the_measure_format():
    m = dict(MEASURES)
    de.set_calculation_groups(m, GROUPS, {"S": {"format_string": "0.00", "expression": None}})
    assert _eval("S", {"CG.Name": ["Fmt"]}, measures=m) == "0.00"
    assert _eval("S2", {"CG.Name": ["Fmt"]}, measures=m) is None


def test_an_item_format_string_expression():
    groups = [dict(GROUPS[0], items=[dict(i) for i in GROUPS[0]["items"]])]
    for i in groups[0]["items"]:
        if i["name"] == "Double":
            i["format_string"] = '"0.0 x"'
    m = dict(MEASURES)
    de.set_calculation_groups(m, groups, {"S": {"format_string": "#,0", "expression": None}})
    got = de.evaluate_format_strings(["S", "S2"], TABLES, m, {"CG.Name": ["Double"]})
    assert got == {"S": "0.0 x", "S2": "0.0 x"}
    assert de.evaluate_format_strings(["S"], TABLES, m, {"CG.Name": ["Plus1"]}) == {}
    assert de.evaluate_format_strings(["S"], TABLES, m, {}) == {}
