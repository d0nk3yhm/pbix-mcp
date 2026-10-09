"""Issue #121, second and third Desktop batteries: the rules for re-selecting
an item, sideways recursion, a higher-precedence item's own measure references,
selection through other columns, one-item groups, and item format strings.

Power BI Desktop 2.152 over ADOMD, one EVALUATE ROW per cell (build_b121b.py),
and over MDX for each cell's FORMAT_STRING (build_b121c.py, the item format
strings set through Desktop's own TOM). Generated from Desktop's output:
  * only a filter on the item column selects: CG[Ordinal] = 2 applies nothing;
  * re-selecting the applied item does not apply it again: CALCULATE([S],
    CG[Name] = "Double") is 120 under Double; another item applies: the same
    with "Plus1" is 122 under Double;
  * a lower-precedence item applies to the measure references in a higher
    item's expression: Ref10 = [S] * 10 with Double is 1200;
  * the outermost item WITH a format string decides it, and its SELECTEDMEASURE()
    is the value being formatted.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

T = {"columns": ["c", "v"], "rows": [["a", 10.0], ["b", 20.0], ["c", 30.0]]}

# ---- build_b121b.py ----------------------------------------------------------
MEASURES_B = {
    "S": "SUM(T[v])", "S2": "[S]", "Sb": 'CALCULATE([S], T[c] = "b")', "SX": "SUMX(VALUES(T[c]), [S])",
    "MD": 'CALCULATE([S], CG[Name] = "Double")', "MP": 'CALCULATE([S], CG[Name] = "Plus1")',
    "MDA": 'CALCULATE([S], CG[Name] = "Double", CG2[Name] = "Add100")',
    "MR": "CALCULATE([S], REMOVEFILTERS(CG))", "MA": "CALCULATE([S], ALL(CG[Name]))",
    "MO": "CALCULATE([S], CG[Ordinal] = 1)", "Sf": "SUM(T[v])", "Sf2": "[Sf]",
    "IsF": "IF(ISFILTERED(CG[Name]), 1, 0)", "Sel": 'SELECTEDVALUE(CG[Name], "none")',
}
FORMATS_B = {"Sf": {"format_string": "0.00", "expression": None},
             "Sf2": {"format_string": "#,0", "expression": None}}
CG_B = [("Base", "SELECTEDMEASURE()"), ("Double", "SELECTEDMEASURE() * 2"), ("Plus1", "SELECTEDMEASURE() + 1"),
        ("Name", "SELECTEDMEASURENAME()"), ("RefS", "[S] * 10"),
        ("SideD", 'CALCULATE(SELECTEDMEASURE(), CG[Name] = "Double")'),
        ("SideP", 'CALCULATE(SELECTEDMEASURE(), CG[Name] = "Plus1") + 1000'),
        ("IsS2", "IF(ISSELECTEDMEASURE([S], [S2]), 1, 0)"), ("Fmt", "SELECTEDMEASUREFORMATSTRING()"),
        ("NoCG", "CALCULATE(SELECTEDMEASURE(), REMOVEFILTERS(CG))")]
CG2_B = [("Add100", "SELECTEDMEASURE() + 100"), ("Ref10", "[S] * 10"), ("RefS2", "[S2] + 0")]
CG3_B = [("Triple", "SELECTEDMEASURE() * 3")]
SELECTIONS_B = {
    "none": {}, "Double": {"CG.Name": ["Double"]}, "Plus1": {"CG.Name": ["Plus1"]}, "Name": {"CG.Name": ["Name"]},
    "RefS": {"CG.Name": ["RefS"]}, "SideD": {"CG.Name": ["SideD"]}, "SideP": {"CG.Name": ["SideP"]},
    "IsS2": {"CG.Name": ["IsS2"]}, "Fmt": {"CG.Name": ["Fmt"]}, "NoCG": {"CG.Name": ["NoCG"]},
    "Ord2": {"CG.Ordinal": [2]}, "Ref10": {"CG2.Name": ["Ref10"]},
    "Double+Ref10": {"CG.Name": ["Double"], "CG2.Name": ["Ref10"]},
    "Plus1+Ref10": {"CG.Name": ["Plus1"], "CG2.Name": ["Ref10"]},
    "Plus1+RefS2": {"CG.Name": ["Plus1"], "CG2.Name": ["RefS2"]},
    "Double+Add100": {"CG.Name": ["Double"], "CG2.Name": ["Add100"]},
    "SideD+Add100": {"CG.Name": ["SideD"], "CG2.Name": ["Add100"]},
    "Triple": {"CG3.Name": ["Triple"]}, "Double+Triple": {"CG.Name": ["Double"], "CG3.Name": ["Triple"]},
    "two": {"CG.Name": ["Double", "Plus1"]},
}


def _group(table, items, precedence, formats=None):
    return {"table": table, "column": "Name", "precedence": precedence,
            "items": [{"name": n, "expression": e, "ordinal": i,
                       "format_string": (formats or {}).get(n)} for i, (n, e) in enumerate(items)]}


def _tables(groups):
    out = {"T": T}
    for table, items in groups:
        out[table] = {"columns": ["Name", "Ordinal"], "rows": [[n, i] for i, (n, _e) in enumerate(items)]}
    return out


TABLES_B = _tables((("CG", CG_B), ("CG2", CG2_B), ("CG3", CG3_B)))
GROUPS_B = [_group("CG", CG_B, 0), _group("CG2", CG2_B, 10), _group("CG3", CG3_B, 5)]

DESKTOP_B = {
    'none|S': 60, 'Double|S': 120, 'Plus1|S': 61, 'Name|S': 'S',
    'RefS|S': 600, 'SideD|S': 120, 'SideP|S': 1061, 'IsS2|S': 1,
    'Fmt|S': None, 'NoCG|S': 60, 'Ord2|S': 60, 'Ref10|S': 600,
    'Double+Ref10|S': 1200, 'Plus1+Ref10|S': 610, 'Plus1+RefS2|S': 61, 'Double+Add100|S': 220,
    'SideD+Add100|S': 220, 'Triple|S': 180, 'Double+Triple|S': 360, 'two|S': 60,
    'none|S2': 60, 'Double|S2': 120, 'Plus1|S2': 61, 'Name|S2': 'S2',
    'RefS|S2': 600, 'SideD|S2': 120, 'SideP|S2': 1061, 'IsS2|S2': 1,
    'Fmt|S2': None, 'NoCG|S2': 60, 'Ord2|S2': 60, 'Ref10|S2': 600,
    'Double+Ref10|S2': 1200, 'Plus1+Ref10|S2': 610, 'Plus1+RefS2|S2': 61, 'Double+Add100|S2': 220,
    'SideD+Add100|S2': 220, 'Triple|S2': 180, 'Double+Triple|S2': 360, 'two|S2': 60,
    'none|Sb': 20, 'Double|Sb': 40, 'Plus1|Sb': 21, 'Name|Sb': 'Sb',
    'RefS|Sb': 600, 'SideD|Sb': 40, 'SideP|Sb': 1021, 'IsS2|Sb': 0,
    'Fmt|Sb': None, 'NoCG|Sb': 20, 'Ord2|Sb': 20, 'Ref10|Sb': 600,
    'Double+Ref10|Sb': 1200, 'Plus1+Ref10|Sb': 610, 'Plus1+RefS2|Sb': 61, 'Double+Add100|Sb': 140,
    'SideD+Add100|Sb': 140, 'Triple|Sb': 60, 'Double+Triple|Sb': 120, 'two|Sb': 20,
    'none|SX': 60, 'Double|SX': 120, 'Plus1|SX': 61, 'Name|SX': 'SX',
    'RefS|SX': 600, 'SideD|SX': 120, 'SideP|SX': 1061, 'IsS2|SX': 0,
    'Fmt|SX': None, 'NoCG|SX': 60, 'Ord2|SX': 60, 'Ref10|SX': 600,
    'Double+Ref10|SX': 1200, 'Plus1+Ref10|SX': 610, 'Plus1+RefS2|SX': 61, 'Double+Add100|SX': 220,
    'SideD+Add100|SX': 220, 'Triple|SX': 180, 'Double+Triple|SX': 360, 'two|SX': 60,
    'none|MD': 120, 'Double|MD': 120, 'Plus1|MD': 121, 'Name|MD': 'MD',
    'RefS|MD': 600, 'SideD|MD': 120, 'SideP|MD': 1121, 'IsS2|MD': 0,
    'Fmt|MD': None, 'NoCG|MD': 120, 'Ord2|MD': 120, 'Ref10|MD': 600,
    'Double+Ref10|MD': 1200, 'Plus1+Ref10|MD': 610, 'Plus1+RefS2|MD': 61, 'Double+Add100|MD': 220,
    'SideD+Add100|MD': 220, 'Triple|MD': 360, 'Double+Triple|MD': 360, 'two|MD': 120,
    'none|MP': 61, 'Double|MP': 122, 'Plus1|MP': 61, 'Name|MP': 'MP',
    'RefS|MP': 600, 'SideD|MP': 122, 'SideP|MP': 1061, 'IsS2|MP': 0,
    'Fmt|MP': None, 'NoCG|MP': 61, 'Ord2|MP': 61, 'Ref10|MP': 600,
    'Double+Ref10|MP': 1200, 'Plus1+Ref10|MP': 610, 'Plus1+RefS2|MP': 61, 'Double+Add100|MP': 222,
    'SideD+Add100|MP': 222, 'Triple|MP': 183, 'Double+Triple|MP': 366, 'two|MP': 61,
    'none|MDA': 220, 'Double|MDA': 320, 'Plus1|MDA': 221, 'Name|MDA': 'MDA',
    'RefS|MDA': 600, 'SideD|MDA': 320, 'SideP|MDA': 1221, 'IsS2|MDA': 0,
    'Fmt|MDA': None, 'NoCG|MDA': 220, 'Ord2|MDA': 220, 'Ref10|MDA': 600,
    'Double+Ref10|MDA': 1200, 'Plus1+Ref10|MDA': 610, 'Plus1+RefS2|MDA': 61, 'Double+Add100|MDA': 220,
    'SideD+Add100|MDA': 220, 'Triple|MDA': 660, 'Double+Triple|MDA': 960, 'two|MDA': 220,
    'none|MR': 60, 'Double|MR': 120, 'Plus1|MR': 61, 'Name|MR': 'MR',
    'RefS|MR': 600, 'SideD|MR': 120, 'SideP|MR': 1061, 'IsS2|MR': 0,
    'Fmt|MR': None, 'NoCG|MR': 60, 'Ord2|MR': 60, 'Ref10|MR': 600,
    'Double+Ref10|MR': 1200, 'Plus1+Ref10|MR': 610, 'Plus1+RefS2|MR': 61, 'Double+Add100|MR': 220,
    'SideD+Add100|MR': 220, 'Triple|MR': 180, 'Double+Triple|MR': 360, 'two|MR': 60,
    'none|MA': 60, 'Double|MA': 120, 'Plus1|MA': 61, 'Name|MA': 'MA',
    'RefS|MA': 600, 'SideD|MA': 120, 'SideP|MA': 1061, 'IsS2|MA': 0,
    'Fmt|MA': None, 'NoCG|MA': 60, 'Ord2|MA': 60, 'Ref10|MA': 600,
    'Double+Ref10|MA': 1200, 'Plus1+Ref10|MA': 610, 'Plus1+RefS2|MA': 61, 'Double+Add100|MA': 220,
    'SideD+Add100|MA': 220, 'Triple|MA': 180, 'Double+Triple|MA': 360, 'two|MA': 60,
    'none|MO': 60, 'Double|MO': 120, 'Plus1|MO': 61, 'Name|MO': 'MO',
    'RefS|MO': 600, 'SideD|MO': 120, 'SideP|MO': 1061, 'IsS2|MO': 0,
    'Fmt|MO': None, 'NoCG|MO': 60, 'Ord2|MO': 60, 'Ref10|MO': 600,
    'Double+Ref10|MO': 1200, 'Plus1+Ref10|MO': 610, 'Plus1+RefS2|MO': 61, 'Double+Add100|MO': 220,
    'SideD+Add100|MO': 220, 'Triple|MO': 180, 'Double+Triple|MO': 360, 'two|MO': 60,
    'none|Sf': 60, 'Double|Sf': 120, 'Plus1|Sf': 61, 'Name|Sf': 'Sf',
    'RefS|Sf': 600, 'SideD|Sf': 120, 'SideP|Sf': 1061, 'IsS2|Sf': 0,
    'Fmt|Sf': '0.00', 'NoCG|Sf': 60, 'Ord2|Sf': 60, 'Ref10|Sf': 600,
    'Double+Ref10|Sf': 1200, 'Plus1+Ref10|Sf': 610, 'Plus1+RefS2|Sf': 61, 'Double+Add100|Sf': 220,
    'SideD+Add100|Sf': 220, 'Triple|Sf': 180, 'Double+Triple|Sf': 360, 'two|Sf': 60,
    'none|Sf2': 60, 'Double|Sf2': 120, 'Plus1|Sf2': 61, 'Name|Sf2': 'Sf2',
    'RefS|Sf2': 600, 'SideD|Sf2': 120, 'SideP|Sf2': 1061, 'IsS2|Sf2': 0,
    'Fmt|Sf2': '#,0', 'NoCG|Sf2': 60, 'Ord2|Sf2': 60, 'Ref10|Sf2': 600,
    'Double+Ref10|Sf2': 1200, 'Plus1+Ref10|Sf2': 610, 'Plus1+RefS2|Sf2': 61, 'Double+Add100|Sf2': 220,
    'SideD+Add100|Sf2': 220, 'Triple|Sf2': 180, 'Double+Triple|Sf2': 360, 'two|Sf2': 60,
    'none|IsF': 0, 'Double|IsF': 2, 'Plus1|IsF': 2, 'Name|IsF': 'IsF',
    'RefS|IsF': 600, 'SideD|IsF': 2, 'SideP|IsF': 1002, 'IsS2|IsF': 0,
    'Fmt|IsF': None, 'NoCG|IsF': 0, 'Ord2|IsF': 0, 'Ref10|IsF': 600,
    'Double+Ref10|IsF': 1200, 'Plus1+Ref10|IsF': 610, 'Plus1+RefS2|IsF': 61, 'Double+Add100|IsF': 102,
    'SideD+Add100|IsF': 102, 'Triple|IsF': 0, 'Double+Triple|IsF': 6, 'two|IsF': 1,
    'none|Sel': 'none', 'Double|Sel': 'ERROR', 'Plus1|Sel': 'ERROR', 'Name|Sel': 'Sel',
    'RefS|Sel': 600, 'SideD|Sel': 'ERROR', 'SideP|Sel': 'ERROR', 'IsS2|Sel': 0,
    'Fmt|Sel': None, 'NoCG|Sel': 'none', 'Ord2|Sel': 'Plus1', 'Ref10|Sel': 600,
    'Double+Ref10|Sel': 1200, 'Plus1+Ref10|Sel': 610, 'Plus1+RefS2|Sel': 61, 'Double+Add100|Sel': 'ERROR',
    'SideD+Add100|Sel': 'ERROR', 'Triple|Sel': 'ERROR', 'Double+Triple|Sel': 'ERROR', 'two|Sel': 'none',
}

FORMATS_C = {   # Desktop 2.152 over MDX: cell FORMAT_STRING -- generated
    'none|S': None, 'Base|S': None, 'Double|S': '0.0 x', 'Plus1|S': None,
    'Keep|S': '#,0.0', 'Add100|S': '#,0.000', 'Same|S': None, 'Triple|S': '#,0',
    'Double+Add100|S': '#,0.000', 'Plus1+Add100|S': '#,0.000', 'Base+Add100|S': '#,0.000', 'Double+Same|S': '0.0 x',
    'Double+Triple|S': '#,0', 'Base+Triple|S': '#,0', 'Keep+Triple|S': '#,0', 'Triple+Add100|S': '#,0.000',
    'none|S2': None, 'Base|S2': None, 'Double|S2': '0.0 x', 'Plus1|S2': None,
    'Keep|S2': '#,0.0', 'Add100|S2': '#,0.000', 'Same|S2': None, 'Triple|S2': '#,0',
    'Double+Add100|S2': '#,0.000', 'Plus1+Add100|S2': '#,0.000', 'Base+Add100|S2': '#,0.000', 'Double+Same|S2': '0.0 x',
    'Double+Triple|S2': '#,0', 'Base+Triple|S2': '#,0', 'Keep+Triple|S2': '#,0', 'Triple+Add100|S2': '#,0.000',
    'none|Sf': '0.00', 'Base|Sf': '0.00', 'Double|Sf': '0.0 x', 'Plus1|Sf': '0.00',
    'Keep|Sf': '#,0.0', 'Add100|Sf': '#,0.000', 'Same|Sf': '0.00', 'Triple|Sf': '#,0',
    'Double+Add100|Sf': '#,0.000', 'Plus1+Add100|Sf': '#,0.000', 'Base+Add100|Sf': '#,0.000', 'Double+Same|Sf': '0.0 x',
    'Double+Triple|Sf': '#,0', 'Base+Triple|Sf': '#,0', 'Keep+Triple|Sf': '#,0', 'Triple+Add100|Sf': '#,0.000',
    'none|Sf2': '#,0', 'Base|Sf2': '#,0', 'Double|Sf2': '0.0 x', 'Plus1|Sf2': '#,0',
    'Keep|Sf2': '#,0.0', 'Add100|Sf2': '#,0.000', 'Same|Sf2': '#,0', 'Triple|Sf2': '#,0',
    'Double+Add100|Sf2': '#,0.000', 'Plus1+Add100|Sf2': '#,0.000', 'Base+Add100|Sf2': '#,0.000', 'Double+Same|Sf2': '0.0 x',
    'Double+Triple|Sf2': '#,0', 'Base+Triple|Sf2': '#,0', 'Keep+Triple|Sf2': '#,0', 'Triple+Add100|Sf2': '#,0.000',
    'none|Fs': None, 'Base|Fs': None, 'Double|Fs': '0.0 x', 'Plus1|Fs': None,
    'Keep|Fs': '#,0.0', 'Add100|Fs': '#,0.000', 'Same|Fs': None, 'Triple|Fs': '0.00',
    'Double+Add100|Fs': '#,0.000', 'Plus1+Add100|Fs': '#,0.000', 'Base+Add100|Fs': '#,0.000', 'Double+Same|Fs': '0.0 x',
    'Double+Triple|Fs': '0.00', 'Base+Triple|Fs': '0.00', 'Keep+Triple|Fs': '0.00', 'Triple+Add100|Fs': '#,0.000',
}


VALUE_B = sorted(k for k, v in DESKTOP_B.items() if v != "ERROR")
ERROR_B = sorted(k for k, v in DESKTOP_B.items() if v == "ERROR")


def _eval_b(cell):
    selection, measure = cell.split("|")
    m = dict(MEASURES_B)
    de.set_calculation_groups(m, GROUPS_B, FORMATS_B)
    de._engine.eval_errors.clear()
    return measure, de.evaluate_measures_batch([measure], TABLES_B, m, dict(SELECTIONS_B[selection]))[measure]


@pytest.mark.parametrize("cell", VALUE_B)
def test_rules_match_desktop(cell):
    _m, got = _eval_b(cell)
    want = DESKTOP_B[cell]
    if isinstance(want, (int, float)):
        assert got == pytest.approx(want)
    else:
        assert got == want


@pytest.mark.parametrize("cell", ERROR_B)
def test_rules_errors_match_desktop(cell):
    measure, got = _eval_b(cell)
    assert got is None
    assert measure in de._engine.eval_errors


# ---- build_b121c.py: item format strings -------------------------------------
MEASURES_C = {"S": "SUM(T[v])", "S2": "[S]", "Sf": "SUM(T[v])", "Sf2": "[Sf]", "Fs": "SELECTEDVALUE(T[c])"}
OWN_C = {"Sf": {"format_string": "0.00", "expression": None}, "Sf2": {"format_string": "#,0", "expression": None}}
CG_C = [("Base", "SELECTEDMEASURE()"), ("Double", "SELECTEDMEASURE() * 2"), ("Plus1", "SELECTEDMEASURE() + 1"),
        ("Keep", "SELECTEDMEASURE()"), ("Fmt", "SELECTEDMEASUREFORMATSTRING()")]
CG2_C = [("Add100", "SELECTEDMEASURE() + 100"), ("Same", "SELECTEDMEASURE()")]
CG3_C = [("Triple", "SELECTEDMEASURE() * 3")]
ITEM_FORMATS_C = {"Double": '"0.0 x"', "Plus1": "SELECTEDMEASUREFORMATSTRING()", "Keep": '"#,0.0"',
                  "Fmt": '"@"', "Add100": '"#,0.000"', "Triple": 'IF(SELECTEDMEASURE() > 100, "#,0", "0.00")'}
TABLES_C = _tables((("CG", CG_C), ("CG2", CG2_C), ("CG3", CG3_C)))
GROUPS_C = [_group("CG", CG_C, 0, ITEM_FORMATS_C), _group("CG2", CG2_C, 10, ITEM_FORMATS_C),
            _group("CG3", CG3_C, 5, ITEM_FORMATS_C)]
WHERE_C = {n: t for t, items in (("CG", CG_C), ("CG2", CG2_C), ("CG3", CG3_C)) for n, _e in items}


@pytest.mark.parametrize("cell", sorted(FORMATS_C))
def test_item_format_strings_match_desktop(cell):
    selection, measure = cell.split("|")
    fc = {} if selection == "none" else {f"{WHERE_C[i]}.Name": [i] for i in selection.split("+")}
    m = dict(MEASURES_C)
    de.set_calculation_groups(m, GROUPS_C, OWN_C)
    item = de.evaluate_format_strings([measure], TABLES_C, m, fc)
    got = item[measure] if measure in item else (OWN_C.get(measure) or {}).get("format_string")
    assert got == FORMATS_C[cell]
