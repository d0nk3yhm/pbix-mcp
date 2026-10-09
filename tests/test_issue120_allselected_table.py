"""Issue #120: ALLSELECTED(<table>) as a TABLE expression is the table's rows
under the selection; its column form keeps only the selection on its own
columns.

The table form returned a CALCULATE marker, so COUNTROWS / MINX / MAXX / SUMX
/ FILTER over it saw no rows: Matrix Bubble Chart's MINX(ALLSELECTED('Date'),
[Sales]) was BLANK and its bubbles never drew. Expected values: Power BI
Desktop 2.152 over ADOMD, each row a SUMMARIZECOLUMNS query -- ungrouped,
grouped, under one TREATAS slicer, and grouped under one. Desktop keeps EVERY
selection reaching the table for the table form (a D[Zone] slicer leaves T two
rows), but ONLY the selection on its own columns for the column form
(COUNTROWS(ALLSELECTED(T[Region])) stays 2 under that slicer, and under
T[Amount], T[Cat] and D[Region] slicers; 1 only under a T[Region] slicer).

Left out until #118 is fixed: an explicit CALCULATE filter inside the measure
(CALCULATE(COUNTROWS(ALLSELECTED(T)), T[Cat] = "A") is 2 in Desktop).
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

pytestmark = pytest.mark.unit

TABLES = {"T": {"columns": ["Region", "Cat", "Amount"],
                "rows": [["North", "A", 10.0], ["North", "B", 20.0], ["South", "A", 30.0], ["South", "B", 15.0]]},
          "D": {"columns": ["Region", "Zone"], "rows": [["North", "Z1"], ["South", "Z2"]]}}
RELS = [{"FromTable": "T", "FromColumn": "Region", "ToTable": "D", "ToColumn": "Region", "IsActive": True,
         "CrossFilteringBehavior": 1, "FromCardinality": 2, "ToCardinality": 1}]
MEASURES = {
    "Sales": "SUM(T[Amount])",
    # the table form (OpenBI doc 51's measures, and more iterators)
    "n_T": "COUNTROWS(ALLSELECTED(T))",
    "n_qT": "COUNTROWS(ALLSELECTED('T'))",
    "n_D": "COUNTROWS(ALLSELECTED(D))",
    "mn_D": "MINX(ALLSELECTED(D), [Sales])",
    "mx_D": "MAXX(ALLSELECTED(D), [Sales])",
    "sx_T": "SUMX(ALLSELECTED(T), T[Amount])",
    "mxx_T": "MAXX(ALLSELECTED(T), T[Amount])",
    "nf_T": "COUNTROWS(FILTER(ALLSELECTED(T), T[Amount] > 15))",
    "av_D": "AVERAGEX(ALLSELECTED(D), [Sales])",
    "cc_D": 'CONCATENATEX(ALLSELECTED(D), D[Region], ",", D[Region], ASC)',
    "share": "DIVIDE([Sales], SUMX(ALLSELECTED(D), [Sales]))",
    "c_t": "COUNTROWS(ALLSELECTED(T))",
    "c_d": "COUNTROWS(ALLSELECTED(D))",
    # the column forms
    "n_col": "COUNTROWS(ALLSELECTED(T[Region]))",
    "c_tr": "COUNTROWS(ALLSELECTED(T[Region]))",
    "c_dr": "COUNTROWS(ALLSELECTED(D[Region]))",
    "c_dz": "COUNTROWS(ALLSELECTED(D[Zone]))",
    "c_tc": "COUNTROWS(ALLSELECTED(T[Cat]))",
    "c_trc": "COUNTROWS(ALLSELECTED(T[Region], T[Cat]))",
    "s_tr": 'CONCATENATEX(ALLSELECTED(T[Region]), T[Region], ",", T[Region], ASC)',
    "s_dr": 'CONCATENATEX(ALLSELECTED(D[Region]), D[Region], ",", D[Region], ASC)',
    "x_tr": "SUMX(ALLSELECTED(T[Region]), [Sales])",
    # guards
    "c_v": "COUNTROWS(VALUES(T[Region]))",
    "m_tr": "CALCULATE(COUNTROWS(T), ALLSELECTED(T[Region]))",
}
DESKTOP = [   # (id, grouping (key, value) or None, slicer, Desktop's answers) -- generated from
             # the battery's own ADOMD output, nothing hand-entered
    ('total', None, {},
     {'n_T': 4, 'n_qT': 4, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 45, 'sx_T': 75, 'mxx_T': 30, 'nf_T': 2, 'av_D': 37.5, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 2, 'c_t': 4, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 4, 'x_tr': 75}),
    ('by_zone=Z1', ('D.Zone', 'Z1'), {},
     {'n_T': 4, 'n_qT': 4, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 45, 'sx_T': 75, 'mxx_T': 30, 'nf_T': 2, 'av_D': 37.5, 'cc_D': 'North,South', 'share': 0.4, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 4, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 30}),
    ('by_zone=Z2', ('D.Zone', 'Z2'), {},
     {'n_T': 4, 'n_qT': 4, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 45, 'sx_T': 75, 'mxx_T': 30, 'nf_T': 2, 'av_D': 37.5, 'cc_D': 'North,South', 'share': 0.6, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 4, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 45}),
    ('by_cat=A', ('T.Cat', 'A'), {},
     {'n_T': 4, 'n_qT': 4, 'n_D': 2, 'n_col': 2, 'mn_D': 10, 'mx_D': 30, 'sx_T': 75, 'mxx_T': 30, 'nf_T': 2, 'av_D': 20, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 2, 'c_t': 4, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 40}),
    ('by_cat=B', ('T.Cat', 'B'), {},
     {'n_T': 4, 'n_qT': 4, 'n_D': 2, 'n_col': 2, 'mn_D': 15, 'mx_D': 20, 'sx_T': 75, 'mxx_T': 30, 'nf_T': 2, 'av_D': 17.5, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 2, 'c_t': 4, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 35}),
    ('slicer_zone_z1', None, {'D.Zone': ['Z1']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 1, 'n_col': 2, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 20, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 1, 'c_tc': 2, 'c_v': 1, 'c_t': 2, 'c_d': 1, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 30}),
    ('slicer_amount_30', None, {'T.Amount': [30.0]},
     {'n_T': 1, 'n_qT': 1, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 30, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 1, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 30}),
    ('slicer_tregion_n', None, {'T.Region': ['North']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 2, 'n_col': 1, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 20, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North,South', 'share': 1, 'c_tr': 1, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 2, 'c_d': 2, 'c_trc': 2, 's_tr': 'North', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 30}),
    ('slicer_dregion_n', None, {'D.Region': ['North']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 1, 'n_col': 2, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 20, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North', 'share': 1, 'c_tr': 2, 'c_dr': 1, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 2, 'c_d': 1, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North', 'm_tr': 2, 'x_tr': 30}),
    ('slicer_cat_a', None, {'T.Cat': ['A']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 2, 'n_col': 2, 'mn_D': 10, 'mx_D': 30, 'sx_T': 40, 'mxx_T': 30, 'nf_T': 1, 'av_D': 20, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 1, 'c_v': 2, 'c_t': 2, 'c_d': 2, 'c_trc': 2, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 2, 'x_tr': 40}),
    ('by_zone_cat_a=Z1', ('D.Zone', 'Z1'), {'T.Cat': ['A']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 2, 'n_col': 2, 'mn_D': 10, 'mx_D': 30, 'sx_T': 40, 'mxx_T': 30, 'nf_T': 1, 'av_D': 20, 'cc_D': 'North,South', 'share': 0.25, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 1, 'c_v': 1, 'c_t': 2, 'c_d': 2, 'c_trc': 2, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 10}),
    ('by_zone_cat_a=Z2', ('D.Zone', 'Z2'), {'T.Cat': ['A']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 2, 'n_col': 2, 'mn_D': 10, 'mx_D': 30, 'sx_T': 40, 'mxx_T': 30, 'nf_T': 1, 'av_D': 20, 'cc_D': 'North,South', 'share': 0.75, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 1, 'c_v': 1, 'c_t': 2, 'c_d': 2, 'c_trc': 2, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 30}),
    ('by_zone_amount_30=Z1', ('D.Zone', 'Z1'), {'T.Amount': [30.0]},
     {'n_T': 1, 'n_qT': 1, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 30, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North,South', 'share': None, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': None, 'c_t': 1, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': None, 'x_tr': None}),
    ('by_zone_amount_30=Z2', ('D.Zone', 'Z2'), {'T.Amount': [30.0]},
     {'n_T': 1, 'n_qT': 1, 'n_D': 2, 'n_col': 2, 'mn_D': 30, 'mx_D': 30, 'sx_T': 30, 'mxx_T': 30, 'nf_T': 1, 'av_D': 30, 'cc_D': 'North,South', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 2, 'c_tc': 2, 'c_v': 1, 'c_t': 1, 'c_d': 2, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 30}),
    ('by_cat_zone_z1=A', ('T.Cat', 'A'), {'D.Zone': ['Z1']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 1, 'n_col': 2, 'mn_D': 10, 'mx_D': 10, 'sx_T': 30, 'mxx_T': 20, 'nf_T': 1, 'av_D': 10, 'cc_D': 'North', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 1, 'c_tc': 2, 'c_v': 1, 'c_t': 2, 'c_d': 1, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 10}),
    ('by_cat_zone_z1=B', ('T.Cat', 'B'), {'D.Zone': ['Z1']},
     {'n_T': 2, 'n_qT': 2, 'n_D': 1, 'n_col': 2, 'mn_D': 20, 'mx_D': 20, 'sx_T': 30, 'mxx_T': 20, 'nf_T': 1, 'av_D': 20, 'cc_D': 'North', 'share': 1, 'c_tr': 2, 'c_dr': 2, 'c_dz': 1, 'c_tc': 2, 'c_v': 1, 'c_t': 2, 'c_d': 1, 'c_trc': 4, 's_tr': 'North,South', 's_dr': 'North,South', 'm_tr': 1, 'x_tr': 20}),
]
CELLS = [(cid, grp, sl, m, want) for cid, grp, sl, exp in DESKTOP for m, want in exp.items()]


@pytest.mark.parametrize("cid,grp,slicer,measure,want", CELLS,
                         ids=[f"{c[0]}:{c[3]}" for c in CELLS])
def test_matches_desktop(cid, grp, slicer, measure, want):
    fc = dict(slicer)
    kw = {}
    if grp:
        fc[grp[0]] = [grp[1]]
        kw = dict(group_by={grp[0]}, selected_filters=dict(slicer))
    got = de.evaluate_measures_smart([measure], TABLES, MEASURES, fc, relationships=RELS,
                                     simulate_row_context=False, **kw)[measure]
    if isinstance(want, (int, float)):
        assert got == pytest.approx(want)
    else:
        assert got == want
