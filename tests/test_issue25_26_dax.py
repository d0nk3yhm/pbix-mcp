"""Issues #25 (r23) + #26 (r24): ALLSELECTED semantics, window functions under
grouped evaluation, ALL-as-FILTER-source, DATEVALUE over columns.

Expected values are the Desktop numbers stated in OpenBI's reports (their
Desktop-parity work) — e.g. r24#2's model gives 60, not 180, under a Cat IN
(A,B) slicer; r23#2's running total is 30/65/105/180 with RANKX 4/3/2/1.
"""
import json

import pytest

from pbix_mcp import server as S


def _mk(tmp_path, name, rows, measures):
    p = str(tmp_path / f"{name}.pbix")
    tables = [{"name": "S",
               "columns": [{"name": "Cat", "data_type": "String"},
                           {"name": "Reg", "data_type": "String"},
                           {"name": "V", "data_type": "Int64"}],
               "rows": rows}]
    r = json.loads(S.pbix_create(p, name, tables_json=json.dumps(tables),
                                 measures_json=json.dumps(measures)))
    assert r["success"], r
    return name


ROWS_4 = [{"Cat": c, "Reg": r, "V": v}
          for c, r, v in [("A", "N", 30), ("B", "N", 35),
                          ("C", "S", 40), ("D", "S", 75)]]  # total 180


def _groups(alias, measures, group_by, fc=""):
    r = json.loads(S.pbix_evaluate_dax_grouped(
        alias, measures, group_by, filter_context=fc))
    assert r["success"], r
    return {tuple(g["key"].values())[0]: g["values"]
            for g in r["data"]["groups"]}


# ---- #26 r24#1: ALLSELECTED() / (table) / (column) as direct CALCULATE args ----

def test_r24_1_allselected_forms_reach_selected_total(tmp_path):
    a = _mk(tmp_path, "r241", ROWS_4, [
        {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
        {"table": "S", "name": "T", "expression":
            "CALCULATE(SUM(S[V]), ALLSELECTED())"},
        {"table": "S", "name": "T_tab", "expression":
            "CALCULATE(SUM(S[V]), ALLSELECTED(S))"},
        {"table": "S", "name": "T_col", "expression":
            "CALCULATE(SUM(S[V]), ALLSELECTED(S[Cat]))"},
    ])
    try:
        g = _groups(a, "Base,T,T_tab,T_col", "S.Cat")
        for cat, want_base in [("A", 30), ("B", 35), ("C", 40), ("D", 75)]:
            assert g[cat]["Base"] == want_base
            assert g[cat]["T"] == 180, f"ALLSELECTED() no-op at {cat}: {g[cat]}"
            assert g[cat]["T_tab"] == 180
            assert g[cat]["T_col"] == 180
    finally:
        S.pbix_close(a)


# ---- #26 r24#2: ALLSELECTED(col) must keep the slicer, drop the grouping ----

def test_r24_2_allselected_column_respects_slicer(tmp_path):
    rows = [{"Cat": "A", "Reg": "N", "V": 30},
            {"Cat": "B", "Reg": "N", "V": 30},
            {"Cat": "C", "Reg": "N", "V": 120}]
    a = _mk(tmp_path, "r242", rows, [
        {"table": "S", "name": "T", "expression":
            "CALCULATE(SUM(S[V]), ALLSELECTED(S[Cat]))"},
    ])
    try:
        g = _groups(a, "T", "S.Cat", fc=json.dumps({"S.Cat": ["A", "B"]}))
        assert g["A"]["T"] == 60, f"want slicer total 60, got {g['A']}"
        assert g["B"]["T"] == 60
    finally:
        S.pbix_close(a)


# ---- #26 r24#3: two column-scoped ALLSELECTED keep an outer filter ----

def test_r24_3_two_allselected_columns_keep_outer_filter(tmp_path):
    rows = [{"Cat": "A", "Reg": "N", "V": 10}, {"Cat": "A", "Reg": "S", "V": 20},
            {"Cat": "B", "Reg": "N", "V": 30}, {"Cat": "B", "Reg": "S", "V": 40}]
    a = _mk(tmp_path, "r243", rows, [
        {"table": "S", "name": "T2", "expression":
            "CALCULATE(SUM(S[V]), ALLSELECTED(S[Cat]), ALLSELECTED(S[Reg]))"},
    ])
    try:
        g = _groups(a, "T2", "S.Cat", fc=json.dumps({"S.Reg": ["N"]}))
        assert g["A"]["T2"] == 40, f"Reg=N dropped: {g['A']}"  # 10+30, not 100
        assert g["B"]["T2"] == 40
    finally:
        S.pbix_close(a)


# ---- #25 r23#2: ALLSELECTED as FILTER source + RANKX over ALLSELECTED ----

def test_r23_2_running_total_and_rankx_over_allselected(tmp_path):
    a = _mk(tmp_path, "r232", ROWS_4, [
        {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
        {"table": "S", "name": "rt", "expression":
            "CALCULATE(SUM(S[V]), FILTER(ALLSELECTED(S[Cat]), "
            "S[Cat] <= MAX(S[Cat])))"},
        {"table": "S", "name": "rk", "expression":
            "RANKX(ALLSELECTED(S[Cat]), CALCULATE(SUM(S[V])))"},
    ])
    try:
        g = _groups(a, "Base,rt,rk", "S.Cat")
        assert [g[c]["rt"] for c in "ABCD"] == [30, 65, 105, 180]
        assert [g[c]["rk"] for c in "ABCD"] == [4, 3, 2, 1]
    finally:
        S.pbix_close(a)


# ---- #25 r23#1: window functions under grouped evaluation ----

def test_r23_1_window_functions_grouped(tmp_path):
    a = _mk(tmp_path, "r231", ROWS_4, [
        {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
        {"table": "S", "name": "rn", "expression":
            "ROWNUMBER(ORDERBY(S[Cat]))"},
        {"table": "S", "name": "rkw", "expression":
            "RANK(ORDERBY(S[Cat] DESC))"},
        {"table": "S", "name": "off", "expression":
            "CALCULATE(SUM(S[V]), OFFSET(-1,,ORDERBY(S[Cat])))"},
        {"table": "S", "name": "idx1", "expression":
            "CALCULATE(SUM(S[V]), INDEX(1,,ORDERBY(S[Cat])))"},
        {"table": "S", "name": "cum", "expression":
            "CALCULATE(SUM(S[V]), WINDOW(1, ABS, 0, REL,, ORDERBY(S[Cat])))"},
    ])
    try:
        g = _groups(a, "Base,rn,rkw,off,idx1,cum", "S.Cat")
        assert [g[c]["rn"] for c in "ABCD"] == [1, 2, 3, 4]
        assert [g[c]["rkw"] for c in "ABCD"] == [4, 3, 2, 1]
        # OFFSET(-1): previous group's sum; blank (None) for the first.
        assert [g[c]["off"] for c in "ABCD"] == [None, 30, 35, 40]
        assert [g[c]["idx1"] for c in "ABCD"] == [30, 30, 30, 30]
        assert [g[c]["cum"] for c in "ABCD"] == [30, 65, 105, 180]
    finally:
        S.pbix_close(a)


# ---- #25 r23#3: ALL(table) / ALL(column) as FILTER sources ----

def test_r23_3_all_as_filter_source(tmp_path):
    a = _mk(tmp_path, "r233", ROWS_4, [
        {"table": "S", "name": "all_filter", "expression":
            "CALCULATE(SUM(S[V]), FILTER(ALL(S), S[V] > 30))"},
        {"table": "S", "name": "all_sumx", "expression":
            "SUMX(FILTER(ALL(S), S[V] > 30), S[V])"},
        {"table": "S", "name": "cr_allcol", "expression":
            "COUNTROWS(FILTER(ALL(S[V]), S[V] > 30))"},
    ])
    try:
        r = json.loads(S.pbix_evaluate_dax(
            a, "all_filter,all_sumx,cr_allcol",
            filter_context=json.dumps({"S.Cat": ["A"]})))
        by = {x["name"]: x.get("value") for x in r["results"]}
        # Table-filter arg replaces the S filters (expansion): 35+40+75.
        assert by["all_filter"] == 150
        assert by["all_sumx"] == 150
        assert by["cr_allcol"] == 3
    finally:
        S.pbix_close(a)


# ---- #25 r23#5: DATEVALUE over columns in calculated columns ----

@pytest.mark.parametrize("idx,dax", [
    (0, "DATEVALUE(S[Cat])"),
    (1, "VAR t = S[Cat] RETURN DATEVALUE(t)"),
])
def test_r23_5_datevalue_over_column_materializes(tmp_path, idx, dax):
    alias = f"dv235_{idx}"
    p = str(tmp_path / f"{alias}.pbix")
    r = json.loads(S.pbix_create(p, alias, tables_json=json.dumps(
        [{"name": "S",
          "columns": [{"name": "Cat", "data_type": "String"}],
          "rows": [{"Cat": "1/8/2009"}, {"Cat": "2/9/2010"}]}])))
    assert r["success"], r
    try:
        r = json.loads(S.pbix_datamodel_add_calculated_column(
            alias, "S", "D", dax))
        assert r["success"], r
        q = json.loads(S.pbix_query_table(alias, table_name="S"))
        assert "2009-01-08" in q["message"], q["message"]
        assert "2010-02-09" in q["message"]
    finally:
        S.pbix_close(alias)
