"""Issue #163: the engine joins a relationship's text keys as Desktop does.

It matched a key by its exact text and joined every row of a key the one side
holds more than once. Desktop's column store compares a text key with its
ASCII case folded, also across the two tables (F's "K1" reaches D's "k1"), and
a relationship joins a repeated one-side key to its LAST row (#162): a filter
on an earlier row reaches no fact row, RELATED returns the last row, and
RELATEDTABLE of an earlier row is empty. A filter from the many side (an
expanded table, CROSSFILTER BOTH) reaches every row that holds the key.

Expected values: Power BI Desktop 2.152 over ADOMD (build_b165.py), the same
as built and after Desktop's refresh, through pbix-mcp's reader and engine."""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

D = [('k1', 'A'), ('K2', 'B'), ('k3', 'C'), ('k3', 'D'), ('k4 ', 'E')]
F = [('K1', 1), ('k2', 10), ('k3', 100), ('k4', 1000), ('k1', 10000), ('k9', 100000)]
DESKTOP = [('related', 'CONCATENATEX(F, F[k] & "=" & RELATED(D[name]), ",", F[v], ASC)', 'K1=A,k2=B,k3=D,k4=E,K1=A,k9='), ('sum_a', 'CALCULATE(SUM(F[v]), D[name] = "A")', '10001'), ('sum_b', 'CALCULATE(SUM(F[v]), D[name] = "B")', '10'), ('sum_c', 'CALCULATE(SUM(F[v]), D[name] = "C")', None), ('sum_d', 'CALCULATE(SUM(F[v]), D[name] = "D")', '100'), ('sum_e', 'CALCULATE(SUM(F[v]), D[name] = "E")', '1000'), ('sum_blank', 'CALCULATE(SUM(F[v]), ISBLANK(D[name]))', '100000'), ('by_name', 'CONCATENATEX(VALUES(D[name]), D[name] & ":" & CALCULATE(SUM(F[v])), ",", D[name], ASC)', ':100000,A:10001,B:10,C:,D:100,E:1000'), ('by_key', 'CONCATENATEX(VALUES(D[k]), D[k] & ":" & CALCULATE(SUM(F[v])), ",", D[k], ASC)', ':100000,k1:10001,K2:10,k3:100,k4:1000'), ('d_keys', 'COUNTROWS(VALUES(D[k]))', '5'), ('rt_c', 'SUMX(FILTER(D, D[name] = "C"), COUNTROWS(RELATEDTABLE(F)))', None), ('rt_d', 'SUMX(FILTER(D, D[name] = "D"), COUNTROWS(RELATEDTABLE(F)))', '1'), ('rt_a', 'SUMX(FILTER(D, D[name] = "A"), COUNTROWS(RELATEDTABLE(F)))', '2'), ('f_k2', 'CALCULATE(SUM(F[v]), D[k] = "k2")', '10'), ('f_K1', 'CALCULATE(SUM(F[v]), F[k] = "k1")', '10001'), ('f_rows', 'COUNTROWS(VALUES(F[k]))', '5'), ('exp_d', 'CALCULATE(COUNTROWS(D), F)', '5'), ('exp_names', 'CONCATENATEX(CALCULATETABLE(VALUES(D[name]), F), D[name], ",", D[name], ASC)', ',A,B,C,D,E'), ('exp_k3', 'CALCULATE(COUNTROWS(D), FILTER(F, F[v] = 100))', '2'), ('exp_k3_names', 'CONCATENATEX(CALCULATETABLE(VALUES(D[name]), FILTER(F, F[v] = 100)), D[name], ",", D[name], ASC)', 'C,D'), ('bidi_k3', 'CALCULATE(COUNTROWS(D), CROSSFILTER(F[k], D[k], BOTH), F[v] = 100)', '2'), ('bidi_k3_names', 'CALCULATE(CONCATENATEX(VALUES(D[name]), D[name], ","), CROSSFILTER(F[k], D[k], BOTH), F[v] = 100)', 'C,D'), ('bidi_k1_names', 'CALCULATE(CONCATENATEX(VALUES(D[name]), D[name], ","), CROSSFILTER(F[k], D[k], BOTH), F[v] = 1)', 'A'), ('rt_b', 'SUMX(FILTER(D, D[name] = "B"), COUNTROWS(RELATEDTABLE(F)))', '1'), ('rel_sum', 'SUMX(F, IF(RELATED(D[name]) = "D", F[v]))', '100'), ('rel_count_c', 'COUNTROWS(FILTER(F, RELATED(D[name]) = "C"))', None)]


@pytest.fixture(scope="module")
def answers(tmp_path_factory):
    import warnings

    from pbix_mcp import server as S
    from pbix_mcp.builder import PBIXBuilder
    from pbix_mcp.dax import engine as de

    warnings.simplefilter("ignore")
    path = str(tmp_path_factory.mktemp("i163") / "b165.pbix")
    b = PBIXBuilder("b165")
    b.add_table("D", [{"name": "k", "data_type": "String"}, {"name": "name", "data_type": "String"}],
                rows=[{"k": k, "name": n} for k, n in D])
    b.add_table("F", [{"name": "k", "data_type": "String"}, {"name": "v", "data_type": "Int64"}],
                rows=[{"k": k, "v": v} for k, v in F])
    b.add_relationship("F", "k", "D", "k")
    b.add_page("Page 1")
    b.save(path)
    S.pbix_open(path, "i163")
    try:
        ctx = S._get_dax_context("i163")
        measures = {name: dax for name, dax, _want in DESKTOP}
        defs = dict(ctx["measure_defs"], **measures)
        mt = dict(ctx.get("measure_tables") or {}, **{k: "F" for k in measures})
        got = de.evaluate_measures_smart(
            list(measures), ctx["tables"], defs, {}, ctx["date_table"], ctx["date_column"],
            ctx["relationships"], simulate_row_context=False, measure_tables=mt,
            model_columns=ctx.get("model_columns"), date_tables=ctx.get("date_tables"))
    finally:
        S.pbix_close("i163")
    return got


def _adomd(v):
    if v is None:
        return None
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return str(v)


@pytest.mark.parametrize("probe,dax,want", DESKTOP, ids=[p[0] for p in DESKTOP])
def test_matches_desktop(answers, probe, dax, want):
    assert _adomd(answers.get(probe)) == want
