"""Issues #49 + #50: CALCULATE filter-argument semantics under grouped eval.

#49 — Desktop evaluates CALCULATE's filter arguments in the OUTER filter
context; the engine evaluated them sequentially against the context as
already modified by the preceding arguments. Repro:
CALCULATE(MAX(col), ALLSELECTED(), VALUES(T[dim])) — VALUES saw the
post-ALLSELECTED (widened) context and returned EVERY dim value, so the
grouped result collapsed to the global max in most cells.

#50 — bare REMOVEFILTERS() was a silent no-op (the reference regex cannot
match empty parens), and ALLEXCEPT as a CALCULATE argument fell into the
generic ALL branch whose single-reference regex cannot parse a
multi-argument call, so it too applied nothing.

Expected values are Desktop numbers per OpenBI's QlikView TOTAL-qualifier
translation reports.
"""
import json

import pytest

from pbix_mcp import server as S

pytestmark = pytest.mark.unit


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


# ---- #49: filter args evaluate in the OUTER context, not sequentially ----

class TestOuterContextFilterArgs:
    def test_values_after_allselected_restores_group(self, tmp_path):
        """VALUES(S[Cat]) must see the grouped (outer) context, not the
        context ALLSELECTED() just widened. Sequential evaluation returned
        the global MAX (75) in every cell but D's."""
        a = _mk(tmp_path, "i49a", ROWS_4, [
            {"table": "S", "name": "M", "expression":
                "CALCULATE(MAX(S[V]), ALLSELECTED(), VALUES(S[Cat]))"},
        ])
        try:
            g = _groups(a, "M", "S.Cat")
            for cat, want in [("A", 30), ("B", 35), ("C", 40), ("D", 75)]:
                assert g[cat]["M"] == want, f"{cat}: {g[cat]}"
        finally:
            S.pbix_close(a)

    def test_predicate_value_evals_in_outer_context(self, tmp_path):
        """The value side of a predicate arg evaluates in the OUTER context.
        CALCULATE(SUM, ALL(S), S[V] = MAX(S[V])) — Desktop computes
        MAX(S[V]) per grouped cell (the outer context), not against the
        ALL-cleared context (where it is the global 75 everywhere)."""
        a = _mk(tmp_path, "i49b", ROWS_4, [
            {"table": "S", "name": "M", "expression":
                "CALCULATE(SUM(S[V]), ALL(S), S[V] = MAX(S[V]))"},
        ])
        try:
            g = _groups(a, "M", "S.Cat")
            for cat, want in [("A", 30), ("B", 35), ("C", 40), ("D", 75)]:
                assert g[cat]["M"] == want, f"{cat}: {g[cat]}"
        finally:
            S.pbix_close(a)


# ---- #50: bare REMOVEFILTERS() ----

class TestBareRemovefilters:
    def test_removefilters_clears_grouping(self, tmp_path):
        a = _mk(tmp_path, "i50a", ROWS_4, [
            {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
            {"table": "S", "name": "T", "expression":
                "CALCULATE(SUM(S[V]), REMOVEFILTERS())"},
        ])
        try:
            g = _groups(a, "Base,T", "S.Cat")
            for cat, want_base in [("A", 30), ("B", 35), ("C", 40),
                                   ("D", 75)]:
                assert g[cat]["Base"] == want_base
                assert g[cat]["T"] == 180, (
                    f"REMOVEFILTERS() no-op at {cat}: {g[cat]}")
        finally:
            S.pbix_close(a)

    def test_removefilters_clears_slicer_too(self, tmp_path):
        """Unlike ALLSELECTED(), REMOVEFILTERS() drops the slicer selection
        as well — the all-cleared 180, not the selected 65."""
        a = _mk(tmp_path, "i50b", ROWS_4, [
            {"table": "S", "name": "T", "expression":
                "CALCULATE(SUM(S[V]), REMOVEFILTERS())"},
        ])
        try:
            g = _groups(a, "T", "S.Cat",
                        fc=json.dumps({"S.Cat": ["A", "B"]}))
            assert g["A"]["T"] == 180
            assert g["B"]["T"] == 180
        finally:
            S.pbix_close(a)


# ---- #50: ALLEXCEPT as a CALCULATE argument ----

ROWS_SPAN = [{"Cat": "A", "Reg": "N", "V": 30},
             {"Cat": "A", "Reg": "S", "V": 20},
             {"Cat": "B", "Reg": "N", "V": 35},
             {"Cat": "B", "Reg": "S", "V": 15}]  # total 100


class TestAllexceptArgument:
    def test_allexcept_keeps_listed_column_only(self, tmp_path):
        """Under a Reg slicer, ALLEXCEPT(S, S[Cat]) drops the Reg filter but
        keeps Cat: A -> 50 (30+20), B -> 50 (35+15). The no-op returned the
        slicer-filtered Base values (30 / 35)."""
        a = _mk(tmp_path, "i50c", ROWS_SPAN, [
            {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
            {"table": "S", "name": "T", "expression":
                "CALCULATE(SUM(S[V]), ALLEXCEPT(S, S[Cat]))"},
        ])
        try:
            g = _groups(a, "Base,T", "S.Cat",
                        fc=json.dumps({"S.Reg": ["N"]}))
            assert g["A"]["Base"] == 30
            assert g["B"]["Base"] == 35
            assert g["A"]["T"] == 50, f"ALLEXCEPT no-op: {g['A']}"
            assert g["B"]["T"] == 50, f"ALLEXCEPT no-op: {g['B']}"
        finally:
            S.pbix_close(a)

    def test_allexcept_multiple_kept_columns(self, tmp_path):
        """Both listed columns survive: grouped by Cat under a Reg slicer,
        ALLEXCEPT(S, S[Cat], S[Reg]) keeps everything that is filtered, so
        it equals Base."""
        a = _mk(tmp_path, "i50d", ROWS_SPAN, [
            {"table": "S", "name": "Base", "expression": "SUM(S[V])"},
            {"table": "S", "name": "T", "expression":
                "CALCULATE(SUM(S[V]), ALLEXCEPT(S, S[Cat], S[Reg]))"},
        ])
        try:
            g = _groups(a, "Base,T", "S.Cat",
                        fc=json.dumps({"S.Reg": ["N"]}))
            assert g["A"]["T"] == g["A"]["Base"] == 30
            assert g["B"]["T"] == g["B"]["Base"] == 35
        finally:
            S.pbix_close(a)
