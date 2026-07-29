"""The compiled expression plans (issue #6) and their semantic guarantees.

The plan cache is a PURE hoisting of _eval_expr's syntactic analysis: every
value-producing branch is the old dispatch code. These tests pin the parts
that could drift — the chain ORDER, the two runtime fallthroughs, and the
plan cache actually being exercised — plus the calc-column loop's new
mask-as-variable contract.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de
from pbix_mcp.dax.calc_tables import evaluate_row_context_column


def _ev(expr, ctx=None, var_scope=None):
    eng = de.DAXEngine()
    return eng._eval_expr(
        expr, ctx or de.DAXContext({}, {}, None, None, None, []), var_scope)


class TestPlanShapes:
    """Analysis must mirror the old dispatch chain exactly."""

    def test_single_terminal_step_for_common_shapes(self):
        cases = {
            '5': de._P_CONST, '"x"': de._P_CONST, 'TRUE': de._P_CONST,
            '1e3': de._P_CONST,
            '(1+2)': de._P_PAREN,
            'a + b': de._P_BINARY, 'a * b': de._P_BINARY,
            'a && b': de._P_LOGICAL,
            'NOT x': de._P_NOT,
            'SUM(T[c])': de._P_FUNC,
            '[Total]': de._P_BRACKET1,
            "'T'[c]": de._P_TCOL,
            '"a" & "b"': de._P_CONCAT,
            '-SUM(T[c])': de._P_NEG,
            'VAR _x = 1 RETURN _x': de._P_VARRET,
        }
        for text, kind in cases.items():
            plan = de._analyze_expr(text)
            assert plan[0][0] == kind, f"{text!r} -> {plan}"

    def test_bare_identifier_is_var_then_tail(self):
        plan = de._analyze_expr('SomeName')
        assert [k for k, _ in plan] == [de._P_MAYBEVAR, de._P_TAIL]

    def test_comparison_keeps_a_fallthrough_step(self):
        """A matched comparison can evaluate to None (a blank side, a type
        error) and the old chain then fell through -- the plan must too."""
        plan = de._analyze_expr('x = y')
        assert plan[0][0] == de._P_CMP
        assert len(plan) > 1

    def test_chain_order_bracket_before_concat(self):
        """[Total & Sales] is a MEASURE name, not string concatenation --
        the bracket branch precedes the & branch in the old chain."""
        plan = de._analyze_expr('[Total & Sales]')
        assert plan[0][0] == de._P_BRACKET1

    def test_chain_order_tcol_before_binary(self):
        """S[a-b] is a column named a-b, not subtraction."""
        plan = de._analyze_expr('S[a-b]')
        assert plan[0][0] == de._P_TCOL

    def test_binary_precedes_function(self):
        """FUNC() + 1 is binary arithmetic whose left side is a call."""
        plan = de._analyze_expr('SUM(T[c]) + 1')
        assert plan[0][0] == de._P_BINARY


class TestRuntimeFallthroughs:
    def test_identifier_resolves_as_var_when_in_scope(self):
        assert _ev('myvar', var_scope={'myvar': 42}) == 42

    def test_identifier_falls_to_table_tail_when_not_a_var(self):
        tables = {'myvar': {'columns': ['a'], 'rows': [[1], [2]]}}
        ctx = de.DAXContext(tables, {}, None, None, None, [])
        got = _ev('myvar', ctx=ctx, var_scope={'other': 1})
        assert isinstance(got, list) and len(got) == 2

    def test_blank_comparison_falls_through_like_the_old_chain(self):
        """BLANK() = "x" coerces and compares; the comparison branch still
        RETURNS a bool here -- the fallthrough is for the None case only, and
        plan and interpreter must agree on which is which."""
        assert _ev('BLANK() = 0') is True
        assert _ev('BLANK() = ""') is True

    def test_plan_cache_is_populated_and_reused(self):
        de._PLAN_CACHE.clear()
        expr = '17 + 25 + 100'
        assert _ev(expr) == 142
        assert expr in de._PLAN_CACHE
        before = de._PLAN_CACHE[expr]
        assert _ev(expr) == 142
        assert de._PLAN_CACHE[expr] is before  # reused, not rebuilt


class TestCalcColumnMaskVariables:
    """The calc-column loop binds masked aggregates / lookups as engine
    variables so the SAME text evaluates on every row."""

    def test_aggregate_mask_binds_once_and_resolves(self):
        cols = ['Year', 'V']
        rows = [[2020, 1], [2021, 2], [2022, 3]]
        snap = {'T': {'columns': cols, 'rows': [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, "('T'[Year] - MIN('T'[Year])) * 10 + 'T'[V]",
            'T', snap, [])
        assert err is None
        assert vals == [1, 12, 23]

    def test_expression_containing_a_mask_token_is_refused(self):
        """A user expression that literally contains __AGG0__ would be
        SHADOWED by our variable binding. The old text-splicing path silently
        replaced it with the masked aggregate -- a wrong value. Refusing is
        the only honest answer."""
        cols = ['A']
        rows = [[1]]
        snap = {'T': {'columns': cols, 'rows': [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, 'VAR __AGG0__ = 1 RETURN MIN(\'T\'[A]) + __AGG0__',
            'T', snap, [])
        assert vals is None
        assert 'reserved' in (err or '')

    def test_unresolved_reference_still_refuses(self):
        cols = ['A']
        rows = [[1], [2]]
        snap = {'T': {'columns': cols, 'rows': [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, "'T'[A] + 'Other'[B]", 'T', snap, [])
        assert vals is None
        assert 'cannot resolve in row context' in err

    def test_string_value_that_looks_like_dax_is_not_reparsed(self):
        """Text substitution re-embedded values as DAX source; a value that
        happened to contain an expression-looking pattern could interact with
        later replacements. Row-context resolution hands the value over as-is."""
        cols = ['S']
        rows = [['a & b'], ['T[x] + 1'], ['"quoted"']]
        snap = {'T': {'columns': cols, 'rows': [list(r) for r in rows]}}
        vals, err = evaluate_row_context_column(
            cols, rows, "UPPER('T'[S])", 'T', snap, [])
        assert err is None
        assert vals == ['A & B', 'T[X] + 1', '"QUOTED"']


class TestColumnDataCache:
    def test_cached_list_is_the_same_object_within_one_context(self):
        tables = {'T': {'columns': ['a'], 'rows': [[1], [2], [3]]}}
        ctx = de.DAXContext(tables, {}, None, None, None, [])
        first = ctx.get_column_data('T', 'a')
        second = ctx.get_column_data('T', 'a')
        assert first is second
        assert first == [1, 2, 3]

    def test_fresh_context_gets_fresh_data(self):
        tables = {'T': {'columns': ['a'], 'rows': [[1]]}}
        c1 = de.DAXContext(tables, {}, None, None, None, [])
        assert c1.get_column_data('T', 'a') == [1]
        tables2 = {'T': {'columns': ['a'], 'rows': [[7]]}}
        c2 = de.DAXContext(tables2, {}, None, None, None, [])
        assert c2.get_column_data('T', 'a') == [7]


@pytest.mark.parametrize("expr,want", [
    ("10 - 3 - 2", 5),
    ("20 / 4 / 5", 1.0),
    ('"0" & 0', "00"),
    ("DATE(2020,1,3) - DATE(2020,1,1)", 2.0),
    ("FORMAT(1, \"000\")", "001"),
    ("IF(BLANK() < 50, 20, 80)", 20),
])
def test_prior_silent_wrong_value_fixes_hold_under_plans(expr, want):
    """The 0.9.54 semantics fixes must survive the compiled-plan rewrite."""
    assert _ev(expr) == want


class TestOperatorPrecedence:
    """Split order IS precedence; every rule here is Desktop-verified.

    The old chain split arithmetic before comparison and before `&`, and NOT
    before `&&`. `a - b < 0` parsed as `a - (b < 0)`: the blank comparison
    became 0, the condition collapsed to `a` (always truthy), and
    Employee[TenureDays] materialized sign-flipped on 1.25M rows.
    GeoSales' conditional-formatting measures answered #D64550 where Desktop
    answers #118DFF. Each expected value below is Power BI Desktop's own
    answer, probed live via ADOMD.
    """

    @pytest.mark.parametrize("expr,desktop_says", [
        ("10 - 3 < 0", False),
        ("3 - 10 < 0", True),
        ("1 + 2 = 3", True),
        ('"a" & 1 + 2', "a3"),
        ('1 + 2 & "b"', "3b"),
        ("2 * 3 - 1", 5),
        ("NOT 1 = 2", True),
        ("NOT FALSE() && FALSE()", False),
        ("5 - 2 - 1 < 3", True),
        ("1 = 1 && 2 = 2", True),
    ])
    def test_desktop_verified_precedence(self, expr, desktop_says):
        assert _ev(expr) == desktop_says

    def test_the_tenuredays_shape(self):
        """IF(a-b<0, b-a, a-b) — the absolute-difference idiom."""
        import datetime as dt
        cols = ["date", "HireDate"]
        rows = [[dt.datetime(2013, 1, 1), dt.datetime(2014, 6, 1)],
                [dt.datetime(2015, 1, 1), dt.datetime(2014, 6, 1)]]
        snap = {"Employee": {"columns": cols, "rows": [list(r) for r in rows]}}
        e = ("IF('Employee'[date]-'Employee'[HireDate]<0,"
             "'Employee'[HireDate]-'Employee'[date],"
             "'Employee'[date]-'Employee'[HireDate])")
        vals, err = evaluate_row_context_column(
            cols, [list(r) for r in rows], e, "Employee", snap, [])
        assert err is None
        assert vals == [516.0, 214.0]  # both POSITIVE — it is an abs()

    def test_the_conditional_formatting_shape(self):
        """IF([a]-[b] < 0, red, blue) — GeoSales' CF measures. Desktop says
        #118DFF for a positive difference; 0.9.54 said #D64550."""
        assert _ev('IF(7 - 3 < 0, "#D64550", "#118DFF")') == "#118DFF"
        assert _ev('IF(3 - 7 < 0, "#D64550", "#118DFF")') == "#D64550"


class TestParenthesesInColumnNames:
    def test_column_named_with_parens_resolves_in_row_context(self):
        """The TCOL branch demanded `'(' not in expr`, so a reference to
        [People using ... (% of population)] fell through to the bare-table
        tail and read BLANK: real Microsoft sample columns bucketed into the
        wrong band on thousands of rows."""
        cols = ["People using at least basic drinking water services (% of population)"]
        rows = [[57.52], [43.0]]
        snap = {"Indicators": {"columns": cols, "rows": [list(r) for r in rows]}}
        e = ("IF(Indicators[People using at least basic drinking water "
             "services (% of population)] < 50, \"low\", \"high\")")
        vals, err = evaluate_row_context_column(
            cols, [list(r) for r in rows], e, "Indicators", snap, [])
        assert err is None
        assert vals == ["high", "low"]


class TestNestedMaskScopeIsCurrentRow:
    """A RELATED mask inside a LOOKUPVALUE search value must resolve against
    THIS row's scope. The scope used to be installed after the LOOKUPVALUE
    resolution, so the nested mask read the PREVIOUS row's value -- every row
    silently materialized the previous row's lookup result, shifted by one.
    Found by adversarial review; Desktop ground truth is the unshifted chain.
    """

    def test_related_inside_lookupvalue_uses_this_rows_value(self):
        rels = [{"FromTable": "T", "FromColumn": "fk",
                 "ToTable": "Third", "ToColumn": "key", "IsActive": 1}]
        snap = {
            "T": {"columns": ["id", "fk"],
                  "rows": [[10, 1], [11, 2], [12, 3]]},
            "Third": {"columns": ["key", "K2"],
                      "rows": [[1, "A"], [2, "B"], [3, "C"]]},
            "Other": {"columns": ["Key", "Val"],
                      "rows": [["A", "vA"], ["B", "vB"], ["C", "vC"]]},
        }
        e = "LOOKUPVALUE(Other[Val], Other[Key], RELATED(Third[K2]))"
        vals, err = evaluate_row_context_column(
            ["id", "fk"], [[10, 1], [11, 2], [12, 3]], e, "T", snap, rels)
        assert err is None
        assert vals == ["vA", "vB", "vC"]  # NOT [None, "vA", "vB"]
