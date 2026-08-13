"""CALCULATE boolean filter arguments (OpenBI findings #9 #5b).

Before 0.9.60 only ``Table[Col] = value`` was honoured. Every other predicate
fell off the end of CALCULATE's filter loop adding NO filter and NO warning, so
the measure quietly returned the UNFILTERED total with
``unsupported_functions`` empty -- a plausible number, silently wrong.

The expected values here are arithmetic over a four-row fixture, so they are
checkable by hand; the same shapes were also confirmed against Power BI
Desktop's own engine on the Agents_Performance corpus file, where five measures
went from BLANK to exactly Desktop's answer.
"""
from __future__ import annotations

import pytest

from pbix_mcp.dax import engine as de

TABLES = {"P": {"columns": ["S", "V"],
                "rows": [["Lead", 100], ["Closed Won", 200],
                         ["Closed Lost", 50], ["Proposal", 300]]}}
TOTAL = 650


def ev(expr):
    ctx = de.DAXContext(TABLES, {"M": expr}, None, None, None, [])
    return de.DAXEngine().evaluate_measure("M", ctx)


C = "CALCULATE(SUM(P[V]), "


class TestComparisonPredicates:
    @pytest.mark.parametrize("pred,want", [
        ('P[S] = "Lead"', 100),          # the one shape that always worked
        ('P[S] <> "Lead"', 550),
        ('P[V] > 100', 500),
        ('P[V] >= 200', 500),
        ('P[V] < 100', 50),
        ('P[V] <= 100', 150),
        ('P[V] > 50 + 50', 500),         # right side is an expression
        ("'P'[S] <> \"Lead\"", 550),     # quoted table name
    ])
    def test_predicate_actually_filters(self, pred, want):
        got = ev(C + pred + ")")
        assert got == want, f"{pred}: got {got}, want {want}"
        assert got != TOTAL or want == TOTAL, "filter was silently dropped"


class TestInPredicate:
    @pytest.mark.parametrize("pred,want", [
        ('P[S] IN {"Lead","Proposal"}', 400),
        ('NOT(P[S] IN {"Closed Won","Closed Lost"})', 400),
        ('NOT(P[S] IN {"Lead"})', 550),
    ])
    def test_in_set(self, pred, want):
        assert ev(C + pred + ")") == want


class TestNotAndKeepfilters:
    """Both used to be swallowed by the equality regex, which matched the
    WRAPPER text and registered a filter on a column named "NOT(P"."""

    @pytest.mark.parametrize("pred,want", [
        ('NOT P[S] = "Lead"', 550),               # no parens
        ('NOT(P[V] >= 200)', 150),
        ('NOT P[V] > 100', 150),
        ('NOT(NOT(P[S] = "Lead"))', 100),         # double negation
        ('KEEPFILTERS(P[S] = "Lead")', 100),
        ('KEEPFILTERS(P[V] > 100)', 500),
    ])
    def test_wrapper_is_peeled(self, pred, want):
        assert ev(C + pred + ")") == want


class TestMultiplePredicates:
    def test_different_columns_are_anded(self):
        assert ev(C + 'P[S] <> "Lead", P[V] > 100)') == 500

    def test_same_column_intersects(self):
        """DAX ANDs multiple filter arguments. Two comparisons on one column
        cannot share a flat spec (both need the "op" key), hence the {"all":
        [...]} conjunction."""
        assert ev(C + 'P[V] > 50, P[V] < 300)') == 300

    def test_outer_filter_is_replaced_not_intersected(self):
        """A CALCULATE predicate OVERRIDES the outer context on that column --
        that override is the whole point of CALCULATE, so it must not be ANDed
        with the outer filter the way sibling arguments are."""
        ctx = de.DAXContext(TABLES, {"M": C + 'P[S] <> "Lead")'},
                            None, None, {"P.S": ["Lead"]}, [])
        assert de.DAXEngine().evaluate_measure("M", ctx) == 550


class TestUnchangedBehaviour:
    @pytest.mark.parametrize("pred,want", [
        ('FILTER(P, P[V] > 100)', 500),
        ('ALL(P)', TOTAL),
        ('VALUES(P[S])', TOTAL),
    ])
    def test_table_valued_filters_still_work(self, pred, want):
        assert ev(C + pred + ")") == want

    @pytest.mark.parametrize("pred", ['P[Nope] > 1', 'P[Nope] = 1'])
    def test_missing_column_refuses_rather_than_being_ignored(self, pred):
        """This test used to pin the OPPOSITE, flagged there as "NOT desirable"
        and to be changed deliberately. This is that change.

        A filter naming a column that does not exist was silently dropped, so
        CALCULATE returned the UNFILTERED total -- a confident wrong number.
        Desktop refuses the expression ("Column 'X' in table 'Y' cannot be found
        or may not be used in this expression"); the engine now does too, which
        surfaces as BLANK rather than a plausible figure. MS_Life_Expectancy is
        the corpus case: five measures sum a column the model does not have, and
        [Health] answered 3,104,480 where Desktop cannot evaluate it at all.
        """
        assert ev(C + pred + ")") is None


class TestInMachinery:
    """``_eval_in`` / ``_in_set_values`` back CALCULATE's IN support. They are
    also correct as a general operator, but that is deliberately NOT wired into
    the expression planner -- see the comment in _analyze_expr. Enabling it made
    seven Agents_Performance measures return a CONFIDENTLY WRONG value instead
    of BLANK. These tests keep the machinery honest for the day that lands.

    The REASON recorded here was wrong, and the correction is worth keeping.
    It said those measures "wrap IN around a RANKX/TOPN chain that is
    independently inaccurate". Measured against Desktop on 2026-07-30, the chain
    is not inaccurate: `[MTD Total Sales] @ StoreType=Catalog` is 1783540.7792
    in Desktop and identical here, and the non-blank-MTD employee count is 1 in
    both. The defect is REVERSE FILTER PROPAGATION -- our single-hop
    relationship index is symmetric, so filtering the many side restricts the
    one side and `SELECTEDVALUE(DimEmployee[EmployeeKey])` answers 213 where
    Desktop answers BLANK. See PROGRESS.md; a directional fix reached 408/408 on
    that file but broke MS_Employee_Hiring's [Actives] and was reverted.
    """

    def _ctx(self):
        return de.DAXContext(TABLES, {}, None, None, None, [])

    @pytest.mark.parametrize("left,right,want", [
        ('"a"', '{"a","b"}', True),
        ('"z"', '{"a","b"}', False),
        ('2', '{1,2,3}', True),
        ('9', '{1,2,3}', False),
        ('1', '{}', False),              # a literal empty set IS the empty set
        ('BLANK()', '{1,2}', False),     # DAX: BLANK() IN {1,2} is FALSE
    ])
    def test_membership(self, left, right, want):
        assert de.DAXEngine()._eval_in(left, right, self._ctx()) is want

    def test_row_constructor_is_refused_not_guessed(self):
        """{(1,"a")} is a multi-column set; comparing against the tuple text
        would be nonsense, so report unknown."""
        assert de.DAXEngine()._in_set_values('{(1,"a"),(2,"b")}', self._ctx()) is None

    def test_empty_table_expression_is_false_not_unknown(self):
        """DAX: membership in an EMPTY table is FALSE.

        This originally returned "unknown" to hedge against a table function the
        engine cannot evaluate in the current scope (VALUES(T[C]) inside a row
        context yields []). That hedge was worse than the thing it guarded: the
        IN step fell through to the next plan step, which could answer with
        something truthy, and Agents_Performance's "Rank Filtering Employyees
        MTD" returned 1 where Desktop returns 0 -- its TOPN is legitimately empty
        and both IN tests should simply be FALSE.
        """
        eng = de.DAXEngine()
        ctx = self._ctx()
        ctx._current_row = {"__table__": "P", "S": "Lead", "V": 100}
        assert eng._in_set_values('VALUES(P[S])', ctx) == []
        assert eng._eval_in('"Lead"', 'VALUES(P[S])', ctx) is False

    def test_single_column_table_expression(self):
        vals = de.DAXEngine()._in_set_values('VALUES(P[S])', self._ctx())
        assert sorted(vals) == ["Closed Lost", "Closed Won", "Lead", "Proposal"]


class TestNumericTypeCoercionInFilters:
    """Issue #39: CALCULATE(expr, T[Year]=2024) went BLANK on Double-typed
    columns — the In-set fast path and make_value_matcher both matched by
    str(), and str(2024) != str(2024.0), so the sugar selected ZERO rows
    while the explicit FILTER form (numeric comparison) answered 350. Both
    sides now match numerically whenever both parse as numbers; the
    literal-first form (2024=T[Year]) also registers instead of silently
    applying nothing."""

    DOUBLE_ROWS = [['A', 2024.0, 100], ['B', 2024.0, 250], ['A', 2023.0, 70]]
    INT_ROWS = [['A', 2024, 100], ['B', 2024, 250], ['A', 2023, 70]]

    def _ev(self, expr, rows):
        from pbix_mcp.dax.engine import DAXContext, DAXEngine
        tables = {'Fact': {'columns': ['Cat', 'Year', 'Sales'], 'rows': rows}}
        return DAXEngine().evaluate_measure(
            'M', DAXContext(tables, {'M': expr}))

    @pytest.mark.parametrize("rows,lit", [
        (DOUBLE_ROWS, "2024"),      # the issue's shape: Double cells, int literal
        (INT_ROWS, "2024.0"),       # and the mirror image
        (INT_ROWS, "2024"),
        (DOUBLE_ROWS, "2024.0"),
    ])
    def test_equality_sugar_matches_filter_form(self, rows, lit):
        sugar = self._ev(
            f"CALCULATE(SUM('Fact'[Sales]), 'Fact'[Year]={lit})", rows)
        explicit = self._ev(
            f"CALCULATE(SUM('Fact'[Sales]), "
            f"FILTER(ALL('Fact'[Year]),'Fact'[Year]={lit}))", rows)
        assert sugar == explicit == 350

    def test_in_and_not_in_coerce(self):
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), 'Fact'[Year] IN {2024})",
            self.DOUBLE_ROWS) == 350
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), NOT('Fact'[Year] IN {2024}))",
            self.DOUBLE_ROWS) == 70

    def test_literal_first_predicate_registers(self):
        # used to fall off the filter loop and answer the 420 grand total
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), 2024='Fact'[Year])",
            self.DOUBLE_ROWS) == 350
        # flipped comparison operators
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), 2023<'Fact'[Year])",
            self.DOUBLE_ROWS) == 350
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), 2024>'Fact'[Year])",
            self.DOUBLE_ROWS) == 70

    def test_no_match_still_blank_and_strings_unaffected(self):
        assert self._ev(
            "CALCULATE(SUM('Fact'[Sales]), 'Fact'[Year]=1999)",
            self.DOUBLE_ROWS) is None
        assert self._ev(
            'CALCULATE(SUM(Fact[Sales]), Fact[Cat]="A")',
            self.DOUBLE_ROWS) == 170

    def test_boolean_true_does_not_alias_one(self):
        from pbix_mcp.dax.engine import DAXContext, DAXEngine
        tables = {'T': {'columns': ['Flag', 'V'],
                        'rows': [[True, 10], [False, 20], [1.0, 40]]}}
        got = DAXEngine().evaluate_measure('M', DAXContext(
            tables, {'M': "CALCULATE(SUM(T[V]), T[Flag] IN {1})"}))
        # the numeric 1.0 row matches; the boolean True row must NOT
        assert got == 40

    def test_caller_filter_context_coerces_too(self):
        # the same str() mismatch hit {"Fact.Year": [2024]} from the tools
        from pbix_mcp.dax.engine import DAXContext, DAXEngine
        tables = {'Fact': {'columns': ['Cat', 'Year', 'Sales'],
                           'rows': self.DOUBLE_ROWS}}
        got = DAXEngine().evaluate_measure('M', DAXContext(
            tables, {'M': "SUM('Fact'[Sales])"},
            filter_context={'Fact.Year': [2024]}))
        assert got == 350
