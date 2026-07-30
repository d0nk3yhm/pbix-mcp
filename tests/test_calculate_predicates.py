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
    def test_missing_column_is_ignored(self, pred):
        """PRE-EXISTING, verified against the unpatched engine: a filter naming a
        column that does not exist is silently ignored, for `=` too. Pinned here
        so a change in that behaviour is a deliberate decision, not a surprise --
        it is NOT desirable."""
        assert ev(C + pred + ")") == TOTAL


class TestInMachinery:
    """``_eval_in`` / ``_in_set_values`` back CALCULATE's IN support. They are
    also correct as a general operator, but that is deliberately NOT wired into
    the expression planner -- see the comment in _analyze_expr. Enabling it made
    seven Agents_Performance measures return a CONFIDENTLY WRONG value instead of
    BLANK, because they wrap IN around a RANKX/TOPN chain that is independently
    inaccurate. These tests keep the machinery honest for the day that lands.
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
