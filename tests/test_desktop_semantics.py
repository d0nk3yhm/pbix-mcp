"""DAX rules read off the live Power BI Desktop engine, not the documentation.

Every expected value in this file was produced by querying the workspace
msmdsrv instance Desktop starts for an open .pbix (ADOMD over localhost), so it
is Desktop's own answer rather than an interpretation of the docs. The rules
here were all WRONG in this engine before, and each one was silently wrong --
a plausible number or a confident zero, never an error.
"""
from __future__ import annotations

import math
from datetime import date, datetime

import pytest

from pbix_mcp.dax import engine as de
from pbix_mcp.dax.engine import _dax_datetime_str, _dax_number_str

TABLES = {
    "T": {"columns": ["n", "s"],
          "rows": [[10, "a"], [20, "b"], [None, "c"], [40, None]]},
}


def ev(expr, tables=None):
    ctx = de.DAXContext(tables or TABLES, {"M": expr}, None, None, None, [])
    return de.DAXEngine().evaluate_measure("M", ctx)


class TestNumberToString:
    """DAX renders a number with at most 15 significant digits, and chooses
    fixed vs scientific notation by WIDTH -- fixed only while it needs at most
    15 decimal places AND at most 15 integer digits."""

    @pytest.mark.parametrize("value,want", [
        (1 / 3, '0.333333333333333'),
        (1561.0900000000001, '1561.09'),
        (0.12467217240315605, '0.124672172403156'),
        (2 / 3 + 1e-16, '0.666666666666667'),
        (-0.11688485470237962, '-0.11688485470238'),
        (4.0, '4'),
        (0.0 * -1, '-0'),
        # fixed / scientific boundary, low end
        (1e-7, '0.0000001'),
        (1.5e-9, '0.0000000015'),
        (1.234567e-9, '0.000000001234567'),      # 15 decimals -> fixed
        (1.2345678e-9, '1.2345678E-09'),         # 16 -> scientific
        (5e-15, '0.000000000000005'),
        (5e-16, '5E-16'),
        (1 / 30, '3.33333333333333E-02'),        # 16 decimals -> scientific
        (1 / 3e8, '3.33333333333333E-09'),
        (1e-21, '1E-21'),
        # fixed / scientific boundary, high end
        (123456789012345.0, '123456789012345'),  # 15 integer digits -> fixed
        (12345678901234.5, '12345678901234.5'),
        (1e14, '100000000000000'),
        (1e15, '1E+15'),                         # 16 -> scientific
        (1.23456789012345678e17, '1.23456789012346E+17'),
        (1.5e22, '1.5E+22'),
    ])
    def test_matches_desktop(self, value, want):
        assert _dax_number_str(value) == want

    def test_through_the_concat_operator(self):
        """The rule has to reach `&`, which is where it actually mattered.

        The corpus's SVG/HTML measures paste numbers into markup; Python's
        17-digit repr made four of them longer than Desktop's output by exactly
        the surplus digits.
        """
        assert ev('"x=" & (1/3)') == 'x=0.333333333333333'
        assert ev('"x=" & (1561.09 * 1)') == 'x=1561.09'

    def test_non_finite_renders_like_desktop(self):
        assert _dax_number_str(float('inf')) == 'inf'
        assert _dax_number_str(float('-inf')) == '-inf'
        assert _dax_number_str(float('nan')) == '-nan(ind)'


class TestDateToString:
    @pytest.mark.parametrize("value,want", [
        (datetime(2025, 7, 1), '7/1/2025'),
        (datetime(2025, 12, 25), '12/25/2025'),
        (date(2025, 11, 3), '11/3/2025'),
        (datetime(2025, 7, 1, 12), '7/1/2025 12:00:00 PM'),
        (datetime(2025, 7, 1, 6), '7/1/2025 6:00:00 AM'),
        (datetime(2025, 7, 1, 0, 0, 1), '7/1/2025 12:00:01 AM'),
        # DAX serial 0 carries no date, so Desktop prints the time alone
        (datetime(1899, 12, 30), '12:00:00 AM'),
        (datetime(1899, 12, 30, 13, 5, 9), '1:05:09 PM'),
    ])
    def test_matches_desktop(self, value, want):
        assert _dax_datetime_str(value) == want


class TestStrictEquality:
    """`==` is the one comparison that does not coerce a blank."""

    @pytest.mark.parametrize("expr,want", [
        ('IF(1==1, "T", "F")', 'T'),
        ('IF("A"=="A", "T", "F")', 'T'),
        ('IF(1==2, "T", "F")', 'F'),
        ('IF(BLANK()==BLANK(), "T", "F")', 'T'),
        # the whole point: `=` coerces, `==` does not
        ('IF(BLANK()==0, "T", "F")', 'F'),
        ('IF(BLANK()=0, "T", "F")', 'T'),
        ('IF(0==BLANK(), "T", "F")', 'F'),
        ('IF(BLANK()=="", "T", "F")', 'F'),
        ('IF(BLANK()="", "T", "F")', 'T'),
        # the other operators must still parse -- `==` is checked first and the
        # scanner must not shred it into two `=` splits
        ('IF(1<>2, "T", "F")', 'T'),
        ('IF(1<=2, "T", "F")', 'T'),
        ('IF(2>=2, "T", "F")', 'T'),
        ('IF(1=1, "T", "F")', 'T'),
    ])
    def test_matches_desktop(self, expr, want):
        assert ev(expr) == want


class TestEmptyAggregatesAreBlank:
    """Desktop returns BLANK, never 0, for an aggregate over no rows.

    Checked with CALCULATE(<agg>, FILTER(ALL(T), FALSE())) and with
    <agg>X(FILTER({1,2,3}, FALSE()), 1) on the live engine.
    """

    EMPTY = {"E": {"columns": ["n"], "rows": []}}

    @pytest.mark.parametrize("expr", [
        'SUM(E[n])', 'AVERAGE(E[n])', 'COUNT(E[n])', 'COUNTA(E[n])',
        'COUNTROWS(E)', 'DISTINCTCOUNT(E[n])', 'DISTINCTCOUNTNOBLANK(E[n])',
        'MIN(E[n])', 'MAX(E[n])', 'MEDIAN(E[n])', 'PRODUCT(E[n])',
        'COUNTBLANK(E[n])',
        'SUMX(FILTER(E, TRUE()), E[n])', 'AVERAGEX(FILTER(E, TRUE()), E[n])',
        'COUNTX(FILTER(E, TRUE()), E[n])', 'MINX(FILTER(E, TRUE()), E[n])',
        'MAXX(FILTER(E, TRUE()), E[n])', 'MEDIANX(FILTER(E, TRUE()), E[n])',
    ])
    def test_blank_not_zero(self, expr):
        got = ev(expr, self.EMPTY)
        assert got is None, f"{expr} returned {got!r}, must be BLANK"

    def test_a_populated_aggregate_is_unaffected(self):
        assert ev('SUM(T[n])') == 70
        assert ev('COUNT(T[n])') == 3
        assert ev('COUNTROWS(T)') == 4


class TestDistinctCountCountsTheBlank:
    """DISTINCTCOUNT counts BLANK as one distinct value; the NOBLANK variant
    does not. MS_Blog_2020_Sep proved it numerically: 119386 distinct customers
    plus 8 blank rows is 119387 in Desktop."""

    def test_blank_is_one_of_the_distinct_values(self):
        assert ev('DISTINCTCOUNT(T[s])') == 4      # a, b, c, BLANK
        assert ev('DISTINCTCOUNTNOBLANK(T[s])') == 3

    def test_count_still_skips_the_blank(self):
        assert ev('COUNT(T[n])') == 3
        assert ev('COUNTA(T[s])') == 3


class TestMround:
    """MROUND rounds half AWAY FROM ZERO, and refuses a sign mismatch."""

    @pytest.mark.parametrize("expr,want", [
        ('MROUND(2.5, 1)', 3),
        ('MROUND(3.5, 1)', 4),
        ('MROUND(7, 3)', 6),
        ('MROUND(7, 0)', 0),
        ('MROUND(-7, -3)', -6),
    ])
    def test_matches_desktop(self, expr, want):
        assert ev(expr) == want

    def test_mixed_signs_are_refused(self):
        """Desktop errors on MROUND(-2.5, 1) rather than returning a number, so
        answering with one would be inventing a result."""
        assert ev('MROUND(-2.5, 1)') is None

    def test_decimal_multiple(self):
        got = ev('MROUND(1.234, 0.05)')
        assert math.isclose(got, 1.25, rel_tol=1e-12)


class TestFormat:
    """FORMAT's numeric custom format strings, every value from Desktop."""

    @pytest.mark.parametrize("value,fmt,want", [
        # A comma immediately before the decimal point SCALES BY 1000; a comma
        # between digits is grouping. Reading the scaling comma as grouping is
        # what made two GeoSales_Dashboard SVG measures longer than Desktop's.
        (2297200.9, "$#,##0,.0K", "$2,297.2K"),
        (2297200.9, "$#,##0,.0", "$2,297.2"),
        (2297200.9, "0,.0", "2297.2"),
        (1234567, "#,##0,,.0M", "1.2M"),
        (2297200.9, "$#,##0", "$2,297,201"),
        # HALF AWAY FROM ZERO, not Python's banker's rounding.
        (1234.5, "#,##0", "1,235"),
        (1235.5, "#,##0", "1,236"),
        (0.125, "0.00", "0.13"),
        (0.135, "0.00", "0.14"),
        # sections: positive;negative;zero
        (-0.05, "0.0%;(0.0%)", "(5.0%)"),
        (0, "0.0;(0.0);zero", "zero"),
        (-1234.5, "#,##0.00", "-1,234.50"),
        # required vs optional digits
        (1, "000", "001"),
        (2.5, "0.##", "2.5"),
        (2, "0.##", "2."),          # the separator survives its dropped digits
        (0.5, "#.##", ".5"),
        (0.1234, "0.0%", "12.3%"),
        # named formats
        (1234.5, "General Number", "1234.5"),
        (1234.5, "Currency", "$1,234.50"),
        (0.25, "Percent", "25.00%"),
    ])
    def test_matches_desktop(self, value, fmt, want):
        assert ev(f'FORMAT({value}, "{fmt}")') == want


class TestIsInScope:
    """ISINSCOPE asks about the query's GROUPING axes, which a single-cell
    measure evaluation has none of -- Desktop answers FALSE there. It is NOT
    ISFILTERED: on the same model Desktop gives
        CALCULATE(ISINSCOPE(T[s]), T[s] = "a")  -> FALSE
        CALCULATE(ISFILTERED(T[s]), T[s] = "a") -> TRUE
    """

    def test_false_without_a_grouping(self):
        assert ev('IF(ISINSCOPE(T[s]), "T", "F")') == 'F'

    def test_false_even_when_filtered(self):
        assert ev('IF(CALCULATE(ISINSCOPE(T[s]), T[s] = "a"), "T", "F")') == 'F'

    def test_isfiltered_still_says_true(self):
        assert ev('IF(CALCULATE(ISFILTERED(T[s]), T[s] = "a"), "T", "F")') == 'T'
