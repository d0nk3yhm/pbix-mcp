"""
DAX Engine Accuracy Tests — Tests targeting known weak spots.
These test the edge cases that break on real-world reports.
Run: python -m pytest tests/test_dax_accuracy.py -v
"""
import math

import pytest

from pbix_mcp.dax.engine import DAXContext, DAXEngine

pytestmark = pytest.mark.unit


@pytest.fixture
def engine():
    return DAXEngine()


@pytest.fixture
def tables():
    return {
        'Sales': {
            'columns': ['ID', 'Date', 'ProductID', 'CustomerID', 'Amount', 'Qty', 'Cost'],
            'rows': [
                [1, '2024-01-15', 'P1', 'C1', 100.0, 2, 60.0],
                [2, '2024-01-20', 'P2', 'C2', 200.0, 3, 110.0],
                [3, '2024-02-10', 'P1', 'C1', 150.0, 1, 90.0],
                [4, '2024-03-05', 'P3', 'C3', 300.0, 5, 180.0],
                [5, '2024-03-15', 'P2', 'C2', 250.0, 4, 140.0],
                [6, '2023-01-10', 'P1', 'C1', 80.0, 2, 50.0],
                [7, '2023-06-20', 'P2', 'C2', 120.0, 3, 70.0],
                [8, '2023-11-05', 'P3', 'C3', 90.0, 1, 55.0],
                [9, '2024-01-15', 'P1', 'C3', 50.0, 1, 30.0],
                [10, '2024-02-28', 'P3', 'C1', 400.0, 6, 240.0],
            ],
        },
        'Products': {
            'columns': ['ProductID', 'Name', 'Category', 'SubCategory'],
            'rows': [
                ['P1', 'Widget A', 'Hardware', 'Tools'],
                ['P2', 'Gadget B', 'Electronics', 'Phones'],
                ['P3', 'Thing C', 'Hardware', 'Parts'],
            ],
        },
        'Customers': {
            'columns': ['CustomerID', 'Name', 'Region', 'Segment'],
            'rows': [
                ['C1', 'Alice', 'East', 'Consumer'],
                ['C2', 'Bob', 'West', 'Corporate'],
                ['C3', 'Charlie', 'East', 'Consumer'],
            ],
        },
        'Calendar': {
            'columns': ['Date', 'Year', 'Month', 'MonthName', 'Quarter', 'YearMonth'],
            'rows': [
                ['2023-01-10', 2023, 1, 'Jan', 1, '2023-01'],
                ['2023-06-20', 2023, 6, 'Jun', 2, '2023-06'],
                ['2023-11-05', 2023, 11, 'Nov', 4, '2023-11'],
                ['2024-01-15', 2024, 1, 'Jan', 1, '2024-01'],
                ['2024-01-20', 2024, 1, 'Jan', 1, '2024-01'],
                ['2024-02-10', 2024, 2, 'Feb', 1, '2024-02'],
                ['2024-02-28', 2024, 2, 'Feb', 1, '2024-02'],
                ['2024-03-05', 2024, 3, 'Mar', 1, '2024-03'],
                ['2024-03-15', 2024, 3, 'Mar', 1, '2024-03'],
            ],
        },
    }


@pytest.fixture
def rels():
    return [
        {'FromTable': 'Sales', 'FromColumn': 'Date', 'ToTable': 'Calendar', 'ToColumn': 'Date', 'IsActive': 1},
        {'FromTable': 'Sales', 'FromColumn': 'ProductID', 'ToTable': 'Products', 'ToColumn': 'ProductID', 'IsActive': 1},
        {'FromTable': 'Sales', 'FromColumn': 'CustomerID', 'ToTable': 'Customers', 'ToColumn': 'CustomerID', 'IsActive': 1},
    ]


def ctx(tables, rels, measures, fc=None):
    return DAXContext(tables, measures, 'Calendar', 'Date', fc, rels)


# ===========================================================================
# 1. BLANK Handling — DAX treats BLANK specially in arithmetic
# ===========================================================================

class TestBlankHandling:
    def test_blank_plus_number(self, engine, tables, rels):
        """BLANK + 5 = 5 in DAX (BLANK is treated as 0 in addition)."""
        c = ctx(tables, rels, {'M': 'BLANK() + 5'})
        # BLANK() + 5 should be 5 (BLANK acts as 0 in arithmetic)
        result = engine.evaluate_measure('M', c)
        assert result == 5  # BLANK + 5 = 5 (BLANK is 0 in arithmetic)

    def test_blank_in_divide(self, engine, tables, rels):
        """DIVIDE with BLANK numerator returns BLANK/0."""
        c = ctx(tables, rels, {'M': 'DIVIDE(BLANK(), 10)'})
        result = engine.evaluate_measure('M', c)
        assert result == 0  # DIVIDE(BLANK, 10) = DIVIDE(0, 10) = 0

    def test_if_blank_check(self, engine, tables, rels):
        """IF(ISBLANK(x), ...) pattern."""
        c = ctx(tables, rels, {'M': 'IF(ISBLANK(BLANK()), "yes", "no")'})
        result = engine.evaluate_measure('M', c)
        assert result == "yes"

    def test_divide_by_zero_is_blank(self, engine, tables, rels):
        """DIVIDE(x, 0) with no alternate returns BLANK (None), not 0."""
        c = ctx(tables, rels, {'M': 'DIVIDE(10, 0)'})
        assert engine.evaluate_measure('M', c) is None

    def test_divide_by_zero_alternate(self, engine, tables, rels):
        """DIVIDE(x, 0, alt) returns the alternate."""
        c = ctx(tables, rels, {'M': 'DIVIDE(10, 0, -1)'})
        assert engine.evaluate_measure('M', c) == -1

    def test_divide_normal(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'DIVIDE(10, 2)'})
        assert engine.evaluate_measure('M', c) == 5.0

    def test_binary_divide_by_zero_is_blank_not_numerator(self, engine, tables, rels):
        """Spaced `a / b` with b==0 returns BLANK, not the numerator (regression)."""
        c = ctx(tables, rels, {'M': '10 / 0'})
        assert engine.evaluate_measure('M', c) is None
        c2 = ctx(tables, rels, {'M': '10 / 2'})
        assert engine.evaluate_measure('M', c2) == 5.0


# ===========================================================================
# 2. Nested CALCULATE — multiple filter modifiers
# ===========================================================================

class TestNestedCalculate:
    def test_calculate_with_literal_filter(self, engine, tables, rels):
        """CALCULATE(SUM(Sales[Amount]), Products[Category] = "Hardware")"""
        c = ctx(tables, rels, {
            'M': "CALCULATE(SUM(Sales[Amount]), Products[Category] = \"Hardware\")"
        })
        result = engine.evaluate_measure('M', c)
        # Hardware = P1+P3: 100+150+80+50+300+400+90 = 1170
        assert result == pytest.approx(1170.0, rel=0.01)

    def test_calculate_removefilters(self, engine, tables, rels):
        """CALCULATE with REMOVEFILTERS to get grand total."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'Pct': "DIVIDE([Total], CALCULATE([Total], REMOVEFILTERS(Products[Category])))"
        }, {'Products.Category': ['Hardware']})
        result = engine.evaluate_measure('Pct', c)
        # REMOVEFILTERS removes the Category filter for the denominator, so this
        # is Hardware's share of the grand total: Hardware = P1(380)+P3(790)=1170,
        # total across all categories = 1740 -> 0.6724.
        # (Pre-0.9.8 this wrongly returned 1.0 because REMOVEFILTERS on an
        # UNQUOTED Products[Category] was silently a no-op.)
        assert isinstance(result, float)
        assert result == pytest.approx(1170 / 1740, abs=1e-4)

    def test_pct_of_total_unquoted_all(self, engine, tables, rels):
        """% of total with UNQUOTED ALL(Table[Col]) — regression for 0.9.8 #1."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'Pct': 'DIVIDE(SUM(Sales[Amount]), CALCULATE(SUM(Sales[Amount]), ALL(Products[Category])))',
        }, {'Products.Category': ['Electronics']})
        # Electronics = P2 = 570; grand total = 1740 -> 0.3276 (NOT 1.0 no-op).
        assert engine.evaluate_measure('Pct', c) == pytest.approx(570 / 1740, abs=1e-4)

    def test_calculate_all_table(self, engine, tables, rels):
        """CALCULATE with ALL('Table') removes all filters on that table."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'AllProducts': "CALCULATE([Total], ALL('Products'))"
        }, {'Products.Category': ['Electronics']})
        total = engine.evaluate_measure('Total', c)
        all_total = engine.evaluate_measure('AllProducts', c)
        # AllProducts should ignore the category filter
        assert all_total >= total


# ===========================================================================
# 3. Complex VAR/RETURN patterns
# ===========================================================================

class TestComplexVarReturn:
    def test_var_with_calculate(self, engine, tables, rels):
        """VAR using CALCULATE inside."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'M': """
                VAR _current = [Total]
                VAR _ly = CALCULATE([Total], DATEADD('Calendar'[Date], -1, YEAR))
                VAR _change = DIVIDE(_current - _ly, _ly)
                RETURN _change
            """
        }, {'Calendar.Year': [2024]})
        result = engine.evaluate_measure('M', c)
        assert isinstance(result, (int, float))
        assert result == pytest.approx(4.0, abs=0.1)  # YoY change: (2024 total - 2023 total) / 2023 total

    def test_var_with_if(self, engine, tables, rels):
        """VAR with conditional logic."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'M': """
                VAR _total = [Total]
                VAR _status = IF(_total > 1000, "High", IF(_total > 500, "Medium", "Low"))
                RETURN _status
            """
        })
        result = engine.evaluate_measure('M', c)
        assert result in ["High", "Medium", "Low"]

    def test_var_referencing_previous_var(self, engine, tables, rels):
        """Each VAR can reference previously declared VARs."""
        c = ctx(tables, rels, {
            'M': """
                VAR _a = 10
                VAR _b = _a * 2
                VAR _c = _a + _b
                RETURN _c
            """
        })
        result = engine.evaluate_measure('M', c)
        assert result == 30  # 10 + 20


# ===========================================================================
# 4. LOOKUPVALUE — common pattern
# ===========================================================================

class TestLookupValue:
    def test_lookupvalue_basic(self, engine, tables, rels):
        """LOOKUPVALUE(Products[Name], Products[ProductID], "P2")"""
        c = ctx(tables, rels, {
            'M': "LOOKUPVALUE(Products[Name], Products[ProductID], \"P2\")"
        })
        result = engine.evaluate_measure('M', c)
        assert result == "Gadget B"

    def test_lookupvalue_not_found(self, engine, tables, rels):
        """LOOKUPVALUE with no match returns BLANK."""
        c = ctx(tables, rels, {
            'M': "LOOKUPVALUE(Products[Name], Products[ProductID], \"P99\")"
        })
        result = engine.evaluate_measure('M', c)
        assert result is None


# ===========================================================================
# 5. Iterator functions with complex expressions
# ===========================================================================

class TestComplexIterators:
    def test_sumx_with_multiply(self, engine, tables, rels):
        """SUMX(Sales, Sales[Qty] * Sales[Amount])"""
        # This requires row-by-row evaluation
        c = ctx(tables, rels, {
            'M': "SUMX(Sales, Sales[Qty] * Sales[Amount])"
        })
        result = engine.evaluate_measure('M', c)
        # SUMX with row-level arithmetic: sum of Qty * Amount per row
        assert result == pytest.approx(6510.0, rel=0.01)

    def test_averagex(self, engine, tables, rels):
        """AVERAGEX over a table."""
        c = ctx(tables, rels, {
            'M': "AVERAGEX(ALL(Products[Category]), [Total])",
            'Total': 'SUM(Sales[Amount])'
        })
        result = engine.evaluate_measure('M', c)
        assert result == pytest.approx(870.0, rel=0.01)  # Average of category totals

    def test_countx_with_condition(self, engine, tables, rels):
        """COUNTX with IF inside."""
        c = ctx(tables, rels, {
            'M': "COUNTX(ALL(Customers[Region]), IF([Total] > 500, 1, BLANK()))",
            'Total': 'SUM(Sales[Amount])'
        })
        result = engine.evaluate_measure('M', c)
        assert isinstance(result, (int, float))


# ===========================================================================
# 6. Text Functions
# ===========================================================================

class TestTextFunctions:
    def test_left(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'LEFT("Hello World", 5)'})
        assert engine.evaluate_measure('M', c) == "Hello"

    def test_right(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'RIGHT("Hello World", 5)'})
        assert engine.evaluate_measure('M', c) == "World"

    def test_mid(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'MID("Hello World", 7, 5)'})
        assert engine.evaluate_measure('M', c) == "World"

    def test_len(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'LEN("Hello")'})
        assert engine.evaluate_measure('M', c) == 5

    def test_upper(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'UPPER("hello")'})
        assert engine.evaluate_measure('M', c) == "HELLO"

    def test_lower(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'LOWER("HELLO")'})
        assert engine.evaluate_measure('M', c) == "hello"

    def test_trim(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'TRIM("  hello  ")'})
        assert engine.evaluate_measure('M', c) == "hello"

    def test_substitute(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SUBSTITUTE("Hello World", "World", "DAX")'})
        assert engine.evaluate_measure('M', c) == "Hello DAX"

    def test_concatenate(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'CONCATENATE("Hello", " World")'})
        assert engine.evaluate_measure('M', c) == "Hello World"

    def test_combinevalues(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'COMBINEVALUES(", ", "A", "B", "C")'})
        assert engine.evaluate_measure('M', c) == "A, B, C"


# ===========================================================================
# 7. Math Functions
# ===========================================================================

class TestMathFunctions:
    def test_ceiling(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'CEILING(4.3, 1)'})
        assert engine.evaluate_measure('M', c) == 5

    def test_floor(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'FLOOR(4.7, 1)'})
        assert engine.evaluate_measure('M', c) == 4

    def test_mod(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'MOD(10, 3)'})
        assert engine.evaluate_measure('M', c) == 1

    def test_power(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'POWER(2, 10)'})
        assert engine.evaluate_measure('M', c) == 1024

    def test_sqrt(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SQRT(144)'})
        assert engine.evaluate_measure('M', c) == 12

    def test_log10(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'LOG10(1000)'})
        assert engine.evaluate_measure('M', c) == pytest.approx(3.0)

    def test_sign_positive(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SIGN(42)'})
        assert engine.evaluate_measure('M', c) == 1

    def test_sign_negative(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SIGN(-7)'})
        assert engine.evaluate_measure('M', c) == -1

    def test_sign_zero(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SIGN(0)'})
        assert engine.evaluate_measure('M', c) == 0

    def test_pi(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'PI()'})
        assert engine.evaluate_measure('M', c) == pytest.approx(math.pi)

    def test_trunc(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'TRUNC(3.789, 1)'})
        assert engine.evaluate_measure('M', c) == pytest.approx(3.7)


# ===========================================================================
# 8. Information Functions
# ===========================================================================

class TestInfoFunctions:
    def test_isnumber(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'ISNUMBER(42)'})
        assert engine.evaluate_measure('M', c) == True

    def test_isnumber_text(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'ISNUMBER("hello")'})
        assert engine.evaluate_measure('M', c) == False

    def test_istext(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'ISTEXT("hello")'})
        assert engine.evaluate_measure('M', c) == True

    def test_isblank_value(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'ISBLANK(42)'})
        assert engine.evaluate_measure('M', c) == False

    def test_coalesce(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'COALESCE(BLANK(), BLANK(), 42)'})
        assert engine.evaluate_measure('M', c) == 42

    def test_iferror(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'IFERROR(DIVIDE(1, 0), -1)'})
        result = engine.evaluate_measure('M', c)
        # DIVIDE(1,0) returns 0 (alt value), IFERROR wraps it
        assert result == 0 or result == -1  # Either DIVIDE's alt or IFERROR's alt


# ===========================================================================
# 9. TOPN
# ===========================================================================

class TestTopN:
    def test_topn_basic(self, engine, tables, rels):
        """TOPN should return top N rows from a table."""
        c = ctx(tables, rels, {
            'M': "COUNTROWS(TOPN(3, ALL(Products[Name]), [Total]))",
            'Total': 'SUM(Sales[Amount])'
        })
        result = engine.evaluate_measure('M', c)
        # Should return 3 (top 3 products)
        assert result == 3  # TOPN(3, ...) should return exactly 3 rows


# ===========================================================================
# 10. Multi-level measure references
# ===========================================================================

class TestMeasureChaining:
    def test_three_level_chain(self, engine, tables, rels):
        """Measure A → Measure B → Measure C."""
        c = ctx(tables, rels, {
            'Revenue': 'SUM(Sales[Amount])',
            'Cost': 'SUM(Sales[Cost])',
            'Profit': '[Revenue] - [Cost]',
            'Margin': 'DIVIDE([Profit], [Revenue])',
        })
        revenue = engine.evaluate_measure('Revenue', c)
        cost = engine.evaluate_measure('Cost', c)
        profit = engine.evaluate_measure('Profit', c)
        margin = engine.evaluate_measure('Margin', c)

        assert revenue == pytest.approx(1740.0)
        assert cost == pytest.approx(1025.0)
        assert profit == pytest.approx(715.0)
        assert margin == pytest.approx(715.0 / 1740.0, rel=0.01)

    def test_measure_with_filter_context(self, engine, tables, rels):
        """Measure chain with filter context applied."""
        c = ctx(tables, rels, {
            'Revenue': 'SUM(Sales[Amount])',
            'Cost': 'SUM(Sales[Cost])',
            'Profit': '[Revenue] - [Cost]',
        }, {'Calendar.Year': [2024]})
        profit = engine.evaluate_measure('Profit', c)
        # 2024 amounts: 100+200+150+300+250+50+400 = 1450
        # 2024 costs: 60+110+90+180+140+30+240 = 850
        assert profit == pytest.approx(600.0)


# ===========================================================================
# 11. FORMAT function
# ===========================================================================

class TestFormat:
    def test_format_number(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'FORMAT(1234.5, "#,##0.00")'})
        result = engine.evaluate_measure('M', c)
        assert "1" in str(result) and "234" in str(result)

    def test_format_percent(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'FORMAT(0.156, "0.0%")'})
        result = engine.evaluate_measure('M', c)
        assert "15" in str(result) or "%" in str(result)

    def test_format_currency(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'FORMAT(1234.5, "$#,##0.00")'})
        result = engine.evaluate_measure('M', c)
        assert "$" in str(result)


# ===========================================================================
# 12. SWITCH with TRUE() pattern
# ===========================================================================

class TestSwitchTrue:
    def test_switch_true_pattern(self, engine, tables, rels):
        """SWITCH(TRUE(), condition1, result1, ...) — common PBI pattern."""
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'M': """
                VAR _t = [Total]
                RETURN SWITCH(
                    TRUE(),
                    _t > 1500, "Very High",
                    _t > 1000, "High",
                    _t > 500, "Medium",
                    "Low"
                )
            """
        })
        result = engine.evaluate_measure('M', c)
        assert result == "Very High"  # Total is 1740


# ===========================================================================
# 13. String concatenation with & operator
# ===========================================================================

class TestStringConcat:
    def test_ampersand_concat(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': '"Total: " & FORMAT(SUM(Sales[Amount]), "#,##0")'})
        result = engine.evaluate_measure('M', c)
        assert "Total:" in str(result)

    def test_ampersand_with_measure(self, engine, tables, rels):
        c = ctx(tables, rels, {
            'Total': 'SUM(Sales[Amount])',
            'M': '"Revenue is $" & FORMAT([Total], "#,##0")'
        })
        result = engine.evaluate_measure('M', c)
        assert "Revenue" in str(result)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])


class TestLogicalOperators:
    """0.9.8 #5: && / || must be logical AND/OR, not string concatenation.

    Uses spaced operators (as DAX / RLS FilterExpressions are written); the
    RLS-substitution cases mirror exactly what pbix_evaluate_rls feeds _eval_expr.
    """

    def test_and(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': '5 > 3 && 2 < 10'})
        assert engine.evaluate_measure('M', c) is True
        c2 = ctx(tables, rels, {'M': '5 > 3 && 2 > 10'})
        assert engine.evaluate_measure('M', c2) is False

    def test_or(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': '5 < 3 || 2 < 10'})
        assert engine.evaluate_measure('M', c) is True
        c2 = ctx(tables, rels, {'M': '5 < 3 || 2 > 10'})
        assert engine.evaluate_measure('M', c2) is False

    def test_if_with_and(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'IF(1 = 1 && 2 = 2, "yes", "no")'})
        assert engine.evaluate_measure('M', c) == "yes"

    def test_precedence_and_binds_tighter_than_or(self, engine, tables, rels):
        # (5 < 3 && anything) || (2 < 10) -> True
        c = ctx(tables, rels, {'M': '5 < 3 && 9 < 1 || 2 < 10'})
        assert engine.evaluate_measure('M', c) is True

    def test_string_concat_still_works(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': '"a" & "b" & "c"'})
        assert engine.evaluate_measure('M', c) == "abc"

    def test_rls_substituted_multi_condition(self, engine, tables, rels):
        """Exactly what pbix_evaluate_rls evaluates per row after substitution.

        Pre-0.9.8 these were string-concatenated (not True) so a multi-condition
        RLS rule reported 0 visible rows.
        """
        def visible(expr):
            r = engine.evaluate_measure('M', ctx(tables, rels, {'M': expr}))
            return r is True or r == 1
        assert visible('"East" = "East" && "yes" = "yes"') is True
        assert visible('"West" = "East" && "yes" = "yes"') is False
        assert visible('"East" = "East" && "no" = "yes"') is False
        assert visible('"East" = "East" || "West" = "East"') is True


class TestOperatorSpacing:
    """0.9.9: operators no longer require surrounding spaces (tokenizer).

    Spaced and unspaced forms must give identical results.
    """

    def test_unspaced_arithmetic(self, engine, tables, rels):
        for spaced, unspaced in [
            ('10 / 2', '10/2'), ('2 * 3 - 1', '2*3-1'),
            ('SUM(Sales[Amount]) / 2', 'SUM(Sales[Amount])/2'),
        ]:
            a = engine.evaluate_measure('M', ctx(tables, rels, {'M': spaced}))
            b = engine.evaluate_measure('M', ctx(tables, rels, {'M': unspaced}))
            assert a == b, (spaced, unspaced, a, b)

    def test_unspaced_division_result(self, engine, tables, rels):
        assert engine.evaluate_measure('M', ctx(tables, rels, {'M': '10/4'})) == 2.5

    def test_sumx_infix_unspaced(self, engine, tables, rels):
        # The old "SUMX infix returns 0" limitation — now correct.
        c = ctx(tables, rels, {'M': 'SUMX(Sales, Sales[Qty]*Sales[Amount])'})
        spaced = engine.evaluate_measure('M', ctx(tables, rels, {'M': 'SUMX(Sales, Sales[Qty] * Sales[Amount])'}))
        assert engine.evaluate_measure('M', c) == spaced
        assert spaced != 0

    def test_unspaced_comparison(self, engine, tables, rels):
        assert engine.evaluate_measure('M', ctx(tables, rels, {'M': 'SUM(Sales[Amount])>=100'})) is True
        assert engine.evaluate_measure('M', ctx(tables, rels, {'M': 'IF(1=1&&2=2, "y", "n")'})) == "y"

    def test_unary_minus_not_split(self, engine, tables, rels):
        # A `-` used as a unary sign must not be treated as a binary operator.
        assert engine.evaluate_measure('M', ctx(tables, rels, {'M': '10*-2'})) == -20
        assert engine.evaluate_measure('M', ctx(tables, rels, {'M': '10 * -2'})) == -20
        # (`5--3` is NOT tested: `--` is a DAX line comment, so `5--3` == 5.)


class TestEvalBudgetGuard:
    """0.9.9: a non-terminating / runaway measure degrades to BLANK instead of
    hanging the tool (defense-in-depth; bounds total sub-expression evals)."""

    def test_over_budget_degrades_to_blank(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SUM(Sales[Amount])'})
        c._max_eval_calls = 1     # forces the budget to trip
        assert engine.evaluate_measure('M', c) is None

    def test_normal_measure_unaffected(self, engine, tables, rels):
        c = ctx(tables, rels, {'M': 'SUM(Sales[Amount])'})
        assert engine.evaluate_measure('M', c) == 1740


class TestWallClockGuard:
    """0.9.12: an O(dim x fact) measure (few eval calls, each scanning a large
    fact) is bounded by a WALL-CLOCK budget on the engine — the eval-call guard
    misses it, and a context-local timer would reset on every per-row sub-context
    an iterator creates. Degrades to BLANK instead of hanging."""

    def _model(self):
        import random
        random.seed(0)
        fact = [[i % 300, 1.0] for i in range(50000)]
        dim = [[k, "D%d" % k] for k in range(300)]
        tables = {"Fact": {"columns": ["K", "v"], "rows": fact},
                  "Dim": {"columns": ["K", "n"], "rows": dim}}
        rels = [{"FromTable": "Fact", "FromColumn": "K", "ToTable": "Dim",
                 "ToColumn": "K", "IsActive": True}]
        measures = {"slow": "SUMX(ALL(Dim), CALCULATE(SUM(Fact[v])))",
                    "fast": "SUM(Fact[v])"}
        return tables, measures, rels

    def test_slow_measure_bounded_to_blank(self):
        import time as _t

        from pbix_mcp.dax.engine import DAXContext, DAXEngine
        tables, measures, rels = self._model()
        eng = DAXEngine()
        eng._max_eval_seconds = 0.3   # tiny budget forces the guard
        c = DAXContext(tables, measures, None, None, None, rels)
        t0 = _t.monotonic()
        result = eng.evaluate_measure("slow", c)
        elapsed = _t.monotonic() - t0
        assert result is None                    # degraded, not hung
        assert elapsed < 3.0                     # wall-clock actually bounded
        # a fast measure on the same engine is unaffected
        assert eng.evaluate_measure("fast", c) == 50000.0

    def test_enough_budget_computes_correctly(self):
        from pbix_mcp.dax.engine import DAXContext, DAXEngine
        tables, measures, rels = self._model()
        eng = DAXEngine()   # default budget (generous)
        c = DAXContext(tables, measures, None, None, None, rels)
        assert eng.evaluate_measure("slow", c) == 50000.0


class TestBareTableIterators:
    """0.9.9: AVERAGEX/MINX/COUNTX over a bare table (multi-column __row__ dicts)
    now compute correctly instead of returning BLANK (KeyError on __column__)."""

    def test_iterators_over_bare_table(self, engine, tables, rels):
        t = {'T': {'columns': ['a', 'b'], 'rows': [[10, 1], [20, 2], [30, 3]]}}
        def ev(x):
            return engine.evaluate_measure('m', DAXContext(t, {'m': x}, None, None, None, []))
        assert ev('AVERAGEX(ALL(T), T[a])') == 20.0
        assert ev('MINX(ALL(T), T[a])') == 10
        assert ev('COUNTX(ALL(T), T[a])') == 3
        assert ev('SUMX(ALL(T), T[a])') == 60   # control (was already correct)

    def test_topn_rankx_over_bare_table(self, engine, tables, rels):
        # 0.9.11: TOPN's order scoring and RANKX's ranking over a bare table
        # (multi-column __row__ dicts) previously KeyError'd on __column__.
        t = {'T': {'columns': ['a', 'b'], 'rows': [[10, 1], [20, 2], [30, 3]]}}
        def ev(x):
            return engine.evaluate_measure('m', DAXContext(t, {'m': x}, None, None, None, []))
        # TOPN(2, ..., DESC) picks a=30 and a=20 -> SUMX(a) = 50
        assert ev('SUMX(TOPN(2, ALL(T), T[a], 0), T[a])') == 50
        # each row ranked by a among {10,20,30} -> ranks 3+2+1 = 6
        assert ev('SUMX(ALL(T), RANKX(ALL(T), T[a]))') == 6
