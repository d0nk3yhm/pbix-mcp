"""
Tests for the DAX evaluation engine.
Run: python -m pytest tests/ -v
"""
import os

import pytest

from pbix_mcp.dax.engine import DAXContext, DAXEngine, evaluate_measures_batch

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Fixtures: sample data for testing
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_tables():
    """Minimal star-schema dataset for testing."""
    return {
        'Sales': {
            'columns': ['OrderID', 'Date', 'ProductID', 'CustomerID', 'Amount', 'Quantity', 'Discount'],
            'rows': [
                [1, '2023-01-15', 'P1', 'C1', 100.0, 2, 0.0],
                [2, '2023-02-20', 'P2', 'C2', 200.0, 3, 0.1],
                [3, '2023-03-10', 'P1', 'C1', 150.0, 1, 0.0],
                [4, '2023-04-05', 'P3', 'C3', 300.0, 5, 0.2],
                [5, '2023-05-15', 'P2', 'C2', 250.0, 4, 0.0],
                [6, '2022-01-10', 'P1', 'C1', 80.0, 2, 0.0],
                [7, '2022-06-20', 'P2', 'C2', 120.0, 3, 0.0],
                [8, '2022-11-05', 'P3', 'C3', 90.0, 1, 0.15],
            ],
        },
        'Products': {
            'columns': ['ProductID', 'Name', 'Category', 'Price'],
            'rows': [
                ['P1', 'Widget', 'Hardware', 50.0],
                ['P2', 'Gadget', 'Electronics', 75.0],
                ['P3', 'Doohickey', 'Hardware', 60.0],
            ],
        },
        'Customers': {
            'columns': ['CustomerID', 'Name', 'Region'],
            'rows': [
                ['C1', 'Alice', 'East'],
                ['C2', 'Bob', 'West'],
                ['C3', 'Charlie', 'East'],
            ],
        },
        'Calendar': {
            'columns': ['Date', 'Year', 'Month', 'Quarter'],
            'rows': [
                ['2022-01-10', 2022, 1, 1],
                ['2022-06-20', 2022, 6, 2],
                ['2022-11-05', 2022, 11, 4],
                ['2023-01-15', 2023, 1, 1],
                ['2023-02-20', 2023, 2, 1],
                ['2023-03-10', 2023, 3, 1],
                ['2023-04-05', 2023, 4, 2],
                ['2023-05-15', 2023, 5, 2],
            ],
        },
    }


@pytest.fixture
def sample_measures():
    """DAX measure definitions."""
    return {
        'Total Sales': 'SUM(Sales[Amount])',
        'Total Quantity': 'SUM(Sales[Quantity])',
        'Avg Sale': 'AVERAGE(Sales[Amount])',
        'Order Count': 'COUNTROWS(Sales)',
        'Avg Price': 'DIVIDE([Total Sales], [Total Quantity])',
        'Profit Margin': 'DIVIDE(SUM(Sales[Amount]) - SUM(Sales[Discount]), SUM(Sales[Amount]))',
        'Sales LY': "CALCULATE([Total Sales], DATEADD('Calendar'[Date], -1, YEAR))",
        'Sales Change': 'DIVIDE([Total Sales] - [Sales LY], [Sales LY])',
        'Distinct Products': 'DISTINCTCOUNT(Sales[ProductID])',
        'Max Sale': 'MAX(Sales[Amount])',
        'Min Sale': 'MIN(Sales[Amount])',
        'Conditional': 'IF([Total Sales] > 500, "High", "Low")',
        'With Vars': """
            VAR _total = [Total Sales]
            VAR _avg = [Avg Sale]
            VAR _ratio = DIVIDE(_total, _avg)
            RETURN _ratio
        """,
        'Category Max': "MAXX(ALL('Products'[Category]), [Total Sales])",
    }


@pytest.fixture
def sample_relationships():
    return [
        {'FromTable': 'Sales', 'FromColumn': 'Date', 'ToTable': 'Calendar', 'ToColumn': 'Date', 'IsActive': 1},
        {'FromTable': 'Sales', 'FromColumn': 'ProductID', 'ToTable': 'Products', 'ToColumn': 'ProductID', 'IsActive': 1},
        {'FromTable': 'Sales', 'FromColumn': 'CustomerID', 'ToTable': 'Customers', 'ToColumn': 'CustomerID', 'IsActive': 1},
    ]


@pytest.fixture
def engine():
    return DAXEngine()


@pytest.fixture
def ctx(sample_tables, sample_measures, sample_relationships):
    return DAXContext(sample_tables, sample_measures, 'Calendar', 'Date', None, sample_relationships)


@pytest.fixture
def ctx_2023(sample_tables, sample_measures, sample_relationships):
    return DAXContext(sample_tables, sample_measures, 'Calendar', 'Date',
                      {'Calendar.Year': [2023]}, sample_relationships)


# ---------------------------------------------------------------------------
# Test: Basic Aggregation Functions
# ---------------------------------------------------------------------------

class TestAggregation:
    def test_sum(self, engine, ctx):
        result = engine.evaluate_measure('Total Sales', ctx)
        assert result == pytest.approx(1290.0)

    def test_sum_filtered(self, engine, ctx_2023):
        result = engine.evaluate_measure('Total Sales', ctx_2023)
        assert result == pytest.approx(1000.0)  # 100+200+150+300+250

    def test_average(self, engine, ctx):
        result = engine.evaluate_measure('Avg Sale', ctx)
        assert result == pytest.approx(1290.0 / 8)

    def test_countrows(self, engine, ctx):
        result = engine.evaluate_measure('Order Count', ctx)
        assert result == 8

    def test_countrows_filtered(self, engine, ctx_2023):
        result = engine.evaluate_measure('Order Count', ctx_2023)
        assert result == 5

    def test_distinctcount(self, engine, ctx):
        result = engine.evaluate_measure('Distinct Products', ctx)
        assert result == 3

    def test_max(self, engine, ctx):
        result = engine.evaluate_measure('Max Sale', ctx)
        assert result == pytest.approx(300.0)

    def test_min(self, engine, ctx):
        result = engine.evaluate_measure('Min Sale', ctx)
        assert result == pytest.approx(80.0)

    def test_sum_quantity(self, engine, ctx):
        result = engine.evaluate_measure('Total Quantity', ctx)
        assert result == 21  # 2+3+1+5+4+2+3+1


# ---------------------------------------------------------------------------
# Test: Computed Measures (DIVIDE, expressions)
# ---------------------------------------------------------------------------

class TestComputed:
    def test_divide(self, engine, ctx):
        result = engine.evaluate_measure('Avg Price', ctx)
        expected = 1290.0 / 21
        assert result == pytest.approx(expected, rel=0.01)

    def test_divide_by_zero(self, engine, ctx):
        ctx_empty = DAXContext(
            {'Sales': {'columns': ['Amount'], 'rows': []}},
            {'Test': 'DIVIDE(1, 0, -1)'},
            relationships=[]
        )
        result = engine.evaluate_measure('Test', ctx_empty)
        assert result == -1

    def test_conditional_high(self, engine, ctx):
        result = engine.evaluate_measure('Conditional', ctx)
        assert result == "High"  # Total Sales 1290 > 500

    def test_conditional_low(self, engine, ctx):
        ctx_low = DAXContext(
            {'Sales': {'columns': ['Amount'], 'rows': [[10.0]]}},
            {'Total Sales': 'SUM(Sales[Amount])', 'Conditional': 'IF([Total Sales] > 500, "High", "Low")'},
            relationships=[]
        )
        result = engine.evaluate_measure('Conditional', ctx_low)
        assert result == "Low"


# ---------------------------------------------------------------------------
# Test: VAR / RETURN
# ---------------------------------------------------------------------------

class TestVarReturn:
    def test_var_return(self, engine, ctx):
        result = engine.evaluate_measure('With Vars', ctx)
        # _total / _avg = 1290 / (1290/8) = 8
        assert result == pytest.approx(8.0)

    def test_var_return_inline(self, engine, ctx):
        ctx.measures['Inline'] = "VAR _x = 10 VAR _y = 20 RETURN _x + _y"
        result = engine.evaluate_measure('Inline', ctx)
        assert result == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Test: Filter Propagation via Relationships
# ---------------------------------------------------------------------------

class TestFilterPropagation:
    def test_year_filter(self, engine, ctx_2023):
        """Year filter on Calendar should propagate to Sales via date relationship."""
        result = engine.evaluate_measure('Total Sales', ctx_2023)
        assert result == pytest.approx(1000.0)

    def test_category_filter(self, engine, sample_tables, sample_measures, sample_relationships):
        """Product category filter should propagate to Sales via ProductID relationship."""
        ctx = DAXContext(sample_tables, sample_measures, 'Calendar', 'Date',
                         {'Products.Category': ['Hardware']}, sample_relationships)
        result = engine.evaluate_measure('Total Sales', ctx)
        # Hardware products: P1 (100+150+80=330) + P3 (300+90=390) = 720
        assert result == pytest.approx(720.0)

    def test_region_filter(self, engine, sample_tables, sample_measures, sample_relationships):
        """Customer region filter should propagate to Sales via CustomerID relationship."""
        ctx = DAXContext(sample_tables, sample_measures, 'Calendar', 'Date',
                         {'Customers.Region': ['East']}, sample_relationships)
        result = engine.evaluate_measure('Total Sales', ctx)
        # East customers: C1 (100+150+80=330) + C3 (300+90=390) = 720
        assert result == pytest.approx(720.0)

    def test_combined_filters(self, engine, sample_tables, sample_measures, sample_relationships):
        """Year + Category filter combined."""
        ctx = DAXContext(sample_tables, sample_measures, 'Calendar', 'Date',
                         {'Calendar.Year': [2023], 'Products.Category': ['Electronics']},
                         sample_relationships)
        result = engine.evaluate_measure('Total Sales', ctx)
        # Electronics (P2) in 2023: 200 + 250 = 450
        assert result == pytest.approx(450.0)


# ---------------------------------------------------------------------------
# Test: Time Intelligence (DATEADD, SAMEPERIODLASTYEAR)
# ---------------------------------------------------------------------------

class TestTimeIntelligence:
    def test_sales_ly_from_2023(self, engine, ctx_2023):
        """Sales LY when filtered to 2023 should return 2022 sales."""
        result = engine.evaluate_measure('Sales LY', ctx_2023)
        # 2022 sales: 80 + 120 + 90 = 290
        assert result == pytest.approx(290.0)

    def test_sales_change(self, engine, ctx_2023):
        """Sales change = (2023 - 2022) / 2022."""
        result = engine.evaluate_measure('Sales Change', ctx_2023)
        # (1000 - 290) / 290 = 2.448...
        assert result == pytest.approx(710.0 / 290.0, rel=0.01)


# ---------------------------------------------------------------------------
# Test: MAXX, ALL, iteration functions
# ---------------------------------------------------------------------------

class TestIterators:
    def test_maxx_all(self, engine, ctx):
        result = engine.evaluate_measure('Category Max', ctx)
        # Hardware: P1+P3 amounts, Electronics: P2 amounts
        # Hardware: 100+150+300+80+90 = 720, Electronics: 200+250+120 = 570
        assert result == pytest.approx(720.0)


# ---------------------------------------------------------------------------
# Test: Expression Parsing
# ---------------------------------------------------------------------------

class TestExpressionParsing:
    def test_string_literal(self, engine, ctx):
        result = engine._eval_expr('"hello"', ctx)
        assert result == "hello"

    def test_numeric_literal(self, engine, ctx):
        assert engine._eval_expr('42', ctx) == 42
        assert engine._eval_expr('3.14', ctx) == pytest.approx(3.14)

    def test_boolean(self, engine, ctx):
        assert engine._eval_expr('TRUE', ctx) == True
        assert engine._eval_expr('FALSE', ctx) == False

    def test_measure_ref(self, engine, ctx):
        result = engine._eval_expr('[Total Sales]', ctx)
        assert result == pytest.approx(1290.0)

    def test_binary_add(self, engine, ctx):
        result = engine._eval_expr('[Total Sales] + [Total Quantity]', ctx)
        assert result == pytest.approx(1311.0)

    def test_binary_subtract(self, engine, ctx):
        result = engine._eval_expr('[Total Sales] - [Total Quantity]', ctx)
        assert result == pytest.approx(1269.0)

    def test_binary_multiply(self, engine, ctx):
        result = engine._eval_expr('10 * 5', ctx)
        assert result == pytest.approx(50.0)

    def test_comparison(self, engine, ctx):
        assert engine._eval_expr('10 > 5', ctx) == True
        assert engine._eval_expr('10 < 5', ctx) == False
        assert engine._eval_expr('10 = 10', ctx) == True
        assert engine._eval_expr('10 <> 5', ctx) == True

    def test_string_concat(self, engine, ctx):
        result = engine._eval_expr('"hello" & " " & "world"', ctx)
        assert result == "hello world"

    def test_nested_parens(self, engine, ctx):
        result = engine._eval_expr('(10 + 5) * 2', ctx)
        assert result == pytest.approx(30.0)


# ---------------------------------------------------------------------------
# Test: Logic Functions
# ---------------------------------------------------------------------------

class TestLogic:
    def test_if_true(self, engine, ctx):
        result = engine._eval_expr('IF(10 > 5, "yes", "no")', ctx)
        assert result == "yes"

    def test_if_false(self, engine, ctx):
        result = engine._eval_expr('IF(10 < 5, "yes", "no")', ctx)
        assert result == "no"

    def test_switch(self, engine, ctx):
        result = engine._eval_expr('SWITCH(2, 1, "one", 2, "two", 3, "three")', ctx)
        assert result == "two"

    def test_and(self, engine, ctx):
        assert engine._eval_expr('AND(TRUE, TRUE)', ctx) == True
        assert engine._eval_expr('AND(TRUE, FALSE)', ctx) == False

    def test_or(self, engine, ctx):
        assert engine._eval_expr('OR(FALSE, TRUE)', ctx) == True
        assert engine._eval_expr('OR(FALSE, FALSE)', ctx) == False

    def test_not(self, engine, ctx):
        assert engine._eval_expr('NOT(TRUE)', ctx) == False
        assert engine._eval_expr('NOT(FALSE)', ctx) == True

    def test_isblank(self, engine, ctx):
        assert engine._eval_expr('ISBLANK(BLANK())', ctx) == True


# ---------------------------------------------------------------------------
# Test: Math Functions
# ---------------------------------------------------------------------------

class TestMath:
    def test_abs(self, engine, ctx):
        assert engine._eval_expr('ABS(-5)', ctx) == 5

    def test_round(self, engine, ctx):
        assert engine._eval_expr('ROUND(3.456, 2)', ctx) == pytest.approx(3.46)

    def test_int(self, engine, ctx):
        assert engine._eval_expr('INT(3.7)', ctx) == 3

    def test_divide_with_alt(self, engine, ctx):
        result = engine._eval_expr('DIVIDE(10, 0, -1)', ctx)
        assert result == -1


# ---------------------------------------------------------------------------
# Test: Batch Evaluation
# ---------------------------------------------------------------------------

class TestBatch:
    def test_batch(self, sample_tables, sample_measures, sample_relationships):
        results = evaluate_measures_batch(
            ['Total Sales', 'Total Quantity', 'Avg Price'],
            sample_tables, sample_measures,
            None, 'Calendar', 'Date', sample_relationships
        )
        assert results['Total Sales'] == pytest.approx(1290.0)
        assert results['Total Quantity'] == 21
        assert results['Avg Price'] == pytest.approx(1290.0 / 21, rel=0.01)

    def test_batch_filtered(self, sample_tables, sample_measures, sample_relationships):
        results = evaluate_measures_batch(
            ['Total Sales', 'Order Count'],
            sample_tables, sample_measures,
            {'Calendar.Year': [2023]},
            'Calendar', 'Date', sample_relationships
        )
        assert results['Total Sales'] == pytest.approx(1000.0)
        assert results['Order Count'] == 5


# ---------------------------------------------------------------------------
# Test: Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_circular_reference(self, engine):
        ctx = DAXContext(
            {'T': {'columns': ['A'], 'rows': [[1]]}},
            {'M1': '[M2]', 'M2': '[M1]'},
            relationships=[]
        )
        # Should not infinite loop — returns 0 due to circular ref prevention
        result = engine.evaluate_measure('M1', ctx)
        assert result == 0

    def test_missing_measure(self, engine, ctx):
        result = engine.evaluate_measure('NonExistent', ctx)
        assert result is None

    def test_empty_table(self, engine):
        ctx = DAXContext(
            {'T': {'columns': ['Amount'], 'rows': []}},
            {'S': 'SUM(T[Amount])'},
            relationships=[]
        )
        result = engine.evaluate_measure('S', ctx)
        assert result == 0

    def test_comment_stripping(self, engine, ctx):
        ctx.measures['WithComment'] = """
            // This is a comment
            SUM(Sales[Amount]) // inline comment
        """
        result = engine.evaluate_measure('WithComment', ctx)
        assert result == pytest.approx(1290.0)


# ---------------------------------------------------------------------------
# Test with real PBIX file (if available)
# ---------------------------------------------------------------------------

class TestWithPBIX:
    PBIX_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             '..', 'OpenBI', 'test_samples', 'GeoSales_Dashboard.pbix')

    @pytest.fixture
    def real_ctx(self):
        """Load real PBIX data if available."""
        if not os.path.exists(self.PBIX_PATH):
            pytest.skip("GeoSales_Dashboard.pbix not found")

        try:
            from pbixray import PBIXRay
            model = PBIXRay(self.PBIX_PATH)

            measures_df = model.dax_measures
            measure_defs = {}
            if measures_df is not None:
                for _, row in measures_df.iterrows():
                    measure_defs[row.get('Name', '')] = row.get('Expression', '')

            rels_df = model.relationships
            relationships = []
            if rels_df is not None:
                for _, row in rels_df.iterrows():
                    relationships.append({
                        'FromTable': row.get('FromTableName', ''),
                        'FromColumn': row.get('FromColumnName', ''),
                        'ToTable': row.get('ToTableName', ''),
                        'ToColumn': row.get('ToColumnName', ''),
                        'IsActive': row.get('IsActive', 1),
                    })

            tables = {}
            for tname in ['fct_Orders', 'dim-Date', 'dim-Geo', 'dim-Product']:
                try:
                    df = model.get_table(tname)
                    if df is not None:
                        tables[tname] = {'columns': list(df.columns), 'rows': df.values.tolist()}
                except:
                    continue

            return DAXContext(tables, measure_defs, 'dim-Date', 'Date',
                             {'dim-Date.Year': [2015]}, relationships)
        except Exception as e:
            pytest.skip(f"Cannot load PBIX: {e}")

    def test_sales_2015(self, engine, real_ctx):
        result = engine.evaluate_measure('Sales', real_ctx)
        assert result == pytest.approx(470533, rel=0.01)

    def test_profit_margin_2015(self, engine, real_ctx):
        result = engine.evaluate_measure('Profit Margin', real_ctx)
        assert result == pytest.approx(0.131, rel=0.01)

    def test_quantity_2015(self, engine, real_ctx):
        result = engine.evaluate_measure('Quantity', real_ctx)
        assert result == pytest.approx(7979, rel=0.01)

    def test_sales_ly_2015(self, engine, real_ctx):
        result = engine.evaluate_measure('Sales LY', real_ctx)
        assert result == pytest.approx(484247, rel=0.01)

    def test_sales_change_2015(self, engine, real_ctx):
        result = engine.evaluate_measure('Sales change', real_ctx)
        assert result == pytest.approx(-0.028, abs=0.005)

    def test_california_2015(self, engine, real_ctx):
        real_ctx.filter_context['dim-Geo.State'] = ['California']
        result = engine.evaluate_measure('Sales', real_ctx)
        assert result == pytest.approx(88444, rel=0.01)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
