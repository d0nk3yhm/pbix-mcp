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
        # Should not infinite loop — raises DAXEvaluationError (caught as None by graceful degradation)
        result = engine.evaluate_measure('M1', ctx)
        assert result is None  # Circular ref detected and handled

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
        # SUM over no rows is BLANK in DAX (Desktop-verified) — so ISBLANK
        # fires; it used to return 0, which made ISBLANK unusable.
        assert result is None

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
    _samples_dir = os.environ.get("PBIX_TEST_SAMPLES", "")
    PBIX_PATH = os.path.join(_samples_dir, "GeoSales_Dashboard.pbix") if _samples_dir else ""

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


# ---------------------------------------------------------------------------
# 0.9.24: iterator row-context evaluation (CONCATENATEX & friends)
# ---------------------------------------------------------------------------

class TestIteratorRowContext:
    """Column refs inside an iterator's COMPOUND scalar expression must resolve
    against the current row (not stringify the column identifier), extension
    columns ([r]/[lbl]) must resolve, plain aggregates typed in the scalar see
    the OUTER filter context (no implicit transition), and CONCATENATEX's
    orderBy/direction args are honored. Desktop-verified: the natural form's
    output matches a real card 1:1 (dax_check.pbix)."""

    REGIONS = {
        'Sales': {
            'columns': ['Region', 'Amount'],
            'rows': [['East', 150.0], ['North', 120.0],
                     ['South', 90.0], ['West', 70.0]],
        }
    }

    def _ev(self, expr):
        engine = DAXEngine()
        ctx = DAXContext(self.REGIONS, {'M': expr})
        return engine.evaluate_measure('M', ctx)

    def test_bare_column_data_order(self):
        # VALUES iterates in data order (order-preserving dedup, not set order)
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), Sales[Region], ", ")'
        ) == 'East, North, South, West'

    def test_column_in_compound_concat(self):
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), Sales[Region] & ": " & '
            'FORMAT(CALCULATE(SUM(Sales[Amount])), "#,0"), " | ")'
        ) == 'East: 150 | North: 120 | South: 90 | West: 70'

    def test_column_inside_format_and_concatenate(self):
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), FORMAT(Sales[Region], "") & "!", ", ")'
        ) == 'East!, North!, South!, West!'
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), CONCATENATE(Sales[Region], "?"), ", ")'
        ) == 'East?, North?, South?, West?'

    def test_selectcolumns_and_addcolumns_named_columns(self):
        assert self._ev(
            'CONCATENATEX(SELECTCOLUMNS(VALUES(Sales[Region]), "r", '
            'Sales[Region] & ""), [r], ", ")'
        ) == 'East, North, South, West'
        assert self._ev(
            'CONCATENATEX(ADDCOLUMNS(VALUES(Sales[Region]), "lbl", '
            'Sales[Region] & "!"), [lbl], ", ")'
        ) == 'East!, North!, South!, West!'

    def test_concatenatex_orderby_desc(self):
        # ASC on the measure reverses the amount order
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), Sales[Region], ", ", '
            'CALCULATE(SUM(Sales[Amount])), ASC)'
        ) == 'West, South, North, East'

    def test_context_transition_workaround_unchanged(self):
        # The CALCULATE(SELECTEDVALUE(...)) form (valid DAX) must keep working
        assert self._ev(
            'CONCATENATEX(VALUES(Sales[Region]), '
            'CALCULATE(SELECTEDVALUE(Sales[Region])) & ": " & '
            'FORMAT(CALCULATE(SUM(Sales[Amount])), "#,0"), " | ", '
            'CALCULATE(SUM(Sales[Amount])), DESC)'
        ) == 'East: 150 | North: 120 | South: 90 | West: 70'

    def test_plain_sum_in_iterator_sees_outer_total(self):
        # Row context does NOT transition a plain aggregate (Desktop semantics)
        assert self._ev('MAXX(VALUES(Sales[Region]), SUM(Sales[Amount]))') == 430.0
        assert self._ev('SUMX(VALUES(Sales[Region]), SUM(Sales[Amount]))') == 1720.0

    def test_calculate_sum_in_iterator_is_row_sliced(self):
        assert self._ev(
            'MAXX(VALUES(Sales[Region]), CALCULATE(SUM(Sales[Amount])))') == 150.0

    def test_sum_over_empty_selection_is_blank(self):
        assert self._ev(
            'IF(ISBLANK(CALCULATE(SUM(Sales[Amount]), Sales[Region] = "Nowhere")), '
            '"blank", "notblank")') == 'blank'

    def test_now_today_and_scientific_literals(self):
        import datetime as _dt
        assert self._ev('IF(ISBLANK(NOW()), "blank", "ok")') == 'ok'
        assert self._ev('FORMAT(TODAY(), "yyyy")') == str(_dt.date.today().year)
        assert self._ev('1e6') == 1000000.0
        assert self._ev('1e6 / 2') == 500000.0


class TestLiteralFirstArithmetic:
    """Issues-9 §5: a bare Table[Col] ref preceded by a literal and operator
    ("1 - S[D]") was swallowed by the column-reference regex as a column of
    table "1 - S", making the whole expression BLANK — ai_report's
    Total Sales (SUMX with a (1 - Discount) factor) evaluated to 0."""

    TABLES = {'S': {'columns': ['Q', 'D'], 'rows': [[3, 0.1], [2, 0.5]]}}

    def _ev(self, expr):
        engine = DAXEngine()
        ctx = DAXContext(self.TABLES, {'M': expr})
        return engine.evaluate_measure('M', ctx)

    def test_literal_first_forms(self):
        cases = {
            "SUMX(S, 1 - S[D])": 1.4,
            "SUMX(S, (1 - S[D]))": 1.4,
            "SUMX(S, 1 + S[D])": 2.6,
            "SUMX(S, 2 * S[D])": 1.2,
            "SUMX(S, 0 - S[D])": -0.6,
            "SUMX(S, S[Q] * (1 - S[D]))": 3.7,
            "SUMX(S, (1 - S[D]) * S[Q])": 3.7,
        }
        for expr, want in cases.items():
            got = self._ev(expr)
            assert got is not None and abs(got - want) < 1e-9, (expr, got, want)

    def test_column_first_forms_still_work(self):
        assert abs(self._ev("SUMX(S, S[D] - 1)") - (-1.4)) < 1e-9
        assert abs(self._ev("SUMX(S, 'S'[D] - 1)") - (-1.4)) < 1e-9
        assert self._ev("SUMX(S, S[Q])") == 5

    def test_unary_minus_on_column_ref(self):
        assert abs(self._ev("SUMX(S, -S[D])") - (-0.6)) < 1e-9
        assert abs(self._ev("SUMX(S, -S[D] + 1)") - 1.4) < 1e-9

    def test_total_sales_shape(self):
        # the exact ai_report shape: SUMX(T, Q * U * (1 - D))
        tables = {'Sales': {'columns': ['Quantity', 'UnitPrice', 'Discount'],
                            'rows': [[3, 1200, 0.08], [2, 100, 0.0]]}}
        engine = DAXEngine()
        ctx = DAXContext(tables, {
            'Total Sales': 'SUMX(Sales, Sales[Quantity] * Sales[UnitPrice] '
                           '* (1 - Sales[Discount]))'})
        got = engine.evaluate_measure('Total Sales', ctx)
        assert abs(got - (3 * 1200 * 0.92 + 200)) < 1e-6

    def test_leading_negative_comparisons_and_concat(self):
        # review round: the unary-minus branch must NOT swallow top-level
        # comparisons/concats whose left operand starts with '-' (unary minus
        # binds tighter than comparison/& in DAX)
        assert self._ev('IF(-1 < 0, 10, 20)') == 10
        assert self._ev('IF(-1 = -1, 10, 20)') == 10
        assert self._ev('IF(-1 <> 1, 10, 20)') == 10
        assert self._ev('IF(-1 >= -2, 10, 20)') == 10
        assert self._ev('-1 & "x"') == "-1x"
        # RLS-substitution shape: negative row value on the comparison LHS
        assert self._ev('-3.0 < 0') is True
        # and inside iterators (fixed by the ordering — was BLANK even at HEAD)
        assert self._ev('SUMX(S, IF(-S[D] < 0, 1, 0))') == 2


# ---------------------------------------------------------------------------
# Issue #35: KEEPFILTERS intersects the outer filter instead of overriding
# ---------------------------------------------------------------------------

class TestKeepFilters:
    """KEEPFILTERS(pred) in CALCULATE keeps the outer filter on the predicate's
    column(s): the allowed set is the INTERSECTION, and an empty intersection
    filters to empty (SUM -> BLANK). The passthrough made every KEEPFILTERS
    measure compute the plain override — silent wrong values under any
    overlapping outer filter (OpenBI docs r25 / issue #35)."""

    TABLES = {'T': {'columns': ['Cat', 'V'],
                    'rows': [['A', 10], ['B', 20], ['C', 30]]}}
    MEASURES = {
        'Plain': 'CALCULATE(SUM(T[V]), T[Cat]="B")',
        'Keep':  'CALCULATE(SUM(T[V]), KEEPFILTERS(T[Cat]="B"))',
        'KeepIn': 'CALCULATE(SUM(T[V]), KEEPFILTERS(T[Cat] IN {"B", "C"}))',
    }

    def _ev(self, name, fc=None):
        engine = DAXEngine()
        ctx = DAXContext(self.TABLES, self.MEASURES, filter_context=fc)
        return engine.evaluate_measure(name, ctx)

    def test_no_outer_filter_matches_plain(self):
        assert self._ev('Plain') == 20
        assert self._ev('Keep') == 20

    def test_disjoint_outer_filter_goes_blank(self):
        # {A} ∩ {B} = ∅ -> BLANK; the plain override still answers 20
        assert self._ev('Plain', {'T.Cat': ['A']}) == 20
        assert self._ev('Keep', {'T.Cat': ['A']}) is None

    def test_matching_outer_filter_unchanged(self):
        assert self._ev('Plain', {'T.Cat': ['B']}) == 20
        assert self._ev('Keep', {'T.Cat': ['B']}) == 20

    def test_partial_overlap_intersects(self):
        # outer {A,C} ∩ IN {B,C} = {C} -> 30 (real DAX); override answers 50
        assert self._ev('KeepIn', {'T.Cat': ['A', 'C']}) == 30
        assert self._ev('KeepIn') == 50

    def test_outer_filter_on_other_column_untouched(self):
        # a filter on a DIFFERENT column must not be intersected away
        assert self._ev('Keep', {'T.V': [20, 30]}) == 20


# ---------------------------------------------------------------------------
# Issue #38: utcnow() deprecation sweep + NOW()/UTCNOW() semantics
# ---------------------------------------------------------------------------

class TestUtcnowRetirement:
    """Issue #38: datetime.utcnow() (deprecated, removal-scheduled) is gone
    from every call site, with each site's naive/aware requirement kept:
    the engine's datetimes stay NAIVE, the FILETIME deltas subtract a naive
    epoch, and metadata_schema's helper is aware ON PURPOSE — .timestamp()
    on the old naive utcnow() value reinterpreted it as LOCAL time, skewing
    stored FILETIMEs by the host's UTC offset."""

    def _ev(self, expr):
        engine = DAXEngine()
        ctx = DAXContext({'T': {'columns': ['V'], 'rows': [[1]]}},
                         {'M': expr})
        return engine.evaluate_measure('M', ctx)

    def test_no_utcnow_call_sites_remain(self):
        import glob
        import os
        root = os.path.join(os.path.dirname(__file__), "..", "src")
        offenders = []
        for path in glob.glob(os.path.join(root, "**", "*.py"),
                              recursive=True):
            with open(path, encoding="utf-8") as fh:
                for ln, line in enumerate(fh, 1):
                    if ".utcnow()" in line:
                        offenders.append(f"{os.path.basename(path)}:{ln}")
        assert not offenders, f"utcnow() call sites remain: {offenders}"

    def test_now_family_is_warning_free_and_naive(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            now = self._ev('NOW()')
            utc = self._ev('UTCNOW()')
            utctoday = self._ev('UTCTODAY()')
            today = self._ev('TODAY()')
        for v in (now, utc, utctoday, today):
            assert v.tzinfo is None, f"{v!r} must stay naive"
        assert utctoday.hour == 0 and today.hour == 0

    def test_now_is_local_and_utcnow_is_utc(self):
        import datetime as _dt
        now = self._ev('NOW()')
        utc = self._ev('UTCNOW()')
        # NOW() - UTCNOW() must equal the host's current UTC offset.
        offset = _dt.datetime.now(
            _dt.timezone.utc).astimezone().utcoffset().total_seconds()
        got = (now - utc).total_seconds()
        assert abs(got - offset) < 5, (
            f"NOW()-UTCNOW() is {got}s; the host UTC offset is {offset}s")

    def test_filetime_helpers_agree(self):
        # metadata_schema's helper used to run on the local wall-clock
        # (naive utcnow -> .timestamp()), so on any host east/west of UTC it
        # disagreed with builder_v2's epoch-delta helper by the UTC offset.
        import warnings

        from pbix_mcp.builder_v2 import _windows_filetime_now as ft_builder
        from pbix_mcp.formats.metadata_schema import (
            _windows_filetime_now as ft_schema,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            a, b = ft_schema(), ft_builder()
        assert abs(a - b) < 5 * 10_000_000, (
            f"FILETIME helpers disagree by {(a - b) / 10_000_000:.1f}s "
            f"(the old local-time skew was the host's whole UTC offset)")


# ---------------------------------------------------------------------------
# Issue #42: date-prefixed compound keys must not merge into one date bucket
# ---------------------------------------------------------------------------

class TestDatePrefixKeysStayDistinct:
    """Issue #42: _as_date parses a date PREFIX, and the value-index aliasing
    keyed every '01/01/2017-CLUSTER n' to the same '2017-01-01' bucket — a
    filter on ONE key returned every key sharing its date prefix, and via a
    relationship the fact table came back UNFILTERED (the grand total,
    silently). Aliasing/equality now use a STRICT whole-value parse; the
    legitimate alias (same date as datetime cell vs ISO string) survives."""

    KEY_SHAPES = [
        ["K1-CLUSTER 1", "K1-CLUSTER 2", "K2-CLUSTER 1", "K2-CLUSTER 2"],
        ["1-CLUSTER 1", "1-CLUSTER 2", "2-CLUSTER 1", "2-CLUSTER 2"],
        ["01/01/2017-CLUSTER 1", "01/01/2017-CLUSTER 2",
         "01/02/2017-CLUSTER 1", "01/02/2017-CLUSTER 2"],
        ["2017-01-01-CLUSTER 1", "2017-01-01-CLUSTER 2",
         "2017-01-02-CLUSTER 1", "2017-01-02-CLUSTER 2"],
    ]

    def test_strict_parse_semantics(self):
        from pbix_mcp.dax.engine import _as_date, _as_date_strict
        assert _as_date("01/01/2017-CLUSTER 1") is not None   # lenient: prefix
        assert _as_date_strict("01/01/2017-CLUSTER 1") is None
        assert _as_date_strict("2017-01-01-CLUSTER 1") is None
        assert str(_as_date_strict("2017-01-01")) == "2017-01-01"
        assert str(_as_date_strict("2009-12-01 00:00:00")) == "2009-12-01"

    @pytest.mark.parametrize("keys", KEY_SHAPES,
                             ids=["text", "number", "us_date", "iso_date"])
    def test_relationship_filter_stays_filtered(self, keys):
        tables = {
            'Dim': {'columns': ['Key', 'Division'],
                    'rows': [[k, "CLUSTER " + k[-1]] for k in keys]},
            'Fact': {'columns': ['Key', 'Amt'], 'rows': [[k, 10] for k in keys]},
        }
        rels = [{'FromTable': 'Fact', 'FromColumn': 'Key',
                 'ToTable': 'Dim', 'ToColumn': 'Key', 'IsActive': 1}]
        got = DAXEngine().evaluate_measure('M', DAXContext(
            tables, {'M': "SUM(Fact[Amt])"}, None, None,
            {'Dim.Division': ['CLUSTER 1']}, rels))
        assert got == 20, f"{keys[0]}: got {got}, the 40 grand total means merged buckets"

    def test_single_compound_key_returns_one_row(self):
        keys = ["01/01/2017-CLUSTER 1", "01/01/2017-CLUSTER 2",
                "01/02/2017-CLUSTER 1"]
        tables = {'Fact': {'columns': ['Key', 'Amt'],
                           'rows': [[k, 10] for k in keys]}}
        got = DAXEngine().evaluate_measure('M', DAXContext(
            tables, {'M': "SUM(Fact[Amt])"},
            filter_context={'Fact.Key': ['01/01/2017-CLUSTER 1']}))
        assert got == 10

    def test_legitimate_date_alias_survives(self):
        from datetime import datetime as _dt
        # ISO string filter must still find datetime cells (the alias's reason
        # to exist), in both the index fast path and the matcher path
        t1 = {'S': {'columns': ['D', 'V'],
                    'rows': [[_dt(2009, 12, 1), 5], [_dt(2009, 12, 2), 7]]}}
        assert DAXEngine().evaluate_measure('M', DAXContext(
            t1, {'M': "SUM(S[V])"},
            filter_context={'S.D': ['2009-12-01']})) == 5
        t2 = {'S': {'columns': ['D', 'V'],
                    'rows': [['2009-12-01 00:00:00', 5],
                             ['2009-12-02 00:00:00', 7]]}}
        assert DAXEngine().evaluate_measure('M', DAXContext(
            t2, {'M': "SUM(S[V])"},
            filter_context={'S.D': ['2009-12-01']})) == 5
