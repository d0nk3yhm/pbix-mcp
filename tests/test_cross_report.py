"""
Cross-report DAX engine tests — validates against multiple PBIX files.
Tests that the engine works generically across different report structures.
Run: python -m pytest tests/test_cross_report.py -v
"""
import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dax_engine import DAXEngine, DAXContext, evaluate_measures_batch


def _load_pbix(path):
    """Load tables, measures, relationships from a PBIX file."""
    from pbixray import PBIXRay
    model = PBIXRay(path)

    measures_df = model.dax_measures
    measure_defs = {}
    if measures_df is not None and not measures_df.empty:
        for _, row in measures_df.iterrows():
            measure_defs[row.get('Name', '')] = row.get('Expression', '')

    rels_df = model.relationships
    relationships = []
    if rels_df is not None and not rels_df.empty:
        for _, row in rels_df.iterrows():
            relationships.append({
                'FromTable': row.get('FromTableName', ''),
                'FromColumn': row.get('FromColumnName', ''),
                'ToTable': row.get('ToTableName', ''),
                'ToColumn': row.get('ToColumnName', ''),
                'IsActive': row.get('IsActive', 1),
            })

    schema = model.schema
    tables = {}
    if schema is not None and not schema.empty:
        for tname in schema['TableName'].unique():
            if tname.startswith('H$') or tname.startswith('R$'):
                continue
            try:
                df = model.get_table(tname)
                if df is not None and not df.empty:
                    tables[tname] = {'columns': list(df.columns), 'rows': df.values.tolist()}
            except Exception:
                pass

    # Load calculated tables from ABF metadata (DATATABLE, GENERATESERIES, etc.)
    # These tables aren't in VertiPaq — they exist only as DAX in metadata.
    try:
        from calc_tables import load_calculated_tables
        tables = load_calculated_tables(path, tables, relationships)
    except Exception:
        pass

    date_table = date_column = None
    for t in tables:
        if 'date' in t.lower() or 'calendar' in t.lower():
            if 'Date' in tables[t]['columns']:
                date_table, date_column = t, 'Date'
                break

    return tables, measure_defs, relationships, date_table, date_column


# ---------------------------------------------------------------------------
# Paths to test PBIX files
# ---------------------------------------------------------------------------
SAMPLES = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'test_samples')
GEOSALES = os.path.join(SAMPLES, '..', '..', 'OpenBI', 'test_samples', 'GeoSales_Dashboard.pbix')
AGENTS = os.path.join(SAMPLES, 'temp_dl', 'Full Dashboards',
                       'Agents Performance - Dashboard', 'Agents Performance - Dashboard.pbix')
ECOMMERCE = os.path.join(SAMPLES, 'temp_dl', 'Full Dashboards',
                          'Ecommerce Conversion Dashboard', 'Ecommerce Conversion Dashboard.pbix')
IT_SUPPORT = os.path.join(SAMPLES, 'temp_dl', 'Full Dashboards',
                           'IT Support Performance Dashboard', 'IT_Support_Ticket_Desk.pbix')


# ---------------------------------------------------------------------------
# GeoSales Dashboard (verified against Power BI Desktop)
# ---------------------------------------------------------------------------

class TestGeoSalesDashboard:
    @pytest.fixture(scope='class')
    def data(self):
        if not os.path.exists(GEOSALES):
            pytest.skip('GeoSales_Dashboard.pbix not found')
        return _load_pbix(GEOSALES)

    def test_loads(self, data):
        tables, measures, rels, dt, dc = data
        assert len(tables) >= 5
        assert len(measures) >= 20
        assert len(rels) >= 5

    def test_sales_unfiltered(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Sales'], tables, measures, None, dt, dc, rels)
        assert r['Sales'] == pytest.approx(2297201, rel=0.01)

    def test_sales_2015(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Sales'], tables, measures,
                                     {'dim-Date.Year': [2015]}, dt, dc, rels)
        assert r['Sales'] == pytest.approx(470533, rel=0.01)

    def test_profit_margin_2015(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Profit Margin'], tables, measures,
                                     {'dim-Date.Year': [2015]}, dt, dc, rels)
        assert r['Profit Margin'] == pytest.approx(0.131, rel=0.05)

    def test_sales_ly(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Sales LY'], tables, measures,
                                     {'dim-Date.Year': [2015]}, dt, dc, rels)
        assert r['Sales LY'] == pytest.approx(484247, rel=0.01)

    def test_sales_change(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Sales change'], tables, measures,
                                     {'dim-Date.Year': [2015]}, dt, dc, rels)
        assert r['Sales change'] == pytest.approx(-0.028, abs=0.005)

    def test_california_2015(self, data):
        tables, measures, rels, dt, dc = data
        r = evaluate_measures_batch(['Sales'], tables, measures,
                                     {'dim-Date.Year': [2015], 'dim-Geo.State': ['California']},
                                     dt, dc, rels)
        assert r['Sales'] == pytest.approx(88444, rel=0.01)

    def test_category_filter(self, data):
        tables, measures, rels, dt, dc = data
        for cat, expected in [('Technology', 162781), ('Office Supplies', 137233), ('Furniture', 170518)]:
            r = evaluate_measures_batch(['Sales'], tables, measures,
                                         {'dim-Date.Year': [2015], 'dim-Product.Category': [cat]},
                                         dt, dc, rels)
            assert r['Sales'] == pytest.approx(expected, rel=0.01), f'{cat} failed'

    def test_all_measures_no_crash(self, data):
        """Every measure should evaluate without crashing."""
        tables, measures, rels, dt, dc = data
        for name in measures:
            try:
                r = evaluate_measures_batch([name], tables, measures, None, dt, dc, rels)
            except Exception as e:
                pytest.fail(f'Measure "{name}" crashed: {e}')

    def test_success_rate_above_90(self, data):
        """At least 90% of measures should return non-None values."""
        tables, measures, rels, dt, dc = data
        success = 0
        for name in measures:
            r = evaluate_measures_batch([name], tables, measures, None, dt, dc, rels)
            if r.get(name) is not None:
                success += 1
        rate = success / len(measures)
        assert rate >= 0.90, f'Success rate {rate:.0%} < 90%'


# ---------------------------------------------------------------------------
# Agents Performance Dashboard
# ---------------------------------------------------------------------------

class TestAgentsPerformance:
    @pytest.fixture(scope='class')
    def data(self):
        if not os.path.exists(AGENTS):
            pytest.skip('Agents Performance PBIX not found')
        return _load_pbix(AGENTS)

    def test_loads(self, data):
        tables, measures, rels, dt, dc = data
        assert len(tables) >= 2
        assert len(measures) >= 50

    def test_all_measures_no_crash(self, data):
        tables, measures, rels, dt, dc = data
        for name in measures:
            try:
                evaluate_measures_batch([name], tables, measures, None, dt, dc, rels)
            except Exception as e:
                pytest.fail(f'Measure "{name}" crashed: {e}')

    def test_success_rate_above_90(self, data):
        tables, measures, rels, dt, dc = data
        success = sum(1 for name in measures
                      if evaluate_measures_batch([name], tables, measures, None, dt, dc, rels).get(name) is not None)
        rate = success / len(measures)
        assert rate >= 0.90, f'Success rate {rate:.0%} < 90%'


# ---------------------------------------------------------------------------
# Ecommerce Dashboard
# ---------------------------------------------------------------------------

class TestEcommerce:
    @pytest.fixture(scope='class')
    def data(self):
        if not os.path.exists(ECOMMERCE):
            pytest.skip('Ecommerce PBIX not found')
        return _load_pbix(ECOMMERCE)

    def test_loads(self, data):
        tables, measures, rels, dt, dc = data
        assert len(tables) >= 1
        assert len(measures) >= 20

    def test_all_measures_no_crash(self, data):
        tables, measures, rels, dt, dc = data
        for name in measures:
            try:
                evaluate_measures_batch([name], tables, measures, None, dt, dc, rels)
            except Exception as e:
                pytest.fail(f'Measure "{name}" crashed: {e}')

    def test_success_rate_above_80(self, data):
        tables, measures, rels, dt, dc = data
        success = sum(1 for name in measures
                      if evaluate_measures_batch([name], tables, measures, None, dt, dc, rels).get(name) is not None)
        rate = success / len(measures)
        assert rate >= 0.80, f'Success rate {rate:.0%} < 80%'


# ---------------------------------------------------------------------------
# IT Support Dashboard
# ---------------------------------------------------------------------------

class TestITSupport:
    @pytest.fixture(scope='class')
    def data(self):
        if not os.path.exists(IT_SUPPORT):
            pytest.skip('IT Support PBIX not found')
        return _load_pbix(IT_SUPPORT)

    def test_loads(self, data):
        tables, measures, rels, dt, dc = data
        assert len(tables) >= 2
        assert len(measures) >= 15

    def test_all_measures_no_crash(self, data):
        tables, measures, rels, dt, dc = data
        for name in measures:
            try:
                evaluate_measures_batch([name], tables, measures, None, dt, dc, rels)
            except Exception as e:
                pytest.fail(f'Measure "{name}" crashed: {e}')

    def test_success_rate_above_90(self, data):
        tables, measures, rels, dt, dc = data
        success = sum(1 for name in measures
                      if evaluate_measures_batch([name], tables, measures, None, dt, dc, rels).get(name) is not None)
        rate = success / len(measures)
        assert rate >= 0.90, f'Success rate {rate:.0%} < 90%'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
