"""Correctness + performance tests for the bucketed per-dimension fast path.

found_issues.md #7: pbix_evaluate_dax_per_dimension re-filtered the whole fact
table once per dimension value (O(values × fact_rows)). engine.evaluate_per_dimension
now groups the fact rows by the propagated join key ONCE (O(fact_rows + values)).
These tests prove the optimized output is identical to the per-value path and
that fact-row scans no longer scale with max_values.
"""
import json
import random

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.dax import engine as dax_engine
from pbix_mcp.dax.engine import DAXContext

FACT_ROWS = 200_000
N_VALUES = 50


def _star_model(fact_rows=FACT_ROWS, n_regions=N_VALUES):
    """Star: Orders -> Regions (direct relationship)."""
    regions = {
        "columns": ["RegionID", "RegionName"],
        "rows": [[i, f"R{i:02d}"] for i in range(n_regions)],
    }
    orders_rows = [[i, i % n_regions, float(i % 100)] for i in range(fact_rows)]
    orders = {"columns": ["OrderID", "RegionID", "Revenue"], "rows": orders_rows}
    tables = {"Regions": regions, "Orders": orders}
    measures = {
        "Total Revenue": "SUM(Orders[Revenue])",
        "Order Count": "COUNTROWS(Orders)",
        "Distinct Days": "DISTINCTCOUNT(Orders[Revenue])",
    }
    rels = [{"FromTable": "Orders", "FromColumn": "RegionID",
             "ToTable": "Regions", "ToColumn": "RegionID", "IsActive": True}]
    return tables, measures, rels


def _independent_groupby(tables, n_regions=N_VALUES):
    rid_to_name = {r[0]: r[1] for r in tables["Regions"]["rows"]}
    rev = {f"R{i:02d}": 0.0 for i in range(n_regions)}
    cnt = {f"R{i:02d}": 0 for i in range(n_regions)}
    days = {f"R{i:02d}": set() for i in range(n_regions)}
    for o in tables["Orders"]["rows"]:
        name = rid_to_name[o[1]]
        rev[name] += o[2]
        cnt[name] += 1
        days[name].add(str(o[2]))
    return rev, cnt, {k: len(v) for k, v in days.items()}


class TestCorrectness:
    def test_fast_equals_independent_groupby(self):
        tables, measures, rels = _star_model()
        vals = sorted({r[1] for r in tables["Regions"]["rows"]}, key=str)
        fast = dax_engine.evaluate_per_dimension(
            ["Total Revenue", "Order Count", "Distinct Days"],
            tables, measures, {}, "Regions.RegionName", "Regions", "RegionName",
            vals, None, None, rels,
        )
        rev, cnt, days = _independent_groupby(tables)
        for v in vals:
            assert fast["Total Revenue"][v] == rev[v]
            assert fast["Order Count"][v] == cnt[v]
            assert fast["Distinct Days"][v] == days[v]

    def test_fast_equals_per_value_path(self):
        """The fast path must equal the slow per-value engine path exactly."""
        tables, measures, rels = _star_model(fact_rows=20_000, n_regions=N_VALUES)
        vals = sorted({r[1] for r in tables["Regions"]["rows"]}, key=str)
        names = ["Total Revenue", "Order Count", "Distinct Days"]
        fast = dax_engine.evaluate_per_dimension(
            names, tables, measures, {}, "Regions.RegionName", "Regions",
            "RegionName", vals, None, None, rels,
        )
        for v in vals:
            slow = dax_engine.evaluate_measures_batch(
                names, tables, measures, {"Regions.RegionName": [v]},
                None, None, rels,
            )
            for m in names:
                assert fast[m][v] == slow[m], (m, v, fast[m][v], slow[m])

    def test_multihop_snowflake_bucketing(self):
        """Two-hop (Regions<-Customers<-Orders) bucketing matches per-value."""
        regions = {"columns": ["RegionID", "RegionName"],
                   "rows": [[1, "West"], [2, "East"], [3, "North"]]}
        customers = {"columns": ["CustomerID", "RegionID"],
                     "rows": [[10, 1], [11, 1], [12, 2]]}
        orders = {"columns": ["OrderID", "CustomerID", "Revenue"],
                  "rows": [[100, 10, 50.0], [101, 11, 30.0], [102, 12, 25.0]]}
        tables = {"Regions": regions, "Customers": customers, "Orders": orders}
        measures = {"Total Revenue": "SUM(Orders[Revenue])"}
        rels = [
            {"FromTable": "Customers", "FromColumn": "RegionID",
             "ToTable": "Regions", "ToColumn": "RegionID", "IsActive": True},
            {"FromTable": "Orders", "FromColumn": "CustomerID",
             "ToTable": "Customers", "ToColumn": "CustomerID", "IsActive": True},
        ]
        vals = ["East", "North", "West"]
        fast = dax_engine.evaluate_per_dimension(
            ["Total Revenue"], tables, measures, {}, "Regions.RegionName",
            "Regions", "RegionName", vals, None, None, rels,
        )
        assert fast["Total Revenue"]["West"] == 80.0
        assert fast["Total Revenue"]["East"] == 25.0
        # no customers -> SUM over an empty selection is BLANK (Desktop semantics)
        assert fast["Total Revenue"]["North"] is None
        # identical to per-value
        for v in vals:
            slow = dax_engine.evaluate_measures_batch(
                ["Total Revenue"], tables, measures, {"Regions.RegionName": [v]},
                None, None, rels,
            )
            assert fast["Total Revenue"][v] == slow["Total Revenue"]

    def test_base_filter_on_fact(self):
        """A base filter directly on the fact must combine with the bucketing."""
        regions = {"columns": ["RegionID", "RegionName"],
                   "rows": [[1, "West"], [2, "East"]]}
        orders = {"columns": ["OrderID", "RegionID", "Flag", "Amt"], "rows": [
            [1, 1, "A", 10.0], [2, 1, "B", 5.0], [3, 1, "A", 20.0],
            [4, 2, "A", 100.0], [5, 2, "B", 7.0],
        ]}
        tables = {"Regions": regions, "Orders": orders}
        measures = {"Total": "SUM(Orders[Amt])"}
        rels = [{"FromTable": "Orders", "FromColumn": "RegionID",
                 "ToTable": "Regions", "ToColumn": "RegionID", "IsActive": True}]
        base = {"Orders.Flag": ["A"]}
        vals = ["East", "West"]
        fast = dax_engine.evaluate_per_dimension(
            ["Total"], tables, measures, base, "Regions.RegionName", "Regions",
            "RegionName", vals, None, None, rels,
        )
        # West Flag=A -> 10+20=30 ; East Flag=A -> 100
        assert fast["Total"]["West"] == 30.0
        assert fast["Total"]["East"] == 100.0
        # identical to per-value with the same base filter
        for v in vals:
            fc = dict(base)
            fc["Regions.RegionName"] = [v]
            slow = dax_engine.evaluate_measures_batch(
                ["Total"], tables, measures, fc, None, None, rels)
            assert fast["Total"][v] == slow["Total"]

    def test_base_filter_on_second_dimension(self):
        """A base filter on a DIFFERENT dimension (cross-table) must be honored."""
        regions = {"columns": ["RegionID", "RegionName"],
                   "rows": [[1, "West"], [2, "East"]]}
        years = {"columns": ["YearID", "Year"], "rows": [[1, 2020], [2, 2021]]}
        orders = {"columns": ["OID", "RegionID", "YearID", "Amt"], "rows": [
            [1, 1, 1, 10.0],   # West 2020
            [2, 1, 2, 50.0],   # West 2021
            [3, 2, 1, 100.0],  # East 2020
            [4, 2, 2, 200.0],  # East 2021
        ]}
        tables = {"Regions": regions, "Years": years, "Orders": orders}
        measures = {"Total": "SUM(Orders[Amt])"}
        rels = [
            {"FromTable": "Orders", "FromColumn": "RegionID",
             "ToTable": "Regions", "ToColumn": "RegionID", "IsActive": True},
            {"FromTable": "Orders", "FromColumn": "YearID",
             "ToTable": "Years", "ToColumn": "YearID", "IsActive": True},
        ]
        base = {"Years.Year": [2020]}
        vals = ["East", "West"]
        fast = dax_engine.evaluate_per_dimension(
            ["Total"], tables, measures, base, "Regions.RegionName", "Regions",
            "RegionName", vals, None, None, rels,
        )
        # only 2020 rows count: West->10, East->100
        assert fast["Total"]["West"] == 10.0
        assert fast["Total"]["East"] == 100.0
        for v in vals:
            fc = dict(base)
            fc["Regions.RegionName"] = [v]
            slow = dax_engine.evaluate_measures_batch(
                ["Total"], tables, measures, fc, None, None, rels)
            assert fast["Total"][v] == slow["Total"], (v, fast["Total"][v], slow["Total"])

    def test_complex_measure_not_bucketed(self):
        """A non-simple measure is omitted from the fast dict (caller falls back)."""
        tables, measures, rels = _star_model(fact_rows=1000)
        measures["Ratio"] = "DIVIDE(SUM(Orders[Revenue]), COUNTROWS(Orders))"
        vals = sorted({r[1] for r in tables["Regions"]["rows"]}, key=str)
        fast = dax_engine.evaluate_per_dimension(
            ["Total Revenue", "Ratio"], tables, measures, {},
            "Regions.RegionName", "Regions", "RegionName", vals, None, None, rels,
        )
        assert "Total Revenue" in fast      # simple -> bucketed
        assert "Ratio" not in fast          # complex -> left for fallback


class TestEndToEndTool:
    def test_tool_produces_distinct_per_value_results(self, tmp_path):
        """The server tool returns correct, distinct per-dimension numbers."""
        p = str(tmp_path / "star.pbix")
        b = PBIXBuilder("T")
        b.add_table("Dim", [
            {"name": "DID", "data_type": "Int64"},
            {"name": "Label", "data_type": "String"},
        ], rows=[{"DID": 1, "Label": "A"}, {"DID": 2, "Label": "B"},
                 {"DID": 3, "Label": "C"}])
        b.add_table("Fact", [
            {"name": "FID", "data_type": "Int64"},
            {"name": "DID", "data_type": "Int64"},
            {"name": "Amt", "data_type": "Double"},
        ], rows=[
            {"FID": 1, "DID": 1, "Amt": 10.0},
            {"FID": 2, "DID": 1, "Amt": 15.0},
            {"FID": 3, "DID": 2, "Amt": 100.0},
        ])
        b.add_relationship("Dim", "DID", "Fact", "DID")
        b.add_measure("Fact", "Total", "SUM(Fact[Amt])")
        # A complex measure that the fast path cannot bucket -> driver must fall
        # back to the per-value path for it, alongside the bucketed "Total".
        b.add_measure("Fact", "Avg Per Row", "DIVIDE(SUM(Fact[Amt]), COUNTROWS(Fact))")
        b.save(p)

        alias = "perdim"
        try:
            server.pbix_open(p, alias)
            out = server.pbix_evaluate_dax_per_dimension(
                alias, "Total,Avg Per Row", "Dim.Label", max_values=10
            )
            parsed = json.loads(out)
            assert parsed["success"] is True
            msg = parsed["message"]
            # A -> Total 25 (fast) / Avg 12.5 (fallback); B -> 100 / 100.
            assert "25.00" in msg
            assert "100.00" in msg
            assert "12.50" in msg   # proves the fallback measure was evaluated
        finally:
            server._open_files.pop(alias, None)


def _assert_fast_matches_per_value(tables, measures, rels, dimension, dim_table,
                                   dim_col, base_fc, vals):
    """The contract: every measure the fast path RETURNS must equal per-value."""
    fast = dax_engine.evaluate_per_dimension(
        list(measures), tables, measures, base_fc, dimension, dim_table,
        dim_col, vals, None, None, rels)
    for m in measures:
        if m not in fast:          # omitted -> driver falls back -> matches by construction
            continue
        for v in vals:
            fc = dict(base_fc or {})
            fc[dimension] = [v]
            slow = dax_engine.evaluate_measures_batch(
                [m], tables, measures, fc, None, None, rels).get(m)
            assert fast[m][v] == slow, (m, v, fast[m][v], slow, base_fc)


class TestAdversarialRegressions:
    """Exact repros of every divergence the adversarial review surfaced.

    Each requires base_fc to filter the dimension's own join path; the fast path
    must now fall back so the tool returns the exact per-value result.
    """

    def test_null_dimension_value_with_base_filter(self):
        _assert_fast_matches_per_value(
            {'Dim': {'columns': ['Key', 'Category', 'Region'],
                     'rows': [[None, 'C', 'W'], [None, None, 'E'], [2, 'C', 'E']]},
             'Fact': {'columns': ['Key', 'Amount'], 'rows': [[2, 10], [None, -3]]}},
            {'S': 'SUM(Fact[Amount])', 'C': 'COUNT(Fact[Amount])', 'R': 'COUNTROWS(Fact)'},
            [{'FromTable': 'Fact', 'FromColumn': 'Key', 'ToTable': 'Dim', 'ToColumn': 'Key', 'IsActive': True}],
            'Dim.Category', 'Dim', 'Category', {'Dim.Region': ['E']}, ['C'])

    def test_nonunique_key_with_base_filter(self):
        _assert_fast_matches_per_value(
            {'Product': {'columns': ['PKey', 'Category', 'Color'],
                         'rows': [[1, None, 'Red'], [1, 'A', 'Blue'], [2, 'A', 'Red']]},
             'Orders': {'columns': ['OID', 'PKey', 'Amount'], 'rows': [[100, 1, 50], [200, 2, 70]]}},
            {'T': 'SUM(Orders[Amount])', 'R': 'COUNTROWS(Orders)'},
            [{'FromTable': 'Orders', 'FromColumn': 'PKey', 'ToTable': 'Product', 'ToColumn': 'PKey', 'IsActive': True}],
            'Product.Category', 'Product', 'Category', {'Product.Color': ['Red']}, ['A'])

    def test_empty_conjunction_grand_total_leak(self):
        _assert_fast_matches_per_value(
            {'Dim': {'columns': ['K', 'Name', 'Region'],
                     'rows': [[1, 'Alpha', 'N'], [2, 'Beta', 'S'], [3, 'Gamma', 'N']]},
             'Fact': {'columns': ['FK', 'Amt'], 'rows': [[1, 10], [1, 15], [2, 20], [3, 30], [3, 40]]}},
            {'S': 'SUM(Fact[Amt])', 'R': 'COUNTROWS(Fact)'},
            [{'FromTable': 'Fact', 'FromColumn': 'FK', 'ToTable': 'Dim', 'ToColumn': 'K', 'IsActive': True}],
            'Dim.Name', 'Dim', 'Name', {'Dim.Region': ['N']}, ['Alpha', 'Beta', 'Gamma'])

    def test_capping_defeats_ambiguity_guard(self):
        _assert_fast_matches_per_value(
            {'D': {'columns': ['K', 'Cat', 'Other'], 'rows': [[1, 'x', 'p'], [1, 'y', 'q'], [9, 'x', 'q']]},
             'F': {'columns': ['FK', 'Amt'], 'rows': [[1, 10]]}},
            {'T': 'SUM(F[Amt])', 'R': 'COUNTROWS(F)'},
            [{'FromTable': 'F', 'FromColumn': 'FK', 'ToTable': 'D', 'ToColumn': 'K', 'IsActive': True}],
            'D.Cat', 'D', 'Cat', {'D.Other': ['q']}, ['x'])


class TestValueKeyHardening:
    def test_equal_hash_values_do_not_collapse(self):
        """A hand-fed non-deduped [1, 1.0, True] must not cross-contaminate buckets."""
        tables = {
            'Dim': {'columns': ['JK', 'V'], 'rows': [[1, 'a'], [2, 'b']]},
            'Fact': {'columns': ['FK', 'Amt'], 'rows': [['a', 10], ['b', 20]]},
        }
        # direct-on-fact dimension so we can force the value list
        tables2 = {'Fact': {'columns': ['Cat', 'Amt'],
                            'rows': [[1, 10], [2, 20]]}}
        measures = {'S': 'SUM(Fact[Amt])'}
        # feed 1, 1.0, True (all == and hash-equal) — must collapse to one key,
        # and the fast result must still equal the per-value result for value 1.
        fast = dax_engine.evaluate_per_dimension(
            ['S'], tables2, measures, {}, 'Fact.Cat', 'Fact', 'Cat',
            [1, 1.0, True], None, None, [])
        if 'S' in fast:
            slow = dax_engine.evaluate_measures_batch(
                ['S'], tables2, measures, {'Fact.Cat': [1]}, None, None, []).get('S')
            # every surviving key must match per-value for that key
            for k, v in fast['S'].items():
                s = dax_engine.evaluate_measures_batch(
                    ['S'], tables2, measures, {'Fact.Cat': [k]}, None, None, []).get('S')
                assert v == s, (k, v, s)


class TestFuzzFastEqualsPerValue:
    """Randomized: hundreds of null/dup-key/capping/base-filter models; the fast
    path must never return a value that differs from the per-value path."""

    def test_fuzz(self):
        rng = random.Random(20260718)
        AGGS = {'S': 'SUM(Fact[Amt])', 'C': 'COUNT(Fact[Amt])', 'R': 'COUNTROWS(Fact)',
                'A': 'AVERAGE(Fact[Amt])', 'MN': 'MIN(Fact[Amt])', 'MX': 'MAX(Fact[Amt])',
                'D': 'DISTINCTCOUNT(Fact[Amt])'}
        cats = ['x', 'y', 'z', None]
        extras = ['p', 'q']
        for _ in range(400):
            n_dim = rng.randint(2, 6)
            keyspace = [rng.choice([1, 2, 3, None]) for _ in range(n_dim)]  # dup + null keys
            dim_rows = [[keyspace[i], rng.choice(cats), rng.choice(extras)] for i in range(n_dim)]
            n_fact = rng.randint(0, 8)
            fact_rows = [[rng.choice([1, 2, 3, 99, None]),
                          rng.choice([1.0, 5.0, -3.0, 10.0, None])] for _ in range(n_fact)]
            tables = {'Dim': {'columns': ['Key', 'Cat', 'Extra'], 'rows': dim_rows},
                      'Fact': {'columns': ['FK', 'Amt'], 'rows': fact_rows}}
            rels = [{'FromTable': 'Fact', 'FromColumn': 'FK', 'ToTable': 'Dim',
                     'ToColumn': 'Key', 'IsActive': True}]
            base_fc = rng.choice([
                {}, {'Dim.Extra': ['p']}, {'Dim.Extra': ['q']},
                {'Dim.Extra': ['ZZZ']},  # impossible
            ])
            allvals = sorted({r[1] for r in dim_rows if r[1] is not None}, key=str)
            if not allvals:
                continue
            # exercise capping: sometimes pass only a subset of the values
            k = rng.randint(1, len(allvals))
            vals = allvals[:k]
            _assert_fast_matches_per_value(
                tables, AGGS, rels, 'Dim.Cat', 'Dim', 'Cat', base_fc, vals)


class TestPerformance:
    def test_scans_independent_of_max_values(self, monkeypatch):
        tables, measures, rels = _star_model()
        vals = sorted({r[1] for r in tables["Regions"]["rows"]}, key=str)

        counter = {"scanned": 0}
        orig_gcd = DAXContext.get_column_data
        orig_gfr = DAXContext.get_filtered_rows

        def gcd(self, table_name, column_name):
            if table_name == "Orders":
                counter["scanned"] += len(self.tables.get("Orders", {}).get("rows", []))
            return orig_gcd(self, table_name, column_name)

        def gfr(self, table_name):
            if table_name == "Orders":
                counter["scanned"] += len(self.tables.get("Orders", {}).get("rows", []))
            return orig_gfr(self, table_name)

        monkeypatch.setattr(DAXContext, "get_column_data", gcd)
        monkeypatch.setattr(DAXContext, "get_filtered_rows", gfr)

        def run(max_values):
            counter["scanned"] = 0
            dax_engine.evaluate_per_dimension(
                ["Total Revenue", "Order Count"], tables, measures, {},
                "Regions.RegionName", "Regions", "RegionName",
                vals[:max_values], None, None, rels,
            )
            return counter["scanned"]

        s5 = run(5)
        s50 = run(50)

        # O(fact_rows): total fact-row scans stay a small multiple of the fact
        # size no matter how many dimension values are evaluated. The old
        # per-value path would scan ~ max_values × fact_rows (5×/50× FACT_ROWS).
        assert s50 <= 5 * FACT_ROWS, (s50, FACT_ROWS)
        # near-independent of max_values (per-value would be ~10× going 5->50)
        assert s50 <= 3 * s5, (s5, s50)
