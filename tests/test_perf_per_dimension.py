"""Correctness + performance tests for the bucketed per-dimension fast path.

found_issues.md #7: pbix_evaluate_dax_per_dimension re-filtered the whole fact
table once per dimension value (O(values × fact_rows)). engine.evaluate_per_dimension
now groups the fact rows by the propagated join key ONCE (O(fact_rows + values)).
These tests prove the optimized output is identical to the per-value path and
that fact-row scans no longer scale with max_values.
"""
import json

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
        assert fast["Total Revenue"]["North"] in (0, 0.0)   # no customers
        # identical to per-value
        for v in vals:
            slow = dax_engine.evaluate_measures_batch(
                ["Total Revenue"], tables, measures, {"Regions.RegionName": [v]},
                None, None, rels,
            )
            assert fast["Total Revenue"][v] == slow["Total Revenue"]

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
