"""Regression tests for multi-hop (snowflake) DAX filter propagation.

Covers found_issues.md #1 (filters on multi-hop dimensions silently dropped)
and #2 (BFS must respect relationship direction; an empty intermediate key-set
must mean zero rows, not the grand total).
"""
from pbix_mcp.dax import engine as dax_engine


def _snowflake_model():
    """Regions <- Customers <- Orders (two-hop snowflake).

    Relationships (FromTable=many, ToTable=one):
      Customers[RegionID] -> Regions[RegionID]
      Orders[CustomerID]  -> Customers[CustomerID]
    """
    tables = {
        "Regions": {
            "columns": ["RegionID", "RegionName"],
            "rows": [[1, "West"], [2, "East"], [3, "North"]],
        },
        "Customers": {
            "columns": ["CustomerID", "RegionID"],
            # C1,C2 in West(1); C3 in East(2); North(3) has no customers
            "rows": [[10, 1], [11, 1], [12, 2]],
        },
        "Orders": {
            "columns": ["OrderID", "CustomerID", "Revenue"],
            "rows": [
                [100, 10, 50.0],   # West
                [101, 11, 30.0],   # West
                [102, 12, 25.0],   # East
            ],
        },
    }
    measures = {"Total Revenue": "SUM(Orders[Revenue])"}
    relationships = [
        {"FromTable": "Customers", "FromColumn": "RegionID",
         "ToTable": "Regions", "ToColumn": "RegionID", "IsActive": True},
        {"FromTable": "Orders", "FromColumn": "CustomerID",
         "ToTable": "Customers", "ToColumn": "CustomerID", "IsActive": True},
    ]
    return tables, measures, relationships


def _eval(fc):
    tables, measures, rels = _snowflake_model()
    res = dax_engine.evaluate_measures_batch(
        ["Total Revenue"], tables, measures, fc, None, None, rels
    )
    return res["Total Revenue"]


class TestMultiHopFilterPropagation:
    def test_two_hop_filter_applies(self):
        """A filter on Regions (2 hops from Orders) must restrict the fact."""
        assert _eval({"Regions.RegionName": ["West"]}) == 80.0   # 50 + 30
        assert _eval({"Regions.RegionName": ["East"]}) == 25.0

    def test_two_hop_distinct_per_region(self):
        """Different regions must yield different values (not the grand total)."""
        west = _eval({"Regions.RegionName": ["West"]})
        east = _eval({"Regions.RegionName": ["East"]})
        assert west != east
        assert west + east == 105.0   # only West+East orders; grand total is 105

    def test_empty_intermediate_means_zero_not_grand_total(self):
        """A region with no customers must return 0/BLANK, not the grand total."""
        # North (RegionID=3) has no customers -> no orders.
        result = _eval({"Regions.RegionName": ["North"]})
        assert result in (0, 0.0, None), f"expected empty result, got {result}"

    def test_no_filter_is_grand_total(self):
        assert _eval({}) == 105.0

    def test_single_hop_still_works(self):
        """Direct (one-hop) filter on Customers must still restrict Orders."""
        # West customers are 10, 11 -> orders 100,101 -> 80
        assert _eval({"Customers.CustomerID": ["10", "11"]}) == 80.0


class TestDirectionSafety:
    def test_no_sibling_leak_through_shared_fact(self):
        """A filter on one dim must NOT leak to a sibling dim via the shared fact.

        Star: Products <- Sales -> Customers. Filtering Products must not
        restrict COUNTROWS(Customers) (single-direction relationships).
        """
        tables = {
            "Products": {"columns": ["ProductID", "Cat"],
                         "rows": [[1, "A"], [2, "B"]]},
            "Customers": {"columns": ["CustomerID", "Name"],
                          "rows": [[10, "x"], [11, "y"], [12, "z"]]},
            "Sales": {"columns": ["SaleID", "ProductID", "CustomerID", "Amt"],
                      "rows": [[100, 1, 10, 5.0], [101, 2, 11, 7.0]]},
        }
        measures = {"Cust Count": "COUNTROWS(Customers)"}
        rels = [
            {"FromTable": "Sales", "FromColumn": "ProductID",
             "ToTable": "Products", "ToColumn": "ProductID", "IsActive": True},
            {"FromTable": "Sales", "FromColumn": "CustomerID",
             "ToTable": "Customers", "ToColumn": "CustomerID", "IsActive": True},
        ]
        res = dax_engine.evaluate_measures_batch(
            ["Cust Count"], tables, measures,
            {"Products.Cat": ["A"]}, None, None, rels
        )
        # All 3 customers remain — the Products filter must not cross the fact.
        assert res["Cust Count"] == 3
