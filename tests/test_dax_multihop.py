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


class TestEmptySelectionNoGrandTotalLeak:
    """0.9.8 #3: an empty cross-filter selection must return BLANK/0, not the
    grand total (single-hop + date paths; mirrors the multi-hop fix)."""

    def test_single_hop_empty_selection_is_blank(self):
        tables = {
            'Dim': {'columns': ['K', 'Name', 'Region'],
                    'rows': [[1, 'Alpha', 'N'], [2, 'Beta', 'S']]},
            'Fact': {'columns': ['FK', 'Amt'], 'rows': [[1, 10], [2, 20]]},
        }
        measures = {'S': 'SUM(Fact[Amt])'}
        rels = [{'FromTable': 'Fact', 'FromColumn': 'FK',
                 'ToTable': 'Dim', 'ToColumn': 'K', 'IsActive': True}]
        # Beta is Region 'S'; filtering Region='N' AND Name='Beta' selects zero
        # Dim rows -> the fact must filter to zero rows -> BLANK (Desktop
        # semantics for SUM over an empty selection), not the grand total 30.
        r = dax_engine.evaluate_measures_batch(
            ['S'], tables, measures, {'Dim.Region': ['N'], 'Dim.Name': ['Beta']},
            None, None, rels)
        assert r['S'] is None

    def test_nonempty_selection_still_correct(self):
        tables = {
            'Dim': {'columns': ['K', 'Region'], 'rows': [[1, 'N'], [2, 'S']]},
            'Fact': {'columns': ['FK', 'Amt'], 'rows': [[1, 10], [2, 20]]},
        }
        measures = {'S': 'SUM(Fact[Amt])'}
        rels = [{'FromTable': 'Fact', 'FromColumn': 'FK',
                 'ToTable': 'Dim', 'ToColumn': 'K', 'IsActive': True}]
        r = dax_engine.evaluate_measures_batch(
            ['S'], tables, measures, {'Dim.Region': ['N']}, None, None, rels)
        assert r['S'] == 10


class TestBidirectionalCrossFilter:
    """OpenBI #5a: CrossFilteringBehavior is now carried into the engine, so a
    bidirectional (=2) relationship adds the reverse multi-hop edge."""

    def test_bidirectional_adds_reverse_edge(self):
        tables = {'Dim': {'columns': ['K', 'V'], 'rows': [[1, 'x']]},
                  'Fact': {'columns': ['FK', 'Amt'], 'rows': [[1, 10]]}}
        single = dax_engine.DAXContext(tables, {}, None, None, None, [
            {'FromTable': 'Fact', 'FromColumn': 'FK', 'ToTable': 'Dim',
             'ToColumn': 'K', 'IsActive': True, 'CrossFilteringBehavior': 1}])
        bidir = dax_engine.DAXContext(tables, {}, None, None, None, [
            {'FromTable': 'Fact', 'FromColumn': 'FK', 'ToTable': 'Dim',
             'ToColumn': 'K', 'IsActive': True, 'CrossFilteringBehavior': 2}])
        assert 'Fact' not in single._rel_adj          # one-direction: Dim->Fact only
        assert 'Fact' in bidir._rel_adj               # bidirectional: reverse edge added


class TestUserelationshipAndCrossfilter:
    """OpenBI #5b: CALCULATE must consume USERELATIONSHIP / CROSSFILTER instead
    of treating them as silent no-op markers."""

    def _roleplay(self, fc, measure):
        # Sales has two keys into Dim; OrderK relationship active, ShipK inactive.
        tables = {
            "Sales": {"columns": ["OrderK", "ShipK", "Amount"],
                      "rows": [["K1", "K2", 100.0], ["K2", "K1", 50.0]]},
            "Dim": {"columns": ["K"], "rows": [["K1"], ["K2"]]},
        }
        measures = {
            "ByOrder": "SUM(Sales[Amount])",
            "ByShip": "CALCULATE(SUM(Sales[Amount]), USERELATIONSHIP(Sales[ShipK], Dim[K]))",
            "NoFilter": "CALCULATE(SUM(Sales[Amount]), CROSSFILTER(Sales[OrderK], Dim[K], None))",
        }
        rels = [
            {"FromTable": "Sales", "FromColumn": "OrderK", "ToTable": "Dim",
             "ToColumn": "K", "IsActive": True},
            {"FromTable": "Sales", "FromColumn": "ShipK", "ToTable": "Dim",
             "ToColumn": "K", "IsActive": False},
        ]
        res = dax_engine.evaluate_measures_batch(
            [measure], tables, measures, fc, None, None, rels)
        return res[measure]

    def test_active_relationship_filters_by_order(self):
        # filter Dim=K1 via the active OrderK relationship -> row with OrderK=K1 (100)
        assert self._roleplay({"Dim.K": ["K1"]}, "ByOrder") == 100.0

    def test_userelationship_switches_to_ship(self):
        # USERELATIONSHIP activates ShipK -> filter Dim=K1 selects row with ShipK=K1 (50)
        assert self._roleplay({"Dim.K": ["K1"]}, "ByShip") == 50.0

    def test_userelationship_differs_from_active(self):
        assert self._roleplay({"Dim.K": ["K1"]}, "ByOrder") != \
               self._roleplay({"Dim.K": ["K1"]}, "ByShip")

    def test_crossfilter_none_removes_propagation(self):
        # CROSSFILTER(..., None) stops the relationship filtering -> grand total (150)
        assert self._roleplay({"Dim.K": ["K1"]}, "NoFilter") == 150.0


class TestDateTableDetection:
    """0.9.11: date-table auto-detection prefers the date dimension on the
    one-side of a relationship over a fact that merely has a Date column."""

    def test_relationship_disambiguates_date_table(self):
        # Fact 'Sales' also has Date+Year (would trip the name-only heuristic);
        # the real date dim 'Calendar' is the ToTable of the relationship.
        tables = {
            "Sales": {"columns": ["Date", "Year", "Amount"], "rows": [["2024-01-01", 2024, 5.0]]},
            "Calendar": {"columns": ["Date", "Year", "Month"], "rows": [["2024-01-01", 2024, 1]]},
        }
        rels = [{"FromTable": "Sales", "FromColumn": "Date",
                 "ToTable": "Calendar", "ToColumn": "Date", "IsActive": True}]
        assert dax_engine.DAXContext._auto_detect_date_table(tables, rels) == "Calendar"

    def test_name_heuristic_still_works_without_relationships(self):
        tables = {"dimDate": {"columns": ["Date", "Year"], "rows": [["2024-01-01", 2024]]}}
        assert dax_engine.DAXContext._auto_detect_date_table(tables, []) == "dimDate"
