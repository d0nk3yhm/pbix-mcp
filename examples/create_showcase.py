"""Build the Northwind Analytics Dashboard — a full showcase of pbix-mcp.

Creates a multi-page Power BI report with:
- 6 tables (Regions, Categories, Customers, Products, Salespeople, Orders)
- 5 relationships (star schema with chained lookups)
- 4 DAX measures (Total Revenue, Order Count, Total Quantity, Avg Order Value)
- 3 pages (Executive Overview, Product Performance, Sales Team)
- 14 visuals (cards, bar charts, pie charts, slicers, detail tables)

Usage:
    pip install pbix-mcp
    python create_showcase.py

Opens in Power BI Desktop with full interactivity — cross-filtering,
slicers, and all cross-table lookups work out of the box.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder

b = PBIXBuilder("Northwind Analytics Dashboard")

# ── Dimension tables ─────────────────────────────────────────────────

b.add_table("Regions", [
    {"name": "RegionID", "data_type": "Int64"},
    {"name": "RegionName", "data_type": "String"},
    {"name": "Country", "data_type": "String"},
    {"name": "Timezone", "data_type": "String"},
    {"name": "Currency", "data_type": "String"},
    {"name": "Priority", "data_type": "Int64"},
], rows=[
    {"RegionID": 1, "RegionName": "Northern Europe", "Country": "Norway", "Timezone": "CET", "Currency": "NOK", "Priority": 1},
    {"RegionID": 2, "RegionName": "Western Europe", "Country": "Germany", "Timezone": "CET", "Currency": "EUR", "Priority": 1},
    {"RegionID": 3, "RegionName": "North America", "Country": "USA", "Timezone": "EST", "Currency": "USD", "Priority": 1},
    {"RegionID": 4, "RegionName": "Asia Pacific", "Country": "Japan", "Timezone": "JST", "Currency": "JPY", "Priority": 1},
    {"RegionID": 5, "RegionName": "South America", "Country": "Brazil", "Timezone": "BRT", "Currency": "BRL", "Priority": 5},
])

b.add_table("Categories", [
    {"name": "CategoryID", "data_type": "Int64"},
    {"name": "CategoryName", "data_type": "String"},
    {"name": "Department", "data_type": "String"},
    {"name": "TaxRate", "data_type": "Double"},
    {"name": "MinAge", "data_type": "Int64"},
    {"name": "SeasonTag", "data_type": "String"},
], rows=[
    {"CategoryID": 1, "CategoryName": "Electronics", "Department": "Technology", "TaxRate": 0.25, "MinAge": 0, "SeasonTag": "No"},
    {"CategoryID": 2, "CategoryName": "Furniture", "Department": "Home & Office", "TaxRate": 0.20, "MinAge": 0, "SeasonTag": "No"},
    {"CategoryID": 3, "CategoryName": "Clothing", "Department": "Fashion", "TaxRate": 0.15, "MinAge": 0, "SeasonTag": "Yes"},
    {"CategoryID": 4, "CategoryName": "Food & Beverage", "Department": "Grocery", "TaxRate": 0.10, "MinAge": 0, "SeasonTag": "Yes"},
    {"CategoryID": 5, "CategoryName": "Sports", "Department": "Recreation", "TaxRate": 0.20, "MinAge": 12, "SeasonTag": "Yes"},
    {"CategoryID": 6, "CategoryName": "Books", "Department": "Education", "TaxRate": 0.0, "MinAge": 0, "SeasonTag": "No"},
])

b.add_table("Customers", [
    {"name": "CustomerID", "data_type": "Int64"},
    {"name": "CustomerName", "data_type": "String"},
    {"name": "RegionID", "data_type": "Int64"},
    {"name": "Segment", "data_type": "String"},
    {"name": "JoinYear", "data_type": "Int64"},
    {"name": "LoyaltyTier", "data_type": "String"},
], rows=[
    {"CustomerID": 1, "CustomerName": "Acme Corp", "RegionID": 1, "Segment": "Enterprise", "JoinYear": 2020, "LoyaltyTier": "Gold"},
    {"CustomerID": 2, "CustomerName": "Globex Inc", "RegionID": 2, "Segment": "Mid-Market", "JoinYear": 2021, "LoyaltyTier": "Silver"},
    {"CustomerID": 3, "CustomerName": "Initech", "RegionID": 3, "Segment": "Enterprise", "JoinYear": 2019, "LoyaltyTier": "Platinum"},
    {"CustomerID": 4, "CustomerName": "Wayne Enterprises", "RegionID": 3, "Segment": "Enterprise", "JoinYear": 2022, "LoyaltyTier": "Gold"},
    {"CustomerID": 5, "CustomerName": "Stark Industries", "RegionID": 4, "Segment": "Enterprise", "JoinYear": 2020, "LoyaltyTier": "Platinum"},
    {"CustomerID": 6, "CustomerName": "Umbrella Corp", "RegionID": 2, "Segment": "Mid-Market", "JoinYear": 2023, "LoyaltyTier": "Bronze"},
    {"CustomerID": 7, "CustomerName": "Cyberdyne Systems", "RegionID": 4, "Segment": "Startup", "JoinYear": 2024, "LoyaltyTier": "Bronze"},
    {"CustomerID": 8, "CustomerName": "Wonka Industries", "RegionID": 1, "Segment": "Mid-Market", "JoinYear": 2021, "LoyaltyTier": "Silver"},
    {"CustomerID": 9, "CustomerName": "Oscorp", "RegionID": 3, "Segment": "Startup", "JoinYear": 2025, "LoyaltyTier": "Bronze"},
    {"CustomerID": 10, "CustomerName": "LexCorp", "RegionID": 5, "Segment": "Enterprise", "JoinYear": 2018, "LoyaltyTier": "Platinum"},
])

b.add_table("Products", [
    {"name": "ProductID", "data_type": "Int64"},
    {"name": "ProductName", "data_type": "String"},
    {"name": "CategoryID", "data_type": "Int64"},
    {"name": "UnitPrice", "data_type": "Double"},
    {"name": "StockQty", "data_type": "Int64"},
    {"name": "Status", "data_type": "String"},
], rows=[
    {"ProductID": 1, "ProductName": "Laptop Pro 16", "CategoryID": 1, "UnitPrice": 1299.00, "StockQty": 150, "Status": "Active"},
    {"ProductID": 2, "ProductName": "Wireless Mouse", "CategoryID": 1, "UnitPrice": 49.99, "StockQty": 500, "Status": "Active"},
    {"ProductID": 3, "ProductName": "Standing Desk", "CategoryID": 2, "UnitPrice": 699.00, "StockQty": 75, "Status": "Active"},
    {"ProductID": 4, "ProductName": "Ergonomic Chair", "CategoryID": 2, "UnitPrice": 449.00, "StockQty": 120, "Status": "Active"},
    {"ProductID": 5, "ProductName": "Winter Jacket", "CategoryID": 3, "UnitPrice": 189.00, "StockQty": 200, "Status": "Active"},
    {"ProductID": 6, "ProductName": "Running Shoes", "CategoryID": 5, "UnitPrice": 129.00, "StockQty": 300, "Status": "Active"},
    {"ProductID": 7, "ProductName": "Organic Coffee", "CategoryID": 4, "UnitPrice": 24.99, "StockQty": 1000, "Status": "Active"},
    {"ProductID": 8, "ProductName": "Python Cookbook", "CategoryID": 6, "UnitPrice": 59.99, "StockQty": 80, "Status": "Active"},
    {"ProductID": 9, "ProductName": "Monitor 27 4K", "CategoryID": 1, "UnitPrice": 549.00, "StockQty": 90, "Status": "Active"},
    {"ProductID": 10, "ProductName": "Desk Lamp", "CategoryID": 2, "UnitPrice": 79.99, "StockQty": 250, "Status": "Discontinued"},
    {"ProductID": 11, "ProductName": "Yoga Mat", "CategoryID": 5, "UnitPrice": 39.99, "StockQty": 400, "Status": "Active"},
    {"ProductID": 12, "ProductName": "Green Tea Set", "CategoryID": 4, "UnitPrice": 34.99, "StockQty": 150, "Status": "Active"},
])

b.add_table("Salespeople", [
    {"name": "SalespersonID", "data_type": "Int64"},
    {"name": "FullName", "data_type": "String"},
    {"name": "RegionID", "data_type": "Int64"},
    {"name": "Title", "data_type": "String"},
    {"name": "HireYear", "data_type": "Int64"},
    {"name": "CommissionRate", "data_type": "Double"},
], rows=[
    {"SalespersonID": 1, "FullName": "Emma Larsen", "RegionID": 1, "Title": "Senior Rep", "HireYear": 2019, "CommissionRate": 0.08},
    {"SalespersonID": 2, "FullName": "Max Mueller", "RegionID": 2, "Title": "Account Exec", "HireYear": 2020, "CommissionRate": 0.06},
    {"SalespersonID": 3, "FullName": "Sarah Johnson", "RegionID": 3, "Title": "VP Sales", "HireYear": 2017, "CommissionRate": 0.10},
    {"SalespersonID": 4, "FullName": "Yuki Tanaka", "RegionID": 4, "Title": "Regional Mgr", "HireYear": 2021, "CommissionRate": 0.07},
    {"SalespersonID": 5, "FullName": "Carlos Silva", "RegionID": 5, "Title": "Sales Rep", "HireYear": 2023, "CommissionRate": 0.05},
    {"SalespersonID": 6, "FullName": "Anna Berg", "RegionID": 1, "Title": "Account Exec", "HireYear": 2022, "CommissionRate": 0.06},
])

# ── Fact table ───────────────────────────────────────────────────────

b.add_table("Orders", [
    {"name": "OrderID", "data_type": "Int64"},
    {"name": "CustomerID", "data_type": "Int64"},
    {"name": "ProductID", "data_type": "Int64"},
    {"name": "SalespersonID", "data_type": "Int64"},
    {"name": "Quantity", "data_type": "Int64"},
    {"name": "Revenue", "data_type": "Double"},
], rows=[
    {"OrderID": 1,  "CustomerID": 1, "ProductID": 1,  "SalespersonID": 1, "Quantity": 5,   "Revenue": 6495.00},
    {"OrderID": 2,  "CustomerID": 3, "ProductID": 3,  "SalespersonID": 3, "Quantity": 10,  "Revenue": 6990.00},
    {"OrderID": 3,  "CustomerID": 5, "ProductID": 9,  "SalespersonID": 4, "Quantity": 20,  "Revenue": 10980.00},
    {"OrderID": 4,  "CustomerID": 2, "ProductID": 7,  "SalespersonID": 2, "Quantity": 100, "Revenue": 2499.00},
    {"OrderID": 5,  "CustomerID": 4, "ProductID": 6,  "SalespersonID": 3, "Quantity": 15,  "Revenue": 1935.00},
    {"OrderID": 6,  "CustomerID": 1, "ProductID": 2,  "SalespersonID": 1, "Quantity": 50,  "Revenue": 2499.50},
    {"OrderID": 7,  "CustomerID": 6, "ProductID": 4,  "SalespersonID": 2, "Quantity": 8,   "Revenue": 3592.00},
    {"OrderID": 8,  "CustomerID": 8, "ProductID": 8,  "SalespersonID": 6, "Quantity": 25,  "Revenue": 1499.75},
    {"OrderID": 9,  "CustomerID": 3, "ProductID": 5,  "SalespersonID": 3, "Quantity": 30,  "Revenue": 5670.00},
    {"OrderID": 10, "CustomerID": 7, "ProductID": 11, "SalespersonID": 4, "Quantity": 40,  "Revenue": 1599.60},
    {"OrderID": 11, "CustomerID": 10, "ProductID": 1, "SalespersonID": 5, "Quantity": 3,   "Revenue": 3897.00},
    {"OrderID": 12, "CustomerID": 5, "ProductID": 12, "SalespersonID": 4, "Quantity": 60,  "Revenue": 2099.40},
    {"OrderID": 13, "CustomerID": 9, "ProductID": 3,  "SalespersonID": 3, "Quantity": 5,   "Revenue": 3495.00},
    {"OrderID": 14, "CustomerID": 4, "ProductID": 9,  "SalespersonID": 3, "Quantity": 12,  "Revenue": 6588.00},
    {"OrderID": 15, "CustomerID": 2, "ProductID": 10, "SalespersonID": 2, "Quantity": 20,  "Revenue": 1599.80},
    {"OrderID": 16, "CustomerID": 1, "ProductID": 1,  "SalespersonID": 1, "Quantity": 8,   "Revenue": 10392.00},
    {"OrderID": 17, "CustomerID": 8, "ProductID": 6,  "SalespersonID": 6, "Quantity": 18,  "Revenue": 2322.00},
    {"OrderID": 18, "CustomerID": 3, "ProductID": 4,  "SalespersonID": 3, "Quantity": 15,  "Revenue": 6735.00},
    {"OrderID": 19, "CustomerID": 7, "ProductID": 7,  "SalespersonID": 4, "Quantity": 200, "Revenue": 4998.00},
    {"OrderID": 20, "CustomerID": 6, "ProductID": 2,  "SalespersonID": 2, "Quantity": 100, "Revenue": 4999.00},
    {"OrderID": 21, "CustomerID": 5, "ProductID": 1,  "SalespersonID": 4, "Quantity": 10,  "Revenue": 12990.00},
    {"OrderID": 22, "CustomerID": 9, "ProductID": 8,  "SalespersonID": 3, "Quantity": 50,  "Revenue": 2999.50},
    {"OrderID": 23, "CustomerID": 4, "ProductID": 5,  "SalespersonID": 3, "Quantity": 25,  "Revenue": 4725.00},
    {"OrderID": 24, "CustomerID": 10, "ProductID": 12, "SalespersonID": 5, "Quantity": 80, "Revenue": 2799.20},
    {"OrderID": 25, "CustomerID": 1, "ProductID": 9,  "SalespersonID": 1, "Quantity": 15,  "Revenue": 8235.00},
])

# ── Relationships (star schema) ─────────────────────────────────────

b.add_relationship("Orders", "CustomerID", "Customers", "CustomerID")
b.add_relationship("Orders", "ProductID", "Products", "ProductID")
b.add_relationship("Orders", "SalespersonID", "Salespeople", "SalespersonID")
b.add_relationship("Customers", "RegionID", "Regions", "RegionID")
b.add_relationship("Products", "CategoryID", "Categories", "CategoryID")

# ── Measures ─────────────────────────────────────────────────────────

b.add_measure("Orders", "Total Revenue", "SUM(Orders[Revenue])")
b.add_measure("Orders", "Total Quantity", "SUM(Orders[Quantity])")
b.add_measure("Orders", "Order Count", "COUNTROWS(Orders)")
b.add_measure("Orders", "Avg Order Value",
              "DIVIDE(SUM(Orders[Revenue]), COUNTROWS(Orders), 0)")

# ── Page 1: Executive Overview ───────────────────────────────────────

b.add_page("Executive Overview", [
    {"name": "revenue_card", "type": "card", "x": 20, "y": 20, "width": 280, "height": 100,
     "config": {"measure": "Total Revenue"}},
    {"name": "orders_card", "type": "card", "x": 320, "y": 20, "width": 280, "height": 100,
     "config": {"measure": "Order Count"}},
    {"name": "qty_card", "type": "card", "x": 620, "y": 20, "width": 280, "height": 100,
     "config": {"measure": "Total Quantity"}},
    {"name": "aov_card", "type": "card", "x": 920, "y": 20, "width": 280, "height": 100,
     "config": {"measure": "Avg Order Value"}},
    {"name": "region_bar", "type": "clusteredBarChart", "x": 20, "y": 140, "width": 580, "height": 320,
     "config": {"category": {"table": "Regions", "column": "RegionName"},
                "measure": "Total Revenue"}},
    {"name": "category_pie", "type": "pieChart", "x": 620, "y": 140, "width": 580, "height": 320,
     "config": {"category": {"table": "Categories", "column": "CategoryName"},
                "measure": "Total Revenue"}},
    {"name": "detail_table", "type": "table", "x": 20, "y": 480, "width": 1180, "height": 280,
     "config": {"columns": [
         {"table": "Customers", "column": "CustomerName"},
         {"table": "Customers", "column": "Segment"},
         {"table": "Regions", "column": "RegionName"},
         {"measure": "Total Revenue"},
         {"measure": "Order Count"},
         {"measure": "Avg Order Value"},
     ]}},
])

# ── Page 2: Product Performance ──────────────────────────────────────

b.add_page("Product Performance", [
    {"name": "cat_slicer", "type": "slicer", "x": 20, "y": 20, "width": 250, "height": 100,
     "config": {"column": {"table": "Categories", "column": "CategoryName"}}},
    {"name": "region_slicer", "type": "slicer", "x": 290, "y": 20, "width": 250, "height": 100,
     "config": {"column": {"table": "Regions", "column": "RegionName"}}},
    {"name": "filtered_revenue", "type": "card", "x": 560, "y": 20, "width": 300, "height": 100,
     "config": {"measure": "Total Revenue"}},
    {"name": "product_table", "type": "table", "x": 20, "y": 140, "width": 700, "height": 400,
     "config": {"columns": [
         {"table": "Products", "column": "ProductName"},
         {"table": "Categories", "column": "CategoryName"},
         {"table": "Products", "column": "UnitPrice"},
         {"measure": "Total Quantity"},
         {"measure": "Total Revenue"},
     ]}},
    {"name": "product_pie", "type": "pieChart", "x": 740, "y": 140, "width": 460, "height": 400,
     "config": {"category": {"table": "Products", "column": "ProductName"},
                "measure": "Total Revenue"}},
])

# ── Page 3: Sales Team ───────────────────────────────────────────────

b.add_page("Sales Team", [
    {"name": "sales_bar", "type": "clusteredBarChart", "x": 20, "y": 20, "width": 580, "height": 350,
     "config": {"category": {"table": "Salespeople", "column": "FullName"},
                "measure": "Total Revenue"}},
    {"name": "sales_pie", "type": "pieChart", "x": 620, "y": 20, "width": 580, "height": 350,
     "config": {"category": {"table": "Salespeople", "column": "FullName"},
                "measure": "Order Count"}},
    {"name": "sales_detail", "type": "table", "x": 20, "y": 390, "width": 1180, "height": 370,
     "config": {"columns": [
         {"table": "Salespeople", "column": "FullName"},
         {"table": "Salespeople", "column": "Title"},
         {"table": "Regions", "column": "RegionName"},
         {"table": "Salespeople", "column": "CommissionRate"},
         {"measure": "Total Revenue"},
         {"measure": "Order Count"},
         {"measure": "Avg Order Value"},
     ]}},
])

# ── Build ────────────────────────────────────────────────────────────

out = os.path.join(os.path.dirname(__file__), "showcase_northwind.pbix")
b.save(out)
print(f"Created: {out}")
print("  6 tables · 36 columns · 5 relationships")
print("  4 measures · 3 pages · 14 visuals")
print("  25 orders · 10 customers · 12 products · 6 salespeople · 5 regions · 6 categories")
print()
print("Open in Power BI Desktop to explore.")
