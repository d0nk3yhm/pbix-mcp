"""Create a star schema Power BI report with multiple relationships.

Demonstrates: 3 dimension tables + 1 fact table + 3 relationships
+ cross-table measures using RELATED().
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder


def main():
    builder = PBIXBuilder()

    # Dimension: Customers
    builder.add_table("Customers", [
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "Segment", "data_type": "String"},
    ], rows=[
        {"CustomerID": 1, "Name": "Acme Corp", "Segment": "Enterprise"},
        {"CustomerID": 2, "Name": "Globex Inc", "Segment": "SMB"},
        {"CustomerID": 3, "Name": "Initech", "Segment": "Enterprise"},
        {"CustomerID": 4, "Name": "Umbrella Corp", "Segment": "SMB"},
    ])

    # Dimension: Products
    builder.add_table("Products", [
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "Product", "data_type": "String"},
        {"name": "Category", "data_type": "String"},
        {"name": "UnitPrice", "data_type": "Double"},
    ], rows=[
        {"ProductID": 1, "Product": "Laptop", "Category": "Hardware", "UnitPrice": 12999},
        {"ProductID": 2, "Product": "Monitor", "Category": "Hardware", "UnitPrice": 4599},
        {"ProductID": 3, "Product": "License", "Category": "Software", "UnitPrice": 2999},
    ])

    # Dimension: Dates
    builder.add_table("Dates", [
        {"name": "DateKey", "data_type": "Int64"},
        {"name": "Date", "data_type": "DateTime"},
        {"name": "Month", "data_type": "String"},
        {"name": "Quarter", "data_type": "String"},
    ], rows=[
        {"DateKey": 1, "Date": "2024-01-15", "Month": "January", "Quarter": "Q1"},
        {"DateKey": 2, "Date": "2024-04-01", "Month": "April", "Quarter": "Q2"},
        {"DateKey": 3, "Date": "2024-07-10", "Month": "July", "Quarter": "Q3"},
        {"DateKey": 4, "Date": "2024-10-20", "Month": "October", "Quarter": "Q4"},
    ])

    # Fact: Sales
    builder.add_table("Sales", [
        {"name": "SaleID", "data_type": "Int64"},
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "DateKey", "data_type": "Int64"},
        {"name": "Qty", "data_type": "Int64"},
    ], rows=[
        {"SaleID": 1, "CustomerID": 1, "ProductID": 1, "DateKey": 1, "Qty": 2},
        {"SaleID": 2, "CustomerID": 2, "ProductID": 3, "DateKey": 1, "Qty": 10},
        {"SaleID": 3, "CustomerID": 1, "ProductID": 2, "DateKey": 2, "Qty": 5},
        {"SaleID": 4, "CustomerID": 3, "ProductID": 1, "DateKey": 2, "Qty": 1},
        {"SaleID": 5, "CustomerID": 4, "ProductID": 3, "DateKey": 3, "Qty": 20},
        {"SaleID": 6, "CustomerID": 2, "ProductID": 2, "DateKey": 3, "Qty": 3},
        {"SaleID": 7, "CustomerID": 1, "ProductID": 3, "DateKey": 4, "Qty": 15},
        {"SaleID": 8, "CustomerID": 3, "ProductID": 1, "DateKey": 4, "Qty": 4},
    ])

    # Relationships (star schema: fact → dimensions)
    builder.add_relationship("Sales", "CustomerID", "Customers", "CustomerID")
    builder.add_relationship("Sales", "ProductID", "Products", "ProductID")
    builder.add_relationship("Sales", "DateKey", "Dates", "DateKey")

    # Measures
    builder.add_measure("Sales", "Total Qty", "SUM(Sales[Qty])")
    builder.add_measure("Sales", "Total Revenue",
                        "SUMX(Sales, Sales[Qty] * RELATED(Products[UnitPrice]))")
    builder.add_measure("Sales", "Order Count", "COUNTROWS(Sales)")
    builder.add_measure("Sales", "Avg Order Size", "AVERAGE(Sales[Qty])")

    path = builder.save("star_schema_report.pbix")
    print(f"Created {path}")
    print("Star schema: Customers + Products + Dates → Sales")
    print("3 relationships, 4 measures including cross-table RELATED()")


if __name__ == "__main__":
    main()
