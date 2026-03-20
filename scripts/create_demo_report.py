#!/usr/bin/env python3
"""Create a cool demo PBIX report with real data, measures, and visuals.

This demonstrates PBIXBuilder's from-scratch capabilities:
- Multiple tables with relationships
- Realistic sales data
- DAX measures (aggregations, YoY, percentages)
- Multiple report pages with visuals

Usage:
    python scripts/create_demo_report.py [output_path]
"""
import os
import sys
import random
from datetime import date, timedelta

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from pbix_mcp.builder import PBIXBuilder


def create_demo_report(output_path: str = "demo_report.pbix") -> str:
    """Build a complete demo PBIX with sales data, measures, and layout."""
    builder = PBIXBuilder(name="Sales Analytics Demo")

    # =========================================================================
    # Table: Products
    # =========================================================================
    products = [
        {"ProductID": "P001", "Name": "Laptop Pro 15", "Category": "Electronics", "SubCategory": "Laptops", "UnitPrice": 1299},
        {"ProductID": "P002", "Name": "Wireless Mouse", "Category": "Electronics", "SubCategory": "Accessories", "UnitPrice": 29},
        {"ProductID": "P003", "Name": "USB-C Hub", "Category": "Electronics", "SubCategory": "Accessories", "UnitPrice": 49},
        {"ProductID": "P004", "Name": "Standing Desk", "Category": "Furniture", "SubCategory": "Desks", "UnitPrice": 599},
        {"ProductID": "P005", "Name": "Ergonomic Chair", "Category": "Furniture", "SubCategory": "Chairs", "UnitPrice": 449},
        {"ProductID": "P006", "Name": "Monitor 27\"", "Category": "Electronics", "SubCategory": "Displays", "UnitPrice": 399},
        {"ProductID": "P007", "Name": "Keyboard Mech", "Category": "Electronics", "SubCategory": "Accessories", "UnitPrice": 89},
        {"ProductID": "P008", "Name": "Desk Lamp", "Category": "Furniture", "SubCategory": "Lighting", "UnitPrice": 35},
        {"ProductID": "P009", "Name": "Webcam HD", "Category": "Electronics", "SubCategory": "Accessories", "UnitPrice": 79},
        {"ProductID": "P010", "Name": "Bookshelf", "Category": "Furniture", "SubCategory": "Storage", "UnitPrice": 199},
    ]
    builder.add_table("Products", [
        {"name": "ProductID", "data_type": "String"},
        {"name": "Name", "data_type": "String"},
        {"name": "Category", "data_type": "String"},
        {"name": "SubCategory", "data_type": "String"},
        {"name": "UnitPrice", "data_type": "Int64"},
    ], products)

    # =========================================================================
    # Table: Customers
    # =========================================================================
    regions = ["North", "South", "East", "West"]
    segments = ["Consumer", "Corporate", "Government"]
    customers = []
    for i in range(1, 51):
        customers.append({
            "CustomerID": f"C{i:03d}",
            "Name": f"Customer {i}",
            "Region": regions[(i - 1) % 4],
            "Segment": segments[(i - 1) % 3],
        })
    builder.add_table("Customers", [
        {"name": "CustomerID", "data_type": "String"},
        {"name": "Name", "data_type": "String"},
        {"name": "Region", "data_type": "String"},
        {"name": "Segment", "data_type": "String"},
    ], customers)

    # =========================================================================
    # Table: Calendar
    # =========================================================================
    cal_rows = []
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    start = date(2023, 1, 1)
    end = date(2024, 12, 31)
    d = start
    while d <= end:
        cal_rows.append({
            "Date": d.isoformat(),
            "Year": d.year,
            "Month": d.month,
            "MonthName": months[d.month - 1],
            "Quarter": f"Q{(d.month - 1) // 3 + 1}",
            "DayOfWeek": d.strftime("%A"),
        })
        d += timedelta(days=1)
    builder.add_table("Calendar", [
        {"name": "Date", "data_type": "String"},
        {"name": "Year", "data_type": "Int64"},
        {"name": "Month", "data_type": "Int64"},
        {"name": "MonthName", "data_type": "String"},
        {"name": "Quarter", "data_type": "String"},
        {"name": "DayOfWeek", "data_type": "String"},
    ], cal_rows)

    # =========================================================================
    # Table: Sales (fact table — 500 rows)
    # =========================================================================
    random.seed(42)  # Reproducible
    sales = []
    product_ids = [p["ProductID"] for p in products]
    product_prices = {p["ProductID"]: p["UnitPrice"] for p in products}
    customer_ids = [c["CustomerID"] for c in customers]

    for i in range(1, 501):
        pid = random.choice(product_ids)
        cid = random.choice(customer_ids)
        qty = random.randint(1, 5)
        price = product_prices[pid]
        discount = random.choice([0, 0, 0, 5, 10, 15, 20])
        sale_date = start + timedelta(days=random.randint(0, 730))
        amount = round(qty * price * (1 - discount / 100), 2)
        cost = round(amount * random.uniform(0.4, 0.7), 2)

        sales.append({
            "OrderID": f"ORD-{i:04d}",
            "Date": sale_date.isoformat(),
            "ProductID": pid,
            "CustomerID": cid,
            "Quantity": qty,
            "Amount": int(amount),
            "Cost": int(cost),
            "Discount": discount,
        })

    builder.add_table("Sales", [
        {"name": "OrderID", "data_type": "String"},
        {"name": "Date", "data_type": "String"},
        {"name": "ProductID", "data_type": "String"},
        {"name": "CustomerID", "data_type": "String"},
        {"name": "Quantity", "data_type": "Int64"},
        {"name": "Amount", "data_type": "Int64"},
        {"name": "Cost", "data_type": "Int64"},
        {"name": "Discount", "data_type": "Int64"},
    ], sales)

    # =========================================================================
    # Relationships
    # =========================================================================
    builder.add_relationship("Sales", "ProductID", "Products", "ProductID")
    builder.add_relationship("Sales", "CustomerID", "Customers", "CustomerID")
    builder.add_relationship("Sales", "Date", "Calendar", "Date")

    # =========================================================================
    # DAX Measures
    # =========================================================================
    measures = [
        ("Sales", "Total Revenue", "SUM(Sales[Amount])"),
        ("Sales", "Total Cost", "SUM(Sales[Cost])"),
        ("Sales", "Total Profit", "[Total Revenue] - [Total Cost]"),
        ("Sales", "Profit Margin", "DIVIDE([Total Profit], [Total Revenue], 0)"),
        ("Sales", "Order Count", "COUNTROWS(Sales)"),
        ("Sales", "Avg Order Value", "DIVIDE([Total Revenue], [Order Count], 0)"),
        ("Sales", "Total Quantity", "SUM(Sales[Quantity])"),
        ("Sales", "Avg Discount", "AVERAGE(Sales[Discount])"),
        ("Sales", "Revenue per Unit", "DIVIDE([Total Revenue], [Total Quantity], 0)"),
        ("Sales", "Customer Count", "DISTINCTCOUNT(Sales[CustomerID])"),
    ]
    for table, name, expr in measures:
        builder.add_measure(table, name, expr)

    # =========================================================================
    # Report Pages
    # =========================================================================
    # Page 1: Overview Dashboard
    builder.add_page("Overview", [
        # Title
        {
            "type": "textbox",
            "x": 20, "y": 10, "width": 600, "height": 50,
            "config": {"text": "Sales Analytics Dashboard"},
        },
        # KPI Cards
        {
            "type": "card",
            "x": 20, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Total Revenue"},
        },
        {
            "type": "card",
            "x": 240, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Total Profit"},
        },
        {
            "type": "card",
            "x": 460, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Order Count"},
        },
        {
            "type": "card",
            "x": 680, "y": 70, "width": 200, "height": 100,
            "config": {"measure": "Profit Margin"},
        },
        # Bar chart: Revenue by Category
        {
            "type": "clusteredBarChart",
            "x": 20, "y": 190, "width": 420, "height": 300,
            "config": {
                "category": {"table": "Products", "column": "Category"},
                "measure": "Total Revenue",
            },
        },
        # Table: Top products
        {
            "type": "tableEx",
            "x": 460, "y": 190, "width": 420, "height": 300,
            "config": {
                "columns": [
                    {"table": "Products", "column": "Name"},
                    {"measure": "Total Revenue"},
                    {"measure": "Total Profit"},
                ],
            },
        },
        # Slicer: Year
        {
            "type": "slicer",
            "x": 900, "y": 70, "width": 150, "height": 100,
            "config": {"column": {"table": "Calendar", "column": "Year"}},
        },
        # Button: Navigate to Details
        {
            "type": "actionButton",
            "x": 900, "y": 190, "width": 150, "height": 40,
            "config": {"text": "View Details →"},
        },
    ])

    # Page 2: Regional Analysis
    builder.add_page("Regional Analysis", [
        {
            "type": "textbox",
            "x": 20, "y": 10, "width": 600, "height": 50,
            "config": {"text": "Regional Performance"},
        },
        {
            "type": "clusteredBarChart",
            "x": 20, "y": 70, "width": 500, "height": 350,
            "config": {
                "category": {"table": "Customers", "column": "Region"},
                "measure": "Total Revenue",
            },
        },
        {
            "type": "donutChart",
            "x": 540, "y": 70, "width": 400, "height": 350,
            "config": {
                "category": {"table": "Customers", "column": "Segment"},
                "measure": "Order Count",
            },
        },
    ])

    # =========================================================================
    # Save
    # =========================================================================
    builder.save(output_path)
    return output_path


if __name__ == "__main__":
    out = sys.argv[1] if len(sys.argv) > 1 else "demo_report.pbix"
    path = create_demo_report(out)
    size = os.path.getsize(path)
    print(f"Created: {path} ({size:,} bytes)")
    print(f"\nContents:")
    print(f"  - 4 tables (Products, Customers, Calendar, Sales)")
    print(f"  - 500 sales orders across 2 years")
    print(f"  - 10 DAX measures")
    print(f"  - 3 relationships")
    print(f"  - 2 pages (Overview Dashboard, Regional Analysis)")
    print(f"  - Visuals: cards, bar chart, table, slicer, button, donut chart")
    print(f"\nOpen in Power BI Desktop to view.")
