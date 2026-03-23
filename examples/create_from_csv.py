"""Create a Power BI report from CSV files.

Reads two CSV files, builds a multi-table PBIX with relationships
and measures, then saves it. Click Refresh in PBI Desktop to
re-import from the CSVs.
"""
import csv
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder


def read_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def main():
    # Create sample CSVs if they don't exist
    os.makedirs("data", exist_ok=True)
    if not os.path.exists("data/products.csv"):
        with open("data/products.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["ProductID", "Product", "Price"])
            w.writeheader()
            w.writerows([
                {"ProductID": 1, "Product": "Laptop", "Price": 12999},
                {"ProductID": 2, "Product": "Monitor", "Price": 4599},
                {"ProductID": 3, "Product": "Keyboard", "Price": 899},
            ])
    if not os.path.exists("data/orders.csv"):
        with open("data/orders.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["OrderID", "ProductID", "Qty"])
            w.writeheader()
            w.writerows([
                {"OrderID": 1, "ProductID": 1, "Qty": 5},
                {"OrderID": 2, "ProductID": 2, "Qty": 3},
                {"OrderID": 3, "ProductID": 3, "Qty": 20},
                {"OrderID": 4, "ProductID": 1, "Qty": 10},
            ])

    # Read CSVs
    products = read_csv("data/products.csv")
    orders = read_csv("data/orders.csv")
    for p in products:
        p["ProductID"] = int(p["ProductID"])
        p["Price"] = float(p["Price"])
    for o in orders:
        o["OrderID"] = int(o["OrderID"])
        o["ProductID"] = int(o["ProductID"])
        o["Qty"] = int(o["Qty"])

    # Build report
    builder = PBIXBuilder()

    builder.add_table("Products", [
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "Product", "data_type": "String"},
        {"name": "Price", "data_type": "Double"},
    ], rows=products,
       source_csv=os.path.abspath("data/products.csv"))

    builder.add_table("Orders", [
        {"name": "OrderID", "data_type": "Int64"},
        {"name": "ProductID", "data_type": "Int64"},
        {"name": "Qty", "data_type": "Int64"},
    ], rows=orders,
       source_csv=os.path.abspath("data/orders.csv"))

    builder.add_relationship("Orders", "ProductID", "Products", "ProductID")
    builder.add_measure("Orders", "Total Qty", "SUM(Orders[Qty])")
    builder.add_measure("Orders", "Total Revenue",
                        "SUMX(Orders, Orders[Qty] * RELATED(Products[Price]))")

    path = builder.save("csv_report.pbix")
    print(f"Created {path}")
    print("Open in Power BI Desktop. Edit the CSVs and click Refresh to update.")


if __name__ == "__main__":
    main()
