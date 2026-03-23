"""Create a Power BI report demonstrating all 6 supported data types.

Verifies: String, Int64, Double, DateTime, Decimal, Boolean
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder


def main():
    builder = PBIXBuilder()
    builder.add_table("AllTypes", [
        {"name": "ID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "Price", "data_type": "Double"},
        {"name": "OrderDate", "data_type": "DateTime"},
        {"name": "Amount", "data_type": "Decimal"},
        {"name": "IsActive", "data_type": "Boolean"},
    ], rows=[
        {"ID": 1, "Name": "Alpha", "Price": 29.99,
         "OrderDate": "2024-01-15", "Amount": 1500, "IsActive": True},
        {"ID": 2, "Name": "Beta", "Price": 49.99,
         "OrderDate": "2024-03-20", "Amount": 2750, "IsActive": False},
        {"ID": 3, "Name": "Gamma", "Price": 14.99,
         "OrderDate": "2024-06-10", "Amount": 500, "IsActive": True},
    ])

    builder.add_measure("AllTypes", "Total Amount", "SUM(AllTypes[Amount])")
    builder.add_measure("AllTypes", "Active Count",
                        "CALCULATE(COUNTROWS(AllTypes), AllTypes[IsActive] = TRUE())")

    path = builder.save("all_types_report.pbix")
    print(f"Created {path}")
    print("All 6 data types: String, Int64, Double, DateTime, Decimal, Boolean")


if __name__ == "__main__":
    main()
