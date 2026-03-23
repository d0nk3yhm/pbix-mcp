"""Create a DirectQuery Power BI report connected to SQL Server.

The report queries the database live — no refresh needed.
INSERT/UPDATE/DELETE in the database is reflected instantly.

Prerequisites:
  - SQL Server (LocalDB, Express, or full) running
  - A database with tables to connect to
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder


def main():
    # Connection details
    server = r"(localdb)\MSSQLLocalDB"  # Change for your server
    database = "TestDQ"

    # Initial data snapshot (required — embedded in PBIX)
    customers = [
        {"CustomerID": 1, "Name": "Acme Corp", "City": "Oslo"},
        {"CustomerID": 2, "Name": "Globex Inc", "City": "Bergen"},
    ]
    orders = [
        {"OrderID": 1, "CustomerID": 1, "Qty": 5},
        {"OrderID": 2, "CustomerID": 2, "Qty": 3},
    ]

    builder = PBIXBuilder()

    builder.add_table("Customers", [
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "City", "data_type": "String"},
    ], rows=customers,
       mode="directquery",
       source_db={"type": "sqlserver", "server": server,
                   "database": database, "table": "Customers"})

    builder.add_table("Orders", [
        {"name": "OrderID", "data_type": "Int64"},
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "Qty", "data_type": "Int64"},
    ], rows=orders,
       mode="directquery",
       source_db={"type": "sqlserver", "server": server,
                   "database": database, "table": "Orders"})

    builder.add_relationship("Orders", "CustomerID", "Customers", "CustomerID")
    builder.add_measure("Orders", "Total Qty", "SUM(Orders[Qty])")

    path = builder.save("directquery_report.pbix")
    print(f"Created {path}")
    print(f"Connected to {server}/{database}")
    print("Open in Power BI Desktop — data queries the database live!")


if __name__ == "__main__":
    main()
