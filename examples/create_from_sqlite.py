"""Create a Power BI report connected to a SQLite database.

Data is imported at build time. Click Refresh in PBI Desktop to
re-import from SQLite (requires SQLite3 ODBC Driver).

Install driver: http://www.ch-werner.de/sqliteodbc/ (64-bit version)
"""
import sqlite3
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from pbix_mcp.builder import PBIXBuilder


def main():
    # Create sample SQLite database
    db_path = os.path.abspath("data/sample.db")
    os.makedirs("data", exist_ok=True)

    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS customers")
    c.execute("DROP TABLE IF EXISTS orders")
    c.execute("""CREATE TABLE customers (
        CustomerID INTEGER PRIMARY KEY, Name TEXT, City TEXT)""")
    c.executemany("INSERT INTO customers VALUES (?,?,?)", [
        (1, "Acme Corp", "Oslo"),
        (2, "Globex Inc", "Bergen"),
        (3, "Initech", "Stockholm"),
    ])
    c.execute("""CREATE TABLE orders (
        OrderID INTEGER PRIMARY KEY, CustomerID INTEGER, Qty INTEGER)""")
    c.executemany("INSERT INTO orders VALUES (?,?,?)", [
        (1, 1, 5), (2, 2, 3), (3, 1, 10), (4, 3, 7),
    ])
    conn.commit()

    # Read data for initial snapshot
    customers = [dict(zip(["CustomerID", "Name", "City"], r))
                 for r in conn.execute("SELECT * FROM customers")]
    orders = [dict(zip(["OrderID", "CustomerID", "Qty"], r))
              for r in conn.execute("SELECT * FROM orders")]
    conn.close()

    for c in customers:
        c["CustomerID"] = int(c["CustomerID"])
    for o in orders:
        o["OrderID"] = int(o["OrderID"])
        o["CustomerID"] = int(o["CustomerID"])
        o["Qty"] = int(o["Qty"])

    # Build report
    builder = PBIXBuilder()
    builder.add_table("Customers", [
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "City", "data_type": "String"},
    ], rows=customers,
       source_db={"type": "sqlite", "path": db_path, "table": "customers"})

    builder.add_table("Orders", [
        {"name": "OrderID", "data_type": "Int64"},
        {"name": "CustomerID", "data_type": "Int64"},
        {"name": "Qty", "data_type": "Int64"},
    ], rows=orders,
       source_db={"type": "sqlite", "path": db_path, "table": "orders"})

    builder.add_relationship("Orders", "CustomerID", "Customers", "CustomerID")
    builder.add_measure("Orders", "Total Qty", "SUM(Orders[Qty])")

    path = builder.save("sqlite_report.pbix")
    print(f"Created {path}")
    print(f"SQLite database: {db_path}")
    print("Open in PBI Desktop. Edit SQLite → click Refresh to update.")


if __name__ == "__main__":
    main()
