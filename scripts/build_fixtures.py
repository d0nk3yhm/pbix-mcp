"""Build real PBIX test fixtures using our own compress/ABF tools."""

import json
import os
import sqlite3
import sys
import tempfile
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


def create_metadata_sqlite():
    """Create a minimal SQLite metadata database."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("CREATE TABLE [Model] (ID INTEGER PRIMARY KEY, Name TEXT)")
    c.execute("INSERT INTO [Model] VALUES (1, 'Model')")

    c.execute("""CREATE TABLE [Table] (
        ID INTEGER PRIMARY KEY, ModelID INTEGER, Name TEXT,
        IsHidden INTEGER DEFAULT 0, Description TEXT DEFAULT ''
    )""")
    c.execute("INSERT INTO [Table] VALUES (1, 1, 'Sales', 0, 'Sales fact table')")
    c.execute("INSERT INTO [Table] VALUES (2, 1, 'Products', 0, 'Product dimension')")
    c.execute("INSERT INTO [Table] VALUES (3, 1, '# Measures', 1, 'Measures')")

    c.execute("""CREATE TABLE [Column] (
        ID INTEGER PRIMARY KEY, TableID INTEGER,
        ExplicitName TEXT, InferredName TEXT,
        Expression TEXT, Type INTEGER DEFAULT 1,
        IsHidden INTEGER DEFAULT 0, IsKey INTEGER DEFAULT 0
    )""")
    c.execute("INSERT INTO [Column] VALUES (1, 1, 'Product', NULL, NULL, 1, 0, 0)")
    c.execute("INSERT INTO [Column] VALUES (2, 1, 'Amount', NULL, NULL, 1, 0, 0)")
    c.execute("INSERT INTO [Column] VALUES (3, 1, 'Quantity', NULL, NULL, 1, 0, 0)")
    c.execute("INSERT INTO [Column] VALUES (4, 1, 'Date', NULL, NULL, 1, 0, 0)")
    c.execute("INSERT INTO [Column] VALUES (5, 2, 'Product', NULL, NULL, 1, 0, 1)")
    c.execute("INSERT INTO [Column] VALUES (6, 2, 'Category', NULL, NULL, 1, 0, 0)")

    c.execute("""CREATE TABLE [Measure] (
        ID INTEGER PRIMARY KEY, TableID INTEGER,
        Name TEXT, Expression TEXT, Description TEXT DEFAULT ''
    )""")
    c.execute("INSERT INTO [Measure] VALUES (1, 3, 'Total Sales', 'SUM(Sales[Amount])', '')")
    c.execute("INSERT INTO [Measure] VALUES (2, 3, 'Avg Price', 'AVERAGE(Sales[Amount])', '')")
    c.execute("INSERT INTO [Measure] VALUES (3, 3, 'Item Count', 'COUNTROWS(Sales)', '')")
    c.execute("INSERT INTO [Measure] VALUES (4, 3, 'Unique Products', 'DISTINCTCOUNT(Sales[Product])', '')")

    c.execute("""CREATE TABLE [Relationship] (
        ID INTEGER PRIMARY KEY, ModelID INTEGER,
        FromTableID INTEGER, FromColumnID INTEGER, FromCardinality INTEGER,
        ToTableID INTEGER, ToColumnID INTEGER, ToCardinality INTEGER,
        IsActive INTEGER DEFAULT 1, CrossFilteringBehavior INTEGER DEFAULT 1,
        Name TEXT DEFAULT ''
    )""")
    c.execute("INSERT INTO [Relationship] VALUES (1, 1, 1, 1, 2, 2, 5, 1, 1, 1, '')")

    c.execute("""CREATE TABLE [Partition] (
        ID INTEGER PRIMARY KEY, TableID INTEGER, Name TEXT,
        Type INTEGER DEFAULT 4, QueryDefinition TEXT
    )""")
    c.execute("INSERT INTO [Partition] VALUES (1, 1, 'Sales', 4, NULL)")
    c.execute("INSERT INTO [Partition] VALUES (2, 2, 'Products', 4, NULL)")

    conn.commit()
    conn.close()

    with open(db_path, "rb") as f:
        data = f.read()
    os.unlink(db_path)
    return data


def build_basic_measures_pbix(base_pbix_path, output_path):
    """Build a real PBIX with custom metadata using GeoSales as DataModel base."""
    from pbix_mcp.formats.abf_rebuild import (
        list_abf_files,
        read_metadata_sqlite,
        rebuild_abf_with_replacement,
    )
    from pbix_mcp.formats.datamodel_roundtrip import (
        compress_datamodel,
        decompress_datamodel,
    )

    # Read base DataModel
    with zipfile.ZipFile(base_pbix_path, "r") as zf:
        dm_data = zf.read("DataModel")

    print(f"Base DataModel: {len(dm_data):,} bytes")
    abf = decompress_datamodel(dm_data)
    print(f"ABF decompressed: {len(abf):,} bytes")

    # Replace metadata with our custom SQLite
    sqlite_bytes = create_metadata_sqlite()
    print(f"Custom metadata: {len(sqlite_bytes):,} bytes")

    # Find the metadata file path in the ABF
    file_log = list_abf_files(abf)
    meta_path = None
    for entry in file_log:
        name = entry.get("path", "") if isinstance(entry, dict) else str(entry)
        if "metadata" in name.lower() and "sqlite" in name.lower():
            meta_path = name
            break
    if not meta_path:
        # Default path
        meta_path = "metadata.sqlitedb"
    print(f"Replacing: {meta_path}")
    new_abf = rebuild_abf_with_replacement(abf, {meta_path: sqlite_bytes})
    print(f"New ABF: {len(new_abf):,} bytes")

    # Recompress
    new_dm = compress_datamodel(new_abf)
    print(f"New DataModel: {len(new_dm):,} bytes")

    # Build layout
    layout = {
        "id": 0,
        "sections": [
            {
                "displayName": "Overview",
                "name": "ReportSection1",
                "ordinal": 0,
                "visualContainers": [
                    {
                        "x": 20, "y": 20, "width": 300, "height": 200,
                        "config": json.dumps({
                            "name": "card_total_sales",
                            "singleVisual": {
                                "visualType": "card",
                                "projections": {"Values": [{"queryRef": "# Measures.Total Sales"}]},
                            },
                        }),
                    },
                    {
                        "x": 340, "y": 20, "width": 400, "height": 300,
                        "config": json.dumps({
                            "name": "chart_by_product",
                            "singleVisual": {
                                "visualType": "clusteredBarChart",
                                "projections": {
                                    "Category": [{"queryRef": "Products.Product"}],
                                    "Y": [{"queryRef": "# Measures.Total Sales"}],
                                },
                            },
                        }),
                    },
                ],
            }
        ],
    }

    # Pack into PBIX
    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        layout_bytes = json.dumps(layout, ensure_ascii=False).encode("utf-16-le")
        zf.writestr("Report/Layout", layout_bytes)
        zf.writestr("DataModel", new_dm)
        zf.writestr("Settings", json.dumps({"version": "5.0"}))
        zf.writestr("Metadata", json.dumps({"version": 3}))
        zf.writestr(
            "[Content_Types].xml",
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"></Types>',
        )

    size = os.path.getsize(output_path)
    print(f"Created {output_path} ({size:,} bytes)")

    # Verify round-trip
    with zipfile.ZipFile(output_path, "r") as zf:
        verify_dm = zf.read("DataModel")
    verify_abf = decompress_datamodel(verify_dm)
    verify_meta = read_metadata_sqlite(verify_abf)

    tmp = tempfile.mktemp(suffix=".db")
    with open(tmp, "wb") as f:
        f.write(verify_meta)
    conn = sqlite3.connect(tmp)
    tables = conn.execute("SELECT Name FROM [Table] WHERE ModelID=1").fetchall()
    measures = conn.execute("SELECT Name FROM [Measure]").fetchall()
    conn.close()
    os.unlink(tmp)

    print(f"Verification: {len(tables)} tables, {len(measures)} measures")
    for t in tables:
        print(f"  Table: {t[0]}")
    for m in measures:
        print(f"  Measure: {m[0]}")

    return True


if __name__ == "__main__":
    base = os.path.join(
        os.path.dirname(__file__), "..", "..", "OpenBI", "test_samples",
        "GeoSales_Dashboard.pbix",
    )
    if not os.path.exists(base):
        print(f"ERROR: Base PBIX not found at {base}")
        sys.exit(1)

    output = os.path.join(
        os.path.dirname(__file__), "..", "tests", "fixtures", "pbix",
        "basic_measures.pbix",
    )
    os.makedirs(os.path.dirname(output), exist_ok=True)
    build_basic_measures_pbix(base, output)
