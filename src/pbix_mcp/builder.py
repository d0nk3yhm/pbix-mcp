"""
PBIX Builder — create valid Power BI .pbix files from scratch.

No existing PBIX file needed. Builds every layer:
  1. SQLite metadata (tables, columns, measures, relationships)
  2. ABF archive containing the metadata
  3. XPress9-compressed DataModel
  4. Report layout JSON
  5. ZIP packaging with all required entries

Usage:
    from pbix_mcp.builder import PBIXBuilder

    builder = PBIXBuilder()
    builder.add_table("Sales", [
        {"name": "Product", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
    ])
    builder.add_measure("Sales", "Total Sales", "SUM(Sales[Amount])")
    builder.add_relationship("Sales", "Product", "Products", "Product")
    builder.save("output.pbix")
"""

import json
import os
import sqlite3
import tempfile
import zipfile


class PBIXBuilder:
    """Build a valid PBIX file from scratch."""

    def __init__(self, name: str = "Model"):
        self._name = name
        self._tables: list[dict] = []
        self._measures: list[dict] = []
        self._relationships: list[dict] = []
        self._pages: list[dict] = []

    def add_table(
        self,
        name: str,
        columns: list[dict],
        hidden: bool = False,
    ) -> "PBIXBuilder":
        """Add a table definition.

        Args:
            name: Table name
            columns: List of {"name": str, "data_type": str} dicts.
                     data_type: "String", "Int64", "Double", "DateTime", "Boolean"
            hidden: Whether the table is hidden (e.g., measure containers)
        """
        self._tables.append({
            "name": name,
            "columns": columns,
            "hidden": hidden,
        })
        return self

    def add_measure(
        self,
        table: str,
        name: str,
        expression: str,
        description: str = "",
    ) -> "PBIXBuilder":
        """Add a DAX measure to a table."""
        self._measures.append({
            "table": table,
            "name": name,
            "expression": expression,
            "description": description,
        })
        return self

    def add_relationship(
        self,
        from_table: str,
        from_column: str,
        to_table: str,
        to_column: str,
    ) -> "PBIXBuilder":
        """Add a relationship between two tables."""
        self._relationships.append({
            "from_table": from_table,
            "from_column": from_column,
            "to_table": to_table,
            "to_column": to_column,
        })
        return self

    def add_page(
        self,
        name: str = "Page 1",
        visuals: list[dict] | None = None,
    ) -> "PBIXBuilder":
        """Add a report page with optional visuals."""
        self._pages.append({
            "name": name,
            "visuals": visuals or [],
        })
        return self

    def _build_metadata_sqlite(self) -> bytes:
        """Build the SQLite metadata database."""
        db_path = tempfile.mktemp(suffix=".db")
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        c.execute("CREATE TABLE [Model] (ID INTEGER PRIMARY KEY, Name TEXT)")
        c.execute("INSERT INTO [Model] VALUES (1, ?)", (self._name,))

        c.execute("""CREATE TABLE [Table] (
            ID INTEGER PRIMARY KEY, ModelID INTEGER, Name TEXT,
            IsHidden INTEGER DEFAULT 0, Description TEXT DEFAULT ''
        )""")

        c.execute("""CREATE TABLE [Column] (
            ID INTEGER PRIMARY KEY, TableID INTEGER,
            ExplicitName TEXT, InferredName TEXT,
            Expression TEXT, Type INTEGER DEFAULT 1,
            IsHidden INTEGER DEFAULT 0, IsKey INTEGER DEFAULT 0
        )""")

        c.execute("""CREATE TABLE [Measure] (
            ID INTEGER PRIMARY KEY, TableID INTEGER,
            Name TEXT, Expression TEXT, Description TEXT DEFAULT ''
        )""")

        c.execute("""CREATE TABLE [Relationship] (
            ID INTEGER PRIMARY KEY, ModelID INTEGER,
            FromTableID INTEGER, FromColumnID INTEGER, FromCardinality INTEGER,
            ToTableID INTEGER, ToColumnID INTEGER, ToCardinality INTEGER,
            IsActive INTEGER DEFAULT 1, CrossFilteringBehavior INTEGER DEFAULT 1,
            Name TEXT DEFAULT ''
        )""")

        c.execute("""CREATE TABLE [Partition] (
            ID INTEGER PRIMARY KEY, TableID INTEGER, Name TEXT,
            Type INTEGER DEFAULT 4, QueryDefinition TEXT
        )""")

        # Insert tables and columns
        table_id = 1
        col_id = 1
        table_id_map = {}

        for tdef in self._tables:
            c.execute(
                "INSERT INTO [Table] VALUES (?, 1, ?, ?, '')",
                (table_id, tdef["name"], 1 if tdef["hidden"] else 0),
            )
            c.execute(
                "INSERT INTO [Partition] VALUES (?, ?, ?, 4, NULL)",
                (table_id, table_id, tdef["name"]),
            )
            table_id_map[tdef["name"]] = table_id

            for col_def in tdef["columns"]:
                c.execute(
                    "INSERT INTO [Column] VALUES (?, ?, ?, NULL, NULL, 1, 0, 0)",
                    (col_id, table_id, col_def["name"]),
                )
                col_id += 1

            table_id += 1

        # Insert measures
        measure_id = 1
        for mdef in self._measures:
            tid = table_id_map.get(mdef["table"])
            if tid is None:
                # Create hidden measure table if needed
                c.execute(
                    "INSERT INTO [Table] VALUES (?, 1, ?, 1, '')",
                    (table_id, mdef["table"]),
                )
                table_id_map[mdef["table"]] = table_id
                tid = table_id
                table_id += 1

            c.execute(
                "INSERT INTO [Measure] VALUES (?, ?, ?, ?, ?)",
                (measure_id, tid, mdef["name"], mdef["expression"], mdef["description"]),
            )
            measure_id += 1

        # Insert relationships
        rel_id = 1
        for rdef in self._relationships:
            from_tid = table_id_map.get(rdef["from_table"], 0)
            to_tid = table_id_map.get(rdef["to_table"], 0)
            c.execute(
                "INSERT INTO [Relationship] VALUES (?, 1, ?, 0, 2, ?, 0, 1, 1, 1, '')",
                (rel_id, from_tid, to_tid),
            )
            rel_id += 1

        conn.commit()
        conn.close()

        with open(db_path, "rb") as f:
            data = f.read()
        os.unlink(db_path)
        return data

    def _build_layout(self) -> bytes:
        """Build the Report/Layout JSON."""
        pages = self._pages or [{"name": "Page 1", "visuals": []}]

        sections = []
        for i, page in enumerate(pages):
            containers = []
            for j, vis in enumerate(page.get("visuals", [])):
                containers.append({
                    "x": vis.get("x", 20 + j * 320),
                    "y": vis.get("y", 20),
                    "width": vis.get("width", 300),
                    "height": vis.get("height", 200),
                    "config": json.dumps({
                        "name": vis.get("name", f"visual_{j}"),
                        "singleVisual": {
                            "visualType": vis.get("type", "card"),
                        },
                    }),
                })
            sections.append({
                "displayName": page["name"],
                "name": f"ReportSection{i + 1}",
                "ordinal": i,
                "visualContainers": containers,
            })

        layout = {"id": 0, "sections": sections}
        return json.dumps(layout, ensure_ascii=False).encode("utf-16-le")

    def build(self) -> bytes:
        """Build the complete PBIX file as bytes."""
        from pbix_mcp.formats.abf_rebuild import build_abf_from_scratch
        from pbix_mcp.formats.datamodel_roundtrip import compress_datamodel

        # 1. Build metadata
        sqlite_bytes = self._build_metadata_sqlite()

        # 2. Build ABF
        abf_bytes = build_abf_from_scratch({"metadata.sqlitedb": sqlite_bytes})

        # 3. Compress to DataModel
        datamodel_bytes = compress_datamodel(abf_bytes)

        # 4. Build layout
        layout_bytes = self._build_layout()

        # 5. Pack into ZIP
        import io
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("Report/Layout", layout_bytes)
            zf.writestr("DataModel", datamodel_bytes)
            zf.writestr("Settings", json.dumps({"version": "5.0"}))
            zf.writestr("Metadata", json.dumps({"version": 3}))
            zf.writestr(
                "[Content_Types].xml",
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                "</Types>",
            )

        return buf.getvalue()

    def save(self, path: str) -> str:
        """Build and save the PBIX file to disk.

        Returns the absolute path of the saved file.
        """
        data = self.build()
        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
        with open(abs_path, "wb") as f:
            f.write(data)
        return abs_path
