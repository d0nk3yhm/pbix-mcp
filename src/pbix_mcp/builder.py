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
        rows: list[dict] | None = None,
        hidden: bool = False,
    ) -> "PBIXBuilder":
        """Add a table definition with optional row data.

        Args:
            name: Table name
            columns: List of {"name": str, "data_type": str} dicts.
                     data_type: "String", "Int64", "Double", "DateTime", "Decimal"
            rows: Optional list of row dicts, e.g. [{"Amount": 100, "Product": "Widget"}]
            hidden: Whether the table is hidden (e.g., measure containers)
        """
        self._tables.append({
            "name": name,
            "columns": columns,
            "rows": rows or [],
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
        """Build the complete PBIX file as bytes.

        Uses a minimal template DataModel from a real PBIX file (shipped
        with the package) and modifies its metadata SQLite to contain our
        custom tables, measures, and relationships. The template's .db.xml
        and internal ABF structure are preserved so Analysis Services can
        load it correctly.
        """
        from pbix_mcp.formats.abf_rebuild import (
            rebuild_abf_with_modified_sqlite,
        )
        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )

        # Capture builder state for the modifier closure
        tables = self._tables
        measures = self._measures
        relationships = self._relationships

        def _modify_metadata(conn: sqlite3.Connection) -> None:
            """Modify the template's metadata to contain our custom schema."""
            c = conn.cursor()

            # Clear existing data (keep schema intact for AS compatibility)
            c.execute("DELETE FROM [Measure]")
            c.execute("DELETE FROM [Relationship]")
            c.execute("DELETE FROM [Column] WHERE TableID IN (SELECT ID FROM [Table] WHERE ModelID=1)")
            c.execute("DELETE FROM [Partition] WHERE TableID IN (SELECT ID FROM [Table] WHERE ModelID=1)")
            c.execute("DELETE FROM [Table] WHERE ModelID=1")

            # Add our tables
            col_id = 1
            for tid, tdef in enumerate(tables, start=1):
                c.execute(
                    "INSERT INTO [Table] (ID, ModelID, Name, IsHidden) VALUES (?, 1, ?, ?)",
                    (tid, tdef["name"], 1 if tdef.get("hidden") else 0),
                )
                # Add partition (required — Type=4 for M/Import)
                c.execute(
                    "INSERT OR IGNORE INTO [Partition] (ID, TableID, Name, Type) VALUES (?, ?, ?, 4)",
                    (tid, tid, f"{tdef['name']}_partition"),
                )
                # Add columns
                for ci, col_def in enumerate(tdef["columns"]):
                    c.execute(
                        "INSERT INTO [Column] (ID, TableID, ExplicitName, InferredName, Type) "
                        "VALUES (?, ?, ?, ?, 1)",
                        (col_id, tid, col_def["name"], col_def["name"]),
                    )
                    col_id += 1
                # Add RowNumber column (required by AS)
                c.execute(
                    "INSERT INTO [Column] (ID, TableID, ExplicitName, InferredName, Type, IsHidden) "
                    "VALUES (?, ?, ?, ?, 3, 1)",
                    (col_id, tid, f"RowNumber-{tdef['name']}", f"RowNumber-{tdef['name']}"),
                )
                col_id += 1

            # Add our measures
            for mid, mdef in enumerate(measures, start=1):
                # Find table ID
                table_id = None
                for tid, tdef in enumerate(tables, start=1):
                    if tdef["name"] == mdef["table"]:
                        table_id = tid
                        break
                if table_id:
                    c.execute(
                        "INSERT INTO [Measure] (ID, TableID, Name, Expression, Description) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (mid, table_id, mdef["name"], mdef["expression"],
                         mdef.get("description", "")),
                    )

            # Add our relationships
            for rid, rdef in enumerate(relationships, start=1):
                from_tid = from_cid = to_tid = to_cid = None
                for tid, tdef in enumerate(tables, start=1):
                    if tdef["name"] == rdef["from_table"]:
                        from_tid = tid
                        for ci, col in enumerate(tdef["columns"]):
                            if col["name"] == rdef["from_column"]:
                                # Find column ID (cumulative)
                                from_cid = sum(len(tables[j]["columns"]) + 1 for j in range(tid - 1)) + ci + 1
                    if tdef["name"] == rdef["to_table"]:
                        to_tid = tid
                        for ci, col in enumerate(tdef["columns"]):
                            if col["name"] == rdef["to_column"]:
                                to_cid = sum(len(tables[j]["columns"]) + 1 for j in range(tid - 1)) + ci + 1
                if all(v is not None for v in [from_tid, from_cid, to_tid, to_cid]):
                    c.execute(
                        "INSERT INTO [Relationship] (ID, ModelID, FromTableID, FromColumnID, "
                        "FromCardinality, ToTableID, ToColumnID, ToCardinality, IsActive) "
                        "VALUES (?, 1, ?, ?, 2, ?, ?, 1, 1)",
                        (rid, from_tid, from_cid, to_tid, to_cid),
                    )

            conn.commit()

        # 1. Load the template DataModel
        template_path = os.path.join(
            os.path.dirname(__file__), "templates", "minimal_datamodel.bin"
        )
        with open(template_path, "rb") as f:
            template_dm = f.read()

        template_abf = decompress_datamodel(template_dm)

        # 2. Modify the template's metadata in-place
        new_abf = rebuild_abf_with_modified_sqlite(template_abf, _modify_metadata)

        # 3. Compress to DataModel
        datamodel_bytes = compress_datamodel(new_abf)

        # 4. Build layout
        layout_bytes = self._build_layout()

        # 5. Pack into ZIP (OPC package format)
        import io
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            # Version — required by PowerBI Desktop (UTF-16LE)
            zf.writestr("Version", "1.31".encode("utf-16-le"))

            # Report/Layout — UTF-16LE encoded JSON
            zf.writestr("Report/Layout", layout_bytes)

            # DataModel — XPress9-compressed ABF
            zf.writestr("DataModel", datamodel_bytes)

            # Settings — UTF-16LE encoded JSON
            settings = {
                "Version": 1,
                "ReportSettings": {},
                "QueriesSettings": {
                    "TypeDetectionEnabled": True,
                    "RelationshipImportEnabled": True,
                    "Version": "2.81.5831.821",
                },
            }
            zf.writestr("Settings", json.dumps(settings).encode("utf-16-le"))

            # Metadata — UTF-16LE encoded JSON
            metadata = {
                "Version": 5,
                "AutoCreatedRelationships": [],
                "FileDescription": "",
                "CreatedFrom": "pbix-mcp",
                "CreatedFromRelease": "0.1.0",
            }
            zf.writestr("Metadata", json.dumps(metadata).encode("utf-16-le"))

            # DiagramLayout — empty but expected
            zf.writestr("DiagramLayout", "{}".encode("utf-16-le"))

            # Content_Types.xml — OPC content types
            zf.writestr(
                "[Content_Types].xml",
                '<?xml version="1.0" encoding="utf-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="json" ContentType=""/>'
                '<Default Extension="xml" ContentType=""/>'
                '<Override PartName="/Version" ContentType=""/>'
                '<Override PartName="/DataModel" ContentType=""/>'
                '<Override PartName="/DiagramLayout" ContentType=""/>'
                '<Override PartName="/Report/Layout" ContentType=""/>'
                '<Override PartName="/Settings" ContentType="application/json"/>'
                '<Override PartName="/Metadata" ContentType="application/json"/>'
                "</Types>",
            )

            # _rels/.rels — OPC relationships
            zf.writestr(
                "_rels/.rels",
                '<?xml version="1.0" encoding="utf-8" standalone="yes"?>'
                '<Relationships xmlns='
                '"http://schemas.openxmlformats.org/package/2006/relationships">'
                "</Relationships>",
            )

            # DataMashup — empty M code container
            # Power BI Desktop regenerates this on first open
            zf.writestr("DataMashup", b"")

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
