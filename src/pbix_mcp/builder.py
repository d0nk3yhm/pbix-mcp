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
        """Build the Report/Layout JSON with proper data bindings."""
        pages = self._pages or [{"name": "Page 1", "visuals": []}]

        # Default measure entity — measures in the template are hosted on
        # "# Measures" table (that's where Power BI puts auto-generated measures).
        default_measure_entity = "# Measures"

        sections = []
        for i, page in enumerate(pages):
            containers = []
            for j, vis in enumerate(page.get("visuals", [])):
                visual_type = vis.get("type", "card")
                cfg = vis.get("config", {})
                single_visual = {"visualType": visual_type}

                # Build data bindings based on visual type and config
                bindings = self._build_visual_bindings(
                    visual_type, cfg, default_measure_entity
                )
                if bindings:
                    single_visual["projections"] = bindings["projections"]
                    single_visual["prototypeQuery"] = bindings["prototypeQuery"]

                containers.append({
                    "x": vis.get("x", 20 + j * 320),
                    "y": vis.get("y", 20),
                    "width": vis.get("width", 300),
                    "height": vis.get("height", 200),
                    "config": json.dumps({
                        "name": vis.get("name", f"visual_{j}"),
                        "singleVisual": single_visual,
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

    @staticmethod
    def _build_visual_bindings(
        visual_type: str,
        cfg: dict,
        default_measure_entity: str,
    ) -> dict | None:
        """Build projections + prototypeQuery for a visual config.

        Returns None for visuals that don't need data bindings (e.g. textbox).
        """
        # Collect all entities referenced and build From/Select lists
        from_sources: dict[str, str] = {}  # entity -> alias
        selects: list[dict] = []
        projections: dict[str, list[dict]] = {}

        def _alias_for(entity: str) -> str:
            """Get or create a short alias for a table/entity."""
            if entity not in from_sources:
                from_sources[entity] = entity[0].lower()
                # Handle alias collisions
                base = from_sources[entity]
                existing = set(from_sources.values()) - {base}
                suffix = 0
                while base in existing:
                    suffix += 1
                    base = entity[0].lower() + str(suffix)
                from_sources[entity] = base
            return from_sources[entity]

        def _add_measure(measure_name: str) -> str:
            """Add a measure to selects, return its queryRef."""
            entity = default_measure_entity
            alias = _alias_for(entity)
            query_ref = f"{entity}.{measure_name}"
            selects.append({
                "Measure": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property": measure_name,
                },
                "Name": query_ref,
            })
            return query_ref

        def _add_column(table: str, column: str) -> str:
            """Add a column to selects, return its queryRef."""
            alias = _alias_for(table)
            query_ref = f"{table}.{column}"
            selects.append({
                "Column": {
                    "Expression": {"SourceRef": {"Source": alias}},
                    "Property": column,
                },
                "Name": query_ref,
            })
            return query_ref

        # --- Card: single measure -----------------------------------------
        if visual_type == "card" and "measure" in cfg:
            ref = _add_measure(cfg["measure"])
            projections["Values"] = [{"queryRef": ref, "active": True}]

        # --- Slicer: single column ----------------------------------------
        elif visual_type == "slicer" and "column" in cfg:
            col_cfg = cfg["column"]
            ref = _add_column(col_cfg["table"], col_cfg["column"])
            projections["Values"] = [{"queryRef": ref, "active": True}]

        # --- Table / matrix: multiple columns and measures ----------------
        elif visual_type in ("tableEx", "table", "matrix") and "columns" in cfg:
            values = []
            for col_def in cfg["columns"]:
                if "measure" in col_def:
                    ref = _add_measure(col_def["measure"])
                else:
                    ref = _add_column(col_def["table"], col_def["column"])
                values.append({"queryRef": ref, "active": True})
            projections["Values"] = values

        # --- Chart types: category + measure ------------------------------
        elif "category" in cfg and "measure" in cfg:
            cat_cfg = cfg["category"]
            cat_ref = _add_column(cat_cfg["table"], cat_cfg["column"])
            meas_ref = _add_measure(cfg["measure"])
            projections["Category"] = [{"queryRef": cat_ref, "active": True}]
            projections["Y"] = [{"queryRef": meas_ref, "active": True}]

        else:
            # No data binding needed (textbox, shapes, etc.)
            return None

        # Assemble the prototypeQuery
        from_list = [
            {"Name": alias, "Entity": entity, "Type": 0}
            for entity, alias in from_sources.items()
        ]

        return {
            "projections": projections,
            "prototypeQuery": {
                "Version": 2,
                "From": from_list,
                "Select": selects,
            },
        }

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
            """Overwrite template measures with our custom ones.

            The .db.xml defines exactly N measures — we can't add beyond
            that count. So we REPLACE existing measure names/expressions
            rather than INSERT new rows. The template has ~49 measures;
            we overwrite the first len(measures) of them.
            """
            c = conn.cursor()

            # Get existing measure IDs in order
            existing = c.execute(
                "SELECT ID, TableID FROM [Measure] ORDER BY ID"
            ).fetchall()

            if not existing:
                return

            # Overwrite existing measures with our custom ones
            for idx, mdef in enumerate(measures):
                if idx < len(existing):
                    mid, tid = existing[idx]
                    c.execute(
                        "UPDATE [Measure] SET Name=?, Expression=?, Description=? WHERE ID=?",
                        (mdef["name"], mdef["expression"],
                         mdef.get("description", ""), mid),
                    )

            # Hide remaining template measures (rename to avoid confusion)
            for idx in range(len(measures), len(existing)):
                mid, tid = existing[idx]
                c.execute(
                    "UPDATE [Measure] SET Name=?, Expression=? WHERE ID=?",
                    (f"_unused_{idx}", "0", mid),
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

        # 5. Pack into ZIP — use the template PBIX as base, replace DataModel + Layout
        # This preserves all the OPC packaging that Power BI Desktop expects
        # (Version, Settings, Metadata, Content_Types, _rels, etc.)
        import io
        template_pbix_path = os.path.join(
            os.path.dirname(__file__), "templates", "minimal_template.pbix"
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(template_pbix_path) as zf_in:
            with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf_out:
                for item in zf_in.namelist():
                    if item == "DataModel":
                        zf_out.writestr(item, datamodel_bytes)
                    elif item == "Report/Layout":
                        zf_out.writestr(item, layout_bytes)
                    else:
                        zf_out.writestr(item, zf_in.read(item))

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
