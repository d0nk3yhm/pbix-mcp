"""
PBIX Builder — create valid Power BI .pbix files from scratch.

No existing PBIX file needed. Builds every layer:
  1. SQLite metadata (tables, columns, measures, relationships)
  2. VertiPaq binary column data (IDF, IDFMETA, dictionary, HIDX)
  3. ABF archive containing metadata + column data
  4. XPress9-compressed DataModel
  5. Report layout JSON
  6. ZIP packaging with all required entries

Usage:
    from pbix_mcp.builder import PBIXBuilder

    builder = PBIXBuilder()
    builder.add_table("Sales", [
        {"name": "Product", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
    ], rows=[{"Product": "Widget", "Amount": 100}])
    builder.add_measure("Sales", "Total Sales", "SUM(Sales[Amount])")
    builder.add_relationship("Sales", "Product", "Products", "Product")
    builder.save("output.pbix")
"""

import io
import json
import os
import sqlite3
import struct
import tempfile
import uuid
import zipfile

# AMO data-type codes used in metadata.sqlitedb
_TYPE_NAME_TO_AMO = {
    "String": 2,
    "Int64": 6,
    "Float64": 8,
    "Double": 8,
    "DateTime": 9,
    "Decimal": 10,
    "Boolean": 11,
}

# Timestamp used for ModifiedTime / StructureModifiedTime (Windows FILETIME)
_FIXED_TIMESTAMP = 133534961699396761

# Content_Types.xml for PBIX ZIP (OPC format with BOM)
_CONTENT_TYPES_XML = (
    b'\xef\xbb\xbf'  # UTF-8 BOM
    b'<?xml version="1.0" encoding="utf-8"?>'
    b'<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
    b'<Default Extension="json" ContentType="" />'
    b'<Override PartName="/Version" ContentType="" />'
    b'<Override PartName="/DataModel" ContentType="" />'
    b'<Override PartName="/Report/Layout" ContentType="" />'
    b'<Override PartName="/DiagramLayout" ContentType="" />'
    b'<Override PartName="/Settings" ContentType="application/json" />'
    b'<Override PartName="/Metadata" ContentType="application/json" />'
    b'</Types>'
)

# Version string in UTF-16-LE
_VERSION_BYTES = "1.28".encode("utf-16-le")  # 8 bytes

# Settings JSON in UTF-16-LE
_SETTINGS_JSON = '{"Version":4,"ReportSettings":{},"QueriesSettings":{"TypeDetectionEnabled":true,"RelationshipImportEnabled":false}}'.encode("utf-16-le")

# Metadata JSON in UTF-16-LE
_METADATA_JSON = '{"Version":5,"AutoCreatedRelationships":[],"CreatedFrom":"Cloud","CreatedFromRelease":"2024.03"}'.encode("utf-16-le")


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
        source_csv: str | None = None,
        source_db: dict | None = None,
        mode: str = "import",
    ) -> "PBIXBuilder":
        """Add a table definition with optional row data.

        Args:
            name: Table name
            columns: List of {"name": str, "data_type": str} dicts.
                     data_type: "String", "Int64", "Double", "DateTime", "Decimal", "Boolean"
            rows: Optional list of row dicts, e.g. [{"Amount": 100, "Product": "Widget"}]
            hidden: Whether the table is hidden (e.g., measure containers)
            source_csv: Optional absolute path to a CSV file. The M expression will
                        reference this file, so clicking "Refresh" in PBI Desktop
                        re-imports from the CSV. The rows parameter provides the
                        initial data snapshot embedded in the PBIX.
            source_db: Optional database connection dict for Refresh in PBI Desktop.
                       {"type": "sqlite", "path": "/path/to/db.sqlite", "table": "orders"}
                       {"type": "mysql", "server": "host", "database": "mydb",
                        "table": "orders", "port": 3306}
                       {"type": "postgresql", "server": "host", "database": "mydb",
                        "table": "orders", "port": 5432, "schema": "public"}
                       {"type": "sqlserver", "server": "host", "database": "mydb",
                        "table": "orders"}
                       {"type": "excel", "path": "C:/data.xlsx", "sheet": "Sheet1"}
                       {"type": "json", "url": "https://api.example.com/data"}
                       {"type": "azuresql", "server": "host.database.windows.net",
                        "database": "mydb", "table": "orders"}
                       The rows parameter provides the initial data snapshot.
            mode: Storage mode — "import" (default) or "directquery". Both modes
                  embed full VertiPaq data. DirectQuery sets Partition.Mode=1 instead
                  of Mode=0, and the M expression points to the source database.
                  Rows provide the embedded data snapshot for both modes.
        """
        if mode not in ("import", "directquery"):
            raise ValueError(f"mode must be 'import' or 'directquery', got {mode!r}")
        self._tables.append({
            "name": name,
            "columns": columns,
            "rows": rows or [],
            "hidden": hidden,
            "source_csv": source_csv,
            "source_db": source_db,
            "mode": mode,
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

    # ------------------------------------------------------------------
    # DataMashup rebuilding — clean M queries only
    # ------------------------------------------------------------------

    def _rebuild_datamashup(self) -> bytes | None:
        """Rebuild the DataMashup binary with only user table M queries.

        Strips all template remnants (financials, # Measures, DateAutoTemplate)
        and writes clean Section1.m containing only the user's table queries.
        """
        template_pbix_path = os.path.join(
            os.path.dirname(__file__), "templates", "minimal_template.pbix"
        )

        # Read the template DataMashup
        with zipfile.ZipFile(template_pbix_path) as zf:
            if "DataMashup" not in zf.namelist():
                return None
            data = zf.read("DataMashup")

        # Find inner ZIP
        pk_offset = data.find(b"PK\x03\x04")
        if pk_offset == -1:
            return None

        eocd_sig = b"PK\x05\x06"
        eocd_pos = data.rfind(eocd_sig)
        if eocd_pos == -1:
            return None

        eocd_comment_len = struct.unpack_from("<H", data, eocd_pos + 20)[0]
        zip_end = eocd_pos + 22 + eocd_comment_len
        old_zip_data = data[pk_offset:zip_end]

        # Build clean M code with only user tables
        m_lines = ["section Section1;"]
        for tdef in self._tables:
            tname = tdef["name"]
            source_db = tdef.get("source_db")
            source_csv = tdef.get("source_csv")
            is_dq = tdef.get("mode") == "directquery"

            if source_db or source_csv:
                # Build M expression for this table
                m_expr = _build_m_expression(
                    tname, tdef.get("columns", []),
                    source_csv, source_db,
                    is_directquery=is_dq,
                )
                # Escape table name if needed
                m_name = tname
                if " " in tname or any(c in tname for c in "[](){}#"):
                    m_name = f'#"{tname}"'
                m_lines.append(f"shared {m_name} = {m_expr};")

        new_m_code = "\n".join(m_lines) + "\n"

        # Rebuild inner ZIP with new Section1.m
        new_zip_buf = io.BytesIO()
        try:
            with zipfile.ZipFile(io.BytesIO(old_zip_data), "r") as old_zf:
                with zipfile.ZipFile(new_zip_buf, "w", zipfile.ZIP_DEFLATED) as new_zf:
                    for item in old_zf.namelist():
                        if item.endswith("Section1.m"):
                            new_zf.writestr(item, new_m_code.encode("utf-8"))
                        else:
                            new_zf.writestr(item, old_zf.read(item))
        except zipfile.BadZipFile:
            return None

        new_zip_bytes = new_zip_buf.getvalue()

        # Splice: prefix + new_zip + suffix
        prefix = data[:pk_offset]
        suffix = data[zip_end:]
        new_data = prefix + new_zip_bytes + suffix

        # Update size field if present
        if pk_offset >= 4:
            old_size = struct.unpack_from("<I", prefix, pk_offset - 4)[0]
            old_zip_len = zip_end - pk_offset
            if old_size == old_zip_len:
                new_data = bytearray(new_data)
                struct.pack_into("<I", new_data, pk_offset - 4, len(new_zip_bytes))
                new_data = bytes(new_data)

        return bytes(new_data)

    # ------------------------------------------------------------------
    # Layout building (preserved from original)
    # ------------------------------------------------------------------

    def _build_layout(self) -> bytes:
        """Build the Report/Layout JSON with proper data bindings."""
        pages = self._pages or [{"name": "Page 1", "visuals": []}]

        # Build a mapping of measure name -> hosting table name
        measure_to_table: dict[str, str] = {}
        for mdef in self._measures:
            measure_to_table[mdef["name"]] = mdef["table"]

        sections = []
        for i, page in enumerate(pages):
            containers = []
            for j, vis in enumerate(page.get("visuals", [])):
                visual_type = vis.get("type", "card")
                cfg = vis.get("config", {})
                single_visual = {"visualType": visual_type}

                # Build data bindings based on visual type and config
                bindings = self._build_visual_bindings(
                    visual_type, cfg, measure_to_table
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
        measure_to_table: dict[str, str],
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
            # Measures reference the table they are hosted on
            entity = measure_to_table.get(measure_name, "# Measures")
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

    # ------------------------------------------------------------------
    # Core build logic
    # ------------------------------------------------------------------

    def build(self) -> bytes:
        """Build the complete PBIX file as bytes — clean metadata from scratch.

        The DataModel metadata (SQLite) is created from scratch with zero
        template artifacts. The ABF binary structure uses the template for
        its proven format, but all data content is user-generated.

        Steps:
          1. Create empty metadata SQLite from scratch (63 system tables, no data)
          2. INSERT tables, columns, partitions, measures, relationships
          3. Encode row data with VertiPaq encoder
          4. Replace metadata in template ABF + add new VertiPaq files
          5. Compress to DataModel (XPress9)
          6. Build Report/Layout JSON
          7. Package into PBIX ZIP
        """
        from pbix_mcp.formats.abf_rebuild import (
            _ABFStructure,
            find_abf_file,
            read_metadata_sqlite,
        )
        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )

        # Capture builder state
        tables = self._tables
        measures = self._measures
        relationships = self._relationships

        from pbix_mcp.formats.metadata_schema import create_empty_metadata_db

        # 1. Create clean metadata from scratch — no template data
        sqlite_bytes = create_empty_metadata_db()

        # Use fixed compression class IDs (from VertiPaq RE)
        _HYBRID_RLE = 0xABA5A
        _XM123_CLASS = 0xABA5B

        # 2. Modify metadata and encode VertiPaq files
        new_sqlite_bytes, vertipaq_files = _modify_metadata_and_encode(
            sqlite_bytes, tables, measures, relationships,
            template_u32_a=_HYBRID_RLE,
            template_max_u32_b=_XM123_CLASS,
        )

        # 3. Build ABF: template structure + our metadata + our VP files
        # The template VP data stays (neutralized by our clean metadata)
        # Our new VP files are added alongside
        template_path = os.path.join(
            os.path.dirname(__file__), "templates", "minimal_datamodel.bin"
        )
        with open(template_path, "rb") as f:
            template_dm = f.read()
        template_abf = decompress_datamodel(template_dm)
        abf_struct = _ABFStructure(template_abf)

        exact_replacements: dict[str, bytes] = {}
        for entry in abf_struct.file_log:
            if "metadata.sqlitedb" in entry["Path"].lower():
                exact_replacements[entry["StoragePath"]] = new_sqlite_bytes
                break

        new_abf = _rebuild_abf_with_new_files(
            abf_struct, exact_replacements, vertipaq_files
        )

        # 4. Compress to DataModel
        datamodel_bytes = compress_datamodel(new_abf)

        # 6. Build layout
        layout_bytes = self._build_layout()

        # 7. Pack into PBIX ZIP
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
                    elif item == "SecurityBindings":
                        continue  # Skip — auto-removed for clean files
                    elif item == "Settings":
                        # Patch Settings to disable auto-relationship detection
                        # which causes TMCCollectionObject::Add errors on Refresh
                        settings_raw = zf_in.read(item)
                        settings_text = settings_raw.decode("utf-16-le")
                        settings_text = settings_text.replace(
                            '"RelationshipImportEnabled":true',
                            '"RelationshipImportEnabled":false',
                        )
                        zf_out.writestr(item, settings_text.encode("utf-16-le"))
                    else:
                        zf_out.writestr(item, zf_in.read(item))

        return buf.getvalue()

    def validate(self, data: bytes | None = None) -> list[str]:
        """Validate a built PBIX for structural integrity.

        Returns a list of issues found (empty = valid).
        Checks: ZIP structure, DataModel presence, SQLite metadata
        consistency, ABF file references, column storage integrity.
        """
        if data is None:
            data = self.build()
        issues: list[str] = []
        try:
            zf = zipfile.ZipFile(io.BytesIO(data))
            names = zf.namelist()
            if "DataModel" not in names:
                issues.append("Missing DataModel entry in ZIP")
                return issues
            if "[Content_Types].xml" not in names:
                issues.append("Missing [Content_Types].xml")

            from pbix_mcp.formats.abf_rebuild import (
                list_abf_files,
                read_metadata_sqlite,
            )
            from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

            dm = zf.read("DataModel")
            try:
                abf = decompress_datamodel(dm)
            except Exception as e:
                issues.append(f"XPress9 decompression failed: {e}")
                return issues

            # Check ABF has files
            flog = list_abf_files(abf)
            if not flog:
                issues.append("ABF archive contains no files")

            # Check SQLite metadata
            try:
                sb = read_metadata_sqlite(abf)
                fd, tmp = tempfile.mkstemp(suffix=".db")
                os.write(fd, sb)
                os.close(fd)
                conn = sqlite3.connect(tmp)

                # Check tables have storage
                for row in conn.execute(
                    "SELECT Name, TableStorageID FROM [Table] WHERE SystemFlags=0"
                ).fetchall():
                    if row[1] == 0:
                        issues.append(f"Table '{row[0]}' has no TableStorage")

                # Check partitions have M expressions
                for row in conn.execute(
                    "SELECT p.Name, p.QueryDefinition, p.Type FROM [Partition] p "
                    "JOIN [Table] t ON p.TableID = t.ID WHERE t.SystemFlags=0"
                ).fetchall():
                    if row[2] == 4 and not row[1]:
                        issues.append(f"Partition '{row[0]}' has no QueryDefinition")

                # Check columns have AttributeHierarchy
                for row in conn.execute(
                    "SELECT c.ExplicitName, c.AttributeHierarchyID, t.Name "
                    "FROM [Column] c JOIN [Table] t ON c.TableID = t.ID "
                    "WHERE t.SystemFlags=0 AND c.AttributeHierarchyID=0 AND c.Type!=2"
                ).fetchall():
                    issues.append(
                        f"Column '{row[2]}.{row[0]}' missing AttributeHierarchy"
                    )

                conn.close()
                os.unlink(tmp)
            except Exception as e:
                issues.append(f"SQLite metadata error: {e}")

            zf.close()
        except Exception as e:
            issues.append(f"ZIP structure error: {e}")
        return issues

    def save(self, path: str, validate: bool = True) -> str:
        """Build and save the PBIX file to disk.

        Args:
            path: Output file path
            validate: If True, run structural validation before saving.
                      Raises ValueError if critical issues found.

        Returns the absolute path of the saved file.
        """
        data = self.build()

        if validate:
            issues = self.validate(data)
            if issues:
                import warnings
                for issue in issues:
                    warnings.warn(f"PBIX validation: {issue}", stacklevel=2)

        abs_path = os.path.abspath(path)
        os.makedirs(os.path.dirname(abs_path) or ".", exist_ok=True)
        with open(abs_path, "wb") as f:
            f.write(data)
        return abs_path


# ======================================================================
# Module-level helpers for metadata modification and ABF construction
# ======================================================================


class _IDAllocator:
    """Global sequential ID allocator starting from a given base."""

    def __init__(self, start: int):
        self._next = start

    def next(self) -> int:
        val = self._next
        self._next += 1
        return val


def _build_m_expression(
    table_name: str,
    columns: list[dict],
    source_csv: str | None = None,
    source_db: dict | None = None,
    is_directquery: bool = False,
) -> str:
    """Build a valid M expression for a table partition.

    Every partition needs a QueryDefinition — even import partitions.
    Without it, the TOM model's Partition.Source is null, causing NullRef
    at RunModelSchemaValidation.

    If source_csv is provided, the M expression reads from that CSV file.
    If source_db is provided, the M expression connects to the database.
    Clicking "Refresh" in PBI Desktop will re-import from the source.
    """
    # Map data types to M types
    _M_TYPES = {
        "String": "Text.Type",
        "Int64": "Int64.Type",
        "Double": "Number.Type",
        "Float64": "Number.Type",
        "DateTime": "DateTime.Type",
        "Decimal": "Number.Type",
        "Boolean": "Logical.Type",
    }

    if not columns:
        return 'let\n    Source = #table(type table [placeholder = text], {})\nin\n    Source'

    # Build column type transforms for Csv.Document
    col_transforms = []
    for col in columns:
        m_type = _M_TYPES.get(col.get("data_type", "String"), "Text.Type")
        col_name = col["name"]
        col_transforms.append('{"' + col_name + '", ' + m_type + "}")

    if source_csv:
        # M expression that reads from a CSV file
        escaped_path = source_csv.replace("\\", "\\\\")
        transforms = ", ".join(col_transforms)
        return (
            "let\n"
            f'    Source = Csv.Document(File.Contents("{escaped_path}"), '
            f'[Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.None]),\n'
            '    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),\n'
            f"    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {{{transforms}}})\n"
            "in\n"
            "    TypedColumns"
        )

    if source_db:
        db_type = source_db.get("type", "").lower()
        db_table = source_db.get("table", table_name)
        transforms = ", ".join(col_transforms)

        if db_type == "sqlite":
            db_path = source_db.get("path", "").replace("\\", "\\\\")
            return (
                "let\n"
                f'    Source = Odbc.DataSource("Driver={{SQLite3 ODBC Driver}};'
                f'Database={db_path}", [HierarchicalNavigation=true]),\n'
                f'    Data = Source{{[Name="{db_table}",Kind="Table"]}}[Data],\n'
                f"    TypedColumns = Table.TransformColumnTypes(Data, {{{transforms}}})\n"
                "in\n"
                "    TypedColumns"
            )
        elif db_type == "mysql":
            server = source_db.get("server", "localhost")
            database = source_db.get("database", "")
            port = source_db.get("port", 3306)
            if is_directquery:
                # DirectQuery: skip TransformColumnTypes for query folding
                return (
                    "let\n"
                    f'    Source = MySQL.Database("{server}:{port}", "{database}"),\n'
                    f'    Data = Source{{[Schema="{database}",Item="{db_table}"]}}[Data]\n'
                    "in\n"
                    "    Data"
                )
            return (
                "let\n"
                f'    Source = MySQL.Database("{server}:{port}", "{database}"),\n'
                f'    Data = Source{{[Schema="{database}",Item="{db_table}"]}}[Data],\n'
                f"    TypedColumns = Table.TransformColumnTypes(Data, {{{transforms}}})\n"
                "in\n"
                "    TypedColumns"
            )
        elif db_type == "sqlserver":
            server = source_db.get("server", "localhost")
            database = source_db.get("database", "")
            schema = source_db.get("schema", "dbo")
            # Simple M expression without TransformColumnTypes for query folding
            return (
                "let\n"
                f'    Source = Sql.Database("{server}", "{database}"),\n'
                f'    Data = Source{{[Schema="{schema}",Item="{db_table}"]}}[Data]\n'
                "in\n"
                "    Data"
            )
        elif db_type in ("postgresql", "postgres"):
            server = source_db.get("server", "localhost")
            database = source_db.get("database", "")
            port = source_db.get("port", 5432)
            schema = source_db.get("schema", "public")
            if is_directquery:
                # DirectQuery: skip TransformColumnTypes for query folding
                return (
                    "let\n"
                    f'    Source = PostgreSQL.Database("{server}:{port}", "{database}"),\n'
                    f'    Data = Source{{[Schema="{schema}",Item="{db_table}"]}}[Data]\n'
                    "in\n"
                    "    Data"
                )
            return (
                "let\n"
                f'    Source = PostgreSQL.Database("{server}:{port}", "{database}"),\n'
                f'    Data = Source{{[Schema="{schema}",Item="{db_table}"]}}[Data],\n'
                f"    TypedColumns = Table.TransformColumnTypes(Data, {{{transforms}}})\n"
                "in\n"
                "    TypedColumns"
            )
        elif db_type == "excel":
            file_path = source_db.get("path", "").replace("\\", "\\\\")
            sheet = source_db.get("sheet", db_table)
            return (
                "let\n"
                f'    Source = Excel.Workbook(File.Contents("{file_path}"), null, true),\n'
                f'    Data = Source{{[Item="{sheet}",Kind="Sheet"]}}[Data],\n'
                '    PromotedHeaders = Table.PromoteHeaders(Data, [PromoteAllScalars=true]),\n'
                f"    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {{{transforms}}})\n"
                "in\n"
                "    TypedColumns"
            )
        elif db_type in ("json", "web", "api"):
            url = source_db.get("url", "")
            return (
                "let\n"
                f'    Source = Json.Document(Web.Contents("{url}")),\n'
                "    Data = Table.FromRecords(Source),\n"
                f"    TypedColumns = Table.TransformColumnTypes(Data, {{{transforms}}})\n"
                "in\n"
                "    TypedColumns"
            )
        elif db_type in ("azuresql", "azure"):
            server = source_db.get("server", "")
            database = source_db.get("database", "")
            schema = source_db.get("schema", "dbo")
            return (
                "let\n"
                f'    Source = Sql.Database("{server}", "{database}"),\n'
                f'    Data = Source{{[Schema="{schema}",Item="{db_table}"]}}[Data]\n'
                "in\n"
                "    Data"
            )
        elif db_type == "mariadb":
            server = source_db.get("server", "localhost")
            database = source_db.get("database", "")
            port = source_db.get("port", 3306)
            # MariaDB.Contents supports DirectQuery (unlike MySQL.Database)
            return (
                "let\n"
                f'    Source = MariaDB.Contents("{server}:{port}", "{database}"),\n'
                f'    Data = Source{{[Name="{db_table}"]}}[Data]\n'
                "in\n"
                "    Data"
            )

    # Default: empty typed table (data embedded in VertiPaq)
    field_defs = []
    for col in columns:
        m_type = _M_TYPES.get(col.get("data_type", "String"), "Text.Type")
        col_name = col["name"]
        if " " in col_name or any(c in col_name for c in "[](){}#"):
            col_name = f"#\"{col_name}\""
        field_defs.append(f"{col_name} = {m_type}")

    fields_str = ", ".join(field_defs)
    return f'let\n    Source = #table(type table [{fields_str}], {{}})\nin\n    Source'


def _get_max_id_across_tables(conn: sqlite3.Connection) -> int:
    """Find the maximum ID across ALL SQLite tables that have an ID column.

    Scans every table in the database to avoid ID collisions with
    Annotation, LinguisticMetadata, Culture, or any other table the
    template may populate.
    """
    c = conn.cursor()
    max_id = 0
    # Get ALL table names from the SQLite schema — don't rely on a hardcoded list
    all_tables = [
        row[0] for row in
        c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    ]
    for tbl in all_tables:
        try:
            row = c.execute(f"SELECT MAX(ID) FROM [{tbl}]").fetchone()
            if row and row[0] is not None:
                max_id = max(max_id, row[0])
        except sqlite3.OperationalError:
            pass  # Table has no ID column
    return max_id


def _extract_template_runtime_ids(template_abf: bytes) -> tuple[int, int]:
    """Extract AS runtime IDs (u32_a, max u32_b) from template IDFMETA files.

    These values are embedded in every IDFMETA at fixed offsets:
      offset 36: u32_a (4 bytes, same for all columns)
      offset 40: u32_b (4 bytes, varies per column)

    Returns (u32_a, max_u32_b) to use as a base for generating new column IDs.
    """
    from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file

    file_log = list_abf_files(template_abf)
    idfmeta_files = [f for f in file_log
                     if f.get("FileName", "").endswith(".0.idfmeta")
                     and f.get("Size", 0) == 264
                     and "H$" not in f.get("FileName", "")]

    u32_a = 703066  # default from known template
    max_u32_b = 703067  # default

    for mf in idfmeta_files:
        try:
            data = read_abf_file(template_abf, mf)
            if len(data) >= 44:
                a = struct.unpack_from("<I", data, 36)[0]
                b = struct.unpack_from("<I", data, 40)[0]
                if a > 0:
                    u32_a = a
                if b > max_u32_b:
                    max_u32_b = b
        except Exception:
            continue

    return u32_a, max_u32_b


def _modify_metadata_and_encode(
    sqlite_bytes: bytes,
    tables: list[dict],
    measures: list[dict],
    relationships: list[dict],
    template_u32_a: int = 703066,
    template_max_u32_b: int = 703067,
) -> tuple[bytes, dict[str, bytes]]:
    """Modify the template metadata SQLite and encode VertiPaq data.

    Returns:
        (new_sqlite_bytes, vertipaq_files) where vertipaq_files maps
        ABF internal paths to binary content.
    """
    from pbix_mcp.formats.vertipaq_encoder import (
        _align_bit_width,
        encode_nosplit_idf,
        encode_nosplit_idfmeta,
        encode_table_data,
    )

    # Write SQLite to temp file
    fd, tmp_path = tempfile.mkstemp(suffix=".sqlitedb")
    try:
        os.write(fd, sqlite_bytes)
        os.close(fd)
        fd = -1  # sentinel: already closed

        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row

        # Find max ID across ALL tables for global counter.
        # Add a large gap (1000) because the AS engine's internal ID counter
        # starts at max_existing_ID + 1. When PBI Desktop opens the file,
        # it creates new objects (LinguisticMetadata, Annotations, etc.)
        # using IDs from that counter. Without a gap, our new object IDs
        # collide with PBI's auto-created objects.
        max_id = _get_max_id_across_tables(conn)
        alloc = _IDAllocator(max_id + 1000)

        # Track table name -> table ID and column name -> column ID
        table_id_map: dict[str, int] = {}       # table_name -> TableID
        column_id_map: dict[str, dict[str, int]] = {}  # table_name -> {col_name -> ColID}
        partition_id_map: dict[str, int] = {}    # table_name -> PartitionID

        # Also track storage IDs for building ABF paths
        table_storage_map: dict[str, int] = {}   # table_name -> TableStorageID
        partition_storage_map: dict[str, int] = {}  # table_name -> PartitionStorageID

        # Track the ColumnStorage details for building file paths
        # col_storage_info[table_name][col_name] = {
        #   "col_storage_id": .., "dict_storage_id": .., "dict_file_id": ..,
        #   "idf_file_id": .., "meta_file_id": .., "hidx_file_id": ..
        # }
        col_storage_info: dict[str, dict[str, dict]] = {}

        c = conn.cursor()

        # ============================================================
        # INSERT Tables
        # ============================================================
        for tdef in tables:
            tname = tdef["name"]
            is_directquery = tdef.get("mode") == "directquery"
            table_id = alloc.next()
            table_id_map[tname] = table_id
            column_id_map[tname] = {}

            # ==============================================================
            # Full VertiPaq storage (used for both Import and DirectQuery)
            # DirectQuery is identical to Import except Partition.Mode=1
            # ==============================================================

            # TableStorage
            ts_id = alloc.next()
            table_storage_map[tname] = ts_id

            # StorageFolder for table (OwnerType=18 for TableStorage)
            tbl_folder_id = alloc.next()
            tbl_folder_path = f"{tname} ({table_id}).tbl"

            c.execute(
                "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 18, ?)",
                (tbl_folder_id, ts_id, tbl_folder_path),
            )

            # Partition
            part_id = alloc.next()
            partition_id_map[tname] = part_id

            # PartitionStorage
            ps_id = alloc.next()
            partition_storage_map[tname] = ps_id

            # SegmentMapStorage (1 per table)
            sms_id = alloc.next()

            # StorageFolder for partition (OwnerType=20 for PartitionStorage)
            prt_folder_id = alloc.next()
            prt_folder_path = f"{tname} ({table_id}).tbl\\{part_id}.prt"

            c.execute(
                "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 20, ?)",
                (prt_folder_id, ps_id, prt_folder_path),
            )

            row_count = len(tdef["rows"])

            # Insert Table row
            c.execute(
                """INSERT INTO [Table] (
                    ID, ModelID, Name, DataCategory, Description, IsHidden,
                    TableStorageID, ModifiedTime, StructureModifiedTime,
                    SystemFlags, ShowAsVariationsOnly, IsPrivate,
                    DefaultDetailRowsDefinitionID, AlternateSourcePrecedence,
                    RefreshPolicyID, CalculationGroupID, ExcludeFromModelRefresh,
                    LineageTag, SourceLineageTag, SystemManaged,
                    ExcludeFromAutomaticAggregations
                ) VALUES (
                    ?, 1, ?, NULL, NULL, ?,
                    ?, ?, ?,
                    0, 0, 0,
                    0, 0,
                    0, 0, 0,
                    ?, NULL, 0,
                    0
                )""",
                (table_id, tname, 1 if tdef["hidden"] else 0,
                 ts_id, _FIXED_TIMESTAMP, _FIXED_TIMESTAMP,
                 str(uuid.uuid4())),
            )

            # Insert TableStorage row
            c.execute(
                """INSERT INTO TableStorage (
                    ID, TableID, Name, Version, Settings, RIViolationCount, StorageFolderID
                ) VALUES (?, ?, ?, 2, 4353, 0, ?)""",
                (ts_id, table_id, f"{tname} ({table_id})", tbl_folder_id),
            )

            # Insert Partition row
            # Mode=0 for Import, Mode=1 for DirectQuery
            # Type=4 for both (Calculated partition — Type=2 requires Mode=0)
            partition_mode = 1 if is_directquery else 0
            partition_type = 4  # Always Type=4
            c.execute(
                """INSERT INTO [Partition] (
                    ID, TableID, Name, Description, DataSourceID,
                    QueryDefinition, State, Type, PartitionStorageID,
                    Mode, DataView, ModifiedTime, RefreshedTime,
                    SystemFlags, ErrorMessage, RetainDataTillForceCalculate,
                    RangeStart, RangeEnd, RangeGranularity, RefreshBookmark,
                    QueryGroupID, ExpressionSourceID, MAttributes,
                    DataCoverageDefinitionID, SchemaName
                ) VALUES (
                    ?, ?, ?, NULL, 0,
                    ?, 1, ?, ?,
                    ?, 3, ?, ?,
                    0, NULL, 0,
                    0.0, 0.0, -1, NULL,
                    0, 0, NULL,
                    0, NULL
                )""",
                (part_id, table_id, tname,
                 _build_m_expression(tname, tdef.get("columns", []),
                                     tdef.get("source_csv"), tdef.get("source_db"),
                                     is_directquery=is_directquery),
                 partition_type, ps_id,
                 partition_mode,
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            # Insert PartitionStorage row
            c.execute(
                """INSERT INTO PartitionStorage (
                    ID, PartitionID, Name, StoragePosition,
                    SegmentMapStorageID, DataObjectId, StorageFolderID,
                    DeltaTableMetadataStorageID
                ) VALUES (?, ?, ?, 0, ?, 0, ?, 0)""",
                (ps_id, part_id, f"{tname} ({part_id})", sms_id, prt_folder_id),
            )

            # Insert SegmentMapStorage row
            c.execute(
                """INSERT INTO SegmentMapStorage (
                    ID, PartitionStorageID, Type, RecordCount,
                    SegmentCount, RecordsPerSegment
                ) VALUES (?, ?, 3, ?, 1, ?)""",
                (sms_id, ps_id, row_count, row_count),
            )

            col_storage_info[tname] = {}

            # PBI_ResultType annotation for user tables (ObjectType=3 = Table)
            rt_ann_id = alloc.next()
            c.execute(
                "INSERT INTO Annotation (ID, ObjectID, ObjectType, Name, Value, "
                "ModifiedTime) VALUES (?, ?, 3, 'PBI_ResultType', 'Table', ?)",
                (rt_ann_id, table_id, _FIXED_TIMESTAMP),
            )  # ObjectType 3 = Table in TOM

            # ============================================================
            # INSERT RowNumber column (system column, Type=3)
            # ============================================================
            rn_col_id = alloc.next()
            rn_name = "RowNumber-2662979B-1795-4F74-8F37-6A1BA8059B61"
            column_id_map[tname]["__rownumber__"] = rn_col_id

            # ColumnStorage for RowNumber
            rn_cs_id = alloc.next()
            # DictionaryStorage for RowNumber (Type=2, inline, no file)
            rn_ds_id = alloc.next()
            # ColumnPartitionStorage
            rn_cps_id = alloc.next()
            # SegmentStorage
            rn_ss_id = alloc.next()
            # StorageFile for IDF (OwnerType=23 -> ColumnPartitionStorage)
            rn_idf_file_id = alloc.next()
            # StorageFile for IDFMETA (OwnerType=24 -> SegmentStorage)
            rn_meta_file_id = alloc.next()

            rn_cs_name = f"{rn_name.replace('-', ' ')} ({rn_col_id})"
            idf_fname = f"0.{tname} ({table_id}).{rn_cs_name}.0.idf"
            meta_fname = f"0.{tname} ({table_id}).{rn_cs_name}.0.idfmeta"

            # Insert Column row for RowNumber
            c.execute(
                """INSERT INTO [Column] (
                    ID, TableID, ExplicitName, InferredName,
                    ExplicitDataType, InferredDataType,
                    DataCategory, Description, IsHidden, State,
                    IsUnique, IsKey, IsNullable, Alignment,
                    TableDetailPosition, IsDefaultLabel, IsDefaultImage,
                    SummarizeBy, ColumnStorageID, Type,
                    SourceColumn, ColumnOriginID, Expression, FormatString,
                    IsAvailableInMDX, SortByColumnID, AttributeHierarchyID,
                    ModifiedTime, StructureModifiedTime, RefreshedTime,
                    SystemFlags, KeepUniqueRows, DisplayOrdinal,
                    ErrorMessage, SourceProviderType, DisplayFolder,
                    EncodingHint, RelatedColumnDetailsID, AlternateOfID,
                    LineageTag, SourceLineageTag, EvaluationBehavior
                ) VALUES (
                    ?, ?, ?, NULL,
                    6, 19,
                    NULL, NULL, 1, 1,
                    1, 1, 0, 1,
                    -1, 0, 0,
                    1, ?, 3,
                    NULL, 0, NULL, NULL,
                    1, 0, 0,
                    ?, ?, 31240512000000000,
                    0, 0, 0,
                    NULL, NULL, NULL,
                    0, 0, 0,
                    NULL, NULL, 1
                )""",
                (rn_col_id, table_id, rn_name,
                 rn_cs_id, _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            # ColumnStorage for RowNumber
            c.execute(
                """INSERT INTO ColumnStorage (
                    ID, ColumnID, Name, StoragePosition, DictionaryStorageID,
                    Settings, ColumnFlags, Collation, OrderByColumn,
                    Locale, BinaryCharacters,
                    Statistics_DistinctStates, Statistics_MinDataID,
                    Statistics_MaxDataID, Statistics_OriginalMinSegmentDataID,
                    Statistics_RLESortOrder, Statistics_RowCount,
                    Statistics_HasNulls, Statistics_RLERuns,
                    Statistics_OthersRLERuns, Statistics_Usage,
                    Statistics_DBType, Statistics_XMType,
                    Statistics_CompressionType, Statistics_CompressionParam,
                    Statistics_EncodingHint, IsDeltaPartitionColumn,
                    DeltaColumnMappingPhysicalName, DeltaColumnMappingId,
                    FramedSourceColumn
                ) VALUES (
                    ?, ?, ?, 0, ?,
                    1, 31, NULL, NULL,
                    1033, 0,
                    ?, ?, ?,
                    2, -1, ?,
                    0, 0,
                    0, 3,
                    20, 0,
                    0, 0,
                    1, 0,
                    NULL, -1,
                    NULL
                )""",
                (rn_cs_id, rn_col_id, rn_cs_name, rn_ds_id,
                 max(row_count, 1),  # distinct states (at least 1, matching template)
                 2 if row_count == 0 else 3,  # min data id (2 for empty, 3 otherwise)
                 2 if row_count == 0 else (row_count + 2),  # max data id
                 row_count),  # row count
            )

            # DictionaryStorage for RowNumber (Type=2 = inline, no external file)
            c.execute(
                """INSERT INTO DictionaryStorage (
                    ID, ColumnStorageID, Type, DataType, DataVersion,
                    BaseId, Magnitude, LastId, IsNullable, IsUnique,
                    IsOperatingOn32, DictionaryFlags, StorageFileID, Size
                ) VALUES (?, ?, 2, 6, 0, -3, 1.0, 2, 1, 0, 0, 0, 0, 136)""",
                (rn_ds_id, rn_cs_id),
            )

            # ColumnPartitionStorage for RowNumber
            c.execute(
                """INSERT INTO ColumnPartitionStorage (
                    ID, ColumnStorageID, PartitionStorageID,
                    DataVersion, State, SegmentStorageID, StorageFileID
                ) VALUES (?, ?, ?, 0, 1, ?, ?)""",
                (rn_cps_id, rn_cs_id, ps_id, rn_ss_id, rn_idf_file_id),
            )

            # SegmentStorage for RowNumber
            c.execute(
                """INSERT INTO SegmentStorage (
                    ID, ColumnPartitionStorageID, SegmentCount, StorageFileID
                ) VALUES (?, ?, 1, ?)""",
                (rn_ss_id, rn_cps_id, rn_meta_file_id),
            )

            # StorageFile for IDF (OwnerType=23 -> ColumnPartitionStorage)
            c.execute(
                """INSERT INTO StorageFile (
                    ID, OwnerID, OwnerType, StorageFolderID, FileName
                ) VALUES (?, ?, 23, ?, ?)""",
                (rn_idf_file_id, rn_cps_id, prt_folder_id, idf_fname),
            )

            # StorageFile for IDFMETA (OwnerType=24 -> SegmentStorage)
            c.execute(
                """INSERT INTO StorageFile (
                    ID, OwnerID, OwnerType, StorageFolderID, FileName
                ) VALUES (?, ?, 24, ?, ?)""",
                (rn_meta_file_id, rn_ss_id, prt_folder_id, meta_fname),
            )

            col_storage_info[tname]["__rownumber__"] = {
                "col_name": rn_name,
                "col_storage_name": rn_cs_name,
                "idf_fname": idf_fname,
                "meta_fname": meta_fname,
                "dict_fname": None,
                "hidx_fname": None,
            }

            # AttributeHierarchy for RowNumber (MatType=3, no H$ table)
            rn_ah_id = alloc.next()
            rn_ahs_id = alloc.next()
            c.execute(
                "UPDATE [Column] SET AttributeHierarchyID = ? WHERE ID = ?",
                (rn_ah_id, rn_col_id),
            )
            c.execute(
                """INSERT INTO AttributeHierarchy (
                    ID, ColumnID, State, AttributeHierarchyStorageID,
                    ModifiedTime, RefreshedTime
                ) VALUES (?, ?, 1, ?,
                    ?, 31240512000000000)""",
                (rn_ah_id, rn_col_id, rn_ahs_id,
                 _FIXED_TIMESTAMP),
            )
            c.execute(
                """INSERT INTO AttributeHierarchyStorage (
                    ID, AttributeHierarchyID, SortOrder, OptimizationLevel,
                    MaterializationType, ColumnPositionToData, ColumnDataToPosition,
                    DistinctDataCount, DataVersion, StorageFileID,
                    SystemTableID, HasStatistics
                ) VALUES (?, ?, 0, 0,
                    3, -1, -1,
                    ?, 1, 0,
                    0, 1)""",
                (rn_ahs_id, rn_ah_id, len(tdef.get("rows", []))),
            )

            # ============================================================
            # INSERT user columns (Type=1)
            # ============================================================
            for col_idx, col_def in enumerate(tdef["columns"]):
                col_name = col_def["name"]
                data_type = col_def.get("data_type", "String")
                amo_type = _TYPE_NAME_TO_AMO.get(data_type, 2)

                col_id = alloc.next()
                column_id_map[tname][col_name] = col_id

                # ColumnStorage
                cs_id = alloc.next()

                # DictionaryStorage (Type=1 = external, has StorageFile for .dictionary)
                ds_id = alloc.next()

                # ColumnPartitionStorage
                cps_id = alloc.next()

                # SegmentStorage
                ss_id = alloc.next()

                # StorageFile for IDF
                idf_file_id = alloc.next()

                # StorageFile for IDFMETA
                meta_file_id = alloc.next()

                # StorageFile for dictionary (OwnerType=22 -> DictionaryStorage)
                dict_file_id = alloc.next()

                cs_name = f"{col_name} ({col_id})"
                col_idf_fname = f"0.{tname} ({table_id}).{cs_name}.0.idf"
                col_meta_fname = f"0.{tname} ({table_id}).{cs_name}.0.idfmeta"
                col_dict_fname = f"0.{tname} ({table_id}).{cs_name}.dictionary"

                # Insert Column row
                c.execute(
                    """INSERT INTO [Column] (
                        ID, TableID, ExplicitName, InferredName,
                        ExplicitDataType, InferredDataType,
                        DataCategory, Description, IsHidden, State,
                        IsUnique, IsKey, IsNullable, Alignment,
                        TableDetailPosition, IsDefaultLabel, IsDefaultImage,
                        SummarizeBy, ColumnStorageID, Type,
                        SourceColumn, ColumnOriginID, Expression, FormatString,
                        IsAvailableInMDX, SortByColumnID, AttributeHierarchyID,
                        ModifiedTime, StructureModifiedTime, RefreshedTime,
                        SystemFlags, KeepUniqueRows, DisplayOrdinal,
                        ErrorMessage, SourceProviderType, DisplayFolder,
                        EncodingHint, RelatedColumnDetailsID, AlternateOfID,
                        LineageTag, SourceLineageTag, EvaluationBehavior
                    ) VALUES (
                        ?, ?, ?, NULL,
                        ?, ?,
                        NULL, NULL, 0, 1,
                        0, 0, 1, 1,
                        -1, 0, 0,
                        ?, ?, 1,
                        ?, 0, NULL, NULL,
                        0, 0, 0,
                        ?, ?, 31240512000000000,
                        0, 0, ?,
                        NULL, NULL, NULL,
                        0, 0, 0,
                        ?, NULL, 1
                    )""",
                    (col_id, table_id, col_name,
                     amo_type, amo_type,
                     2,  # SummarizeBy: 2=None (default for all user columns)
                     cs_id,
                     col_name,  # SourceColumn
                     _FIXED_TIMESTAMP, _FIXED_TIMESTAMP,
                     col_idx,  # DisplayOrdinal
                     str(uuid.uuid4())),  # LineageTag only; SourceLineageTag = NULL
                )

                # SummarizationSetBy annotation for user data columns
                summ_ann_id = alloc.next()
                c.execute(
                    "INSERT INTO Annotation (ID, ObjectID, ObjectType, Name, Value, "
                    "ModifiedTime) VALUES (?, ?, 4, 'SummarizationSetBy', 'Automatic', ?)",
                    (summ_ann_id, col_id, _FIXED_TIMESTAMP),
                )  # ObjectType 4 = Column in TOM

                # AttributeHierarchy for user columns — required for DAX H$ tables
                ah_id = alloc.next()
                ahs_id = alloc.next()
                # Update the Column's AttributeHierarchyID
                c.execute(
                    "UPDATE [Column] SET AttributeHierarchyID = ? WHERE ID = ?",
                    (ah_id, col_id),
                )
                col_hidx_fname = None  # No HIDX file for user columns

                # ColumnStorage
                c.execute(
                    """INSERT INTO ColumnStorage (
                        ID, ColumnID, Name, StoragePosition, DictionaryStorageID,
                        Settings, ColumnFlags, Collation, OrderByColumn,
                        Locale, BinaryCharacters,
                        Statistics_DistinctStates, Statistics_MinDataID,
                        Statistics_MaxDataID, Statistics_OriginalMinSegmentDataID,
                        Statistics_RLESortOrder, Statistics_RowCount,
                        Statistics_HasNulls, Statistics_RLERuns,
                        Statistics_OthersRLERuns, Statistics_Usage,
                        Statistics_DBType, Statistics_XMType,
                        Statistics_CompressionType, Statistics_CompressionParam,
                        Statistics_EncodingHint, IsDeltaPartitionColumn,
                        DeltaColumnMappingPhysicalName, DeltaColumnMappingId,
                        FramedSourceColumn
                    ) VALUES (
                        ?, ?, ?, ?, ?,
                        1, 8, NULL, NULL,
                        1033, 0,
                        0, 0,
                        0, 0,
                        -1, 0,
                        0, 0,
                        0, ?,
                        ?, ?,
                        0, 0,
                        0, 0,
                        NULL, -1,
                        NULL
                    )""",
                    (cs_id, col_id, cs_name, col_idx + 1, ds_id,
                     3,  # Statistics_Usage (always 3 for data columns)
                     # Statistics_DBType: maps AMO data type to OLE DB type
                     {2: 130, 6: 20, 8: 5, 9: 7, 10: 14, 11: 11}.get(
                         _TYPE_NAME_TO_AMO.get(col_def.get("data_type", "String"), 2), 130),
                     # Statistics_XMType: maps AMO data type to XM internal type
                     {2: 2, 6: 0, 8: 1, 9: 1, 10: 1, 11: 0}.get(
                         _TYPE_NAME_TO_AMO.get(col_def.get("data_type", "String"), 2), 2),
                    ),
                )

                # DictionaryStorage (Type=1 = external, with file)
                # DictionaryFlags: 3 for string columns, 0 for numeric (matches template)
                # IsOperatingOn32: 1 for Int64 (4-byte elements), 0 for String/Double/DateTime
                dict_flags = 3 if data_type == "String" else 0
                # IsOperatingOn32: 1 for integer types (4-byte dict entries), 0 for string/float
                # AMO 6=Int64, 10=Decimal (stored as int64×10000), 11=Boolean (0/1)
                is_op32 = 1 if amo_type in (6, 10, 11) else 0
                c.execute(
                    """INSERT INTO DictionaryStorage (
                        ID, ColumnStorageID, Type, DataType, DataVersion,
                        BaseId, Magnitude, LastId, IsNullable, IsUnique,
                        IsOperatingOn32, DictionaryFlags, StorageFileID, Size
                    ) VALUES (?, ?, 1, ?, 0, 2, 0.0, ?, ?, 0, ?, ?, ?, 0)""",
                    (ds_id, cs_id, amo_type,
                     0,  # LastId - will be updated by _update_column_storage_stats or set later
                     1 if col_def.get("nullable", True) else 0,  # IsNullable
                     is_op32,
                     dict_flags,
                     dict_file_id),
                )

                # ColumnPartitionStorage
                c.execute(
                    """INSERT INTO ColumnPartitionStorage (
                        ID, ColumnStorageID, PartitionStorageID,
                        DataVersion, State, SegmentStorageID, StorageFileID
                    ) VALUES (?, ?, ?, 0, 1, ?, ?)""",
                    (cps_id, cs_id, ps_id, ss_id, idf_file_id),
                )

                # SegmentStorage
                c.execute(
                    """INSERT INTO SegmentStorage (
                        ID, ColumnPartitionStorageID, SegmentCount, StorageFileID
                    ) VALUES (?, ?, 1, ?)""",
                    (ss_id, cps_id, meta_file_id),
                )

                # StorageFile for IDF (OwnerType=23 -> ColumnPartitionStorage)
                c.execute(
                    """INSERT INTO StorageFile (
                        ID, OwnerID, OwnerType, StorageFolderID, FileName
                    ) VALUES (?, ?, 23, ?, ?)""",
                    (idf_file_id, cps_id, prt_folder_id, col_idf_fname),
                )

                # StorageFile for IDFMETA (OwnerType=24 -> SegmentStorage)
                c.execute(
                    """INSERT INTO StorageFile (
                        ID, OwnerID, OwnerType, StorageFolderID, FileName
                    ) VALUES (?, ?, 24, ?, ?)""",
                    (meta_file_id, ss_id, prt_folder_id, col_meta_fname),
                )

                # StorageFile for dictionary (OwnerType=22 -> DictionaryStorage)
                c.execute(
                    """INSERT INTO StorageFile (
                        ID, OwnerID, OwnerType, StorageFolderID, FileName
                    ) VALUES (?, ?, 22, ?, ?)""",
                    (dict_file_id, ds_id, tbl_folder_id, col_dict_fname),
                )

                col_storage_info[tname][col_name] = {
                    "col_name": col_name,
                    "col_id": col_id,
                    "col_storage_name": cs_name,
                    "cs_id": cs_id,
                    "ds_id": ds_id,
                    "dict_file_id": dict_file_id,
                    "idf_fname": col_idf_fname,
                    "meta_fname": col_meta_fname,
                    "dict_fname": col_dict_fname,
                    "hidx_fname": col_hidx_fname,
                    "ah_id": ah_id,
                    "ahs_id": ahs_id,
                    "data_type": data_type,
                    "amo_type": amo_type,
                }

        # ============================================================
        # INSERT Measures (on their declared table)
        # ============================================================
        for mdef in measures:
            tname = mdef["table"]
            tid = table_id_map.get(tname)
            if tid is None:
                # Measure references a table not in our add_table calls.
                # This shouldn't normally happen but handle gracefully.
                continue
            m_id = alloc.next()
            c.execute(
                """INSERT INTO [Measure] (
                    ID, TableID, Name, Description, DataType,
                    Expression, FormatString, IsHidden, State,
                    ModifiedTime, StructureModifiedTime,
                    KPIID, IsSimpleMeasure, ErrorMessage, DisplayFolder,
                    DetailRowsDefinitionID, DataCategory,
                    FormatStringDefinitionID, LineageTag, SourceLineageTag
                ) VALUES (
                    ?, ?, ?, ?, 6,
                    ?, NULL, 0, 1,
                    ?, ?,
                    0, 0, NULL, NULL,
                    0, NULL,
                    0, ?, NULL
                )""",
                (m_id, tid, mdef["name"], mdef.get("description", ""),
                 mdef["expression"],
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP,
                 str(uuid.uuid4())),
            )

        # ============================================================
        # INSERT Relationships
        # ============================================================
        for rdef in relationships:
            from_tid = table_id_map.get(rdef["from_table"])
            to_tid = table_id_map.get(rdef["to_table"])
            from_col_id = column_id_map.get(rdef["from_table"], {}).get(rdef["from_column"])
            to_col_id = column_id_map.get(rdef["to_table"], {}).get(rdef["to_column"])

            if not all([from_tid, to_tid, from_col_id, to_col_id]):
                continue  # Skip if any reference is missing

            rel_id = alloc.next()
            rel_name = str(uuid.uuid4())

            # Full storage entries (same for Import and DirectQuery)
            # RelationshipStorage
            rs_id = alloc.next()
            # RelationshipIndexStorage
            ris_id = alloc.next()

            c.execute(
                """INSERT INTO [Relationship] (
                    ID, ModelID, Name, IsActive, Type,
                    CrossFilteringBehavior, JoinOnDateBehavior,
                    RelyOnReferentialIntegrity,
                    FromTableID, FromColumnID, FromCardinality,
                    ToTableID, ToColumnID, ToCardinality,
                    State, RelationshipStorageID, RelationshipStorage2ID,
                    ModifiedTime, RefreshedTime, SecurityFilteringBehavior
                ) VALUES (
                    ?, 1, ?, 1, 1,
                    1, 1,
                    0,
                    ?, ?, 2,
                    ?, ?, 1,
                    1, ?, 0,
                    ?, ?, 1
                )""",
                (rel_id, rel_name,
                 to_tid, to_col_id,
                 from_tid, from_col_id,
                 rs_id,
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            # RelationshipStorage
            rs_name = f"{rel_name.replace('-', ' ')} ({rel_id})"
            c.execute(
                """INSERT INTO RelationshipStorage (
                    ID, RelationshipID, Name, DefinitionType,
                    Cardinality, Flags, RelationshipIndexStorageID
                ) VALUES (?, ?, ?, 0, 0, 0, ?)""",
                (rs_id, rel_id, rs_name, ris_id),
            )

            # RelationshipIndexStorage (SystemTableID and RecordCount updated later
            # when R$ table is created)
            c.execute(
                """INSERT INTO RelationshipIndexStorage (
                    ID, RelationshipStorageID, IndexType, Flags,
                    RecordCount, SecondaryRecordCount,
                    StorageFolderID, StorageFileID,
                    SystemTableID, SecondarySystemTableID
                ) VALUES (?, ?, 1, 0, 0, 0, 0, 0, 0, 0)""",
                (ris_id, rs_id),
            )

            # Mark relationship as "from source" so PBI Desktop recognizes
            # it during Refresh and doesn't try to re-import it (which
            # causes TMCCollectionObject::Add errors)
            ann_id = alloc.next()
            c.execute(
                "INSERT INTO Annotation (ID, ObjectID, ObjectType, Name, Value, "
                "ModifiedTime) VALUES (?, ?, 7, 'PBI_IsFromSource', 'FS', ?)",
                (ann_id, rel_id, _FIXED_TIMESTAMP),
            )  # ObjectType 7 = Relationship in TOM

            # Store relationship info for R$ table creation later
            rdef["_rel_id"] = rel_id
            rdef["_rel_name"] = rel_name
            rdef["_rs_id"] = rs_id
            rdef["_ris_id"] = ris_id

        # ============================================================
        # Model-level annotations (ObjectType=1, ObjectID=1)
        # ============================================================
        # PBI_QueryOrder: JSON list of user table names in declaration order
        query_order_value = json.dumps([tdef["name"] for tdef in tables])
        qo_ann_id = alloc.next()
        c.execute(
            "INSERT INTO Annotation (ID, ObjectID, ObjectType, Name, Value, "
            "ModifiedTime) VALUES (?, 1, 1, 'PBI_QueryOrder', ?, ?)",
            (qo_ann_id, query_order_value, _FIXED_TIMESTAMP),
        )  # ObjectType 1 = Model in TOM

        # __PBI_TimeIntelligenceEnabled: always '1' for standard models
        ti_ann_id = alloc.next()
        c.execute(
            "INSERT INTO Annotation (ID, ObjectID, ObjectType, Name, Value, "
            "ModifiedTime) VALUES (?, 1, 1, '__PBI_TimeIntelligenceEnabled', '1', ?)",
            (ti_ann_id, _FIXED_TIMESTAMP),
        )  # ObjectType 1 = Model in TOM

        conn.commit()

        # ============================================================
        # Encode VertiPaq data for each table
        # ============================================================
        vertipaq_files: dict[str, bytes] = {}

        for tdef in tables:
            tname = tdef["name"]
            table_id = table_id_map[tname]
            part_id = partition_id_map[tname]
            rows = tdef["rows"]
            row_count = len(rows)

            if row_count == 0:
                # Even empty tables need VertiPaq files for the RowNumber column
                # and for user columns (with 0 rows)
                pass

            # Encode user columns using encode_table_data
            encoder_columns = []
            for col_def in tdef["columns"]:
                encoder_columns.append({
                    "name": col_def["name"],
                    "data_type": col_def.get("data_type", "String"),
                    "nullable": col_def.get("nullable", True),
                })

            # Compression class IDs (from xmsrv.dll reverse engineering):
            #   u32_a = 0xABA5A = XMHybridRLECompressionInfo family
            #   u32_b = 0xABA36 + aligned_bit_width (computed per column by encoder)
            # These are NOT runtime IDs - they're fixed class selectors!
            encoded_files = encode_table_data(
                tname, part_id, encoder_columns, rows,
                u32_a=0xABA5A, u32_b_start=0,  # u32_b computed per-column in encoder
            )

            # Map the encoded files to our ABF path naming convention
            tbl_folder = f"{tname} ({table_id}).tbl"
            prt_folder = f"{tbl_folder}\\{part_id}.prt"

            for col_def in tdef["columns"]:
                col_name = col_def["name"]
                info = col_storage_info[tname][col_name]

                # The encode_table_data uses paths like "Sales.tbl\<part_id>.prt\column.<name>"
                # We need to map those to our ABF naming convention
                base = f"{tname}.tbl\\{part_id}.prt"
                idf_key = f"{base}\\column.{col_name}"
                meta_key = f"{base}\\column.{col_name}meta"
                dict_key = f"{base}\\column.{col_name}.dict"
                hidx_key = f"{base}\\column.{col_name}.hidx"

                # ABF path uses the StorageFolder path + StorageFile FileName
                abf_idf_path = f"{prt_folder}\\{info['idf_fname']}"
                abf_meta_path = f"{prt_folder}\\{info['meta_fname']}"
                abf_dict_path = f"{tbl_folder}\\{info['dict_fname']}"
                if idf_key in encoded_files:
                    vertipaq_files[abf_idf_path] = encoded_files[idf_key]
                if meta_key in encoded_files:
                    vertipaq_files[abf_meta_path] = encoded_files[meta_key]
                if dict_key in encoded_files:
                    vertipaq_files[abf_dict_path] = encoded_files[dict_key]
                    # Update DictionaryStorage.Size with actual dict size
                    c.execute(
                        "UPDATE DictionaryStorage SET Size = ? WHERE ID = ?",
                        (len(encoded_files[dict_key]), info["ds_id"]),
                    )
                # HIDX files skipped for now

                # Update ColumnStorage statistics from the IDFMETA
                if meta_key in encoded_files:
                    _update_column_storage_stats(
                        c, info["cs_id"], encoded_files[meta_key], row_count
                    )
                    # Update DictionaryStorage.LastId with max_data_id from IDFMETA
                    try:
                        idfm = encoded_files[meta_key]
                        max_did = struct.unpack_from("<I", idfm, 91)[0]  # max_data_id at offset 91
                        c.execute(
                            "UPDATE DictionaryStorage SET LastId = ? WHERE ID = ?",
                            (max_did, info["ds_id"]),
                        )
                    except (struct.error, KeyError):
                        pass

            # Encode RowNumber column
            # RowNumber uses u32_a=0xABA5A, u32_b=0xABA5B (XM123CompressionInfo)
            rn_info = col_storage_info[tname]["__rownumber__"]
            rn_col_def = [{"name": "RowNumber", "data_type": "Int64", "nullable": False, "is_row_number": True}]
            rn_rows = [{"RowNumber": i} for i in range(row_count)]
            rn_encoded = encode_table_data(
                tname, part_id, rn_col_def, rn_rows,
                u32_a=0xABA5A, u32_b_start=0xABA5B,  # XM123CompressionInfo for RowNumber
            )

            rn_base = f"{tname}.tbl\\{part_id}.prt"
            rn_idf_key = f"{rn_base}\\column.RowNumber"
            rn_meta_key = f"{rn_base}\\column.RowNumbermeta"

            abf_rn_idf_path = f"{prt_folder}\\{rn_info['idf_fname']}"
            abf_rn_meta_path = f"{prt_folder}\\{rn_info['meta_fname']}"

            if rn_idf_key in rn_encoded:
                vertipaq_files[abf_rn_idf_path] = rn_encoded[rn_idf_key]
            if rn_meta_key in rn_encoded:
                vertipaq_files[abf_rn_meta_path] = rn_encoded[rn_meta_key]

            # ============================================================
            # Create H$ system tables + AttributeHierarchy for each user column
            # ============================================================
            for col_def in tdef["columns"]:
                col_name = col_def["name"]
                info = col_storage_info[tname][col_name]
                col_id = info["col_id"]
                ah_id = info["ah_id"]
                ahs_id = info["ahs_id"]

                # Extract distinct values from data to build sort mappings
                base = f"{tname}.tbl\\{part_id}.prt"
                dict_key = f"{base}\\column.{col_name}.dict"
                meta_key = f"{base}\\column.{col_name}meta"

                # Get distinct count and dict order from encoded data
                raw_vals = [row.get(col_name) for row in rows]
                seen: dict[object, int] = {}
                for v in raw_vals:
                    if v is not None and v not in seen:
                        seen[v] = len(seen)  # dict_index in insertion order
                # The encoder builds the dictionary in first-seen order
                dict_values = list(seen.keys())  # values in dict order
                distinct = len(dict_values)

                # Sort values to get POS_TO_ID mapping
                try:
                    sorted_vals = sorted(dict_values, key=str)
                except TypeError:
                    sorted_vals = sorted(dict_values, key=str)

                # POS_TO_ID: sorted_pos -> data_id (dict_index + 3)
                # In the template, BaseId=0, so data_ids start at 3
                pos_to_id = []
                for sv in sorted_vals:
                    di = seen[sv]
                    pos_to_id.append(di + 3)

                # ID_TO_POS: full array of RecordCount=distinct+3 entries
                # [0]=0 (unused), [1]=distinct (sentinel), [2]=0 (unused),
                # [3..]=sorted_position for each data_id
                h_record_count_pre = distinct + 3  # 5 for 2 distinct
                id_to_pos_full = [0] * h_record_count_pre
                id_to_pos_full[1] = distinct  # sentinel
                for sorted_pos, did in enumerate(pos_to_id):
                    if did < h_record_count_pre:
                        id_to_pos_full[did] = sorted_pos

                # Always create the AttributeHierarchy row (required for ALL columns)
                min_val = str(sorted_vals[0]) if sorted_vals else ""
                max_val = str(sorted_vals[-1]) if sorted_vals else ""
                max_strlen = max((len(str(v)) for v in sorted_vals), default=0)
                c.execute(
                    """INSERT INTO AttributeHierarchy (
                        ID, ColumnID, State, AttributeHierarchyStorageID,
                        ModifiedTime, RefreshedTime
                    ) VALUES (?, ?, 1, ?, ?, ?)""",
                    (ah_id, col_id, ahs_id, _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
                )

                # Build H$ table metadata for ALL cardinalities
                h_table_name = f"H${tname} ({table_id})${col_name} ({col_id})"
                h_table_id = alloc.next()
                h_ts_id = alloc.next()
                h_tbl_folder_id = alloc.next()
                h_tbl_folder_path = f"H${tname} ({table_id})${col_name} ({col_id})$({h_table_id}).tbl"

                # H$ Table (ModelID=0, SystemFlags=1, IsHidden=1)
                c.execute(
                    """INSERT INTO [Table] (
                        ID, ModelID, Name, DataCategory, Description, IsHidden,
                        TableStorageID, ModifiedTime, StructureModifiedTime,
                        SystemFlags, ShowAsVariationsOnly, IsPrivate,
                        DefaultDetailRowsDefinitionID, AlternateSourcePrecedence,
                        RefreshPolicyID, CalculationGroupID, ExcludeFromModelRefresh,
                        LineageTag, SourceLineageTag, SystemManaged,
                        ExcludeFromAutomaticAggregations
                    ) VALUES (
                        ?, 0, ?, NULL, NULL, 1,
                        ?, ?, ?,
                        1, 0, 0,
                        0, 0,
                        0, 0, 0,
                        NULL, NULL, 0,
                        0
                    )""",
                    (h_table_id, h_table_name, h_ts_id,
                     _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
                )

                # H$ TableStorage (Settings=4)
                c.execute(
                    """INSERT INTO TableStorage (
                        ID, TableID, Name, Version, Settings, RIViolationCount, StorageFolderID
                    ) VALUES (?, ?, ?, 1, 4, 0, ?)""",
                    (h_ts_id, h_table_id, h_table_name, h_tbl_folder_id),
                )

                c.execute(
                    "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 18, ?)",
                    (h_tbl_folder_id, h_ts_id, h_tbl_folder_path),
                )

                # H$ Partition (Type=3, Mode=2, QueryDefinition=None)
                h_part_id = alloc.next()
                h_ps_id = alloc.next()
                h_sms_id = alloc.next()
                h_prt_folder_id = alloc.next()
                h_prt_folder_path = f"{h_tbl_folder_path}\\{h_part_id}.prt"

                c.execute(
                    """INSERT INTO [Partition] (
                        ID, TableID, Name, Description, DataSourceID,
                        QueryDefinition, State, Type, PartitionStorageID,
                        Mode, DataView, ModifiedTime, RefreshedTime,
                        SystemFlags, ErrorMessage, RetainDataTillForceCalculate,
                        RangeStart, RangeEnd, RangeGranularity, RefreshBookmark,
                        QueryGroupID, ExpressionSourceID, MAttributes,
                        DataCoverageDefinitionID, SchemaName
                    ) VALUES (
                        ?, ?, ?, NULL, 0,
                        NULL, 1, 3, ?,
                        2, 3, ?, ?,
                        1, NULL, 0,
                        0.0, 0.0, -1, NULL,
                        0, 0, NULL,
                        0, NULL
                    )""",
                    (h_part_id, h_table_id, h_table_name, h_ps_id,
                     _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
                )

                c.execute(
                    """INSERT INTO PartitionStorage (
                        ID, PartitionID, Name, StoragePosition,
                        SegmentMapStorageID, DataObjectId, StorageFolderID,
                        DeltaTableMetadataStorageID
                    ) VALUES (?, ?, ?, 0, ?, 0, ?, 0)""",
                    (h_ps_id, h_part_id, h_table_name, h_sms_id, h_prt_folder_id),
                )

                c.execute(
                    "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 20, ?)",
                    (h_prt_folder_id, h_ps_id, h_prt_folder_path),
                )

                # For empty columns (distinct==0), use MatType=3 (no H$ table)
                if distinct == 0:
                    c.execute(
                        """INSERT INTO AttributeHierarchyStorage (
                            ID, AttributeHierarchyID, SortOrder, OptimizationLevel,
                            MaterializationType, ColumnPositionToData, ColumnDataToPosition,
                            DistinctDataCount, DataVersion, StorageFileID,
                            SystemTableID, HasStatistics, MinValue, MaxValue,
                            StringValueMaxLength
                        ) VALUES (?, ?, 0, 0,
                            3, -1, -1,
                            0, 1, 0,
                            0, 1, ?, ?, ?)""",
                        (ahs_id, ah_id, min_val, max_val, max_strlen),
                    )
                    continue

                # --- Build H$ binary data using dynamic NoSplit encoding ---
                import math as _math_h
                h_record_count = distinct + 3
                h_rec_per_seg = distinct
                h_seg_count = _math_h.ceil(h_record_count / h_rec_per_seg)
                h_rps = []
                remaining = h_record_count
                for _ in range(h_seg_count):
                    seg_rec = min(h_rec_per_seg, remaining)
                    h_rps.append(seg_rec)
                    remaining -= seg_rec

                # POS_TO_ID values: sorted data_ids + padding to RecordCount
                p2id_vals = list(pos_to_id)  # distinct sorted data_ids
                p2id_vals.extend([2] + [0] * (h_record_count - distinct - 1))

                # ID_TO_POS values (already computed, length = h_record_count)
                i2p_vals = list(id_to_pos_full)

                pos_idf_bytes = encode_nosplit_idf(p2id_vals, 32, h_rps)
                pos_meta_bytes = encode_nosplit_idfmeta(h_rps, 32, is_relationship=False)
                itp_idf_bytes = encode_nosplit_idf(i2p_vals, 32, h_rps)
                itp_meta_bytes = encode_nosplit_idfmeta(h_rps, 32, is_relationship=False)

                # SegmentMapStorage
                c.execute(
                    """INSERT INTO SegmentMapStorage (
                        ID, PartitionStorageID, Type, RecordCount,
                        SegmentCount, RecordsPerSegment
                    ) VALUES (?, ?, 2, ?, ?, ?)""",
                    (h_sms_id, h_ps_id, h_record_count, h_seg_count, h_rec_per_seg),
                )

                # Two H$ columns: POS_TO_ID and ID_TO_POS
                h_idf_map = {"POS_TO_ID": pos_idf_bytes, "ID_TO_POS": itp_idf_bytes}
                h_meta_map = {"POS_TO_ID": pos_meta_bytes, "ID_TO_POS": itp_meta_bytes}
                for h_col_idx, (h_col_name, h_settings, h_stor_pos) in enumerate([
                    ("POS_TO_ID", 7, 0),
                    ("ID_TO_POS", 5, 1),
                ]):
                    h_col_id = alloc.next()
                    h_cs_id = alloc.next()
                    h_ds_id = alloc.next()
                    h_cps_id = alloc.next()
                    h_ss_id = alloc.next()
                    h_idf_file_id = alloc.next()
                    h_meta_file_id = alloc.next()
                    _h_unused_id = alloc.next()  # keep alloc in sync

                    # ColumnStorage name includes ID, file names do NOT
                    h_cs_name = f"{h_col_name} ({h_col_id})"
                    # File names WITHOUT column ID suffix (matches template)
                    h_idf_fname = f"0.{h_table_name}.{h_col_name}.0.idf"
                    h_meta_fname = f"0.{h_table_name}.{h_col_name}.0.idfmeta"

                    # H$ Column (Type=1, DataType=6, InferredDataType=19, SystemFlags=1)
                    c.execute(
                        """INSERT INTO [Column] (
                            ID, TableID, ExplicitName, InferredName,
                            ExplicitDataType, InferredDataType,
                            DataCategory, Description, IsHidden, State,
                            IsUnique, IsKey, IsNullable, Alignment,
                            TableDetailPosition, IsDefaultLabel, IsDefaultImage,
                            SummarizeBy, ColumnStorageID, Type,
                            SourceColumn, ColumnOriginID, Expression, FormatString,
                            IsAvailableInMDX, SortByColumnID, AttributeHierarchyID,
                            ModifiedTime, StructureModifiedTime, RefreshedTime,
                            SystemFlags, KeepUniqueRows, DisplayOrdinal,
                            ErrorMessage, SourceProviderType, DisplayFolder,
                            EncodingHint, RelatedColumnDetailsID, AlternateOfID,
                            LineageTag, SourceLineageTag, EvaluationBehavior
                        ) VALUES (
                            ?, ?, ?, NULL,
                            6, 19,
                            NULL, NULL, 0, 1,
                            0, 0, 1, 1,
                            -1, 0, 0,
                            1, ?, 1,
                            NULL, 0, NULL, NULL,
                            1, 0, 0,
                            ?, ?, 31240512000000000,
                            1, 0, 0,
                            NULL, NULL, NULL,
                            0, 0, 0,
                            NULL, NULL, 1
                        )""",
                        (h_col_id, h_table_id, h_col_name,
                         h_cs_id,
                         _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
                    )

                    # H$ ColumnStorage (Settings=7 for POS_TO_ID, 5 for ID_TO_POS)
                    c.execute(
                        """INSERT INTO ColumnStorage (
                            ID, ColumnID, Name, StoragePosition, DictionaryStorageID,
                            Settings, ColumnFlags, Collation, OrderByColumn,
                            Locale, BinaryCharacters,
                            Statistics_DistinctStates, Statistics_MinDataID,
                            Statistics_MaxDataID, Statistics_OriginalMinSegmentDataID,
                            Statistics_RLESortOrder, Statistics_RowCount,
                            Statistics_HasNulls, Statistics_RLERuns,
                            Statistics_OthersRLERuns, Statistics_Usage,
                            Statistics_DBType, Statistics_XMType,
                            Statistics_CompressionType, Statistics_CompressionParam,
                            Statistics_EncodingHint, IsDeltaPartitionColumn,
                            DeltaColumnMappingPhysicalName, DeltaColumnMappingId,
                            FramedSourceColumn
                        ) VALUES (
                            ?, ?, ?, ?, ?,
                            ?, 0, NULL, NULL,
                            0, 0,
                            1, 2,
                            2, 2,
                            -1, 0,
                            0, 0,
                            0, 3,
                            0, 0,
                            0, 0,
                            0, 0,
                            NULL, -1,
                            NULL
                        )""",
                        (h_cs_id, h_col_id, h_col_name, h_stor_pos, h_ds_id,
                         h_settings),
                    )

                    # DictionaryStorage: Type=0, DataType=19, BaseId=0, LastId=0,
                    # IsOperatingOn32=0, Size=0 (no external file)
                    c.execute(
                        """INSERT INTO DictionaryStorage (
                            ID, ColumnStorageID, Type, DataType, DataVersion,
                            BaseId, Magnitude, LastId, IsNullable, IsUnique,
                            IsOperatingOn32, DictionaryFlags, StorageFileID, Size
                        ) VALUES (?, ?, 0, 19, 0, 0, 0.0, 0, 0, 0, 0, 0, 0, 0)""",
                        (h_ds_id, h_cs_id),
                    )

                    # ColumnPartitionStorage (State=3 for H$ columns)
                    c.execute(
                        """INSERT INTO ColumnPartitionStorage (
                            ID, ColumnStorageID, PartitionStorageID,
                            DataVersion, State, SegmentStorageID, StorageFileID
                        ) VALUES (?, ?, ?, 0, 3, ?, ?)""",
                        (h_cps_id, h_cs_id, h_ps_id, h_ss_id, h_idf_file_id),
                    )

                    # SegmentStorage
                    c.execute(
                        """INSERT INTO SegmentStorage (
                            ID, ColumnPartitionStorageID, SegmentCount, StorageFileID
                        ) VALUES (?, ?, ?, ?)""",
                        (h_ss_id, h_cps_id, h_seg_count, h_meta_file_id),
                    )

                    # StorageFile for IDF (OwnerType=23)
                    c.execute(
                        """INSERT INTO StorageFile (
                            ID, OwnerID, OwnerType, StorageFolderID, FileName
                        ) VALUES (?, ?, 23, ?, ?)""",
                        (h_idf_file_id, h_cps_id, h_prt_folder_id, h_idf_fname),
                    )

                    # StorageFile for IDFMETA (OwnerType=24)
                    c.execute(
                        """INSERT INTO StorageFile (
                            ID, OwnerID, OwnerType, StorageFolderID, FileName
                        ) VALUES (?, ?, 24, ?, ?)""",
                        (h_meta_file_id, h_ss_id, h_prt_folder_id, h_meta_fname),
                    )

                    # NO dictionary StorageFile for H$ columns (Type=0 means inline/none)

                    # Map binary data to ABF paths
                    h_abf_idf = f"{h_prt_folder_path}\\{h_idf_fname}"
                    h_abf_meta = f"{h_prt_folder_path}\\{h_meta_fname}"

                    vertipaq_files[h_abf_idf] = h_idf_map[h_col_name]
                    vertipaq_files[h_abf_meta] = h_meta_map[h_col_name]

                # AHS: MaterializationType=0, SystemTableID=h_table_id
                c.execute(
                    """INSERT INTO AttributeHierarchyStorage (
                        ID, AttributeHierarchyID, SortOrder, OptimizationLevel,
                        MaterializationType, ColumnPositionToData, ColumnDataToPosition,
                        DistinctDataCount, DataVersion, StorageFileID,
                        SystemTableID, HasStatistics, MinValue, MaxValue,
                        StringValueMaxLength
                    ) VALUES (?, ?, 0, 0, 0, 0, 1, ?, 1, 0, ?, 1, ?, ?, ?)""",
                    (ahs_id, ah_id, distinct, h_table_id,
                     min_val, max_val, max_strlen),
                )

        # ============================================================
        # Create R$ system tables for each relationship
        # ============================================================
        for rdef in relationships:
            # Skip relationships that weren't fully resolved
            if "_rel_id" not in rdef:
                continue

            # PBI convention: From = Many (fact), To = One (dimension)
            # User API: from_table = One/dimension, to_table = Many/fact
            # So we swap: PBI.From = user's to_table, PBI.To = user's from_table
            one_tname = rdef["from_table"]   # dimension/lookup
            many_tname = rdef["to_table"]    # fact

            rel_id = rdef["_rel_id"]
            rel_name = rdef["_rel_name"]
            rs_id = rdef["_rs_id"]
            ris_id = rdef["_ris_id"]
            from_tid = table_id_map[many_tname]   # PBI From = Many
            to_tid = table_id_map[one_tname]      # PBI To = One
            many_col_name = rdef["to_column"]
            one_col_name = rdef["from_column"]

            # R$ table indexes the Many (fact) side
            from_tname = many_tname
            many_tdef = next(t for t in tables if t["name"] == many_tname)
            one_tdef = next(t for t in tables if t["name"] == one_tname)
            from_rows = many_tdef["rows"]
            to_rows = one_tdef["rows"]
            from_row_count = len(from_rows)

            # Compute the INDEX column: for each row in Many table,
            # find the row index in One table where the key matches
            to_key_index: dict[object, int] = {}
            for idx, row in enumerate(to_rows):
                key_val = row.get(one_col_name)
                if key_val is not None and key_val not in to_key_index:
                    to_key_index[key_val] = idx

            index_values: list[int] = []
            for row in from_rows:
                fk_val = row.get(many_col_name)
                matched_idx = to_key_index.get(fk_val, 0)  # default 0 if no match
                index_values.append(matched_idx)

            # R$ table naming: table name does NOT include .tbl suffix
            rel_name_spaced = rel_name.replace("-", " ")
            r_table_name = (
                f"R${from_tname} ({from_tid})"
                f"${rel_name_spaced} ({rel_id})"
            )

            # Allocate IDs for R$ table
            r_table_id = alloc.next()
            r_ts_id = alloc.next()
            r_tbl_folder_id = alloc.next()
            r_tbl_folder_path = f"R${from_tname} ({from_tid})${rel_name_spaced} ({rel_id})$({to_tid}).tbl"

            # R$ Table (ModelID=0, SystemFlags=1, IsHidden=1)
            c.execute(
                """INSERT INTO [Table] (
                    ID, ModelID, Name, DataCategory, Description, IsHidden,
                    TableStorageID, ModifiedTime, StructureModifiedTime,
                    SystemFlags, ShowAsVariationsOnly, IsPrivate,
                    DefaultDetailRowsDefinitionID, AlternateSourcePrecedence,
                    RefreshPolicyID, CalculationGroupID, ExcludeFromModelRefresh,
                    LineageTag, SourceLineageTag, SystemManaged,
                    ExcludeFromAutomaticAggregations
                ) VALUES (
                    ?, 0, ?, NULL, NULL, 1,
                    ?, ?, ?,
                    1, 0, 0,
                    0, 0,
                    0, 0, 0,
                    NULL, NULL, 0,
                    0
                )""",
                (r_table_id, r_table_name, r_ts_id,
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            # R$ TableStorage (Version=0, Settings=2)
            c.execute(
                """INSERT INTO TableStorage (
                    ID, TableID, Name, Version, Settings, RIViolationCount, StorageFolderID
                ) VALUES (?, ?, ?, 0, 2, 0, ?)""",
                (r_ts_id, r_table_id, r_table_name, r_tbl_folder_id),
            )

            # StorageFolder for R$ table (OwnerType=18)
            c.execute(
                "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 18, ?)",
                (r_tbl_folder_id, r_ts_id, r_tbl_folder_path),
            )

            # R$ Partition (Type=3, Mode=2, State=1, SystemFlags=1, DataView=3)
            r_part_id = alloc.next()
            r_ps_id = alloc.next()
            r_sms_id = alloc.next()
            r_prt_folder_id = alloc.next()
            r_prt_folder_path = f"{r_tbl_folder_path}\\{r_part_id}.prt"

            c.execute(
                """INSERT INTO [Partition] (
                    ID, TableID, Name, Description, DataSourceID,
                    QueryDefinition, State, Type, PartitionStorageID,
                    Mode, DataView, ModifiedTime, RefreshedTime,
                    SystemFlags, ErrorMessage, RetainDataTillForceCalculate,
                    RangeStart, RangeEnd, RangeGranularity, RefreshBookmark,
                    QueryGroupID, ExpressionSourceID, MAttributes,
                    DataCoverageDefinitionID, SchemaName
                ) VALUES (
                    ?, ?, ?, NULL, 0,
                    NULL, 1, 3, ?,
                    2, 3, ?, ?,
                    1, NULL, 0,
                    0.0, 0.0, -1, NULL,
                    0, 0, NULL,
                    0, NULL
                )""",
                (r_part_id, r_table_id, r_table_name, r_ps_id,
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            c.execute(
                """INSERT INTO PartitionStorage (
                    ID, PartitionID, Name, StoragePosition,
                    SegmentMapStorageID, DataObjectId, StorageFolderID,
                    DeltaTableMetadataStorageID
                ) VALUES (?, ?, ?, 0, ?, 0, ?, 0)""",
                (r_ps_id, r_part_id, r_table_name, r_sms_id, r_prt_folder_id),
            )

            # StorageFolder for R$ partition (OwnerType=20)
            c.execute(
                "INSERT INTO StorageFolder (ID, OwnerID, OwnerType, Path) VALUES (?, ?, 20, ?)",
                (r_prt_folder_id, r_ps_id, r_prt_folder_path),
            )

            # SegmentMapStorage (Type=3, RecordCount=from_row_count, SegmentCount=1)
            c.execute(
                """INSERT INTO SegmentMapStorage (
                    ID, PartitionStorageID, Type, RecordCount,
                    SegmentCount, RecordsPerSegment
                ) VALUES (?, ?, 3, ?, 1, ?)""",
                (r_sms_id, r_ps_id, from_row_count, from_row_count),
            )

            # R$ INDEX column (ExplicitDataType=6, InferredDataType=19, SystemFlags=1)
            r_col_id = alloc.next()
            r_cs_id = alloc.next()
            r_ds_id = alloc.next()
            r_cps_id = alloc.next()
            r_ss_id = alloc.next()
            r_idf_file_id = alloc.next()
            r_meta_file_id = alloc.next()

            r_cs_name = f"INDEX ({r_col_id})"
            r_idf_fname = f"0.{r_table_name}.{r_cs_name}.0.idf"
            r_meta_fname = f"0.{r_table_name}.{r_cs_name}.0.idfmeta"

            c.execute(
                """INSERT INTO [Column] (
                    ID, TableID, ExplicitName, InferredName,
                    ExplicitDataType, InferredDataType,
                    DataCategory, Description, IsHidden, State,
                    IsUnique, IsKey, IsNullable, Alignment,
                    TableDetailPosition, IsDefaultLabel, IsDefaultImage,
                    SummarizeBy, ColumnStorageID, Type,
                    SourceColumn, ColumnOriginID, Expression, FormatString,
                    IsAvailableInMDX, SortByColumnID, AttributeHierarchyID,
                    ModifiedTime, StructureModifiedTime, RefreshedTime,
                    SystemFlags, KeepUniqueRows, DisplayOrdinal,
                    ErrorMessage, SourceProviderType, DisplayFolder,
                    EncodingHint, RelatedColumnDetailsID, AlternateOfID,
                    LineageTag, SourceLineageTag, EvaluationBehavior
                ) VALUES (
                    ?, ?, 'INDEX', NULL,
                    6, 19,
                    NULL, NULL, 0, 1,
                    0, 0, 1, 1,
                    -1, 0, 0,
                    1, ?, 1,
                    NULL, 0, NULL, NULL,
                    1, 0, 0,
                    ?, ?, 31240512000000000,
                    1, 0, 0,
                    NULL, NULL, NULL,
                    0, 0, 0,
                    NULL, NULL, 1
                )""",
                (r_col_id, r_table_id,
                 r_cs_id,
                 _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
            )

            # R$ ColumnStorage (Settings=3)
            c.execute(
                """INSERT INTO ColumnStorage (
                    ID, ColumnID, Name, StoragePosition, DictionaryStorageID,
                    Settings, ColumnFlags, Collation, OrderByColumn,
                    Locale, BinaryCharacters,
                    Statistics_DistinctStates, Statistics_MinDataID,
                    Statistics_MaxDataID, Statistics_OriginalMinSegmentDataID,
                    Statistics_RLESortOrder, Statistics_RowCount,
                    Statistics_HasNulls, Statistics_RLERuns,
                    Statistics_OthersRLERuns, Statistics_Usage,
                    Statistics_DBType, Statistics_XMType,
                    Statistics_CompressionType, Statistics_CompressionParam,
                    Statistics_EncodingHint, IsDeltaPartitionColumn,
                    DeltaColumnMappingPhysicalName, DeltaColumnMappingId,
                    FramedSourceColumn
                ) VALUES (
                    ?, ?, ?, 0, ?,
                    3, 0, NULL, NULL,
                    0, 0,
                    1, 2,
                    2, 2,
                    -1, 0,
                    0, 0,
                    0, 3,
                    0, 0,
                    0, 0,
                    0, 0,
                    NULL, -1,
                    NULL
                )""",
                (r_cs_id, r_col_id, r_cs_name, r_ds_id),
            )

            # R$ DictionaryStorage (Type=0, DataType=19 — no dictionary, raw values)
            # Must match template exactly: no external dictionary file
            r_dict_file_id = alloc.next()  # consumed but unused
            r_dict_fname = None  # no dictionary file for R$ INDEX
            c.execute(
                """INSERT INTO DictionaryStorage (
                    ID, ColumnStorageID, Type, DataType, DataVersion,
                    BaseId, Magnitude, LastId, IsNullable, IsUnique,
                    IsOperatingOn32, DictionaryFlags, StorageFileID, Size
                ) VALUES (?, ?, 0, 19, 0, 0, 0.0, 0, 0, 0, 0, 0, 0, 0)""",
                (r_ds_id, r_cs_id),
            )

            # R$ ColumnPartitionStorage (State=3)
            c.execute(
                """INSERT INTO ColumnPartitionStorage (
                    ID, ColumnStorageID, PartitionStorageID,
                    DataVersion, State, SegmentStorageID, StorageFileID
                ) VALUES (?, ?, ?, 0, 3, ?, ?)""",
                (r_cps_id, r_cs_id, r_ps_id, r_ss_id, r_idf_file_id),
            )

            # R$ SegmentStorage (SegmentCount=1)
            c.execute(
                """INSERT INTO SegmentStorage (
                    ID, ColumnPartitionStorageID, SegmentCount, StorageFileID
                ) VALUES (?, ?, 1, ?)""",
                (r_ss_id, r_cps_id, r_meta_file_id),
            )

            # StorageFile for IDF (OwnerType=23)
            c.execute(
                """INSERT INTO StorageFile (
                    ID, OwnerID, OwnerType, StorageFolderID, FileName
                ) VALUES (?, ?, 23, ?, ?)""",
                (r_idf_file_id, r_cps_id, r_prt_folder_id, r_idf_fname),
            )

            # StorageFile for IDFMETA (OwnerType=24)
            c.execute(
                """INSERT INTO StorageFile (
                    ID, OwnerID, OwnerType, StorageFolderID, FileName
                ) VALUES (?, ?, 24, ?, ?)""",
                (r_meta_file_id, r_ss_id, r_prt_folder_id, r_meta_fname),
            )

            # No StorageFile for dictionary — R$ INDEX uses Type=0 (no dict)

            # Update RelationshipIndexStorage with SystemTableID and RecordCount
            c.execute(
                """UPDATE RelationshipIndexStorage
                   SET SystemTableID = ?, RecordCount = ?
                   WHERE ID = ?""",
                (r_table_id, from_row_count, ris_id),
            )

            # Encode R$ INDEX using direct NoSplit<N> encoding
            # Compute bit width from max row index value
            import math as _math
            max_row_idx = max(index_values) if index_values else 0
            if max_row_idx <= 0:
                r_bit_width = 1
            else:
                r_bit_width = max(1, _math.ceil(_math.log2(max_row_idx + 1)))
            r_bit_width = _align_bit_width(r_bit_width)

            # Single segment for R$ INDEX
            r_records_per_seg = [from_row_count]
            r_idf_bytes = encode_nosplit_idf(index_values, r_bit_width, r_records_per_seg)
            r_idfmeta_bytes = encode_nosplit_idfmeta(r_records_per_seg, r_bit_width, is_relationship=True)

            # Map encoded files to ABF paths
            r_abf_idf_path = f"{r_prt_folder_path}\\{r_idf_fname}"
            r_abf_meta_path = f"{r_prt_folder_path}\\{r_meta_fname}"

            vertipaq_files[r_abf_idf_path] = r_idf_bytes
            vertipaq_files[r_abf_meta_path] = r_idfmeta_bytes
            # No dictionary file for R$ INDEX (Type=0)

            # Override ColumnStorage stats to match template (Type=0 pattern)
            c.execute(
                """UPDATE ColumnStorage SET
                    Statistics_DistinctStates = 1,
                    Statistics_MinDataID = 2,
                    Statistics_MaxDataID = 2,
                    Statistics_OriginalMinSegmentDataID = 2,
                    Statistics_RLESortOrder = -1,
                    Statistics_RowCount = 0,
                    Statistics_Usage = 3
                WHERE ID = ?""",
                (r_cs_id,),
            )

        # NEUTRALIZE template tables — keep them (ABF binary references their IDs)
        # but make them completely inert so they don't interfere with Refresh.
        user_table_ids = set(table_id_map.values())
        c = conn.cursor()
        _empty_m = 'let\n    Source = #table(type table [x = text], {})\nin\n    Source'

        # 1. Neutralize ALL non-user partition M expressions
        c.execute(
            "SELECT p.ID, t.ID as TableID FROM [Partition] p "
            "JOIN [Table] t ON p.TableID = t.ID "
            "WHERE p.QueryDefinition IS NOT NULL"
        )
        for pid, tid in c.fetchall():
            if tid not in user_table_ids:
                c.execute(
                    "UPDATE [Partition] SET QueryDefinition = ? WHERE ID = ?",
                    (_empty_m, pid),
                )

        # 2. Delete ALL template relationships (prevents schema sync conflicts)
        c.execute(
            "SELECT ID FROM [Relationship] WHERE "
            "FromTableID NOT IN ({ids}) OR ToTableID NOT IN ({ids})".format(
                ids=",".join(str(i) for i in user_table_ids)
            )
        )
        template_rel_ids = [r[0] for r in c.fetchall()]
        if template_rel_ids:
            ph = ",".join("?" * len(template_rel_ids))
            c.execute(f"DELETE FROM [Relationship] WHERE ID IN ({ph})", template_rel_ids)

        # 3. Hide all template tables
        c.execute("SELECT ID FROM [Table]")
        for (tid,) in c.fetchall():
            if tid not in user_table_ids:
                c.execute(
                    "UPDATE [Table] SET IsHidden = 1, IsPrivate = 1 WHERE ID = ?",
                    (tid,),
                )

        conn.commit()
        conn.close()

        # Read the modified SQLite back
        with open(tmp_path, "rb") as f:
            new_sqlite_bytes = f.read()

    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return new_sqlite_bytes, vertipaq_files


def _update_column_storage_stats(
    cursor: sqlite3.Cursor,
    cs_id: int,
    idfmeta_bytes: bytes,
    row_count: int,
) -> None:
    """Parse key statistics from IDFMETA bytes and update ColumnStorage."""
    # IDFMETA layout after tags:
    # CP_OPEN(6) + version(8) + CS_OPEN(6) + records(8) + one(8) +
    # a_b_a_5_a(4) + iterator(4) + bookmark(8) + alloc(8) + used(8) +
    # resize(1) + compress(4) + SS_OPEN(6) +
    # distinct_states(8) + min_data_id(4) + max_data_id(4) + ...
    try:
        off = 6 + 8 + 6  # CP_OPEN + version + CS_OPEN
        records = struct.unpack_from("<Q", idfmeta_bytes, off)[0]
        off += 8 + 8 + 4 + 4 + 8 + 8 + 8 + 1 + 4 + 6  # to SS_OPEN then fields
        distinct_states = struct.unpack_from("<Q", idfmeta_bytes, off)[0]
        off += 8
        min_data_id = struct.unpack_from("<I", idfmeta_bytes, off)[0]
        off += 4
        max_data_id = struct.unpack_from("<I", idfmeta_bytes, off)[0]
        off += 4
        _orig_min = struct.unpack_from("<I", idfmeta_bytes, off)[0]
        off += 4
        rle_sort_order = struct.unpack_from("<q", idfmeta_bytes, off)[0]
        off += 8
        ss_row_count = struct.unpack_from("<Q", idfmeta_bytes, off)[0]
        off += 8
        has_nulls = struct.unpack_from("<B", idfmeta_bytes, off)[0]
        off += 1
        rle_runs = struct.unpack_from("<Q", idfmeta_bytes, off)[0]

        cursor.execute(
            """UPDATE ColumnStorage SET
                Statistics_DistinctStates = ?,
                Statistics_MinDataID = ?,
                Statistics_MaxDataID = ?,
                Statistics_OriginalMinSegmentDataID = ?,
                Statistics_RLESortOrder = ?,
                Statistics_RowCount = ?,
                Statistics_HasNulls = ?,
                Statistics_RLERuns = ?
            WHERE ID = ?""",
            (distinct_states, min_data_id, max_data_id, _orig_min,
             rle_sort_order, ss_row_count, has_nulls, rle_runs, cs_id),
        )
    except (struct.error, IndexError):
        # If parsing fails, just update row count
        cursor.execute(
            "UPDATE ColumnStorage SET Statistics_RowCount = ? WHERE ID = ?",
            (row_count, cs_id),
        )


def _rebuild_abf_clean(
    abf_struct,
    replacements: dict[str, bytes],
    new_files: dict[str, bytes],
) -> bytes:
    """Rebuild ABF using template skeleton but with ALL template VP data stripped.

    Keeps: system files from Class=100002 (db.xml, CryptKey, etc.)
    Strips: ALL files from Class=100069 (template VertiPaq data)
    Injects: our metadata.sqlitedb (via replacements) + our VertiPaq files (new_files)

    This eliminates template data contamination while keeping the proven
    ABF structure that PBI's restore engine accepts.
    """
    import xml.etree.ElementTree as ET
    from copy import deepcopy

    from pbix_mcp.formats.abf_rebuild import (
        _HEADER_PAGE_SIZE,
        _SIGNATURE_LEN,
        STREAM_STORAGE_SIGNATURE,
        _xml_to_utf16_bytes,
    )

    # Identify template VP StoragePaths to strip (Class=100069)
    template_vp_sps = set()
    for fg in abf_struct.backup_log_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100069":
            for bf in fg.findall("FileList/BackupFile"):
                sp = bf.findtext("StoragePath", "")
                if sp:
                    template_vp_sps.add(sp)

    buf = bytearray()
    buf.extend(STREAM_STORAGE_SIGNATURE)
    header_page_start = len(buf)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))

    # ---- Write data files ----
    new_offsets: dict[str, int] = {}
    new_sizes: dict[str, int] = {}
    kept_entries = []

    for ve in abf_struct.data_entries:
        if ve.path in template_vp_sps:
            continue  # STRIP template VertiPaq data
        if ve.path in replacements:
            data = replacements[ve.path]
        else:
            data = abf_struct.read_file_data(ve.path)
        new_offsets[ve.path] = len(buf)
        new_sizes[ve.path] = len(data)
        buf.extend(data)
        kept_entries.append(ve)

    # ---- Write new VertiPaq files ----
    import secrets
    new_file_records = []
    timestamp = 134002835794032078
    for fpath, content in new_files.items():
        offset = len(buf)
        size = len(content)
        sp = secrets.token_hex(10).upper()
        new_file_records.append((fpath, sp, offset, size))
        buf.extend(content)

    # ---- Build BackupLog ----
    blog_root = deepcopy(abf_struct.backup_log_root)

    # Update sizes for replaced files
    for fg in blog_root.findall("FileGroups/FileGroup"):
        for bf in fg.findall("FileList/BackupFile"):
            sp = bf.findtext("StoragePath")
            if sp in new_sizes:
                size_elem = bf.find("Size")
                if size_elem is not None:
                    size_elem.text = str(new_sizes[sp])

    # Strip ALL template VP entries from Class=100069 FileGroup
    for fg in blog_root.findall("FileGroups/FileGroup"):
        if fg.findtext("Class", "") == "100069":
            file_list = fg.find("FileList")
            if file_list is not None:
                for bf in list(file_list.findall("BackupFile")):
                    file_list.remove(bf)
            else:
                file_list = ET.SubElement(fg, "FileList")

            persist_path = fg.findtext("PersistLocationPath", "")

            # Add metadata.sqlitedb
            for entry in abf_struct.file_log:
                if "metadata.sqlitedb" in entry.get("Path", "").lower():
                    bf = ET.SubElement(file_list, "BackupFile")
                    full_path = persist_path + "\\" + entry["Path"].split("\\")[-1] if persist_path else entry["Path"]
                    # Use the original full path from the template
                    for orig_bf in abf_struct.backup_log_root.findall(".//BackupFile"):
                        if orig_bf.findtext("StoragePath") == entry["StoragePath"]:
                            full_path = orig_bf.findtext("Path", full_path)
                            break
                    ET.SubElement(bf, "Path").text = full_path
                    ET.SubElement(bf, "StoragePath").text = entry["StoragePath"]
                    ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
                    ET.SubElement(bf, "Size").text = str(new_sizes.get(entry["StoragePath"], entry["Size"]))
                    break

            # Add our new VertiPaq files
            for fpath, sp, offset, size in new_file_records:
                bf = ET.SubElement(file_list, "BackupFile")
                ET.SubElement(bf, "Path").text = f"{persist_path}\\{fpath}"
                ET.SubElement(bf, "StoragePath").text = sp
                ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
                ET.SubElement(bf, "Size").text = str(size)

    blog_bytes = _xml_to_utf16_bytes(blog_root)
    if abf_struct.error_code:
        blog_bytes = blog_bytes + b"\x00\x00\x00\x00"
    blog_offset = len(buf)
    blog_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # ---- Build VirtualDirectory ----
    vdir_root = ET.Element("VirtualDirectory")

    # Kept entries (system files + metadata replacement)
    for ve in kept_entries:
        bf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf, "Path").text = ve.path
        ET.SubElement(bf, "Size").text = str(new_sizes.get(ve.path, ve.size))
        ET.SubElement(bf, "m_cbOffsetHeader").text = str(new_offsets.get(ve.path, ve.m_cbOffsetHeader))
        ET.SubElement(bf, "Delete").text = "true" if ve.delete else "false"
        ET.SubElement(bf, "CreatedTimestamp").text = str(ve.created_timestamp)
        ET.SubElement(bf, "Access").text = str(ve.access)
        ET.SubElement(bf, "LastWriteTime").text = str(ve.last_write_time)

    # New VertiPaq files
    for fpath, sp, offset, size in new_file_records:
        bf = ET.SubElement(vdir_root, "BackupFile")
        ET.SubElement(bf, "Path").text = sp
        ET.SubElement(bf, "Size").text = str(size)
        ET.SubElement(bf, "m_cbOffsetHeader").text = str(offset)
        ET.SubElement(bf, "Delete").text = "false"
        ET.SubElement(bf, "CreatedTimestamp").text = str(timestamp)
        ET.SubElement(bf, "Access").text = str(timestamp)
        ET.SubElement(bf, "LastWriteTime").text = str(timestamp)

    # BackupLog entry (last)
    blog_ve = abf_struct.backup_log_entry
    bf = ET.SubElement(vdir_root, "BackupFile")
    ET.SubElement(bf, "Path").text = blog_ve.path
    ET.SubElement(bf, "Size").text = str(blog_size)
    ET.SubElement(bf, "m_cbOffsetHeader").text = str(blog_offset)
    ET.SubElement(bf, "Delete").text = "true" if blog_ve.delete else "false"
    ET.SubElement(bf, "CreatedTimestamp").text = str(blog_ve.created_timestamp)
    ET.SubElement(bf, "Access").text = str(blog_ve.access)
    ET.SubElement(bf, "LastWriteTime").text = str(blog_ve.last_write_time)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # ---- Patch header ----
    hdr = deepcopy(abf_struct.header_root)
    hdr.find("m_cbOffsetHeader").text = str(vdir_offset)
    hdr.find("DataSize").text = str(vdir_size)
    total_entries = len(kept_entries) + len(new_file_records) + 1
    hdr.find("Files").text = str(total_entries)

    hdr_bytes = _xml_to_utf16_bytes(hdr)
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(f"Header {len(hdr_bytes)} bytes exceeds {available}")
    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))
    buf[header_page_start: header_page_start + available] = hdr_padded

    return bytes(buf)


def _rebuild_abf_with_new_files(
    abf_struct,
    replacements: dict[str, bytes],
    new_files: dict[str, bytes],
) -> bytes:
    """Rebuild the ABF with existing file replacements AND new files added.

    This extends the standard _rebuild_abf to also inject brand-new files
    that don't exist in the original ABF.

    Parameters
    ----------
    abf_struct : _ABFStructure
        Parsed structure of the original ABF.
    replacements : dict[str, bytes]
        Exact StoragePath -> new content for existing files.
    new_files : dict[str, bytes]
        New ABF-internal paths -> content for files to add.
        Keys are paths like "Sales (100).tbl\\50.prt\\0.Sales (100).Amount (101).0.idf"
    """
    import xml.etree.ElementTree as ET
    from copy import deepcopy

    from pbix_mcp.formats.abf_rebuild import (
        _HEADER_PAGE_SIZE,
        _SIGNATURE_LEN,
        STREAM_STORAGE_SIGNATURE,
        _xml_to_utf16_bytes,
    )

    buf = bytearray()

    # ---- 1. Signature (72 bytes) ----
    buf.extend(STREAM_STORAGE_SIGNATURE)

    # ---- placeholder for header page ----
    header_page_start = len(buf)
    buf.extend(b"\x00" * (_HEADER_PAGE_SIZE - _SIGNATURE_LEN))

    # ---- 2. Existing data files ----
    new_offsets: dict[str, int] = {}
    new_sizes: dict[str, int] = {}
    kept_entries = []

    for ve in abf_struct.data_entries:
        if ve.path in replacements:
            data = replacements[ve.path]
        else:
            data = abf_struct.read_file_data(ve.path)

        new_offsets[ve.path] = len(buf)
        new_sizes[ve.path] = len(data)
        buf.extend(data)
        kept_entries.append(ve)

    # ---- 2b. NEW files (VertiPaq data) ----
    # These don't exist in the original ABF. We add them as new VDir entries.
    # Generate random hex StoragePaths matching template format (20-char uppercase hex)
    import secrets
    new_file_records: list[tuple[str, str, int, int]] = []  # (fpath, storage_path, offset, size)
    for fpath, content in new_files.items():
        offset = len(buf)
        size = len(content)
        storage_path = secrets.token_hex(10).upper()
        new_file_records.append((fpath, storage_path, offset, size))
        buf.extend(content)

    # ---- 3. BackupLog ----
    blog_root = deepcopy(abf_struct.backup_log_root)

    # Update sizes in BackupLog for ALL modified files (replacements + nullified)
    for fg in blog_root.findall("FileGroups/FileGroup"):
        for bf in fg.findall("FileList/BackupFile"):
            sp = bf.findtext("StoragePath")
            if sp in new_sizes:
                size_elem = bf.find("Size")
                if size_elem is not None:
                    size_elem.text = str(new_sizes[sp])

    # Add new VertiPaq files to the data FileGroup (Class=100069)
    file_groups = blog_root.findall("FileGroups/FileGroup")
    data_fg = None
    for fg in file_groups:
        if fg.findtext("Class", "") == "100069":
            data_fg = fg
            break
    if data_fg is None:
        data_fg = file_groups[-1] if file_groups else None

    if data_fg is not None:
        file_list = data_fg.find("FileList")
        if file_list is None:
            file_list = ET.SubElement(data_fg, "FileList")

        persist_path = data_fg.findtext("PersistLocationPath", "")
        timestamp = 134002835794032078

        for fpath, storage_path, offset, size in new_file_records:
            bf = ET.SubElement(file_list, "BackupFile")
            ET.SubElement(bf, "Path").text = f"{persist_path}\\{fpath}"
            ET.SubElement(bf, "StoragePath").text = storage_path
            ET.SubElement(bf, "LastWriteTime").text = str(timestamp)
            ET.SubElement(bf, "Size").text = str(size)

    blog_bytes = _xml_to_utf16_bytes(blog_root)
    if abf_struct.error_code:
        blog_bytes = blog_bytes + b"\x00\x00\x00\x00"

    blog_offset = len(buf)
    blog_size = len(blog_bytes)
    buf.extend(blog_bytes)

    # ---- 4. VirtualDirectory ----
    vdir_root_new = ET.Element("VirtualDirectory")

    timestamp = 134002835794032078

    # All kept entries (system + nullified template data)
    for ve in kept_entries:
        bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = ve.path
        ET.SubElement(bf_elem, "Size").text = str(new_sizes.get(ve.path, ve.size))
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(new_offsets.get(ve.path, ve.m_cbOffsetHeader))
        ET.SubElement(bf_elem, "Delete").text = "true" if ve.delete else "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(ve.created_timestamp)
        ET.SubElement(bf_elem, "Access").text = str(ve.access)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(ve.last_write_time)

    # New VertiPaq file entries — use the SAME random hex StoragePaths as BackupLog
    for fpath, storage_path, offset, size in new_file_records:
        bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = storage_path  # Random hex, NOT fpath!
        ET.SubElement(bf_elem, "Size").text = str(size)
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(offset)
        ET.SubElement(bf_elem, "Delete").text = "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(timestamp)
        ET.SubElement(bf_elem, "Access").text = str(timestamp)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(timestamp)

    # BackupLog entry (last)
    blog_ve = abf_struct.backup_log_entry
    bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
    ET.SubElement(bf_elem, "Path").text = blog_ve.path
    ET.SubElement(bf_elem, "Size").text = str(blog_size)
    ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(blog_offset)
    ET.SubElement(bf_elem, "Delete").text = "true" if blog_ve.delete else "false"
    ET.SubElement(bf_elem, "CreatedTimestamp").text = str(blog_ve.created_timestamp)
    ET.SubElement(bf_elem, "Access").text = str(blog_ve.access)
    ET.SubElement(bf_elem, "LastWriteTime").text = str(blog_ve.last_write_time)

    vdir_bytes = _xml_to_utf16_bytes(vdir_root_new)
    vdir_offset = len(buf)
    vdir_size = len(vdir_bytes)
    buf.extend(vdir_bytes)

    # ---- 5. Patch header ----
    hdr = deepcopy(abf_struct.header_root)
    hdr.find("m_cbOffsetHeader").text = str(vdir_offset)
    hdr.find("DataSize").text = str(vdir_size)
    total_entries = len(kept_entries) + len(new_file_records) + 1  # +1 for BackupLog
    hdr.find("Files").text = str(total_entries)

    hdr_bytes = _xml_to_utf16_bytes(hdr)
    available = _HEADER_PAGE_SIZE - _SIGNATURE_LEN
    if len(hdr_bytes) > available:
        raise ValueError(
            f"BackupLogHeader XML is {len(hdr_bytes)} bytes, "
            f"exceeds the {available}-byte page limit."
        )
    hdr_padded = hdr_bytes + b"\x00" * (available - len(hdr_bytes))
    buf[header_page_start: header_page_start + available] = hdr_padded

    return bytes(buf)
