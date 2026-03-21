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
        """Build the complete PBIX file as bytes.

        Uses a minimal template DataModel from a real PBIX file (shipped
        with the package) and modifies its metadata SQLite to contain our
        custom tables, measures, and relationships.  Also encodes VertiPaq
        binary data for row data and inserts all required storage-layer
        metadata entries.

        Steps:
          1. Decompress the template DataModel to get the ABF blob
          2. Extract metadata SQLite from the ABF
          3. Find max IDs across ALL tables for global counter
          4. INSERT our tables, columns, partitions, measures, relationships
          5. INSERT all storage-layer entries (TableStorage, ColumnStorage, etc.)
          6. Encode row data with encode_table_data()
          7. Rebuild ABF with modified metadata AND new VertiPaq binary files
          8. Compress to DataModel, build layout, package into PBIX ZIP
        """
        from pbix_mcp.formats.abf_rebuild import (
            rebuild_abf_with_replacement,
            read_metadata_sqlite,
            list_abf_files,
            find_abf_file,
        )
        from pbix_mcp.formats.datamodel_roundtrip import (
            compress_datamodel,
            decompress_datamodel,
        )
        from pbix_mcp.formats.vertipaq_encoder import encode_table_data

        # Capture builder state
        tables = self._tables
        measures = self._measures
        relationships = self._relationships

        # 1. Load and decompress the template DataModel
        template_path = os.path.join(
            os.path.dirname(__file__), "templates", "minimal_datamodel.bin"
        )
        with open(template_path, "rb") as f:
            template_dm = f.read()

        template_abf = decompress_datamodel(template_dm)

        # 2. Extract the metadata SQLite
        sqlite_bytes = read_metadata_sqlite(template_abf)

        # 3. Modify metadata and collect VertiPaq files
        new_sqlite_bytes, vertipaq_files = _modify_metadata_and_encode(
            sqlite_bytes, tables, measures, relationships
        )

        # 4. Build the replacement dict for the ABF
        # We need to replace metadata.sqlitedb AND add new VertiPaq files
        file_log = list_abf_files(template_abf)

        # Find the metadata.sqlitedb StoragePath
        replacements: dict[str, bytes] = {}
        meta_entry = find_abf_file(file_log, "metadata.sqlitedb")
        if meta_entry:
            replacements[meta_entry["StoragePath"]] = new_sqlite_bytes

        # For VertiPaq files, we need to add them as new entries in the ABF.
        # Use rebuild_abf_with_replacement which only handles existing paths.
        # We need a different approach: rebuild from scratch using the
        # build_abf_from_scratch function which accepts arbitrary files.
        #
        # Strategy: extract all existing ABF files, add our new ones, rebuild.
        from pbix_mcp.formats.abf_rebuild import _ABFStructure, _rebuild_abf

        abf_struct = _ABFStructure(template_abf)

        # Build exact replacements: metadata.sqlitedb replacement
        exact_replacements: dict[str, bytes] = {}
        for entry in abf_struct.file_log:
            if "metadata.sqlitedb" in entry["Path"].lower():
                exact_replacements[entry["StoragePath"]] = new_sqlite_bytes
                break

        # For new VertiPaq files, we need to add new VDir entries.
        # The _rebuild_abf function only replaces existing entries.
        # So we must extend the ABF structure with new entries.
        new_abf = _rebuild_abf_with_new_files(
            abf_struct, exact_replacements, vertipaq_files
        )

        # 5. Compress to DataModel
        datamodel_bytes = compress_datamodel(new_abf)

        # 6. Build layout
        layout_bytes = self._build_layout()

        # 7. Pack into ZIP
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


def _get_max_id_across_tables(conn: sqlite3.Connection) -> int:
    """Find the maximum ID across ALL tables that have an ID column."""
    c = conn.cursor()
    max_id = 0
    tables_to_check = [
        "Table", "Column", "Partition", "Measure", "Relationship",
        "TableStorage", "ColumnStorage", "PartitionStorage",
        "ColumnPartitionStorage", "DictionaryStorage", "SegmentStorage",
        "SegmentMapStorage", "StorageFile", "StorageFolder",
        "AttributeHierarchy", "AttributeHierarchyStorage",
        "RelationshipStorage", "RelationshipIndexStorage",
    ]
    for tbl in tables_to_check:
        try:
            row = c.execute(f"SELECT MAX(ID) FROM [{tbl}]").fetchone()
            if row and row[0] is not None:
                max_id = max(max_id, row[0])
        except sqlite3.OperationalError:
            pass  # Table doesn't exist
    return max_id


def _modify_metadata_and_encode(
    sqlite_bytes: bytes,
    tables: list[dict],
    measures: list[dict],
    relationships: list[dict],
) -> tuple[bytes, dict[str, bytes]]:
    """Modify the template metadata SQLite and encode VertiPaq data.

    Returns:
        (new_sqlite_bytes, vertipaq_files) where vertipaq_files maps
        ABF internal paths to binary content.
    """
    from pbix_mcp.formats.vertipaq_encoder import encode_table_data

    # Write SQLite to temp file
    fd, tmp_path = tempfile.mkstemp(suffix=".sqlitedb")
    try:
        os.write(fd, sqlite_bytes)
        os.close(fd)
        fd = None

        conn = sqlite3.connect(tmp_path)
        conn.row_factory = sqlite3.Row

        # Find max ID across ALL tables for global counter
        max_id = _get_max_id_across_tables(conn)
        alloc = _IDAllocator(max_id + 1)

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
            table_id = alloc.next()
            table_id_map[tname] = table_id
            column_id_map[tname] = {}

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
                    NULL, 1, 2, ?,
                    0, 3, ?, ?,
                    0, NULL, 0,
                    0.0, 0.0, -1, NULL,
                    0, 0, NULL,
                    0, NULL
                )""",
                (part_id, table_id, tname, ps_id,
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

            # ============================================================
            # INSERT RowNumber column (system column, Type=3)
            # ============================================================
            rn_col_id = alloc.next()
            rn_name = f"RowNumber-{str(uuid.uuid4()).upper()}"
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

            rn_cs_name = f"{rn_name} ({rn_col_id})"
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
                    ?, 3, ?,
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
                 row_count,  # distinct states
                 row_count + 2,  # max data id
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

                # StorageFile for HIDX (OwnerType=27)
                hidx_file_id = alloc.next()

                cs_name = f"{col_name} ({col_id})"
                col_idf_fname = f"0.{tname} ({table_id}).{cs_name}.0.idf"
                col_meta_fname = f"0.{tname} ({table_id}).{cs_name}.0.idfmeta"
                col_dict_fname = f"0.{tname} ({table_id}).{cs_name}.dictionary"
                col_hidx_fname = f"1.H${tname} ({table_id})${cs_name}.hidx"

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
                        1, 0, 0,
                        ?, ?, 31240512000000000,
                        1, 0, ?,
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
                     str(uuid.uuid4())),
                )

                # Insert AttributeHierarchy for this column
                ah_id = alloc.next()
                c.execute(
                    """INSERT INTO AttributeHierarchy (
                        ID, ColumnID, State, AttributeHierarchyStorageID,
                        ModifiedTime, RefreshedTime
                    ) VALUES (?, ?, 1, 0, ?, ?)""",
                    (ah_id, col_id, _FIXED_TIMESTAMP, _FIXED_TIMESTAMP),
                )

                # Update the Column to reference the AttributeHierarchy
                c.execute(
                    "UPDATE [Column] SET AttributeHierarchyID = ? WHERE ID = ?",
                    (ah_id, col_id),
                )

                # Insert AttributeHierarchyStorage
                ahs_id = alloc.next()
                c.execute(
                    """INSERT INTO AttributeHierarchyStorage (
                        ID, AttributeHierarchyID, SortOrder, OptimizationLevel,
                        MaterializationType, ColumnPositionToData,
                        ColumnDataToPosition, DistinctDataCount,
                        DataVersion, StorageFileID, SystemTableID,
                        HasStatistics, MinValue, MaxValue, StringValueMaxLength
                    ) VALUES (?, ?, 0, 0, 3, -1, -1, 0, 1, 0, 0, 0, NULL, NULL, 0)""",
                    (ahs_id, ah_id),
                )

                # Update AttributeHierarchy to reference AttributeHierarchyStorage
                c.execute(
                    "UPDATE AttributeHierarchy SET AttributeHierarchyStorageID = ? WHERE ID = ?",
                    (ahs_id, ah_id),
                )

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
                        1, 3, NULL, NULL,
                        1033, 0,
                        0, 0,
                        0, 0,
                        -1, 0,
                        0, 0,
                        0, 0,
                        0, 0,
                        0, 0,
                        0, 0,
                        NULL, -1,
                        NULL
                    )""",
                    (cs_id, col_id, cs_name, col_idx + 1, ds_id),
                )

                # DictionaryStorage (Type=1 = external, with file)
                c.execute(
                    """INSERT INTO DictionaryStorage (
                        ID, ColumnStorageID, Type, DataType, DataVersion,
                        BaseId, Magnitude, LastId, IsNullable, IsUnique,
                        IsOperatingOn32, DictionaryFlags, StorageFileID, Size
                    ) VALUES (?, ?, 1, ?, 0, 2, 0.0, 0, 0, 0, 0, 0, ?, 0)""",
                    (ds_id, cs_id, amo_type, dict_file_id),
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

                # StorageFile for HIDX (OwnerType=27)
                c.execute(
                    """INSERT INTO StorageFile (
                        ID, OwnerID, OwnerType, StorageFolderID, FileName
                    ) VALUES (?, ?, 27, ?, ?)""",
                    (hidx_file_id, ds_id, tbl_folder_id, col_hidx_fname),
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
                 from_tid, from_col_id,
                 to_tid, to_col_id,
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

            # RelationshipIndexStorage
            c.execute(
                """INSERT INTO RelationshipIndexStorage (
                    ID, RelationshipStorageID, IndexType, Flags,
                    RecordCount, SecondaryRecordCount,
                    StorageFolderID, StorageFileID,
                    SystemTableID, SecondarySystemTableID
                ) VALUES (?, ?, 1, 0, 0, 0, 0, 0, 0, 0)""",
                (ris_id, rs_id),
            )

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

            encoded_files = encode_table_data(tname, part_id, encoder_columns, rows)

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
                abf_hidx_path = f"{tbl_folder}\\{info['hidx_fname']}"

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
                if hidx_key in encoded_files:
                    vertipaq_files[abf_hidx_path] = encoded_files[hidx_key]

                # Update ColumnStorage statistics from the IDFMETA
                if meta_key in encoded_files:
                    _update_column_storage_stats(
                        c, info["cs_id"], encoded_files[meta_key], row_count
                    )

            # Encode RowNumber column
            rn_info = col_storage_info[tname]["__rownumber__"]
            rn_col_def = [{"name": "RowNumber", "data_type": "Int64", "nullable": False}]
            rn_rows = [{"RowNumber": i} for i in range(row_count)]
            rn_encoded = encode_table_data(tname, part_id, rn_col_def, rn_rows)

            rn_base = f"{tname}.tbl\\{part_id}.prt"
            rn_idf_key = f"{rn_base}\\column.RowNumber"
            rn_meta_key = f"{rn_base}\\column.RowNumbermeta"

            abf_rn_idf_path = f"{prt_folder}\\{rn_info['idf_fname']}"
            abf_rn_meta_path = f"{prt_folder}\\{rn_info['meta_fname']}"

            if rn_idf_key in rn_encoded:
                vertipaq_files[abf_rn_idf_path] = rn_encoded[rn_idf_key]
            if rn_meta_key in rn_encoded:
                vertipaq_files[abf_rn_meta_path] = rn_encoded[rn_meta_key]

        conn.commit()
        conn.close()

        # Read the modified SQLite back
        with open(tmp_path, "rb") as f:
            new_sqlite_bytes = f.read()

    finally:
        if fd is not None:
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
        STREAM_STORAGE_SIGNATURE,
        _HEADER_PAGE_SIZE,
        _SIGNATURE_LEN,
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

    for ve in abf_struct.data_entries:
        if ve.path in replacements:
            data = replacements[ve.path]
        else:
            data = abf_struct.read_file_data(ve.path)

        new_offsets[ve.path] = len(buf)
        new_sizes[ve.path] = len(data)
        buf.extend(data)

    # ---- 2b. NEW files (VertiPaq data) ----
    # These don't exist in the original ABF. We add them as new VDir entries.
    new_file_records: list[tuple[str, int, int]] = []  # (path, offset, size)
    for fpath, content in new_files.items():
        offset = len(buf)
        size = len(content)
        new_file_records.append((fpath, offset, size))
        buf.extend(content)

    # ---- 3. BackupLog ----
    blog_root = deepcopy(abf_struct.backup_log_root)

    # Update sizes in BackupLog for replaced files
    for fg in blog_root.findall("FileGroups/FileGroup"):
        for bf in fg.findall("FileList/BackupFile"):
            sp = bf.findtext("StoragePath")
            if sp in new_sizes:
                size_elem = bf.find("Size")
                if size_elem is not None:
                    size_elem.text = str(new_sizes[sp])

    # Add new files to the BackupLog FileGroup
    # Find the database FileGroup (the one with Class=100002 or the last one)
    file_groups = blog_root.findall("FileGroups/FileGroup")
    db_fg = file_groups[-1] if file_groups else None
    if db_fg is not None:
        file_list = db_fg.find("FileList")
        if file_list is None:
            file_list = ET.SubElement(db_fg, "FileList")

        persist_path = db_fg.findtext("PersistLocationPath", "")
        timestamp = 134002835794032078

        for fpath, offset, size in new_file_records:
            bf = ET.SubElement(file_list, "BackupFile")
            ET.SubElement(bf, "Path").text = f"{persist_path}\\{fpath}"
            ET.SubElement(bf, "StoragePath").text = fpath
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

    # Existing entries
    for ve in abf_struct.data_entries:
        bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = ve.path
        ET.SubElement(bf_elem, "Size").text = str(new_sizes.get(ve.path, ve.size))
        ET.SubElement(bf_elem, "m_cbOffsetHeader").text = str(new_offsets.get(ve.path, ve.m_cbOffsetHeader))
        ET.SubElement(bf_elem, "Delete").text = "true" if ve.delete else "false"
        ET.SubElement(bf_elem, "CreatedTimestamp").text = str(ve.created_timestamp)
        ET.SubElement(bf_elem, "Access").text = str(ve.access)
        ET.SubElement(bf_elem, "LastWriteTime").text = str(ve.last_write_time)

    # New VertiPaq file entries
    for fpath, offset, size in new_file_records:
        bf_elem = ET.SubElement(vdir_root_new, "BackupFile")
        ET.SubElement(bf_elem, "Path").text = fpath
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
    total_entries = len(abf_struct.data_entries) + len(new_file_records) + 1
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
