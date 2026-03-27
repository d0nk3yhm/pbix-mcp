"""
Native DataModel reader -- replaces PBIXRay for reading PBIX data models.

Reads schema, measures, relationships, Power Query expressions, calculated columns,
table statistics, and actual table data from a PBIX file's DataModel using our own
ABF parser and VertiPaq decoder.

Public API
----------
ModelReader(pbix_path)
    Open a PBIX and provide access to its data model.

    .schema          -> list[dict]   (table/column schema)
    .dax_measures    -> list[dict]   (DAX measure definitions)
    .relationships   -> list[dict]   (model relationships)
    .power_query     -> list[dict]   (M expressions from model)
    .dax_columns     -> list[dict]   (calculated columns)
    .statistics      -> list[dict]   (table row/column counts)
    .get_table(name) -> dict         ({columns: [...], rows: [...]})
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
import zipfile
from typing import Optional

from pbix_mcp.formats.abf_rebuild import (
    list_abf_files,
    read_abf_file,
    read_metadata_sqlite,
)
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


class ModelReader:
    """
    Native replacement for PBIXRay.

    Opens a PBIX file and provides read access to the data model schema,
    measures, relationships, Power Query expressions, and table data.
    """

    def __init__(self, pbix_path: str):
        self._path = pbix_path
        self._dm_bytes: Optional[bytes] = None
        self._abf_bytes: Optional[bytes] = None
        self._metadata_db_bytes: Optional[bytes] = None
        self._metadata_cache: dict = {}

    def _ensure_datamodel(self):
        """Extract and decompress the DataModel from the PBIX."""
        if self._abf_bytes is not None:
            return

        with zipfile.ZipFile(self._path, "r") as zf:
            # DataModel is stored as a ZIP entry
            dm_names = [n for n in zf.namelist() if n.lower() == "datamodel"]
            if not dm_names:
                raise ValueError("No DataModel found in PBIX file")
            self._dm_bytes = zf.read(dm_names[0])

        self._abf_bytes = decompress_datamodel(self._dm_bytes)
        self._metadata_db_bytes = read_metadata_sqlite(self._abf_bytes)

    def _query_metadata(self, sql: str, params: tuple = ()) -> list[dict]:
        """Run a SQL query against metadata.sqlitedb and return rows as dicts."""
        self._ensure_datamodel()
        tmp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        try:
            tmp_db.write(self._metadata_db_bytes)
            tmp_db.close()
            conn = sqlite3.connect(tmp_db.name)
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            result = [dict(row) for row in rows]
            conn.close()
            return result
        finally:
            os.unlink(tmp_db.name)

    @property
    def schema(self) -> list[dict]:
        """
        Get the data model schema -- all tables, columns, and data types.

        Returns list of dicts with keys: TableName, ColumnName, DataType,
        IsHidden, Description.
        """
        if "schema" in self._metadata_cache:
            return self._metadata_cache["schema"]

        rows = self._query_metadata("""
            SELECT t.Name AS TableName,
                   c.ExplicitName AS ColumnName,
                   c.ExplicitDataType AS DataTypeCode,
                   c.IsHidden,
                   c.Description,
                   c.Type AS ColumnType
            FROM [Column] c
            JOIN [Table] t ON c.TableID = t.ID
            ORDER BY t.Name, c.ID
        """)

        _type_map = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
                     10: "Decimal", 11: "Boolean", 17: "Binary"}

        for row in rows:
            row["DataType"] = _type_map.get(row.pop("DataTypeCode", 0), "Unknown")
            # ColumnType: 1=data, 2=RowNumber, 3=calculated
            ct = row.pop("ColumnType", 1)
            row["IsCalculated"] = (ct == 3)

        self._metadata_cache["schema"] = rows
        return rows

    @property
    def dax_measures(self) -> list[dict]:
        """
        Get all DAX measures from the data model.

        Returns list of dicts with keys: TableName, Name, Expression,
        FormatString, Description, IsHidden.
        """
        if "measures" in self._metadata_cache:
            return self._metadata_cache["measures"]

        rows = self._query_metadata("""
            SELECT t.Name AS TableName,
                   m.Name,
                   m.Expression,
                   m.FormatString,
                   m.Description,
                   m.IsHidden
            FROM [Measure] m
            JOIN [Table] t ON m.TableID = t.ID
            ORDER BY t.Name, m.Name
        """)

        self._metadata_cache["measures"] = rows
        return rows

    @property
    def relationships(self) -> list[dict]:
        """
        Get all relationships in the data model.

        Returns list of dicts with keys: FromTableName, FromColumnName,
        ToTableName, ToColumnName, IsActive, CrossFilteringBehavior.
        """
        if "relationships" in self._metadata_cache:
            return self._metadata_cache["relationships"]

        rows = self._query_metadata("""
            SELECT ft.Name AS FromTableName,
                   fc.ExplicitName AS FromColumnName,
                   tt.Name AS ToTableName,
                   tc.ExplicitName AS ToColumnName,
                   r.IsActive,
                   r.CrossFilteringBehavior
            FROM [Relationship] r
            JOIN [Column] fc ON r.FromColumnID = fc.ID
            JOIN [Table] ft ON fc.TableID = ft.ID
            JOIN [Column] tc ON r.ToColumnID = tc.ID
            JOIN [Table] tt ON tc.TableID = tt.ID
            ORDER BY r.ID
        """)

        self._metadata_cache["relationships"] = rows
        return rows

    @property
    def power_query(self) -> list[dict]:
        """
        Get Power Query (M) expressions from the data model.

        Returns list of dicts with keys: TableName, PartitionName, Expression.
        """
        if "power_query" in self._metadata_cache:
            return self._metadata_cache["power_query"]

        rows = self._query_metadata("""
            SELECT t.Name AS TableName,
                   p.Name AS PartitionName,
                   p.QueryDefinition AS Expression
            FROM [Partition] p
            JOIN [Table] t ON p.TableID = t.ID
            WHERE p.QueryDefinition IS NOT NULL AND p.QueryDefinition != ''
            ORDER BY t.Name, p.Name
        """)

        self._metadata_cache["power_query"] = rows
        return rows

    @property
    def dax_columns(self) -> list[dict]:
        """
        Get all DAX calculated columns from the model.

        Returns list of dicts with keys: TableName, ColumnName, Expression,
        DataType, FormatString, IsHidden.
        """
        if "dax_columns" in self._metadata_cache:
            return self._metadata_cache["dax_columns"]

        _type_map = {2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
                     10: "Decimal", 11: "Boolean"}

        rows = self._query_metadata("""
            SELECT t.Name AS TableName,
                   c.ExplicitName AS ColumnName,
                   c.Expression,
                   c.ExplicitDataType AS DataTypeCode,
                   c.FormatString,
                   c.IsHidden
            FROM [Column] c
            JOIN [Table] t ON c.TableID = t.ID
            WHERE c.Type = 3 AND c.Expression IS NOT NULL AND c.Expression != ''
            ORDER BY t.Name, c.ID
        """)

        for row in rows:
            row["DataType"] = _type_map.get(row.pop("DataTypeCode", 0), "Unknown")

        self._metadata_cache["dax_columns"] = rows
        return rows

    @property
    def statistics(self) -> list[dict]:
        """
        Get table statistics (row counts, column counts).

        Returns list of dicts with keys: TableName, ColumnCount, RowCount.
        """
        if "statistics" in self._metadata_cache:
            return self._metadata_cache["statistics"]

        self._ensure_datamodel()
        from pbix_mcp.formats.vertipaq_decoder import decode_idfmeta

        file_log = list_abf_files(self._abf_bytes)

        # Get table/column info from metadata, including first data column ID
        tables_meta = self._query_metadata("""
            SELECT t.ID, t.Name AS TableName,
                   COUNT(CASE WHEN c.Type != 2 THEN 1 END) AS ColumnCount,
                   MIN(CASE WHEN c.Type = 1 THEN c.ID END) AS FirstDataColID
            FROM [Table] t
            LEFT JOIN [Column] c ON c.TableID = t.ID
            GROUP BY t.ID, t.Name
            ORDER BY t.Name
        """)

        result = []
        for tm in tables_meta:
            table_name = tm["TableName"]
            table_id = tm["ID"]
            col_count = tm["ColumnCount"]
            first_col_id = tm.get("FirstDataColID")

            # Try to get row count from first column's IDFMETA
            row_count = 0
            for entry in file_log:
                path = entry["Path"]
                # Match by table name/ID and idfmeta extension
                is_table = (f"{table_name} ({table_id})" in path or
                           path.startswith(f"{table_name}.tbl"))
                if not is_table:
                    continue
                if not path.endswith(".idfmeta"):
                    if not (path.endswith("meta") and "column." in path):
                        continue
                if "RowNumber" in path:
                    continue
                try:
                    meta_bytes = read_abf_file(self._abf_bytes, entry)
                    meta_info = decode_idfmeta(meta_bytes)
                    if not meta_info["is_row_number"]:
                        row_count = meta_info["row_count"]
                        break
                except Exception:
                    continue

            result.append({
                "TableName": table_name,
                "ColumnCount": col_count,
                "RowCount": row_count,
            })

        self._metadata_cache["statistics"] = result
        return result

    def get_table(self, table_name: str, max_rows: int = 0) -> dict:
        """
        Read actual table data from the VertiPaq store.

        Parameters
        ----------
        table_name : str
            Name of the table to read.
        max_rows : int
            Maximum rows to return (0 = all).

        Returns
        -------
        dict with keys: columns (list[str]), rows (list[list])
        """
        self._ensure_datamodel()
        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf

        result = read_table_from_abf(
            self._abf_bytes, table_name, self._metadata_db_bytes
        )

        if max_rows > 0 and len(result["rows"]) > max_rows:
            result["rows"] = result["rows"][:max_rows]

        return result


def format_schema_table(schema: list[dict]) -> str:
    """Format schema as a readable table string (replaces DataFrame.to_string)."""
    if not schema:
        return "No schema found."

    # Build column-aligned table
    headers = ["TableName", "ColumnName", "DataType", "IsHidden"]
    rows = []
    for r in schema:
        rows.append([
            str(r.get("TableName", "")),
            str(r.get("ColumnName", "")),
            str(r.get("DataType", "")),
            str(r.get("IsHidden", "")),
        ])

    return _format_table(headers, rows)


def format_measures_table(measures: list[dict]) -> str:
    """Format measures as a readable table string."""
    if not measures:
        return "No DAX measures found."

    headers = ["TableName", "Name", "Expression", "FormatString"]
    rows = []
    for m in measures:
        expr = str(m.get("Expression", ""))
        if len(expr) > 120:
            expr = expr[:117] + "..."
        rows.append([
            str(m.get("TableName", "")),
            str(m.get("Name", "")),
            expr,
            str(m.get("FormatString", "") or ""),
        ])

    return _format_table(headers, rows)


def format_relationships_table(rels: list[dict]) -> str:
    """Format relationships as a readable table string."""
    if not rels:
        return "No relationships found."

    headers = ["FromTableName", "FromColumnName", "ToTableName", "ToColumnName", "IsActive"]
    rows = []
    for r in rels:
        rows.append([
            str(r.get("FromTableName", "")),
            str(r.get("FromColumnName", "")),
            str(r.get("ToTableName", "")),
            str(r.get("ToColumnName", "")),
            str(r.get("IsActive", "")),
        ])

    return _format_table(headers, rows)


def format_power_query_table(pq: list[dict]) -> str:
    """Format Power Query expressions as a readable table string."""
    if not pq:
        return "No Power Query expressions found in model."

    headers = ["TableName", "PartitionName", "Expression"]
    rows = []
    for p in pq:
        expr = str(p.get("Expression", ""))
        if len(expr) > 200:
            expr = expr[:197] + "..."
        rows.append([
            str(p.get("TableName", "")),
            str(p.get("PartitionName", "")),
            expr,
        ])

    return _format_table(headers, rows)


def format_dax_columns_table(cols: list[dict]) -> str:
    """Format calculated columns as a readable table string."""
    if not cols:
        return "No DAX calculated columns found."

    headers = ["TableName", "ColumnName", "Expression", "DataType"]
    rows = []
    for c in cols:
        expr = str(c.get("Expression", ""))
        if len(expr) > 120:
            expr = expr[:117] + "..."
        rows.append([
            str(c.get("TableName", "")),
            str(c.get("ColumnName", "")),
            expr,
            str(c.get("DataType", "")),
        ])

    return _format_table(headers, rows)


def format_statistics_table(stats: list[dict]) -> str:
    """Format table statistics as a readable table string."""
    if not stats:
        return "No tables found."

    headers = ["TableName", "ColumnCount", "RowCount"]
    rows = []
    for s in stats:
        rows.append([
            str(s.get("TableName", "")),
            str(s.get("ColumnCount", "")),
            str(s.get("RowCount", "")),
        ])

    return _format_table(headers, rows)


def format_table_data(table_data: dict, max_rows: int = 50) -> str:
    """Format table data as a readable table string."""
    columns = table_data.get("columns", [])
    all_rows = table_data.get("rows", [])

    if not columns or not all_rows:
        return "No data found."

    display_rows = all_rows[:max_rows]

    headers = columns
    str_rows = []
    for row in display_rows:
        str_rows.append([_format_cell(v) for v in row])

    result = _format_table(headers, str_rows)
    if len(all_rows) > max_rows:
        result += f"\n... ({len(all_rows)} rows total, showing first {max_rows})"
    return result


def _format_cell(value) -> str:
    """Format a single cell value for display."""
    if value is None:
        return ""
    import datetime as _dt
    if isinstance(value, _dt.datetime):
        if value.hour == 0 and value.minute == 0 and value.second == 0:
            return value.strftime("%Y-%m-%d")
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        if value == int(value):
            return str(int(value))
        return f"{value:.4f}"
    return str(value)


def _format_table(headers: list[str], rows: list[list[str]], max_col_width: int = 60) -> str:
    """Format headers + rows as an aligned text table."""
    if not rows:
        return "  ".join(headers)

    # Compute column widths
    n_cols = len(headers)
    widths = [len(h) for h in headers]
    for row in rows:
        for i in range(min(len(row), n_cols)):
            cell = str(row[i])
            if len(cell) > max_col_width:
                cell = cell[:max_col_width - 3] + "..."
            widths[i] = max(widths[i], len(cell))

    # Cap widths
    widths = [min(w, max_col_width) for w in widths]

    # Build output
    lines = []
    # Header
    hdr = "  ".join(h.ljust(widths[i]) for i, h in enumerate(headers))
    lines.append(hdr)

    # Data rows
    for row in rows:
        cells = []
        for i in range(n_cols):
            cell = str(row[i]) if i < len(row) else ""
            if len(cell) > widths[i]:
                cell = cell[:widths[i] - 3] + "..."
            cells.append(cell.ljust(widths[i]))
        lines.append("  ".join(cells))

    return "\n".join(lines)
