"""Issue #155: a table rewrite keeps a calculation group's source columns.

pbix_set_table_data rewrites the model through the builder, which names each
column's SourceColumn after the column. A calculation group's two data
columns must read Desktop's fixed source columns, 'Name' (text) and
'Ordinal' (whole number), whatever they are called -- so once the group's
column had been renamed (Awesome Chocolates calls it "Time"), any table
rewrite produced a file Power BI Desktop 2.152 refuses: "Calculation group
table 'Time Intelligence' supports maximum two data columns: one is string
datatype with source column 'Name' and the other is integer datatype with
source column 'Ordinal'." The rebuild re-wired the group already
(CalculationGroupID, partition Type 7); it now restores the source columns
too, and the rewritten Awesome Chocolates opens in Desktop with its
calculation items answering as before."""
from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import zipfile

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

pytestmark = pytest.mark.unit


def _group_columns(pbix_bytes: bytes, table: str) -> dict:
    """{column: (SourceColumn, ExplicitDataType)} of a table's data columns, and
    whether the table is still wired to its calculation group."""
    dm = zipfile.ZipFile(io.BytesIO(pbix_bytes)).read("DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(dm))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        try:
            tid, gid = conn.execute("SELECT ID, CalculationGroupID FROM [Table] WHERE Name = ?",
                                    (table,)).fetchone()
            cols = {n: (s, t) for n, s, t in conn.execute(
                "SELECT ExplicitName, SourceColumn, ExplicitDataType FROM [Column] "
                "WHERE TableID = ? AND Type = 1", (tid,))}
            ptype = conn.execute("SELECT Type FROM [Partition] WHERE TableID = ?", (tid,)).fetchone()[0]
        finally:
            conn.close()
    finally:
        os.unlink(tmp)
    return {"columns": cols, "wired": bool(gid), "partition_type": ptype}


def test_a_renamed_group_column_keeps_its_source_column(tmp_path):
    b = PBIXBuilder("b155")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "v", "data_type": "Double"}],
                rows=[{"c": "a", "v": 10.0}, {"c": "b", "v": 20.0}])
    b.add_measure("T", "S", "SUM(T[v])")
    b.add_page("Page 1")
    src = tmp_path / "b155.pbix"
    src.write_bytes(b.build())
    out = tmp_path / "b155_rewritten.pbix"
    assert json.loads(server.pbix_open(str(src), "i155"))["success"]
    try:
        r = json.loads(server.pbix_datamodel_add_calculation_group("i155", "TI", json.dumps(
            [{"name": "Base", "expression": "SELECTEDMEASURE()"},
             {"name": "Double", "expression": "SELECTEDMEASURE() * 2"}])))
        assert r["success"], r
        # renamed, as a Desktop user does: the source column stays 'Name'
        r = json.loads(server.pbix_datamodel_modify_column("i155", "TI", "Name", "ExplicitName", "Time"))
        assert r["success"], r
        r = json.loads(server.pbix_set_table_data("i155", "T", json.dumps({
            "columns": [{"name": "c", "data_type": "String"}, {"name": "v", "data_type": "Double"}],
            "rows": [{"c": "a", "v": 10.0}, {"c": "b", "v": 20.0}, {"c": "c", "v": 30.0}]})))
        assert r["success"], r
        r = json.loads(server.pbix_evaluate_dax("i155", "S"))
        assert r["results"][0]["value"] == pytest.approx(60.0)
        assert json.loads(server.pbix_save("i155", str(out), overwrite=True, backup=False))["success"]
    finally:
        server.pbix_close("i155", force=True)
    g = _group_columns(out.read_bytes(), "TI")
    assert g["columns"] == {"Time": ("Name", 2), "Ordinal": ("Ordinal", 6)}
    assert g["wired"] and g["partition_type"] == 7
