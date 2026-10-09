"""Issue #136: every column of a built model is available in MDX.

PBIXBuilder wrote Column.IsAvailableInMDX = 0 on every data column outside a
user hierarchy, where Desktop writes 1 on every column. MDX clients -- Analyze
in Excel, an MDX query -- then saw no attribute hierarchy: SELECT
{[Measures].[S]} ON 0 FROM [Model] WHERE ([T].[c].&[a]) was empty. And as the
server's table rewrite (pbix_set_table_data) rebuilds the model through the
builder, one edit of a Desktop-authored file took its columns out of MDX (34
of Awesome Chocolates' 109).

The attribute-hierarchy storage was there already (the builder writes an H$
table for each column), so the flag is all it takes. Power BI Desktop 2.152
over ADOMD (build_b136.py): with it, MDSCHEMA_HIERARCHIES lists every column,
and MDX slicers and member sets answer as DAX does -- text, numbers, dates,
TRUE/FALSE, the BLANK member, a related table, a user hierarchy, a hidden
table, an empty one -- the same as built and after Desktop's own refresh."""
from __future__ import annotations

import io
import json
import os
import sqlite3
import tempfile
import zipfile
from datetime import datetime

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

pytestmark = pytest.mark.unit


def _columns(pbix_bytes: bytes) -> dict:
    """{(table, column): IsAvailableInMDX} for the data columns of the user tables."""
    dm = zipfile.ZipFile(io.BytesIO(pbix_bytes)).read("DataModel")
    meta = read_metadata_sqlite(decompress_datamodel(dm))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, meta)
    os.close(fd)
    try:
        conn = sqlite3.connect(tmp)
        try:
            rows = conn.execute(
                "SELECT t.Name, c.ExplicitName, c.Type, c.IsAvailableInMDX FROM [Column] c "
                "JOIN [Table] t ON c.TableID = t.ID WHERE t.SystemFlags = 0").fetchall()
        finally:
            conn.close()
    finally:
        os.unlink(tmp)
    return {(t, c): (typ, mdx) for t, c, typ, mdx in rows
            if not str(t).startswith(("H$", "R$", "U$"))}


def _builder() -> PBIXBuilder:
    b = PBIXBuilder("b136")
    b.add_table("D", [{"name": "c", "data_type": "String"}, {"name": "grp", "data_type": "String"}],
                rows=[{"c": c, "grp": g} for c, g in (("a", "g1"), ("b", "g1"), ("c", "g2"))])
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "v", "data_type": "Double"},
                      {"name": "n", "data_type": "Int64"}, {"name": "d", "data_type": "DateTime"},
                      {"name": "b", "data_type": "Boolean"}, {"name": "m", "data_type": "Decimal"},
                      {"name": "z", "data_type": "String"}],
                rows=[{"c": c, "v": v, "n": n, "d": datetime(2024, 1, n), "b": n % 2 == 1, "m": 1.5, "z": None}
                      for c, v, n in (("a", 10.0, 1), ("b", 20.0, 2), ("c", 30.0, 3), (None, 40.0, 4))])
    b.add_relationship("T", "c", "D", "c")
    b.add_measure("T", "S", "SUM(T[v])")
    b.add_table("E", [{"name": "x", "data_type": "String"}], rows=[])
    b.add_table("H", [{"name": "h", "data_type": "String"}], rows=[{"h": "p"}], hidden=True)
    b.add_user_hierarchy("D", "Geo", [{"name": "grp", "column": "grp"}, {"name": "c", "column": "c"}])
    b.add_page("Page 1")
    return b


DATA_COLUMNS = {("D", "c"), ("D", "grp"), ("T", "c"), ("T", "v"), ("T", "n"), ("T", "d"),
                ("T", "b"), ("T", "m"), ("T", "z"), ("E", "x"), ("H", "h")}


def test_every_built_column_is_available_in_mdx():
    cols = _columns(_builder().build())
    data = {k: mdx for k, (typ, mdx) in cols.items() if typ == 1}
    assert set(data) == DATA_COLUMNS
    assert data == dict.fromkeys(DATA_COLUMNS, 1)
    # RowNumber, as before
    assert all(mdx == 1 for (typ, mdx) in cols.values() if typ == 3)


def test_a_table_rewrite_keeps_the_columns_in_mdx(tmp_path):
    src = tmp_path / "b136.pbix"
    src.write_bytes(_builder().build())
    out = tmp_path / "b136_rewritten.pbix"
    assert json.loads(server.pbix_open(str(src), "i136"))["success"]
    try:
        res = json.loads(server.pbix_set_table_data("i136", "D", json.dumps({
            "columns": [{"name": "c", "data_type": "String"}, {"name": "grp", "data_type": "String"}],
            "rows": [{"c": "a", "grp": "g9"}, {"c": "b", "grp": "g9"}, {"c": "c", "grp": "g2"}]})))
        assert res["success"], res
        assert json.loads(server.pbix_save("i136", str(out), overwrite=True, backup=False))["success"]
    finally:
        server.pbix_close("i136", force=True)
    cols = _columns(out.read_bytes())
    data = {k: mdx for k, (typ, mdx) in cols.items() if typ == 1}
    assert DATA_COLUMNS <= set(data)
    assert {k: data[k] for k in DATA_COLUMNS} == dict.fromkeys(DATA_COLUMNS, 1)
