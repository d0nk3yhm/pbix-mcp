"""Issue #156: a built model orders text as Desktop does.

PBIXBuilder wrote each text column's attribute hierarchy (the H$ table's
POS_TO_ID) in Python's code-point order -- "1x" < "APAC" < "Americas" < "Bz"
< "Zeta" < "_x" < "a10" ... < "éclair" -- and Desktop's engine reads that order
for ORDER BY, SUMMARIZECOLUMNS, TOPN, MIN / MAX of text and sorted visuals, so
a built model sorted text differently until refreshed. It now writes the
model's collation (pbix_mcp.dax.collation.column_order_key).

Expected: Power BI Desktop 2.152 over ADOMD (build_b156.py), the order of the
same column after Desktop's own refresh; the built file now answers the same
as built and refreshed (12 of 12 queries)."""
from __future__ import annotations

import os
import sqlite3
import struct
import tempfile
import zipfile

import pytest

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import list_abf_files, read_abf_file, read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
from pbix_mcp.formats.vertipaq_decoder import decode_dictionary

pytestmark = pytest.mark.unit

DESKTOP_ORDER = [
    '_x',
    '1x',
    'a10',
    'a2',
    'Americas',
    'APAC',
    'apple',
    'b',
    'ba',
    'Bz',
    'eclair',
    'éclair',
    'olive',
    'Ölund',
    'Zeta',
]


def _nosplit32(buf, records):
    out, pos = [], 0
    for seg_records in records:
        (wc,) = struct.unpack_from("<Q", buf, pos)
        pos += 8
        seg = []
        for _ in range(wc):
            (w,) = struct.unpack_from("<Q", buf, pos)
            pos += 8
            seg += [w & 0xFFFFFFFF, (w >> 32) & 0xFFFFFFFF]
        out += seg[:seg_records]
    return out


def _hierarchy_order(pbix_path, table, column):
    """The column's values in its attribute hierarchy's order, and the
    AttributeHierarchyStorage Min / MaxValue."""
    abf = decompress_datamodel(zipfile.ZipFile(pbix_path).read("DataModel"))
    files = list_abf_files(abf)
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, read_metadata_sqlite(abf))
    os.close(fd)
    try:
        con = sqlite3.connect(tmp)
        cid, hrc, hrps, mn, mx = con.execute(
            """SELECT c.ID, sms.RecordCount, sms.RecordsPerSegment, ahs.MinValue, ahs.MaxValue
               FROM [Column] c JOIN [Table] t ON c.TableID = t.ID
               JOIN AttributeHierarchy ah ON ah.ColumnID = c.ID
               JOIN AttributeHierarchyStorage ahs ON ah.AttributeHierarchyStorageID = ahs.ID
               JOIN [Partition] hp ON hp.TableID = ahs.SystemTableID
               JOIN PartitionStorage hps ON hps.PartitionID = hp.ID
               JOIN SegmentMapStorage sms ON sms.PartitionStorageID = hps.ID
               WHERE t.Name = ? AND c.ExplicitName = ?""", (table, column)).fetchone()
        con.close()
    finally:
        os.unlink(tmp)
    tag = f"{column} ({cid})"
    pos_idf = next(e for e in files if "H$" in e["Path"] and tag in e["Path"]
                   and ".POS_TO_ID." in e["Path"] and e["Path"].endswith(".idf"))
    dict_e = next(e for e in files if not e["Path"].startswith(("H$", "R$")) and tag in e["Path"]
                  and e["Path"].endswith(".dictionary"))
    segs, rem = [], hrc
    while rem > 0:
        segs.append(min(hrps, rem))
        rem -= segs[-1]
    pos_to_id = _nosplit32(read_abf_file(abf, pos_idf), segs)
    _, values = decode_dictionary(read_abf_file(abf, dict_e))
    return [values[i - 3] for i in pos_to_id if i >= 3], mn, mx


def test_built_text_hierarchy_is_desktops_order(tmp_path):
    b = PBIXBuilder("b156")
    b.add_table("T", [{"name": "c", "data_type": "String"}, {"name": "v", "data_type": "Int64"}],
                rows=[{"c": c, "v": i} for i, c in enumerate(
                    ["b", "APAC", "Americas", "apple", "Zeta", "a10", "a2", "Bz", "ba", "_x", "1x",
                     "\u00e9clair", "eclair", "\u00d6lund", "olive"])])
    b.add_page("Page 1")
    out = str(tmp_path / "b156.pbix")
    b.save(out)
    order, mn, mx = _hierarchy_order(out, "T", "c")
    assert order == DESKTOP_ORDER
    assert (mn, mx) == ("_x", "Zeta")
