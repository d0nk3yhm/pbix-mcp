"""Regression: ABF binary splice must keep the VirtualDirectory DataSize correct.

`splice_metadata_in_abf` re-writes the BackupLogHeader ``DataSize`` (the byte
length of the VirtualDirectory the reader slices off ``m_cbOffsetHeader``). It
used to size that region from the position of ``<VirtualDirectory>`` — which for
a UTF-16-LE VDir sits *after* the 2-byte BOM, while ``m_cbOffsetHeader`` points
*at* the BOM. The result was a ``DataSize`` 2 bytes short: the reader dropped the
final ``>`` of ``</VirtualDirectory>`` and every subsequent parse raised
``unclosed token``. Power BI Desktop tolerated it, but pbix-mcp's own reader
(any read after a measure/metadata edit) broke — badly for large DAX measures
(e.g. HTML-content measures) that grow the VDir.
"""
import io
import zipfile

from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import (
    _parse_header_xml,
    list_abf_files,
    read_metadata_sqlite,
    rebuild_abf_with_modified_sqlite,
)
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


def _minimal_abf() -> bytes:
    b = PBIXBuilder("T")
    b.add_table("Data", [{"name": "ID", "data_type": "Int64"}],
                rows=[{"ID": i} for i in range(5)])
    pbix = b.build()
    dm = zipfile.ZipFile(io.BytesIO(pbix)).read("DataModel")
    return decompress_datamodel(dm)


def _add_measure(name: str, expr: str):
    def _mod(conn):
        c = conn.cursor()
        tid = c.execute("SELECT ID FROM [Table] WHERE Name='Data'").fetchone()[0]
        mrow = c.execute(
            "SELECT Value FROM DBPROPERTIES WHERE Name='MAXID'").fetchone()
        new_id = (int(mrow[0]) if mrow else 1000) + 1
        c.execute(
            "INSERT INTO Measure (ID, TableID, Name, DataType, Expression, "
            "IsHidden, State, KPIID, IsSimpleMeasure, DetailRowsDefinitionID, "
            "FormatStringDefinitionID) VALUES (?,?,?,6,?,0,1,0,0,0,0)",
            (new_id, tid, name, expr),
        )
        c.execute("UPDATE DBPROPERTIES SET Value=? WHERE Name='MAXID'",
                  (str(new_id),))
        conn.commit()
    return _mod


def _assert_vdir_consistent(abf: bytes):
    hdr = _parse_header_xml(abf)
    voff = int(hdr.findtext("m_cbOffsetHeader"))
    dsize = int(hdr.findtext("DataSize"))
    # The sliced region must be exactly the VDir and end on its closing tag.
    raw = abf[voff:voff + dsize]
    close = "</VirtualDirectory>".encode("utf-16-le")
    assert raw.endswith(close), (
        "DataSize does not reach the VirtualDirectory close tag "
        f"(voff={voff}, DataSize={dsize}) — off-by-BOM regression")
    # And the reader must parse the whole ABF file log without error.
    files = list_abf_files(abf)
    assert files, "list_abf_files returned nothing"
    # No embedded file may overlap its neighbour or run past the blob: the splice
    # shifts every offset after the metadata by size_diff, and a collision in that
    # shift (two offsets differing by exactly size_diff) used to leave one entry
    # stale, overlapping the next segment — Power BI then fails the DBCC data-
    # segment check on load.
    ordered = sorted(files, key=lambda f: f["m_cbOffsetHeader"])
    for i, f in enumerate(ordered):
        end = f["m_cbOffsetHeader"] + f["Size"]
        assert end <= len(abf), f"{f['FileName']} runs past the ABF blob"
        if i + 1 < len(ordered):
            assert end <= ordered[i + 1]["m_cbOffsetHeader"], (
                f"{f['FileName']} offset overlaps the next segment "
                "(stale-offset splice regression)")


def test_splice_small_measure_keeps_datasize_valid():
    abf = _minimal_abf()
    out = rebuild_abf_with_modified_sqlite(abf, _add_measure("M", "SUM(Data[ID])"))
    _assert_vdir_consistent(out)
    read_metadata_sqlite(out)  # must not raise


def test_splice_large_html_measure_and_repeated_edits():
    """A multi-KB HTML-string measure grows the VDir; repeated splices must
    each stay readable (this is the exact HTML-visual authoring path)."""
    abf = _minimal_abf()
    html = '"' + ("<div style='padding:8px'>x</div>" * 300) + '"'  # ~9.6 KB
    for i in range(4):
        abf = rebuild_abf_with_modified_sqlite(abf, _add_measure(f"H{i}", html))
        _assert_vdir_consistent(abf)
    measures = read_metadata_sqlite(abf)  # must not raise
    assert measures
