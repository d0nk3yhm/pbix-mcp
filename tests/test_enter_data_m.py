"""Regression tests for issues-13 item 1: inline-data tables must survive a
service/Desktop Refresh.

`pbix_set_table_data` / `pbix_create` embed rows into VertiPaq, but the
partition's M (QueryDefinition) used to be a headers-only `#table(..., {})`.
That renders on open, but a Refresh re-runs the empty M and the table — and
every visual bound to it — goes blank. The fix embeds the rows as Desktop's
"Enter data" literal so a Refresh reproduces the same rows.
"""
import base64
import json
import os
import re
import sqlite3
import tempfile
import zipfile
import zlib

from pbix_mcp.builder import (
    PBIXBuilder,
    _build_enter_data_m,
    _build_m_expression,
    _cell_to_m_text,
)


def _decode_payload(m: str):
    """Extract the base64, raw-inflate, and JSON-parse the embedded matrix —
    exactly what Power Query's Binary.Decompress(..., Compression.Deflate) does."""
    b64 = re.search(r'Binary\.FromText\("([^"]+)"', m).group(1)
    raw = zlib.decompress(base64.b64decode(b64), -zlib.MAX_WBITS)
    return json.loads(raw)


class TestCellSerialization:
    def test_values(self):
        assert _cell_to_m_text(None) is None
        assert _cell_to_m_text(True) == "TRUE"
        assert _cell_to_m_text(False) == "FALSE"
        assert _cell_to_m_text(10) == "10"
        assert _cell_to_m_text(3.0) == "3"          # integer-valued float
        assert _cell_to_m_text(0.153) == "0.153"    # decimal preserved
        assert _cell_to_m_text("Sør") == "Sør"


class TestBuildEnterDataM:
    COLS = [
        {"name": "Region", "data_type": "String"},
        {"name": "Sales Amount", "data_type": "Double"},
        {"name": "Units", "data_type": "Int64"},
        {"name": "Active", "data_type": "Boolean"},
    ]
    ROWS = [
        {"Region": "Nord", "Sales Amount": 1234.56, "Units": 10, "Active": True},
        {"Region": "Sør", "Sales Amount": 0.153, "Units": 3, "Active": False},
        {"Region": None, "Sales Amount": None, "Units": 0, "Active": None},
    ]

    def test_roundtrip_matches_rows(self):
        m = _build_enter_data_m(self.COLS, self.ROWS)
        assert m is not None
        assert _decode_payload(m) == [
            ["Nord", "1234.56", "10", "TRUE"],
            ["Sør", "0.153", "3", "FALSE"],
            [None, None, "0", None],
        ]

    def test_shape_is_desktop_enter_data(self):
        m = _build_enter_data_m(self.COLS, self.ROWS)
        assert "Table.FromRows(Json.Document(Binary.Decompress(" in m
        assert "Compression.Deflate" in m
        assert "Table.TransformColumnTypes(Source" in m
        assert '"en-US"' in m  # invariant parse, immune to model culture

    def test_column_name_with_space_escaped(self):
        m = _build_enter_data_m(self.COLS, self.ROWS)
        assert '#"Sales Amount" = _t' in m          # type ascription
        assert '{"Sales Amount", type number}' in m  # transform

    def test_transform_types(self):
        m = _build_enter_data_m(self.COLS, self.ROWS)
        assert '{"Units", Int64.Type}' in m
        assert '{"Active", type logical}' in m
        assert '{"Region", type text}' in m

    def test_empty_rows_returns_none(self):
        assert _build_enter_data_m(self.COLS, []) is None

    def test_oversized_payload_returns_none(self, monkeypatch):
        import pbix_mcp.builder as bmod
        monkeypatch.setattr(bmod, "_ENTER_DATA_MAX_B64", 50)  # tiny cap
        assert _build_enter_data_m(self.COLS, self.ROWS) is None


class TestBuildMExpressionDispatch:
    COLS = [{"name": "K", "data_type": "String"},
            {"name": "V", "data_type": "Int64"}]
    ROWS = [{"K": "a", "V": 1}, {"K": "b", "V": 2}]

    def test_rows_no_source_uses_enter_data(self):
        m = _build_m_expression("T", self.COLS, rows=self.ROWS)
        assert "Table.FromRows" in m
        assert "#table(type table" not in m

    def test_no_rows_uses_headers_only(self):
        m = _build_m_expression("T", self.COLS, rows=[])
        assert "#table(type table" in m
        assert "Table.FromRows" not in m

    def test_source_csv_takes_precedence_over_rows(self):
        m = _build_m_expression("T", self.COLS, source_csv="/tmp/x.csv",
                                rows=self.ROWS)
        assert "Csv.Document" in m
        assert "Table.FromRows" not in m

    def test_source_db_takes_precedence_over_rows(self):
        m = _build_m_expression("T", self.COLS,
                                source_db={"type": "sqlite", "path": "/tmp/x.db"},
                                rows=self.ROWS)
        assert "Odbc.DataSource" in m
        assert "Table.FromRows" not in m


class TestEndToEnd:
    def _partition_qd(self, pbix_path, table_name):
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        with zipfile.ZipFile(pbix_path) as z:
            db = read_metadata_sqlite(decompress_datamodel(z.read("DataModel")))
        fd, tmp = tempfile.mkstemp(suffix=".db")
        os.write(fd, db)
        os.close(fd)
        try:
            conn = sqlite3.connect(tmp)
            row = conn.execute(
                "SELECT p.QueryDefinition FROM [Partition] p "
                "JOIN [Table] t ON p.TableID = t.ID WHERE t.Name = ?",
                (table_name,),
            ).fetchone()
            conn.close()
            return row[0]
        finally:
            os.unlink(tmp)

    def test_inline_table_partition_is_refreshable(self, tmp_path):
        from pbix_mcp.formats.model_reader import ModelReader
        p = str(tmp_path / "inline.pbix")
        b = PBIXBuilder("Inline")
        b.add_table("Config", [
            {"name": "Key", "data_type": "String"},
            {"name": "Value", "data_type": "Double"},
        ], rows=[{"Key": "alpha", "Value": 1.5}, {"Key": "beta", "Value": 2.25}])
        b.add_page("P")
        b.save(p)

        qd = self._partition_qd(p, "Config")
        assert "Table.FromRows" in qd
        assert "#table(type table" not in qd
        # the embedded payload reproduces exactly the rows
        assert _decode_payload(qd) == [["alpha", "1.5"], ["beta", "2.25"]]

        # and VertiPaq still reads back (initial open is unaffected)
        td = ModelReader(p).get_table("Config", max_rows=10)
        assert td["rows"] == [["alpha", 1.5], ["beta", 2.25]]
