"""Issue #34: TMDL import (inverse of the exporter) + PBIP open/save.

Round-trip contract: pbix_export_tmdl -> pbix_import_tmdl -> pbix_export_tmdl
produces byte-identical TMDL files. PBIP contract: a project folder opens as
a live session, edits apply, pbix_save persists them back into the folder.
"""
import json
import os

import pytest

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.tmdl_reader import (
    parse_tmdl_document,
    parse_tmdl_string,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Parser unit tests
# ---------------------------------------------------------------------------

class TestTmdlParser:
    def test_object_names_and_props(self):
        doc = (
            "table 'My Table'\n"
            "\tisHidden\n"
            "\tlineageTag: abc-123\n"
            "\n"
            "\tcolumn 'Region'\n"
            "\t\tdataType: string\n"
            "\t\tisKey\n"
        )
        nodes = parse_tmdl_document(doc)
        assert len(nodes) == 1
        t = nodes[0]
        assert t.kind == "table" and t.name == "My Table"
        assert "isHidden" in t.flags
        assert t.props["lineageTag"] == "abc-123"
        cols = t.all("column")
        assert len(cols) == 1 and cols[0].name == "Region"
        assert cols[0].props["dataType"] == "string"
        assert "isKey" in cols[0].flags

    def test_quoted_name_escapes(self):
        nodes = parse_tmdl_document("table 'O''Brien''s'\n")
        assert nodes[0].name == "O'Brien's"

    def test_inline_and_multiline_expressions(self):
        doc = (
            "table T\n"
            "\tmeasure 'One Line' = SUM(T[V])\n"
            "\t\tformatString: #,0\n"
            "\n"
            "\tmeasure 'Multi' =\n"
            "\t\t\tVAR x = 1\n"
            "\t\t\tRETURN x\n"
            "\t\tisHidden\n"
        )
        t = parse_tmdl_document(doc)[0]
        one, multi = t.all("measure")
        assert one.expression == "SUM(T[V])"
        assert one.props["formatString"] == "#,0"
        assert multi.expression == "VAR x = 1\nRETURN x"
        assert "isHidden" in multi.flags

    def test_partition_source_block(self):
        doc = (
            "table T\n"
            "\tpartition 'T-part' = m\n"
            "\t\tmode: import\n"
            "\t\tsource =\n"
            "\t\t\t\tlet\n"
            "\t\t\t\t    Source = 1\n"
            "\t\t\t\tin\n"
            "\t\t\t\t    Source\n"
        )
        t = parse_tmdl_document(doc)[0]
        p = t.all("partition")[0]
        assert p.expression == "m"
        assert p.props["mode"] == "import"
        src = p.child("source")
        assert src.expression == "let\n    Source = 1\nin\n    Source"

    def test_relationship_refs(self):
        doc = (
            "relationship abc-def\n"
            "\tfromColumn: 'Fact Sales'.'Region ID'\n"
            "\ttoColumn: Geo.RegionID\n"
            "\tisActive: false\n"
            "\tcrossFilteringBehavior: bothDirections\n"
        )
        model = parse_tmdl_string(doc + "\ntable T\n\tcolumn C\n\t\tdataType: string\n")
        r = model["relationships"][0]
        assert (r["from_table"], r["from_column"]) == ("Fact Sales", "Region ID")
        assert (r["to_table"], r["to_column"]) == ("Geo", "RegionID")
        assert r["is_active"] is False
        assert r["cross_filtering_behavior"] == 2

    def test_extended_property_and_lineage(self):
        doc = (
            "table P\n"
            "\tcolumn 'Fields'\n"
            "\t\tdataType: string\n"
            "\t\tlineageTag: tag-1\n"
            "\t\textendedProperty ParameterMetadata =\n"
            '\t\t\t\t{"version":3,"kind":2}\n'
        )
        model = parse_tmdl_string(doc)
        col = model["tables"][0]["columns"][0]
        assert col["lineage_tag"] == "tag-1"
        eps = col["extended_properties"]
        assert eps == [{"name": "ParameterMetadata", "type": 1,
                        "value": '{"version":3,"kind":2}'}]

    def test_hierarchy_levels(self):
        doc = (
            "table Geo\n"
            "\tcolumn Country\n"
            "\t\tdataType: string\n"
            "\thierarchy 'Geography'\n"
            "\t\tlevel Country\n"
            "\t\t\tcolumn: Country\n"
        )
        model = parse_tmdl_string(doc)
        h = model["tables"][0]["hierarchies"][0]
        assert h["name"] == "Geography"
        assert h["levels"] == [{"name": "Country", "column": "Country",
                                "lineage_tag": None}]

    def test_no_tables_raises(self):
        with pytest.raises(ValueError, match="No tables"):
            parse_tmdl_string("database D\n\tcompatibilityLevel: 1567\n")


# ---------------------------------------------------------------------------
# Round trip: export -> import -> export is byte-stable
# ---------------------------------------------------------------------------

def _build_source_pbix(path: str) -> None:
    b = PBIXBuilder("DemoModel")
    b.add_table("Sales", [
        {"name": "Region", "data_type": "String"},
        {"name": "Amount", "data_type": "Double"},
        {"name": "Qty", "data_type": "Int64"},
    ], rows=[{"Region": "East", "Amount": 10.0, "Qty": 1},
             {"Region": "West", "Amount": 20.0, "Qty": 2}])
    b.add_table("Geo", [
        {"name": "Region", "data_type": "String"},
        {"name": "Country", "data_type": "String"},
    ], rows=[{"Region": "East", "Country": "US"},
             {"Region": "West", "Country": "US"}])
    b.add_measure("Sales", "Total Amount", "SUM(Sales[Amount])",
                  format_string="#,0.00")
    b.add_measure("Sales", "Multi Line",
                  "VAR x = SUM(Sales[Qty])\nRETURN x * 2")
    b.add_relationship("Sales", "Region", "Geo", "Region")
    b.add_user_hierarchy("Geo", "Geography", [
        {"name": "Country", "column": "Country"},
        {"name": "Region", "column": "Region"},
    ])
    b.save(path)


def _tree_bytes(root: str) -> dict:
    out = {}
    for dirpath, _dirs, files in os.walk(root):
        for f in files:
            p = os.path.join(dirpath, f)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, root).replace("\\", "/")] = fh.read()
    return out


class TestTmdlRoundTrip:
    def test_export_import_export_byte_stable(self, tmp_path):
        src = str(tmp_path / "src.pbix")
        _build_source_pbix(src)
        try:
            assert json.loads(server.pbix_open(src, "rt_src"))["success"]
            assert json.loads(server.pbix_set_rls_role(
                "rt_src", "USOnly", "Geo", '[Country] = "US"'))["success"]

            dir_a = str(tmp_path / "tmdlA")
            assert json.loads(server.pbix_export_tmdl("rt_src", dir_a))["success"]

            imported = str(tmp_path / "imported.pbix")
            out = json.loads(server.pbix_import_tmdl(dir_a, imported, alias="rt_imp"))
            assert out["success"], out.get("message")

            dir_b = str(tmp_path / "tmdlB")
            assert json.loads(server.pbix_export_tmdl("rt_imp", dir_b))["success"]

            a, bb = _tree_bytes(dir_a), _tree_bytes(dir_b)
            assert set(a) == set(bb), (set(a) ^ set(bb))
            for rel in a:
                assert a[rel] == bb[rel], f"{rel} differs:\n" + \
                    bb[rel].decode("utf-8", "replace")
        finally:
            for al in ("rt_src", "rt_imp"):
                server._open_files.pop(al, None)
                server._dax_cache.pop(al, None)

    def test_import_preserves_lineage_and_field_param_metadata(self, tmp_path):
        # Hand-written TMDL with lineage tags + a ParameterMetadata extended
        # property; both must land in the metadata and survive re-export.
        tdir = tmp_path / "tmdl" / "tables"
        tdir.mkdir(parents=True)
        (tmp_path / "tmdl" / "database.tmdl").write_text(
            "database Lineage\n\tcompatibilityLevel: 1567\n", encoding="utf-8")
        (tdir / "P.tmdl").write_text(
            "table 'P'\n"
            "\tlineageTag: tab-tag\n"
            "\n"
            "\tcolumn 'Fields'\n"
            "\t\tdataType: string\n"
            "\t\tlineageTag: col-tag\n"
            "\t\textendedProperty ParameterMetadata =\n"
            '\t\t\t\t{"version":3,"kind":2}\n'
            "\n"
            "\tmeasure 'M' = 1\n"
            "\t\tlineageTag: meas-tag\n",
            encoding="utf-8")
        out_pbix = str(tmp_path / "lineage.pbix")
        out = json.loads(server.pbix_import_tmdl(
            str(tmp_path / "tmdl"), out_pbix, alias="rt_lin"))
        assert out["success"], out.get("message")
        try:
            dir_b = str(tmp_path / "tmdlB")
            assert json.loads(server.pbix_export_tmdl("rt_lin", dir_b))["success"]
            txt = open(os.path.join(dir_b, "tables", "P.tmdl"),
                       encoding="utf-8").read()
            assert "lineageTag: tab-tag" in txt
            assert "lineageTag: col-tag" in txt
            assert "lineageTag: meas-tag" in txt
            assert "extendedProperty ParameterMetadata" in txt
            assert '{"version":3,"kind":2}' in txt
        finally:
            server._open_files.pop("rt_lin", None)
            server._dax_cache.pop("rt_lin", None)

    def test_import_single_document_string(self, tmp_path):
        doc = tmp_path / "one.tmdl"
        doc.write_text(
            "database One\n"
            "\tcompatibilityLevel: 1567\n"
            "\n"
            "table T\n"
            "\tcolumn 'V'\n"
            "\t\tdataType: double\n"
            "\tmeasure 'Sum V' = SUM(T[V])\n",
            encoding="utf-8")
        out = json.loads(server.pbix_import_tmdl(
            str(doc), str(tmp_path / "one.pbix"), alias="rt_one"))
        assert out["success"], out.get("message")
        try:
            meas = json.loads(server.pbix_get_model_measures("rt_one"))
            assert meas["success"] and "Sum V" in meas["message"]
        finally:
            server._open_files.pop("rt_one", None)
            server._dax_cache.pop("rt_one", None)

    def test_import_missing_path_clean_error(self, tmp_path):
        out = json.loads(server.pbix_import_tmdl(
            str(tmp_path / "nope"), str(tmp_path / "x.pbix")))
        assert out["success"] is False
        assert "not found" in out["message"]


# ---------------------------------------------------------------------------
# PBIP: open a project folder as a live doc, edit, save back
# ---------------------------------------------------------------------------

class TestPbipOpenSave:
    def test_pbip_open_edit_save_cycle(self, tmp_path):
        src = str(tmp_path / "demo.pbix")
        b = PBIXBuilder("DemoModel")
        b.add_table("Sales", [
            {"name": "Region", "data_type": "String"},
            {"name": "Amount", "data_type": "Double"},
        ], rows=[{"Region": "East", "Amount": 10.0}])
        b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
        b.add_page("Page 1")
        b.save(src)

        pbip_dir = str(tmp_path / "demo_pbip")
        try:
            assert json.loads(server.pbix_open(src, "pb_src"))["success"]
            assert json.loads(server.pbix_export_pbip("pb_src", pbip_dir))["success"]
        finally:
            server._open_files.pop("pb_src", None)

        try:
            out = json.loads(server.pbix_open_pbip(pbip_dir, "pb_proj"))
            assert out["success"], out.get("message")

            # the model half is live
            meas = json.loads(server.pbix_get_model_measures("pb_proj"))
            assert meas["success"] and "Total" in meas["message"]

            # edit + save back into the folder
            out = json.loads(server.pbix_datamodel_add_measure(
                "pb_proj", "Sales", "Doubled", "SUM(Sales[Amount]) * 2"))
            assert out["success"], out.get("message")
            out = json.loads(server.pbix_save("pb_proj"))
            assert out["success"], out.get("message")

            sales_tmdl = os.path.join(
                pbip_dir, "demo.SemanticModel", "definition", "tables",
                "Sales.tmdl")
            txt = open(sales_tmdl, encoding="utf-8").read()
            assert "Doubled" in txt

            # save-as still converts to a real .pbix
            conv = str(tmp_path / "converted.pbix")
            out = json.loads(server.pbix_save("pb_proj", output_path=conv))
            assert out["success"] and os.path.exists(conv)
        finally:
            server._open_files.pop("pb_proj", None)
            server._dax_cache.pop("pb_proj", None)

    def test_open_pbip_rejects_non_project(self, tmp_path):
        out = json.loads(server.pbix_open_pbip(str(tmp_path)))
        assert out["success"] is False
        assert ".pbip" in out["message"]
