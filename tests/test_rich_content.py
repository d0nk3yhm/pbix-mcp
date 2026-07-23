"""Rich-content authoring (issues-8): public-visual references (Deneb),
ImageUrl DataCategory authoring, Desktop-complete field parameters, and SVG
data-URI measure codegen.

Ground truth for the field-parameter metadata shape is a Desktop-authored
file (test_corpus/Ecommerce_Conversion.pbix — two genuine field parameters
with ParameterMetadata = {"version":3,"kind":2}); the Deneb registration
shape is service-verified (app.powerbi.com, certified-only tenant).
"""
import json
import os
import sqlite3
import tempfile

import pytest

from pbix_mcp import server, svg_measures
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

pytestmark = pytest.mark.unit

DENEB_GUID = "deneb7E15AEF80B9E4D4F8E12924291ECE89A"


def _build_sales_pbix(path):
    b = PBIXBuilder("T")
    b.add_table("Sales", [
        {"name": "Idx", "data_type": "Int64"},
        {"name": "Region", "data_type": "String"},
        {"name": "Revenue", "data_type": "Double"},
    ], rows=[{"Idx": 1, "Region": "N", "Revenue": 100.0},
             {"Idx": 2, "Region": "S", "Revenue": 300.0},
             {"Idx": 3, "Region": "E", "Revenue": 200.0}])
    b.add_measure("Sales", "Total Revenue", "SUM(Sales[Revenue])")
    b.add_measure("Sales", "Total Units", "COUNTROWS(Sales)")
    b.save(path)


def _metadata_conn(alias):
    """Open the alias's current metadata sqlite; caller closes conn and
    unlinks the returned path."""
    info = server._open_files[alias]
    with open(os.path.join(info["work_dir"], "DataModel"), "rb") as f:
        db = read_metadata_sqlite(decompress_datamodel(f.read()))
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, db)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    conn.row_factory = sqlite3.Row
    return conn, tmp


def _cleanup(*aliases):
    for a in aliases:
        server._open_files.pop(a, None)
        server._dax_cache.pop(a, None)


class TestReferencePublicVisual:
    """§1.3: register an AppSource visual by GUID — no file payload."""

    def test_register_dedupe_and_reference_only(self, tmp_path):
        p = str(tmp_path / "deneb.pbix")
        _build_sales_pbix(p)
        alias = "rc_ref"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_reference_public_visual(alias, DENEB_GUID))
            assert out["success"]
            assert out["data"]["publicCustomVisuals"] == [DENEB_GUID]
            # idempotent
            out = json.loads(server.pbix_reference_public_visual(alias, DENEB_GUID))
            assert out["success"]
            assert out["data"]["publicCustomVisuals"] == [DENEB_GUID]

            wd = server._open_files[alias]["work_dir"]
            layout = server._get_layout(wd)
            assert layout["publicCustomVisuals"] == [DENEB_GUID]
            # ZERO file parts — this is the whole point vs pbix_add_custom_visual
            assert not os.path.exists(
                os.path.join(wd, "Report", "CustomVisuals"))
        finally:
            _cleanup(alias)

    def test_invalid_guid_rejected(self, tmp_path):
        p = str(tmp_path / "deneb2.pbix")
        _build_sales_pbix(p)
        alias = "rc_ref2"
        try:
            server.pbix_open(p, alias)
            for bad in ("bad-guid!", "", "a b", "x;DROP"):
                out = json.loads(server.pbix_reference_public_visual(alias, bad))
                assert out["success"] is False, bad
        finally:
            _cleanup(alias)

    def test_remove_custom_visual_deregisters_reference(self, tmp_path):
        p = str(tmp_path / "deneb3.pbix")
        _build_sales_pbix(p)
        alias = "rc_ref3"
        try:
            server.pbix_open(p, alias)
            json.loads(server.pbix_reference_public_visual(alias, DENEB_GUID))
            out = json.loads(server.pbix_remove_custom_visual(alias, DENEB_GUID))
            assert out["success"]
            layout = server._get_layout(server._open_files[alias]["work_dir"])
            assert layout["publicCustomVisuals"] == []
        finally:
            _cleanup(alias)

    def test_deneb_visual_authoring_end_to_end(self, tmp_path):
        """The full service-verified recipe: reference + objects.vega string
        Literals + dataset-role binding."""
        p = str(tmp_path / "deneb4.pbix")
        _build_sales_pbix(p)
        alias = "rc_ref4"

        def lit(s):
            return {"expr": {"Literal": {"Value": "'" + s.replace("'", "''") + "'"}}}

        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "P1")
            json.loads(server.pbix_reference_public_visual(alias, DENEB_GUID))
            spec = json.dumps({"data": {"name": "dataset"}, "mark": "bar"})
            cfg = json.dumps({"singleVisual": {
                "visualType": DENEB_GUID,
                "projections": {"dataset": [
                    {"queryRef": "Sales.Region"},
                    {"queryRef": "Sales.Total Revenue"}]},
                "prototypeQuery": {
                    "Version": 2,
                    "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
                    "Select": [
                        {"Column": {"Expression": {"SourceRef": {"Source": "s"}},
                                    "Property": "Region"}, "Name": "Sales.Region"},
                        {"Measure": {"Expression": {"SourceRef": {"Source": "s"}},
                                     "Property": "Total Revenue"},
                         "Name": "Sales.Total Revenue"},
                    ],
                },
                "objects": {"vega": [{"properties": {
                    "provider": lit("vegaLite"),
                    "version": lit("6.4.1"),
                    "jsonSpec": lit(spec),
                    "jsonConfig": lit("{}"),
                }}]},
            }})
            out = json.loads(server.pbix_add_visual(
                alias, 0, DENEB_GUID, config_json=cfg))
            assert out["success"], out

            layout = server._get_layout(server._open_files[alias]["work_dir"])
            vc = layout["sections"][0]["visualContainers"][-1]
            sv = json.loads(vc["config"])["singleVisual"]
            assert sv["visualType"] == DENEB_GUID
            props = sv["objects"]["vega"][0]["properties"]
            assert props["provider"]["expr"]["Literal"]["Value"] == "'vegaLite'"
            assert "dataset" in props["jsonSpec"]["expr"]["Literal"]["Value"]
            # the dataset role compiled into query/dataTransforms
            dt = json.loads(vc["dataTransforms"])
            assert dt["projectionOrdering"].get("dataset") == [0, 1]
        finally:
            _cleanup(alias)


class TestDataCategoryAuthoring:
    """§2.2: data_category on measures + table columns, surviving rebuilds."""

    def test_add_measure_with_data_category(self, tmp_path):
        p = str(tmp_path / "dc1.pbix")
        _build_sales_pbix(p)
        alias = "rc_dc1"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_datamodel_add_measure(
                alias, "Sales", "Img",
                '"data:image/svg+xml;utf8,<svg/>"',
                data_category="ImageUrl"))
            assert out["success"], out
            conn, tmp = _metadata_conn(alias)
            try:
                dc = conn.execute(
                    "SELECT DataCategory FROM Measure WHERE Name='Img'"
                ).fetchone()[0]
                assert dc == "ImageUrl"
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)

    def test_modify_measure_sets_data_category(self, tmp_path):
        p = str(tmp_path / "dc2.pbix")
        _build_sales_pbix(p)
        alias = "rc_dc2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_datamodel_modify_measure(
                alias, "Total Revenue", "SUM(Sales[Revenue])",
                new_data_category="ImageUrl"))
            assert out["success"], out
            conn, tmp = _metadata_conn(alias)
            try:
                dc = conn.execute(
                    "SELECT DataCategory FROM Measure WHERE Name='Total Revenue'"
                ).fetchone()[0]
                assert dc == "ImageUrl"
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)

    def test_set_table_data_column_category_survives_rebuild(self, tmp_path):
        """The critical regression: DataCategory (measure AND column) must
        survive a later rebuild-based edit — the collection queries used to
        drop it."""
        p = str(tmp_path / "dc3.pbix")
        _build_sales_pbix(p)
        alias = "rc_dc3"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_set_table_data(alias, "Imgs", json.dumps({
                "columns": [
                    {"name": "K", "data_type": "String"},
                    {"name": "Img", "data_type": "String",
                     "data_category": "ImageUrl"},
                ],
                "rows": [{"K": "a", "Img": "data:image/svg+xml;utf8,<svg/>"}],
            })))
            assert out["success"], out
            out = json.loads(server.pbix_datamodel_modify_measure(
                alias, "Total Units", "COUNTROWS(Sales)",
                new_data_category="ImageUrl"))
            assert out["success"], out

            # ANY rebuild-based edit (another set_table_data) used to wipe it
            out = json.loads(server.pbix_set_table_data(alias, "Other", json.dumps({
                "columns": [{"name": "X", "data_type": "Int64"}],
                "rows": [{"X": 1}],
            })))
            assert out["success"], out

            conn, tmp = _metadata_conn(alias)
            try:
                dc = conn.execute(
                    "SELECT DataCategory FROM [Column] WHERE ExplicitName='Img'"
                ).fetchone()[0]
                assert dc == "ImageUrl", "column DataCategory lost in rebuild"
                mdc = conn.execute(
                    "SELECT DataCategory FROM Measure WHERE Name='Total Units'"
                ).fetchone()[0]
                assert mdc == "ImageUrl", "measure DataCategory lost in rebuild"
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)


class TestFieldParameterComplete:
    """§3.2: the Desktop-complete field parameter (ground truth:
    Ecommerce_Conversion.pbix)."""

    def _create(self, alias):
        return json.loads(server.pbix_datamodel_add_field_parameter(
            alias, "Metric", json.dumps([
                {"display": "Revenue", "ref": "Sales[Total Revenue]"},
                {"display": "Units", "ref": "'Sales'[Total Units]"},
            ])))

    def _assert_desktop_shape(self, conn):
        t = dict(conn.execute(
            "SELECT * FROM [Table] WHERE Name='Metric'").fetchone())
        assert t["SystemFlags"] == 2
        cols = [dict(r) for r in conn.execute(
            "SELECT * FROM [Column] WHERE TableID=? AND Type IN (1,4) "
            "AND ExplicitName NOT LIKE 'RowNumber%' ORDER BY ID", (t["ID"],))]
        assert [c["ExplicitName"] for c in cols] == \
            ["Metric", "Metric Fields", "Metric Order"]
        disp, flds, order = cols
        for c, src in zip(cols, ("[Value1]", "[Value2]", "[Value3]")):
            assert c["Type"] == 4 and c["ExplicitDataType"] == 1
            assert c["SourceColumn"] == src
            # Desktop stamps SystemFlags=2 + IsAvailableInMDX=1 on every
            # calc-table column (ground truth: Ecommerce corpus)
            assert c["SystemFlags"] == 2 and c["IsAvailableInMDX"] == 1
        assert disp["IsHidden"] == 0 and disp["SortByColumnID"] == order["ID"]
        assert disp["SummarizeBy"] == 2 and disp["InferredDataType"] == 2
        assert flds["IsHidden"] == 1 and flds["SortByColumnID"] == order["ID"]
        assert order["IsHidden"] == 1 and order["FormatString"] == "0"
        assert order["SummarizeBy"] == 3 and order["InferredDataType"] == 6

        part = dict(conn.execute(
            "SELECT * FROM [Partition] WHERE TableID=?", (t["ID"],)).fetchone())
        assert part["Type"] == 2 and part["Mode"] == 0
        assert part["SystemFlags"] == 2
        assert "NAMEOF('Sales'[Total Revenue])" in part["QueryDefinition"]

        ep = [dict(r) for r in conn.execute(
            "SELECT * FROM ExtendedProperty WHERE Name='ParameterMetadata' "
            "AND ObjectID=?", (flds["ID"],))]
        assert len(ep) == 1
        assert ep[0]["ObjectType"] == 4 and ep[0]["Type"] == 1
        assert ep[0]["Value"] == '{"version":3,"kind":2}'

        rcd = dict(conn.execute(
            "SELECT * FROM RelatedColumnDetails WHERE ColumnID=?",
            (disp["ID"],)).fetchone())
        gbc = dict(conn.execute(
            "SELECT * FROM GroupByColumn WHERE RelatedColumnDetailsID=?",
            (rcd["ID"],)).fetchone())
        assert gbc["GroupingColumnID"] == flds["ID"]
        assert disp["RelatedColumnDetailsID"] == rcd["ID"]

        # MAXID invariant (issue #4) must hold after the metadata splice
        maxid = int(conn.execute(
            "SELECT Value FROM DBPROPERTIES WHERE Name='MAXID'").fetchone()[0])
        actual = 0
        for (n,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"):
            try:
                m = conn.execute(f"SELECT MAX(ID) FROM [{n}]").fetchone()[0]
                if m is not None:
                    actual = max(actual, int(m))
            except sqlite3.Error:
                continue
        assert maxid >= actual

    def test_creates_desktop_shape(self, tmp_path):
        p = str(tmp_path / "fp1.pbix")
        _build_sales_pbix(p)
        alias = "rc_fp1"
        try:
            server.pbix_open(p, alias)
            out = self._create(alias)
            assert out["success"], out
            conn, tmp = _metadata_conn(alias)
            try:
                self._assert_desktop_shape(conn)
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)

    def test_unknown_ref_fails_loud(self, tmp_path):
        p = str(tmp_path / "fp2.pbix")
        _build_sales_pbix(p)
        alias = "rc_fp2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_datamodel_add_field_parameter(
                alias, "Bad", json.dumps(
                    [{"display": "X", "ref": "Sales[No Such Measure]"}])))
            assert out["success"] is False
            assert "not found" in out["message"]
        finally:
            _cleanup(alias)

    def test_survives_rebuild_and_multiple_params(self, tmp_path):
        """The reason this shape is safe to author: rebuild-based edits used
        to refuse models with calculated partitions."""
        p = str(tmp_path / "fp3.pbix")
        _build_sales_pbix(p)
        alias = "rc_fp3"
        try:
            server.pbix_open(p, alias)
            assert self._create(alias)["success"]

            # 1) a rebuild-based edit succeeds (no UnsupportedModelEditError)
            out = json.loads(server.pbix_set_table_data(alias, "Extra", json.dumps({
                "columns": [{"name": "X", "data_type": "Int64"}],
                "rows": [{"X": 1}]})))
            assert out["success"], out

            # 2) a SECOND field parameter (itself a rebuild) coexists
            out = json.loads(server.pbix_datamodel_add_field_parameter(
                alias, "Metric2", json.dumps(
                    [{"display": "R", "ref": "Sales[Total Revenue]"}])))
            assert out["success"], out

            conn, tmp = _metadata_conn(alias)
            try:
                self._assert_desktop_shape(conn)  # first param re-stamped intact
                n = conn.execute(
                    "SELECT COUNT(*) FROM ExtendedProperty "
                    "WHERE Name='ParameterMetadata'").fetchone()[0]
                assert n == 2
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)

    def test_dax_over_field_parameter(self, tmp_path):
        """SWITCH(SELECTEDVALUE(...)) over the parameter table evaluates —
        i.e. our own readers handle the Type=4 columns."""
        p = str(tmp_path / "fp4.pbix")
        _build_sales_pbix(p)
        alias = "rc_fp4"
        try:
            server.pbix_open(p, alias)
            assert self._create(alias)["success"]
            out = json.loads(server.pbix_datamodel_add_measure(
                alias, "Sales", "Selected",
                "SWITCH(SELECTEDVALUE('Metric'[Metric Order]), "
                "0, [Total Revenue], 1, [Total Units], [Total Revenue])"))
            assert out["success"], out
            ev = json.loads(server.pbix_evaluate_dax(
                alias=alias, measures="Selected",
                filter_context='{"Metric.Metric Order": [1]}'))
            assert ev["success"] and ev["results"][0]["value"] == 3, ev
        finally:
            _cleanup(alias)


class TestSvgMeasures:
    """§3.3: SVG data-URI measure codegen."""

    def test_catalog(self):
        out = json.loads(server.pbix_svg_measure())
        assert out["success"]
        assert set(out["data"]["templates"]) == {
            "data_bar", "bullet", "pill", "icon_updown", "sparkline"}

    def test_render_all_kinds_data_uri_hygiene(self):
        specs = {
            "data_bar": {"value": "[M]", "max_value": "100"},
            "bullet": {"value": "[M]", "target": "50", "max_value": "100"},
            "pill": {"text": 'FORMAT([M], "0")'},
            "icon_updown": {"value": "[M]"},
            "sparkline": {"table": "T", "category": "C", "value": "[M]"},
        }
        for kind, spec in specs.items():
            out = json.loads(server.pbix_svg_measure(kind, json.dumps(spec)))
            assert out["success"], (kind, out)
            dax = out["data"]["dax"]
            assert "data:image/svg+xml;utf8," in dax
            # every color percent-encoded — a raw '#' would truncate the URI.
            # (the '#' search argument of the runtime escape chain
            # SUBSTITUTE(..., "#", "%23") is an instruction, not URI content)
            stripped = dax.replace('"#", "%23"', "").replace("%23", "")
            assert "#" not in stripped, kind
            assert out["data"]["chars"] < 32000

    def test_bad_inputs_clean_errors(self):
        out = json.loads(server.pbix_svg_measure("nope", "{}"))
        assert out["success"] is False and out["error_code"] == "BAD_TEMPLATE"
        out = json.loads(server.pbix_svg_measure(
            "data_bar", json.dumps({"value": "[M]", "max_value": "1",
                                    "fill": "javascript:alert(1)"})))
        assert out["success"] is False
        out = json.loads(server.pbix_svg_measure(
            "data_bar", json.dumps({"value": "[M]", "max_value": "1",
                                    "nope": 1})))
        assert out["success"] is False
        with pytest.raises(ValueError):
            svg_measures.render("data_bar", {"value": "", "max_value": "1"})

    def test_turnkey_add_and_engine_evaluation(self, tmp_path):
        p = str(tmp_path / "svg1.pbix")
        _build_sales_pbix(p)
        alias = "rc_svg1"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_svg_measure(
                "data_bar",
                json.dumps({"value": "[Total Revenue]", "max_value": "1200"}),
                alias=alias, measure_name="Rev Bar"))
            assert out["success"] and out["data"]["added"], out

            conn, tmp = _metadata_conn(alias)
            try:
                dc = conn.execute(
                    "SELECT DataCategory FROM Measure WHERE Name='Rev Bar'"
                ).fetchone()[0]
                assert dc == "ImageUrl"
            finally:
                conn.close()
                os.unlink(tmp)

            ev = json.loads(server.pbix_evaluate_dax(
                alias=alias, measures="Rev Bar"))
            val = ev["results"][0]["value"]
            assert ev["success"] and isinstance(val, str)
            assert val.startswith("data:image/svg+xml;utf8,<svg"), val
            # 600 / 1200 of the 120px default width = 60px filled
            assert "width='60'" in val, val
        finally:
            _cleanup(alias)

    def test_sparkline_evaluates_with_normalized_points(self, tmp_path):
        p = str(tmp_path / "svg2.pbix")
        _build_sales_pbix(p)
        alias = "rc_svg2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_svg_measure(
                "sparkline",
                json.dumps({"table": "Sales", "category": "Idx",
                            "value": "[Total Revenue]"}),
                alias=alias, measure_name="Spark"))
            assert out["success"], out
            ev = json.loads(server.pbix_evaluate_dax(alias=alias, measures="Spark"))
            val = ev["results"][0]["value"]
            assert isinstance(val, str) and "polyline" in val, ev
            pts = val.split("points='")[1].split("'")[0].split()
            assert len(pts) == 3  # one per Idx value
            # min revenue (100) sits at the bottom, max (300) at the top
            ys = [float(pt.split(",")[1]) for pt in pts]
            assert ys[0] == max(ys) and ys[1] == min(ys)
        finally:
            _cleanup(alias)


class TestReviewRoundHardening:
    """Regressions from the pre-release adversarial review of this round."""

    def test_display_names_with_parens_do_not_poison_rebuilds(self, tmp_path):
        """A display like "Growth :)" or "a) Revenue" must not make the FP
        undetectable (which locked EVERY later rebuild-based edit behind
        MODEL_EDIT_UNSUPPORTED)."""
        p = str(tmp_path / "paren.pbix")
        _build_sales_pbix(p)
        alias = "rc_paren"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_datamodel_add_field_parameter(
                alias, "P", json.dumps([
                    {"display": "Growth :)", "ref": "Sales[Total Revenue]"},
                    {"display": "a) Revenue (net", "ref": "Sales[Total Units]"},
                ])))
            assert out["success"], out
            # the poisoning repro: the very next rebuild-based edit
            out = json.loads(server.pbix_set_table_data(alias, "X", json.dumps({
                "columns": [{"name": "A", "data_type": "Int64"}],
                "rows": [{"A": 1}]})))
            assert out["success"], out
            # FP still intact, rows preserved
            ctx = server._get_dax_context(alias)
            assert [r[0] for r in ctx["tables"]["P"]["rows"]] == \
                ["Growth :)", "a) Revenue (net"]
        finally:
            _cleanup(alias)

    def test_commented_query_definition_still_detected(self, tmp_path):
        """Desktop field parameters carry DAX comments in the QueryDefinition
        (ground truth: M-W-D in the Ecommerce corpus) — detection must strip
        them before the round-trip parse."""
        p = str(tmp_path / "cmt.pbix")
        _build_sales_pbix(p)
        alias = "rc_cmt"
        try:
            server.pbix_open(p, alias)
            assert json.loads(server.pbix_datamodel_add_field_parameter(
                alias, "M", json.dumps(
                    [{"display": "R", "ref": "Sales[Total Revenue]"}])))["success"]
            # inject a Desktop-style comment (with an unpaired paren!) into the QD
            out = json.loads(server.pbix_datamodel_modify_metadata(
                alias,
                "UPDATE Partition SET QueryDefinition = "
                "'{ -- selector (note" + chr(10) +
                "    (\"R\", NAMEOF(''Sales''[Total Revenue]), 0)" + chr(10) +
                "}' WHERE Name = 'M'"))
            assert out["success"], out
            # rebuild-based edit must still preserve the FP
            out = json.loads(server.pbix_set_table_data(alias, "Y", json.dumps({
                "columns": [{"name": "B", "data_type": "Int64"}],
                "rows": [{"B": 1}]})))
            assert out["success"], out
            conn, tmp = _metadata_conn(alias)
            try:
                part = conn.execute(
                    "SELECT Type FROM [Partition] WHERE Name='M'").fetchone()
                assert part["Type"] == 2  # still a calculated partition
            finally:
                conn.close()
                os.unlink(tmp)
        finally:
            _cleanup(alias)

    def test_remove_table_is_escape_hatch_for_calc_tables(self, tmp_path):
        """Removing a genuinely unsupported calculated table must succeed —
        the refusal used to fire before remove_tables was honored."""
        p = str(tmp_path / "hatch.pbix")
        _build_sales_pbix(p)
        alias = "rc_hatch"
        try:
            server.pbix_open(p, alias)
            # simulate an unparseable calculated table
            out = json.loads(server.pbix_set_table_data(alias, "Calc", json.dumps({
                "columns": [{"name": "A", "data_type": "Int64"}],
                "rows": [{"A": 1}]})))
            assert out["success"]
            out = json.loads(server.pbix_datamodel_modify_metadata(
                alias, "UPDATE Partition SET Type = 2, QueryDefinition = "
                "'DATATABLE(\"A\", INTEGER, {{1}})' WHERE Name = 'Calc'"))
            assert out["success"], out
            # other rebuild edits refuse (calc table present) ...
            out = json.loads(server.pbix_set_table_data(alias, "Z", json.dumps({
                "columns": [{"name": "B", "data_type": "Int64"}],
                "rows": [{"B": 1}]})))
            assert out["success"] is False
            assert out["error_code"] == "MODEL_EDIT_UNSUPPORTED"
            # ... but REMOVING the calc table itself works
            out = json.loads(server.pbix_datamodel_remove_table(alias, "Calc"))
            assert out["success"], out
            # and the model is clean again
            out = json.loads(server.pbix_set_table_data(alias, "Z", json.dumps({
                "columns": [{"name": "B", "data_type": "Int64"}],
                "rows": [{"B": 1}]})))
            assert out["success"], out
        finally:
            _cleanup(alias)

    def test_pill_runtime_text_uri_escaped(self, tmp_path):
        """Runtime '#'/'%' in pill text must be percent-encoded, or the data
        URI is truncated at the fragment."""
        p = str(tmp_path / "pill.pbix")
        _build_sales_pbix(p)
        alias = "rc_pill"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_svg_measure(
                "pill", json.dumps({"text": '"Rank #1 <100%>"'}),
                alias=alias, measure_name="P"))
            assert out["success"], out
            ev = json.loads(server.pbix_evaluate_dax(alias=alias, measures="P"))
            val = ev["results"][0]["value"]
            body = val.split("utf8,", 1)[1]
            assert "#" not in body.replace("%23", ""), val
            assert "%23" in body and "%25" in body, val
            assert "&lt;" in body and "&gt;" in body, val
        finally:
            _cleanup(alias)

    def test_sparkline_escapes_table_apostrophe(self):
        dax = svg_measures.sparkline("O'Brien", "Idx", "[M]")
        assert "'O''Brien'[Idx]" in dax
        assert "'O'Brien'" not in dax.replace("''", "")
        # pre-quoted form unwraps first
        dax = svg_measures.sparkline("'My Table'", "Idx", "[M]")
        assert "'My Table'[Idx]" in dax

    @pytest.mark.integration
    def test_desktop_calc_table_reads_with_inferred_names(self):
        """Corpus-gated: Desktop DATATABLE calc-table columns store NULL
        ExplicitName (name in InferredName) — reads must name them, not crash."""
        corpus = os.environ.get("PBIX_TEST_SAMPLES")
        f = os.path.join(corpus or "", "Ecommerce_Conversion.pbix")
        if not corpus or not os.path.exists(f):
            pytest.skip("public test corpus not present")
        import zipfile

        from pbix_mcp.formats.vertipaq_decoder import read_table_from_abf
        with zipfile.ZipFile(f) as zf:
            abf = decompress_datamodel(zf.read("DataModel"))
        db = read_metadata_sqlite(abf)
        t = read_table_from_abf(abf, "MTD-QTD Selection", db)
        assert t["columns"] and all(c is not None for c in t["columns"])
        # and the genuine field parameters still read their NAMEOF tuples
        t = read_table_from_abf(abf, "KPI_#1", db)
        assert t["columns"] == ["Parameter", "Parameter Fields", "Parameter Order"]
        assert t["rows"][0][1].startswith("'# Measures'[")
