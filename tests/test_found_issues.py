"""Regression tests for issues found while building OpenBI (found_issues.md).

- #4  MAXID invariant: MAXID >= highest object id after a build.
- #5  pbix_save must NOT clear the modified flag on a copy-export.
- #6  pbix_get_default_filters must return a JSON envelope (not a bare string).
- #A  Bracketed/table-qualified measure names must NOT silently evaluate to
      BLANK in the evaluate tools; unknown names raise a typed error.
- #B  A bad dimension string must return the parse message, not the masked
      "'ValueError' object has no attribute 'message'" AttributeError.
- #C  Visual-level sort authoring: prototypeQuery.OrderBy + compiled query.
"""
import json
import os
import sqlite3
import tempfile
import zipfile

from pbix_mcp import server
from pbix_mcp.builder import PBIXBuilder
from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel


def _build_minimal_pbix(path):
    b = PBIXBuilder("T")
    b.add_table("Items", [
        {"name": "ID", "data_type": "Int64"},
        {"name": "Name", "data_type": "String"},
        {"name": "Price", "data_type": "Double"},
    ], rows=[{"ID": 1, "Name": "A", "Price": 9.99}])
    b.save(path)


def _read_maxid_and_max_object_id(pbix_path):
    """Return (declared MAXID, actual highest object id across metadata tables)."""
    with zipfile.ZipFile(pbix_path) as zf:
        abf = decompress_datamodel(zf.read("DataModel"))
    db = read_metadata_sqlite(abf)
    fd, tmp = tempfile.mkstemp(suffix=".db")
    os.write(fd, db)
    os.close(fd)
    conn = sqlite3.connect(tmp)
    try:
        cur = conn.cursor()
        cur.execute("SELECT Value FROM DBPROPERTIES WHERE Name='MAXID'")
        row = cur.fetchone()
        maxid = int(row[0]) if row and row[0] is not None else None
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        names = [r[0] for r in cur.fetchall()]
        actual_max = 0
        for n in names:
            try:
                cur.execute(f"SELECT MAX(ID) FROM [{n}]")
                m = cur.fetchone()[0]
                if m is not None:
                    actual_max = max(actual_max, int(m))
            except sqlite3.Error:
                continue
        return maxid, actual_max
    finally:
        conn.close()
        try:
            os.remove(tmp)
        except OSError:
            pass


class TestMaxIdInvariant:
    def test_builder_maxid_ge_max_object_id(self, tmp_path):
        """Issue #4: a freshly built file must satisfy MAXID >= highest id."""
        p = str(tmp_path / "maxid.pbix")
        _build_minimal_pbix(p)
        maxid, actual_max = _read_maxid_and_max_object_id(p)
        if maxid is None:
            import pytest
            pytest.skip("could not read metadata db from built pbix")
        assert maxid >= actual_max, (
            f"MAXID={maxid} < highest object id={actual_max} "
            "(Power BI would crash on TMCCollectionObject::Add)"
        )


class TestSaveModifiedFlag:
    def test_copy_export_keeps_modified(self, tmp_path):
        """Issue #5: saving to a DIFFERENT path must not mark the session clean."""
        p = str(tmp_path / "orig.pbix")
        _build_minimal_pbix(p)
        alias = "issue5"
        try:
            server.pbix_open(p, alias)
            # make an edit (sets modified=True)
            server.pbix_add_page(alias, "Page X")
            assert server._open_files[alias]["modified"] is True

            # export a COPY to a different path
            copy_path = str(tmp_path / "copy.pbix")
            server.pbix_save(alias, output_path=copy_path)
            # the ORIGINAL still has unsaved edits -> must stay modified
            assert server._open_files[alias]["modified"] is True, (
                "copy-export wrongly cleared the modified flag (data-loss risk)"
            )

            # saving back to the original clears it
            server.pbix_save(alias)
            assert server._open_files[alias]["modified"] is False
        finally:
            server._open_files.pop(alias, None)

    def test_close_after_copy_export_refuses(self, tmp_path):
        """A copy-export must not let pbix_close silently discard work."""
        p = str(tmp_path / "orig2.pbix")
        _build_minimal_pbix(p)
        alias = "issue5b"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "Page Y")
            server.pbix_save(alias, output_path=str(tmp_path / "copy2.pbix"))
            # close without force must refuse (still modified)
            result = server.pbix_close(alias, force=False)
            assert "unsaved" in result.lower() or "modified" in result.lower()
            assert alias in server._open_files  # not closed
        finally:
            server._open_files.pop(alias, None)


class TestGroupedVisualWriteCoords:
    def test_child_stored_group_relative(self, tmp_path):
        """Issue #8: adding a visual under a singleVisualGroup stores group-relative x/y."""
        p = str(tmp_path / "grp.pbix")
        _build_minimal_pbix(p)
        alias = "issue8"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]

            # Inject a singleVisualGroup container at absolute (200, 100).
            layout = server._get_layout(wd)
            sections = layout.get("sections") or layout.get("pages") or []
            page = sections[0]
            group_cfg = {"name": "grp1", "singleVisualGroup": {"displayName": "G"}}
            page.setdefault("visualContainers", []).append({
                "x": 200, "y": 100, "z": 0, "width": 400, "height": 300,
                "config": json.dumps(group_cfg),
            })
            server._set_layout(wd, layout)

            # Add a child visual at ABSOLUTE (250, 150) -> expect stored (50, 50).
            child_cfg = json.dumps({"parentGroupName": "grp1"})
            server.pbix_add_visual(alias, 0, "card", x=250, y=150,
                                   width=100, height=80, config_json=child_cfg)

            layout2 = server._get_layout(wd)
            page2 = (layout2.get("sections") or layout2.get("pages"))[0]
            child = None
            for vc in page2["visualContainers"]:
                cfg = json.loads(vc.get("config", "{}"))
                if cfg.get("parentGroupName") == "grp1":
                    child = vc
                    break
            assert child is not None, "child visual not found"
            assert child["x"] == 50 and child["y"] == 50, (
                f"expected group-relative (50,50), got ({child['x']},{child['y']})"
            )
        finally:
            server._open_files.pop(alias, None)

    def test_top_level_visual_still_absolute(self, tmp_path):
        """A visual with no parent group keeps absolute page coords (regression)."""
        p = str(tmp_path / "top.pbix")
        _build_minimal_pbix(p)
        alias = "issue8b"
        try:
            server.pbix_open(p, alias)
            wd = server._open_files[alias]["work_dir"]
            server.pbix_add_visual(alias, 0, "card", x=120, y=90,
                                   width=100, height=80)
            layout = server._get_layout(wd)
            page = (layout.get("sections") or layout.get("pages"))[0]
            vc = page["visualContainers"][-1]
            assert vc["x"] == 120 and vc["y"] == 90
        finally:
            server._open_files.pop(alias, None)


class TestDefaultFiltersEnvelope:
    def test_returns_json_envelope(self, tmp_path):
        """Issue #6: success path must be valid JSON like every other tool."""
        p = str(tmp_path / "df.pbix")
        _build_minimal_pbix(p)
        alias = "issue6"
        try:
            server.pbix_open(p, alias)
            result = server.pbix_get_default_filters(alias, 0)
            parsed = json.loads(result)  # must not raise
            assert parsed["success"] is True
            assert "data" in parsed
            assert "filters" in parsed["data"]
        finally:
            server._open_files.pop(alias, None)


class TestFormatVisualDeepMerge:
    """OpenBI #1: pbix_format_visual must deep-merge nested object properties,
    not replace the whole object (which dropped unspecified siblings)."""

    def test_partial_border_update_keeps_siblings(self, tmp_path):
        p = str(tmp_path / "fmt.pbix")
        _build_minimal_pbix(p)
        alias = "fmtmerge"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "P1")
            server.pbix_add_visual(alias, 0, "card", x=10, y=10, width=100, height=80)
            server.pbix_format_visual(alias, 0, 0, json.dumps(
                {"border": {"color": "#E55A2B", "width": 2, "radius": 12}}))
            # partial update: only the colour
            server.pbix_format_visual(alias, 0, 0, json.dumps({"border": {"color": "#00AA00"}}))
            layout = server._get_layout(server._open_files[alias]["work_dir"])
            page = (layout.get("sections") or layout.get("pages"))[0]
            cfg = json.loads(page["visualContainers"][0]["config"])
            props = cfg["singleVisual"]["vcObjects"]["border"][0]["properties"]
            assert "width" in props and "radius" in props, "sibling props dropped!"
            assert "color" in props
        finally:
            server._open_files.pop(alias, None)


class TestBookmarkDisplayMode:
    """OpenBI #2: a visibility bookmark must never write display.mode='visible'
    (not a valid Power BI enum). Visible visuals omit mode; only hidden ones get
    display.mode='hidden'."""

    def test_visible_visual_has_no_mode(self, tmp_path):
        p = str(tmp_path / "bm.pbix")
        _build_minimal_pbix(p)
        alias = "bmtest"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "P1")
            server.pbix_add_visual(alias, 0, "card", x=10, y=10, width=100, height=80)
            server.pbix_add_visual(alias, 0, "card", x=120, y=10, width=100, height=80)
            work = server._open_files[alias]["work_dir"]
            layout = server._get_layout(work)
            page = (layout.get("sections") or layout.get("pages"))[0]
            names = [json.loads(vc["config"])["name"] for vc in page["visualContainers"]]
            assert len(names) >= 2
            hide, keep = names[0], names[1]

            server.pbix_add_bookmark(alias, "HideFirst", hidden_visuals=hide)

            layout = server._get_layout(work)
            cfg = json.loads(layout["config"])
            bm = cfg["bookmarks"][-1]
            vcs = bm["explorationState"]["sections"]
            # dig out the visualContainers map for the (only) section
            section = next(iter(vcs.values()))
            containers = section["visualContainers"]

            # the whole bookmark must contain no "visible"
            assert '"visible"' not in json.dumps(bm)
            # hidden visual -> display.mode == "hidden"
            assert containers[hide]["singleVisual"]["display"]["mode"] == "hidden"
            # visible visual -> no display / no mode
            assert "display" not in containers[keep].get("singleVisual", {})
        finally:
            server._open_files.pop(alias, None)


class TestFormatObjectCoverage:
    """OpenBI #1 gap: the friendly formatter must map `labels` (a Card's Callout
    value colour/size) and `categoryLabels`, not silently drop them."""

    def test_labels_maps_to_objects_labels(self):
        objs = server._build_format_objects({"labels": {"color": "#00AA00", "fontSize": 24}})
        assert "labels" in objs["_objects"] if "_objects" in objs else "labels" in objs
        # _build_format_objects returns {"_objects":..,"_vcObjects":..}
        got = objs.get("_objects", objs)
        assert "labels" in got
        props = got["labels"][0]["properties"]
        assert "color" in props and "fontSize" in props

    def test_datalabels_still_works(self):
        objs = server._build_format_objects({"dataLabels": {"color": "#112233"}})
        got = objs.get("_objects", objs)
        assert "labels" in got

    def test_category_labels_still_mapped(self):
        # categoryLabels was already covered (pie/donut); confirm it still works.
        objs = server._build_format_objects({"categoryLabels": {"color": "#334455", "fontSize": 10}})
        got = objs.get("_objects", objs)
        assert "categoryLabels" in got
        assert "categoryLabelFontColor" in got["categoryLabels"][0]["properties"]


def _build_pbix_with_measures(path):
    """A pbix with a category column and two measures, for DAX-evaluate tests."""
    b = PBIXBuilder("T")
    b.add_table("Items", [
        {"name": "Category", "data_type": "String"},
        {"name": "Price", "data_type": "Double"},
    ], rows=[
        {"Category": "A", "Price": 10.0},
        {"Category": "A", "Price": 20.0},
        {"Category": "B", "Price": 5.0},
    ])
    b.add_measure("Items", "Total Price", "SUM(Items[Price])")
    b.add_measure("Items", "Item Count", "COUNTROWS(Items)")
    b.save(path)


class TestBracketedMeasureNames:
    """Issue #A: [Measure] / 'Table'[Measure] silently evaluated to BLANK
    because measure_defs is keyed by bare names — every form must now resolve
    to the same values, and an unknown name must raise a typed error."""

    def _per_dim_values(self, alias, measures):
        out = server.pbix_evaluate_dax_per_dimension(
            alias=alias, measures=measures, dimension="Items.Category")
        parsed = json.loads(out)
        assert parsed["success"] is True, out
        assert "(null)" not in parsed["message"], out
        return parsed["message"]

    def test_all_forms_equal_bare_form(self, tmp_path):
        p = str(tmp_path / "brkt.pbix")
        _build_pbix_with_measures(p)
        alias = "issueA"
        try:
            server.pbix_open(p, alias)
            bare = self._per_dim_values(alias, "Total Price, Item Count")
            assert "30.00" in bare and "5.00" in bare  # A=30, B=5
            for form in ("[Total Price], [Item Count]",
                         "'Items'[Total Price], Items[Item Count]"):
                assert self._per_dim_values(alias, form) == bare, form
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_evaluate_dax_bracketed(self, tmp_path):
        p = str(tmp_path / "brkt2.pbix")
        _build_pbix_with_measures(p)
        alias = "issueA2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_evaluate_dax(
                alias=alias, measures="'Items'[Total Price]"))
            assert out["success"] is True
            assert out["results"][0]["name"] == "Total Price"
            assert out["results"][0]["value"] == 35.0
            assert out["results"][0]["status"] == "ok"
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_unknown_measure_typed_error(self, tmp_path):
        p = str(tmp_path / "brkt3.pbix")
        _build_pbix_with_measures(p)
        alias = "issueA3"
        try:
            server.pbix_open(p, alias)
            for tool in (server.pbix_evaluate_dax,
                         lambda **kw: server.pbix_evaluate_dax_per_dimension(
                             dimension="Items.Category", **kw)):
                out = json.loads(tool(alias=alias, measures="[Total Pric]"))
                assert out["success"] is False
                assert out["error_code"] == "DAX_MEASURE_NOT_FOUND"
                assert "Total Price" in out["message"]  # close-match hint
                assert "Traceback" not in out["message"]
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_split_respects_brackets_and_quotes(self):
        assert server._split_measure_list("[A, B],'T, U'[C], D") == \
            ["[A, B]", "'T, U'[C]", "D"]

    def test_normalization_forms(self):
        defs = {"Pipeline Value": "1", "Win %": "2"}
        assert server._parse_measure_names(
            "[Pipeline Value], 'Sales'[Win %], Pipeline Value", defs) == \
            ["Pipeline Value", "Win %", "Pipeline Value"]


class TestDimensionParseTypedError:
    """Issue #B: the per-dimension tool's `except ValueError` handler read
    e.message/e.code, which plain ValueError lacks — the handler itself raised
    AttributeError and the tool reported a double traceback."""

    def test_bracket_dimension_clean_error(self, tmp_path):
        p = str(tmp_path / "dim.pbix")
        _build_pbix_with_measures(p)
        alias = "issueB"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_evaluate_dax_per_dimension(
                alias=alias, measures="Total Price", dimension="Items[Category]"))
            assert out["success"] is False
            assert "Expected 'Table.Column' format" in out["message"]
            assert "has no attribute" not in out["message"]
            assert "Traceback" not in out["message"]
            assert out["error_code"] == "DIMENSION_INVALID"
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_dimension_parse_error_type(self):
        import pytest

        from pbix_mcp.errors import DimensionParseError, PBIXMCPError
        from pbix_mcp.models.requests import DimensionRef
        with pytest.raises(DimensionParseError) as ei:
            DimensionRef.parse("NoDotHere")
        # both a ValueError (old callers) and a typed PBIXMCPError (.message/.code)
        assert isinstance(ei.value, ValueError)
        assert isinstance(ei.value, PBIXMCPError)
        assert ei.value.code == "DIMENSION_INVALID"
        assert "Expected 'Table.Column' format" in ei.value.message


class TestVisualSortAuthoring:
    """Issue #C: no visual-level sort was ever authored — the service fell back
    to category-ascending order on every pbix-mcp visual. Opt-in sort_by must
    write prototypeQuery.OrderBy AND the same clause in the compiled query."""

    _CFG = json.dumps({"singleVisual": {
        "visualType": "clusteredColumnChart",
        "projections": {"Category": [{"queryRef": "Items.Category"}],
                        "Y": [{"queryRef": "Items.Total Price"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "i", "Entity": "Items", "Type": 0}],
            "Select": [
                {"Column": {"Expression": {"SourceRef": {"Source": "i"}},
                            "Property": "Category"}, "Name": "Items.Category"},
                {"Measure": {"Expression": {"SourceRef": {"Source": "i"}},
                             "Property": "Total Price"}, "Name": "Items.Total Price"},
            ],
        },
    }})

    def _last_vc(self, alias):
        layout = server._get_layout(server._open_files[alias]["work_dir"])
        return (layout.get("sections") or layout.get("pages"))[0]["visualContainers"][-1]

    def test_add_visual_sort_in_prototype_and_query(self, tmp_path):
        p = str(tmp_path / "sort.pbix")
        _build_pbix_with_measures(p)
        alias = "issueC"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_add_visual(
                alias, 0, "clusteredColumnChart", config_json=self._CFG,
                sort_by="[Total Price]", sort_direction="desc"))
            assert out["success"] is True, out
            vc = self._last_vc(alias)
            ob = json.loads(vc["config"])["singleVisual"]["prototypeQuery"]["OrderBy"]
            assert ob == [{"Direction": 2, "Expression": {"Measure": {
                "Expression": {"SourceRef": {"Source": "i"}},
                "Property": "Total Price"}}}]
            q = json.loads(vc["query"])
            assert q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["OrderBy"] == ob
        finally:
            server._open_files.pop(alias, None)

    def test_add_visual_unknown_sort_field_fails_loud(self, tmp_path):
        p = str(tmp_path / "sort2.pbix")
        _build_pbix_with_measures(p)
        alias = "issueC2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_add_visual(
                alias, 0, "clusteredColumnChart", config_json=self._CFG,
                sort_by="[Nope]"))
            assert out["success"] is False
            assert "matches none" in out["message"]
        finally:
            server._open_files.pop(alias, None)

    def test_set_visual_sort_and_clear(self, tmp_path):
        p = str(tmp_path / "sort3.pbix")
        _build_pbix_with_measures(p)
        alias = "issueC3"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_visual(alias, 0, "clusteredColumnChart",
                                   config_json=self._CFG)
            out = json.loads(server.pbix_set_visual_sort(
                alias, 0, 0, sort_by="Items.Category", sort_direction="asc"))
            assert out["success"] is True, out
            vc = self._last_vc(alias)
            ob = json.loads(vc["config"])["singleVisual"]["prototypeQuery"]["OrderBy"]
            assert ob[0]["Direction"] == 1 and "Column" in ob[0]["Expression"]
            q = json.loads(vc["query"])
            assert q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["OrderBy"] == ob

            out = json.loads(server.pbix_set_visual_sort(alias, 0, 0, sort_by=""))
            assert out["success"] is True
            vc = self._last_vc(alias)
            assert "OrderBy" not in json.loads(vc["config"])["singleVisual"]["prototypeQuery"]
        finally:
            server._open_files.pop(alias, None)

    def test_pbir_export_translates_order_by(self, tmp_path):
        p = str(tmp_path / "sort4.pbix")
        _build_pbix_with_measures(p)
        alias = "issueC4"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_visual(alias, 0, "clusteredColumnChart",
                                   config_json=self._CFG,
                                   sort_by="'Items'[Total Price]")
            cfg = json.loads(self._last_vc(alias)["config"])
            pbir = server._pbix_config_to_pbir_visual(cfg, 0, 0, 300, 200)
            sd = pbir["visual"]["query"]["sortDefinition"]
            assert sd["isDefaultSort"] is False
            assert sd["sort"] == [{"field": {"Measure": {
                "Expression": {"SourceRef": {"Entity": "Items"}},
                "Property": "Total Price"}}, "direction": "Descending"}]
        finally:
            server._open_files.pop(alias, None)


class TestMeasureNameEdgeCases:
    """Hardening from the pre-release adversarial review of issue #A/#C fixes."""

    def test_apostrophe_in_bare_name_does_not_swallow_comma(self):
        assert server._split_measure_list("Tom's Margin, Sales") == \
            ["Tom's Margin", "Sales"]
        assert server._parse_measure_names(
            "Tom's Margin, Sales", {"Tom's Margin": "1", "Sales": "2"}) == \
            ["Tom's Margin", "Sales"]

    def test_escaped_quote_table_qualifier(self):
        # 'O''Brien Sales'[M] -> table "O'Brien Sales", measure M
        assert server._parse_measure_names(
            "'O''Brien Sales'[M]", {"M": "1"}) == ["M"]

    def test_exact_model_name_beats_normalization(self):
        # A real measure literally named "Cost [USD]" must never be re-parsed
        # as table "Cost" + measure "USD".
        defs = {"Cost [USD]": "1", "USD": "2"}
        assert server._parse_measure_names("Cost [USD]", defs) == ["Cost [USD]"]
        # ...but when no exact match exists, normalization still applies.
        assert server._parse_measure_names("Cost [USD]", {"USD": "2"}) == ["USD"]

    def test_case_insensitive_fallback_to_canonical(self):
        # Power BI names are case-insensitive; resolve to the model's casing
        # (matching attach_order_by's semantics on the sort path).
        defs = {"Total Price": "1"}
        assert server._parse_measure_names("[total price]", defs) == ["Total Price"]
        assert server._parse_measure_names("TOTAL PRICE", defs) == ["Total Price"]

    def test_pbir_export_skips_unknown_order_by_shapes(self):
        # A HierarchyLevel OrderBy (Desktop-authored) must not leak alias-based
        # SourceRefs into the PBIR sortDefinition — skip -> default marker.
        cfg = {"name": "v1", "singleVisual": {
            "visualType": "lineChart",
            "projections": {"Category": [{"queryRef": "d.H.Year"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "d", "Entity": "Dates", "Type": 0}],
                "Select": [{"HierarchyLevel": {"Expression": {}}, "Name": "d.H.Year"}],
                "OrderBy": [{"Direction": 1, "Expression": {
                    "HierarchyLevel": {"Expression": {"Hierarchy": {
                        "Expression": {"SourceRef": {"Source": "d"}}}}}}}],
            },
        }}
        pbir = server._pbix_config_to_pbir_visual(cfg, 0, 0, 100, 100)
        assert pbir["visual"]["query"]["sortDefinition"] == \
            {"sort": [], "isDefaultSort": True}


class TestIssues9DefaultFilterSemantics:
    """Issues-9 §1: opt-out + page-scoped defaults, consistent machinery."""

    def _build(self, path):
        b = PBIXBuilder("T")
        b.add_table("Sales", [
            {"name": "Region", "data_type": "String"},
            {"name": "Amount", "data_type": "Int64"},
        ], rows=[{"Region": "N", "Amount": 10}, {"Region": "S", "Amount": 30}])
        b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
        b.save(path)

    def _add_slicer_with_default(self, alias, page_index, value):
        """A slicer whose filter carries a persisted default selection."""
        cfg = json.dumps({"singleVisual": {
            "visualType": "slicer",
            "projections": {"Values": [{"queryRef": "Sales.Region"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
                "Select": [{"Column": {"Expression": {"SourceRef": {"Source": "s"}},
                                       "Property": "Region"}, "Name": "Sales.Region"}],
            }}})
        out = json.loads(server.pbix_add_visual(alias, page_index, "slicer",
                                                config_json=cfg))
        assert out["success"], out
        # persist the default selection where slicers store it:
        # config.singleVisual.objects.general[].properties.filter.filter
        wd = server._open_files[alias]["work_dir"]
        layout = server._get_layout(wd)
        vc = layout["sections"][page_index]["visualContainers"][-1]
        config = json.loads(vc["config"])
        sv = config["singleVisual"]
        sv.setdefault("objects", {})["general"] = [{"properties": {"filter": {
            "filter": {
                "Version": 2,
                "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
                "Where": [{"Condition": {"In": {
                    "Expressions": [{"Column": {
                        "Expression": {"SourceRef": {"Source": "s"}},
                        "Property": "Region"}}],
                    "Values": [[{"Literal": {"Value": f"'{value}'"}}]]}}}],
            }}}}]
        vc["config"] = json.dumps(config)
        server._set_layout(wd, layout)

    def test_opt_out_and_page_scoping(self, tmp_path):
        p = str(tmp_path / "df.pbix")
        self._build(p)
        alias = "iss9df"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "P1")
            server.pbix_add_page(alias, "P2")
            # page 1 (index 1) gets a slicer defaulting Region = S
            self._add_slicer_with_default(alias, 1, "S")

            def total(**kw):
                out = json.loads(server.pbix_evaluate_dax(
                    alias=alias, measures="Total", **kw))
                return out["results"][0]["value"]

            assert total() == 30                        # all-pages defaults (historic)
            assert total(apply_default_filters=False) == 40   # raw model opt-out
            assert total(page_index=0) == 40            # page 0 has no slicers
            assert total(page_index=1) == 30            # the slicer's own page
            # explicit filter_context always wins over defaults
            assert total(filter_context='{"Sales.Region": ["N"]}') == 10
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)

    def test_per_dimension_flag_parity(self, tmp_path):
        # Default filter on Region; iterate a DIFFERENT column (Cat) — the
        # iterated dimension's own key is always owned by the per-value loop
        # (pre-existing contract), so the default must land on another column
        # to be observable.
        p = str(tmp_path / "dfp.pbix")
        b = PBIXBuilder("T")
        b.add_table("Sales", [
            {"name": "Region", "data_type": "String"},
            {"name": "Cat", "data_type": "String"},
            {"name": "Amount", "data_type": "Int64"},
        ], rows=[{"Region": "N", "Cat": "X", "Amount": 10},
                 {"Region": "S", "Cat": "X", "Amount": 30},
                 {"Region": "S", "Cat": "Y", "Amount": 5}])
        b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
        b.save(p)
        alias = "iss9dfp"
        try:
            server.pbix_open(p, alias)
            server.pbix_add_page(alias, "P1")
            self._add_slicer_with_default(alias, 0, "S")

            def x_value(**kw):
                out = json.loads(server.pbix_evaluate_dax_per_dimension(
                    alias=alias, measures="Total", dimension="Sales.Cat", **kw))
                line = [ln for ln in out["message"].splitlines()
                        if ln.startswith("X")][0]
                return line.split()[-1]

            assert x_value() == "40"                          # raw (historic)
            assert x_value(apply_default_filters=True) == "30"  # S-default applied
            assert x_value(apply_default_filters=True, page_index=0) == "30"
            # a page with no slicers contributes nothing
            assert x_value(apply_default_filters=True, page_index=1) == "40"
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)


class TestIssues9DataCategoryErgonomics:
    """Issues-9 §2: category changes without re-sending the expression, and a
    first-class clearing path."""

    def _build(self, path):
        b = PBIXBuilder("T")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.add_measure("S", "M", "SUM(S[A])", data_category="ImageUrl")
        b.save(path)

    def _category(self, alias):
        info = server._open_files[alias]
        import sqlite3 as _sq
        import tempfile as _tf

        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel
        with open(os.path.join(info["work_dir"], "DataModel"), "rb") as f:
            db = read_metadata_sqlite(decompress_datamodel(f.read()))
        fd, tmp = _tf.mkstemp(suffix=".db")
        os.write(fd, db)
        os.close(fd)
        conn = _sq.connect(tmp)
        try:
            row = conn.execute(
                "SELECT DataCategory, Expression FROM Measure WHERE Name='M'"
            ).fetchone()
            return row
        finally:
            conn.close()
            os.unlink(tmp)

    def test_modify_without_expression_and_clear(self, tmp_path):
        p = str(tmp_path / "dc.pbix")
        self._build(p)
        alias = "iss9dc"
        try:
            server.pbix_open(p, alias)
            # change ONLY the category — expression untouched
            out = json.loads(server.pbix_datamodel_modify_measure(
                alias, "M", new_data_category="WebUrl"))
            assert out["success"], out
            cat, expr = self._category(alias)
            assert cat == "WebUrl" and expr == "SUM(S[A])"

            # change ONLY the format string
            out = json.loads(server.pbix_datamodel_modify_measure(
                alias, "M", new_format_string="0.0%"))
            assert out["success"], out

            # no-op call fails loud
            out = json.loads(server.pbix_datamodel_modify_measure(alias, "M"))
            assert out["success"] is False
            assert out["error_code"] == "NOTHING_TO_CHANGE"

            # first-class CLEAR
            out = json.loads(server.pbix_datamodel_set_measure_category(alias, "M"))
            assert out["success"], out
            cat, expr = self._category(alias)
            assert cat is None and expr == "SUM(S[A])"

            # and set again via the dedicated setter
            out = json.loads(server.pbix_datamodel_set_measure_category(
                alias, "M", "ImageUrl"))
            assert out["success"], out
            assert self._category(alias)[0] == "ImageUrl"

            # unknown measure -> clean error
            out = json.loads(server.pbix_datamodel_set_measure_category(alias, "Nope"))
            assert out["success"] is False and "not found" in out["message"]
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)


class TestIssues9GuidHyphens:
    """Issues-9 §4: legacy PBI_CV_<GUID> hyphenated marketplace ids register."""

    def test_hyphenated_guid_accepted_verbatim(self, tmp_path):
        p = str(tmp_path / "guid.pbix")
        b = PBIXBuilder("T")
        b.add_table("S", [{"name": "A", "data_type": "Int64"}], rows=[{"A": 1}])
        b.save(p)
        alias = "iss9guid"
        legacy = "PBI_CV_23E12E97-A82F-4667-B8D6-2ECA76A3E8F2"
        try:
            server.pbix_open(p, alias)
            out = json.loads(server.pbix_reference_public_visual(alias, legacy))
            assert out["success"], out
            assert out["data"]["publicCustomVisuals"] == [legacy]  # verbatim
            for bad in ("a b", "x;y", "x!y", ""):
                out = json.loads(server.pbix_reference_public_visual(alias, bad))
                assert out["success"] is False, bad
        finally:
            server._open_files.pop(alias, None)


class TestIssues9PageIndexValidation:
    """Review round: an out-of-range page_index must error loudly, never
    silently mean 'raw model' (indistinguishable from a slicer-less page)."""

    def test_out_of_range_page_index_errors(self, tmp_path):
        p = str(tmp_path / "oor.pbix")
        b = PBIXBuilder("T")
        b.add_table("Items", [{"name": "Name", "data_type": "String"},
                              {"name": "Price", "data_type": "Double"}],
                    rows=[{"Name": "A", "Price": 1.0}])
        b.add_measure("Items", "Total", "SUM(Items[Price])")
        b.save(p)
        alias = "iss9oor"
        try:
            server.pbix_open(p, alias)
            for tool_kwargs in (
                dict(fn=server.pbix_evaluate_dax, measures="Total"),
                dict(fn=server.pbix_evaluate_dax_per_dimension,
                     measures="Total", dimension="Items.Name",
                     apply_default_filters=True),
            ):
                fn = tool_kwargs.pop("fn")
                out = json.loads(fn(alias=alias, page_index=99, **tool_kwargs))
                assert out["success"] is False, out
                assert "out of range" in out["message"], out
        finally:
            server._open_files.pop(alias, None)
            server._dax_cache.pop(alias, None)
