"""
Tests for the report data-binding compiler (query + dataTransforms).

Without these on every data visualContainer, Power BI Desktop's report loader
fails the whole report ("Failed to load the report") once the report carries any
report-level config / visual objects, even though the data model opens fine.
Verified against real Power BI Desktop: an OpenBI report lacking them fails; the
same report with them injected loads.
"""

import json
import zipfile

import pytest

from pbix_mcp.report_binding import attach_order_by, compile_visual_binding

pytestmark = pytest.mark.unit


def _card_sv():
    return {
        "visualType": "card",
        "projections": {"Values": [{"queryRef": "Sales.Total Revenue", "active": True}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
            "Select": [{
                "Measure": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Revenue"},
                "Name": "Sales.Total Revenue",
            }],
        },
    }


def _pie_series_sv():
    return {
        "visualType": "pieChart",
        "projections": {"Series": [{"queryRef": "Products.Product"}], "Y": [{"queryRef": "Sales.Total Qty"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0},
                     {"Name": "p", "Entity": "Products", "Type": 0}],
            "Select": [
                {"Column": {"Expression": {"SourceRef": {"Source": "p"}}, "Property": "Product"}, "Name": "Products.Product"},
                {"Measure": {"Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Qty"}, "Name": "Sales.Total Qty"},
            ],
        },
    }


class TestCompileBinding:
    def test_card_measure(self):
        q, dt = compile_visual_binding(_card_sv(), lambda e, p, m: None)
        cmd = q["Commands"][0]["SemanticQueryDataShapeCommand"]
        # query mirrors prototypeQuery + NativeReferenceName + binding
        assert cmd["Query"]["Select"][0]["NativeReferenceName"] == "Total Revenue"
        assert cmd["Binding"]["Primary"]["Groupings"] == [{"Projections": [0]}]
        assert cmd["Binding"]["Version"] == 1
        assert "isPivoted" not in cmd["Binding"]
        assert cmd["ExecutionMetricsKind"] == 1
        # dataTransforms
        assert dt["projectionOrdering"] == {"Values": [0]}
        sel = dt["selects"][0]
        assert sel["queryName"] == "Sales.Total Revenue"
        assert sel["roles"] == {"Values": True}
        assert sel["type"]["underlyingType"] == 259           # measure
        assert sel["expr"]["Measure"]["Expression"]["SourceRef"] == {"Entity": "Sales"}
        assert dt["queryMetadata"]["Select"][0]["Type"] == 1
        assert dt["visualElements"][0]["DataRoles"] == [{"Name": "Values", "Projection": 0, "isActive": False}]

    def test_pie_series_is_pivoted(self):
        q, dt = compile_visual_binding(_pie_series_sv(), lambda e, p, m: "String" if p == "Product" else None)
        b = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert b.get("isPivoted") is True                      # Series x value
        assert b["Primary"]["Groupings"] == [{"Projections": [0, 1]}]
        # string column type codes
        col_sel = next(s for s in dt["selects"] if s["queryName"] == "Products.Product")
        assert col_sel["type"]["underlyingType"] == 1
        assert col_sel["expr"]["Column"]["Expression"]["SourceRef"] == {"Entity": "Products"}
        qm = {s["Name"]: s["Type"] for s in dt["queryMetadata"]["Select"]}
        assert qm["Products.Product"] == 2048

    def test_category_y_chart_not_pivoted(self):
        sv = _pie_series_sv()
        sv["visualType"] = "clusteredColumnChart"
        sv["projections"] = {"Category": [{"queryRef": "Products.Product"}], "Y": [{"queryRef": "Sales.Total Qty"}]}
        q, _dt = compile_visual_binding(sv, lambda e, p, m: None)
        b = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert "isPivoted" not in b

    def test_datareduction_and_subtotal_per_visual(self):
        # table -> Window.Count 500 + Subtotal; line/bar -> Window.Count 1000;
        # card/pie -> Top{}. (Desktop-authored ground truth.)
        def dr(sv):
            q, _ = compile_visual_binding(sv, lambda e, p, m: None)
            return q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        card = dr(_card_sv())
        assert card["DataReduction"] == {"DataVolume": 3, "Primary": {"Top": {}}}
        assert "Subtotal" not in card["Primary"]["Groupings"][0]

        tbl = _pie_series_sv(); tbl["visualType"] = "tableEx"
        b = dr(tbl)
        assert b["DataReduction"] == {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}}
        assert b["Primary"]["Groupings"][0]["Subtotal"] == 1

        line = _pie_series_sv(); line["visualType"] = "lineChart"
        assert dr(line)["DataReduction"] == {"DataVolume": 4, "Primary": {"Window": {"Count": 1000}}}

    def test_order_by_carried_into_compiled_query(self):
        # attach_order_by writes prototypeQuery.OrderBy; the compile deep-copies
        # the prototype, so the compiled query must carry the same clause.
        sv = _pie_series_sv()
        ref, dcode = attach_order_by(sv, "[Total Qty]", "desc")
        assert (ref, dcode) == ("Sales.Total Qty", 2)
        expected = [{"Direction": 2, "Expression": {"Measure": {
            "Expression": {"SourceRef": {"Source": "s"}}, "Property": "Total Qty"}}}]
        assert sv["prototypeQuery"]["OrderBy"] == expected
        q, _dt = compile_visual_binding(sv, lambda e, p, m: None)
        assert q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["OrderBy"] == expected

    def test_type_codes_by_data_type(self):
        # underlyingType tracks the field VALUE type, for columns AND measures.
        sv = {
            "visualType": "tableEx",
            "projections": {"Values": [{"queryRef": "T.S"}, {"queryRef": "T.I"},
                                       {"queryRef": "T.D"}, {"queryRef": "T.When"}]},
            "prototypeQuery": {"Version": 2, "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "S"}, "Name": "T.S"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "I"}, "Name": "T.I"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "D"}, "Name": "T.D"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "When"}, "Name": "T.When"},
                ]}}
        types = {"S": "String", "I": "Int64", "D": "Decimal", "When": "DateTime"}
        _q, dt = compile_visual_binding(sv, lambda e, p, m: types[p])
        codes = {s["queryName"]: s["type"]["underlyingType"] for s in dt["selects"]}
        assert codes == {"T.S": 1, "T.I": 260, "T.D": 259, "T.When": 519}
        qmt = {s["Name"]: s["Type"] for s in dt["queryMetadata"]["Select"]}
        assert qmt == {"T.S": 2048, "T.I": 3, "T.D": 1, "T.When": 4}

    def test_native_reference_dedup_query_only(self):
        # Same Property from two tables: query dedups NativeReferenceName; the
        # queryMetadata Restatement and displayName keep the raw name.
        sv = {
            "visualType": "tableEx",
            "projections": {"Values": [{"queryRef": "A.ID"}, {"queryRef": "B.ID"}]},
            "prototypeQuery": {"Version": 2,
                "From": [{"Name": "a", "Entity": "A", "Type": 0}, {"Name": "b", "Entity": "B", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "a"}}, "Property": "ID"}, "Name": "A.ID"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "b"}}, "Property": "ID"}, "Name": "B.ID"},
                ]}}
        q, dt = compile_visual_binding(sv, lambda e, p, m: "Int64")
        nrn = [s["NativeReferenceName"] for s in q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"]]
        assert nrn == ["ID", "ID1"]                       # query dedups
        assert [s["Restatement"] for s in dt["queryMetadata"]["Select"]] == ["ID", "ID"]  # not deduped
        assert [s["displayName"] for s in dt["selects"]] == ["ID", "ID"]

    def test_no_projections_returns_none(self):
        assert compile_visual_binding({"visualType": "textbox"}) == (None, None)
        assert compile_visual_binding({"visualType": "shape", "prototypeQuery": {"Select": []}}) == (None, None)

    def test_every_select_query_name_matches(self):
        # The report's suggested structural gate.
        for sv in (_card_sv(), _pie_series_sv()):
            q, dt = compile_visual_binding(sv, lambda e, p, m: None)
            proto_names = {s["Name"] for s in q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"]}
            for s in dt["selects"]:
                assert s["queryName"] in proto_names

    def test_matrix_rows_columns_values(self):
        # matrix with a column field crosses rows (Primary) against columns +
        # values (Secondary). NO isPivoted. Byte-exact to Matrix Bubble Chart.
        sv = {
            "visualType": "matrix",
            "projections": {"Rows": [{"queryRef": "T.Class"}],
                            "Columns": [{"queryRef": "T.Sex"}],
                            "Values": [{"queryRef": "T.Rate"}]},
            "prototypeQuery": {"Version": 2, "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Class"}, "Name": "T.Class"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Sex"}, "Name": "T.Sex"},
                    {"Measure": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Rate"}, "Name": "T.Rate"},
                ]}}
        q, dt = compile_visual_binding(sv, lambda e, p, m: None)
        b = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert b["Primary"]["Groupings"] == [{"Projections": [0]}]           # rows
        assert b["Secondary"]["Groupings"] == [{"Projections": [1, 2]}]      # cols + values
        assert b["DataReduction"] == {"DataVolume": 3, "Primary": {"Window": {"Count": 100}},
                                      "Secondary": {"Top": {"Count": 100}}}
        assert "isPivoted" not in b
        roles = {(r["Name"], r["isActive"]) for r in dt["visualElements"][0]["DataRoles"]}
        assert ("Rows", True) in roles and ("Columns", True) in roles and ("Values", False) in roles

    def test_matrix_without_columns_is_flat(self):
        # a matrix with only Rows + Values (no column field) collapses to a
        # single Primary grouping with a subtotal (like a table).
        sv = {
            "visualType": "matrix",
            "projections": {"Rows": [{"queryRef": "T.Class"}], "Values": [{"queryRef": "T.Rate"}]},
            "prototypeQuery": {"Version": 2, "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Class"}, "Name": "T.Class"},
                    {"Measure": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Rate"}, "Name": "T.Rate"},
                ]}}
        q, _ = compile_visual_binding(sv, lambda e, p, m: None)
        b = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert b["Primary"]["Groupings"] == [{"Projections": [0, 1], "Subtotal": 1}]
        assert "Secondary" not in b
        assert b["DataReduction"] == {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}}

    def test_slicer_include_empty_groups(self):
        # slicer: empty Window (no Count), IncludeEmptyGroups, active data role.
        sv = {
            "visualType": "slicer",
            "projections": {"Values": [{"queryRef": "T.Sex"}]},
            "prototypeQuery": {"Version": 2, "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [{"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Sex"}, "Name": "T.Sex"}]}}
        q, dt = compile_visual_binding(sv, lambda e, p, m: "String")
        b = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert b["DataReduction"] == {"DataVolume": 3, "Primary": {"Window": {}}}
        assert b["IncludeEmptyGroups"] is True
        assert dt["visualElements"][0]["DataRoles"] == [{"Name": "Values", "Projection": 0, "isActive": True}]


class TestToolRegistration:
    """Guard against a helper stealing @mcp.tool() from a real tool (a function
    defined between the decorator and its target hijacks the decorator)."""

    def test_public_tools_registered_and_helper_not(self):
        from pbix_mcp.server import mcp

        tm = getattr(mcp, "_tool_manager", None)
        names = set(getattr(tm, "_tools", {}).keys()) if tm else set()
        assert names, "no tools registered"
        for t in ("pbix_add_visual", "pbix_open", "pbix_save", "pbix_get_pages",
                  "pbix_create", "pbix_add_page"):
            assert t in names, f"{t} not registered as an MCP tool"
        # internal helper must NOT be exposed as a tool
        assert "_report_type_resolver" not in names


class TestBuilderEmitsBinding:
    def test_built_report_has_query_and_datatransforms(self, tmp_path):
        from pbix_mcp.builder import PBIXBuilder

        path = str(tmp_path / "r.pbix")
        b = PBIXBuilder("R")
        b.add_table("Sales", [{"name": "Product", "data_type": "String"},
                              {"name": "Amount", "data_type": "Double"}],
                    rows=[{"Product": "A", "Amount": 1.0}, {"Product": "B", "Amount": 2.0}])
        b.add_measure("Sales", "Total", "SUM(Sales[Amount])")
        b.add_page("P1", visuals=[
            {"type": "card", "config": {"measure": "Total"}},
            {"type": "clusteredColumnChart",
             "config": {"category": {"table": "Sales", "column": "Product"}, "measure": "Total"}},
            {"type": "textbox", "config": {}},
        ])
        b.save(path, validate=True)
        with zipfile.ZipFile(path) as z:
            layout = json.loads(z.read("Report/Layout").decode("utf-16-le"))
        vcs = layout["sections"][0]["visualContainers"]
        data_vcs = [vc for vc in vcs if "singleVisual" in json.loads(vc["config"])
                    and json.loads(vc["config"])["singleVisual"].get("projections")]
        assert data_vcs, "expected data visuals"
        for vc in data_vcs:
            assert vc.get("query"), "data visual missing query"
            assert vc.get("dataTransforms"), "data visual missing dataTransforms"
            # every select's queryName must resolve to a prototypeQuery select
            proto = json.loads(vc["config"])["singleVisual"]["prototypeQuery"]
            proto_names = {s["Name"] for s in proto["Select"]}
            dt = json.loads(vc["dataTransforms"])
            for s in dt["selects"]:
                assert s["queryName"] in proto_names
        # textbox must NOT get a binding
        tb = [vc for vc in vcs if json.loads(vc["config"])["singleVisual"].get("visualType") == "textbox"]
        assert tb and "query" not in tb[0]


def _resolver(entity, prop, is_measure):
    return {"Region": "String", "Amount": "Double", "Value": "Int64"}.get(prop)


def _cart_col_sv(vt, value_prop):
    """A cartesian chart with a Category column and a plain numeric COLUMN on Y."""
    return {
        "visualType": vt,
        "projections": {"Category": [{"queryRef": "cat"}], "Y": [{"queryRef": "val"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "t", "Entity": "Sales", "Type": 0}],
            "Select": [
                {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Region"}, "Name": "cat"},
                {"Column": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": value_prop}, "Name": "val"},
            ],
        },
    }


class TestValueColumnAggregation:
    """A plain numeric column on a value axis must be implicitly Summed — IN THE
    PROTOTYPE QUERY, not just the compiled query. Desktop re-derives the live
    data query from config.singleVisual.prototypeQuery + projections, so an
    unaggregated column there renders an empty chart even when the compiled
    query carries an Aggregation (Desktop-verified). Ground truth: AI Sample
    barChart stores Sum(Entity.Property) in the prototype and repoints the
    projection queryRef at it."""

    def test_double_column_on_y_is_summed(self):
        sv = _cart_col_sv("clusteredColumnChart", "Amount")
        q, dt = compile_visual_binding(sv, _resolver)
        # the PROTOTYPE itself is rewritten (the part Desktop re-derives from)
        proto_val = sv["prototypeQuery"]["Select"][1]
        assert "Aggregation" in proto_val and "Column" not in proto_val
        assert proto_val["Aggregation"]["Function"] == 0  # Sum
        assert proto_val["Name"] == "Sum(Sales.Amount)"   # Desktop queryRef naming
        assert sv["projections"]["Y"] == [{"queryRef": "Sum(Sales.Amount)"}]
        # and the compiled query matches
        val = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"][1]
        assert "Aggregation" in val and val["Name"] == "Sum(Sales.Amount)"
        binding = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Binding"]
        assert binding["Primary"]["Groupings"] == [{"Projections": [0, 1]}]
        assert "Aggregation" in dt["selects"][1]["expr"]
        assert dt["selects"][1]["queryName"] == "Sum(Sales.Amount)"
        assert dt["selects"][1]["roles"] == {"Y": True}

    def test_int64_sum_uses_260_codes(self):
        _, dt = compile_visual_binding(_cart_col_sv("barChart", "Value"), _resolver)
        assert dt["selects"][1]["type"]["underlyingType"] == 260

    def test_non_numeric_value_column_counts(self):
        sv = _cart_col_sv("columnChart", "Region")  # String column on Y
        sv["prototypeQuery"]["Select"][1]["Column"]["Property"] = "Region"
        compile_visual_binding(sv, _resolver)
        proto_val = sv["prototypeQuery"]["Select"][1]
        assert proto_val["Aggregation"]["Function"] == 5  # CountNonNull
        assert proto_val["Name"] == "CountNonNull(Sales.Region)"

    def test_measure_on_y_is_not_wrapped(self):
        sv = _cart_col_sv("columnChart", "Amount")
        sv["prototypeQuery"]["Select"][1] = {
            "Measure": {"Expression": {"SourceRef": {"Source": "t"}}, "Property": "Total"}, "Name": "val"}
        q, _ = compile_visual_binding(sv, _resolver)
        val = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"][1]
        assert "Measure" in val and "Aggregation" not in val

    def test_table_shows_raw_columns(self):
        sv = _cart_col_sv("tableEx", "Amount")
        sv["projections"] = {"Values": [{"queryRef": "cat"}, {"queryRef": "val"}]}
        q, _ = compile_visual_binding(sv, _resolver)
        assert all("Aggregation" not in s
                   for s in q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"])

    def test_explicit_aggregation_select_is_handled(self):
        sv = _cart_col_sv("columnChart", "Amount")
        sv["prototypeQuery"]["Select"][1] = {
            "Aggregation": {"Expression": {"Column": {
                "Expression": {"SourceRef": {"Source": "t"}}, "Property": "Amount"}}, "Function": 0},
            "Name": "val"}
        q, dt = compile_visual_binding(sv, _resolver)
        val = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["Select"][1]
        assert "Aggregation" in val
        # dataTransforms expr entity-rewrites the inner column
        inner = dt["selects"][1]["expr"]["Aggregation"]["Expression"]["Column"]
        assert inner["Expression"]["SourceRef"] == {"Entity": "Sales"}


class TestSummarizeByDefaults:
    """Numeric columns must be SummarizeBy=Default(1) so Power BI can implicitly
    aggregate them on a value axis; text/date/bool stay None(2). SummarizeBy=None
    on a numeric column makes a cartesian chart render empty even with an
    Aggregation in the binding (Desktop won't aggregate a 'don't summarize' col)."""

    def test_numeric_columns_are_summable(self, tmp_path):
        import sqlite3
        import tempfile as _tf

        from pbix_mcp.builder import PBIXBuilder
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        p = str(tmp_path / "t.pbix")
        b = PBIXBuilder("T")
        b.add_table("F", [
            {"name": "Txt", "data_type": "String"},
            {"name": "I", "data_type": "Int64"},
            {"name": "D", "data_type": "Double"},
            {"name": "Dec", "data_type": "Decimal"},
            {"name": "Dt", "data_type": "DateTime"},
            {"name": "B", "data_type": "Boolean"},
        ], rows=[{"Txt": "a", "I": 1, "D": 1.5, "Dec": 2.0, "Dt": "2020-01-01", "B": True}])
        b.save(p)
        abf = decompress_datamodel(zipfile.ZipFile(p).read("DataModel"))
        fd, db = _tf.mkstemp(suffix=".db")
        import os as _os
        _os.write(fd, read_metadata_sqlite(abf)); _os.close(fd)
        try:
            con = sqlite3.connect(db)
            sb = dict(con.execute(
                "SELECT ExplicitName, SummarizeBy FROM [Column] "
                "WHERE ExplicitName IN ('Txt','I','D','Dec','Dt','B')").fetchall())
            con.close()
        finally:
            _os.unlink(db)
        assert sb["I"] == 1 and sb["D"] == 1 and sb["Dec"] == 1   # numeric -> Default
        assert sb["Txt"] == 2 and sb["Dt"] == 2 and sb["B"] == 2  # else -> None


def _bar_bare_col_sv():
    """Bar chart with a BARE numeric column on Y (becomes an implicit Sum)."""
    return {
        "visualType": "barChart",
        "projections": {"Category": [{"queryRef": "Products.Product"}],
                        "Y": [{"queryRef": "Sales.Amount"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0},
                     {"Name": "p", "Entity": "Products", "Type": 0}],
            "Select": [
                {"Column": {"Expression": {"SourceRef": {"Source": "p"}},
                            "Property": "Product"}, "Name": "Products.Product"},
                {"Column": {"Expression": {"SourceRef": {"Source": "s"}},
                            "Property": "Amount"}, "Name": "Sales.Amount"},
            ],
        },
    }


class TestAttachOrderBy:
    """Visual-level sort authoring (found_issues #C)."""

    def test_all_reference_forms_resolve(self):
        for form in ("Total Qty", "[Total Qty]", "'Sales'[Total Qty]",
                     "Sales[Total Qty]", "Sales.Total Qty", "total qty"):
            sv = _pie_series_sv()
            ref, dcode = attach_order_by(sv, form, "asc")
            assert (ref, dcode) == ("Sales.Total Qty", 1), form

    def test_direction_aliases(self):
        for d, code in (("asc", 1), ("Ascending", 1), ("desc", 2),
                        ("DESCENDING", 2), ("1", 1), ("2", 2)):
            sv = _pie_series_sv()
            assert attach_order_by(sv, "Total Qty", d)[1] == code, d

    def test_column_sort(self):
        sv = _pie_series_sv()
        ref, _ = attach_order_by(sv, "'Products'[Product]", "asc")
        assert ref == "Products.Product"
        ob = sv["prototypeQuery"]["OrderBy"][0]
        assert ob["Expression"] == {"Column": {
            "Expression": {"SourceRef": {"Source": "p"}}, "Property": "Product"}}

    def test_unknown_field_raises_with_available(self):
        with pytest.raises(ValueError, match="matches none"):
            attach_order_by(_pie_series_sv(), "[Nope]")

    def test_bad_direction_raises(self):
        with pytest.raises(ValueError, match="direction"):
            attach_order_by(_pie_series_sv(), "Total Qty", "sideways")

    def test_no_prototype_raises(self):
        with pytest.raises(ValueError, match="prototypeQuery"):
            attach_order_by({"visualType": "card"}, "X")

    def test_implicit_aggregation_rewrites_order_by(self):
        # Sorting by a bare numeric value-role column: when the compile turns
        # the column into an implicit Sum aggregation, the OrderBy must follow
        # (Desktop stores the Aggregation expression in OrderBy for such sorts).
        sv = _bar_bare_col_sv()
        ref, _ = attach_order_by(sv, "[Amount]", "desc")
        assert ref == "Sales.Amount"
        q, _dt = compile_visual_binding(
            sv, lambda e, p, m: "Double" if p == "Amount" else "String")
        expected_ob = [{"Direction": 2, "Expression": {"Aggregation": {
            "Expression": {"Column": {"Expression": {"SourceRef": {"Source": "s"}},
                                      "Property": "Amount"}},
            "Function": 0}}}]
        assert sv["prototypeQuery"]["OrderBy"] == expected_ob
        assert q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]["OrderBy"] == expected_ob
        # the select itself was renamed to the Sum(...) aggregation as before
        assert sv["prototypeQuery"]["Select"][1]["Name"] == "Sum(Sales.Amount)"

    def test_sort_by_aggregation_select_directly(self):
        # A prototype whose select is ALREADY an Aggregation (e.g. re-sorting a
        # compiled visual): matching by inner property must still work.
        sv = _bar_bare_col_sv()
        compile_visual_binding(sv, lambda e, p, m: "Double" if p == "Amount" else "String")
        ref, _ = attach_order_by(sv, "[Amount]", "asc")
        assert ref == "Sum(Sales.Amount)"
        ob = sv["prototypeQuery"]["OrderBy"][0]
        assert "Aggregation" in ob["Expression"]

    def test_hierarchy_level_select_refused_cleanly(self):
        # A HierarchyLevel select (Desktop date hierarchies) matched by bare
        # name must raise a clean ValueError, not UnboundLocalError.
        sv = {
            "visualType": "lineChart",
            "projections": {"Category": [{"queryRef": "d.Date Hierarchy.Year"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "d", "Entity": "Dates", "Type": 0}],
                "Select": [{
                    "HierarchyLevel": {"Expression": {"Hierarchy": {
                        "Expression": {"SourceRef": {"Source": "d"}},
                        "Hierarchy": "Date Hierarchy"}}, "Level": "Year"},
                    "Name": "d.Date Hierarchy.Year",
                }],
            },
        }
        with pytest.raises(ValueError, match="unsupported expression shape"):
            attach_order_by(sv, "d.Date Hierarchy.Year", "asc")
        assert "OrderBy" not in sv["prototypeQuery"]

    def test_escaped_quote_table_reference(self):
        # DAX escapes an apostrophe in a table name by doubling it:
        # 'O''Brien Sales'[Qty] refers to table "O'Brien Sales".
        sv = {
            "visualType": "barChart",
            "projections": {"Y": [{"queryRef": "O'Brien Sales.Qty"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "o", "Entity": "O'Brien Sales", "Type": 0}],
                "Select": [{
                    "Measure": {"Expression": {"SourceRef": {"Source": "o"}},
                                "Property": "Qty"},
                    "Name": "O'Brien Sales.Qty",
                }],
            },
        }
        ref, _ = attach_order_by(sv, "'O''Brien Sales'[Qty]", "desc")
        assert ref == "O'Brien Sales.Qty"
