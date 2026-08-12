"""Binding a field parameter into a visual (issue #8 / OpenBI findings #19).

The naive shape — the parameter's display column straight into a projection —
makes Desktop silently degrade the well to an implicit Count. The working
shape was extracted from a Desktop-authored binding, and every literal
asserted here (the queryFieldParametersByRole layout, the NAMEOF-style
triple-quoted Where literal, the sourceFieldParameters _kind values) is that
ground truth verbatim.
"""
from __future__ import annotations

import json
import os
import uuid

import pytest

from pbix_mcp import server
from pbix_mcp.report_binding import _nameof_literal


@pytest.fixture()
def fp_report(tmp_path):
    """A from-scratch report: Sales table + 3 measures + 'Metric' parameter +
    a column chart bound to Sales.Month — the findings' own repro fixture."""
    path = str(tmp_path / f"fp_{uuid.uuid4().hex[:8]}.pbix")
    alias = "fp" + uuid.uuid4().hex[:8]
    tables = [{
        "name": "Sales",
        "columns": [
            {"name": "Month", "data_type": "String"},
            {"name": "Revenue", "data_type": "Double"},
            {"name": "Units", "data_type": "Int64"},
        ],
        "rows": [
            {"Month": "Jan", "Revenue": 100.0, "Units": 10},
            {"Month": "Feb", "Revenue": 150.0, "Units": 12},
            {"Month": "Mar", "Revenue": 90.0, "Units": 8},
        ],
    }]
    measures = [
        {"table": "Sales", "name": "Total Revenue",
         "expression": "SUM(Sales[Revenue])"},
        {"table": "Sales", "name": "Total Units",
         "expression": "SUM(Sales[Units])"},
        {"table": "Sales", "name": "Total Profit",
         "expression": "SUM(Sales[Revenue]) * 0.3"},
    ]
    r = json.loads(server.pbix_create(
        path, alias, json.dumps(tables), json.dumps(measures)))
    assert r.get("success"), r
    r = json.loads(server.pbix_datamodel_add_field_parameter(
        alias, "Metric", json.dumps([
            {"display": "Revenue", "ref": "Sales[Total Revenue]"},
            {"display": "Units", "ref": "Sales[Total Units]"},
            {"display": "Profit", "ref": "Sales[Total Profit]"},
        ])))
    assert r.get("success"), r
    cfg = {"singleVisual": {
        "visualType": "clusteredColumnChart",
        "projections": {"Category": [{"queryRef": "Sales.Month"}]},
        "prototypeQuery": {
            "Version": 2,
            "From": [{"Name": "s", "Entity": "Sales", "Type": 0}],
            "Select": [{
                "Column": {"Expression": {"SourceRef": {"Source": "s"}},
                           "Property": "Month"},
                "Name": "Sales.Month",
            }],
        },
    }}
    r = json.loads(server.pbix_add_visual(
        alias, 0, "clusteredColumnChart", 40, 40, 400, 300,
        json.dumps(cfg)))
    assert r.get("success"), r
    yield alias
    try:
        server.pbix_close(alias, force=True)
    except Exception:
        pass


def _visual(alias, idx):
    raw = json.loads(server.pbix_get_layout_raw(alias))
    layout = json.loads(raw["message"])
    vc = layout["sections"][0]["visualContainers"][idx]
    return (json.loads(vc["config"]),
            json.loads(vc["query"]) if vc.get("query") else None,
            json.loads(vc["dataTransforms"]) if vc.get("dataTransforms") else None)


class TestNameofLiteral:
    def test_matches_desktop_ground_truth_verbatim(self):
        """'Sales'[Total Revenue] -> '''Sales''[Total Revenue]' — copied from
        the Desktop-authored file's compiled Where clause."""
        assert _nameof_literal("Sales", "Total Revenue") == \
            "'''Sales''[Total Revenue]'"


class TestBindFieldParameter:
    def test_all_five_pieces_match_the_desktop_shape(self, fp_report):
        alias = fp_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric"))
        assert r.get("success"), r

        config, query, dt = _visual(alias, 0)
        sv = config["singleVisual"]

        # 1. projection holds the RESOLVED field, prototype Select carries
        #    NativeReferenceName
        assert sv["projections"]["Y"] == [{"queryRef": "Sales.Total Revenue"}]
        sel = [s for s in sv["prototypeQuery"]["Select"]
               if s.get("Name") == "Sales.Total Revenue"]
        assert sel and "Measure" in sel[0]
        assert sel[0]["NativeReferenceName"] == "Revenue"

        # 2. queryFieldParametersByRole — Desktop-authored layout verbatim
        assert sv["queryFieldParametersByRole"]["Y"] == [{
            "index": 0, "length": 1,
            "expr": {"Column": {
                "Expression": {"SourceRef": {"Entity": "Metric"}},
                "Property": "Metric"}},
        }]

        # 3. columnProperties restates the display label
        assert sv["columnProperties"]["Sales.Total Revenue"] == {
            "displayName": "Revenue"}

        # 4. compiled query: parameter table joined + Where over the hidden
        #    Fields column with the NAMEOF triple-quoted literal
        q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        entities = {f["Entity"] for f in q["From"]}
        assert {"Sales", "Metric"} <= entities
        wh = q["Where"][0]["Condition"]["In"]
        assert wh["Expressions"][0]["Column"]["Property"] == "Metric Fields"
        assert wh["Values"] == [[{"Literal": {
            "Value": "'''Sales''[Total Revenue]'"}}]]

        # 5. dataTransforms select carries sourceFieldParameters provenance
        rev = [s for s in dt["selects"]
               if s.get("queryName") == "Sales.Total Revenue"]
        assert rev
        assert rev[0]["sourceFieldParameters"] == [{
            "expr": {"_kind": 2,
                     "source": {"_kind": 0, "entity": "Metric"},
                     "ref": "Metric"},
            "displayName": "Metric",
        }]

    def test_initial_field_by_display_name_and_by_ref(self, fp_report):
        alias = fp_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric", initial_field="Units"))
        assert r.get("success"), r
        config, query, _dt = _visual(alias, 0)
        assert config["singleVisual"]["projections"]["Y"] == [
            {"queryRef": "Sales.Total Units"}]
        q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        assert q["Where"][0]["Condition"]["In"]["Values"] == [[
            {"Literal": {"Value": "'''Sales''[Total Units]'"}}]]

        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric",
            initial_field="Sales[Total Profit]"))
        assert r.get("success"), r
        config, _q, _dt = _visual(alias, 0)
        assert config["singleVisual"]["columnProperties"][
            "Sales.Total Profit"] == {"displayName": "Profit"}

    def test_rebinding_replaces_not_duplicates(self, fp_report):
        alias = fp_report
        for field in ("Revenue", "Units", "Revenue"):
            r = json.loads(server.pbix_bind_field_parameter(
                alias, 0, 0, "Y", "Metric", initial_field=field))
            assert r.get("success"), r
        config, query, _dt = _visual(alias, 0)
        sv = config["singleVisual"]
        assert len(sv["projections"]["Y"]) == 1
        names = [s.get("Name") for s in sv["prototypeQuery"]["Select"]]
        assert names.count("Sales.Total Revenue") == 1
        assert "Sales.Total Units" not in names  # replaced, not accreted
        q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        assert len(q["Where"]) == 1

    def test_unknown_parameter_and_field_are_refused(self, fp_report):
        alias = fp_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "NotAParameter"))
        assert not r.get("success")
        assert "not a field parameter" in r.get("message", "")
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric", initial_field="Margin"))
        assert not r.get("success")
        assert "not one of" in r.get("message", "")


class TestNaiveShapeWarns:
    def test_add_visual_warns_on_unbound_parameter_projection(self, fp_report):
        """The findings' exact wrong shape must WARN, not pass silently."""
        alias = fp_report
        cfg = {"singleVisual": {
            "visualType": "clusteredColumnChart",
            "projections": {"Y": [{"queryRef": "Metric.Metric"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "m", "Entity": "Metric", "Type": 0}],
                "Select": [{
                    "Column": {"Expression": {"SourceRef": {"Source": "m"}},
                               "Property": "Metric"},
                    "Name": "Metric.Metric",
                }],
            },
        }}
        r = json.loads(server.pbix_add_visual(
            alias, 0, "clusteredColumnChart", 40, 400, 400, 300,
            json.dumps(cfg)))
        assert r.get("success"), r
        warns = " ".join(r.get("warnings") or [])
        assert "queryFieldParametersByRole" in warns
        assert "pbix_bind_field_parameter" in warns

    def test_bound_visual_does_not_warn(self, fp_report):
        alias = fp_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric"))
        assert r.get("success"), r
        # rebinding produces no unbound-parameter warning
        assert not any("degrade" in w for w in (r.get("warnings") or []))


class TestSavedRoundTrip:
    def test_binding_survives_save_and_reopen(self, fp_report, tmp_path):
        alias = fp_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric"))
        assert r.get("success"), r
        out = str(tmp_path / "fp_saved.pbix")
        r = json.loads(server.pbix_save(alias, out, overwrite=True,
                                        backup=False))
        assert r.get("success"), r
        alias2 = "fp2" + uuid.uuid4().hex[:6]
        server.pbix_open(out, alias2)
        try:
            config, query, dt = _visual(alias2, 0)
            sv = config["singleVisual"]
            assert "queryFieldParametersByRole" in sv
            q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
            assert q["Where"][0]["Condition"]["In"]["Values"][0][0][
                "Literal"]["Value"] == "'''Sales''[Total Revenue]'"
            assert os.path.getsize(out) > 0
        finally:
            server.pbix_close(alias2, force=True)


class TestSortInteraction:
    """Rebinding must not leave the compiled query ordering by a field it no
    longer selects. Found by adversarial probing after 0.9.57 shipped: after
    pbix_set_visual_sort on the bound field, rebinding Y to a different field
    dropped that select but left the OrderBy pointing at it — a dangling
    reference in the compiled query.
    """

    def _bound_and_sorted(self, alias):
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric", initial_field="Revenue"))
        assert r.get("success"), r
        r = json.loads(server.pbix_set_visual_sort(
            alias, 0, 0, "Sales.Total Revenue", "desc"))
        assert r.get("success"), r

    def test_wiring_survives_a_resort(self, fp_report):
        alias = fp_report
        self._bound_and_sorted(alias)
        config, query, _dt = _visual(alias, 0)
        sv = config["singleVisual"]
        assert "queryFieldParametersByRole" in sv
        q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        assert q.get("Where"), "the parameter Where clause was lost by re-sort"

    def test_rebind_leaves_no_dangling_order_by(self, fp_report):
        alias = fp_report
        self._bound_and_sorted(alias)
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 0, "Y", "Metric", initial_field="Units"))
        assert r.get("success"), r
        config, query, _dt = _visual(alias, 0)
        sv = config["singleVisual"]
        selects = {s["Name"] for s in sv["prototypeQuery"]["Select"]}
        assert "Sales.Total Revenue" not in selects  # replaced
        q = query["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        blob = json.dumps(q.get("OrderBy") or [])
        # the OrderBy must point at the NEW field, never the dropped one
        assert "Total Revenue" not in blob, f"dangling OrderBy: {blob}"
        assert "Total Units" in blob, f"sort intent lost: {blob}"

class TestNoDataRoleVisuals:
    """A visual with no field wells cannot host a field parameter. This used to
    "succeed", leaving a textbox carrying a query + dataTransforms + a Y
    projection -- incoherent, and pbix_doctor does not flag it."""

    @pytest.mark.parametrize("vtype", ["textbox", "image", "shape"])
    def test_refused_and_visual_left_untouched(self, fp_report, vtype):
        alias = fp_report
        r = json.loads(server.pbix_add_visual(
            alias, 0, vtype, 10, 10, 200, 60, ""))
        assert r.get("success"), r
        before = _visual(alias, 1)
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 1, "Y", "Metric"))
        assert not r.get("success"), r
        assert "no field wells" in r.get("message", "")
        after = _visual(alias, 1)
        assert after == before, "a refused bind must not mutate the visual"
        assert after[1] is None, "a non-data visual must not gain a query"

    def test_a_real_data_visual_still_binds(self, fp_report):
        """The guard must not catch anything with wells."""
        r = json.loads(server.pbix_bind_field_parameter(
            fp_report, 0, 0, "Y", "Metric"))
        assert r.get("success"), r


class TestCoveredColumnStaysBare:
    """Issues #36 + #37: a parameter-covered projection is never Sum-wrapped
    (the wrap breaks the queryRef <-> NAMEOF correspondence Desktop resolves
    the well through — #36, Desktop-verified). But a RAW COLUMN in an
    AGGREGATING value role renders empty either way (bare, Desktop drops the
    un-aggregated well — #37, Desktop-verified), so a column-backed parameter
    aimed at Y/Values/Y2/Size of a chart is REFUSED at bind time with an
    actionable message; grouping roles (Category/Axis/Rows — the documented
    dimension-swap use) still take column parameters, projected bare."""

    @pytest.fixture()
    def col_param_report(self, tmp_path):
        """The issue's own repro: F(Metric, Sales, Cost) + a parameter over
        the two numeric COLUMNS + two identical column charts."""
        path = str(tmp_path / f"fp36_{uuid.uuid4().hex[:8]}.pbix")
        alias = "fp36" + uuid.uuid4().hex[:8]
        tables = [{
            "name": "F",
            "columns": [
                {"name": "Metric", "data_type": "String"},
                {"name": "Sales", "data_type": "Double"},
                {"name": "Cost", "data_type": "Double"},
            ],
            "rows": [
                {"Metric": "Alpha", "Sales": 120.0, "Cost": 80.0},
                {"Metric": "Beta", "Sales": 200.0, "Cost": 90.0},
            ],
        }]
        r = json.loads(server.pbix_create(path, alias, json.dumps(tables)))
        assert r.get("success"), r
        r = json.loads(server.pbix_datamodel_add_field_parameter(
            alias, "Metric switch", json.dumps([
                {"display": "Sales", "ref": "F[Sales]"},
                {"display": "Cost", "ref": "F[Cost]"},
            ])))
        assert r.get("success"), r
        cfg = {"singleVisual": {
            "visualType": "columnChart",
            "projections": {"Category": [{"queryRef": "F.Metric"}],
                            "Y": [{"queryRef": "Sum(F.Sales)"}]},
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "f", "Entity": "F", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "f"}},
                                "Property": "Metric"}, "Name": "F.Metric"},
                    {"Aggregation": {"Expression": {"Column": {
                        "Expression": {"SourceRef": {"Source": "f"}},
                        "Property": "Sales"}}, "Function": 0},
                     "Name": "Sum(F.Sales)"},
                ],
            },
        }}
        for _ in range(2):
            r = json.loads(server.pbix_add_visual(
                alias, 0, "columnChart", 40, 40, 560, 360, json.dumps(cfg)))
            assert r.get("success"), r
        yield alias
        try:
            server.pbix_close(alias, force=True)
        except Exception:
            pass

    def test_column_parameter_on_value_role_is_refused(self, col_param_report):
        # Issue #37: bare column on Y renders empty in Desktop, so the bind
        # must fail LOUDLY at author time, with an actionable message.
        alias = col_param_report
        before = _visual(alias, 1)
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 1, "Y", "Metric switch", initial_field="F[Sales]"))
        assert not r.get("success"), r
        msg = r.get("message", "")
        assert "must list MEASURES" in msg
        assert "'Sales' ('F'[Sales])" in msg          # names the offenders
        assert "pbix_datamodel_add_measure" in msg    # actionable remedy
        assert "Category/Axis/Rows" in msg            # the valid alternative
        assert _visual(alias, 1) == before, \
            "a refused bind must not mutate the visual"

    def test_column_parameter_on_grouping_role_binds_bare(
            self, col_param_report):
        # Grouping roles are the documented dimension-swap use: the covered
        # column binds, projected BARE (never Sum-wrapped — issue #36), with
        # the full oracle shape around it.
        alias = col_param_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 1, "Category", "Metric switch",
            initial_field="F[Sales]"))
        assert r.get("success"), r
        config, q, dt = _visual(alias, 1)
        sv = config["singleVisual"]

        # 1. the projection is the BARE column ref the NAMEOF resolves to
        assert sv["projections"]["Category"] == [{"queryRef": "F.Sales"}]
        # 2. the prototype select stays a bare Column (no Aggregation wrap),
        #    with the display name as NativeReferenceName
        c_sel = next(s for s in sv["prototypeQuery"]["Select"]
                     if s["Name"] == "F.Sales")
        assert "Column" in c_sel and "Aggregation" not in c_sel
        assert c_sel["NativeReferenceName"] == "Sales"
        # 3. columnProperties keyed by the ACTUAL projection queryRef
        assert sv["columnProperties"]["F.Sales"] == {"displayName": "Sales"}
        # 4. compiled query: parameter entity joined + NAMEOF Where, and the
        #    select is the same bare column
        qq = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        assert any(f.get("Entity") == "Metric switch" for f in qq["From"])
        assert any(s.get("Name") == "F.Sales" and "Column" in s
                   for s in qq["Select"])
        where = json.dumps(qq.get("Where"))
        assert "Metric switch Fields" in where
        assert "'''F''[Sales]'" in where
        # 5. dataTransforms mirrors the projection with provenance
        c_dt = next(s for s in dt["selects"] if s["queryName"] == "F.Sales")
        assert c_dt["roles"] == {"Category": True}
        assert c_dt["sourceFieldParameters"] == [{
            "expr": {"_kind": 2,
                     "source": {"_kind": 0, "entity": "Metric switch"},
                     "ref": "Metric switch"},
            "displayName": "Metric switch"}]

    def test_column_parameter_on_table_values_still_binds(
            self, col_param_report):
        # Chandoo oracle: a covered COLUMN in a tableEx Values well is valid
        # Desktop shape (tables show raw values) — the refusal must not
        # catch it.
        alias = col_param_report
        r = json.loads(server.pbix_add_visual(
            alias, 0, "tableEx", 40, 420, 560, 200, json.dumps({
                "singleVisual": {
                    "visualType": "tableEx",
                    "projections": {"Values": [{"queryRef": "F.Metric"}]},
                    "prototypeQuery": {
                        "Version": 2,
                        "From": [{"Name": "f", "Entity": "F", "Type": 0}],
                        "Select": [{
                            "Column": {
                                "Expression": {"SourceRef": {"Source": "f"}},
                                "Property": "Metric"},
                            "Name": "F.Metric"}],
                    },
                }})))
        assert r.get("success"), r
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 2, "Values", "Metric switch"))
        assert r.get("success"), r
        config, _q, _dt = _visual(alias, 2)
        assert config["singleVisual"]["projections"]["Values"] == [
            {"queryRef": "F.Sales"}]

    def test_unbound_sibling_still_gets_implicit_sum(self, col_param_report):
        # the OTHER chart on the page keeps the normal value-role behavior
        alias = col_param_report
        r = json.loads(server.pbix_bind_field_parameter(
            alias, 0, 1, "Category", "Metric switch"))
        assert r.get("success"), r
        config, q, _dt = _visual(alias, 0)
        assert config["singleVisual"]["projections"]["Y"] == [
            {"queryRef": "Sum(F.Sales)"}]
        qq = q["Commands"][0]["SemanticQueryDataShapeCommand"]["Query"]
        y = next(s for s in qq["Select"] if s["Name"] == "Sum(F.Sales)")
        assert "Aggregation" in y


class TestApplyImplicitAggregationsSkipsCovered:
    """Unit level: apply_implicit_aggregations must skip exactly the
    parameter-covered projections and still wrap uncovered ones."""

    def test_covered_skipped_uncovered_wrapped(self):
        from pbix_mcp.report_binding import apply_implicit_aggregations
        sv = {
            "visualType": "columnChart",
            "projections": {
                "Category": [{"queryRef": "T.Cat"}],
                "Y": [{"queryRef": "T.A"}, {"queryRef": "T.B"}],
            },
            "queryFieldParametersByRole": {
                "Y": [{"index": 0, "length": 1, "expr": {"Column": {
                    "Expression": {"SourceRef": {"Entity": "P"}},
                    "Property": "P"}}}],
            },
            "prototypeQuery": {
                "Version": 2,
                "From": [{"Name": "t", "Entity": "T", "Type": 0}],
                "Select": [
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                                "Property": "Cat"}, "Name": "T.Cat"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                                "Property": "A"}, "Name": "T.A"},
                    {"Column": {"Expression": {"SourceRef": {"Source": "t"}},
                                "Property": "B"}, "Name": "T.B"},
                ],
            },
        }
        renamed = apply_implicit_aggregations(
            sv, lambda e, p, m: "Double" if p in ("A", "B") else "String")
        # covered T.A untouched; uncovered T.B wrapped
        assert "T.A" not in renamed
        assert renamed.get("T.B") == "Sum(T.B)"
        assert sv["projections"]["Y"] == [
            {"queryRef": "T.A"}, {"queryRef": "Sum(T.B)"}]
        names = [s["Name"] for s in sv["prototypeQuery"]["Select"]]
        assert names == ["T.Cat", "T.A", "Sum(T.B)"]
