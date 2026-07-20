"""Compile a visual's ``projections`` + ``prototypeQuery`` into the compiled
data binding (``query`` + ``dataTransforms``) that Power BI Desktop's report
loader requires on every legacy ``Report/Layout`` visualContainer.

Without this, Desktop fails the whole report with "Something went wrong — Failed
to load the report" once the report carries any report-level ``config`` /
visual ``objects`` (as OpenBI/pbix-mcp reports do), even though the data model
opens cleanly. Verified against real Power BI Desktop: an OpenBI report that
lacked these fails to load; injecting them makes it load.

The mapping is mechanical — everything needed is already in ``projections``
(role -> [{queryRef}]) and ``prototypeQuery`` (Version/From/ordered Select, each
Select carrying its queryRef ``Name`` and a Column/Measure expression). The
type codes were taken from real Desktop reports (sales_demo, GeoSales corpus).
"""

from __future__ import annotations

import copy

_TABLE_TYPES = {"tableEx", "table"}
# matrix / pivotTable cross rows against columns — a fundamentally different
# binding (Primary rows + Secondary columns) from a flat table.
_MATRIX_TYPES = {"matrix", "pivotTable"}
_SLICER_TYPES = {"slicer", "advancedSlicerVisual"}
_CARTESIAN_TYPES = {
    "barChart", "columnChart", "clusteredBarChart", "clusteredColumnChart",
    "stackedBarChart", "stackedColumnChart", "hundredPercentStackedBarChart",
    "hundredPercentStackedColumnChart", "lineChart", "areaChart",
    "stackedAreaChart", "lineClusteredColumnComboChart",
    "lineStackedColumnComboChart", "scatterChart", "ribbonChart", "funnel",
}
# Roles that act as a value/measure axis (paired with Series => pivoted).
_VALUE_ROLES = {"Y", "Values", "Y2", "Size"}
# Model value types Power BI implicitly Sums when a bare column lands on a value
# axis (everything else is Count-ed). QueryAggregateFunction: Sum=0, CountNonNull=5.
_NUMERIC_TYPES = {"Int64", "Double", "Decimal"}
_AGG_SUM = 0
_AGG_COUNTNONNULL = 5

# (underlyingType, queryMetadata.Type) per model data type — taken verbatim from
# Desktop-authored reports (sales_demo + GeoSales/IT_Support/etc. corpus). Keyed
# on the field's VALUE type, for BOTH columns and measures (an integer measure
# like SUM(int) is 260/3, a currency column like UnitPrice is 259/1).
_TYPE_CODES: dict[str, tuple[int, int]] = {
    "String": (1, 2048),
    "Boolean": (1, 2048),
    "Int64": (260, 3),
    "Double": (259, 1),
    "Decimal": (259, 1),
    "DateTime": (519, 4),
}
_DEFAULT_CODES = (259, 1)  # unknown -> numeric/decimal (the common measure case)


def _type_codes(data_type: str | None) -> tuple[int, int]:
    return _TYPE_CODES.get(data_type or "", _DEFAULT_CODES)


def _data_reduction(visual_type: str) -> dict:
    """DataReduction matching Desktop for the visual family."""
    if visual_type in _CARTESIAN_TYPES:
        return {"DataVolume": 4, "Primary": {"Window": {"Count": 1000}}}
    if visual_type in _TABLE_TYPES:
        return {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}}
    if visual_type in _SLICER_TYPES:
        return {"DataVolume": 3, "Primary": {"Window": {"Count": 100}}}
    # card / pie / donut / default
    return {"DataVolume": 3, "Primary": {"Top": {}}}


def apply_implicit_aggregations(single_visual: dict, resolve_type=None) -> dict[str, str]:
    """Rewrite plain value-role columns in the PROTOTYPE query to Aggregations.

    Power BI Desktop re-derives the live data query from
    ``config.singleVisual.prototypeQuery`` + ``projections`` — aggregating only
    the compiled ``query`` is not enough (Desktop-verified: the chart stays
    empty). When Desktop's own field well receives a numeric column on a value
    axis it stores an ``Aggregation`` select named ``Sum(Entity.Property)`` in
    the prototype and points the projection at that name (ground truth: AI
    Sample barChart). This mirrors that: every bare ``Column`` select projected
    into a value role (``Y``/``Values``/``Y2``/``Size``) becomes
    ``{"Aggregation": {"Expression": {"Column": ...}, "Function": Sum|CountNonNull}}``
    with the Desktop queryRef naming, and the projections are updated in place.
    Flat grids (``table``/``tableEx``) and slicers show raw values and are left
    alone; measures and explicit aggregations are untouched.

    Returns a mapping of old queryRef -> new queryRef (empty when nothing changed).
    """
    proto = single_visual.get("prototypeQuery") or {}
    projections = single_visual.get("projections") or {}
    visual_type = single_visual.get("visualType", "")
    selects = proto.get("Select") or []
    if not selects or visual_type in _TABLE_TYPES or visual_type in _SLICER_TYPES:
        return {}

    alias2entity = {f.get("Name"): f.get("Entity") for f in proto.get("From", [])}
    ref2role = {it.get("queryRef"): role
                for role, items in projections.items() for it in items}

    renamed: dict[str, str] = {}
    for sel in selects:
        if "Column" not in sel:
            continue                       # measures / explicit aggregations
        old_ref = sel.get("Name")
        if ref2role.get(old_ref) not in _VALUE_ROLES:
            continue
        col = sel["Column"]
        src = col.get("Expression", {}).get("SourceRef", {})
        entity = alias2entity.get(src.get("Source"), src.get("Entity"))
        prop = col.get("Property", "")
        vtype = None
        if resolve_type is not None:
            try:
                vtype = resolve_type(entity, prop, False)
            except Exception:
                vtype = None
        func = _AGG_SUM if vtype in _NUMERIC_TYPES else _AGG_COUNTNONNULL
        fn_name = "Sum" if func == _AGG_SUM else "CountNonNull"
        new_ref = (f"{fn_name}({entity}.{prop})" if entity and prop
                   else f"{fn_name}({old_ref})")
        sel.pop("Column")
        sel["Aggregation"] = {"Expression": {"Column": col}, "Function": func}
        sel["Name"] = new_ref
        renamed[old_ref] = new_ref

    if renamed:
        for items in projections.values():
            for it in items:
                ref = it.get("queryRef")
                if ref in renamed:
                    it["queryRef"] = renamed[ref]
    return renamed


def compile_visual_binding(single_visual: dict, resolve_type=None):
    """Compile ``query`` and ``dataTransforms`` dicts for one visual.

    Parameters
    ----------
    single_visual : dict
        The ``config.singleVisual`` object (needs ``projections`` and
        ``prototypeQuery`` with a non-empty ``Select``).
    resolve_type : callable | None
        ``resolve_type(entity, prop, is_measure) -> data_type_str | None``.
        Used only to pick a column's type code; measures are self-describing.

    Returns
    -------
    (query, data_transforms) : tuple[dict, dict] | (None, None)
        ``(None, None)`` when the visual has no bindable projections (textbox,
        shape, image, button, …) and therefore needs no data binding.

    Side effect: bare value-role columns are rewritten to implicit Aggregations
    IN ``single_visual`` itself (prototypeQuery + projections) via
    :func:`apply_implicit_aggregations` — the caller must persist the mutated
    config, or Desktop re-derives an unaggregated query and the chart is empty.
    """
    proto = single_visual.get("prototypeQuery")
    projections = single_visual.get("projections") or {}
    visual_type = single_visual.get("visualType", "")
    if not proto or not proto.get("Select"):
        return None, None

    # Mirror Desktop's field well: a numeric column dropped on a value axis is
    # stored as Sum(...) in the PROTOTYPE (and the projection re-pointed).
    apply_implicit_aggregations(single_visual, resolve_type)

    selects = proto["Select"]
    alias2entity = {f.get("Name"): f.get("Entity") for f in proto.get("From", [])}
    ref2idx = {s.get("Name"): i for i, s in enumerate(selects)}

    def _node(sel):
        # Unwrap an Aggregation (e.g. Sum(col)) to its inner Column/Measure so the
        # Property / SourceRef helpers work for aggregate selects too.
        if "Aggregation" in sel:
            inner = sel["Aggregation"].get("Expression", {})
            return inner.get("Column") or inner.get("Measure") or {}
        return sel.get("Measure") or sel.get("Column") or {}

    def _native(sel):
        return _node(sel).get("Property", sel.get("Name", ""))

    def _entity_of(sel):
        src = _node(sel).get("Expression", {}).get("SourceRef", {})
        return alias2entity.get(src.get("Source"), src.get("Entity"))

    def _rewrite_src(node):
        ref = node.get("Expression", {}).get("SourceRef", {})
        node["Expression"]["SourceRef"] = {
            "Entity": alias2entity.get(ref.get("Source"), ref.get("Source"))}

    def _entity_expr(sel):
        """The Column / Measure / Aggregation expr with SourceRef.Source rewritten
        to .Entity (implicit value-role aggregations were already applied to the
        prototype by :func:`apply_implicit_aggregations`)."""
        if "Aggregation" in sel:
            agg = copy.deepcopy(sel["Aggregation"])
            inner = agg.get("Expression", {})
            for key in ("Column", "Measure"):
                if key in inner:
                    _rewrite_src(inner[key])
            return {"Aggregation": agg}
        key = "Measure" if "Measure" in sel else "Column"
        node = copy.deepcopy(sel[key])
        _rewrite_src(node)
        return {key: node}

    # Deduplicate NativeReferenceName across selects (query only): two fields
    # sharing a Property name (Products.ProductID and Sales.ProductID) get
    # "ProductID" and "ProductID1", matching Desktop. The queryMetadata
    # Restatement and dataTransforms displayName keep the raw name.
    native_names: list[str] = []
    _seen: dict[str, int] = {}
    for s in selects:
        base = _native(s)
        cnt = _seen.get(base, -1) + 1
        _seen[base] = cnt
        native_names.append(base if cnt == 0 else f"{base}{cnt}")

    # --- query: SemanticQueryDataShapeCommand ---
    q = copy.deepcopy(proto)
    for i, s in enumerate(q["Select"]):
        s["NativeReferenceName"] = native_names[i]
    n = len(selects)
    all_idx = list(range(n))
    roles_present = set(projections.keys())

    # role -> ordered select indices (used by both binding + dataTransforms)
    projection_ordering: dict[str, list[int]] = {}
    ref2role: dict[str, str] = {}
    for role, items in projections.items():
        idxs = []
        for it in items:
            ref = it.get("queryRef")
            if ref in ref2idx:
                idxs.append(ref2idx[ref])
                ref2role[ref] = role
        if idxs:
            projection_ordering[role] = idxs

    is_matrix = visual_type in _MATRIX_TYPES
    is_slicer = visual_type in _SLICER_TYPES
    is_table = visual_type in _TABLE_TYPES
    # A binding is "pivoted" when a Series/legend dimension is crossed with a
    # value axis (matches the Desktop-authored sales_demo pie: Series + Y). This
    # is a data-shape property of pie/donut/stacked charts — NOT matrices, which
    # express rows-vs-columns through a Secondary grouping instead.
    pivoted = "Series" in roles_present and bool(roles_present & _VALUE_ROLES)

    if is_matrix:
        # matrix / pivotTable: rows on the Primary axis (one grouping per row
        # level), columns + values crossed on the Secondary axis. Ground truth:
        # Matrix Bubble Chart.pbix + Contoso IBCS. A matrix with no column field
        # collapses to a flat single-grouping table.
        rows = projection_ordering.get("Rows", [])
        cols = projection_ordering.get("Columns", [])
        vals = projection_ordering.get("Values", [])
        if not (rows or cols or vals):        # untagged projections -> rows
            rows = all_idx
        if cols:
            primary = [{"Projections": [i]} for i in rows] or [{"Projections": []}]
            binding = {
                "Primary": {"Groupings": primary},
                "Secondary": {"Groupings": [{"Projections": cols + vals}]},
                "DataReduction": {"DataVolume": 3,
                                  "Primary": {"Window": {"Count": 100}},
                                  "Secondary": {"Top": {"Count": 100}}},
                "Version": 1,
            }
        else:
            binding = {
                "Primary": {"Groupings": [{"Projections": rows + vals, "Subtotal": 1}]},
                "DataReduction": {"DataVolume": 3, "Primary": {"Window": {"Count": 500}}},
                "Version": 1,
            }
    elif is_slicer:
        # slicer: enumerate every distinct value (empty Window, no Count) and
        # keep empty groups. Ground truth: AI Sample / Cars Sales / Matrix Bubble.
        binding = {
            "Primary": {"Groupings": [{"Projections": all_idx}]},
            "DataReduction": {"DataVolume": 3, "Primary": {"Window": {}}},
            "IncludeEmptyGroups": True,
            "Version": 1,
        }
    else:
        grouping: dict = {"Projections": all_idx}
        if is_table:
            grouping["Subtotal"] = 1
        binding = {
            "Primary": {"Groupings": [grouping]},
            "DataReduction": _data_reduction(visual_type),
            "Version": 1,
        }
        if pivoted:
            binding["isPivoted"] = True
    query = {"Commands": [{"SemanticQueryDataShapeCommand": {
        "Query": q, "Binding": binding, "ExecutionMetricsKind": 1,
    }}]}

    # --- dataTransforms ---
    # A DataRole is "active" for the dimensions the visual pivots on: matrix
    # rows/columns, and slicer values (Desktop marks these isActive:true).
    def _is_active(role: str) -> bool:
        if is_matrix:
            return role in ("Rows", "Columns")
        if is_slicer:
            return True
        return False

    data_roles: list[dict] = []
    for role, idxs in projection_ordering.items():
        for ix in idxs:
            data_roles.append({"Name": role, "Projection": ix,
                               "isActive": _is_active(role)})

    qm_select: list[dict] = []
    dt_selects: list[dict] = []
    for i, s in enumerate(selects):
        ref = s.get("Name")
        native = _native(s)  # raw (un-deduped) for Restatement / displayName
        is_measure = "Measure" in s
        dtype = None
        if resolve_type is not None:
            try:
                dtype = resolve_type(_entity_of(s), _native(s), is_measure)
            except Exception:
                dtype = None
        underlying, tcode = _type_codes(dtype)
        qm_select.append({"Restatement": native, "Name": ref, "Type": tcode})
        role = ref2role.get(ref)
        dt_selects.append({
            "displayName": native,
            "queryName": ref,
            "roles": ({role: True} if role else {}),
            "type": {"category": None, "underlyingType": underlying},
            "expr": _entity_expr(s),
        })

    data_transforms = {
        "projectionOrdering": projection_ordering,
        "queryMetadata": {"Select": qm_select},
        "visualElements": [{"DataRoles": data_roles}],
        "selects": dt_selects,
    }
    return query, data_transforms
