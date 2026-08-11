"""TMDL (Tabular Model Definition Language) reader — the inverse of the
exporter in server.py Section 10b.

Parses a TMDL folder (database.tmdl / model.tmdl / expressions.tmdl /
relationships.tmdl / tables/*.tmdl / roles/*.tmdl) — or a single TMDL
document string — into a plain model dict that
``server.pbix_import_tmdl`` / ``server.pbix_open_pbip`` turn back into a
working PBIX (issue #34).

Format notes (pinned against both this project's exporter and Desktop-
authored PBIP output):

* Structure is indentation-based (tabs). An object is ``kind [Name]``,
  optionally quoted (``table 'My Table'``); a property is ``key: value`` one
  level deeper; a bare word (``isHidden``) is a boolean flag.
* ``kind 'Name' = <rest>`` carries an inline expression (single-line
  measures / calculated columns / ``partition 'P' = m``).
* ``kind 'Name' =`` with nothing after ``=`` opens a multi-line expression
  BLOCK: the following lines indented at least two levels deeper than the
  declaration. Properties of the same object keep sitting exactly ONE level
  deeper — that is how ``expression X =`` + M block + ``lineageTag:`` on the
  next line coexist.
* Unknown properties/objects (annotations, changedProperty, variations...)
  are parsed structurally and simply ignored by the assembler, so
  Desktop-authored TMDL loads without tripping over vocabulary this reader
  does not model.
"""

from __future__ import annotations

import os
import re


# TOM data-type name (TMDL) <-> AMO ExplicitDataType code, and the builder's
# type names. Mirrors the exporter's _dtype_map.
TMDL_TO_AMO_TYPE = {
    "string": 2, "int64": 6, "double": 8, "dateTime": 9,
    "decimal": 10, "boolean": 11, "binary": 17, "variant": 20,
}
AMO_TO_BUILDER_TYPE = {
    2: "String", 6: "Int64", 8: "Double", 9: "DateTime",
    10: "Decimal", 11: "Boolean",
    # binary/variant have no builder encoding; imported as String schema-wise
    17: "String", 20: "String",
}
# TMDL crossFilteringBehavior keyword -> TOM code (1 = oneDirection default)
CROSS_FILTER_CODES = {"onedirection": 1, "bothdirections": 2, "automatic": 3}
# TMDL cardinality keyword -> TOM code
CARDINALITY_CODES = {"none": 0, "one": 1, "many": 2}
# TMDL summarizeBy keyword (lowercased) -> TOM code
SUMMARIZE_BY_CODES = {
    "default": 1, "none": 2, "sum": 3, "min": 4, "max": 5,
    "count": 6, "average": 7, "distinctcount": 8,
}
SUMMARIZE_BY_NAMES = {1: "default", 2: "none", 3: "sum", 4: "min", 5: "max",
                      6: "count", 7: "average", 8: "distinctCount"}


class TmdlNode:
    """One parsed TMDL object: ``kind [name] [= expression]`` plus its
    ``key: value`` properties, bare boolean flags, and nested child objects."""

    __slots__ = ("kind", "name", "expression", "props", "flags", "children")

    def __init__(self, kind: str, name: str = ""):
        self.kind = kind
        self.name = name
        self.expression: str | None = None
        self.props: dict[str, str] = {}
        self.flags: set[str] = set()
        self.children: list[TmdlNode] = []

    def child(self, kind: str) -> "TmdlNode | None":
        for c in self.children:
            if c.kind == kind:
                return c
        return None

    def all(self, kind: str) -> list["TmdlNode"]:
        return [c for c in self.children if c.kind == kind]

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"TmdlNode({self.kind!r}, {self.name!r})"


def _unquote(name: str) -> str:
    """Undo TMDL name quoting: ``'My ''quoted'' name'`` -> ``My 'quoted' name``.
    Also tolerates the exporter's backslash escapes (\\\\ and \\")."""
    name = name.strip()
    if len(name) >= 2 and name.startswith("'") and name.endswith("'"):
        inner = name[1:-1]
        inner = inner.replace("''", "'")
        inner = inner.replace('\\"', '"').replace("\\\\", "\\")
        return inner
    return name


def _split_object_header(rest: str) -> tuple[str, str | None]:
    """Split ``'Name' = expr`` / ``Name = expr`` / ``'Name'`` / ``Name`` into
    (name, inline_expression_or_None). The ``=`` is only recognized OUTSIDE
    the quoted name."""
    rest = rest.strip()
    if rest.startswith("'"):
        # scan to the closing quote, honoring '' escapes
        i = 1
        while i < len(rest):
            if rest[i] == "'":
                if i + 1 < len(rest) and rest[i + 1] == "'":
                    i += 2
                    continue
                break
            i += 1
        name = _unquote(rest[: i + 1])
        tail = rest[i + 1:].strip()
    else:
        eq = rest.find("=")
        if eq >= 0:
            name = rest[:eq].strip()
            tail = rest[eq:].strip()
        else:
            name = rest
            tail = ""
    if tail.startswith("="):
        return name, tail[1:].strip()
    return name, None


_PROP_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$")
# Object kinds that may carry a name / expression. Anything else that looks
# like ``word rest`` is still parsed as an object of kind ``word``.
_BARE_WORD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _indent_of(line: str) -> int:
    n = 0
    while n < len(line) and line[n] == "\t":
        n += 1
    return n


def parse_tmdl_document(text: str) -> list[TmdlNode]:
    """Parse one TMDL document into its top-level object nodes."""
    raw_lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    # (indent, content) with blanks kept as None markers (block separators)
    lines: list[tuple[int, str] | None] = []
    for ln in raw_lines:
        if not ln.strip():
            lines.append(None)
        else:
            lines.append((_indent_of(ln), ln))

    roots: list[TmdlNode] = []
    # stack of (indent, node)
    stack: list[tuple[int, TmdlNode]] = []
    i = 0
    n = len(lines)
    while i < n:
        item = lines[i]
        if item is None:
            i += 1
            continue
        indent, line = item
        content = line[indent:]

        # pop the stack down to this line's parent
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1] if stack else None

        m = _PROP_RE.match(content)
        if m and parent is not None and m.group(2) != "":
            parent.props[m.group(1)] = m.group(2).strip()
            i += 1
            continue
        if m and parent is not None and m.group(2) == "":
            # ``key:`` with empty value — treat as empty-string property
            parent.props[m.group(1)] = ""
            i += 1
            continue

        # object / flag / expression-opener
        parts = content.split(None, 1)
        kind = parts[0]
        rest = parts[1] if len(parts) > 1 else ""
        eq_only = False
        inline_expr: str | None = None
        name = ""
        if rest:
            if rest.strip() == "=":
                eq_only = True
            else:
                name, inline_expr = _split_object_header(rest)
                if inline_expr == "":
                    eq_only = True
                    inline_expr = None
        elif kind.endswith("="):
            kind = kind[:-1].strip()
            eq_only = True

        node = TmdlNode(kind, name)
        if inline_expr is not None:
            node.expression = inline_expr

        if eq_only:
            # Multi-line expression block: following lines indented >= indent+2.
            block: list[str] = []
            j = i + 1
            pending_blanks = 0
            while j < n:
                nxt = lines[j]
                if nxt is None:
                    pending_blanks += 1
                    j += 1
                    continue
                nindent, nline = nxt
                if nindent >= indent + 2:
                    block.extend([""] * pending_blanks)
                    pending_blanks = 0
                    # dedent by exactly indent+2 tabs when present
                    strip = min(indent + 2, _indent_of(nline))
                    block.append(nline[strip:])
                    j += 1
                    continue
                break
            if block:
                node.expression = "\n".join(block)
                i = j - 1  # continue scanning from the first non-block line
            else:
                node.expression = ""

        if not rest and not eq_only and _BARE_WORD_RE.match(kind) \
                and parent is not None:
            # bare word: boolean flag on the parent (isHidden, isKey, ...) —
            # but ALSO push as a node so container words (dataAccessOptions)
            # can collect children.
            parent.flags.add(kind)

        if parent is None:
            roots.append(node)
        else:
            parent.children.append(node)
        stack.append((indent, node))
        i += 1
    return roots


# ---------------------------------------------------------------------------
# Model assembly
# ---------------------------------------------------------------------------


def _parse_column_ref(value: str) -> tuple[str, str]:
    """``'Table'.'Column'`` / ``Table.Column`` / ``'T with space'.Col`` ->
    (table, column)."""
    value = value.strip()
    parts: list[str] = []
    buf = ""
    in_quote = False
    i = 0
    while i < len(value):
        ch = value[i]
        if ch == "'":
            if in_quote and i + 1 < len(value) and value[i + 1] == "'":
                buf += "'"
                i += 2
                continue
            in_quote = not in_quote
        elif ch == "." and not in_quote:
            parts.append(buf)
            buf = ""
        else:
            buf += ch
        i += 1
    parts.append(buf)
    if len(parts) != 2:
        raise ValueError(f"Expected 'Table'.'Column' reference, got: {value!r}")
    return parts[0].replace('\\"', '"').replace("\\\\", "\\"), \
        parts[1].replace('\\"', '"').replace("\\\\", "\\")


def _bool_prop(node: TmdlNode, key: str) -> bool:
    if key in node.flags:
        return True
    v = node.props.get(key, "").strip().lower()
    return v == "true"


def _assemble_column(cnode: TmdlNode) -> dict:
    dtype_name = cnode.props.get("dataType", "string")
    amo = TMDL_TO_AMO_TYPE.get(dtype_name, 2)
    col: dict = {
        "name": cnode.name,
        "data_type_amo": amo,
        "data_type": AMO_TO_BUILDER_TYPE.get(amo, "String"),
        "tmdl_data_type": dtype_name,
        "source_column": cnode.props.get("sourceColumn"),
        "is_hidden": _bool_prop(cnode, "isHidden"),
        "is_key": _bool_prop(cnode, "isKey"),
        "format_string": cnode.props.get("formatString"),
        "lineage_tag": cnode.props.get("lineageTag"),
        "data_category": cnode.props.get("dataCategory"),
        "display_folder": cnode.props.get("displayFolder"),
        "sort_by_column": (_unquote(cnode.props["sortByColumn"])
                           if "sortByColumn" in cnode.props else None),
        "summarize_by": (SUMMARIZE_BY_CODES.get(
            cnode.props["summarizeBy"].strip().lower())
            if "summarizeBy" in cnode.props else None),
        "expression": cnode.expression,  # non-None => calculated column
        "extended_properties": _assemble_extended_properties(cnode),
    }
    return col


def _assemble_extended_properties(node: TmdlNode) -> list[dict]:
    out = []
    for ep in node.all("extendedProperty"):
        value = ep.expression if ep.expression is not None else ep.props.get("value", "")
        # JSON block (type 1) when it parses as JSON, else string (type 0)
        ep_type = 1
        try:
            import json as _json
            _json.loads(value)
        except Exception:
            ep_type = 0
        out.append({"name": ep.name, "type": ep_type, "value": value})
    return out


def _assemble_table(tnode: TmdlNode) -> dict:
    table: dict = {
        "name": tnode.name,
        "is_hidden": _bool_prop(tnode, "isHidden"),
        "lineage_tag": tnode.props.get("lineageTag"),
        "columns": [], "measures": [], "partitions": [], "hierarchies": [],
        "extended_properties": _assemble_extended_properties(tnode),
    }
    for cnode in tnode.all("column"):
        table["columns"].append(_assemble_column(cnode))
    for mnode in tnode.all("measure"):
        table["measures"].append({
            "name": mnode.name,
            "expression": mnode.expression or "",
            "format_string": mnode.props.get("formatString"),
            "is_hidden": _bool_prop(mnode, "isHidden"),
            "display_folder": mnode.props.get("displayFolder"),
            "data_category": mnode.props.get("dataCategory"),
            "lineage_tag": mnode.props.get("lineageTag"),
            "extended_properties": _assemble_extended_properties(mnode),
        })
    for pnode in tnode.all("partition"):
        # ``partition 'P' = m`` / ``= calculated`` / ``= entity`` ...; the
        # actual query text sits in the nested ``source =`` node.
        kind = (pnode.expression or "m").strip().lower()
        src = pnode.child("source")
        table["partitions"].append({
            "name": pnode.name,
            "kind": kind if kind in ("m", "calculated", "entity", "query") else "m",
            "mode": pnode.props.get("mode", "import").strip().lower(),
            "source": (src.expression if src is not None and src.expression
                       else (src.props.get("expression", "") if src else "")),
        })
    for hnode in tnode.all("hierarchy"):
        levels = []
        for lnode in hnode.all("level"):
            levels.append({
                "name": lnode.name,
                "column": _unquote(lnode.props.get("column", "")),
                "lineage_tag": lnode.props.get("lineageTag"),
            })
        table["hierarchies"].append({
            "name": hnode.name,
            "is_hidden": _bool_prop(hnode, "isHidden"),
            "lineage_tag": hnode.props.get("lineageTag"),
            "display_folder": hnode.props.get("displayFolder"),
            "levels": levels,
        })
    return table


def _assemble_relationship(rnode: TmdlNode) -> dict:
    ft, fc = _parse_column_ref(rnode.props.get("fromColumn", "."))
    tt, tc = _parse_column_ref(rnode.props.get("toColumn", "."))
    is_active = not (rnode.props.get("isActive", "").strip().lower() == "false")
    cfb = CROSS_FILTER_CODES.get(
        rnode.props.get("crossFilteringBehavior", "").strip().lower(), 1)
    return {
        "name": rnode.name,
        "from_table": ft, "from_column": fc,
        "to_table": tt, "to_column": tc,
        "is_active": is_active,
        "cross_filtering_behavior": cfb,
        "from_cardinality": CARDINALITY_CODES.get(
            rnode.props.get("fromCardinality", "").strip().lower(), 2),
        "to_cardinality": CARDINALITY_CODES.get(
            rnode.props.get("toCardinality", "").strip().lower(), 1),
    }


def assemble_model(nodes: list[TmdlNode]) -> dict:
    """Fold parsed top-level nodes (from one or many documents) into a model
    dict. Multiple calls' outputs can be merged by parse order."""
    model: dict = {
        "name": "Model", "compatibility_level": 1567,
        "culture": None, "default_powerbi_data_source_version": None,
        "discourage_implicit_measures": False, "source_query_culture": None,
        "data_access_options": None,
        "expressions": [], "tables": [], "relationships": [], "roles": [],
    }
    for node in nodes:
        k = node.kind
        if k == "database":
            model["name"] = node.name or model["name"]
            cl = node.props.get("compatibilityLevel")
            if cl and cl.isdigit():
                model["compatibility_level"] = int(cl)
        elif k == "model":
            model["culture"] = node.props.get("culture", model["culture"])
            dsv = node.props.get("defaultPowerBIDataSourceVersion")
            if dsv:
                model["default_powerbi_data_source_version"] = \
                    {"powerBI_V1": 1, "powerBI_V3": 2}.get(dsv)
            if "discourageImplicitMeasures" in node.flags:
                model["discourage_implicit_measures"] = True
            sqc = node.props.get("sourceQueryCulture")
            if sqc:
                model["source_query_culture"] = sqc
            dao = node.child("dataAccessOptions")
            if dao is not None:
                model["data_access_options"] = {
                    "legacyRedirects": "legacyRedirects" in dao.flags,
                    "returnErrorValuesAsNull":
                        "returnErrorValuesAsNull" in dao.flags,
                }
        elif k == "expression":
            model["expressions"].append({
                "name": node.name,
                "expression": node.expression or "",
                "lineage_tag": node.props.get("lineageTag"),
            })
        elif k == "table":
            model["tables"].append(_assemble_table(node))
        elif k == "relationship":
            model["relationships"].append(_assemble_relationship(node))
        elif k == "role":
            perms = []
            for tp in node.all("tablePermission"):
                perms.append({
                    "table": tp.name,
                    "filter_expression": tp.props.get("filterExpression", ""),
                })
            model["roles"].append({
                "name": node.name,
                "model_permission": node.props.get("modelPermission", "read"),
                "table_permissions": perms,
            })
        # anything else (annotations, cultures, changedProperty, ...) ignored
    return model


def parse_tmdl_folder(folder: str) -> dict:
    """Parse a TMDL definition folder into a model dict.

    Reads (when present, all optional): database.tmdl, model.tmdl,
    expressions.tmdl, relationships.tmdl, tables/*.tmdl, roles/*.tmdl.
    Table/role file order is alphabetical (the on-disk order); object order
    WITHIN each document is preserved.
    """
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"TMDL folder not found: {folder}")
    nodes: list[TmdlNode] = []

    def _read(path: str) -> None:
        with open(path, "r", encoding="utf-8-sig") as f:
            nodes.extend(parse_tmdl_document(f.read()))

    for fname in ("database.tmdl", "model.tmdl", "expressions.tmdl",
                  "relationships.tmdl"):
        p = os.path.join(folder, fname)
        if os.path.exists(p):
            _read(p)
    for sub in ("tables", "roles"):
        d = os.path.join(folder, sub)
        if os.path.isdir(d):
            for fname in sorted(os.listdir(d)):
                if fname.lower().endswith(".tmdl"):
                    _read(os.path.join(d, fname))
    model = assemble_model(nodes)
    if not model["tables"]:
        raise ValueError(
            f"No tables found in TMDL folder: {folder} — expected "
            f"tables/*.tmdl files (or a single-document import).")
    return model


def parse_tmdl_string(text: str) -> dict:
    """Parse a single TMDL document (possibly holding database/model/tables/
    relationships/roles all in one) into a model dict."""
    model = assemble_model(parse_tmdl_document(text))
    if not model["tables"]:
        raise ValueError("No tables found in the TMDL document.")
    return model
