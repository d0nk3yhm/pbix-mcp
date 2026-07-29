"""
Calculated Table Evaluator
==========================
Reads ABF metadata to find calculated tables (DATATABLE, GENERATESERIES, CALENDAR, etc.)
and evaluates their DAX expressions to produce table data.

This handles calculated tables that are not materialized in VertiPaq column stores
— they exist only as DAX expressions in the metadata.
"""

import os
import re
import sqlite3
import tempfile
import zipfile
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional


def load_calculated_tables(
    pbix_path: str,
    existing_tables: Dict[str, dict],
    relationships: Optional[List[dict]] = None,
) -> Dict[str, dict]:
    """
    Read ABF metadata from a PBIX file, find all calculated tables,
    evaluate their DAX expressions, and return the combined table dict.

    Args:
        pbix_path: Path to the .pbix file
        existing_tables: Already-loaded tables {name: {columns, rows}}
        relationships: Model relationships list

    Returns:
        Updated tables dict with calculated tables added
    """
    tables = dict(existing_tables)  # Don't modify the original
    db_bytes = None  # Will be set if metadata extraction succeeds

    try:
        from pbix_mcp.formats.abf_rebuild import read_metadata_sqlite
        from pbix_mcp.formats.datamodel_roundtrip import decompress_datamodel

        with zipfile.ZipFile(pbix_path, 'r') as zf:
            dm_data = zf.read('DataModel')
        abf_data = decompress_datamodel(dm_data)
        db_bytes = read_metadata_sqlite(abf_data)

        tmp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        tmp_db.write(db_bytes)
        tmp_db.close()

        try:
            conn = sqlite3.connect(tmp_db.name)
            conn.row_factory = sqlite3.Row

            # Find ALL calculated tables (Partition.Type = 2)
            calc_rows = conn.execute("""
                SELECT t.ID, t.Name, p.QueryDefinition
                FROM [Table] t
                JOIN [Partition] p ON p.TableID = t.ID
                WHERE t.ModelID = 1 AND p.Type = 2 AND p.QueryDefinition IS NOT NULL
            """).fetchall()

            # Build definitions
            calc_defs = {}
            for row in calc_rows:
                tname = row['Name']
                expr = row['QueryDefinition']
                if tname and expr and tname not in tables:
                    # Get columns from metadata
                    col_rows = conn.execute("""
                        SELECT ExplicitName, Expression
                        FROM [Column]
                        WHERE TableID = ? AND ExplicitName IS NOT NULL
                              AND ExplicitName NOT LIKE 'RowNumber%'
                    """, (row['ID'],)).fetchall()

                    columns = [cr['ExplicitName'] for cr in col_rows]
                    calc_cols = [(cr['ExplicitName'], cr['Expression'])
                                 for cr in col_rows if cr['Expression']]

                    calc_defs[tname] = {
                        'expression': expr.strip(),
                        'columns': columns,
                        'calc_columns': calc_cols,
                    }

            conn.close()
        finally:
            os.unlink(tmp_db.name)

        if calc_defs:
            # Topological sort: evaluate tables that others depend on first
            eval_order = _topo_sort(calc_defs, tables)

            # Evaluate each calculated table
            for tname in eval_order:
                try:
                    tdef = calc_defs[tname]
                    expr = tdef['expression']
                    result = _evaluate_table_expression(expr, tname, tdef, tables, relationships)

                    if result:
                        tables[tname] = result
                except Exception:
                    pass  # Skip silently

    except Exception:
        pass  # If ABF reading fails entirely, just return existing tables

    # --- Evaluate calculated columns ---
    # Calculated columns have DAX expressions that are evaluated per-row
    # and added as new columns to existing tables.
    try:
        tables = _evaluate_calculated_columns(tables, db_bytes, relationships)
    except Exception:
        pass

    return tables


def _evaluate_calculated_columns(
    tables: Dict[str, dict],
    db_bytes: bytes,
    relationships: List[dict],
) -> Dict[str, dict]:
    """Evaluate calculated columns and add them to their parent tables.

    Calculated columns have a DAX expression in the Column.Expression field.
    They are evaluated per-row, with each row's values available as column references.
    """
    if not db_bytes:
        return tables

    tmp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    tmp_db.write(db_bytes)
    tmp_db.close()

    try:
        conn = sqlite3.connect(tmp_db.name)
        conn.row_factory = sqlite3.Row

        # Find all calculated columns
        calc_cols = conn.execute("""
            SELECT c.ExplicitName, c.Expression, t.Name as TableName
            FROM [Column] c
            JOIN [Table] t ON c.TableID = t.ID
            WHERE c.Expression IS NOT NULL AND c.Expression != ''
              AND c.ExplicitName IS NOT NULL
              AND c.ExplicitName NOT LIKE 'RowNumber%'
              AND t.ModelID = 1
        """).fetchall()

        conn.close()
    finally:
        os.unlink(tmp_db.name)

    if not calc_cols:
        return tables

    from pbix_mcp.dax import engine as dax_engine

    for cc in calc_cols:
        col_name = cc['ExplicitName']
        expr = cc['Expression'].strip()
        table_name = cc['TableName']

        tbl = tables.get(table_name)
        if not tbl:
            continue

        # Skip if column already exists
        if col_name in tbl['columns']:
            continue

        # Strip comments from expression
        clean_expr = strip_dax_comments(expr)
        clean_expr = re.sub(r'//[^\n]*', '', clean_expr)
        clean_expr = clean_expr.strip()

        # Evaluate per-row: for each row, set up a row context and evaluate
        engine = dax_engine.DAXEngine()
        new_values = []

        for row in tbl['rows']:
            # Create a row context: table[column] references resolve to this row's values
            row_data = {}
            for ci, cn in enumerate(tbl['columns']):
                row_data[cn] = row[ci]

            # Evaluate expression with row context
            try:
                # Replace table[column] references with the row's actual values
                row_expr = clean_expr
                for cn, val in row_data.items():
                    # Replace 'TableName'[ColumnName] and TableName[ColumnName]
                    patterns = [
                        f"'{table_name}'[{cn}]",
                        f"{table_name}[{cn}]",
                    ]
                    for pat in patterns:
                        if pat in row_expr:
                            if isinstance(val, str):
                                row_expr = row_expr.replace(pat, f'"{val}"')
                            elif val is None:
                                row_expr = row_expr.replace(pat, 'BLANK()')
                            else:
                                row_expr = row_expr.replace(pat, str(val))

                ctx = dax_engine.DAXContext(tables, {}, None, None, None, relationships or [])
                result = engine._eval_expr(row_expr, ctx)
                new_values.append(result)
            except Exception:
                new_values.append(None)

        # Add the calculated column to the table
        tbl['columns'].append(col_name)
        for i, row in enumerate(tbl['rows']):
            row.append(new_values[i] if i < len(new_values) else None)

    return tables


# ---------------------------------------------------------------------------
# Calculated-column AUTHORING support (reliability gate + row-context evaluator)
# ---------------------------------------------------------------------------
# The per-row evaluator below substitutes a row's same-table column values into
# the expression and evaluates it. That is correct ONLY for row-context
# expressions over the column's OWN table. It SILENTLY produces wrong values for
# aggregations (SUM(...) -> 0), context transition (CALCULATE), and relationship
# navigation (RELATED) — none of which have a real row context here. So before
# materializing a calculated column we gate the expression: same-table refs
# only, and none of these functions. Anything else is refused, never stored
# wrong (matches Power BI Desktop, which computes these server-side).

# A plain aggregate over the target table's OWN column is deterministic in a
# calculated column: a calc column has no filter context beyond its own row, so
# MIN('Date'[Year]) really is the minimum of the whole column, every row. These
# are allowed -- but ONLY when no filter-context function appears anywhere in
# the expression, because inside CALCULATE the same call means something else.
#
# Deliberately SMALL. Every name here has been checked against Power BI
# Desktop's own engine; a function is not added until its result matches.
# Excluded on purpose:
#   COUNTROWS  -- takes a TABLE, not a column, so the "own column" argument
#                 check below cannot vet it and COUNTROWS(OtherTable) would
#                 silently return 0;
#   DISTINCTCOUNT / COUNT / COUNTA / COUNTBLANK -- our implementations disagree
#                 with DAX on how BLANK and the empty string are counted
#                 (DISTINCTCOUNT drops BLANK, i.e. it is really
#                 DISTINCTCOUNTNOBLANK), so they would be off by one;
#   MEDIAN / PRODUCT / GEOMEAN / STDEV / VAR -- simply not verified yet.
_CALC_SELF_AGG_FUNCS = frozenset({
    "sum", "average", "min", "max",
})

# Iterators are NOT in the set above: they take a table argument and a row
# expression, so reproducing them needs a real iteration context.
#
# Statistical functions are spelled with a DOT in DAX (STDEV.S, VAR.P,
# PERCENTILE.INC, STDEVX.P, PERCENTILEX.INC). Listing only the underscore
# spellings left the real names matching neither set, so they were refused by
# nothing -- and `_CALC_FUNC_RE` accepts dots, so both spellings are listed.
_CALC_CONTEXT_FUNCS = frozenset({
    "sumx", "averagex", "minx", "maxx", "countx", "countax", "medianx",
    "percentile", "percentilex", "percentile_inc", "percentile_exc",
    "percentile.inc", "percentile.exc", "percentilex.inc", "percentilex.exc",
    "productx", "geomeanx", "stdevx_s", "stdevx_p", "varx_s", "varx_p",
    "stdevx.s", "stdevx.p", "varx.s", "varx.p",
    # Not allowed above, so they must be refused explicitly.
    "countrows", "count", "counta", "countblank", "distinctcount",
    "distinctcountnoblank", "median", "product", "geomean", "averagea",
    "mina", "maxa", "stdev_s", "stdev_p", "var_s", "var_p",
    "stdev.s", "stdev.p", "var.s", "var.p",
    "rank", "rankx", "topn", "sample", "concatenatex",
    # context transition / filter / relationship / table iterators
    "related", "relatedtable", "calculate", "calculatetable", "earlier",
    "earliest", "all", "allexcept", "allselected", "allnoblankrow",
    "allcrossfiltered", "removefilters", "filter", "values", "distinct",
    "selectedvalue", "lookupvalue", "keepfilters", "summarize",
    "summarizecolumns", "addcolumns", "groupby", "crossjoin", "union", "except",
    "intersect", "generate", "generateall", "naturalinnerjoin",
    "naturalleftouterjoin", "selectcolumns", "treatas", "userelationship",
    "crossfilter", "datatable", "row", "path", "pathitem", "isinscope",
    "isfiltered", "iscrossfiltered", "hasonevalue", "hasonefilter",
})
_CALC_UNSAFE_FUNCS = _CALC_SELF_AGG_FUNCS | _CALC_CONTEXT_FUNCS
_CALC_REF_RE = re.compile(r"(?:'([^']+)'|([A-Za-z_][A-Za-z0-9_]*))\s*\[")
_CALC_FUNC_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(")

# DAX reserved words that can legally sit immediately before a column
# reference. `VAR _x = 1 RETURN [Col] * _x` is a SAME-table expression, but the
# regex above reads the bare word before `[` as a table name and reported
# "references another table 'RETURN'" -- refusing the single most common modern
# DAX idiom with a nonsense reason.
#
# Only the BARE branch is excluded. DAX requires a table whose name is a
# reserved word to be quoted, so a real table named Return is written
# 'Return'[Col] and still matches the quoted branch. That asymmetry is what
# makes this safe: skipping a bare keyword can never hide a genuine cross-table
# reference, which would turn a refusal into a silently wrong value.
_DAX_RESERVED_BEFORE_REF = frozenset({"var", "return", "not", "in"})


_LOOKUPVALUE_RE = re.compile(r"\bLOOKUPVALUE\s*\(", re.I)
_COL_REF_FULL_RE = re.compile(r"^(?:'([^']+)'|([A-Za-z_]\w*))?\s*\[([^\]]+)\]$")
_LV_MASK = "__LV{}__"
_LV_MASK_RE = re.compile(r"__LV(\d+)__")


def _scan_calls(expr: str, name_re: "re.Pattern[str]"):
    """Yield (start, end) of every call matching `name_re` in `expr`.

    Generalises `_scan_aggregate_calls`. Walks the SHADOW to the matching close
    paren so a paren inside a string literal or a column name cannot unbalance
    the count.
    """
    shadow = _agg_shadow(expr)
    i = 0
    while True:
        m = name_re.search(shadow, i)
        if not m:
            return
        depth, j = 0, m.end() - 1
        while j < len(shadow):
            ch = shadow[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    j += 1
                    break
            j += 1
        yield m.start(), j
        i = j


def _split_args(argstr: str) -> "list[str]":
    """Split a call's argument text on TOP-LEVEL commas.

    Splits on the shadow so a comma inside a string literal or a bracketed
    column name (`[Rate, net]` is a legal column name) is not a separator.
    """
    shadow = _agg_shadow(argstr)
    parts, depth, start = [], 0, 0
    for k, ch in enumerate(shadow):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(argstr[start:k])
            start = k + 1
    parts.append(argstr[start:])
    return [p.strip() for p in parts]


def _parse_column_ref(text: str, target_table: str, target_columns):
    """(table, column) for a lone column reference, or None if not one.

    A BARE `[Col]` resolves to the target table only when that table really has
    the column. Guessing otherwise would silently look the value up in the
    wrong table -- `LOOKUPVALUE([Index], [id], [parentId])` is a legal
    self-lookup, but the same shape naming a column the target lacks means the
    author meant some other table and we must refuse instead.
    """
    m = _COL_REF_FULL_RE.match(text.strip())
    if not m:
        return None
    tbl, col = (m.group(1) or m.group(2)), m.group(3)
    if tbl:
        return tbl, col
    if target_columns is not None and col not in target_columns:
        return None
    return target_table, col


def _parse_lookupvalue(call: str, target_table: str, target_columns,
                       known_tables):
    """Parse one LOOKUPVALUE call into a spec, or return a refusal string.

    LOOKUPVALUE(<result>, <search1>, <value1>, [<searchN>, <valueN>...]
                [, <alternateResult>]) -- an ODD argument count has no
    alternate result, an EVEN one ends with it.

    Every result/search column must live on ONE table, and that table's data
    must be available; the values are row-context expressions over the target
    table. Anything else is refused rather than guessed.
    """
    open_paren = call.find("(")
    args = _split_args(call[open_paren + 1:-1])
    if len(args) < 3:
        return "LOOKUPVALUE needs at least a result column, a search column and a value"
    alt = None
    if len(args) % 2 == 0:
        alt = args[-1]
        args = args[:-1]
    res = _parse_column_ref(args[0], target_table, target_columns)
    if not res:
        return (f"LOOKUPVALUE's result argument {args[0]!r} is not a column "
                f"reference this engine can resolve")
    lv_table, result_col = res
    pairs = []
    for k in range(1, len(args), 2):
        sc = _parse_column_ref(args[k], target_table, target_columns)
        if not sc:
            return (f"LOOKUPVALUE's search argument {args[k]!r} is not a "
                    f"column reference this engine can resolve")
        if sc[0].lower() != lv_table.lower():
            return (f"LOOKUPVALUE mixes tables: result is on '{lv_table}' but "
                    f"'{sc[1]}' is on '{sc[0]}'")
        pairs.append((sc[1], args[k + 1]))
    if known_tables is not None:
        avail = {t.lower(): cols for t, cols in known_tables.items()}
        cols = avail.get(lv_table.lower())
        if cols is None:
            return (f"LOOKUPVALUE reads table '{lv_table}', whose data is not "
                    f"available to this engine")
        have = {c.lower() for c in cols}
        for cn in [result_col] + [p[0] for p in pairs]:
            if cn.lower() not in have:
                return (f"LOOKUPVALUE references '{lv_table}'[{cn}], which "
                        f"that table does not have")
    return {"table": lv_table, "result": result_col, "pairs": pairs,
            "alt": alt}


def _mask_lookupvalue_calls(expr: str, target_table: str, target_columns,
                            known_tables):
    """(masked_expr, specs, reason).

    Replaces each supported LOOKUPVALUE call with a `__LV<n>__` placeholder --
    no brackets, no parens -- so the caller's cross-table reference scan and
    function scan do not see the lookup table or the LOOKUPVALUE name itself.
    The value expressions are pulled OUT of the mask and validated separately
    by the caller, so masking never smuggles an unsupported sub-expression past
    the gate.
    """
    spans = list(_scan_calls(expr, _LOOKUPVALUE_RE))
    if not spans:
        return expr, [], None
    out: "list[str]" = []
    specs: "list[dict]" = []
    i = 0
    for a, b in spans:
        spec = _parse_lookupvalue(expr[a:b], target_table, target_columns,
                                  known_tables)
        if isinstance(spec, str):
            return expr, [], spec
        out.append(expr[i:a])
        out.append(_LV_MASK.format(len(specs)))
        specs.append(spec)
        i = b
    out.append(expr[i:])
    return "".join(out), specs, None


# ---------------------------------------------------------------------------
# CALCULATE(<aggregate>, FILTER(<own table>, <predicate>)) in a row context
# ---------------------------------------------------------------------------
# In a calculated column, CALCULATE first performs CONTEXT TRANSITION -- the
# current row becomes a filter on every column of its table. A FILTER over that
# SAME table, passed as CALCULATE's filter argument, then REPLACES that filter
# entirely. So `CALCULATE(SUM(T[x]), FILTER(T, cond))` and
# `CALCULATE(SUM(T[x]), FILTER(ALL(T), cond))` both reduce to "aggregate over
# every row of T satisfying cond" -- the current row does not restrict it.
# That equivalence is what makes this tractable, and it is confirmed against
# Desktop's own stored values, not assumed (see tests).
#
# Doing it literally would be O(rows^2): MS_Covid_Tracking's COVID table has
# 1,740,185 rows, so a per-row table scan is ~3e12 operations. Instead the
# predicate is compiled into either a hash index (equality terms) or a prefix
# aggregate over a sorted key (one inequality term), both of which answer every
# row in one lookup.
_CALCULATE_RE = re.compile(r"\bCALCULATE\s*\(", re.I)
_FILTER_CALL_RE = re.compile(r"^\s*FILTER\s*\(", re.I)
_ALL_CALL_RE = re.compile(r"^\s*ALL\s*\(\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*\)\s*$",
                          re.I)
_BARE_TABLE_RE = re.compile(r"^\s*(?:'([^']+)'|([A-Za-z_]\w*))\s*$")
_CALC_MASK = "__CALC{}__"
_VAR_DEF_RE = re.compile(r"\bVAR\s+([A-Za-z_]\w*)\s*=", re.I)
_RETURN_KW_RE = re.compile(r"\bRETURN\b", re.I)
_EARLIER_CALL_RE = re.compile(r"\bEARLIER\s*\(\s*((?:'[^']+'|\w+)?\[[^\]]+\])\s*\)",
                              re.I)
# Longest first so "<=" is not read as "<" then "=".
_CMP_OPS = ("<=", ">=", "<>", "=", "<", ">")
_CALC_AGGS = {"sum", "min", "max", "average", "count", "counta",
              "countrows", "distinctcount"}
# Only the DAY interval: MONTH/QUARTER/YEAR carry end-of-month rules that
# have not been checked against Desktop, so they stay refused.
_DATEADD_DAY_RE = re.compile(
    r"^\(*\s*DATEADD\s*\(\s*(.+?)\s*,\s*(-?\d+)\s*,\s*DAY\s*\)\s*\)*$",
    re.I | re.S)

_RELATED_RE = re.compile(r"\bRELATED\s*\(", re.I)
_REL_MASK = "__REL{}__"


def _parse_calculate(call: str, target_table: str, target_columns,
                     var_list):
    """Parse CALCULATE(<agg>, FILTER(<own table>, <pred>)) into a spec.

    Returns a spec dict or a refusal string. Deliberately narrow: exactly one
    filter argument, over the column's OWN table, with a predicate that is a
    conjunction of comparisons whose left side is one of that table's columns.
    Anything else -- KEEPFILTERS, a second filter, an aggregate over another
    table, an OR, a computed left side -- is refused, because the whole point
    of this gate is that a shape we cannot reproduce exactly never materialises.
    """
    open_paren = call.find("(")
    args = _split_args(call[open_paren + 1:-1])
    if len(args) != 2:
        return ("CALCULATE with %d argument(s) is not supported here; only "
                "CALCULATE(<aggregate>, FILTER(...)) is" % len(args))
    agg_text, filt_text = args[0], args[1]

    agg_open = agg_text.find("(")
    if agg_open < 0 or not agg_text.rstrip().endswith(")"):
        return f"CALCULATE's first argument {agg_text!r} is not an aggregate"
    agg_name = agg_text[:agg_open].strip().lower()
    if agg_name not in _CALC_AGGS:
        return (f"CALCULATE over '{agg_name.upper()}' is not supported here; "
                f"only {', '.join(sorted(a.upper() for a in _CALC_AGGS))} are")
    agg_arg = agg_text[agg_open + 1:agg_text.rstrip().rfind(")")].strip()
    agg_col = None
    if agg_name == "countrows":
        m = _BARE_TABLE_RE.match(agg_arg)
        if not m or (m.group(1) or m.group(2)).lower() != target_table.lower():
            return "COUNTROWS must name this column's own table"
    else:
        ref = _parse_column_ref(agg_arg, target_table, target_columns)
        if not ref:
            return (f"{agg_name.upper()}({agg_arg}) is not a single column of "
                    f"'{target_table}'")
        if ref[0].lower() != target_table.lower():
            return (f"{agg_name.upper()} aggregates '{ref[0]}', not this "
                    f"column's own table '{target_table}'")
        agg_col = ref[1]

    if not _FILTER_CALL_RE.match(filt_text):
        return ("CALCULATE's filter argument must be a FILTER(...) over this "
                "column's own table")
    f_open = filt_text.find("(")
    f_args = _split_args(filt_text[f_open + 1:filt_text.rstrip().rfind(")")])
    if len(f_args) != 2:
        return "FILTER takes a table and a predicate"
    tbl_text, pred = f_args
    m_all = _ALL_CALL_RE.match(tbl_text)
    m_bare = _BARE_TABLE_RE.match(tbl_text)
    named = ((m_all.group(1) or m_all.group(2)) if m_all
             else (m_bare.group(1) or m_bare.group(2)) if m_bare else None)
    if not named or named.lower() != target_table.lower():
        return (f"FILTER must scan this column's own table '{target_table}'; "
                f"{tbl_text.strip()!r} is not supported here")

    if _split_top_level(pred, "||") != [pred.strip()]:
        return "FILTER predicates combined with || are not supported here"
    eq_terms, ineq = [], None
    for term in _split_top_level(pred, "&&"):
        parts = _split_comparison(term)
        if not parts:
            return (f"FILTER predicate term {term.strip()!r} is not a simple "
                    f"comparison this engine can compile")
        lhs, op, rhs = parts
        ref = _parse_column_ref(lhs, target_table, target_columns)
        if not ref or ref[0].lower() != target_table.lower():
            return (f"FILTER predicate compares {lhs.strip()!r}, which is not "
                    f"a column of '{target_table}'")
        rhs_inlined = _inline_vars(rhs, var_list)
        # EARLIER(<col>) means the OUTER row -- exactly what the right-hand
        # side is evaluated against here, so it reduces to the column itself.
        rhs_inlined = _EARLIER_CALL_RE.sub(lambda m: m.group(1), rhs_inlined)
        if op in ("=", "=="):
            eq_terms.append((ref[1], rhs_inlined))
        elif op == "<>":
            return "FILTER with <> is not supported here"
        else:
            if ineq is not None:
                return ("FILTER with more than one inequality is not "
                        "supported here")
            ineq = (ref[1], op, rhs_inlined)
    return {"agg": agg_name, "agg_col": agg_col, "eq": eq_terms, "ineq": ineq}


def _mask_calculate_calls(expr: str, target_table: str, target_columns,
                          var_list):
    """(masked_expr, specs, reason) -- see _mask_lookupvalue_calls."""
    spans = list(_scan_calls(expr, _CALCULATE_RE))
    if not spans:
        return expr, [], None
    out: "list[str]" = []
    specs: "list[dict]" = []
    i = 0
    for a, b in spans:
        spec = _parse_calculate(expr[a:b], target_table, target_columns,
                                var_list)
        if isinstance(spec, str):
            return expr, [], spec
        out.append(expr[i:a])
        out.append(_CALC_MASK.format(len(specs)))
        specs.append(spec)
        i = b
    out.append(expr[i:])
    return "".join(out), specs, None


def _replace_identifier(text: str, name: str, repl: str) -> str:
    """Replace whole-word `name` outside string literals and bracketed names."""
    shadow = _agg_shadow(text)
    pat = re.compile(r"\b" + re.escape(name) + r"\b")
    out, i = [], 0
    for m in pat.finditer(shadow):
        out.append(text[i:m.start()])
        out.append(repl)
        i = m.end()
    out.append(text[i:])
    return "".join(out)


def _extract_top_level_vars(expr: str):
    """([(name, definition)], body) for `VAR a = .. VAR b = .. RETURN body`.

    Only DEPTH-0 definitions: a VAR inside a nested call belongs to that call's
    own scope and must not be hoisted out of it.
    """
    shadow = _agg_shadow(expr)
    marks = []
    depth, i = 0, 0
    while i < len(shadow):
        ch = shadow[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            m = _VAR_DEF_RE.match(shadow, i)
            if m:
                marks.append(("VAR", m.start(), m.end(), m.group(1)))
                i = m.end()
                continue
            m2 = _RETURN_KW_RE.match(shadow, i)
            if m2:
                marks.append(("RETURN", m2.start(), m2.end(), None))
                i = m2.end()
                continue
        i += 1
    if not marks:
        return [], expr
    out = []
    for k, (kind, start, end, name) in enumerate(marks):
        if kind != "VAR":
            continue
        stop = marks[k + 1][1] if k + 1 < len(marks) else len(expr)
        out.append((name, expr[end:stop].strip()))
    ret = [m for m in marks if m[0] == "RETURN"]
    body = expr[ret[-1][2]:] if ret else expr
    return out, body


def _inline_vars(text: str, var_list) -> str:
    """`text` with every VAR reference replaced by its definition.

    DAX VARs are scoped in order, so each definition is resolved against the
    ones before it first. Inlining lets a CALCULATE's predicate be compiled
    into a plain function of the outer row without reimplementing scoping.
    """
    resolved: "list[tuple[str, str]]" = []
    for name, defn in var_list:
        d = defn
        for pname, pdef in resolved:
            d = _replace_identifier(d, pname, f"({pdef})")
        resolved.append((name, d))
    out = text
    for name, d in resolved:
        out = _replace_identifier(out, name, f"({d})")
    return out


def _split_top_level(text: str, sep: str):
    """Split on a top-level operator, using the shadow."""
    shadow = _agg_shadow(text)
    parts, depth, start, k = [], 0, 0, 0
    while k < len(shadow):
        ch = shadow[k]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0 and shadow.startswith(sep, k):
            parts.append(text[start:k])
            k += len(sep)
            start = k
            continue
        k += 1
    parts.append(text[start:])
    return [p.strip() for p in parts]


def _split_comparison(term: str):
    """(lhs, op, rhs) for a top-level comparison, or None."""
    shadow = _agg_shadow(term)
    depth = 0
    for k, ch in enumerate(shadow):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif depth == 0:
            for op in _CMP_OPS:
                if shadow.startswith(op, k):
                    # "=" inside "<=" / ">=" / "<>" is handled by ordering.
                    return (term[:k].strip(), op,
                            term[k + len(op):].strip())
    return None


def _related_paths(target_table: str, dest_table: str, relationships,
                   limit: int = 8):
    """Every many-to-one chain of ACTIVE relationships from target to dest.

    RELATED only ever walks from the MANY side to the ONE side, which in AMO
    metadata is From -> To. Returns a list of hop lists; the caller refuses
    unless there is exactly one, because two paths mean the DAX is ambiguous
    without USERELATIONSHIP and picking one would silently invent values.
    """
    edges: "dict[str, list[dict]]" = {}
    for r in relationships or []:
        if not r.get("IsActive", 1):
            continue
        edges.setdefault((r.get("FromTable") or "").lower(), []).append(r)
    found: "list[list[dict]]" = []
    stack: "list[tuple[str, list[dict], set[str]]]" = [
        ((target_table or "").lower(), [], {(target_table or "").lower()})]
    while stack:
        node, path, seen = stack.pop()
        if len(path) >= limit:
            continue
        for r in edges.get(node, []):
            nxt = (r.get("ToTable") or "").lower()
            if nxt in seen:
                continue
            if nxt == (dest_table or "").lower():
                found.append(path + [r])
                if len(found) > 1:
                    return found
            else:
                stack.append((nxt, path + [r], seen | {nxt}))
    return found


def _parse_related(call: str, target_table: str, target_columns):
    """(dest_table, dest_column) for RELATED(<one column ref>), or a reason.

    The column MUST name its table. A bare RELATED([Col]) would have to be
    resolved by searching every related table for that name, and a wrong guess
    stores wrong values on every row -- so it is refused instead.
    """
    open_paren = call.find("(")
    args = _split_args(call[open_paren + 1:-1])
    if len(args) != 1:
        return "RELATED takes exactly one column reference"
    m = _COL_REF_FULL_RE.match(args[0].strip())
    if not m:
        return f"RELATED's argument {args[0]!r} is not a column reference"
    tbl, col = (m.group(1) or m.group(2)), m.group(3)
    if not tbl:
        return (f"RELATED([{col}]) does not name its table; this engine will "
                f"not guess which related table it means")
    if tbl.lower() == (target_table or "").lower():
        return (f"RELATED('{tbl}'[{col}]) points at the column's own table, "
                f"which has no related row to fetch")
    return tbl, col


def _mask_related_calls(expr: str, target_table: str, target_columns,
                        known_tables, relationships):
    """(masked_expr, specs, reason) -- see _mask_lookupvalue_calls."""
    spans = list(_scan_calls(expr, _RELATED_RE))
    if not spans:
        return expr, [], None
    out: "list[str]" = []
    specs: "list[dict]" = []
    i = 0
    for a, b in spans:
        got = _parse_related(expr[a:b], target_table, target_columns)
        if isinstance(got, str):
            return expr, [], got
        dest_table, dest_col = got
        if known_tables is not None:
            avail = {t.lower(): cols for t, cols in known_tables.items()}
            cols = avail.get(dest_table.lower())
            if cols is None:
                return expr, [], (
                    f"RELATED reads table '{dest_table}', whose data is not "
                    f"available to this engine")
            if dest_col.lower() not in {c.lower() for c in cols}:
                return expr, [], (
                    f"RELATED references '{dest_table}'[{dest_col}], which "
                    f"that table does not have")
        paths = _related_paths(target_table, dest_table, relationships)
        if not paths:
            return expr, [], (
                f"RELATED('{dest_table}'[{dest_col}]): no active many-to-one "
                f"relationship path from '{target_table}' to '{dest_table}'")
        if len(paths) > 1:
            return expr, [], (
                f"RELATED('{dest_table}'[{dest_col}]): more than one active "
                f"relationship path from '{target_table}' -- ambiguous without "
                f"USERELATIONSHIP, so it is refused rather than guessed")
        out.append(expr[i:a])
        out.append(_REL_MASK.format(len(specs)))
        specs.append({"table": dest_table, "column": dest_col,
                      "path": paths[0]})
        i = b
    out.append(expr[i:])
    return "".join(out), specs, None


def related_table_names(expression: str) -> "set[str]":
    """Tables a RELATED in `expression` reads, for lazy loading. See
    `lookupvalue_table_names` -- best effort, the gate re-validates."""
    out: "set[str]" = set()
    e = strip_dax_comments(expression or "")
    for a, b in _scan_calls(e, _RELATED_RE):
        call = e[a:b]
        args = _split_args(call[call.find("(") + 1:-1])
        if len(args) == 1:
            m = _COL_REF_FULL_RE.match(args[0].strip())
            if m and (m.group(1) or m.group(2)):
                out.add(m.group(1) or m.group(2))
    return out


def lookupvalue_table_names(expression: str) -> "set[str]":
    """Table names a LOOKUPVALUE in `expression` reads from.

    Best-effort and deliberately un-validating: it exists so a caller can DECIDE
    WHICH TABLES TO LOAD before the gate runs. The gate re-parses properly and
    refuses anything that does not hold up, so a wrong guess here costs a
    needless table read, never a wrong value.
    """
    out: "set[str]" = set()
    e = strip_dax_comments(expression or "")
    for a, b in _scan_calls(e, _LOOKUPVALUE_RE):
        call = e[a:b]
        args = _split_args(call[call.find("(") + 1:-1])
        for arg in args:
            m = _COL_REF_FULL_RE.match(arg.strip())
            if m and (m.group(1) or m.group(2)):
                out.add(m.group(1) or m.group(2))
    return out


def calc_column_unsupported_reason(expression: str, target_table: str,
                                   columns=None, known_tables=None,
                                   relationships=None):
    """Return why a calc-column expression can't be safely materialized, or None.

    None = the expression is a row-context expression over ``target_table``'s
    own columns that our per-row evaluator can reproduce faithfully. A non-None
    string is a human-readable reason the caller should REFUSE with (rather than
    store silently-wrong values).

    ``columns`` is the target table's column names when the caller knows them.
    It lets an aggregate over a misspelled column be refused instead of quietly
    answering 0; callers without the list simply lose that one check.

    ``known_tables`` maps table name -> column names for the tables whose DATA
    the caller can supply. It is what makes LOOKUPVALUE supportable: without it
    every cross-table read is still refused, so a caller that cannot produce
    other tables' rows keeps the old, strictly-safe behaviour.
    """
    e = expand_variation_accessors(strip_dax_comments(expression))
    if not e.strip():
        return "expression is empty"
    if known_tables is not None:
        # CALCULATE(<agg>, FILTER(<own table>, <pred>)) is compiled, not
        # iterated. Masked first so its predicate's own column references are
        # not mistaken for the outer expression's.
        _vars, _body = _extract_top_level_vars(e)
        e, calc_specs, calc_why = _mask_calculate_calls(
            e, target_table, columns, _vars)
        if calc_why:
            return calc_why
        if calc_specs and columns is None:
            return ("CALCULATE needs this table's column list to compile its "
                    "filter, which this caller did not supply")
        # RELATED needs the relationship graph as well as the data. Without it
        # the path cannot be resolved, so the call stays refused by the
        # cross-table scan below rather than guessed at.
        if relationships is not None:
            e, rel_specs, rel_why = _mask_related_calls(
                e, target_table, columns, known_tables, relationships)
            if rel_why:
                return rel_why
            for spec in rel_specs:
                fk = spec["path"][0].get("FromColumn")
                if columns is not None and fk not in columns:
                    return (f"RELATED('{spec['table']}'[{spec['column']}]) "
                            f"joins on '{target_table}'[{fk}], which this "
                            f"table does not have")
        e, lv_specs, lv_why = _mask_lookupvalue_calls(
            e, target_table, columns, known_tables)
        if lv_why:
            return lv_why
        # The masked-out value expressions still have to hold up on their own:
        # they are evaluated in the target table's row context, so anything the
        # gate would refuse there must be refused here too.
        for spec in lv_specs:
            for _sc, val_expr in spec["pairs"]:
                why = calc_column_unsupported_reason(
                    val_expr, target_table, columns, known_tables)
                if why:
                    return f"inside LOOKUPVALUE: {why}"
            if spec["alt"]:
                why = calc_column_unsupported_reason(
                    spec["alt"], target_table, columns, known_tables)
                if why:
                    return f"inside LOOKUPVALUE: {why}"
    for quoted, bare in _CALC_REF_RE.findall(e):
        if not quoted and bare.lower() in _DAX_RESERVED_BEFORE_REF:
            continue
        tbl = quoted or bare
        if tbl.lower() != target_table.lower():
            return (
                f"references another table '{tbl}'. Only expressions over "
                f"'{target_table}''s own columns are supported — cross-table "
                f"navigation (RELATED/relationships) is computed by the "
                f"service, not this engine."
            )
    # Name every offending function, in the order they appear. Reporting only
    # the first alphabetically told the author about ALL when what they wrote
    # was RANKX.
    # Scan the SHADOW so a function name occurring inside a string or inside a
    # COLUMN NAME is not read as a call: a column legitimately named
    # "Total Count (n)" was refused for "using COUNT".
    seen: list[str] = []
    for fn in _CALC_FUNC_RE.findall(_agg_shadow(e)):
        up = fn.upper()
        if fn.lower() in _CALC_CONTEXT_FUNCS and up not in seen:
            seen.append(up)
    if seen:
        return (
            f"uses {', '.join(repr(f) for f in seen)}, which need a table scan "
            f"/ filter context this engine can't evaluate per-row. CALCULATE, "
            f"RELATED, the X-iterators and table functions are not supported "
            f"in authored calculated columns."
        )

    # Each allowed aggregate must take exactly ONE column of this table.
    # `MIN(x, y)` is DAX's scalar overload, not an aggregate at all, and
    # `COUNTROWS(Other)` names a table no other check here can see -- both
    # would otherwise evaluate with no row context and store 0 on every row.
    # When `columns` is known, the column must also exist: `MIN([Yeer])` is a
    # typo the engine would answer 0 to rather than fail.
    for call in _iter_aggregate_calls(e):
        arg = _agg_column_arg(call)
        fname = call[:call.find("(")].strip().upper()
        if arg is None:
            # MIN/MAX also have a SCALAR overload, MIN(<expr1>, <expr2>). With
            # no column reference anywhere inside it there is nothing for the
            # row substitution to reach and nothing to mask, so it is just
            # arithmetic over literals and VARs -- which is how Power BI's own
            # binning clamps a bin number: MIN(__BinNumber, __Count - 1).
            # A column reference inside it is the dangerous case and still goes.
            if not _refs_any_column(call):
                continue
            return (
                f"'{fname}' is used with something other than a single column "
                f"of '{target_table}'. Only the whole-column form, e.g. "
                f"{fname}('{target_table}'[Column]), is supported — the "
                f"two-argument scalar form over columns and table arguments "
                f"are not."
            )
        tbl, col = arg
        if tbl and tbl.lower() != target_table.lower():
            return (
                f"'{fname}' aggregates a column of another table '{tbl}'. Only "
                f"'{target_table}''s own columns are supported."
            )
        if columns is not None and col not in columns:
            return (
                f"'{fname}' aggregates '[{col}]', which is not a column of "
                f"'{target_table}'."
            )
    # Plain aggregates survive only on their own. Inside a filter-context
    # function the same MIN means something different, and that case is
    # already refused by the loop above.
    return None


# A column reference: optional 'Table' or Table qualifier then [Name]. Applied
# only after string literals are blanked, so a bracket inside "text [like this]"
# is never mistaken for one.
_COLUMN_REF = re.compile(r"(?:'[^']+'|\b\w+)?\[[^\]]+\]")
_STRING_LITERAL = re.compile(r'"(?:[^"]|"")*"')

# `'Fact'[EstimatedCloseDate].[Date]` — the auto date/time VARIATION accessor.
# A `Variation` metadata row links the column to a hidden LocalDateTable through
# a relationship, and `.[Date]` reads that table's Date column. Because the
# relationship joins on the date itself, the value is simply the date part of
# the column, needing no traversal at evaluation time.
#
# VERIFIED against Power BI Desktop's own stored values for
# test_corpus/MS_Revenue_Opportunities.pbix `Fact[Date]`: 458/458 rows equal the
# date part of `Fact[EstimatedCloseDate]`.
_VARIATION_ACCESSOR = re.compile(
    r"((?:'[^']+'|\b\w+)?\[[^\]]+\])\s*\.\s*\[([^\]]+)\]")

# Only `.[Date]` is verified. The other parts map to auto-date TEMPLATE columns
# whose values are locale-dependent display strings ("January", "Qtr 1"), so
# they are deliberately left unexpanded — the unresolved-reference check then
# refuses them rather than guessing.
_VARIATION_VERIFIED = {"date"}


def expand_variation_accessors(expr: str) -> str:
    """Rewrite `X.[Date]` to the date part of X, using verified primitives only.

    DATE/YEAR/MONTH/DAY are all separately checked against Desktop, so the
    expansion introduces no new unverified behaviour and drops any time
    component exactly as the LocalDateTable's Date column does.
    """
    def _sub(m: "re.Match[str]") -> str:
        col, part = m.group(1), m.group(2)
        if part.strip().lower() not in _VARIATION_VERIFIED:
            return m.group(0)
        return f"DATE(YEAR({col}), MONTH({col}), DAY({col}))"

    return _VARIATION_ACCESSOR.sub(_sub, expr or "")


def _strip_strings(expr: str) -> str:
    return _STRING_LITERAL.sub('""', expr or "")


def _unresolved_refs(row_expr: str) -> set:
    """Column references still present after per-row substitution.

    Every reference to the target table's own columns has been replaced by a
    literal by this point, so whatever is left points at another table or at
    nothing. Both evaluate to blank rather than raising, which is exactly how a
    calculated column ends up materialized with wrong values.
    """
    return set(_COLUMN_REF.findall(_strip_strings(row_expr)))


_AGG_CALL_RE = re.compile(
    r"\b(" + "|".join(sorted(_CALC_SELF_AGG_FUNCS)) + r")\s*\(", re.IGNORECASE)
# An identifier, deliberately: the row loop binds each masked aggregate as
# an engine VARIABLE (evaluated once -- the gate only admits row-independent
# aggregates), so the mask must be resolvable by the engine's var lookup.
_AGG_MASK = "__AGG{}__"
# The ONLY argument shape treated as a whole-column aggregate: exactly one
# column reference, optionally table-qualified. Anything else -- a second
# argument, a bare table name, an expression -- is not this.
_AGG_SOLE_COLUMN_RE = re.compile(
    r"^\s*(?:'([^']+)'|([A-Za-z_]\w*))?\s*\[([^\]]+)\]\s*$")


def _agg_column_arg(call: str):
    """(table_or_None, column) if `call` is AGG(<one column reference>), else None.

    MIN and MAX have a SECOND, scalar overload -- ``MIN(<expr1>, <expr2>)`` --
    that is not an aggregate at all. Treating ``MIN('T'[Amount], 0)`` as one
    hid its column reference from the per-row substitution, and the engine then
    evaluated a bare reference with no row context: 0 on every row, reported as
    success. Requiring a single column argument rejects that, and also rejects
    ``COUNTROWS(OtherTable)``, whose bare table name no other guard can see.
    """
    open_paren = call.find("(")
    if open_paren < 0 or not call.endswith(")"):
        return None
    m = _AGG_SOLE_COLUMN_RE.match(call[open_paren + 1:-1])
    if not m:
        return None
    return (m.group(1) or m.group(2)), m.group(3)


def strip_dax_comments(expr: str) -> str:
    """Remove ``--`` and ``//`` line comments and ``/* */`` blocks.

    Comment markers INSIDE a string literal are left alone. Stripping with a
    plain regex deleted the rest of the line whenever a value contained one --
    a URL ("https://..."), an ISO range ("2024-01--2024-06"), a double dash in
    prose -- taking whole column references with it. The expression still
    evaluated, to a plausible wrong value, and the unresolved-reference check
    could not help because it only ever sees the text AFTER stripping: the
    reference was already gone.
    """
    text = expr or ""
    # Locate markers on a copy whose string literals are blanked, then cut from
    # the ORIGINAL at those offsets.
    shadow = _STRING_LITERAL.sub(
        lambda m: '"' + "\x01" * (len(m.group(0)) - 2) + '"', text)
    out, i = [], 0
    while i < len(text):
        line_cut = None
        for marker in ("--", "//"):
            p = shadow.find(marker, i)
            if p >= 0 and (line_cut is None or p < line_cut):
                line_cut = p
        block = shadow.find("/*", i)
        if block >= 0 and (line_cut is None or block < line_cut):
            end = shadow.find("*/", block + 2)
            out.append(text[i:block])
            i = len(text) if end < 0 else end + 2
            continue
        if line_cut is None:
            out.append(text[i:])
            break
        nl = shadow.find("\n", line_cut)
        out.append(text[i:line_cut])
        if nl < 0:
            break
        i = nl
    return "".join(out)


def _agg_shadow(expr: str) -> str:
    """`expr` with string literals and bracketed identifiers blanked in place.

    Same length as the original, so match offsets still index into `expr`.
    Two things must not be scanned for aggregate names:
      * a STRING containing one, e.g. IF(T[Cat] = "MIN(", T[A], T[B]) -- that
        masked from inside the literal to the end of the expression, hiding
        T[A] and T[B] from both the row substitution and the reference check;
      * a COLUMN NAMED after one, e.g. [Total Count (n)] or [Max (temp)] --
        scanning the raw text chopped the name in half.
    """
    out = _STRING_LITERAL.sub(
        lambda m: '"' + "\x01" * (len(m.group(0)) - 2) + '"', expr)
    return re.sub(r"\[[^\]]*\]",
                  lambda m: "[" + "\x01" * (len(m.group(0)) - 2) + "]", out)


def _scan_aggregate_calls(expr: str):
    """Yield (start, end) of every aggregate call in `expr`."""
    shadow = _agg_shadow(expr)
    i = 0
    while True:
        m = _AGG_CALL_RE.search(shadow, i)
        if not m:
            return
        # Walk the SHADOW to the matching close paren: its string literals are
        # blanked, so a '(' or ')' inside one cannot unbalance the count.
        depth, j = 0, m.end() - 1
        while j < len(shadow):
            ch = shadow[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    j += 1
                    break
            j += 1
        yield m.start(), j
        i = j


def _iter_aggregate_calls(expr: str):
    """Every aggregate call in `expr`, as source text.

    Shares the scanner with _mask_aggregate_calls so the gate and the evaluator
    can never disagree about what counts as an aggregate call.
    """
    return [expr[a:b] for a, b in _scan_aggregate_calls(expr)]


def _mask_aggregate_calls(expr: str) -> "tuple[str, list[str]]":
    """Hide whole aggregate calls so per-row substitution cannot reach inside.

    The row loop replaces every reference to the target table's columns with
    that row's literal value. Applied to ``MIN('Date'[Year])`` that yields
    ``MIN(2015)`` -- the current row's year rather than the column minimum, and
    a different wrong answer on every row. Masking the whole call first leaves
    it for the engine, which evaluates it against the table snapshot.
    """
    out: list[str] = []
    spans: list[str] = []
    i = 0
    for a, b in _scan_aggregate_calls(expr):
        out.append(expr[i:a])
        call = expr[a:b]
        # Only a single-column argument is a whole-column aggregate. Anything
        # else (the two-argument scalar MIN/MAX, a bare table, an expression)
        # is left in place so the row substitution treats it normally and the
        # gate can refuse it.
        if _agg_column_arg(call) is None:
            out.append(call)
        else:
            out.append(_AGG_MASK.format(len(spans)))
            spans.append(call)
        i = b
    out.append(expr[i:])
    return "".join(out), spans


def _unmask_aggregate_calls(expr: str, spans: "list[str]") -> str:
    for k, span in enumerate(spans):
        expr = expr.replace(_AGG_MASK.format(k), span)
    return expr


def _refs_any_column(expr: str) -> bool:
    """Whether the expression reads any column at all.

    A constant expression such as BLANK() cannot be blank *because* a reference
    failed to resolve, so the all-blank heuristic must not fire on it.
    """
    return bool(_COLUMN_REF.search(_strip_strings(expr)))


def _dax_literal(val) -> str:
    """A python value as DAX source text."""
    if isinstance(val, str):
        return '"' + val.replace('"', '""') + '"'
    if val is None:
        return "BLANK()"
    if isinstance(val, bool):
        return "TRUE()" if val else "FALSE()"
    if isinstance(val, (datetime, date)):
        # A date must go in QUOTED -- bare 2024-01-15 00:00:00 is not
        # parseable DAX, which made every date-part expression fail.
        return '"' + val.isoformat() + '"'
    return str(val)


def _subst_row(expr: str, row_data: dict, target_table: str) -> str:
    """Replace every reference to the target table's columns with this row's value."""
    for cn, val in row_data.items():
        lit = None
        for pat in (f"'{target_table}'[{cn}]", f"{target_table}[{cn}]"):
            if pat in expr:
                if lit is None:
                    lit = _dax_literal(val)
                expr = expr.replace(pat, lit)
    return expr


def _lv_key(val):
    """Normalise a value for LOOKUPVALUE matching.

    Strings compare case-INSENSITIVELY: Power BI models use a case-insensitive
    collation by default, so LOOKUPVALUE(T[X], T[Cat], "abc") really does match
    a stored "ABC". Numbers unify int/float so 3 matches 3.0, and a midnight
    datetime matches the plain date -- the same normalisation the corpus
    ground-truth comparison uses.
    """
    if val is None or isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.casefold()
    if isinstance(val, datetime):
        return val.date() if (val.hour, val.minute, val.second,
                              val.microsecond) == (0, 0, 0, 0) else val
    if isinstance(val, date):
        return val
    if isinstance(val, (int, float)):
        f = float(val)
        return int(f) if f.is_integer() else f
    return val


def _build_lv_index(spec: dict, all_tables: Dict[str, dict]):
    """key(tuple of search values) -> set of distinct result values, or a reason."""
    src = None
    for tname, tdata in (all_tables or {}).items():
        if tname.lower() == spec["table"].lower():
            src = tdata
            break
    if src is None:
        return (f"LOOKUPVALUE reads table '{spec['table']}', whose data is "
                f"not available to this engine")
    cols = list(src.get("columns") or [])
    lower = {c.lower(): i for i, c in enumerate(cols)}
    try:
        res_i = lower[spec["result"].lower()]
        search_i = [lower[sc.lower()] for sc, _v in spec["pairs"]]
    except KeyError as exc:
        return (f"LOOKUPVALUE references '{spec['table']}'[{exc.args[0]}], "
                f"which that table does not have")
    index: Dict[tuple, set] = {}
    for r in src.get("rows") or []:
        key = tuple(_lv_key(r[i]) for i in search_i)
        index.setdefault(key, set()).add(_lv_key(r[res_i]))
    # Keep one representative of each result value in its ORIGINAL form; the
    # normalised key is only for matching, never for the value we store.
    raw: Dict[tuple, object] = {}
    for r in src.get("rows") or []:
        key = tuple(_lv_key(r[i]) for i in search_i)
        raw.setdefault(key, r[res_i])
    return {"distinct": index, "raw": raw}


def _build_related_resolver(spec: dict, all_tables: Dict[str, dict]):
    """value of the target row's FK -> the related row's column value.

    Walks the hop chain once per DISTINCT key and memoises, so a fact table
    with a handful of distinct foreign keys costs a handful of walks rather
    than one per row.
    """
    def _tbl(name):
        for tname, tdata in (all_tables or {}).items():
            if tname.lower() == (name or "").lower():
                return tdata
        return None

    hops = []
    for hop in spec["path"]:
        dst = _tbl(hop.get("ToTable"))
        if dst is None:
            return (f"RELATED needs table '{hop.get('ToTable')}', whose data "
                    f"is not available to this engine")
        cols = list(dst.get("columns") or [])
        lower = {c.lower(): i for i, c in enumerate(cols)}
        pk = (hop.get("ToColumn") or "").lower()
        if pk not in lower:
            return (f"RELATED: '{hop.get('ToTable')}' has no column "
                    f"'{hop.get('ToColumn')}' to join on")
        by_key: Dict[object, list] = {}
        for r in dst.get("rows") or []:
            by_key.setdefault(_lv_key(r[lower[pk]]), r)
        hops.append((by_key, lower))
    last_cols = hops[-1][1]
    res_i = last_cols.get(spec["column"].lower())
    if res_i is None:
        return (f"RELATED: '{spec['table']}' has no column "
                f"'{spec['column']}'")
    # Each hop after the first joins on the PREVIOUS table's FK column.
    fk_after = [(hop.get("FromColumn") or "").lower()
                for hop in spec["path"][1:]]
    memo: Dict[object, object] = {}

    def resolve(fk_value):
        key = _lv_key(fk_value)
        if key in memo:
            return memo[key]
        val = None
        cur = key
        for i, (by_key, lower) in enumerate(hops):
            row = by_key.get(cur)
            if row is None:
                val = None
                break
            if i == len(hops) - 1:
                val = row[res_i]
            else:
                nxt_i = lower.get(fk_after[i])
                if nxt_i is None:
                    val = None
                    break
                cur = _lv_key(row[nxt_i])
        memo[key] = val
        return val

    return resolve


def _ord_key(v):
    """A sortable key for the inequality column, or None if not orderable."""
    if isinstance(v, bool):
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, date):
        return datetime(v.year, v.month, v.day)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return v
    return None


def _running(agg: str, vals: "list"):
    """Prefix aggregate over `vals`, as a list where [i] covers vals[:i+1]."""
    out: "list" = []
    if agg == "sum":
        acc = 0.0
        for v in vals:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                acc += float(v)
            out.append(acc)
    elif agg in ("count", "counta"):
        acc = 0
        for v in vals:
            if v is not None:
                acc += 1
            out.append(acc)
    elif agg == "countrows":
        for i in range(len(vals)):
            out.append(i + 1)
    elif agg == "distinctcount":
        # DAX's DISTINCTCOUNT counts BLANK as one distinct value.
        seen: set = set()
        for v in vals:
            seen.add(_lv_key(v))
            out.append(len(seen))
    elif agg in ("min", "max"):
        cur_k, cur_v = None, None
        for v in vals:
            k = _ord_key(v)
            if k is not None and (
                    cur_k is None or (k < cur_k if agg == "min" else k > cur_k)):
                cur_k, cur_v = k, v
            out.append(cur_v)
    elif agg == "average":
        acc, n = 0.0, 0
        for v in vals:
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                acc += float(v)
                n += 1
            out.append(acc / n if n else None)
    return out


def _aggregate(agg: str, vals: "list"):
    """Whole-group aggregate, matching `_running`'s final element."""
    run = _running(agg, vals)
    return run[-1] if run else (0 if agg in
                                ("count", "counta", "countrows",
                                 "distinctcount") else None)


def _build_calculate_resolver(spec, columns, rows, outer_fns):
    """Compile the CALCULATE into one lookup per row.

    Equality terms become a hash index; a single inequality becomes a prefix
    aggregate over each group sorted by that column. Both are built once, so
    the per-row cost is a dict lookup (plus a bisect when there is an
    inequality) rather than a scan of the table.
    """
    import bisect

    lower = {c.lower(): i for i, c in enumerate(columns)}
    eq_idx = []
    for col, _rhs in spec["eq"]:
        if col.lower() not in lower:
            return f"FILTER references '{col}', which this table does not have"
        eq_idx.append(lower[col.lower()])
    agg = spec["agg"]
    val_i = (lower.get((spec["agg_col"] or "").lower())
             if spec["agg_col"] else None)
    if spec["agg_col"] and val_i is None:
        return (f"{agg.upper()} references '{spec['agg_col']}', which this "
                f"table does not have")

    def _val(r):
        return None if val_i is None else r[val_i]

    if spec["ineq"] is None:
        groups: "dict[tuple, list]" = {}
        for r in rows:
            groups.setdefault(
                tuple(_lv_key(r[i]) for i in eq_idx), []).append(_val(r))
        table = {k: _aggregate(agg, v) for k, v in groups.items()}
        empty = _aggregate(agg, [])

        def resolve(row_data):
            key = tuple(_lv_key(fn(row_data)) for fn in outer_fns["eq"])
            return table.get(key, empty)
        return resolve

    ineq_col, op, _rhs = spec["ineq"]
    if ineq_col.lower() not in lower:
        return (f"FILTER references '{ineq_col}', which this table does not "
                f"have")
    ineq_i = lower[ineq_col.lower()]
    buckets: "dict[tuple, list]" = {}
    for r in rows:
        k = _ord_key(r[ineq_i])
        if k is None:
            return (f"FILTER compares '{ineq_col}', which holds values this "
                    f"engine cannot order (blank or mixed types)")
        buckets.setdefault(
            tuple(_lv_key(r[i]) for i in eq_idx), []).append((k, _val(r)))
    compiled = {}
    for key, pairs in buckets.items():
        pairs.sort(key=lambda p: p[0])
        keys = [p[0] for p in pairs]
        vals = [p[1] for p in pairs]
        if op in ("<", "<="):
            compiled[key] = (keys, _running(agg, vals), False)
        else:
            rev = _running(agg, list(reversed(vals)))
            compiled[key] = (keys, list(reversed(rev)), True)
    empty = _aggregate(agg, [])

    def resolve(row_data):
        key = tuple(_lv_key(fn(row_data)) for fn in outer_fns["eq"])
        got = compiled.get(key)
        if got is None:
            return empty
        keys, prefix, suffix = got
        thr = _ord_key(outer_fns["ineq"](row_data))
        if thr is None:
            return empty
        if not suffix:
            n = (bisect.bisect_right(keys, thr) if op == "<="
                 else bisect.bisect_left(keys, thr))
            return prefix[n - 1] if n else empty
        n = (bisect.bisect_left(keys, thr) if op == ">="
             else bisect.bisect_right(keys, thr))
        return prefix[n] if n < len(prefix) else empty
    return resolve


def _outer_value_fn(rhs: str, target_table: str, columns, engine, dax_engine,
                    all_tables, relationships):
    """A function of the outer row for a predicate's right-hand side.

    A bare column reference reads the column directly. Anything else is
    evaluated by the engine but MEMOISED on just the columns it mentions --
    `DATEADD(COVID[Date], -1, DAY)` over 1.7M rows becomes one evaluation per
    distinct date rather than 1.7M.
    """
    idx = {c.lower(): i for i, c in enumerate(columns)}
    ref = _parse_column_ref(rhs, target_table, columns)
    if ref and ref[0].lower() == target_table.lower():
        i = idx.get(ref[1].lower())
        if i is not None:
            return lambda row_data: row_data.get(columns[i])

    # DATEADD is a TIME-INTELLIGENCE function: the engine answers it with a
    # marker tuple, not a scalar. Left alone, that tuple simply never matched
    # any index key, so `... - CALCULATE(SUM(x), FILTER(t, date = yesterday))`
    # quietly returned the unreduced value on every row. In a row context over
    # a single date it is just a day shift, so compute it directly.
    m_da = _DATEADD_DAY_RE.match(rhs.strip())
    if m_da:
        inner = _outer_value_fn(m_da.group(1), target_table, columns, engine,
                               dax_engine, all_tables, relationships)
        delta = int(m_da.group(2))

        def shift(row_data):
            base = inner(row_data)
            if isinstance(base, datetime):
                return base + timedelta(days=delta)
            if isinstance(base, date):
                return base + timedelta(days=delta)
            if base is None:
                return None
            raise ValueError(
                "DATEADD was given a non-date value this engine cannot shift")
        return shift
    used = [c for c in columns
            if f"'{target_table}'[{c}]" in rhs or f"{target_table}[{c}]" in rhs]
    memo: dict = {}

    def fn(row_data):
        key = tuple(_lv_key(row_data.get(c)) for c in used)
        if key in memo:
            return memo[key]
        sub = _subst_row(rhs, {c: row_data.get(c) for c in used}, target_table)
        stray = _unresolved_refs(sub)
        if stray:
            raise ValueError(
                "FILTER value references a column this engine cannot resolve "
                "in row context: " + ", ".join(sorted(stray)))
        ctx = dax_engine.DAXContext(all_tables, {}, None, None, None,
                                    relationships or [])
        got = engine._eval_expr(sub, ctx)
        # A filter value MUST be a scalar. The engine represents the
        # time-intelligence functions as marker tuples and tables as lists;
        # either would compare unequal to every stored value and silently
        # collapse the whole CALCULATE to its empty result, so refuse instead.
        if isinstance(got, (tuple, list, dict, set)):
            raise ValueError(
                f"FILTER compares against {rhs.strip()!r}, which this engine "
                f"does not reduce to a single value")
        memo[key] = got
        return got
    return fn


_MASK_TOKEN_RE = re.compile(r"__(?:AGG|CALC|REL|LV)\d+__")


def _lv_row_value(spec, idx, row_data, target_table, engine, dax_engine,
                  all_tables, relationships):
    """One row's LOOKUPVALUE result as a VALUE -- (value, error).

    Same semantics as the old text-splicing resolver: several matching rows
    must agree or the column is refused; a miss yields the alternate result if
    supplied, else BLANK. Search values and the alternate expression are still
    substituted-and-checked as text, so an unresolvable reference inside them
    refuses exactly as before instead of silently reading as blank.
    """
    key = []
    for _sc, val_expr in spec["pairs"]:
        sub = _subst_row(val_expr, row_data, target_table)
        stray = _unresolved_refs(sub)
        if stray:
            return None, (
                "LOOKUPVALUE search value references a column this engine "
                "cannot resolve in row context: " + ", ".join(sorted(stray)))
        try:
            ctx = dax_engine.DAXContext(
                all_tables, {}, None, None, None, relationships or [])
            key.append(_lv_key(engine._eval_expr(sub, ctx)))
        except Exception as exc:  # noqa: BLE001 - refuse, don't guess
            return None, f"LOOKUPVALUE search value failed to evaluate: {exc}"
    hits = idx["distinct"].get(tuple(key))
    if hits is None:
        if spec["alt"] is not None:
            sub = _subst_row(spec["alt"], row_data, target_table)
            stray = _unresolved_refs(sub)
            if stray:
                return None, (
                    "references a column this engine cannot resolve in row "
                    "context: " + ", ".join(sorted(stray)))
            try:
                ctx = dax_engine.DAXContext(
                    all_tables, {}, None, None, None, relationships or [])
                return engine._eval_expr(sub, ctx), None
            except Exception as exc:  # noqa: BLE001
                return None, f"row evaluation failed: {exc}"
        return None, None
    if len(hits) > 1:
        return None, (
            f"LOOKUPVALUE('{spec['table']}'[{spec['result']}], ...) "
            f"matches rows with different values -- ambiguous, which is an "
            f"error in DAX")
    return idx["raw"][tuple(key)], None


def evaluate_row_context_column(
    columns: List[str],
    rows: List[list],
    expression: str,
    target_table: str,
    all_tables: Dict[str, dict],
    relationships: Optional[List[dict]] = None,
):
    """Evaluate a row-context calc-column expression against a table's rows.

    Returns ``(values, error)``. ``error`` is None on success; otherwise a
    string and ``values`` is None. The caller MUST have already cleared
    ``calc_column_unsupported_reason``. Any per-row evaluation exception, or an
    all-blank result on a non-empty table (a tell-tale of a silently unresolved
    reference), is treated as an error so nothing wrong is materialized.
    """
    from pbix_mcp.dax import engine as dax_engine

    clean = expand_variation_accessors(strip_dax_comments(expression)).strip()
    # Refuse the pathological expression that already contains a mask-shaped
    # identifier BEFORE any masking runs -- binding our masks as variables
    # would silently shadow it. After masking, the tokens are legitimately
    # everywhere, so the check only means anything here.
    if _MASK_TOKEN_RE.search(clean):
        return None, (
            "expression contains a reserved __AGG/__CALC/__REL/__LV token, "
            "which this engine uses internally")
    engine = dax_engine.DAXEngine()
    values: list = []
    unresolved: set = set()
    # LOOKUPVALUE reads ANOTHER table, so it is neither row-substitutable nor a
    # whole-column aggregate. Mask each call to a placeholder and resolve it per
    # row from a prebuilt index -- built once, not per row, or a lookup over an
    # n-row dimension would make the column O(n*m).
    known = {t: (v.get("columns") or [])
             for t, v in (all_tables or {}).items()}
    _vars, _body = _extract_top_level_vars(clean)
    clean, calc_specs, calc_why = _mask_calculate_calls(
        clean, target_table, columns, _vars)
    if calc_why:
        return None, calc_why
    calc_resolvers = []
    for spec in calc_specs:
        outer = {"eq": [_outer_value_fn(rhs, target_table, columns, engine,
                                        dax_engine, all_tables, relationships)
                        for _c, rhs in spec["eq"]],
                 "ineq": (_outer_value_fn(spec["ineq"][2], target_table,
                                          columns, engine, dax_engine,
                                          all_tables, relationships)
                          if spec["ineq"] else None)}
        built = _build_calculate_resolver(spec, columns, rows, outer)
        if isinstance(built, str):
            return None, built
        calc_resolvers.append(built)
    clean, rel_specs, rel_why = _mask_related_calls(
        clean, target_table, columns, known, relationships)
    if rel_why:
        return None, rel_why
    rel_resolvers = []
    for spec in rel_specs:
        built = _build_related_resolver(spec, all_tables)
        if isinstance(built, str):
            return None, built
        rel_resolvers.append(built)
    clean, lv_specs, lv_why = _mask_lookupvalue_calls(
        clean, target_table, columns, known)
    if lv_why:
        return None, lv_why
    lv_index = []
    for spec in lv_specs:
        built = _build_lv_index(spec, all_tables)
        if isinstance(built, str):
            return None, built
        lv_index.append(built)
    # Aggregate calls are row-independent, so mask them once rather than per row.
    masked, agg_spans = _mask_aggregate_calls(clean)

    # Evaluate each masked aggregate ONCE -- the gate only admits whole-column
    # aggregates of this table, which are the same value on every row -- and
    # bind it as an engine variable under its mask token.
    base_scope: dict = {}
    for n, call in enumerate(agg_spans):
        try:
            actx = dax_engine.DAXContext(
                all_tables, {}, None, None, None, relationships or [])
            base_scope[_AGG_MASK.format(n)] = engine._eval_expr(call, actx)
        except Exception as e:  # noqa: BLE001 - refuse, don't store garbage
            return None, f"row evaluation failed: {e}"

    # The expression text is IDENTICAL on every row now (no per-row literal
    # substitution), so the unresolved-reference safety check no longer needs
    # to run per row: probe ONE row's substitution. Same text, same column
    # set, same set of strays on every row.
    if rows:
        probe_data = {cn: rows[0][ci] for ci, cn in enumerate(columns)}
        unresolved = _unresolved_refs(_subst_row(masked, probe_data,
                                                 target_table))
        if unresolved:
            return None, (
                "references a column this engine cannot resolve in row "
                "context: " + ", ".join(sorted(unresolved)))

    # A plain row expression is a PURE FUNCTION of the columns it names, so
    # rows that agree on those columns give the same answer. Memoising on that
    # tuple collapses the work to the number of DISTINCT inputs. Only enabled
    # when nothing per-row-stateful is in play -- CALCULATE/RELATED/LOOKUPVALUE
    # resolve against columns that may not appear in the masked text, so
    # memoising those could reuse a wrong answer.
    memo_cols = None
    memo: dict = {}
    per_row_masks = bool(calc_specs or rel_specs or lv_specs)
    if not per_row_masks:
        memo_cols = [c for c in columns
                     if f"'{target_table}'[{c}]" in masked
                     or f"{target_table}[{c}]" in masked]
        # No per-row scope entries: the aggregate bindings are constant, so
        # the engine-level scope can be installed once for the whole loop.
        engine._current_var_scope = base_scope or None

    for row in rows:
        row_data = {cn: row[ci] for ci, cn in enumerate(columns)}
        if memo_cols is not None:
            key = tuple(_lv_key(row_data.get(c)) for c in memo_cols)
            if key in memo:
                values.append(memo[key])
                continue
        if per_row_masks:
            scope = dict(base_scope)
            if calc_specs:
                try:
                    for n in range(len(calc_specs)):
                        scope[_CALC_MASK.format(n)] = calc_resolvers[n](row_data)
                except ValueError as exc:
                    return None, str(exc)
            for n, spec in enumerate(rel_specs):
                fk = spec["path"][0].get("FromColumn")
                for cn, val in row_data.items():
                    if cn.lower() == (fk or "").lower():
                        scope[_REL_MASK.format(n)] = rel_resolvers[n](val)
                        break
                else:
                    return None, (
                        f"RELATED joins on a column '{fk}' that is not in "
                        f"this row")
            # Install THIS row's scope BEFORE resolving LOOKUPVALUEs: a
            # RELATED/CALCULATE mask can sit inside a LOOKUPVALUE search value
            # (the gate admits it), and _lv_row_value evaluates that value
            # through the engine. Installing the scope after the loop made the
            # nested mask resolve against the PREVIOUS row's scope -- every
            # row silently materialized the previous row's lookup result,
            # shifted by one. The LV results join the same dict, so the main
            # evaluation below sees them too.
            engine._current_var_scope = scope
            for n, spec in enumerate(lv_specs):
                val, lv_err = _lv_row_value(
                    spec, lv_index[n], row_data, target_table, engine,
                    dax_engine, all_tables, relationships)
                if lv_err:
                    return None, lv_err
                scope[_LV_MASK.format(n)] = val
        try:
            ctx = dax_engine.DAXContext(
                all_tables, {}, None, None, None, relationships or [])
            # Column refs resolve through the engine's own row context, so the
            # SAME text evaluates on every row and its plan is parsed once.
            ctx._current_row = {'__table__': target_table, **row_data}
            got = engine._eval_expr(masked, ctx)
            values.append(got)
            if memo_cols is not None and len(memo) < 200_000:
                memo[tuple(_lv_key(row_data.get(c)) for c in memo_cols)] = got
        except Exception as e:  # noqa: BLE001 - refuse, don't store garbage
            return None, f"row evaluation failed: {e}"
    engine._current_var_scope = None
    if engine.unsupported_functions:
        return None, (
            "expression uses unsupported function(s): "
            + ", ".join(sorted(engine.unsupported_functions)))
    # A reference the substitution above did not replace belongs to another
    # table (or does not exist). The engine reads it as blank, so the column
    # would materialize as silently wrong rather than fail. Name it and refuse.
    if unresolved:
        return None, (
            "references a column this engine cannot resolve in row context: "
            + ", ".join(sorted(unresolved)))
    # All-blank used to be refused outright as a tell-tale of an unresolved
    # reference. That is now detected directly above, and the guess had a false
    # positive that blocked a whole file: a column whose expression is literally
    # BLANK() is legitimately blank on every row.
    if rows and all(v is None for v in values) and _refs_any_column(clean):
        return None, (
            "every row evaluated to blank — the expression likely references "
            "a column or name that doesn't resolve in this engine")
    return values, None


# ---------------------------------------------------------------------------
# Calculated-TABLE authoring support (evaluate + normalize + reliability gate)
# ---------------------------------------------------------------------------
# Our table functions are not uniformly faithful. Verified against the engine:
#   TOPN / ADDCOLUMNS / DATATABLE / GENERATESERIES / a bare table reference
#     return correct, fully-named rows;
#   DISTINCT / VALUES return a ('__table__','__column__','__value__') shape
#     that normalizes cleanly to one named column;
#   SUMMARIZE / SUMMARIZECOLUMNS / SELECTCOLUMNS were refused here until they
#     were fixed (extension columns were dropped, and SELECTCOLUMNS raised on a
#     plain table) — they now round-trip faithfully, including grouping by a
#     RELATED table's column, so they are allowed;
#   GROUPBY and the join/index helpers below are still not implemented, and the
#     evaluator refuses anything that reports an unsupported function anyway —
#     they stay listed so the refusal names the function explicitly.
_CALC_TABLE_LOSSY_FUNCS = {
    "groupby", "naturalinnerjoin", "naturalleftouterjoin",
    "substitutewithindex", "addmissingitems",
}
_CALC_TABLE_FUNC_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)\s*\(")


def calc_table_unsupported_reason(expression: str) -> Optional[str]:
    """Return why a calc-table expression can't be materialized, or None."""
    e = strip_dax_comments(expression)
    if not e.strip():
        return "expression is empty"
    for fn in _CALC_TABLE_FUNC_RE.findall(e):
        if fn.lower() in _CALC_TABLE_LOSSY_FUNCS:
            return (
                f"uses '{fn.upper()}', which this engine does not reproduce "
                f"faithfully (its extension/selected columns are dropped or it "
                f"cannot be evaluated). Author this table in Power BI Desktop"
            )
    return None


def evaluate_calc_table_expression(
    expression: str,
    tables: Dict[str, dict],
    measures: Optional[Dict[str, str]] = None,
    relationships: Optional[List[dict]] = None,
):
    """Evaluate a calculated-table DAX expression to ``{columns, rows}``.

    Returns ``(result, error)``; ``error`` is None on success. Refuses (rather
    than returning a partial/blank table) when the engine reports an
    unsupported function, the result isn't a row set, the rows carry no usable
    column names, or the column sets are inconsistent — every one of which
    would otherwise persist a silently-wrong table.
    """
    from pbix_mcp.dax import engine as dax_engine

    clean = strip_dax_comments(expression).strip()
    engine = dax_engine.DAXEngine()
    ctx = dax_engine.DAXContext(
        tables, measures or {}, None, None, None, relationships or [])
    try:
        result = engine._eval_expr(clean, ctx)
    except Exception as e:  # noqa: BLE001 — refuse, never persist garbage
        return None, f"evaluation failed: {e}"

    if engine.unsupported_functions:
        return None, ("expression uses unsupported function(s): "
                      + ", ".join(sorted(engine.unsupported_functions)))
    if not isinstance(result, list) or not result:
        return None, ("expression did not evaluate to a non-empty table "
                      "(this engine cannot reproduce it)")
    if not all(isinstance(r, dict) for r in result):
        return None, "expression did not evaluate to a row set"

    meta = {"__table__", "__column__", "__value__", "__row__"}
    # Single-column shape produced by DISTINCT()/VALUES(). Only when the rows
    # carry NO named columns of their own — some results (DATATABLE, SUMMARIZE)
    # set __column__/__value__ *alongside* real named columns, and treating
    # those as single-column would silently drop the rest.
    if all("__value__" in r and "__column__" in r
           and not [k for k in r if k not in meta] for r in result):
        col = result[0].get("__column__")
        if not col:
            return None, "single-column result has no column name"
        return {"columns": [col],
                "rows": [[r.get("__value__")] for r in result]}, None

    cols = [k for k in result[0].keys() if k not in meta]
    if not cols:
        return None, ("result rows carry no column names — this engine cannot "
                      "reproduce the expression's shape")
    for r in result:
        if [k for k in r.keys() if k not in meta] != cols:
            return None, "result rows have inconsistent columns"
    return {"columns": cols,
            "rows": [[r.get(c) for c in cols] for r in result]}, None


def _topo_sort(calc_defs: dict, existing_tables: dict) -> list:
    """Topological sort of calculated tables by dependency."""
    order = []
    visited = set()

    def visit(name):
        if name in visited:
            return
        visited.add(name)
        if name in calc_defs:
            expr = calc_defs[name]['expression']
            # Check if this expression references other calculated tables
            for other in calc_defs:
                if other != name and other in expr:
                    visit(other)
        order.append(name)

    for name in calc_defs:
        visit(name)

    return order


def _evaluate_table_expression(
    expr: str,
    tname: str,
    tdef: dict,
    tables: dict,
    relationships: list,
) -> Optional[dict]:
    """Evaluate a single calculated table DAX expression."""

    # Strip comments
    clean = strip_dax_comments(expr)
    clean = re.sub(r'//[^\n]*', '', clean)
    clean = clean.strip()

    # 1. GENERATESERIES(start, end, step)
    gs_match = re.match(r'GENERATESERIES\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)', clean, re.IGNORECASE)
    if gs_match:
        start, end, step = int(gs_match.group(1)), int(gs_match.group(2)), int(gs_match.group(3))
        col_name = tdef['columns'][0] if tdef['columns'] else 'Value'
        rows = [[i] for i in range(start, end + 1, step)]
        return {'columns': [col_name], 'rows': rows}

    # 2. DATATABLE("col", TYPE, ..., {{val1, val2}, ...})
    if re.match(r'DATATABLE\s*\(', clean, re.IGNORECASE):
        return _parse_datatable(clean, tdef)

    # 2b. Field parameter tables: { ("Display", NAMEOF('Table'[Col]), 0), ... }
    fp_result = _parse_field_parameter(clean, tdef)
    if fp_result:
        return fp_result

    # 3. Table name reference (another calculated table)
    ref_name = clean.strip("'\"")
    if ref_name in tables:
        ref = tables[ref_name]
        return {'columns': list(ref['columns']), 'rows': [list(r) for r in ref['rows']]}

    # 4. CALENDAR-generating expressions (complex VAR/RETURN with dates)
    if 'CALENDAR' in clean.upper():
        return _generate_calendar(tables, tdef)

    # 5. Try DAX engine evaluation
    try:
        from pbix_mcp.dax import engine as dax_engine
        engine = dax_engine.DAXEngine()
        ctx = dax_engine.DAXContext(tables, {}, None, None, None, relationships or [])
        result = engine._eval_expr(clean, ctx)
        if isinstance(result, list) and result:
            return _convert_dax_result(result, tdef)
    except Exception:
        pass

    return None


def _extract_balanced_tuples(text: str) -> list:
    """Extract balanced parenthesized groups from text, handling nested parens.

    Quote-aware: parens inside "double-quoted" strings are text, not structure
    (a display name like "Growth :)" or "a) Revenue" must not close the
    tuple). DAX's doubled-quote escape ("") toggles the state twice — a no-op,
    which is exactly right for balance counting."""
    results = []
    i = 0
    in_str = False
    while i < len(text):
        ch = text[i]
        if ch == '"':
            in_str = not in_str
            i += 1
            continue
        if ch == '(' and not in_str:
            depth = 1
            start = i + 1
            i += 1
            while i < len(text) and depth > 0:
                ch = text[i]
                if ch == '"':
                    in_str = not in_str
                elif not in_str:
                    if ch == '(':
                        depth += 1
                    elif ch == ')':
                        depth -= 1
                i += 1
            if depth == 0:
                results.append(text[start:i - 1])
        else:
            i += 1
    return results


def _parse_field_parameter(expr: str, tdef: dict) -> Optional[dict]:
    """Parse Power BI field parameter tables.

    Format: { ("Display", NAMEOF('Table'[Column]), OrderNum), ... }
    or:     { ("Display", NAMEOF('Table'[Column]), OrderNum, NAMEOF(...)), ... }

    These create tables with 3+ columns:
      - Parameter (display name)
      - Parameter Fields (NAMEOF result as string, e.g. "'Table'[Column]")
      - Parameter Order (integer)
    """
    # Match pattern: starts with { and contains NAMEOF
    if not (expr.strip().startswith('{') and 'NAMEOF' in expr.upper()):
        return None

    # Get column names from metadata
    col_names = tdef.get('columns', [])
    if not col_names:
        col_names = ['Parameter', 'Parameter Fields', 'Parameter Order']

    # Extract row tuples: ("display", NAMEOF('Table'[Col]), 0)
    rows = []
    # Find all balanced (...) groups inside the outer {}
    inner = expr.strip().strip('{}').strip()
    # Use balanced-parenthesis extraction since NAMEOF(...) nests parens
    tuple_pattern = _extract_balanced_tuples(inner)

    for t in tuple_pattern:
        # Parse the tuple: "Display Name", NAMEOF('Table'[Col]), 0
        parts = []
        remaining = t.strip()

        while remaining:
            remaining = remaining.strip().lstrip(',').strip()
            if not remaining:
                break

            # Quoted string (DAX escapes an embedded quote by doubling it)
            if remaining.startswith('"'):
                buf = []
                j = 1
                while j < len(remaining):
                    if remaining[j] == '"':
                        if j + 1 < len(remaining) and remaining[j + 1] == '"':
                            buf.append('"')
                            j += 2
                            continue
                        break
                    buf.append(remaining[j])
                    j += 1
                parts.append(''.join(buf))
                remaining = remaining[j + 1:]
            # NAMEOF('Table'[Column])
            elif remaining.upper().startswith('NAMEOF'):
                m = re.match(r"NAMEOF\s*\(\s*'([^']+)'\s*\[([^\]]+)\]\s*\)", remaining, re.IGNORECASE)
                if m:
                    # Store as "'Table'[Column]" format
                    parts.append(f"'{m.group(1)}'[{m.group(2)}]")
                    remaining = remaining[m.end():]
                else:
                    break
            # Number
            elif re.match(r'^-?\d', remaining):
                m = re.match(r'(-?\d+(?:\.\d+)?)', remaining)
                if m:
                    val = float(m.group(1)) if '.' in m.group(1) else int(m.group(1))
                    parts.append(val)
                    remaining = remaining[m.end():]
                else:
                    break
            else:
                # Skip unknown tokens
                m = re.match(r'[^,)]+', remaining)
                if m:
                    parts.append(m.group(0).strip())
                    remaining = remaining[m.end():]
                else:
                    break

        if parts:
            # Pad or trim to match column count
            while len(parts) < len(col_names):
                parts.append(None)
            rows.append(parts[:len(col_names)])

    if rows:
        return {'columns': col_names, 'rows': rows}
    return None


def _parse_datatable(expr: str, tdef: dict) -> Optional[dict]:
    """Parse DATATABLE("col", TYPE, ..., {{val1, val2}, ...})"""
    # Find the data block (everything inside the outermost {})
    brace_pos = expr.find('{{')
    if brace_pos < 0:
        brace_pos = expr.find('{')
    if brace_pos < 0:
        return None

    col_defs_str = expr[expr.index('(') + 1:brace_pos].rstrip().rstrip(',')
    data_block = expr[brace_pos:]

    # Parse column definitions
    # Split by comma, but respect quoted strings
    parts = _split_respecting_quotes(col_defs_str)

    col_names = []
    col_types = []
    i = 0
    while i + 1 < len(parts):
        name = parts[i].strip().strip('"\'')
        type_str = parts[i + 1].strip().upper()
        if type_str in ('INTEGER', 'STRING', 'BOOLEAN', 'DOUBLE', 'CURRENCY', 'DATETIME'):
            col_names.append(name)
            col_types.append(type_str)
            i += 2
        else:
            break

    if not col_names:
        # Single-column DATATABLE: DATATABLE("col", TYPE, {{"val1"}, {"val2"}})
        # Try extracting from metadata columns
        if tdef.get('columns'):
            col_names = list(tdef['columns'])
            col_types = ['STRING'] * len(col_names)

    if not col_names:
        return None

    # Extract row data from {{ ... }} blocks
    rows = []
    row_blocks = re.findall(r'\{([^{}]+)\}', data_block)
    for block in row_blocks:
        values = [v.strip().strip('"\'') for v in block.split(',')]
        if len(values) >= len(col_names):
            row = []
            for j, cn in enumerate(col_names):
                raw = values[j]
                if j < len(col_types):
                    ct = col_types[j]
                else:
                    ct = 'STRING'

                if ct == 'INTEGER':
                    try: row.append(int(float(raw)))
                    except: row.append(0)
                elif ct in ('DOUBLE', 'CURRENCY'):
                    try: row.append(float(raw))
                    except: row.append(0.0)
                elif ct == 'BOOLEAN':
                    row.append(raw.upper() in ('TRUE', '1'))
                else:
                    row.append(raw)
            rows.append(row)
        elif len(values) == 1 and len(col_names) == 1:
            # Single-column table
            raw = values[0]
            rows.append([raw])

    if rows:
        return {'columns': col_names, 'rows': rows}
    return None


def _split_respecting_quotes(s: str) -> list:
    """Split string by commas, respecting quoted strings."""
    parts = []
    current = ''
    in_quote = False
    quote_char = None
    for ch in s:
        if ch in ('"', "'") and not in_quote:
            in_quote = True
            quote_char = ch
            current += ch
        elif ch == quote_char and in_quote:
            in_quote = False
            current += ch
        elif ch == ',' and not in_quote:
            parts.append(current)
            current = ''
        else:
            current += ch
    if current.strip():
        parts.append(current)
    return parts


def _generate_calendar(tables: dict, tdef: dict) -> Optional[dict]:
    """Generate a calendar table from fact table date ranges."""
    min_date = max_date = None

    # Scan tables for date columns, prefer fact tables
    sorted_tables = sorted(
        tables.items(),
        key=lambda x: (
            0 if any(k in x[0].lower() for k in ['fact', 'sales', 'order', 'transaction']) else 1,
            -len(x[1]['rows'])
        )
    )

    for ft_name, ft_data in sorted_tables:
        for ci, col in enumerate(ft_data['columns']):
            if 'date' in col.lower() or 'datekey' in col.lower():
                for row in ft_data['rows']:
                    v = row[ci]
                    d = _to_date(v)
                    if d:
                        if min_date is None or d < min_date: min_date = d
                        if max_date is None or d > max_date: max_date = d
                if min_date:
                    break
        if min_date:
            break

    if not min_date or not max_date:
        return None

    # Extend to full years
    start = date(min_date.year, 1, 1)
    end = date(max_date.year, 12, 31)

    # Get column names from metadata or use defaults
    meta_cols = tdef.get('columns', [])
    if meta_cols:
        col_names = meta_cols
    else:
        col_names = ['Date', 'Year', 'MonthNumber', 'Month', 'Day', 'DayOfWeek',
                     'Quarter', 'DateWithTransactions']

    # Generate rows
    cal_rows = []
    d = start
    while d <= end:
        row_data = {
            'Date': d.isoformat() + 'T00:00:00',
            'Year': d.year,
            'MonthNumber': d.month,
            'Month': d.strftime('%B'),
            'MonthName': d.strftime('%B'),
            'ShortMonth': d.strftime('%b'),
            'Day': d.day,
            'DayOfWeek': d.strftime('%A'),
            'DayOfWeekNumber': d.isoweekday() % 7,
            'Quarter': (d.month - 1) // 3 + 1,
            'QuarterLabel': f'Q{(d.month - 1) // 3 + 1}',
            'WeekNumber': d.isocalendar()[1],
            'DateWithTransactions': True,
            'Year Month': f'{d.year} {d.strftime("%B")}',
            'Year Quarter': f'{d.year} Q{(d.month - 1) // 3 + 1}',
        }

        # Build row matching column names
        row = []
        for cn in col_names:
            # Try exact match, then case-insensitive
            val = row_data.get(cn)
            if val is None:
                for k, v in row_data.items():
                    if k.lower().replace(' ', '') == cn.lower().replace(' ', ''):
                        val = v
                        break
            if val is None:
                val = None  # Unknown column
            row.append(val)

        cal_rows.append(row)
        d += timedelta(days=1)

    return {'columns': col_names, 'rows': cal_rows}


def _to_date(v) -> Optional[date]:
    """Convert various types to a date object."""
    if v is None:
        return None
    # pandas Timestamp
    if hasattr(v, 'date') and callable(v.date):
        try: return v.date()
        except: return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    if isinstance(v, str):
        try: return date.fromisoformat(v.split('T')[0][:10])
        except: return None
    if isinstance(v, (int, float)):
        try:
            ds = str(int(v))
            if len(ds) == 8:
                return date(int(ds[:4]), int(ds[4:6]), int(ds[6:]))
        except: pass
    return None


def _convert_dax_result(result: list, tdef: dict) -> Optional[dict]:
    """Convert DAX engine result (list of dicts) to table format."""
    if not result or not isinstance(result[0], dict):
        return None

    meta_keys = {'__table__', '__column__', '__value__'}
    sample = result[0]
    result_cols = [k for k in sample.keys() if k not in meta_keys]

    if tdef.get('columns'):
        col_names = tdef['columns']
    else:
        col_names = result_cols

    rows = []
    for row_dict in result:
        row = []
        for cn in col_names:
            val = row_dict.get(cn)
            if val is None:
                # Try fuzzy match
                for k, v in row_dict.items():
                    if k not in meta_keys and k.lower() == cn.lower():
                        val = v
                        break
            row.append(val)
        rows.append(row)

    return {'columns': col_names, 'rows': rows}
