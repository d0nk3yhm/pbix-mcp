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
    relationships: List[dict] = None,
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
        clean_expr = re.sub(r'--[^\n]*', '', expr)
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
    clean = re.sub(r'--[^\n]*', '', expr)
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
    """Extract balanced parenthesized groups from text, handling nested parens."""
    results = []
    i = 0
    while i < len(text):
        if text[i] == '(':
            depth = 1
            start = i + 1
            i += 1
            while i < len(text) and depth > 0:
                if text[i] == '(':
                    depth += 1
                elif text[i] == ')':
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

            # Quoted string
            if remaining.startswith('"'):
                end = remaining.index('"', 1)
                parts.append(remaining[1:end])
                remaining = remaining[end + 1:]
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
