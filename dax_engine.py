"""
OpenBI — DAX Engine
====================
Evaluates DAX measure expressions against VertiPaq data.

Supports:
- Aggregation: SUM, AVERAGE, COUNT, COUNTROWS, MIN, MAX, DISTINCTCOUNT
- Iteration: SUMX, MAXX, FILTER
- Math: DIVIDE, ABS, ROUND, INT
- Logic: IF, SWITCH, AND, OR, NOT, ISBLANK
- Time Intelligence: DATEADD, SAMEPERIODLASTYEAR, CALCULATE with date filters
- Filter: CALCULATE, REMOVEFILTERS, ALL, ALLSELECTED, VALUES, FILTER
- Text: CONCATENATE, FORMAT, SELECTEDVALUE
- Table references: table[column] syntax
- Measure references: [MeasureName] syntax
- VAR / RETURN: variable declarations with expression evaluation
- String concatenation with &
"""

import re
import math
from datetime import datetime, timedelta
from typing import Any, Optional
from collections import defaultdict


class DAXContext:
    """Execution context for DAX evaluation — holds table data and filter state."""

    def __init__(self, tables: dict, measures: dict, date_table: str = None,
                 date_column: str = None, filter_context: dict = None,
                 relationships: list = None):
        """
        tables: { 'tableName': { 'columns': [...], 'rows': [[...], ...] } }
        measures: { 'MeasureName': 'DAX expression string' }
        date_table: name of the date dimension table
        date_column: name of the date column in the date table
        filter_context: { 'tableName.columnName': [allowed_values] }
        relationships: [ { FromTable, FromColumn, ToTable, ToColumn, IsActive } ]
        """
        self.tables = tables
        self.measures = measures
        self.date_table = date_table or 'dim-Date'
        self.date_column = date_column or 'Date'
        self.filter_context = filter_context or {}
        self.relationships = relationships or []
        self._measure_cache = {}
        self._eval_stack = set()  # Prevent circular refs
        # Build relationship index: { (fromTable, toTable): { fromCol, toCol } }
        self._rel_index = {}
        for rel in self.relationships:
            if rel.get('IsActive'):
                ft = rel.get('FromTable', '')
                tt = rel.get('ToTable', '')
                fc = rel.get('FromColumn', '')
                tc = rel.get('ToColumn', '')
                if ft and tt and fc and tc:
                    self._rel_index[(ft, tt)] = {'from_col': fc, 'to_col': tc}
                    self._rel_index[(tt, ft)] = {'from_col': tc, 'to_col': fc}

    def _find_col_idx(self, cols: list, col_name: str) -> int:
        """Find column index by name, with fuzzy matching."""
        for i, c in enumerate(cols):
            if c == col_name:
                return i
        # Fuzzy: try case-insensitive and hyphen/underscore normalization
        norm = col_name.lower().replace('-', '_').replace(' ', '_')
        for i, c in enumerate(cols):
            if c.lower().replace('-', '_').replace(' ', '_') == norm:
                return i
        return -1

    def _get_cross_table_filters(self, table_name: str) -> list:
        """
        Get ALL cross-table filters that apply to a target table.
        Uses model relationships to propagate dimension filters to fact tables.
        Returns list of (allowed_values_set, fact_col_idx) tuples.
        """
        if not self.filter_context:
            return []

        tbl = self.tables.get(table_name)
        if not tbl:
            return []

        result_filters = []

        # Group filter context entries by source table
        table_filters = {}
        for fk, values in self.filter_context.items():
            parts = fk.split('.', 1)
            if len(parts) == 2:
                src_table, src_col = parts
                if src_table != table_name:  # Only cross-table filters
                    if src_table not in table_filters:
                        table_filters[src_table] = []
                    table_filters[src_table].append((src_col, values))

        for src_table, col_filters in table_filters.items():
            src_tbl = self.tables.get(src_table)
            if not src_tbl:
                continue

            # Find relationship between source dim table and target table
            rel = self._rel_index.get((table_name, src_table))
            if not rel:
                # Try via date table special handling (for Year/Month filters on date dim)
                if src_table == self.date_table:
                    result_filters.extend(self._get_date_cross_filter(table_name, src_tbl, col_filters))
                continue

            # Direct relationship exists: filter source dim table, get join key values
            fact_join_col = rel['from_col']  # Column in target (fact) table
            dim_join_col = rel['to_col']     # Column in source (dim) table

            fact_join_idx = self._find_col_idx(tbl['columns'], fact_join_col)
            dim_join_idx = self._find_col_idx(src_tbl['columns'], dim_join_col)
            if fact_join_idx < 0 or dim_join_idx < 0:
                continue

            # Filter dim table rows by all filters on that table
            filtered_dim_rows = src_tbl['rows']
            for src_col, values in col_filters:
                col_idx = self._find_col_idx(src_tbl['columns'], src_col)
                if col_idx >= 0:
                    allowed = set(str(v) for v in values)
                    filtered_dim_rows = [r for r in filtered_dim_rows if str(r[col_idx]) in allowed]

            # Get allowed join key values
            allowed_keys = set(str(r[dim_join_idx]) for r in filtered_dim_rows)
            if allowed_keys:
                result_filters.append((allowed_keys, fact_join_idx))

        return result_filters

    def _get_date_cross_filter(self, table_name, date_tbl, col_filters):
        """Handle date dimension filters (Year, Month etc.) that need Date column resolution."""
        date_cols = date_tbl['columns']
        date_col_idx = self._find_col_idx(date_cols, self.date_column)
        if date_col_idx < 0:
            return []

        # Filter date rows
        filtered_rows = date_tbl['rows']
        for col_name, values in col_filters:
            col_idx = self._find_col_idx(date_cols, col_name)
            if col_idx >= 0:
                allowed = set(str(v) for v in values)
                filtered_rows = [r for r in filtered_rows if str(r[col_idx]) in allowed]

        allowed_dates = set(str(r[date_col_idx]) for r in filtered_rows)
        if not allowed_dates:
            return []

        # Find date column in fact table via relationship or heuristic
        tbl = self.tables.get(table_name)
        if not tbl:
            return []

        # Try relationship first
        rel = self._rel_index.get((table_name, self.date_table))
        if rel:
            fact_date_idx = self._find_col_idx(tbl['columns'], rel['from_col'])
            if fact_date_idx >= 0:
                return [(allowed_dates, fact_date_idx)]

        # Fallback: heuristic date column names
        for dcn in ['Order Date', 'Date', 'OrderDate', 'Transaction Date']:
            fact_date_idx = self._find_col_idx(tbl['columns'], dcn)
            if fact_date_idx >= 0:
                return [(allowed_dates, fact_date_idx)]

        return []

    def get_column_data(self, table_name: str, column_name: str) -> list:
        """Get all values for a column, respecting current filter context."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        cols = tbl['columns']
        col_idx = self._find_col_idx(cols, column_name)
        if col_idx < 0:
            return []

        rows = tbl['rows']

        # Apply direct filter context for this column
        filter_key = f"{table_name}.{column_name}"
        if filter_key in self.filter_context:
            allowed = set(str(v) for v in self.filter_context[filter_key])
            rows = [row for row in rows if str(row[col_idx]) in allowed]

        # Apply ALL cross-table filters (star-schema propagation via relationships)
        for allowed_vals, join_idx in self._get_cross_table_filters(table_name):
            rows = [row for row in rows if str(row[join_idx]) in allowed_vals]

        return [row[col_idx] for row in rows]

    def get_filtered_rows(self, table_name: str) -> list:
        """Get rows of a table after applying filter context."""
        tbl = self.tables.get(table_name)
        if not tbl:
            return []
        rows = tbl['rows']
        cols = tbl['columns']

        # Apply all direct filters for this table
        filtered = rows
        for fk, allowed_values in self.filter_context.items():
            parts = fk.split('.', 1)
            if parts[0] == table_name and len(parts) == 2:
                col_name = parts[1]
                col_idx = self._find_col_idx(cols, col_name)
                if col_idx >= 0:
                    allowed_set = set(str(v) for v in allowed_values)
                    filtered = [r for r in filtered if str(r[col_idx]) in allowed_set]

        # Apply ALL cross-table filters via relationships
        for allowed_vals, join_idx in self._get_cross_table_filters(table_name):
            filtered = [row for row in filtered if str(row[join_idx]) in allowed_vals]

        return filtered

    def with_filters(self, extra_filters: dict) -> 'DAXContext':
        """Create a new context with additional filters applied."""
        new_filters = dict(self.filter_context)
        new_filters.update(extra_filters)
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        ctx._measure_cache = {}
        return ctx

    def without_filters(self, keys: list) -> 'DAXContext':
        """Create a new context with specified filters removed."""
        new_filters = {k: v for k, v in self.filter_context.items() if k not in keys}
        ctx = DAXContext(self.tables, self.measures, self.date_table,
                         self.date_column, new_filters, self.relationships)
        return ctx


class DAXEngine:
    """Evaluates DAX expressions."""

    def __init__(self):
        self._current_var_scope = None  # Active variable scope during VAR/RETURN eval
        self._func_map = {
            'SUM': self._fn_sum,
            'AVERAGE': self._fn_average,
            'COUNT': self._fn_count,
            'COUNTROWS': self._fn_countrows,
            'MIN': self._fn_min,
            'MAX': self._fn_max,
            'DISTINCTCOUNT': self._fn_distinctcount,
            'DIVIDE': self._fn_divide,
            'ABS': self._fn_abs,
            'ROUND': self._fn_round,
            'INT': self._fn_int,
            'IF': self._fn_if,
            'SWITCH': self._fn_switch,
            'AND': self._fn_and,
            'OR': self._fn_or,
            'NOT': self._fn_not,
            'ISBLANK': self._fn_isblank,
            'CALCULATE': self._fn_calculate,
            'REMOVEFILTERS': self._fn_removefilters,
            'ALL': self._fn_all,
            'DATEADD': self._fn_dateadd,
            'SAMEPERIODLASTYEAR': self._fn_sameperiodlastyear,
            'VALUES': self._fn_values,
            'SELECTEDVALUE': self._fn_selectedvalue,
            'FORMAT': self._fn_format,
            'CONCATENATE': self._fn_concatenate,
            'SUMX': self._fn_sumx,
            'MAXX': self._fn_maxx,
            'FILTER': self._fn_filter,
            'BLANK': self._fn_blank,
        }

    def evaluate_measure(self, measure_name: str, ctx: DAXContext) -> Any:
        """Evaluate a named measure in the given context."""
        # Check cache
        try:
            fc_key = tuple(sorted((k, tuple(v) if isinstance(v, list) else v) for k, v in ctx.filter_context.items())) if ctx.filter_context else ()
            cache_key = (measure_name, fc_key)
        except:
            cache_key = None
        if cache_key and cache_key in ctx._measure_cache:
            return ctx._measure_cache[cache_key]

        # Prevent circular references
        if measure_name in ctx._eval_stack:
            return 0
        ctx._eval_stack.add(measure_name)

        expr = ctx.measures.get(measure_name)
        if expr is None:
            ctx._eval_stack.discard(measure_name)
            return None

        try:
            result = self._eval_expr(expr.strip(), ctx)
            if cache_key:
                ctx._measure_cache[cache_key] = result
            return result
        except Exception as e:
            # Graceful degradation
            return None
        finally:
            ctx._eval_stack.discard(measure_name)

    def _eval_expr(self, expr: str, ctx: DAXContext, var_scope: dict = None) -> Any:
        """Evaluate a DAX expression string.

        var_scope: dict of variable names (e.g. '_max') to their evaluated values.
        Used internally for VAR/RETURN support.
        """
        # Merge explicit var_scope with instance-level scope (from VAR/RETURN blocks).
        # Explicit var_scope takes priority; instance scope provides fallback so
        # that function handlers calling _eval_expr without var_scope still see vars.
        if var_scope is None and self._current_var_scope:
            var_scope = self._current_var_scope
        elif var_scope and self._current_var_scope:
            merged = dict(self._current_var_scope)
            merged.update(var_scope)
            var_scope = merged

        expr = expr.strip()
        if not expr:
            return None

        # Strip leading/trailing comments
        lines = expr.split('\n')
        clean_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('//'):
                continue
            # Remove inline comments
            comment_pos = stripped.find('//')
            if comment_pos > 0:
                stripped = stripped[:comment_pos].rstrip()
            clean_lines.append(stripped)
        expr = ' '.join(clean_lines).strip()

        if not expr:
            return None

        # -----------------------------------------------------------
        # VAR / RETURN support
        # -----------------------------------------------------------
        # Detect if the expression contains VAR ... RETURN blocks.
        # We look for the pattern:  VAR _name = <expr> ... RETURN <expr>
        # This must be checked BEFORE any other parsing so that the whole
        # multi-statement block is handled as a unit.
        if re.search(r'\bVAR\b', expr, re.IGNORECASE) and re.search(r'\bRETURN\b', expr, re.IGNORECASE):
            return self._eval_var_return(expr, ctx, var_scope)

        # -----------------------------------------------------------
        # Parenthesised sub-expression: ( expr )
        # -----------------------------------------------------------
        if expr.startswith('(') and expr.endswith(')'):
            # Verify the parens actually wrap the whole expression
            depth = 0
            wraps_all = True
            for i, ch in enumerate(expr):
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    depth -= 1
                if depth == 0 and i < len(expr) - 1:
                    wraps_all = False
                    break
            if wraps_all:
                return self._eval_expr(expr[1:-1].strip(), ctx, var_scope)

        # String literal
        if expr.startswith('"') and expr.endswith('"') and expr.count('"') == 2:
            return expr[1:-1]

        # Numeric literal
        try:
            if '.' in expr:
                return float(expr)
            return int(expr)
        except ValueError:
            pass

        # Boolean/special
        if expr.upper() == 'TRUE':
            return True
        if expr.upper() == 'FALSE':
            return False

        # -----------------------------------------------------------
        # Variable reference: identifiers starting with _
        # -----------------------------------------------------------
        if var_scope and re.match(r'^_[A-Za-z0-9_]+$', expr):
            var_name = expr
            if var_name in var_scope:
                return var_scope[var_name]
            # Case-insensitive fallback
            for k, v in var_scope.items():
                if k.lower() == var_name.lower():
                    return v

        # Measure reference: [MeasureName] — only if no operators between brackets
        if expr.startswith('[') and expr.endswith(']') and expr.count('[') == 1 and expr.count(']') == 1:
            measure_name = expr[1:-1]
            return self.evaluate_measure(measure_name, ctx)

        # Table[Column] reference
        col_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]", expr)
        if col_match and '(' not in expr:
            table_name = col_match.group(1).strip()
            col_name = col_match.group(2).strip()
            return (table_name, col_name)  # Return as column ref

        # Simple column ref without table: [Column] — only if it's the entire expression
        if re.match(r'^\[[^\]]+\]$', expr):
            inner = expr[1:-1]
            return self.evaluate_measure(inner, ctx)

        # Function call: FUNC(args)
        func_match = re.match(r'([A-Za-z_]\w*)\s*\(', expr)
        if func_match:
            func_name = func_match.group(1).upper()
            # Find matching closing paren
            args_str = self._extract_args(expr[func_match.end()-1:])
            if args_str is not None:
                args_text = args_str[1:-1]  # Strip parens
                fn = self._func_map.get(func_name)
                if fn:
                    return fn(args_text, ctx)
                # Unknown function — try to evaluate args
                return None

        # Binary operations: +, -, *, /
        result = self._eval_binary(expr, ctx, var_scope)
        if result is not None:
            return result

        # Comparison: <, >, <=, >=, =, <>
        result = self._eval_comparison(expr, ctx, var_scope)
        if result is not None:
            return result

        # String concatenation with &
        if '&' in expr:
            parts = self._split_top_level(expr, '&')
            if len(parts) > 1:
                results = [str(self._eval_expr(p.strip(), ctx, var_scope) or '') for p in parts]
                return ''.join(results)

        return None

    def _eval_var_return(self, expr: str, ctx: DAXContext, var_scope: dict = None) -> Any:
        """Parse and evaluate a VAR ... RETURN block.

        Extracts all VAR declarations, evaluates them in order (each can
        reference previously declared variables), then evaluates the
        RETURN expression with the full variable scope.
        """
        scope = dict(var_scope) if var_scope else {}

        # We need to split the expression into VAR declarations and a RETURN part.
        # Strategy: use a regex to find top-level VAR and RETURN keywords.
        # We work on the joined, comment-stripped expression.

        # Tokenize into VAR blocks and the RETURN expression.
        # Split on VAR keyword (case-insensitive) that appears as a word boundary.
        # First, find all VAR ... = ... segments and the RETURN segment.

        # Build a list of tokens: [ ('VAR', '_name', 'expression'), ..., ('RETURN', 'expression') ]
        # We'll use a simple state-machine approach scanning word by word.

        # Normalise whitespace
        text = re.sub(r'\s+', ' ', expr).strip()

        var_decls = []
        return_expr = None

        # Find all VAR declarations and RETURN using regex on the normalized text.
        # Pattern: VAR <name> = <expression> (terminated by next VAR or RETURN)
        # We find positions of all top-level VAR and RETURN keywords.
        keyword_positions = []
        for m in re.finditer(r'\b(VAR|RETURN)\b', text, re.IGNORECASE):
            keyword_positions.append((m.start(), m.group().upper(), m.end()))

        for idx, (pos, kw, end_pos) in enumerate(keyword_positions):
            # Determine where this block ends (next keyword position or end of string)
            if idx + 1 < len(keyword_positions):
                block_end = keyword_positions[idx + 1][0]
            else:
                block_end = len(text)

            block_text = text[end_pos:block_end].strip()

            if kw == 'VAR':
                # Parse: _name = expression
                var_match = re.match(r'(_[A-Za-z0-9_]+)\s*=\s*(.*)', block_text, re.DOTALL)
                if var_match:
                    var_name = var_match.group(1)
                    var_expr = var_match.group(2).strip()
                    var_decls.append((var_name, var_expr))
            elif kw == 'RETURN':
                return_expr = block_text

        # Evaluate each VAR declaration in order.
        # Set _current_var_scope so that function handlers (which don't receive
        # var_scope directly) can still resolve variable references.
        prev_scope = self._current_var_scope
        try:
            for var_name, var_expr in var_decls:
                self._current_var_scope = scope
                val = self._eval_expr(var_expr, ctx, scope)
                scope[var_name] = val

            # Evaluate RETURN expression
            self._current_var_scope = scope
            if return_expr:
                return self._eval_expr(return_expr, ctx, scope)

            return None
        finally:
            self._current_var_scope = prev_scope

    def _extract_args(self, expr: str) -> Optional[str]:
        """Extract balanced parentheses from expression starting with '('."""
        if not expr.startswith('('):
            return None
        depth = 0
        in_string = False
        for i, ch in enumerate(expr):
            if ch == '"' and (i == 0 or expr[i-1] != '\\'):
                in_string = not in_string
            if in_string:
                continue
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1
                if depth == 0:
                    return expr[:i+1]
        return None

    def _split_args(self, args_str: str) -> list:
        """Split function arguments at top-level commas."""
        return self._split_top_level(args_str, ',')

    def _split_top_level(self, expr: str, delimiter: str) -> list:
        """Split expression at top-level delimiter, respecting parens and strings."""
        parts = []
        current = []
        depth = 0
        in_string = False
        i = 0
        while i < len(expr):
            ch = expr[i]
            if ch == '"':
                in_string = not in_string
            if in_string:
                current.append(ch)
                i += 1
                continue
            if ch == '(':
                depth += 1
            elif ch == ')':
                depth -= 1

            if depth == 0 and expr[i:i+len(delimiter)] == delimiter:
                parts.append(''.join(current))
                current = []
                i += len(delimiter)
                continue
            current.append(ch)
            i += 1
        parts.append(''.join(current))
        return parts

    def _eval_binary(self, expr: str, ctx: DAXContext, var_scope: dict = None) -> Any:
        """Evaluate binary arithmetic: +, -, *, /"""
        # Split at lowest precedence first (+ and -)
        for op in ['+', '-']:
            parts = self._split_top_level(expr, f' {op} ')
            if len(parts) > 1:
                left = self._eval_expr(parts[0].strip(), ctx, var_scope)
                right = self._eval_expr((' ' + op + ' ').join(parts[1:]).strip(), ctx, var_scope)
                if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                    return left + right if op == '+' else left - right
                return None

        # Then * and /
        for op in ['*', '/']:
            parts = self._split_top_level(expr, f' {op} ')
            if len(parts) > 1:
                left = self._eval_expr(parts[0].strip(), ctx, var_scope)
                right = self._eval_expr((' ' + op + ' ').join(parts[1:]).strip(), ctx, var_scope)
                if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                    if op == '*':
                        return left * right
                    elif right != 0:
                        return left / right
                    else:
                        return None
                return None

        return None

    def _eval_comparison(self, expr: str, ctx: DAXContext, var_scope: dict = None) -> Any:
        """Evaluate comparison operators."""
        for op_str, op_fn in [('<>', lambda a, b: a != b), ('>=', lambda a, b: a >= b),
                               ('<=', lambda a, b: a <= b), ('>', lambda a, b: a > b),
                               ('<', lambda a, b: a < b), ('=', lambda a, b: a == b)]:
            parts = self._split_top_level(expr, f' {op_str} ')
            if len(parts) == 2:
                left = self._eval_expr(parts[0].strip(), ctx, var_scope)
                right = self._eval_expr(parts[1].strip(), ctx, var_scope)
                if left is not None and right is not None:
                    try:
                        return op_fn(left, right)
                    except:
                        return None
        return None

    # =========================================================================
    # DAX Functions
    # =========================================================================

    def _fn_sum(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            table_name, col_name = ref
            values = ctx.get_column_data(table_name, col_name)
            return sum(v for v in values if isinstance(v, (int, float)))
        return 0

    def _fn_average(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            table_name, col_name = ref
            values = [v for v in ctx.get_column_data(table_name, col_name) if isinstance(v, (int, float))]
            return sum(values) / len(values) if values else 0
        return 0

    def _fn_count(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            table_name, col_name = ref
            return len([v for v in ctx.get_column_data(table_name, col_name) if v is not None])
        return 0

    def _fn_countrows(self, args_str: str, ctx: DAXContext) -> Any:
        table_name = args_str.strip().strip("'")
        rows = ctx.get_filtered_rows(table_name)
        return len(rows)

    def _fn_min(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) == 1:
            ref = self._eval_expr(args[0].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                values = [v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float))]
                return min(values) if values else 0
        elif len(args) == 2:
            a = self._eval_expr(args[0].strip(), ctx)
            b = self._eval_expr(args[1].strip(), ctx)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                return min(a, b)
        return 0

    def _fn_max(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) == 1:
            ref = self._eval_expr(args[0].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                values = [v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float))]
                return max(values) if values else 0
        elif len(args) == 2:
            a = self._eval_expr(args[0].strip(), ctx)
            b = self._eval_expr(args[1].strip(), ctx)
            if isinstance(a, (int, float)) and isinstance(b, (int, float)):
                return max(a, b)
        return 0

    def _fn_distinctcount(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            return len(set(str(v) for v in values if v is not None))
        return 0

    def _fn_divide(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        numerator = self._eval_expr(args[0].strip(), ctx)
        denominator = self._eval_expr(args[1].strip(), ctx)
        alt = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else 0

        if isinstance(numerator, (int, float)) and isinstance(denominator, (int, float)):
            if denominator == 0:
                return alt if alt is not None else 0
            return numerator / denominator
        return alt

    def _fn_abs(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._eval_expr(args_str.strip(), ctx)
        return abs(val) if isinstance(val, (int, float)) else None

    def _fn_round(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        digits = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        return round(val, digits) if isinstance(val, (int, float)) else None

    def _fn_int(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._eval_expr(args_str.strip(), ctx)
        return int(val) if isinstance(val, (int, float)) else None

    def _fn_if(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        condition = self._eval_expr(args[0].strip(), ctx)
        if condition:
            return self._eval_expr(args[1].strip(), ctx)
        elif len(args) > 2:
            return self._eval_expr(args[2].strip(), ctx)
        return None

    def _fn_switch(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 3:
            return None
        test_val = self._eval_expr(args[0].strip(), ctx)
        i = 1
        while i < len(args) - 1:
            case_val = self._eval_expr(args[i].strip(), ctx)
            if test_val == case_val:
                return self._eval_expr(args[i + 1].strip(), ctx)
            i += 2
        # Default (odd number of remaining args)
        if len(args) % 2 == 0:
            return self._eval_expr(args[-1].strip(), ctx)
        return None

    def _fn_and(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        return all(self._eval_expr(a.strip(), ctx) for a in args)

    def _fn_or(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        return any(self._eval_expr(a.strip(), ctx) for a in args)

    def _fn_not(self, args_str: str, ctx: DAXContext) -> Any:
        return not self._eval_expr(args_str.strip(), ctx)

    def _fn_isblank(self, args_str: str, ctx: DAXContext) -> Any:
        val = self._eval_expr(args_str.strip(), ctx)
        return val is None or val == ''

    def _fn_blank(self, args_str: str, ctx: DAXContext) -> Any:
        return None

    def _fn_calculate(self, args_str: str, ctx: DAXContext) -> Any:
        """CALCULATE(expression, filter1, filter2, ...)"""
        args = self._split_args(args_str)
        if not args:
            return None

        base_expr = args[0].strip()
        new_ctx = ctx

        # Process filter arguments
        for i in range(1, len(args)):
            filter_arg = args[i].strip()

            # REMOVEFILTERS / ALL
            if filter_arg.upper().startswith('REMOVEFILTERS') or filter_arg.upper().startswith('ALL'):
                # Extract the column/table reference
                inner_match = re.search(r"\(\s*'?([^'\)]+)'?\s*(?:\[([^\]]+)\])?\s*\)", filter_arg)
                if inner_match:
                    table = inner_match.group(1).strip()
                    col = inner_match.group(2)
                    if col:
                        new_ctx = new_ctx.without_filters([f"{table}.{col}"])
                    else:
                        # Remove all filters for this table
                        keys_to_remove = [k for k in new_ctx.filter_context if k.startswith(f"{table}.")]
                        new_ctx = new_ctx.without_filters(keys_to_remove)
                continue

            # DATEADD
            if filter_arg.upper().startswith('DATEADD'):
                new_ctx = self._apply_dateadd_filter(filter_arg, new_ctx)
                continue

            # SAMEPERIODLASTYEAR
            if filter_arg.upper().startswith('SAMEPERIODLASTYEAR'):
                new_ctx = self._apply_dateadd_filter(
                    f"DATEADD({filter_arg[19:-1].strip()}, -1, YEAR)", new_ctx)
                continue

        return self._eval_expr(base_expr, new_ctx)

    def _apply_dateadd_filter(self, expr: str, ctx: DAXContext) -> DAXContext:
        """Apply DATEADD as a filter context modification."""
        # Parse DATEADD(column, offset, interval)
        match = re.search(r"DATEADD\s*\(\s*'?([^'\[]+)'?\s*\[([^\]]+)\]\s*,\s*(-?\d+)\s*,\s*(\w+)\s*\)", expr, re.IGNORECASE)
        if not match:
            return ctx

        date_table = match.group(1).strip()
        date_col = match.group(2).strip()
        offset = int(match.group(3))
        interval = match.group(4).upper()

        # Get all date values from the date table
        tbl = ctx.tables.get(date_table)
        if not tbl:
            return ctx

        cols = tbl['columns']
        date_col_idx = None
        for i, c in enumerate(cols):
            if c == date_col:
                date_col_idx = i
                break
        if date_col_idx is None:
            return ctx

        # Get year column if available
        year_col_idx = None
        for i, c in enumerate(cols):
            if c.lower() == 'year':
                year_col_idx = i
                break

        if interval == 'YEAR' and year_col_idx is not None:
            # Shift years: get current year range and shift by offset
            current_years = set()
            for row in ctx.get_filtered_rows(date_table):
                yr = row[year_col_idx]
                if yr is not None:
                    current_years.add(yr)

            if not current_years:
                # No filter — use all years
                for row in tbl['rows']:
                    yr = row[year_col_idx]
                    if yr is not None:
                        current_years.add(yr)

            shifted_years = {yr + offset for yr in current_years}

            # Find dates that fall in the shifted year range
            shifted_dates = []
            for row in tbl['rows']:
                yr = row[year_col_idx]
                if yr in shifted_years:
                    shifted_dates.append(row[date_col_idx])

            if shifted_dates:
                new_filters = dict(ctx.filter_context)
                # Replace ALL date table filters with the shifted dates
                # Remove any existing date table filters (Year, Date, etc.)
                keys_to_remove = [k for k in new_filters if k.startswith(f"{date_table}.")]
                for k in keys_to_remove:
                    del new_filters[k]
                # Set the shifted date filter
                new_filters[f"{date_table}.{date_col}"] = shifted_dates
                return DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                                  ctx.date_column, new_filters, ctx.relationships)

        return ctx

    def _fn_removefilters(self, args_str: str, ctx: DAXContext) -> Any:
        """REMOVEFILTERS — returns a marker for CALCULATE to process."""
        return ('__REMOVEFILTERS__', args_str.strip())

    def _fn_all(self, args_str: str, ctx: DAXContext) -> Any:
        """ALL — when used inside CALCULATE returns a marker; when used as a
        table expression (e.g. ALL('table'[column])) returns all distinct
        values of that column ignoring any active filters."""
        ref = args_str.strip()
        # Try to parse as a column reference: 'table'[column]
        col_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]", ref)
        if col_match:
            table_name = col_match.group(1).strip()
            col_name = col_match.group(2).strip()
            # Return all values ignoring filters — use raw table data
            tbl = ctx.tables.get(table_name)
            if tbl:
                col_idx = ctx._find_col_idx(tbl['columns'], col_name)
                if col_idx >= 0:
                    # Return list of {column: value} dicts for iteration
                    all_values = list(set(row[col_idx] for row in tbl['rows'] if row[col_idx] is not None))
                    return [{'__table__': table_name, '__column__': col_name, '__value__': v} for v in all_values]
        # Fallback: marker for CALCULATE
        return ('__ALL__', ref)

    def _fn_dateadd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATEADD — returns a marker for CALCULATE to process."""
        return ('__DATEADD__', args_str.strip())

    def _fn_sameperiodlastyear(self, args_str: str, ctx: DAXContext) -> Any:
        return ('__DATEADD__', args_str.strip())

    def _fn_values(self, args_str: str, ctx: DAXContext) -> Any:
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            return list(set(values))
        return []

    def _fn_selectedvalue(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        ref = self._eval_expr(args[0].strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = list(set(ctx.get_column_data(ref[0], ref[1])))
            if len(values) == 1:
                return values[0]
        default = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else None
        return default

    def _fn_format(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        fmt = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else None
        if val is None:
            return ''
        if fmt and isinstance(val, (int, float)):
            # Handle common DAX format strings
            fmt_str = str(fmt)
            # "0" or "0.0" or "#,##0" style — count decimal places
            if re.match(r'^[#0,.]+$', fmt_str):
                # Count digits after decimal point
                if '.' in fmt_str:
                    decimals = len(fmt_str.split('.')[-1])
                else:
                    decimals = 0
                use_comma = ',' in fmt_str
                formatted = f"{val:,.{decimals}f}" if use_comma else f"{val:.{decimals}f}"
                return formatted
            # "0.0%" style
            if fmt_str.endswith('%'):
                inner_fmt = fmt_str[:-1]
                if '.' in inner_fmt:
                    decimals = len(inner_fmt.split('.')[-1])
                else:
                    decimals = 0
                return f"{val * 100:.{decimals}f}%"
            # "$#,##0" or "$#,##0.00" style
            if fmt_str.startswith('$'):
                inner = fmt_str[1:]
                if '.' in inner:
                    decimals = len(inner.split('.')[-1])
                else:
                    decimals = 0
                return f"${val:,.{decimals}f}"
        return str(val)

    def _fn_concatenate(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        parts = [str(self._eval_expr(a.strip(), ctx) or '') for a in args]
        return ''.join(parts)

    def _fn_maxx(self, args_str: str, ctx: DAXContext) -> Any:
        """MAXX(table_expr, expression) — iterate over table, return max of expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        max_val = None
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    result = self._eval_expr(row_expr, row_ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
        return max_val if max_val is not None else 0

    def _fn_sumx(self, args_str: str, ctx: DAXContext) -> Any:
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        # First arg is table expression, second is expression to evaluate per row
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            total = 0
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # Row from ALL/VALUES — create a filtered context for this row
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    result = self._eval_expr(row_expr, row_ctx)
                    if isinstance(result, (int, float)):
                        total += result
                else:
                    # Simple case: evaluate constant per row
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        total += result
            return total
        return 0

    def _fn_maxx(self, args_str: str, ctx: DAXContext) -> Any:
        """MAXX(table_expression, expression) — iterate over table rows,
        evaluate expression for each row, return the maximum value."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            max_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    # Row from ALL/VALUES — create a filtered context for this row
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    result = self._eval_expr(row_expr, row_ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        if max_val is None or result > max_val:
                            max_val = result
            return max_val if max_val is not None else 0
        # Fallback: if table_ref is a column ref, get max of column
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return max(values) if values else 0
        return 0

    def _fn_filter(self, args_str: str, ctx: DAXContext) -> Any:
        """FILTER(table, condition) — returns filtered table rows.
        For now supports simple table name references."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        # If table_ref is a list (from ALL/VALUES), filter it
        if isinstance(table_ref, list):
            # For each row, evaluate condition in that row's context
            filtered = []
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    cond = self._eval_expr(args[1].strip(), row_ctx)
                    if cond:
                        filtered.append(row_item)
            return filtered
        return []


# =========================================================================
# API for the backend
# =========================================================================

_engine = DAXEngine()


def evaluate_measure(measure_name: str, tables: dict, measures: dict,
                     filter_context: dict = None,
                     date_table: str = None, date_column: str = None,
                     relationships: list = None) -> Any:
    """Evaluate a single DAX measure."""
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    return _engine.evaluate_measure(measure_name, ctx)


def evaluate_measures_batch(measure_names: list, tables: dict, measures: dict,
                            filter_context: dict = None,
                            date_table: str = None, date_column: str = None,
                            relationships: list = None) -> dict:
    """Evaluate multiple measures, returning { name: value }."""
    ctx = DAXContext(tables, measures, date_table, date_column, filter_context, relationships)
    results = {}
    for name in measure_names:
        results[name] = _engine.evaluate_measure(name, ctx)
    return results
