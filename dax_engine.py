"""
OpenBI — DAX Engine
====================
Evaluates DAX measure expressions against VertiPaq data.

Supports 150+ DAX functions:
- Aggregation: SUM, AVERAGE, COUNT, COUNTROWS, MIN, MAX, DISTINCTCOUNT, PRODUCT, MEDIAN
- Iteration: SUMX, MAXX, MINX, AVERAGEX, COUNTX, COUNTAX, COUNTBLANK
- Table: TOPN, ADDCOLUMNS, SUMMARIZE, SUMMARIZECOLUMNS, SELECTCOLUMNS, DISTINCT,
         UNION, EXCEPT, INTERSECT, CROSSJOIN, DATATABLE, ROW, TREATAS, GENERATE, GENERATEALL, GENERATESERIES
- Filter: CALCULATE, REMOVEFILTERS, ALL, ALLEXCEPT, ALLSELECTED, KEEPFILTERS,
          VALUES, FILTER, HASONEVALUE, HASONEFILTER, ISFILTERED, ISCROSSFILTERED,
          USERELATIONSHIP, EARLIER, EARLIEST
- Time Intelligence: DATEADD, SAMEPERIODLASTYEAR, DATESYTD/MTD/QTD, TOTALYTD/MTD/QTD,
                     PREVIOUSMONTH/QUARTER/YEAR, NEXTMONTH/QUARTER/YEAR, PARALLELPERIOD,
                     STARTOFMONTH/QUARTER/YEAR, ENDOFMONTH/QUARTER/YEAR,
                     OPENINGBALANCEMONTH/QUARTER/YEAR, CLOSINGBALANCEMONTH/QUARTER/YEAR,
                     FIRSTDATE, LASTDATE, DATESBETWEEN, DATESINPERIOD, CALENDAR, CALENDARAUTO
- Math: DIVIDE, ABS, ROUND, INT, CEILING, FLOOR, MOD, POWER, SQRT, LOG, LOG10, LN, EXP,
        SIGN, TRUNC, EVEN, ODD, FACT, GCD, LCM, RAND, RANDBETWEEN, PI, CURRENCY, FIXED
- Text: CONCATENATE, FORMAT, SELECTEDVALUE, LEFT, RIGHT, MID, LEN, UPPER, LOWER, PROPER,
        TRIM, SUBSTITUTE, REPLACE, REPT, SEARCH, FIND, CONTAINSSTRING, CONTAINSSTRINGEXACT,
        EXACT, UNICHAR, UNICODE, VALUE, COMBINEVALUES, PATHCONTAINS, PATHITEM, PATHLENGTH
- Logic: IF, SWITCH, AND, OR, NOT, ISBLANK, BLANK, TRUE, FALSE, IFERROR, COALESCE, CONTAINS
- Info: ISNUMBER, ISTEXT, ISNONTEXT, ISLOGICAL, ISERROR, USERNAME, USERPRINCIPALNAME,
        LOOKUPVALUE
- Relationship: RELATED, RELATEDTABLE, CROSSFILTER
- Table references: table[column] syntax
- Measure references: [MeasureName] syntax
- VAR / RETURN: variable declarations with expression evaluation
- String concatenation with &
"""

import re
import math
import random
import statistics
from datetime import datetime, timedelta, date
from typing import Any, Optional, List
from collections import defaultdict
from calendar import monthrange
from functools import reduce


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
        self.date_column = date_column or 'Date'
        # Auto-detect date table if not provided
        if date_table:
            self.date_table = date_table
        else:
            self.date_table = self._auto_detect_date_table(tables)
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

    @staticmethod
    def _auto_detect_date_table(tables: dict) -> str:
        """Auto-detect the date/calendar dimension table from available tables."""
        # Pass 1: table name contains 'date' and has a 'Date' column
        for tname, tdata in tables.items():
            if 'date' in tname.lower() and 'Date' in tdata.get('columns', []):
                return tname
        # Pass 2: common date-table prefixes (dimDate, DimDate, Calendar, etc.)
        for tname, tdata in tables.items():
            tlow = tname.lower().replace(' ', '').replace('-', '').replace('_', '')
            if tlow in ('dimdate', 'datetable', 'calendar', 'datekey', 'dates'):
                for cname in tdata.get('columns', []):
                    if cname.lower() == 'date':
                        return tname
        # Pass 3: any table with Date + Year/Month columns (likely a date dimension)
        for tname, tdata in tables.items():
            cols_lower = [c.lower() for c in tdata.get('columns', [])]
            if 'date' in cols_lower and ('year' in cols_lower or 'month' in cols_lower):
                return tname
        return 'dim-Date'  # fallback default

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
            # --- Aggregation ---
            'SUM': self._fn_sum,
            'AVERAGE': self._fn_average,
            'COUNT': self._fn_count,
            'COUNTROWS': self._fn_countrows,
            'MIN': self._fn_min,
            'MAX': self._fn_max,
            'DISTINCTCOUNT': self._fn_distinctcount,
            'PRODUCT': self._fn_product,
            'MEDIAN': self._fn_median,
            # --- Iteration ---
            'SUMX': self._fn_sumx,
            'MAXX': self._fn_maxx,
            'MINX': self._fn_minx,
            'AVERAGEX': self._fn_averagex,
            'COUNTX': self._fn_countx,
            'COUNTAX': self._fn_countax,
            'COUNTBLANK': self._fn_countblank,
            # --- Math ---
            'DIVIDE': self._fn_divide,
            'ABS': self._fn_abs,
            'ROUND': self._fn_round,
            'INT': self._fn_int,
            'CEILING': self._fn_ceiling,
            'FLOOR': self._fn_floor,
            'MOD': self._fn_mod,
            'POWER': self._fn_power,
            'SQRT': self._fn_sqrt,
            'LOG': self._fn_log,
            'LOG10': self._fn_log10,
            'LN': self._fn_ln,
            'EXP': self._fn_exp,
            'SIGN': self._fn_sign,
            'TRUNC': self._fn_trunc,
            'EVEN': self._fn_even,
            'ODD': self._fn_odd,
            'FACT': self._fn_fact,
            'GCD': self._fn_gcd,
            'LCM': self._fn_lcm,
            'RAND': self._fn_rand,
            'RANDBETWEEN': self._fn_randbetween,
            'PI': self._fn_pi,
            'CURRENCY': self._fn_currency,
            'FIXED': self._fn_fixed,
            # --- Logic ---
            'IF': self._fn_if,
            'SWITCH': self._fn_switch,
            'AND': self._fn_and,
            'OR': self._fn_or,
            'NOT': self._fn_not,
            'ISBLANK': self._fn_isblank,
            'BLANK': self._fn_blank,
            'TRUE': self._fn_true,
            'FALSE': self._fn_false,
            'IFERROR': self._fn_iferror,
            'COALESCE': self._fn_coalesce,
            'CONTAINS': self._fn_contains,
            # --- Filter ---
            'CALCULATE': self._fn_calculate,
            'REMOVEFILTERS': self._fn_removefilters,
            'ALL': self._fn_all,
            'ALLEXCEPT': self._fn_allexcept,
            'ALLSELECTED': self._fn_allselected,
            'KEEPFILTERS': self._fn_keepfilters,
            'VALUES': self._fn_values,
            'SELECTEDVALUE': self._fn_selectedvalue,
            'FILTER': self._fn_filter,
            'HASONEVALUE': self._fn_hasonevalue,
            'HASONEFILTER': self._fn_hasonefilter,
            'ISFILTERED': self._fn_isfiltered,
            'ISCROSSFILTERED': self._fn_iscrossfiltered,
            'USERELATIONSHIP': self._fn_userelationship,
            'EARLIER': self._fn_earlier,
            'EARLIEST': self._fn_earliest,
            # --- Table ---
            'TOPN': self._fn_topn,
            'ADDCOLUMNS': self._fn_addcolumns,
            'SUMMARIZE': self._fn_summarize,
            'SUMMARIZECOLUMNS': self._fn_summarizecolumns,
            'SELECTCOLUMNS': self._fn_selectcolumns,
            'DISTINCT': self._fn_distinct,
            'UNION': self._fn_union,
            'EXCEPT': self._fn_except,
            'INTERSECT': self._fn_intersect,
            'CROSSJOIN': self._fn_crossjoin,
            'DATATABLE': self._fn_datatable,
            'ROW': self._fn_row,
            'TREATAS': self._fn_treatas,
            'GENERATE': self._fn_generate,
            'GENERATEALL': self._fn_generateall,
            'GENERATESERIES': self._fn_generateseries,
            # --- Text ---
            'FORMAT': self._fn_format,
            'CONCATENATE': self._fn_concatenate,
            'LEFT': self._fn_left,
            'RIGHT': self._fn_right,
            'MID': self._fn_mid,
            'LEN': self._fn_len,
            'UPPER': self._fn_upper,
            'LOWER': self._fn_lower,
            'PROPER': self._fn_proper,
            'TRIM': self._fn_trim,
            'SUBSTITUTE': self._fn_substitute,
            'REPLACE': self._fn_replace,
            'REPT': self._fn_rept,
            'SEARCH': self._fn_search,
            'FIND': self._fn_find,
            'CONTAINSSTRING': self._fn_containsstring,
            'CONTAINSSTRINGEXACT': self._fn_containsstringexact,
            'EXACT': self._fn_exact,
            'UNICHAR': self._fn_unichar,
            'UNICODE': self._fn_unicode,
            'VALUE': self._fn_value,
            'COMBINEVALUES': self._fn_combinevalues,
            'CONCATENATEX': self._fn_concatenatex,
            'RANKX': self._fn_rankx,
            'PATHCONTAINS': self._fn_pathcontains,
            'PATHITEM': self._fn_pathitem,
            'PATHLENGTH': self._fn_pathlength,
            # --- Time Intelligence ---
            'DATEADD': self._fn_dateadd,
            'SAMEPERIODLASTYEAR': self._fn_sameperiodlastyear,
            'DATESYTD': self._fn_datesytd,
            'DATESMTD': self._fn_datesmtd,
            'DATESQTD': self._fn_datesqtd,
            'TOTALYTD': self._fn_totalytd,
            'TOTALMTD': self._fn_totalmtd,
            'TOTALQTD': self._fn_totalqtd,
            'PREVIOUSMONTH': self._fn_previousmonth,
            'PREVIOUSQUARTER': self._fn_previousquarter,
            'PREVIOUSYEAR': self._fn_previousyear,
            'NEXTMONTH': self._fn_nextmonth,
            'NEXTQUARTER': self._fn_nextquarter,
            'NEXTYEAR': self._fn_nextyear,
            'PARALLELPERIOD': self._fn_parallelperiod,
            'STARTOFMONTH': self._fn_startofmonth,
            'ENDOFMONTH': self._fn_endofmonth,
            'STARTOFQUARTER': self._fn_startofquarter,
            'ENDOFQUARTER': self._fn_endofquarter,
            'STARTOFYEAR': self._fn_startofyear,
            'ENDOFYEAR': self._fn_endofyear,
            'OPENINGBALANCEMONTH': self._fn_openingbalancemonth,
            'CLOSINGBALANCEMONTH': self._fn_closingbalancemonth,
            'OPENINGBALANCEQUARTER': self._fn_openingbalancequarter,
            'CLOSINGBALANCEQUARTER': self._fn_closingbalancequarter,
            'OPENINGBALANCEYEAR': self._fn_openingbalanceyear,
            'CLOSINGBALANCEYEAR': self._fn_closingbalanceyear,
            'FIRSTDATE': self._fn_firstdate,
            'LASTDATE': self._fn_lastdate,
            'DATESBETWEEN': self._fn_datesbetween,
            'DATESINPERIOD': self._fn_datesinperiod,
            'CALENDAR': self._fn_calendar,
            'CALENDARAUTO': self._fn_calendarauto,
            # --- Information ---
            'ISNUMBER': self._fn_isnumber,
            'ISTEXT': self._fn_istext,
            'ISNONTEXT': self._fn_isnontext,
            'ISLOGICAL': self._fn_islogical,
            'ISERROR': self._fn_iserror,
            'USERNAME': self._fn_username,
            'USERPRINCIPALNAME': self._fn_userprincipalname,
            'LOOKUPVALUE': self._fn_lookupvalue,
            # --- Relationship ---
            'RELATED': self._fn_related,
            'RELATEDTABLE': self._fn_relatedtable,
            'CROSSFILTER': self._fn_crossfilter,
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

        # Strip comments: DAX supports // and -- for single-line comments
        # Must not strip -- inside strings (e.g., SVG attributes)
        lines = expr.split('\n')
        clean_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('//') or stripped.startswith('--'):
                continue
            # Remove inline comments (// or --) but NOT inside strings
            in_str = False
            result_chars = []
            i = 0
            while i < len(stripped):
                ch = stripped[i]
                if ch == '"':
                    in_str = not in_str
                if not in_str:
                    if stripped[i:i+2] == '//' or stripped[i:i+2] == '--':
                        break  # Rest of line is comment
                result_chars.append(ch)
                i += 1
            stripped = ''.join(result_chars).rstrip()
            if stripped:
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
        if var_scope and re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', expr):
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

        # NOT prefix without parens: "NOT expr" or "not expr"
        not_prefix = re.match(r'(?i)^not\s+(.+)$', expr)
        if not_prefix:
            inner_val = self._eval_expr(not_prefix.group(1).strip(), ctx, var_scope)
            if inner_val is None or inner_val == 0 or inner_val == '' or inner_val is False:
                return True
            return False

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
                var_match = re.match(r'([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)', block_text, re.DOTALL)
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

    def _make_row_context(self, row_item: dict, ctx: 'DAXContext') -> 'DAXContext':
        """Create a filter context from a row dict, filtering on ALL columns of the row.
        This implements the row context → filter context transition."""
        meta_keys = {'__table__', '__column__', '__value__'}
        table_name = row_item.get('__table__', '')
        filters = {}
        for k, v in row_item.items():
            if k in meta_keys or v is None:
                continue
            filters[f"{table_name}.{k}"] = [v]
        # Also add the primary column filter
        col = row_item.get('__column__', '')
        val = row_item.get('__value__')
        if col and val is not None:
            filters[f"{table_name}.{col}"] = [val]
        return ctx.with_filters(filters)

    def _resolve_row_result(self, result, row_item, row_ctx):
        """Resolve a column reference result in a row iteration context.
        If result is a (table, column) tuple, resolve it to a concrete value."""
        if isinstance(result, tuple) and len(result) == 2:
            if isinstance(row_item, dict) and '__table__' in row_item:
                if result[0] == row_item['__table__'] and result[1] == row_item['__column__']:
                    return row_item['__value__']
            # Different column — get single value from filtered context
            vals = list(set(row_ctx.get_column_data(result[0], result[1])))
            if len(vals) == 1:
                return vals[0]
            return None
        return result

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

            # TREATAS
            if filter_arg.upper().startswith('TREATAS'):
                result = self._eval_expr(filter_arg, new_ctx)
                if isinstance(result, dict) and '__treatas__' in result:
                    extra = {}
                    for fk, fv in result.items():
                        if fk != '__treatas__':
                            extra[fk] = fv
                    if extra:
                        new_ctx = new_ctx.with_filters(extra)
                continue

            # Time-intelligence table filters (DATESYTD, DATESMTD, DATESQTD,
            # TOTALYTD, TOTALMTD, TOTALQTD, PREVIOUSMONTH, PREVIOUSQUARTER,
            # PREVIOUSYEAR, PARALLELPERIOD, etc.)
            ti_prefixes = ('DATESYTD', 'DATESMTD', 'DATESQTD', 'PREVIOUSMONTH',
                           'PREVIOUSQUARTER', 'PREVIOUSYEAR', 'NEXTMONTH',
                           'NEXTQUARTER', 'NEXTYEAR', 'PARALLELPERIOD',
                           'DATESBETWEEN', 'DATESINPERIOD')
            fa_upper = filter_arg.upper().split('(')[0].strip()
            if fa_upper in ti_prefixes:
                result = self._eval_expr(filter_arg, new_ctx)
                if isinstance(result, list) and result:
                    # Time-intelligence returns list of date-row dicts
                    first = result[0]
                    if isinstance(first, dict) and '__table__' in first:
                        tbl_name = first['__table__']
                        col_name = first['__column__']
                        date_vals = [r['__value__'] for r in result]
                        new_filters = dict(new_ctx.filter_context)
                        # Remove existing date table filters
                        keys_to_remove = [k for k in new_filters if k.startswith(f"{tbl_name}.")]
                        for k in keys_to_remove:
                            del new_filters[k]
                        new_filters[f"{tbl_name}.{col_name}"] = date_vals
                        new_ctx = DAXContext(new_ctx.tables, new_ctx.measures, new_ctx.date_table,
                                             new_ctx.date_column, new_filters, new_ctx.relationships)
                continue

            # FILTER(table, condition) or other table-returning expressions
            # Evaluate the filter arg — if it returns a list of row dicts,
            # extract filter values grouped by table.column
            result = self._eval_expr(filter_arg, new_ctx)
            if isinstance(result, list) and result:
                first = result[0]
                if isinstance(first, dict) and '__table__' in first:
                    groups = {}
                    if '__row__' in first:
                        # Multi-column row dict from ALL(Table) + FILTER
                        for row_item in result:
                            tbl_name = row_item['__table__']
                            for col_name, val in row_item.items():
                                if col_name.startswith('__'):
                                    continue
                                key = f"{tbl_name}.{col_name}"
                                if key not in groups:
                                    groups[key] = []
                                groups[key].append(val)
                    else:
                        # Single-column row dict from ALL(Table[Col]) or VALUES
                        for row_item in result:
                            key = f"{row_item['__table__']}.{row_item['__column__']}"
                            if key not in groups:
                                groups[key] = []
                            groups[key].append(row_item['__value__'])
                    if groups:
                        new_ctx = new_ctx.with_filters(groups)
                continue

            # Simple column = value filter: Table[Col] = value
            eq_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]\s*=\s*(.*)", filter_arg)
            if eq_match:
                tbl_name = eq_match.group(1).strip()
                col_name = eq_match.group(2).strip()
                val = self._eval_expr(eq_match.group(3).strip(), new_ctx)
                if val is not None:
                    new_ctx = new_ctx.with_filters({f"{tbl_name}.{col_name}": [val]})
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

        # Table-level ALL: ALL('TableName') — return all rows as multi-column row dicts
        table_name = ref.strip("'").strip()
        tbl = ctx.tables.get(table_name)
        if tbl and tbl.get('rows'):
            # Return full row dicts with all columns for FILTER to iterate
            result = []
            cols = tbl['columns']
            for row in tbl['rows']:
                row_dict = {'__table__': table_name, '__row__': True}
                for ci, col_name in enumerate(cols):
                    row_dict[col_name] = row[ci] if ci < len(row) else None
                result.append(row_dict)
            return result

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
            values = list(set(ctx.get_column_data(ref[0], ref[1])))
            # Return as row-dict list so CONCATENATEX / FILTER / iterators work
            return [{'__table__': ref[0], '__column__': ref[1], '__value__': v} for v in values]
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

    def _fn_sumx(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMX(table_expression, expression) — iterate over table rows, sum expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            total = 0
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        total += result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        total += result
            return total
        return 0

    def _fn_maxx(self, args_str: str, ctx: DAXContext) -> Any:
        """MAXX(table_expression, expression) — iterate over table rows, return max."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            max_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = self._make_row_context(row_item, ctx)
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
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

    def _fn_minx(self, args_str: str, ctx: DAXContext) -> Any:
        """MINX(table_expression, expression) — iterate over table rows, return min."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        if isinstance(table_ref, list):
            min_val = None
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        if min_val is None or result < min_val:
                            min_val = result
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        if min_val is None or result < min_val:
                            min_val = result
            return min_val if min_val is not None else 0
        if isinstance(table_ref, tuple) and len(table_ref) == 2:
            values = [v for v in ctx.get_column_data(table_ref[0], table_ref[1]) if isinstance(v, (int, float))]
            return min(values) if values else 0
        return 0

    def _fn_averagex(self, args_str: str, ctx: DAXContext) -> Any:
        """AVERAGEX(table_expression, expression) — iterate over table rows, average expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        values = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    table_name = row_item['__table__']
                    col_name = row_item['__column__']
                    val = row_item['__value__']
                    row_ctx = ctx.with_filters({f"{table_name}.{col_name}": [val]})
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if isinstance(result, (int, float)):
                        values.append(result)
        return sum(values) / len(values) if values else 0

    def _fn_countx(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTX(table_expression, expression) — count non-blank numeric results per row."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        count = 0
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                else:
                    result = self._eval_expr(row_expr, ctx)
                if result is not None and result != '':
                    count += 1
        return count

    def _fn_countax(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTAX(table_expression, expression) — count non-blank results (like COUNTX but counts text too)."""
        # In DAX, COUNTAX counts non-blank values of any type; functionally same as COUNTX here
        return self._fn_countx(args_str, ctx)

    def _fn_countblank(self, args_str: str, ctx: DAXContext) -> Any:
        """COUNTBLANK(column) — count blank values in a column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            return sum(1 for v in values if v is None or v == '')
        return 0

    def _fn_product(self, args_str: str, ctx: DAXContext) -> Any:
        """PRODUCT(column) — multiply all values in column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = [v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float))]
            if not values:
                return 0
            result = 1
            for v in values:
                result *= v
            return result
        return 0

    def _fn_median(self, args_str: str, ctx: DAXContext) -> Any:
        """MEDIAN(column) — return median value."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = sorted(v for v in ctx.get_column_data(ref[0], ref[1]) if isinstance(v, (int, float)))
            if not values:
                return 0
            return statistics.median(values)
        return 0

    def _fn_filter(self, args_str: str, ctx: DAXContext) -> Any:
        """FILTER(table, condition) — returns filtered table rows."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if isinstance(table_ref, list):
            filtered = []
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    if '__row__' in row_item:
                        # Multi-column row dict from ALL(Table)
                        table_name = row_item['__table__']
                        extra_filters = {}
                        for col_name, val in row_item.items():
                            if col_name.startswith('__'):
                                continue
                            extra_filters[f"{table_name}.{col_name}"] = [val]
                        row_ctx = ctx.with_filters(extra_filters)
                        cond = self._eval_expr(args[1].strip(), row_ctx)
                        if cond:
                            filtered.append(row_item)
                    else:
                        # Single-column row dict from ALL(Table[Column]) or VALUES
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
    # Table functions
    # =========================================================================

    def _fn_topn(self, args_str: str, ctx: DAXContext) -> Any:
        """TOPN(n, table, orderBy, order) — return top N rows."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        n = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(n, (int, float)):
            return []
        n = int(n)
        table_ref = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(table_ref, list):
            return []

        order_expr = args[2].strip() if len(args) > 2 else None
        # order: 1 or ASC = ascending, 0 or DESC = descending (default DESC)
        descending = True
        if len(args) > 3:
            order_val = args[3].strip().upper()
            if order_val in ('1', 'ASC'):
                descending = False

        if order_expr:
            # Evaluate order expression for each row and sort
            scored = []
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
                    score = self._eval_expr(order_expr, row_ctx)
                else:
                    score = self._eval_expr(order_expr, ctx)
                scored.append((row_item, score if isinstance(score, (int, float)) else 0))
            scored.sort(key=lambda x: x[1], reverse=descending)
            return [item for item, _ in scored[:n]]
        else:
            return table_ref[:n]

    def _fn_addcolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """ADDCOLUMNS(table, name, expression, ...) — add computed columns to table."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return table_ref

        # Parse name/expression pairs
        extended = []
        for row_item in table_ref:
            new_item = dict(row_item) if isinstance(row_item, dict) else row_item
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
            else:
                row_ctx = ctx
            # Process name/expression pairs
            i = 1
            while i + 1 < len(args):
                col_name = self._eval_expr(args[i].strip(), ctx)
                col_val = self._eval_expr(args[i + 1].strip(), row_ctx)
                if isinstance(new_item, dict):
                    new_item[str(col_name)] = col_val
                i += 2
            extended.append(new_item)
        return extended

    def _fn_summarize(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMMARIZE(table, groupBy1, groupBy2, ...) — group by columns.
        Returns list of row dicts with distinct combinations of the group-by columns."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_name = args[0].strip().strip("'")
        rows = ctx.get_filtered_rows(table_name)
        tbl = ctx.tables.get(table_name)
        if not tbl or not rows:
            return []

        # Collect group-by column indices
        group_cols = []
        for i in range(1, len(args)):
            ref = self._eval_expr(args[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                col_idx = ctx._find_col_idx(tbl['columns'], ref[1])
                if col_idx >= 0:
                    group_cols.append((ref[1], col_idx))

        if not group_cols:
            return []

        # Get distinct combinations
        seen = set()
        result = []
        for row in rows:
            key = tuple(row[idx] for _, idx in group_cols)
            if key not in seen:
                seen.add(key)
                row_dict = {'__table__': table_name}
                for col_name, col_idx in group_cols:
                    row_dict[col_name] = row[col_idx]
                # Use first group col as the iteration column
                row_dict['__column__'] = group_cols[0][0]
                row_dict['__value__'] = row[group_cols[0][1]]
                result.append(row_dict)
        return result

    def _fn_summarizecolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """SUMMARIZECOLUMNS(groupBy1, ..., name, expression) — summarize with measures.
        Simplified: treats it like SUMMARIZE for the group-by columns."""
        args = self._split_args(args_str)
        if not args:
            return []
        # Find group-by columns (column refs) vs name/expression pairs (string, expression)
        group_refs = []
        for arg in args:
            ref = self._eval_expr(arg.strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                group_refs.append(ref)
            else:
                break  # Rest are name/expression pairs
        if not group_refs:
            return []
        # Use first table as base
        table_name = group_refs[0][0]
        return self._fn_summarize(f"'{table_name}', " + ", ".join(f"'{t}'[{c}]" for t, c in group_refs), ctx)

    def _fn_selectcolumns(self, args_str: str, ctx: DAXContext) -> Any:
        """SELECTCOLUMNS(table, name, expression, ...) — select/rename columns."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
            else:
                row_ctx = ctx
            new_row = {}
            i = 1
            first_name = None
            first_val = None
            while i + 1 < len(args):
                col_name = self._eval_expr(args[i].strip(), ctx)
                col_val = self._eval_expr(args[i + 1].strip(), row_ctx)
                col_name_str = str(col_name) if col_name else f"col_{i}"
                new_row[col_name_str] = col_val
                if first_name is None:
                    first_name = col_name_str
                    first_val = col_val
                i += 2
            if isinstance(row_item, dict) and '__table__' in row_item:
                new_row['__table__'] = row_item['__table__']
                new_row['__column__'] = row_item['__column__']
                new_row['__value__'] = row_item['__value__']
            result.append(new_row)
        return result

    def _fn_distinct(self, args_str: str, ctx: DAXContext) -> Any:
        """DISTINCT(column_or_table) — distinct values, respecting filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = ctx.get_column_data(ref[0], ref[1])
            seen = set()
            result = []
            for v in values:
                key = str(v)
                if key not in seen:
                    seen.add(key)
                    result.append({'__table__': ref[0], '__column__': ref[1], '__value__': v})
            return result
        if isinstance(ref, list):
            # Deduplicate table rows
            seen = set()
            result = []
            for item in ref:
                if isinstance(item, dict) and '__value__' in item:
                    key = str(item['__value__'])
                else:
                    key = str(item)
                if key not in seen:
                    seen.add(key)
                    result.append(item)
            return result
        return []

    def _fn_union(self, args_str: str, ctx: DAXContext) -> Any:
        """UNION(table1, table2) — combine two tables."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        result = []
        if isinstance(t1, list):
            result.extend(t1)
        if isinstance(t2, list):
            result.extend(t2)
        return result

    def _fn_except(self, args_str: str, ctx: DAXContext) -> Any:
        """EXCEPT(table1, table2) — rows in table1 not in table2."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list):
            return []
        if not isinstance(t2, list):
            return t1
        t2_keys = set()
        for item in t2:
            if isinstance(item, dict) and '__value__' in item:
                t2_keys.add(str(item['__value__']))
            else:
                t2_keys.add(str(item))
        result = []
        for item in t1:
            if isinstance(item, dict) and '__value__' in item:
                key = str(item['__value__'])
            else:
                key = str(item)
            if key not in t2_keys:
                result.append(item)
        return result

    def _fn_intersect(self, args_str: str, ctx: DAXContext) -> Any:
        """INTERSECT(table1, table2) — rows in both tables."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list) or not isinstance(t2, list):
            return []
        t2_keys = set()
        for item in t2:
            if isinstance(item, dict) and '__value__' in item:
                t2_keys.add(str(item['__value__']))
            else:
                t2_keys.add(str(item))
        result = []
        for item in t1:
            if isinstance(item, dict) and '__value__' in item:
                key = str(item['__value__'])
            else:
                key = str(item)
            if key in t2_keys:
                result.append(item)
        return result

    def _fn_crossjoin(self, args_str: str, ctx: DAXContext) -> Any:
        """CROSSJOIN(table1, table2) — cartesian product."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        t1 = self._eval_expr(args[0].strip(), ctx)
        t2 = self._eval_expr(args[1].strip(), ctx)
        if not isinstance(t1, list) or not isinstance(t2, list):
            return []
        result = []
        for item1 in t1:
            for item2 in t2:
                merged = {}
                if isinstance(item1, dict):
                    merged.update(item1)
                if isinstance(item2, dict):
                    merged.update({f"_2_{k}": v for k, v in item2.items()})
                result.append(merged)
        return result

    def _fn_datatable(self, args_str: str, ctx: DAXContext) -> Any:
        """DATATABLE(name, type, ..., {{val1, val2}, {val3, val4}}) — create inline table."""
        # Split column definitions from data block
        # Find the outermost { which starts the data rows
        brace_pos = args_str.find('{{')
        if brace_pos < 0:
            brace_pos = args_str.find('{')

        if brace_pos < 0:
            return []

        col_defs_str = args_str[:brace_pos].rstrip().rstrip(',')
        data_block = args_str[brace_pos:]

        # Parse column definitions: "name", TYPE pairs
        col_args = self._split_args(col_defs_str)
        col_names = []
        col_types = []
        i = 0
        while i + 1 < len(col_args):
            name_val = col_args[i].strip().strip('"\'')
            type_val = col_args[i + 1].strip().upper()
            if type_val in ('INTEGER', 'STRING', 'BOOLEAN', 'DOUBLE', 'CURRENCY', 'DATETIME'):
                col_names.append(name_val)
                col_types.append(type_val)
                i += 2
            else:
                break

        if not col_names:
            return []

        # Parse data rows from {{ ... }, { ... }} blocks
        rows = []
        # Extract individual row blocks {val1, val2}
        row_pattern = re.findall(r'\{([^{}]+)\}', data_block)
        for row_str in row_pattern:
            values = [v.strip().strip('"\'') for v in row_str.split(',')]
            if len(values) >= len(col_names):
                row = {}
                for j, cn in enumerate(col_names):
                    raw = values[j]
                    if col_types[j] == 'INTEGER':
                        try: row[cn] = int(float(raw))
                        except: row[cn] = 0
                    elif col_types[j] in ('DOUBLE', 'CURRENCY'):
                        try: row[cn] = float(raw)
                        except: row[cn] = 0.0
                    elif col_types[j] == 'BOOLEAN':
                        row[cn] = raw.upper() in ('TRUE', '1')
                    else:
                        row[cn] = raw
                row['__table__'] = '__datatable__'
                row['__column__'] = col_names[0]
                row['__value__'] = row.get(col_names[0])
                rows.append(row)

        return rows

    def _fn_row(self, args_str: str, ctx: DAXContext) -> Any:
        """ROW(name, expression, ...) — single row table."""
        args = self._split_args(args_str)
        row = {'__table__': '__row__'}
        i = 0
        first_name = None
        while i + 1 < len(args):
            col_name = self._eval_expr(args[i].strip(), ctx)
            col_val = self._eval_expr(args[i + 1].strip(), ctx)
            col_name_str = str(col_name) if col_name else f"col_{i}"
            row[col_name_str] = col_val
            if first_name is None:
                first_name = col_name_str
                row['__column__'] = col_name_str
                row['__value__'] = col_val
            i += 2
        return [row]

    def _fn_treatas(self, args_str: str, ctx: DAXContext) -> Any:
        """TREATAS(table, column1, column2, ...) — apply table values as filter.
        Returns a marker for CALCULATE to process."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        # Extract target column references
        target_cols = []
        for i in range(1, len(args)):
            ref = self._eval_expr(args[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                target_cols.append(ref)
        if isinstance(table_ref, list) and target_cols:
            # Extract values and return as filter marker
            values = []
            for item in table_ref:
                if isinstance(item, dict) and '__value__' in item:
                    values.append(item['__value__'])
            if values and target_cols:
                return ('__TREATAS__', target_cols[0], values)
        return table_ref

    def _fn_generate(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATE(table1, table2_expr) — like CROSS APPLY (inner join behavior)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
            else:
                row_ctx = ctx
            inner = self._eval_expr(args[1].strip(), row_ctx)
            if isinstance(inner, list) and inner:
                for inner_item in inner:
                    merged = {}
                    if isinstance(row_item, dict):
                        merged.update(row_item)
                    if isinstance(inner_item, dict):
                        merged.update({f"_inner_{k}": v for k, v in inner_item.items()})
                    result.append(merged)
        return result

    def _fn_generateall(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATEALL(table1, table2_expr) — like CROSS APPLY (includes empty inner)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        table_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(table_ref, list):
            return []
        result = []
        for row_item in table_ref:
            if isinstance(row_item, dict) and '__table__' in row_item:
                row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
            else:
                row_ctx = ctx
            inner = self._eval_expr(args[1].strip(), row_ctx)
            if isinstance(inner, list) and inner:
                for inner_item in inner:
                    merged = {}
                    if isinstance(row_item, dict):
                        merged.update(row_item)
                    if isinstance(inner_item, dict):
                        merged.update({f"_inner_{k}": v for k, v in inner_item.items()})
                    result.append(merged)
            else:
                # GENERATEALL keeps rows even when inner is empty
                result.append(row_item if isinstance(row_item, dict) else {'__value__': row_item})
        return result

    def _fn_generateseries(self, args_str: str, ctx: DAXContext) -> Any:
        """GENERATESERIES(start, end, step) — generate a single-column table [Value]."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        start_val = self._eval_expr(args[0].strip(), ctx)
        end_val = self._eval_expr(args[1].strip(), ctx)
        step_val = self._eval_expr(args[2].strip(), ctx) if len(args) >= 3 else 1
        try:
            start_val = float(start_val) if start_val is not None else 0
            end_val = float(end_val) if end_val is not None else 0
            step_val = float(step_val) if step_val is not None else 1
        except (TypeError, ValueError):
            return []
        if step_val == 0:
            return []
        # Use int if all values are whole numbers
        use_int = (start_val == int(start_val) and end_val == int(end_val)
                   and step_val == int(step_val))
        rows = []
        current = start_val
        max_rows = 1000000  # safety limit
        if step_val > 0:
            while current <= end_val + 1e-9 and len(rows) < max_rows:
                val = int(current) if use_int else current
                rows.append({'Value': val, '__table__': '__generateseries__',
                             '__column__': 'Value', '__value__': val})
                current += step_val
        elif step_val < 0:
            while current >= end_val - 1e-9 and len(rows) < max_rows:
                val = int(current) if use_int else current
                rows.append({'Value': val, '__table__': '__generateseries__',
                             '__column__': 'Value', '__value__': val})
                current += step_val
        return rows

    # =========================================================================
    # Filter functions
    # =========================================================================

    def _fn_allexcept(self, args_str: str, ctx: DAXContext) -> Any:
        """ALLEXCEPT(table, column1, column2, ...) — remove all filters except on specified columns."""
        args = self._split_args(args_str)
        if not args:
            return ('__ALLEXCEPT__', args_str.strip())
        table_name = args[0].strip().strip("'")
        # Columns to keep
        keep_cols = set()
        for i in range(1, len(args)):
            ref = self._eval_expr(args[i].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                keep_cols.add(f"{ref[0]}.{ref[1]}")

        # When used as table expression, return all rows of table ignoring non-keep filters
        tbl = ctx.tables.get(table_name)
        if tbl:
            # Build a context that only retains filters on the keep columns
            new_filters = {k: v for k, v in ctx.filter_context.items()
                          if k in keep_cols or not k.startswith(f"{table_name}.")}
            new_ctx = DAXContext(ctx.tables, ctx.measures, ctx.date_table,
                                ctx.date_column, new_filters, ctx.relationships)
            rows = new_ctx.get_filtered_rows(table_name)
            cols = tbl['columns']
            if rows and len(cols) > 0:
                result = []
                seen = set()
                for row in rows:
                    key = tuple(str(row[i]) for i in range(len(cols)))
                    if key not in seen:
                        seen.add(key)
                        result.append({'__table__': table_name, '__column__': cols[0], '__value__': row[0]})
                return result
        return ('__ALLEXCEPT__', args_str.strip())

    def _fn_allselected(self, args_str: str, ctx: DAXContext) -> Any:
        """ALLSELECTED(column_or_table) — respect only external (slicer) filters.
        Approximation: returns all distinct values from filtered context (same as VALUES)."""
        # NOTE: True ALLSELECTED requires distinguishing external vs internal filters,
        # which is not tracked in this simplified engine. We approximate with VALUES behavior.
        ref = args_str.strip()
        col_match = re.match(r"'?([^'\[\]]+)'?\s*\[([^\]]+)\]", ref)
        if col_match:
            table_name = col_match.group(1).strip()
            col_name = col_match.group(2).strip()
            values = ctx.get_column_data(table_name, col_name)
            unique = list(set(values))
            return [{'__table__': table_name, '__column__': col_name, '__value__': v} for v in unique]
        return ('__ALLSELECTED__', ref)

    def _fn_keepfilters(self, args_str: str, ctx: DAXContext) -> Any:
        """KEEPFILTERS(expression) — intersect rather than replace filter context.
        Approximation: just evaluate the expression (KEEPFILTERS modifies CALCULATE behavior)."""
        # NOTE: True KEEPFILTERS changes how CALCULATE applies filters (intersection vs replacement).
        # In this simplified engine, we just evaluate the inner expression.
        return self._eval_expr(args_str.strip(), ctx)

    def _fn_hasonevalue(self, args_str: str, ctx: DAXContext) -> Any:
        """HASONEVALUE(column) — check if exactly one distinct value in filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            values = list(set(ctx.get_column_data(ref[0], ref[1])))
            return len(values) == 1
        return False

    def _fn_hasonefilter(self, args_str: str, ctx: DAXContext) -> Any:
        """HASONEFILTER(column) — check if exactly one direct filter on column."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            filter_key = f"{ref[0]}.{ref[1]}"
            if filter_key in ctx.filter_context:
                return len(ctx.filter_context[filter_key]) == 1
        return False

    def _fn_isfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISFILTERED(column_or_table) — check if column/table has any direct filter."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            filter_key = f"{ref[0]}.{ref[1]}"
            return filter_key in ctx.filter_context
        # Table-only reference (string) — check if any filter matches the table
        if isinstance(ref, str):
            table_name = ref.strip("'\"")
            return any(k.split('.')[0] == table_name for k in ctx.filter_context)
        return False

    def _fn_iscrossfiltered(self, args_str: str, ctx: DAXContext) -> Any:
        """ISCROSSFILTERED(column) — check if column is cross-filtered via relationships."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            # Check direct filter
            filter_key = f"{ref[0]}.{ref[1]}"
            if filter_key in ctx.filter_context:
                return True
            # Check cross-table filters
            cross_filters = ctx._get_cross_table_filters(ref[0])
            return len(cross_filters) > 0
        return False

    def _fn_userelationship(self, args_str: str, ctx: DAXContext) -> Any:
        """USERELATIONSHIP(column1, column2) — activate an inactive relationship.
        Returns a marker for CALCULATE to process."""
        # NOTE: Full implementation would require modifying the relationship index.
        # This returns a marker that CALCULATE can interpret.
        return ('__USERELATIONSHIP__', args_str.strip())

    def _fn_earlier(self, args_str: str, ctx: DAXContext) -> Any:
        """EARLIER(column, n) — row context from n levels up.
        Limitation: This engine does not maintain a row context stack.
        Returns the current column value as an approximation."""
        args = self._split_args(args_str)
        ref = self._eval_expr(args[0].strip(), ctx)
        # NOTE: EARLIER requires a row context stack which this engine doesn't maintain.
        # We return the column reference so it can be used in comparisons.
        return ref

    def _fn_earliest(self, args_str: str, ctx: DAXContext) -> Any:
        """EARLIEST(column) — outermost row context.
        Limitation: Same as EARLIER — no row context stack."""
        return self._fn_earlier(args_str, ctx)

    # =========================================================================
    # Math functions
    # =========================================================================

    def _fn_ceiling(self, args_str: str, ctx: DAXContext) -> Any:
        """CEILING(number, significance) — round up to nearest multiple of significance."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        sig = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.ceil(val / sig) * sig
        return val

    def _fn_floor(self, args_str: str, ctx: DAXContext) -> Any:
        """FLOOR(number, significance) — round down to nearest multiple of significance."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        sig = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 1
        if isinstance(val, (int, float)) and isinstance(sig, (int, float)) and sig != 0:
            return math.floor(val / sig) * sig
        return val

    def _fn_mod(self, args_str: str, ctx: DAXContext) -> Any:
        """MOD(number, divisor) — modulo."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        val = self._eval_expr(args[0].strip(), ctx)
        divisor = self._eval_expr(args[1].strip(), ctx)
        if isinstance(val, (int, float)) and isinstance(divisor, (int, float)) and divisor != 0:
            return val % divisor
        return None

    def _fn_power(self, args_str: str, ctx: DAXContext) -> Any:
        """POWER(base, exponent) — exponentiation."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        base = self._eval_expr(args[0].strip(), ctx)
        exp = self._eval_expr(args[1].strip(), ctx)
        if isinstance(base, (int, float)) and isinstance(exp, (int, float)):
            return math.pow(base, exp)
        return None

    def _fn_sqrt(self, args_str: str, ctx: DAXContext) -> Any:
        """SQRT(number) — square root."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.sqrt(val)
        return None

    def _fn_log(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG(number, base) — logarithm with specified base (default 10)."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        base = self._eval_expr(args[1].strip(), ctx) if len(args) > 1 else 10
        if isinstance(val, (int, float)) and val > 0 and isinstance(base, (int, float)) and base > 0:
            return math.log(val, base)
        return None

    def _fn_log10(self, args_str: str, ctx: DAXContext) -> Any:
        """LOG10(number) — base-10 logarithm."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log10(val)
        return None

    def _fn_ln(self, args_str: str, ctx: DAXContext) -> Any:
        """LN(number) — natural logarithm."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val > 0:
            return math.log(val)
        return None

    def _fn_exp(self, args_str: str, ctx: DAXContext) -> Any:
        """EXP(number) — e^x."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            return math.exp(val)
        return None

    def _fn_sign(self, args_str: str, ctx: DAXContext) -> Any:
        """SIGN(number) — returns -1, 0, or 1."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            if val > 0:
                return 1
            elif val < 0:
                return -1
            return 0
        return None

    def _fn_trunc(self, args_str: str, ctx: DAXContext) -> Any:
        """TRUNC(number, digits) — truncate to specified decimal places."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        digits = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 0
        if isinstance(val, (int, float)):
            multiplier = 10 ** digits
            return int(val * multiplier) / multiplier
        return None

    def _fn_even(self, args_str: str, ctx: DAXContext) -> Any:
        """EVEN(number) — round up to nearest even integer."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 != 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_odd(self, args_str: str, ctx: DAXContext) -> Any:
        """ODD(number) — round up to nearest odd integer."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            result = math.ceil(abs(val))
            if result % 2 == 0:
                result += 1
            return result if val >= 0 else -result
        return None

    def _fn_fact(self, args_str: str, ctx: DAXContext) -> Any:
        """FACT(number) — factorial."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)) and val >= 0:
            return math.factorial(int(val))
        return None

    def _fn_gcd(self, args_str: str, ctx: DAXContext) -> Any:
        """GCD(a, b) — greatest common divisor."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._eval_expr(args[0].strip(), ctx)
        b = self._eval_expr(args[1].strip(), ctx)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return math.gcd(int(a), int(b))
        return None

    def _fn_lcm(self, args_str: str, ctx: DAXContext) -> Any:
        """LCM(a, b) — least common multiple."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        a = self._eval_expr(args[0].strip(), ctx)
        b = self._eval_expr(args[1].strip(), ctx)
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            a_int, b_int = int(a), int(b)
            if a_int == 0 or b_int == 0:
                return 0
            return abs(a_int * b_int) // math.gcd(a_int, b_int)
        return None

    def _fn_rand(self, args_str: str, ctx: DAXContext) -> Any:
        """RAND() — random number between 0 and 1."""
        return random.random()

    def _fn_randbetween(self, args_str: str, ctx: DAXContext) -> Any:
        """RANDBETWEEN(min, max) — random integer between min and max."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        lo = self._eval_expr(args[0].strip(), ctx)
        hi = self._eval_expr(args[1].strip(), ctx)
        if isinstance(lo, (int, float)) and isinstance(hi, (int, float)):
            return random.randint(int(lo), int(hi))
        return 0

    def _fn_pi(self, args_str: str, ctx: DAXContext) -> Any:
        """PI() — returns 3.14159..."""
        return math.pi

    def _fn_currency(self, args_str: str, ctx: DAXContext) -> Any:
        """CURRENCY(value) — convert to currency (fixed-point decimal, 4 decimal places)."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            return round(val, 4)
        return 0

    def _fn_fixed(self, args_str: str, ctx: DAXContext) -> Any:
        """FIXED(number, decimals, no_commas) — format number as text with fixed decimals."""
        args = self._split_args(args_str)
        val = self._eval_expr(args[0].strip(), ctx)
        decimals = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 2
        no_commas = self._eval_expr(args[2].strip(), ctx) if len(args) > 2 else False
        if isinstance(val, (int, float)):
            if no_commas:
                return f"{val:.{decimals}f}"
            return f"{val:,.{decimals}f}"
        return str(val)

    # =========================================================================
    # Text functions
    # =========================================================================

    def _fn_left(self, args_str: str, ctx: DAXContext) -> Any:
        """LEFT(text, n) — leftmost n characters."""
        args = self._split_args(args_str)
        text = self._eval_expr(args[0].strip(), ctx)
        n = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 1
        if text is not None:
            return str(text)[:n]
        return ''

    def _fn_right(self, args_str: str, ctx: DAXContext) -> Any:
        """RIGHT(text, n) — rightmost n characters."""
        args = self._split_args(args_str)
        text = self._eval_expr(args[0].strip(), ctx)
        n = int(self._eval_expr(args[1].strip(), ctx)) if len(args) > 1 else 1
        if text is not None:
            s = str(text)
            return s[-n:] if n <= len(s) else s
        return ''

    def _fn_mid(self, args_str: str, ctx: DAXContext) -> Any:
        """MID(text, start, n) — substring from start position (1-based) for n characters."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return ''
        text = self._eval_expr(args[0].strip(), ctx)
        start = int(self._eval_expr(args[1].strip(), ctx))
        n = int(self._eval_expr(args[2].strip(), ctx))
        if text is not None:
            s = str(text)
            return s[start - 1:start - 1 + n]  # DAX uses 1-based indexing
        return ''

    def _fn_len(self, args_str: str, ctx: DAXContext) -> Any:
        """LEN(text) — length of text."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            return len(str(val))
        return 0

    def _fn_upper(self, args_str: str, ctx: DAXContext) -> Any:
        """UPPER(text) — convert to uppercase."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).upper() if val is not None else ''

    def _fn_lower(self, args_str: str, ctx: DAXContext) -> Any:
        """LOWER(text) — convert to lowercase."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).lower() if val is not None else ''

    def _fn_proper(self, args_str: str, ctx: DAXContext) -> Any:
        """PROPER(text) — capitalize first letter of each word."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).title() if val is not None else ''

    def _fn_trim(self, args_str: str, ctx: DAXContext) -> Any:
        """TRIM(text) — remove leading/trailing spaces."""
        val = self._eval_expr(args_str.strip(), ctx)
        return str(val).strip() if val is not None else ''

    def _fn_substitute(self, args_str: str, ctx: DAXContext) -> Any:
        """SUBSTITUTE(text, old, new, instance) — replace text occurrences."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        old = str(self._eval_expr(args[1].strip(), ctx) or '')
        new = str(self._eval_expr(args[2].strip(), ctx) or '')
        if len(args) > 3:
            instance = int(self._eval_expr(args[3].strip(), ctx) or 1)
            # Replace only the nth occurrence
            count = 0
            result = []
            i = 0
            while i < len(text):
                if text[i:i + len(old)] == old:
                    count += 1
                    if count == instance:
                        result.append(new)
                        i += len(old)
                        continue
                result.append(text[i])
                i += 1
            return ''.join(result)
        return text.replace(old, new)

    def _fn_replace(self, args_str: str, ctx: DAXContext) -> Any:
        """REPLACE(text, start, n, new) — replace by position."""
        args = self._split_args(args_str)
        if len(args) < 4:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        start = int(self._eval_expr(args[1].strip(), ctx)) - 1  # DAX is 1-based
        n = int(self._eval_expr(args[2].strip(), ctx))
        new = str(self._eval_expr(args[3].strip(), ctx) or '')
        return text[:start] + new + text[start + n:]

    def _fn_rept(self, args_str: str, ctx: DAXContext) -> Any:
        """REPT(text, n) — repeat text n times."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        text = str(self._eval_expr(args[0].strip(), ctx) or '')
        n = int(self._eval_expr(args[1].strip(), ctx) or 0)
        return text * max(0, n)

    def _fn_search(self, args_str: str, ctx: DAXContext) -> Any:
        """SEARCH(find, within, start) — find position (case-insensitive, 1-based). Returns -1 if not found."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return -1
        find_text = str(self._eval_expr(args[0].strip(), ctx) or '').lower()
        within_text = str(self._eval_expr(args[1].strip(), ctx) or '').lower()
        start = int(self._eval_expr(args[2].strip(), ctx)) - 1 if len(args) > 2 else 0
        pos = within_text.find(find_text, start)
        return pos + 1 if pos >= 0 else -1  # DAX returns 1-based

    def _fn_find(self, args_str: str, ctx: DAXContext) -> Any:
        """FIND(find, within, start) — find position (case-sensitive, 1-based). Returns -1 if not found."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return -1
        find_text = str(self._eval_expr(args[0].strip(), ctx) or '')
        within_text = str(self._eval_expr(args[1].strip(), ctx) or '')
        start = int(self._eval_expr(args[2].strip(), ctx)) - 1 if len(args) > 2 else 0
        pos = within_text.find(find_text, start)
        return pos + 1 if pos >= 0 else -1

    def _fn_containsstring(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINSSTRING(within, find) — case-insensitive contains check."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        within = str(self._eval_expr(args[0].strip(), ctx) or '').lower()
        find = str(self._eval_expr(args[1].strip(), ctx) or '').lower()
        return find in within

    def _fn_containsstringexact(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINSSTRINGEXACT(within, find) — case-sensitive contains check."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        within = str(self._eval_expr(args[0].strip(), ctx) or '')
        find = str(self._eval_expr(args[1].strip(), ctx) or '')
        return find in within

    def _fn_exact(self, args_str: str, ctx: DAXContext) -> Any:
        """EXACT(text1, text2) — case-sensitive string comparison."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        t1 = str(self._eval_expr(args[0].strip(), ctx) or '')
        t2 = str(self._eval_expr(args[1].strip(), ctx) or '')
        return t1 == t2

    def _fn_unichar(self, args_str: str, ctx: DAXContext) -> Any:
        """UNICHAR(number) — return unicode character for code point."""
        val = self._eval_expr(args_str.strip(), ctx)
        if isinstance(val, (int, float)):
            try:
                return chr(int(val))
            except (ValueError, OverflowError):
                return ''
        return ''

    def _fn_unicode(self, args_str: str, ctx: DAXContext) -> Any:
        """UNICODE(text) — return unicode code point of first character."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            s = str(val)
            if s:
                return ord(s[0])
        return 0

    def _fn_value(self, args_str: str, ctx: DAXContext) -> Any:
        """VALUE(text) — convert text to number."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is None:
            return 0
        try:
            s = str(val).replace(',', '').replace('$', '').replace('%', '').strip()
            if '.' in s:
                return float(s)
            return int(s)
        except (ValueError, TypeError):
            return 0

    def _fn_combinevalues(self, args_str: str, ctx: DAXContext) -> Any:
        """COMBINEVALUES(delimiter, value1, value2, ...) — join values with delimiter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        delimiter = str(self._eval_expr(args[0].strip(), ctx) or '')
        parts = [str(self._eval_expr(a.strip(), ctx) or '') for a in args[1:]]
        return delimiter.join(parts)

    def _fn_concatenatex(self, args_str: str, ctx: DAXContext) -> Any:
        """CONCATENATEX(table, expression, delimiter) — iterate table, evaluate expression per row, join with delimiter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        table_ref = self._eval_expr(args[0].strip(), ctx)
        row_expr = args[1].strip()
        delimiter = str(self._eval_expr(args[2].strip(), ctx) or '') if len(args) > 2 else ''

        parts = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
                    result = self._eval_expr(row_expr, row_ctx)
                    result = self._resolve_row_result(result, row_item, row_ctx)
                    if result is not None:
                        parts.append(str(result))
                else:
                    result = self._eval_expr(row_expr, ctx)
                    if result is not None:
                        parts.append(str(result))
        return delimiter.join(parts)

    def _fn_rankx(self, args_str: str, ctx: DAXContext) -> Any:
        """RANKX(table, expression, value, order, ties) — rank a value within a table's evaluated expression."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        table_ref = self._eval_expr(args[0].strip(), ctx)
        rank_expr = args[1].strip()
        # Value to rank (optional — defaults to the expression evaluated in current context)
        value_expr = args[2].strip() if len(args) > 2 else None
        order_str = args[3].strip().upper() if len(args) > 3 else 'DESC'
        is_desc = 'DESC' in order_str

        # Get the value to rank
        if value_expr:
            current_val = self._eval_expr(value_expr, ctx)
        else:
            current_val = self._eval_expr(rank_expr, ctx)

        if not isinstance(current_val, (int, float)):
            return None

        # Evaluate expression for all rows in the table
        all_vals = []
        if isinstance(table_ref, list):
            for row_item in table_ref:
                if isinstance(row_item, dict) and '__table__' in row_item:
                    row_ctx = ctx.with_filters({f"{row_item['__table__']}.{row_item['__column__']}": [row_item['__value__']]})
                    result = self._eval_expr(rank_expr, row_ctx)
                    if isinstance(result, (int, float)):
                        all_vals.append(result)

        if not all_vals:
            return 1

        # Sort and find rank
        if is_desc:
            all_vals.sort(reverse=True)
        else:
            all_vals.sort()

        # Dense ranking
        unique_sorted = sorted(set(all_vals), reverse=is_desc)
        for i, v in enumerate(unique_sorted):
            if abs(v - current_val) < 0.0001:
                return i + 1
        return len(unique_sorted) + 1

    def _fn_pathcontains(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHCONTAINS(path, item) — check if pipe-delimited path contains item."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return False
        path = str(self._eval_expr(args[0].strip(), ctx) or '')
        item = str(self._eval_expr(args[1].strip(), ctx) or '')
        return item in path.split('|')

    def _fn_pathitem(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHITEM(path, position, type) — get item at position in pipe-delimited path (1-based)."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return ''
        path = str(self._eval_expr(args[0].strip(), ctx) or '')
        pos = int(self._eval_expr(args[1].strip(), ctx) or 1)
        parts = path.split('|')
        if 1 <= pos <= len(parts):
            return parts[pos - 1]
        return ''

    def _fn_pathlength(self, args_str: str, ctx: DAXContext) -> Any:
        """PATHLENGTH(path) — count items in pipe-delimited path."""
        val = self._eval_expr(args_str.strip(), ctx)
        if val is not None:
            path = str(val)
            if path:
                return len(path.split('|'))
        return 0

    # =========================================================================
    # Logical functions
    # =========================================================================

    def _fn_true(self, args_str: str, ctx: DAXContext) -> Any:
        """TRUE() — boolean true."""
        return True

    def _fn_false(self, args_str: str, ctx: DAXContext) -> Any:
        """FALSE() — boolean false."""
        return False

    def _fn_iferror(self, args_str: str, ctx: DAXContext) -> Any:
        """IFERROR(expression, fallback) — return fallback if expression errors."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return None
        try:
            result = self._eval_expr(args[0].strip(), ctx)
            if result is None:
                return self._eval_expr(args[1].strip(), ctx)
            return result
        except Exception:
            return self._eval_expr(args[1].strip(), ctx)

    def _fn_coalesce(self, args_str: str, ctx: DAXContext) -> Any:
        """COALESCE(value1, value2, ...) — return first non-blank value."""
        args = self._split_args(args_str)
        for arg in args:
            val = self._eval_expr(arg.strip(), ctx)
            if val is not None and val != '':
                return val
        return None

    def _fn_contains(self, args_str: str, ctx: DAXContext) -> Any:
        """CONTAINS(table, column, value, ...) — check if table contains a row with specified values."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return False
        table_name = args[0].strip().strip("'")
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return False

        # Parse column/value pairs
        criteria = []
        i = 1
        while i + 1 < len(args):
            ref = self._eval_expr(args[i].strip(), ctx)
            value = self._eval_expr(args[i + 1].strip(), ctx)
            if isinstance(ref, tuple) and len(ref) == 2:
                col_idx = ctx._find_col_idx(tbl['columns'], ref[1])
                if col_idx >= 0:
                    criteria.append((col_idx, value))
            i += 2

        if not criteria:
            return False

        rows = ctx.get_filtered_rows(table_name)
        for row in rows:
            match = True
            for col_idx, value in criteria:
                if str(row[col_idx]) != str(value):
                    match = False
                    break
            if match:
                return True
        return False

    # =========================================================================
    # Information functions
    # =========================================================================

    def _fn_isnumber(self, args_str: str, ctx: DAXContext) -> Any:
        """ISNUMBER(value) — check if value is numeric."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, (int, float))

    def _fn_istext(self, args_str: str, ctx: DAXContext) -> Any:
        """ISTEXT(value) — check if value is text."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, str)

    def _fn_isnontext(self, args_str: str, ctx: DAXContext) -> Any:
        """ISNONTEXT(value) — check if value is not text."""
        val = self._eval_expr(args_str.strip(), ctx)
        return not isinstance(val, str)

    def _fn_islogical(self, args_str: str, ctx: DAXContext) -> Any:
        """ISLOGICAL(value) — check if value is boolean."""
        val = self._eval_expr(args_str.strip(), ctx)
        return isinstance(val, bool)

    def _fn_iserror(self, args_str: str, ctx: DAXContext) -> Any:
        """ISERROR(value) — check if expression results in error."""
        try:
            val = self._eval_expr(args_str.strip(), ctx)
            return val is None
        except Exception:
            return True

    def _fn_username(self, args_str: str, ctx: DAXContext) -> Any:
        """USERNAME() — returns empty string (server-side function)."""
        return ''

    def _fn_userprincipalname(self, args_str: str, ctx: DAXContext) -> Any:
        """USERPRINCIPALNAME() — returns empty string (server-side function)."""
        return ''

    def _fn_lookupvalue(self, args_str: str, ctx: DAXContext) -> Any:
        """LOOKUPVALUE(result_column, search_column, search_value, ...) — vlookup equivalent."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return None

        result_ref = self._eval_expr(args[0].strip(), ctx)
        if not isinstance(result_ref, tuple) or len(result_ref) != 2:
            return None

        table_name, result_col = result_ref
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return None

        result_col_idx = ctx._find_col_idx(tbl['columns'], result_col)
        if result_col_idx < 0:
            return None

        # Build search criteria: pairs of (column_ref, value)
        criteria = []
        i = 1
        while i + 1 < len(args):
            search_ref = self._eval_expr(args[i].strip(), ctx)
            search_val = self._eval_expr(args[i + 1].strip(), ctx)
            if isinstance(search_ref, tuple) and len(search_ref) == 2:
                col_idx = ctx._find_col_idx(tbl['columns'], search_ref[1])
                if col_idx >= 0:
                    criteria.append((col_idx, search_val))
            i += 2

        if not criteria:
            return None

        # Search through all rows (ignoring filter context for lookup)
        for row in tbl['rows']:
            match = True
            for col_idx, search_val in criteria:
                if str(row[col_idx]) != str(search_val):
                    match = False
                    break
            if match:
                return row[result_col_idx]

        # Return alternate value if provided
        if len(args) > 1 + len(criteria) * 2:
            return self._eval_expr(args[-1].strip(), ctx)
        return None

    # =========================================================================
    # Relationship functions
    # =========================================================================

    def _fn_related(self, args_str: str, ctx: DAXContext) -> Any:
        """RELATED(column) — follow relationship to get a value from a related table.
        Approximation: looks up value via relationship index and current filter context."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if not isinstance(ref, tuple) or len(ref) != 2:
            return None
        target_table, target_col = ref
        # Try to find a related value via relationships
        tbl = ctx.tables.get(target_table)
        if not tbl:
            return None
        target_col_idx = ctx._find_col_idx(tbl['columns'], target_col)
        if target_col_idx < 0:
            return None
        # Get filtered rows from the target table
        rows = ctx.get_filtered_rows(target_table)
        if rows:
            return rows[0][target_col_idx]
        return None

    def _fn_relatedtable(self, args_str: str, ctx: DAXContext) -> Any:
        """RELATEDTABLE(table) — follow relationship to get filtered related table rows."""
        table_name = args_str.strip().strip("'")
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return []
        rows = ctx.get_filtered_rows(table_name)
        cols = tbl['columns']
        result = []
        for row in rows:
            row_dict = {'__table__': table_name, '__column__': cols[0] if cols else '', '__value__': row[0] if row else None}
            for i, col in enumerate(cols):
                row_dict[col] = row[i]
            result.append(row_dict)
        return result

    def _fn_crossfilter(self, args_str: str, ctx: DAXContext) -> Any:
        """CROSSFILTER(column1, column2, direction) — modify cross-filter direction.
        Returns a marker for CALCULATE to process."""
        # NOTE: Full implementation would modify relationship filter propagation direction.
        return ('__CROSSFILTER__', args_str.strip())

    # =========================================================================
    # Time Intelligence functions — helpers
    # =========================================================================

    def _parse_date(self, val) -> Optional[datetime]:
        """Try to parse a value as a date."""
        if isinstance(val, datetime):
            return val
        if isinstance(val, date):
            return datetime(val.year, val.month, val.day)
        if isinstance(val, str):
            for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M:%S', '%m/%d/%Y %H:%M:%S'):
                try:
                    return datetime.strptime(val, fmt)
                except ValueError:
                    continue
        if isinstance(val, (int, float)):
            # Excel serial date: days since 1899-12-30
            try:
                return datetime(1899, 12, 30) + timedelta(days=int(val))
            except (ValueError, OverflowError):
                pass
        return None

    def _get_date_column_dates(self, args_str: str, ctx: DAXContext) -> tuple:
        """Parse a date column reference and return (table_name, col_name, list_of_dates).
        Returns (table_name, col_name, dates) where dates are datetime objects."""
        ref = self._eval_expr(args_str.strip(), ctx)
        if isinstance(ref, tuple) and len(ref) == 2:
            table_name, col_name = ref
            raw_values = ctx.get_column_data(table_name, col_name)
            dates = []
            for v in raw_values:
                d = self._parse_date(v)
                if d:
                    dates.append(d)
            return table_name, col_name, dates
        return None, None, []

    def _make_date_table_result(self, table_name: str, col_name: str, dates: list) -> list:
        """Convert a list of datetime objects into the standard row-dict format."""
        result = []
        for d in dates:
            result.append({
                '__table__': table_name,
                '__column__': col_name,
                '__value__': d.strftime('%Y-%m-%d')
            })
        return result

    def _get_all_date_table_dates(self, table_name: str, col_name: str, ctx: DAXContext) -> list:
        """Get ALL dates from the date table (ignoring filter context)."""
        tbl = ctx.tables.get(table_name)
        if not tbl:
            return []
        col_idx = ctx._find_col_idx(tbl['columns'], col_name)
        if col_idx < 0:
            return []
        dates = []
        for row in tbl['rows']:
            d = self._parse_date(row[col_idx])
            if d:
                dates.append(d)
        return dates

    # =========================================================================
    # Time Intelligence functions — Date Sets
    # =========================================================================

    def _fn_datesytd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESYTD(dates, yearEndDate) — year to date dates."""
        args = self._split_args(args_str)
        table_name, col_name, dates = self._get_date_column_dates(args[0].strip(), ctx)
        if not dates:
            return []
        # Year end date (default Dec 31)
        year_end_month = 12
        year_end_day = 31
        if len(args) > 1:
            ye = self._eval_expr(args[1].strip(), ctx)
            if isinstance(ye, str):
                try:
                    parts = ye.split('/')
                    if len(parts) == 2:
                        year_end_month = int(parts[0])
                        year_end_day = int(parts[1])
                except ValueError:
                    pass

        max_date = max(dates)
        # YTD: from start of fiscal year to max_date
        if year_end_month == 12 and year_end_day == 31:
            year_start = datetime(max_date.year, 1, 1)
        else:
            # Fiscal year
            if max_date.month > year_end_month or (max_date.month == year_end_month and max_date.day > year_end_day):
                year_start = datetime(max_date.year, year_end_month, year_end_day) + timedelta(days=1)
            else:
                year_start = datetime(max_date.year - 1, year_end_month, year_end_day) + timedelta(days=1)

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        ytd = [d for d in all_dates if year_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, ytd)

    def _fn_datesmtd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESMTD(dates) — month to date dates."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        month_start = datetime(max_date.year, max_date.month, 1)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        mtd = [d for d in all_dates if month_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, mtd)

    def _fn_datesqtd(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESQTD(dates) — quarter to date dates."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        quarter_start_month = ((max_date.month - 1) // 3) * 3 + 1
        quarter_start = datetime(max_date.year, quarter_start_month, 1)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        qtd = [d for d in all_dates if quarter_start <= d <= max_date]
        return self._make_date_table_result(table_name, col_name, qtd)

    def _fn_totalytd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALYTD(expression, dates, filter, yearEndDate) — year to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        # Get YTD dates
        ytd_dates = self._fn_datesytd(', '.join(args[1:]), ctx)
        if not ytd_dates:
            return self._eval_expr(expr, ctx)
        # Apply date filter
        if ytd_dates and isinstance(ytd_dates, list) and ytd_dates:
            first = ytd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in ytd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    def _fn_totalmtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALMTD(expression, dates) — month to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        mtd_dates = self._fn_datesmtd(args[1].strip(), ctx)
        if mtd_dates and isinstance(mtd_dates, list) and mtd_dates:
            first = mtd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in mtd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    def _fn_totalqtd(self, args_str: str, ctx: DAXContext) -> Any:
        """TOTALQTD(expression, dates) — quarter to date total."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        qtd_dates = self._fn_datesqtd(args[1].strip(), ctx)
        if qtd_dates and isinstance(qtd_dates, list) and qtd_dates:
            first = qtd_dates[0]
            if isinstance(first, dict) and '__table__' in first:
                date_values = [item['__value__'] for item in qtd_dates]
                new_ctx = ctx.with_filters({f"{first['__table__']}.{first['__column__']}": date_values})
                return self._eval_expr(expr, new_ctx)
        return self._eval_expr(expr, ctx)

    # =========================================================================
    # Time Intelligence — Period Navigation
    # =========================================================================

    def _fn_previousmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSMONTH(dates) — dates from the previous month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        # Previous month
        if max_date.month == 1:
            prev_year, prev_month = max_date.year - 1, 12
        else:
            prev_year, prev_month = max_date.year, max_date.month - 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if d.year == prev_year and d.month == prev_month]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_previousquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSQUARTER(dates) — dates from the previous quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        current_q = (max_date.month - 1) // 3 + 1
        if current_q == 1:
            prev_q_start = datetime(max_date.year - 1, 10, 1)
            prev_q_end = datetime(max_date.year - 1, 12, 31)
        else:
            prev_q_start_month = (current_q - 2) * 3 + 1
            prev_q_end_month = (current_q - 1) * 3
            prev_q_start = datetime(max_date.year, prev_q_start_month, 1)
            _, last_day = monthrange(max_date.year, prev_q_end_month)
            prev_q_end = datetime(max_date.year, prev_q_end_month, last_day)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if prev_q_start <= d <= prev_q_end]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_previousyear(self, args_str: str, ctx: DAXContext) -> Any:
        """PREVIOUSYEAR(dates) — dates from the previous year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        prev_year = max_date.year - 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        prev = [d for d in all_dates if d.year == prev_year]
        return self._make_date_table_result(table_name, col_name, prev)

    def _fn_nextmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTMONTH(dates) — dates from the next month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        if max_date.month == 12:
            next_year, next_month = max_date.year + 1, 1
        else:
            next_year, next_month = max_date.year, max_date.month + 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if d.year == next_year and d.month == next_month]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_nextquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTQUARTER(dates) — dates from the next quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        current_q = (max_date.month - 1) // 3 + 1
        if current_q == 4:
            nq_start = datetime(max_date.year + 1, 1, 1)
            nq_end = datetime(max_date.year + 1, 3, 31)
        else:
            nq_start_month = current_q * 3 + 1
            nq_end_month = (current_q + 1) * 3
            nq_start = datetime(max_date.year, nq_start_month, 1)
            _, last_day = monthrange(max_date.year, nq_end_month)
            nq_end = datetime(max_date.year, nq_end_month, last_day)
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if nq_start <= d <= nq_end]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_nextyear(self, args_str: str, ctx: DAXContext) -> Any:
        """NEXTYEAR(dates) — dates from the next year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        next_year = max_date.year + 1
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        nxt = [d for d in all_dates if d.year == next_year]
        return self._make_date_table_result(table_name, col_name, nxt)

    def _fn_parallelperiod(self, args_str: str, ctx: DAXContext) -> Any:
        """PARALLELPERIOD(dates, offset, interval) — shift dates by offset intervals."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_name, col_name, dates = self._get_date_column_dates(args[0].strip(), ctx)
        if not dates:
            return []
        offset = self._eval_expr(args[1].strip(), ctx)
        interval = args[2].strip().upper()
        if not isinstance(offset, (int, float)):
            return []
        offset = int(offset)

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        min_date = min(dates)
        max_date = max(dates)

        if interval in ('YEAR', 'YEARS'):
            shifted = [d for d in all_dates
                       if datetime(min_date.year + offset, min_date.month, 1) <= d <=
                          datetime(max_date.year + offset, max_date.month,
                                   monthrange(max_date.year + offset, max_date.month)[1])]
        elif interval in ('QUARTER', 'QUARTERS'):
            def shift_quarter(d, off):
                new_month = d.month + off * 3
                new_year = d.year + (new_month - 1) // 12
                new_month = ((new_month - 1) % 12) + 1
                return datetime(new_year, new_month, 1)
            q_start = shift_quarter(datetime(min_date.year, ((min_date.month - 1) // 3) * 3 + 1, 1), offset)
            q_end_month = q_start.month + 2
            q_end_year = q_start.year
            if q_end_month > 12:
                q_end_month -= 12
                q_end_year += 1
            _, last_day = monthrange(q_end_year, q_end_month)
            q_end = datetime(q_end_year, q_end_month, last_day)
            shifted = [d for d in all_dates if q_start <= d <= q_end]
        elif interval in ('MONTH', 'MONTHS'):
            def shift_month(d, off):
                new_month = d.month + off
                new_year = d.year + (new_month - 1) // 12
                new_month = ((new_month - 1) % 12) + 1
                return new_year, new_month
            sy, sm = shift_month(min_date, offset)
            ey, em = shift_month(max_date, offset)
            start = datetime(sy, sm, 1)
            _, last_day = monthrange(ey, em)
            end = datetime(ey, em, last_day)
            shifted = [d for d in all_dates if start <= d <= end]
        elif interval in ('DAY', 'DAYS'):
            delta = timedelta(days=offset)
            start = min_date + delta
            end = max_date + delta
            shifted = [d for d in all_dates if start <= d <= end]
        else:
            shifted = []

        return self._make_date_table_result(table_name, col_name, shifted)

    # =========================================================================
    # Time Intelligence — Start/End of Period
    # =========================================================================

    def _fn_startofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFMONTH(dates) — first date of the month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        start = datetime(min_date.year, min_date.month, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofmonth(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFMONTH(dates) — last date of the month."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        _, last_day = monthrange(max_date.year, max_date.month)
        end = datetime(max_date.year, max_date.month, last_day)
        return self._make_date_table_result(table_name, col_name, [end])

    def _fn_startofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFQUARTER(dates) — first date of the quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        q_start_month = ((min_date.month - 1) // 3) * 3 + 1
        start = datetime(min_date.year, q_start_month, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofquarter(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFQUARTER(dates) — last date of the quarter."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        q_end_month = ((max_date.month - 1) // 3 + 1) * 3
        _, last_day = monthrange(max_date.year, q_end_month)
        end = datetime(max_date.year, q_end_month, last_day)
        return self._make_date_table_result(table_name, col_name, [end])

    def _fn_startofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """STARTOFYEAR(dates) — first date of the year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        min_date = min(dates)
        start = datetime(min_date.year, 1, 1)
        return self._make_date_table_result(table_name, col_name, [start])

    def _fn_endofyear(self, args_str: str, ctx: DAXContext) -> Any:
        """ENDOFYEAR(dates) — last date of the year."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        max_date = max(dates)
        end = datetime(max_date.year, 12, 31)
        return self._make_date_table_result(table_name, col_name, [end])

    # =========================================================================
    # Time Intelligence — Opening/Closing Balance
    # =========================================================================

    def _fn_openingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEMONTH(expression, dates, filter) — evaluate at last date of previous month."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        # End of previous month
        eop = datetime(min_date.year, min_date.month, 1) - timedelta(days=1)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eop.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalancemonth(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEMONTH(expression, dates, filter) — evaluate at last date of current month."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        _, last_day = monthrange(max_date.year, max_date.month)
        eom = datetime(max_date.year, max_date.month, last_day)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eom.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_openingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEQUARTER(expression, dates, filter) — evaluate at last date before quarter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        q_start_month = ((min_date.month - 1) // 3) * 3 + 1
        eoq = datetime(min_date.year, q_start_month, 1) - timedelta(days=1)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoq.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalancequarter(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEQUARTER(expression, dates, filter) — evaluate at last date of quarter."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        q_end_month = ((max_date.month - 1) // 3 + 1) * 3
        _, last_day = monthrange(max_date.year, q_end_month)
        eoq = datetime(max_date.year, q_end_month, last_day)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoq.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_openingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """OPENINGBALANCEYEAR(expression, dates, filter) — evaluate at last date of previous year."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        min_date = min(dates)
        eoy = datetime(min_date.year - 1, 12, 31)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoy.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    def _fn_closingbalanceyear(self, args_str: str, ctx: DAXContext) -> Any:
        """CLOSINGBALANCEYEAR(expression, dates, filter) — evaluate at last date of year."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return 0
        expr = args[0].strip()
        table_name, col_name, dates = self._get_date_column_dates(args[1].strip(), ctx)
        if not dates:
            return self._eval_expr(expr, ctx)
        max_date = max(dates)
        eoy = datetime(max_date.year, 12, 31)
        new_ctx = ctx.with_filters({f"{table_name}.{col_name}": [eoy.strftime('%Y-%m-%d')]})
        return self._eval_expr(expr, new_ctx)

    # =========================================================================
    # Time Intelligence — Date Range Functions
    # =========================================================================

    def _fn_firstdate(self, args_str: str, ctx: DAXContext) -> Any:
        """FIRSTDATE(dates) — earliest date in filter context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        earliest = min(dates)
        return self._make_date_table_result(table_name, col_name, [earliest])

    def _fn_lastdate(self, args_str: str, ctx: DAXContext) -> Any:
        """LASTDATE(dates) — latest date in filter context."""
        table_name, col_name, dates = self._get_date_column_dates(args_str.strip(), ctx)
        if not dates:
            return []
        latest = max(dates)
        return self._make_date_table_result(table_name, col_name, [latest])

    def _fn_datesbetween(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESBETWEEN(dates, start, end) — dates between start and end."""
        args = self._split_args(args_str)
        if len(args) < 3:
            return []
        table_name, col_name, _ = self._get_date_column_dates(args[0].strip(), ctx)
        if not table_name:
            return []
        start_val = self._eval_expr(args[1].strip(), ctx)
        end_val = self._eval_expr(args[2].strip(), ctx)
        start_date = self._parse_date(start_val)
        end_date = self._parse_date(end_val)
        if not start_date or not end_date:
            return []
        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        between = [d for d in all_dates if start_date <= d <= end_date]
        return self._make_date_table_result(table_name, col_name, between)

    def _fn_datesinperiod(self, args_str: str, ctx: DAXContext) -> Any:
        """DATESINPERIOD(dates, start, offset, interval) — dates in a period from start."""
        args = self._split_args(args_str)
        if len(args) < 4:
            return []
        table_name, col_name, _ = self._get_date_column_dates(args[0].strip(), ctx)
        if not table_name:
            return []
        start_val = self._eval_expr(args[1].strip(), ctx)
        offset = self._eval_expr(args[2].strip(), ctx)
        interval = args[3].strip().upper()
        start_date = self._parse_date(start_val)
        if not start_date or not isinstance(offset, (int, float)):
            return []
        offset = int(offset)

        if interval in ('DAY', 'DAYS'):
            if offset >= 0:
                end_date = start_date + timedelta(days=offset - 1)
            else:
                end_date = start_date
                start_date = start_date + timedelta(days=offset + 1)
        elif interval in ('MONTH', 'MONTHS'):
            new_month = start_date.month + offset
            new_year = start_date.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            if offset >= 0:
                _, last_day = monthrange(new_year, new_month)
                end_date = datetime(new_year, new_month, min(start_date.day, last_day))
            else:
                end_date = start_date
                _, last_day = monthrange(new_year, new_month)
                start_date = datetime(new_year, new_month, min(start_date.day, last_day))
        elif interval in ('QUARTER', 'QUARTERS'):
            new_month = start_date.month + offset * 3
            new_year = start_date.year + (new_month - 1) // 12
            new_month = ((new_month - 1) % 12) + 1
            if offset >= 0:
                _, last_day = monthrange(new_year, new_month)
                end_date = datetime(new_year, new_month, min(start_date.day, last_day))
            else:
                end_date = start_date
                _, last_day = monthrange(new_year, new_month)
                start_date = datetime(new_year, new_month, min(start_date.day, last_day))
        elif interval in ('YEAR', 'YEARS'):
            if offset >= 0:
                end_date = datetime(start_date.year + offset, start_date.month, start_date.day)
            else:
                end_date = start_date
                start_date = datetime(start_date.year + offset, start_date.month, start_date.day)
        else:
            return []

        if start_date > end_date:
            start_date, end_date = end_date, start_date

        all_dates = self._get_all_date_table_dates(table_name, col_name, ctx)
        in_period = [d for d in all_dates if start_date <= d <= end_date]
        return self._make_date_table_result(table_name, col_name, in_period)

    def _fn_calendar(self, args_str: str, ctx: DAXContext) -> Any:
        """CALENDAR(start, end) — generate a date table between start and end."""
        args = self._split_args(args_str)
        if len(args) < 2:
            return []
        start_val = self._eval_expr(args[0].strip(), ctx)
        end_val = self._eval_expr(args[1].strip(), ctx)
        start_date = self._parse_date(start_val)
        end_date = self._parse_date(end_val)
        if not start_date or not end_date:
            return []
        result = []
        current = start_date
        while current <= end_date:
            result.append({
                '__table__': '__calendar__',
                '__column__': 'Date',
                '__value__': current.strftime('%Y-%m-%d')
            })
            current += timedelta(days=1)
        return result

    def _fn_calendarauto(self, args_str: str, ctx: DAXContext) -> Any:
        """CALENDARAUTO() — generate a date table spanning all dates in the model.
        Scans all date-like columns to find min/max range."""
        min_date = None
        max_date = None
        for tbl_name, tbl in ctx.tables.items():
            for i, col in enumerate(tbl['columns']):
                for row in tbl['rows']:
                    d = self._parse_date(row[i])
                    if d:
                        if min_date is None or d < min_date:
                            min_date = d
                        if max_date is None or d > max_date:
                            max_date = d
                        break  # Just check first row of each column for perf
        if not min_date or not max_date:
            return []
        # Extend to full calendar years
        start = datetime(min_date.year, 1, 1)
        end = datetime(max_date.year, 12, 31)
        result = []
        current = start
        while current <= end:
            result.append({
                '__table__': '__calendar__',
                '__column__': 'Date',
                '__value__': current.strftime('%Y-%m-%d')
            })
            current += timedelta(days=1)
        return result


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
