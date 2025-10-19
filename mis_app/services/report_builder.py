# services/report_builder.py - Django Report Builder Service

from os import name
import re
import json
from django.shortcuts import get_object_or_404
from django.core.exceptions import PermissionDenied
import pandas as pd
from datetime import datetime
from sqlalchemy import select, func, literal_column, inspect, table, column, and_, or_, cast, text
from sqlalchemy.sql.sqltypes import Integer, Numeric, Float, BigInteger, SmallInteger, DECIMAL
from sqlalchemy.exc import OperationalError, DBAPIError
from django.conf import settings
from django.utils import timezone

from mis_app.permissions import PermissionManager
from ..models import ExternalConnection, ConnectionJoin, SavedReport, ReportShare
from ..utils import get_external_engine
from ..transformation_engine import TransformationEngine
import logging
from collections import deque

logger = logging.getLogger(__name__)

class ReportBuilderService:
    """Service class for report building functionality"""
    
    def __init__(self):
        self.logger = logger

    def _label_for(field_name, agg_type):
        base = field_name.replace(".", "_")
        return f"{base}_{agg_type.lower()}" if agg_type != "NONE" else base

    def _is_numeric_sqla_type(self, sqla_type) -> bool:
        try:
            return isinstance(sqla_type, (Integer, Numeric, Float, BigInteger, SmallInteger, DECIMAL))
        except Exception:
            return False
    def _get_database_specific_functions(self, engine):
        name = (engine.dialect.name or "").lower()
        def month_expr(c):
            if name == "postgresql": return func.date_trunc("month", c)
            if name == "sqlite": return func.strftime("%Y-%m-01", c)
            return func.date_format(c, "%Y-%m-01") # MySQL/MariaDB
        def year_expr(c):
            if name == "postgresql": return func.date_trunc("year", c)
            if name == "sqlite": return func.strftime("%Y-01-01", c)
            return func.date_format(c, "%Y-01-01") # MySQL/MariaDB
        def quarter_expr(c):
            if name == "postgresql": return func.date_trunc("quarter", c)
            if name == "sqlite":
                return func.printf("%04d-Q%d", func.strftime("%Y", c), ((cast(func.strftime("%m", c), Integer) - 1) / 3 + 1))
            return func.concat(func.date_format(c, "%Y"), "-Q", func.quarter(c)) # MySQL/MariaDB

        return {"month": month_expr, "year": year_expr, "quarter": quarter_expr}

    
    
    def build_advanced_report(self, report_config, user, return_query_obj=False):
        """
        Build and execute a report query with intelligent join pathfinding,
        supporting calculated fields, custom filtering, and aggregation logic.
        """
        try:
            # --- 1. EXTRACT INPUTS ---
            columns = report_config.get("columns", [])
            groups = report_config.get("groups", [])
            filters = report_config.get("filters", [])
            sorts = report_config.get("sorts", [])
            connection_id = report_config.get("connection_id")
            
            # This extracts the calculated fields definitions from the main config
            calculated_fields_defs = {
                field['name']: field['formula'] 
                for field in report_config.get('calculated_fields', [])
            }

            if not columns and not groups:
                return None, 0, "Please select at least one column or group to display."

            # --- 2. GET DATABASE ENGINE ---
            engine = get_external_engine(connection_id, user)
            if not engine:
                return None, 0, "Database connection failed."

            # --- 3. GATHER ALL FIELDS AND TABLES FROM THE CONFIG ---
            all_fields = set()
            formula_ref = re.compile(r'\[([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\]')

            def _maybe_add_field(field_str):
                if field_str and "." in field_str and not field_str.startswith('calc.'):
                    all_fields.add(field_str)

            for col in columns:
                field_name = col.get("field")
                if field_name and field_name.startswith('calc.'):
                    calc_name = field_name.split('.', 1)[1]
                    formula = calculated_fields_defs.get(calc_name, "")
                    for t, c in formula_ref.findall(formula):
                        all_fields.add(f"{t}.{c}")
                else:
                    _maybe_add_field(field_name)

            for item in groups + filters + sorts:
                _maybe_add_field(item.get("field"))

            initial_table_names = sorted({f.split(".")[0] for f in all_fields})

            # Identify subreport sources prefixed with report__<id>
            subreport_tables = [t for t in initial_table_names if str(t).startswith('report__')]
            base_tables = [t for t in initial_table_names if not str(t).startswith('report__')]

            # --- 4. GET SAVED JOINS FROM THE DATA MODEL ---
            connection_obj = get_object_or_404(ExternalConnection, id=connection_id)
            if not (connection_obj.owner_id == user.id or PermissionManager.user_can_access_connection(user, connection_obj)):
                raise PermissionDenied("Connection access denied.")

            saved_joins_query = ConnectionJoin.objects.filter(connection=connection_obj)
            all_model_joins = []
            for j in saved_joins_query:
                # Simply store the table and column names
                all_model_joins.append({
                    "left_table": j.left_table,
                    "left_column": j.left_column,
                    "right_table": j.right_table,
                    "right_column": j.right_column,
                    "type": (j.join_type or "INNER").upper()
                })

            # --- 5. BUILD JOIN GRAPH & DETERMINE REQUIRED JOINS ---
            graph = {}
            for join in all_model_joins:
                lt, rt = join["left_table"], join["right_table"]
                graph.setdefault(lt, []).append(rt)
                graph.setdefault(rt, []).append(lt)

            required_joins = []
            if len(initial_table_names) > 1:
                # Use Breadth-First Search (BFS) to find the shortest path of joins that connects all required tables.
                start_node = initial_table_names[0]
                queue = deque([(start_node, [])])  # The queue stores tuples of: (current_table, path_of_joins_to_get_here)
                visited_nodes = {start_node}
                solution_path = None

                while queue:
                    current_table, path = queue.popleft()

                    # Check if this path is a solution (i.e., it connects all required tables)
                    tables_covered_by_path = {start_node}
                    for j in path:
                        tables_covered_by_path.add(j['left_table'])
                        tables_covered_by_path.add(j['right_table'])
                    
                    if set(initial_table_names).issubset(tables_covered_by_path):
                        solution_path = path
                        break  # Found the shortest path, so we can stop searching.

                    # If not a solution yet, explore the neighbors of the current table.
                    for neighbor in graph.get(current_table, []):
                        if neighbor not in visited_nodes:
                            visited_nodes.add(neighbor)
                            join_to_use = next((j for j in all_model_joins if {j['left_table'], j['right_table']} == {current_table, neighbor}), None)
                            if join_to_use:
                                new_path = path + [join_to_use]
                                queue.append((neighbor, new_path))
                
                if solution_path:
                    required_joins = solution_path
                else:
                    return None, 0, f"Could not find a join path to connect all selected tables. Check your data model."
            
            # --- 6. BUILD THE QUERY OBJECT WITH JOINS ---
            inspector = inspect(engine)
            # Gather all tables needed, including intermediate ones from the path
            all_path_tables = set(base_tables)
            for j in required_joins:
                all_path_tables.add(j['left_table'])
                all_path_tables.add(j['right_table'])

            tables_dict = {name: table(name, *[column(c['name']) for c in inspector.get_columns(name)]) for name in all_path_tables}

            # Build subreport queries and add as alias tables
            for sr in subreport_tables:
                try:
                    sr_id = sr.split('__', 1)[1]
                except Exception:
                    return None, 0, f"Invalid subreport reference: {sr}"
                saved = SavedReport.objects.filter(id=sr_id).first()
                if not saved:
                    return None, 0, f"Referenced report not found: {sr_id}"
                # permissions: owner or shared with user
                if saved.owner_id != getattr(user, 'id', None) and not ReportShare.objects.filter(report_id=saved.id, user_id=getattr(user, 'id', None)).exists():
                    return None, 0, f"Access denied to report {saved.report_name}"
                sub_cfg = dict(saved.report_config or {})
                # Enforce same connection
                if str(sub_cfg.get('connection_id')) != str(connection_id):
                    return None, 0, f"Report '{saved.report_name}' uses a different connection."
                # Build subquery
                sub_q, _, err = self.build_advanced_report(sub_cfg, user, return_query_obj=True)
                if err:
                    return None, 0, err
                sub_alias = sub_q.alias(sr)
                tables_dict[sr] = sub_alias

            # Determine starting from_obj: prefer first base table else first subreport
            if base_tables:
                from_obj = tables_dict[base_tables[0]]
                processed_tables = {base_tables[0]}
            elif subreport_tables:
                from_obj = tables_dict[subreport_tables[0]]
                processed_tables = {subreport_tables[0]}
            else:
                return None, 0, "No source tables selected."

            # Iteratively build the FROM clause using the found joins
            for join_data in required_joins:
                l_table, r_table = join_data['left_table'], join_data['right_table']
                
                target_table_name = None
                if l_table in processed_tables and r_table not in processed_tables:
                    target_table_name = r_table
                elif r_table in processed_tables and l_table not in processed_tables:
                    target_table_name = l_table
                
                if target_table_name:
                    processed_tables.add(target_table_name)
                    target_table_obj = tables_dict[target_table_name]
                    
                    left_tbl_obj = tables_dict[join_data['left_table']]
                    right_tbl_obj = tables_dict[join_data['right_table']]
                    dynamic_condition = left_tbl_obj.c[join_data['left_column']] == right_tbl_obj.c[join_data['right_column']]
                    
                    from_obj = from_obj.join(
                        target_table_obj,
                        dynamic_condition,
                        isouter=(join_data['type'] == "LEFT")
                    )

            # Apply manual joins from report_config if provided (supports subreports)
            manual_joins = []
            for mj in (report_config.get('joins') or []):
                lc = (mj.get('left_col') or '')
                rc = (mj.get('right_col') or '')
                jtype = (mj.get('type') or 'INNER').upper()
                if '.' not in lc or '.' not in rc: continue
                lt, lc_name = lc.split('.', 1)
                rt, rc_name = rc.split('.', 1)
                manual_joins.append({
                    'left_table': lt,
                    'left_column': lc_name,
                    'right_table': rt,
                    'right_column': rc_name,
                    'type': jtype
                })

            # Try to join remaining tables using manual joins first
            all_joins_pool = list(required_joins) + manual_joins
            made_progress = True
            while made_progress:
                made_progress = False
                for join_data in list(all_joins_pool):
                    l_table, r_table = join_data['left_table'], join_data['right_table']
                    l_known, r_known = l_table in tables_dict, r_table in tables_dict
                    if not (l_known and r_known):
                        continue
                    target_table_name = None
                    if l_table in processed_tables and r_table not in processed_tables:
                        target_table_name = r_table
                    elif r_table in processed_tables and l_table not in processed_tables:
                        target_table_name = l_table
                    if target_table_name:
                        processed_tables.add(target_table_name)
                        left_tbl_obj = tables_dict[join_data['left_table']]
                        right_tbl_obj = tables_dict[join_data['right_table']]
                        try:
                            dynamic_condition = left_tbl_obj.c[join_data['left_column']] == right_tbl_obj.c[join_data['right_column']]
                        except Exception:
                            return None, 0, f"Invalid join columns: {join_data}"
                        from_obj = from_obj.join(
                            tables_dict[target_table_name],
                            dynamic_condition,
                            isouter=(join_data['type'] == 'LEFT')
                        )
                        all_joins_pool.remove(join_data)
                        made_progress = True

            # --- 7. Final Validation ---
            if not set(initial_table_names).issubset(processed_tables):
                 return None, 0, "Could not construct a valid join path to connect all selected tables."
            
            # --- 8. SELECT, GROUP BY, FILTERS, AND SORTS ---
            select_clauses, group_by_clauses, where_parts, order_parts = [], [], [], []
            grouped_fields = set() # A helper to track which fields are for grouping

            # A. Process Groups first to define the aggregation level
            db_funcs = self._get_database_specific_functions(engine)
            for grp in groups:
                fstr = grp.get("field")
                if not fstr or "." not in fstr: 
                    continue
                t, c = fstr.split(".", 1)
                if t not in tables_dict:
                    return None, 0, f"Unknown table in group: {t}"
                field_obj = getattr(tables_dict[t].c, c)

                method = (grp.get("method") or "exact").lower()
                label  = f"{fstr.replace('.', '_')}_{method}"

                if method in db_funcs:
                    expr = db_funcs[method](field_obj)
                elif method == "range":
                    # numeric bucketing
                    range_size = grp.get("range_size", 10)
                    expr = (func.floor(field_obj / range_size) * range_size)
                else:
                    expr = field_obj

                select_clauses.append(expr.label(label))
                group_by_clauses.append(expr)
                grouped_fields.add(fstr)

            # Handle columns (including calculated fields)
            for col in columns:
                field_name = col.get("field")
                if field_name and field_name.startswith("calc."):
                    calc_name = field_name.split(".", 1)[1]
                    formula = calculated_fields_defs.get(calc_name)
                    if formula:
                        expr = literal_column(formula)
                        agg = (col.get("agg") or "NONE").upper()
                        alias = f"calc_{calc_name}_{agg}" if agg != "NONE" else f"calc_{calc_name}"

                        if agg != "NONE":
                            select_clauses.append(getattr(func, agg.lower())(expr).label(alias))
                        else:
                            select_clauses.append(expr.label(alias))
                            if groups:
                                group_by_clauses.append(expr)
                elif field_name and "." in field_name:
                    t, c = field_name.split(".", 1)
                    if t not in tables_dict: continue
                    field_obj = getattr(tables_dict[t].c, c)
                    
                    # Determine the aggregation function
                    agg_func_name = (col.get("agg") or "NONE").upper()
                    
                    # If the report is grouped and no aggregation is specified, apply a default one.
                    if groups and agg_func_name == "NONE":
                        # Check if the column type is numeric to decide between SUM and COUNT
                        is_numeric = isinstance(field_obj.type, (Integer, Numeric, Float, BigInteger, SmallInteger, DECIMAL))
                        agg_func_name = "SUM" if is_numeric else "COUNT"

                    # Create the final aliased expression
                    alias = f"{field_name.replace('.', '_')}_{agg_func_name}" if agg_func_name != "NONE" else field_name.replace('.', '_')
                    
                    if agg_func_name != "NONE":
                        # Apply the aggregation function (e.g., func.sum, func.count)
                        expr = getattr(func, agg_func_name.lower())(field_obj).label(alias)
                    else: 
                        # If not a grouped query, just select the raw column
                        expr = field_obj.label(alias)

                    select_clauses.append(expr)

            if not select_clauses:
                return None, 0, "No valid columns or groups were selected for the report."

            # C. Construct the query from the clauses
            query = select(*select_clauses).select_from(from_obj)
            
            # Filters (WHERE clause)
            all_filters = filters + report_config.get('user_filters', []) # <-- ADD THIS LINE
            for flt in all_filters:
                fstr = flt.get("field")
                if not fstr or "." not in fstr: continue
                t, c = fstr.split(".", 1)
                if t not in tables_dict: continue
                field_obj = getattr(tables_dict[t].c, c)
                op = (flt.get("op") or "=").upper()
                val = flt.get("val")

                if op in ("IS NULL", "IS NOT NULL"):
                    if op == "IS NULL":
                        where_parts.append(field_obj == None)
                    else:
                        where_parts.append(field_obj != None)
                    continue

                if val is None or val == '': continue
                
                if op == "=": where_parts.append(field_obj == val)
                elif op == "!=": where_parts.append(field_obj != val)
                elif op == ">": where_parts.append(field_obj > val)
                elif op == "<": where_parts.append(field_obj < val)
                elif op == "LIKE": where_parts.append(field_obj.like(f"%{val}%"))
                elif op == "IN":
                    vlist = [v.strip() for v in val.split(',')]
                    where_parts.append(field_obj.in_(vlist))

            if where_parts: query = query.where(and_(*where_parts))

            # Group Bys
            if group_by_clauses:
                query = query.group_by(*group_by_clauses)

            # Sorts (ORDER BY clause)
            for srt in sorts:
                fstr = srt.get("field")
                # This part needs to be updated to handle sorting by aggregated fields
                # For now, we'll keep it simple
                if not fstr or "." not in fstr: continue
                t, c = fstr.split(".", 1)
                if t not in tables_dict: continue
                field_obj = getattr(tables_dict[t].c, c)
                direction = (srt.get("dir") or "ASC").upper()
                order_parts.append(field_obj.desc() if direction == "DESC" else field_obj.asc())
            if order_parts: query = query.order_by(*order_parts)

            # --- 9. BUILD AND EXECUTE THE FINAL QUERY ---
            query = select(*select_clauses).select_from(from_obj)
            
            if where_parts: query = query.where(and_(*where_parts))
            if group_by_clauses: query = query.group_by(*group_by_clauses)
            if order_parts: query = query.order_by(*order_parts)

            # --- DEBUG: Print the generated SQL query ---
            print("="*50)
            print("DEBUG: Final Query to be Executed")
            try:
                # This will compile the query into a string for your specific database
                print(query.compile(engine, compile_kwargs={"literal_binds": True}))
            except Exception as e:
                print(f"Could not compile query for debugging: {e}")
            print("="*50)
            # --- END DEBUG ---

            if return_query_obj:
                return query, None, None

            # Pagination
            page = report_config.get("page", 1)
            page_size = report_config.get("page_size", 100)

            # --- FIX: Sanitize pagination inputs to prevent TypeErrors ---
            try:
                page = int(page)
                page_size = int(page_size)
                if page < 1: page = 1
                if page_size < 1: page_size = 100
            except (ValueError, TypeError):
                # Fallback to defaults if conversion fails
                page = 1
                page_size = 100

            offset = (page - 1) * page_size
            
            # Get total count before applying limit/offset
            count_query = select(func.count()).select_from(query.alias())
            with engine.connect() as conn:
                total_rows = conn.execute(count_query).scalar()
            
            final_query = query.limit(page_size).offset(offset)

            with engine.connect() as conn:
                df = pd.read_sql(final_query, conn)
                df.columns = [c.get('name') for c in final_query.column_descriptions]
                return df, total_rows, None

        except (OperationalError, DBAPIError) as e:
            # This block specifically catches database connection errors
            self.logger.error(f"Database connection error during report build: {e}")
            
            # Try to get the connection nickname for an even friendlier message
            try:
                connection = ExternalConnection.objects.get(id=connection_id)
                db_name = connection.nickname
            except Exception:
                db_name = "the selected database"

            return None, 0, f"The report failed because the database '{db_name}' is not connected. Please check its status."
        
        except Exception as e:
            # This is a fallback for all other unexpected errors
            self.logger.error(f"Report building error: {e}", exc_info=True)
            return None, 0, f"An unexpected error occurred: {str(e)}"


        def quarter_expr(c):
            if name == "postgresql":
                return func.date_trunc("quarter", c)
            elif name in ("sqlite",):
                return func.printf(
                    "%04d-Q%d",
                    func.strftime("%Y", c),
                    ((cast(func.strftime("%m", c), Integer) - 1) / 3 + 1)
                )
            elif name in ("mysql", "mariadb"):
                return func.concat(func.date_format(c, "%Y"), "-Q", func.quarter(c))
            return c

        def year_expr(c):
            if name == "postgresql":
                return func.date_trunc("year", c)
            elif name in ("sqlite",):
                return func.strftime("%Y-01-01", c)
            elif name in ("mysql", "mariadb"):
                return func.date_format(c, "%Y-01-01")
            return c

        def date_expr(c):
            if name == "postgresql":
                return func.date_trunc("day", c)
            elif name in ("sqlite", "mysql", "mariadb"):
                return func.date(c)
            return c

        return {
            "month": month_expr,
            "quarter": quarter_expr,
            "year": year_expr,
            "date": date_expr,
        }

    def validate_report_config(self, report_config):
        """Validate report configuration"""
        errors = []
        warnings = []
        
        # Basic validation
        if not report_config.get('connection_id'):
            errors.append('Connection ID is required')
        
        columns = report_config.get('columns', [])
        groups = report_config.get('groups', [])
        
        if not columns and not groups:
            errors.append('At least one column or group must be specified')
        
        # Validate calculated fields
        calculated_fields = report_config.get('calculated_fields', [])
        for field in calculated_fields:
            if not field.get('name'):
                errors.append('Calculated field name is required')
            if not field.get('formula'):
                errors.append('Calculated field formula is required')
        
        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings
        }

    def get_report_suggestions(self, connection_id, user):
        """Get intelligent report suggestions for a connection"""
        suggestions = []
        
        try:
            engine = get_external_engine(connection_id, user)
            if not engine:
                return suggestions
            
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            
            for table in tables[:5]:  # Limit to first 5 tables
                columns = inspector.get_columns(table)
                numeric_columns = [col for col in columns if self._is_column_numeric(col)]
                text_columns = [col for col in columns if not self._is_column_numeric(col)]
                
                if numeric_columns and text_columns:
                    suggestions.append({
                        'title': f'Summary Report for {table.title()}',
                        'description': f'Group by {text_columns[0]["name"]} and sum {numeric_columns[0]["name"]}',
                        'config': {
                            'columns': [{'field': f'{table}.{numeric_columns[0]["name"]}', 'agg': 'SUM'}],
                            'groups': [{'field': f'{table}.{text_columns[0]["name"]}'}]
                        }
                    })
                    
        except Exception as e:
            logger.error(f"Error generating suggestions: {e}")
            
        return suggestions
    
    def _is_column_numeric(self, column_info):
        """Check if a column is numeric based on its type"""
        column_type = str(column_info['type']).lower()
        numeric_types = ['integer', 'bigint', 'smallint', 'decimal', 'numeric', 'real', 'double', 'float']
        return any(num_type in column_type for num_type in numeric_types)
