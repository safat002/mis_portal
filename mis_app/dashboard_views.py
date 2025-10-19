# mis_app/dashboard_views.py
from __future__ import annotations

import json
import re
import logging
import uuid as _uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from sqlalchemy import inspect, text


from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Q

from django.http import (
    HttpRequest,
    HttpResponse,
    JsonResponse,
    HttpResponseForbidden,
)
from django.shortcuts import get_object_or_404, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_POST

from collections.abc import Mapping

from .models import (
    Dashboard,
    DashboardShare,
    ExternalConnection,
    DashboardDataContext,
    User,
    ConnectionJoin,
    Widget,
)
from .permissions import PermissionManager
from .utils import get_external_engine, upgrade_or_default_config_v2
from .services.report_builder import ReportBuilderService

from .dashboard_join_helper import extract_tables_from_query_config, auto_apply_joins_for_query, infer_join_path, merge_explicit_and_inferred

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

@dataclass
class PermissionFlags:
    can_view: bool
    can_edit: bool
    is_owner: bool

def _json_body(request: HttpRequest):
    try:
        return json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return {}

def _split_qualified(name: str):
    """Return (schema, table) if qualified as 'schema.table', else (None, name)"""
    if not name:
        return (None, name)
    parts = str(name).split(".", 1)
    return (parts[0], parts[1]) if len(parts) == 2 else (None, name)


def _normalize_rows_to_mappings(data):
    """
    Convert any iterable of rows into a list[dict]-like structure so later code
    can safely use .get(...). Handles:
      - dict / Mapping
      - SQLAlchemy Row / RowMapping via ._mapping
      - tuple/list -> {"col0": v0, "col1": v1, ...}
      - scalars -> {"value": scalar}
    """
    out = []
    for r in (data or []):
        try:
            if isinstance(r, Mapping):
                out.append(dict(r))
            elif hasattr(r, "_mapping"):                              # SQLAlchemy Row
                out.append(dict(r._mapping))
            elif isinstance(r, (list, tuple)):
                out.append({f"col{i}": v for i, v in enumerate(r)})
            else:
                out.append({"value": r})
        except Exception:
            # last-resort: stringified row
            out.append({"value": str(r)})
    return out



@login_required
@require_POST
def table_columns_batch_api(request: HttpRequest) -> JsonResponse:
    data = _json_body(request)
    connection_id = (data.get("connection_id") or "").strip()
    tables = data.get("tables") or []
    
    if not connection_id or not isinstance(tables, list) or not tables:
        return JsonResponse({"success": False, "error": "connection_id and non-empty tables[] are required."}, status=400)

    try:
        if not PermissionManager.check_user_permission(request.user, "connection", connection_id, "view"):
            return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    except Exception as e:
        logger.exception("Permission check error: %s", e)
        return JsonResponse({"success": False, "error": "Permission check failed"}, status=500)

    conn = get_object_or_404(ExternalConnection, id=connection_id) 
    try:
        engine = get_external_engine(connection_id, request.user)
    except Exception as e:
        logger.exception("Engine init failed: %s", e)
        return JsonResponse({"success": False, "error": "Failed to open connection."}, status=500)

    if engine is None:
        return JsonResponse({"success": False, "error": "Failed to create database engine. Check server logs for details."}, status=500)

    allowed = None
    try:
        allowed_raw = PermissionManager.get_user_accessible_tables(request.user, connection_id)
        if allowed_raw in (None, True):
            allowed = None
        else:
            tmp = []
            for t in allowed_raw:
                tmp.append(str(t.name) if hasattr(t, 'name') else str(t))
            allowed = set(tmp)
    except Exception as e:
        logger.warning("get_user_accessible_tables failed: %s (proceeding as unrestricted)", e)

    out = []
    errs = {}
    try:
        inspector = inspect(engine)
        for raw in tables:
            is_allowed = allowed is None or raw in allowed
            if not is_allowed:
                errs[raw] = "Not permitted"
                continue

            schema, tbl = _split_qualified(raw)
            try:
                kwargs = {'schema': schema} if schema else {}
                cols = inspector.get_columns(tbl, **kwargs)
                for c in cols:
                    out.append({
                        "source": raw,
                        "name": c.get("name"),
                        "type": str(c.get("type", "")),
                        "fullName": f"{raw}.{c.get('name')}",
                    })
            except Exception as ex:
                errs[raw] = str(ex)
                logger.warning("Column introspection failed for %s: %s", raw, ex)
    except Exception as e:
        logger.exception("SQLAlchemy inspect(engine) failed: %s", e)
        return JsonResponse({"success": False, "error": "Introspection init failed."}, status=500)
        
    return JsonResponse({"success": True, "columns": out, "errors": errs})

def _get_perm_flags(user: User, dashboard: Dashboard) -> PermissionFlags:
    """
    Compute dashboard permissions:
      - Owners always have edit.
      - Admins (user_type=="Admin") get full access.
      - Otherwise check DashboardShare for 'edit' / 'view'.
    """
    is_owner = (dashboard.owner_id == user.id)
    if getattr(user, "user_type", "") == "Admin" or getattr(user, "is_superuser", False):
        return PermissionFlags(can_view=True, can_edit=True, is_owner=is_owner)

    if is_owner:
        return PermissionFlags(can_view=True, can_edit=True, is_owner=True)

    # Share record (if any)
    share = DashboardShare.objects.filter(dashboard=dashboard, user=user).first()
    if share:
        if share.permission == "edit":
            return PermissionFlags(can_view=True, can_edit=True, is_owner=False)
        return PermissionFlags(can_view=True, can_edit=False, is_owner=False)

    # Default: no access
    return PermissionFlags(can_view=False, can_edit=False, is_owner=False)


def _parse_json(request: HttpRequest) -> Dict[str, Any]:
    try:
        return json.loads(request.body.decode("utf-8") or "{}")
    except Exception:
        return {}
    
def _user_has_admin(user) -> bool:
    return getattr(user, "is_superuser", False) or getattr(user, "user_type", "") == "Admin"

def _user_can_edit(user, dashboard) -> bool:
    flags = _get_perm_flags(user, dashboard)
    return flags.can_edit

def _get_pinned_ids_for_user(request, user):
    """
    Returns (getter, setter) closures for pinned ids.
    - If user.pinned_dashboards is a real RelatedManager (has values_list/set), use it.
    - Otherwise, use session-backed pins.
    """
    session_key = "pinned_dashboard_ids"

    mgr = getattr(user, "pinned_dashboards", None)
    # Use DB-backed only if it quacks like a RelatedManager
    if hasattr(mgr, "values_list") and hasattr(mgr, "set"):
        def getter():
            return list(mgr.values_list("id", flat=True))
        def setter(new_ids):
            mgr.set(Dashboard.objects.filter(id__in=new_ids))
        return getter, setter

    # Session fallback (safe for all users)
    def getter():
        raw = request.session.get(session_key, [])
        return [str(x) for x in raw]
    def setter(new_ids):
        request.session[session_key] = [str(x) for x in new_ids]
        request.session.modified = True
    return getter, setter



@login_required
@require_http_methods(["POST"])
def dashboard_pin_api(request: HttpRequest, dashboard_id: _uuid.UUID) -> JsonResponse:
    """
    Toggle pin for the current user.
    Response: {success: true, pinned: bool}
    """
    dash = get_object_or_404(Dashboard, id=dashboard_id)
    if not _get_perm_flags(request.user, dash).can_view:
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    get_pins, set_pins = _get_pinned_ids_for_user(request, request.user)
    pins = [str(i) for i in get_pins()]
    sid = str(dash.id)
    pinned = None
    if sid in pins:
        pins.remove(sid)
        pinned = False
    else:
        pins.append(sid)
        pinned = True
    set_pins(pins)
    return JsonResponse({"success": True, "pinned": pinned})


@login_required
@require_http_methods(["POST"])
def dashboard_duplicate_api(request: HttpRequest, dashboard_id: _uuid.UUID) -> JsonResponse:
    """
    Duplicate a dashboard (title + ' (Copy)') including its config and DataContext.
    Response: {success: true, new_id: "..."}
    """
    src = get_object_or_404(Dashboard, id=dashboard_id)
    if not _get_perm_flags(request.user, src).can_view:
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    with transaction.atomic():
        new_title = f"{src.title or 'Untitled'} (Copy)"
        dup = Dashboard.objects.create(
            title=new_title,
            owner=request.user,  # copy to current user
        )
        # copy config as-is (widgets live in JSON; no FK)
        dup.config = src.config
        dup.save(update_fields=["config"])

        # copy DataContext if exists
        ctx = DashboardDataContext.objects.filter(dashboard=src).first()
        if ctx:
            DashboardDataContext.objects.create(
                dashboard=dup,
                connection=ctx.connection,
                selected_tables=ctx.selected_tables,
                joins=ctx.joins,
            )

    return JsonResponse({"success": True, "new_id": str(dup.id)})


@login_required
@require_http_methods(["DELETE"])
def dashboard_delete_api(request: HttpRequest, dashboard_id: _uuid.UUID) -> JsonResponse:
    """
    Delete a dashboard. Only owner or admin.
    Response: {success: true}
    """
    dash = get_object_or_404(Dashboard, id=dashboard_id)
    if not (_user_has_admin(request.user) or dash.owner_id == request.user.id):
        return JsonResponse({"success": False, "error": "Only the owner or an admin can delete."}, status=403)

    dash.delete()
    return JsonResponse({"success": True})


def _sample_rows_for_widget(widget_type: str) -> List[Dict[str, Any]]:
    """
    Minimal placeholder so the UI shows something before your data engine is wired.
    Only used when an explicit flag is on and for widgets with *no* query yet.
    """
    USE_SAMPLES = False  # <— turn samples off
    if not USE_SAMPLES:
        return []

    if widget_type in ("bar", "line", "area"):
        return [{"x": "A", "y": 12}, {"x": "B", "y": 19}, {"x": "C", "y": 7}]
    if widget_type in ("pie", "doughnut", "donut"):
        return [{"x": "North", "y": 35}, {"x": "South", "y": 25}, {"x": "East", "y": 20}, {"x": "West", "y": 20}]
    if widget_type in ("kpi", "number"):
        return [{"label": "Total", "value": 12345}]
    if widget_type == "table":
        return [{"Country": "BD", "Sales": 1200}, {"Country": "IN", "Sales": 980}]
    return [{"x": "Sample", "y": 1}]

def _process_widget_data(user: User, widget_type: str, widget_config: Dict, data_context: Dict) -> List[Dict[str, Any]]:
    """
    Process widget configuration and return actual data based on field assignments.
    """
    try:
        logger.info(f"Processing {widget_type} widget with config: {widget_config}")
        
        connection_id = data_context.get('connection_id')
        tables = data_context.get('tables', [])
        joins = data_context.get('joins', [])
        
        # This is the main dispatcher logic
        if widget_type in ("bar", "line", "area"):
            return _process_chart_data(user, widget_type, widget_config, data_context)
        elif widget_type in ("pie", "doughnut"):
            return _process_pie_data(user, widget_config, connection_id, tables, joins)
        elif widget_type == "kpi":
            return _process_kpi_data(user, widget_config, connection_id, tables, joins)
        elif widget_type == "table":
            return _process_table_data(user, widget_config, connection_id, tables, joins)
        else:
            # Fallback for any other widget type
            return _sample_rows_for_widget(widget_type)
            
    except Exception as e:
        logger.error(f"Error processing widget data: {str(e)}")
        return [{"error": f"An error occurred: {e}"}]
    
def _process_pie_data(
    user: User,
    widget_config: Dict[str, Any],
    connection_id: str,
    tables: List[str],
    joins: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    data_context = {
        'connection_id': connection_id,
        'tables': tables,
        'joins': joins,
    }
    return _process_chart_data(user, 'pie', widget_config, data_context)

def _process_kpi_data(user: User, widget_config: Dict, connection_id: str, tables: List[str], joins: List[Dict]) -> List[Dict[str, Any]]:
    """Process KPI widget data with single-table focus"""
    try:
        # Extract value field from config
        value_field = None
        aggregation = 'sum'
        slots = widget_config.get('slots', {})
        
        if 'primaryMeasure' in slots and slots['primaryMeasure']:
            value_field = slots['primaryMeasure'][0].get('fieldId')
            aggregation = slots['primaryMeasure'][0].get('aggregation', 'sum')
        elif widget_config.get('dataConfig', {}).get('valueField'):
            value_config = widget_config['dataConfig']['valueField']
            value_field = value_config.get('field')
            aggregation = value_config.get('aggregation', 'sum')
        
        if not value_field:
            return [{"label": "Total", "value": 0}]
        
        # For KPI, only use single table to avoid join complexity
        if connection_id and tables and '.' in value_field:
            table, column = value_field.split('.', 1)
            
            # Verify the table is in available tables
            if table not in tables:
                # Try to find the table in available tables
                available_table = next((t for t in tables if table in t), None)
                if not available_table:
                    return [{"label": "Value", "value": 0}]
                table = available_table
            
            engine = get_external_engine(connection_id, user)
            if engine:
                with engine.connect() as conn:
                    # Simple single-table query
                    sql = f'SELECT {aggregation.upper()}("{table}"."{column}") as value FROM "{table}" LIMIT 1'
                    logger.info(f"KPI SQL: {sql}")
                    
                    result = conn.execute(text(sql))
                    row = result.first()
                    if row and row[0] is not None:
                        return [{"label": "Value", "value": row[0]}]
        
        # Fallback to sample data
        return [{"label": "Total", "value": 12345}]
        
    except Exception as e:
        logger.error(f"Error in _process_kpi_data: {e}")
        return [{"label": "Error", "value": 0, "error": str(e)}]

def _process_table_data(
    user: User,
    widget_config: Dict,
    connection_id: str,
    tables: List[str],
    joins: List[Dict]
) -> List[Dict[str, Any]]:
    """Process table widget data via the chart pipeline."""
    try:
        data_context = {
            'connection_id': connection_id,
            'tables': tables,
            'joins': joins or [],
        }
        rows = _process_chart_data(user, 'table', widget_config, data_context)
        return rows if isinstance(rows, list) else []
    except Exception as e:
        logger.error(f"Error in _process_table_data: {e}", exc_info=True)
        return [{"error": f"Failed to process table data: {e}"}]


def _process_chart_data(user: User, widget_type: str, widget_config: Dict, data_context: Dict) -> List[Dict]:
    """
    Process chart data with proper JOIN handling for indirect joins
    """
    logger.info(f"Processing {widget_type} chart with config: {widget_config}")

    connection_id = data_context.get('connection_id')
    tables = data_context.get('tables', [])
    joins = data_context.get('joins', []) or []

    if not connection_id or not tables:
        return [{"error": "No connection or tables configured"}]

    try:
        slots = widget_config.get('slots', {}) or {}
        data_config = widget_config.get('dataConfig', {}) or {}
        dimensions: List[str] = []
        measures: List[str] = []
        field_tables = set()

        def _record_table(field_id: Optional[str]) -> None:
            if not field_id or '.' not in field_id:
                return
            table = field_id.split('.', 1)[0]
            field_tables.add(table)

        # Process category/series slots for dimensions
        for slot_key in ['category', 'series', 'row', 'slices']:
            for field in slots.get(slot_key, []) or []:
                fid = field.get('fieldId')
                if not fid:
                    continue
                dimensions.append(fid)
                _record_table(fid)

        # Process measures slots
        measure_objects: List[Dict[str, Any]] = []
        for slot_key in ['measures', 'measure', 'primaryMeasure', 'value']:
            for field in slots.get(slot_key, []) or []:
                fid = field.get('fieldId')
                if not fid:
                    continue
                measure_objects.append({
                    'field': fid,
                    'aggregation': field.get('aggregation', 'sum')
                })
                _record_table(fid)

        # If no measures found, check dataConfig
        if not measure_objects and data_config.get('measures'):
            for field in data_config['measures']:
                if isinstance(field, str):
                    measure_objects.append({'field': field, 'aggregation': 'sum'})
                    _record_table(field)
                elif isinstance(field, dict) and field.get('field'):
                    measure_objects.append(field)
                    _record_table(field.get('field'))

        if not measure_objects and data_config.get('yFields'):
            measure_objects = list(data_config['yFields'])
            for field in measure_objects:
                if isinstance(field, dict):
                    _record_table(field.get('field'))
                elif isinstance(field, str):
                    _record_table(field)

        logger.info(f"Chart dimensions: {dimensions}, measures: {measure_objects}")

        if not dimensions and not measure_objects:
            return [{"error": "No dimensions or measures configured"}]

        # Get all tables mentioned in fields
        all_fields = dimensions + [
            m['field'] for m in measure_objects
            if isinstance(m, dict) and m.get('field')
        ]
        for field in all_fields:
            _record_table(field)

        required_tables = set(field_tables)
        needs_joins = len(field_tables) > 1

        if needs_joins:
            def _join_to_col_format(join_spec):
                if not isinstance(join_spec, dict):
                    return None

                left_col = join_spec.get('left_col')
                right_col = join_spec.get('right_col')
                join_type = (join_spec.get('type') or join_spec.get('join_type') or 'INNER').upper()

                if not left_col or not right_col:
                    left_table = join_spec.get('left_table')
                    left_column = join_spec.get('left_column')
                    right_table = join_spec.get('right_table')
                    right_column = join_spec.get('right_column')

                    if left_table and left_column:
                        left_col = left_column if '.' in left_column else f"{left_table}.{left_column}"
                    if right_table and right_column:
                        right_col = right_column if '.' in right_column else f"{right_table}.{right_column}"

                if not left_col or not right_col:
                    return None

                return {'left_col': left_col, 'right_col': right_col, 'type': join_type}

            def _join_to_table_format(join_spec):
                if not isinstance(join_spec, dict):
                    return None

                join_type = (join_spec.get('type') or join_spec.get('join_type') or 'INNER').upper()
                left_col = join_spec.get('left_col')
                right_col = join_spec.get('right_col')

                if left_col and right_col and '.' in left_col and '.' in right_col:
                    left_table, left_column = left_col.rsplit('.', 1)
                    right_table, right_column = right_col.rsplit('.', 1)
                else:
                    left_table = join_spec.get('left_table')
                    left_column = join_spec.get('left_column')
                    right_table = join_spec.get('right_table')
                    right_column = join_spec.get('right_column')
                    if not (left_table and left_column and right_table and right_column):
                        return None

                return {
                    'left_table': left_table,
                    'left_column': left_column,
                    'right_table': right_table,
                    'right_column': right_column,
                    'join_type': join_type
                }

            def _as_col_joins(join_list):
                normalized = []
                for spec in join_list or []:
                    converted = _join_to_col_format(spec)
                    if converted:
                        normalized.append(converted)
                return normalized

            def _as_table_joins(join_list):
                normalized = []
                for spec in join_list or []:
                    converted = _join_to_table_format(spec)
                    if converted:
                        normalized.append(converted)
                return normalized

            initial_join_count = len(joins)

            if not joins:
                inferred = infer_join_path(connection_id, list(required_tables), user)
                if inferred:
                    joins = _as_table_joins(inferred)
                    data_context['joins'] = joins
                    if joins:
                        logger.info("Auto-inferred %d join(s) for chart widget", len(joins))
                else:
                    logger.warning("Multiple tables %s detected but no join path was found", sorted(required_tables))
                    error_msg = (
                        f"Query requires joins between tables: {sorted(required_tables)}. "
                        f"No join path was found in Data Context."
                    )
                    return [{"error": error_msg}]
            else:
                inferred = infer_join_path(connection_id, list(required_tables), user)
                if inferred:
                    merged = merge_explicit_and_inferred(_as_col_joins(joins), inferred)
                else:
                    merged = _as_col_joins(joins)
                joined_tables = _as_table_joins(merged)
                if joined_tables:
                    added = len(joined_tables) - initial_join_count
                    joins = joined_tables
                    data_context['joins'] = joins
                    if added > 0:
                        logger.info("Augmented joins with %d inferred hop(s)", added)

        if needs_joins and joins:
            connection_graph = {}
            for join in joins:
                left_table = join.get('left_table')
                right_table = join.get('right_table')

                if left_table not in connection_graph:
                    connection_graph[left_table] = []
                if right_table not in connection_graph:
                    connection_graph[right_table] = []

                connection_graph[left_table].append(right_table)
                connection_graph[right_table].append(left_table)

            def are_tables_connected(tables_to_check, graph):
                if not tables_to_check:
                    return True

                visited = set()
                stack = [list(tables_to_check)[0]]

                while stack:
                    current = stack.pop()
                    if current in visited:
                        continue
                    visited.add(current)

                    for neighbor in graph.get(current, []):
                        if neighbor not in visited:
                            stack.append(neighbor)

                return all(table in visited for table in tables_to_check)

            tables_connected = are_tables_connected(required_tables, connection_graph)

            if not tables_connected:
                logger.warning(f"Tables {required_tables} are not fully connected by available joins")
                return [{"error": f"Query requires joins between tables: {sorted(required_tables)}. Please configure joins in Data Context."}]

        elif needs_joins and not joins:
            logger.warning(f"Multiple tables {required_tables} detected but no joins configured")
            return [{"error": f"Query requires joins between tables: {sorted(required_tables)}. Please configure joins in Data Context."}]

        engine = get_external_engine(connection_id, user)
        if not engine:
            return [{"error": "Could not connect to database"}]

        select_parts = []
        group_by_parts = []
        order_by_parts = []

        for dim in dimensions:
            if '.' in dim:
                table, col = dim.split('.', 1)
                select_parts.append(f'"{table}"."{col}" AS "{col}"')
                group_by_parts.append(f'"{table}"."{col}"')
                order_by_parts.append(f'"{table}"."{col}"')

        for measure in measure_objects:
            field = measure.get('field') if isinstance(measure, dict) else None
            if not field or '.' not in field:
                continue
            agg = (measure.get('aggregation') if isinstance(measure, dict) else 'sum') or 'sum'
            table, col = field.split('.', 1)
            select_parts.append(f'{agg.upper()}("{table}"."{col}") AS "{col}"')

        if not select_parts:
            return [{"error": "No valid fields to query"}]

        base_table = None
        for field in dimensions + [
            m['field'] for m in measure_objects
            if isinstance(m, dict) and m.get('field')
        ]:
            if '.' in field:
                base_table = field.split('.', 1)[0]
                break

        if not base_table:
            return [{"error": "Could not determine base table"}]

        from_clause = f'FROM "{base_table}"'

        if needs_joins and joins:
            join_clauses = []
            used_tables = {base_table}
            max_iterations = len(joins) * 2
            for _ in range(max_iterations):
                join_added = False
                for j in joins:
                    left_tbl = j.get('left_table')
                    right_tbl = j.get('right_table')
                    left_col = j.get('left_column')
                    right_col = j.get('right_column')
                    join_type = (j.get('join_type') or 'INNER').upper()

                    # Grow the connected FROM tree regardless of whether the new node
                    # is directly referenced by a field (Steiner nodes are allowed).
                    if left_tbl in used_tables and right_tbl not in used_tables:
                        join_clauses.append(
                            f'{join_type} JOIN "{right_tbl}" ON "{left_tbl}"."{left_col}" = "{right_tbl}"."{right_col}"'
                        )
                        used_tables.add(right_tbl)
                        join_added = True
                        continue

                    if right_tbl in used_tables and left_tbl not in used_tables:
                        join_clauses.append(
                            f'{join_type} JOIN "{left_tbl}" ON "{right_tbl}"."{right_col}" = "{left_tbl}"."{left_col}"'
                        )
                        used_tables.add(left_tbl)
                        join_added = True
                        continue


                if field_tables.issubset(used_tables) or not join_added:
                    break

            if join_clauses:
                from_clause += " " + " ".join(join_clauses)
                logger.info(f"Using joins: {join_clauses}")

        select_sql = ", ".join(select_parts)
        group_by_sql = f"GROUP BY {', '.join(group_by_parts)}" if group_by_parts else ""
        order_by_sql = f"ORDER BY {', '.join(order_by_parts)}" if order_by_parts else ""
        limit_sql = "LIMIT 200"
        sql = f"SELECT {select_sql} {from_clause} {group_by_sql} {order_by_sql} {limit_sql}".strip()
        sql = " ".join(sql.split())

        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = [dict(row._mapping) for row in result]

            logger.info(f"Query returned {len(rows)} rows")

            if widget_type in ('pie', 'doughnut') and rows:
                row_dicts = _normalize_rows_to_mappings(rows)
                if row_dicts and len(row_dicts[0].keys()) >= 2:
                    keys = list(row_dicts[0].keys())
                    return [{"x": r.get(keys[0]), "y": r.get(keys[1])} for r in row_dicts]

            return rows

    except Exception as e:
        logger.error(f"Error in _process_chart_data: {str(e)}", exc_info=True)
        return [{"error": f"Query failed: {str(e)}"}]


# -----------------------------------------------------------------------------
# Pages
# -----------------------------------------------------------------------------

@login_required
def dashboard_management_view(request: HttpRequest) -> HttpResponse:
    user = request.user
    if getattr(user, "user_type", "") == "Admin":
        owned = Dashboard.objects.all().order_by("-updated_at")
        shared = Dashboard.objects.none()
    else:
        owned = Dashboard.objects.filter(owner=user).order_by("-updated_at")
        shared = (
            Dashboard.objects.filter(dashboardshare__user=user)
            .exclude(owner=user)
            .order_by("-updated_at")
        )

    # Pinned list for the Pinned row (used by dashboard_management.html)
    get_pins, _ = _get_pinned_ids_for_user(request, user)
    pinned_ids = [str(i) for i in get_pins()]

    context = {
        "owned_dashboards": owned,
        "shared_dashboards": shared,
        "can_create_dashboards": True,
        "initial_pins_json": json.dumps(pinned_ids),  # <<< add this
    }
    return render(request, "dashboard_management.html", context)


@login_required
def dashboard_design_view(request: HttpRequest, dashboard_id: _uuid.UUID) -> HttpResponse:
    """
    Page: The designer/canvas. Provides connections for the Data Context UI.
    """
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    perms = _get_perm_flags(request.user, dashboard)
    if not perms.can_view:
        return HttpResponseForbidden("You do not have access to this dashboard.")

    # Connections visible to the user (owner, internal, or via permission manager)
    connections = ExternalConnection.objects.filter(is_active=True).order_by("nickname")
    context = {
        "dashboard": dashboard,
        "connections": connections,
        "can_edit": perms.can_edit,
        "is_owner": perms.is_owner,
    }
    return render(request, "dashboard_design.html", context)


# -----------------------------------------------------------------------------
# APIs
# -----------------------------------------------------------------------------
def get_widget_config(dashboard_id, widget_id):
    """Get widget configuration from database"""
    try:
        widget = Widget.objects.get(id=widget_id, dashboard_id=dashboard_id)
        return widget.config  # Adjust based on your actual field name
    except Widget.DoesNotExist:
        return None
    
@login_required
@require_POST
def widget_data_api(request, dashboard_id, widget_id):
    """
    Enhanced widget data API that automatically applies joins from data model.
    """
    try:
        # Your existing code to get widget config...
        widget_config = get_widget_config(dashboard_id, widget_id)
        query_config = widget_config.get('dataConfig', {})
        connection_id = widget_config.get('connection_id')
        
        # Extract tables used in this query
        tables_used = extract_tables_from_query_config(query_config)
        
        # Auto-apply joins from data model
        existing_joins = query_config.get('joins', [])
        auto_joins = auto_apply_joins_for_query(connection_id, tables_used, existing_joins)
        
        if auto_joins and auto_joins != existing_joins:
            logger.info(f"Auto-applied {len(auto_joins) - len(existing_joins)} joins for widget {widget_id}")
            query_config['joins'] = auto_joins
        
        service = ReportBuilderService()
        result = service.execute_query(query_config, request.user)
        
        return JsonResponse({
            'success': True,
            'rows': result['rows'],
            'columns': result['columns']
        })
        
    except Exception as e:
        logger.error(f"Error in widget data API: {e}")
        return JsonResponse({'success': False, 'error': str(e)})

@login_required
@require_POST
def create_dashboard_api(request: HttpRequest) -> JsonResponse:
    """
    POST { "dashboard_name": "Q3 Sales" }
    -> { success, dashboard_id }
    """
    data = _parse_json(request)
    name = (data.get("dashboard_name") or "").strip()
    if not name:
        return JsonResponse({"success": False, "error": "Dashboard name is required."}, status=400)

    with transaction.atomic():
        dash = Dashboard.objects.create(
            title=name,
            owner=request.user,
        )
        cfg, _ = upgrade_or_default_config_v2(None, title=name)
        dash.config = cfg
        dash.save(update_fields=["config"])

    return JsonResponse({"success": True, "dashboard_id": str(dash.id)}, status=201)


@login_required
@require_http_methods(["GET", "POST"])
def dashboard_config_api(request: HttpRequest, dashboard_id: _uuid.UUID) -> JsonResponse:
    """
    GET  -> returns dashboard.config JSON (multi-page aware)
    POST -> saves config (used by autosave)
    """
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    perms = _get_perm_flags(request.user, dashboard)
    if not perms.can_view:
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    if request.method == "GET":
        cfg, migrated = upgrade_or_default_config_v2(
            dashboard.config or {}, title=dashboard.title or "Untitled Dashboard"
        )
        if migrated:
            dashboard.config = cfg
            dashboard.save(update_fields=["config"])
        return JsonResponse(cfg, safe=False)

    # POST
    if not perms.can_edit:
        return JsonResponse({"success": False, "error": "You have view-only access."}, status=403)

    existing_cfg = dashboard.config or {}
    new_cfg = _parse_json(request)
    if not isinstance(new_cfg, dict):
        return JsonResponse({"success": False, "error": "Invalid config payload"}, status=400)

    # >>> PRESERVE KEYS NOT IN AUTOSAVE PAYLOAD <<<
    for key in ("data_context", "theme", "global_filters"):
        if key in existing_cfg and key not in new_cfg:
            new_cfg[key] = existing_cfg[key]

    cfg, _ = upgrade_or_default_config_v2(
        new_cfg, title=dashboard.title or "Untitled Dashboard"
    )
    dashboard.config = cfg
    dashboard.updated_at = timezone.now()
    dashboard.save(update_fields=["config", "updated_at"])
    return JsonResponse({"success": True})


@login_required
@require_http_methods(["GET", "POST"])
def dashboard_data_context_api(request, dashboard_id):
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    perms = _get_perm_flags(request.user, dashboard)

    if request.method == "GET":
        cfg = dashboard.config or {}
        data_context = cfg.get("data_context", {})
        
        # If no joins exist but we have tables, suggest default joins
        if not data_context.get('joins') and data_context.get('tables'):
            default_joins = create_default_joins(
                data_context.get('connection_id'), 
                data_context.get('tables')
            )
            if default_joins:
                data_context['default_joins_suggested'] = default_joins
        
        return JsonResponse({"success": True, "data_context": data_context})

    # POST
    if not perms.can_edit:
        return JsonResponse({"success": False, "error": "Permission denied."}, status=403)

    try:
        payload = json.loads(request.body or "{}")
        connection_id = payload.get("connection_id")
        tables = payload.get("tables") or payload.get("selected_tables") or []
        joins = payload.get("joins") or []
        
        # If no joins provided but we have multiple tables, create default joins
        if not joins and len(tables) >= 2:
            joins = create_default_joins(connection_id, tables)
            logger.info(f"Auto-created {len(joins)} default joins for tables {tables}")

        cfg = dashboard.config or {}
        cfg["data_context"] = {
            "connection_id": connection_id,
            "tables": tables,
            "joins": joins,
        }
        dashboard.config = cfg
        dashboard.save(update_fields=["config"])
        
        logger.info(f"Saved data context: {len(tables)} tables, {len(joins)} joins")
        return JsonResponse({"success": True})
        
    except Exception as e:
        logger.error(f"Error saving data context: {e}")
        return JsonResponse({"success": False, "error": str(e)}, status=400)


@login_required
@require_POST
def dashboard_widget_data_api(request: HttpRequest, dashboard_id: _uuid.UUID) -> JsonResponse:
    """
    Generic widget data endpoint used by designer previews.
    Returns: {"success": True, "rows": [...]}
    """
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    perms = _get_perm_flags(request.user, dashboard)
    if not perms.can_view:
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    try:
        payload = _parse_json(request) or {}
    except Exception as e:
        return JsonResponse({"success": True, "rows": [{"error": f"Bad JSON: {e}"}]})

    # --- Normalize incoming keys ------------------------------------------------
    widget_type = (payload.get("type") or payload.get("widget_type") or "").lower()
    widget_config = payload.get("dataConfig") or payload.get("widget_config") or {}

    # Allow the client to override/preview context; otherwise use dashboard config
    dc_override = payload.get("data_context")
    if isinstance(dc_override, dict):
        data_context: Dict[str, Any] = dc_override
    else:
        data_context = (dashboard.config or {}).get("data_context", {}) or {}

    connection_id = data_context.get("connection_id")
    tables = data_context.get("tables") or []
    joins = data_context.get("joins") or []  # kept for future use / logging

    # --- Dispatch by widget type -----------------------------------------------
    try:
        # TABLE: if you have a dedicated table processor, call it here.
        if widget_type == "table":
            rows = _process_chart_data(request.user, widget_config, connection_id, tables)
            # no adaptation; front-end table builder handles generic rows
            return JsonResponse({"success": True, "rows": rows})

        # KPI / NUMBER: run chart path and reduce to first numeric
        if widget_type in ("kpi", "number"):
            rows = _process_chart_data(request.user, widget_config, connection_id, tables)
            # Make a friendly single-row format if possible
            kpi_row: Dict[str, Any] = {}
            if isinstance(rows, list) and rows:
                first = rows[0]
                if isinstance(first, Mapping):
                    # pick first numeric-ish column
                    for k, v in first.items():
                        if isinstance(v, (int, float)):
                            kpi_row = {"label": str(k), "value": v}
                            break
                    if not kpi_row:
                        # fallback to first key
                        k0 = next(iter(first.keys()), "value")
                        kpi_row = {"label": str(k0), "value": first.get(k0)}
                else:
                    # positional fallback
                    if isinstance(first, (list, tuple)):
                        kpi_row = {"label": "value", "value": first[0] if first else None}
                    else:
                        kpi_row = {"label": "value", "value": first}
            else:
                kpi_row = {"label": "Total", "value": 0}
            return JsonResponse({"success": True, "rows": [kpi_row]})

                # PIE / DOUGHNUT: run the same SQL path, then adapt to [{x, y}]
        if widget_type in ("pie", "doughnut", "donut"):
            raw_rows = _process_chart_data(request.user, widget_config, connection_id, tables)

            # Normalize so we can safely use .get(...)
            rows_map = _normalize_rows_to_mappings(raw_rows)

            if not rows_map:
                return JsonResponse({"success": True, "rows": []})

            # Choose label and value columns
            cols = list(rows_map[0].keys())
            if len(cols) == 1:
                label_col, value_col = cols[0], cols[0]
            else:
                label_col = cols[0]
                # pick the first numeric-ish column after the label
                value_col = None
                for c in cols[1:]:
                    try:
                        for rr in rows_map:
                            v = rr.get(c)
                            if v is None:
                                continue
                            # treat numeric-ish strings as numeric too
                            if isinstance(v, (int, float)) or (isinstance(v, str) and v.replace('.', '', 1).isdigit()):
                                value_col = c
                                break
                        if value_col:
                            break
                    except Exception:
                        continue
                if not value_col:
                    value_col = cols[1]  # fallback

            pie_rows = [{"x": r.get(label_col), "y": r.get(value_col)} for r in rows_map]

            # Optional logging
            try:
                logger.info("Pie rows (%d). label_col=%s value_col=%s sample=%s",
                            len(pie_rows), label_col, value_col, pie_rows[:3])
            except Exception:
                pass

            return JsonResponse({"success": True, "rows": pie_rows})

    except Exception as e:
        # Do NOT raise; the front-end expects success + error row to render inline.
        try:
            logger.exception("Error processing widget (%s): %s", widget_type, e)
        except Exception:
            pass
        return JsonResponse({"success": True, "rows": [{"error": str(e)}]})

@login_required
@require_POST
def dashboard_widget_instance_data_api(
    request: HttpRequest,
    dashboard_id: _uuid.UUID,
    widget_id: _uuid.UUID,
) -> JsonResponse:
    """Enhanced widget data API with auto-join functionality for indirect joins"""
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    perms = _get_perm_flags(request.user, dashboard)
    if not perms.can_view:
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    body = _parse_json(request)
    wtype = (body.get("type") or "").lower()
    data_config = body.get("dataConfig", {})
    
    logger.info(f"Widget API - Type: {wtype}")
    logger.info(f"Processing {wtype} chart with config: {data_config}")

    # Get data context from dashboard config
    dashboard_config = dashboard.config or {}
    data_context = dashboard_config.get("data_context", {})
    
    # AUTO-JOIN PATCH: Extract tables and auto-apply joins (including indirect)
    connection_id = data_context.get('connection_id')
    if connection_id:
        # Extract tables from the query configuration
        tables_used = set()
        
        # Extract from slots configuration
        slots = data_config.get('slots', {})
        for slot_key in ['category', 'series', 'measures', 'measure', 'primaryMeasure', 'value', 'row', 'slices']:
            if slot_key in slots:
                for field in slots[slot_key]:
                    field_id = field.get('fieldId')
                    if field_id and '.' in field_id:
                        table = field_id.split('.', 1)[0]
                        tables_used.add(table)
        
        # Extract from direct config
        for field_list in [data_config.get('dimensions', []), data_config.get('measures', [])]:
            for field in field_list:
                if isinstance(field, str) and '.' in field:
                    tables_used.add(field.split('.', 1)[0])
                elif isinstance(field, dict) and field.get('field') and '.' in field.get('field'):
                    tables_used.add(field['field'].split('.', 1)[0])
        
        tables_used = list(tables_used)
        
        # Apply auto-joins if multiple tables
        if len(tables_used) >= 2:
            try:
                # Get ALL saved joins for this connection (not just direct between used tables)
                saved_joins = ConnectionJoin.objects.filter(connection_id=connection_id)
                
                if saved_joins.exists():
                    # Build a graph of all available joins
                    join_graph = {}
                    for join in saved_joins:
                        left_table = join.left_table
                        right_table = join.right_table
                        
                        if left_table not in join_graph:
                            join_graph[left_table] = []
                        if right_table not in join_graph:
                            join_graph[right_table] = []
                        
                        join_graph[left_table].append({
                            'table': right_table,
                            'left_column': join.left_column,
                            'right_column': join.right_column,
                            'join_type': join.join_type or 'INNER'
                        })
                        join_graph[right_table].append({
                            'table': left_table, 
                            'left_column': join.right_column,  # reversed
                            'right_column': join.left_column,   # reversed
                            'join_type': join.join_type or 'INNER'
                        })
                    
                    # Find the shortest path between all required tables
                    def find_join_path(start_table, target_tables, graph):
                        """Find joins needed to connect start_table to all target_tables"""
                        from collections import deque
                        
                        visited = {start_table}
                        queue = deque([(start_table, [])])  # (current_table, path_so_far)
                        needed_joins = []
                        connected_tables = {start_table}
                        
                        while queue and len(connected_tables) < len(target_tables):
                            current_table, path = queue.popleft()
                            
                            for neighbor in graph.get(current_table, []):
                                neighbor_table = neighbor['table']
                                if neighbor_table not in visited:
                                    visited.add(neighbor_table)
                                    new_path = path + [{
                                        'left_table': current_table,
                                        'left_column': neighbor['left_column'],
                                        'right_table': neighbor_table,
                                        'right_column': neighbor['right_column'],
                                        'join_type': neighbor['join_type']
                                    }]
                                    
                                    # If this neighbor is one of our target tables, add the path
                                    if neighbor_table in target_tables:
                                        needed_joins.extend(new_path)
                                        connected_tables.add(neighbor_table)
                                    
                                    queue.append((neighbor_table, new_path))
                        
                        return needed_joins
                    
                    # Try to find a base table and connect all others to it
                    if tables_used:
                        base_table = tables_used[0]
                        target_tables = set(tables_used[1:])
                        
                        if target_tables:
                            auto_joins = find_join_path(base_table, target_tables, join_graph)
                            
                            if auto_joins:
                                # Add auto-joins to data_context
                                if 'joins' not in data_context:
                                    data_context['joins'] = []
                                
                                # Only add joins that aren't already there
                                existing_joins_set = {
                                    (j.get('left_table'), j.get('left_column'), j.get('right_table'), j.get('right_column'))
                                    for j in data_context['joins']
                                }
                                
                                for join in auto_joins:
                                    join_key = (join['left_table'], join['left_column'], join['right_table'], join['right_column'])
                                    if join_key not in existing_joins_set:
                                        data_context['joins'].append(join)
                                
                                logger.info(f"Auto-applied {len(auto_joins)} indirect joins for widget {widget_id}")
                                
            except Exception as e:
                logger.warning(f"Auto-join application failed: {e}")
    
    try:
        # For chart types, use the enhanced chart processor
        if wtype in ("bar", "line", "area", "pie", "doughnut"):
            rows = _process_chart_data(request.user, wtype, data_config, data_context)
        elif wtype == "kpi":
            # For KPI, use simplified single-table approach
            rows = _process_kpi_data(request.user, data_config, 
                                   data_context.get('connection_id'),
                                   data_context.get('tables', []), 
                                   data_context.get('joins', []))
        elif wtype == "table":
            # For tables, also use chart processor but return raw data
            rows = _process_chart_data(request.user, wtype, data_config, data_context)
        else:
            rows = [{"error": f"Unsupported widget type: {wtype}"}]
        
        logger.info(f"Returning {len(rows)} rows for widget {widget_id}")
        return JsonResponse({"success": True, "rows": rows})
        
    except Exception as e:
        logger.error(f"Error processing widget data for {widget_id}: {e}", exc_info=True)
        return JsonResponse({
            "success": False, 
            "error": f"Failed to process widget data: {str(e)}"
        }, status=500)

# Accessing data as per user permission and import join form data model and join suggestion

def _user_accessible_connections(user):
    """
    Return queryset of ExternalConnection visible to this user.
    Mirrors the logic you already use in dashboard_design_view.
    """
    qs = ExternalConnection.objects.filter(Q(owner=user) | Q(is_internal=True))
    # include permission-based accessible connections (via groups/permissions)
    try:
        all_perms = PermissionManager.get_user_permissions(user)
        accessible_ids = set()
        if isinstance(all_perms, dict):
            for key, level in all_perms.items():
                if level != 'none' and key.startswith('connection:'):
                    nickname = key.split(':', 1)[1]
                    for cid in ExternalConnection.objects.filter(nickname=nickname).values_list('id', flat=True):
                        accessible_ids.add(cid)
        elif isinstance(all_perms, list):
            for p in all_perms:
                if p.get('resource_type') in ('connection', 'table') and p.get('resource_id'):
                    if p.get('permission_level') not in ('none', None):
                        accessible_ids.add(p['resource_id'])
        if accessible_ids:
            qs = (qs | ExternalConnection.objects.filter(id__in=accessible_ids)).distinct()
    except Exception:
        pass
    return qs

def _filter_hidden_tables(connection, tables):
    """Respect connection.hidden_tables if present."""
    hidden = set()
    try:
        raw = connection.hidden_tables
        if raw:
            if isinstance(raw, str):
                hidden = set([t.strip() for t in raw.split(',') if t.strip()])
            elif isinstance(raw, (list, tuple, set)):
                hidden = set(raw)
    except Exception:
        pass
    return [t for t in tables if t not in hidden]

def _allowed_tables_for_user(user, connection, all_tables):
    """
    If you have table-level permissions, apply them here.
    For now:
      - remove hidden tables
      - (optionally) apply PermissionManager table rules, if available in your project
    """
    tables = _filter_hidden_tables(connection, all_tables)

    # Optional: apply table-level allowlist from PermissionManager (if your system exposes it)
    try:
        all_perms = PermissionManager.get_user_permissions(user)
        # If your permissions are table-scoped like: {"table:orders": "view"} or list entries
        # convert them to a set of table names permitted and intersect. Otherwise skip.
        explicit_table_allows = set()
        if isinstance(all_perms, dict):
            for k, level in all_perms.items():
                if level != 'none' and k.startswith('table:'):
                    explicit_table_allows.add(k.split(':', 1)[1])
        elif isinstance(all_perms, list):
            for p in all_perms:
                if p.get('resource_type') == 'table' and p.get('permission_level') not in ('none', None):
                    # if your system encodes table identity as p['name'] or something else,
                    # adjust this mapping accordingly.
                    if p.get('name'):
                        explicit_table_allows.add(p['name'])

        if explicit_table_allows:
            tables = [t for t in tables if t in explicit_table_allows]
    except Exception:
        pass

    return tables

@login_required
def list_connections_api(request):
    """
    GET: list of connections the user can see
    """
    qs = _user_accessible_connections(request.user)
    payload = [{
        'id': str(c.id),
        'nickname': c.nickname,
        'db_type': c.db_type,
        'is_default': getattr(c, 'is_default', False),
    } for c in qs.order_by('nickname')]
    return JsonResponse({'success': True, 'connections': payload})

@login_required
def connection_tables_api(request, connection_id):
    """
    GET: list permitted tables for this connection (filtered by hidden tables + permissions)
    """
    try:
        conn = ExternalConnection.objects.get(id=connection_id)
    except ExternalConnection.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Connection not found.'}, status=404)

    # basic visibility check; reuse same pattern as _user_accessible_connections
    if not _user_accessible_connections(request.user).filter(id=conn.id).exists():
        return JsonResponse({'success': False, 'error': 'Not allowed for this connection.'}, status=403)

    try:
        engine = get_external_engine(connection_id, request.user)
        insp = inspect(engine)
        all_tables = insp.get_table_names()
        permitted = _allowed_tables_for_user(request.user, conn, all_tables)
        return JsonResponse({'success': True, 'tables': permitted})
    except Exception as e:
        return JsonResponse({'success': False, 'error': f'Failed to list tables: {str(e)}'}, status=500)

def _normalize_table_name(name: str) -> str:
    if not name:
        return ""
    s = str(name)
    if "." in s:
        s = s.split(".", 1)[1]
    return s.strip().lower()


def _predefined_join_edges(connection_id, tables):
    """
    Enhanced join discovery with better table matching
    """
    try:
        qs = ConnectionJoin.objects.filter(connection_id=connection_id)
    except Exception:
        return []

    # Normalize incoming tables
    tblset_norm = {_normalize_table_name(t) for t in (tables or []) if t}
    
    edges = []
    for j in qs:
        lt_raw = getattr(j, "left_table", None)
        rt_raw = getattr(j, "right_table", None)
        lc = getattr(j, "left_column", None)
        rc = getattr(j, "right_column", None)
        jt = (getattr(j, "join_type", "") or "INNER").upper()

        if not (lt_raw and rt_raw and lc and rc):
            continue

        lt_norm = _normalize_table_name(lt_raw)
        rt_norm = _normalize_table_name(rt_raw)

        # More flexible matching - check if either normalized table is in our set
        if lt_norm in tblset_norm and rt_norm in tblset_norm:
            edges.append({
                "left_table": lt_norm,
                "left_column": lc,
                "right_table": rt_norm,
                "right_column": rc,
                "join_type": jt,
                "source": "predefined",
                "confidence": 1.0,
            })
    
    logger.info(f"Found {len(edges)} predefined joins for tables {tblset_norm}")
    return edges

def create_default_joins(connection_id, tables):
    """
    Create default joins for common table relationships
    """
    common_joins = [
        # Common join patterns for your schema
        {
            "left_table": "mk_customers", 
            "left_column": "id",
            "right_table": "mk_payments", 
            "right_column": "customer_id",
            "join_type": "INNER"
        },
        {
            "left_table": "mk_meal_kits",
            "left_column": "id", 
            "right_table": "mk_payments",
            "right_column": "meal_kit_id",
            "join_type": "INNER"
        }
    ]
    
    # Filter to only include joins between selected tables
    table_set = set(tables)
    applicable_joins = []
    
    for join in common_joins:
        left_table_norm = _normalize_table_name(join["left_table"])
        right_table_norm = _normalize_table_name(join["right_table"])
        
        if left_table_norm in table_set and right_table_norm in table_set:
            applicable_joins.append(join)
    
    return applicable_joins


def _fk_edges_from_introspection(engine, tables):
    insp = inspect(engine)
    edges = []
    # build a map original->normalized for quick reuse
    norm = {t: _normalize_table_name(t) for t in tables}
    for t in tables:
        try:
            fks = insp.get_foreign_keys(t) or []
        except Exception:
            continue
        for fk in fks:
            rt = fk.get("referred_table")
            rc = (fk.get("referred_columns") or [None])[0]
            lc = (fk.get("constrained_columns") or [None])[0]
            if rt and lc and rc:
                # Only add if referred table is among our selection (by normalized name)
                if _normalize_table_name(rt) in {norm[x] for x in tables}:
                    edges.append({
                        "left_table":  norm[t],
                        "left_column": lc,
                        "right_table": _normalize_table_name(rt),
                        "right_column": rc,
                        "join_type":   "INNER",
                        "source":      "auto_fk",
                        "confidence":  0.9,
                    })
    return edges


def _heuristic_edges(engine, tables):
    insp = inspect(engine)
    cols = {}
    norm = {t: _normalize_table_name(t) for t in tables}
    for t in tables:
        try:
            cols[t] = {c["name"] for c in insp.get_columns(t)}
        except Exception:
            cols[t] = set()

    edges = []
    for a in tables:
        for b in tables:
            if a >= b:  # avoid dup/self
                continue

            ca, cb = cols[a], cols[b]
            a_norm, b_norm = norm[a], norm[b]

            # pattern 1: a.customer_id -> b.id (prefix matches b)
            for col in list(ca):
                m = re.match(r"(.+)_id$", col)
                if m and ("id" in cb):
                    prefix = m.group(1).lower()
                    if prefix in b_norm or prefix.rstrip("s") in b_norm:
                        edges.append({
                            "left_table":  a_norm,
                            "left_column": col,
                            "right_table": b_norm,
                            "right_column": "id",
                            "join_type":   "INNER",
                            "source":      "auto_name",
                            "confidence":  0.6,
                        })

            # pattern 2: common business keys
            for k in (ca & cb) & {"id", "code", "key", "sku", "employee_id", "customer_id"}:
                edges.append({
                    "left_table":  a_norm,
                    "left_column": k,
                    "right_table": b_norm,
                    "right_column": k,
                    "join_type":   "INNER",
                    "source":      "auto_name",
                    "confidence":  0.5,
                })
    return edges



def _connect_tables_greedily(required_tables, edges, allow_steiner=True):
    """
    Connect all tables in 'required_tables' using the smallest set of edges we can
    find greedily. If allow_steiner=True, we may traverse intermediate tables that
    are NOT in 'required_tables' (multi-hop paths).
    'edges' must use normalized table names for left/right_table.
    """
    req = set(required_tables or [])
    if len(req) <= 1:
        return []

    # priority: predefined > auto_fk > auto_name, then higher confidence
    source_rank = {"predefined": 0, "auto_fk": 1, "auto_name": 2}
    E = sorted(edges, key=lambda e: (source_rank.get(e.get("source"), 9),
                                     -float(e.get("confidence", 0))))

    # Start with one required table
    connected = {next(iter(req))}
    chosen = []

    def req_connected():
        return req.issubset(connected)

    # We’ll keep adding the best edge that grows the connected component.
    # If allow_steiner, we may add nodes not in 'req'.
    while not req_connected():
        best = None

        for e in E:
            lt, rt = e["left_table"], e["right_table"]
            grows_from_connected = (lt in connected) ^ (rt in connected)  # exactly one side inside
            if not grows_from_connected:
                continue

            # If we don't allow Steiner nodes, only accept edges whose "new" endpoint is required
            new_node = rt if lt in connected else lt
            if (not allow_steiner) and (new_node not in req):
                continue

            best = e
            break

        if not best:
            # No more edges that expand towards other required nodes
            break

        chosen.append(best)
        connected.add(best["left_table"])
        connected.add(best["right_table"])

    # If we still didn't connect all required nodes, return what we have (caller will show an error)
    return chosen




@login_required
@require_POST
def suggest_joins_api(request):
    """Enhanced join suggestion with better error handling"""
    try:
        data = _json_body(request)
    except Exception:
        return JsonResponse({"success": False, "error": "Invalid JSON body."}, status=400)

    connection_id = (data.get("connection_id") or "").strip()
    tables_in = data.get("tables") or data.get("selected_tables") or []
    
    if not connection_id:
        return JsonResponse({"success": False, "error": "connection_id is required."}, status=400)
    
    if not isinstance(tables_in, list) or len(tables_in) < 2:
        return JsonResponse({
            "success": True,
            "joins": [],
            "needs_manual": True,
            "message": "Select at least two tables to suggest joins."
        })

    try:
        if not PermissionManager.check_user_permission(request.user, "connection", connection_id, "view"):
            return JsonResponse({"success": False, "error": "Forbidden."}, status=403)
    except Exception as e:
        logger.exception("Permission check failed: %s", e)
        return JsonResponse({"success": False, "error": "Permission check failed."}, status=500)

    # Get all available tables for discovery
    try:
        conn_obj = ExternalConnection.objects.get(id=connection_id)
        engine = get_external_engine(connection_id, request.user)
        insp = inspect(engine)
        all_tables = insp.get_table_names()
        discovery_tables = list(set(tables_in + all_tables))  # Include all tables for better discovery
    except Exception as e:
        logger.error(f"Failed to get tables for connection {connection_id}: {e}")
        return JsonResponse({"success": False, "error": f"Failed to access database: {str(e)}"})

    # Discover joins from all sources
    edges = []
    
    try:
        edges.extend(_predefined_join_edges(connection_id, discovery_tables))
    except Exception as e:
        logger.warning("Predefined join discovery failed: %s", e)

    if engine:
        try:
            edges.extend(_fk_edges_from_introspection(engine, discovery_tables))
        except Exception as e:
            logger.warning("FK join discovery failed: %s", e)
        
        try:
            edges.extend(_heuristic_edges(engine, discovery_tables))
        except Exception as e:
            logger.warning("Heuristic join discovery failed: %s", e)

    # Filter edges to only include the requested tables
    requested_tables_set = set(tables_in)
    filtered_edges = []
    for edge in edges:
        if (edge['left_table'] in requested_tables_set and 
            edge['right_table'] in requested_tables_set):
            filtered_edges.append(edge)

    # Choose minimal set of joins
    try:
        chosen = _connect_tables_greedily(tables_in, filtered_edges)
        needs_manual = len(chosen) < (len(requested_tables_set) - 1)
        
        return JsonResponse({
            "success": True,
            "joins": chosen,
            "edges": filtered_edges,
            "needs_manual": needs_manual,
            "diagnostics": {
                "requested_tables": tables_in,
                "available_edges": len(filtered_edges),
                "chosen_joins": len(chosen)
            }
        })
        
    except Exception as e:
        logger.exception("Join selection failed: %s", e)
        return JsonResponse({
            "success": True, 
            "joins": [],
            "needs_manual": True,
            "error": "Could not automatically connect all tables"
        })




@login_required
@require_POST
def debug_widget_data(request: HttpRequest, dashboard_id: _uuid.UUID, widget_id: _uuid.UUID) -> JsonResponse:
    """
    Debug endpoint to test widget data flow
    """
    dashboard = get_object_or_404(Dashboard, id=dashboard_id)
    
    body = _parse_json(request)
    logger.info(f"DEBUG WIDGET CALL:")
    logger.info(f"Widget ID: {widget_id}")
    logger.info(f"Type: {body.get('type')}")
    logger.info(f"Config: {body.get('dataConfig')}")
    logger.info(f"Dashboard config: {dashboard.config}")
    
    # Return simple test data
    return JsonResponse({
        "success": True,
        "rows": [
            {"label": "Test KPI", "value": 9999},
            {"label": "Another", "value": 8888}
        ]
    })

def _extract_fields_from_config(widget_config: Dict) -> Dict:
    """Extracts dimensions and measures from the dataConfig slots."""
    dimensions = []
    measures = []
    
    slots = widget_config.get('slots', {})
    for slot_key, assigned_fields in slots.items():
        if not isinstance(assigned_fields, list):
            continue
        for field in assigned_fields:
            field_id = field.get('fieldId')
            if not field_id:
                continue
            
            # The 'appliedRole' determines if it's treated as a dimension or measure
            if field.get('appliedRole') == 'measure':
                measures.append({
                    "field": field_id,
                    "aggregation": field.get('aggregation', 'sum')
                })
            else:
                dimensions.append({"field": field_id})
                
    return {"dimensions": dimensions, "measures": measures}

@login_required
def connection_table_columns_api(request, connection_id, table_name):
    """
    GET: list columns for a specific table within a connection.
    """
    try:
        conn_obj = ExternalConnection.objects.get(id=connection_id)
    except ExternalConnection.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Connection not found.'}, status=404)

    # Check if the user has permission to access this connection
    if not _user_accessible_connections(request.user).filter(id=conn_obj.id).exists():
        return JsonResponse({'success': False, 'error': 'Permission denied for this connection.'}, status=403)

    try:
        engine = get_external_engine(connection_id, request.user)
        insp = inspect(engine)
        
        # Check if table exists before getting columns
        all_tables = insp.get_table_names()
        if table_name not in all_tables:
            return JsonResponse({'success': False, 'error': f'Table "{table_name}" not found in connection.'}, status=404)
        
        # Get columns and format them as the frontend expects ({'name': ..., 'type': ...})
        columns = insp.get_columns(table_name)
        
        # The 'type' object from sqlalchemy is not directly JSON serializable, so we convert it to a string.
        formatted_columns = [
            {'name': col['name'], 'type': str(col['type'])}
            for col in columns
        ]
        
        return JsonResponse({'success': True, 'columns': formatted_columns})
    except Exception as e:
        logger.error(f"Failed to list columns for table '{table_name}': {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': f'Failed to list columns: {str(e)}'}, status=500)
    
