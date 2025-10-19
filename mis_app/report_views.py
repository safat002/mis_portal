# Enhanced Django Views for Report Builder
# This adds all the Flask functionality to your existing Django views

from collections import deque
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
import csv
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse
from django.shortcuts import redirect, render, get_object_or_404
from django.views.decorators.http import require_http_methods, require_POST
from django.views.decorators.csrf import csrf_exempt
from django.core.paginator import Paginator
from django.utils import timezone
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.db.models import Q
from sqlalchemy import inspect
from .models import ExternalConnection, SavedReport, ConnectionJoin, ReportShare
from .services.report_builder import ReportBuilderService
from .utils import get_external_engine, log_user_action
from .transformation_engine import TransformationEngine
from django.template.loader import render_to_string
from sqlalchemy import table, column, select, func
from .permissions import PermissionManager
from sqlalchemy.sql.sqltypes import Integer, Numeric, Float, BigInteger, SmallInteger, DECIMAL

import pdb


logger = logging.getLogger(__name__)

def _is_numeric_sqla_type(sqla_type):
    """Helper to check if a SQLAlchemy type is numeric."""
    try:
        return isinstance(sqla_type, (Integer, Numeric, Float, BigInteger, SmallInteger, DECIMAL))
    except Exception:
        return False

# ---- helpers (put near your other helpers) ----
NUMERIC_TYPE_TOKENS = (
    'int', 'bigint', 'smallint', 'decimal', 'numeric', 'real',
    'double', 'float', 'money'
)

def _to_json_safe(v):
    # order matters: check NaN/NaT early
    if v is None:
        return None
    # pandas/numpy missing
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    # numpy scalars
    if isinstance(v, (np.integer, )):
        return int(v)
    if isinstance(v, (np.floating, )):
        # guard against nan again
        return None if (isinstance(v, float) and (v != v)) else float(v)
    if isinstance(v, (np.bool_, )):
        return bool(v)
    # pandas/numpy datetimes
    if isinstance(v, (pd.Timestamp, )):
        return v.to_pydatetime().isoformat()
    if isinstance(v, (np.datetime64, )):
        return pd.to_datetime(v).to_pydatetime().isoformat()
    # dates/datetimes
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    # fallthrough
    return v

def _json_safe_obj(obj):
    if isinstance(obj, dict):
        return {str(k): _json_safe_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_json_safe_obj(x) for x in obj]
    return _to_json_safe(obj)

def _is_numeric_type_string(type_str: str) -> bool:
    s = (type_str or '').lower()
    return any(tok in s for tok in NUMERIC_TYPE_TOKENS)

def _infer_is_numeric(table_name: str, col_name: str, columns_meta=None) -> bool:
    """
    columns_meta: optional dict -> {(table, col) : sqlalchemy_type_string or db type}
    If you already gathered types via Inspector, send them in. Otherwise fall back to name heuristics.
    """
    if columns_meta:
        t = columns_meta.get((table_name, col_name))
        if t:
            return _is_numeric_type_string(str(t))
    # last-resort heuristic by column name
    name = f"{table_name}.{col_name}".lower()
    likely_numeric_name = any(k in name for k in ('amount', 'qty', 'quantity', 'price', 'total', 'sum', 'count', 'no', 'num', 'rate', 'cost'))
    return likely_numeric_name

def coerce_aggs_for_grouping(report_config: dict, columns_meta=None):
    """
    If there are groups, mutate columns so blank/NONE aggs become SUM for numeric, COUNT otherwise.
    Returns (mutated_columns, had_groups)
    """
    cols = report_config.get('columns', []) or []
    has_groups = bool(report_config.get('groups'))
    if not has_groups:
        return cols, False

    new_cols = []
    for c in cols:
        field = c.get('field')
        agg = (c.get('agg') or 'NONE').upper()
        if agg != 'NONE':
            new_cols.append(c)
            continue

        # parse table.col
        if field and '.' in field:
            table, col = field.split('.', 1)
        else:
            table, col = None, None

        if table and col and _infer_is_numeric(table, col, columns_meta):
            new_cols.append({**c, 'agg': 'SUM'})
        else:
            new_cols.append({**c, 'agg': 'COUNT'})
    report_config['columns'] = new_cols
    return new_cols, True

# ================================================================
# REPORT BUILDER VIEWS
# ================================================================

@login_required(login_url='mis_app:login')
def report_builder_view(request):
    """
    Final corrected version that correctly fetches connections based on both
    connection-level and table-level group permissions.
    """
    # --- Report fetching logic ---
    if request.user.is_admin_level():
        all_reports = SavedReport.objects.all().order_by('-updated_at')
    else:
        owned_reports = SavedReport.objects.filter(owner=request.user)
        shared_reports = SavedReport.objects.filter(reportshare__user=request.user).exclude(owner=request.user)
        all_reports = list(owned_reports) + list(shared_reports)

    paginator = Paginator(all_reports, 10)
    page_obj = paginator.get_page(request.GET.get('page'))

    # --- FINAL Connection Fetching Logic ---
    if request.user.is_admin_level():
        connections = ExternalConnection.objects.filter(is_active=True).order_by('nickname')
    else:
        # 1. Get all of the user's database-related permissions
        all_db_permissions = PermissionManager.get_user_permissions(request.user)
        
        # 2. Extract a unique set of all connection IDs they have any access to
        accessible_connection_ids = set()
        for perm in all_db_permissions:
            if perm.get('resource_type') in ['connection', 'table'] and perm.get('resource_id'):
                accessible_connection_ids.add(perm['resource_id'])

        # 3. Build the query to get connections the user owns OR has group access to
        connections = ExternalConnection.objects.filter(
            Q(is_active=True) & 
            (
                Q(owner=request.user) | 
                Q(id__in=list(accessible_connection_ids))
            )
        ).distinct().order_by('nickname')

    all_joins = list(ConnectionJoin.objects.values('left_table', 'left_column', 'right_table', 'right_column'))
    
    context = {
        'reports': page_obj,
        'connections': connections,
        'can_create_reports': True,
        'can_share_reports': request.user.user_type in ['Admin', 'Moderator'],
        'all_joins_json': json.dumps(all_joins),
        'default_connection_id': request.user.default_database_id if request.user.default_database else None,
    }

    return render(request, 'report_builder.html', context)

@login_required
@require_POST
def profile_data_api(request):
    """
    Takes a report config (+ optional recipe), executes a limited query to get a data preview,
    optionally applies the recipe to the preview, and returns the preview and column profiling info.
    """
    try:
        payload = json.loads(request.body)
        report_config = payload if isinstance(payload, dict) else {}
        recipe = report_config.get('data_prep_recipe') or report_config.get('recipe') or []

        # Limit rows for preview
        preview_config = {**report_config}
        preview_config['page'] = 1
        preview_config['page_size'] = 500

        service = ReportBuilderService()
        df, total_rows, error = service.build_advanced_report(preview_config, request.user)
        if error:
            return JsonResponse({'success': False, 'error': error}, status=400)

        # Apply recipe to the preview (client expects to see the transformed sample)
        if df is not None and len(recipe) > 0:
            transformer = TransformationEngine(df)
            df = transformer.apply_recipe(recipe)

        if df is None or df.empty:
            return JsonResponse({
                'success': True,
                'preview_data': [],
                'column_metadata': {},
                'message': 'No data returned for this configuration.'
            })

        # Profile
        transformer = TransformationEngine(df)
        column_metadata = {}
        for col_name in df.columns:
            profile = transformer.get_column_profile(col_name)
            if profile:
                profile['inferred_type'] = 'numeric' if df[col_name].dtype in ['int64', 'float64'] else 'text'
                column_metadata[col_name] = profile

        # Ensure preview rows are JSON-safe
        records = df.to_dict('records')
        preview_data = _json_safe_obj(records)

        # Ensure column_metadata is JSON-safe (profiles often contain numpy ints/floats)
        column_metadata = _json_safe_obj(column_metadata)

        return JsonResponse({
            'success': True,
            'preview_data': preview_data,
            'column_metadata': column_metadata,
        })

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON format.'}, status=400)
    except Exception as e:
        logger.error(f"Error profiling data: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

    
@login_required
def data_prep_modal_content(request):
    """
    A simple view that just renders and returns the HTML content for the
    data preparation modal.
    """
    return render(request, 'modals/data_prep_modal_content.html')
    
# ================================================================
# REPORT API ENDPOINTS - MATCHING FLASK FUNCTIONALITY
# ================================================================

@login_required
@require_POST
def build_report_api(request):
    """Main report building API with calculated fields, aliases, and validation."""
    try:
        data = json.loads(request.body or '{}')
        report_config = data

        connection_id = report_config.get('connection_id')
        if not connection_id:
            return JsonResponse({'error': 'Connection ID is required'}, status=400)
        try:
            connection = ExternalConnection.objects.get(id=connection_id)
        except ExternalConnection.DoesNotExist:
            return JsonResponse({'error': 'Connection not found'}, status=404)
        if not (connection.owner_id == request.user.id or PermissionManager.user_can_access_connection(request.user, connection)):
            return JsonResponse({'error': 'Connection not found or access denied'}, status=403)

        # columns_meta is optional; skip if you don't have it ready
        coerced_columns, has_groups = coerce_aggs_for_grouping(report_config, columns_meta=None)

        print("\n--- NEW REPORT EXECUTION ---") # Helps separate logs

        # Build the mapping for reorder & formula resolution
        field_to_df_col_map = {}

        # 2a) groups
        for g in report_config.get('groups', []) or []:
            g_field = g.get('field')
            g_method = (g.get('method') or 'exact').lower()
            if g_field:
                df_col = g_field.replace('.', '_') + f"_{g_method}"
                field_to_df_col_map[g_field] = df_col

        # 2b) columns (measures) -- NOW include their final agg suffix
        for c in report_config.get('columns', []) or []:
            f = c.get('field')
            agg = (c.get('agg') or 'NONE').upper()
            if f:
                suffix = '' if agg == 'NONE' else f"_{agg}"
                field_to_df_col_map[f] = f.replace('.', '_') + suffix

        # 2c) calcs map to themselves (list-safe)
        calcs_def = report_config.get('calculated_fields') or []
        calc_field_names = []
        if isinstance(calcs_def, dict):
            # legacy shape: {"Total Revenue": "...", ...}
            calc_field_names = [f"calc__{name}" for name in calcs_def.keys()]
        else:
            # current shape: [{"name": "...", "formula": "..."}, ...]
            calc_field_names = [f"calc__{c.get('name')}" for c in calcs_def if c.get('name')]

        for calc_field in calc_field_names:
            field_to_df_col_map[calc_field] = calc_field
        
        print(f"DEBUG: Field to DF Column Map: {field_to_df_col_map}")

        service_config, used_calculated_fields, user_requested_columns, column_alias_map = inject_calculated_fields(report_config)

        service = ReportBuilderService()
        df, total_rows, error = service.build_advanced_report(service_config, request.user)
        
        if df is not None:
            print(f"DEBUG: Columns after service call: {df.columns.to_list()}")
        else:
            print("DEBUG: DataFrame from service is None.")

        if error:
            return JsonResponse({'error': error}, status=400)

        if df is not None and not df.empty and used_calculated_fields:
            try:
                df = apply_calculated_fields(df, used_calculated_fields, field_to_df_col_map)
                print(f"DEBUG: Columns after applying calculations: {df.columns.to_list()}")
            except Exception as calc_error:
                return JsonResponse({'error': f'Formula error: {str(calc_error)}'}, status=400)
        
        user_group_fields   = [g['field'] for g in (report_config.get('groups') or [])]
        user_measure_fields = [c['field'] for c in (report_config.get('columns') or []) if not str(c.get('field','')).startswith('calc__')]
        user_calc_fields    = [c['field'] for c in (report_config.get('columns') or []) if str(c.get('field','')).startswith('calc__')]

        display_order = user_group_fields + user_measure_fields + user_calc_fields
        df = reorder_columns(df, display_order, field_to_df_col_map)

        print(f"DEBUG: Columns after reordering: {df.columns.to_list()}")

        # --- Data Prep (MVP: apply on DataFrame result) ---
        recipe = (report_config.get('data_prep_recipe') or [])
        if df is not None and len(recipe) > 0:
            try:
                transformer = TransformationEngine(df)
                df = transformer.apply_recipe(recipe)
            except Exception as e:
                # Don't break execution; surface a gentle message so users can fix the step
                logger.warning(f"Data Prep recipe failed on execute: {e}")

        if df is None or df.empty:
            return JsonResponse({ 'success': True, 'data': {'headers': [], 'rows': []}, 'total_rows': 0, 'message': 'No data returned for the specified criteria' })

        df_safe = df.replace({pd.NaT: None, pd.NA: None, np.nan: None})
        # ... (rest of function is the same)
        df_safe = df_safe.where(pd.notnull(df_safe), None)
        df_safe.rename(columns=column_alias_map, inplace=True)
        headers = list(df_safe.columns)
        rows = df_safe.to_dict('records')
        page = report_config.get('page', 1)
        page_size = report_config.get('page_size', 100)
        total_pages = (total_rows + page_size - 1) // page_size if page_size > 0 else 0
        try:
            log_user_action( request.user, 'execute_report', 'report', f"report_{timezone.now().strftime('%Y%m%d_%H%M%S')}", f"Report executed with {total_rows} rows", { 'connection_id': str(connection.id), 'connection_name': connection.nickname, 'row_count': total_rows, 'columns_count': len(report_config.get('columns', [])), 'filters_count': len(report_config.get('filters', [])), 'has_groups': bool(report_config.get('groups', [])), })
        except Exception as log_error:
            logger.warning(f"Failed to log execution: {log_error}")
        return JsonResponse({ 'success': True, 'data': {'headers': headers, 'rows': rows}, 'total_rows': total_rows, 'pagination': { 'current_page': page, 'page_size': page_size, 'total_pages': total_pages, }, 'execution_time': timezone.now().isoformat() })

    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON in request body'}, status=400)
    except Exception as e:
        logger.error(f"Error building report: {e}", exc_info=True)
        return JsonResponse({'error': f'Report execution failed: {str(e)}'}, status=500)
    
def inject_calculated_fields(report_config):
    import re
    service_config = report_config.copy()
    user_requested_columns = [c['field'] for c in report_config.get('columns', [])]
    column_alias_map = {}
    calculated_fields_definitions = report_config.get('calculated_fields', [])
    used_calculated_fields = {}
    source_columns_needed = set()
    service_columns = []

    formula_map = {f['name']: f['formula'] for f in calculated_fields_definitions}

    for col_def in report_config.get('columns', []):
        field_name = col_def['field']
        alias = col_def.get('alias')
        if alias:
            column_alias_map[field_name.replace('.', '_')] = alias

        if field_name.startswith('calc__'):
            calc_name = field_name.split('__', 1)[1]
            if calc_name in formula_map:
                formula = formula_map[calc_name]
                used_calculated_fields[field_name] = formula
                source_cols = re.findall(r'\[([^\]]+)\]', formula)
                source_columns_needed.update(source_cols)
        else:
            service_columns.append(col_def)

    current_fields = {c['field'] for c in service_columns if isinstance(c, dict) and 'field' in c}
    has_groups = bool(report_config.get('groups'))
    for sc in source_columns_needed:
        if sc not in current_fields:
            agg = 'SUM' if has_groups else 'NONE'
            service_columns.append({'field': sc, 'agg': agg})

    service_config['columns'] = service_columns
    return service_config, used_calculated_fields, user_requested_columns, column_alias_map

def apply_calculated_fields(df, used_calculated_fields, field_to_df_col_map):
    import re
    engine = TransformationEngine(df)

    # Helper to find the actual column name that exists in df
    def _resolve_df_col(original_name: str) -> str:
        # 1) try precomputed mapping
        candidate = field_to_df_col_map.get(original_name, original_name.replace('.', '_'))
        if candidate in df.columns:
            return candidate

        # 2) try common agg/group suffixes
        bases = [original_name.replace('.', '_')]
        suffixes = ['', '_SUM', '_AVG', '_COUNT', '_MIN', '_MAX',
                    '_exact', '_year', '_quarter', '_month', '_week']
        for base in bases:
            for suf in suffixes:
                cand = f"{base}{suf}" if suf else base
                if cand in df.columns:
                    return cand

        # 3) last resort: best-effort case-insensitive lookup
        lower_map = {c.lower(): c for c in df.columns}
        return lower_map.get(candidate.lower(), candidate)

    for new_col_name, formula in used_calculated_fields.items():
        def resolve_col_name(match):
            original_name = match.group(1)
            df_col_name = _resolve_df_col(original_name)
            return f"[{df_col_name}]"

        adapted_formula = re.sub(r'\[([^\]]+)\]', resolve_col_name, formula)
        print(f"DEBUG: Applying formula for {new_col_name}: {adapted_formula}")

        engine._apply_single_step({
            'strategy': 'calculate',
            'column': new_col_name,  # New column; engine now allows creation
            'params': {'new_column': new_col_name, 'formula': adapted_formula}
        })

    return engine.df


def reorder_columns(df, requested, field_to_df_col_map):
    final_order = []
    for name in requested:
        df_name = field_to_df_col_map.get(name, name)
        if df_name in df.columns:
            final_order.append(df_name)
    return df[final_order] if final_order else df

@login_required
def get_connections_api(request):
    """Get database connections - matches Flask /api/get_db_connections"""
    try:
        connections = ExternalConnection.objects.filter(owner=request.user).order_by('nickname')
        
        connections_data = []
        for conn in connections:
            # Check connection status (simplified)
            engine = get_external_engine(conn.id, request.user)
            connected = False
            if engine is not None:
                try:
                    with engine.connect() as cx:
                        cx.exec_driver_sql("SELECT 1")
                    connected = True
                except Exception:
                    connected = False
            
            connections_data.append({
                "id": str(conn.id),
                "nickname": conn.nickname,
                "db_type": conn.db_type,
                "host": conn.host,
                "port": conn.port,
                "db_name": conn.db_name,
                "filepath": conn.filepath,
                "is_default": conn.is_default,
                "connected": connected
            })
        
        return JsonResponse(connections_data, safe=False)
    except Exception as e:
        logger.error(f"Error fetching connections: {e}")
        return JsonResponse({'error': str(e)}, status=500)

@login_required
def get_tables_api(request):
    """Get visible tables for connection - matches Flask /api/get_tables"""
    connection_id = request.GET.get('connection_id')
    
    try:
        if not connection_id:
            return JsonResponse({'tables': [], 'error': 'Connection ID required'})

        connection = get_object_or_404(ExternalConnection, id=connection_id)

        # Must have at least 'view' at connection or table level
        if not PermissionManager.user_can_access_connection(request.user, connection):
            return JsonResponse({'tables': [], 'error': 'Permission denied'}, status=403)

        is_owner = getattr(connection, 'owner_id', None) == getattr(request.user, 'id', None)
        if request.user.is_admin_level() or is_owner:
            engine = get_external_engine(connection_id, request.user)
            if not engine:
                return JsonResponse({'tables': [], 'error': 'Database connection failed'})
            inspector = inspect(engine)
            schema = connection.schema if connection.db_type == 'postgresql' else None
            all_tables = inspector.get_table_names(schema=schema)

            hidden = set(filter(None, (connection.hidden_tables or '').split(',')))
            visible = sorted([t for t in all_tables if t not in hidden])

            allowed = PermissionManager.get_user_accessible_tables(request.user, connection_id)
            if not request.user.is_admin_level() and isinstance(allowed, list) and allowed:
                visible = [t for t in visible if t in set(allowed)]
            return JsonResponse({'tables': visible})

        # Non-owner path: just return granted tables (sorted)
        allowed = PermissionManager.get_user_accessible_tables(request.user, connection_id)
        return JsonResponse({'tables': sorted(allowed or [])})
        
    except Exception as e:
        logger.error(f"Error getting tables: {e}")
        return JsonResponse({'tables': [], 'error': str(e)})

@login_required
def get_columns_for_tables_api(request):
    """Get columns for multiple tables - matches Flask /api/get_columns_for_tables"""
    if request.method != 'POST':
        return JsonResponse({'columns': [], 'error': 'POST method required'})

    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        tables_to_query = data.get('tables', [])

        if not connection_id:
            return JsonResponse({'columns': [], 'error': 'Connection ID required'})

        if not isinstance(tables_to_query, list):
            return JsonResponse({'columns': [], 'error': 'Tables must be provided as a list'})

        connection = get_object_or_404(ExternalConnection, id=connection_id)

        if not PermissionManager.user_can_access_connection(request.user, connection):
            return JsonResponse({'columns': [], 'error': 'Permission denied'}, status=403)

        accessible_tables = PermissionManager.get_user_accessible_tables(request.user, connection_id)
        if accessible_tables is not None:
            allowed = {table for table in accessible_tables}
            tables_to_query = [table for table in tables_to_query if table in allowed]
            if not tables_to_query:
                return JsonResponse({'columns': [], 'error': 'Permission denied'}, status=403)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'columns': [], 'error': 'Database connection failed'})

        inspector = inspect(engine)
        schema = None
        if connection.db_type == 'postgresql' and connection.schema:
            schema_candidate = connection.schema.strip()
            if schema_candidate:
                schema = schema_candidate

        all_columns = []
        for table_name in tables_to_query:
            try:
                columns = inspector.get_columns(table_name, schema=schema)
                for col in columns:
                    all_columns.append({
                        "fullName": f"{table_name}.{col['name']}",
                        "name": col['name'],
                        "source": table_name,
                        "type": str(col['type']),
                        "is_numeric": _is_numeric_type(col['type'])
                    })
            except Exception as e:
                logger.warning(f"Could not get columns for table {table_name}: {e}")

        return JsonResponse({'columns': all_columns})

    except json.JSONDecodeError:
        return JsonResponse({'columns': [], 'error': 'Invalid JSON'})
    except Exception as e:
        logger.error(f"Error getting columns: {e}")
        return JsonResponse({'columns': [], 'error': str(e)})

@login_required
@login_required
def get_my_reports_api(request):
    """Get user's reports, including those owned and those shared with them."""
    try:
        user = request.user
        
        # Reports shared with the current user
        shared_report_ids = ReportShare.objects.filter(user=user).values_list('report_id', flat=True)
        
        # Combine owned reports and shared reports
        reports = SavedReport.objects.filter(
            Q(owner=user) | Q(id__in=shared_report_ids)
        ).distinct().order_by('-updated_at')
        
        # Get permissions for shared reports
        shares = ReportShare.objects.filter(user=user, report__in=reports).values('report_id', 'permission')
        permissions = {str(share['report_id']): share['permission'] for share in shares}

        data = []
        for report in reports:
            if report.owner == user:
                permission = 'owner'
            else:
                permission = permissions.get(str(report.id), 'view') # Default to 'view' if something is wrong

            data.append({
                'id': str(report.id),
                'name': report.report_name,
                'owner': report.owner.username,
                'permission': permission,
                'updated_at': report.updated_at.strftime('%Y-%m-%d %H:%M'),
            })

        return JsonResponse({'success': True, 'reports': data})
    except Exception as e:
        logger.error(f"Error in get_my_reports_api: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ADD THE FOLLOWING THREE NEW VIEWS TO THE END OF THE FILE

@login_required
def list_users_api(request):
    """Returns a list of all users, excluding the current user."""
    User = get_user_model()
    # Exclude current user and superusers if desired
    users = User.objects.exclude(id=request.user.id).exclude(is_superuser=True).values('id', 'username')
    return JsonResponse({'success': True, 'users': list(users)})

@login_required
def get_report_shares_api(request, report_id):
    """Gets the current sharing settings for a report."""
    report = get_object_or_404(SavedReport, id=report_id, owner=request.user)
    shares = ReportShare.objects.filter(report=report)
    data = [{
        'user_id': share.user.id,
        'username': share.user.username,
        'permission': share.permission
    } for share in shares]
    return JsonResponse({'success': True, 'shares': data})

@login_required
@require_POST
def update_report_shares_api(request, report_id):
    """Updates the sharing settings for a report."""
    report = get_object_or_404(SavedReport, id=report_id, owner=request.user)
    data = json.loads(request.body)
    shares_data = data.get('shares', [])

    # Delete existing shares for this report
    ReportShare.objects.filter(report=report).delete()

    User = get_user_model()
    new_shares = []
    for share_info in shares_data:
        try:
            user_to_share_with = User.objects.get(id=share_info.get('user_id'))
            new_shares.append(
                ReportShare(
                    report=report,
                    user=user_to_share_with,
                    permission=share_info.get('permission', 'view'),
                    shared_by=request.user
                )
            )

        except User.DoesNotExist:
            continue # Skip if user doesn't exist

    # Bulk create new shares
    ReportShare.objects.bulk_create(new_shares)
    return JsonResponse({'success': True, 'message': 'Sharing settings updated.'})

@login_required
@require_POST
def save_report_api(request):
    """Save new report - matches Flask /api/save_report"""
    try:
        data = json.loads(request.body)
        report_name = data.get('report_name', '').strip()
        report_config = data.get('report_config', {})
        description = data.get('description', '').strip()

        if not report_name:
            return JsonResponse({
                'success': False,
                'error': 'Report name is required'
            }, status=400)

        if not report_config:
            return JsonResponse({
                'success': False,
                'error': 'Report configuration is required'
            }, status=400)

        # Validate report configuration
        service = ReportBuilderService()
        validation = service.validate_report_config(report_config)
        if not validation['valid']:
            return JsonResponse({
                'success': False,
                'error': 'Invalid report configuration',
                'validation_errors': validation['errors']
            }, status=400)

        # Check for duplicate names
        if SavedReport.objects.filter(owner=request.user, report_name=report_name).exists():
            return JsonResponse({
                'success': False,
                'error': f'A report named "{report_name}" already exists'
            }, status=409)

        # Create new report
        new_report = SavedReport.objects.create(
            owner=request.user,
            report_name=report_name,
            report_config=report_config,
            description=description
        )

        # Log the action
        try:
            log_user_action(
                request.user,
                'save_report',
                'report',
                str(new_report.id),
                f"Report '{report_name}' saved",
                {
                    'report_id': str(new_report.id),
                    'report_name': report_name,
                    'has_calculated_fields': bool(report_config.get('calculated_fields', [])),
                    'columns_count': len(report_config.get('columns', [])),
                }
            )
        except Exception as log_error:
            logger.warning(f"Failed to log report save: {log_error}")

        return JsonResponse({
            'success': True,
            'report_id': str(new_report.id),
            'message': f'Report "{report_name}" saved successfully',
            'warnings': validation.get('warnings', [])
        })

    except json.JSONDecodeError:
        return JsonResponse({'success': False, 'error': 'Invalid JSON in request body'}, status=400)
    except Exception as e:
        logger.error(f"Error saving report: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
def get_report_config_api(request, report_id):
    """Get report configuration - matches Flask /api/get_report_config/<report_id>"""
    try:
        report = get_object_or_404(SavedReport, id=report_id, owner=request.user)
        
        return JsonResponse({
            'success': True,
            'config': report.report_config,
            'pivot_config': getattr(report, 'pivot_config', {}),
            'name': report.report_name,
            'permission': 'edit'  # User owns the report
        })
        
    except SavedReport.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Report not found'}, status=404)
    except Exception as e:
        logger.error(f"Error getting report config: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@require_POST
def update_report_api(request, report_id):
    """Update existing report - matches Flask update functionality"""
    try:
        data = json.loads(request.body)
        report_config = data.get('report_config')
        report_name = data.get('report_name')

        report = get_object_or_404(SavedReport, id=report_id, owner=request.user)
        
        if report_name:
            report.report_name = report_name.strip()
        
        if report_config:
            report.report_config = report_config

        report.save()

        return JsonResponse({
            'success': True,
            'message': f'Report "{report.report_name}" updated successfully'
        })
    except Exception as e:
        logger.error(f"Error updating report {report_id}: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@require_POST
def check_join_path_api(request):
    """
    Enhanced join path checking that properly uses data model joins.
    """
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        tables = data.get('tables', [])

        if not connection_id or len(tables) < 2:
            return JsonResponse({'success': True, 'path_exists': True, 'message': 'No join needed'})

        # Get connection and validate access
        connection = get_object_or_404(ExternalConnection, id=connection_id)
        if not PermissionManager.user_can_access_connection(request.user, connection):
            return JsonResponse({'tables': [], 'error': 'Permission denied'}, status=403)
        
        # Get all saved joins for this connection
        saved_joins = ConnectionJoin.objects.filter(connection=connection)
        
        # Build the graph properly
        graph = {}
        for join in saved_joins:
            try:
                left_table = join.left_table
                right_table = join.right_table
                
                # Add bidirectional connection
                if left_table not in graph:
                    graph[left_table] = []
                if right_table not in graph:
                    graph[right_table] = []
                    
                graph[left_table].append(right_table)
                graph[right_table].append(left_table)
            except Exception as e:
                logger.warning(f"Invalid join skipped: {e}")
                continue

        # Check connectivity using BFS
        if not graph:
            return JsonResponse({
                'success': True, 
                'path_exists': False,
                'message': 'No joins defined in data model'
            })

        # Start BFS from first table
        visited = set()
        queue = deque([tables[0]])
        
        while queue:
            current_table = queue.popleft()
            if current_table in visited:
                continue
                
            visited.add(current_table)
            
            # Add all connected tables to queue
            for neighbor in graph.get(current_table, []):
                if neighbor not in visited:
                    queue.append(neighbor)

        # Check if all required tables are connected
        path_exists = all(table in visited for table in tables)
        
        return JsonResponse({
            'success': True,
            'path_exists': path_exists,
            'message': f'Path found for {len(visited)} of {len(tables)} tables' if path_exists else 'No path found'
        })

    except Exception as e:
        logger.error(f"Error checking join path: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@require_POST  
def export_report_excel_api(request):
    """Export report to Excel - matches Flask export functionality"""
    try:
        data = json.loads(request.body)
        # --- FIX: Read headers and rows from the top level of the request data ---
        headers = data.get('headers', [])
        rows = data.get('rows', [])
        report_config = data.get('report_config', {})
        
        if not headers or not rows:
            return JsonResponse({'success': False, 'error': 'No data to export'}, status=400)

        # Create DataFrame
        df = pd.DataFrame(rows)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_name = report_config.get('name', 'report')
        safe_name = "".join(c for c in report_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"{safe_name}_{timestamp}.xlsx"

        # Create Excel response
        response = HttpResponse(
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        with pd.ExcelWriter(response, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Report Data', index=False)
            
            # Add metadata sheet
            metadata = pd.DataFrame([
                ['Export Date', datetime.now().strftime('%Y-%m-%d %H:%M:%S')],
                ['User', request.user.username],
                ['Total Rows', len(rows)],
                ['Columns', len(headers)],
                ['Filters Applied', len(report_config.get('filters', []))],
                ['Groups Applied', len(report_config.get('groups', []))],
            ], columns=['Property', 'Value'])
            metadata.to_excel(writer, sheet_name='Metadata', index=False)

        # Log export action
        try:
            log_user_action(
                request.user,
                'export_report',
                'report',
                f'export_{timestamp}',
                'Report exported as Excel',
                {'format': 'excel', 'row_count': len(rows), 'filename': filename}
            )
        except Exception as log_error:
            logger.warning(f"Failed to log export action: {log_error}")

        return response

    except Exception as e:
        logger.error(f"Error exporting to Excel: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@require_POST
def get_filter_values_api(request):
    """
    Gets distinct values for a list of fields, to be used in dynamic filters.
    """
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        fields = data.get('fields', [])

        if not connection_id or not fields:
            return JsonResponse({'success': False, 'error': 'Connection ID and fields are required.'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'error': 'Database connection failed.'}, status=500)

        results = {}
        with engine.connect() as conn:
            for field_name in fields:
                try:
                    table_name, column_name = field_name.split('.')
                    tbl = table(table_name, column(column_name))
                    
                    # Check if column is numeric for min/max
                    inspector = inspect(engine)
                    columns = inspector.get_columns(table_name)
                    col_info = next((c for c in columns if c['name'] == column_name), None)
                    is_numeric = False
                    if col_info:
                        is_numeric = _is_numeric_sqla_type(col_info['type'])

                    if is_numeric:
                        # For numeric columns, get min and max
                        query = select(func.min(tbl.c[column_name]), func.max(tbl.c[column_name]))
                        min_val, max_val = conn.execute(query).first()
                        results[field_name] = {'min': min_val, 'max': max_val}
                    else:
                        # For other columns, get distinct values
                        query = select(tbl.c[column_name]).distinct().limit(100) # Limit to 100 distinct values
                        distinct_values = [row[0] for row in conn.execute(query)]
                        results[field_name] = {'values': distinct_values}
                except Exception as e:
                    logger.warning(f"Could not get filter values for {field_name}: {e}")
                    results[field_name] = {'error': str(e)}

        return JsonResponse({'success': True, 'data': results})

    except Exception as e:
        logger.error(f"Error getting filter values: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
@require_POST
def find_joins_api(request):
    """
    Finds and returns the necessary joins to connect a given list of tables
    using a spanning tree algorithm.
    """
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        tables = data.get('tables', [])

        if not connection_id or len(tables) < 2:
            return JsonResponse({'success': True, 'joins': []})

        connection = get_object_or_404(ExternalConnection, id=connection_id)
        if not PermissionManager.user_can_access_connection(request.user, connection):
            return JsonResponse({'success': False, 'error': 'Permission denied.'}, status=403)
        
        saved_joins_query = ConnectionJoin.objects.filter(connection=connection)
        all_model_joins = [{
            "left_col": f"{j.left_table}.{j.left_column}", 
            "right_col": f"{j.right_table}.{j.right_column}", 
            "type": (j.join_type or "INNER").upper(),
            "left_table": j.left_table,
            "right_table": j.right_table
        } for j in saved_joins_query]

        graph = {}
        for join in all_model_joins:
            left = join["left_table"]
            right = join["right_table"]
            if left not in graph: graph[left] = []
            if right not in graph: graph[right] = []
            graph[left].append(right)
            graph[right].append(left)

        # Check for connectivity
        q = deque([tables[0]])
        visited = {tables[0]}
        while q:
            current = q.popleft()
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    q.append(neighbor)
        
        if not all(t in visited for t in tables):
            return JsonResponse({'success': True, 'joins': []}) # No path exists

        # Build a spanning tree of joins
        joins_to_return = []
        q = deque([tables[0]])
        visited_for_joins = {tables[0]}
        while q:
            current_table = q.popleft()
            for neighbor in graph.get(current_table, []):
                if neighbor in tables and neighbor not in visited_for_joins:
                    visited_for_joins.add(neighbor)
                    q.append(neighbor)
                    
                    join_to_use = next((j for j in all_model_joins if {j['left_table'], j['right_table']} == {current_table, neighbor}), None)
                    if join_to_use:
                        joins_to_return.append(join_to_use)

        final_joins = [{
            'left_col': j['left_col'],
            'right_col': j['right_col'],
            'type': j['type']
        } for j in joins_to_return]

        return JsonResponse({'success': True, 'joins': final_joins})

    except Exception as e:
        logger.error(f"Error finding joins: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# ================================================================
# HELPER FUNCTIONS
# ================================================================

def report_detail_api(request, report_id):
    """
    Get the full configuration and metadata for a single saved report.
    """
    try:
        report = get_object_or_404(SavedReport, id=report_id, owner=request.user)
        
        return JsonResponse({
            'success': True,
            'id': str(report.id),  # <--- THIS IS THE CRITICAL FIX
            'config': report.report_config,
            'name': report.report_name,
            'description': getattr(report, 'description', ''),
            'permission': 'edit'
        })
    except SavedReport.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Report not found'}, status=404)
    except Exception as e:
        logger.error(f"Error getting report detail for ID {report_id}: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


def get_report_suggestions_api(request, connection_id):
    """
    Provides intelligent report suggestions for a given connection.
    """
    try:
        service = ReportBuilderService()
        suggestions = service.get_report_suggestions(connection_id, request.user)
        return JsonResponse({'success': True, 'suggestions': suggestions})
    except Exception as e:
        logger.error(f"Error generating report suggestions: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@require_POST
def validate_report_config_api(request):
    """
    Validates a report configuration without executing it.
    """
    try:
        report_config = json.loads(request.body)
        service = ReportBuilderService()
        validation = service.validate_report_config(report_config)
        return JsonResponse(validation)
    except json.JSONDecodeError:
        return JsonResponse({'valid': False, 'errors': ['Invalid JSON format.']}, status=400)
    except Exception as e:
        logger.error(f"Error validating report config: {e}")
        return JsonResponse({'valid': False, 'errors': [str(e)]}, status=500)

def _is_numeric_type(column_type):
    """Check if column type is numeric"""
    type_str = str(column_type).lower()
    numeric_types = ['integer', 'bigint', 'smallint', 'decimal', 'numeric', 'real', 'double', 'float']
    return any(num_type in type_str for num_type in numeric_types)

@login_required
@require_POST
def auto_find_joins_api(request):
    """
    Automatically find and suggest joins for the given tables based on common column names.
    This is a simple heuristic and may not find all complex joins.
    """
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        tables = data.get('tables', [])
        
        if not connection_id or len(tables) < 2:
            return JsonResponse({'success': True, 'joins': [], 'message': 'Not enough tables to join.'})
            
        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'error': 'Database connection failed.'}, status=500)
            
        inspector = inspect(engine)
        suggested_joins = []
        
        # Get columns for each table involved
        table_columns = {}
        for table_name in tables:
            try:
                # Store column names in a set for efficient lookup
                table_columns[table_name] = {col['name'].lower() for col in inspector.get_columns(table_name)}
            except Exception as e:
                logger.warning(f"Could not inspect table {table_name}: {e}")
        
        # Suggest joins based on common column names (a common simple heuristic)
        # This iterates through unique pairs of tables
        for i, table1 in enumerate(tables):
            for table2 in tables[i+1:]:
                if table1 not in table_columns or table2 not in table_columns:
                    continue
                
                # Find the intersection of column names between the two tables
                common_cols = table_columns[table1].intersection(table_columns[table2])
                
                for col in common_cols:
                    # Avoid joining on generic 'id' columns if other options exist, unless it's the only option
                    if len(common_cols) > 1 and col in ['id', 'pk']:
                        continue
                    
                    suggested_joins.append({
                        'left_col': f"{table1}.{col}",
                        'right_col': f"{table2}.{col}",
                        'type': 'INNER',
                    })
                    # Break after finding one common column to avoid multiple joins between the same two tables
                    break
        
        return JsonResponse({
            'success': True,
            'joins': suggested_joins
        })
        
    except Exception as e:
        logger.error(f"Error during auto-join detection: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)
    
@login_required
@require_POST
def export_report_csv_api(request):
    """Export report to CSV."""
    try:
        data = json.loads(request.body)
        headers = data.get('headers', [])
        rows = data.get('rows', [])
        
        if not headers or not rows:
            return JsonResponse({'success': False, 'error': 'No data to export'}, status=400)

        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="report_{timezone.now().strftime("%Y%m%d")}.csv"'

        writer = csv.writer(response)
        writer.writerow(headers)
        for row in rows:
            writer.writerow([row.get(h) for h in headers])

        return response
    except Exception as e:
        logger.error(f"Error exporting to CSV: {e}")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)