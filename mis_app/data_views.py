# mis_app/data_views.py

"""
Django Views for Data Management
Complete implementation with all Flask functionality converted to Django
"""
from .permissions import table_permission_required
from .permissions import PermissionManager
import json
import logging
import os
import uuid
import pandas as pd
import numpy as np
import openpyxl
import re
import traceback
from datetime import datetime, date
from decimal import Decimal

from django.contrib.auth.decorators import login_required
from django.core.paginator import Paginator
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_http_methods, require_POST
from django.core.files.storage import FileSystemStorage
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt
from django.middleware.csrf import get_token
from werkzeug.utils import secure_filename

from sqlalchemy import text, inspect, table, column, delete, MetaData, Integer, BigInteger, SmallInteger, Float, Numeric, DECIMAL, REAL
from sqlalchemy.types import VARCHAR, TEXT, BOOLEAN, DATE, TIMESTAMP
from sqlalchemy.exc import IntegrityError, OperationalError

from .models import ExternalConnection, User
from .utils import get_external_engine
from .permissions import table_permission_required, connection_permission_required 

logger = logging.getLogger(__name__)


def _read_csv_with_fallback(path: str, low_memory: bool = False) -> pd.DataFrame:
    """Read a CSV trying common encodings to avoid UnicodeDecodeError.
    Tries utf-8, utf-8-sig, cp1252, and latin1 in order.
    """
    encodings = ["utf-8", "utf-8-sig", "cp1252", "latin1"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, low_memory=low_memory, encoding=enc)
        except UnicodeDecodeError as e:
            last_err = e
            continue
        except Exception:
            # If it's not an encoding error, let the caller handle it
            raise
    if last_err:
        raise last_err
    raise ValueError("Unable to read CSV with fallback encodings")

def infer_sql_type(column_data: pd.Series) -> str:
    """
    Infers the most appropriate SQL data type for a given pandas Series.
    Handles numeric, datetime, boolean, categorical, and text types with fallback logic.
    """

    if column_data.empty or column_data.dropna().empty:
        return 'VARCHAR(255)'  # Default for empty or null-only columns

    total = len(column_data)
    non_null = column_data.notna().sum()
    null_ratio = 1 - (non_null / total)

    # Normalize input
    raw = column_data.dropna()

    # 1. Check for boolean-like values
    bool_values = {'true', 'false', 'yes', 'no', '1', '0'}
    raw_str = raw.astype(str).str.lower().str.strip()
    if raw_str.isin(bool_values).sum() / len(raw_str) > 0.95:
        return 'BOOLEAN'

    # 2. Check for numeric types
    numeric = pd.to_numeric(raw, errors='coerce')
    numeric_valid = numeric.notna().sum() / len(raw)
    if numeric_valid > 0.9:
        if (numeric.dropna() % 1 != 0).any():
            return 'FLOAT'
        else:
            max_val = numeric.max()
            if max_val < 2**31:
                return 'INT'
            else:
                return 'BIGINT'

    # 3. Check for datetime types
    try:
        datetime = pd.to_datetime(raw, errors='coerce', infer_datetime_format=True)
        datetime_valid = datetime.notna().sum() / len(raw)
        if datetime_valid > 0.9:
            return 'DATETIME'
    except Exception:
        pass

    # 4. Check for categorical/enumerated types
    unique_vals = raw_str.nunique()
    if unique_vals <= 20 and unique_vals / len(raw_str) < 0.5:
        max_enum_len = raw_str.str.len().max()
        safe_enum_len = max(int(max_enum_len * 1.2) + 10, 20)
        return f'VARCHAR({safe_enum_len})'  # Treat as ENUM-like

    # 5. Fallback to text
    max_len = raw_str.str.len().max()
    if max_len > 1000:
        return 'TEXT'
    else:
        safe_len = max(int(max_len * 1.2) + 10, 50)
        return f'VARCHAR({safe_len})'

def normalize_header(header):
    """Cleans a header string to be a normalized identifier."""
    return re.sub(r'[^a-z0-9_]', '_', str(header).lower())

def sanitize_column_name(col_name):
    """Cleans up a column name to be SQL-friendly."""
    col_name = str(col_name).strip()
    col_name = re.sub(r'\s+', '_', col_name)  # Replace spaces with underscores
    col_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name)  # Remove special characters
    return col_name.lower()

def is_numeric_type(col_type):
    """Checks if a SQLAlchemy column type is numeric."""
    return isinstance(col_type, (Integer, BigInteger, SmallInteger, Float, Numeric, DECIMAL, REAL))

def get_visible_tables(connection_id=None, user=None):
    """Return tables the user can see for a connection, honoring group/table permissions and hidden tables."""
    try:
        if not (connection_id and user):
            return []

        # 1) Load the connection (no owner gate) and verify the user has at least 'view' at connection OR table level
        connection = get_object_or_404(ExternalConnection, id=connection_id)
        if not PermissionManager.user_can_access_connection(user, connection):
            return []

        # 2) If user is admin OR owner: read schema, then apply hidden-tables filter
        is_owner = getattr(connection, 'owner_id', None) == getattr(user, 'id', None)
        if getattr(user, 'is_admin_level', lambda: False)() or is_owner:
            # ... (Existing logic for fetching tables for Admin/Owner remains here)
            engine = get_external_engine(connection_id, user)
            if not engine:
                return []
            inspector = inspect(engine)
            schema = connection.schema if connection.db_type == 'postgresql' and connection.schema else None
            all_tables = inspector.get_table_names(schema=schema)

            hidden = {t.strip() for t in (connection.hidden_tables or '').split(',') if t.strip()}
            visible = sorted([t for t in all_tables if t not in hidden])

            # Apply explicit table grants if user is not admin but is owner, etc. (this check is secondary now)
            allowed = PermissionManager.get_user_accessible_tables(user, connection_id)
            if not getattr(user, 'is_admin_level', lambda: False)() and isinstance(allowed, list) and allowed:
                visible = [t for t in visible if t in set(allowed)]
            return visible

        # 3) Non-owner, non-admin: Get the granted table list or 'None' for blanket access.
        tables = PermissionManager.get_user_accessible_tables(user, connection_id)

        # --- CRITICAL FIX STARTS HERE ---
        # If None => blanket connection access (Uploader/Moderator): read schema and apply hidden-table filter
        if tables is None:
            # The user has connection-level permission, so we fetch all non-hidden tables.
            engine = get_external_engine(connection_id, user)
            if not engine:
                return []
            inspector = inspect(engine)
            schema = connection.schema if connection.db_type == 'postgresql' and connection.schema else None
            all_tables = inspector.get_table_names(schema=schema)

            hidden = {t.strip() for t in (connection.hidden_tables or '').split(',') if t.strip()}
            return sorted([t for t in all_tables if t not in hidden])

        # Otherwise, return just the explicitly granted table list (or empty if tables is [])
        return sorted(tables or [])
        # --- CRITICAL FIX ENDS HERE ---

    except Exception:
        return []
    
def get_quote_char(engine):
    """Returns the appropriate quote character for the database dialect."""
    if engine.dialect.name == 'mysql':
        return "`"
    else:
        return '"'

@login_required
@table_permission_required('table', permission_level='upload')
def upload_data_api(request):
    """Upload data to an existing table."""
    try:
        connection_id = request.POST.get('connection_id')
        table_name = request.POST.get('table_name')
        has_headers = request.POST.get('has_headers') == 'true'
        replace_table = request.POST.get('replace_table') == 'true'
        file = request.FILES.get('file')

        if not all([connection_id, table_name, file]):
            return JsonResponse({'success': False, 'error': 'Missing connection, table name, or file.'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'error': 'Database connection failed.'}, status=500)

        filename = secure_filename(file.name)
        file_ext = os.path.splitext(filename)[1].lower()

        # Read the file
        if file_ext == '.csv':
            df = pd.read_csv(file, header=0 if has_headers else None)
        elif file_ext in ['.xls', '.xlsx']:
            df = pd.read_excel(file, header=0 if has_headers else None)
        else:
            return JsonResponse({'success': False, 'error': 'Unsupported file type. Please use CSV or Excel.'}, status=400)
            
        # Set column names if no headers
        if not has_headers:
            df.columns = [f'column_{i+1}' for i in range(len(df.columns))]

        # Normalize column names
        df.columns = [normalize_header(col) for col in df.columns]
        
        # Upload to database
        upload_method = 'replace' if replace_table else 'append'
        df.to_sql(table_name, engine, if_exists=upload_method, index=False)

        return JsonResponse({
            'success': True,
            'message': f'Successfully uploaded {len(df)} rows to table "{table_name}".'
        })
    except Exception as e:
        logger.error(f"Error during file upload: {str(e)}")
        if "does not exist" in str(e):
             return JsonResponse({'success': False, 'error': f'Upload failed. Table "{table_name}" does not exist in the database.'}, status=500)
        return JsonResponse({'success': False, 'error': f'An error occurred during upload: {str(e)}'}, status=500)

# API Views
@login_required
@require_POST
def check_password(request):
    """Verifies the current user's password."""
    try:
        data = json.loads(request.body)
        password = data.get('password')
        if not request.user.check_password(password):
            return JsonResponse({'success': False, 'message': 'Incorrect password.'}, status=403)
        return JsonResponse({'success': True})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=400)

@login_required
@require_POST
def inspect_file(request):
    """Inspect an uploaded file and return metadata like sheet names."""
    try:
        if 'file' not in request.FILES:
            return JsonResponse({'success': False, 'message': 'No file provided.'}, status=400)
        
        file = request.FILES['file']
        filename = secure_filename(file.name)
        
        # Check file type
        if not (filename.endswith('.csv') or filename.endswith(('.xls', '.xlsx'))):
            return JsonResponse({'success': False, 'message': 'Unsupported file type.'}, status=400)
        
        # Save file temporarily
        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        temp_filename = f"{uuid.uuid4().hex}_{filename}"
        temp_file_path = os.path.join(upload_dir, temp_filename)
        
        with open(temp_file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)
        
        # Extract metadata
        sheets = []
        if filename.endswith(('.xls', '.xlsx')):
            try:
                workbook = openpyxl.load_workbook(temp_file_path, read_only=True, data_only=True)
                sheets = workbook.sheetnames
            except Exception as e:
                os.remove(temp_file_path)
                return JsonResponse({'success': False, 'message': f'Could not read Excel file: {e}'}, status=500)
        
        return JsonResponse({
            'success': True, 
            'temp_filename': temp_filename, 
            'sheets': sheets,
            'filename': filename
        })
    except Exception as e:
        logger.error(f"Error in inspect_file: {str(e)}")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='view')
def get_columns_for_table(request, connection_id, table_name):
    """Gets detailed column information for a single table."""
    try:
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'error': 'Database connection failed.'}, status=500)

        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)

        # Build an auto-increment/identity map by engine
        auto_flags = {}
        dialect = engine.dialect.name
        quote = engine.dialect.identifier_preparer.quote

        # Try to honor schema for Postgres, and database for MySQL
        table_schema = None
        try:
            ext_conn = ExternalConnection.objects.filter(id=connection_id).only('schema', 'db_name', 'db_type').first()
            if ext_conn and ext_conn.db_type == 'postgresql':
                table_schema = (ext_conn.schema or None)
        except Exception:
            table_schema = None

        try:
            with engine.connect() as conn:
                if dialect == 'postgresql':
                    params = {'t': table_name}
                    schema_clause = ''
                    if table_schema:
                        schema_clause = ' AND table_schema = :s'
                        params['s'] = table_schema
                    sql = (
                        "SELECT column_name, is_identity, column_default "
                        "FROM information_schema.columns "
                        "WHERE table_name = :t" + schema_clause
                    )
                    rows = conn.execute(text(sql), params).fetchall()
                    for r in rows:
                        colname = r[0]
                        is_ident = (r[1] or '').upper() == 'YES'
                        coldef = r[2] or ''
                        is_seq_default = isinstance(coldef, str) and 'nextval(' in coldef
                        auto_flags[colname] = bool(is_ident or is_seq_default)
                elif dialect == 'mysql':
                    # Use information_schema for portability
                    db_name = getattr(ext_conn, 'db_name', None)
                    params = {'t': table_name}
                    where_db = ''
                    if db_name:
                        where_db = ' AND table_schema = :db'
                        params['db'] = db_name
                    sql = (
                        "SELECT column_name, extra FROM information_schema.columns "
                        "WHERE table_name = :t" + where_db
                    )
                    rows = conn.execute(text(sql), params).fetchall()
                    for r in rows:
                        colname = r[0]
                        extra = (r[1] or '').lower()
                        auto_flags[colname] = 'auto_increment' in extra
                else:
                    # sqlite and others: no auto-increment toggle supported in UI
                    pass
        except Exception:
            # Best effort; leave auto_flags empty on errors
            auto_flags = {}

        column_details = [
            {
                'name': col['name'],
                'type': str(col['type']),
                'auto_increment': bool(auto_flags.get(col['name'], False)),
                'nullable': bool(col.get('nullable', True)),
            }
            for col in columns
        ]
        
        try:
            pk_constraint = inspector.get_pk_constraint(table_name)
            pks = pk_constraint.get('constrained_columns', []) if pk_constraint else []
        except:
            pks = []
        
        return JsonResponse({'success': True, 'columns': column_details, 'pks': pks})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def rename_table(request):
    """Rename a table."""
    try:
        
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        old_name = data.get('old_name')
        new_name = data.get('new_name')
        
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)
        
        with engine.begin() as connection:
            if engine.dialect.name in ['postgresql', 'sqlite']:
                query = text(f'ALTER TABLE "{old_name}" RENAME TO "{new_name}"')
            elif engine.dialect.name == 'mysql':
                query = text(f'RENAME TABLE `{old_name}` TO `{new_name}`')
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported database type: {engine.dialect.name}'}, status=400)
            connection.execute(query)
        
        return JsonResponse({'success': True, 'message': f'Table {old_name} renamed to {new_name}.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def rename_column(request):
    """Rename a column."""
    try:
        # Parse payload
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        old_name = data.get('old_name')
        new_name = data.get('new_name')

        # Add debug logging
        logger.info(f"Rename column request: {data}")

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)

        # Validate inputs
        if not table_name or not old_name or new_name is None:
            return JsonResponse({'success': False, 'message': 'Missing required parameters.'}, status=400)

        # If no change, return success (no-op)
        if str(old_name) == str(new_name):
            return JsonResponse({'success': True, 'message': 'Column name unchanged.'})

        # Validate new column identifier (backend rule keeps UI-safe identifiers)
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', str(new_name)):
            return JsonResponse({'success': False, 'message': 'Invalid new column name.'}, status=400)

        with engine.begin() as connection:
            q = get_quote_char(engine)
            inspector = inspect(engine)
            # Get existing columns to guard against conflicts
            existing_cols = [c['name'] for c in inspector.get_columns(table_name)]

            if old_name not in existing_cols:
                return JsonResponse({'success': False, 'message': f'Column {old_name} not found.'}, status=404)

            # Prevent duplicate-column error: if target already exists, fail gracefully
            if new_name in existing_cols:
                return JsonResponse({'success': False, 'message': f"Column '{new_name}' already exists."}, status=409)

            if engine.dialect.name in ['postgresql', 'sqlite']:
                query = text(f'ALTER TABLE {q}{table_name}{q} RENAME COLUMN {q}{old_name}{q} TO {q}{new_name}{q}')
            elif engine.dialect.name == 'mysql':
                # For MySQL, we need to know the column type to use CHANGE COLUMN
                cols = inspector.get_columns(table_name)
                col_info = next((c for c in cols if c['name'] == old_name), None)
                if not col_info:
                    return JsonResponse({'success': False, 'message': f'Column {old_name} not found.'}, status=404)
                col_type = str(col_info['type'])
                query = text(f'ALTER TABLE {q}{table_name}{q} CHANGE COLUMN {q}{old_name}{q} {q}{new_name}{q} {col_type}')
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported database type: {engine.dialect.name}'}, status=400)
            
            connection.execute(query)

        return JsonResponse({'success': True, 'message': f'Column {old_name} renamed to {new_name}.'})
    except Exception as e:
        logger.error(f"Error renaming column: {str(e)}")
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def truncate_table(request):
    """Truncate a table (delete all data)."""
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        password = data.get('password')
        
        if not request.user.check_password(password):
            return JsonResponse({'success': False, 'message': 'Incorrect password.'}, status=403)
        
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)
        
        with engine.begin() as connection:
            if engine.dialect.name == 'mysql':
                query = text(f"TRUNCATE TABLE `{table_name}`")
            else:
                query = text(f'DELETE FROM "{table_name}"')
            connection.execute(query)
        
        return JsonResponse({'success': True, 'message': f'All data from {table_name} has been deleted.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def drop_table(request):
    """
    Permanently drops a table from the external database.
    Hardened against SQL injection, permission abuse, and partial failures.
    """
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        password = data.get('password')
        cascade = bool(data.get('cascade'))

        if not all([connection_id, table_name, password]):
            return JsonResponse({'success': False, 'message': 'Missing required parameters.'}, status=400)

        if not request.user.check_password(password):
            return JsonResponse({'success': False, 'message': 'Incorrect password.'}, status=403)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)

        q = get_quote_char(engine)
        inspector = inspect(engine)
        existing_tables = [t.lower() for t in inspector.get_table_names()]
        if table_name.lower() not in existing_tables:
            return JsonResponse({'success': False, 'message': f"Table '{table_name}' does not exist."}, status=404)

        with engine.begin() as connection:
            if engine.dialect.name == 'postgresql' and cascade:
                query = text(f'DROP TABLE {q}{table_name}{q} CASCADE')
            else:
                query = text(f'DROP TABLE {q}{table_name}{q}')
            connection.execute(query)

        logger.info(f"[{request.user.username}] Dropped table '{table_name}' on connection {connection_id}")
        return JsonResponse({'success': True, 'message': f"Table '{table_name}' has been permanently deleted."})

    except Exception as e:
        msg = str(e)
        logger.error(f"[{request.user.username}] Error dropping table '{table_name}' on connection {connection_id}: {msg}")
        # Handle PostgreSQL dependency error (SQLSTATE 2BP01)
        if 'dependent objects' in msg.lower() or 'Use DROP ... CASCADE' in msg or '2BP01' in msg:
            return JsonResponse({
                'success': False,
                'message': msg,
                'requires_cascade': True,
            }, status=409)
        return JsonResponse({'success': False, 'message': f"An error occurred: {msg}"}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def delete_rows(request):
    """Delete specific rows from a table."""
    try:
                
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        pks_to_delete = data.get('pks')
        pk_column = data.get('pk_column')
        
        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)
        
        with engine.begin() as connection:
            q = get_quote_char(engine)
            
            # Fix the query to properly handle different data types
            if engine.dialect.name == 'postgresql':
                # For PostgreSQL, we need to cast the values if they're numeric
                try:
                    # Try to convert to integers to check if they're numeric
                    [int(pk) for pk in pks_to_delete]
                    placeholders = ','.join([f':pk{i}' for i in range(len(pks_to_delete))])
                    query_sql = f'DELETE FROM {q}{table_name}{q} WHERE {q}{pk_column}{q}::text IN ({placeholders})'
                except ValueError:
                    # If not numeric, treat as strings
                    placeholders = ','.join([f':pk{i}' for i in range(len(pks_to_delete))])
                    query_sql = f'DELETE FROM {q}{table_name}{q} WHERE {q}{pk_column}{q} IN ({placeholders})'
            else:
                # For other databases
                placeholders = ','.join([f':pk{i}' for i in range(len(pks_to_delete))])
                query_sql = f'DELETE FROM {q}{table_name}{q} WHERE {q}{pk_column}{q} IN ({placeholders})'

            params = {f'pk{i}': pk for i, pk in enumerate(pks_to_delete)}
            result = connection.execute(text(query_sql), params)
        
        return JsonResponse({'success': True, 'message': f'Successfully deleted {result.rowcount} rows.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='view')
def get_table_data(request, connection_id, table_name):
    """Get table data with pagination and proper error handling."""
    try:
        if not re.match(r'^[a-zA-Z0-9_]+$', table_name):
            return JsonResponse({'data': [], 'error': 'Invalid table name format'}, status=400)

        connection_details = get_object_or_404(ExternalConnection, id=connection_id)
        if not PermissionManager.user_can_access_connection(request.user, connection_details):
            return JsonResponse({'success': False, 'data': [], 'error': 'Permission denied.'}, status=403)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'data': [], 'error': 'External database not connected.'}, status=400)

        schema = None
        if connection_details.db_type == 'postgresql' and connection_details.schema:
            schema_candidate = connection_details.schema.strip()
            if schema_candidate:
                schema = schema_candidate

        with engine.connect() as connection:
            q = get_quote_char(engine)
            if schema:
                qualified_table = f"{q}{schema}{q}.{q}{table_name}{q}"
            else:
                qualified_table = f"{q}{table_name}{q}"

            query = text(f'SELECT * FROM {qualified_table} LIMIT 1000')
            result = connection.execute(query)

            rows = []
            for row in result.fetchall():
                row_dict = {key: (str(value) if isinstance(value, (datetime, date)) else float(value) if isinstance(value, Decimal) else value) for key, value in row._mapping.items()}
                rows.append(row_dict)

            stats_query = text(f'SELECT COUNT(*) as row_count FROM {qualified_table}')
            stats_result = connection.execute(stats_query)
            row_count = stats_result.fetchone()[0]

            inspector = inspect(engine)
            columns = inspector.get_columns(table_name, schema=schema)
            
            stats = {
                'total_rows': row_count,
                'total_columns': len(columns),
                'table_size': f"{row_count} rows",
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
            return JsonResponse({'success': True, 'data': rows, 'stats': stats, 'columns': [col['name'] for col in columns]})
    except Exception as e:
        traceback.print_exc()
        logger.error(f"Error in get_table_data: {str(e)}")
        return JsonResponse({'success': False, 'data': [], 'error': f'Server-side error: {str(e)}'}, status=500)

@login_required
@require_POST
def preview_data(request):
    """Preview data from uploaded file."""
    try:
        data = json.loads(request.body)
        temp_filename = data.get('temp_filename')
        table_name = data.get('table_name')
        connection_id = data.get('connection_id')
        sheet_name = data.get('sheet_name')
        
        if not all([temp_filename, table_name, connection_id]):
            return JsonResponse({'success': False, 'message': 'Missing required data.'}, status=400)
        
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'External database not connected.'})
        
        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        temp_file_path = os.path.join(upload_dir, temp_filename)
        
        if not os.path.exists(temp_file_path):
            return JsonResponse({'success': False, 'message': 'Temporary file not found. Please try again.'})
        
        if temp_filename.lower().endswith('.csv'):
            df = pd.read_csv(temp_file_path, low_memory=False)
        else:
            normalized_sheet = 0 if sheet_name in (None, '', 'null', 'undefined') else sheet_name
            df = pd.read_excel(temp_file_path, sheet_name=normalized_sheet)
        
        inspector = inspect(engine)
        db_columns = [col['name'] for col in inspector.get_columns(table_name)]
        file_columns = list(df.columns)
        normalized_file_cols = {normalize_header(c): c for c in file_columns}
        
        initial_mapping = [{'db_column': db_col, 'file_column': normalized_file_cols.get(normalize_header(db_col), '')} for db_col in db_columns]
        
        preview_json = df.head(5).to_json(orient='split')
        
        return JsonResponse({
            'success': True,
            'db_columns': db_columns,
            'file_columns': file_columns,
            'initial_mapping': initial_mapping,
            'preview_data': preview_json,
            'temp_filename': temp_filename
        })
    except Exception as e:
        traceback.print_exc()
        return JsonResponse({'success': False, 'message': f'An error occurred: {str(e)}'})

@login_required
@table_permission_required('table', permission_level='upload')
def confirm_upload(request):
    """
    Confirm and execute data upload to an existing table with data cleaning and PK generation.
    Hardened against dirty columns, malformed types, and partial failures.
    """
    data = {}  # Ensure availability in final except block
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        temp_filename = data.get('temp_filename')
        user_mapping = data.get('mapping', [])
        upload_method = data.get('upload_method', 'append')

        if not all([connection_id, table_name, temp_filename, user_mapping]):
            return JsonResponse({'success': False, 'message': 'Missing required data.'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'External database not connected.'}, status=500)

        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        temp_filepath = os.path.join(upload_dir, temp_filename)

        if not os.path.exists(temp_filepath):
            return JsonResponse({'success': False, 'message': 'Temporary file not found.'}, status=404)

        # Load the source file (CSV with robust encoding fallback, or Excel with an optional sheet)
        file_ext = os.path.splitext(temp_filename)[1].lower()
        if file_ext == '.csv':
            df = _read_csv_with_fallback(temp_filepath, low_memory=False)
        else:
            sheet_name = data.get('sheet_name')
            normalized_sheet = 0 if sheet_name in (None, '', 'null', 'undefined') else sheet_name
            df = pd.read_excel(temp_filepath, sheet_name=normalized_sheet)

        # Build rename map and validate
        rename_map = {
            item['file_column']: item['db_column']
            for item in user_mapping
            if item.get('file_column') and item.get('db_column')
        }
        if not rename_map:
            os.remove(temp_filepath)
            return JsonResponse({'success': False, 'message': 'No columns were mapped. Upload cancelled.'}, status=400)

        # Inject missing columns as NaN
        for col in rename_map.keys():
            if col not in df.columns:
                df[col] = None

        df_to_upload = df[list(rename_map.keys())].copy()
        df_to_upload.rename(columns=rename_map, inplace=True)

        # Inspect DB schema
        inspector = inspect(engine)
        db_columns_info = inspector.get_columns(table_name)
        db_col_names = [col['name'] for col in db_columns_info]

        numeric_types = {
            'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
            'FLOAT', 'REAL', 'DECIMAL', 'NUMERIC',
            'DOUBLE', 'DOUBLE PRECISION', 'FLOAT8', 'FLOAT4', 'MONEY'  # add these
        }
        date_types = {
            'DATE', 'DATETIME',
            'TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE', 'TIMESTAMP WITH TIME ZONE'  # add these
        }

        for col_info in db_columns_info:
            col_name = col_info['name']
            if col_name not in df_to_upload.columns:
                continue
            try:
                col_type = str(col_info['type']).upper().split('(')[0]
                if col_type in numeric_types:
                    df_to_upload[col_name] = df_to_upload[col_name].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                    df_to_upload[col_name] = pd.to_numeric(df_to_upload[col_name], errors='coerce')
                elif col_type in date_types:
                    df_to_upload[col_name] = pd.to_datetime(df_to_upload[col_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    df_to_upload[col_name] = df_to_upload[col_name].astype(str).str.strip()
            except Exception as clean_error:
                logger.warning(f"[{request.user.username}] Failed to clean column '{col_name}': {clean_error}")

        # --- PK Generation Logic ---
        if 'sl_no_pk' in db_col_names and 'sl_no_pk' not in df_to_upload.columns:
            start_index = 0
            if upload_method == 'append':
                try:
                    with engine.connect() as connection:
                        q = get_quote_char(engine)
                        result = connection.execute(text(f"SELECT MAX(sl_no_pk) FROM {q}{table_name}{q}"))
                        max_val = result.scalar_one_or_none()
                        if max_val is not None:
                            start_index = int(max_val)
                except Exception as pk_error:
                    logger.warning(f"[{request.user.username}] Failed to fetch max PK: {pk_error}")

            df_to_upload.insert(0, 'sl_no_pk', range(start_index + 1, start_index + 1 + len(df_to_upload)))
        # --- End PK Generation ---

        # Upload within transaction
        with engine.begin() as connection:
            df_to_upload.to_sql(table_name, connection, if_exists=upload_method, index=False)

        os.remove(temp_filepath)
        return JsonResponse({
            'success': True,
            'message': f"Successfully uploaded {len(df_to_upload)} rows to '{table_name}'."
        })

    except Exception as e:
        # Cleanup temp file
        try:
            temp_filepath = os.path.join(settings.MEDIA_ROOT, 'uploads', data.get('temp_filename', ''))
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
        except Exception as cleanup_error:
            logger.warning(f"Failed to clean up temp file: {cleanup_error}")

        logger.error(f"[{request.user.username}] Error during confirm_upload to '{data.get('table_name', '?')}': {str(e)}", exc_info=True)
        return JsonResponse({'success': False, 'message': f"An error occurred during final upload: {str(e)}"}, status=500)

@login_required
@require_POST
def create_table(request):
    """Create a new table manually."""
    try:
        
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        
        if not connection_id:
            return JsonResponse({'success': False, 'message': 'Connection ID is required.'}, status=400)
        
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'External database not connected.'}, status=400)
        
        table_name = data.get('table_name')
        columns = data.get('columns')
        
        if not table_name or not columns:
            return JsonResponse({'success': False, 'message': 'Table name and columns are required.'})
        
        if not table_name.isidentifier():
            return JsonResponse({'success': False, 'message': f'Invalid table name: {table_name}'})
        
        sql_parts, primary_keys = [], []
        for col in columns:
            col_name = col.get('name')
            if not col_name or not col_name.isidentifier():
                return JsonResponse({'success': False, 'message': f'Invalid column name: {col_name}'})
            sql_parts.append(f'"{col_name}" {col.get("type")}')
            if col.get('pk'):
                primary_keys.append(f'"{col_name}"')
        
        if primary_keys:
            sql_parts.append(f"PRIMARY KEY ({', '.join(primary_keys)})")
        
        sql_query = text(f"CREATE TABLE \"{table_name}\" ({', '.join(sql_parts)})")
        with engine.begin() as connection:
            connection.execute(sql_query)
        
        return JsonResponse({'success': True, 'message': f'Table {table_name} created successfully.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'Database error: {e}'})

@login_required
@table_permission_required('table', permission_level='edit')
def add_column(request):
    """Add a new column to an existing table."""
    try:

        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        
        if not connection_id:
            return JsonResponse({'success': False, 'message': 'Connection ID is required.'}, status=400)
        
        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'External database not connected.'}, status=400)
        
        table_name = data.get('table_name')
        column_name = data.get('column_name')
        column_type = data.get('column_type')
        allow_null = data.get('allow_null')
        if allow_null is None:
            allow_null = True  # default: allow nulls
        allow_null = bool(allow_null)
        
        # Add debug logging
        logger.info(f"Add column request: {data}")
        
        if not all([table_name, column_name, column_type]):
            return JsonResponse({'success': False, 'message': 'Missing required data.'}, status=400)
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            return JsonResponse({'success': False, 'message': 'Invalid column name.'}, status=400)
        
        # Use the appropriate quote character for the database
        q = get_quote_char(engine)
        null_clause = '' if allow_null else ' NOT NULL'

        # SQLite limitation: Adding NOT NULL requires a DEFAULT value; surface a clear error
        if engine.dialect.name == 'sqlite' and not allow_null:
            return JsonResponse({'success': False, 'message': 'SQLite requires a DEFAULT when adding a NOT NULL column. Please allow NULL or add the column with a default via SQL.'}, status=400)

        sql_query = text(f'ALTER TABLE {q}{table_name}{q} ADD COLUMN {q}{column_name}{q} {column_type}{null_clause}')
        
        with engine.begin() as connection:
            connection.execute(sql_query)
        
        return JsonResponse({'success': True, 'message': f'Column {column_name} added to table {table_name}.'})
    except Exception as e:
        logger.error(f"Error adding column: {str(e)}")
        return JsonResponse({'success': False, 'message': f'Database error: {str(e)}'})

@login_required
@table_permission_required('table', permission_level='edit')
def drop_column(request):
    """Drop a column from a table."""
    try:
                
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database not connected.'}, status=400)
        
        table_name = data.get('table_name')
        column_name = data.get('column_name')
        
        with engine.begin() as connection:
            connection.execute(text(f'ALTER TABLE "{table_name}" DROP COLUMN "{column_name}"'))
        
        return JsonResponse({'success': True, 'message': f'Column {column_name} deleted successfully.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='upload')
def modify_column_type(request):
    import json, re, traceback
    from sqlalchemy import text

    if request.method != 'POST':
        return JsonResponse({'success': False, 'message': 'Invalid method'}, status=405)

    try:
        data = json.loads(request.body or '{}')
    except Exception:
        return JsonResponse({'success': False, 'message': 'Invalid JSON body'}, status=400)

    connection_id = data.get('connection_id')
    table_name    = data.get('table') or data.get('table_name')
    column_name   = data.get('column') or data.get('column_name')
    new_type_raw  = (data.get('new_type') or '').strip()
    new_type      = new_type_raw.upper()
    dry_run       = data.get('dry_run') is True

    if not (connection_id and table_name and column_name and new_type):
        return JsonResponse({'success': False, 'message': 'Missing parameters'}, status=400)

    if not re.match(r'^[A-Z0-9_ (),.]+$', new_type):
        return JsonResponse({'success': False, 'message': f'Invalid type: {new_type_raw}'}, status=400)

    try:
        engine  = get_external_engine(connection_id, request.user)
        dialect = engine.dialect.name
        quote   = engine.dialect.identifier_preparer.quote

        with engine.begin() as conn:
            q_table  = quote(table_name)
            q_column = quote(column_name)

            if dialect == 'postgresql':
                result = handle_postgres_type_change(conn, q_table, q_column, new_type, dry_run)
            elif dialect == 'mysql':
                result = handle_mysql_type_change(conn, q_table, q_column, new_type, dry_run)
            elif dialect == 'sqlite':
                result = handle_sqlite_column_type_change(conn, table_name, column_name, new_type)
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported engine: {dialect}'}, status=400)

        return JsonResponse({'success': True, 'message': result})

    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e), 'trace': traceback.format_exc()}, status=500)
    

def handle_postgres_type_change(conn, q_table, q_column, new_type, dry_run=False):
    numeric_regex = r"^-?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?$"
    using_expr = None

    # Build USING expression
    if new_type.startswith(('DECIMAL', 'NUMERIC')) or new_type in {
        'INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT', 'REAL', 'DOUBLE PRECISION'
    }:
        using_expr = f"""
        CASE
            WHEN trim({q_column}::text) ~ '{numeric_regex}'
            THEN trim({q_column}::text):: {new_type}
            ELSE NULL
        END
        """
    elif new_type.startswith('DATE'):
        formats = ['YYYY-MM-DD', 'MM/DD/YYYY', 'MM-DD-YYYY', 'DD/MM/YYYY']
        parts = [f"to_date({q_column}::text,'{fmt}')" for fmt in formats]
        using_expr = f"COALESCE({', '.join(parts)})::{new_type}"
    elif new_type.startswith('TIMESTAMP'):
        formats = ['YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD', 'MM/DD/YYYY', 'MM-DD-YYYY', 'DD/MM/YYYY']
        parts = [f"to_timestamp({q_column}::text,'{fmt}')" for fmt in formats]
        using_expr = f"COALESCE({', '.join(parts)})::{new_type}"
    else:
        using_expr = f"{q_column}::{new_type}"

    if dry_run:
        return f"DRY RUN: ALTER TABLE {q_table} ALTER COLUMN {q_column} TYPE {new_type} USING {using_expr}"

    conn.execute(text(f'ALTER TABLE {q_table} ALTER COLUMN {q_column} DROP DEFAULT'))
    conn.execute(text(f'ALTER TABLE {q_table} ALTER COLUMN {q_column} TYPE {new_type} USING {using_expr}'))
    return f"PostgreSQL column {q_column} changed to {new_type}."

def handle_mysql_type_change(conn, q_table, q_column, new_type, dry_run=False):
    if dry_run:
        return f"DRY RUN: ALTER TABLE {q_table} MODIFY COLUMN {q_column} {new_type}"

    conn.execute(text(f"UPDATE {q_table} SET {q_column} = NULL WHERE TRIM({q_column}) = ''"))
    conn.execute(text(f'ALTER TABLE {q_table} MODIFY COLUMN {q_column} {new_type}'))
    return f"MySQL column {q_column} changed to {new_type}."

def handle_sqlite_column_type_change(conn, table_name, column_name, new_type):
    # Your existing SQLite rebuild logic goes here
    # Return a message like:
    return f"SQLite column {column_name} rebuilt with type {new_type}."




def handle_sqlite_column_type_change(connection, table_name, column_name, new_type):
    """Handle SQLite column type changes by creating a new table and copying data."""
    try:
        inspector = inspect(connection.engine)
        columns = inspector.get_columns(table_name)
        pks = inspector.get_pk_constraint(table_name)['constrained_columns']
        new_table_name = f"{table_name}_new"
        
        column_defs = []
        for col in columns:
            col_type = new_type if col['name'] == column_name else str(col['type'])
            col_def = f'"{col["name"]}" {col_type}'
            if col['name'] in pks:
                col_def += " PRIMARY KEY"
            column_defs.append(col_def)
        
        create_sql = text(f'CREATE TABLE "{new_table_name}" ({", ".join(column_defs)})')
        connection.execute(create_sql)
        
        column_names = [f'"{col["name"]}"' for col in columns]
        insert_sql = text(f'INSERT INTO "{new_table_name}" ({", ".join(column_names)}) SELECT {", ".join(column_names)} FROM "{table_name}"')
        connection.execute(insert_sql)
        
        connection.execute(text(f'DROP TABLE "{table_name}"'))
        connection.execute(text(f'ALTER TABLE "{new_table_name}" RENAME TO "{table_name}"'))
        
        return JsonResponse({'success': True, 'message': f'Data type for {column_name} changed to {new_type}.'})
    except Exception as e:
        try:
            connection.execute(text(f'DROP TABLE IF EXISTS "{new_table_name}"'))
        except:
            pass
        raise e

@login_required
@table_permission_required('table', permission_level='edit')
def set_primary_key(request):
    """Set primary key for a table."""
    try:
                
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database not connected.'}, status=400)
        
        table_name = data.get('table_name')
        columns = data.get('columns')
        
        with engine.begin() as connection:
            dialect = engine.dialect.name
            q = engine.dialect.identifier_preparer.quote
            qtbl = q(table_name)
            if dialect == 'postgresql':
                try:
                    connection.execute(text(f'ALTER TABLE {qtbl} DROP CONSTRAINT IF EXISTS {q(table_name + "_pkey")}'))
                except Exception:
                    pass
                if columns:
                    cols = ', '.join(q(c) for c in columns)
                    connection.execute(text(f'ALTER TABLE {qtbl} ADD PRIMARY KEY ({cols})'))
            elif dialect == 'mysql':
                # Drop existing PK
                try:
                    connection.execute(text(f'ALTER TABLE {qtbl} DROP PRIMARY KEY'))
                except Exception:
                    pass
                if columns:
                    cols = ', '.join(q(c) for c in columns)
                    connection.execute(text(f'ALTER TABLE {qtbl} ADD PRIMARY KEY ({cols})'))
            elif dialect == 'sqlite':
                # Not supported without table rebuild
                return JsonResponse({'success': False, 'message': 'SQLite primary key changes are not supported via UI.'}, status=400)
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported engine: {dialect}'}, status=400)
        
        return JsonResponse({'success': True, 'message': 'Primary key updated successfully.'})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def set_nullable(request):
    """Set or drop NOT NULL constraint for a column."""
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        column_name = data.get('column_name')
        allow_null = data.get('allow_null')
        if allow_null is None:
            allow_null = True
        allow_null = bool(allow_null)

        if not (connection_id and table_name and column_name):
            return JsonResponse({'success': False, 'message': 'Missing parameters'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database connection failed.'}, status=500)

        dialect = engine.dialect.name
        q = engine.dialect.identifier_preparer.quote
        qtbl = q(table_name)
        qcol = q(column_name)

        # SQLite requires table rebuild; not supported here
        if dialect == 'sqlite':
            return JsonResponse({'success': False, 'message': 'SQLite nullability changes are not supported via UI.'}, status=400)

        with engine.begin() as conn:
            if not allow_null:
                # Validate no NULL values exist
                res = conn.execute(text(f'SELECT COUNT(*) FROM {qtbl} WHERE {qcol} IS NULL'))
                count = res.scalar() or 0
                if count:
                    return JsonResponse({'success': False, 'message': f"Cannot set NOT NULL: {count} rows have NULL in '{column_name}'."}, status=400)

            if dialect == 'postgresql':
                if allow_null:
                    conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} DROP NOT NULL'))
                else:
                    conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} SET NOT NULL'))
            elif dialect == 'mysql':
                # Need current type for MODIFY
                inspector = inspect(engine)
                cols = inspector.get_columns(table_name)
                col_info = next((c for c in cols if c['name'] == column_name), None)
                if not col_info:
                    return JsonResponse({'success': False, 'message': f"Column '{column_name}' not found."}, status=404)
                col_type = str(col_info['type'])
                null_kw = 'NULL' if allow_null else 'NOT NULL'
                conn.execute(text(f'ALTER TABLE {qtbl} MODIFY COLUMN {qcol} {col_type} {null_kw}'))
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported engine: {dialect}'}, status=400)

        return JsonResponse({'success': True, 'message': f"Column '{column_name}' nullability updated."})
    except Exception as e:
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
@table_permission_required('table', permission_level='edit')
def set_auto_increment(request):
    """Toggle auto-increment for a column when supported by the engine."""
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name') or data.get('table')
        column_name = data.get('column_name') or data.get('column')
        enable = bool(data.get('enable'))

        if not (connection_id and table_name and column_name):
            return JsonResponse({'success': False, 'message': 'Missing parameters'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'Database not connected.'}, status=400)

        dialect = engine.dialect.name
        quote = engine.dialect.identifier_preparer.quote

        with engine.begin() as conn:
            qtbl = quote(table_name)
            qcol = quote(column_name)

            if dialect == 'postgresql':
                # Detect current identity/default status to avoid invalid operations
                # Try to honor explicit schema when available
                try:
                    ext_conn = ExternalConnection.objects.filter(id=connection_id).only('schema', 'db_type').first()
                    table_schema = (ext_conn.schema or None) if ext_conn and ext_conn.db_type == 'postgresql' else None
                except Exception:
                    table_schema = None

                params = {'t': table_name, 'c': column_name}
                schema_filter = ''
                if table_schema:
                    schema_filter = ' AND table_schema = :s'
                    params['s'] = table_schema

                info_sql = (
                    'SELECT is_identity, identity_generation, column_default '
                    'FROM information_schema.columns '
                    'WHERE table_name = :t AND column_name = :c' + schema_filter + ' '
                    'ORDER BY ordinal_position LIMIT 1'
                )
                is_identity = None
                col_default = None
                try:
                    row = conn.execute(text(info_sql), params).fetchone()
                    if row:
                        is_identity = (row[0] or '').upper() == 'YES'
                        col_default = row[2]
                except Exception:
                    # If introspection fails, proceed cautiously
                    is_identity = None
                    col_default = None

                if enable:
                    # If already identity, treat as success
                    if is_identity is True:
                        pass  # already enabled
                    else:
                        # Prefer identity if supported; fall back to sequence default only when not identity
                        try:
                            with conn.begin_nested():
                                conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} ADD GENERATED BY DEFAULT AS IDENTITY'))
                                is_identity = True
                        except Exception:
                            # Fallback only if column is not identity
                            if is_identity:
                                # Should not happen, but be safe
                                pass
                            else:
                                seq = f"{table_name}_{column_name}_seq"
                                qseq = quote(seq)
                                conn.execute(text(f'CREATE SEQUENCE IF NOT EXISTS {qseq}'))
                                conn.execute(text(f"ALTER TABLE {qtbl} ALTER COLUMN {qcol} SET DEFAULT nextval('{seq}')"))
                                conn.execute(text(f'ALTER SEQUENCE {qseq} OWNED BY {qtbl}.{qcol}'))
                else:
                    # Disable: drop identity if present; otherwise drop sequence default if present
                    try:
                        if is_identity:
                            conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} DROP IDENTITY IF EXISTS'))
                        # If default is a nextval(), drop default
                        if col_default and isinstance(col_default, str) and 'nextval(' in col_default:
                            conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} DROP DEFAULT'))
                    except Exception:
                        # Last resort: attempt to drop default regardless
                        try:
                            conn.execute(text(f'ALTER TABLE {qtbl} ALTER COLUMN {qcol} DROP DEFAULT'))
                        except Exception:
                            pass
            elif dialect == 'mysql':
                # Need current type and nullability
                insp = inspect(engine)
                cols = insp.get_columns(table_name)
                info = next((c for c in cols if c['name'] == column_name), None)
                if not info:
                    return JsonResponse({'success': False, 'message': 'Column not found'}, status=404)
                coltype = str(info['type'])
                notnull = 'NOT NULL'  # AUTO_INCREMENT requires NOT NULL
                ai = 'AUTO_INCREMENT' if enable else ''
                # MySQL quote uses backticks, but quote() will handle
                conn.execute(text(f'ALTER TABLE {qtbl} MODIFY COLUMN {qcol} {coltype} {notnull} {ai}'.strip()))
            elif dialect == 'sqlite':
                return JsonResponse({'success': False, 'message': 'Auto-increment toggle is not supported for SQLite via UI.'}, status=400)
            else:
                return JsonResponse({'success': False, 'message': f'Unsupported engine: {dialect}'}, status=400)

        return JsonResponse({'success': True, 'message': f'Auto-increment {"enabled" if enable else "disabled"} for {column_name}.'})
    except Exception as e:
        logger.error('set_auto_increment failed: %s', e, exc_info=True)
        return JsonResponse({'success': False, 'message': str(e)}, status=500)

@login_required
def get_visible_tables_for_connection(request, connection_id):
    """API endpoint to get a list of visible tables for a specific connection."""
    try:
        visible_tables = get_visible_tables(connection_id, request.user) 
        return JsonResponse(visible_tables, safe=False)
    except Exception as e:
        logger.error(f"Error getting visible tables: {str(e)}")
        traceback.print_exc()
        return JsonResponse({'error': str(e)}, status=500)
    
@login_required
def get_table_schema_api(request, table_name):
    """Get table schema information."""
    try:
        connection_id = request.GET.get('connection_id')
        if not connection_id:
            return JsonResponse({'error': 'Connection ID is required'}, status=400)
        
        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'error': 'Database connection failed'}, status=500)
        
        inspector = inspect(engine)
        columns = inspector.get_columns(table_name)
        
        return JsonResponse({'success': True, 'columns': [{'name': col['name'], 'type': str(col['type'])} for col in columns]})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)

@login_required
@require_POST
def get_detailed_columns(request, connection_id):
    """Get detailed column information for multiple tables."""
    try:
        engine = get_external_engine(connection_id, request.user) # FIXED
        if not engine:
            return JsonResponse({'columns': [], 'error': 'External database not connected.'})
        
        data = json.loads(request.body)
        tables_to_query = data.get('tables', [])
        inspector = inspect(engine)
        all_columns = []
        
        for table_name in tables_to_query:
            try:
                columns = inspector.get_columns(table_name)
                for col in columns:
                    all_columns.append({
                        'full_name': f"{table_name}.{col['name']}",
                        'source': table_name,
                        'name': col['name'],
                        'is_numeric': is_numeric_type(col['type'])
                    })
            except Exception as e:
                print(f"Could not get columns for table {table_name}: {e}")
        
        return JsonResponse({'columns': all_columns})
    except Exception as e:
        return JsonResponse({'columns': [], 'error': str(e)})

# Main view
@login_required
def data_management_view(request):
    """Main data management page."""
    try:
        from .services.permissions import get_accessible_connections  # add import at top if not present

        if request.user.user_type in ['Admin', 'Moderator']:
            connections = ExternalConnection.objects.all().order_by('nickname')
        else:
            # include owned connections + group-permitted connections
            connections = get_accessible_connections(request.user)
        
        # Work out the default connection for this user in the DM UI
        user_default_id = getattr(request.user, 'default_database_id', None)
        default_id = None
        if user_default_id and any(str(c.id) == str(user_default_id) for c in connections):
            default_id = user_default_id
        else:
            # Fallback to a connection explicitly marked as default, else first available
            try:
                default_conn = next((c for c in connections if getattr(c, 'is_default', False)), None)
            except Exception:
                default_conn = None
            if default_conn is not None:
                default_id = default_conn.id
            elif connections.exists():
                default_id = connections.first().id

        context = {
            'connections': connections,
            'user_can_upload': request.user.can_upload_data(),
            'user_can_modify': request.user.can_modify_schema(),
            'user_can_delete_rows': request.user.can_delete_rows(),
            'user_can_truncate': request.user.can_truncate_tables(),
            'user_can_drop': request.user.can_drop_tables(),
            # Used by the frontend app to preselect the connection in the dropdown
            'default_connection_id': default_id,
        }
        
        return render(request, 'data_management.html', context)
    
    except Exception as e:
        logger.error(f"Error in data_management_view: {str(e)}")
        return render(request, 'data_management.html', {'connections': [], 'error': str(e)})
    
def get_best_pandas_dtype(series):
    """Analyzes a pandas Series and suggests the best data type."""
    if series.empty:
        return 'VARCHAR(255)'
    
    # Convert to string and clean
    series_str = series.astype(str).str.strip()
    
    # Check for numeric values
    cleaned_series_for_numeric = series_str.str.replace(r'[$,%]', '', regex=True).str.replace(r'^\((.*)\)$', r'-\1', regex=True)
    numeric_vals = pd.to_numeric(cleaned_series_for_numeric, errors='coerce')
    
    # If more than 80% are numeric
    if numeric_vals.notna().sum() / len(series) > 0.8:
        if (numeric_vals.dropna() % 1 == 0).all():
            return 'INTEGER'
        else:
            return 'FLOAT'
    
    # Check for dates with different formats
    try:
        # Try multiple date formats
        date_formats = ['%m/%d/%Y', '%d/%m/%Y', '%Y-%m-%d', '%m-%d-%Y']
        for fmt in date_formats:
            try:
                date_vals = pd.to_datetime(series_str, format=fmt, errors='raise')
                if date_vals.notna().sum() / len(series) > 0.7:
                    return 'DATE'
            except:
                continue
    except Exception:
        pass
    
    # Check for booleans
    bool_keywords = {'true', 'false', 'yes', 'no', 't', 'f', '1', '0', 'y', 'n'}
    if series_str.str.lower().isin(bool_keywords).sum() / len(series) > 0.8:
        return 'BOOLEAN'
    
    # Default to text
    max_length = series_str.str.len().max()
    if max_length > 255:
        return 'TEXT'
    else:
        return f'VARCHAR({int(max_length * 1.2)})'  # Add some buffer

@login_required
@require_POST
def analyze_upload(request):
    """
    Analyzes uploaded file to suggest table schema using the robust infer_sql_type helper.
    Optimized for speed and resilience.
    """
    try:

        data = json.loads(request.body)
        temp_filename = data.get('temp_filename')
        sheet_name = data.get('sheet_name')

        if not temp_filename:
            return JsonResponse({'error': "Missing temporary file name."}, status=400)

        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        temp_filepath = os.path.join(upload_dir, temp_filename)

        if not os.path.exists(temp_filepath):
            return JsonResponse({'error': "Temporary file not found."}, status=404)

        # --- Robust File Reading ---
        try:
            if temp_filename.lower().endswith('.csv'):
                try:
                    df = pd.read_csv(temp_filepath, low_memory=False, on_bad_lines='skip')
                except UnicodeDecodeError:
                    logger.warning(f"[{request.user.username}] UTF-8 decoding failed. Retrying with 'latin-1'.")
                    df = pd.read_csv(temp_filepath, low_memory=False, on_bad_lines='skip', encoding='latin-1')
            else:
                excel_file = pd.ExcelFile(temp_filepath, engine='openpyxl')
                if sheet_name and sheet_name not in excel_file.sheet_names:
                    return JsonResponse({'error': f"Sheet '{sheet_name}' not found."}, status=400)
                df = excel_file.parse(sheet_name or excel_file.sheet_names[0])
        except Exception as read_error:
            logger.error(f"[{request.user.username}] Failed to read file '{temp_filename}': {read_error}", exc_info=True)
            return JsonResponse({'error': f"Failed to read file: {str(read_error)}"}, status=500)

        if df.empty or len(df.columns) == 0:
            return JsonResponse({'error': "Uploaded file is empty or has no usable columns."}, status=400)

        # --- Deduplicate and Sanitize Column Names ---
        original_columns = list(df.columns)
        new_columns, counts = [], {}
        for col in original_columns:
            clean = sanitize_column_name(str(col))
            if clean in counts:
                counts[clean] += 1
                new_columns.append(f"{clean}_{counts[clean]}")
            else:
                counts[clean] = 0
                new_columns.append(clean)
        df.columns = new_columns
        column_name_map = dict(zip(new_columns, original_columns))

        df.dropna(how='all', inplace=True)

        # --- Schema Inference ---
        schema = []
        for col in df.columns:
            original_name = column_name_map.get(col, col)
            full_column_data = df[col].dropna()

            try:
                dtype = infer_sql_type(full_column_data)
            except Exception as dtype_error:
                logger.warning(f"[{request.user.username}] Failed to infer type for '{col}': {dtype_error}")
                dtype = 'VARCHAR(255)'

            is_pk_candidate = (
                col == 'id' or col.endswith('_id') or
                (not full_column_data.empty and full_column_data.nunique() == len(full_column_data))
            )

            schema.append({
                'original_name': original_name,
                'sql_name': col,
                'sql_type': str(dtype),
                'is_pk': is_pk_candidate
            })

        # --- Table Name Suggestion ---
        try:
            table_name_suggestion = sanitize_column_name(os.path.splitext(temp_filename.split('_', 1)[1])[0])
        except Exception:
            table_name_suggestion = sanitize_column_name(os.path.splitext(temp_filename)[0])

        # --- Preview Data ---
        if all(k == v for k, v in column_name_map.items()):
            preview_data = df.head(15).replace({np.nan: None}).to_dict(orient='records')
        else:
            preview_data_raw = df.head(15).replace({np.nan: None}).to_dict(orient='records')
            preview_data = [
                {column_name_map.get(k, k): v for k, v in row.items()}
                for row in preview_data_raw
            ]

        return JsonResponse({
            'success': True,
            'table_name': table_name_suggestion,
            'schema': schema,
            'preview_data': preview_data,
            'temp_filename': temp_filename
        })

    except Exception as e:
        logger.error(f"[{request.user.username}] Error in analyze_upload: {str(e)}", exc_info=True)
        return JsonResponse({'error': f"Analysis failed: {str(e)}"}, status=500)


@login_required
@require_POST
def create_table_from_import(request):
    """
    Creates a table from a user-confirmed schema and imports data from the temp file.
    Hardened against dirty column names, malformed types, encoding issues, and operational hazards.
    """
    if not request.user.can_upload_data():
        return JsonResponse({'error': "Permission Denied"}, status=403)

    data = {}
    try:
        data = json.loads(request.body)
        table_name = data.get('table_name')
        schema = data.get('schema')
        temp_filename = data.get('temp_filename')
        connection_id = data.get('connection_id')

        if not all([connection_id, table_name, schema, temp_filename]):
            return JsonResponse({'error': "Missing required data."}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'error': "External database not connected."}, status=400)

        q = get_quote_char(engine)
        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        temp_filepath = os.path.join(upload_dir, temp_filename)

        if not os.path.exists(temp_filepath):
            return JsonResponse({'error': "Temporary file not found. Please re-upload."}, status=404)

        try:
            inspector = inspect(engine)
            existing_tables_lower = [t.lower() for t in inspector.get_table_names()]
            if table_name.lower() in existing_tables_lower:
                return JsonResponse({'error': f"Table '{table_name}' already exists."}, status=409)
        except Exception as inspect_error:
            logger.warning(f"[{request.user.username}] Could not inspect tables: {inspect_error}")

        with engine.begin() as connection:
            if not any(col.get('is_pk') for col in schema):
                schema.insert(0, {'sql_name': 'sl_no_pk', 'sql_type': 'BIGINT', 'is_pk': True})

            for col in schema:
                if not col.get('sql_name') or not col.get('sql_type'):
                    return JsonResponse({'error': "Invalid schema definition."}, status=400)

            sql_parts = [f"{q}{col['sql_name']}{q} {col['sql_type']}" for col in schema]
            pk_clause = ", ".join(f"{q}{col['sql_name']}{q}" for col in schema if col.get('is_pk'))
            if pk_clause:
                sql_parts.append(f"PRIMARY KEY ({pk_clause})")

            create_sql = text(f"CREATE TABLE {q}{table_name}{q} ({', '.join(sql_parts)});")
            connection.execute(create_sql)

            try:
                if temp_filename.lower().endswith('.csv'):
                    try:
                        df = pd.read_csv(temp_filepath, low_memory=False, on_bad_lines='skip')
                    except UnicodeDecodeError:
                        logger.warning(f"[{request.user.username}] UTF-8 decoding failed. Retrying with 'latin-1'.")
                        df = pd.read_csv(temp_filepath, low_memory=False, on_bad_lines='skip', encoding='latin-1')
                else:
                    df = pd.read_excel(temp_filepath, engine='openpyxl')
            except Exception as read_error:
                logger.error(f"[{request.user.username}] Failed to read file '{temp_filename}': {read_error}", exc_info=True)
                raise

            original_columns = list(df.columns)
            new_columns, counts = [], {}
            for col in original_columns:
                clean = sanitize_column_name(str(col))
                if clean in counts:
                    counts[clean] += 1
                    new_columns.append(f"{clean}_{counts[clean]}")
                else:
                    counts[clean] = 0
                    new_columns.append(clean)
            df.columns = new_columns

            numeric_types = {'INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'FLOAT', 'DECIMAL', 'NUMERIC', 'REAL'}
            date_types = {'DATE', 'DATETIME', 'TIMESTAMP'}

            for col in schema:
                col_name = col['sql_name']
                if col_name not in df.columns:
                    continue
                try:
                    col_type_str = col['sql_type'].upper()
                    base_type = col_type_str.split('(')[0]

                    if base_type in numeric_types:
                        df[col_name] = df[col_name].astype(str).str.replace(r'[^\d.-]', '', regex=True)
                        df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
                    elif base_type in date_types:
                        df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                    elif base_type == 'VARCHAR':
                        # Use regex to find the max length from the schema (e.g., VARCHAR(150))
                        match = re.search(r'\((\d+)\)', col_type_str)
                        if match:
                            max_len = int(match.group(1))
                            # Truncate strings that are too long
                            df[col_name] = df[col_name].astype(str).str.slice(0, max_len)
                    else: # For TEXT or other types, just strip whitespace
                        df[col_name] = df[col_name].astype(str).str.strip()
                except Exception as clean_error:
                    logger.warning(f"[{request.user.username}] Failed to clean column '{col_name}': {clean_error}")
            # --- END OF NEW LOGIC ---

            expected_cols = [col['sql_name'] for col in schema]
            if 'sl_no_pk' in expected_cols and 'sl_no_pk' not in df.columns:
                df.insert(0, 'sl_no_pk', range(1, len(df) + 1))
            
            df = df.reindex(columns=expected_cols)
            df.to_sql(table_name, connection, if_exists='append', index=False)

        os.remove(temp_filepath)
        return JsonResponse({
            'success': True,
            'message': f"Successfully created table '{table_name}' and imported {len(df)} rows."
        })

    except (OperationalError, IntegrityError) as e:
        error_code = getattr(e.orig, 'args', [None])[0]
        error_msg = str(e).lower()
        logger.warning(f"[{request.user.username}] DB error during import: {error_msg}")
        if "already exists" in error_msg or error_code == 1050:
            return JsonResponse({'error': f"Table '{table_name}' already exists. Possibly a duplicate request."}, status=409)
        if "deadlock" in error_msg or error_code == 1213:
            return JsonResponse({'error': "Database deadlock detected. Please retry."}, status=409)
        if os.path.exists(temp_filepath): os.remove(temp_filepath)
        return JsonResponse({'error': f"A database error occurred: {str(e)}"}, status=500)

    except Exception as e:
        logger.error(f"[{request.user.username}] Unexpected error: {str(e)}", exc_info=True)
        if 'temp_filepath' in locals() and os.path.exists(temp_filepath): os.remove(temp_filepath)
        try:
            with engine.begin() as connection:
                connection.execute(text(f"DROP TABLE IF EXISTS {q}{table_name}{q}"))
        except Exception as drop_error:
            logger.error(f"Failed to drop table '{table_name}' during cleanup: {drop_error}")
        return JsonResponse({'error': f"An unexpected error occurred: {str(e)}"}, status=500)
    
@login_required
@require_POST
def preview_upload_for_matching(request):
    """
    Analyzes an uploaded file and compares its columns to a target table's columns
    to prepare for user-driven column mapping.
    """
    try:
        data = json.loads(request.body)
        temp_filename = data.get('temp_filename')
        table_name = data.get('table_name')
        connection_id = data.get('connection_id')
        sheet_name = data.get('sheet_name')

        if not all([temp_filename, table_name, connection_id]):
            return JsonResponse({'success': False, 'message': 'Missing required data.'}, status=400)

        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'message': 'External database not connected.'})

        upload_dir = os.path.join(settings.MEDIA_ROOT, 'uploads')
        temp_filepath = os.path.join(upload_dir, temp_filename)
        if not os.path.exists(temp_filepath):
            return JsonResponse({'success': False, 'message': 'Temporary file not found.'})

        if temp_filename.lower().endswith('.csv'):
            df = _read_csv_with_fallback(temp_filepath, low_memory=False)
        else:
            normalized_sheet = 0 if sheet_name in (None, '', 'null', 'undefined') else sheet_name
            df = pd.read_excel(temp_filepath, sheet_name=normalized_sheet)

        inspector = inspect(engine)
        db_columns = [col['name'] for col in inspector.get_columns(table_name)]
        file_columns = list(df.columns)
        
        # Suggest an initial mapping by matching cleaned column names
        normalized_file_cols = {normalize_header(c): c for c in file_columns}
        initial_mapping = []
        for db_col in db_columns:
            normalized_db_col = normalize_header(db_col)
            best_match = normalized_file_cols.get(normalized_db_col, '')
            initial_mapping.append({"db_column": db_col, "file_column": best_match})

        preview_json = df.head(15).to_json(orient='split', index=False)

        return JsonResponse({
            'success': True,
            'db_columns': db_columns,
            'file_columns': file_columns,
            'initial_mapping': initial_mapping,
            'preview_data': preview_json,
            'temp_filename': temp_filename
        })
    except Exception as e:
        logger.error(f"Error in preview_upload_for_matching: {str(e)}")
        return JsonResponse({'success': False, 'message': f'An error occurred: {str(e)}'})
