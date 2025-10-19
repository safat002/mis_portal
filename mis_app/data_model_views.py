# mis_app/data_model_views.py
import json
import logging
# Add Q for filtering permitted connections
from django.db.models import Q 
from django.shortcuts import render, get_object_or_404
from django.http import JsonResponse
from django.contrib.auth.decorators import login_required
from django.views.decorators.http import require_POST, require_GET
from django.db import transaction
from sqlalchemy import inspect
from .models import ExternalConnection, ConnectionJoin, CanvasLayout
from .utils import get_external_engine
# Import PermissionManager and the decorators
from .permissions import connection_permission_required, PermissionManager 

logger = logging.getLogger(__name__)

@login_required
def data_model_designer_view(request):
    """Renders the main data model designer page with all accessible connections."""
    
    # FIX 2: Widen connection filter using list comprehension + PermissionManager
    all_conns = ExternalConnection.objects.all().order_by('nickname')
    connections = [c for c in all_conns if PermissionManager.user_can_access_connection(request.user, c)]
    
    context = {
        'connections': connections,
    }
    return render(request, 'data_model.html', context)

def get_permitted_connections_for_view(request):
    # 1. Start with owned connections
    owned_qs = ExternalConnection.objects.filter(Q(owner=request.user) | Q(is_internal=True))

    # 2. Add connections the user has any (view/upload/edit) grant to
    perms = PermissionManager.get_user_permissions(request.user)
    permitted_ids = set()

    if isinstance(perms, list):
        for p in perms:
            # Check for connection-level permissions that are not 'none'
            if p.get('resource_type') == 'connection' and p.get('resource_id') and p.get('permission_level') != 'none':
                permitted_ids.add(str(p['resource_id']))
    
    # 3. Handle legacy dict support (if needed, otherwise can remove this block)
    elif isinstance(perms, dict):
        for key, lvl in perms.items():
            if lvl != 'none' and key.startswith('connection:'):
                nickname = key.split(':', 1)[1]
                # Note: This is an expensive lookup; rely on the list format if possible.
                permitted_ids.update(map(str, ExternalConnection.objects.filter(nickname=nickname).values_list('id', flat=True)))
    
    group_qs = ExternalConnection.objects.filter(id__in=list(permitted_ids))
    
    # 4. Union owned + permitted and return the unique set
    all_connection_ids = set(owned_qs.values_list('id', flat=True)) | set(group_qs.values_list('id', flat=True))
    
    return ExternalConnection.objects.filter(id__in=list(all_connection_ids)).order_by('nickname')


@require_GET
@login_required
@connection_permission_required(permission_level='view')
def get_model_for_connection(request, connection_id):
    """
    Returns:
      {
        success: true,
        tables: [{ name, columns: [{name,type,is_primary,is_foreign}]}],
        joins: [{left_table,left_column,right_table,right_column,join_type,cardinality}],
        layout: [{table_name,x_pos,y_pos,collapsed}]
      }
    """
    conn = get_object_or_404(ExternalConnection, id=connection_id)

    # Get SQLAlchemy engine (supports either signature your project may use)
    try:
        engine = get_external_engine(conn)  # preferred
    except TypeError:
        engine = get_external_engine(connection_id, request.user)  # fallback

    try:
        insp = inspect(engine)
        schema = getattr(conn, "schema", None) or None

        tables = []
        for tname in insp.get_table_names(schema=schema):
            # PKs
            pk = insp.get_pk_constraint(tname, schema=schema) or {}
            pk_cols = set(pk.get("constrained_columns") or [])

            # FKs
            fk_cols = set()
            for fk in insp.get_foreign_keys(tname, schema=schema) or []:
                for c in fk.get("constrained_columns", []) or []:
                    fk_cols.add(c)

            # UNIQUEs (single-column only)
            unique_cols = set()
            for uc in insp.get_unique_constraints(tname, schema=schema) or []:
                col_names = uc.get("column_names") or []
                if len(col_names) == 1:
                    unique_cols.add(col_names[0])


            # Columns (with richer flags for frontend auto-cardinality & icons)
            cols = []
            for col in insp.get_columns(tname, schema=schema) or []:
                cname = col.get("name")
                ctype = str(col.get("type") or "")
                ctype_l = ctype.lower()
                is_numeric = any(tok in ctype_l for tok in ["int", "numeric", "decimal", "float", "double", "real"])

                cols.append({
                    "name": cname,
                    "type": ctype,
                    "is_primary": cname in pk_cols,
                    "is_pk": cname in pk_cols,            # alias expected by some UIs
                    "is_foreign": cname in fk_cols,
                    "is_unique": cname in unique_cols,    # single-column unique index
                    "is_numeric": is_numeric,
                })


            tables.append({"name": tname, "columns": cols})

    except Exception as e:
        return JsonResponse({"success": False, "error": f"Inspector error: {e}"}, status=500)

    # Saved joins & layout from your app DB
    joins = list(
        ConnectionJoin.objects
        .filter(connection=conn)
        .values("left_table", "left_column", "right_table", "right_column", "join_type", "cardinality")
    )

    layout = list(
        CanvasLayout.objects
        .filter(connection=conn)
        .values("table_name", "x_pos", "y_pos", "collapsed")
    )

    return JsonResponse({"success": True, "tables": tables, "joins": joins, "layout": layout})



@require_POST
@login_required
@connection_permission_required(permission_level='edit')
def save_model_for_connection(request, connection_id):
    """
    Body (from your frontend):
      {
        "layout": [{"tableName":"...","x":123,"y":456,"collapsed":false}, ...],
        "joins":  [{"leftTable":"A","leftColumn":"id","rightTable":"B","rightColumn":"a_id","joinType":"INNER","cardinality":"one-to-many"}, ...]
      }
    """
    conn = get_object_or_404(ExternalConnection, id=connection_id)

    # Parse JSON
    try:
        payload = json.loads(request.body.decode("utf-8"))
    except Exception:
        return JsonResponse({"success": False, "error": "Invalid JSON body."}, status=400)

    layout_in = payload.get("layout") or []
    joins_in  = payload.get("joins") or []

    # Normalize payload keys (accept camel/snake)
    def norm_layout(r):
        return {
            "table_name": r.get("tableName") or r.get("table_name"),
            "x_pos": int(r.get("x") or 0),
            "y_pos": int(r.get("y") or 0),
            "collapsed": bool(r.get("collapsed") or False),
        }

    def norm_join(j):
        return {
            "left_table":   j.get("leftTable") or j.get("left_table"),
            "left_column":  j.get("leftColumn") or j.get("left_column"),
            "right_table":  j.get("rightTable") or j.get("right_table"),
            "right_column": j.get("rightColumn") or j.get("right_column"),
            "join_type":   (j.get("joinType") or j.get("join_type") or "INNER").upper(),
            "cardinality":  j.get("cardinality") or "one-to-many",
        }

    norm_layouts = [x for x in (norm_layout(r) for r in layout_in) if x["table_name"]]
    norm_joins   = [x for x in (norm_join(j)  for j in joins_in)
                    if x["left_table"] and x["left_column"] and x["right_table"] and x["right_column"]]

    # Save atomically
    try:
        with transaction.atomic():
            # Replace all joins for this connection (UI sends the full set)
            ConnectionJoin.objects.filter(connection=conn).delete()
            if norm_joins:
                ConnectionJoin.objects.bulk_create([
                    ConnectionJoin(
                        connection=conn,
                        left_table=j["left_table"],
                        left_column=j["left_column"],
                        right_table=j["right_table"],
                        right_column=j["right_column"],
                        join_type=j["join_type"],
                        cardinality=j["cardinality"],
                        created_by=request.user,  # REQUIRED: model has NOT NULL
                    ) for j in norm_joins
                ], batch_size=500)

            # Replace all layout rows for this connection
            CanvasLayout.objects.filter(connection=conn).delete()
            if norm_layouts:
                CanvasLayout.objects.bulk_create([
                    CanvasLayout(
                        connection=conn,
                        table_name=r["table_name"],
                        x_pos=r["x_pos"],
                        y_pos=r["y_pos"],
                        collapsed=r["collapsed"],
                    ) for r in norm_layouts
                ], batch_size=500)

    except Exception as e:
        return JsonResponse({"success": False, "error": f"DB error: {e}"}, status=500)

    return JsonResponse({"success": True, "message": "Data model saved."})


def get_data_model_api(request, connection_id):  # not 'id', not 'conn_id'
    return get_model_for_connection(request, connection_id)

@require_POST
def save_data_model_api(request, connection_id):
    return save_model_for_connection(request, connection_id)


@login_required
def data_model_designer(request):
    """Duplicate renderer for the main data model designer page (keeping both for URL compatibility)."""
    
    # FIX 2: Widen connection filter using list comprehension + PermissionManager
    all_conns = ExternalConnection.objects.all().order_by('nickname')
    connections = [c for c in all_conns if PermissionManager.user_can_access_connection(request.user, c)]
    
    return render(request, 'data_model.html', {
        'connections': connections, 
        'full_width_page': True,
        'current_user': request.user
    })

@login_required
@connection_permission_required(permission_level='view')
def test_connection(request, connection_id):
    """API endpoint to test a single database connection."""
    try:
        engine = get_external_engine(connection_id, request.user)
        if engine:
            engine.dispose()
            return JsonResponse({'success': True, 'message': "Connection successful!"})
        else:
            return JsonResponse({'success': False, 'error': "Connection failed. Check credentials and network."})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)



@login_required
@require_GET
def suggest_joins(request, connection_id):
    """
    Suggests possible joins between tables in a given connection.
    Uses heuristics based on column names, primary keys, and foreign key patterns.
    """
    try:
        engine = get_external_engine(connection_id, request.user)
        if not engine:
            return JsonResponse({'success': False, 'error': "Database connection failed."}, status=500)

        inspector = inspect(engine)
        tables = inspector.get_table_names()
        suggested_joins = []

        # Cache column metadata for efficiency
        table_columns = {
            table: inspector.get_columns(table)
            for table in tables
        }

        # Build a map of primary keys
        table_primary_keys = {
            table: next((col['name'] for col in cols if col.get('primary_key')), None)
            for table, cols in table_columns.items()
        }

        # Heuristic join detection
        for left_table in tables:
            left_cols = table_columns[left_table]

            for right_table in tables:
                if left_table == right_table:
                    continue

                right_cols = table_columns[right_table]
                right_pk = table_primary_keys.get(right_table)

                if not right_pk:
                    continue

                # Heuristic 1: foreign key naming convention
                fk_candidates = [f"{right_table}_id", f"{right_pk}", f"{right_table}_{right_pk}"]

                for left_col in left_cols:
                    left_col_name = left_col['name']
                    if left_col_name in fk_candidates:
                        suggested_joins.append({
                            "left_table": left_table,
                            "left_column": left_col_name,
                            "right_table": right_table,
                            "right_column": right_pk,
                            "join_type": "INNER",
                            "cardinality": "many-to-one",
                            "confidence": "high"
                        })

                # Heuristic 2: exact column name match
                for left_col in left_cols:
                    for right_col in right_cols:
                        if left_col['name'] == right_col['name'] and left_col['name'] != right_pk:
                            suggested_joins.append({
                                "left_table": left_table,
                                "left_column": left_col['name'],
                                "right_table": right_table,
                                "right_column": right_col['name'],
                                "join_type": "INNER",
                                "cardinality": "ambiguous",
                                "confidence": "medium"
                            })

        # Deduplicate suggestions
        seen = set()
        final_joins = []
        for join in suggested_joins:
            key = (
                join['left_table'], join['left_column'],
                join['right_table'], join['right_column']
            )
            if key not in seen:
                seen.add(key)
                final_joins.append(join)

        return JsonResponse({'success': True, 'joins': final_joins})

    except Exception as e:
        logger.error(f"[{request.user.username}] Failed to suggest joins for connection {connection_id}: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': f"Join suggestion failed: {str(e)}"}, status=500)

@login_required
@require_POST
def validate_model(request, connection_id):
    """API endpoint to validate the data model for issues."""
    try:
        data = json.loads(request.body)
        # Implement validation logic here
        issues = []
        # Add validation logic as needed
        
        return JsonResponse({'success': True, 'issues': issues})
    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)}, status=500)