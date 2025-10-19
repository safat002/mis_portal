# mis_app/dashboard_join_helper.py
import logging
from django.db.models import Q
from .models import ExternalConnection, ConnectionJoin
from collections import deque
from typing import List, Dict
from .permissions import PermissionManager

logger = logging.getLogger(__name__)

def extract_tables_from_query_config(query_config):
    """
    Extract all tables used in a query configuration.
    """
    tables = set()
    
    # Extract from dimensions
    for dimension in query_config.get('dimensions', []):
        if '.' in dimension:
            table = dimension.split('.')[0]
            tables.add(table)
    
    # Extract from measures
    for measure in query_config.get('measures', []):
        if '.' in measure:
            table = measure.split('.')[0]
            tables.add(table)
    
    # Extract from filters
    filters = query_config.get('filters', [])
    for filter_item in filters:
        field = filter_item.get('field', '')
        if '.' in field:
            table = field.split('.')[0]
            tables.add(table)
    
    return list(tables)


def auto_apply_joins_for_query(connection_id, tables_used, existing_joins=None):
    """
    Automatically find and apply joins from the data model for dashboard queries.
    """
    if existing_joins is None:
        existing_joins = []
    
    # If we have multiple tables, look for joins in the data model
    if len(tables_used) >= 2:
        try:
            # Get all saved joins for this connection
            saved_joins = ConnectionJoin.objects.filter(
                connection_id=connection_id,
                left_table__in=tables_used,
                right_table__in=tables_used
            )
            
            # Convert to the format expected by the query engine
            auto_joins = []
            for join in saved_joins:
                join_config = {
                    'left_col': f"{join.left_table}.{join.left_column}",
                    'right_col': f"{join.right_table}.{join.right_column}",
                    'type': join.join_type or 'INNER'
                }
                
                # Only add if not already in existing joins
                if not any(
                    j.get('left_col') == join_config['left_col'] and 
                    j.get('right_col') == join_config['right_col']
                    for j in existing_joins
                ):
                    auto_joins.append(join_config)
            
            return existing_joins + auto_joins
            
        except Exception as e:
            logger.warning(f"Failed to auto-apply joins for connection {connection_id}: {e}")
    
    return existing_joins

def extract_tables_from_query_config(query_config):
    """
    Extract all tables used in a query configuration.
    """
    tables = set()
    
    # Extract from dimensions
    for dimension in query_config.get('dimensions', []):
        if '.' in dimension:
            table = dimension.split('.')[0]
            tables.add(table)
    
    # Extract from measures
    for measure in query_config.get('measures', []):
        if '.' in measure:
            table = measure.split('.')[0]
            tables.add(table)
    
    # Extract from filters (if present in your dashboard config)
    filters = query_config.get('filters', [])
    for filter_item in filters:
        field = filter_item.get('field', '')
        if '.' in field:
            table = field.split('.')[0]
            tables.add(table)
    
    return list(tables)

def infer_join_path(connection_id: str, tables: List[str], user) -> List[Dict]:
    """
    Returns a minimal set of joins that connect all 'tables' using the saved data-model joins.
    Works even when tables are not directly joined (multi-hop).
    Output format:
      [{'left_col': 't1.id', 'right_col': 't2.t1_id', 'type': 'INNER'}, ...]
    """
    if not tables or len(tables) < 2:
        return []

    conn = ExternalConnection.objects.get(id=connection_id)
    if not PermissionManager.user_can_access_connection(user, conn):
        return []

    # Build undirected graph with edge metadata (the ConnectionJoin object)
    edges_by_table = {}
    for j in ConnectionJoin.objects.filter(connection=conn):
        edges_by_table.setdefault(j.left_table, []).append(('R', j))   # left -> right
        edges_by_table.setdefault(j.right_table, []).append(('L', j))  # right -> left

    start = tables[0]
    # BFS predecessor map: node -> (prev_node, join_obj_used_to_reach_node)
    pred = {start: (None, None)}
    q = deque([start])

    while q:
        cur = q.popleft()
        for dir_flag, join_obj in edges_by_table.get(cur, []):
            if dir_flag == 'R':
                nxt = join_obj.right_table if join_obj.left_table == cur else None
            else:  # 'L'
                nxt = join_obj.left_table if join_obj.right_table == cur else None
            if not nxt:
                continue
            if nxt not in pred:
                pred[nxt] = (cur, join_obj)
                q.append(nxt)

    # Not all required tables are reachable → no indirect path exists
    if not all(t in pred for t in tables):
        return []

    # Backtrack from each table to start and collect the unique joins
    unique = {}
    for t in tables[1:]:
        cur = t
        while pred[cur][0] is not None:
            prev, join_obj = pred[cur]
            key = (join_obj.left_table, join_obj.left_column, join_obj.right_table, join_obj.right_column)
            unique.setdefault(key, join_obj)
            cur = prev

    # Materialize to the format used by the SQL builder
    inferred = []
    for (lt, lc, rt, rc), j in unique.items():
        inferred.append({
            'left_col': f"{lt}.{lc}",
            'right_col': f"{rt}.{rc}",
            'type': (j.join_type or 'INNER').upper()
        })
    return inferred


def merge_explicit_and_inferred(explicit: List[Dict], inferred: List[Dict]) -> List[Dict]:
    """De-duplicate and merge Data Context joins with inferred joins."""
    seen = set()
    out = []
    for lst in (explicit or []), (inferred or []):
        for j in lst:
            key = (j['left_col'], j['right_col'], j.get('type', 'INNER').upper())
            if key not in seen:
                seen.add(key)
                out.append({'left_col': j['left_col'], 'right_col': j['right_col'], 'type': j.get('type', 'INNER').upper()})
    return out