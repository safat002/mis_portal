# mis_app/permissions.py

"""
Permission system for MIS Application
FIXES:
1. Implements Cache Versioning for instant invalidation (no delete_pattern needed).
2. Updates user_can_access_connection to honor 'upload' and 'edit' grants.
"""
import re
import json
from functools import wraps
import logging
from django.db.models import Q
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.contrib.auth.decorators import login_required
from django.core.exceptions import PermissionDenied
from django.core.cache import cache
from django.utils import timezone

# Assuming these imports are correct based on your project structure
from .services.external_db import ExternalDBService 
from .models import User, UserGroup, GroupPermission, UserPermission, ExternalConnection

logger = logging.getLogger(__name__)


class PermissionManager:
    """Centralized permission management with caching and ID-aware checks."""
    
    CACHE_TIMEOUT = 300  # 5 minutes
    LEVELS = {'none': 0, 'view': 1, 'upload': 2, 'edit': 3, 'admin': 4}
    
    # --- Cache Versioning Helpers (FIX 3) ---
    @staticmethod
    def _get_ver(user_id):
        """Gets the current permission version for a user."""
        return cache.get(f'permver:{user_id}', 1)

    @staticmethod
    def _bump_ver(user_id):
        """Increments the permission version, invalidating all old keys (atomically)."""
        # Attempts to atomically increment the key. If it fails (e.g., key doesn't exist
        # or cache is LocMemCache which doesn't fully support incr), it sets it manually.
        try:
            cache.incr(f'permver:{user_id}', delta=1)
        except ValueError:
            # Handle non-existent key (first access) or non-supporting cache backend
            current_ver = cache.get(f'permver:{user_id}', 1)
            cache.set(f'permver:{user_id}', current_ver + 1, PermissionManager.CACHE_TIMEOUT * 2)
        except NotImplementedError:
            # Handle backends that don't implement incr()
            current_ver = cache.get(f'permver:{user_id}', 1)
            cache.set(f'permver:{user_id}', current_ver + 1, PermissionManager.CACHE_TIMEOUT * 2)
    
    # --- Core Helpers ---
    @staticmethod
    def _level_at_least(user_level, required_level):
        """Compare permission levels"""
        return PermissionManager.LEVELS.get(user_level, 0) >= PermissionManager.LEVELS.get(required_level, 0)
    
    @staticmethod
    def check_user_permission(user, resource_type, resource_name='*', required_level='view'):
        """
        Check if user has required permission level for a resource (by ID or Name).
        
        This fixed version ensures correct lookup priorities:
        1. Superuser/Admin bypass.
        2. Specific resource grant (by ID or Name).
        3. Universal wildcard grant ('*').
        """
        
        # Superusers and Admins have all permissions
        if getattr(user, 'is_superuser', False) or getattr(user, 'user_type', '') == 'Admin':
            return True
        
        # 1. Role-based checks for system functions (Efficiency fix)
        if resource_type == 'user_management' and user.can_manage_users(): return True
        if resource_type == 'database_management' and user.can_manage_database(): return True
        # Data management permission handling is generally deferred to resource-level grants
        
        # Normalize the identifier (connection ID, dashboard ID, etc.)
        key = '*' if resource_name is None else str(resource_name)
        ver = PermissionManager._get_ver(user.id)  # Use version in key (FIX 3)

        # 2. Check Cache (Use versioned key)
        cache_key = f"perm:v{ver}:{user.id}:{resource_type}:{key}"
        cached_level = cache.get(cache_key)
        if cached_level is not None:
            return PermissionManager._level_at_least(cached_level, required_level)

        highest = 'none'

        # --- CORE FIX: Use Q objects to match by resource_name ('key' or '*') OR by resource_id (key) ---

        # 3. Direct user perm
        q_user_filter = Q(resource_type=resource_type) & Q(user=user)
        # Match where: (resource_name is the specific key OR '*') OR (resource_id is the specific key)
        q_lookup = Q(resource_name__in=[key, '*']) | Q(resource_id=key)

        up = (UserPermission.objects
              .filter(q_user_filter & q_lookup)
              .order_by('-permission_level')
              .first())
        
        if up and not up.is_expired(): 
            highest = up.permission_level

        # 4. Group perms
        for group in getattr(user, 'user_groups', []).all():
            q_group_filter = Q(resource_type=resource_type) & Q(group=group)
            
            # Use the same lookup filter
            gp = (GroupPermission.objects
                  .filter(q_group_filter & q_lookup)
                  .order_by('-permission_level')
                  .first())
            
            # Only update if the group grant is higher than the current highest grant
            if gp and PermissionManager._level_at_least(gp.permission_level, highest):
                highest = gp.permission_level

        # 5. Cache and return (Use versioned key)
        cache.set(cache_key, highest, PermissionManager.CACHE_TIMEOUT)
        return PermissionManager._level_at_least(highest, required_level)
    
    @staticmethod
    def get_user_permissions(user, resource_type=None):
        """
        Get all permissions for a user.
        """
        ver = PermissionManager._get_ver(user.id)  # Use version in key (FIX 3)
        # Cache key includes user ID and resource type filter
        cache_key = f"user_all_permissions_v{ver}_{user.id}_{resource_type or 'all'}"
        cached_permissions = cache.get(cache_key)
        
        if cached_permissions is not None:
            return cached_permissions

        permissions = {}

        # Get direct user permissions first
        direct_perms_query = UserPermission.objects.filter(user=user)
        if resource_type:
            direct_perms_query = direct_perms_query.filter(resource_type=resource_type)
        
        for perm in direct_perms_query:
            # FIX: Only include non-expired permissions
            if not perm.is_expired(): 
                key = f"{perm.resource_type}:{perm.resource_id}:{perm.resource_name}"
                permissions[key] = {
                    'resource_type': perm.resource_type, 
                    'resource_id': str(perm.resource_id) if perm.resource_id else None,
                    'resource_name': perm.resource_name, 
                    'permission_level': perm.permission_level
                }

        # Get group permissions (overridden by direct permissions)
        for group in user.user_groups.filter(is_active=True):
            group_perms_query = GroupPermission.objects.filter(group=group)
            if resource_type:
                group_perms_query = group_perms_query.filter(resource_type=resource_type)
            
            for perm in group_perms_query:
                key = f"{perm.resource_type}:{perm.resource_id}:{perm.resource_name}"
                if key not in permissions:
                    permissions[key] = {
                        'resource_type': perm.resource_type, 
                        'resource_id': str(perm.resource_id) if perm.resource_id else None,
                        'resource_name': perm.resource_name, 
                        'permission_level': perm.permission_level
                    }

        final_permissions_list = list(permissions.values())
        
        cache.set(cache_key, final_permissions_list, PermissionManager.CACHE_TIMEOUT)
        return final_permissions_list
    
    @staticmethod
    def get_user_accessible_tables(user, connection_id=None):
        """
        Get a list of all table names a user can access for a given connection.
        Returns None if the user has blanket access (Admin, Owner, or connection-level grant).
        
        FIX: This method's logic is streamlined and fixed to resolve the table visibility issue.
        """
        # Admin/Superuser gets all visible tables (signal 'None' to fetch all)
        if user.is_superuser or user.user_type == 'Admin':
            return None 

        accessible_tables = set()
        
        # Check if the user is the owner (Owner gets all visible tables)
        is_owner = connection_id and ExternalConnection.objects.filter(id=connection_id, owner=user).exists()
        if is_owner:
            return None 

        # Determine if the user has blanket access to the entire connection via permission grant
        # This is the key fix for Uploaders/Moderators who have connection-level grants.
        if connection_id and (
            PermissionManager.check_user_permission(user, 'connection', connection_id, 'view') or
            PermissionManager.check_user_permission(user, 'connection', connection_id, 'upload') or
            PermissionManager.check_user_permission(user, 'connection', connection_id, 'edit')
        ):
            return None
        # Load specific tables the user has been granted access to.
        table_permissions = PermissionManager.get_user_permissions(user, 'table')
        
        for perm in table_permissions:
            perm_connection_id = perm.get('resource_id')
            table_name = perm.get('resource_name')

            if connection_id and str(perm_connection_id) != str(connection_id):
                continue
            
            if PermissionManager._level_at_least(perm.get('permission_level'), 'view') and table_name:
                accessible_tables.add(table_name)

        # Add tables the user personally uploaded (if applicable).
        if user.can_upload_data() and connection_id:
            from .models import UploadedTable
            uploaded_tables_query = UploadedTable.objects.filter(uploaded_by=user, connection_id=connection_id)
            user_uploaded_tables = uploaded_tables_query.values_list('table_name', flat=True)
            accessible_tables.update(user_uploaded_tables)

        return sorted(list(accessible_tables))

    @staticmethod
    def user_can_access_connection(user, connection):
        """
        Check if a user can access a connection either directly or via permissions.
        FIX: Include upload and edit as valid access levels (FIX 2).
        """
        if user.is_superuser or getattr(user, 'user_type', None) == 'Admin':
            return True
        
        conn_id = str(connection.id)
        
        # Owner check
        if getattr(connection, 'owner_id', None) == getattr(user, 'id', None):
            return True

        # FIX: Check for view OR upload OR edit permission
        for level in ('view', 'upload', 'edit'):
            if PermissionManager.check_user_permission(user, 'connection', conn_id, level):
                return True
        return False  # No access found

    @staticmethod
    def clear_user_cache(user_id):
        """
        Clears cached permissions by bumping the version number (FIX 3).
        This works reliably across all cache backends.
        """
        PermissionManager._bump_ver(user_id)


# --- DECORATORS (Flexible and ID-Aware) ---

def _extract_ids_from_request(request, kwargs):
    """
    Extract connection_id and table_name from JSON body, POST, GET, URL kwargs,
    with common alias keys. Works for GET and POST alike.
    """
    # 1) Parse JSON body only for methods that usually carry a body
    data = {}
    if request.method in ('POST', 'PUT', 'PATCH') and request.body:
        try:
            data = json.loads(request.body.decode('utf-8'))
        except Exception:
            data = {}

    # 2) Merge URL kwargs with resolver_match.kwargs (extra safety)
    rm_kwargs = getattr(getattr(request, 'resolver_match', None), 'kwargs', {}) or {}
    kw = {**rm_kwargs, **(kwargs or {})}

    # 3) Read from body, POST, GET, or URL kwargs with common aliases
    connection_id = (
        data.get('connection_id') or data.get('connectionId') or
        request.POST.get('connection_id') or request.POST.get('connectionId') or
        request.GET.get('connection_id') or request.GET.get('connectionId') or
        kw.get('connection_id') or kw.get('connectionId') or
        kw.get('id') or kw.get('pk') or kw.get('conn_id')
    )

    table_name = (
        data.get('table_name') or data.get('tableName') or data.get('table') or
        request.POST.get('table_name') or request.POST.get('tableName') or request.POST.get('table') or
        request.GET.get('table_name') or request.GET.get('tableName') or request.GET.get('table') or
        kw.get('table_name') or kw.get('tableName') or kw.get('table')
    )

    # 4) Last-resort: parse UUID from path for /api/model/{get|save}/<uuid>/
    if not connection_id:
        m = re.search(r'/api/model/(?:get|save)/([0-9a-fA-F-]{36})/?', request.path_info or '')
        if m:
            connection_id = m.group(1)

    return connection_id, table_name




def connection_permission_required(resource_type='connection', resource_name=None, permission_level='view'):
    """Decorator to check connection-level permission, flexible on request keys."""
    def decorator(view_func):
        @wraps(view_func)
        @login_required
        def _wrapped(request, *args, **kwargs):
            connection_id, _ = _extract_ids_from_request(request, kwargs)
            
            if not connection_id:
                return JsonResponse({'success': False, 'error': 'connection_id is required'}, status=400)

            if not PermissionManager.check_user_permission(request.user, 'connection', str(connection_id), permission_level):
                return JsonResponse({'success': False, 'error': f'Permission denied: {permission_level} access required on this connection.'}, status=403)

            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator


def table_permission_required(resource_type='table', resource_name=None, permission_level='view'):
    """Decorator to check table-level permission, with fallback to connection, flexible on keys."""
    def decorator(view_func):
        @wraps(view_func)
        @login_required
        def _wrapped_view(request, *args, **kwargs):
            connection_id, table_name = _extract_ids_from_request(request, kwargs)

            if not connection_id or not table_name:
                return JsonResponse({'success': False, 'error': 'connection_id and table_name are required.'}, status=400)

            # 1) Check direct table grant (by name)
            has_table = PermissionManager.check_user_permission(request.user, 'table', table_name, permission_level)

            # 2) Check parent connection grant (by ID string)
            has_conn = has_table or PermissionManager.check_user_permission(request.user, 'connection', str(connection_id), permission_level)

            if not has_conn:
                return JsonResponse({'success': False, 'error': f"Permission denied: You don't have required permission on this table/connection."}, status=403)

            return view_func(request, *args, **kwargs)
        return _wrapped_view
    return decorator


# --- SYSTEM ROLE DECORATORS (kept for compatibility) ---

def admin_required(view_func):
    """Decorator to require admin privileges"""
    @wraps(view_func)
    @login_required
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.can_access_admin():
            return JsonResponse({'success': False, 'error': 'Admin access required'}, status=403)
        return view_func(request, *args, **kwargs)
    return _wrapped_view


def user_management_required(view_func):
    """Decorator to require user management privileges"""
    @wraps(view_func)
    @login_required
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.can_manage_users():
            return JsonResponse({'success': False, 'error': 'User management privileges required'}, status=403)
        return view_func(request, *args, **kwargs)
    return _wrapped_view


def database_management_required(view_func):
    """Decorator to require database management privileges"""
    @wraps(view_func)
    @login_required
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.can_manage_database():
            return JsonResponse({'success': False, 'error': 'Database management privileges required'}, status=403)
        return view_func(request, *args, **kwargs)
    return _wrapped_view


def upload_required(view_func):
    """Decorator to require upload privileges"""
    @wraps(view_func)
    @login_required
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.can_upload_data():
            return JsonResponse({'success': False, 'error': 'Upload privileges required'}, status=403)
        return view_func(request, *args, **kwargs)
    return _wrapped_view


def schema_modify_required(view_func):
    """Decorator to require schema modification privileges"""
    @wraps(view_func)
    @login_required
    def _wrapped_view(request, *args, **kwargs):
        if not request.user.can_modify_schema():
            return JsonResponse({'success': False, 'error': 'Schema modification privileges required'}, status=403)
        return view_func(request, *args, **kwargs)
    return _wrapped_view


class OwnershipMixin:
    """Mixin to check resource ownership (Kept for compatibility)"""
    
    def check_ownership_or_permission(self, request, obj, permission_level='view'):
        """Check if user owns the object or has permission through groups"""
        # Owner always has access
        if hasattr(obj, 'owner') and obj.owner == request.user:
            return True
        
        # Admin has access to everything
        if request.user.user_type == 'Admin':
            return True
        
        # Check if shared with user
        if hasattr(obj, 'shared_with'):
            shared = obj.shared_with.filter(id=request.user.id).first()
            if shared:
                # Check share permission level
                share_model = obj.__class__.__name__ + 'Share'
                if hasattr(obj, share_model.lower() + '_set'):
                    share_obj = getattr(obj, share_model.lower() + '_set').filter(user=request.user).first()
                    if share_obj and PermissionManager._level_at_least(share_obj.permission, permission_level):
                        return True
        
        return False
    
INTELLIGENT_IMPORT_PERMISSIONS = {
    'upload': ['Uploader', 'Moderator', 'Admin'],
    'approve_schema': ['Moderator', 'Admin'],
    'approve_import': ['Moderator', 'Admin'],
    'rollback': ['Admin'],
    'view_all_sessions': ['Moderator', 'Admin'],
}

def check_intelligent_import_permission(user, action):
    """Check if user has permission for intelligent import action"""
    if not user.is_authenticated:
        return False
    
    allowed_roles = INTELLIGENT_IMPORT_PERMISSIONS.get(action, [])
    return user.user_type in allowed_roles
