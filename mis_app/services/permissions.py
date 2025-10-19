
# mis_app/services/permissions.py

from django.contrib.auth import get_user_model
from django.db.models import Q
from mis_app.models import ExternalConnection
from mis_app.permissions import PermissionManager

User = get_user_model()

def has_permission(user, permission_codename, obj):
    """Check if a user has a specific permission for a given object."""
    if user.is_superuser or getattr(user, 'user_type', None) == 'Admin':
        return True

    if getattr(obj, 'owner_id', None) == getattr(user, 'id', None):
        return True

    resource_name = str(obj.id)
    if permission_codename == 'view':
        return PermissionManager.user_can_access_connection(user, obj)

    return PermissionManager.check_user_permission(
        user,
        'connection',
        resource_name,
        permission_codename
    )

def get_accessible_connections(user):
    """Get all active connections a user can access."""
    base_qs = ExternalConnection.objects.filter(is_active=True)
    if user.is_superuser or getattr(user, 'user_type', None) == 'Admin':
        return base_qs.order_by('nickname')

    all_permissions = PermissionManager.get_user_permissions(user)
    connection_ids = {
        str(perm.get('resource_id'))
        for perm in all_permissions
        if perm.get('resource_id') and perm.get('resource_type') in ['connection', 'table']
    }

    query = Q(owner=user)
    if connection_ids:
        query |= Q(id__in=list(connection_ids))

    return base_qs.filter(query).distinct().order_by('nickname')
