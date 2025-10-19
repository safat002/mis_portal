"""
MIS App Services Package
Centralized business logic and service layer
"""

# Import main services for easy access
from .notification_service import notification_service
from .permissions import has_permission, get_accessible_connections

__all__ = [
    'notification_service',
    'has_permission',
    'get_accessible_connections',
]
