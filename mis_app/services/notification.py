"""
Django Notification Service
Comprehensive notification system for Django MIS application

Handles all types of notifications across the application
"""

import logging
import json
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from django.db import transaction
from django.utils import timezone
from django.core.mail import send_mail
from django.conf import settings
from django.template.loader import render_to_string
from django.contrib.auth import get_user_model
from django.db.models import Q

from ..models import (
    Notification,
    User,
    Dashboard,
    SavedReport,
    ExternalConnection,
    AuditLog,
    ExportHistory,
)
from ..utils import log_user_action

logger = logging.getLogger(__name__)
User = get_user_model()


class NotificationService:
    """
    Centralized notification service for all application events
    """
    
    def __init__(self):
        self.notification_types = {
            # Dashboard notifications
            'dashboard_shared': {
                'title': 'Dashboard Shared',
                'priority': 'medium',
                'channels': ['in_app', 'email']
            },
            'dashboard_access_changed': {
                'title': 'Dashboard Access Changed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'dashboard_data_refresh': {
                'title': 'Dashboard Data Updated',
                'priority': 'low',
                'channels': ['in_app']
            },
            'dashboard_refresh_failed': {
                'title': 'Dashboard Refresh Failed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'widget_error': {
                'title': 'Widget Error',
                'priority': 'high',
                'channels': ['in_app']
            },
            
            # Report notifications
            'report_shared': {
                'title': 'Report Shared',
                'priority': 'medium',
                'channels': ['in_app', 'email']
            },
            'report_execution_complete': {
                'title': 'Report Execution Complete',
                'priority': 'medium',
                'channels': ['in_app']
            },
            'report_execution_failed': {
                'title': 'Report Execution Failed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'scheduled_report_success': {
                'title': 'Scheduled Report Success',
                'priority': 'low',
                'channels': ['in_app']
            },
            'scheduled_report_failed': {
                'title': 'Scheduled Report Failed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            
            # Data notifications
            'data_source_added': {
                'title': 'New Data Source Added',
                'priority': 'low',
                'channels': ['in_app']
            },
            'data_upload_complete': {
                'title': 'Data Upload Complete',
                'priority': 'medium',
                'channels': ['in_app']
            },
            'data_upload_failed': {
                'title': 'Data Upload Failed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'data_freshness_warning': {
                'title': 'Stale Data Warning',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'schema_change_detected': {
                'title': 'Database Schema Changed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'connection_health_degraded': {
                'title': 'Connection Health Issues',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            
            # User and access notifications
            'role_changed': {
                'title': 'User Role Changed',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'ownership_transferred': {
                'title': 'Ownership Transferred',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'team_invite': {
                'title': 'Team Invitation',
                'priority': 'medium',
                'channels': ['in_app', 'email']
            },
            'security_alert': {
                'title': 'Security Alert',
                'priority': 'critical',
                'channels': ['in_app', 'email', 'sms']
            },
            
            # System notifications
            'performance_regression': {
                'title': 'Performance Issues Detected',
                'priority': 'medium',
                'channels': ['in_app']
            },
            'storage_warning': {
                'title': 'Storage Limit Warning',
                'priority': 'medium',
                'channels': ['in_app', 'email']
            },
            'maintenance_scheduled': {
                'title': 'Scheduled Maintenance',
                'priority': 'low',
                'channels': ['in_app', 'banner']
            },
            
            # Collaboration notifications
            'comment_added': {
                'title': 'New Comment',
                'priority': 'medium',
                'channels': ['in_app']
            },
            'mention_received': {
                'title': 'You Were Mentioned',
                'priority': 'medium',
                'channels': ['in_app', 'email']
            },
            'review_requested': {
                'title': 'Review Requested',
                'priority': 'medium',
                'channels': ['in_app']
            },
            
            # Intelligence notifications
            'anomaly_detected': {
                'title': 'Data Anomaly Detected',
                'priority': 'high',
                'channels': ['in_app', 'email']
            },
            'kpi_threshold_reached': {
                'title': 'KPI Threshold Reached',
                'priority': 'medium',
                'channels': ['in_app']
            },
            'insight_generated': {
                'title': 'New Insights Available',
                'priority': 'low',
                'channels': ['in_app']
            }
        }

    def get_recent_activity(
        self,
        user: User,
        limit: int = 15,
        include_exports: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Build a unified activity feed for a user combining notifications,
        audit log entries, and optional export history items.
        """
        activity_items: List[Dict[str, Any]] = []

        notifications = (
            Notification.objects.filter(recipient=user)
            .order_by("-created_at")[: limit * 2]
        )
        audit_logs = (
            AuditLog.objects.filter(user=user)
            .order_by("-created_at")[: limit * 2]
        )
        exports = (
            ExportHistory.objects.filter(user=user)
            .order_by("-created_at")[: limit]
            if include_exports
            else []
        )

        for notification in notifications:
            activity_items.append(
                {
                    "timestamp": notification.created_at,
                    "title": notification.title,
                    "message": notification.message,
                    "type": notification.type or "info",
                    "source": "notification",
                    "icon": self._get_notification_icon(notification.type),
                    "is_unread": not notification.is_read,
                    "metadata": notification.metadata or {},
                }
            )

        for log in audit_logs:
            activity_items.append(
                {
                    "timestamp": log.created_at,
                    "title": self._build_audit_title(log),
                    "message": self._build_audit_message(log),
                    "type": log.action,
                    "source": "audit",
                    "icon": self._get_action_icon(log.action),
                    "metadata": log.details or {},
                }
            )

        for export in exports:
            activity_items.append(
                {
                    "timestamp": export.created_at,
                    "title": f"Exported {export.source_name}",
                    "message": f"{export.filename} ({export.format_type.upper()}) with {export.row_count} rows",
                    "type": "export",
                    "source": "export",
                    "icon": "fa-file-download",
                    "metadata": {
                        "format": export.format_type,
                        "rows": export.row_count,
                        "filename": export.filename,
                    },
                }
            )

        activity_items.sort(key=lambda item: item["timestamp"], reverse=True)
        return activity_items[:limit]

    def _get_notification_icon(self, notification_type: Optional[str]) -> str:
        icons = {
            "success": "fa-circle-check text-success",
            "warning": "fa-triangle-exclamation text-warning",
            "error": "fa-circle-xmark text-danger",
            "system": "fa-gear text-secondary",
        }
        return icons.get(notification_type, "fa-bell text-info")

    def _get_action_icon(self, action: Optional[str]) -> str:
        icons = {
            "create": "fa-plus-circle text-success",
            "update": "fa-pen-to-square text-primary",
            "delete": "fa-trash text-danger",
            "login": "fa-right-to-bracket text-success",
            "logout": "fa-right-from-bracket text-muted",
            "export": "fa-file-export text-info",
            "import": "fa-file-import text-info",
            "share": "fa-share-nodes text-primary",
            "permission_change": "fa-user-shield text-warning",
            "upload": "fa-cloud-upload-alt text-info",
            "download": "fa-cloud-download-alt text-info",
        }
        return icons.get(action, "fa-circle-info text-secondary")

    def _build_audit_title(self, log: AuditLog) -> str:
        object_label = log.object_name or log.object_type or "Resource"
        return f"{log.get_action_display()} {object_label}".strip()

    def _build_audit_message(self, log: AuditLog) -> str:
        parts: List[str] = []
        if log.object_type:
            parts.append(f"Type: {log.object_type}")
        if log.details:
            summary = log.details.get("summary")
            if summary:
                parts.append(summary)
        if log.old_values and log.new_values:
            changed_fields = ", ".join(log.new_values.keys())
            parts.append(f"Updated fields: {changed_fields}")
        if not parts:
            parts.append("Activity logged for this resource.")
        return " • ".join(parts)

    def create_notification(self, notification_type: str, recipient: User, 
                          title: str = None, message: str = None, 
                          metadata: Dict = None, related_object_type: str = None,
                          related_object_id: str = None) -> 'Notification':
        """
        Create a new notification
        
        Args:
            notification_type: Type of notification
            recipient: User to receive notification
            title: Override default title
            message: Notification message
            metadata: Additional metadata
            related_object_type: Type of related object (dashboard, report, etc.)
            related_object_id: ID of related object
            
        Returns:
            Created notification instance
        """
        try:
            type_config = self.notification_types.get(notification_type, {})
            
            notification = Notification.objects.create(
                type=notification_type,
                recipient=recipient,
                title=title or type_config.get('title', 'Notification'),
                message=message or '',
                priority=type_config.get('priority', 'medium'),
                metadata=metadata or {},
                related_object_type=related_object_type,
                related_object_id=related_object_id
            )
            
            # Send through configured channels
            self._send_through_channels(notification, type_config.get('channels', ['in_app']))
            
            return notification
            
        except Exception as e:
            logger.error(f"Error creating notification: {e}")
            raise
    
    def _send_through_channels(self, notification: 'Notification', channels: List[str]):
        """Send notification through specified channels"""
        for channel in channels:
            try:
                if channel == 'email':
                    self._send_email_notification(notification)
                elif channel == 'sms':
                    self._send_sms_notification(notification)
                elif channel == 'slack':
                    self._send_slack_notification(notification)
                # in_app and banner are handled by storing in database
            except Exception as e:
                logger.error(f"Error sending notification via {channel}: {e}")
    
    def _send_email_notification(self, notification: 'Notification'):
        """Send email notification"""
        try:
            if not notification.recipient.email:
                return
            
            subject = notification.title
            
            # Render email template
            html_message = render_to_string('emails/notification.html', {
                'notification': notification,
                'recipient': notification.recipient,
                'site_url': getattr(settings, 'SITE_URL', 'http://localhost:8000')
            })
            
            send_mail(
                subject=subject,
                message=notification.message,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[notification.recipient.email],
                html_message=html_message,
                fail_silently=False
            )
            
            logger.info(f"Email notification sent to {notification.recipient.email}")
            
        except Exception as e:
            logger.error(f"Error sending email notification: {e}")
    
    def _send_sms_notification(self, notification: 'Notification'):
        """Send SMS notification (placeholder for SMS service integration)"""
        # Integrate with SMS service like Twilio, AWS SNS, etc.
        logger.info(f"SMS notification would be sent: {notification.message}")
    
    def _send_slack_notification(self, notification: 'Notification'):
        """Send Slack notification (placeholder for Slack integration)"""
        # Integrate with Slack API
        logger.info(f"Slack notification would be sent: {notification.message}")
    
    # Dashboard-specific notifications
    def notify_dashboard_shared(self, dashboard: Dashboard, shared_with: User, 
                               shared_by: User, permission_level: str):
        """Notify when dashboard is shared"""
        message = f"{shared_by.username} shared the dashboard '{dashboard.title}' with you ({permission_level} access)."
        
        return self.create_notification(
            'dashboard_shared',
            shared_with,
            message=message,
            metadata={
                'dashboard_id': str(dashboard.id),
                'shared_by': shared_by.username,
                'permission_level': permission_level
            },
            related_object_type='dashboard',
            related_object_id=str(dashboard.id)
        )
    
    def notify_dashboard_access_changed(self, dashboard: Dashboard, user: User, 
                                      old_permission: str, new_permission: str):
        """Notify when dashboard access permissions change"""
        if new_permission:
            message = f"Your access to dashboard '{dashboard.title}' has been changed to {new_permission}."
        else:
            message = f"Your access to dashboard '{dashboard.title}' has been revoked."
        
        return self.create_notification(
            'dashboard_access_changed',
            user,
            message=message,
            metadata={
                'dashboard_id': str(dashboard.id),
                'old_permission': old_permission,
                'new_permission': new_permission
            },
            related_object_type='dashboard',
            related_object_id=str(dashboard.id)
        )
    
    def notify_dashboard_refresh(self, dashboard: Dashboard, success: bool, 
                               error_message: str = None, affected_widgets: List = None):
        """Notify about dashboard data refresh"""
        if success:
            message = f"Dashboard '{dashboard.title}' has been successfully updated with fresh data."
            notification_type = 'dashboard_data_refresh'
        else:
            message = f"Failed to refresh dashboard '{dashboard.title}'. {error_message or 'Unknown error occurred.'}"
            notification_type = 'dashboard_refresh_failed'
        
        # Notify dashboard owner
        return self.create_notification(
            notification_type,
            dashboard.owner,
            message=message,
            metadata={
                'dashboard_id': str(dashboard.id),
                'success': success,
                'error_message': error_message,
                'affected_widgets': affected_widgets or []
            },
            related_object_type='dashboard',
            related_object_id=str(dashboard.id)
        )
    
    # Report-specific notifications
    def notify_report_shared(self, report: SavedReport, shared_with: User, 
                           shared_by: User, permission_level: str):
        """Notify when report is shared"""
        message = f"{shared_by.username} shared the report '{report.report_name}' with you ({permission_level} access)."
        
        return self.create_notification(
            'report_shared',
            shared_with,
            message=message,
            metadata={
                'report_id': str(report.id),
                'shared_by': shared_by.username,
                'permission_level': permission_level
            },
            related_object_type='report',
            related_object_id=str(report.id)
        )
    
    def notify_report_execution(self, report: SavedReport, user: User, 
                              success: bool, row_count: int = None, 
                              runtime_seconds: float = None, error_message: str = None):
        """Notify about report execution"""
        if success:
            message = f"Report '{report.report_name}' executed successfully. Generated {row_count} rows in {runtime_seconds:.1f}s."
            notification_type = 'report_execution_complete'
        else:
            message = f"Report '{report.report_name}' execution failed. {error_message or 'Unknown error occurred.'}"
            notification_type = 'report_execution_failed'
        
        return self.create_notification(
            notification_type,
            user,
            message=message,
            metadata={
                'report_id': str(report.id),
                'success': success,
                'row_count': row_count,
                'runtime_seconds': runtime_seconds,
                'error_message': error_message
            },
            related_object_type='report',
            related_object_id=str(report.id)
        )
    
    # Data-specific notifications
    def notify_data_upload(self, user: User, filename: str, success: bool, 
                         row_count: int = None, error_message: str = None):
        """Notify about data upload completion"""
        if success:
            message = f"Data upload '{filename}' completed successfully. {row_count} rows imported."
            notification_type = 'data_upload_complete'
        else:
            message = f"Data upload '{filename}' failed. {error_message or 'Unknown error occurred.'}"
            notification_type = 'data_upload_failed'
        
        return self.create_notification(
            notification_type,
            user,
            message=message,
            metadata={
                'filename': filename,
                'success': success,
                'row_count': row_count,
                'error_message': error_message
            }
        )
    
    def notify_schema_change(self, connection: ExternalConnection, 
                           change_type: str, affected_tables: List[str]):
        """Notify about database schema changes"""
        message = f"Schema change detected in '{connection.nickname}': {change_type}. Affected tables: {', '.join(affected_tables)}"
        
        # Notify connection owner
        return self.create_notification(
            'schema_change_detected',
            connection.owner,
            message=message,
            metadata={
                'connection_id': str(connection.id),
                'change_type': change_type,
                'affected_tables': affected_tables
            },
            related_object_type='connection',
            related_object_id=str(connection.id)
        )
    
    def notify_connection_health(self, connection: ExternalConnection, 
                               health_status: str, details: str = None):
        """Notify about connection health issues"""
        message = f"Connection '{connection.nickname}' health status: {health_status}."
        if details:
            message += f" Details: {details}"
        
        return self.create_notification(
            'connection_health_degraded',
            connection.owner,
            message=message,
            metadata={
                'connection_id': str(connection.id),
                'health_status': health_status,
                'details': details
            },
            related_object_type='connection',
            related_object_id=str(connection.id)
        )
    
    # User and security notifications
    def notify_role_change(self, user: User, old_role: str, new_role: str, changed_by: User):
        """Notify about user role changes"""
        message = f"Your role has been changed from {old_role} to {new_role} by {changed_by.username}."
        
        return self.create_notification(
            'role_changed',
            user,
            message=message,
            metadata={
                'old_role': old_role,
                'new_role': new_role,
                'changed_by': changed_by.username
            }
        )
    
    def notify_security_alert(self, user: User, alert_type: str, details: str):
        """Notify about security alerts"""
        message = f"Security alert: {alert_type}. {details}"
        
        return self.create_notification(
            'security_alert',
            user,
            message=message,
            metadata={
                'alert_type': alert_type,
                'details': details,
                'timestamp': timezone.now().isoformat()
            }
        )
    
    # Bulk notification methods
    def notify_multiple_users(self, notification_type: str, users: List[User], 
                            message: str, **kwargs):
        """Send notification to multiple users"""
        notifications = []
        for user in users:
            notification = self.create_notification(
                notification_type, user, message=message, **kwargs
            )
            notifications.append(notification)
        return notifications
    
    def notify_system_wide(self, notification_type: str, message: str, 
                          user_types: List[str] = None, **kwargs):
        """Send system-wide notification"""
        users_query = User.objects.filter(is_active=True)
        
        if user_types:
            users_query = users_query.filter(user_type__in=user_types)
        
        return self.notify_multiple_users(
            notification_type, list(users_query), message, **kwargs
        )
    
    # Notification management methods
    def mark_as_read(self, notification_id: str, user: User) -> bool:
        """Mark notification as read"""
        try:
            notification = Notification.objects.get(id=notification_id, recipient=user)
            notification.is_read = True
            notification.read_at = timezone.now()
            notification.save()
            return True
        except Notification.DoesNotExist:
            return False
    
    def mark_all_as_read(self, user: User) -> int:
        """Mark all notifications as read for user"""
        count = Notification.objects.filter(
            recipient=user, is_read=False
        ).update(
            is_read=True, 
            read_at=timezone.now()
        )
        return count
    
    def get_user_notifications(self, user: User, unread_only: bool = False, 
                             limit: int = 50) -> List['Notification']:
        """Get notifications for user"""
        notifications = Notification.objects.filter(recipient=user)
        
        if unread_only:
            notifications = notifications.filter(is_read=False)
        
        return list(notifications.order_by('-created_at')[:limit])
    
    def get_unread_count(self, user: User) -> int:
        """Get count of unread notifications for user"""
        return Notification.objects.filter(recipient=user, is_read=False).count()
    
    def cleanup_old_notifications(self, days: int = 30):
        """Clean up notifications older than specified days"""
        cutoff_date = timezone.now() - timedelta(days=days)
        deleted_count = Notification.objects.filter(
            created_at__lt=cutoff_date
        ).delete()[0]
        
        logger.info(f"Cleaned up {deleted_count} old notifications")
        return deleted_count
    
    def get_notification_preferences(self, user: User) -> Dict[str, Any]:
        """Get user notification preferences"""
        # This would integrate with a user preferences model
        # For now, return default preferences
        return {
            'email_enabled': True,
            'in_app_enabled': True,
            'email_frequency': 'immediate',  # immediate, daily, weekly
            'quiet_hours': {
                'enabled': False,
                'start_time': '22:00',
                'end_time': '08:00'
            },
            'notification_types': {
                notification_type: True 
                for notification_type in self.notification_types.keys()
            }
        }
    
    def update_notification_preferences(self, user: User, preferences: Dict[str, Any]):
        """Update user notification preferences"""
        # This would update a user preferences model
        # Implementation depends on your preferences storage approach
        pass


# Global notification service instance
notification_service = NotificationService()
