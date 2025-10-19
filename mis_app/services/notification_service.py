"""
Notification Service for Django MIS Application
Handles all types of notifications with multiple delivery channels
"""

import logging
from typing import Dict, Any, List, Optional
from django.contrib.auth import get_user_model
from django.utils import timezone
from django.conf import settings
from django.template.loader import render_to_string
from django.core.mail import send_mail
from django.db import transaction

logger = logging.getLogger(__name__)
User = get_user_model()


class NotificationService:
    """Centralized notification service for all notification types"""
    
    def __init__(self):
        self.enabled_channels = {
            'in_app': True,
            'email': getattr(settings, 'EMAIL_BACKEND', None) is not None,
            'sms': False,  # Enable when SMS provider is configured
            'slack': False,  # Enable when Slack integration is configured
        }
    
    def create_notification(self, notification_type: str, recipient: User, 
                          title: str, message: str, priority: str = 'medium',
                          metadata: Dict[str, Any] = None, 
                          related_object_type: str = None,
                          related_object_id: str = None) -> Optional['Notification']:
        """
        Create a new notification
        
        Args:
            notification_type: Type of notification
            recipient: User to receive notification
            title: Notification title
            message: Notification message
            priority: Priority level (low, medium, high, critical)
            metadata: Additional data
            related_object_type: Type of related object
            related_object_id: ID of related object
            
        Returns:
            Created notification instance
        """
        try:
            from ..models import Notification
            
            with transaction.atomic():
                notification = Notification.objects.create(
                    type=notification_type,
                    recipient=recipient,
                    title=title,
                    message=message,
                    priority=priority,
                    metadata=metadata or {},
                    related_object_type=related_object_type,
                    related_object_id=str(related_object_id) if related_object_id else None
                )
                
                # Send through enabled channels
                self._send_notification(notification)
                
                logger.info(f"Notification created for {recipient.username}: {title}")
                return notification
                
        except Exception as e:
            logger.error(f"Failed to create notification: {e}")
            return None
    
    def _send_notification(self, notification: 'Notification'):
        """Send notification through all enabled channels"""
        
        # Always store in-app notification (already created)
        if self.enabled_channels['in_app']:
            pass  # Already stored in database
        
        # Send email notification for high/critical priority
        if (self.enabled_channels['email'] and 
            notification.priority in ['high', 'critical']):
            self._send_email_notification(notification)
        
        # Send SMS for critical notifications
        if (self.enabled_channels['sms'] and 
            notification.priority == 'critical'):
            self._send_sms_notification(notification)
        
        # Send Slack notification
        if self.enabled_channels['slack']:
            self._send_slack_notification(notification)
    
    def _send_email_notification(self, notification: 'Notification'):
        """Send email notification"""
        try:
            subject = f"[MIS] {notification.title}"
            
            # Use template if available
            try:
                message = render_to_string('emails/notification.html', {
                    'notification': notification,
                    'recipient': notification.recipient,
                    'site_name': getattr(settings, 'SITE_NAME', 'MIS Platform'),
                })
            except:
                message = f"{notification.message}\n\n---\nMIS Platform"
            
            send_mail(
                subject=subject,
                message=notification.message,  # Plain text fallback
                html_message=message,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[notification.recipient.email],
                fail_silently=True
            )
            
            logger.info(f"Email sent to {notification.recipient.email}")
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def _send_sms_notification(self, notification: 'Notification'):
        """Send SMS notification (placeholder for future implementation)"""
        # Implementation would depend on SMS provider (Twilio, etc.)
        logger.info(f"SMS notification would be sent to {notification.recipient.username}")
    
    def _send_slack_notification(self, notification: 'Notification'):
        """Send Slack notification (placeholder for future implementation)"""
        # Implementation would use Slack API
        logger.info(f"Slack notification would be sent for {notification.title}")
    
    def mark_as_read(self, notification_id: str, user: User) -> bool:
        """Mark notification as read"""
        try:
            from ..models import Notification
            
            notification = Notification.objects.get(
                id=notification_id,
                recipient=user
            )
            
            if not notification.is_read:
                notification.is_read = True
                notification.read_at = timezone.now()
                notification.save()
                
                logger.info(f"Notification {notification_id} marked as read")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark notification as read: {e}")
            return False
    
    def get_unread_count(self, user: User) -> int:
        """Get count of unread notifications for user"""
        try:
            from ..models import Notification
            return Notification.objects.filter(
                recipient=user,
                is_read=False
            ).count()
        except Exception as e:
            logger.error(f"Failed to get unread count: {e}")
            return 0
    
    def get_notifications(self, user: User, limit: int = 50) -> List['Notification']:
        """Get recent notifications for user"""
        try:
            from ..models import Notification
            return list(Notification.objects.filter(
                recipient=user
            ).order_by('-created_at')[:limit])
        except Exception as e:
            logger.error(f"Failed to get notifications: {e}")
            return []
    
    # Dashboard-specific notifications
    def notify_dashboard_shared(self, dashboard, recipient: User, shared_by: User, permission: str):
        """Notify user that a dashboard was shared with them"""
        self.create_notification(
            notification_type='dashboard_shared',
            recipient=recipient,
            title=f'Dashboard "{dashboard.title}" shared with you',
            message=f'{shared_by.username} shared the dashboard "{dashboard.title}" with {permission} access.',
            metadata={
                'dashboard_id': str(dashboard.id),
                'shared_by': shared_by.username,
                'permission': permission
            },
            related_object_type='dashboard',
            related_object_id=dashboard.id
        )
    
    def notify_dashboard_data_refresh(self, dashboard, user: User, success: bool, 
                                    error_message: str = None):
        """Notify about dashboard data refresh"""
        if success:
            self.create_notification(
                notification_type='dashboard_data_refresh',
                recipient=user,
                title=f'Dashboard "{dashboard.title}" updated',
                message='Dashboard data has been refreshed successfully.',
                priority='low',
                metadata={'dashboard_id': str(dashboard.id)},
                related_object_type='dashboard',
                related_object_id=dashboard.id
            )
        else:
            self.create_notification(
                notification_type='dashboard_refresh_failed',
                recipient=user,
                title=f'Dashboard "{dashboard.title}" refresh failed',
                message=f'Dashboard data refresh failed: {error_message}',
                priority='high',
                metadata={
                    'dashboard_id': str(dashboard.id),
                    'error': error_message
                },
                related_object_type='dashboard',
                related_object_id=dashboard.id
            )
    
    # Report-specific notifications
    def notify_report_shared(self, report, recipient: User, shared_by: User, permission: str):
        """Notify user that a report was shared with them"""
        self.create_notification(
            notification_type='report_shared',
            recipient=recipient,
            title=f'Report "{report.report_name}" shared with you',
            message=f'{shared_by.username} shared the report "{report.report_name}" with {permission} access.',
            metadata={
                'report_id': str(report.id),
                'shared_by': shared_by.username,
                'permission': permission
            },
            related_object_type='report',
            related_object_id=report.id
        )
    
    def notify_report_execution(self, report, user: User, success: bool, 
                              row_count: int = None, runtime_seconds: float = None,
                              error_message: str = None):
        """Notify about report execution"""
        if success:
            self.create_notification(
                notification_type='report_execution_complete',
                recipient=user,
                title=f'Report "{report.report_name}" completed',
                message=f'Report executed successfully: {row_count} rows in {runtime_seconds:.1f}s',
                priority='low',
                metadata={
                    'report_id': str(report.id),
                    'row_count': row_count,
                    'runtime_seconds': runtime_seconds
                },
                related_object_type='report',
                related_object_id=report.id
            )
        else:
            self.create_notification(
                notification_type='report_execution_failed',
                recipient=user,
                title=f'Report "{report.report_name}" failed',
                message=f'Report execution failed: {error_message}',
                priority='high',
                metadata={
                    'report_id': str(report.id),
                    'error': error_message
                },
                related_object_type='report',
                related_object_id=report.id
            )
    
    # Data-specific notifications
    def notify_data_upload(self, user: User, filename: str, success: bool,
                          row_count: int = None, error_message: str = None):
        """Notify about data upload completion"""
        if success:
            self.create_notification(
                notification_type='data_upload_complete',
                recipient=user,
                title=f'Data upload completed: {filename}',
                message=f'Successfully uploaded {row_count} rows from {filename}',
                priority='low',
                metadata={
                    'filename': filename,
                    'row_count': row_count
                }
            )
        else:
            self.create_notification(
                notification_type='data_upload_failed',
                recipient=user,
                title=f'Data upload failed: {filename}',
                message=f'Upload failed: {error_message}',
                priority='medium',
                metadata={
                    'filename': filename,
                    'error': error_message
                }
            )
    
    def notify_connection_health(self, connection, status: str, message: str):
        """Notify about database connection health issues"""
        if status in ['unhealthy', 'error']:
            self.create_notification(
                notification_type='connection_health_degraded',
                recipient=connection.owner,
                title=f'Database connection issue: {connection.nickname}',
                message=f'Connection "{connection.nickname}" is {status}: {message}',
                priority='high' if status == 'error' else 'medium',
                metadata={
                    'connection_id': str(connection.id),
                    'status': status,
                    'db_type': connection.db_type
                },
                related_object_type='connection',
                related_object_id=connection.id
            )
    
    # User and security notifications
    def notify_role_change(self, user: User, old_role: str, new_role: str, changed_by: User):
        """Notify user about role change"""
        self.create_notification(
            notification_type='role_changed',
            recipient=user,
            title='Your role has been updated',
            message=f'Your role has been changed from {old_role} to {new_role} by {changed_by.username}',
            priority='medium',
            metadata={
                'old_role': old_role,
                'new_role': new_role,
                'changed_by': changed_by.username
            }
        )
    
    def notify_security_alert(self, user: User, alert_type: str, details: str):
        """Send security alert notification"""
        self.create_notification(
            notification_type='security_alert',
            recipient=user,
            title=f'Security Alert: {alert_type}',
            message=f'Security event detected: {details}',
            priority='critical',
            metadata={
                'alert_type': alert_type,
                'details': details
            }
        )
    
    # System notifications
    def notify_performance_issue(self, users: List[User], issue_type: str, details: str):
        """Notify about system performance issues"""
        for user in users:
            self.create_notification(
                notification_type='performance_regression',
                recipient=user,
                title=f'Performance Issue: {issue_type}',
                message=f'System performance issue detected: {details}',
                priority='medium',
                metadata={
                    'issue_type': issue_type,
                    'details': details
                }
            )
    
    def notify_maintenance(self, users: List[User], maintenance_type: str, 
                          scheduled_time: str, duration: str):
        """Notify about scheduled maintenance"""
        for user in users:
            self.create_notification(
                notification_type='maintenance_scheduled',
                recipient=user,
                title=f'Scheduled Maintenance: {maintenance_type}',
                message=f'Maintenance scheduled for {scheduled_time} (Duration: {duration})',
                priority='medium',
                metadata={
                    'maintenance_type': maintenance_type,
                    'scheduled_time': scheduled_time,
                    'duration': duration
                }
            )
    
    # Cleanup old notifications
    def cleanup_old_notifications(self, days: int = 30):
        """Remove old notifications"""
        try:
            from ..models import Notification
            from datetime import timedelta
            
            cutoff_date = timezone.now() - timedelta(days=days)
            
            deleted_count, _ = Notification.objects.filter(
                created_at__lt=cutoff_date
            ).delete()
            
            logger.info(f"Cleaned up {deleted_count} old notifications")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup notifications: {e}")
            return 0


# Create global instance
notification_service = NotificationService()