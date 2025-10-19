"""
Django Signals for MIS Application - Fixed Version
Handle model lifecycle events with safe imports
"""

import logging
from django.db.models.signals import post_save, post_delete, pre_save
from django.dispatch import receiver
from django.contrib.auth.signals import user_logged_in, user_logged_out
from django.utils import timezone

from mis_app.models import DashboardVersionHistory

logger = logging.getLogger(__name__)

# Safe imports with error handling
try:
    from .models import (
        Dashboard, SavedReport, ExternalConnection, Widget, 
        DashboardShare, User, Notification, DashboardVersionHistory
    )
    MODELS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Models not available during import: {e}")
    MODELS_AVAILABLE = False

# Safe service imports
try:
    from .services.notification_service import notification_service
    NOTIFICATION_SERVICE_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Notification service not available: {e}")
    NOTIFICATION_SERVICE_AVAILABLE = False

try:
    from .utils import log_user_action
    UTILS_AVAILABLE = True
except ImportError as e:
    logger.warning(f"Utils not available: {e}")
    UTILS_AVAILABLE = False


# Only register signals if models are available
if MODELS_AVAILABLE:
    
    @receiver(post_save, sender=Dashboard)
    def dashboard_post_save(sender, instance, created, **kwargs):
        """Handle dashboard creation and updates"""
        try:
            if created:
                logger.info(f"Dashboard created: {instance.title} by {instance.owner.username}")
                
                # Log creation in audit trail
                if UTILS_AVAILABLE:
                    log_user_action(
                        instance.owner,
                        'create',
                        'dashboard',
                        str(instance.id),
                        f'Created dashboard: {instance.title}',
                        {'title': instance.title}
                    )
        except Exception as e:
            logger.error(f"Error in dashboard_post_save signal: {e}")


    @receiver(post_delete, sender=Dashboard)
    def dashboard_post_delete(sender, instance, **kwargs):
        """Handle dashboard deletion"""
        try:
            logger.info(f"Dashboard deleted: {instance.title}")
        except Exception as e:
            logger.error(f"Error in dashboard_post_delete signal: {e}")


    @receiver(post_save, sender=DashboardShare)
    def dashboard_share_post_save(sender, instance, created, **kwargs):
        """Handle dashboard sharing"""
        try:
            if created:
                logger.info(f"Dashboard {instance.dashboard.title} shared with {instance.user.username}")
                
                # Send notification
                if NOTIFICATION_SERVICE_AVAILABLE:
                    notification_service.notify_dashboard_shared(
                        instance.dashboard,
                        instance.user,
                        instance.shared_by,
                        instance.permission
                    )
        except Exception as e:
            logger.error(f"Error in dashboard_share_post_save signal: {e}")


    @receiver(post_save, sender=SavedReport)
    def report_post_save(sender, instance, created, **kwargs):
        """Handle report creation and updates"""
        try:
            if created:
                logger.info(f"Report created: {instance.report_name} by {instance.owner.username}")
                
                # Log creation
                if UTILS_AVAILABLE:
                    log_user_action(
                        instance.owner,
                        'create',
                        'saved_report',
                        str(instance.id),
                        f'Created report: {instance.report_name}',
                        {'report_name': instance.report_name}
                    )
        except Exception as e:
            logger.error(f"Error in report_post_save signal: {e}")


    @receiver(post_save, sender=ExternalConnection)
    def connection_post_save(sender, instance, created, **kwargs):
        """Handle database connection creation and updates"""
        try:
            if created:
                logger.info(f"Database connection created: {instance.nickname} by {instance.owner.username}")
                
                # Test the connection in background (if tasks are available)
                try:
                    from .tasks import test_connection_health
                    test_connection_health.delay(str(instance.id))
                except ImportError:
                    logger.info("Background tasks not available for connection testing")
        except Exception as e:
            logger.error(f"Error in connection_post_save signal: {e}")


    @receiver(post_save, sender=Widget)
    def widget_post_save(sender, instance, created, **kwargs):
        """Handle widget creation and updates"""
        try:
            if created:
                logger.info(f"Widget created: {instance.title} on dashboard {instance.dashboard.title}")
        except Exception as e:
            logger.error(f"Error in widget_post_save signal: {e}")


    @receiver(user_logged_in)
    def user_logged_in_handler(sender, request, user, **kwargs):
        """Handle user login"""
        try:
            logger.info(f"User logged in: {user.username}")
            
            # Update last login time
            user.last_login = timezone.now()
            user.save(update_fields=['last_login'])
            
            # Log login action
            if UTILS_AVAILABLE:
                log_user_action(
                    user,
                    'login',
                    'user',
                    str(user.id),
                    f'User logged in',
                    {
                        'ip_address': request.META.get('REMOTE_ADDR', 'unknown'),
                        'user_agent': request.META.get('HTTP_USER_AGENT', 'unknown')[:200]
                    }
                )
        except Exception as e:
            logger.error(f"Error in user_logged_in_handler signal: {e}")


    @receiver(user_logged_out)
    def user_logged_out_handler(sender, request, user, **kwargs):
        """Handle user logout"""
        try:
            if user:
                logger.info(f"User logged out: {user.username}")
                
                # Log logout action
                if UTILS_AVAILABLE:
                    log_user_action(
                        user,
                        'logout',
                        'user',
                        str(user.id),
                        f'User logged out',
                        {}
                    )
        except Exception as e:
            logger.error(f"Error in user_logged_out_handler signal: {e}")


    @receiver(pre_save, sender=User)
    def user_pre_save(sender, instance, **kwargs):
        """Handle user model changes before saving"""
        try:
            # Check if user type changed
            if instance.pk:
                try:
                    old_instance = User.objects.get(pk=instance.pk)
                    if old_instance.user_type != instance.user_type:
                        # User role changed - will trigger notification after save
                        instance._role_changed = {
                            'old_role': old_instance.user_type,
                            'new_role': instance.user_type
                        }
                except User.DoesNotExist:
                    pass
        except Exception as e:
            logger.error(f"Error in user_pre_save signal: {e}")


    @receiver(post_save, sender=User)
    def user_post_save(sender, instance, created, **kwargs):
        """Handle user creation and updates"""
        try:
            if created:
                logger.info(f"User created: {instance.username}")
                
                # Welcome notification for new users
                if NOTIFICATION_SERVICE_AVAILABLE:
                    notification_service.create_notification(
                        'welcome',
                        instance,
                        title='Welcome to MIS Platform',
                        message='Welcome! You can now start creating reports and dashboards.',
                        metadata={'user_type': instance.user_type}
                    )
            
            # Check if role changed
            if hasattr(instance, '_role_changed'):
                role_change = instance._role_changed
                
                # Send role change notification
                if NOTIFICATION_SERVICE_AVAILABLE:
                    notification_service.notify_role_change(
                        instance,
                        role_change['old_role'],
                        role_change['new_role'],
                        instance  # In practice, this would be the user who made the change
                    )
                
                # Clean up the temporary attribute
                delattr(instance, '_role_changed')
        except Exception as e:
            logger.error(f"Error in user_post_save signal: {e}")


    @receiver(post_save, sender=Notification)
    def notification_post_save(sender, instance, created, **kwargs):
        """Handle notification creation"""
        try:
            if created:
                logger.debug(f"Notification created: {instance.title} for {instance.recipient.username}")
                
                # Here you could trigger real-time notifications via WebSocket
                # or push notifications to mobile apps
                
                # Example: Send to WebSocket channel (requires channels)
                # try:
                #     from channels.layers import get_channel_layer
                #     from asgiref.sync import async_to_sync
                #     
                #     channel_layer = get_channel_layer()
                #     async_to_sync(channel_layer.group_send)(
                #         f"user_{instance.recipient.id}",
                #         {
                #             "type": "notification_message",
                #             "notification": {
                #                 "id": str(instance.id),
                #                 "title": instance.title,
                #                 "message": instance.message,
                #                 "type": instance.type,
                #                 "created_at": instance.created_at.isoformat()
                #             }
                #         }
                #     )
                # except ImportError:
                #     pass  # Channels not available
        except Exception as e:
            logger.error(f"Error in notification_post_save signal: {e}")


# Custom signal for data freshness monitoring
from django.dispatch import Signal

data_freshness_check = Signal()

@receiver(data_freshness_check)
def handle_data_freshness_check(sender, connection_id, table_name, last_update, **kwargs):
    """Handle data freshness monitoring"""
    try:
        logger.info(f"Data freshness check for {table_name} in connection {connection_id}")
        
        # Calculate time since last update
        time_since_update = timezone.now() - last_update
        
        # If data is older than threshold (e.g., 24 hours), send alert
        if time_since_update.total_seconds() > 86400:  # 24 hours
            if MODELS_AVAILABLE and NOTIFICATION_SERVICE_AVAILABLE:
                try:
                    connection = ExternalConnection.objects.get(id=connection_id)
                    
                    notification_service.create_notification(
                        'data_freshness_warning',
                        connection.owner,
                        title='Stale Data Alert',
                        message=f'Table {table_name} has not been updated for {time_since_update.days} days',
                        metadata={
                            'connection_id': connection_id,
                            'table_name': table_name,
                            'hours_since_update': time_since_update.total_seconds() / 3600
                        }
                    )
                except ExternalConnection.DoesNotExist:
                    logger.warning(f"Connection {connection_id} not found for freshness check")
    except Exception as e:
        logger.error(f"Error in handle_data_freshness_check signal: {e}")


# Performance monitoring signals
query_executed = Signal()

@receiver(query_executed)
def handle_query_executed(sender, query, execution_time, user, **kwargs):
    """Handle query execution monitoring"""
    try:
        # Log slow queries
        if execution_time > 5.0:  # Queries taking more than 5 seconds
            logger.warning(f"Slow query detected: {execution_time:.2f}s for user {user.username}")
            
            # Could send notification to admins about slow queries
            if MODELS_AVAILABLE and NOTIFICATION_SERVICE_AVAILABLE:
                admin_users = User.objects.filter(user_type='Admin')
                for admin in admin_users:
                    notification_service.create_notification(
                        'performance_warning',
                        admin,
                        title='Slow Query Detected',
                        message=f'Query took {execution_time:.2f} seconds to execute',
                        metadata={
                            'execution_time': execution_time,
                            'user': user.username,
                            'query_preview': query[:100] + '...' if len(query) > 100 else query
                        }
                    )
    except Exception as e:
        logger.error(f"Error in handle_query_executed signal: {e}")


# Security monitoring signals
security_event = Signal()

@receiver(security_event)
def handle_security_event(sender, event_type, user, details, **kwargs):
    """Handle security events"""
    try:
        logger.warning(f"Security event: {event_type} for user {user.username}")
        
        # Send security alert
        if NOTIFICATION_SERVICE_AVAILABLE:
            notification_service.notify_security_alert(
                user,
                event_type,
                details
            )
        
        # Notify admins for critical events
        if event_type in ['multiple_failed_logins', 'suspicious_activity'] and MODELS_AVAILABLE:
            admin_users = User.objects.filter(user_type='Admin')
            for admin in admin_users:
                if NOTIFICATION_SERVICE_AVAILABLE:
                    notification_service.create_notification(
                        'security_alert',
                        admin,
                        title=f'Security Alert: {event_type}',
                        message=f'Security event for user {user.username}: {details}',
                        metadata={
                            'event_type': event_type,
                            'affected_user': user.username,
                            'details': details
                        }
                    )
    except Exception as e:
        logger.error(f"Error in handle_security_event signal: {e}")

    else:
        logger.warning("Models not available - signals not registered")

@receiver(post_save, sender=Dashboard)
def create_dashboard_version_on_save(sender, instance, created, **kwargs):
    """
    Creates a new version history snapshot whenever a dashboard's
    v2 config is meaningfully updated.
    """
    try:
        # Avoid creating history for newly created, empty dashboards
        if created and not instance.config_v2:
            return

        last_version = DashboardVersionHistory.objects.filter(dashboard=instance).first()

        # Check if the config has actually changed since the last version
        if last_version and last_version.config_snapshot == instance.config_v2:
            return # No change, no new version needed

        # Determine the new version number
        new_version_number = (last_version.version_number + 1) if last_version else 1

        DashboardVersionHistory.objects.create(
            dashboard=instance,
            version_number=new_version_number,
            config_snapshot=instance.config_v2,
            saved_by=instance.owner # In a real scenario, you might pass the request user
        )
        logger.info(f"Created version {new_version_number} for dashboard '{instance.title}'")

    except Exception as e:
        logger.error(f"Error in create_dashboard_version_on_save signal: {e}")