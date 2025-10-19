"""
Django Celery Tasks for Background Processing
Handles long-running operations, scheduled tasks, and notifications
"""

import logging
from celery import shared_task
from django.utils import timezone
from django.contrib.auth import get_user_model
from datetime import timedelta

from .models import Dashboard, SavedReport, ExternalConnection, Notification
from .services.report_builder import ReportBuilderService
from .services.dashboard import DashboardService
from .services.notification_service import notification_service
from .services.external_db import ExternalDBService
from .utils import log_user_action

logger = logging.getLogger(__name__)
User = get_user_model()


@shared_task(bind=True, max_retries=3, default_retry_delay=60)
def execute_scheduled_report(self, report_id, user_id, schedule_config=None):
    """Execute a scheduled report and handle notifications"""
    try:
        report = SavedReport.objects.get(id=report_id)
        user = User.objects.get(id=user_id)
        
        logger.info(f"Executing scheduled report {report.report_name} for user {user.username}")
        
        # Initialize report service
        report_service = ReportBuilderService()
        
        # Execute the report
        start_time = timezone.now()
        df, total_rows, error = report_service.build_advanced_report(
            report.report_config, user
        )
        end_time = timezone.now()
        runtime_seconds = (end_time - start_time).total_seconds()
        
        if error:
            # Report execution failed
            logger.error(f"Scheduled report {report.report_name} failed: {error}")
            
            # Send failure notification
            notification_service.notify_report_execution(
                report, user, False, error_message=error
            )
            
            # Retry if not max retries reached
            if self.request.retries < self.max_retries:
                raise self.retry(countdown=60 * (2 ** self.request.retries))
            
            return {'success': False, 'error': error}
        
        # Report execution successful
        logger.info(f"Scheduled report {report.report_name} completed: {total_rows} rows in {runtime_seconds:.1f}s")
        
        # Send success notification
        notification_service.notify_report_execution(
            report, user, True, total_rows, runtime_seconds
        )
        
        # If configured, save or export the results
        if schedule_config:
            export_format = schedule_config.get('export_format')
            if export_format and df is not None:
                # This would integrate with export service
                pass
        
        return {
            'success': True,
            'row_count': total_rows,
            'runtime_seconds': runtime_seconds
        }
        
    except (SavedReport.DoesNotExist, User.DoesNotExist) as e:
        logger.error(f"Scheduled report task failed: {e}")
        return {'success': False, 'error': str(e)}
    
    except Exception as e:
        logger.error(f"Unexpected error in scheduled report: {e}", exc_info=True)
        
        # Retry on unexpected errors
        if self.request.retries < self.max_retries:
            raise self.retry(countdown=60 * (2 ** self.request.retries))
        
        return {'success': False, 'error': str(e)}


@shared_task
def refresh_dashboard_data(dashboard_id, user_id):
    """Refresh all widget data in a dashboard"""
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id)
        user = User.objects.get(id=user_id)
        
        logger.info(f"Refreshing dashboard {dashboard.title}")
        
        # Initialize dashboard service
        dashboard_service = DashboardService()
        
        # Refresh dashboard data
        result = dashboard_service.refresh_dashboard_data(dashboard_id, user)
        
        logger.info(f"Dashboard refresh completed: {result}")
        
        return result
        
    except (Dashboard.DoesNotExist, User.DoesNotExist) as e:
        logger.error(f"Dashboard refresh task failed: {e}")
        return {'success': False, 'error': str(e)}
    
    except Exception as e:
        logger.error(f"Unexpected error in dashboard refresh: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


@shared_task
def monitor_connection_health():
    """Monitor health of all database connections"""
    logger.info("Starting connection health monitoring")
    
    connections = ExternalConnection.objects.filter(is_active=True)
    healthy_count = 0
    unhealthy_count = 0
    
    for connection in connections:
        try:
            db_service = ExternalDBService(str(connection.id))
            is_healthy = db_service.test_connection()
            
            if is_healthy:
                healthy_count += 1
                # Update last successful check
                connection.last_health_check = timezone.now()
                connection.health_status = 'healthy'
            else:
                unhealthy_count += 1
                connection.health_status = 'unhealthy'
                
                # Notify connection owner
                notification_service.notify_connection_health(
                    connection, 'unhealthy', 'Connection test failed'
                )
            
            connection.save()
            
        except Exception as e:
            logger.error(f"Error checking connection {connection.nickname}: {e}")
            unhealthy_count += 1
            
            connection.health_status = 'error'
            connection.save()
            
            # Notify connection owner
            notification_service.notify_connection_health(
                connection, 'error', str(e)
            )
    
    logger.info(f"Connection health check completed: {healthy_count} healthy, {unhealthy_count} unhealthy")
    
    return {
        'healthy_connections': healthy_count,
        'unhealthy_connections': unhealthy_count,
        'total_connections': len(connections)
    }


@shared_task
def detect_data_anomalies(connection_id, table_name, user_id):
    """Detect anomalies in data and send notifications"""
    try:
        connection = ExternalConnection.objects.get(id=connection_id)
        user = User.objects.get(id=user_id)
        
        logger.info(f"Detecting anomalies in {table_name} for connection {connection.nickname}")
        
        db_service = ExternalDBService(connection_id)
        
        # Get recent data (last 24 hours worth)
        # This is a simplified anomaly detection - in practice you'd use
        # more sophisticated statistical methods
        query = f"""
        SELECT COUNT(*) as row_count, 
               AVG(CASE WHEN numeric_column IS NOT NULL THEN numeric_column END) as avg_value
        FROM {table_name} 
        WHERE created_at >= NOW() - INTERVAL '24 hours'
        """
        
        result = db_service.execute_query(query)
        
        if result['success'] and result['data']:
            current_metrics = result['data'][0]
            
            # Compare with historical averages (simplified)
            # In practice, you'd implement proper statistical analysis
            
            # For demo purposes, detect if row count is significantly different
            if current_metrics['row_count'] == 0:
                # No data in last 24 hours - potential issue
                notification_service.create_notification(
                    'anomaly_detected',
                    user,
                    title='Data Anomaly Detected',
                    message=f'No new data in {table_name} for the last 24 hours',
                    metadata={
                        'table_name': table_name,
                        'connection_id': connection_id,
                        'anomaly_type': 'no_data'
                    }
                )
        
        return {'success': True, 'table': table_name}
        
    except Exception as e:
        logger.error(f"Error detecting anomalies: {e}", exc_info=True)
        return {'success': False, 'error': str(e)}


@shared_task
def send_daily_digest():
    """Send daily digest notifications to users"""
    logger.info("Generating daily digest notifications")
    
    users = User.objects.filter(is_active=True)
    sent_count = 0
    
    for user in users:
        try:
            # Get user's notifications from last 24 hours
            yesterday = timezone.now() - timedelta(days=1)
            notifications = Notification.objects.filter(
                recipient=user,
                created_at__gte=yesterday,
                is_read=False
            ).order_by('-created_at')
            
            if notifications.exists():
                # Group notifications by type
                notification_groups = {}
                for notification in notifications:
                    if notification.type not in notification_groups:
                        notification_groups[notification.type] = []
                    notification_groups[notification.type].append(notification)
                
                # Create digest message
                digest_content = "Daily Activity Digest:\n\n"
                for notification_type, group_notifications in notification_groups.items():
                    digest_content += f"• {len(group_notifications)} {notification_type.replace('_', ' ').title()} notifications\n"
                
                # Send digest notification
                notification_service.create_notification(
                    'daily_digest',
                    user,
                    title='Daily Activity Digest',
                    message=digest_content,
                    metadata={
                        'notification_count': len(notifications),
                        'groups': list(notification_groups.keys())
                    }
                )
                
                sent_count += 1
                
        except Exception as e:
            logger.error(f"Error sending digest to user {user.username}: {e}")
    
    logger.info(f"Daily digest sent to {sent_count} users")
    return {'sent_count': sent_count}


@shared_task
def cleanup_old_data():
    """Clean up old data and logs"""
    logger.info("Starting data cleanup")
    
    # Clean up old notifications (older than 30 days)
    thirty_days_ago = timezone.now() - timedelta(days=30)
    old_notifications = Notification.objects.filter(created_at__lt=thirty_days_ago)
    notifications_deleted = old_notifications.count()
    old_notifications.delete()
    
    # Clean up old audit logs (older than 90 days)
    ninety_days_ago = timezone.now() - timedelta(days=90)
    old_audit_logs = None  # AuditLog.objects.filter(created_at__lt=ninety_days_ago)
    audit_logs_deleted = 0  # old_audit_logs.count() if old_audit_logs else 0
    # old_audit_logs.delete() if old_audit_logs else None
    
    logger.info(f"Cleanup completed: {notifications_deleted} notifications, {audit_logs_deleted} audit logs deleted")
    
    return {
        'notifications_deleted': notifications_deleted,
        'audit_logs_deleted': audit_logs_deleted
    }


@shared_task
def process_data_upload(upload_id, user_id):
    """Process uploaded data file in background"""
    try:
        # This would process a file upload
        # Implementation depends on your file upload model
        logger.info(f"Processing data upload {upload_id}")
        
        user = User.objects.get(id=user_id)
        
        # Simulate processing
        # In practice, this would:
        # 1. Read the uploaded file
        # 2. Validate data format
        # 3. Clean and transform data
        # 4. Insert into database
        # 5. Update upload status
        # 6. Send notification
        
        # For demo, just send success notification
        notification_service.notify_data_upload(
            user, 
            f"upload_{upload_id}.csv", 
            True, 
            row_count=1000
        )
        
        return {'success': True, 'upload_id': upload_id}
        
    except Exception as e:
        logger.error(f"Error processing upload {upload_id}: {e}")
        
        # Send failure notification
        try:
            user = User.objects.get(id=user_id)
            notification_service.notify_data_upload(
                user, 
                f"upload_{upload_id}.csv", 
                False, 
                error_message=str(e)
            )
        except:
            pass
        
        return {'success': False, 'error': str(e)}


@shared_task
def generate_performance_insights():
    """Generate performance insights and recommendations"""
    logger.info("Generating performance insights")
    
    try:
        # Analyze system performance metrics
        insights = {
            'slow_reports': [],
            'underused_dashboards': [],
            'connection_issues': [],
            'recommendations': []
        }
        
        # Find slow reports (this would analyze actual execution times)
        # For demo purposes, just return sample insights
        insights['recommendations'] = [
            "Consider adding indexes to frequently queried tables",
            "Some dashboards haven't been accessed in 30 days",
            "Database connection pool could be optimized"
        ]
        
        # Notify admins with insights
        admin_users = User.objects.filter(user_type='Admin')
        for admin in admin_users:
            notification_service.create_notification(
                'performance_insights',
                admin,
                title='Weekly Performance Insights',
                message=f"Performance analysis complete. {len(insights['recommendations'])} recommendations available.",
                metadata=insights
            )
        
        return insights
        
    except Exception as e:
        logger.error(f"Error generating performance insights: {e}")
        return {'success': False, 'error': str(e)}


# Periodic tasks configuration (for celery beat)
# This would go in your celery.py or settings.py
CELERY_BEAT_SCHEDULE = {
    'monitor-connection-health': {
        'task': 'mis_app.tasks.monitor_connection_health',
        'schedule': 300.0,  # Every 5 minutes
    },
    'send-daily-digest': {
        'task': 'mis_app.tasks.send_daily_digest',
        'schedule': 86400.0,  # Daily
    },
    'cleanup-old-data': {
        'task': 'mis_app.tasks.cleanup_old_data',
        'schedule': 604800.0,  # Weekly
    },
    'generate-performance-insights': {
        'task': 'mis_app.tasks.generate_performance_insights',
        'schedule': 604800.0,  # Weekly
    },
}