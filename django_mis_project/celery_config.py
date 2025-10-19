"""
Celery Configuration for Django MIS Project
Fixed circular import issues with proper naming
"""

import os
from celery import Celery
from django.conf import settings

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'django_mis_project.settings')

# Create celery app with proper naming to avoid circular imports
app = Celery('django_mis_project')

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Load task modules from all registered Django apps.
app.autodiscover_tasks()

# Configure beat schedule
app.conf.beat_schedule = {
    'monitor-connection-health': {
        'task': 'mis_app.tasks.monitor_connection_health',
        'schedule': 300.0,  # Every 5 minutes
    },
    'send-daily-digest': {
        'task': 'mis_app.tasks.send_daily_digest',
        'schedule': 86400.0,  # Daily at midnight
        'options': {'queue': 'notifications'}
    },
    'cleanup-old-data': {
        'task': 'mis_app.tasks.cleanup_old_data',
        'schedule': 604800.0,  # Weekly
        'options': {'queue': 'maintenance'}
    },
    'generate-performance-insights': {
        'task': 'mis_app.tasks.generate_performance_insights',
        'schedule': 604800.0,  # Weekly
        'options': {'queue': 'analytics'}
    },
}

# Task routing configuration
app.conf.task_routes = {
    # Report tasks
    'mis_app.tasks.execute_scheduled_report': {'queue': 'reports'},
    
    # Dashboard tasks
    'mis_app.tasks.refresh_dashboard_data': {'queue': 'dashboards'},
    
    # Data processing tasks
    'mis_app.tasks.process_data_upload': {'queue': 'data_processing'},
    'mis_app.tasks.detect_data_anomalies': {'queue': 'analytics'},
    
    # Notification tasks
    'mis_app.tasks.send_daily_digest': {'queue': 'notifications'},
    
    # Maintenance tasks
    'mis_app.tasks.cleanup_old_data': {'queue': 'maintenance'},
    'mis_app.tasks.monitor_connection_health': {'queue': 'maintenance'},
    
    # Analytics tasks
    'mis_app.tasks.generate_performance_insights': {'queue': 'analytics'},
}

# Task configuration
app.conf.update(
    # Serialization
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    
    # Time settings
    timezone=settings.TIME_ZONE if hasattr(settings, 'TIME_ZONE') else 'UTC',
    enable_utc=True,
    
    # Task execution settings
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes hard limit
    task_soft_time_limit=25 * 60,  # 25 minutes soft limit
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    
    # Worker settings
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
    worker_disable_rate_limits=False,
    
    # Result backend settings
    result_expires=3600,  # 1 hour
    result_compression='gzip',
    
    # Queue settings
    task_default_queue='default',
    task_default_exchange='default',
    task_default_exchange_type='direct',
    task_default_routing_key='default',
    
    # Monitoring
    worker_send_task_events=True,
    task_send_sent_event=True,
)

# Queue definitions
app.conf.task_create_missing_queues = True

# Custom task base class
from celery import Task

class DatabaseTask(Task):
    """
    Custom task base class that ensures database connections
    are properly closed after task execution
    """
    def __call__(self, *args, **kwargs):
        try:
            return self.run(*args, **kwargs)
        finally:
            from django.db import connection
            connection.close()

# Set default task base class
app.Task = DatabaseTask

@app.task(bind=True)
def debug_task(self):
    """Debug task for testing Celery setup"""
    from django.conf import settings as django_settings
    return {
        'request': str(self.request),
        'settings': {
            'DEBUG': getattr(django_settings, 'DEBUG', None),
            'DATABASES': bool(getattr(django_settings, 'DATABASES', None)),
        }
    }

# Error handling for task failures
from celery.signals import task_failure

@task_failure.connect
def task_failure_handler(sender=None, task_id=None, exception=None, 
                        traceback=None, einfo=None, **kwargs):
    """Handle task failures by logging and potentially notifying admins"""
    import logging
    logger = logging.getLogger(__name__)
    
    logger.error(
        f"Task {sender.name} (ID: {task_id}) failed with exception: {exception}",
        exc_info=einfo
    )
    
    # Optionally send notifications for critical task failures
    critical_tasks = [
        'mis_app.tasks.monitor_connection_health',
        'mis_app.tasks.execute_scheduled_report',
    ]
    
    if sender.name in critical_tasks:
        # Could send email/notification here
        logger.critical(f"Critical task {sender.name} failed: {exception}")

# Task retry configuration
from celery.exceptions import Retry

def exponential_backoff(task_self, countdown=60, max_retries=3):
    """
    Exponential backoff retry strategy
    """
    retry_count = task_self.request.retries
    if retry_count >= max_retries:
        raise Exception(f"Task failed after {max_retries} retries")
    
    # Exponential backoff: 60s, 180s, 540s, etc.
    backoff = countdown * (3 ** retry_count)
    raise task_self.retry(countdown=int(backoff))

# Health check task
@app.task(bind=True)
def health_check(self):
    """
    Health check task to verify Celery is working
    """
    from django.db import connection
    from django.core.cache import cache
    
    # Check database
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
        db_status = "OK"
    except Exception as e:
        db_status = f"ERROR: {str(e)}"
    
    # Check cache
    try:
        cache.set('celery_health_check', 'OK', 60)
        cache_status = "OK" if cache.get('celery_health_check') == 'OK' else "ERROR"
    except Exception as e:
        cache_status = f"ERROR: {str(e)}"
    
    return {
        'status': 'healthy',
        'timestamp': self.request.id,
        'database': db_status,
        'cache': cache_status,
        'worker_id': self.request.hostname,
    }

# Periodic task to clean up expired results
@app.task
def cleanup_expired_results():
    """Clean up expired task results"""
    try:
        # This would clean up expired results from the result backend
        # Implementation depends on your result backend (Redis, DB, etc.)
        app.backend.cleanup()
        return {'status': 'completed', 'message': 'Expired results cleaned up'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}