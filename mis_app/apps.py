"""
Django MIS Application Configuration
Updated to safely load signals after models are ready
"""

from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class MisAppConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'mis_app'
    verbose_name = 'Management Information System'

    def ready(self):
        """
        Import signals once the app is ready
        This ensures models are loaded before signals try to import them
        """
        try:
            from . import signals  # noqa
            logger.info("MIS app signals loaded successfully")
        except ImportError as e:
            logger.warning(f"Could not import signals: {e}")
            # During initial migrations, models might not exist yet
            # This is expected behavior
    pass