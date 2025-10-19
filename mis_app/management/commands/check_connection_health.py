# management/commands/check_connection_health.py
from django.core.management.base import BaseCommand
from mis_app.models import ExternalConnection
from mis_app.utils import get_external_engine
from django.utils import timezone
from sqlalchemy import text

class Command(BaseCommand):
    help = 'Check health of all database connections'
    
    def handle(self, *args, **options):
        for connection in ExternalConnection.objects.filter(is_active=True):
            try:
                engine = get_external_engine(connection.id, connection.owner)
                
                if not engine:
                    connection.health_status = 'error'
                    connection.save(update_fields=['health_status'])
                    continue
                
                # Test the connection
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
                
                connection.health_status = 'healthy'
                connection.last_health_check = timezone.now()
                connection.save(update_fields=['health_status', 'last_health_check'])
                
                self.stdout.write(self.style.SUCCESS(f"{connection.nickname}: Healthy"))
                
            except Exception as e:
                connection.health_status = 'error'
                connection.last_health_check = timezone.now()
                connection.save(update_fields=['health_status', 'last_health_check'])
                
                self.stdout.write(self.style.ERROR(f"{connection.nickname}: Error - {str(e)}"))