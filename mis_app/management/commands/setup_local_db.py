from django.core.management.base import BaseCommand
from mis_app.models import ExternalConnection
from django.contrib.auth import get_user_model

User = get_user_model()

class Command(BaseCommand):
    help = 'Sets up a local SQLite database connection for testing'
    
    def handle(self, *args, **options):
        # Get the first user
        user = User.objects.first()
        if not user:
            self.stdout.write(self.style.ERROR('No users found. Please create a user first.'))
            return
        
        # Create a local SQLite connection
        conn, created = ExternalConnection.objects.get_or_create(
            nickname='Local SQLite Database',
            owner=user,
            defaults={
                'db_type': 'sqlite',
                'filepath': 'local_data.db',
                'health_status': 'healthy'
            }
        )
        
        if created:
            self.stdout.write(self.style.SUCCESS('Created local SQLite database connection'))
        else:
            self.stdout.write(self.style.SUCCESS('Local SQLite database connection already exists'))