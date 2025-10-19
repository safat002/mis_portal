from django.core.management.base import BaseCommand
from django.db import transaction
from mis_app.models import UserGroup, GroupPermission


class Command(BaseCommand):
    help = 'Create default user groups and permissions'

    def handle(self, *args, **options):
        with transaction.atomic():
            # Create Administrators group
            admin_group, created = UserGroup.objects.get_or_create(
                name='Administrators',
                defaults={
                    'description': 'Full system administrators',
                    'color': '#dc3545',
                    'is_system_group': True
                }
            )
            
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'Created group: {admin_group.name}')
                )
            
            # Create other default groups...
            # (Implementation continues...)