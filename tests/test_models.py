# tests/test_models.py

from django.test import TestCase
from django.contrib.auth import get_user_model
from mis_app.models import UserGroup, GroupPermission, UserPermission, ExternalConnection

User = get_user_model()

class PermissionModelTests(TestCase):

    def setUp(self):
        self.user = User.objects.create_user(username='testuser', email='test@example.com', password='password', user_type='User')
        self.admin_user = User.objects.create_user(username='adminuser', email='admin@example.com', password='password', user_type='Admin')
        self.group = UserGroup.objects.create(name='Test Group', description='A group for testing')
        self.connection = ExternalConnection.objects.create(owner=self.admin_user, nickname='Test Connection')

    def test_user_model(self):
        self.assertEqual(self.user.user_type, 'User')
        self.assertEqual(self.admin_user.user_type, 'Admin')

    def test_user_group_model(self):
        self.assertEqual(self.group.name, 'Test Group')
        self.group.users.add(self.user)
        self.assertEqual(self.group.users.count(), 1)

    def test_group_permission_model(self):
        permission = GroupPermission.objects.create(
            group=self.group,
            resource_type='connection',
            resource_id=str(self.connection.id),
            resource_name=self.connection.nickname,
            permission_level='view'
        )
        self.assertEqual(permission.group, self.group)
        self.assertEqual(permission.permission_level, 'view')

    def test_user_permission_model(self):
        permission = UserPermission.objects.create(
            user=self.user,
            resource_type='connection',
            resource_id=str(self.connection.id),
            resource_name=self.connection.nickname,
            permission_level='edit'
        )
        self.assertEqual(permission.user, self.user)
        self.assertEqual(permission.permission_level, 'edit')
