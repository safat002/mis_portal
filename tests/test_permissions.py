
import pytest
from django.test import TestCase
from django.contrib.auth import get_user_model
from mis_app.models import UserGroup, GroupPermission, ExternalConnection, UploadedTable
User = get_user_model()

class TestPermissions(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', password='password')
        self.group = UserGroup.objects.create(name='Test Group')
        self.group.users.add(self.user)

        self.connection1 = ExternalConnection.objects.create(owner=self.user, nickname='Conn1', db_type='sqlite')
        self.table1 = UploadedTable.objects.create(uploaded_by=self.user, connection=self.connection1, table_name='Table1')

        self.connection2 = ExternalConnection.objects.create(owner=self.user, nickname='Conn2', db_type='sqlite')
        self.table2 = UploadedTable.objects.create(uploaded_by=self.user, connection=self.connection2, table_name='Table2')

        # Grant permission to table1 only
        GroupPermission.objects.create(
            group=self.group,
            resource_type='table',
            resource_name='Table1',
            permission_level='view'
        )

    def test_user_only_sees_permitted_connections_and_tables(self):
        # This test should fail until the permission logic is implemented
        
        # TODO: Import the permission functions once they are created
        # from mis_app.permissions import get_permitted_connections, get_permitted_tables

        # permitted_connections = get_permitted_connections(self.user)
        # self.assertEqual(len(permitted_connections), 1)
        # self.assertEqual(permitted_connections[0].nickname, 'Conn1')

        # permitted_tables = get_permitted_tables(self.user, self.connection1)
        # self.assertEqual(len(permitted_tables), 1)
        # self.assertEqual(permitted_tables[0].table_name, 'Table1')

        # permitted_tables_for_conn2 = get_permitted_tables(self.user, self.connection2)
        # self.assertEqual(len(permitted_tables_for_conn2), 0)
        
        self.fail("Permissions logic not yet implemented.")

    def test_user_with_no_permissions_sees_no_connections(self):
        unprivileged_user = User.objects.create_user(username='no_perms', password='password')
        # TODO: Import the permission functions once they are created
        # from mis_app.permissions import get_permitted_connections

        # permitted_connections = get_permitted_connections(unprivileged_user)
        # self.assertEqual(len(permitted_connections), 0)

        self.fail("Permissions logic not yet implemented.")