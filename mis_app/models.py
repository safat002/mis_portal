# mis_app/models.py

"""
Enhanced Django Models for MIS Application
Complete user management, permissions, and multi-database support
"""

import os
import uuid
import json
from urllib.parse import quote_plus
from django.contrib.auth.models import AbstractUser
from django.db import models
from django.utils import timezone
from django.core.validators import EmailValidator, RegexValidator
from django.core.exceptions import ValidationError
from django.contrib.contenttypes.models import ContentType
from django.contrib.contenttypes.fields import GenericForeignKey
from django.conf import settings


class User(AbstractUser):
    """Enhanced User model with comprehensive features"""
    
    USER_TYPE_CHOICES = [
        ('Admin', 'Administrator'),
        ('Moderator', 'Moderator'),
        ('Uploader', 'Uploader'),
        ('User', 'Regular User'),
        ('Viewer', 'Read-Only User'),
    ]
    
    THEME_CHOICES = [
        ('light', 'Light Theme'),
        ('dark', 'Dark Theme'),
        ('corporate', 'Corporate Theme'),
        ('ocean_blue', 'Ocean Blue'),
        ('royal_purple', 'Royal Purple'),
        ('sunset_orange', 'Sunset Orange'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    email = models.EmailField(unique=True, validators=[EmailValidator()])
    user_type = models.CharField(max_length=20, choices=USER_TYPE_CHOICES, default='User')
    
    # Profile fields
    phone_number = models.CharField(
        max_length=20, 
        blank=True, 
        validators=[RegexValidator(regex=r'^\+?1?\d{9,15}$', message="Enter a valid phone number")]
    )
    department = models.CharField(max_length=100, blank=True)
    job_title = models.CharField(max_length=100, blank=True)
    manager = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='subordinates')
    
    # Preferences
    theme_preference = models.CharField(max_length=20, choices=THEME_CHOICES, default='corporate')
    user_timezone = models.CharField(max_length=50, default='UTC')
    language = models.CharField(max_length=10, default='en')
    default_database = models.ForeignKey('ExternalConnection', on_delete=models.SET_NULL, null=True, blank=True, help_text='Default database connection')
    
    # Pinned items (stored as JSON lists of UUIDs)
    pinned_dashboards = models.JSONField(default=list, blank=True)
    pinned_reports = models.JSONField(default=list, blank=True)
    
    # Status tracking
    is_email_verified = models.BooleanField(default=False)
    email_verification_token = models.CharField(max_length=64, blank=True)
    password_reset_token = models.CharField(max_length=64, blank=True)
    password_reset_expires = models.DateTimeField(null=True, blank=True)
    last_password_change = models.DateTimeField(default=timezone.now)
    
    # Login tracking
    last_activity = models.DateTimeField(null=True, blank=True)
    login_attempts = models.IntegerField(default=0)
    locked_until = models.DateTimeField(null=True, blank=True)
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, blank=True, related_name='created_users')
    
    class Meta:
        db_table = 'users'
        ordering = ['-date_joined']
        indexes = [
            models.Index(fields=['user_type']),
            models.Index(fields=['department']),
            models.Index(fields=['is_active', 'user_type']),
            models.Index(fields=['email']),
        ]
    
    def __str__(self):
        return f"{self.username} ({self.get_user_type_display()})"
    
    def get_full_name(self):
        """Return the first_name plus the last_name, with a space in between."""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.username
    
    def can_manage_users(self):
        """Check if user can manage other users (Point 6)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator']
    
    def is_admin_level(self):
        """Check if user is an Admin or superuser."""
        return self.is_superuser or self.user_type == 'Admin'
    
    def can_access_admin(self):
        """Check if user can access admin interface"""
        return self.user_type == 'Admin' or self.is_staff
    
    def can_manage_database(self):
        """Check if user can access the Database Management page (Point 1)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator']
    
    def can_view_data_management(self):
        """Check if user can view the Data Management page (Point 5.1)"""
        # All authenticated users can view the page, but actions are restricted.
        return self.is_authenticated
    
    def can_upload_data(self):
        """Check if user can upload new files/data (Point 5.2)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator', 'Uploader']
    
    def can_modify_schema(self):
        """Check if user can add/rename/drop tables and columns (Point 5.3)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator']
    
    def can_delete_rows(self):
        """Check if user can delete rows from tables (Point 5.3)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator']
    
    def can_truncate_tables(self):
        """Check if user can truncate entire tables (admin-only)."""
        return self.is_superuser or self.user_type == 'Admin'

    def can_drop_tables(self):
        """Check if user can drop tables (admin-only)."""
        return self.is_superuser or self.user_type == 'Admin'

    def can_view_data_model(self):
        """Check if user can view the data model designer (Point 7)"""
        # All authenticated users can view and arrange the data model
        return self.is_authenticated
    
    def can_save_data_model(self):
        """Check if user can save changes to the data model (Point 7)"""
        return self.is_superuser or self.user_type in ['Admin', 'Moderator']
    
    def is_account_locked(self):
        """Check if account is temporarily locked"""
        if self.locked_until:
            return timezone.now() < self.locked_until
        return False
    
    def reset_login_attempts(self):
        """Reset failed login attempts"""
        self.login_attempts = 0
        self.locked_until = None
        self.save(update_fields=['login_attempts', 'locked_until'])
    
    def increment_login_attempts(self):
        """Increment failed login attempts and lock if necessary"""
        self.login_attempts += 1
        if self.login_attempts >= 5:  # Lock after 5 failed attempts
            self.locked_until = timezone.now() + timezone.timedelta(minutes=30)
        self.save(update_fields=['login_attempts', 'locked_until'])


class UserGroup(models.Model):
    """User groups for permission management"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    name = models.CharField(max_length=100, unique=True)
    description = models.TextField(blank=True)
    color = models.CharField(max_length=7, default='#007bff', help_text='Hex color code for UI display')

    # Group settings
    is_active = models.BooleanField(default=True)
    is_system_group = models.BooleanField(default=False, help_text='System groups cannot be deleted')

    # Members
    users = models.ManyToManyField(
        User,
        through='GroupMembership',
        through_fields=('group', 'user'),
        related_name='user_groups'
    )

    # Hierarchy
    parent_group = models.ForeignKey(
        'self',
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='child_groups'
    )

    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    created_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_groups'
    )

    class Meta:
        db_table = 'user_groups'
        ordering = ['name']

    def __str__(self):
        return self.name
    
    def get_all_users(self):
        """Get all users including those from child groups"""
        user_ids = set(self.users.values_list('id', flat=True))
        for child_group in self.child_groups.all():
            user_ids.update(child_group.get_all_users().values_list('id', flat=True))
        return User.objects.filter(id__in=user_ids)


class GroupMembership(models.Model):
    """Through model for User-Group relationship with additional fields"""
    
    ROLE_CHOICES = [
        ('member', 'Member'),
        ('admin', 'Group Admin'),
        ('owner', 'Group Owner'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE)
    
    # Membership details
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='member')
    joined_at = models.DateTimeField(auto_now_add=True)
    added_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='added_memberships')
    
    class Meta:
        db_table = 'group_memberships'
        unique_together = ['user', 'group']
        ordering = ['-joined_at']
    
    def __str__(self):
        return f"{self.user.username} in {self.group.name}"


class GroupPermission(models.Model):
    """Permissions assigned to groups"""
    
    PERMISSION_LEVELS = [
        ('none', 'No Access'),
        ('view', 'View Only'),
        ('edit', 'Edit'),
        ('admin', 'Full Admin'),
    ]
    
    RESOURCE_TYPES = [
        ('connection', 'Database Connection'),
        ('table', 'Database Table'),
        ('report', 'Report'),
        ('dashboard', 'Dashboard'),
        ('system', 'System Function'),
        ('data_model', 'Data Model'),
        ('data_management', 'Data Management'),
        ('database_management', 'Database Management'),
        ('user_management', 'User Management'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    group = models.ForeignKey(UserGroup, on_delete=models.CASCADE, related_name='permissions')
    
    # Resource identification
    resource_type = models.CharField(max_length=30, choices=RESOURCE_TYPES)
    resource_id = models.CharField(max_length=255, blank=True, help_text='ID of specific resource (optional)')
    resource_name = models.CharField(max_length=255, help_text='Name/identifier of the resource')
    
    # Permission level
    permission_level = models.CharField(max_length=10, choices=PERMISSION_LEVELS, default='view')
    
    # Additional settings
    conditions = models.JSONField(default=dict, blank=True, help_text='Additional permission conditions')
    is_inherited = models.BooleanField(default=False, help_text='Inherited from parent group')
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    granted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True)
    
    class Meta:
        db_table = 'group_permissions'
        unique_together = ['group', 'resource_type', 'resource_name']
        indexes = [
            models.Index(fields=['resource_type', 'resource_name']),
            models.Index(fields=['permission_level']),
        ]
    
    def __str__(self):
        return f"{self.group.name}: {self.permission_level} on {self.resource_type} {self.resource_name}"


class UserPermission(models.Model):
    """Direct permissions assigned to individual users (overrides group permissions)"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='direct_permissions')
    
    # Resource identification
    resource_type = models.CharField(max_length=30, choices=GroupPermission.RESOURCE_TYPES)
    resource_id = models.CharField(max_length=255, blank=True)
    resource_name = models.CharField(max_length=255)
    
    # Permission level
    permission_level = models.CharField(max_length=10, choices=GroupPermission.PERMISSION_LEVELS, default='view')
    
    # Additional settings
    conditions = models.JSONField(default=dict, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True, help_text='Optional expiration date')
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    granted_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='granted_permissions')
    
    class Meta:
        db_table = 'user_permissions'
        unique_together = ['user', 'resource_type', 'resource_name']
    
    def __str__(self):
        return f"{self.user.username}: {self.permission_level} on {self.resource_type} {self.resource_name}"
    
    def is_expired(self):
        """Check if permission is expired"""
        if self.expires_at:
            return timezone.now() > self.expires_at
        return False


class ExternalConnection(models.Model):
    """Database connections for external data sources"""
    
    DB_TYPE_CHOICES = [
        ('postgresql', 'PostgreSQL'),
        ('mysql', 'MySQL'),
        ('sqlite', 'SQLite'),
        ('mssql', 'SQL Server'),
        ('oracle', 'Oracle'),
        ('snowflake', 'Snowflake'),
    ]
    
    HEALTH_STATUS_CHOICES = [
        ('healthy', 'Healthy'),
        ('unhealthy', 'Unhealthy'),
        ('error', 'Error'),
        ('unknown', 'Unknown'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='connections')
    
    # Connection details
    nickname = models.CharField(max_length=100)
    db_type = models.CharField(max_length=20, choices=DB_TYPE_CHOICES)
    host = models.CharField(max_length=255, blank=True)
    port = models.CharField(max_length=10, blank=True)
    username = models.CharField(max_length=100, blank=True)
    password = models.CharField(max_length=255, blank=True)  # Should be encrypted in production
    db_name = models.CharField(max_length=100, blank=True)
    schema = models.CharField(max_length=100, blank=True)
    filepath = models.CharField(max_length=500, blank=True)  # For SQLite
    
    # Settings
    is_default = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)
    is_internal = models.BooleanField(default=False, help_text='Internal SQLite database')
    hidden_tables = models.TextField(blank=True, help_text='Comma-separated list of tables to hide')
    
    # Health monitoring
    health_status = models.CharField(max_length=20, choices=HEALTH_STATUS_CHOICES, default='unknown')
    last_health_check = models.DateTimeField(null=True, blank=True)
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'external_connections'
        unique_together = ['owner', 'nickname']
        ordering = ['nickname']
        indexes = [
            models.Index(fields=['owner', 'is_active']),
            models.Index(fields=['is_default']),
        ]
    
    def __str__(self):
        return f"{self.nickname} ({self.db_type})"

    def get_connection_config(self) -> dict:
        """Return configuration details for establishing a connection."""
        engine = (self.db_type or "").lower()
        host = self.host or "localhost"
        port = self.port
        if not port:
            if engine == "postgresql":
                port = "5432"
            elif engine == "mysql":
                port = "3306"

        return {
            "engine": engine,
            "host": host,
            "port": str(port) if port else "",
            "user": self.username or "",
            "password": self.password or "",
            "database": self.db_name or "",
            "schema": self.schema or "",
            "filepath": self.filepath or "",
        }

    def get_connection_uri(self) -> str:
        """Build a SQLAlchemy-compatible connection URI."""
        config = self.get_connection_config()
        engine = config["engine"]

        if engine == "sqlite":
            db_path = config["filepath"] or config["database"]
            if not db_path:
                raise ValueError("SQLite connections require a filepath or database name.")

            if not os.path.isabs(db_path):
                db_path = os.path.join(settings.BASE_DIR, db_path)

            return f"sqlite:///{db_path}"

        if engine not in {"postgresql", "mysql"}:
            raise ValueError(f"Unsupported database engine: {engine}")

        user = quote_plus(config["user"]) if config["user"] else ""
        password = quote_plus(config["password"]) if config["password"] else ""

        auth = ""
        if user:
            auth = user
            if password:
                auth += f":{password}"
            auth += "@"

        host = config["host"] or "localhost"
        port = f":{config['port']}" if config["port"] else ""

        database = config["database"]
        if not database:
            raise ValueError("Database name is required for external connections.")

        driver = "postgresql"
        if engine == "mysql":
            driver = "mysql+pymysql"

        return f"{driver}://{auth}{host}{port}/{database}"


class UploadedTable(models.Model):
    """Track tables uploaded by users"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    uploaded_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='uploaded_tables')
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE)
    
    # Table details
    table_name = models.CharField(max_length=255)
    original_filename = models.CharField(max_length=255, blank=True)
    row_count = models.IntegerField(default=0)
    column_count = models.IntegerField(default=0)
    file_size = models.BigIntegerField(default=0, help_text='Original file size in bytes')
    
    # Permissions
    is_public = models.BooleanField(default=False)
    allowed_groups = models.ManyToManyField(UserGroup, blank=True, related_name='accessible_tables')
    
    # Metadata
    upload_date = models.DateTimeField(auto_now_add=True)
    last_modified = models.DateTimeField(auto_now=True)
    description = models.TextField(blank=True)
    tags = models.JSONField(default=list, blank=True)
    
    class Meta:
        db_table = 'uploaded_tables'
        unique_together = ['connection', 'table_name']
        ordering = ['-upload_date']
        indexes = [
            models.Index(fields=['uploaded_by', '-upload_date']),
            models.Index(fields=['connection', 'table_name']),
        ]
    
    def __str__(self):
        return f"{self.table_name} (uploaded by {self.uploaded_by.username})"


class SavedReport(models.Model):
    """User-saved reports with configurations"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='saved_reports')
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE, null=True, blank=True)

    # Report details
    report_name = models.CharField(max_length=200)
    description = models.TextField(blank=True)

    # Configuration
    report_config = models.JSONField(default=dict)
    pivot_config = models.JSONField(default=dict, blank=True)
    data_prep_recipe = models.JSONField(default=list, blank=True)

    # Sharing
    shared_with = models.ManyToManyField(
        User,
        through='ReportShare',
        through_fields=('report', 'user'),
        related_name='shared_reports'
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_accessed = models.DateTimeField(null=True, blank=True)
    access_count = models.IntegerField(default=0)

    class Meta:
        db_table = 'saved_reports'
        unique_together = ['owner', 'report_name']
        ordering = ['-updated_at']
        indexes = [
            models.Index(fields=['owner', '-updated_at']),
            models.Index(fields=['connection']),
        ]

    def __str__(self):
        return f"{self.report_name} (by {self.owner.username})"


class ReportShare(models.Model):
    """Sharing permissions for reports"""
    
    PERMISSION_CHOICES = [
        ('view', 'View Only'),
        ('edit', 'Can Edit'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    report = models.ForeignKey(SavedReport, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    permission = models.CharField(max_length=10, choices=PERMISSION_CHOICES, default='view')
    shared_at = models.DateTimeField(auto_now_add=True)
    shared_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='shared_reports_by')
    
    class Meta:
        db_table = 'report_shares'
        unique_together = ['report', 'user']
    
    def __str__(self):
        return f"{self.report.report_name} shared with {self.user.username}"


class Dashboard(models.Model):
    """User dashboards"""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE, related_name='dashboards')

    # Dashboard details
    title = models.CharField(max_length=200)
    description = models.TextField(blank=True)

    # Configuration
    config = models.JSONField(default=dict)
    config_v2 = models.JSONField(default=dict, blank=True)
    config_version = models.CharField(max_length=10, default='v1')

    # Settings
    is_public = models.BooleanField(default=False)

    # Sharing
    shared_with = models.ManyToManyField(
        User,
        through='DashboardShare',
        through_fields=('dashboard', 'user'),
        related_name='shared_dashboards'
    )

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_accessed = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'dashboards'
        ordering = ['-updated_at']
        indexes = [
            models.Index(fields=['owner', '-updated_at']),
            models.Index(fields=['is_public']),
        ]

    def __str__(self):
        return f"{self.title} (by {self.owner.username})"


class DashboardShare(models.Model):
    """Sharing permissions for dashboards"""
    
    PERMISSION_CHOICES = [
        ('view', 'View Only'),
        ('edit', 'Can Edit'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    permission = models.CharField(max_length=10, choices=PERMISSION_CHOICES, default='view')
    shared_at = models.DateTimeField(auto_now_add=True)
    shared_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name='shared_dashboards_by')
    
    class Meta:
        db_table = 'dashboard_shares'
        unique_together = ['dashboard', 'user']
    
    def __str__(self):
        return f"{self.dashboard.title} shared with {self.user.username}"


class Widget(models.Model):
    """Dashboard widgets"""
    
    WIDGET_TYPES = [
        ('chart', 'Chart'),
        ('table', 'Table'),
        ('metric', 'Metric'),
        ('text', 'Text'),
        ('filter', 'Filter'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(Dashboard, on_delete=models.CASCADE, related_name='widgets')
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    
    # Widget details
    title = models.CharField(max_length=200)
    type = models.CharField(max_length=20, choices=WIDGET_TYPES)
    config = models.JSONField(default=dict)
    
    # Layout
    position_x = models.IntegerField(default=0)
    position_y = models.IntegerField(default=0)
    width = models.IntegerField(default=4)
    height = models.IntegerField(default=4)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'widgets'
        ordering = ['position_y', 'position_x']
    
    def __str__(self):
        return f"{self.title} ({self.type})"


class Notification(models.Model):
    """User notifications"""
    
    NOTIFICATION_TYPES = [
        ('info', 'Information'),
        ('warning', 'Warning'),
        ('error', 'Error'),
        ('success', 'Success'),
        ('system', 'System'),
    ]
    
    PRIORITY_CHOICES = [
        ('low', 'Low'),
        ('normal', 'Normal'),
        ('high', 'High'),
        ('urgent', 'Urgent'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    recipient = models.ForeignKey(User, on_delete=models.CASCADE, related_name='notifications')
    
    # Notification content
    title = models.CharField(max_length=200)
    message = models.TextField()
    type = models.CharField(max_length=20, choices=NOTIFICATION_TYPES, default='info')
    priority = models.CharField(max_length=10, choices=PRIORITY_CHOICES, default='normal')
    
    # Status
    is_read = models.BooleanField(default=False)
    read_at = models.DateTimeField(null=True, blank=True)
    
    # Optional action
    action_url = models.URLField(blank=True)
    action_text = models.CharField(max_length=50, blank=True)
    metadata = models.JSONField(default=dict, blank=True)
    related_object_type = models.CharField(max_length=50, blank=True, null=True)
    related_object_id = models.CharField(max_length=255, blank=True, null=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'notifications'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['recipient', 'is_read', '-created_at']),
        ]
    
    def __str__(self):
        return f"{self.title} for {self.recipient.username}"


class AuditLog(models.Model):
    """Comprehensive audit logging for user actions"""
    
    ACTION_TYPES = [
        ('create', 'Create'),
        ('update', 'Update'),
        ('delete', 'Delete'),
        ('login', 'Login'),
        ('logout', 'Logout'),
        ('access', 'Access'),
        ('export', 'Export'),
        ('import', 'Import'),
        ('share', 'Share'),
        ('permission_change', 'Permission Change'),
        ('upload', 'Upload'),
        ('download', 'Download'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # User and session info
    user = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    username = models.CharField(max_length=150, help_text='Cached username in case user is deleted')
    session_id = models.CharField(max_length=40, blank=True)
    
    # Action details
    action = models.CharField(max_length=20, choices=ACTION_TYPES)
    object_type = models.CharField(max_length=50, blank=True)
    object_id = models.CharField(max_length=255, blank=True)
    object_name = models.CharField(max_length=255, blank=True)
    
    # Additional context
    details = models.JSONField(default=dict, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    user_agent = models.TextField(blank=True)
    
    # Changes tracking (for update actions)
    old_values = models.JSONField(default=dict, blank=True)
    new_values = models.JSONField(default=dict, blank=True)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'audit_logs'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['action', '-created_at']),
            models.Index(fields=['object_type', '-created_at']),
            models.Index(fields=['ip_address', '-created_at']),
        ]
    
    def __str__(self):
        return f"{self.username} - {self.action} - {self.object_type} - {self.created_at}"


class ExportHistory(models.Model):
    """Track data exports for audit purposes"""
    
    FORMAT_CHOICES = [
        ('csv', 'CSV'),
        ('excel', 'Excel'),
        ('pdf', 'PDF'),
        ('json', 'JSON'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='exports')
    
    # Export details
    filename = models.CharField(max_length=255)
    format_type = models.CharField(max_length=10, choices=FORMAT_CHOICES)
    row_count = models.IntegerField()
    file_size = models.BigIntegerField(help_text='File size in bytes')
    
    # Source information
    source_type = models.CharField(max_length=50)  # 'report', 'dashboard', 'table', etc.
    source_id = models.CharField(max_length=255, blank=True)
    source_name = models.CharField(max_length=255)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    downloaded_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'export_history'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', '-created_at']),
            models.Index(fields=['format_type']),
        ]
    
    def __str__(self):
        return f"{self.filename} by {self.user.username}"


# Continue with remaining models from your existing models.py...
# (CleanedDataSource, DrillDownPath, ConnectionJoin, CanvasLayout, DashboardDataContext)
class CleanedDataSource(models.Model):
    """Cleaned data sources created by users"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE)
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE)
    
    name = models.CharField(max_length=200)
    view_name = models.CharField(max_length=200)
    
    # Configuration
    config_json = models.JSONField(default=dict)
    recipe_json = models.JSONField(default=list)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'cleaned_data_sources'
        unique_together = ['owner', 'name']
    
    def __str__(self):
        return f"Cleaned: {self.name}"


class DrillDownPath(models.Model):
    """Define drill-down hierarchies for data exploration"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE)
    table_name = models.CharField(max_length=100)
    field_order = models.TextField(help_text='Comma-separated field names in drill-down order')
    
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    
    class Meta:
        db_table = 'drill_down_paths'
        unique_together = ['connection', 'table_name']
    
    def __str__(self):
        return f"Drill-down for {self.table_name}"


class ConnectionJoin(models.Model):
    """Define relationships between tables for automatic joining"""
    
    JOIN_TYPES = [
        ('INNER', 'Inner Join'),
        ('LEFT', 'Left Join'),
        ('RIGHT', 'Right Join'),
        ('FULL', 'Full Outer Join'),
    ]

    CARDINALITY_CHOICES = [
        ('one-to-one', 'One-to-One (1:1)'),
        ('one-to-many', 'One-to-Many (1:N)'),
        ('many-to-one', 'Many-to-One (N:1)'),
        ('many-to-many', 'Many-to-Many (N:M)'),
    ]

    cardinality = models.CharField(max_length=20, choices=CARDINALITY_CHOICES, default='one-to-many')
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE)
    
    # Join definition
    left_table = models.CharField(max_length=100)
    left_column = models.CharField(max_length=100)
    right_table = models.CharField(max_length=100)
    right_column = models.CharField(max_length=100)
    join_type = models.CharField(max_length=10, choices=JOIN_TYPES, default='INNER')
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    
    class Meta:
        db_table = 'connection_joins'
        unique_together = ['connection', 'left_table', 'left_column', 'right_table', 'right_column']
    
    def __str__(self):
        return f"{self.left_table}.{self.left_column} -> {self.right_table}.{self.right_column}"


class CanvasLayout(models.Model):
    """Store canvas layouts for dashboard design (REPURPOSED FOR CONNECTION MODEL)"""
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # FIX: Change ForeignKey from Dashboard to ExternalConnection
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE, related_name='canvas_layouts')
    
    # Add table name for which layout is being saved
    table_name = models.CharField(max_length=255)
    x_pos = models.IntegerField(default=0)
    y_pos = models.IntegerField(default=0)
    collapsed = models.BooleanField(default=False)

    # layout_data = models.JSONField(default=dict) # DELETE this line if it exists

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'canvas_layouts'
        # Add unique constraint to prevent duplicate layouts per connection/table
        unique_together = ['connection', 'table_name'] 
    
    def __str__(self):
        return f"Layout for {self.table_name} on {self.connection.nickname}"


class DashboardDataContext(models.Model):
    """
    Stores the dashboard's data context:
      - connection_id: UUID of the external connection used to query data
      - selected_tables: list of table names chosen for this dashboard
      - joins: list of join definitions between selected_tables
    Backward compatible with legacy `context_data` JSON.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # Keep existing relation
    dashboard = models.OneToOneField(
        'Dashboard',
        on_delete=models.CASCADE,
        related_name='data_context'
    )

    # NEW first-class fields (what your views/readers should use)
    connection_id = models.UUIDField(
        null=True, blank=True, db_index=True,
        help_text="UUID of the external connection configured for this dashboard."
    )
    selected_tables = models.JSONField(
        default=list, blank=True,
        help_text="List of table names selected in the data context."
    )
    joins = models.JSONField(
        default=list, blank=True,
        help_text="List of join specifications among selected tables."
    )
    calculated_fields = models.JSONField(
        default=list, blank=True,
        help_text="List of calculated fields: [{'name', 'formula', 'aggregation'}]."
    )

    # Keep legacy context_data to avoid breaking older code; will be auto-synced
    context_data = models.JSONField(
        default=dict, blank=True,
        help_text="(Legacy) Arbitrary JSON blob for the data context. "
                  "Kept for backward compatibility; new code should use first-class fields."
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'dashboard_data_contexts'
        indexes = [
            models.Index(fields=['dashboard']),
            models.Index(fields=['connection_id']),
        ]
        verbose_name = "Dashboard Data Context"
        verbose_name_plural = "Dashboard Data Contexts"

    def __str__(self):
        title = getattr(self.dashboard, 'title', str(self.dashboard_id))
        return f"Data context for {title}"

    # -------------------------
    # Backward-compat utilities
    # -------------------------

    def normalize_from_legacy(self, save_if_changed: bool = True) -> bool:
        """
        Populate first-class fields from legacy `context_data` if they are empty.
        Returns True if any field was modified.
        """
        changed = False
        blob = self.context_data or {}

        # connection_id may be stored as str in legacy; try to coerce
        if not self.connection_id:
            cid = blob.get('connection_id') or blob.get('connection')  # 'connection' used sometimes in old code
            if cid:
                try:
                    # Accept UUID string or actual UUID
                    self.connection_id = uuid.UUID(str(cid))
                except Exception:
                    # If it's not a UUID, store nothing (views handle None safely)
                    pass
                else:
                    changed = True

        # selected_tables
        if not self.selected_tables:
            st = blob.get('selected_tables') or blob.get('tables') or []
            if isinstance(st, list):
                self.selected_tables = st
                changed = True

        # joins
        if not self.joins:
            j = blob.get('joins') or []
            if isinstance(j, list):
                self.joins = j
                changed = True

        # calculated_fields
        if not self.calculated_fields:
            cf = blob.get('calculated_fields') or []
            if isinstance(cf, list):
                self.calculated_fields = cf
                changed = True

        if changed and save_if_changed:
            # Ensure updated_at advances if we changed the row
            self.updated_at = timezone.now()
            super().save(update_fields=['connection_id', 'selected_tables', 'joins', 'calculated_fields', 'updated_at'])

        return changed

    def to_public_dict(self) -> dict:
        """
        Canonical payload used by the API responses.
        """
        # Make sure fields are normalized before returning
        self.normalize_from_legacy(save_if_changed=False)
        return {
            'connection_id': str(self.connection_id) if self.connection_id else None,
            'selected_tables': self.selected_tables or [],
            'joins': self.joins or [],
            'calculated_fields': self.calculated_fields or [],
        }

    def save(self, *args, **kwargs):
        """
        Override save to keep `context_data` loosely in sync so very old code
        (that still reads `context_data`) doesn’t break.
        """
        # Normalize first (pull from legacy if empty)
        self.normalize_from_legacy(save_if_changed=False)

        # Write back a minimal mirror for backward compatibility
        blob = self.context_data or {}
        # Only mirror simple primitives; avoid clobbering unrelated keys in legacy blob
        blob.setdefault('connection_id', str(self.connection_id) if self.connection_id else None)
        blob.setdefault('selected_tables', self.selected_tables or [])
        blob.setdefault('joins', self.joins or [])
        blob.setdefault('calculated_fields', self.calculated_fields or [])
        self.context_data = blob

        super().save(*args, **kwargs)
    
class DashboardVersionHistory(models.Model):
    """
    Stores a snapshot of a Dashboard's configuration at a specific
    point in time, creating a version history.
    """
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    dashboard = models.ForeignKey(
        Dashboard,
        on_delete=models.CASCADE,
        related_name='version_history'
    )
    version_number = models.PositiveIntegerField()
    config_snapshot = models.JSONField(
        help_text="A JSON snapshot of the dashboard's config_v2 at this version."
    )
    saved_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="The user who saved this version."
    )
    saved_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-saved_at']
        verbose_name = "Dashboard Version History"
        verbose_name_plural = "Dashboard Version Histories"
        # Ensures each version number is unique per dashboard
        unique_together = ('dashboard', 'version_number')

    def __str__(self):
        return f"{self.dashboard.title} - Version {self.version_number}"
