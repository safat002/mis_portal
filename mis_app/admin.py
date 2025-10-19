"""
Django Admin Configuration for MIS Application
Enhanced admin interface for all models
"""

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.utils.html import format_html
from django.urls import reverse
from django.utils.safestring import mark_safe
import json

from .models import (
    User, UserGroup, ExternalConnection, GroupPermission, UserPermission, GroupMembership,
    SavedReport, ReportShare, Dashboard, DashboardShare,
    Widget, Notification, ExportHistory, CleanedDataSource,
    DrillDownPath, ConnectionJoin, CanvasLayout,
    DashboardDataContext, AuditLog
)


@admin.register(User)
class UserAdmin(BaseUserAdmin):
    """Enhanced User admin"""
    list_display = ('username', 'email', 'user_type', 'is_active', 'date_joined', 'last_login')
    list_filter = ('user_type', 'is_active', 'is_staff', 'is_superuser', 'date_joined')
    search_fields = ('username', 'email', 'first_name', 'last_name')
    ordering = ('-date_joined',)
    
    fieldsets = BaseUserAdmin.fieldsets + (
        ('MIS Settings', {
            'fields': ('user_type', 'pinned_dashboards', 'pinned_reports')
        }),
    )
    
    readonly_fields = ('created_at', 'updated_at')


@admin.register(UserGroup)
class UserGroupAdmin(admin.ModelAdmin):
    """User Group admin"""
    list_display = ('name', 'description', 'user_count', 'created_at')
    search_fields = ('name', 'description')
    filter_horizontal = ('users',)
    readonly_fields = ('created_at', 'updated_at')
    
    def user_count(self, obj):
        return obj.users.count()
    user_count.short_description = 'Users'


@admin.register(ExternalConnection)
class ExternalConnectionAdmin(admin.ModelAdmin):
    """External Connection admin"""
    list_display = ('nickname', 'db_type', 'host', 'owner', 'health_status', 'is_active', 'created_at')
    list_filter = ('db_type', 'health_status', 'is_active', 'created_at')
    search_fields = ('nickname', 'host', 'db_name')
    readonly_fields = ('created_at', 'updated_at', 'last_health_check')
    
    fieldsets = (
        ('Basic Information', {
            'fields': ('nickname', 'db_type', 'owner', 'is_active')
        }),
        ('Connection Details', {
            'fields': ('host', 'port', 'username', 'password', 'db_name', 'schema', 'filepath')
        }),
        ('Settings', {
            'fields': ('is_default', 'hidden_tables')
        }),
        ('Health Status', {
            'fields': ('health_status', 'last_health_check'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    actions = ['test_connections', 'mark_as_default']

    def has_view_permission(self, request, obj=None):
        """Allow superusers to view all connections."""
        if request.user.is_superuser:
            return True
        if obj is not None:
            return obj.owner == request.user
        return True

    def has_change_permission(self, request, obj=None):
        """Allow superusers to change all connections."""
        if request.user.is_superuser:
            return True
        if obj is not None:
            return obj.owner == request.user
        return True

    def has_delete_permission(self, request, obj=None):
        """Allow superusers to delete all connections."""
        if request.user.is_superuser:
            return True
        if obj is not None:
            return obj.owner == request.user
        return True

    def get_queryset(self, request):
        """
        Return all connections for superusers, and only owned connections for other users.
        """
        qs = super().get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(owner=request.user)
    
    def test_connections(self, request, queryset):
        """Test selected connections"""
        from .services.external_db import ExternalDBService
        
        tested = 0
        for connection in queryset:
            try:
                service = ExternalDBService(str(connection.id))
                is_healthy = service.test_connection()
                connection.health_status = 'healthy' if is_healthy else 'unhealthy'
                connection.save()
                tested += 1
            except Exception:
                connection.health_status = 'error'
                connection.save()
        
        self.message_user(request, f'Tested {tested} connections')
    
    test_connections.short_description = 'Test selected connections'
    
    def mark_as_default(self, request, queryset):
        """Mark connection as default"""
        if queryset.count() != 1:
            self.message_user(request, 'Please select exactly one connection', level='ERROR')
            return
        
        # Unmark all as default
        ExternalConnection.objects.update(is_default=False)
        
        # Mark selected as default
        queryset.update(is_default=True)
        self.message_user(request, 'Default connection updated')
    
    mark_as_default.short_description = 'Mark as default connection'


@admin.register(SavedReport)
class SavedReportAdmin(admin.ModelAdmin):
    """Saved Report admin"""
    list_display = ('report_name', 'owner', 'created_at', 'updated_at', 'shared_count')
    list_filter = ('created_at', 'updated_at')
    search_fields = ('report_name', 'owner__username')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        (None, {
            'fields': ('report_name', 'owner')
        }),
        ('Configuration', {
            'fields': ('report_config', 'pivot_config', 'data_prep_recipe'),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def shared_count(self, obj):
        return obj.shared_with.count()
    shared_count.short_description = 'Shared With'


@admin.register(Dashboard)
class DashboardAdmin(admin.ModelAdmin):
    """Dashboard admin"""
    list_display = ('title', 'owner', 'is_public', 'widget_count', 'created_at', 'updated_at')
    list_filter = ('is_public', 'created_at', 'updated_at')
    search_fields = ('title', 'description', 'owner__username')
    readonly_fields = ('created_at', 'updated_at')
    
    fieldsets = (
        (None, {
            'fields': ('title', 'description', 'owner', 'is_public')
        }),
        ('Configuration', {
            'fields': ('config',),
            'classes': ('collapse',)
        }),
        ('Timestamps', {
            'fields': ('created_at', 'updated_at'),
            'classes': ('collapse',)
        })
    )
    
    def widget_count(self, obj):
        return obj.widgets.count()
    widget_count.short_description = 'Widgets'


@admin.register(Widget)
class WidgetAdmin(admin.ModelAdmin):
    """Widget admin"""
    list_display = ('title', 'type', 'dashboard', 'created_by', 'created_at')
    list_filter = ('type', 'created_at')
    search_fields = ('title', 'dashboard__title')
    readonly_fields = ('created_at', 'updated_at')


@admin.register(Notification)
class NotificationAdmin(admin.ModelAdmin):
    """Notification admin"""
    list_display = ('title', 'type', 'recipient', 'priority', 'is_read', 'created_at')
    list_filter = ('type', 'priority', 'is_read', 'created_at')
    search_fields = ('title', 'message', 'recipient__username')
    readonly_fields = ('created_at',)
    
    actions = ['mark_as_read', 'mark_as_unread']
    
    def mark_as_read(self, request, queryset):
        """Mark notifications as read"""
        from django.utils import timezone
        count = queryset.filter(is_read=False).update(is_read=True, read_at=timezone.now())
        self.message_user(request, f'Marked {count} notifications as read')
    
    mark_as_read.short_description = 'Mark as read'
    
    def mark_as_unread(self, request, queryset):
        """Mark notifications as unread"""
        count = queryset.filter(is_read=True).update(is_read=False, read_at=None)
        self.message_user(request, f'Marked {count} notifications as unread')
    
    mark_as_unread.short_description = 'Mark as unread'


@admin.register(ExportHistory)
class ExportHistoryAdmin(admin.ModelAdmin):
    """Export History admin"""
    list_display = ('filename', 'user', 'format_type', 'row_count', 'file_size_formatted', 'created_at')
    list_filter = ('format_type', 'created_at')
    search_fields = ('filename', 'user__username')
    readonly_fields = ('created_at',)
    
    def file_size_formatted(self, obj):
        """Format file size in human readable format"""
        from .utils import format_file_size
        return format_file_size(obj.file_size)
    file_size_formatted.short_description = 'File Size'


@admin.register(AuditLog)
class AuditLogAdmin(admin.ModelAdmin):
    """Audit Log admin"""
    list_display = ('user', 'action', 'object_type', 'object_name', 'created_at')
    list_filter = ('action', 'object_type', 'created_at')
    search_fields = ('user__username', 'object_name', 'object_type')
    readonly_fields = ('created_at',)
    
    def has_add_permission(self, request):
        return False  # Audit logs should not be manually created
    
    def has_change_permission(self, request, obj=None):
        return False  # Audit logs should not be modified


@admin.register(CleanedDataSource)
class CleanedDataSourceAdmin(admin.ModelAdmin):
    """Cleaned Data Source admin"""
    list_display = ('name', 'view_name', 'owner', 'connection', 'updated_at')
    list_filter = ('owner', 'connection')
    search_fields = ('name', 'view_name')
    readonly_fields = ('created_at', 'updated_at')


@admin.register(ConnectionJoin)
class ConnectionJoinAdmin(admin.ModelAdmin):
    """Connection Join admin"""
    list_display = ('connection', 'left_table', 'left_column', 'right_table', 'right_column', 'join_type')
    list_filter = ('join_type', 'created_at')
    search_fields = ('left_table', 'right_table')


# Inline admins for related objects
class WidgetInline(admin.TabularInline):
    model = Widget
    extra = 0
    readonly_fields = ('created_at',)


class GroupPermissionInline(admin.TabularInline):
    model = GroupPermission
    extra = 0


class ReportShareInline(admin.TabularInline):
    model = ReportShare
    extra = 0


class DashboardShareInline(admin.TabularInline):
    model = DashboardShare
    extra = 0


class UserPermissionInline(admin.TabularInline):
    model = UserPermission
    fk_name = "user"
    extra = 0

# Update existing admins to include inlines
UserAdmin.inlines = [UserPermissionInline]
UserGroupAdmin.inlines = [GroupPermissionInline]
DashboardAdmin.inlines = [WidgetInline, DashboardShareInline]
SavedReportAdmin.inlines = [ReportShareInline]

# Admin site customization
admin.site.site_header = 'MIS Administration'
admin.site.site_title = 'MIS Admin'
admin.site.index_title = 'Management Information System Administration'

# Register remaining models with simple admin
admin.site.register(GroupPermission)
admin.site.register(ReportShare)
admin.site.register(DashboardShare)
admin.site.register(DrillDownPath)
admin.site.register(CanvasLayout)
admin.site.register(DashboardDataContext)
admin.site.register(UserPermission)
admin.site.register(GroupMembership)