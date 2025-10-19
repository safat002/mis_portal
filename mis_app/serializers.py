"""
Django REST Framework Serializers for MIS Application

Serializers for API endpoints
"""

from rest_framework import serializers
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
import json

from .models import (
    User, UserGroup, ExternalConnection, SavedReport, Dashboard,
    CleanedDataSource, ConnectionJoin, DataUpload, AuditLog
)


class UserSerializer(serializers.ModelSerializer):
    """Serializer for User model"""
    
    password = serializers.CharField(write_only=True, validators=[validate_password])
    pinned_dashboard_count = serializers.SerializerMethodField()
    pinned_report_count = serializers.SerializerMethodField()
    
    class Meta:
        model = User
        fields = [
            'id', 'username', 'email', 'first_name', 'last_name',
            'user_type', 'is_active', 'date_joined', 'last_login',
            'password', 'pinned_dashboard_count', 'pinned_report_count'
        ]
        extra_kwargs = {
            'password': {'write_only': True},
            'date_joined': {'read_only': True},
            'last_login': {'read_only': True},
        }
    
    def get_pinned_dashboard_count(self, obj):
        return len(obj.pinned_dashboards or [])
    
    def get_pinned_report_count(self, obj):
        return len(obj.pinned_reports or [])
    
    def create(self, validated_data):
        password = validated_data.pop('password')
        user = User.objects.create_user(**validated_data)
        user.set_password(password)
        user.save()
        return user
    
    def update(self, instance, validated_data):
        password = validated_data.pop('password', None)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        
        if password:
            instance.set_password(password)
        
        instance.save()
        return instance


class UserGroupSerializer(serializers.ModelSerializer):
    """Serializer for UserGroup model"""
    
    user_count = serializers.SerializerMethodField()
    permission_count = serializers.SerializerMethodField()
    
    class Meta:
        model = UserGroup
        fields = ['id', 'name', 'description', 'user_count', 'permission_count', 'created_at']
        read_only_fields = ['created_at']
    
    def get_user_count(self, obj):
        return obj.users.count()
    
    def get_permission_count(self, obj):
        return obj.permissions.count()


class ExternalConnectionSerializer(serializers.ModelSerializer):
    """Serializer for ExternalConnection model"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    connection_status = serializers.SerializerMethodField()
    
    class Meta:
        model = ExternalConnection
        fields = [
            'id', 'nickname', 'db_type', 'host', 'port', 'username', 
            'password', 'db_name', 'schema', 'filepath', 'is_default',
            'hidden_tables', 'owner', 'owner_name', 'connection_status',
            'created_at', 'updated_at'
        ]
        extra_kwargs = {
            'password': {'write_only': True},
            'created_at': {'read_only': True},
            'updated_at': {'read_only': True},
        }
    
    def get_connection_status(self, obj):
        from .services.external_db import ExternalDBService
        service = ExternalDBService(str(obj.id))
        return {
            'connected': service.test_connection(),
            'last_tested': None  # Could store last test time
        }


class SavedReportSerializer(serializers.ModelSerializer):
    """Serializer for SavedReport model"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    connection_name = serializers.CharField(source='connection.nickname', read_only=True)
    can_edit = serializers.SerializerMethodField()
    share_count = serializers.SerializerMethodField()
    
    class Meta:
        model = SavedReport
        fields = [
            'id', 'report_name', 'report_config', 'data_prep_recipe',
            'pivot_config', 'owner', 'owner_name', 'connection',
            'connection_name', 'is_public', 'can_edit', 'share_count',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']
    
    def get_can_edit(self, obj):
        request = self.context.get('request')
        if not request or not request.user:
            return False
        
        from .utils import check_report_permission
        permission = check_report_permission(obj, request.user)
        return permission == 'edit'
    
    def get_share_count(self, obj):
        return obj.reportshare_set.count()


class DashboardSerializer(serializers.ModelSerializer):
    """Serializer for Dashboard model"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    can_edit = serializers.SerializerMethodField()
    share_count = serializers.SerializerMethodField()
    widget_count = serializers.SerializerMethodField()
    
    class Meta:
        model = Dashboard
        fields = [
            'id', 'name', 'description', 'config_json', 'config_version',
            'owner', 'owner_name', 'is_public', 'can_edit', 'share_count',
            'widget_count', 'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']
    
    def get_can_edit(self, obj):
        request = self.context.get('request')
        if not request or not request.user:
            return False
        
        from .utils import check_dashboard_permission
        permission = check_dashboard_permission(obj, request.user)
        return permission == 'edit'
    
    def get_share_count(self, obj):
        return obj.dashboardshare_set.count()
    
    def get_widget_count(self, obj):
        if obj.config_json and 'widgets' in obj.config_json:
            return len(obj.config_json['widgets'])
        return 0


class CleanedDataSourceSerializer(serializers.ModelSerializer):
    """Serializer for CleanedDataSource model"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    connection_name = serializers.CharField(source='connection.nickname', read_only=True)
    
    class Meta:
        model = CleanedDataSource
        fields = [
            'id', 'name', 'description', 'owner', 'owner_name',
            'connection', 'connection_name', 'source_table_name',
            'materialized_as', 'view_name', 'config_json', 'recipe_json',
            'created_at', 'updated_at'
        ]
        read_only_fields = ['created_at', 'updated_at']


class ConnectionJoinSerializer(serializers.ModelSerializer):
    """Serializer for ConnectionJoin model"""
    
    connection_name = serializers.CharField(source='connection.nickname', read_only=True)
    
    class Meta:
        model = ConnectionJoin
        fields = [
            'id', 'connection', 'connection_name', 'left_table', 'left_column',
            'right_table', 'right_column', 'join_type', 'cardinality', 'created_at'
        ]
        read_only_fields = ['created_at']


class DataUploadSerializer(serializers.ModelSerializer):
    """Serializer for DataUpload model"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    connection_name = serializers.CharField(source='connection.nickname', read_only=True)
    file_size_mb = serializers.SerializerMethodField()
    
    class Meta:
        model = DataUpload
        fields = [
            'id', 'filename', 'original_filename', 'file_size', 'file_size_mb',
            'content_type', 'status', 'error_message', 'rows_processed',
            'owner', 'owner_name', 'connection', 'connection_name',
            'target_table_name', 'processing_options', 'created_at',
            'updated_at', 'completed_at'
        ]
        read_only_fields = [
            'file_size', 'status', 'error_message', 'rows_processed',
            'created_at', 'updated_at', 'completed_at'
        ]
    
    def get_file_size_mb(self, obj):
        return round(obj.file_size / (1024 * 1024), 2)


class AuditLogSerializer(serializers.ModelSerializer):
    """Serializer for AuditLog model"""
    
    user_name = serializers.CharField(source='user.username', read_only=True)
    
    class Meta:
        model = AuditLog
        fields = [
            'id', 'user', 'user_name', 'action', 'object_type', 'object_id',
            'object_name', 'details', 'ip_address', 'user_agent', 'timestamp'
        ]
        read_only_fields = ['timestamp']


# Specialized serializers for specific use cases

class DashboardSummarySerializer(serializers.ModelSerializer):
    """Lightweight serializer for dashboard listings"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    widget_count = serializers.SerializerMethodField()
    is_pinned = serializers.SerializerMethodField()
    
    class Meta:
        model = Dashboard
        fields = ['id', 'name', 'owner_name', 'widget_count', 'is_pinned', 'updated_at']
    
    def get_widget_count(self, obj):
        if obj.config_json and 'widgets' in obj.config_json:
            return len(obj.config_json['widgets'])
        return 0
    
    def get_is_pinned(self, obj):
        request = self.context.get('request')
        if request and request.user.is_authenticated:
            pinned = request.user.pinned_dashboards or []
            return str(obj.id) in pinned
        return False


class ReportSummarySerializer(serializers.ModelSerializer):
    """Lightweight serializer for report listings"""
    
    owner_name = serializers.CharField(source='owner.username', read_only=True)
    connection_name = serializers.CharField(source='connection.nickname', read_only=True)
    is_pinned = serializers.SerializerMethodField()
    
    class Meta:
        model = SavedReport
        fields = ['id', 'report_name', 'owner_name', 'connection_name', 'is_pinned', 'updated_at']
    
    def get_is_pinned(self, obj):
        request = self.context.get('request')
        if request and request.user.is_authenticated:
            pinned = request.user.pinned_reports or []
            return str(obj.id) in pinned
        return False


class TableColumnSerializer(serializers.Serializer):
    """Serializer for table column information"""
    
    name = serializers.CharField()
    type = serializers.CharField()
    nullable = serializers.BooleanField(default=True)
    primary_key = serializers.BooleanField(default=False)
    is_numeric = serializers.BooleanField(default=False)
    

class DatabaseTableSerializer(serializers.Serializer):
    """Serializer for database table information"""
    
    name = serializers.CharField()
    row_count = serializers.IntegerField(required=False)
    columns = TableColumnSerializer(many=True, required=False)


class TransformationStepSerializer(serializers.Serializer):
    """Serializer for data transformation steps"""
    
    id = serializers.CharField()
    strategy = serializers.CharField()
    column = serializers.CharField()
    params = serializers.DictField(default=dict)
    timestamp = serializers.DateTimeField(required=False)


class DataPreviewSerializer(serializers.Serializer):
    """Serializer for data preview responses"""
    
    headers = serializers.ListField(child=serializers.CharField())
    rows = serializers.ListField(child=serializers.DictField())
    total_rows = serializers.IntegerField()
    transformations_applied = serializers.IntegerField(default=0)