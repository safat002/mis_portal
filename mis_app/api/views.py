"""
Django API Views
Converted from Flask routes.py

Main API endpoints for Django MIS application
"""

import re
import traceback
import json
import math
import logging
from typing import Dict, List, Optional, Any
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth.decorators import login_required
from django.contrib.auth import authenticate, login, logout
from django.shortcuts import get_object_or_404
from django.db import transaction
from django.core.paginator import Paginator
from django.utils import timezone
from django.core.cache import cache
import pandas as pd

from ..models import (
    User, SavedReport, Dashboard, ExternalConnection, 
    ConnectionJoin, CleanedDataSource, AuditLog
)
from ..services.report_builder import ReportBuilderService
from ..services.data_preparation import DataPreparationService
from ..services.external_db import ExternalDBService
from ..services.transformation_engine import TransformationEngine
from ..utils import log_user_action

logger = logging.getLogger(__name__)


# Helper Functions
def _json_sanitize(obj):
    """Recursively convert NaN/Inf to None for JSON serialization"""
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        return {k: _json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_sanitize(v) for v in obj]
    return obj


def _convert_df_numerics(df):
    """Safely convert object columns to numeric types"""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass
    return df


def check_dashboard_permission(dashboard_id, user):
    """Check user permissions for dashboard"""
    try:
        dashboard = Dashboard.objects.get(id=dashboard_id)
        
        # Admins and moderators can edit any dashboard
        if user.user_type in ['Admin', 'Moderator']:
            return 'edit'
        
        # Owner can edit
        if dashboard.owner == user:
            return 'edit'
        
        # Check shared permissions
        if user in dashboard.shared_with.all():
            # You might want to add a permission field to the share relationship
            return 'view'  # Default to view permission
        
        return None
    except Dashboard.DoesNotExist:
        return None


# Authentication Views
@csrf_exempt
def login_view(request):
    """User login endpoint"""
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            username = data.get('username')
            password = data.get('password')
            
            user = authenticate(request, username=username, password=password)
            if user:
                login(request, user)
                return JsonResponse({
                    'success': True,
                    'message': 'Login successful',
                    'user': {
                        'id': user.id,
                        'username': user.username,
                        'email': user.email,
                        'user_type': user.user_type
                    }
                })
            else:
                return JsonResponse({
                    'success': False,
                    'error': 'Invalid username or password'
                }, status=401)
        except Exception as e:
            return JsonResponse({
                'success': False,
                'error': str(e)
            }, status=400)
    
    return JsonResponse({'error': 'Only POST method allowed'}, status=405)


@login_required
def logout_view(request):
    """User logout endpoint"""
    logout(request)
    return JsonResponse({'success': True, 'message': 'Logged out successfully'})


# User Management Views
@login_required
def get_users(request):
    """Get list of all users (Admin only)"""
    if request.user.user_type != 'Admin':
        return JsonResponse({'error': 'Permission denied'}, status=403)
    
    users = User.objects.all()
    return JsonResponse([{
        'id': u.id,
        'username': u.username,
        'email': u.email,
        'user_type': u.user_type,
        'is_active': u.is_active,
        'date_joined': u.date_joined.isoformat()
    } for u in users], safe=False)


@csrf_exempt
@login_required
def create_user(request):
    """Create new user (Admin only)"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    if request.user.user_type != 'Admin':
        return JsonResponse({'error': 'Permission denied'}, status=403)
    
    try:
        data = json.loads(request.body)
        username = data.get('username')
        email = data.get('email')
        password = data.get('password')
        user_type = data.get('user_type')
        
        if not all([username, email, password, user_type]):
            return JsonResponse({'error': 'All fields are required'}, status=400)
        
        if User.objects.filter(username=username).exists():
            return JsonResponse({'error': 'Username already exists'}, status=409)
        
        if User.objects.filter(email=email).exists():
            return JsonResponse({'error': 'Email already exists'}, status=409)
        
        user = User.objects.create_user(
            username=username,
            email=email,
            password=password,
            user_type=user_type
        )
        
        # Log user creation
        log_user_action(
            request.user, 'create_user', 'user', str(user.id),
            f'Created user: {username}', {'user_type': user_type}
        )
        
        return JsonResponse({
            'success': True,
            'message': 'User created successfully',
            'user': {
                'id': user.id,
                'username': user.username,
                'email': user.email,
                'user_type': user.user_type
            }
        }, status=201)
        
    except Exception as e:
        logger.error(f'Error creating user: {e}')
        return JsonResponse({'error': str(e)}, status=500)


# Database Connection Views
@login_required
def get_db_connections(request):
    """Get database connections for current user"""
    try:
        connections = []
        user_connections = ExternalConnection.objects.filter(owner=request.user).order_by('nickname')
        
        for conn in user_connections:
            # Check connection status
            db_service = ExternalDBService(str(conn.id))
            connected = db_service.test_connection()
            
            connections.append({
                'id': str(conn.id),
                'name': conn.nickname,
                'db_type': conn.db_type,
                'host': conn.host,
                'port': conn.port,
                'db_name': conn.db_name,
                'filepath': conn.filepath,
                'is_default': conn.is_default,
                'connected': connected,
                'created_at': conn.created_at.isoformat()
            })
        
        return JsonResponse({'success': True, 'connections': connections})
        
    except Exception as e:
        logger.error(f'Error getting connections: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
def get_db_connection_details(request):
    """Get details for specific database connection"""
    connection_id = request.GET.get('id')
    if not connection_id:
        return JsonResponse({'success': False, 'error': 'Connection ID required'}, status=400)
    
    try:
        connection = get_object_or_404(ExternalConnection, id=connection_id, owner=request.user)
        
        # Get tables for this connection
        db_service = ExternalDBService(connection_id)
        all_tables = []
        
        try:
            all_tables = db_service.get_visible_tables()
        except Exception as e:
            logger.warning(f'Error getting tables for connection {connection_id}: {e}')
        
        connection_dict = {
            'id': str(connection.id),
            'name': connection.nickname,
            'db_type': connection.db_type,
            'host': connection.host,
            'port': connection.port,
            'username': connection.username,
            'db_name': connection.db_name,
            'filepath': connection.filepath,
            'schema': connection.schema,
            'hidden_tables': connection.hidden_tables or '',
            'is_default': connection.is_default,
            'tables': all_tables
        }
        
        return JsonResponse({'success': True, 'connection': connection_dict})
        
    except ExternalConnection.DoesNotExist:
        return JsonResponse({'success': False, 'error': 'Connection not found'}, status=404)
    except Exception as e:
        logger.error(f'Error getting connection details: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@csrf_exempt
@login_required
def save_db_connection(request):
    """Save new or update existing database connection"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        # Handle both JSON and form data
        if request.content_type == 'application/json':
            data = json.loads(request.body)
        else:
            data = request.POST.dict()
        
        connection_id = data.get('connection_id')
        connection_name = data.get('connection_name')
        
        if not connection_name:
            return JsonResponse({'success': False, 'error': 'Connection name is required'}, status=400)
        
        with transaction.atomic():
            if connection_id:
                connection = get_object_or_404(ExternalConnection, id=connection_id, owner=request.user)
            else:
                connection = ExternalConnection(owner=request.user)
            
            # Update connection fields
            connection.nickname = connection_name
            connection.db_type = data.get('db_type')
            connection.host = data.get('host', '')
            connection.port = data.get('port', '')
            connection.username = data.get('username', '')
            connection.password = data.get('password', '')
            connection.db_name = data.get('db_name', '')
            connection.filepath = data.get('filepath', '')
            connection.schema = data.get('schema', '')
            connection.hidden_tables = data.get('hidden_tables', '')
            connection.is_default = data.get('is_default', False)
            
            connection.save()
            
            # Test the connection
            db_service = ExternalDBService(str(connection.id))
            test_result = db_service.test_connection()
            
            # Log the action
            action = 'update_connection' if connection_id else 'create_connection'
            log_user_action(
                request.user, action, 'external_connection', str(connection.id),
                f'{action.replace("_", " ").title()}: {connection_name}',
                {'db_type': connection.db_type, 'test_result': test_result}
            )
            
            return JsonResponse({
                'success': True,
                'message': f'Connection {"updated" if connection_id else "created"} successfully',
                'connection_id': str(connection.id),
                'test_result': test_result
            })
            
    except Exception as e:
        logger.error(f'Error saving connection: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# Report Management Views
@csrf_exempt
@login_required
def execute_report(request):
    """Execute report with given configuration"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        report_config = data.get('report_config', {})
        
        if not report_config:
            return JsonResponse({'error': 'Report configuration is required'}, status=400)
        
        # Initialize report builder service
        report_service = ReportBuilderService()
        
        # Execute the report
        df, total_rows, error = report_service.build_advanced_report(
            report_config, request.user
        )
        
        if error:
            return JsonResponse({'success': False, 'error': error}, status=400)
        
        if df is None or df.empty:
            return JsonResponse({
                'success': True,
                'data': {'columns': [], 'rows': []},
                'total_rows': 0
            })
        
        # Convert DataFrame to JSON-serializable format
        df = _convert_df_numerics(df)
        
        # Prepare response data
        columns = [{'name': col, 'type': str(df[col].dtype)} for col in df.columns]
        rows = _json_sanitize(df.to_dict('records'))
        
        return JsonResponse({
            'success': True,
            'data': {
                'columns': columns,
                'rows': rows
            },
            'total_rows': total_rows,
            'current_page': report_config.get('page', 1),
            'page_size': len(rows)
        })
        
    except Exception as e:
        logger.error(f'Error executing report: {e}', exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@csrf_exempt
@login_required
def save_report(request):
    """Save a report configuration"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        report_name = data.get('report_name')
        report_config = data.get('report_config')
        pivot_config = data.get('pivot_config')
        
        if not report_name or not report_config:
            return JsonResponse({
                'success': False,
                'error': 'Report name and configuration are required'
            }, status=400)
        
        # Create new report
        report = SavedReport.objects.create(
            report_name=report_name,
            report_config=report_config,
            pivot_config=pivot_config,
            owner=request.user
        )
        
        # Log the action
        log_user_action(
            request.user, 'save_report', 'saved_report', str(report.id),
            f'Saved report: {report_name}',
            {'columns_count': len(report_config.get('columns', []))}
        )
        
        return JsonResponse({
            'success': True,
            'message': f'Report "{report_name}" saved successfully',
            'report_id': str(report.id)
        })
        
    except Exception as e:
        logger.error(f'Error saving report: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@csrf_exempt
@login_required
def update_report(request, report_id):
    """Update existing report"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        report = get_object_or_404(SavedReport, id=report_id)
        
        # Check permissions
        if report.owner != request.user:
            # Check if user has edit permissions through sharing
            # This would need to be implemented based on your sharing model
            return JsonResponse({'success': False, 'error': 'Permission denied'}, status=403)
        
        data = json.loads(request.body)
        
        # Update report
        if 'report_config' in data:
            report.report_config = data['report_config']
        if 'pivot_config' in data:
            report.pivot_config = data['pivot_config']
        
        report.save()
        
        # Log the action
        log_user_action(
            request.user, 'update_report', 'saved_report', str(report.id),
            f'Updated report: {report.report_name}', {}
        )
        
        return JsonResponse({
            'success': True,
            'message': f'Report "{report.report_name}" updated successfully'
        })
        
    except Exception as e:
        logger.error(f'Error updating report: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
def get_my_reports(request):
    """Get reports owned by or shared with current user"""
    try:
        # Get owned reports
        owned_reports = SavedReport.objects.filter(owner=request.user)
        
        # Get shared reports (would need proper sharing model)
        # shared_reports = SavedReport.objects.filter(shared_with=request.user)
        
        reports_data = []
        for report in owned_reports:
            reports_data.append({
                'id': str(report.id),
                'name': report.report_name,
                'owner': report.owner.username,
                'created_at': report.created_at.isoformat(),
                'updated_at': report.updated_at.isoformat(),
                'is_owner': True,
                'permission': 'edit'
            })
        
        return JsonResponse({
            'success': True,
            'reports': reports_data
        })
        
    except Exception as e:
        logger.error(f'Error getting reports: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# Data Preparation Views
@csrf_exempt
@login_required
def analyze_data_profile(request):
    """Analyze data profile for a table"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        connection_id = data.get('connection_id')
        table_name = data.get('table_name')
        sample_size = data.get('sample_size', 1000)
        
        if not connection_id or not table_name:
            return JsonResponse({
                'error': 'Connection ID and table name are required'
            }, status=400)
        
        # Initialize data preparation service
        prep_service = DataPreparationService()
        
        # Analyze the data
        profile = prep_service.analyze_data_profile(connection_id, table_name, sample_size)
        
        return JsonResponse({
            'success': True,
            'profile': profile
        })
        
    except Exception as e:
        logger.error(f'Error analyzing data profile: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@csrf_exempt
@login_required
def create_cleaned_data_source(request):
    """Create a cleaned data source with applied recipe"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        name = data.get('name')
        connection_id = data.get('connection_id')
        original_table = data.get('original_table')
        recipe = data.get('recipe', [])
        
        if not all([name, connection_id, original_table]):
            return JsonResponse({
                'error': 'Name, connection ID, and original table are required'
            }, status=400)
        
        # Initialize data preparation service
        prep_service = DataPreparationService()
        
        # Save cleaned dataset
        cleaned_id = prep_service.save_cleaned_dataset(
            connection_id, original_table, recipe, request.user, name
        )
        
        return JsonResponse({
            'success': True,
            'message': f'Cleaned data source "{name}" created successfully',
            'cleaned_id': cleaned_id
        })
        
    except Exception as e:
        logger.error(f'Error creating cleaned data source: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
def get_cleaned_data_sources(request):
    """Get cleaned data sources for current user"""
    try:
        sources = CleanedDataSource.objects.filter(created_by=request.user).order_by('name')
        
        sources_data = []
        for source in sources:
            sources_data.append({
                'id': str(source.id),
                'name': source.name,
                'original_table': source.original_table,
                'connection_id': str(source.connection_id),
                'recipe_steps': len(source.recipe) if source.recipe else 0,
                'created_at': source.created_at.isoformat()
            })
        
        return JsonResponse({
            'success': True,
            'sources': sources_data
        })
        
    except Exception as e:
        logger.error(f'Error getting cleaned data sources: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# AI Analysis Views
@csrf_exempt
@login_required
def analyze_report(request):
    """AI analysis of report data"""
    if request.method != 'POST':
        return JsonResponse({'error': 'Only POST method allowed'}, status=405)
    
    try:
        data = json.loads(request.body)
        report_data = data.get('data', {})
        
        if not report_data or 'rows' not in report_data or not report_data['rows']:
            return JsonResponse({'error': 'No data available to analyze'}, status=400)
        
        # For now, return a mock response
        # In production, this would integrate with an AI service like OpenAI or Gemini
        mock_analysis = """
        **Key Insights from Data Analysis:**
        
        • **Trend Analysis**: The data shows consistent growth patterns across most metrics
        • **Data Quality**: Dataset appears clean with minimal missing values
        • **Outliers**: A few notable outliers detected that may require investigation
        • **Seasonality**: Clear seasonal patterns visible in time-based data
        • **Recommendations**: Consider focusing on top-performing segments for optimization
        """
        
        return JsonResponse({
            'success': True,
            'analysis': mock_analysis
        })
        
    except Exception as e:
        logger.error(f'Error analyzing report: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# Utility Views
@login_required
def get_table_columns(request):
    """Get columns for a specific table"""
    connection_id = request.GET.get('connection_id')
    table_name = request.GET.get('table_name')
    
    if not connection_id or not table_name:
        return JsonResponse({
            'error': 'Connection ID and table name are required'
        }, status=400)
    
    try:
        db_service = ExternalDBService(connection_id)
        columns = db_service.get_table_columns(table_name)
        
        return JsonResponse({
            'success': True,
            'columns': columns
        })
        
    except Exception as e:
        logger.error(f'Error getting table columns: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
def get_table_sample_data(request):
    """Get sample data from a table"""
    connection_id = request.GET.get('connection_id')
    table_name = request.GET.get('table_name')
    limit = int(request.GET.get('limit', 100))
    
    if not connection_id or not table_name:
        return JsonResponse({
            'error': 'Connection ID and table name are required'
        }, status=400)
    
    try:
        db_service = ExternalDBService(connection_id)
        
        # Execute simple SELECT query
        query = f'SELECT * FROM "{table_name}" LIMIT {limit}'
        result = db_service.execute_query(query)
        
        if result['success']:
            return JsonResponse({
                'success': True,
                'data': result['data'],
                'columns': result.get('columns', [])
            })
        else:
            return JsonResponse({
                'success': False,
                'error': result.get('error', 'Unknown error')
            }, status=500)
        
    except Exception as e:
        logger.error(f'Error getting sample data: {e}')
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


# Health Check
def health_check(request):
    """Health check endpoint"""
    return JsonResponse({
        'status': 'healthy',
        'timestamp': timezone.now().isoformat()
    })