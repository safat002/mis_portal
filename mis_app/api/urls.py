"""
Django URL Configuration for MIS App API
"""

from django.urls import path, include
from . import views as api_views

# API URL patterns
api_urlpatterns = [
    # Authentication
    path('auth/login/', api_views.login_view, name='api_login'),
    path('auth/logout/', api_views.logout_view, name='api_logout'),
    
    # User Management
    path('users/', api_views.get_users, name='api_get_users'),
    path('users/create/', api_views.create_user, name='api_create_user'),
    
    # Database Connections
    path('connections/', api_views.get_db_connections, name='api_get_connections'),
    path('connections/details/', api_views.get_db_connection_details, name='api_get_connection_details'),
    path('connections/save/', api_views.save_db_connection, name='api_save_connection'),
    
    # Reports
    path('reports/execute/', api_views.execute_report, name='api_execute_report'),
    path('reports/save/', api_views.save_report, name='api_save_report'),
    path('reports/update/<str:report_id>/', api_views.update_report, name='api_update_report'),
    path('reports/my/', api_views.get_my_reports, name='api_get_my_reports'),
    
    # Data Preparation
    path('data/profile/', api_views.analyze_data_profile, name='api_analyze_data_profile'),
    path('data/cleaned/create/', api_views.create_cleaned_data_source, name='api_create_cleaned_data'),
    path('data/cleaned/', api_views.get_cleaned_data_sources, name='api_get_cleaned_data'),
    
    # Utilities
    path('tables/columns/', api_views.get_table_columns, name='api_get_table_columns'),
    path('tables/sample/', api_views.get_table_sample_data, name='api_get_sample_data'),
    
    # AI Analysis
    path('ai/analyze/', api_views.analyze_report, name='api_analyze_report'),
    
    # Health Check
    path('health/', api_views.health_check, name='api_health_check'),
]

# Main URL patterns
urlpatterns = [
    # API routes
    path('api/', include(api_urlpatterns)),
]