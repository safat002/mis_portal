# mis_app/urls.py (Updated with data management URLs)

"""
URL Configuration for MIS Application
Updated to include all data management endpoints
"""

from django.shortcuts import redirect
from django.urls import path, include
from . import views, data_views, report_views, data_model_views, dashboard_views
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static


app_name = 'mis_app'


urlpatterns = [
    # --- Main Page & Auth ---
    path('', lambda request: redirect('mis_app:home', permanent=False)),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
    # path('register/', views.register_view, name='register'),
    path('home/', views.home_view, name='home'),

    # --- Intelligent Import ---
    path('', include('intelligent_import.urls')),

    # --- Main Application Views ---
    path('report-builder/', report_views.report_builder_view, name='report_builder'),
    
    path('data-management/', data_views.data_management_view, name='data_management'),
    path('database-management/', views.database_management_view, name='database_management'),
    path('user-management/', views.user_management_view, name='user_management'),

    # --- User Management Views ---
    path('groups/create/', views.create_group, name='create_group'),
    path('groups/<uuid:group_id>/edit/', views.edit_group, name='edit_group'),
    path('api/groups/<uuid:group_id>/', views.group_detail_api, name='api_group_detail'),
    path('api/groups/<uuid:group_id>/permissions/add/', views.add_group_permission_api, name='api_add_group_permission'),
    path('api/groups/<uuid:group_id>/permissions/manage/', views.manage_group_database_permission_api, name='api_manage_group_database_permission'),
    path('api/groups/<uuid:group_id>/permissions/database/update/', views.update_group_database_permissions_api, name='api_update_group_database_permissions'),
    path('api/permissions/<uuid:permission_id>/delete/', views.delete_group_permission_api, name='api_delete_group_permission'),
    path('import-users/', views.import_users_view, name='import_users'),
    path('api/groups/', views.groups_api, name='api_groups'),

    # New user management URLs
    path('api/users/', views.users_api, name='api_users'),
    path('api/users/<uuid:user_id>/', views.user_detail_api, name='api_user_detail'),
    path('api/users/<uuid:user_id>/upload-permissions/', views.update_user_upload_permissions_api, name='api_update_user_upload_permissions'),
    path('api/users/<uuid:user_id>/permissions/manage/', views.manage_user_database_permission_api, name='api_manage_user_database_permission'),
    path('api/users/bulk-action/', views.bulk_user_actions_api, name='bulk_user_action'),
    path('api/theme/switch/', views.switch_theme_api, name='switch_theme'),
    path('api/user/set-default-connection/', views.set_default_connection_api, name='set_default_connection'),

    # --- Data Management API Endpoints ---
    path('api/data/check-password/', data_views.check_password, name='api_check_password'),
    path('api/data/inspect-file/', data_views.inspect_file, name='api_inspect_file'),
    path('api/analyze_upload/', data_views.analyze_upload, name='api_analyze_upload'),
    path('api/create_table_from_import/', data_views.create_table_from_import, name='api_create_table_from_import'),
    path('api/data/preview-data/', data_views.preview_data, name='api_preview_data'),
    path('api/data/preview-upload-matching/', data_views.preview_upload_for_matching, name='api_preview_upload_matching'),
    path('api/data/confirm-upload/', data_views.confirm_upload, name='api_confirm_upload'),
    path('api/data/create-table/', data_views.create_table, name='api_create_table'),
    path('api/data/rename-table/', data_views.rename_table, name='api_rename_table'),
    path('api/data/truncate-table/', data_views.truncate_table, name='api_truncate_table'),
    path('api/data/drop-table/', data_views.drop_table, name='api_drop_table'),
    path('api/data/delete-rows/', data_views.delete_rows, name='api_delete_rows'),
    path('api/data/upload-data/', data_views.upload_data_api, name='api_upload_data'),
    path('api/data/add-column/', data_views.add_column, name='api_add_column'),
    path('api/data/rename-column/', data_views.rename_column, name='api_rename_column'),
    path('api/data/drop-column/', data_views.drop_column, name='api_drop_column'),
    path('api/data/modify-column-type/', data_views.modify_column_type, name='api_modify_column_type'),
    path('api/data/set-primary-key/', data_views.set_primary_key, name='api_set_primary_key'),
    path('api/data/set-nullable/', data_views.set_nullable, name='api_set_nullable'),
    path('api/data/set-auto-increment/', data_views.set_auto_increment, name='api_set_auto_increment'),
    path('api/data/get-table-data/<uuid:connection_id>/<str:table_name>/', data_views.get_table_data, name='api_get_table_data'),
    path('api/data/table-columns/<uuid:connection_id>/<str:table_name>/', data_views.get_columns_for_table, name='api_get_columns_for_table'),
    path('api/data/visible-tables/<uuid:connection_id>/', data_views.get_visible_tables_for_connection, name='api_get_visible_tables'),
    path('api/data/modify-column-type/', data_views.modify_column_type, name='modify_column_type'),

    # Intelligent import urls
    
    # --- Report Builder API Endpoints ---
    path('api/check_join_path/', report_views.check_join_path_api, name='api_check_join_path'),
    path('api/reports/execute/', report_views.build_report_api, name='execute_report_api'),
    path('api/reports/save/', report_views.save_report_api, name='save_report_api'),
    path('api/reports/profile_data/', report_views.profile_data_api, name='profile_data_api'),
    path('api/reports/my/', report_views.get_my_reports_api, name='get_my_reports_api'),
    path('api/reports/<uuid:report_id>/', report_views.report_detail_api, name='report_detail_api'),
    path('api/reports/find-joins/', report_views.find_joins_api, name='api_find_joins'),
    path('api/reports/get-filter-values/', report_views.get_filter_values_api, name='api_get_filter_values'),
    path('api/reports/export/', report_views.export_report_excel_api, name='export_report_api'),
    path('api/reports/suggestions/<uuid:connection_id>/', report_views.get_report_suggestions_api, name='report_suggestions'),
    path('api/reports/validate/', report_views.validate_report_config_api, name='validate_report_config'),

    # --- Dashboard API Endpoints ---
   # Page views
    path('dashboard/management/', dashboard_views.dashboard_management_view, name='dashboard_management'),
    path('dashboard-management/', lambda r: redirect('mis_app:dashboard_management', permanent=True)),
    path('dashboard/design/<uuid:dashboard_id>/', dashboard_views.dashboard_design_view, name='dashboard_design'),

    # --- Dashboard & Widget APIs ---
    path('api/dashboard/create/', dashboard_views.create_dashboard_api, name='create_dashboard_api'),
    path('api/dashboard/<uuid:dashboard_id>/config/', dashboard_views.dashboard_config_api, name='dashboard_config_api'),
    path('api/dashboard/<uuid:dashboard_id>/data_context/', dashboard_views.dashboard_data_context_api, name='dashboard_data_context_api'),
    path('api/dashboard/<uuid:dashboard_id>/widget_data/', dashboard_views.dashboard_widget_data_api, name='dashboard_widget_data_api'),
    path('api/dashboard/<uuid:dashboard_id>/widget/<uuid:widget_id>/data/', dashboard_views.dashboard_widget_instance_data_api, name='dashboard_widget_instance_data_api'),
    path("api/table-columns/", dashboard_views.table_columns_batch_api, name="table_columns_batch_api"),
    
    # Single dashboard actions
    path('api/dashboard/<uuid:dashboard_id>/pin/', dashboard_views.dashboard_pin_api, name='dashboard_pin_api'),
    path('api/dashboard/<uuid:dashboard_id>/duplicate/', dashboard_views.dashboard_duplicate_api, name='dashboard_duplicate_api'),
    path('api/dashboard/<uuid:dashboard_id>/', dashboard_views.dashboard_delete_api, name='dashboard_delete_api'),
    
    # --- Connection and Schema APIs (ORDER IS IMPORTANT HERE) ---
  
    # The new, more specific 'columns' URL comes FIRST
    path('api/connections/<uuid:connection_id>/tables/<str:table_name>/columns/', dashboard_views.connection_table_columns_api, name='connection_table_columns_api'),
    
    # The more general 'tables' URL comes AFTER
    path('api/connections/<uuid:connection_id>/tables/', dashboard_views.connection_tables_api, name='connection_tables_api'),
    
    path('api/connections/<uuid:connection_id>/suggest_joins/', dashboard_views.suggest_joins_api, name='suggest_joins_api'),

    # debug function
    path('api/debug/dashboard/<uuid:dashboard_id>/widget/<uuid:widget_id>/data/', dashboard_views.debug_widget_data, name='debug_widget_data'),

    # --- Database Connection API Endpoints ---
    path('api/connections/', views.connections_api, name='api_connections'),
    path('api/connections/<uuid:connection_id>/', views.connection_detail_api, name='api_connection_detail'),
    path('api/connections/<uuid:connection_id>/test/', views.test_database_connection, name='api_test_connection'),
    path('api/connections/<uuid:connection_id>/tables/all/', views.get_all_connection_tables_api, name='api_get_all_connection_tables'),

    # --- Utility API Endpoints ---
    path('api/validate-sql/', views.validate_sql, name='api_validate_sql'),
    path('api/get-csrf-token/', views.get_csrf_token, name='api_get_csrf_token'),
    path('data-prep-modal-content/', report_views.data_prep_modal_content, name='data_prep_modal_content'),

    # --- New Report Building Endpoints ---
    path('api/build_report/', report_views.build_report_api, name='api_build_report'),

    # --- New Connection Management Endpoints ---
    path('api/get_db_connections/', report_views.get_connections_api, name='api_get_db_connections'),
    path('api/get_tables/', report_views.get_tables_api, name='api_get_tables'),
    path('api/get_columns_for_tables/', report_views.get_columns_for_tables_api, name='api_get_columns_for_tables'),

    # --- New Report Management Endpoints ---
    path('api/get_my_reports/', report_views.get_my_reports_api, name='api_get_my_reports'),
    path('api/save_report/', report_views.save_report_api, name='api_save_report'),
    path('api/get_report_config/<uuid:report_id>/', report_views.get_report_config_api, name='api_get_report_config'),
    path('api/update_report/<uuid:report_id>/', report_views.update_report_api, name='api_update_report'),
    path('api/users/list/', report_views.list_users_api, name='api_list_users'),
    path('api/reports/<uuid:report_id>/shares/', report_views.get_report_shares_api, name='api_get_report_shares'),
    path('api/reports/<uuid:report_id>/shares/update/', report_views.update_report_shares_api, name='api_update_report_shares'),
    path('api/reports/export/csv/', report_views.export_report_csv_api, name='api_export_csv'),

    # --- New Advanced Features Endpoints ---
    path('api/export_excel/', report_views.export_report_excel_api, name='api_export_excel'),

    # --- Data Model Endpoints ---
    path('data-model/', data_model_views.data_model_designer_view, name='data_model_designer'),
    path('data-model/api/test_connection/<uuid:connection_id>/', data_model_views.test_connection, name='test_connection'),
    path('data-model/api/suggest_joins/<uuid:connection_id>/', data_model_views.suggest_joins, name='suggest_joins'),
    path('data-model/api/validate_model/<uuid:connection_id>/', data_model_views.validate_model, name='validate_model'),
    path('api/model/get/<uuid:connection_id>/', data_model_views.get_data_model_api, name='api_get_data_model'),
    path('api/model/save/<uuid:connection_id>/', data_model_views.save_data_model_api, name='api_save_data_model'),
    path('api/model/get/<uuid:connection_id>/',  data_model_views.get_model_for_connection,  name='api_model_get'),
    path('api/model/save/<uuid:connection_id>/', data_model_views.save_model_for_connection, name='api_model_save'),
]

urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
