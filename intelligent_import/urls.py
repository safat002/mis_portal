from django.urls import path
from . import views
from mis_app import data_views

app_name = 'intelligent_import'

urlpatterns = [
    # Dashboard and main views
    path('', views.intelligent_import_dashboard, name='dashboard'),

    # API endpoints
    path('api/sessions/', views.list_sessions, name='list_sessions'),
    path('api/upload-analyze/', views.upload_and_analyze, name='upload_analyze'),
    path('api/define-mapping/', views.define_column_mapping, name='define_mapping'),
    path('api/validate-preview-data/', views.validate_and_preview_data, name='validate_preview_data'),
    path('api/approve-import/', views.approve_and_import, name='approve_import'),
    path('api/session/<uuid:session_id>/duplicates/decisions/', views.save_duplicate_decisions, name='save_duplicate_decisions'),
    path('api/session/<uuid:session_id>/final-review/', views.final_review_summary, name='final_review_summary'),
    path('api/session/<uuid:session_id>/status/', views.get_session_status, name='get_session_status'),
    path('api/session/<uuid:session_id>/request-approval/', views.request_approval, name='request_approval'),
    path('api/session/<uuid:session_id>/reopen-mapping/', views.reopen_mapping, name='reopen_mapping'),
    # In-app notifications (toast integration)
    path('api/notifications/unread/', views.unread_notifications_api, name='unread_notifications_api'),
    path('api/notifications/mark-read/', views.mark_notifications_read_api, name='mark_notifications_read_api'),

    # Template API endpoints
    path('api/report-templates/', views.report_templates_api, name='report_templates_api'),
    path("api/report-templates/<uuid:template_id>/", views.report_template_detail_api, name="report_template_detail_api"),
    path("api/report-templates/<uuid:template_id>/fields/", views.report_template_fields_api, name="report_template_fields_api"),
    path("api/report-templates/<uuid:template_id>/mapping/", views.report_template_mapping_api, name="report_template_mapping_api"),
    path("api/report-templates/<uuid:template_id>/approve-schema/", views.report_template_approve_schema_api, name="report_template_approve_schema_api"),
    # Generic schema modification
    path('api/modify-schema/plan/', views.modify_schema_plan_api, name='modify_schema_plan_api'),
    path('api/modify-schema/apply/', views.modify_schema_apply_api, name='modify_schema_apply_api'),
    path('api/suggest-mapping/', views.suggest_mapping_api, name='suggest_mapping_api'),
    path('api/session/<uuid:session_id>/report-template/', views.set_session_report_template_api, name='set_session_report_template'),

    # Session management
    path('api/session/<uuid:session_id>/enter/', views.enter_session, name='enter_session'),
    path('api/session/<uuid:session_id>/cancel/', views.cancel_session, name='cancel_session'),
    path('api/session/<uuid:session_id>/delete/', views.delete_session, name='delete_session'),

    # Additional session views
    path('session/<uuid:session_id>/', views.session_detail, name='session_detail'),
    path('session/<uuid:session_id>/approve-master-data/', views.manage_master_data_candidates, name='manage_master_data'),

     # Metadata (tables, columns)
    path(
        "api/metadata/tables/",
        views.metadata_tables_api,
        name="metadata_tables_api",
    ),
    path(
        "api/metadata/tables/<str:schema>/<str:table>/columns/",
        views.metadata_table_columns_api,
        name="metadata_table_columns_api",
    ),

     # Use the selected external connection for tables/columns
    path(
        "api/connections/<uuid:connection_id>/tables/",
        data_views.get_visible_tables_for_connection,
        name="ii_visible_tables_for_connection",
    ),
    path(
        "api/connections/<uuid:connection_id>/tables/<str:table_name>/columns/",
        data_views.get_columns_for_table,
        name="ii_columns_for_table",
    ),
]
