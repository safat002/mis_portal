# intelligent_import/migrations/0001_initial.py
"""
Django migration for Intelligent Import System
"""

from django.db import migrations, models
import django.db.models.deletion
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('mis_app', '0001_initial'),  # Adjust based on your existing migrations
    ]

    operations = [
        # Schema Template
        migrations.CreateModel(
            name='SchemaTemplate',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('name', models.CharField(help_text="Template name (e.g., 'Export Order Report')", max_length=200)),
                ('description', models.TextField(blank=True, help_text='Detailed description')),
                ('ownership_type', models.CharField(choices=[('personal', 'Personal Template'), ('team', 'Team Template'), ('global', 'Global Template')], default='team', max_length=20)),
                ('version', models.IntegerField(default=1, help_text='Template version')),
                ('is_active', models.BooleanField(default=True)),
                ('filename_patterns', models.JSONField(default=list, help_text='Patterns to auto-detect this template from filenames')),
                ('header_patterns', models.JSONField(default=list, help_text='Header patterns to identify this report type')),
                ('schema_definition', models.JSONField(default=dict, help_text='Complete schema definition including tables, columns, relationships')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('created_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='created_schema_templates', to='mis_app.user')),
                ('updated_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='updated_schema_templates', to='mis_app.user')),
            ],
            options={
                'db_table': 'intelligent_import_schema_templates',
                'ordering': ['-created_at'],
            },
        ),

        # Import Session
        migrations.CreateModel(
            name='ImportSession',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('original_filename', models.CharField(max_length=255)),
                ('temp_filename', models.CharField(blank=True, max_length=255)),
                ('file_size', models.BigIntegerField(help_text='File size in bytes')),
                ('file_hash', models.CharField(blank=True, help_text='File content hash', max_length=128)),
                ('detected_template', models.CharField(blank=True, max_length=200)),
                ('status', models.CharField(choices=[('file_uploaded', 'File Uploaded'), ('analyzing', 'Analyzing File'), ('template_suggested', 'Template Suggested'), ('schema_defined', 'Schema Defined'), ('schema_approved', 'Schema Approved'), ('data_validated', 'Data Validated'), ('pending_approval', 'Pending Final Approval'), ('approved', 'Approved for Import'), ('creating_schema', 'Creating Database Schema'), ('importing_data', 'Importing Data'), ('completed', 'Completed'), ('failed', 'Failed'), ('cancelled', 'Cancelled'), ('rolled_back', 'Rolled Back')], default='file_uploaded', max_length=20)),
                ('header_row', models.IntegerField(default=0)),
                ('total_rows', models.IntegerField(default=0)),
                ('proposed_schema', models.JSONField(default=dict, help_text='AI-suggested schema definition')),
                ('final_schema', models.JSONField(default=dict, help_text='User-approved final schema')),
                ('created_tables', models.JSONField(default=list, help_text='List of tables created in this session')),
                ('column_mapping', models.JSONField(default=dict, help_text='File columns to schema fields mapping')),
                ('validation_results', models.JSONField(default=dict, help_text='Data validation results')),
                ('preview_data', models.JSONField(default=dict, help_text='Sample processed data for preview')),
                ('master_data_suggestions', models.JSONField(default=dict, help_text='Suggested new master data')),
                ('approval_comments', models.TextField(blank=True)),
                ('user_comments', models.TextField(blank=True)),
                ('system_notes', models.JSONField(default=list, help_text='System processing notes')),
                ('imported_record_count', models.IntegerField(default=0)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('started_at', models.DateTimeField(blank=True, null=True)),
                ('completed_at', models.DateTimeField(blank=True, null=True)),
                ('approved_at', models.DateTimeField(blank=True, null=True)),
                ('approved_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='approved_intelligent_imports', to='mis_app.user')),
                ('connection', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='mis_app.externalconnection')),
                ('schema_template', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='intelligent_import.schematemplate')),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='intelligent_import_sessions', to='mis_app.user')),
            ],
            options={
                'db_table': 'intelligent_import_sessions',
                'ordering': ['-created_at'],
            },
        ),

        # Schema Audit Log
        migrations.CreateModel(
            name='SchemaAuditLog',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('action', models.CharField(choices=[('create_table', 'Create Table'), ('alter_table', 'Alter Table'), ('create_index', 'Create Index'), ('create_enum', 'Create Enum Type'), ('drop_table', 'Drop Table'), ('rollback_schema', 'Rollback Schema Changes')], max_length=20)),
                ('table_name', models.CharField(max_length=100)),
                ('sql_statement', models.TextField(help_text='Actual SQL executed')),
                ('sql_preview', models.TextField(blank=True, help_text='SQL shown to user for approval')),
                ('success', models.BooleanField(default=True)),
                ('error_message', models.TextField(blank=True)),
                ('affected_rows', models.IntegerField(default=0)),
                ('execution_time_ms', models.IntegerField(default=0)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('approved_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='approved_schema_changes', to='mis_app.user')),
                ('executed_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='mis_app.user')),
                ('import_session', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='schema_audit_logs', to='intelligent_import.importsession')),
                ('schema_template', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='intelligent_import.schematemplate')),
            ],
            options={
                'db_table': 'intelligent_import_schema_audit_log',
                'ordering': ['-created_at'],
            },
        ),

        # Data Lineage
        migrations.CreateModel(
            name='DataLineage',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('target_table', models.CharField(max_length=100)),
                ('target_record_id', models.CharField(help_text='Primary key of imported record', max_length=100)),
                ('source_row_number', models.IntegerField(help_text='Row number in source file')),
                ('original_data', models.JSONField(help_text='Original data from file')),
                ('transformed_data', models.JSONField(help_text='Final data inserted')),
                ('operation', models.CharField(choices=[('insert', 'Insert'), ('update', 'Update'), ('skip', 'Skipped'), ('error', 'Error')], default='insert', max_length=10)),
                ('is_rolled_back', models.BooleanField(default=False)),
                ('rolled_back_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('import_session', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='data_lineage', to='intelligent_import.importsession')),
                ('rolled_back_by', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, related_name='rolled_back_data', to='mis_app.user')),
            ],
            options={
                'db_table': 'intelligent_import_data_lineage',
            },
        ),

        # System Configuration
        migrations.CreateModel(
            name='SystemConfiguration',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('max_file_size_mb', models.IntegerField(default=100)),
                ('chunk_size', models.IntegerField(default=1000)),
                ('session_timeout_hours', models.IntegerField(default=24)),
                ('enforce_naming_conventions', models.BooleanField(default=True)),
                ('auto_create_indexes', models.BooleanField(default=True)),
                ('require_schema_approval', models.BooleanField(default=True)),
                ('similarity_threshold', models.FloatField(default=0.2)),
                ('duplicate_check_enabled', models.BooleanField(default=True)),
                ('retain_temp_files_days', models.IntegerField(default=90)),
                ('retain_sessions_days', models.IntegerField(default=365)),
                ('retain_audit_logs_years', models.IntegerField(default=7)),
                ('enable_ai_suggestions', models.BooleanField(default=True)),
                ('enable_smart_column_matching', models.BooleanField(default=True)),
                ('notification_channels', models.JSONField(default=list, help_text='Enabled notification channels')),
                ('created_at', models.DateTimeField(auto_now_add=True)),
                ('updated_at', models.DateTimeField(auto_now=True)),
                ('updated_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='mis_app.user')),
            ],
            options={
                'db_table': 'intelligent_import_system_config',
            },
        ),

        # Add indexes for performance
        migrations.AddIndex(
            model_name='importsession',
            index=models.Index(fields=['user', 'status'], name='ii_sess_user_status_idx'),
        ),
        migrations.AddIndex(
            model_name='importsession',
            index=models.Index(fields=['file_hash'], name='ii_sess_file_hash_idx'),
        ),
        migrations.AddIndex(
            model_name='importsession',
            index=models.Index(fields=['created_at'], name='ii_sess_created_idx'),
        ),
        migrations.AddIndex(
            model_name='schemaauditlog',
            index=models.Index(fields=['import_session', 'action'], name='ii_audit_session_action_idx'),
        ),
        migrations.AddIndex(
            model_name='schemaauditlog',
            index=models.Index(fields=['table_name'], name='ii_audit_table_idx'),
        ),
        migrations.AddIndex(
            model_name='schemaauditlog',
            index=models.Index(fields=['created_at'], name='ii_audit_created_idx'),
        ),
        migrations.AddIndex(
            model_name='datalineage',
            index=models.Index(fields=['import_session', 'target_table'], name='ii_lineage_session_table_idx'),
        ),
        migrations.AddIndex(
            model_name='datalineage',
            index=models.Index(fields=['target_table', 'target_record_id'], name='ii_lineage_target_idx'),
        ),
        migrations.AddIndex(
            model_name='datalineage',
            index=models.Index(fields=['is_rolled_back'], name='ii_lineage_rollback_idx'),
        ),

        # Add unique constraints
        migrations.AddConstraint(
            model_name='schematemplate',
            constraint=models.UniqueConstraint(fields=['name', 'version'], name='unique_template_version'),
        ),
    ]