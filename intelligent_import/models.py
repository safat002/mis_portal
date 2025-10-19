# intelligent_import/models.py
"""
Intelligent Import System Models
Separate from existing MIS models to avoid conflicts
"""

import uuid
import json
import hashlib
from django.db import models
from django.utils import timezone
from django.core.validators import RegexValidator
from mis_app.models import User, ExternalConnection
from django.conf import settings
from django.db.models import JSONField

# --- REPORT TEMPLATES ---

class ReportTemplate(models.Model):
    OWNERSHIP_CHOICES = (("team", "Team"), ("private", "Private"))
    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    name = models.CharField(max_length=255, unique=True)
    description = models.TextField(blank=True)
    ownership_type = models.CharField(max_length=16, choices=OWNERSHIP_CHOICES, default="team")
    version = models.IntegerField(default=1)
    is_active = models.BooleanField(default=True)

    # detection helpers
    filename_patterns = models.JSONField(default=list, blank=True)
    header_patterns = models.JSONField(default=list, blank=True)

    # optional default landing table for convenience
    target_table = models.CharField(max_length=255, blank=True)

    # NEW: the headers list (what the template expects)
    fields = models.JSONField(default=list, blank=True)

    # NEW: header -> mapping rows
    mapping = models.JSONField(default=dict, blank=True)

    # NEW: schema proposals (accumulate until approved)
    schema_proposals = models.JSONField(default=list, blank=True)

    # audit
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="rt_created")
    updated_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True, related_name="rt_updated")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

class ReportTemplateHeader(models.Model):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    template = models.ForeignKey(ReportTemplate, on_delete=models.CASCADE, related_name="headers")
    source_header = models.CharField(max_length=255)                    # exactly as it appears in file
    target_table = models.CharField(max_length=255, blank=True)         # e.g. fact_sewing_production
    target_column = models.CharField(max_length=255, blank=True)        # e.g. production_qty
    data_type = models.CharField(max_length=64, blank=True)             # text, int, number, date
    is_required = models.BooleanField(default=False)
    default_value = models.CharField(max_length=255, blank=True)

    # master resolution (e.g., Unit Name -> unit_code)
    master_data_source = models.CharField(max_length=64, blank=True)    # "units", "buyers", "styles", ...
    master_output_field = models.CharField(max_length=64, blank=True)   # e.g. "unit_code"
    strict = models.BooleanField(default=False)                         # you chose auto-create pending => False
    depends_on = models.JSONField(default=list, blank=True)  # keep but we’ll avoid using it

    transform = models.CharField(max_length=128, blank=True)            # e.g., "trim|upper", "date:%Y-%m-%d"

    class Meta:
        unique_together = [("template", "source_header")]

# --- PENDING MASTERS (generic) ---

class PendingMaster(models.Model):
    ENTITY_CHOICES = (
        ("units", "Units"),
        ("buyers", "Buyers"),
        ("styles", "Styles"),
        ("lines", "Lines"),
        ("vendors", "Vendors"),
        ("colors", "Colors"),
        ("airports", "Airports"),
    )
    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    entity = models.CharField(max_length=32, choices=ENTITY_CHOICES)
    payload = JSONField()                  # raw values detected (e.g., {"unit_name": "Unit-1"})
    status = models.CharField(max_length=16, default="pending")  # pending/approved/rejected
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

# --- ALIAS (minimal, only Units to start; extend similarly for Buyers/Styles later) ---

class UnitAlias(models.Model):
    id = models.UUIDField(primary_key=True, editable=False, default=uuid.uuid4)
    unit_name_alias = models.CharField(max_length=255, unique=True)
    unit_code = models.CharField(max_length=64)  # canonical code


class ImportSession(models.Model):
    """
    Track individual intelligent import sessions.
    """
    STATUS_CHOICES = [
        ('file_uploaded', 'File Uploaded'),
        ('analyzing', 'Analyzing File'),
        ('template_suggested', 'Template Suggested'),
        ('mapping_defined', 'Column Mapping Defined'),
        ('mapping_approved', 'Mapping Approved'),
        ('data_validated', 'Data Validated'),
        ('pending_approval', 'Pending Final Approval'),
        ('approved', 'Approved for Import'),
        ('importing_data', 'Importing Data'),
        ('completed', 'Completed'),
        ('failed', 'Failed'),
        ('cancelled', 'Cancelled'),
        ('rolled_back', 'Rolled Back'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Session ownership
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='intelligent_import_sessions')
    connection = models.ForeignKey(ExternalConnection, on_delete=models.CASCADE)
    
    # File information
    original_filename = models.CharField(max_length=255)
    temp_filename = models.CharField(max_length=255, blank=True)
    file_size = models.BigIntegerField(help_text="File size in bytes")
    file_hash = models.CharField(max_length=128, blank=True, help_text="File content hash")
    
    # Template and mapping
    report_template = models.ForeignKey(ReportTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    detected_template = models.CharField(max_length=200, blank=True)
    # Explicit target table for this session (may override template)
    target_table = models.CharField(max_length=255, blank=True)
    
    # Session state
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='file_uploaded')
    header_row = models.IntegerField(default=0)
    total_rows = models.IntegerField(default=0)
    
    # Analysis and mapping details
    analysis_summary = models.JSONField(default=dict, help_text="Key results from the file analyzer")
    column_mapping = models.JSONField(default=dict, help_text="File columns to schema fields mapping for this session")
    
    # Data validation results
    validation_results = models.JSONField(default=dict, help_text="Data validation results")
    preview_data = models.JSONField(default=dict, help_text="Sample processed data for preview")
    
    # Master data handling
    master_data_suggestions = models.JSONField(default=dict, help_text="Suggested new master data")
    
    # Approval workflow
    approved_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True, 
        related_name='approved_intelligent_imports'
    )
    approved_at = models.DateTimeField(null=True, blank=True)
    approval_comments = models.TextField(blank=True)
    
    # Comments and notes
    user_comments = models.TextField(blank=True)
    system_notes = models.JSONField(default=list, help_text="System processing notes")
    
    # Import results
    imported_record_count = models.IntegerField(default=0)
    import_progress = models.IntegerField(default=0)
    # Optional overall import mode (kept for compatibility; per‑header mode preferred)
    import_mode = models.CharField(max_length=20, blank=True, default='auto')
    
    # Timestamps
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    
    class Meta:
        db_table = 'intelligent_import_sessions'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['user', 'status']),
            models.Index(fields=['file_hash']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"{self.original_filename} - {self.status}"
    
    def add_system_note(self, message, level='info'):
        """Add timestamped system note"""
        note = {
            'timestamp': timezone.now().isoformat(),
            'level': level,
            'message': message
        }
        self.system_notes.append(note)
        self.save(update_fields=['system_notes', 'updated_at'])
    
    def generate_file_hash(self, file_content):
        """Generate hash for deduplication"""
        content_hash = hashlib.sha256(file_content).hexdigest()
        metadata = {
            'filename': self.original_filename,
            'user_id': str(self.user.id),
            'connection_id': str(self.connection.id)
        }
        metadata_hash = hashlib.md5(json.dumps(metadata, sort_keys=True).encode()).hexdigest()
        return f"{content_hash}_{metadata_hash}"


class MasterDataCandidate(models.Model):
    """
    Tracks new master data entries that need user approval before being added to the main database.
    """
    STATUS_CHOICES = [
        ('pending', 'Pending Approval'),
        ('approved', 'Approved'),
        ('rejected', 'Rejected'),
    ]

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    import_session = models.ForeignKey(ImportSession, on_delete=models.CASCADE, related_name='master_data_candidates')
    target_master_table = models.CharField(max_length=100, help_text="e.g., mis_app_buyer")
    proposed_value = models.CharField(max_length=255, help_text="The new value from the file (e.g., a new buyer name)")
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending')
    
    # Approval details
    reviewed_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    reviewed_at = models.DateTimeField(null=True, blank=True)
    reviewer_comments = models.TextField(blank=True)

    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'intelligent_import_master_data_candidates'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['import_session', 'status']),
            models.Index(fields=['target_master_table']),
        ]

    def __str__(self):
        return f"'{self.proposed_value}' for {self.target_master_table} ({self.status})"


class ImportAuditLog(models.Model):
    """
    Audit log for all actions during the intelligent import process.
    """
    ACTION_CHOICES = [
        ('data_insert', 'Insert Data'),
        ('data_update', 'Update Data'),
        ('master_data_create', 'Create Master Data'),
        ('import_rollback', 'Rollback Import'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Related session and template
    import_session = models.ForeignKey(ImportSession, on_delete=models.CASCADE, related_name='import_audit_logs')
    report_template = models.ForeignKey(ReportTemplate, on_delete=models.SET_NULL, null=True, blank=True)
    
    # Action details
    action = models.CharField(max_length=20, choices=ACTION_CHOICES)
    table_name = models.CharField(max_length=100)
    
    # SQL or details of the action
    details = models.TextField(help_text="Details of the action, e.g., SQL statement or summary")
    
    # Execution details
    executed_by = models.ForeignKey(User, on_delete=models.CASCADE)
    approved_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='approved_import_actions'
    )
    
    # Results
    success = models.BooleanField(default=True)
    error_message = models.TextField(blank=True)
    affected_rows = models.IntegerField(default=0)
    execution_time_ms = models.IntegerField(default=0)
    
    # Audit fields
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'intelligent_import_audit_log'
        ordering = ['-created_at']
        indexes = [
            models.Index(fields=['import_session', 'action']),
            models.Index(fields=['table_name']),
            models.Index(fields=['created_at']),
        ]
    
    def __str__(self):
        return f"{self.action} on {self.table_name} by {self.executed_by.username}"


class DataLineage(models.Model):
    """
    Track lineage of imported data for audit and rollback
    """
    OPERATION_CHOICES = [
        ('insert', 'Insert'),
        ('update', 'Update'),
        ('skip', 'Skipped'),
        ('error', 'Error'),
    ]
    
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    
    # Related session
    import_session = models.ForeignKey(ImportSession, on_delete=models.CASCADE, related_name='data_lineage')
    
    # Target record information
    target_table = models.CharField(max_length=100)
    target_record_id = models.CharField(max_length=100, help_text="Primary key of imported record")
    
    # Source data
    source_row_number = models.IntegerField(help_text="Row number in source file")
    original_data = models.JSONField(help_text="Original data from file")
    transformed_data = models.JSONField(help_text="Final data inserted")
    
    # Operation details
    operation = models.CharField(max_length=10, choices=OPERATION_CHOICES, default='insert')
    
    # Rollback information
    is_rolled_back = models.BooleanField(default=False)
    rolled_back_at = models.DateTimeField(null=True, blank=True)
    rolled_back_by = models.ForeignKey(
        User, on_delete=models.SET_NULL, null=True, blank=True,
        related_name='rolled_back_data'
    )
    
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        db_table = 'intelligent_import_data_lineage'
        indexes = [
            models.Index(fields=['import_session', 'target_table']),
            models.Index(fields=['target_table', 'target_record_id']),
            models.Index(fields=['is_rolled_back']),
        ]
    
    def __str__(self):
        return f"{self.target_table}[{self.target_record_id}] from session {self.import_session_id}"


class SystemConfiguration(models.Model):
    """
    System-wide configuration for intelligent import
    """
    # File processing settings
    max_file_size_mb = models.IntegerField(default=100)
    chunk_size = models.IntegerField(default=1000)
    session_timeout_hours = models.IntegerField(default=24)
    
    # Schema creation settings
    enforce_naming_conventions = models.BooleanField(default=True)
    auto_create_indexes = models.BooleanField(default=True)
    require_schema_approval = models.BooleanField(default=True)
    
    # Data validation settings
    similarity_threshold = models.FloatField(default=0.2)
    duplicate_check_enabled = models.BooleanField(default=True)
    
    # Audit and retention
    retain_temp_files_days = models.IntegerField(default=90)
    retain_sessions_days = models.IntegerField(default=365)
    retain_audit_logs_years = models.IntegerField(default=7)
    
    # AI/ML features
    enable_ai_suggestions = models.BooleanField(default=True)
    enable_smart_column_matching = models.BooleanField(default=True)
    
    # Notification settings
    notification_channels = models.JSONField(
        default=list,
        help_text="Enabled notification channels"
    )
    
    # System metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    updated_by = models.ForeignKey(User, on_delete=models.CASCADE)
    
    class Meta:
        db_table = 'intelligent_import_system_config'
    
    @classmethod
    def get_config(cls):
        """Get system configuration, creating default if not exists"""
        try:
            return cls.objects.get()
        except cls.DoesNotExist:
            return cls.objects.create(
                updated_by_id=1  # Assume admin user with ID 1
            )


# Data type choices for schema definition
DATA_TYPE_CHOICES = [
    ('TEXT', 'Text (VARCHAR)'),
    ('INTEGER', 'Integer'),
    ('DECIMAL', 'Decimal/Float'),
    ('DATE', 'Date'),
    ('DATETIME', 'Date & Time'),
    ('BOOLEAN', 'Yes/No (Boolean)'),
    ('ENUM', 'Dropdown List (Enum)'),
    ('JSON', 'JSON Data'),
]

# Constraint type choices
CONSTRAINT_CHOICES = [
    ('NOT_NULL', 'Required (NOT NULL)'),
    ('UNIQUE', 'Unique'),
    ('PRIMARY_KEY', 'Primary Key'),
    ('FOREIGN_KEY', 'Foreign Key'),
    ('CHECK', 'Check Constraint'),
    ('INDEX', 'Database Index'),
]
