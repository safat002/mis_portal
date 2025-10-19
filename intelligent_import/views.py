# intelligent_import/views.py
"""
Django Views for Intelligent Import System
Handles schema-driven import with dynamic table creation
"""

from difflib import SequenceMatcher
import os
import json
import logging
import math
from datetime import datetime, date, time
from decimal import Decimal
from typing import Dict, List, Any, Optional  # Add List here
from django.conf import settings
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import JsonResponse
from django.db.models import Count
from django.shortcuts import get_object_or_404, render, redirect
from django.utils import timezone
from django.utils.text import get_valid_filename
from django.views.decorators.http import require_http_methods, require_POST
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET
from django.views.decorators.cache import never_cache
from sqlalchemy import create_engine, text
from .services.master_data_service import plan_schema_changes, apply_schema_changes
from .naming_policy import normalize_snake, table_name as np_table_name
from django.db import IntegrityError
from django.db import connection
try:
    from .services.schema_analyzer import SchemaAnalyzer  # same app folder
except Exception:
    SchemaAnalyzer = None

def _is_manager(user):
    return getattr(user, "user_type", None) in ("Admin", "Moderator") or user.is_staff or user.is_superuser

@login_required
@require_http_methods(["GET","POST"])
def report_templates_api(request):
    if request.method == "GET":
        qs = ReportTemplate.objects.all().order_by("name")
        data = [{"id": str(t.id), "name": t.name, "description": t.description or "", "is_active": bool(t.is_active)} for t in qs]
        return JsonResponse({"success": True, "templates": data})
    # POST
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    try:
        payload = json.loads(request.body or "{}")
        name = (payload.get("name") or "").strip()
        if not name:
            return JsonResponse({"success": False, "error": "Name required"}, status=400)
        t = ReportTemplate.objects.create(name=name, description=(payload.get("description") or "").strip(), is_active=True)
        return JsonResponse({"success": True, "id": str(t.id), "name": t.name})
    except IntegrityError:
        return JsonResponse({"success": False, "error": "name_conflict"}, status=409)
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_http_methods(["PUT","DELETE"])
def report_template_detail_api(request, template_id):
    try:
        t = ReportTemplate.objects.get(id=template_id)
    except ReportTemplate.DoesNotExist:
        return JsonResponse({"success": False, "error": "Not found"}, status=404)
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    if request.method == "DELETE":
        t.delete()
        return JsonResponse({"success": True})
    # PUT
    try:
        payload = json.loads(request.body or "{}")
        if "name" in payload:
            t.name = (payload["name"] or "").strip()
        if "description" in payload:
            t.description = payload["description"] or ""
        if "is_active" in payload:
            t.is_active = bool(payload["is_active"])
        t.save()
        return JsonResponse({"success": True})
    except IntegrityError:
        return JsonResponse({"success": False, "error": "name_conflict"}, status=409)
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_http_methods(["GET","PUT"])
def report_template_fields_api(request, template_id):
    try:
        t = ReportTemplate.objects.get(id=template_id)
    except ReportTemplate.DoesNotExist:
        return JsonResponse({"success": False, "error": "Not found"}, status=404)

    if request.method == "GET":
        return JsonResponse({"success": True, "fields": t.fields or []})

    # PUT
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    try:
        payload = json.loads(request.body or "{}")
        fields = payload.get("fields")
        if not isinstance(fields, list):
            return JsonResponse({"success": False, "error": "fields must be a list"}, status=400)
        # normalize to snake? we keep original header text here; mapping will set actual db column names.
        t.fields = [str(x).strip() for x in fields if str(x).strip()]
        t.save()
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_http_methods(["GET","PUT"])
def report_template_mapping_api(request, template_id):
    try:
        t = ReportTemplate.objects.get(id=template_id)
    except ReportTemplate.DoesNotExist:
        return JsonResponse({"success": False, "error": "Not found"}, status=404)

    if request.method == "GET":
        return JsonResponse({
            "success": True,
            "mapping": t.mapping or {},
            "schema_proposals": t.schema_proposals or []
        })

    # PUT (save mapping + stage schema proposals)
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    try:
        payload = json.loads(request.body or "{}")
        mapping = payload.get("mapping") or {}
        proposals = []

        # Build proposals from "New Table..." / "New Column..." choices
        table_client_ids = {}  # map client temp id -> final (resolved by planner)
        for hdr, m in mapping.items():
            if m.get("create_table"):
                # { "role": "fact|ref", "label": "User input", "client_id": "guid-for-client" }
                role = m["create_table"].get("role") or "fact"
                label = m["create_table"].get("label") or hdr
                client_id = m["create_table"].get("client_id") or f"t_{hdr}"
                proposals.append({"action": "create_table", "role": role, "label": label, "client_id": client_id})
                table_client_ids[client_id] = None  # will be filled post-plan

        for hdr, m in mapping.items():
            if m.get("create_column"):
                # { "table": "... or None if table_client_id used", "table_client_id": "...", "label": "...", "type": "TEXT|..." }
                proposals.append({
                    "action": "add_column",
                    "table": m["create_column"].get("table"),
                    "table_client_id": m["create_column"].get("table_client_id"),
                    "label": m["create_column"].get("label") or hdr,
                    "type": (m["create_column"].get("type") or "TEXT").upper()
                })

        t.mapping = mapping
        t.schema_proposals = proposals
        t.version += 1
        t.save()
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_http_methods(["POST"])
def report_template_approve_schema_api(request, template_id):
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    try:
        t = ReportTemplate.objects.get(id=template_id)
    except ReportTemplate.DoesNotExist:
        return JsonResponse({"success": False, "error": "Not found"}, status=404)

    proposals = t.schema_proposals or []
    plan = plan_schema_changes(proposals)
    try:
        apply_schema_changes(plan)  # transactional
    except Exception as e:
        return JsonResponse({"success": False, "error": f"DDL failed: {e}"}, status=400)

    # clear proposals after success
    t.schema_proposals = []
    t.save()
    return JsonResponse({"success": True, "summary": plan.get("summary", []), "name_map": plan.get("name_map", {})})

@login_required
@require_POST
def save_duplicate_decisions(request, session_id):
    """Persist per-row duplicate decisions into the session.analysis_summary.
    Expects JSON: { "action_by_row": {"<index>": "approve|skip", ...} }
    """
    try:
        session = ImportSession.objects.get(id=session_id, user=request.user)
    except ImportSession.DoesNotExist:
        return JsonResponse({"success": False, "error": "Session not found"}, status=404)
    try:
        payload = json.loads(request.body or "{}")
        decisions = payload.get("action_by_row") or {}
        if not isinstance(decisions, dict):
            return JsonResponse({"success": False, "error": "action_by_row must be an object"}, status=400)
        analysis = session.analysis_summary or {}
        dup = analysis.get("duplicate_decisions") or {}
        dup["action_by_row"] = decisions
        analysis["duplicate_decisions"] = dup
        session.analysis_summary = analysis
        session.save(update_fields=["analysis_summary", "updated_at"])
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_GET
def final_review_summary(request, session_id):
    """Return a summary of the final review for a session (counts by operation)."""
    try:
        session = ImportSession.objects.get(id=session_id, user=request.user)
    except ImportSession.DoesNotExist:
        return JsonResponse({"success": False, "error": "Session not found"}, status=404)
    counts = (
        DataLineage.objects.filter(import_session=session)
        .values("operation")
        .annotate(count=Count("id"))
    )
    summary = {row["operation"]: row["count"] for row in counts}
    return JsonResponse({
        "success": True,
        "status": session.status,
        "imported_record_count": session.imported_record_count,
        "progress": session.import_progress,
        "operations": summary,
    })

@login_required
@require_POST
def request_approval(request, session_id):
    """Mark a session as pending final approval with optional comments."""
    try:
        session = ImportSession.objects.get(id=session_id, user=request.user)
    except ImportSession.DoesNotExist:
        return JsonResponse({"success": False, "error": "Session not found"}, status=404)
    try:
        payload = json.loads(request.body or "{}")
        comments = payload.get("comments", "")
        session.status = "pending_approval"
        session.approval_comments = comments
        session.save(update_fields=["status", "approval_comments", "updated_at"])
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_POST
def reopen_mapping(request, session_id):
    """Reopen mapping phase for a session."""
    try:
        session = ImportSession.objects.get(id=session_id, user=request.user)
    except ImportSession.DoesNotExist:
        return JsonResponse({"success": False, "error": "Session not found"}, status=404)
    session.status = "mapping_defined"
    session.save(update_fields=["status", "updated_at"])
    return JsonResponse({"success": True})

@login_required
@require_GET
def unread_notifications_api(request):
    """Return a best-effort list of unread notifications (placeholder)."""
    return JsonResponse({"success": True, "notifications": []})

@login_required
@require_POST
def mark_notifications_read_api(request):
    """Mark notifications as read (placeholder)."""
    return JsonResponse({"success": True})

@login_required
@require_POST
def modify_schema_plan_api(request):
    """Plan DDL changes from proposals: delegates to master_data_service.plan_schema_changes."""
    try:
        payload = json.loads(request.body or "{}")
        proposals = payload.get("proposals") or []
        plan = plan_schema_changes(proposals)
        return JsonResponse({"success": True, "plan": plan})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_POST
def modify_schema_apply_api(request):
    """Apply DDL changes from a plan: delegates to master_data_service.apply_schema_changes."""
    try:
        payload = json.loads(request.body or "{}")
        plan = payload.get("plan") or {}
        apply_schema_changes(plan)
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

from .models import (
    ReportTemplate, ImportSession, ReportTemplateHeader, SystemConfiguration,
    ImportAuditLog, DataLineage, MasterDataCandidate
)
from mis_app.models import User, ExternalConnection
from mis_app.permissions import PermissionManager
from .services.data_processing import (
    process_and_validate_data,
    execute_data_import,
    get_table_schema_from_db,
)
from collections import Counter

AUTO_UNIQUE_KEYS = {"id", "code", "number", "no", "ref", "uid", "guid"}

def _estimate_dup_ratio(df, cols):
    if not cols: return 0.0
    if any(c not in df.columns for c in cols): return 0.0
    tuples = list(map(tuple, df[cols].astype(str).fillna("").values.tolist()))
    n = len(tuples)
    if n == 0: return 0.0
    seen = Counter(tuples)
    dups = sum(v - 1 for v in seen.values() if v > 1)
    return dups / max(1, n)

def _infer_key(df):
    cols_lower = [c.lower() for c in df.columns]
    # prefer single strong identifier
    for c in cols_lower:
      if any(k in c for k in AUTO_UNIQUE_KEYS):
        return [df.columns[cols_lower.index(c)]]
    # try 2-col composite with likely identifiers
    candidates = [df.columns[i] for i,c in enumerate(cols_lower) if any(k in c for k in AUTO_UNIQUE_KEYS)]
    if len(candidates) >= 2:
        return candidates[:2]
    return []

def choose_import_strategy(df_sample, target_table_exists, target_table_rows, mapping, explicit=None):
    """
    Returns one of: 'append' | 'replace' | 'upsert'
    """
    if explicit in {"append", "replace", "upsert"}:
        return explicit

    if not target_table_exists:
        return "append"

    if target_table_rows == 0:
        return "append"

    key = _infer_key(df_sample)
    dup_ratio = _estimate_dup_ratio(df_sample, key) if key else 0.0

    # If we have a plausible key and many duplicates in incoming data,
    # we likely want to upsert to avoid blowing away historical rows.
    if key and dup_ratio >= 0.15:
        return "upsert"

    # If we don’t have a key and columns look identical to target table,
    # large incoming vs small existing may favor replace; keep conservative:
    if len(df_sample) > 0 and target_table_rows > 0 and (len(df_sample) * 3) < target_table_rows:
        # small patch loads → append
        return "append"

    # default fallback
    return "append"

def _infer_target_table(template):
    """Pick a target table for a template:
       1) use template.target_table if set
       2) else use the most common target_table among headers (prefer tables starting with 'fact_')"""
    if not template:
        return ""
    if getattr(template, "target_table", ""):
        return template.target_table
    try:
        headers = list(template.headers.all())
    except Exception:
        headers = []
    tbls = [h.target_table for h in headers if getattr(h, "target_table", "")]
    if not tbls:
        return ""
    fact_tbls = [t for t in tbls if t.startswith("fact_")]
    pool = fact_tbls or tbls
    return Counter(pool).most_common(1)[0][0]




logger = logging.getLogger(__name__)



try:
    import numpy as np
    import pandas as pd
except ImportError:  # pragma: no cover
    np = None
    pd = None


def _convert_to_builtin(value):
    """Recursively convert numpy/pandas objects to JSON-serializable builtins.

    Also normalizes NaN/Inf to None to satisfy strict JSON constraints
    (e.g., SQLite JSON_VALID or MySQL JSON) and avoids invalid tokens.
    """
    # Normalize pandas NA/NaN first
    if 'pd' in globals() and pd is not None:
        try:
            # pd.isna handles pd.NA, np.nan, NaT
            if pd.isna(value):
                return None
        except Exception:
            pass

    # Date/time
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()

    # Decimal
    if isinstance(value, Decimal):
        try:
            f = float(value)
            return f if math.isfinite(f) else None
        except Exception:
            return None

    # Containers
    if isinstance(value, dict):
        return {str(key): _convert_to_builtin(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_convert_to_builtin(item) for item in value]

    # Pandas timestamp
    if 'pd' in globals() and pd is not None and isinstance(value, pd.Timestamp):
        return value.isoformat()

    # Numpy scalars/arrays
    if 'np' in globals() and np is not None:
        # Handle arrays
        if isinstance(value, np.ndarray):
            return [_convert_to_builtin(v) for v in value.tolist()]
        # Handle numeric/bool generics explicitly
        for cls, caster in (
            (getattr(np, 'integer', ()), int),
            (getattr(np, 'floating', ()), float),
            (getattr(np, 'bool_', None), bool),
        ):
            if cls and isinstance(value, cls):
                try:
                    v = caster(value)
                except Exception:
                    return None
                if isinstance(v, float) and not math.isfinite(v):
                    return None
                return v
        # Generic fallback
        if isinstance(value, np.generic):
            try:
                v = value.item()
            except Exception:
                return None
            if isinstance(v, float) and not math.isfinite(v):
                return None
            return v

    # Lists through .tolist()
    if hasattr(value, "tolist"):
        try:
            out = value.tolist()
            return _convert_to_builtin(out)
        except Exception:  # pragma: no cover - fallback to item() next
            pass

    # Scalar through .item()
    if hasattr(value, "item"):
        try:
            v = value.item()
            if isinstance(v, float) and not math.isfinite(v):
                return None
            return v
        except Exception:  # pragma: no cover
            pass

    # Plain float finiteness check
    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def intelligent_import_permission_required(permission_level='upload'):
    """Decorator for intelligent import permissions"""
    def decorator(view_func):
        from functools import wraps
        
        @wraps(view_func)
        @login_required
        def _wrapped(request, *args, **kwargs):
            # Check if user has intelligent import permissions
            if not PermissionManager.check_user_permission(
                request.user, 'intelligent_import', '*', permission_level
            ):
                return JsonResponse({
                    'success': False, 
                    'error': f'Intelligent import permission required: {permission_level}'
                }, status=403)
            return view_func(request, *args, **kwargs)
        return _wrapped
    return decorator


@login_required
def intelligent_import_dashboard(request):
    """Main intelligent import dashboard"""
    user_sessions = ImportSession.objects.filter(
        user=request.user
    ).select_related('report_template', 'connection').order_by('-created_at')[:20]
    
    # Get pending approvals
    pending_approvals = []
    if request.user.user_type in ['Moderator', 'Admin']:
        pending_approvals = ImportSession.objects.filter(
            status='pending_approval'
        ).select_related('user', 'report_template').order_by('created_at')
    
    # Get available report templates
    templates = ReportTemplate.objects.filter(
        is_active=True
    ).order_by('name')
    
    # Get available connections and determine active selection
    connections_qs = ExternalConnection.objects.filter(
        owner=request.user,
        is_active=True
    ).order_by('nickname')
    connections = list(connections_qs)

    connection_param = request.GET.get('connection')
    session_connection_id = request.session.get('intelligent_import_connection_id')

    def _match_connection(conn_id):
        if not conn_id:
            return None
        for connection in connections:
            if str(connection.id) == str(conn_id):
                return connection
        return None

    active_connection = (
        _match_connection(connection_param)
        or _match_connection(session_connection_id)
    )

    if not active_connection and len(connections) == 1:
        active_connection = connections[0]

    if active_connection:
        request.session['intelligent_import_connection_id'] = str(active_connection.id)
    else:
        request.session.pop('intelligent_import_connection_id', None)

    connections_data = [
        {
            'id': str(connection.id),
            'nickname': connection.nickname,
            'db_type': connection.db_type,
            'db_type_display': connection.get_db_type_display(),
            'db_name': connection.db_name,
            'schema': connection.schema,
            'host': connection.host,
            'is_default': connection.is_default,
        }
        for connection in connections
    ]
    
    # Get system configuration
    config = SystemConfiguration.get_config()
    
    context = {
        'user_sessions': user_sessions,
        'pending_approvals': pending_approvals,
        'templates': templates,
        'connections': connections,
        'config': config,
        'can_approve': request.user.user_type in ['Moderator', 'Admin'],
        'active_connection': active_connection,
        'connections_data': connections_data,
    }
    
    return render(request, 'intelligent_import/dashboard.html', context)


@login_required
@intelligent_import_permission_required('upload')
@require_POST
def upload_and_analyze(request):
    """Upload file and perform intelligent analysis"""
    try:
        # Validate request payload
        uploaded_file = request.FILES.get('file')
        connection_id = request.POST.get('connection_id') or request.session.get('intelligent_import_connection_id')

        logger.info(
            "Upload request received: user=%s file_present=%s connection_id=%s",
            request.user.username,
            bool(uploaded_file),
            connection_id,
        )

        if not uploaded_file:
            return JsonResponse({
                'success': False,
                'error': 'No file provided for analysis.'
            }, status=400)

        if not connection_id:
            return JsonResponse({
                'success': False,
                'error': 'No database connection selected for this import. Please return to Database Management and choose a destination first.'
            }, status=400)

        # Validate file size
        config = SystemConfiguration.get_config()
        max_bytes = config.max_file_size_mb * 1024 * 1024
        if uploaded_file.size > max_bytes:
            return JsonResponse({
                'success': False,
                'error': f'File too large. Maximum size: {config.max_file_size_mb}MB'
            }, status=400)

        # Validate file extension
        allowed_extensions = ['.csv', '.xlsx', '.xls']
        file_ext = os.path.splitext(uploaded_file.name)[1].lower()
        if file_ext not in allowed_extensions:
            return JsonResponse({
                'success': False,
                'error': f'Unsupported file format. Allowed: {", ".join(allowed_extensions)}'
            }, status=400)

        # Validate connection ownership
        connection = get_object_or_404(ExternalConnection, id=connection_id, owner=request.user)

        # Create import session
        session = ImportSession.objects.create(
            user=request.user,
            connection=connection,
            original_filename=uploaded_file.name,
            file_size=uploaded_file.size,
            status='analyzing'
        )

        # Save uploaded file to temp location
        safe_original = get_valid_filename(uploaded_file.name)
        temp_filename = f"{session.id}_{safe_original}"
        temp_dir = os.path.join(settings.MEDIA_ROOT, 'intelligent_import_temp')
        os.makedirs(temp_dir, exist_ok=True)
        temp_path = os.path.join(temp_dir, temp_filename)

        with open(temp_path, 'wb') as temp_file:
            for chunk in uploaded_file.chunks():
                temp_file.write(chunk)

        session.temp_filename = temp_filename

        # Generate file hash for deduplication
        with open(temp_path, 'rb') as f:
            file_content = f.read()
        session.file_hash = session.generate_file_hash(file_content)

        recent_duplicate = ImportSession.objects.filter(
            file_hash=session.file_hash,
            created_at__gte=timezone.now() - timezone.timedelta(days=30)
        ).exclude(id=session.id).first()

        if recent_duplicate:
            session.delete()
            try:
                os.remove(temp_path)
            except Exception as cleanup_err:
                logger.warning("Failed to delete temp file for duplicate: %s", cleanup_err)
            return JsonResponse({
                'success': False,
                'error': f'Duplicate file detected. Previously imported on {recent_duplicate.created_at.strftime("%Y-%m-%d %H:%M")}',
                'duplicate_session_id': str(recent_duplicate.id)
            }, status=400)

        session.save()

        # Analyze file structure
        try:
            template_qs = ReportTemplate.objects.filter(is_active=True).order_by('name')
            templates_list = list(template_qs)
            analyzer = SchemaAnalyzer(connection, existing_templates=templates_list)
            analysis_results = analyzer.analyze_file_structure(temp_path)
            safe_results = _convert_to_builtin(analysis_results)

            template_options = [
                {
                    'id': str(template.id),
                    'name': template.name,
                    'target_table': template.target_table,
                }
                for template in templates_list
            ]

            # Store analysis results
            session.analysis_summary = {
                'file_analysis': safe_results.get('file_analysis', {}),
                'suggested_target': safe_results.get('suggested_target'),
                'target_table_suggestions': safe_results.get('target_table_suggestions', []),
                'template_match': safe_results.get('template_match'),
                'template_candidates': safe_results.get('template_candidates', []),
                'detected_template_reason': safe_results.get('detected_template_reason'),
                'confidence_score': safe_results.get('confidence_score'),
                'target_columns': safe_results.get('target_columns', {}),
                'suggested_mapping': safe_results.get('suggested_mapping', {}),
            }
            session.header_row = 0
            session.total_rows = safe_results.get('file_analysis', {}).get('total_rows', 0)
            session.status = 'template_suggested'

            suggested_mapping = safe_results.get('suggested_mapping') or {}
            if suggested_mapping:
                cleaned_mapping = {}
                for source_column, mapping_details in suggested_mapping.items():
                    if not isinstance(mapping_details, dict):
                        continue
                    cleaned_mapping[source_column] = {
                        key: value
                        for key, value in mapping_details.items()
                        if key != 'sample_values'
                    }
                session.column_mapping = cleaned_mapping

            template_lookup = {str(tpl.id): tpl for tpl in templates_list}
            template_match = safe_results.get('template_match') or {}
            selected_template = None
            if template_match:
                selected_template = template_lookup.get(template_match.get('template_id'))
                session.analysis_summary['template_match'] = template_match

            if selected_template:
                session.report_template = selected_template
                session.detected_template = selected_template.name
            else:
                session.report_template = None
                session.detected_template = template_match.get('template_name', '')

            session.save()

            suggested_target = safe_results.get('suggested_target')
            if suggested_target:
                target_table_name = suggested_target.get('table_name')
                score_pct = round(suggested_target.get('score', 0) * 100)
                session.add_system_note(
                    f"Suggested target table '{target_table_name}' identified (confidence {score_pct}%)."
                )

            detected_reason = safe_results.get('detected_template_reason')
            if selected_template:
                reason_labels = {
                    'filename_pattern': 'filename pattern',
                    'column_similarity': 'column similarity',
                }
                reasons = template_match.get('reasons') or []
                if detected_reason and detected_reason not in reasons:
                    reasons.insert(0, detected_reason)
                readable_reasons = [reason_labels.get(reason, reason) for reason in reasons] or ['system analysis']
                reason_text = ' & '.join(readable_reasons)
                score_pct = round(template_match.get('score', 0) * 100) if template_match else 0
                session.add_system_note(
                    f"Report template '{selected_template.name}' selected based on {reason_text} "
                    f"(confidence {score_pct}%)."
                )

            return JsonResponse({
                'success': True,
                'session_id': str(session.id),
                'analysis_results': safe_results,
                'template_options': template_options,
                'detected_template_id': template_match.get('template_id'),
                'detected_template_reason': detected_reason,
                'selected_template_id': str(selected_template.id) if selected_template else None,
            })
        except Exception as e:
            logger.error("Schema analysis failed: %s", str(e), exc_info=True)
            return JsonResponse({"success": False, "error": f"Analyzer error: {e}"}, status=400)

    except ValueError as e:
        logger.error("Upload and analysis validation error: %s", str(e))
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=400)




@login_required
@require_POST
def define_column_mapping(request):
    """User defines or modifies the column mapping for the import."""
    try:
        data = json.loads(request.body)
        session_id = data.get('session_id')
        column_mapping = data.get('column_mapping', {})
        user_comments = data.get('comments', '')
        import_mode = data.get('import_mode', 'auto')
        
        session = get_object_or_404(ImportSession, id=session_id)

        if request.user.user_type not in ['Moderator', 'Admin']:
            return JsonResponse({'success': False, 'error': 'Only moderators or admins can edit column mappings.'}, status=403)

        if session.status not in ['template_suggested', 'mapping_defined']:
            return JsonResponse({'success': False, 'error': f'Invalid session status: {session.status}'}, status=400)
        
        session.column_mapping = column_mapping
        session.user_comments = user_comments
        session.import_mode = import_mode
        session.status = 'mapping_defined'
        session.save()
        
        session.add_system_note("Column mapping definition saved.")
        
        return JsonResponse({'success': True, 'message': 'Column mapping saved successfully.'})
        
    except Exception as e:
        logger.error(f"Define column mapping failed: {str(e)}", exc_info=True)
        return JsonResponse({'success': False, 'error': f'Failed to save column mapping: {str(e)}'}, status=500)

@login_required
@require_POST
def update_report_template(request, session_id):
    """Update the report template selection for a session."""
    try:
        data = json.loads(request.body or "{}")
        template_id = data.get('template_id')

        session = get_object_or_404(ImportSession, id=session_id)

        # Allow session owner or elevated roles to make this change
        if session.user != request.user and request.user.user_type not in ['Moderator', 'Admin']:
            return JsonResponse({'success': False, 'error': 'Permission denied.'}, status=403)

        template_obj = None
        if template_id:
            template_obj = get_object_or_404(ReportTemplate, id=template_id, is_active=True)

        summary = session.analysis_summary or {}

        if template_obj:
            session.report_template = template_obj
            session.detected_template = template_obj.name
            summary['template_match'] = {
                'template_id': str(template_obj.id),
                'template_name': template_obj.name,
                'score': summary.get('template_match', {}).get('score'),
                'reasons': ['manual_selection'],
            }
            summary['detected_template_reason'] = 'manual_selection'
            action_note = f"Report template set to '{template_obj.name}'"
        else:
            session.report_template = None
            session.detected_template = ''
            summary['template_match'] = None
            summary['detected_template_reason'] = 'manual_selection'
            action_note = "Report template cleared"

        summary = _convert_to_builtin(summary)
        session.analysis_summary = summary
        session.save(update_fields=['report_template', 'detected_template', 'analysis_summary', 'updated_at'])
        session.add_system_note(f"{action_note} by {request.user.username}.")

        return JsonResponse({
            'success': True,
            'selected_template_id': str(template_obj.id) if template_obj else None,
        })

    except Exception as e:
        logger.error(f"Update report template failed for session {session_id}: {str(e)}", exc_info=True)
        return JsonResponse({'success': False, 'error': f'Failed to update report template: {str(e)}'}, status=500)

@login_required
@require_POST
def approve_mapping(request):
    """Approve the column mapping (moderator/admin only)."""
    try:
        data = json.loads(request.body)
        session_id = data.get('session_id')
        approval_comments = data.get('comments', '')
        
        session = get_object_or_404(ImportSession, id=session_id)
        
        if not request.user.user_type in ['Moderator', 'Admin']:
            return JsonResponse({'success': False, 'error': 'Permission denied - cannot approve mapping.'}, status=403)
        
        if session.status != 'mapping_defined':
            return JsonResponse({'success': False, 'error': f'Invalid session status: {session.status}'}, status=400)
        
        session.approved_by = request.user
        session.approved_at = timezone.now()
        session.approval_comments = approval_comments
        session.status = 'mapping_approved'
        session.save()
        
        session.add_system_note(f"Mapping approved by {request.user.username}")
        
        return JsonResponse({'success': True, 'message': 'Mapping approved successfully'})
        
    except Exception as e:
        logger.error(f"Mapping approval failed: {str(e)}", exc_info=True)
        return JsonResponse({'success': False, 'error': f'Mapping approval failed: {str(e)}'}, status=500)


@login_required
@require_POST
def validate_and_preview_data(request):
    """Validate mapped data and generate preview"""
    try:
        data = json.loads(request.body)
        session_id = data.get('session_id')

        session = get_object_or_404(ImportSession, id=session_id, user=request.user)

        if not getattr(session, "target_table", ""):
            tpl = None
            if getattr(session, "report_template_id", None):
                tpl = ReportTemplate.objects.filter(id=session.report_template_id).prefetch_related("headers").first()
            inferred = _infer_target_table(tpl)
            if inferred:
                session.target_table = inferred
                session.save(update_fields=["target_table", "updated_at"])
            else:
                return JsonResponse({
                    "success": False,
                    "error": "Import session is missing a target table definition. Select a template with a default target table, or set one in the template builder."
                }, status=400)

        # Allow validation after analysis/mapping steps are complete.
        allowed_statuses = {'template_suggested', 'mapping_defined', 'mapping_approved', 'data_validated', 'pending_approval'}
        if session.status not in allowed_statuses:
            return JsonResponse({
                'success': False,
                'error': f'Invalid session status: {session.status}'
            }, status=400)

        if not session.column_mapping:
            return JsonResponse({
                'success': False,
                'error': 'Column mapping is empty. Please map file columns to destination fields before validating.'
            }, status=400)

        # Build temp path & validate presence
        temp_path = os.path.join(
            settings.MEDIA_ROOT, 'intelligent_import_temp',
            session.temp_filename or ''
        )
        if not session.temp_filename or not os.path.exists(temp_path):
            # The source file we validate is missing - guide the UI to restart analysis
            return JsonResponse({
                'success': False,
                'error': ('Source file for validation not found. '
                          'Please restart the session to re-run file analysis.')
            }, status=400)

        # Process file with column mapping
        validation_results = process_and_validate_data(
            session, temp_path, session.column_mapping
        )

        # Store results (sanitize for strict JSON)
        session.validation_results = _convert_to_builtin(validation_results['validation_results'])
        session.preview_data = _convert_to_builtin(validation_results['preview_data'])
        session.analysis_summary = session.analysis_summary or {}
        session.analysis_summary['last_validation'] = {
            'run_at': timezone.now().isoformat(),
            'total_rows': validation_results['summary']['total_rows'],
        }
        session.status = 'pending_approval'
        session.save()

        session.add_system_note(
            f"Data validation completed. {validation_results['summary']['total_rows']} rows processed"
        )
        if validation_results['validation_results']['total_warnings']:
            session.add_system_note(
                f"Validation generated {validation_results['validation_results']['total_warnings']} warning(s).",
                level='warning'
            )

        return JsonResponse({
            'success': True,
            **validation_results
        })

    except Exception as e:
        logger.error(f"Data validation failed: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Data validation failed: {str(e)}'
        }, status=500)



@login_required
@require_POST
def approve_and_import(request):
    """Final approval and data import execution"""
    try:
        data = json.loads(request.body)
        session_id = data.get('session_id')
        final_comments = data.get('comments', '')
        import_mode = data.get('import_mode', 'auto')
        
        session = get_object_or_404(ImportSession, id=session_id)
        
        # Check approval permissions
        if not _can_approve_import(request.user, session.user):
            return JsonResponse({
                'success': False,
                'error': 'Permission denied - cannot approve import'
            }, status=403)
        
        if session.status != 'pending_approval':
            return JsonResponse({
                'success': False,
                'error': f'Invalid session status: {session.status}'
            }, status=400)
        
        # Update session
        session.approved_by = request.user
        session.approved_at = timezone.now()
        session.approval_comments = final_comments
        session.import_mode = import_mode
        session.status = 'importing_data'
        session.started_at = timezone.now()
        session.save()
        
        # Get a sample of the dataframe to choose import strategy
        temp_path = os.path.join(settings.MEDIA_ROOT, 'intelligent_import_temp', session.temp_filename)
        payload, df, context = process_and_validate_data(
            session, temp_path, session.column_mapping, return_dataframe=True
        )
        df_sample = df.head(200)

        # Detect if target table exists + rowcount
        engine = create_engine(session.connection.get_connection_uri())
        target_table_name = session.report_template.target_table
        with engine.connect() as conn:
            target_exists = engine.dialect.has_table(conn, target_table_name)
            target_rows = 0
            if target_exists:
                try:
                    target_rows = conn.execute(text(f'SELECT COUNT(*) FROM "{target_table_name}"' )).scalar() or 0
                except Exception:
                    target_rows = 0

        effective_mode = choose_import_strategy(df_sample, target_exists, target_rows, session.column_mapping, None if import_mode=="auto" else import_mode)

        # Execute data import
        try:
            import_results = execute_data_import(session, effective_mode=effective_mode)
            
            if import_results['success']:
                session.status = 'completed'
                session.completed_at = timezone.now()
                session.imported_record_count = import_results['imported_count']
                session.add_system_note(f"Import completed successfully. {import_results['imported_count']} records imported")
            else:
                session.status = 'failed'
                session.add_system_note(f"Import failed: {import_results.get('error', 'Unknown error')}")
            
            session.save()
            
            # Clean up temp file
            temp_path = os.path.join(
                settings.MEDIA_ROOT, 'intelligent_import_temp', 
                session.temp_filename
            )
            if os.path.exists(temp_path):
                os.remove(temp_path)
            
            return JsonResponse({
                'success': import_results['success'],
                'import_results': import_results
            })
            
        except Exception as e:
            session.status = 'failed'
            session.add_system_note(f"Import execution failed: {str(e)}", 'error')
            session.save()
            raise
        
    except Exception as e:
        logger.error(f"Import approval and execution failed: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Import failed: {str(e)}'
        }, status=500)


@login_required
@require_POST
def rollback_import(request, session_id):
    """Rollback completed import (admin only)"""
    try:
        data = json.loads(request.body)
        rollback_reason = data.get('reason', '')
        
        session = get_object_or_404(ImportSession, id=session_id)
        
        # Only admins can rollback
        if request.user.user_type != 'Admin':
            return JsonResponse({
                'success': False,
                'error': 'Only administrators can rollback imports'
            }, status=403)
        
        if session.status != 'completed':
            return JsonResponse({
                'success': False,
                'error': 'Can only rollback completed imports'
            }, status=400)
        
        # Execute rollback
        rollback_results = execute_rollback(session, request.user, rollback_reason)
        
        if rollback_results['success']:
            session.status = 'rolled_back'
            session.save()
            session.add_system_note(f"Import rolled back by {request.user.username}: {rollback_reason}")
        
        return JsonResponse({
            'success': rollback_results['success'],
            'rollback_results': rollback_results
        })
        
    except Exception as e:
        logger.error(f"Rollback failed: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Rollback failed: {str(e)}'
        }, status=500)


@login_required
def get_session_status(request, session_id):
    """Get current status of import session"""
    try:
        session = ImportSession.objects.filter(id=session_id).first()
        if not session:
            return JsonResponse({
                'success': True,
                'message': 'Session already cancelled.'
            })

        # Check access permissions
        if (session.user != request.user and 
            request.user.user_type not in ['Moderator', 'Admin']):
            return JsonResponse({
                'success': False,
                'error': 'Permission denied'
            }, status=403)
        
        # Get recent audit logs
        recent_logs = ImportAuditLog.objects.filter(
            import_session=session
        ).order_by('-created_at')[:5]
        
        audit_summary = []
        for log in recent_logs:
            audit_summary.append({
                'action': log.get_action_display(),
                'table_name': log.table_name,
                'success': log.success,
                'created_at': log.created_at.isoformat()
            })
        
        return JsonResponse({
            'success': True,
            'session': {
                'id': str(session.id),
                'status': session.status,
                'created_at': session.created_at.isoformat(),
                'updated_at': session.updated_at.isoformat(),
                'total_rows': session.total_rows,
                'imported_record_count': session.imported_record_count,
                'system_notes': session.system_notes[-5:],  # Last 5 notes
            },
            'audit_summary': audit_summary,
            'can_rollback': (request.user.user_type == 'Admin' and 
                           session.status == 'completed')
        })
        
    except Exception as e:
        logger.error(f"Status check failed: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Status check failed: {str(e)}'
        }, status=500)


def _can_approve_import(approver: User, uploader: User) -> bool:
    """Check if approver can approve uploader's imports"""
    return approver.user_type in ['Admin', 'Moderator']


def execute_rollback(session: ImportSession, rollback_user: User, reason: str) -> Dict[str, Any]:
    """Execute rollback of imported data"""
    try:
        # Get all data lineage for this session
        lineage_records = DataLineage.objects.filter(
            import_session=session,
            is_rolled_back=False
        )
        
        rolled_back_count = 0
        
        # Mark records as rolled back (soft delete approach)
        with transaction.atomic():
            for lineage in lineage_records:
                # Mark lineage as rolled back
                lineage.is_rolled_back = True
                lineage.rolled_back_at = timezone.now()
                lineage.rolled_back_by = rollback_user
                lineage.save()
                
                rolled_back_count += 1
        
        return {
            'success': True,
            'records_rolled_back': rolled_back_count,
            'rollback_reason': reason,
            'rollback_timestamp': timezone.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Rollback execution failed: {str(e)}")
        return {
            'success': False,
            'error': str(e),
            'records_rolled_back': 0
        }

@login_required
@require_GET
@never_cache
def list_sessions(request):
    qs = ImportSession.objects.filter(user=request.user).order_by('-created_at')[:20]
    sessions = [{
        'id': str(s.id),
        'original_filename': s.original_filename,
        'status': s.status,
        'created_at': s.created_at.isoformat(),
        'connection': getattr(s.connection, 'nickname', ''),
        'total_rows': s.total_rows,
        'imported_record_count': s.imported_record_count,
        'user': s.user.username,
    } for s in qs]
    return JsonResponse({'success': True, 'sessions': sessions})

@login_required
@require_GET
def enter_session(request, session_id):
    session = ImportSession.objects.filter(id=session_id).first()
    if not session:
        return JsonResponse({'success': False, 'error': 'Session not found'}, status=404)
    if session.user != request.user and request.user.user_type not in ['Moderator', 'Admin']:
        return JsonResponse({'success': False, 'error': 'Permission denied'}, status=403)

    template_qs = ReportTemplate.objects.filter(is_active=True).order_by('name')
    template_options = [
        {
            'id': str(template.id),
            'name': template.name,
            'target_table': template.target_table,
        }
        for template in template_qs
    ]

    analysis = _convert_to_builtin(session.analysis_summary or {})
    validation_results = _convert_to_builtin(session.validation_results or {})
    preview_data = _convert_to_builtin(session.preview_data or {})
    column_mapping = _convert_to_builtin(session.column_mapping or {})
    template_match = analysis.get('template_match') or {}
    detected_template_id = template_match.get('template_id')
    detected_template_reason = analysis.get('detected_template_reason')
    selected_template_id = str(session.report_template_id) if session.report_template_id else None

    # Define the workflow step based on the session status
    status_to_step = {
        'template_suggested': 'mapping',
        'mapping_defined': 'mapping',
        'mapping_approved': 'validate',
        'data_validated': 'validate',
        'pending_approval': 'import',
        'importing_data': 'import',
        'completed': 'done',
        'failed': 'upload',
        'cancelled': 'upload',
        'rolled_back': 'done',
    }
    step = status_to_step.get(session.status, 'upload')

    # Create the final payload for the frontend
    payload = {
        'success': True,
        'session_id': str(session.id),
        'status': session.status,
        'step': step,
        'analysis_results': analysis,
        'target_columns': analysis.get('target_columns', {}),
        'suggested_mapping': analysis.get('suggested_mapping', {}),
        'column_mapping': column_mapping,
        'validation_results': validation_results,
        'preview_data': preview_data,
        'file_info': analysis.get('file_analysis', {'total_rows': session.total_rows}),
        'connection': getattr(session.connection, 'nickname', None),
        'template_options': template_options,
        'selected_template_id': selected_template_id,
        'detected_template_id': detected_template_id,
        'detected_template_reason': detected_template_reason,
    }
    return JsonResponse(payload)



@login_required
@require_POST
def delete_session(request, session_id):
    """
    Delete a session (and its temp file if still around).
    Allowed for: owner or Moderator/Admin.
    Only in terminal-ish states by default to avoid data loss.
    """
    try:
        session = ImportSession.objects.filter(id=session_id).first()
        if not session:
            return JsonResponse({'success': True, 'message': 'Session already removed.'})

        if (session.user != request.user and 
            request.user.user_type not in ['Moderator', 'Admin']):
            return JsonResponse({'success': False, 'error': 'Permission denied'}, status=403)

        # Allow owners to delete their own sessions regardless of state.
        # Non-owners require Moderator/Admin role.
        if session.user != request.user and request.user.user_type not in ['Moderator', 'Admin']:
            return JsonResponse({
                'success': False,
                'error': 'Permission denied'
            }, status=403)

        # Clean up temp file if present
        if session.temp_filename:
            temp_path = os.path.join(settings.MEDIA_ROOT, 'intelligent_import_temp', session.temp_filename)
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
            except Exception:
                # Non-fatal
                pass

        session.delete()
        return JsonResponse({'success': True, 'message': 'Session deleted successfully.'})
    except Exception as e:
        logger.exception("delete_session failed")
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


@login_required
def session_detail(request, session_id):
    """Show detailed view of an import session"""
    try:
        session = get_object_or_404(ImportSession, id=session_id)

        # Check access permissions
        if (session.user != request.user and
            request.user.user_type not in ['Moderator', 'Admin']):
            return JsonResponse({
                'success': False,
                'error': 'Permission denied'
            }, status=403)

        # Get related audit logs
        audit_logs_qs = ImportAuditLog.objects.filter(
            import_session=session
        ).order_by('-created_at')
        
        audit_logs = [{
            'action': log.get_action_display(),
            'table_name': log.table_name,
            'details': log.details,
            'success': log.success,
            'error_message': log.error_message,
            'executed_by': log.executed_by.username,
            'created_at': log.created_at.isoformat()
        } for log in audit_logs_qs]

        # Get data lineage
        data_lineage_qs = DataLineage.objects.filter(
            import_session=session
        ).order_by('-created_at')[:100]  # Limit to recent records
        
        data_lineage = [{
            'target_table': lin.target_table,
            'target_record_id': lin.target_record_id,
            'source_row_number': lin.source_row_number,
            'operation': lin.get_operation_display(),
            'is_rolled_back': lin.is_rolled_back,
            'created_at': lin.created_at.isoformat()
        } for lin in data_lineage_qs]

        session_data = {
            'id': str(session.id),
            'original_filename': session.original_filename,
            'file_size': session.file_size,
            'status': session.get_status_display(),
            'status_raw': session.status,
            'created_at': session.created_at.isoformat(),
            'updated_at': session.updated_at.isoformat(),
            'user': session.user.username,
            'connection': session.connection.nickname,
            'total_rows': session.total_rows,
            'imported_record_count': session.imported_record_count,
            'column_mapping': session.column_mapping,
            'user_comments': session.user_comments,
            'system_notes': session.system_notes,
        }

        return JsonResponse({
            'success': True,
            'session': session_data,
            'audit_logs': audit_logs,
            'data_lineage': data_lineage,
            'can_rollback': (request.user.user_type == 'Admin' and
                           session.status == 'completed')
        })

    except Exception as e:
        logger.error(f"Session detail failed: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Failed to load session details: {str(e)}'
        }, status=500)


@login_required 
@require_POST
def cancel_session(request, session_id):
    """Cancel an import session"""
    try:
        session = get_object_or_404(ImportSession, id=session_id)
        
        # Check access permissions
        if (session.user != request.user and 
            request.user.user_type not in ['Moderator', 'Admin']):
            return JsonResponse({
                'success': False,
                'error': 'Permission denied'
            }, status=403)
        
        # Check if session can be cancelled
        cancellable_states = [
            'file_uploaded',
            'analyzing',
            'template_suggested',
            'mapping_defined',
            'mapping_approved',
            'data_validated',
            'pending_approval',
        ]
        # If already cancelled, treat as idempotent success
        if session.status == 'cancelled':
            return JsonResponse({
                'success': True,
                'message': 'Session already cancelled.'
            })

        if session.status not in cancellable_states:
            return JsonResponse({
                'success': False,
                'error': f'Session cannot be cancelled in its current state: {session.get_status_display()}'
            }, status=400)
        
        # Cancel the session
        session.status = 'cancelled'
        session.add_system_note(f"Session cancelled by user {request.user.username}.")
        session.save()
        
        return JsonResponse({
            'success': True,
            'message': 'Session cancelled successfully.'
        })
        
    except Exception as e:
        logger.error(f"Session cancellation failed for session {session_id}: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Failed to cancel session: {str(e)}'
        }, status=500)

@login_required
@require_POST
def restart_session(request, session_id):
    """Restart an import session by rerunning file analysis."""
    try:
        session = get_object_or_404(ImportSession, id=session_id)

        # Basic permission: owner or moderator/admin
        if (session.user != request.user and 
            request.user.user_type not in ['Moderator', 'Admin']):
            return JsonResponse({'success': False, 'error': 'Permission denied'}, status=403)

        # Require a temp file name
        if not session.temp_filename:
            return JsonResponse({
                'success': False,
                'error': 'No uploaded file is associated with this session. Please re-upload the file and try again.'
            }, status=400)

        temp_path = os.path.join(settings.MEDIA_ROOT, 'intelligent_import_temp', session.temp_filename)

        # Require the file to exist
        if not os.path.exists(temp_path):
            return JsonResponse({
                'success': False,
                'error': 'The temporary file for this session could not be found. Please upload the file again.'
            }, status=400)

        template_qs = ReportTemplate.objects.filter(is_active=True).order_by('name')
        templates_list = list(template_qs)
        analyzer = SchemaAnalyzer(session.connection, existing_templates=templates_list)

        # Re-analyze the file
        analysis_results = analyzer.analyze_file_structure(temp_path)
        analysis_results = _convert_to_builtin(analysis_results)

        template_options = [
            {
                'id': str(template.id),
                'name': template.name,
                'target_table': template.target_table,
            }
            for template in templates_list
        ]

        # Reset session fields
        session.column_mapping = {}
        session.validation_results = {}
        session.preview_data = {}
        session.analysis_summary = {
            'file_analysis': analysis_results.get('file_analysis', {}),
            'suggested_target': analysis_results.get('suggested_target'),
            'target_table_suggestions': analysis_results.get('target_table_suggestions', []),
            'template_match': analysis_results.get('template_match'),
            'template_candidates': analysis_results.get('template_candidates', []),
            'detected_template_reason': analysis_results.get('detected_template_reason'),
            'confidence_score': analysis_results.get('confidence_score'),
            'target_columns': analysis_results.get('target_columns', {}),
            'suggested_mapping': analysis_results.get('suggested_mapping', {}),
        }

        suggested_mapping = analysis_results.get('suggested_mapping') or {}
        if suggested_mapping:
            cleaned_mapping = {}
            for source_column, mapping_details in suggested_mapping.items():
                if not isinstance(mapping_details, dict):
                    continue
                cleaned_mapping[source_column] = {
                    key: value for key, value in mapping_details.items()
                    if key != 'sample_values'
                }
            session.column_mapping = cleaned_mapping

        template_lookup = {str(tpl.id): tpl for tpl in templates_list}
        template_match = analysis_results.get('template_match') or {}
        selected_template = None
        if template_match:
            selected_template = template_lookup.get(template_match.get('template_id'))
            session.analysis_summary['template_match'] = template_match

        if selected_template:
            session.report_template = selected_template
            session.detected_template = selected_template.name
        else:
            session.report_template = None
            session.detected_template = template_match.get('template_name', '')

        session.total_rows = analysis_results.get('file_analysis', {}).get('total_rows', 0)
        session.imported_record_count = 0
        session.status = 'template_suggested'
        session.save()

        suggested_target = analysis_results.get('suggested_target')
        if suggested_target:
            target_table_name = suggested_target.get('table_name')
            score_pct = round(suggested_target.get('score', 0) * 100)
            session.add_system_note(
                f"Suggested target table '{target_table_name}' identified after restart (confidence {score_pct}%)."
            )

        detected_reason = analysis_results.get('detected_template_reason')
        if selected_template:
            reason_labels = {
                'filename_pattern': 'filename pattern',
                'column_similarity': 'column similarity',
            }
            reasons = template_match.get('reasons') or []
            if detected_reason and detected_reason not in reasons:
                reasons.insert(0, detected_reason)
            readable_reasons = [reason_labels.get(reason, reason) for reason in reasons] or ['system analysis']
            reason_text = ' & '.join(readable_reasons)
            score_pct = round(template_match.get('score', 0) * 100) if template_match else 0
            session.add_system_note(
                f"Report template '{selected_template.name}' selected after restart based on {reason_text} "
                f"(confidence {score_pct}%)."
            )

        session.add_system_note(f"Session restarted by {request.user.username}.")
        session.add_system_note("File analysis completed successfully after restart.")

        return JsonResponse({
            'success': True,
            'session_id': str(session.id),
            'analysis_results': analysis_results,
            'template_options': template_options,
            'detected_template_id': template_match.get('template_id'),
            'detected_template_reason': detected_reason,
            'selected_template_id': str(selected_template.id) if selected_template else None,
        })

    except Exception as e:
        logger.error(f"Restart session failed for session {session_id}: {str(e)}", exc_info=True)
        return JsonResponse({
            'success': False,
            'error': f'Failed to restart session: {str(e)}'
        }, status=500)



@login_required
@intelligent_import_permission_required('approve')
def manage_master_data_candidates(request, session_id):
    """Display and process master data candidates for an import session."""
    session = get_object_or_404(ImportSession, id=session_id)
    candidates = MasterDataCandidate.objects.filter(import_session=session, status='pending')

    if request.method == 'POST':
        approved_ids = request.POST.getlist('approved')
        rejected_ids = request.POST.getlist('rejected')

        # In a real implementation, you would have a service to create the master data records
        # For now, we just update the status
        MasterDataCandidate.objects.filter(id__in=approved_ids).update(status='approved', reviewed_by=request.user, reviewed_at=timezone.now())
        MasterDataCandidate.objects.filter(id__in=rejected_ids).update(status='rejected', reviewed_by=request.user, reviewed_at=timezone.now())

        session.add_system_note(f'{len(approved_ids)} master data candidates approved and {len(rejected_ids)} rejected by {request.user.username}.')

        return redirect('intelligent_import:dashboard') # Or back to the session detail page

    context = {
        'session': session,
        'candidates': candidates
    }
    return render(request, 'intelligent_import/approve_master_data.html', context)


@login_required
@require_GET
def get_table_columns_api(request, session_id):
    """API endpoint to get the columns for the session's target table."""
    try:
        session = get_object_or_404(ImportSession, id=session_id)
        if not session.report_template or not session.report_template.target_table:
            return JsonResponse({'success': False, 'error': 'No target table configured for this session.'}, status=400)

        schema_info = get_table_schema_from_db(session)
        target_def = schema_info['tables'][session.report_template.target_table]

        columns = []
        for name, metadata in target_def['columns'].items():
            column_entry = {
                'name': name,
                'data_type': metadata.get('data_type'),
                'nullable': metadata.get('nullable'),
                'is_primary_key': metadata.get('is_primary_key'),
            }
            if metadata.get('foreign_key'):
                column_entry['foreign_key'] = metadata['foreign_key']
            columns.append(column_entry)

        return JsonResponse({
            'success': True,
            'columns': columns,
            'primary_key': target_def.get('primary_key', []),
        })

    except Exception as e:
        logger.error(f"Failed to get table columns for session {session_id}: {e}", exc_info=True)
        return JsonResponse({'success': False, 'error': str(e)}, status=500)


from django.http import Http404
from django.db.models import Q

def _role(user):
    return getattr(user, "user_type", "Uploader")

def _can_create_or_edit_shared(user):
    return _role(user) in ["Admin", "Moderator"]

@login_required 
@require_http_methods(["POST"])
def report_template_headers_api(request, template_id):
    # Bulk add/update headers to an existing template (Admin/Moderator; Uploader creates pending in session instead)
    tpl = ReportTemplate.objects.filter(id=template_id, is_active=True).first()
    if not tpl:
        raise Http404()
    if not _can_create_or_edit_shared(request.user):
        return JsonResponse({"success": False, "error": "Permission denied"}, status=403)

    payload = json.loads(request.body or "{}")
    headers = payload.get("headers") or []
    with transaction.atomic():
        for h in headers:
            obj, created = ReportTemplateHeader.objects.update_or_create(
                template=tpl, source_header=h.get("source_header",""),
                defaults=dict(
                    target_table=h.get("target_table",""),
                    target_column=h.get("target_column",""),
                    data_type=h.get("data_type",""),
                    is_required=bool(h.get("is_required", False)),
                    default_value=h.get("default_value",""),
                    master_data_source=h.get("master_data_source",""),
                    master_output_field=h.get("master_output_field",""),
                    transform=h.get("transform",""),
                    depends_on=h.get("depends_on") or [],
                    strict=bool(h.get("strict", False)),
                )
            )
    # bump version for visibility
    tpl.version += 1
    tpl.updated_by = request.user
    tpl.save(update_fields=["version", "updated_by", "updated_at"])
    return JsonResponse({"success": True, "template": {"id": str(tpl.id), "version": tpl.version}})

@login_required
@require_http_methods(["POST"])
def set_session_report_template_api(request, session_id):
    payload = json.loads(request.body or "{}")
    template_id = payload.get("template_id")
    client_tbl = (payload.get("target_table") or "").strip()

    session = ImportSession.objects.filter(id=session_id, user=request.user).first()
    if not session: raise Http404()

    tpl = ReportTemplate.objects.filter(id=template_id).prefetch_related("headers").first()
    inferred = _infer_target_table(tpl)
    chosen_tbl = client_tbl or inferred or ""

    # Update selected report template
    session.report_template_id = template_id
    # Record selection details in analysis summary (avoid non-existent model fields)
    analysis = session.analysis_summary or {}
    if tpl:
        prev_score = None
        try:
            prev_score = (analysis.get('template_match') or {}).get('score')
        except Exception:
            prev_score = None
        analysis['template_match'] = {
            'template_id': str(tpl.id),
            'template_name': tpl.name,
            'score': prev_score,
            'reasons': ['manual_selection'],
        }
        analysis['detected_template_reason'] = 'manual_selection'
    # Optionally record the chosen target table hint for UI
    if chosen_tbl:
        analysis['selected_target_table'] = chosen_tbl
    session.analysis_summary = analysis
    session.save(update_fields=["report_template_id", "analysis_summary", "updated_at"])

    return JsonResponse({
        "success": True,
        "selected_template_id": str(tpl.id) if tpl else None,
        "detected_template_id": str(tpl.id) if tpl else None,
        "detected_template_reason": "manual_selection",
        "target_table": chosen_tbl,
    })

# --- Suggestions: header -> best (table.column) ---
def _synonyms():
    # seed dictionary
    return {
        "company": ["company_name"],
        "unit": ["unit_name", "factory"],
        "buyer name": ["buyer_name"],
        "style name": ["buyer_style", "style"],
        "cs id": ["cs_id"],
        "po no": ["po_no", "purchase order"],
        "production qty": ["production_qty", "output", "qty"],
        "produced minutes": ["produce_minutes", "produced minutes"],
        "sam-factory": ["sam_factory", "sam"],
        "fob audited": ["fob_audited", "fob"],
        "cm audited": ["cm_audited", "cm"],
        "season": ["season"],
        "category": ["category"],
        "color": ["color_name"],
        "line": ["line_name"],
        "year": ["year"], "month": ["month", "month_num"],
    }

# Example: a minimal catalog of allowed tables & columns for suggestions
_ALLOWED_COLUMNS = {
    "fact_sewing_production": ["production_qty", "produce_minutes", "sam_factory", "fob_audited", "cm_audited", "year", "month_num"],
    "dim_company": ["company_name"],
    "dim_unit": ["unit_name", "unit_code"],
    "dim_buyer": ["buyer_name"],
    "dim_style": ["buyer_style", "category", "season"],
    "dim_order": ["cs_id", "po_no", "order_qty", "allocated_qty", "ex_factory_date"],
    "dim_line": ["line_name"],
    "dim_color": ["color_name"],
}

def _score(header, table, column):
    h = header.strip().lower()
    cand = column.replace("_"," ").lower()
    score = 0.0
    if h == cand: score += 0.8
    elif h in cand or cand in h: score += 0.6
    # synonyms
    for key, vals in _synonyms().items():
        if key in h:
            if column in vals or any(v.replace(" ","_") == column for v in vals):
                score += 0.2
    # context boost could be added if many headers pick same table
    return min(score, 1.0)

@login_required
@require_http_methods(["POST"])
def suggest_mapping_api(request):
    """
    Suggests column-to-schema mappings based on headers and optional session context.

    Input:  { "headers": ["Unit", "Buyer Name", ...], "session_id": "<optional>" }
    Output: { "success": true, "suggestions": { "Unit": [ {table, column, score, create?, fk_to?}, ... ] } }
    """
    try:
        payload = json.loads(request.body or "{}")
        headers = payload.get("headers") or []
        session_id = payload.get("session_id")

        if not headers or not isinstance(headers, list):
            return JsonResponse({"success": False, "error": "headers must be a non-empty list"}, status=400)

        suggestions = {h: [] for h in headers}
        analyzer = None

        # Try analyzer with session context
        if session_id:
            session = ImportSession.objects.filter(id=session_id, user=request.user).first()
            if session and session.temp_filename:
                try:
                    temp_path = os.path.join(settings.MEDIA_ROOT, 'intelligent_import_temp', session.temp_filename)
                    templates = list(ReportTemplate.objects.filter(is_active=True))
                    analyzer = SchemaAnalyzer(session.connection, existing_templates=templates)
                    analysis = analyzer.analyze_file_structure(temp_path)

                    norm = analysis.get("normalized_proposal") or {}
                    target_cols = (analysis.get("target_columns") or {}).keys()
                    fk_map = norm.get("fk_map") or {}

                    def rank_for(header):
                        ranked = []
                        h = header.strip()
                        for col in target_cols:
                            cand = col.replace("_", " ").lower()
                            if h.lower() == cand:
                                score = 0.95
                            elif h.lower() in cand or cand in h.lower():
                                score = 0.8
                            else:
                                score = SequenceMatcher(None, h.lower(), cand).ratio() * 0.7
                            ranked.append({
                                "table": analysis.get("suggested_target", {}).get("table_name"),
                                "column": col,
                                "score": round(score, 3)
                            })
                        if header in fk_map:
                            ranked.append({
                                "table": fk_map[header]["create_dimension_table"],
                                "column": header,
                                "score": 0.9,
                                "create": True,
                                "fk_to": norm.get("fact_table"),
                            })
                        return sorted(ranked, key=lambda x: x["score"], reverse=True)[:5]

                    for h in headers:
                        suggestions[h] = rank_for(h)

                except Exception as e:
                    logger.exception("Analyzer failed during suggest_mapping")
                    return JsonResponse({"success": False, "error": f"{e.__class__.__name__}: {e}"}, status=500)

        # Fallback to static catalog if analyzer not available
        if analyzer is None:
            for h in headers:
                ranked = []
                for tbl, cols in _ALLOWED_COLUMNS.items():
                    for col in cols:
                        ranked.append({
                            "table": tbl,
                            "column": col,
                            "score": _score(h, tbl, col)
                        })
                suggestions[h] = sorted(ranked, key=lambda x: x["score"], reverse=True)[:5]

        return JsonResponse({"success": True, "suggestions": suggestions})

    except Exception as e:
        logger.exception("suggest_mapping_api failed")
        return JsonResponse({"success": False, "error": f"{e.__class__.__name__}: {e}"}, status=500)


def _is_manager(user):
    # adjust to your roles; example reads window.currentUser.user_type in the UI
    return getattr(user, "user_type", None) in ("Admin", "Moderator") or user.is_staff or user.is_superuser

def _suggest_unique_name(base):
    """
    Suggest 'base', 'base (2)', 'base (3)', ... that doesn't exist.
    """
    from .models import ReportTemplate
    # fetch all possibly colliding names once
    exists = set(
        ReportTemplate.objects
        .filter(Q(name=base) | Q(name__startswith=base + " ("))
        .values_list("name", flat=True)
    )
    if base not in exists:
        return base
    n = 2
    while True:
        cand = f"{base} ({n})"
        if cand not in exists:
            return cand
        n += 1

@login_required
@require_http_methods(["GET", "POST"])
def report_templates_api(request):
    if request.method == "GET":
        qs = ReportTemplate.objects.all().order_by("name")
        data = [{"id": str(t.id), "name": t.name, "description": t.description or "", "is_active": bool(t.is_active)} for t in qs]
        return JsonResponse({"success": True, "templates": data})

    # POST (create)
    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)
    try:
        payload = json.loads(request.body or "{}")
        base = (payload.get("name") or "").strip()
        if not base:
            return JsonResponse({"success": False, "error": "Name required"}, status=400)

        # Optional: allow client to accept server suggestion on conflict
        name = payload.get("final_name") or base

        t = ReportTemplate.objects.create(
            name=name,
            description=(payload.get("description") or "").strip(),
            is_active=True,
        )
        return JsonResponse({"success": True, "id": str(t.id), "name": t.name})
    except IntegrityError:
        suggested = _suggest_unique_name(base)
        return JsonResponse(
            {"success": False, "error": "name_conflict", "suggested_name": suggested},
            status=409
        )
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)

@login_required
@require_http_methods(["PUT", "DELETE"])
def report_template_detail_api(request, template_id):
    try:
        t = ReportTemplate.objects.get(id=template_id)
    except ReportTemplate.DoesNotExist:
        return JsonResponse({"success": False, "error": "Not found"}, status=404)

    if not _is_manager(request.user):
        return JsonResponse({"success": False, "error": "Forbidden"}, status=403)

    if request.method == "DELETE":
        t.delete()
        return JsonResponse({"success": True})

    # PUT (update)
    try:
        payload = json.loads(request.body or "{}")
        name = (payload.get("name") or t.name).strip()
        if not name:
            return JsonResponse({"success": False, "error": "Name required"}, status=400)
        t.name = name
        t.description = (payload.get("description") or "")
        if "is_active" in payload:
            t.is_active = bool(payload["is_active"])
        t.save()
        return JsonResponse({"success": True})
    except Exception as e:
        return JsonResponse({"success": False, "error": str(e)}, status=400)


def _db_engine():
    eng = settings.DATABASES.get("default", {}).get("ENGINE", "")
    # normalize
    if "postgres" in eng:
        return "postgres"
    if "mysql" in eng:
        return "mysql"
    if "sqlite" in eng:
        return "sqlite"
    return "unknown"

def _fetchall(q, params=None):
    with connection.cursor() as cur:
        cur.execute(q, params or [])
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

@login_required
@require_http_methods(["GET"])
def metadata_tables_api(request):
    """
    GET /intelligent-import/api/metadata/tables/?schema=public
    Lists base tables for the connected DB. Works on Postgres/MySQL/SQLite.
    """
    try:
        engine = _db_engine()
        schema = request.GET.get("schema", "public")

        if engine == "postgres":
            rows = _fetchall(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE' AND table_schema=%s
                ORDER BY table_name
                """,
                [schema],
            )
            data = [{"schema": r["table_schema"], "table": r["table_name"]} for r in rows]

        elif engine == "mysql":
            # MySQL ignores schema param (use current DB)
            rows = _fetchall("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
            # rows look like: {'Tables_in_dbname': 'table', 'Table_type': 'BASE TABLE'}
            # build a consistent shape; schema ~ current database()
            current_db = _fetchall("SELECT DATABASE() AS db")[0]["db"]
            data = [{"schema": current_db, "table": list(r.values())[0]} for r in rows]

        elif engine == "sqlite":
            rows = _fetchall("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            data = [{"schema": "main", "table": r["name"]} for r in rows]

        else:
            return JsonResponse({"success": False, "error": "Unsupported DB engine"}, status=400)

        return JsonResponse({"success": True, "tables": data})
    except Exception as e:
        # never return HTML errors; keep UI functional
        return JsonResponse({"success": False, "error": f"metadata_tables: {e.__class__.__name__}: {e}"}, status=500)

@login_required
@require_http_methods(["GET"])
def metadata_table_columns_api(request, schema, table):
    """
    GET /intelligent-import/api/metadata/tables/<schema>/<table>/columns/
    Lists columns for a given table. Works on Postgres/MySQL/SQLite.
    """
    try:
        engine = _db_engine()

        if engine == "postgres":
            rows = _fetchall(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema=%s AND table_name=%s
                ORDER BY ordinal_position
                """,
                [schema, table],
            )
            if not rows:
                return JsonResponse({"success": False, "error": "not_found"}, status=404)
            data = [{"name": r["column_name"], "type": r["data_type"], "nullable": (r["is_nullable"] == "YES")} for r in rows]

        elif engine == "mysql":
            # schema param ignored; describe from current DB
            rows = _fetchall(f"DESCRIBE `{table}`")
            # rows: Field, Type, Null, Key, Default, Extra
            if not rows:
                return JsonResponse({"success": False, "error": "not_found"}, status=404)
            data = [{"name": r["Field"], "type": r["Type"], "nullable": (r["Null"] == "YES")} for r in rows]

        elif engine == "sqlite":
            rows = _fetchall(f"PRAGMA table_info('{table}')")
            # rows: cid, name, type, notnull, dflt_value, pk
            if not rows:
                return JsonResponse({"success": False, "error": "not_found"}, status=404)
            data = [{"name": r["name"], "type": r["type"], "nullable": (r["notnull"] == 0)} for r in rows]

        else:
            return JsonResponse({"success": False, "error": "Unsupported DB engine"}, status=400)

        return JsonResponse({"success": True, "columns": data})
    except Exception as e:
        return JsonResponse({"success": False, "error": f"metadata_columns: {e.__class__.__name__}: {e}"}, status=500)
