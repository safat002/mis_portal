from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
import json
import os

from django.conf import settings

from intelligent_import.models import ImportSession, ReportTemplate
from intelligent_import.services.data_processing import process_and_validate_data


def _infer_target_table(template: ReportTemplate | None) -> str:
    """Lightweight inference mirroring views._infer_target_table logic."""
    if not template:
        return ""
    if getattr(template, "target_table", ""):
        return template.target_table
    try:
        headers = list(template.headers.all())
    except Exception:
        headers = []
    tbls = [getattr(h, "target_table", "") for h in headers if getattr(h, "target_table", "")]
    if not tbls:
        return ""
    fact_tbls = [t for t in tbls if t.startswith("fact_")]
    pool = fact_tbls or tbls
    # most common
    best = {}
    for t in pool:
        best[t] = best.get(t, 0) + 1
    return sorted(best.items(), key=lambda x: x[1], reverse=True)[0][0]


class Command(BaseCommand):
    help = "Dry-run validation for an Intelligent Import session; prints JSON output."

    def add_arguments(self, parser):
        parser.add_argument("--session", dest="session_id", help="ImportSession UUID", default=None)
        parser.add_argument("--limit", dest="limit", type=int, default=10, help="Preview rows to show")

    def handle(self, *args, **options):
        session_id = options.get("session_id")
        limit = options.get("limit") or 10

        session = None
        if session_id:
            session = ImportSession.objects.filter(id=session_id).first()
            if not session:
                raise CommandError(f"Session not found: {session_id}")
        else:
            session = ImportSession.objects.order_by("-created_at").first()
            if not session:
                raise CommandError("No ImportSession rows found.")

        if not session.target_table:
            tpl = None
            if getattr(session, "report_template_id", None):
                tpl = ReportTemplate.objects.filter(id=session.report_template_id).prefetch_related("headers").first()
            inferred = _infer_target_table(tpl)
            if inferred:
                session.target_table = inferred
                session.save(update_fields=["target_table", "updated_at"])  # keep for UI consistency

        temp_path = os.path.join(
            settings.MEDIA_ROOT, "intelligent_import_temp", session.temp_filename or ""
        )
        if not session.temp_filename or not os.path.exists(temp_path):
            raise CommandError(
                "Temporary file missing for this session. Please re-upload or restart analysis."
            )

        if not session.column_mapping:
            raise CommandError("Column mapping is empty for this session.")

        try:
            payload = process_and_validate_data(session, temp_path, session.column_mapping)
        except Exception as exc:
            raise CommandError(f"Validation raised: {exc}")

        # Optionally trim preview size for readability
        preview = payload.get("preview_data", {}).get("sample_data", [])
        if isinstance(preview, list) and len(preview) > limit:
            payload["preview_data"]["sample_data"] = preview[:limit]

        self.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))

