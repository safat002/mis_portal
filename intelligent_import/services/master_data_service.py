# intelligent_import/services/master_data_service.py
"""
Service for handling master data lookups and candidate creation.
"""

import logging
from typing import Dict, List, Set, Tuple

from django.apps import apps
from django.db import IntegrityError, transaction

from ..models import ImportSession, MasterDataCandidate

logger = logging.getLogger(__name__)


class MasterDataService:
    """
    Resolve human-readable names to primary keys and raise master data candidates
    for any missing entries.
    """

    def __init__(self, session: ImportSession):
        self.session = session

    def get_ids_from_names(
        self,
        target_model_name: str,
        names: List[str],
        *,
        lookup_field: str = "name",
    ) -> Tuple[Dict[str, int], Set[str]]:
        if not names:
            return {}, set()

        try:
            TargetModel = apps.get_model(target_model_name)
        except LookupError:
            logger.error("Master data model '%s' could not be resolved.", target_model_name)
            return {}, set(names)

        if not hasattr(TargetModel, lookup_field):
            logger.warning(
                "Model '%s' does not have lookup field '%s'; falling back to 'name'.",
                target_model_name,
                lookup_field,
            )
            lookup_field = "name"

        unique_names = {name.strip() for name in names if name}
        if not unique_names:
            return {}, set()

        name_to_id_map: Dict[str, int] = {}
        not_found: Set[str] = set()

        for raw_value in unique_names:
            filter_kwargs = {f"{lookup_field}__iexact": raw_value}
            obj = TargetModel.objects.filter(**filter_kwargs).first()
            if obj:
                name_to_id_map[raw_value] = obj.pk
            else:
                not_found.add(raw_value)

        logger.debug(
            "Master data lookup for '%s' on field '%s': %s found, %s missing.",
            target_model_name,
            lookup_field,
            len(name_to_id_map),
            len(not_found),
        )
        return name_to_id_map, not_found

    def create_master_data_candidates(
        self,
        target_model_name: str,
        not_found_names: Set[str],
    ) -> int:
        if not not_found_names:
            return 0

        try:
            TargetModel = apps.get_model(target_model_name)
        except LookupError:
            logger.error(
                "Cannot raise master data candidates for unknown model '%s'.",
                target_model_name,
            )
            return 0

        target_table = TargetModel._meta.db_table
        existing_values = set(
            MasterDataCandidate.objects.filter(
                import_session=self.session,
                target_master_table=target_table,
                proposed_value__in=list(not_found_names),
            ).values_list("proposed_value", flat=True)
        )

        names_to_create = [value for value in not_found_names if value not in existing_values]
        if not names_to_create:
            return 0

        candidates = [
            MasterDataCandidate(
                import_session=self.session,
                target_master_table=target_table,
                proposed_value=value,
            )
            for value in names_to_create
        ]

        try:
            with transaction.atomic():
                MasterDataCandidate.objects.bulk_create(candidates, ignore_conflicts=True)
        except IntegrityError as exc:
            logger.warning("Master data candidate creation encountered an issue: %s", exc)

        logger.info(
            "Queued %s master data candidate(s) for table '%s'.",
            len(candidates),
            target_table,
        )
        return len(candidates)

from django.db import connection, transaction
from ..naming_policy import normalize_snake, table_name as np_table_name

def _ensure_unique_name(existing: set, base: str, maxlen=63):
    if base not in existing:
        existing.add(base)
        return base
    n = 2
    while True:
        cand = f"{base}_{n}"[:maxlen]
        if cand not in existing:
            existing.add(cand)
            return cand
        n += 1

def plan_schema_changes(schema_proposals):
    """
    Input: list of {"action": "create_table"|"add_column", ...}
    Returns: {"ddl": [sql...], "summary": [...], "name_map": {...}}
    Does NOT execute anything.
    """
    ddl = []
    summary = []
    name_map = {}
    taken_tables = set()
    taken_cols = {}  # {table: set(columns)}

    # probe live db for names to avoid collisions
    with connection.cursor() as cur:
        cur.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema='public'""")
        for (t,) in cur.fetchall():
            taken_tables.add(t)
        cur.execute("""
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema='public'
        """)
        for t, c in cur.fetchall():
            taken_cols.setdefault(t, set()).add(c)

    # Helper: resolve current PK constraint name (Postgres)
    def _pk_constraint_name(table: str) -> str | None:
        try:
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT tc.constraint_name
                    FROM information_schema.table_constraints tc
                    WHERE tc.table_schema = 'public'
                      AND tc.table_name = %s
                      AND tc.constraint_type = 'PRIMARY KEY'
                    LIMIT 1
                    """,
                    [table],
                )
                row = cur.fetchone()
                return row[0] if row else None
        except Exception:
            return None

    for p in schema_proposals or []:
        act = p.get("action")
        if act == "create_table":
            role = p.get("role") or "fact"
            label = p.get("label") or "x"
            base = np_table_name(role, label)
            tname = _ensure_unique_name(taken_tables, base)

            # standard columns
            ddl.append(f'CREATE TABLE "{tname}" ({tname}_id BIGSERIAL PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());')
            summary.append(f"Create table {tname}")
            name_map[p.get("client_id") or label] = tname
            taken_cols.setdefault(tname, set()).update({f"{tname}_id", "created_at", "updated_at"})

        elif act == "add_column":
            table = p.get("table")
            if p.get("table_client_id") and p["table_client_id"] in name_map:
                table = name_map[p["table_client_id"]]
            if not table:
                summary.append("Skipped add_column (missing table)")
                continue

            col_label = p.get("label") or "x"
            col_type = (p.get("type") or "TEXT").upper()
            if col_type not in {"TEXT","INTEGER","BIGINT","DECIMAL","NUMERIC","DATE","TIMESTAMP","TIMESTAMPTZ","BOOLEAN"}:
                col_type = "TEXT"

            base_col = normalize_snake(col_label, maxlen=60)
            existing = taken_cols.setdefault(table, set())
            cname = _ensure_unique_name(existing, base_col, maxlen=63)

            ddl.append(f'ALTER TABLE "{table}" ADD COLUMN "{cname}" {col_type};')
            summary.append(f"Add column {table}.{cname} ({col_type})")
        else:
            # Extended schema operations
            if act == "alter_column_type":
                table = p.get("table")
                column = p.get("column")
                new_type = (p.get("new_type") or "TEXT").upper()
                using = p.get("using")  # optional USING expression
                if table and column and new_type:
                    stmt = f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {new_type}'
                    if using:
                        stmt += f' USING {using}'
                    stmt += ';'
                    ddl.append(stmt)
                    summary.append(f"Alter column type {table}.{column} -> {new_type}")
                else:
                    summary.append("Skipped alter_column_type (missing args)")

            elif act == "set_not_null":
                table = p.get("table")
                column = p.get("column")
                is_not_null = bool(p.get("is_not_null", True))
                if table and column:
                    stmt = f'ALTER TABLE "{table}" ALTER COLUMN "{column}" '
                    stmt += 'SET NOT NULL;' if is_not_null else 'DROP NOT NULL;'
                    ddl.append(stmt)
                    summary.append(f"{'Set' if is_not_null else 'Drop'} NOT NULL {table}.{column}")
                else:
                    summary.append("Skipped set_not_null (missing args)")

            elif act == "set_primary_key":
                table = p.get("table")
                cols = p.get("columns") or []
                if table and cols:
                    conname = _pk_constraint_name(table) or f"{table}_pkey"
                    ddl.append(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{conname}";')
                    cols_sql = ", ".join(f'"{c}"' for c in cols)
                    ddl.append(f'ALTER TABLE "{table}" ADD PRIMARY KEY ({cols_sql});')
                    summary.append(f"Set primary key on {table} ({', '.join(cols)})")
                else:
                    summary.append("Skipped set_primary_key (missing args)")

            elif act == "drop_primary_key":
                table = p.get("table")
                if table:
                    conname = _pk_constraint_name(table) or f"{table}_pkey"
                    ddl.append(f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{conname}";')
                    summary.append(f"Drop primary key on {table}")
                else:
                    summary.append("Skipped drop_primary_key (missing args)")

            elif act == "set_auto_increment":
                # Postgres: create sequence if needed and set default nextval
                table = p.get("table")
                column = p.get("column")
                if table and column:
                    seq = f"{table}_{column}_seq"
                    ddl.append(f'CREATE SEQUENCE IF NOT EXISTS "public"."{seq}" OWNED BY "{table}"."{column}";')
                    ddl.append(f"ALTER TABLE \"{table}\" ALTER COLUMN \"{column}\" SET DEFAULT nextval('\"{seq}\"');")
                    # try to sync sequence to max(col)
                    ddl.append(
                        f"SELECT setval('\"{seq}\"', COALESCE((SELECT MAX(\"{column}\") FROM \"{table}\"), 0));"
                    )
                    summary.append(f"Set auto-increment on {table}.{column}")
                else:
                    summary.append("Skipped set_auto_increment (missing args)")
            else:
                summary.append(f"Unknown action: {act}")

    return {"ddl": ddl, "summary": summary, "name_map": name_map}

def apply_schema_changes(plan):
    """
    Execute DDL atomically. Raises on error.
    """
    ddl = plan.get("ddl") or []
    if not ddl:
        return
    with transaction.atomic():
        with connection.cursor() as cur:
            for sql in ddl:
                cur.execute(sql)
