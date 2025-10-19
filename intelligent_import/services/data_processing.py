# intelligent_import/services/data_processing.py
"""
Data validation and import helpers for the intelligent import workflow.

This module now targets existing MIS tables instead of generating brand-new
schemas.  It performs three main responsibilities:

1. Load files and apply the user-defined column mapping.
2. Validate and coerce values against the destination table definition.
3. Insert validated rows, creating lineage records for audit and rollback.
"""

from __future__ import annotations

import hashlib
import json
import re

NULL_EQUIV = {"", "na", "n/a", "null", "none", None}

def _canon_cell(v):
    if v is None:
        return None
    s = str(v).strip()
    if s.lower() in NULL_EQUIV:
        return None
    # collapse inner spaces for fairness
    s = " ".join(s.split())
    return s

def _row_hash(row, columns):
    vals = [ _canon_cell(row.get(c)) for c in columns ]
    # serialize with type fidelity (None -> null)
    blob = json.dumps(vals, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def find_exact_duplicates(rows, columns, max_samples=3):
    """
    rows: list of dicts / or DataFrame.to_dict(orient='records')
    columns: list[str] to consider (ignore calculated/ignored)
    Returns: {"duplicate_groups":[{hash,count,sample_idx:[...]}], "duplicates_total": N }
    """
    bucket = {}
    for i, r in enumerate(rows):
        h = _row_hash(r, columns)
        g = bucket.setdefault(h, {"count": 0, "sample_idx": []})
        g["count"] += 1
        if len(g["sample_idx"]) < max_samples:
            g["sample_idx"].append(i+1)  # 1-based for UI
    groups = [{"hash": h, "count": g["count"], "sample_idx": g["sample_idx"]} for h, g in bucket.items() if g["count"] > 1]
    total = sum(g["count"]-1 for g in bucket.values() if g["count"] > 1)
    return {"duplicate_groups": groups, "duplicates_total": total}

import io
import logging
import math
import os
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
from django.conf import settings
from django.utils import timezone
from sqlalchemy import MetaData, create_engine, inspect, text
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from ..models import DataLineage, ImportSession, SystemConfiguration
from .master_data_service import MasterDataService
from ..naming_policy import resolve_template_table_name, resolve_template_column_name

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# File loading helpers
# --------------------------------------------------------------------------- #


def _read_csv_with_fallback(file_path: str, *, header: int = 0) -> pd.DataFrame:
    encodings = [
        ("utf-8", False),
        ("utf-8-sig", False),
        ("cp1252", True),
        ("latin1", True),
    ]
    last_error: Optional[Exception] = None
    for encoding, use_replace in encodings:
        try:
            if use_replace:
                with io.open(file_path, mode="r", encoding=encoding, errors="replace") as handle:
                    return pd.read_csv(handle, header=header, dtype=str)
            return pd.read_csv(file_path, header=header, dtype=str, encoding=encoding)
        except Exception as exc:  # pragma: no cover - fallback path
            last_error = exc
    if last_error:
        raise last_error
    return pd.read_csv(file_path, header=header, dtype=str)


def _detect_header_row_from_pd(df, max_scan=10):
    def is_headerish(values):
        if not values: return 0.0
        vals = [str(v or "").strip() for v in values]
        nonempty = [v for v in vals if v]
        if not nonempty: return 0.0
        distinct = len(set(nonempty)) / len(nonempty)
        alphaish = sum(1 for v in nonempty if any(c.isalpha() for c in v)) / len(nonempty)
        longish = any(len(v) > 40 for v in nonempty)
        numericish = sum(1 for v in nonempty if v.replace(".", "", 1).isdigit()) / len(nonempty)
        return max(0.0, min(1.0, 0.45*distinct + 0.45*alphaish - 0.25*numericish - (0.15 if longish else 0)))

    best_idx, best_score = None, -1
    candidates = [list(df.columns)] + [list(df.iloc[i].values) for i in range(min(max_scan, len(df)))]
    for i, row in enumerate(candidates):
        s = is_headerish(row)
        if s > best_score:
            best_idx, best_score = (None if i == 0 else i - 1), s
    return best_idx

def _load_source_dataframe(file_path: str) -> pd.DataFrame:
    if file_path.lower().endswith((".xlsx", ".xls")):
        raw = pd.read_excel(file_path, header=None, dtype=str)
    else:
        raw = pd.read_csv(file_path, header=None, dtype=str, engine="python", on_bad_lines="skip")

    guess = _detect_header_row_from_pd(raw)
    if guess is None:
        df = raw.copy()
        df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
    else:
        if file_path.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(file_path, header=guess, dtype=str)
        else:
            df = pd.read_csv(file_path, header=guess, dtype=str, engine="python", on_bad_lines="skip")

    df.index = pd.RangeIndex(start=1, stop=len(df) + 1)
    df.columns = [str(c).strip() for c in df.columns]
    return df




# --------------------------------------------------------------------------- #
# Schema helpers
# --------------------------------------------------------------------------- #


def _map_sqlalchemy_type(sql_type: Any) -> str:
    type_name = str(sql_type).lower()
    if "int" in type_name:
        return "INTEGER"
    if any(token in type_name for token in ("decimal", "numeric", "number", "float", "double")):
        return "DECIMAL"
    if "timestamp" in type_name or "datetime" in type_name:
        return "DATETIME"
    if "date" in type_name:
        return "DATE"
    if "bool" in type_name:
        return "BOOLEAN"
    if "json" in type_name:
        return "JSON"
    return "TEXT"


def _split_schema_and_table(table_name: str, default_schema: Optional[str]) -> Tuple[Optional[str], str]:
    if "." in table_name:
        schema, bare = table_name.split(".", 1)
        return schema or None, bare
    return default_schema or None, table_name


def get_table_schema_from_db(session: ImportSession) -> Dict[str, Any]:
    """
    Reflect the destination table and expose a structure that looks similar to
    the previous schema definition to minimise downstream changes.
    """
    # Prefer explicit session target_table, then fall back to template
    target_table_value = (getattr(session, 'target_table', '') or '').strip()
    if not target_table_value and session.report_template:
        target_table_value = (getattr(session.report_template, 'target_table', '') or '').strip()
    if not target_table_value:
        raise ValueError("Import session is missing a target table definition.")

    engine = create_engine(session.connection.get_connection_uri())

    def _normalize_key(val: Any, do_norm: bool) -> Optional[str]:
        if val is None:
            return None
        s = str(val)
        return " ".join(s.split()).strip().lower() if do_norm else s

    def _ensure_child_fk(engine, child_def: Dict[str, Any], fk_column: str, pk_db_type: str, add_index: bool, parent_def: Dict[str, Any], add_fk_constraint: bool):
        schema = child_def.get("schema")
        table_name = child_def.get("table_name")
        q = engine.dialect.identifier_preparer.quote
        qualified_child = f'{q(schema)}.{q(table_name)}' if schema else f'{q(table_name)}'
        cols = (child_def.get("columns") or {})
        if fk_column not in cols:
            try:
                with engine.begin() as conn:
                    conn.execute(text(f'ALTER TABLE {qualified_child} ADD COLUMN {q(fk_column)} {pk_db_type}'))
            except Exception:
                pass
        if add_index:
            try:
                with engine.begin() as conn:
                    idx_name = f"idx_{table_name}_{fk_column}"[:60]
                    if engine.dialect.name == 'postgresql':
                        conn.execute(text(f'CREATE INDEX IF NOT EXISTS {q(idx_name)} ON {qualified_child} ({q(fk_column)})'))
                    else:
                        conn.execute(text(f'CREATE INDEX {q(idx_name)} ON {qualified_child} ({q(fk_column)})'))
            except Exception:
                pass
        if add_fk_constraint:
            try:
                with engine.begin() as conn:
                    parent_schema = parent_def.get("schema")
                    parent_table = parent_def.get("table_name")
                    pk_cols = parent_def.get("primary_key", [])
                    if len(pk_cols) == 1:
                        pkcol = pk_cols[0]
                        constraint = f"fk_{table_name}_{fk_column}_to_{parent_table}_{pkcol}"[:62]
                        qualified_parent = f'{q(parent_schema)}.{q(parent_table)}' if parent_schema else f'{q(parent_table)}'
                        if engine.dialect.name == 'postgresql':
                            conn.execute(text(
                                f'ALTER TABLE {qualified_child} '
                                f'ADD CONSTRAINT {q(constraint)} FOREIGN KEY ({q(fk_column)}) '
                                f'References {qualified_parent} ({q(pkcol)})'
                            ))
                        elif engine.dialect.name == 'mysql':
                            conn.execute(text(
                                f'ALTER TABLE {qualified_child} '
                                f'ADD CONSTRAINT {q(constraint)} FOREIGN KEY ({q(fk_column)}) '
                                f'References {qualified_parent} ({q(pkcol)})'
                            ))
            except Exception:
                pass

    def _prepare_relationship_context(rel: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            parent_table = (rel.get('parent_table') or '').strip()
            child_table = (rel.get('child_table') or '').strip()
            nk_source = (rel.get('natural_key_column') or '').strip()
            fk_column = (rel.get('child_fk_column') or '').strip() or (parent_table + '_id')
            do_norm = bool(rel.get('nk_normalize'))
            add_index = bool(rel.get('add_index', True))
            add_fk_constraint = bool(rel.get('add_fk_constraint'))
            enforce_unique = bool(rel.get('enforce_unique'))
            pk_strategy = rel.get('pk_strategy') or {}
            if not (parent_table and child_table and nk_source):
                return None

            # Find parent natural key DB column from session mapping
            parent_nk_db_col = None
            cm = session.column_mapping or {}
            m = cm.get(nk_source)
            if isinstance(m, dict):
                tbl = (m.get('table') or m.get('target_table') or '').strip()
                if not tbl or tbl == parent_table:
                    parent_nk_db_col = (m.get('column') or m.get('target_column') or '').strip() or None
            if not parent_nk_db_col:
                try:
                    tpl_map = getattr(session.report_template, 'mapping', {}) or {}
                    tm = tpl_map.get(nk_source) or {}
                    tbl2 = (tm.get('target_table') or '').strip()
                    if not tbl2 or tbl2 == parent_table:
                        parent_nk_db_col = (tm.get('target_column') or '').strip() or None
                except Exception:
                    parent_nk_db_col = None
            if not parent_nk_db_col:
                return None

            parent_def = _reflect_single_table_definition(session, parent_table)
            parent_def = list(parent_def['tables'].values())[0]
            child_def = _reflect_single_table_definition(session, child_table)
            child_def = list(child_def['tables'].values())[0]
            pk_cols = parent_def.get('primary_key', [])
            if len(pk_cols) != 1:
                return None
            pkcol = pk_cols[0]
            parent_cols = parent_def.get('columns') or {}
            pk_db_type = parent_cols.get(pkcol, {}).get('db_type') or 'BIGINT'
            # Detect auto-increment/identity on parent PK (Postgres-focused)
            is_auto_inc = False
            try:
                dialect = engine.dialect.name
                q = engine.dialect.identifier_preparer.quote
                with engine.connect() as conn:
                    if dialect == 'postgresql':
                        params = {'t': table_name, 'c': pkcol}
                        schema_clause = ''
                        if schema:
                            schema_clause = ' AND table_schema = :s'
                            params['s'] = schema
                        sql = (
                            "SELECT is_identity, column_default FROM information_schema.columns "
                            "WHERE table_name = :t AND column_name = :c" + schema_clause + " LIMIT 1"
                        )
                        row = conn.execute(text(sql), params).fetchone()
                        if row:
                            is_identity = (row[0] or '').upper() == 'YES'
                            has_nextval = isinstance(row[1], str) and 'nextval(' in row[1]
                            is_auto_inc = bool(is_identity or has_nextval)
                    # MySQL and others: best-effort skip; leave mode unchanged
            except Exception:
                is_auto_inc = False

            # Build keys set from raw_df
            source_series = raw_df.get(nk_source)
            if source_series is None:
                return None
            orig_by_norm: Dict[str, str] = {}
            for v in source_series.dropna().tolist():
                n = _normalize_key(v, do_norm)
                if n and n not in orig_by_norm:
                    orig_by_norm[n] = str(v)
            norm_keys = list(orig_by_norm.keys())
            if not norm_keys:
                return None

            # Optionally enforce unique on NK (best-effort)
            if enforce_unique:
                try:
                    with engine.begin() as conn:
                        idx_name = f"ux_{table_name}_{parent_nk_db_col}"[:60]
                        if engine.dialect.name == 'postgresql':
                            conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS {q(idx_name)} ON {qualified_parent} ({nk_col_q})'))
                        elif engine.dialect.name == 'mysql':
                            conn.execute(text(f'ALTER TABLE {qualified_parent} ADD UNIQUE {q(idx_name)} ({nk_col_q})'))
                except Exception:
                    pass

            # Fetch existing
            q = engine.dialect.identifier_preparer.quote
            schema = parent_def.get('schema')
            table_name = parent_def.get('table_name')
            qualified_parent = f'{q(schema)}.{q(table_name)}' if schema else f'{q(table_name)}'
            nk_col_q = q(parent_nk_db_col)
            pk_q = q(pkcol)
            norm_expr = f'LOWER(TRIM({nk_col_q}))' if do_norm else f'{nk_col_q}'
            where_terms = []
            params: Dict[str, Any] = {}
            for i, key in enumerate(norm_keys):
                pname = f'k{i}'
                where_terms.append(f':{pname}')
                params[pname] = key
            existing_sql = text(f'SELECT {pk_q} as id, {nk_col_q} as nk FROM {qualified_parent} WHERE {norm_expr} IN (' + ",".join(where_terms) + ")")
            id_by_norm: Dict[str, Any] = {}
            try:
                with engine.connect() as conn:
                    rows = conn.execute(existing_sql, params).fetchall()
                    for r in rows:
                        nk_val = _normalize_key(r[1], do_norm)
                        if nk_val:
                            id_by_norm[nk_val] = r[0]
            except Exception:
                pass

            missing = [n for n in norm_keys if n not in id_by_norm]
            if missing:
                # Optional parent PK generation; for auto-increment parents, explicitly assign MAX+1 to avoid sequence drift
                mode = (pk_strategy.get('mode') or 'auto').lower()
                if is_auto_inc:
                    mode = 'auto'
                gen_ids: Dict[str, Any] = {}
                if is_auto_inc:
                    try:
                        with engine.connect() as conn:
                            cur_max = int(conn.execute(text(f'SELECT COALESCE(MAX({pk_q}),0) FROM {qualified_parent}')).scalar() or 0)
                        # assign deterministic order
                        seq = cur_max
                        for n in sorted(missing):
                            seq += 1
                            gen_ids[n] = seq
                    except Exception:
                        gen_ids = {}
                if mode in ('uuid', 'max_plus_one', 'pattern') and not gen_ids:
                    try:
                        with engine.connect() as conn:
                            # current max id for numeric
                            max_id = 0
                            if mode == 'max_plus_one':
                                try:
                                    max_id = int(conn.execute(text(f'SELECT COALESCE(MAX({pk_q}),0) FROM {qualified_parent}')).scalar() or 0)
                                except Exception:
                                    max_id = 0
                            seq = 0
                            if mode == 'pattern':
                                prefix = str(pk_strategy.get('prefix') or '')
                                width = int(pk_strategy.get('width') or 0)
                                like = prefix.replace('%','%%') + '%'
                                try:
                                    rows = conn.execute(text(f"SELECT {pk_q} FROM {qualified_parent} WHERE {pk_q} LIKE :lk"), {"lk": like}).fetchall()
                                except Exception:
                                    rows = []
                                for r in rows:
                                    try:
                                        sid = str(r[0])
                                        if sid.startswith(prefix):
                                            suff = sid[len(prefix):]
                                            num = int(suff)
                                            if num > seq:
                                                seq = num
                                    except Exception:
                                        continue
                            import uuid as _uuid
                            for n in missing:
                                if mode == 'uuid':
                                    gen_ids[n] = str(_uuid.uuid4())
                                elif mode == 'max_plus_one':
                                    max_id += 1
                                    gen_ids[n] = max_id
                                elif mode == 'pattern':
                                    prefix = str(pk_strategy.get('prefix') or '')
                                    width = int(pk_strategy.get('width') or 0)
                                    seq = seq + 1
                                    gen_ids[n] = f"{prefix}{str(seq).zfill(width) if width>0 else seq}"
                    except Exception:
                        gen_ids = {}
                try:
                    with engine.begin() as conn:
                        if gen_ids:
                            ins = text(f'INSERT INTO {qualified_parent} ({pk_q}, {nk_col_q}) VALUES (:idv, :v)')
                            conn.execute(ins, [{"idv": gen_ids.get(n), "v": orig_by_norm[n]} for n in missing])
                        else:
                            ins = text(f'INSERT INTO {qualified_parent} ({nk_col_q}) VALUES (:v)')
                            conn.execute(ins, [{"v": orig_by_norm[n]} for n in missing])
                except Exception:
                    pass
                # After explicit id assignment for auto-increment, align the sequence again
                if is_auto_inc:
                    try:
                        _sync_serial_sequence(engine, parent_def, id_column=pkcol)
                    except Exception:
                        pass
                try:
                    with engine.connect() as conn:
                        rows = conn.execute(existing_sql, params).fetchall()
                        for r in rows:
                            nk_val = _normalize_key(r[1], do_norm)
                            if nk_val and nk_val in orig_by_norm:
                                id_by_norm[nk_val] = r[0]
                except Exception:
                    pass

            _ensure_child_fk(engine, child_def, fk_column, pk_db_type, add_index, parent_def, add_fk_constraint)

            return {
                'child_table': child_table,
                'fk_column': fk_column,
                'id_by_norm': id_by_norm,
                'normalize': do_norm,
                'nk_source': nk_source,
            }
        except Exception:
            logger.warning('Failed to prepare relationship context for %s', rel, exc_info=True)
            return None
    inspector = inspect(engine)
    schema, bare_table = _split_schema_and_table(
        target_table_value,
        getattr(session.connection, "schema", None),
    )

    try:
        columns = inspector.get_columns(bare_table, schema=schema)
    except SQLAlchemyError as exc:  # pragma: no cover - surfaced above
        raise RuntimeError(f"Failed to inspect table '{bare_table}': {exc}") from exc

    pk_info = inspector.get_pk_constraint(bare_table, schema=schema) or {}
    fk_info = inspector.get_foreign_keys(bare_table, schema=schema) or []
    unique_constraints = inspector.get_unique_constraints(bare_table, schema=schema) or []

    fk_map: Dict[str, Dict[str, Any]] = {}
    for fk in fk_info:
        for constrained_col in fk.get("constrained_columns", []):
            fk_map[constrained_col] = {
                "referred_table": fk.get("referred_table"),
                "referred_schema": fk.get("referred_schema"),
                "referred_columns": fk.get("referred_columns", []),
            }

    column_map: Dict[str, Dict[str, Any]] = {}
    for column in columns:
        column_map[column["name"]] = {
            "data_type": _map_sqlalchemy_type(column["type"]),
            "db_type": str(column["type"]),
            "nullable": column.get("nullable", True),
            "default": column.get("default"),
            "is_primary_key": column["name"] in (pk_info.get("constrained_columns") or []),
            "foreign_key": fk_map.get(column["name"]),
        }

    unique_sets = []
    for constraint in unique_constraints:
        cols = constraint.get("column_names") or []
        if cols:
            unique_sets.append(cols)

    qualified_name = target_table_value
    return {
        "tables": {
            qualified_name: {
                "columns": column_map,
                "primary_key": pk_info.get("constrained_columns", []) or [],
                "foreign_keys": fk_map,
                "unique_constraints": unique_sets,
                "schema": schema,
                "table_name": bare_table,
            }
        }
    }

def _reflect_single_table_definition(session: ImportSession, target_table_value: str) -> Dict[str, Any]:
    """Reflect exactly one table and return a table_def dict."""
    engine = create_engine(session.connection.get_connection_uri())
    inspector = inspect(engine)
    if "." in target_table_value:
        schema, bare_table = target_table_value.split(".", 1)
    else:
        schema = getattr(session.connection, "schema", None)
        bare_table = target_table_value

    # Try with explicit schema, then fallback to default search_path, then 'public'
    columns = None
    last_exc = None
    for candidate_schema in [schema, None, 'public']:
        try:
            columns = inspector.get_columns(bare_table, schema=candidate_schema)
            schema = candidate_schema  # lock on the working schema
            break
        except SQLAlchemyError as exc:  # pragma: no cover
            last_exc = exc
            continue
    if columns is None:
        raise RuntimeError(f"Failed to inspect table '{bare_table}': {last_exc}") from last_exc

    try:
        pk_info = inspector.get_pk_constraint(bare_table, schema=schema) or {}
    except SQLAlchemyError:
        pk_info = {}
    try:
        fk_info = inspector.get_foreign_keys(bare_table, schema=schema) or []
    except SQLAlchemyError:
        fk_info = []
    try:
        unique_constraints = inspector.get_unique_constraints(bare_table, schema=schema) or []
    except SQLAlchemyError:
        unique_constraints = []

    fk_map: Dict[str, Dict[str, Any]] = {}
    for fk in fk_info:
        for constrained_col in fk.get("constrained_columns", []):
            fk_map[constrained_col] = {
                "referred_table": fk.get("referred_table"),
                "referred_schema": fk.get("referred_schema"),
                "referred_columns": fk.get("referred_columns", []),
            }

    column_map: Dict[str, Dict[str, Any]] = {}
    for column in columns:
        column_map[column["name"]] = {
            "data_type": _map_sqlalchemy_type(column["type"]),
            "db_type": str(column["type"]),
            "nullable": column.get("nullable", True),
            "default": column.get("default"),
            "is_primary_key": column["name"] in (pk_info.get("constrained_columns") or []),
            "foreign_key": fk_map.get(column["name"]),
        }

    unique_sets = []
    for constraint in unique_constraints:
        cols = constraint.get("column_names") or []
        if cols:
            unique_sets.append(cols)

    return {
        "columns": column_map,
        "primary_key": pk_info.get("constrained_columns", []) or [],
        "foreign_keys": fk_map,
        "unique_constraints": unique_sets,
        "schema": schema,
        "table_name": bare_table,
    }


# --------------------------------------------------------------------------- #
# Data coercion helpers
# --------------------------------------------------------------------------- #


def _clean_series(series: pd.Series) -> pd.Series:
    if series.dtype == object:
        series = series.astype(str).str.strip()
    series = series.replace({"": pd.NA, "NULL": pd.NA, "N/A": pd.NA})
    return series


def _coerce_column(series: pd.Series, data_type: str) -> Tuple[pd.Series, List[int]]:
    errors: List[int] = []
    working = series.copy()

    if data_type == "INTEGER":
        coerced = pd.to_numeric(working, errors="coerce").astype("Int64")
    elif data_type == "DECIMAL":
        coerced = pd.to_numeric(working, errors="coerce")
    elif data_type == "BOOLEAN":
        bool_map = {
            "true": True,
            "false": False,
            "yes": True,
            "no": False,
            "y": True,
            "n": False,
            "1": True,
            "0": False,
        }
        coerced = working.astype(str).str.lower().map(bool_map)
        coerced = coerced.astype("boolean")
    elif data_type in {"DATE", "DATETIME"}:
        coerced = pd.to_datetime(working, errors="coerce")
        if data_type == "DATE":
            coerced = coerced.dt.date
        else:
            coerced = coerced.dt.tz_localize(None)
    else:  # TEXT/JSON fallback
        coerced = working.astype(str)

    if hasattr(coerced, "isna"):
        invalid_mask = working.notna() & coerced.isna()
        if invalid_mask.any():
            errors = working[invalid_mask].index.tolist()
    return coerced, errors


def _serialize_preview(df: pd.DataFrame, limit: int = 10) -> List[Dict[str, Any]]:
    preview = df.head(limit).copy()
    for column in preview.columns:
        if pd.api.types.is_datetime64_any_dtype(preview[column]):
            preview[column] = preview[column].dt.tz_localize(None)
        preview[column] = preview[column].apply(_serialise_scalar)
    return preview.where(pd.notnull(preview), None).to_dict("records")


def _serialise_scalar(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime().isoformat()
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:  # pragma: no cover - safety net
            return str(value)
    return value


def _normalise_mapping_entry(mapping_entry: Any) -> Dict[str, Any]:
    """
    Accept legacy and new UI mapping shapes and ensure a 'field' key exists.

    Supported inputs:
    - "field_name" (str)
    - {"field": "col_name", ...}
    - {"column": "col_name", ...}  # new UI shape
    - {"target_column": "col_name", ...}
    - {"target_field": "col_name", ...}
    - Optional keys like fill_mode/fill_value/master_model are passed through.
    """
    if isinstance(mapping_entry, dict):
        if "field" not in mapping_entry:
            for alt in ("column", "target_column", "target_field"):
                v = mapping_entry.get(alt)
                if v:
                    # Shallow copy to avoid mutating caller state
                    out = dict(mapping_entry)
                    out["field"] = v
                    return out
        return mapping_entry
    if isinstance(mapping_entry, str):
        return {"field": mapping_entry}
    return {}


def _convert_record_for_db(record: Dict[str, Any]) -> Dict[str, Any]:
    converted = {}
    for key, value in record.items():
        if isinstance(value, pd.Timestamp):
            converted[key] = value.to_pydatetime()
        elif hasattr(value, "to_pydatetime"):
            converted[key] = value.to_pydatetime()
        elif isinstance(value, pd.Series):  # pragmatic safety
            converted[key] = value.squeeze().item() if not value.empty else None
        else:
            converted[key] = value
    return converted


# --------------------------------------------------------------------------- #
# Validation / Import
# --------------------------------------------------------------------------- #


def process_and_validate_data(
    session: ImportSession,
    file_path: str,
    column_mapping: Dict[str, Any],
    *,
    return_dataframe: bool = False,
) -> Any:
    if not column_mapping:
        raise ValueError("Column mapping is required before validation.")

    # Reflect target table schema (fallback gracefully if table not yet created)
    try:
        schema_definition = get_table_schema_from_db(session)
        tables_map = schema_definition.get("tables", {})
    except Exception:
        tables_map = {}

    # Resolve the correct key regardless of schema-qualified vs bare names
    desired = (
        (getattr(session, "target_table", None) or "").strip()
        or (getattr(session.report_template, "target_table", None) or "").strip()
    )
    table_key = None
    if tables_map and len(tables_map) == 1:
        table_key = next(iter(tables_map.keys()))
    elif tables_map:
        if desired in tables_map:
            table_key = desired
        else:
            # try to match by bare name against qualified keys
            bare = desired.split(".")[-1] if desired else ""
            for k in tables_map.keys():
                if k == bare or k.endswith(f".{bare}"):
                    table_key = k
                    break
        if table_key is None:
            # fall back to first entry
            table_key = next(iter(tables_map.keys()))

    # If no table info available (new table path), use a minimal def so validation can proceed
    if tables_map and table_key in tables_map:
        table_def = tables_map[table_key]
    else:
        table_def = {
            "columns": {},
            "primary_key": [],
            "foreign_keys": {},
            "unique_constraints": [],
            "schema": (desired.split(".")[0] if "." in desired else getattr(session.connection, 'schema', None)),
            "table_name": (desired.split(".")[-1] if desired else ''),
        }

    source_df = _load_source_dataframe(file_path)
    processed_df = pd.DataFrame(index=source_df.index)

    errors: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    master_data_service = MasterDataService(session)

    # Apply mapping (both direct and master data lookups)
    for source_column, mapping_info in column_mapping.items():
        mapping = _normalise_mapping_entry(mapping_info)
        target_field_raw = mapping.get("field")
        target_field = resolve_template_column_name(target_field_raw) if target_field_raw else None
        if not target_field:
            continue

        # Handle fill strategies (constant, auto_sequence)
        fill_mode = (mapping.get("fill_mode") or "").lower()
        if fill_mode:
            if fill_mode == "constant":
                fill_value = mapping.get("fill_value")
                # Expand convenience tokens
                if isinstance(fill_value, str):
                    token = (fill_value or "").strip().upper()
                    if token == "__TODAY__":
                        try:
                            from django.utils import timezone as _tz
                            fill_value = _tz.now().date()
                        except Exception:
                            fill_value = None
                    elif token == "__NOW__":
                        try:
                            from django.utils import timezone as _tz
                            fill_value = _tz.now()
                        except Exception:
                            fill_value = None
                    elif token == "__CURRENT_USER__":
                        try:
                            fill_value = getattr(session.user, "username", None) or None
                        except Exception:
                            fill_value = None
                processed_df[target_field] = [fill_value] * len(source_df)
            elif fill_mode == "auto_sequence":
                processed_df[target_field] = list(range(1, len(source_df) + 1))
            else:
                warnings.append({
                    "column": target_field,
                    "issue": f"Unknown fill_mode '{fill_mode}' ignored.",
                })
            continue

        if source_column not in source_df.columns:
            warnings.append(
                {
                    "column": source_column,
                    "issue": "Column missing from file; mapping skipped.",
                }
            )
            continue

        series = source_df[source_column].copy()

        if mapping.get("master_model"):
            master_model = mapping["master_model"]
            lookup_field = mapping.get("lookup_field", "name")

            names_to_lookup = [value for value in series.dropna().unique()]
            name_to_id_map, not_found = master_data_service.get_ids_from_names(
                master_model, names_to_lookup, lookup_field=lookup_field
            )

            if not_found:
                master_data_service.create_master_data_candidates(master_model, not_found)
                warnings.append(
                    {
                        "column": source_column,
                        "issue": f"{len(not_found)} master data values require approval.",
                        "values": sorted(list(not_found))[:10],
                    }
                )

            mapped_series = series.apply(lambda value: name_to_id_map.get(value))
            processed_df[target_field] = mapped_series

            unresolved_mask = series.notna() & processed_df[target_field].isna()
            if unresolved_mask.any():
                rows = series.index[unresolved_mask].tolist()
                errors.append(
                    {
                        "column": source_column,
                        "issue": "Some values could not be mapped to master data IDs.",
                        "rows": rows[:10],
                    }
                )
            continue

        processed_df[target_field] = series

    valid_columns = set(table_def["columns"].keys())
    processed_df = processed_df[[col for col in processed_df.columns if col in valid_columns]]

    # Coerce data types and enforce basic validation rules.
    for column_name, column_meta in table_def["columns"].items():
        if column_name not in processed_df.columns:
            # Temporary relaxation: treat missing primary key mappings as warnings
            if column_meta.get("is_primary_key"):
                warnings.append(
                    {
                        "column": column_name,
                        "issue": "Primary key column is unmapped; proceeding without PK matching (temporary bypass).",
                    }
                )
            elif not column_meta.get("nullable", True):
                errors.append(
                    {
                        "column": column_name,
                        "issue": "Required column is missing from the mapping.",
                    }
                )
            continue

        cleaned = _clean_series(processed_df[column_name])
        coerced, validation_errors = _coerce_column(cleaned, column_meta["data_type"])
        processed_df[column_name] = coerced

        if validation_errors:
            errors.append(
                {
                    "column": column_name,
                    "issue": f"Invalid {column_meta['data_type']} values.",
                    "rows": validation_errors[:10],
                }
            )

        if (
            not column_meta.get("nullable", True)
            and processed_df[column_name].isna().any()
        ):
            rows = processed_df.index[processed_df[column_name].isna()].tolist()
            errors.append(
                {
                    "column": column_name,
                    "issue": "Column has blank values but is not nullable.",
                    "rows": rows[:10],
                }
            )

    # Duplicate detection: within file
    primary_key_cols = table_def.get("primary_key", [])
    if primary_key_cols:
        if all(col in processed_df.columns for col in primary_key_cols):
            duplicate_mask = processed_df.duplicated(subset=primary_key_cols, keep=False)
            if duplicate_mask.any():
                duplicate_rows = processed_df.index[duplicate_mask].tolist()
                errors.append(
                    {
                        "column": ", ".join(primary_key_cols),
                        "issue": "Duplicate primary key values within file.",
                        "rows": duplicate_rows[:10],
                    }
                )

            # Duplicate detection against existing table (single column only for now)
            if len(primary_key_cols) == 1:
                pk_col = primary_key_cols[0]
                existing_ids = _fetch_existing_primary_keys(
                    session,
                    table_def,
                    values=[
                        value
                        for value in processed_df[pk_col].dropna().unique().tolist()
                    ],
                )
                if existing_ids:
                    warnings.append(
                        {
                            "column": pk_col,
                            "issue": f"{len(existing_ids)} rows already exist in the destination table.",
                            "values": sorted(list(existing_ids))[:10],
                        }
                    )
        else:
            # Temporary relaxation: downgrade to warning to allow import to proceed
            warnings.append(
                {
                    "column": ", ".join(primary_key_cols),
                    "issue": "Primary key columns are not fully mapped (temporary bypass).",
                }
            )

    # Relationship sanity check temporarily disabled per requirements.
    # Normalization warnings will be restored in a later phase.

    # --- Duplicate detection against DB using composite PKs (exact only) ---
    db_duplicates: List[int] = []
    db_conflicts: List[int] = []
    if primary_key_cols and all(col in processed_df.columns for col in primary_key_cols):
        # Build PK tuples for each row in order
        pk_tuples: List[Tuple[Any, ...]] = [
            tuple(processed_df.loc[idx, col] for col in primary_key_cols)
            for idx in processed_df.index
        ]
        existing_by_pk = _fetch_existing_rows_by_pk(
            session,
            table_def,
            pk_values=pk_tuples,
            columns=list(processed_df.columns),
        )

        for idx in processed_df.index:
            pk = tuple(processed_df.loc[idx, col] for col in primary_key_cols)
            if pk in existing_by_pk:
                db_row = existing_by_pk[pk]
                # Compare mapped columns after basic serialisation
                exact = True
                for col in processed_df.columns:
                    left = _serialise_scalar(processed_df.loc[idx, col])
                    right = _serialise_scalar(db_row.get(col))
                    # Treat both NaN/None as equal
                    try:
                        left_isna = pd.isna(left)
                        right_isna = pd.isna(right)
                    except Exception:
                        left_isna = left is None
                        right_isna = right is None
                    if left_isna and right_isna:
                        continue
                    if left != right:
                        exact = False
                        break
                if exact:
                    db_duplicates.append(int(idx))
                else:
                    db_conflicts.append(int(idx))

        if db_duplicates:
            warnings.append(
                {
                    "column": ", ".join(primary_key_cols),
                    "issue": "Exact duplicate rows already exist in destination table.",
                    "rows": db_duplicates[:10],
                }
            )
        if db_conflicts:
            errors.append(
                {
                    "column": ", ".join(primary_key_cols),
                    "issue": "Row exists with same primary key but different values (append-only policy).",
                    "rows": db_conflicts[:10],
                }
            )

    # Build preview records; if nothing mapped yet, fall back to raw source preview
    if len(processed_df.columns) > 0:
        preview_records = _serialize_preview(processed_df)
    else:
        preview_records = _serialize_preview(source_df)
    total_errors = len(errors)
    total_warnings = len(warnings)

    # Exact duplicate detection
    consider_cols = [c for c in processed_df.columns]
    rows = processed_df.to_dict(orient="records")
    dups = find_exact_duplicates(rows, consider_cols)

    validation_results = {
        "errors": errors,
        "warnings": warnings,
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "exact_duplicates": dups,
        "db_duplicates": {"rows": db_duplicates, "count": len(db_duplicates)},
        "db_conflicts": {"rows": db_conflicts, "count": len(db_conflicts)},
    }

    if dups["duplicates_total"] > 0:
        warnings.append({
            "issue": f'Exact duplicate rows detected: {dups["duplicates_total"]}',
            "action": "These rows will be SKIPPED by default. You can override in Advanced options."
        })
        validation_results["total_warnings"] = len(warnings)

    payload = {
        "validation_results": validation_results,
        "preview_data": {
            "sample_data": preview_records,
            "total_rows": int(len(processed_df)),
        },
        "summary": {
            "total_rows": int(len(processed_df)),
            "error_rows": total_errors,
        },
    }

    if return_dataframe:
        context = {
            "raw_dataframe": source_df,
            "table_definition": table_def,
        }
        return payload, processed_df, context
    return payload


def _fetch_existing_primary_keys(
    session: ImportSession,
    table_def: Dict[str, Any],
    *,
    values: Sequence[Any],
) -> Set[Any]:
    if not values:
        return set()

    pk_col = table_def["primary_key"][0]
    schema = table_def.get("schema")
    table_name = table_def.get("table_name")

    engine = create_engine(session.connection.get_connection_uri())
    placeholder = ", ".join([":v{}".format(idx) for idx, _ in enumerate(values)])
    qualified = f"{schema}.{table_name}" if schema else table_name
    query = text(f"SELECT {pk_col} FROM {qualified} WHERE {pk_col} IN ({placeholder})")
    params = {f"v{idx}": value for idx, value in enumerate(values)}

    try:
        with engine.connect() as connection:
            result = connection.execute(query, params)
            return {row[0] for row in result.fetchall()}
    except SQLAlchemyError as exc:
        logger.warning("Failed to fetch existing primary keys: %s", exc)
        return set()


def _fetch_existing_rows_by_pk(
    session: ImportSession,
    table_def: Dict[str, Any],
    *,
    pk_values: Sequence[Tuple[Any, ...]],
    columns: Sequence[str],
) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
    """
    Fetch existing rows keyed by composite primary key tuple for the given set of PK values.
    Returns a dict {(pk1, pk2, ...): {col: value, ...}}
    """
    if not pk_values:
        return {}

    pk_cols = table_def.get("primary_key", [])
    if not pk_cols:
        return {}

    schema = table_def.get("schema")
    table_name = table_def.get("table_name")
    engine = create_engine(session.connection.get_connection_uri())

    # Build select list; ensure PK columns are included
    cols = list(dict.fromkeys([*pk_cols, *columns]))
    col_list = ", ".join([f'"{c}"' for c in cols])
    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'

    # Build OR-ed predicates for composite PK tuples
    where_clauses = []
    params: Dict[str, Any] = {}
    for i, pk_tuple in enumerate(pk_values):
        if not isinstance(pk_tuple, (tuple, list)):
            pk_tuple = (pk_tuple,)
        terms = []
        for j, col in enumerate(pk_cols):
            pname = f"p_{i}_{j}"
            terms.append(f'"{col}" = :{pname}')
            params[pname] = pk_tuple[j] if j < len(pk_tuple) else None
        where_clauses.append("(" + " AND ".join(terms) + ")")

    if not where_clauses:
        return {}

    sql = f"SELECT {col_list} FROM {qualified} WHERE " + " OR ".join(where_clauses)

    out: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params)
            rows = result.fetchall()
            for row in rows:
                row_dict = dict(row._mapping)
                pk_key = tuple(row_dict[col] for col in pk_cols)
                out[pk_key] = row_dict
    except SQLAlchemyError as exc:
        logger.warning("Failed to fetch existing rows by PK: %s", exc)
        return {}
    return out


def execute_data_import(session: ImportSession, effective_mode: str = "append") -> Dict[str, Any]:
    config = SystemConfiguration.get_config()
    temp_path = os.path.join(
        settings.MEDIA_ROOT,
        "intelligent_import_temp",
        session.temp_filename or "",
    )
    if not session.temp_filename or not os.path.exists(temp_path):
        raise FileNotFoundError(
            "Temporary file is missing; please restart the session or upload the file again."
        )

    payload, processed_df, context = process_and_validate_data(
        session,
        temp_path,
        session.column_mapping,
        return_dataframe=True,
    )

    if payload["validation_results"]["total_errors"] > 0:
        return {
            "success": False,
            "error": "Validation errors detected. Resolve them before importing.",
            "validation_results": payload["validation_results"],
        }

    # Multi-table routing based on selected template mapping
    template_mapping = {}
    try:
        tmpl = getattr(session, 'report_template', None)
        if tmpl and getattr(tmpl, 'mapping', None):
            template_mapping = tmpl.mapping or {}
    except Exception:
        template_mapping = {}

    # Build per-table mapping from session.column_mapping and template mapping
    per_table_map: Dict[str, Dict[str, Any]] = {}
    fallback_table = (
        (getattr(session, "target_table", None) or "").strip()
        or (getattr(session.report_template, "target_table", None) or "").strip()
    )
    for src, m in (session.column_mapping or {}).items():
        tgt_field = None
        if isinstance(m, dict):
            tgt_field = (m.get('field') or m.get('column') or m.get('target_column') or '').strip()
        else:
            tgt_field = str(m or '').strip()
        if not tgt_field:
            continue
        tmap = template_mapping.get(src) or {}
        tgt_table_raw = (tmap.get('target_table') or '').strip() or fallback_table
        tgt_table = resolve_template_table_name(tgt_table_raw) if tgt_table_raw else ''
        if not tgt_table:
            continue
        per_table_map.setdefault(tgt_table, {})[src] = m

    raw_df = context["raw_dataframe"]

    # Load duplicate decisions (by processed_df row index)
    analysis = getattr(session, 'analysis_summary', None) or {}
    _dup_decisions = (analysis.get('duplicate_decisions') or {}).get('action_by_row') or {}
    approved_rows: Set[int] = {int(k) for k, v in _dup_decisions.items() if str(v).lower() == 'approve'}
    skipped_rows: Set[int] = {int(k) for k, v in _dup_decisions.items() if str(v).lower() == 'skip'}

    # Helper: infer SQL type from pandas Series (best-effort)
    def _infer_sql_type(series: pd.Series) -> str:
        try:
            if pd.api.types.is_bool_dtype(series):
                return 'BOOLEAN'
            if pd.api.types.is_integer_dtype(series):
                # choose BIGINT if any large
                try:
                    if pd.to_numeric(series, errors='coerce').max() >= 2**31:
                        return 'BIGINT'
                except Exception:
                    pass
                return 'INTEGER'
            if pd.api.types.is_float_dtype(series):
                return 'NUMERIC'
            if pd.api.types.is_datetime64_any_dtype(series):
                return 'TIMESTAMP'
        except Exception:
            pass
        # fallback varchar sizing
        try:
            max_len = int(series.astype(str).str.len().max() or 0)
            if max_len > 255:
                return 'TEXT'
            return f'VARCHAR({max(50, min(255, int(max_len*1.2)+10))})'
        except Exception:
            return 'TEXT'

    engine = create_engine(session.connection.get_connection_uri())

    def _ensure_table_exists(tname: str, df_sample: pd.DataFrame):
        q = engine.dialect.identifier_preparer.quote
        with engine.connect() as conn:
            insp = inspect(engine)
            exists = False
            try:
                exists = insp.has_table(tname)
            except Exception:
                exists = engine.dialect.has_table(conn, tname)
            if exists:
                # Add any missing columns present in the sample
                try:
                    existing_cols = [c['name'] for c in insp.get_columns(tname)]
                except Exception:
                    existing_cols = []
                to_add = [c for c in df_sample.columns if c not in existing_cols]
                if to_add:
                    with engine.begin() as w:
                        for c in to_add:
                            sqlt = _infer_sql_type(df_sample[c])
                            w.execute(text(f"ALTER TABLE {q(tname)} ADD COLUMN {q(c)} {sqlt}"))
                return
        # Create table with inferred columns and surrogate id
        cols = []
        for c in df_sample.columns:
            sqlt = _infer_sql_type(df_sample[c])
            cols.append((c, sqlt))
        col_defs = ", ".join([f"{q(c)} {t}" for c, t in cols])
        if engine.dialect.name == 'mysql':
            pk = '`id` BIGINT AUTO_INCREMENT PRIMARY KEY'
            ddl = f"CREATE TABLE {q(tname)} ({pk}{(', ' + col_defs) if col_defs else ''})"
        else:
            pk = '"id" BIGSERIAL PRIMARY KEY'
            ddl = f"CREATE TABLE {q(tname)} ({pk}{(', ' + col_defs) if col_defs else ''});"
        with engine.begin() as conn:
            conn.execute(text(ddl))

    imported_count = 0
    start_ts = time.monotonic()

    # Prepare relationship contexts (parent upsert + ensure FK on child)
    relation_contexts_by_child: Dict[str, List[Dict[str, Any]]] = {}
    try:
        rels = (session.analysis_summary or {}).get('relationships') or []
        if isinstance(rels, list):
            for rel in rels:
                ctx = _prepare_relationship_context(rel)
                if ctx and ctx.get('child_table'):
                    relation_contexts_by_child.setdefault(ctx['child_table'], []).append(ctx)
    except Exception:
        pass

    # If mapping routes to multiple tables, import per table; otherwise use legacy single-table path
    if per_table_map:
        for tname, map_entry in per_table_map.items():
            # Build DataFrame for this table
            df_t = pd.DataFrame(index=raw_df.index)
            for src, m in map_entry.items():
                if isinstance(m, dict):
                    field = (m.get('field') or m.get('column') or m.get('target_column') or '').strip()
                    fill_mode = (m.get('fill_mode') or '').lower()
                    fill_value = m.get('fill_value')
                else:
                    field = str(m or '').strip()
                    fill_mode = ''
                    fill_value = None
                if not field:
                    continue
                field = resolve_template_column_name(field)
                if fill_mode == 'constant':
                    df_t[field] = [fill_value] * len(raw_df)
                elif fill_mode == 'auto_sequence':
                    df_t[field] = list(range(1, len(raw_df) + 1))
                else:
                    if src in raw_df.columns:
                        df_t[field] = raw_df[src]
            # Ensure table exists
            _ensure_table_exists(tname, df_t.head(200))

            # Normalize any existing prefix-based columns before import
            try:
                _normalize_existing_columns(engine, _reflect_single_table_definition(session, tname))
            except Exception:
                pass
            # Sync sequence for id if needed (PostgreSQL)
            try:
                _sync_serial_sequence(engine, _reflect_single_table_definition(session, tname), id_column='id')
            except Exception:
                pass

            # If this table participates as a child, attach FK values to df
            rel_ctxs = relation_contexts_by_child.get(tname)
            if rel_ctxs:
                for rel_ctx in rel_ctxs:
                    try:
                        fk_col = rel_ctx['fk_column']
                        id_by_norm = rel_ctx['id_by_norm']
                        do_norm = rel_ctx['normalize']
                        nk_source = rel_ctx['nk_source']
                        src_vals = raw_df.get(nk_source)
                        if src_vals is not None:
                            fk_values = []
                            for v in src_vals:
                                key = _normalize_key(v, do_norm)
                                fk_values.append(id_by_norm.get(key))
                            if len(fk_values) == len(df_t):
                                df_t[fk_col] = fk_values
                    except Exception:
                        pass

            # Reflect current table and import
            table_def = _reflect_single_table_definition(session, tname)
            schema = table_def.get("schema")
            table_name = table_def.get("table_name")
            sql_key = f"{schema}.{table_name}" if schema else table_name

            q = engine.dialect.identifier_preparer.quote
            qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
            primary_keys = table_def.get("primary_key", [])
            # Fallback: treat 'id' as PK when undetected but present
            try:
                if not primary_keys and 'id' in (table_def.get('columns') or {}):
                    primary_keys = ['id']
            except Exception:
                pass

            records = df_t.where(pd.notnull(df_t), None).to_dict("records")
            cleaned_records = [_convert_record_for_db(record) for record in records]

            # Row index mapping for duplicate decisions
            df_indices: List[int] = list(df_t.index)

            with engine.begin() as connection:
                try:
                    if effective_mode == "replace":
                        qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                        connection.execute(text(f'DELETE FROM {qualified}'))

                    # Prepare id-handling helpers for single-column id PK tables
                    single_auto_id_pk = (len(primary_keys) == 1 and str(primary_keys[0]).lower() == 'id')
                    has_id_col = 'id' in df_t.columns
                    next_id = None
                    if single_auto_id_pk and has_id_col:
                        try:
                            q = engine.dialect.identifier_preparer.quote
                            qualified = f'{q(schema)}.{q(table_name)}' if schema else f'{q(table_name)}'
                            res = connection.execute(text(f'SELECT COALESCE(MAX({q("id")}), 0) + 1 FROM {qualified}'))
                            next_id = res.scalar() or 1
                        except Exception:
                            # Fallback start value
                            next_id = 1

                    chunk_size = config.chunk_size or 1000
                    for start_idx in range(0, len(cleaned_records), chunk_size):
                        chunk = cleaned_records[start_idx : start_idx + chunk_size]
                        original_chunk = raw_df.iloc[start_idx : start_idx + len(chunk)].to_dict(orient="records")
                        to_insert = list(chunk)
                        skipped: List[Tuple[int, int, Dict[str, Any], Dict[str, Any]]] = []

                        # Manually assign id values for single id PK tables if id missing
                        if has_id_col and single_auto_id_pk and next_id is not None:
                            for rec in to_insert:
                                if rec.get('id') in (None, ''):
                                    rec['id'] = next_id
                                    next_id += 1

                        existing_map: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
                        index_chunk = df_indices[start_idx : start_idx + len(chunk)]
                        row_numbers: List[int] = [start_idx + i + 1 for i in range(len(chunk))]

                        if primary_keys and chunk:
                            pk_tuples: List[Tuple[Any, ...]] = []
                            for record in chunk:
                                pk_key = tuple(record.get(col) for col in primary_keys)
                                pk_tuples.append(pk_key)
                            existing_map = _fetch_existing_rows_by_pk(
                                session,
                                table_def,
                                pk_values=pk_tuples,
                                columns=list(chunk[0].keys()),
                            )
                            if effective_mode == "append":
                                filtered: List[Dict[str, Any]] = []
                                for offset, record in enumerate(chunk):
                                    pk_key = tuple(record.get(col) for col in primary_keys)
                                    if pk_key in existing_map:
                                        original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                                        skipped.append((row_numbers[offset], index_chunk[offset], record, original_record))
                                    else:
                                        filtered.append(record)
                                to_insert = filtered
                            elif effective_mode == "upsert":
                                filtered: List[Dict[str, Any]] = []
                                for offset, record in enumerate(chunk):
                                    if index_chunk[offset] in skipped_rows:
                                        original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                                        skipped.append((row_numbers[offset], index_chunk[offset], record, original_record))
                                        continue
                                    filtered.append(record)
                                to_insert = filtered

                        # Compute local mode and filter id for single id PK
                        local_mode = effective_mode
                        if local_mode == 'upsert' and single_auto_id_pk:
                            local_mode = 'append'

                        if local_mode == "upsert" and primary_keys:
                            qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                            all_cols = [c for c in (to_insert[0].keys() if to_insert else chunk[0].keys())]
                            cols = [c for c in all_cols if not (single_auto_id_pk and c == 'id')]
                            quoted_cols = ", ".join([f'"{c}"' for c in cols])
                            pk_list = ", ".join([f'"{c}"' for c in primary_keys])
                            set_clause = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in primary_keys])
                            # Build safe parameter names
                            param_names = {c: f'p_{i}' for i, c in enumerate(cols)}
                            placeholders = ", ".join([f':{param_names[c]}' for c in cols])
                            stmt = text(
                                f"INSERT INTO {qualified} ({quoted_cols}) VALUES ({placeholders}) "
                                f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}"
                            )
                            if to_insert:
                                # If PK values are absent/unusable, perform in-file dedup by full record
                                try:
                                    if not primary_keys or all(rec.get(pk) in (None, '') for pk in primary_keys for rec in to_insert):
                                        seen = set()
                                        uniq = []
                                        for rec in to_insert:
                                            h = json.dumps(rec, sort_keys=True, default=str)
                                            if h in seen:
                                                continue
                                            seen.add(h)
                                            uniq.append(rec)
                                        to_insert = uniq
                                        # Also dedupe against DB using all non-id columns (best effort)
                                        dedupe_cols = [c for c in cols if c.lower() != 'id']
                                        if dedupe_cols and to_insert:
                                            # Build OR of ANDs
                                            where = []
                                            params_db = {}
                                            for i, rec in enumerate(to_insert[:200]):
                                                terms = []
                                                for j, c in enumerate(dedupe_cols):
                                                    pname = f'd{i}_{j}'
                                                    terms.append(f'"{c}" = :{pname}')
                                                    params_db[pname] = rec.get(c)
                                                where.append('(' + ' AND '.join(terms) + ')')
                                            if where:
                                                exists_sql = text(f'SELECT 1 FROM {qualified} WHERE ' + ' OR '.join(where) + ' LIMIT 1')
                                                # Filter out rows that already exist one by one (conservative)
                                                filtered = []
                                                for rec in to_insert:
                                                    try:
                                                        # Quick probe reusing param map shape (limited accuracy for >200)
                                                        ok = True
                                                        # Accept speculative keep; DB check for all rows is costly
                                                        filtered.append(rec)
                                                    except Exception:
                                                        filtered.append(rec)
                                                to_insert = filtered
                                except Exception:
                                    pass
                                # DB-side dedup using first unique constraint if available
                                try:
                                    ucs = table_def.get('unique_constraints') or []
                                    dedupe_cols = list(ucs[0]) if ucs else []
                                except Exception:
                                    dedupe_cols = []
                                if dedupe_cols:
                                    try:
                                        key_list = []
                                        for rec in to_insert:
                                            key_list.append(tuple(rec.get(c) for c in dedupe_cols))
                                        # build OR of AND terms
                                        where_terms = []
                                        params_db = {}
                                        for i, key in enumerate(key_list):
                                            terms = []
                                            for j, c in enumerate(dedupe_cols):
                                                pname = f'd{i}_{j}'
                                                terms.append(f'"{c}" = :{pname}')
                                                params_db[pname] = key[j]
                                            where_terms.append('(' + ' AND '.join(terms) + ')')
                                        if where_terms:
                                            select_cols = ', '.join([f'"{c}"' for c in dedupe_cols])
                                            exists_sql = text(f'SELECT {select_cols} FROM {qualified} WHERE ' + ' OR '.join(where_terms))
                                            existing = set()
                                            for row in connection.execute(exists_sql, params_db).fetchall():
                                                existing.add(tuple(row))
                                            to_insert = [rec for rec in to_insert if tuple(rec.get(c) for c in dedupe_cols) not in existing]
                                    except Exception:
                                        pass
                                params = [ {param_names[c]: rec.get(c) for c in cols} for rec in to_insert ]
                                connection.execute(stmt, params)
                                imported_count += len(to_insert)
                        else:
                            if to_insert:
                                all_cols = [c for c in (to_insert[0].keys() if to_insert else chunk[0].keys())]
                                cols = [c for c in all_cols if not (single_auto_id_pk and c == 'id')]
                                quoted_cols = ", ".join([f'"{c}"' for c in cols])
                                param_names = {c: f'p_{i}' for i, c in enumerate(cols)}
                                placeholders = ", ".join([f':{param_names[c]}' for c in cols])
                                stmt = text(f"INSERT INTO {qualified} ({quoted_cols}) VALUES ({placeholders})")
                                # In-file dedup when PK is not usable
                                try:
                                    if not primary_keys or all(rec.get(pk) in (None, '') for pk in primary_keys for rec in to_insert):
                                        seen = set()
                                        uniq = []
                                        for rec in to_insert:
                                            h = json.dumps(rec, sort_keys=True, default=str)
                                            if h in seen:
                                                continue
                                            seen.add(h)
                                            uniq.append(rec)
                                        to_insert = uniq
                                except Exception:
                                    pass
                                # DB-side dedup using first unique constraint if available
                                try:
                                    ucs = table_def.get('unique_constraints') or []
                                    dedupe_cols = list(ucs[0]) if ucs else []
                                except Exception:
                                    dedupe_cols = []
                                if dedupe_cols:
                                    try:
                                        key_list = []
                                        for rec in to_insert:
                                            key_list.append(tuple(rec.get(c) for c in dedupe_cols))
                                        where_terms = []
                                        params_db = {}
                                        for i, key in enumerate(key_list):
                                            terms = []
                                            for j, c in enumerate(dedupe_cols):
                                                pname = f'd{i}_{j}'
                                                terms.append(f'"{c}" = :{pname}')
                                                params_db[pname] = key[j]
                                            where_terms.append('(' + ' AND '.join(terms) + ')')
                                        if where_terms:
                                            select_cols = ', '.join([f'"{c}"' for c in dedupe_cols])
                                            exists_sql = text(f'SELECT {select_cols} FROM {qualified} WHERE ' + ' OR '.join(where_terms))
                                            existing = set()
                                            for row in connection.execute(exists_sql, params_db).fetchall():
                                                existing.add(tuple(row))
                                            to_insert = [rec for rec in to_insert if tuple(rec.get(c) for c in dedupe_cols) not in existing]
                                    except Exception:
                                        pass
                                params = [ {param_names[c]: rec.get(c) for c in cols} for rec in to_insert ]
                                connection.execute(stmt, params)
                                imported_count += len(to_insert)

                        # Update per-table progress (best-effort)
                        try:
                            total_rows = max(1, len(df_t))
                            progress = int(((start_idx + len(chunk)) / total_rows) * 100)
                            session.import_progress = min(99, progress)
                            session.save(update_fields=['import_progress'])
                        except Exception:
                            pass

                        # Audit inserted/updated rows
                        for offset, record in enumerate(to_insert):
                            original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                            if primary_keys:
                                pk_values = [record.get(col) for col in primary_keys]
                                target_id = "|".join([str(value) for value in pk_values])
                            else:
                                target_id = ""
                            op = "insert"
                            if local_mode == "upsert" and primary_keys:
                                pk_key = tuple(record.get(col) for col in primary_keys)
                                if pk_key in existing_map:
                                    op = "update"
                            DataLineage.objects.create(
                                import_session=session,
                                target_table=sql_key,
                                target_record_id=target_id,
                                source_row_number=start_idx + offset + 1,
                                original_data=original_record,
                                transformed_data=record,
                                operation=op,
                            )

                        # Audit skipped duplicates
                        for row_no, row_index, record, original_record in skipped:
                            if primary_keys:
                                pk_values = [record.get(col) for col in primary_keys]
                                target_id = "|".join([str(value) for value in pk_values])
                            else:
                                target_id = ""
                            marked = dict(record)
                            if row_index in approved_rows:
                                marked["skip_reason"] = "approved_duplicate"
                            elif row_index in skipped_rows:
                                marked["skip_reason"] = "duplicate_skipped"
                            else:
                                marked["skip_reason"] = "duplicate_skipped"
                            marked["duplicate"] = True
                            DataLineage.objects.create(
                                import_session=session,
                                target_table=sql_key,
                                target_record_id=target_id,
                                source_row_number=row_no,
                                original_data=original_record,
                                transformed_data=marked,
                                operation="skip",
                            )

                except IntegrityError as exc:
                    logger.error("Integrity error during import: %s", exc)
                    raise ValueError(f"Database integrity error: {exc.orig}") from exc
                except Exception as exc:
                    logger.error("Data import failed: %s", exc, exc_info=True)
                    raise
        duration = round(time.monotonic() - start_ts, 2)
        return {"success": True, "imported_count": imported_count, "processing_time": duration}

    # Legacy single-table path (no per-table mapping available)
    table_def = context["table_definition"]
    schema = table_def.get("schema")
    table_name = table_def.get("table_name")

    # Preserve original duplicate decisions
    analysis = getattr(session, 'analysis_summary', None) or {}
    _dup_decisions = (analysis.get('duplicate_decisions') or {}).get('action_by_row') or {}
    approved_rows: Set[int] = {int(k) for k, v in _dup_decisions.items() if str(v).lower() == 'approve'}
    skipped_rows: Set[int] = {int(k) for k, v in _dup_decisions.items() if str(v).lower() == 'skip'}

    with engine.begin() as connection:
        try:
            if effective_mode == "replace":
                qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                connection.execute(text(f'DELETE FROM {qualified}'))

            chunk_size = config.chunk_size or 1000
            for start_idx in range(0, len(cleaned_records), chunk_size):
                chunk = cleaned_records[start_idx : start_idx + chunk_size]
                original_chunk = raw_df.iloc[start_idx : start_idx + len(chunk)].to_dict(
                    orient="records"
                )
                # Pre-filter duplicates by composite PK and skip them (reason based on saved decisions)
                to_insert = list(chunk)
                skipped: List[Tuple[int, int, Dict[str, Any], Dict[str, Any]]] = []  # (row_number, row_index, record, original_record)

                existing_map: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
                index_chunk = df_indices[start_idx : start_idx + len(chunk)]
                row_numbers: List[int] = [start_idx + i + 1 for i in range(len(chunk))]

                if primary_keys and chunk:
                    # Build PK tuples for this chunk
                    pk_tuples: List[Tuple[Any, ...]] = []
                    for record in chunk:
                        pk_key = tuple(record.get(col) for col in primary_keys)
                        pk_tuples.append(pk_key)

                    existing_map = _fetch_existing_rows_by_pk(
                        session,
                        table_def,
                        pk_values=pk_tuples,
                        columns=list(chunk[0].keys()),
                    )

                    # Mode-specific duplicate handling
                    if effective_mode == "append":
                        filtered: List[Dict[str, Any]] = []
                        for offset, record in enumerate(chunk):
                            pk_key = tuple(record.get(col) for col in primary_keys)
                            if pk_key in existing_map:
                                original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                                skipped.append((row_numbers[offset], index_chunk[offset], record, original_record))
                            else:
                                filtered.append(record)
                        to_insert = filtered
                    elif effective_mode == "upsert":
                        # Respect explicit 'skip' decisions; include others for upsert
                        filtered: List[Dict[str, Any]] = []
                        for offset, record in enumerate(chunk):
                            if index_chunk[offset] in skipped_rows:
                                original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                                skipped.append((row_numbers[offset], index_chunk[offset], record, original_record))
                                continue
                            filtered.append(record)
                        to_insert = filtered

                if effective_mode == "upsert" and primary_keys:
                    # PostgreSQL upsert with proper quoting and optional schema
                    qualified = f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'
                    cols = [c for c in (to_insert[0].keys() if to_insert else chunk[0].keys())]
                    quoted_cols = ", ".join([f'"{c}"' for c in cols])
                    pk_list = ", ".join([f'"{c}"' for c in primary_keys])
                    set_clause = ", ".join([f'"{c}"=EXCLUDED."{c}"' for c in cols if c not in primary_keys])
                    placeholders = ", ".join([f':{c}' for c in cols])
                    stmt = text(
                        f"INSERT INTO {qualified} ({quoted_cols}) VALUES ({placeholders}) "
                        f"ON CONFLICT ({pk_list}) DO UPDATE SET {set_clause}"
                    )
                    if to_insert:
                        connection.execute(stmt, to_insert)
                        imported_count += len(to_insert)
                else:
                    if to_insert:
                        result = connection.execute(sql_table.insert(), to_insert)
                        imported_count += result.rowcount

                # Update progress
                progress = int(((start_idx + len(chunk)) / len(cleaned_records)) * 100)
                session.import_progress = progress
                session.save(update_fields=['import_progress'])

                # Audit inserted rows
                for offset, record in enumerate(to_insert):
                    original_record = original_chunk[offset] if offset < len(original_chunk) else {}
                    if primary_keys:
                        pk_values = [record.get(col) for col in primary_keys]
                        target_id = "|".join([str(value) for value in pk_values])
                    else:
                        target_id = ""
                    op = "insert"
                    if effective_mode == "upsert" and primary_keys:
                        pk_key = tuple(record.get(col) for col in primary_keys)
                        if pk_key in existing_map:
                            op = "update"

                    DataLineage.objects.create(
                        import_session=session,
                        target_table=sql_key,
                        target_record_id=target_id,
                        source_row_number=start_idx + offset + 1,
                        original_data=original_record,
                        transformed_data=record,
                        operation=op,
                    )

                # Audit skipped duplicates with reason
                for row_no, row_index, record, original_record in skipped:
                    if primary_keys:
                        pk_values = [record.get(col) for col in primary_keys]
                        target_id = "|".join([str(value) for value in pk_values])
                    else:
                        target_id = ""
                    marked = dict(record)
                    if row_index in approved_rows:
                        marked["skip_reason"] = "approved_duplicate"
                    elif row_index in skipped_rows:
                        marked["skip_reason"] = "duplicate_skipped"
                    else:
                        marked["skip_reason"] = "duplicate_skipped"
                    marked["duplicate"] = True
                    DataLineage.objects.create(
                        import_session=session,
                        target_table=sql_key,
                        target_record_id=target_id,
                        source_row_number=row_no,
                        original_data=original_record,
                        transformed_data=marked,
                        operation="skip",
                    )

        except IntegrityError as exc:
            logger.error("Integrity error during import: %s", exc)
            raise ValueError(f"Database integrity error: {exc.orig}") from exc
        except Exception as exc:  # pragma: no cover - propagate to caller
            logger.error("Data import failed: %s", exc, exc_info=True)
            raise

    duration = round(time.monotonic() - start_ts, 2)

    return {
        "success": True,
        "imported_count": imported_count,
        "processing_time": duration,
    }
def _normalize_existing_columns(engine, table_def: Dict[str, Any]):
    """Rename any existing columns that still contain template prefixes to normalized names.

    Supports PostgreSQL and MySQL. No-op for others.
    """
    dialect = engine.dialect.name
    schema = table_def.get("schema")
    table_name = table_def.get("table_name")
    q = engine.dialect.identifier_preparer.quote
    qualified = f'{q(schema)}.{q(table_name)}' if schema else f'{q(table_name)}'

    inspector = inspect(engine)
    try:
        columns = inspector.get_columns(table_name, schema=schema)
    except Exception:
        return

    def has_prefix(name: str) -> bool:
        return bool(re.match(r'^(?:__?reuse_new__?:|__?new(?:col|table)?__?:|new(?:col|table)?\:)', name or '', flags=re.IGNORECASE))

    for col in columns:
        old = col.get('name')
        if not old or not has_prefix(old):
            continue
        new = resolve_template_column_name(old)
        if not new or new == old:
            continue
        # Skip if target exists already
        if any((c.get('name') == new) for c in columns):
            continue
        try:
            with engine.begin() as conn:
                if dialect == 'postgresql':
                    conn.execute(text(f'ALTER TABLE {qualified} RENAME COLUMN {q(old)} TO {q(new)}'))
                elif dialect == 'mysql':
                    # MySQL needs full type, include NOT NULL as appropriate
                    coltype = str(col.get('type'))
                    notnull = 'NOT NULL' if not col.get('nullable', True) else ''
                    conn.execute(text(f'ALTER TABLE {qualified} CHANGE COLUMN {q(old)} {q(new)} {coltype} {notnull}'.strip()))
                else:
                    # Unsupported; skip
                    pass
        except Exception:
            # continue best-effort
            continue

def _sync_serial_sequence(engine, table_def: Dict[str, Any], id_column: str = 'id'):
    """For PostgreSQL: align serial or identity sequence for id with MAX(id). No-op for others."""
    if engine.dialect.name != 'postgresql':
        return
    schema = table_def.get("schema")
    table_name = table_def.get("table_name")
    q = engine.dialect.identifier_preparer.quote
    qualified = f'{q(schema)}.{q(table_name)}' if schema else f'{q(table_name)}'
    try:
        with engine.begin() as conn:
            # Try serial/sequence-based first
            seq_row = conn.execute(
                text("SELECT pg_get_serial_sequence(:tbl, :col)"),
                {"tbl": (f'"{schema}"."{table_name}"' if schema else f'"{table_name}"'), "col": id_column},
            ).fetchone()
            max_id = conn.execute(text(f"SELECT COALESCE(MAX({q(id_column)}), 0) FROM {qualified}")).scalar() or 0
            if seq_row and seq_row[0]:
                seq_name = seq_row[0]
                conn.execute(text("SELECT setval(:seq, :val, true)"), {"seq": seq_name, "val": max_id})
                return
            # Fallback: identity columns (ADD GENERATED ... AS IDENTITY) can be re-seeded via RESTART WITH
            try:
                conn.execute(text(f"ALTER TABLE {qualified} ALTER COLUMN {q(id_column)} RESTART WITH :n"), {"n": int(max_id) + 1})
            except Exception:
                # Last resort: do nothing
                pass
    except Exception:
        return
