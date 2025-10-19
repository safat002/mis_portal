# intelligent_import/services/schema_analyzer.py
"""
Analyzer that suggests target MIS tables and column mappings for imported files.

The previous implementation focused on proposing brand-new schemas.  This version
inspects the existing MIS database, ranks likely destination tables, and builds
column mapping suggestions so that uploaded files can be landed into the current
schema without creating new tables.
"""

from __future__ import annotations

import io
import logging
import os, re
from dataclasses import dataclass
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)

# Threshold for considering a similarity score as a valid match.
MIN_MATCH_THRESHOLD = 0.45


@dataclass
class TableMetadata:
    """
    Snapshot of an existing table (columns, primary keys, foreign keys).
    """

    table_name: str
    schema: Optional[str]
    columns: Dict[str, Dict[str, Any]]
    primary_key: List[str]
    foreign_keys: Dict[str, Dict[str, Any]]


import warnings

DATE_FORMATS = [
    "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y", "%d/%m/%Y",
    "%Y/%m/%d", "%d.%m.%Y", "%m.%d.%Y",
    "%Y-%m-%d %H:%M:%S", "%d-%m-%Y %H:%M:%S",
]

def _guess_date_format(series: pd.Series) -> str | None:
    s = series.dropna().astype(str).head(100)
    for fmt in DATE_FORMATS:
        try:
            pd.to_datetime(s, format=fmt, errors="raise")
            return fmt
        except Exception:
            continue
    return None

class SchemaAnalyzer:
    """
    Examine a file and recommend the most appropriate MIS table plus column mapping.
    """

    def __init__(self, db_connection, existing_templates: Optional[Iterable[Any]] = None) -> None:
        self.db_connection = db_connection
        self.existing_templates = list(existing_templates or [])
        self.engine: Optional[Engine] = self._create_engine()
        self.inspector = inspect(self.engine) if self.engine else None

        # Cache of discovered tables keyed by fully qualified name.
        self._table_cache: Dict[str, TableMetadata] = {}

    # --------------------------------------------------------------------- #
    # Public API
    # --------------------------------------------------------------------- #
    def analyze_file_structure(self, file_path: str, header_row: int = 0) -> Dict[str, Any]:
        """
        Produce a holistic analysis of the uploaded file:
        - basic file facts (rows, columns, sample rows)
        - ranked list of existing tables that look compatible
        - suggested column mapping for the top match
        """
        df = self._load_file(file_path, header_row)
        file_columns = list(df.columns)

        analysis: Dict[str, Any] = {
            "file_analysis": self._summarise_file(df),
            "suggested_target": None,
            "target_table_suggestions": [],
            "suggested_mapping": {},
            "unmapped_columns": file_columns,
            "template_match": None,
            "template_candidates": [],
            "confidence_score": 0.0,
            "detected_template_reason": None,
            "target_columns": {},
        }

        if not self.inspector:
            logger.warning("Database inspector unavailable; returning file-only analysis.")
            return analysis

        table_candidates = self._rank_candidate_tables(file_columns)
        analysis["target_table_suggestions"] = table_candidates

        template_candidates: List[Dict[str, Any]] = []
        filename_candidates = self._match_template_by_filename(file_path)
        if filename_candidates:
            template_candidates.extend(filename_candidates)

        table_meta: Optional[TableMetadata] = None
        top_candidate: Optional[Dict[str, Any]] = None
        if table_candidates:
            top_candidate = table_candidates[0]
            analysis["suggested_target"] = top_candidate
            analysis["confidence_score"] = round(top_candidate.get("score", 0.0), 3)

            table_meta = self._get_table_metadata(top_candidate["table_name"])
            if table_meta:
                column_candidate = self._match_template(
                    top_candidate["table_name"], file_columns
                )
                if column_candidate:
                    template_candidates.append(column_candidate)
            else:
                logger.warning(
                    "Unable to reflect table metadata for '%s'; skipping mapping suggestions.",
                    top_candidate["table_name"],
                )

        combined_templates = self._combine_template_candidates(template_candidates)
        if combined_templates:
            analysis["template_candidates"] = combined_templates
            best_template = combined_templates[0]
            analysis["template_match"] = best_template
            reasons = best_template.get("reasons") or []
            if reasons:
                analysis["detected_template_reason"] = reasons[0]
        else:
            best_template = None

        # Build a normalized model proposal regardless of direct template success.
        # This will be used to optionally suggest dimension creation for unmapped columns.
        try:
            normalized = self._propose_normalized_model(
                df,
                file_columns,
                top_candidate["table_name"] if top_candidate else None,
            )
        except Exception as exc:
            logger.debug("Normalized model proposal failed: %s", exc)
            normalized = {}
        analysis["normalized_proposal"] = normalized

        if table_meta:
            analysis["target_columns"] = table_meta.columns
            mapping, unmapped = self._suggest_column_mapping(
                file_columns,
                table_meta,
                template_match=best_template,
                dataframe=df,
            )
            # Enhance mapping: suggest creating dimension table + FK for unmapped categorical columns.
            try:
                fk_map = (normalized.get("fk_map") if isinstance(normalized, dict) else {}) or {}
                for col in list(unmapped):
                    if col in fk_map:
                        mapping[col] = {
                            **fk_map[col],
                            "note": "Suggest creating dimension and storing FK in fact",
                        }
                        unmapped.remove(col)
            except Exception as exc:
                logger.debug("Augmenting unmapped columns with FK suggestions failed: %s", exc)
            analysis["suggested_mapping"] = mapping
            analysis["unmapped_columns"] = unmapped
        else:
            logger.info(
                "No table metadata available for mapping suggestion for file '%s'.", file_path
            )

        return analysis

    # --------------------------------------------------------------------- #
    # File helpers
    # --------------------------------------------------------------------- #
    def _read_csv_with_fallback(self, path, header=None):
        import pandas as pd
        # Keep your previous options if you had them
        return pd.read_csv(path, header=header, dtype=str, engine="python", on_bad_lines="skip")

    def _load_file(self, file_path: str, header_row=None):
        """
        Backward-compatible: accepts legacy header_row but ignores it (we auto-detect).
        """
        import os, pandas as pd, logging
        logger = logging.getLogger(__name__)

        # Read raw without header so we can detect the header row
        try:
            if file_path.lower().endswith((".xlsx", ".xls")):
                raw = pd.read_excel(file_path, header=None, dtype=str)
            else:
                raw = self._read_csv_with_fallback(file_path, header=None)
        except Exception:
            logger.exception("Failed to read file %s", file_path)
            raise

        guess = self._detect_header_row_from_df(raw)
        if guess is None:
            df = raw.copy()
            df.columns = [f"col_{i+1}" for i in range(df.shape[1])]
        else:
            if file_path.lower().endswith((".xlsx", ".xls")):
                df = pd.read_excel(file_path, header=guess, dtype=str)
            else:
                df = self._read_csv_with_fallback(file_path, header=guess)

        df.columns = [str(c).strip() for c in df.columns]
        try:
            df._source_name = os.path.basename(file_path)
        except Exception:
            pass
        return df

    def _summarise_file(self, df: pd.DataFrame) -> Dict[str, Any]:
        sample_records = df.head(5).replace({pd.NA: None}).to_dict("records")
        return {
            "total_rows": int(len(df)),
            "total_columns": int(len(df.columns)),
            "columns": list(df.columns),
            "sample_data": sample_records,
        }

    # --------------------------------------------------------------------- #
    # Database metadata helpers
    # --------------------------------------------------------------------- #
    def _create_engine(self) -> Optional[Engine]:
        try:
            return create_engine(self.db_connection.get_connection_uri())
        except ModuleNotFoundError as exc:
            missing = getattr(exc, "name", "required database driver")
            logger.warning(
                "Cannot analyse tables for connection '%s'; "
                "install driver '%s' to enable intelligent mapping.",
                getattr(self.db_connection, "nickname", "unknown"),
                missing,
            )
        except Exception as exc:
            logger.warning(
                "Failed to create engine for connection '%s': %s",
                getattr(self.db_connection, "nickname", "unknown"),
                exc,
            )
        return None

    def _get_table_metadata(self, table_name: str) -> Optional[TableMetadata]:
        if not self.inspector:
            return None

        normalized = self._normalize_table_name(table_name)
        if normalized in self._table_cache:
            return self._table_cache[normalized]

        schema, bare_table = self._split_schema_and_table(table_name)
        try:
            columns = self.inspector.get_columns(bare_table, schema=schema)
        except SQLAlchemyError as exc:
            logger.debug("Skipping table '%s': %s", table_name, exc)
            return None

        pk_info = self.inspector.get_pk_constraint(bare_table, schema=schema) or {}
        fk_info = self.inspector.get_foreign_keys(bare_table, schema=schema) or []

        fk_map: Dict[str, Dict[str, Any]] = {}
        for fk in fk_info:
            for constrained_col in fk.get("constrained_columns", []):
                fk_map[constrained_col] = {
                    "referred_table": fk.get("referred_table"),
                    "referred_schema": fk.get("referred_schema"),
                    "referred_columns": fk.get("referred_columns"),
                }

        column_map: Dict[str, Dict[str, Any]] = {}
        for col in columns:
            column_map[col["name"]] = {
                "data_type": self._map_sqlalchemy_type(col["type"]),
                "db_type": str(col["type"]),
                "nullable": col.get("nullable", True),
                "default": col.get("default"),
                "is_primary_key": col["name"] in (pk_info.get("constrained_columns") or []),
                "foreign_key": fk_map.get(col["name"]),
            }

        metadata = TableMetadata(
            table_name=bare_table if not schema else f"{schema}.{bare_table}",
            schema=schema,
            columns=column_map,
            primary_key=pk_info.get("constrained_columns", []) or [],
            foreign_keys=fk_map,
        )

        self._table_cache[normalized] = metadata
        return metadata

    def _rank_candidate_tables(self, file_columns: List[str]) -> List[Dict[str, Any]]:
        candidates: List[Dict[str, Any]] = []
        if not self.inspector:
            return candidates

        lower_file_cols = [self._normalize_value(col) for col in file_columns]
        table_names = self._collect_candidate_table_names()

        for table_name in table_names:
            metadata = self._get_table_metadata(table_name)
            if not metadata:
                continue

            matches: List[Tuple[str, str, float]] = []
            for source_col in file_columns:
                target_col, similarity = self._find_best_column_match(
                    source_col,
                    metadata.columns.keys(),
                )
                if similarity >= MIN_MATCH_THRESHOLD:
                    matches.append((source_col, target_col, round(similarity, 3)))

            if not matches:
                continue

            coverage = len(matches) / max(len(file_columns), 1)
            average_similarity = sum(sim for _, _, sim in matches) / len(matches)
            score = round((coverage * 0.6) + (average_similarity * 0.4), 3)

            # Slight bonus when an official template exists for the table.
            if any(t.target_table == metadata.table_name for t in self.existing_templates):
                score = min(1.0, score + 0.05)

            matched_columns = [
                {"source_column": src, "target_column": tgt, "similarity": sim}
                for src, tgt, sim in matches
            ]
            unmatched = [col for col in file_columns if col not in {m[0] for m in matches}]

            candidates.append(
                {
                    "table_name": metadata.table_name,
                    "score": score,
                    "matched_columns": matched_columns,
                    "unmatched_columns": unmatched,
                    "primary_key": metadata.primary_key,
                    "total_columns": len(metadata.columns),
                    "column_details": metadata.columns,
                }
            )

        candidates.sort(key=lambda item: item["score"], reverse=True)
        return candidates

    def _collect_candidate_table_names(self) -> List[str]:
        """
        Gather a sensible set of target tables. Priority is given to tables referenced
        by existing ReportTemplate instances; if none exist we fall back to listing
        every table in the connection.
        """
        template_tables = {
            template.target_table
            for template in self.existing_templates
            if getattr(template, "target_table", None)
        }
        if template_tables:
            return list(template_tables)

        try:
            schema = getattr(self.db_connection, "schema", None) or None
            return self.inspector.get_table_names(schema=schema)
        except SQLAlchemyError as exc:
            logger.warning("Unable to enumerate tables: %s", exc)
            return []

    def _match_template_by_filename(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Attempt to identify templates based on filename patterns defined on the template.
        """
        filename = os.path.basename(file_path or "").lower()
        if not filename or not self.existing_templates:
            return []

        matches: List[Dict[str, Any]] = []
        for template in self.existing_templates:
            patterns = getattr(template, "filename_patterns", None) or []
            for raw_pattern in patterns:
                pattern = (raw_pattern or "").strip()
                if not pattern:
                    continue

                regex = self._pattern_to_regex(pattern)
                matched = False
                try:
                    if re.search(regex, filename, re.IGNORECASE):
                        matched = True
                except re.error:
                    if pattern.lower() in filename:
                        matched = True

                if matched:
                    relative_score = len(pattern) / max(len(filename), 1)
                    score = round(min(0.95, max(0.5, relative_score)), 3)
                    matches.append(
                        {
                            "template_id": str(template.id),
                            "template_name": template.name,
                            "target_table": template.target_table,
                            "score": score,
                            "column_mapping": template.column_mapping or {},
                            "reason": "filename_pattern",
                            "pattern": pattern,
                        }
                    )
                    break  # Stop after first matching pattern per template
        return matches

    def _pattern_to_regex(self, pattern: str) -> str:
        """
        Convert simple wildcard patterns into regex. If the pattern already looks like a regex,
        return it unchanged.
        """
        if any(ch in pattern for ch in ["\\", "^", "$", "+", "?", "{", "}", "[", "]", "(", ")", "|"]):
            return pattern
        escaped = re.escape(pattern)
        return escaped.replace(r"\*", ".*")

    def _combine_template_candidates(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Merge template candidates gathered from different heuristics into a combined list
        with aggregated scores and reasons.
        """
        if not candidates:
            return []

        combined: Dict[str, Dict[str, Any]] = {}
        for candidate in candidates:
            template_id = candidate.get("template_id")
            if not template_id:
                continue

            entry = combined.setdefault(
                template_id,
                {
                    "template_id": template_id,
                    "template_name": candidate.get("template_name"),
                    "target_table": candidate.get("target_table"),
                    "score": 0.0,
                    "column_mapping": candidate.get("column_mapping") or {},
                    "reasons": [],
                    "details": [],
                },
            )

            score = candidate.get("score", 0.0) or 0.0
            entry["score"] = max(entry["score"], score)

            incoming_mapping = candidate.get("column_mapping") or {}
            if incoming_mapping and not entry.get("column_mapping"):
                entry["column_mapping"] = incoming_mapping

            reason = candidate.get("reason")
            if reason and reason not in entry["reasons"]:
                entry["reasons"].append(reason)

            extra = {
                key: val
                for key, val in candidate.items()
                if key
                not in {
                    "template_id",
                    "template_name",
                    "target_table",
                    "score",
                    "column_mapping",
                    "reason",
                }
            }
            if extra:
                entry["details"].append(extra)

        merged = list(combined.values())
        merged.sort(key=lambda item: item.get("score", 0.0), reverse=True)

        # Ensure reasons and details are serialisable lists
        for entry in merged:
            entry["reasons"] = entry.get("reasons") or []
            entry["details"] = entry.get("details") or []
            entry["score"] = round(entry.get("score", 0.0), 3)
        return merged

    # --------------------------------------------------------------------- #
    # Mapping helpers
    # --------------------------------------------------------------------- #
    def _suggest_column_mapping(
        self,
        file_columns: List[str],
        table_metadata: TableMetadata,
        *,
        template_match: Optional[Dict[str, Any]] = None,
        dataframe: Optional[pd.DataFrame] = None,
    ) -> Tuple[Dict[str, Any], List[str]]:
        mapping: Dict[str, Any] = {}
        unmatched: List[str] = []

        template_mapping = template_match.get("column_mapping") if template_match else {}
        normalized_template = {
            self._normalize_value(src): value for src, value in (template_mapping or {}).items()
        }

        for source_col in file_columns:
            normalized_source = self._normalize_value(source_col)
            template_suggestion = normalized_template.get(normalized_source)
            column_meta = None
            suggestion: Dict[str, Any] = {}
            confidence = 0.0

            if template_suggestion:
                suggestion = self._normalize_template_mapping(template_suggestion)
                target_field = suggestion.get("field")
                column_meta = table_metadata.columns.get(target_field)
                confidence = 0.95  # template matches get very high confidence

            if not suggestion:
                target_field, similarity = self._find_best_column_match(
                    source_col,
                    table_metadata.columns.keys(),
                )
                if similarity >= MIN_MATCH_THRESHOLD:
                    column_meta = table_metadata.columns.get(target_field)
                    suggestion = {"field": target_field}
                    confidence = similarity

            if not suggestion or not column_meta:
                unmatched.append(source_col)
                continue

            fk_details = column_meta.get("foreign_key") or {}
            mapping[source_col] = {
                "field": suggestion["field"],
                "confidence": round(confidence, 3),
                "data_type": column_meta["data_type"],
                "nullable": column_meta["nullable"],
                "is_primary_key": column_meta["is_primary_key"],
            }

            if fk_details:
                mapping[source_col]["master_table"] = fk_details.get("referred_table")
                referred_schema = fk_details.get("referred_schema")
                if referred_schema:
                    mapping[source_col]["master_schema"] = referred_schema

            if "master_model" in suggestion:
                mapping[source_col]["master_model"] = suggestion["master_model"]
            if "lookup_field" in suggestion:
                mapping[source_col]["lookup_field"] = suggestion["lookup_field"]

            if dataframe is not None:
                sample_series = dataframe[source_col]
                mapping[source_col]["sample_values"] = self._sample_values(sample_series)

        return mapping, unmatched

    def _match_template(self, table_name: str, file_columns: List[str]) -> Optional[Dict[str, Any]]:
        """
        Identify the best ReportTemplate for the resolved table (if any).
        """
        candidates = [
            template
            for template in self.existing_templates
            if getattr(template, "target_table", None) == table_name
        ]
        if not candidates:
            return None

        best_template = None
        best_score = 0.0

        file_column_set = {self._normalize_value(col) for col in file_columns}

        for template in candidates:
            mapping = template.column_mapping or {}
            normalized_keys = {self._normalize_value(key) for key in mapping.keys()}
            intersection = file_column_set.intersection(normalized_keys)
            if not normalized_keys:
                continue

            coverage = len(intersection) / len(normalized_keys)
            file_coverage = len(intersection) / max(len(file_column_set), 1)
            score = (coverage * 0.7) + (file_coverage * 0.3)

            if score > best_score:
                best_score = score
                best_template = template

        if not best_template:
            return None

        return {
            "template_id": str(best_template.id),
            "template_name": best_template.name,
            "score": round(best_score, 3),
            "column_mapping": best_template.column_mapping or {},
            "reason": "column_similarity",
            "target_table": table_name,
        }

    # --------------------------------------------------------------------- #
    # Utility helpers
    # --------------------------------------------------------------------- #
    def _find_best_column_match(
        self,
        source_column: str,
        candidate_columns: Iterable[str],
    ) -> Tuple[Optional[str], float]:
        normalized_source = self._normalize_value(source_column)
        best_match: Optional[str] = None
        best_score = 0.0

        for candidate in candidate_columns:
            candidate_norm = self._normalize_value(candidate)
            score = SequenceMatcher(None, normalized_source, candidate_norm).ratio()
            if score > best_score:
                best_match = candidate
                best_score = score

        return best_match, best_score

    def _normalize_template_mapping(self, value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            return {"field": value}
        return {}

    def _normalize_value(self, raw: Any) -> str:
        text = str(raw or "").lower()
        text = re.sub(r"[^a-z0-9]+", "_", text)
        return text.strip("_")

    def _normalize_table_name(self, table_name: str) -> str:
        schema, bare = self._split_schema_and_table(table_name)
        return f"{schema}.{bare}".lower() if schema else bare.lower()

    def _split_schema_and_table(self, table_name: str) -> Tuple[Optional[str], str]:
        if "." in table_name:
            schema, bare = table_name.split(".", 1)
            return schema or None, bare
        schema = getattr(self.db_connection, "schema", None) or None
        return schema, table_name

    def _map_sqlalchemy_type(self, sql_type: Any) -> str:
        type_name = str(sql_type).lower()
        if "int" in type_name:
            return "INTEGER"
        if any(token in type_name for token in ("decimal", "numeric", "number", "float")):
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

    def _sample_values(self, series: pd.Series, sample_size: int = 5) -> List[Any]:
        sample = series.dropna().unique()[:sample_size]
        replacements = []
        for value in sample:
            if isinstance(value, (pd.Timestamp,)):
                replacements.append(value.isoformat())
            else:
                replacements.append(value)
        return replacements

# --- robust header-row detection ---------------------------------
    def _is_headerish(self, values):
        if not values:
            return 0.0
        vals = [str(v or "").strip() for v in values]
        nonempty = [v for v in vals if v]
        if not nonempty:
            return 0.0
        distinct = len(set(nonempty)) / len(nonempty)        # many different tokens
        alphaish = sum(1 for v in nonempty if any(c.isalpha() for c in v)) / len(nonempty)
        very_long = any(len(v) > 40 for v in nonempty)       # penalize giant cells
        numericish = sum(1 for v in nonempty if v.replace(".", "", 1).isdigit()) / len(nonempty)
        score = 0.45*distinct + 0.45*alphaish - 0.25*numericish - (0.15 if very_long else 0)
        return max(0.0, min(1.0, score))

    def _detect_header_row_from_df(self, df, max_scan=10):
        """
        Look at df.columns and first N rows and pick the most 'header-like' row.
        Returns:
        None -> use existing df.columns (no data row is header)
        int  -> 0-based row index to pass as pandas header=
        """
        best_idx, best_score = None, -1.0
        candidates = [list(df.columns)] + [list(df.iloc[i].values) for i in range(min(max_scan, len(df)))]
        for i, row in enumerate(candidates):
            s = self._is_headerish(row)
            if s > best_score:
                best_score = s
                best_idx = None if i == 0 else i - 1   # i==0 means keep current columns
        return best_idx

    # --- NEW: column classification & normalized proposal ------------------
    def _classify_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        Return {"col_name": "measure"|"dimension"} based on dtype, cardinality, and token patterns.
        """
        out = {}
        for col in df.columns:
            s = df[col].dropna().astype(str)
            # numeric?
            numeric_ratio = pd.to_numeric(s, errors="coerce").notna().mean() if len(s) else 0.0
            unique_ratio = (s.nunique() / max(len(s), 1)) if len(s) else 0.0
            
            fmt = _guess_date_format(s)
            if fmt:
                parsed = pd.to_datetime(s, errors="coerce", format=fmt)
            else:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    parsed = pd.to_datetime(s, errors="coerce")  # fall back to dateutil quietly

            looks_date = parsed.notna().mean() if len(s) else 0.0
            if numeric_ratio > 0.85 and looks_date < 0.5:
                out[col] = "measure"
            else:
                # high-uniqueness ID-like goes to dimension too (name lists, codes, etc.)
                out[col] = "dimension"
        return out

    def _propose_normalized_model(self, df: pd.DataFrame, file_columns: list, chosen_table: Optional[str]) -> Dict[str, Any]:
        """
        Build a normalized layout proposal:
        - fact table name
        - dimension tables to create (if missing)
        - FK mapping suggestions
        """
        classes = self._classify_columns(df)
        measures = [c for c, t in classes.items() if t == "measure"]
        dimensions = [c for c, t in classes.items() if t == "dimension"]

        # Fact name heuristic
        base = os.path.splitext(os.path.basename(getattr(df, "_source_name", "import_file")))[0]
        base = re.sub(r"[^a-zA-Z0-9]+", "_", base).strip("_").lower() or "import"
        fact_name = chosen_table or f"fact_{base}"

        dim_defs = []
        fk_suggestions = {}
        for dim_col in dimensions:
            norm = re.sub(r"[^a-zA-Z0-9]+", "_", dim_col).strip("_").lower()
            dim_table = f"dim_{norm}"
            dim_defs.append({
                "table": dim_table,
                "columns": [
                    {"name": "id", "type": "INTEGER", "is_pk": True},
                    {"name": f"{norm}_name", "type": "TEXT", "nullable": False},
                ],
                "unique": [[f"{norm}_name"]],
            })
            fk_suggestions[dim_col] = {
                "field": f"{norm}_id",
                "master_model": "",  # you may map to real Django model later if exists
                "create_dimension_table": dim_table,
            }

        # Ensure at least one relationship
        has_fk = len(fk_suggestions) > 0
        model = {
            "fact_table": fact_name,
            "dimensions": dim_defs,
            "fk_map": fk_suggestions,
            "has_relationship": has_fk,
            "measures": measures,
        }
        return model
