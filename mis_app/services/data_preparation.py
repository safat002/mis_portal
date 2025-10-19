"""
Django Data Preparation Service
Converted from Flask data_prep.py

Provides intelligent data cleaning and preparation functionality
"""

import json
import re
import logging
import pandas as pd
from typing import Dict, List, Optional, Any
from django.core.cache import cache
from django.utils import timezone
from sqlalchemy import inspect, text, func, case, cast, String, Integer, Float, Date, DateTime
from sqlalchemy.sql import select, column
from pandas.api.types import is_numeric_dtype

from ..models import ExternalConnection, CleanedDataSource, AuditLog
from .external_db import ExternalDBService

logger = logging.getLogger(__name__)


class DataPreparationService:
    """
    Service for data cleaning, preparation, and profiling
    """
    
    def __init__(self):
        self.cache_timeout = 600  # 10 minutes
        
        # Data quality strategies
        self.strategies = {
            'handle_missing': self._handle_missing_values,
            'remove_duplicates': self._remove_duplicates,
            'cast_type': self._cast_data_type,
            'find_replace': self._find_and_replace,
            'outlier_detection': self._detect_outliers,
            'normalize': self._normalize_values,
            'standardize': self._standardize_values,
            'encode_categorical': self._encode_categorical,
            'parse_dates': self._parse_dates,
            'extract_features': self._extract_features
        }
    
    def apply_data_prep_recipe(self, base_query: str, recipe: List[Dict], engine) -> str:
        """
        Apply data preparation recipe to a base SQL query by wrapping it in CTEs
        
        Args:
            base_query: The base SQL query
            recipe: List of preparation steps
            engine: SQLAlchemy engine
            
        Returns:
            Final SQL query string with all transformations applied
        """
        if not recipe:
            return base_query
        
        try:
            # Determine column types from base query
            with engine.connect() as conn:
                sample_df = pd.read_sql(text(f"SELECT * FROM ({base_query}) LIMIT 1"), conn)
                all_columns = list(sample_df.columns)
                column_types = sample_df.dtypes.to_dict()
        except Exception as e:
            logger.error(f"Could not determine column types for recipe: {e}")
            return base_query
        
        ctes = []
        last_cte_name = "report_base_data"
        ctes.append(f'{last_cte_name} AS ({base_query})')
        
        for i, step in enumerate(recipe):
            current_cte_name = f"prep_step_{i+1}"
            strategy = step.get("strategy")
            col = step.get("column")
            params = step.get("params", {})
            
            if col not in all_columns:
                continue
            
            # Generate SQL for this step
            sql_clause = self._generate_step_sql(
                strategy, col, params, all_columns, 
                column_types, last_cte_name, engine
            )
            
            if sql_clause:
                ctes.append(f"{current_cte_name} AS ({sql_clause})")
                last_cte_name = current_cte_name
        
        final_query = f"WITH {', '.join(ctes)} SELECT * FROM {last_cte_name}"
        return final_query
    
    def _generate_step_sql(self, strategy: str, col: str, params: Dict, 
                          all_columns: List[str], column_types: Dict, 
                          last_cte_name: str, engine) -> Optional[str]:
        """Generate SQL for a specific preparation step"""
        
        other_cols_str = ", ".join([f'"{c}"' for c in all_columns if c != col])
        is_numeric = is_numeric_dtype(column_types.get(col))
        
        if strategy == "handle_missing":
            return self._generate_missing_sql(col, params, other_cols_str, last_cte_name, is_numeric)
        
        elif strategy == "remove_duplicates":
            return self._generate_dedup_sql(params, all_columns, last_cte_name)
        
        elif strategy == "cast_type":
            return self._generate_cast_sql(col, params, other_cols_str, last_cte_name)
        
        elif strategy == "find_replace":
            return self._generate_replace_sql(col, params, other_cols_str, last_cte_name)
        
        elif strategy == "outlier_detection":
            return self._generate_outlier_sql(col, params, other_cols_str, last_cte_name, is_numeric)
        
        elif strategy == "normalize":
            return self._generate_normalize_sql(col, params, other_cols_str, last_cte_name, is_numeric)
        
        elif strategy == "standardize":
            return self._generate_standardize_sql(col, params, other_cols_str, last_cte_name, is_numeric)
        
        elif strategy == "parse_dates":
            return self._generate_date_parse_sql(col, params, other_cols_str, last_cte_name)
        
        else:
            logger.warning(f"Unknown strategy: {strategy}")
            return None
    
    def _generate_missing_sql(self, col: str, params: Dict, other_cols: str, 
                             cte_name: str, is_numeric: bool) -> str:
        """Generate SQL for handling missing values"""
        method = params.get("method")
        
        if method == "custom":
            fill_value = str(params.get("value", "")).replace("'", "''")
            formula = f"COALESCE(CAST(\"{col}\" AS VARCHAR), '{fill_value}')"
            return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
        
        elif method == "mean" and is_numeric:
            subquery = f'(SELECT AVG("{col}") FROM {cte_name} WHERE "{col}" IS NOT NULL)'
            formula = f'COALESCE("{col}", {subquery})'
            return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
        
        elif method == "median" and is_numeric:
            subquery = f'(SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{col}") FROM {cte_name} WHERE "{col}" IS NOT NULL)'
            formula = f'COALESCE("{col}", {subquery})'
            return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
        
        elif method == "mode":
            # Most frequent value
            subquery = f'''(SELECT "{col}" FROM {cte_name} 
                           WHERE "{col}" IS NOT NULL 
                           GROUP BY "{col}" 
                           ORDER BY COUNT(*) DESC LIMIT 1)'''
            formula = f'COALESCE("{col}", {subquery})'
            return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
        
        elif method == "drop":
            return f'SELECT * FROM {cte_name} WHERE "{col}" IS NOT NULL'
        
        return f'SELECT * FROM {cte_name}'
    
    def _generate_dedup_sql(self, params: Dict, all_columns: List[str], cte_name: str) -> str:
        """Generate SQL for removing duplicates"""
        partition_cols = params.get("columns", all_columns)
        if not partition_cols:
            partition_cols = all_columns
        
        cols_partition_str = ", ".join([f'"{c}"' for c in partition_cols])
        all_cols_str = ", ".join([f'"{c}"' for c in all_columns])
        
        return f"""SELECT {all_cols_str} FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY {cols_partition_str} ORDER BY (SELECT NULL)) as rn 
            FROM {cte_name}
        ) as sub WHERE rn = 1"""
    
    def _generate_cast_sql(self, col: str, params: Dict, other_cols: str, cte_name: str) -> str:
        """Generate SQL for type casting"""
        new_type = params.get("new_type", "string").lower()
        sql_type_map = {
            "string": "VARCHAR", 
            "integer": "INTEGER", 
            "float": "FLOAT",
            "decimal": "DECIMAL",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "boolean": "BOOLEAN"
        }
        
        sql_type = sql_type_map.get(new_type, "VARCHAR")
        formula = f'CAST("{col}" AS {sql_type})'
        return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
    
    def _generate_replace_sql(self, col: str, params: Dict, other_cols: str, cte_name: str) -> str:
        """Generate SQL for find and replace"""
        find_text = str(params.get("find", "")).replace("'", "''")
        replace_text = str(params.get("replace", "")).replace("'", "''")
        
        if params.get("regex", False):
            # Use regex replace if supported
            formula = f"REGEXP_REPLACE(CAST(\"{col}\" AS VARCHAR), '{find_text}', '{replace_text}')"
        else:
            formula = f"REPLACE(CAST(\"{col}\" AS VARCHAR), '{find_text}', '{replace_text}')"
        
        return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
    
    def _generate_outlier_sql(self, col: str, params: Dict, other_cols: str, 
                             cte_name: str, is_numeric: bool) -> str:
        """Generate SQL for outlier detection and handling"""
        if not is_numeric:
            return f'SELECT * FROM {cte_name}'
        
        method = params.get("method", "iqr")
        action = params.get("action", "cap")  # cap, remove, or flag
        
        if method == "iqr":
            # Interquartile Range method
            q1_query = f'PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "{col}")'
            q3_query = f'PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "{col}")'
            
            if action == "remove":
                return f'''SELECT * FROM {cte_name} 
                          WHERE "{col}" BETWEEN 
                              ({q1_query} - 1.5 * ({q3_query} - {q1_query})) AND 
                              ({q3_query} + 1.5 * ({q3_query} - {q1_query}))'''
            
            elif action == "cap":
                lower_bound = f'({q1_query} - 1.5 * ({q3_query} - {q1_query}))'
                upper_bound = f'({q3_query} + 1.5 * ({q3_query} - {q1_query}))'
                formula = f'''CASE 
                    WHEN "{col}" < {lower_bound} THEN {lower_bound}
                    WHEN "{col}" > {upper_bound} THEN {upper_bound}
                    ELSE "{col}"
                END'''
                return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
        
        return f'SELECT * FROM {cte_name}'
    
    def _generate_normalize_sql(self, col: str, params: Dict, other_cols: str, 
                               cte_name: str, is_numeric: bool) -> str:
        """Generate SQL for normalization (0-1 scaling)"""
        if not is_numeric:
            return f'SELECT * FROM {cte_name}'
        
        min_query = f'MIN("{col}")'
        max_query = f'MAX("{col}")'
        
        formula = f'''CASE 
            WHEN ({max_query} - {min_query}) = 0 THEN 0
            ELSE ("{col}" - {min_query}) / ({max_query} - {min_query})
        END'''
        
        return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
    
    def _generate_standardize_sql(self, col: str, params: Dict, other_cols: str, 
                                 cte_name: str, is_numeric: bool) -> str:
        """Generate SQL for standardization (z-score)"""
        if not is_numeric:
            return f'SELECT * FROM {cte_name}'
        
        mean_query = f'AVG("{col}")'
        stddev_query = f'STDDEV("{col}")'
        
        formula = f'''CASE 
            WHEN {stddev_query} = 0 THEN 0
            ELSE ("{col}" - {mean_query}) / {stddev_query}
        END'''
        
        return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
    
    def _generate_date_parse_sql(self, col: str, params: Dict, other_cols: str, cte_name: str) -> str:
        """Generate SQL for date parsing"""
        date_format = params.get("format", "YYYY-MM-DD")
        
        # Convert common format strings to SQL
        format_map = {
            "YYYY-MM-DD": "YYYY-MM-DD",
            "MM/DD/YYYY": "MM/DD/YYYY",
            "DD/MM/YYYY": "DD/MM/YYYY",
            "YYYY/MM/DD": "YYYY/MM/DD"
        }
        
        sql_format = format_map.get(date_format, "YYYY-MM-DD")
        formula = f'TRY_CAST("{col}" AS DATE)'
        
        return f'SELECT {other_cols}, {formula} AS "{col}" FROM {cte_name}'
    
    def analyze_data_profile(self, connection_id: str, table_name: str, 
                           sample_size: int = 1000) -> Dict[str, Any]:
        """
        Analyze data to provide insights for proactive cleaning suggestions
        
        Args:
            connection_id: Database connection ID
            table_name: Name of table to analyze
            sample_size: Number of rows to sample for analysis
            
        Returns:
            Dictionary with column statistics and suggestions
        """
        try:
            db_service = ExternalDBService(connection_id)
            engine = db_service.get_engine()
            
            # Get column information
            inspector = inspect(engine)
            columns = inspector.get_columns(table_name)
            
            profile = {}
            
            with engine.connect() as conn:
                for col in columns:
                    col_name = col['name']
                    col_type = str(col['type'])
                    
                    # Get basic statistics
                    stats_query = f'''
                        SELECT 
                            COUNT(*) as total,
                            COUNT("{col_name}") as non_null,
                            COUNT(DISTINCT "{col_name}") as distinct_count
                        FROM "{table_name}" 
                        LIMIT {sample_size}
                    '''
                    
                    result = conn.execute(text(stats_query))
                    stats = result.fetchone()
                    
                    column_profile = {
                        'type': col_type,
                        'total_count': stats[0],
                        'non_null_count': stats[1],
                        'null_count': stats[0] - stats[1],
                        'distinct_count': stats[2],
                        'suggestions': []
                    }
                    
                    # Generate suggestions
                    suggestions = self._generate_cleaning_suggestions(column_profile, col_name, col_type)
                    column_profile['suggestions'] = suggestions
                    
                    # Get sample values
                    sample_query = f'SELECT DISTINCT "{col_name}" FROM "{table_name}" WHERE "{col_name}" IS NOT NULL LIMIT 5'
                    sample_result = conn.execute(text(sample_query))
                    column_profile['sample_values'] = [row[0] for row in sample_result.fetchall()]
                    
                    profile[col_name] = column_profile
            
            return profile
            
        except Exception as e:
            logger.error(f"Error analyzing data profile: {e}")
            return {}
    
    def _generate_cleaning_suggestions(self, profile: Dict, col_name: str, col_type: str) -> List[Dict]:
        """Generate intelligent cleaning suggestions based on data profile"""
        suggestions = []
        
        total_count = profile.get('total_count', 0)
        null_count = profile.get('null_count', 0)
        distinct_count = profile.get('distinct_count', 0)
        
        if total_count == 0:
            return suggestions
        
        null_percentage = null_count / total_count
        
        # Missing values suggestions
        if null_percentage > 0.1:  # More than 10% nulls
            suggestions.append({
                'strategy': 'handle_missing',
                'message': f'{null_percentage:.1%} of values are missing',
                'priority': 'high' if null_percentage > 0.3 else 'medium',
                'params': {'method': 'mean' if 'INT' in col_type or 'FLOAT' in col_type else 'mode'}
            })
        
        # Data type suggestions
        if distinct_count == 2 and 'INT' in col_type:
            suggestions.append({
                'strategy': 'cast_type',
                'message': 'Only two distinct values - consider converting to boolean',
                'priority': 'medium',
                'params': {'new_type': 'boolean'}
            })
        
        # Duplicate detection
        uniqueness_ratio = distinct_count / (total_count - null_count) if (total_count - null_count) > 0 else 0
        if uniqueness_ratio < 0.95 and distinct_count > 1:  # Less than 95% unique
            suggestions.append({
                'strategy': 'remove_duplicates',
                'message': f'Only {uniqueness_ratio:.1%} of values are unique',
                'priority': 'medium',
                'params': {'columns': [col_name]}
            })
        
        # Outlier detection for numeric columns
        if 'INT' in col_type or 'FLOAT' in col_type or 'DECIMAL' in col_type:
            suggestions.append({
                'strategy': 'outlier_detection',
                'message': 'Numeric column - check for outliers',
                'priority': 'low',
                'params': {'method': 'iqr', 'action': 'cap'}
            })
        
        # Date parsing suggestions
        if 'VARCHAR' in col_type and any(pattern in col_name.lower() for pattern in ['date', 'time', 'created', 'updated']):
            suggestions.append({
                'strategy': 'parse_dates',
                'message': 'Column name suggests date data - consider parsing as date',
                'priority': 'medium',
                'params': {'format': 'YYYY-MM-DD'}
            })
        
        return suggestions
    
    def generate_recipe_from_suggestions(self, profile: Dict, selected_suggestions: List[str] = None) -> List[Dict]:
        """Generate a data preparation recipe from analysis suggestions"""
        recipe = []
        
        for col_name, col_profile in profile.items():
            suggestions = col_profile.get('suggestions', [])
            
            for suggestion in suggestions:
                # If specific suggestions are selected, only include those
                if selected_suggestions and suggestion['strategy'] not in selected_suggestions:
                    continue
                
                # Only include high and medium priority suggestions by default
                if suggestion['priority'] in ['high', 'medium']:
                    recipe_step = {
                        'strategy': suggestion['strategy'],
                        'column': col_name,
                        'params': suggestion.get('params', {}),
                        'description': suggestion['message']
                    }
                    recipe.append(recipe_step)
        
        return recipe
    
    def validate_recipe(self, recipe: List[Dict], connection_id: str, table_name: str) -> Dict[str, Any]:
        """Validate a data preparation recipe"""
        errors = []
        warnings = []
        
        try:
            db_service = ExternalDBService(connection_id)
            engine = db_service.get_engine()
            
            # Check if table exists and get columns
            inspector = inspect(engine)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
            
            for i, step in enumerate(recipe):
                step_errors = []
                
                # Check required fields
                if 'strategy' not in step:
                    step_errors.append('Missing strategy')
                
                if 'column' not in step:
                    step_errors.append('Missing column')
                elif step['column'] not in columns:
                    step_errors.append(f"Column '{step['column']}' not found in table")
                
                # Strategy-specific validation
                strategy = step.get('strategy')
                if strategy == 'handle_missing':
                    method = step.get('params', {}).get('method')
                    if method == 'custom' and 'value' not in step.get('params', {}):
                        step_errors.append('Custom fill method requires value parameter')
                
                elif strategy == 'cast_type':
                    new_type = step.get('params', {}).get('new_type')
                    if new_type not in ['string', 'integer', 'float', 'decimal', 'date', 'datetime', 'boolean']:
                        step_errors.append(f"Invalid target type: {new_type}")
                
                if step_errors:
                    errors.append(f"Step {i+1}: {', '.join(step_errors)}")
            
            return {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings
            }
            
        except Exception as e:
            return {
                'valid': False,
                'errors': [f'Validation error: {str(e)}'],
                'warnings': []
            }
    
    def save_cleaned_dataset(self, connection_id: str, original_table: str, 
                           recipe: List[Dict], user, name: str = None) -> str:
        """Save a cleaned dataset with applied recipe"""
        try:
            # Generate unique name if not provided
            if not name:
                timestamp = timezone.now().strftime('%Y%m%d_%H%M%S')
                name = f"cleaned_{original_table}_{timestamp}"
            
            # Create database record
            cleaned_source = CleanedDataSource.objects.create(
                name=name,
                original_table=original_table,
                connection_id=connection_id,
                recipe=recipe,
                created_by=user
            )
            
            # Log the action
            from ..utils import log_user_action
            log_user_action(
                user,
                'create_cleaned_dataset',
                'cleaned_data_source',
                str(cleaned_source.id),
                name,
                {
                    'original_table': original_table,
                    'recipe_steps': len(recipe),
                    'connection_id': connection_id
                }
            )
            
            return str(cleaned_source.id)
            
        except Exception as e:
            logger.error(f"Error saving cleaned dataset: {e}")
            raise
    
    def get_recipe_templates(self) -> List[Dict]:
        """Get predefined recipe templates for common data cleaning tasks"""
        templates = [
            {
                'name': 'Basic Cleaning',
                'description': 'Handle missing values and remove duplicates',
                'recipe': [
                    {
                        'strategy': 'handle_missing',
                        'column': '__placeholder__',
                        'params': {'method': 'mean'},
                        'description': 'Fill missing numeric values with mean'
                    },
                    {
                        'strategy': 'remove_duplicates',
                        'column': '__placeholder__',
                        'params': {'columns': []},
                        'description': 'Remove duplicate rows'
                    }
                ]
            },
            {
                'name': 'Outlier Handling',
                'description': 'Detect and handle outliers in numeric data',
                'recipe': [
                    {
                        'strategy': 'outlier_detection',
                        'column': '__placeholder__',
                        'params': {'method': 'iqr', 'action': 'cap'},
                        'description': 'Cap outliers using IQR method'
                    }
                ]
            },
            {
                'name': 'Text Standardization',
                'description': 'Standardize text data',
                'recipe': [
                    {
                        'strategy': 'find_replace',
                        'column': '__placeholder__',
                        'params': {'find': '  ', 'replace': ' '},
                        'description': 'Remove extra spaces'
                    },
                    {
                        'strategy': 'cast_type',
                        'column': '__placeholder__',
                        'params': {'new_type': 'string'},
                        'description': 'Ensure string type'
                    }
                ]
            },
            {
                'name': 'Date Processing',
                'description': 'Parse and standardize date columns',
                'recipe': [
                    {
                        'strategy': 'parse_dates',
                        'column': '__placeholder__',
                        'params': {'format': 'YYYY-MM-DD'},
                        'description': 'Parse dates in standard format'
                    }
                ]
            }
        ]
        
        return templates