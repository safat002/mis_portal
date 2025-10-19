# mis_app/services/schema_intelligence_service.py
"""
Schema Intelligence Service
Provides intelligent suggestions for schema design, column mapping, and normalization
"""

import re
import difflib
from typing import Dict, List, Tuple, Optional, Any
from fuzzywuzzy import fuzz
from django.db import connection
from sqlalchemy import inspect, text
import pandas as pd
import logging

from ..smart_import_models import ImportTemplate, SmartImportConfiguration
from ..services.external_db import ExternalDBService

logger = logging.getLogger(__name__)


class SchemaIntelligenceService:
    """Intelligent schema suggestions and normalization guidance"""
    
    def __init__(self, db_connection):
        self.db_connection = db_connection
        self.db_service = ExternalDBService(db_connection)
        self.config = SmartImportConfiguration.get_config()
    
    def analyze_file_columns(self, df: pd.DataFrame, template_id: str = None) -> Dict[str, Any]:
        """
        Analyze uploaded file columns and provide intelligent suggestions
        """
        results = {
            'column_analysis': [],
            'schema_suggestions': {},
            'normalization_opportunities': [],
            'similar_existing_tables': [],
            'suggested_relationships': []
        }
        
        # Analyze each column
        for column in df.columns:
            if pd.isna(column) or not str(column).strip():
                continue
                
            analysis = self._analyze_single_column(column, df[column])
            results['column_analysis'].append(analysis)
        
        # Find existing similar tables
        results['similar_existing_tables'] = self._find_similar_tables(df.columns.tolist())
        
        # Suggest normalization structure
        results['schema_suggestions'] = self._suggest_schema_structure(results['column_analysis'])
        
        # Identify normalization opportunities
        results['normalization_opportunities'] = self._identify_normalization_opportunities(
            results['column_analysis']
        )
        
        # Suggest relationships between tables
        results['suggested_relationships'] = self._suggest_table_relationships(
            results['schema_suggestions']
        )
        
        return results
    
    def _analyze_single_column(self, column_name: str, data: pd.Series) -> Dict[str, Any]:
        """Analyze a single column and provide suggestions"""
        
        # Basic statistics
        non_null_count = data.notna().sum()
        unique_count = data.nunique()
        total_count = len(data)
        
        # Sample values
        sample_values = data.dropna().head(10).tolist()
        
        # Data type inference
        suggested_type = self._infer_data_type(data)
        
        # Check for existing similar columns
        similar_columns = self._find_similar_columns(column_name, data)
        
        # Normalization suggestion
        normalization_suggestion = self._suggest_column_normalization(column_name, data)
        
        # Naming convention fix
        standardized_name = self._standardize_column_name(column_name)
        
        return {
            'original_name': column_name,
            'standardized_name': standardized_name,
            'suggested_type': suggested_type,
            'sample_values': sample_values,
            'statistics': {
                'total_count': total_count,
                'non_null_count': non_null_count,
                'unique_count': unique_count,
                'null_percentage': ((total_count - non_null_count) / total_count) * 100,
                'uniqueness_ratio': unique_count / non_null_count if non_null_count > 0 else 0
            },
            'similar_existing_columns': similar_columns,
            'normalization_suggestion': normalization_suggestion,
            'constraints_suggested': self._suggest_constraints(data, unique_count, non_null_count, total_count)
        }
    
    def _infer_data_type(self, data: pd.Series) -> Dict[str, Any]:
        """Infer the best data type for a column"""
        
        non_null_data = data.dropna()
        if len(non_null_data) == 0:
            return {'type': 'VARCHAR', 'length': 255, 'confidence': 0.5}
        
        # Try to infer numeric types
        numeric_count = 0
        for value in non_null_data.head(100):  # Sample first 100 non-null values
            try:
                float(str(value).replace(',', '').replace('$', '').replace('%', ''))
                numeric_count += 1
            except (ValueError, TypeError):
                pass
        
        numeric_ratio = numeric_count / min(len(non_null_data), 100)
        
        if numeric_ratio > 0.8:
            # Check if integers
            integer_count = 0
            for value in non_null_data.head(100):
                try:
                    val = float(str(value).replace(',', '').replace('$', '').replace('%', ''))
                    if val.is_integer():
                        integer_count += 1
                except (ValueError, TypeError):
                    pass
            
            if integer_count / min(len(non_null_data), 100) > 0.9:
                max_val = non_null_data.max()
                if max_val < 2147483647:  # 32-bit integer limit
                    return {'type': 'INTEGER', 'confidence': 0.9}
                else:
                    return {'type': 'BIGINT', 'confidence': 0.9}
            else:
                return {'type': 'DECIMAL', 'precision': 15, 'scale': 2, 'confidence': 0.85}
        
        # Try to infer date types
        date_count = 0
        for value in non_null_data.head(50):
            if self._is_likely_date(str(value)):
                date_count += 1
        
        if date_count / min(len(non_null_data), 50) > 0.7:
            if self._contains_time_info(non_null_data.head(20)):
                return {'type': 'DATETIME', 'confidence': 0.85}
            else:
                return {'type': 'DATE', 'confidence': 0.85}
        
        # Try to infer boolean
        unique_values = set(str(v).lower().strip() for v in non_null_data.unique())
        boolean_values = {'true', 'false', 'yes', 'no', '1', '0', 'y', 'n', 'active', 'inactive'}
        if len(unique_values) <= 2 and unique_values.issubset(boolean_values):
            return {'type': 'BOOLEAN', 'confidence': 0.9}
        
        # Check for enum (limited distinct values)
        unique_count = data.nunique()
        total_count = len(non_null_data)
        
        if unique_count <= 20 and unique_count / total_count < 0.1:
            enum_values = [str(v) for v in data.unique() if pd.notna(v)]
            return {
                'type': 'ENUM',
                'values': enum_values,
                'confidence': 0.8
            }
        
        # Default to VARCHAR
        max_length = max(len(str(v)) for v in non_null_data.head(100))
        
        if max_length > 500:
            return {'type': 'TEXT', 'confidence': 0.7}
        else:
            suggested_length = min(max(max_length * 1.5, 50), 255)  # Add some buffer
            return {'type': 'VARCHAR', 'length': int(suggested_length), 'confidence': 0.7}
    
    def _is_likely_date(self, value_str: str) -> bool:
        """Check if a string value looks like a date"""
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY or DD/MM/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY or DD-MM-YYYY
            r'\d{1,2}/\d{1,2}/\d{4}',  # M/D/YYYY
            r'\d{1,2}-\d{1,2}-\d{4}',  # M-D-YYYY
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, value_str.strip()):
                return True
        return False
    
    def _contains_time_info(self, data_sample: pd.Series) -> bool:
        """Check if date values contain time information"""
        for value in data_sample:
            if ':' in str(value) or 'am' in str(value).lower() or 'pm' in str(value).lower():
                return True
        return False
    
    def _find_similar_columns(self, column_name: str, data: pd.Series) -> List[Dict[str, Any]]:
        """Find similar columns in existing database tables"""
        try:
            inspector = inspect(self.db_service.get_engine())
            all_tables = inspector.get_table_names()
            
            similar_columns = []
            column_name_clean = self._standardize_column_name(column_name)
            
            for table_name in all_tables:
                try:
                    columns = inspector.get_columns(table_name)
                    
                    for col in columns:
                        col_name = col['name']
                        
                        # Calculate name similarity
                        name_similarity = fuzz.ratio(column_name_clean, col_name)
                        
                        if name_similarity > 70:  # 70% similarity threshold
                            # Check data similarity if possible
                            data_similarity = self._calculate_data_similarity(
                                data, table_name, col_name
                            )
                            
                            overall_score = (name_similarity * 0.7) + (data_similarity * 0.3)
                            
                            if overall_score > 60:
                                similar_columns.append({
                                    'table_name': table_name,
                                    'column_name': col_name,
                                    'column_type': str(col['type']),
                                    'name_similarity': name_similarity,
                                    'data_similarity': data_similarity,
                                    'overall_score': overall_score,
                                    'recommendation': self._get_similarity_recommendation(overall_score)
                                })
                
                except Exception as e:
                    logger.warning(f"Could not analyze table {table_name}: {str(e)}")
                    continue
            
            # Sort by overall score
            similar_columns.sort(key=lambda x: x['overall_score'], reverse=True)
            return similar_columns[:5]  # Return top 5 matches
            
        except Exception as e:
            logger.error(f"Error finding similar columns: {str(e)}")
            return []
    
    def _calculate_data_similarity(self, new_data: pd.Series, table_name: str, column_name: str) -> float:
        """Calculate similarity between new column data and existing column data"""
        try:
            # Sample existing data
            query = f"SELECT DISTINCT {column_name} FROM {table_name} LIMIT 100"
            
            with self.db_service.get_engine().connect() as conn:
                result = conn.execute(text(query))
                existing_values = set(str(row[0]).lower().strip() for row in result if row[0] is not None)
            
            if not existing_values:
                return 0.0
            
            # Sample new data
            new_values = set(str(v).lower().strip() for v in new_data.dropna().unique())
            
            if not new_values:
                return 0.0
            
            # Calculate Jaccard similarity
            intersection = existing_values.intersection(new_values)
            union = existing_values.union(new_values)
            
            if not union:
                return 0.0
            
            jaccard_similarity = len(intersection) / len(union)
            return jaccard_similarity * 100  # Convert to percentage
            
        except Exception as e:
            logger.warning(f"Could not calculate data similarity for {table_name}.{column_name}: {str(e)}")
            return 0.0
    
    def _get_similarity_recommendation(self, score: float) -> str:
        """Get recommendation based on similarity score"""
        if score > 90:
            return "REUSE_EXISTING"
        elif score > 80:
            return "CONSIDER_REUSING"
        elif score > 70:
            return "REVIEW_MANUALLY"
        else:
            return "CREATE_NEW"
    
    def _suggest_column_normalization(self, column_name: str, data: pd.Series) -> Dict[str, Any]:
        """Suggest normalization approach for a column"""
        
        standardized_name = self._standardize_column_name(column_name)
        unique_count = data.nunique()
        total_count = len(data.dropna())
        
        # Check if this should be a master table
        if self._is_master_data_candidate(standardized_name, unique_count, total_count):
            return {
                'type': 'EXTRACT_TO_MASTER',
                'suggested_master_table': f"{standardized_name}s",
                'foreign_key_column': f"{standardized_name}_id",
                'reasoning': f"Low cardinality ({unique_count} unique values) suggests master data table"
            }
        
        # Check if it's a composite field that should be split
        split_suggestion = self._suggest_column_split(column_name, data)
        if split_suggestion:
            return split_suggestion
        
        return {
            'type': 'KEEP_AS_IS',
            'reasoning': 'No normalization needed'
        }
    
    def _is_master_data_candidate(self, column_name: str, unique_count: int, total_count: int) -> bool:
        """Determine if a column should be extracted to master data table"""
        
        # Common master data patterns
        master_keywords = ['buyer', 'supplier', 'customer', 'vendor', 'company', 'unit', 'department', 
                          'category', 'style', 'color', 'size', 'season', 'brand', 'product']
        
        name_suggests_master = any(keyword in column_name.lower() for keyword in master_keywords)
        
        # Low cardinality suggests lookup table
        cardinality_ratio = unique_count / total_count if total_count > 0 else 0
        low_cardinality = cardinality_ratio < 0.1 and unique_count > 1
        
        # Reasonable number of unique values (not too few, not too many)
        reasonable_count = 2 <= unique_count <= 1000
        
        return name_suggests_master and low_cardinality and reasonable_count
    
    def _suggest_column_split(self, column_name: str, data: pd.Series) -> Optional[Dict[str, Any]]:
        """Suggest splitting composite columns"""
        
        # Check for common composite patterns
        sample_values = data.dropna().head(20).astype(str)
        
        # Look for delimiter patterns
        delimiters = ['-', '_', '/', '|', ':']
        
        for delimiter in delimiters:
            split_counts = []
            for value in sample_values:
                parts = value.split(delimiter)
                split_counts.append(len(parts))
            
            # If most values split consistently into multiple parts
            if len(set(split_counts)) <= 2 and max(split_counts) > 1 and min(split_counts) > 1:
                avg_parts = sum(split_counts) / len(split_counts)
                
                return {
                    'type': 'SPLIT_COLUMN',
                    'delimiter': delimiter,
                    'suggested_columns': [f"{column_name}_part_{i+1}" for i in range(int(avg_parts))],
                    'reasoning': f"Values consistently split into {int(avg_parts)} parts with '{delimiter}'"
                }
        
        return None
    
    def _standardize_column_name(self, name: str) -> str:
        """Convert column name to standard format"""
        if pd.isna(name):
            return "unnamed_column"
        
        # Convert to string and clean
        name = str(name).strip()
        
        # Replace spaces and special characters with underscores
        name = re.sub(r'[^\w]+', '_', name)
        
        # Convert to lowercase
        name = name.lower()
        
        # Remove multiple consecutive underscores
        name = re.sub(r'_+', '_', name)
        
        # Remove leading/trailing underscores
        name = name.strip('_')
        
        # Ensure it starts with a letter
        if name and not name[0].isalpha():
            name = 'col_' + name
        
        # Handle empty names
        if not name:
            name = 'unnamed_column'
        
        return name
    
    def _suggest_constraints(self, data: pd.Series, unique_count: int, non_null_count: int, total_count: int) -> List[str]:
        """Suggest database constraints for a column"""
        
        constraints = []
        
        # NOT NULL constraint
        null_percentage = ((total_count - non_null_count) / total_count) * 100 if total_count > 0 else 100
        if null_percentage < 5:  # Less than 5% nulls
            constraints.append('NOT_NULL')
        
        # UNIQUE constraint
        uniqueness_ratio = unique_count / non_null_count if non_null_count > 0 else 0
        if uniqueness_ratio > 0.95:  # More than 95% unique
            constraints.append('UNIQUE')
        
        # Primary key candidate
        if uniqueness_ratio == 1.0 and null_percentage == 0:
            constraints.append('PRIMARY_KEY_CANDIDATE')
        
        return constraints
    
    def _find_similar_tables(self, column_names: List[str]) -> List[Dict[str, Any]]:
        """Find existing tables with similar column structures"""
        
        try:
            inspector = inspect(self.db_service.get_engine())
            all_tables = inspector.get_table_names()
            
            standardized_columns = [self._standardize_column_name(col) for col in column_names]
            similar_tables = []
            
            for table_name in all_tables:
                try:
                    table_columns = inspector.get_columns(table_name)
                    table_column_names = [col['name'] for col in table_columns]
                    
                    # Calculate similarity
                    similarity = self._calculate_column_set_similarity(
                        standardized_columns, table_column_names
                    )
                    
                    if similarity > 40:  # 40% similarity threshold
                        similar_tables.append({
                            'table_name': table_name,
                            'similarity_score': similarity,
                            'matching_columns': self._find_matching_columns(standardized_columns, table_column_names),
                            'recommendation': self._get_table_similarity_recommendation(similarity)
                        })
                
                except Exception as e:
                    logger.warning(f"Could not analyze table {table_name}: {str(e)}")
                    continue
            
            # Sort by similarity
            similar_tables.sort(key=lambda x: x['similarity_score'], reverse=True)
            return similar_tables[:5]
            
        except Exception as e:
            logger.error(f"Error finding similar tables: {str(e)}")
            return []
    
    def _calculate_column_set_similarity(self, columns1: List[str], columns2: List[str]) -> float:
        """Calculate similarity between two sets of column names"""
        
        if not columns1 or not columns2:
            return 0.0
        
        set1 = set(columns1)
        set2 = set(columns2)
        
        # Exact matches
        exact_matches = set1.intersection(set2)
        
        # Fuzzy matches for remaining columns
        remaining1 = set1 - exact_matches
        remaining2 = set2 - exact_matches
        
        fuzzy_matches = 0
        for col1 in remaining1:
            best_match_score = 0
            for col2 in remaining2:
                score = fuzz.ratio(col1, col2)
                if score > best_match_score:
                    best_match_score = score
            
            if best_match_score > 70:  # 70% similarity threshold
                fuzzy_matches += 1
        
        total_matches = len(exact_matches) + fuzzy_matches
        total_columns = max(len(columns1), len(columns2))
        
        return (total_matches / total_columns) * 100
    
    def _find_matching_columns(self, columns1: List[str], columns2: List[str]) -> List[Dict[str, Any]]:
        """Find matching columns between two lists"""
        
        matches = []
        set2 = set(columns2)
        
        for col1 in columns1:
            if col1 in set2:
                matches.append({
                    'file_column': col1,
                    'table_column': col1,
                    'match_type': 'exact'
                })
            else:
                # Find best fuzzy match
                best_match = None
                best_score = 0
                
                for col2 in columns2:
                    score = fuzz.ratio(col1, col2)
                    if score > best_score and score > 70:
                        best_score = score
                        best_match = col2
                
                if best_match:
                    matches.append({
                        'file_column': col1,
                        'table_column': best_match,
                        'match_type': 'fuzzy',
                        'similarity': best_score
                    })
        
        return matches
    
    def _get_table_similarity_recommendation(self, similarity: float) -> str:
        """Get recommendation based on table similarity"""
        if similarity > 80:
            return "EXTEND_EXISTING_TABLE"
        elif similarity > 60:
            return "CONSIDER_EXTENDING"
        elif similarity > 40:
            return "REVIEW_FOR_REUSE"
        else:
            return "CREATE_NEW_TABLE"
    
    def _suggest_schema_structure(self, column_analysis: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Suggest overall schema structure for the import"""
        
        main_table_columns = []
        master_tables = {}
        
        for col_info in column_analysis:
            norm_suggestion = col_info['normalization_suggestion']
            
            if norm_suggestion['type'] == 'EXTRACT_TO_MASTER':
                # This column should become a master table
                master_table_name = norm_suggestion['suggested_master_table']
                master_tables[master_table_name] = {
                    'columns': [
                        {'name': 'id', 'type': 'INTEGER', 'constraints': ['PRIMARY_KEY', 'AUTO_INCREMENT']},
                        {'name': 'name', 'type': 'VARCHAR', 'length': 255, 'constraints': ['NOT_NULL', 'UNIQUE']},
                        {'name': 'code', 'type': 'VARCHAR', 'length': 50, 'constraints': ['UNIQUE']},
                        {'name': 'status', 'type': 'ENUM', 'values': ['Active', 'Inactive'], 'default': 'Active'},
                        {'name': 'created_at', 'type': 'TIMESTAMP', 'constraints': ['NOT_NULL']},
                        {'name': 'updated_at', 'type': 'TIMESTAMP', 'constraints': ['NOT_NULL']},
                    ],
                    'original_column': col_info['original_name']
                }
                
                # Add foreign key to main table
                main_table_columns.append({
                    'name': norm_suggestion['foreign_key_column'],
                    'type': 'INTEGER',
                    'constraints': ['FOREIGN_KEY'],
                    'references': f"{master_table_name}.id",
                    'original_column': col_info['original_name']
                })
                
            elif norm_suggestion['type'] == 'SPLIT_COLUMN':
                # Add split columns to main table
                for split_col in norm_suggestion['suggested_columns']:
                    main_table_columns.append({
                        'name': split_col,
                        'type': 'VARCHAR',
                        'length': 255,
                        'constraints': [],
                        'original_column': col_info['original_name']
                    })
            else:
                # Keep as regular column
                col_type = col_info['suggested_type']['type']
                column_def = {
                    'name': col_info['standardized_name'],
                    'type': col_type,
                    'constraints': col_info['constraints_suggested'],
                    'original_column': col_info['original_name']
                }
                
                # Add type-specific properties
                if col_type == 'VARCHAR' and 'length' in col_info['suggested_type']:
                    column_def['length'] = col_info['suggested_type']['length']
                elif col_type == 'DECIMAL':
                    column_def['precision'] = col_info['suggested_type'].get('precision', 15)
                    column_def['scale'] = col_info['suggested_type'].get('scale', 2)
                elif col_type == 'ENUM':
                    column_def['values'] = col_info['suggested_type']['values']
                
                main_table_columns.append(column_def)
        
        # Add standard audit columns to main table
        main_table_columns.extend([
            {'name': 'id', 'type': 'INTEGER', 'constraints': ['PRIMARY_KEY', 'AUTO_INCREMENT']},
            {'name': 'import_session_id', 'type': 'VARCHAR', 'length': 36, 'constraints': []},
            {'name': 'import_row_number', 'type': 'INTEGER', 'constraints': []},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'constraints': ['NOT_NULL']},
            {'name': 'updated_at', 'type': 'TIMESTAMP', 'constraints': ['NOT_NULL']},
        ])
        
        return {
            'main_table': {
                'columns': main_table_columns
            },
            'master_tables': master_tables,
            'recommended_indexes': self._suggest_indexes(main_table_columns)
        }
    
    def _suggest_indexes(self, columns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Suggest database indexes"""
        
        indexes = []
        
        # Index on foreign keys
        for col in columns:
            if 'FOREIGN_KEY' in col.get('constraints', []):
                indexes.append({
                    'name': f"idx_{col['name']}",
                    'columns': [col['name']],
                    'type': 'btree'
                })
        
        # Index on import session for lineage queries
        indexes.append({
            'name': 'idx_import_session_id',
            'columns': ['import_session_id'],
            'type': 'btree'
        })
        
        # Index on created_at for time-based queries
        indexes.append({
            'name': 'idx_created_at',
            'columns': ['created_at'],
            'type': 'btree'
        })
        
        return indexes
    
    def _identify_normalization_opportunities(self, column_analysis: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Identify opportunities to improve normalization"""
        
        opportunities = []
        
        # Look for repeated patterns that suggest normalization
        for col_info in column_analysis:
            # Check for composite values
            if col_info['normalization_suggestion']['type'] == 'SPLIT_COLUMN':
                opportunities.append({
                    'type': 'SPLIT_COMPOSITE_FIELD',
                    'column': col_info['original_name'],
                    'suggestion': col_info['normalization_suggestion'],
                    'benefit': 'Improves data integrity and enables better querying'
                })
            
            # Check for master data extraction
            elif col_info['normalization_suggestion']['type'] == 'EXTRACT_TO_MASTER':
                opportunities.append({
                    'type': 'EXTRACT_LOOKUP_TABLE',
                    'column': col_info['original_name'],
                    'suggestion': col_info['normalization_suggestion'],
                    'benefit': 'Reduces data redundancy and improves consistency'
                })
        
        return opportunities
    
    def _suggest_table_relationships(self, schema_suggestions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Suggest relationships between tables"""
        
        relationships = []
        main_table = schema_suggestions.get('main_table', {})
        master_tables = schema_suggestions.get('master_tables', {})
        
        # Find foreign key relationships
        for col in main_table.get('columns', []):
            if 'FOREIGN_KEY' in col.get('constraints', []):
                ref_table = col.get('references', '').split('.')[0]
                
                relationships.append({
                    'type': 'many_to_one',
                    'from_table': 'main_table',
                    'from_column': col['name'],
                    'to_table': ref_table,
                    'to_column': 'id',
                    'relationship_name': f"{ref_table}_relationship"
                })
        
        return relationships