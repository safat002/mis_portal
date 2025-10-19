# mis_app/services/dynamic_schema_service.py
"""
Dynamic Schema Service
Handles creation and modification of database tables based on templates
"""

import logging
from typing import Dict, List, Any, Optional
from django.db import transaction
from sqlalchemy import create_engine, text, inspect, MetaData, Table, Column
from sqlalchemy.exc import SQLAlchemyError

from ..smart_import_models import ImportSession, SchemaEvolutionLog, ImportTemplate
from ..services.external_db import ExternalDBService

logger = logging.getLogger(__name__)


class DynamicSchemaService:
    """Service for creating and managing database schemas dynamically"""
    
    def __init__(self, db_connection, user):
        self.db_connection = db_connection
        self.user = user
        self.db_service = ExternalDBService(db_connection)
        self.engine = self.db_service.get_engine()
    
    def create_schema_from_template(self, session: ImportSession, schema_definition: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create database schema based on template definition
        """
        results = {
            'success': False,
            'created_tables': [],
            'executed_ddl': [],
            'errors': [],
            'warnings': []
        }
        
        try:
            with transaction.atomic():
                # Create main table
                main_table_result = self._create_main_table(
                    session, schema_definition.get('main_table', {})
                )
                results['created_tables'].extend(main_table_result['tables'])
                results['executed_ddl'].extend(main_table_result['ddl'])
                
                # Create master tables
                master_tables = schema_definition.get('master_tables', {})
                for table_name, table_def in master_tables.items():
                    master_result = self._create_master_table(session, table_name, table_def)
                    results['created_tables'].extend(master_result['tables'])
                    results['executed_ddl'].extend(master_result['ddl'])
                
                # Create indexes
                indexes = schema_definition.get('recommended_indexes', [])
                for index_def in indexes:
                    index_result = self._create_index(session, index_def)
                    if index_result['success']:
                        results['executed_ddl'].extend(index_result['ddl'])
                
                # Create foreign key constraints
                fk_result = self._create_foreign_keys(session, schema_definition)
                results['executed_ddl'].extend(fk_result['ddl'])
                
                results['success'] = True
                
        except Exception as e:
            logger.error(f"Schema creation failed: {str(e)}")
            results['errors'].append(str(e))
            results['success'] = False
        
        return results
    
    def _create_main_table(self, session: ImportSession, table_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create the main data table"""
        
        table_name = f"import_data_{session.id.hex[:8]}"
        columns = table_definition.get('columns', [])
        
        # Build CREATE TABLE statement
        ddl_parts = [f"CREATE TABLE {table_name} ("]
        column_definitions = []
        
        for col in columns:
            col_def = self._build_column_definition(col)
            if col_def:
                column_definitions.append(col_def)
        
        ddl_parts.append("  " + ",\n  ".join(column_definitions))
        ddl_parts.append(")")
        
        create_table_ddl = "\n".join(ddl_parts)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_ddl))
                conn.commit()
            
            # Log the schema change
            self._log_schema_change(
                session=session,
                operation='create_table',
                table_name=table_name,
                ddl_statement=create_table_ddl,
                success=True
            )
            
            return {
                'tables': [table_name],
                'ddl': [create_table_ddl],
                'success': True
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create main table: {str(e)}")
            
            self._log_schema_change(
                session=session,
                operation='create_table',
                table_name=table_name,
                ddl_statement=create_table_ddl,
                success=False,
                error_message=str(e)
            )
            
            raise
    
    def _create_master_table(self, session: ImportSession, table_name: str, table_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create or alter a master data table"""
        
        # Check if table already exists
        inspector = inspect(self.engine)
        table_exists = table_name in inspector.get_table_names()
        
        if table_exists:
            return self._alter_master_table(session, table_name, table_definition)
        else:
            return self._create_new_master_table(session, table_name, table_definition)
    
    def _create_new_master_table(self, session: ImportSession, table_name: str, table_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new master data table"""
        
        columns = table_definition.get('columns', [])
        
        # Build CREATE TABLE statement
        ddl_parts = [f"CREATE TABLE {table_name} ("]
        column_definitions = []
        
        for col in columns:
            col_def = self._build_column_definition(col)
            if col_def:
                column_definitions.append(col_def)
        
        ddl_parts.append("  " + ",\n  ".join(column_definitions))
        ddl_parts.append(")")
        
        create_table_ddl = "\n".join(ddl_parts)
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_table_ddl))
                conn.commit()
            
            self._log_schema_change(
                session=session,
                operation='create_table',
                table_name=table_name,
                ddl_statement=create_table_ddl,
                success=True
            )
            
            return {
                'tables': [table_name],
                'ddl': [create_table_ddl],
                'success': True
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create master table {table_name}: {str(e)}")
            
            self._log_schema_change(
                session=session,
                operation='create_table',
                table_name=table_name,
                ddl_statement=create_table_ddl,
                success=False,
                error_message=str(e)
            )
            
            raise
    
    def _alter_master_table(self, session: ImportSession, table_name: str, table_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Alter existing master table to add new columns if needed"""
        
        inspector = inspect(self.engine)
        existing_columns = {col['name']: col for col in inspector.get_columns(table_name)}
        
        new_columns = table_definition.get('columns', [])
        ddl_statements = []
        
        # Check for new columns to add
        for col in new_columns:
            col_name = col['name']
            
            if col_name not in existing_columns:
                # Need to add this column
                col_def = self._build_column_definition(col, for_alter=True)
                if col_def:
                    alter_ddl = f"ALTER TABLE {table_name} ADD COLUMN {col_def}"
                    ddl_statements.append(alter_ddl)
        
        # Execute ALTER statements
        executed_ddl = []
        
        for ddl in ddl_statements:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text(ddl))
                    conn.commit()
                
                self._log_schema_change(
                    session=session,
                    operation='alter_table',
                    table_name=table_name,
                    ddl_statement=ddl,
                    success=True
                )
                
                executed_ddl.append(ddl)
                
            except SQLAlchemyError as e:
                logger.error(f"Failed to alter table {table_name}: {str(e)}")
                
                self._log_schema_change(
                    session=session,
                    operation='alter_table',
                    table_name=table_name,
                    ddl_statement=ddl,
                    success=False,
                    error_message=str(e)
                )
                
                raise
        
        return {
            'tables': [table_name] if executed_ddl else [],
            'ddl': executed_ddl,
            'success': True
        }
    
    def _build_column_definition(self, column_config: Dict[str, Any], for_alter: bool = False) -> str:
        """Build SQL column definition from configuration"""
        
        col_name = column_config['name']
        col_type = column_config['type']
        constraints = column_config.get('constraints', [])
        
        # Build type definition
        type_def = self._get_sql_type_definition(column_config)
        
        # Build constraint definitions
        constraint_parts = []
        
        for constraint in constraints:
            if constraint == 'NOT_NULL':
                constraint_parts.append('NOT NULL')
            elif constraint == 'UNIQUE' and not for_alter:
                constraint_parts.append('UNIQUE')
            elif constraint == 'PRIMARY_KEY' and not for_alter:
                constraint_parts.append('PRIMARY KEY')
            elif constraint == 'AUTO_INCREMENT' and not for_alter:
                constraint_parts.append('AUTO_INCREMENT')
        
        # Handle default values
        if 'default' in column_config:
            default_val = column_config['default']
            if isinstance(default_val, str):
                constraint_parts.append(f"DEFAULT '{default_val}'")
            else:
                constraint_parts.append(f"DEFAULT {default_val}")
        
        # Combine parts
        parts = [col_name, type_def] + constraint_parts
        return ' '.join(parts)
    
    def _get_sql_type_definition(self, column_config: Dict[str, Any]) -> str:
        """Convert column configuration to SQL type definition"""
        
        col_type = column_config['type']
        
        if col_type == 'VARCHAR':
            length = column_config.get('length', 255)
            return f"VARCHAR({length})"
        
        elif col_type == 'TEXT':
            return "TEXT"
        
        elif col_type == 'INTEGER':
            return "INTEGER"
        
        elif col_type == 'BIGINT':
            return "BIGINT"
        
        elif col_type == 'DECIMAL':
            precision = column_config.get('precision', 15)
            scale = column_config.get('scale', 2)
            return f"DECIMAL({precision},{scale})"
        
        elif col_type == 'FLOAT':
            return "FLOAT"
        
        elif col_type == 'DATE':
            return "DATE"
        
        elif col_type == 'DATETIME':
            return "DATETIME"
        
        elif col_type == 'TIMESTAMP':
            return "TIMESTAMP"
        
        elif col_type == 'BOOLEAN':
            return "BOOLEAN"
        
        elif col_type == 'ENUM':
            values = column_config.get('values', [])
            if values:
                enum_values = "', '".join(values)
                return f"ENUM('{enum_values}')"
            else:
                return "VARCHAR(50)"  # Fallback
        
        elif col_type == 'JSON':
            return "JSON"
        
        else:
            # Default fallback
            return "VARCHAR(255)"
    
    def _create_index(self, session: ImportSession, index_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create database index"""
        
        index_name = index_definition['name']
        columns = index_definition['columns']
        index_type = index_definition.get('type', 'btree')
        table_name = index_definition.get('table_name', f"import_data_{session.id.hex[:8]}")
        
        # Build CREATE INDEX statement
        columns_str = ', '.join(columns)
        create_index_ddl = f"CREATE INDEX {index_name} ON {table_name} ({columns_str})"
        
        try:
            with self.engine.connect() as conn:
                conn.execute(text(create_index_ddl))
                conn.commit()
            
            self._log_schema_change(
                session=session,
                operation='create_index',
                table_name=table_name,
                ddl_statement=create_index_ddl,
                success=True
            )
            
            return {
                'success': True,
                'ddl': [create_index_ddl]
            }
            
        except SQLAlchemyError as e:
            logger.error(f"Failed to create index {index_name}: {str(e)}")
            
            self._log_schema_change(
                session=session,
                operation='create_index',
                table_name=table_name,
                ddl_statement=create_index_ddl,
                success=False,
                error_message=str(e)
            )
            
            return {
                'success': False,
                'ddl': [],
                'error': str(e)
            }
    
    def _create_foreign_keys(self, session: ImportSession, schema_definition: Dict[str, Any]) -> Dict[str, Any]:
        """Create foreign key constraints"""
        
        ddl_statements = []
        main_table_name = f"import_data_{session.id.hex[:8]}"
        main_table = schema_definition.get('main_table', {})
        
        # Find columns with foreign key constraints
        for col in main_table.get('columns', []):
            if 'FOREIGN_KEY' in col.get('constraints', []):
                references = col.get('references', '')
                if references:
                    ref_table, ref_column = references.split('.')
                    
                    constraint_name = f"fk_{main_table_name}_{col['name']}"
                    fk_ddl = (f"ALTER TABLE {main_table_name} "
                             f"ADD CONSTRAINT {constraint_name} "
                             f"FOREIGN KEY ({col['name']}) "
                             f"REFERENCES {ref_table}({ref_column})")
                    
                    try:
                        with self.engine.connect() as conn:
                            conn.execute(text(fk_ddl))
                            conn.commit()
                        
                        self._log_schema_change(
                            session=session,
                            operation='create_constraint',
                            table_name=main_table_name,
                            ddl_statement=fk_ddl,
                            success=True
                        )
                        
                        ddl_statements.append(fk_ddl)
                        
                    except SQLAlchemyError as e:
                        logger.warning(f"Failed to create foreign key constraint: {str(e)}")
                        
                        self._log_schema_change(
                            session=session,
                            operation='create_constraint',
                            table_name=main_table_name,
                            ddl_statement=fk_ddl,
                            success=False,
                            error_message=str(e)
                        )
        
        return {
            'ddl': ddl_statements,
            'success': True
        }
    
    def _log_schema_change(self, session: ImportSession, operation: str, table_name: str, 
                          ddl_statement: str, success: bool, error_message: str = None):
        """Log schema change to audit trail"""
        
        try:
            SchemaEvolutionLog.objects.create(
                import_session=session,
                connection=session.connection,
                operation=operation,
                table_name=table_name,
                ddl_statement=ddl_statement,
                executed_successfully=success,
                error_message=error_message or '',
                executed_by=self.user,
                rollback_ddl=self._generate_rollback_ddl(operation, table_name, ddl_statement)
            )
        except Exception as e:
            logger.error(f"Failed to log schema change: {str(e)}")
    
    def _generate_rollback_ddl(self, operation: str, table_name: str, ddl_statement: str) -> str:
        """Generate DDL to rollback a schema change"""
        
        try:
            if operation == 'create_table':
                return f"DROP TABLE IF EXISTS {table_name}"
            
            elif operation == 'alter_table' and 'ADD COLUMN' in ddl_statement:
                # Extract column name from ALTER TABLE statement
                column_match = re.search(r'ADD COLUMN (\w+)', ddl_statement)
                if column_match:
                    column_name = column_match.group(1)
                    return f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
            
            elif operation == 'create_index':
                # Extract index name
                index_match = re.search(r'CREATE INDEX (\w+)', ddl_statement)
                if index_match:
                    index_name = index_match.group(1)
                    return f"DROP INDEX {index_name}"
            
            elif operation == 'create_constraint':
                # Extract constraint name
                constraint_match = re.search(r'ADD CONSTRAINT (\w+)', ddl_statement)
                if constraint_match:
                    constraint_name = constraint_match.group(1)
                    return f"ALTER TABLE {table_name} DROP CONSTRAINT {constraint_name}"
            
            return ""  # No rollback available
            
        except Exception as e:
            logger.warning(f"Could not generate rollback DDL: {str(e)}")
            return ""
    
    def rollback_schema_changes(self, session: ImportSession) -> Dict[str, Any]:
        """Rollback all schema changes for a session"""
        
        results = {
            'success': False,
            'rolled_back_changes': [],
            'errors': []
        }
        
        try:
            # Get all schema changes for this session in reverse order
            schema_changes = SchemaEvolutionLog.objects.filter(
                import_session=session,
                executed_successfully=True,
                is_rolled_back=False
            ).order_by('-executed_at')
            
            with transaction.atomic():
                for change in schema_changes:
                    if change.rollback_ddl:
                        try:
                            with self.engine.connect() as conn:
                                conn.execute(text(change.rollback_ddl))
                                conn.commit()
                            
                            # Mark as rolled back
                            change.is_rolled_back = True
                            change.rolled_back_at = timezone.now()
                            change.rolled_back_by = self.user
                            change.save()
                            
                            results['rolled_back_changes'].append({
                                'operation': change.operation,
                                'table_name': change.table_name,
                                'rollback_ddl': change.rollback_ddl
                            })
                            
                        except SQLAlchemyError as e:
                            error_msg = f"Failed to rollback {change.operation} on {change.table_name}: {str(e)}"
                            logger.error(error_msg)
                            results['errors'].append(error_msg)
            
            results['success'] = len(results['errors']) == 0
            
        except Exception as e:
            logger.error(f"Schema rollback failed: {str(e)}")
            results['errors'].append(str(e))
        
        return results
    
    def preview_schema_ddl(self, schema_definition: Dict[str, Any], session_id: str) -> List[str]:
        """Generate preview of DDL statements that would be executed"""
        
        ddl_statements = []
        
        try:
            # Main table DDL
            main_table_name = f"import_data_{session_id[:8]}"
            main_table = schema_definition.get('main_table', {})
            
            if main_table:
                ddl_parts = [f"CREATE TABLE {main_table_name} ("]
                column_definitions = []
                
                for col in main_table.get('columns', []):
                    col_def = self._build_column_definition(col)
                    if col_def:
                        column_definitions.append(col_def)
                
                ddl_parts.append("  " + ",\n  ".join(column_definitions))
                ddl_parts.append(")")
                
                ddl_statements.append("\n".join(ddl_parts))
            
            # Master tables DDL
            master_tables = schema_definition.get('master_tables', {})
            for table_name, table_def in master_tables.items():
                # Check if table exists
                inspector = inspect(self.engine)
                table_exists = table_name in inspector.get_table_names()
                
                if not table_exists:
                    # CREATE TABLE
                    ddl_parts = [f"CREATE TABLE {table_name} ("]
                    column_definitions = []
                    
                    for col in table_def.get('columns', []):
                        col_def = self._build_column_definition(col)
                        if col_def:
                            column_definitions.append(col_def)
                    
                    ddl_parts.append("  " + ",\n  ".join(column_definitions))
                    ddl_parts.append(")")
                    
                    ddl_statements.append("\n".join(ddl_parts))
                else:
                    # ALTER TABLE (add missing columns)
                    existing_columns = {col['name']: col for col in inspector.get_columns(table_name)}
                    
                    for col in table_def.get('columns', []):
                        if col['name'] not in existing_columns:
                            col_def = self._build_column_definition(col, for_alter=True)
                            if col_def:
                                ddl_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_def}")
            
            # Index DDL
            indexes = schema_definition.get('recommended_indexes', [])
            for index_def in indexes:
                index_name = index_def['name']
                columns = index_def['columns']
                table_name = index_def.get('table_name', main_table_name)
                
                columns_str = ', '.join(columns)
                ddl_statements.append(f"CREATE INDEX {index_name} ON {table_name} ({columns_str})")
            
            # Foreign key constraints
            for col in main_table.get('columns', []):
                if 'FOREIGN_KEY' in col.get('constraints', []):
                    references = col.get('references', '')
                    if references:
                        ref_table, ref_column = references.split('.')
                        constraint_name = f"fk_{main_table_name}_{col['name']}"
                        fk_ddl = (f"ALTER TABLE {main_table_name} "
                                 f"ADD CONSTRAINT {constraint_name} "
                                 f"FOREIGN KEY ({col['name']}) "
                                 f"REFERENCES {ref_table}({ref_column})")
                        ddl_statements.append(fk_ddl)
            
        except Exception as e:
            logger.error(f"DDL preview generation failed: {str(e)}")
            ddl_statements.append(f"-- ERROR: Could not generate preview: {str(e)}")
        
        return ddl_statements