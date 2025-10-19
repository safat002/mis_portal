"""
External Database Service for Django MIS Application

This module handles connections to external databases using SQLAlchemy.
Converted from Flask external_db service to Django service.
"""

import logging
from typing import Dict, List, Optional, Any
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from django.core.cache import cache
from django.conf import settings

from ..models import ExternalConnection

logger = logging.getLogger(__name__)

# Connection pool for external databases
_connection_engines = {}


class ExternalDBService:
    """
    Service for managing external database connections and operations
    """
    def __init__(self, connection_id: str):
        self.connection_id = connection_id
        self._connection = None
        self._engine = None

    @property
    def connection(self) -> Optional[ExternalConnection]:
        """Get the connection model instance"""
        if not self._connection:
            try:
                self._connection = ExternalConnection.objects.get(id=self.connection_id)
            except ExternalConnection.DoesNotExist:
                logger.error(f"Connection {self.connection_id} not found")
                return None
        return self._connection


    
    @property
    def engine(self):
        """Get or create SQLAlchemy engine"""
        if self.connection_id not in _connection_engines:
            if not self.connection:
                return None
            
            try:
                connect_args = {}
                if self.connection.db_type != 'sqlite':
                    connect_args['connect_timeout'] = 30

                engine = create_engine(
                    self.connection.get_connection_uri(),
                    pool_pre_ping=True,
                    pool_recycle=3600,
                    connect_args=connect_args
                )
                _connection_engines[self.connection_id] = engine
                logger.info(f"Created database engine for connection {self.connection.nickname}")
            except Exception as e:
                logger.error(f"Failed to create engine for {self.connection_id}: {str(e)}")
                return None
        
        return _connection_engines.get(self.connection_id)
    
    def test_connection(self) -> bool:
        """
        Test if the database connection is working
        """
        if not self.engine:
            return False
        
        try:
            with self.engine.connect() as conn:
                # Simple test query that works on most databases
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Connection test failed for {self.connection_id}: {str(e)}")
            return False
    
    def disconnect(self):
        """
        Disconnect from the external database
        """
        if self.connection_id in _connection_engines:
            try:
                engine = _connection_engines[self.connection_id]
                if engine:
                    engine.dispose()
                    del _connection_engines[self.connection_id]
                    logger.info(f"Disconnected from database {self.connection_id}")
            except Exception as e:
                logger.error(f"Error disconnecting from {self.connection_id}: {str(e)}")

    
    def get_visible_tables(self) -> List[str]:
        """Get list of visible tables from the database"""
        if not self.engine:
            return []

        # Check if engine is still valid
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))  # Simple test query
        except Exception as e:
            logger.error(f"Connection to {self.connection_id} is invalid: {str(e)}")
            self.disconnect()
            return []

        connection = self.connection
        schema = None
        schema_cache_token = 'default'
        if connection and connection.schema:
            schema_candidate = connection.schema.strip()
            if schema_candidate:
                schema_cache_token = schema_candidate
                if connection.db_type == 'postgresql':
                    schema = schema_candidate

        cache_key = f"tables_{self.connection_id}_{schema_cache_token}"
        cached_tables = cache.get(cache_key)
        if cached_tables:
            return cached_tables

        try:
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names(schema=schema)

            hidden_tables_set = set()
            if connection and connection.hidden_tables:
                hidden_tables_set = {
                    t.strip()
                    for t in connection.hidden_tables.split(',')
                    if t.strip()
                }

            visible_tables = [table for table in all_tables if table not in hidden_tables_set]

            # Cache for 5 minutes
            cache.set(cache_key, visible_tables, 300)
            return visible_tables

        except Exception as e:
            logger.error(f"Error getting tables for {self.connection_id}: {str(e)}")
            return []

    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        if not self.engine:
            return []

        # Validate engine connection
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))  # Test query
        except Exception as e:
            logger.error(f"Connection to {self.connection_id} is invalid: {str(e)}")
            self.disconnect()
            return []

        cache_key = f"columns_{self.connection_id}_{table_name}"
        cached_columns = cache.get(cache_key)
        if cached_columns:
            return cached_columns

        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            columns_data = []
            for col in columns:
                columns_data.append({
                    'name': col['name'],
                    'type': str(col['type']),
                    'nullable': col.get('nullable', True),
                    'default': col.get('default'),
                    'primary_key': col.get('primary_key', False),
                    'is_numeric': self._is_numeric_type(col['type']),
                })

            # Cache for 10 minutes
            cache.set(cache_key, columns_data, 600)
            return columns_data

        except Exception as e:
            logger.error(f"Error getting columns for {table_name} in {self.connection_id}: {str(e)}")
            return []

    
    def execute_query(self, query: str, params: Dict = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame
        """
        if not self.engine:
            raise ValueError("Database connection not available")
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params or {})
            return df
            
        except Exception as e:
            logger.error(f"Error executing query in {self.connection_id}: {str(e)}")
            raise
    
    def get_table_preview(self, table_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Get a preview of table data
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Get the total number of rows in a table
        """
        try:
            query = f"SELECT COUNT(*) as count FROM {table_name}"
            result = self.execute_query(query)
            return result.iloc[0]['count']
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {str(e)}")
            return 0
    
    def get_column_stats(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Get basic statistics for a column
        """
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT({column_name}) as non_null_count,
                COUNT(DISTINCT {column_name}) as distinct_count
            FROM {table_name}
            """
            result = self.execute_query(query)
            stats = result.iloc[0].to_dict()
            
            stats['null_count'] = stats['total_count'] - stats['non_null_count']
            stats['null_percentage'] = (stats['null_count'] / stats['total_count']) * 100 if stats['total_count'] > 0 else 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats for {column_name} in {table_name}: {str(e)}")
            return {}
    
    def get_distinct_values(self, table_name: str, column_name: str, limit: int = 100) -> List[Any]:
        """
        Get distinct values for a column (useful for filters)
        """
        try:
            query = f"SELECT DISTINCT {column_name} FROM {table_name} WHERE {column_name} IS NOT NULL ORDER BY {column_name} LIMIT {limit}"
            result = self.execute_query(query)
            return result[column_name].tolist()
            
        except Exception as e:
            logger.error(f"Error getting distinct values for {column_name}: {str(e)}")
            return []
    
    def create_view(self, view_name: str, query: str) -> bool:
        """
        Create a database view
        """
        if not self.engine:
            return False
        
        try:
            with self.engine.connect() as conn:
                # Drop view if exists
                drop_query = f"DROP VIEW IF EXISTS {view_name}"
                conn.execute(text(drop_query))
                
                # Create new view
                create_query = f"CREATE VIEW {view_name} AS {query}"
                conn.execute(text(create_query))
                conn.commit()
            
            logger.info(f"Created view {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating view {view_name}: {str(e)}")
            return False
    
    def drop_view(self, view_name: str) -> bool:
        """
        Drop a database view
        """
        if not self.engine:
            return False
        
        try:
            with self.engine.connect() as conn:
                query = f"DROP VIEW IF EXISTS {view_name}"
                conn.execute(text(query))
                conn.commit()
            
            logger.info(f"Dropped view {view_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error dropping view {view_name}: {str(e)}")
            return False
    
    def get_database_info(self) -> Dict[str, Any]:
        """
        Get general information about the database
        """
        if not self.connection or not self.engine:
            return {}
        
        try:
            info = {
                'connection_name': self.connection.nickname,
                'db_type': self.connection.db_type,
                'host': self.connection.host,
                'database': self.connection.db_name,
                'schema': self.connection.schema,
                'connected': self.test_connection(),
            }
            
            if info['connected']:
                inspector = inspect(self.engine)
                info['table_count'] = len(inspector.get_table_names())
                
                # Get database version if possible
                try:
                    with self.engine.connect() as conn:
                        if self.connection.db_type == 'postgresql':
                            result = conn.execute(text("SELECT version()"))
                        elif self.connection.db_type == 'mysql':
                            result = conn.execute(text("SELECT VERSION()"))
                        elif self.connection.db_type == 'sqlite':
                            result = conn.execute(text("SELECT sqlite_version()"))
                        else:
                            result = None
                        
                        if result:
                            info['version'] = result.fetchone()[0]
                except:
                    pass
            
            return info
            
        except Exception as e:
            logger.error(f"Error getting database info: {str(e)}")
            return {'error': str(e)}
    
    def _is_numeric_type(self, sql_type) -> bool:
        """
        Check if a SQLAlchemy type is numeric
        """
        from sqlalchemy.sql.sqltypes import (
            Integer, Numeric, Float, BigInteger, 
            SmallInteger, DECIMAL, REAL
        )
        
        try:
            return isinstance(sql_type, (
                Integer, Numeric, Float, BigInteger, 
                SmallInteger, DECIMAL, REAL
            ))
        except:
            # Fallback: check type name
            type_name = str(sql_type).upper()
            numeric_types = [
                'INTEGER', 'INT', 'BIGINT', 'SMALLINT', 'TINYINT',
                'DECIMAL', 'NUMERIC', 'FLOAT', 'REAL', 'DOUBLE',
                'MONEY', 'NUMBER'
            ]
            return any(num_type in type_name for num_type in numeric_types)
    
    @staticmethod
    def get_connection_status(connection_id: str) -> Dict[str, Any]:
        """
        Get the status of a connection
        """
        service = ExternalDBService(connection_id)
        return {
            'connected': service.test_connection(),
            'connection_id': connection_id,
            'engine_cached': connection_id in _connection_engines,
        }
    
    @staticmethod
    def disconnect_all():
        """
        Disconnect all external database connections
        """
        global _connection_engines
        for connection_id, engine in list(_connection_engines.items()):
            try:
                engine.dispose()
                logger.info(f"Disconnected from {connection_id}")
            except Exception as e:
                logger.error(f"Error disconnecting from {connection_id}: {str(e)}")
        
        _connection_engines.clear()
    
    @staticmethod  
    def cleanup_connections():
        """
        Clean up stale connections (should be called periodically)
        """
        global _connection_engines
        stale_connections = []
        
        for connection_id, engine in _connection_engines.items():
            try:
                # Try to execute a simple query to test if connection is still valid
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            except:
                stale_connections.append(connection_id)
        
        for connection_id in stale_connections:
            try:
                _connection_engines[connection_id].dispose()
                del _connection_engines[connection_id]
                logger.info(f"Cleaned up stale connection: {connection_id}")
            except:
                pass


# Utility functions for backward compatibility
def get_external_engine(connection_id: str):
    """
    Get SQLAlchemy engine for external connection (backward compatibility)
    """
    service = ExternalDBService(connection_id)
    return service.engine


def disconnect_external_db(connection_id: str = None):
    """
    Disconnect external database (backward compatibility)
    """
    if connection_id:
        service = ExternalDBService(connection_id)
        service.disconnect()
    else:
        ExternalDBService.disconnect_all()


def get_external_db_status(connection_id: str) -> Dict[str, Any]:
    """
    Get external database status (backward compatibility)
    """
    return ExternalDBService.get_connection_status(connection_id)