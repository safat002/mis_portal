# mis_app/services.py
"""
Business logic and service layer for MIS Application
"""

import json
import logging
import pandas as pd
from typing import Dict, List, Any, Optional
from django.db import transaction
from django.utils import timezone
from .models import (
    ExternalConnection, SavedReport, Dashboard, Widget,
    CleanedDataSource, TransformationTemplate, Notification
)

logger = logging.getLogger(__name__)

class DataTransformationService:
    """Service for handling data transformation operations."""
    
    @staticmethod
    def apply_transformation_recipe(df: pd.DataFrame, recipe: List[Dict]) -> pd.DataFrame:
        """Apply a transformation recipe to a DataFrame."""
        result_df = df.copy()
        
        for step in recipe:
            step_type = step.get('type')
            params = step.get('params', {})
            
            try:
                if step_type == 'remove_duplicates':
                    result_df = result_df.drop_duplicates(subset=params.get('columns'))
                
                elif step_type == 'handle_missing':
                    strategy = params.get('strategy', 'drop')
                    columns = params.get('columns', [])
                    
                    if strategy == 'drop':
                        result_df = result_df.dropna(subset=columns if columns else None)
                    elif strategy == 'fill':
                        fill_value = params.get('fill_value', 0)
                        if columns:
                            result_df[columns] = result_df[columns].fillna(fill_value)
                        else:
                            result_df = result_df.fillna(fill_value)
                
                elif step_type == 'change_data_type':
                    column = params.get('column')
                    new_type = params.get('new_type')
                    if column and new_type:
                        result_df[column] = result_df[column].astype(new_type)
                
                elif step_type == 'find_replace':
                    column = params.get('column')
                    find_value = params.get('find_value')
                    replace_value = params.get('replace_value')
                    if column:
                        result_df[column] = result_df[column].replace(find_value, replace_value)
                
                elif step_type == 'split_column':
                    column = params.get('column')
                    delimiter = params.get('delimiter', ',')
                    new_columns = params.get('new_columns', [])
                    if column and new_columns:
                        split_data = result_df[column].str.split(delimiter, expand=True)
                        for i, new_col in enumerate(new_columns):
                            if i < len(split_data.columns):
                                result_df[new_col] = split_data[i]
                
                elif step_type == 'merge_columns':
                    columns = params.get('columns', [])
                    new_column = params.get('new_column')
                    separator = params.get('separator', ' ')
                    if columns and new_column:
                        result_df[new_column] = result_df[columns].apply(
                            lambda x: separator.join(x.astype(str)), axis=1
                        )
                
                elif step_type == 'filter_rows':
                    column = params.get('column')
                    operator = params.get('operator', '==')
                    value = params.get('value')
                    
                    if column and value is not None:
                        if operator == '==':
                            result_df = result_df[result_df[column] == value]
                        elif operator == '!=':
                            result_df = result_df[result_df[column] != value]
                        elif operator == '>':
                            result_df = result_df[result_df[column] > value]
                        elif operator == '<':
                            result_df = result_df[result_df[column] < value]
                        elif operator == 'contains':
                            result_df = result_df[result_df[column].str.contains(str(value), na=False)]
                
                elif step_type == 'sort_data':
                    column = params.get('column')
                    ascending = params.get('ascending', True)
                    if column:
                        result_df = result_df.sort_values(by=column, ascending=ascending)
                
                elif step_type == 'add_calculated_column':
                    new_column = params.get('new_column')
                    formula = params.get('formula')
                    if new_column and formula:
                        # Simple formula evaluation (be careful with security in production)
                        result_df[new_column] = result_df.eval(formula)
                
            except Exception as e:
                logger.error(f"Error applying transformation step {step_type}: {e}")
                continue
        
        return result_df
    
    @staticmethod
    def generate_sql_from_recipe(table_name: str, recipe: List[Dict]) -> str:
        """Generate SQL query from transformation recipe."""
        sql_parts = [f"SELECT * FROM {table_name}"]
        where_conditions = []
        
        for step in recipe:
            step_type = step.get('type')
            params = step.get('params', {})
            
            if step_type == 'filter_rows':
                column = params.get('column')
                operator = params.get('operator', '==')
                value = params.get('value')
                
                if column and value is not None:
                    if operator == '==':
                        where_conditions.append(f"{column} = '{value}'")
                    elif operator == '!=':
                        where_conditions.append(f"{column} != '{value}'")
                    elif operator == '>':
                        where_conditions.append(f"{column} > {value}")
                    elif operator == '<':
                        where_conditions.append(f"{column} < {value}")
                    elif operator == 'contains':
                        where_conditions.append(f"{column} LIKE '%{value}%'")
        
        if where_conditions:
            sql_parts.append("WHERE " + " AND ".join(where_conditions))
        
        return " ".join(sql_parts)

class ReportService:
    """Service for handling report operations."""
    
    @staticmethod
    def execute_report(report_id: str, user) -> Dict[str, Any]:
        """Execute a saved report and return results."""
        try:
            report = SavedReport.objects.get(id=report_id, owner=user)
            
            # Update execution stats
            report.last_executed = timezone.now()
            report.execution_count += 1
            report.save()
            
            # Get data based on report configuration
            config = report.report_config
            connection = report.connection
            
            # This would contain the actual data fetching logic
            # For now, return mock data
            return {
                'success': True,
                'data': [],
                'metadata': {
                    'report_name': report.report_name,
                    'executed_at': report.last_executed.isoformat(),
                    'row_count': 0
                }
            }
            
        except SavedReport.DoesNotExist:
            return {'success': False, 'error': 'Report not found'}
        except Exception as e:
            logger.error(f"Error executing report {report_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    @staticmethod
    def export_report(report_id: str, format_type: str, user) -> Dict[str, Any]:
        """Export report data in specified format."""
        try:
            report = SavedReport.objects.get(id=report_id, owner=user)
            
            # Execute report to get data
            result = ReportService.execute_report(report_id, user)
            
            if not result['success']:
                return result
            
            # Handle different export formats
            if format_type == 'csv':
                # Convert to CSV
                pass
            elif format_type == 'excel':
                # Convert to Excel
                pass
            elif format_type == 'pdf':
                # Convert to PDF
                pass
            
            return {
                'success': True,
                'download_url': f'/api/reports/{report_id}/download/',
                'format': format_type
            }
            
        except Exception as e:
            logger.error(f"Error exporting report {report_id}: {e}")
            return {'success': False, 'error': str(e)}

class DashboardService:
    """Service for handling dashboard operations."""
    
    @staticmethod
    def get_dashboard_data(dashboard_id: str, user) -> Dict[str, Any]:
        """Get all data for a dashboard including widgets."""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, owner=user)
            widgets = Widget.objects.filter(dashboard=dashboard, is_active=True)
            
            dashboard_data = {
                'id': str(dashboard.id),
                'title': dashboard.title,
                'description': dashboard.description,
                'layout_config': dashboard.layout_config,
                'theme': dashboard.theme,
                'global_filters': dashboard.global_filters,
                'widgets': []
            }
            
            for widget in widgets:
                widget_data = DashboardService.get_widget_data(widget)
                dashboard_data['widgets'].append(widget_data)
            
            return {'success': True, 'dashboard': dashboard_data}
            
        except Dashboard.DoesNotExist:
            return {'success': False, 'error': 'Dashboard not found'}
        except Exception as e:
            logger.error(f"Error loading dashboard {dashboard_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    @staticmethod
    def get_widget_data(widget: Widget) -> Dict[str, Any]:
        """Get data for a specific widget."""
        return {
            'id': str(widget.id),
            'widget_type': widget.widget_type,
            'title': widget.title,
            'position_config': widget.position_config,
            'data_config': widget.data_config,
            'display_options': widget.display_options,
            'filter_config': widget.filter_config,
            'data': []  # This would be populated with actual data
        }
    
    @staticmethod
    def save_dashboard_layout(dashboard_id: str, layout_config: Dict, user) -> Dict[str, Any]:
        """Save dashboard layout configuration."""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id, owner=user)
            dashboard.layout_config = layout_config
            dashboard.save()
            
            return {'success': True, 'message': 'Layout saved successfully'}
            
        except Dashboard.DoesNotExist:
            return {'success': False, 'error': 'Dashboard not found'}
        except Exception as e:
            logger.error(f"Error saving dashboard layout {dashboard_id}: {e}")
            return {'success': False, 'error': str(e)}

class NotificationService:
    """Service for handling notifications."""
    
    @staticmethod
    def create_notification(recipient, title: str, message: str, 
                          notification_type: str = 'info', 
                          sender=None, action_url: str = None) -> Notification:
        """Create a new notification."""
        return Notification.objects.create(
            recipient=recipient,
            sender=sender,
            title=title,
            message=message,
            type=notification_type,
            action_url=action_url
        )
    
    @staticmethod
    def send_bulk_notification(recipients: List, title: str, message: str,
                             notification_type: str = 'info') -> int:
        """Send notification to multiple users."""
        notifications = []
        for recipient in recipients:
            notifications.append(
                Notification(
                    recipient=recipient,
                    title=title,
                    message=message,
                    type=notification_type
                )
            )
        
        created_notifications = Notification.objects.bulk_create(notifications)
        return len(created_notifications)
    
    @staticmethod
    def mark_all_read(user) -> int:
        """Mark all notifications as read for a user."""
        updated = Notification.objects.filter(
            recipient=user, 
            is_read=False
        ).update(
            is_read=True, 
            read_at=timezone.now()
        )
        return updated

class ConnectionService:
    """Service for handling database connections."""
    
    @staticmethod
    def test_connection(connection_id: str, user) -> Dict[str, Any]:
        """Test database connection and update health status."""
        try:
            connection = ExternalConnection.objects.get(id=connection_id, owner=user)
            
            # Import here to avoid circular imports
            from .views import get_external_engine
            
            engine = get_external_engine(connection_id, user)
            
            if not engine:
                connection.health_status = 'error'
                connection.last_health_check = timezone.now()
                connection.save()
                return {'success': False, 'error': 'Failed to create engine'}
            
            # Test the connection
            with engine.connect() as conn:
                conn.execute("SELECT 1")
            
            # Update health status
            connection.health_status = 'healthy'
            connection.last_health_check = timezone.now()
            connection.save()
            
            return {
                'success': True, 
                'message': f'Connection to {connection.nickname} is healthy'
            }
            
        except ExternalConnection.DoesNotExist:
            return {'success': False, 'error': 'Connection not found'}
        except Exception as e:
            # Update health status to error
            try:
                connection = ExternalConnection.objects.get(id=connection_id, owner=user)
                connection.health_status = 'error'
                connection.last_health_check = timezone.now()
                connection.save()
            except:
                pass
            
            logger.error(f"Connection test failed for {connection_id}: {e}")
            return {'success': False, 'error': str(e)}
    
    @staticmethod
    def get_connection_info(connection_id: str, user) -> Dict[str, Any]:
        """Get detailed connection information."""
        try:
            connection = ExternalConnection.objects.get(id=connection_id, owner=user)
            
            return {
                'success': True,
                'connection': {
                    'id': str(connection.id),
                    'nickname': connection.nickname,
                    'dbtype': connection.dbtype,
                    'host': connection.host,
                    'port': connection.port,
                    'dbname': connection.dbname,
                    'schema': connection.schema,
                    'health_status': connection.health_status,
                    'last_health_check': connection.last_health_check.isoformat() if connection.last_health_check else None,
                    'created_at': connection.created_at.isoformat()
                }
            }
            
        except ExternalConnection.DoesNotExist:
            return {'success': False, 'error': 'Connection not found'}
        except Exception as e:
            logger.error(f"Error getting connection info {connection_id}: {e}")
            return {'success': False, 'error': str(e)}