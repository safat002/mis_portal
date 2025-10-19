"""
Django Dashboard Service
Service for dashboard management, widget operations, and real-time updates

Handles dashboard creation, sharing, permissions, and widget management
"""

import json
import logging
from typing import Dict, List, Optional, Any, Tuple
from django.db import transaction
from django.utils import timezone
from django.core.cache import cache
from django.db.models import Q
import pandas as pd

from ..models import Dashboard, Widget, User, ExternalConnection, DashboardShare, AuditLog
from .report_builder import ReportBuilderService
from .external_db import ExternalDBService
from .notification_service import notification_service
from ..utils import log_user_action

logger = logging.getLogger(__name__)


class DashboardService:
    """
    Service for managing dashboards and widgets
    """
    
    def __init__(self):
        self.cache_timeout = 300  # 5 minutes
        self.report_service = ReportBuilderService()
        
        # Widget type configurations
        self.widget_types = {
            'chart': {
                'name': 'Chart Widget',
                'description': 'Display data as various chart types',
                'data_required': True,
                'config_schema': {
                    'chart_type': ['bar', 'line', 'pie', 'area', 'scatter'],
                    'x_axis': 'string',
                    'y_axis': 'string',
                    'color_scheme': 'string'
                }
            },
            'metric': {
                'name': 'Metric Widget',
                'description': 'Show single key performance indicators',
                'data_required': True,
                'config_schema': {
                    'metric_field': 'string',
                    'aggregation': ['sum', 'avg', 'count', 'min', 'max'],
                    'format': 'string',
                    'comparison': 'object'
                }
            },
            'table': {
                'name': 'Data Table',
                'description': 'Display data in tabular format',
                'data_required': True,
                'config_schema': {
                    'columns': 'array',
                    'page_size': 'number',
                    'sortable': 'boolean'
                }
            },
            'filter': {
                'name': 'Filter Widget',
                'description': 'Interactive filters for dashboard',
                'data_required': False,
                'config_schema': {
                    'filter_type': ['dropdown', 'daterange', 'text'],
                    'target_widgets': 'array',
                    'field': 'string'
                }
            },
            'text': {
                'name': 'Text Widget',
                'description': 'Static text and markdown content',
                'data_required': False,
                'config_schema': {
                    'content': 'string',
                    'format': ['plain', 'markdown', 'html']
                }
            }
        }
    
    def create_dashboard(self, title: str, description: str, user: User, 
                        config: Dict = None) -> Dashboard:
        """
        Create a new dashboard
        
        Args:
            title: Dashboard title
            description: Dashboard description
            user: Owner user
            config: Dashboard configuration
            
        Returns:
            Created dashboard instance
        """
        try:
            with transaction.atomic():
                dashboard = Dashboard.objects.create(
                    title=title,
                    description=description,
                    owner=user,
                    config=config or {},
                    is_public=False
                )
                
                # Log creation
                log_user_action(
                    user, 'create_dashboard', 'dashboard', str(dashboard.id),
                    f'Created dashboard: {title}', {'title': title}
                )
                
                return dashboard
                
        except Exception as e:
            logger.error(f"Error creating dashboard: {e}")
            raise
    
    def update_dashboard(self, dashboard_id: str, user: User, 
                        title: str = None, description: str = None,
                        config: Dict = None, is_public: bool = None) -> Dashboard:
        """Update existing dashboard"""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if not self._can_edit_dashboard(dashboard, user):
                raise PermissionError("Insufficient permissions to edit dashboard")
            
            # Update fields
            if title is not None:
                dashboard.title = title
            if description is not None:
                dashboard.description = description
            if config is not None:
                dashboard.config = config
            if is_public is not None:
                dashboard.is_public = is_public
            
            dashboard.updated_at = timezone.now()
            dashboard.save()
            
            # Log update
            log_user_action(
                user, 'update_dashboard', 'dashboard', str(dashboard.id),
                f'Updated dashboard: {dashboard.title}', {}
            )
            
            return dashboard
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error updating dashboard: {e}")
            raise
    
    def delete_dashboard(self, dashboard_id: str, user: User) -> bool:
        """Delete dashboard"""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if dashboard.owner != user and user.user_type != 'Admin':
                raise PermissionError("Only dashboard owner or admin can delete")
            
            title = dashboard.title
            
            # Delete associated widgets
            Widget.objects.filter(dashboard=dashboard).delete()
            
            # Delete dashboard
            dashboard.delete()
            
            # Log deletion
            log_user_action(
                user, 'delete_dashboard', 'dashboard', dashboard_id,
                f'Deleted dashboard: {title}', {'title': title}
            )
            
            return True
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error deleting dashboard: {e}")
            raise
    
    def share_dashboard(self, dashboard_id: str, user: User, 
                       share_with_users: List[str], permission: str = 'view') -> List[DashboardShare]:
        """
        Share dashboard with other users
        
        Args:
            dashboard_id: Dashboard to share
            user: User sharing the dashboard
            share_with_users: List of user IDs to share with
            permission: Permission level ('view' or 'edit')
            
        Returns:
            List of created share instances
        """
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if not self._can_edit_dashboard(dashboard, user):
                raise PermissionError("Insufficient permissions to share dashboard")
            
            shares = []
            with transaction.atomic():
                for user_id in share_with_users:
                    try:
                        target_user = User.objects.get(id=user_id)
                        
                        # Create or update share
                        share, created = DashboardShare.objects.get_or_create(
                            dashboard=dashboard,
                            user=target_user,
                            defaults={'permission': permission, 'shared_by': user}
                        )
                        
                        if not created:
                            share.permission = permission
                            share.shared_by = user
                            share.save()
                        
                        shares.append(share)
                        
                        # Send notification
                        notification_service.notify_dashboard_shared(
                            dashboard, target_user, user, permission
                        )
                        
                    except User.DoesNotExist:
                        logger.warning(f"User {user_id} not found for sharing")
                        continue
            
            # Log sharing
            log_user_action(
                user, 'share_dashboard', 'dashboard', str(dashboard.id),
                f'Shared dashboard with {len(shares)} users',
                {'shared_count': len(shares), 'permission': permission}
            )
            
            return shares
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error sharing dashboard: {e}")
            raise
    
    def create_widget(self, dashboard_id: str, widget_type: str, 
                     title: str, config: Dict, user: User,
                     position: Dict = None) -> Widget:
        """
        Create new widget on dashboard
        
        Args:
            dashboard_id: Target dashboard
            widget_type: Type of widget
            title: Widget title
            config: Widget configuration
            user: User creating widget
            position: Widget position and size
            
        Returns:
            Created widget instance
        """
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if not self._can_edit_dashboard(dashboard, user):
                raise PermissionError("Insufficient permissions to add widget")
            
            # Validate widget type
            if widget_type not in self.widget_types:
                raise ValueError(f"Invalid widget type: {widget_type}")
            
            # Create widget
            widget = Widget.objects.create(
                dashboard=dashboard,
                type=widget_type,
                title=title,
                config=config,
                position=position or {'x': 0, 'y': 0, 'w': 4, 'h': 3},
                created_by=user
            )
            
            # Log creation
            log_user_action(
                user, 'create_widget', 'widget', str(widget.id),
                f'Added {widget_type} widget to dashboard',
                {'widget_type': widget_type, 'dashboard_id': dashboard_id}
            )
            
            return widget
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error creating widget: {e}")
            raise
    
    def update_widget(self, widget_id: str, user: User, 
                     title: str = None, config: Dict = None,
                     position: Dict = None) -> Widget:
        """Update existing widget"""
        try:
            widget = Widget.objects.get(id=widget_id)
            
            # Check permissions
            if not self._can_edit_dashboard(widget.dashboard, user):
                raise PermissionError("Insufficient permissions to edit widget")
            
            # Update fields
            if title is not None:
                widget.title = title
            if config is not None:
                widget.config = config
            if position is not None:
                widget.position = position
            
            widget.updated_at = timezone.now()
            widget.save()
            
            # Invalidate cache
            self._invalidate_widget_cache(widget_id)
            
            # Log update
            log_user_action(
                user, 'update_widget', 'widget', str(widget.id),
                f'Updated widget: {widget.title}', {}
            )
            
            return widget
            
        except Widget.DoesNotExist:
            raise ValueError(f"Widget {widget_id} not found")
        except Exception as e:
            logger.error(f"Error updating widget: {e}")
            raise
    
    def delete_widget(self, widget_id: str, user: User) -> bool:
        """Delete widget"""
        try:
            widget = Widget.objects.get(id=widget_id)
            
            # Check permissions
            if not self._can_edit_dashboard(widget.dashboard, user):
                raise PermissionError("Insufficient permissions to delete widget")
            
            title = widget.title
            
            # Invalidate cache
            self._invalidate_widget_cache(widget_id)
            
            # Delete widget
            widget.delete()
            
            # Log deletion
            log_user_action(
                user, 'delete_widget', 'widget', widget_id,
                f'Deleted widget: {title}', {'title': title}
            )
            
            return True
            
        except Widget.DoesNotExist:
            raise ValueError(f"Widget {widget_id} not found")
        except Exception as e:
            logger.error(f"Error deleting widget: {e}")
            raise
    
    def get_widget_data(self, widget_id: str, user: User, 
                       filters: Dict = None) -> Dict[str, Any]:
        """
        Get data for a specific widget
        
        Args:
            widget_id: Widget ID
            user: User requesting data
            filters: Optional filters to apply
            
        Returns:
            Widget data and metadata
        """
        try:
            widget = Widget.objects.get(id=widget_id)
            
            # Check permissions
            if not self._can_view_dashboard(widget.dashboard, user):
                raise PermissionError("Insufficient permissions to view widget data")
            
            # Check cache first
            cache_key = f"widget_data_{widget_id}_{hash(str(filters))}"
            cached_data = cache.get(cache_key)
            if cached_data:
                return cached_data
            
            # Get widget configuration
            config = widget.config
            widget_type = widget.type
            
            # Generate data based on widget type
            if widget_type in ['chart', 'metric', 'table']:
                data = self._get_data_widget_data(widget, filters)
            elif widget_type == 'filter':
                data = self._get_filter_widget_data(widget)
            elif widget_type == 'text':
                data = self._get_text_widget_data(widget)
            else:
                data = {'error': f'Unknown widget type: {widget_type}'}
            
            # Add metadata
            result = {
                'widget_id': widget_id,
                'type': widget_type,
                'title': widget.title,
                'data': data,
                'last_updated': timezone.now().isoformat(),
                'config': config
            }
            
            # Cache result
            cache.set(cache_key, result, self.cache_timeout)
            
            return result
            
        except Widget.DoesNotExist:
            raise ValueError(f"Widget {widget_id} not found")
        except Exception as e:
            logger.error(f"Error getting widget data: {e}")
            return {
                'widget_id': widget_id,
                'error': str(e),
                'last_updated': timezone.now().isoformat()
            }
    
    def _get_data_widget_data(self, widget: Widget, filters: Dict = None) -> Dict[str, Any]:
        """Get data for data-driven widgets (chart, metric, table)"""
        try:
            config = widget.config
            
            # Build report configuration from widget config
            report_config = {
                'connection_id': config.get('connection_id'),
                'columns': config.get('columns', []),
                'groups': config.get('groups', []),
                'filters': config.get('filters', []),
                'sorts': config.get('sorts', []),
                'page_size': config.get('page_size', 1000)
            }
            
            # Apply additional filters
            if filters:
                report_config['filters'] = report_config['filters'] + [filters]
            
            # Execute report
            df, total_rows, error = self.report_service.build_advanced_report(
                report_config, widget.created_by
            )
            
            if error:
                return {'error': error}
            
            if df is None or df.empty:
                return {'data': [], 'row_count': 0}
            
            # Format data based on widget type
            if widget.type == 'metric':
                return self._format_metric_data(df, config)
            elif widget.type == 'chart':
                return self._format_chart_data(df, config)
            elif widget.type == 'table':
                return self._format_table_data(df, config)
            
            return {'data': df.to_dict('records'), 'row_count': len(df)}
            
        except Exception as e:
            logger.error(f"Error getting data widget data: {e}")
            return {'error': str(e)}
    
    def _format_metric_data(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Format data for metric widget"""
        try:
            metric_field = config.get('metric_field')
            aggregation = config.get('aggregation', 'sum')
            
            if not metric_field or metric_field not in df.columns:
                return {'error': 'Invalid metric field'}
            
            # Calculate metric value
            if aggregation == 'sum':
                value = df[metric_field].sum()
            elif aggregation == 'avg':
                value = df[metric_field].mean()
            elif aggregation == 'count':
                value = df[metric_field].count()
            elif aggregation == 'min':
                value = df[metric_field].min()
            elif aggregation == 'max':
                value = df[metric_field].max()
            else:
                value = df[metric_field].sum()  # Default to sum
            
            # Format value
            format_str = config.get('format', '{:,.0f}')
            try:
                formatted_value = format_str.format(value)
            except:
                formatted_value = str(value)
            
            return {
                'value': value,
                'formatted_value': formatted_value,
                'aggregation': aggregation,
                'field': metric_field,
                'row_count': len(df)
            }
            
        except Exception as e:
            return {'error': f'Error formatting metric data: {str(e)}'}
    
    def _format_chart_data(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Format data for chart widget"""
        try:
            chart_type = config.get('chart_type', 'bar')
            x_axis = config.get('x_axis')
            y_axis = config.get('y_axis')
            
            if not x_axis or not y_axis:
                return {'error': 'Chart requires x_axis and y_axis configuration'}
            
            if x_axis not in df.columns or y_axis not in df.columns:
                return {'error': 'Invalid axis columns'}
            
            # Prepare chart data
            chart_data = []
            for _, row in df.iterrows():
                chart_data.append({
                    'x': row[x_axis],
                    'y': row[y_axis]
                })
            
            return {
                'chart_type': chart_type,
                'data': chart_data,
                'x_axis': x_axis,
                'y_axis': y_axis,
                'row_count': len(df)
            }
            
        except Exception as e:
            return {'error': f'Error formatting chart data: {str(e)}'}
    
    def _format_table_data(self, df: pd.DataFrame, config: Dict) -> Dict[str, Any]:
        """Format data for table widget"""
        try:
            # Get column configuration
            columns = config.get('columns', [])
            if not columns:
                columns = [{'field': col, 'title': col} for col in df.columns]
            
            # Apply column selection
            selected_columns = [col['field'] for col in columns if col['field'] in df.columns]
            if selected_columns:
                df = df[selected_columns]
            
            return {
                'columns': columns,
                'data': df.to_dict('records'),
                'row_count': len(df)
            }
            
        except Exception as e:
            return {'error': f'Error formatting table data: {str(e)}'}
    
    def _get_filter_widget_data(self, widget: Widget) -> Dict[str, Any]:
        """Get data for filter widget"""
        try:
            config = widget.config
            filter_type = config.get('filter_type', 'dropdown')
            
            if filter_type == 'dropdown':
                # Get distinct values for dropdown
                connection_id = config.get('connection_id')
                field = config.get('field')
                
                if connection_id and field:
                    db_service = ExternalDBService(connection_id)
                    # This would need to be implemented to get distinct values
                    # For now, return placeholder
                    return {
                        'filter_type': filter_type,
                        'options': ['Option 1', 'Option 2', 'Option 3']
                    }
            
            return {'filter_type': filter_type}
            
        except Exception as e:
            return {'error': f'Error getting filter data: {str(e)}'}
    
    def _get_text_widget_data(self, widget: Widget) -> Dict[str, Any]:
        """Get data for text widget"""
        config = widget.config
        return {
            'content': config.get('content', ''),
            'format': config.get('format', 'plain')
        }
    
    def get_dashboard_data(self, dashboard_id: str, user: User, 
                          filters: Dict = None) -> Dict[str, Any]:
        """Get complete dashboard data including all widgets"""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if not self._can_view_dashboard(dashboard, user):
                raise PermissionError("Insufficient permissions to view dashboard")
            
            # Get all widgets
            widgets = Widget.objects.filter(dashboard=dashboard).order_by('created_at')
            
            widget_data = []
            for widget in widgets:
                try:
                    data = self.get_widget_data(str(widget.id), user, filters)
                    widget_data.append({
                        'id': str(widget.id),
                        'type': widget.type,
                        'title': widget.title,
                        'position': widget.position,
                        'data': data.get('data'),
                        'config': widget.config,
                        'last_updated': data.get('last_updated')
                    })
                except Exception as e:
                    logger.error(f"Error getting data for widget {widget.id}: {e}")
                    widget_data.append({
                        'id': str(widget.id),
                        'type': widget.type,
                        'title': widget.title,
                        'position': widget.position,
                        'error': str(e)
                    })
            
            return {
                'dashboard': {
                    'id': str(dashboard.id),
                    'title': dashboard.title,
                    'description': dashboard.description,
                    'config': dashboard.config,
                    'last_updated': dashboard.updated_at.isoformat()
                },
                'widgets': widget_data,
                'permissions': self._get_user_dashboard_permissions(dashboard, user)
            }
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error getting dashboard data: {e}")
            raise
    
    def _can_view_dashboard(self, dashboard: Dashboard, user: User) -> bool:
        """Check if user can view dashboard"""
        if dashboard.owner == user:
            return True
        
        if dashboard.is_public:
            return True
        
        if user.user_type in ['Admin', 'Moderator']:
            return True
        
        # Check sharing permissions
        return DashboardShare.objects.filter(
            dashboard=dashboard, user=user
        ).exists()
    
    def _can_edit_dashboard(self, dashboard: Dashboard, user: User) -> bool:
        """Check if user can edit dashboard"""
        if dashboard.owner == user:
            return True
        
        if user.user_type in ['Admin', 'Moderator']:
            return True
        
        # Check edit permissions through sharing
        return DashboardShare.objects.filter(
            dashboard=dashboard, user=user, permission='edit'
        ).exists()
    
    def _get_user_dashboard_permissions(self, dashboard: Dashboard, user: User) -> Dict[str, bool]:
        """Get user permissions for dashboard"""
        return {
            'can_view': self._can_view_dashboard(dashboard, user),
            'can_edit': self._can_edit_dashboard(dashboard, user),
            'can_delete': dashboard.owner == user or user.user_type == 'Admin',
            'can_share': self._can_edit_dashboard(dashboard, user),
            'is_owner': dashboard.owner == user
        }
    
    def _invalidate_widget_cache(self, widget_id: str):
        """Invalidate cache for widget"""
        # This would invalidate all cache entries for the widget
        # In a real implementation, you might use cache tagging
        cache_pattern = f"widget_data_{widget_id}_*"
        # Django doesn't have built-in cache pattern deletion
        # You might use django-cache-machine or implement custom solution
        pass
    
    def refresh_dashboard_data(self, dashboard_id: str, user: User) -> Dict[str, Any]:
        """Refresh all widget data in dashboard"""
        try:
            dashboard = Dashboard.objects.get(id=dashboard_id)
            
            # Check permissions
            if not self._can_view_dashboard(dashboard, user):
                raise PermissionError("Insufficient permissions to refresh dashboard")
            
            # Get all widgets and refresh their data
            widgets = Widget.objects.filter(dashboard=dashboard)
            refreshed_widgets = []
            errors = []
            
            for widget in widgets:
                try:
                    # Clear cache for widget
                    self._invalidate_widget_cache(str(widget.id))
                    
                    # Get fresh data
                    data = self.get_widget_data(str(widget.id), user)
                    refreshed_widgets.append(str(widget.id))
                    
                except Exception as e:
                    logger.error(f"Error refreshing widget {widget.id}: {e}")
                    errors.append({
                        'widget_id': str(widget.id),
                        'widget_title': widget.title,
                        'error': str(e)
                    })
            
            # Send notification about refresh
            success = len(errors) == 0
            notification_service.notify_dashboard_refresh(
                dashboard, success, 
                error_message=f"{len(errors)} widgets failed" if errors else None,
                affected_widgets=refreshed_widgets
            )
            
            # Log refresh
            log_user_action(
                user, 'refresh_dashboard', 'dashboard', str(dashboard.id),
                f'Refreshed dashboard data',
                {
                    'widgets_refreshed': len(refreshed_widgets),
                    'errors': len(errors)
                }
            )
            
            return {
                'success': success,
                'refreshed_widgets': refreshed_widgets,
                'errors': errors,
                'timestamp': timezone.now().isoformat()
            }
            
        except Dashboard.DoesNotExist:
            raise ValueError(f"Dashboard {dashboard_id} not found")
        except Exception as e:
            logger.error(f"Error refreshing dashboard: {e}")
            raise
    
    def get_widget_types(self) -> List[Dict[str, Any]]:
        """Get available widget types and their configurations"""
        return [
            {
                'type': widget_type,
                'name': config['name'],
                'description': config['description'],
                'data_required': config['data_required'],
                'config_schema': config['config_schema']
            }
            for widget_type, config in self.widget_types.items()
        ]


# Global dashboard service instance
dashboard_service = DashboardService()