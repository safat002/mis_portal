# transformation_engine.py - Django version of Flask transformation engine
# This is the Django port of your Flask transformation engine

import pandas as pd
import numpy as np
import re
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class TransformationEngine:
    """
    Django version of the Flask transformation engine for data preparation.
    Handles data cleaning, transformation, and preparation tasks.
    """
    
    def __init__(self, dataframe):
        """Initialize with a pandas DataFrame"""
        self.df = dataframe.copy() if dataframe is not None else pd.DataFrame()
        self.original_df = dataframe.copy() if dataframe is not None else pd.DataFrame()
        self.history = []
    
    def apply_recipe(self, recipe):
        """Apply a list of transformation steps to the dataframe"""
        if not recipe:
            return self.df
        
        for step in recipe:
            try:
                self._apply_single_step(step)
            except Exception as e:
                logger.warning(f"Failed to apply transformation step {step}: {e}")
                self.history.append({
                    'step': step,
                    'status': 'failed',
                    'error': str(e)
                })
        
        return self.df
    
    def _apply_single_step(self, step):
        """Apply a single transformation step"""
        strategy = step.get('strategy') or step.get('action')
        column = step.get('column')
        params = step.get('params', {})
        
        if not strategy or not column:
            return
        
        # >>> CHANGE: only enforce "must exist" for strategies that need an existing column
        needs_existing = strategy not in ['calculate', 'rename_column', 'duplicate_column']
        if needs_existing and column not in self.df.columns:
            logger.warning(f"Column '{column}' not found in dataframe")
            return
        
        # Record step start
        step_record = {
            'step': step,
            'status': 'started',
            'timestamp': datetime.now()
        }
        
        try:
            if strategy in ['fill', 'handle_missing']:
                self._handle_missing_values(column, params)
            elif strategy in ['cast', 'change_type']:
                self._change_column_type(column, params)
            elif strategy == 'remove_outliers':
                self._remove_outliers(column, params)
            elif strategy == 'normalize':
                self._normalize_column(column, params)
            elif strategy == 'standardize':
                self._standardize_column(column, params)
            elif strategy == 'binning':
                self._bin_column(column, params)
            elif strategy == 'date_extract':
                self._extract_date_components(column, params)
            elif strategy == 'text_clean':
                self._clean_text(column, params)
            elif strategy == 'regex_extract':
                self._regex_extract(column, params)
            elif strategy == 'calculate':
                self._calculate_column(column, params)
            elif strategy == 'drop_column':
                self._drop_column(column)
            elif strategy == 'rename_column':
                self._rename_column(column, params)
            elif strategy == 'duplicate_column':
                self._duplicate_column(column, params)
            else:
                logger.warning(f"Unknown transformation strategy: {strategy}")
            
            step_record['status'] = 'completed'
            
        except Exception as e:
            step_record['status'] = 'failed'
            step_record['error'] = str(e)
            raise e
        finally:
            self.history.append(step_record)
    
    def _handle_missing_values(self, column, params):
        """Handle missing values in a column"""
        method = params.get('method', 'mean').lower()
        
        if method == 'mean':
            if self.df[column].dtype in ['int64', 'float64']:
                fill_value = self.df[column].mean()
                self.df[column] = self.df[column].fillna(fill_value)
        elif method == 'median':
            if self.df[column].dtype in ['int64', 'float64']:
                fill_value = self.df[column].median()
                self.df[column] = self.df[column].fillna(fill_value)
        elif method == 'mode':
            mode_values = self.df[column].mode()
            if not mode_values.empty:
                self.df[column] = self.df[column].fillna(mode_values.iloc[0])
        elif method == 'zero':
            self.df[column] = self.df[column].fillna(0)
        elif method == 'forward_fill':
            self.df[column] = self.df[column].fillna(method='ffill')
        elif method == 'backward_fill':
            self.df[column] = self.df[column].fillna(method='bfill')
        elif method == 'custom':
            custom_value = params.get('value', 0)
            self.df[column] = self.df[column].fillna(custom_value)
        elif method == 'drop':
            self.df = self.df.dropna(subset=[column])
    
    def _change_column_type(self, column, params):
        """Change the data type of a column"""
        to_type = params.get('to_type') or params.get('new_type', '').lower()
        
        if to_type in ['int', 'integer']:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce').astype('Int64')
        elif to_type in ['float', 'double', 'numeric', 'decimal']:
            self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        elif to_type in ['str', 'string', 'text']:
            self.df[column] = self.df[column].astype(str).fillna('')
        elif to_type == 'datetime':
            self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
        elif to_type == 'category':
            self.df[column] = self.df[column].astype('category')
        elif to_type == 'bool':
            self.df[column] = self.df[column].astype(bool)
    
    def _remove_outliers(self, column, params):
        """Remove outliers from a numeric column"""
        if self.df[column].dtype not in ['int64', 'float64']:
            return
        
        method = params.get('method', 'iqr').lower()
        
        if method == 'iqr':
            Q1 = self.df[column].quantile(0.25)
            Q3 = self.df[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            mask = (self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)
        elif method == 'zscore':
            threshold = params.get('threshold', 3)
            zscore = np.abs((self.df[column] - self.df[column].mean()) / self.df[column].std())
            mask = zscore <= threshold
        elif method == 'percentile':
            lower_percentile = params.get('lower_percentile', 1)
            upper_percentile = params.get('upper_percentile', 99)
            lower_bound = self.df[column].quantile(lower_percentile / 100)
            upper_bound = self.df[column].quantile(upper_percentile / 100)
            mask = (self.df[column] >= lower_bound) & (self.df[column] <= upper_bound)
        
        action = params.get('action', 'remove').lower()
        if action == 'remove':
            self.df = self.df[mask]
        elif action == 'cap':
            if method == 'iqr':
                self.df[column] = self.df[column].clip(lower=lower_bound, upper=upper_bound)
    
    def _normalize_column(self, column, params):
        """Normalize a numeric column to 0-1 range"""
        if self.df[column].dtype not in ['int64', 'float64']:
            return
        
        min_val = self.df[column].min()
        max_val = self.df[column].max()
        
        if max_val != min_val:
            self.df[column] = (self.df[column] - min_val) / (max_val - min_val)
    
    def _standardize_column(self, column, params):
        """Standardize a numeric column (z-score normalization)"""
        if self.df[column].dtype not in ['int64', 'float64']:
            return
        
        mean_val = self.df[column].mean()
        std_val = self.df[column].std()
        
        if std_val != 0:
            self.df[column] = (self.df[column] - mean_val) / std_val
    
    def _bin_column(self, column, params):
        """Bin a numeric column into categories"""
        if self.df[column].dtype not in ['int64', 'float64']:
            return
        
        bins = params.get('bins', 5)
        labels = params.get('labels')
        
        if isinstance(bins, int):
            binned = pd.cut(self.df[column], bins=bins, labels=labels)
        else:  # bins is a list of bin edges
            binned = pd.cut(self.df[column], bins=bins, labels=labels)
        
        new_column_name = params.get('new_column', f'{column}_binned')
        self.df[new_column_name] = binned
    
    def _extract_date_components(self, column, params):
        """Extract components from a datetime column"""
        if self.df[column].dtype != 'datetime64[ns]':
            # Try to convert to datetime first
            self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
        
        components = params.get('components', ['year', 'month', 'day'])
        
        for component in components:
            if component == 'year':
                self.df[f'{column}_year'] = self.df[column].dt.year
            elif component == 'month':
                self.df[f'{column}_month'] = self.df[column].dt.month
            elif component == 'day':
                self.df[f'{column}_day'] = self.df[column].dt.day
            elif component == 'weekday':
                self.df[f'{column}_weekday'] = self.df[column].dt.day_name()
            elif component == 'quarter':
                self.df[f'{column}_quarter'] = self.df[column].dt.quarter
            elif component == 'week':
                self.df[f'{column}_week'] = self.df[column].dt.isocalendar().week
    
    def _clean_text(self, column, params):
        """Clean text in a column"""
        if self.df[column].dtype != 'object':
            return
        
        operations = params.get('operations', ['strip', 'lower'])
        
        for operation in operations:
            if operation == 'strip':
                self.df[column] = self.df[column].str.strip()
            elif operation == 'lower':
                self.df[column] = self.df[column].str.lower()
            elif operation == 'upper':
                self.df[column] = self.df[column].str.upper()
            elif operation == 'title':
                self.df[column] = self.df[column].str.title()
            elif operation == 'remove_digits':
                self.df[column] = self.df[column].str.replace(r'\d+', '', regex=True)
            elif operation == 'remove_special_chars':
                self.df[column] = self.df[column].str.replace(r'[^a-zA-Z0-9\s]', '', regex=True)
            elif operation == 'remove_extra_spaces':
                self.df[column] = self.df[column].str.replace(r'\s+', ' ', regex=True)
    
    def _regex_extract(self, column, params):
        """Extract text using regular expressions"""
        if self.df[column].dtype != 'object':
            return
        
        pattern = params.get('pattern', '')
        new_column = params.get('new_column', f'{column}_extracted')
        
        if pattern:
            self.df[new_column] = self.df[column].str.extract(pattern, expand=False)
    
    def _calculate_column(self, column, params):
        """Create a calculated column based on a formula"""
        formula = params.get('formula', '')
        new_column = params.get('new_column', column)
        
        if formula:
            safe_formula = self._prepare_formula(formula)
            try:
                safe_globals = {
                    '__builtins__': {}, 'np': np, 'pd': pd,
                    'abs': abs, 'max': max, 'min': min,
                    'sum': sum, 'round': round
                }
                safe_locals = {'df': self.df}
                
                result = eval(safe_formula, safe_globals, safe_locals)
                self.df[new_column] = result
            except Exception as e:
                logger.error(f"Error evaluating formula '{formula}': {e}")
                raise
    
    def _prepare_formula(self, formula):
        """Prepare formula for safe evaluation"""
        # Replace column references like [column_name] with df['column_name']
        pattern = r'\[([a-zA-Z_][a-zA-Z0-9_]*)\]'
        formula = re.sub(pattern, r"df['\1']", formula)
        return formula
    
    def _drop_column(self, column):
        """Drop a column from the dataframe"""
        if column in self.df.columns:
            self.df = self.df.drop(columns=[column])
    
    def _rename_column(self, column, params):
        """Rename a column"""
        new_name = params.get('new_name', column)
        if column in self.df.columns and new_name != column:
            self.df = self.df.rename(columns={column: new_name})
    
    def _duplicate_column(self, column, params):
        """Duplicate a column with a new name"""
        new_name = params.get('new_name', f'{column}_copy')
        if column in self.df.columns:
            self.df[new_name] = self.df[column].copy()
    
    def get_column_profile(self, column):
        """Get statistical profile of a column"""
        if column not in self.df.columns:
            return None
        
        profile = {
            'column': column,
            'dtype': str(self.df[column].dtype),
            'total_count': len(self.df[column]),
            'null_count': self.df[column].isnull().sum(),
            'unique_count': self.df[column].nunique(),
        }
        
        # Add numeric statistics for numeric columns
        if self.df[column].dtype in ['int64', 'float64']:
            profile.update({
                'mean': self.df[column].mean(),
                'median': self.df[column].median(),
                'std': self.df[column].std(),
                'min': self.df[column].min(),
                'max': self.df[column].max(),
                'q25': self.df[column].quantile(0.25),
                'q75': self.df[column].quantile(0.75),
            })
        
        # Add text statistics for object columns
        elif self.df[column].dtype == 'object':
            profile.update({
                'max_length': self.df[column].astype(str).str.len().max(),
                'min_length': self.df[column].astype(str).str.len().min(),
                'avg_length': self.df[column].astype(str).str.len().mean(),
            })
        
        return profile
    
    def get_transformation_suggestions(self, column):
        """Get suggested transformations for a column"""
        if column not in self.df.columns:
            return []
        
        suggestions = []
        col_data = self.df[column]
        
        # Check for missing values
        if col_data.isnull().sum() > 0:
            suggestions.append({
                'strategy': 'handle_missing',
                'reason': f'Column has {col_data.isnull().sum()} missing values',
                'params': {'method': 'mean' if col_data.dtype in ['int64', 'float64'] else 'mode'}
            })
        
        # Check for outliers in numeric columns
        if col_data.dtype in ['int64', 'float64']:
            Q1 = col_data.quantile(0.25)
            Q3 = col_data.quantile(0.75)
            IQR = Q3 - Q1
            outliers = ((col_data < (Q1 - 1.5 * IQR)) | (col_data > (Q3 + 1.5 * IQR))).sum()
            
            if outliers > 0:
                suggestions.append({
                    'strategy': 'remove_outliers',
                    'reason': f'Column has {outliers} outliers',
                    'params': {'method': 'iqr'}
                })
        
        # Check for high cardinality in categorical columns
        if col_data.dtype == 'object' and col_data.nunique() > len(col_data) * 0.5:
            suggestions.append({
                'strategy': 'text_clean',
                'reason': 'Text column might benefit from cleaning',
                'params': {'operations': ['strip', 'lower']}
            })
        
        return suggestions
    
    def reset_to_original(self):
        """Reset dataframe to original state"""
        self.df = self.original_df.copy()
        self.history = []
        return self.df