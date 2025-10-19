"""
Transformation Engine Service for Django MIS Application

This module handles data transformation operations using pandas.
Converted from Flask transformation engine to Django service.
"""

import pandas as pd
from pandas.api.types import is_numeric_dtype
import uuid
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class TransformationEngine:
    """
    Applies a series of data preparation steps (a recipe) to a pandas DataFrame.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.original_df = df.copy()
        self.df = df.copy()
        self.history = []
    
    def apply_recipe(self, recipe: list):
        """
        Apply a sequence of transformation steps to the DataFrame.
        """
        for step in recipe:
            if not step.get('column') or step['column'] not in self.df.columns:
                logger.warning(f"Skipping step, column {step.get('column')} not in DataFrame.")
                continue
            
            strategy = step.get('strategy')
            method_to_call = self._get_strategy_method(strategy)
            
            if callable(method_to_call):
                method_to_call(step)
            else:
                raise ValueError(f"Unknown strategy: {strategy}")
            
            # Log the transformation
            self.history.append({
                'id': step.get('id', str(uuid.uuid4())),
                'strategy': strategy,
                'column': step.get('column'),
                'timestamp': datetime.utcnow().isoformat(),
            })
        
        return self.df.copy()
    
    def fill_missing(self, step):
        """
        Fill missing values in a column
        """
        col = step['column']
        params = step.get('params', {})
        method = params.get('method', 'mean').lower()
        fill_value = None
        
        # Check if column can be treated as numeric
        is_numeric = is_numeric_dtype(self.df[col])
        can_be_numeric = False
        
        if not is_numeric:
            try:
                test_conversion = pd.to_numeric(self.df[col], errors='coerce')
                valid_conversions = test_conversion.notna().sum()
                if valid_conversions / len(self.df[col]) > 0.8:  # 80% of values can be converted
                    can_be_numeric = True
            except:
                can_be_numeric = False
        
        is_effectively_numeric = is_numeric or can_be_numeric
        
        if method == 'mean':
            if is_effectively_numeric:
                if can_be_numeric:
                    numeric_col = pd.to_numeric(self.df[col], errors='coerce')
                    fill_value = numeric_col.mean()
                else:
                    fill_value = self.df[col].mean()
            else:
                method = 'mode'  # Fallback to mode for non-numeric
                logger.info(f"Column {col} is not numeric. Using mode instead of mean.")
        
        elif method == 'median':
            if is_effectively_numeric:
                if can_be_numeric:
                    numeric_col = pd.to_numeric(self.df[col], errors='coerce')
                    fill_value = numeric_col.median()
                else:
                    fill_value = self.df[col].median()
            else:
                method = 'mode'  # Fallback to mode for non-numeric
                logger.info(f"Column {col} is not numeric. Using mode instead of median.")
        
        elif method == 'mode':
            if not self.df[col].mode().empty:
                fill_value = self.df[col].mode(dropna=True).iloc[0]
        
        elif method == 'custom':
            fill_value = params.get('value')
        
        elif method == 'zero':
            if is_effectively_numeric:
                fill_value = 0
            else:
                logger.warning(f"Skipping fill with zero on non-numeric column {col}.")
                return
        
        if fill_value is not None:
            self.df[col] = self.df[col].fillna(fill_value)
    
    def remove_duplicates(self, step):
        """
        Remove duplicate rows
        """
        params = step.get('params', {})
        columns = params.get('columns')
        
        if columns and all(c in self.df.columns for c in columns):
            self.df = self.df.drop_duplicates(subset=columns)
        else:
            self.df = self.df.drop_duplicates()
    
    def cast_type(self, step):
        """
        Change column data type
        """
        col = step['column']
        params = step.get('params', {})
        new_type = (params.get('new_type') or params.get('to_type', '')).lower()
        
        if not new_type:
            return
        
        if new_type in ['int', 'integer']:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce').astype('Int64')
        elif new_type in ['float', 'double', 'numeric', 'decimal']:
            self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
        elif new_type in ['str', 'string', 'text']:
            self.df[col] = self.df[col].astype(str).fillna('')
        elif new_type in ['datetime', 'timestamp']:
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        elif new_type in ['bool', 'boolean']:
            self.df[col] = self.df[col].astype(bool)
    
    def find_replace(self, step):
        """
        Find and replace values in a column
        """
        col = step['column']
        params = step.get('params', {})
        find_value = params.get('find')
        replace_value = params.get('replace', '')
        
        if find_value is not None:
            self.df[col] = self.df[col].astype(str).str.replace(
                str(find_value), 
                str(replace_value), 
                regex=False
            )
    
    def split_column(self, step):
        """
        Split a column into multiple columns
        """
        col = step['column']
        params = step.get('params', {})
        delimiter = params.get('delimiter', ',')
        new_cols = params.get('new_columns', [])
        max_split = params.get('max_split', -1)
        
        if not new_cols:
            return
        
        # Split the column
        splits = self.df[col].str.split(delimiter, n=max_split, expand=True)
        
        # Add new columns
        for i, new_col in enumerate(new_cols):
            if i < splits.shape[1]:
                self.df[new_col] = splits[i]
            else:
                self.df[new_col] = None
    
    def merge_columns(self, step):
        """
        Merge multiple columns into one
        """
        params = step.get('params', {})
        columns = params.get('columns', [])
        new_col = params.get('new_column')
        delimiter = params.get('delimiter', ' ')
        
        if not columns or not new_col:
            return
        
        # Filter columns that exist
        existing_cols = [col for col in columns if col in self.df.columns]
        
        if existing_cols:
            self.df[new_col] = self.df[existing_cols].astype(str).agg(delimiter.join, axis=1)
    
    def handle_outliers(self, step):
        """
        Handle outliers in numeric columns
        """
        col = step['column']
        params = step.get('params', {})
        method = params.get('method', 'iqr').lower()
        
        if not is_numeric_dtype(self.df[col]):
            logger.warning(f"Cannot handle outliers for non-numeric column {col}")
            return
        
        if method == 'iqr':
            Q1 = self.df[col].quantile(0.25)
            Q3 = self.df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            action = params.get('action', 'remove')
            if action == 'remove':
                self.df = self.df[(self.df[col] >= lower_bound) & (self.df[col] <= upper_bound)]
            elif action == 'cap':
                self.df[col] = self.df[col].clip(lower=lower_bound, upper=upper_bound)
        
        elif method == 'zscore':
            z_scores = abs((self.df[col] - self.df[col].mean()) / self.df[col].std())
            threshold = params.get('threshold', 3)
            
            action = params.get('action', 'remove')
            if action == 'remove':
                self.df = self.df[z_scores <= threshold]
            elif action == 'cap':
                mask = z_scores > threshold
                median_val = self.df[col].median()
                self.df.loc[mask, col] = median_val
    
    def encode_categorical(self, step):
        """
        Encode categorical variables
        """
        col = step['column']
        params = step.get('params', {})
        method = params.get('method', 'label').lower()
        
        if method == 'label':
            # Label encoding
            unique_values = self.df[col].unique()
            label_map = {val: i for i, val in enumerate(unique_values) if pd.notna(val)}
            self.df[col] = self.df[col].map(label_map)
        
        elif method == 'onehot':
            # One-hot encoding
            prefix = params.get('prefix', col)
            dummy_cols = pd.get_dummies(self.df[col], prefix=prefix, dummy_na=False)
            self.df = pd.concat([self.df, dummy_cols], axis=1)
            # Optionally drop the original column
            if params.get('drop_original', True):
                self.df = self.df.drop(columns=[col])
    
    def normalize_text(self, step):
        """
        Normalize text in a column
        """
        col = step['column']
        params = step.get('params', {})
        operations = params.get('operations', [])
        
        for operation in operations:
            if operation == 'lowercase':
                self.df[col] = self.df[col].str.lower()
            elif operation == 'uppercase':
                self.df[col] = self.df[col].str.upper()
            elif operation == 'title':
                self.df[col] = self.df[col].str.title()
            elif operation == 'strip':
                self.df[col] = self.df[col].str.strip()
            elif operation == 'remove_spaces':
                self.df[col] = self.df[col].str.replace(' ', '', regex=False)
            elif operation == 'remove_punctuation':
                import string
                self.df[col] = self.df[col].str.translate(str.maketrans('', '', string.punctuation))
    
    def create_date_features(self, step):
        """
        Extract date features from datetime column
        """
        col = step['column']
        params = step.get('params', {})
        features = params.get('features', [])
        
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(self.df[col]):
            self.df[col] = pd.to_datetime(self.df[col], errors='coerce')
        
        dt_col = self.df[col].dt
        
        for feature in features:
            if feature == 'year':
                self.df[f'{col}_year'] = dt_col.year
            elif feature == 'month':
                self.df[f'{col}_month'] = dt_col.month
            elif feature == 'day':
                self.df[f'{col}_day'] = dt_col.day
            elif feature == 'weekday':
                self.df[f'{col}_weekday'] = dt_col.weekday
            elif feature == 'quarter':
                self.df[f'{col}_quarter'] = dt_col.quarter
            elif feature == 'dayofyear':
                self.df[f'{col}_dayofyear'] = dt_col.dayofyear
    
    def _get_strategy_method(self, strategy_name):
        """
        Maps a strategy name from the recipe to a class method.
        """
        methods = {
            'fill_missing': self.fill_missing,
            'handle_missing': self.fill_missing,  # Alias
            'remove_duplicates': self.remove_duplicates,
            'cast_type': self.cast_type,
            'change_type': self.cast_type,  # Alias
            'find_replace': self.find_replace,
            'split_column': self.split_column,
            'merge_columns': self.merge_columns,
            'handle_outliers': self.handle_outliers,
            'encode_categorical': self.encode_categorical,
            'normalize_text': self.normalize_text,
            'create_date_features': self.create_date_features,
        }
        
        return methods.get(strategy_name)
    
    def get_transformation_summary(self):
        """
        Get a summary of all transformations applied
        """
        summary = {
            'total_steps': len(self.history),
            'original_shape': self.original_df.shape,
            'final_shape': self.df.shape,
            'transformations': self.history,
            'columns_added': list(set(self.df.columns) - set(self.original_df.columns)),
            'columns_removed': list(set(self.original_df.columns) - set(self.df.columns)),
        }
        
        return summary
    
    def reset(self):
        """
        Reset the DataFrame to its original state
        """
        self.df = self.original_df.copy()
        self.history = []
        return self.df
    
    def get_step_preview(self, step_index: int):
        """
        Get a preview of the data after applying steps up to step_index
        """
        if step_index < 0 or step_index >= len(self.history):
            return None
        
        # Apply steps up to step_index
        temp_engine = TransformationEngine(self.original_df)
        for i in range(step_index + 1):
            step = {
                'strategy': self.history[i]['strategy'],
                'column': self.history[i]['column'],
                'params': {}  # Would need to store params in history for full functionality
            }
            method = temp_engine._get_strategy_method(step['strategy'])
            if callable(method):
                method(step)
        
        return temp_engine.df