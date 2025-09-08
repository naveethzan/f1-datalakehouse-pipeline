"""
Common utility functions for F1 Data Pipeline.

This module provides shared utility functions used across the pipeline.
Only contains functions that are actually used to keep the codebase simple.
"""

import logging
import pandas as pd
import numpy as np
from typing import Any, Dict, List


def get_logger(name: str = "f1_pipeline") -> logging.Logger:
    """
    Get logger instance with consistent configuration.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def clean_dataframe_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame for optimal Parquet writing.
    
    Performs data type optimization and cleaning operations to ensure
    the DataFrame is ready for efficient Parquet storage.
    
    Args:
        df: Input DataFrame to clean
        
    Returns:
        Cleaned DataFrame optimized for Parquet storage
    """
    if df.empty:
        return df
    
    df_cleaned = df.copy()
    
    # Handle infinite values
    df_cleaned = df_cleaned.replace([np.inf, -np.inf], np.nan)
    
    # Convert object columns to appropriate types
    for col in df_cleaned.columns:
        if df_cleaned[col].dtype == 'object':
            # Try to convert to numeric if possible
            try:
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='ignore')
            except:
                pass
    
    return df_cleaned


def validate_dataframe_schema(df: pd.DataFrame, expected_columns: List[str]) -> Dict[str, Any]:
    """
    Validate DataFrame schema against expected columns.
    
    Args:
        df: DataFrame to validate
        expected_columns: List of expected column names
        
    Returns:
        Validation results dictionary
    """
    validation_result = {
        'valid': True,
        'missing_columns': [],
        'extra_columns': [],
        'column_count': len(df.columns),
        'expected_count': len(expected_columns)
    }
    
    # Check for missing columns
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        validation_result['valid'] = False
        validation_result['missing_columns'] = list(missing_columns)
    
    # Check for extra columns
    extra_columns = set(df.columns) - set(expected_columns)
    if extra_columns:
        validation_result['extra_columns'] = list(extra_columns)
    
    return validation_result


def format_bytes(bytes_value: int) -> str:
    """
    Format bytes value into human-readable string.
    
    Args:
        bytes_value: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB", "500 KB")
    """
    if bytes_value == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while bytes_value >= 1024 and i < len(size_names) - 1:
        bytes_value /= 1024.0
        i += 1
    
    return f"{bytes_value:.1f} {size_names[i]}"