"""
Shared Parquet settings for F1 data pipeline.

This module contains consolidated Parquet settings used across both
DAGs and jobs for consistent data storage and processing.
"""

# Consolidated Parquet settings for F1 data characteristics
F1_PARQUET_SETTINGS = {
    'compression': 'snappy',
    'engine': 'pyarrow',
    'index': False,
    'row_group_size': 50000,
    'use_dictionary': True,
    'compression_level': None,  # Use default for snappy
    'write_statistics': True,
    'coerce_timestamps': 'ms',  # Millisecond precision for timestamps
    'allow_truncated_timestamps': True
}
