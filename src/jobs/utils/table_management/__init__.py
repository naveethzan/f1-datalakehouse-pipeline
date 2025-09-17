"""
Table Management Layer - Iceberg Tables and Schemas
"""

from src.jobs.utils.table_management.iceberg_manager import IcebergTableManager
from src.jobs.utils.table_management.schemas import (
    get_ddl_columns,
    get_table_schema,
    get_partitions,
    get_all_table_names,
    SILVER_SCHEMAS,
    get_gold_table_names,
    generate_gold_table_ddl
)

__all__ = [
    'IcebergTableManager',
    'get_ddl_columns',
    'get_table_schema',
    'get_partitions',
    'get_all_table_names',
    'SILVER_SCHEMAS',
    'get_gold_table_names',
    'generate_gold_table_ddl'
]