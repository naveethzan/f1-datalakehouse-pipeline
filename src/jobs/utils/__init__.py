"""
F1 Data Pipeline Utilities - Organized by Function
"""

# Core transformation functions - Most commonly used
from src.jobs.utils.transformations.common import (
    normalize_grand_prix_name,
    standardize_team_name,
    calculate_time_millis,
    add_audit_columns,
    calculate_race_points,
    standardize_race_status,
    write_to_iceberg
)

# SCD processing
from src.jobs.utils.transformations.scd import (
    process_incremental_scd,
    detect_driver_team_changes,
    generate_scd_records_from_changes,
    build_scd2_records,
    get_drivers_schema
)

# Table management
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

# Data access patterns
from src.jobs.utils.data_access.bronze_readers import (
    read_full_year_drivers,
    read_drivers_for_gp,
    read_full_year_sessions,
    read_sessions_for_gp,
    read_bronze_data_by_mode,
    validate_bronze_data
)

from src.jobs.utils.data_access.silver_readers import (
    validate_silver_data
)

# Analytics & business logic
from src.jobs.utils.analytics.business_logic import F1BusinessLogic

__all__ = [
    # Core transformation functions
    'normalize_grand_prix_name',
    'standardize_team_name', 
    'calculate_time_millis',
    'add_audit_columns',
    'calculate_race_points',
    'standardize_race_status',
    'write_to_iceberg',
    
    # SCD functions
    'process_incremental_scd',
    'detect_driver_team_changes',
    'generate_scd_records_from_changes',
    'build_scd2_records',
    'get_drivers_schema',
    
    # Table management
    'IcebergTableManager',
    'get_ddl_columns',
    'get_table_schema',
    'get_partitions',
    'get_all_table_names',
    'SILVER_SCHEMAS',
    'get_gold_table_names',
    'generate_gold_table_ddl',
    
    # Data access
    'read_full_year_drivers',
    'read_drivers_for_gp',
    'read_full_year_sessions',
    'read_sessions_for_gp',
    'read_bronze_data_by_mode',
    'validate_bronze_data',
    'validate_silver_data',
    
    # Analytics
    'F1BusinessLogic'
]
