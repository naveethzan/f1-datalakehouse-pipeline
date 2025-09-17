"""
Core Transformation Layer - Common Functions and SCD Processing
"""

from src.jobs.utils.transformations.common import (
    normalize_grand_prix_name,
    standardize_team_name,
    calculate_time_millis,
    add_audit_columns,
    calculate_race_points,
    standardize_race_status,
    write_to_iceberg
)

from src.jobs.utils.transformations.scd import (
    process_incremental_scd,
    detect_driver_team_changes,
    generate_scd_records_from_changes,
    build_scd2_records,
    get_drivers_schema
)

__all__ = [
    'normalize_grand_prix_name',
    'standardize_team_name',
    'calculate_time_millis',
    'add_audit_columns',
    'calculate_race_points',
    'standardize_race_status',
    'write_to_iceberg',
    'process_incremental_scd',
    'detect_driver_team_changes',
    'generate_scd_records_from_changes',
    'build_scd2_records',
    'get_drivers_schema'
]