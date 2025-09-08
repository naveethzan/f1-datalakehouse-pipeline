"""
Configuration modules for F1 Bronze to Silver transformation jobs.

This package contains all configuration-related modules including:
- Unified F1 pipeline configuration (NEW - Phase 1)
- Spark session configuration (Legacy - will be removed in Phase 2)
- Iceberg table configurations (Legacy - will be removed in Phase 2)
- Unified Silver and Gold table schema definitions
"""

# New unified configuration (Phase 1)
from .f1_config import (
    F1_CONFIG, get_config, get_table_config, get_spark_configs_for_layer, 
    get_iceberg_properties, get_s3_paths, get_glue_job_arguments,
    get_silver_config, get_gold_config, get_aws_services_config, get_s3_config,
    get_cloudwatch_config, get_parquet_config, get_lineage_config, 
    get_iceberg_config, get_task_config, reload
)

# Schema definitions (Keep)
from .schemas import SilverTableSchemas, GoldTableSchemas

# Legacy imports removed - using unified configuration

__all__ = [
    # New unified configuration
    'F1_CONFIG',
    'get_config',
    'get_table_config',
    'get_spark_configs_for_layer',
    'get_iceberg_properties',
    'get_s3_paths',
    'get_glue_job_arguments',
    # Helper functions for backward compatibility
    'get_silver_config',
    'get_gold_config',
    'get_aws_services_config',
    'get_s3_config',
    'get_cloudwatch_config',
    'get_parquet_config',
    'get_lineage_config',
    'get_iceberg_config',
    'get_task_config',
    'reload',
    # Schema definitions
    'SilverTableSchemas',
    'GoldTableSchemas'
]