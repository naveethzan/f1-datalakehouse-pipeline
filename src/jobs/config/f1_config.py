"""
Unified F1 Pipeline Configuration
Single source of truth for all F1 pipeline settings

This file consolidates all configuration from:
- SparkConfigManager (393 lines) -> Simplified to essential settings
- IcebergConfigManager (278 lines) -> Simplified to essential settings  
- SimpleGlueConfig (nested class) -> Extracted to simple config
"""

# S3 Configuration
S3_CONFIG = {
    'bucket': 'f1-data-lake-naveeth',
    'region': 'us-east-1',
    'bronze_prefix': 'bronze/',
    'silver_prefix': 'silver/',
    'gold_prefix': 'gold/',
    'warehouse_prefix': 'warehouse/',
    'temp_prefix': 'temp/',
    'spark_logs_prefix': 'spark-logs/'
}

# Database Configuration
DATABASE_CONFIG = {
    'silver': 'f1_silver_db',
    'gold': 'f1_gold_db'
}

# Table Configuration - Essential settings only
TABLE_CONFIG = {
    'sessions_silver': {
        'partitioning': 'year, grand_prix_name',
        'file_size': '128MB',
        'target_records': 500
    },
    'drivers_silver': {
        'partitioning': None,
        'file_size': '64MB',
        'target_records': 25
    },
    'qualifying_results_silver': {
        'partitioning': 'year, grand_prix_name',
        'file_size': '128MB',
        'target_records': 500
    },
    'race_results_silver': {
        'partitioning': 'year, grand_prix_name',
        'file_size': '128MB',
        'target_records': 500
    },
    'laps_silver': {
        'partitioning': 'year, grand_prix_name',
        'file_size': '256MB',
        'target_records': 40000
    },
    'pitstops_silver': {
        'partitioning': 'year, grand_prix_name',
        'file_size': '128MB',
        'target_records': 1000
    },
    # Gold tables
    'driver_performance_summary_qualifying': {
        'partitioning': 'grand_prix_name',
        'file_size': '128MB',
        'target_records': 500
    },
    'driver_performance_summary_race': {
        'partitioning': 'grand_prix_name',
        'file_size': '128MB',
        'target_records': 500
    },
    'championship_tracker': {
        'partitioning': 'round_number',
        'file_size': '128MB',
        'target_records': 500
    },
    'team_strategy_analysis': {
        'partitioning': 'team_name',
        'file_size': '128MB',
        'target_records': 250
    },
    'race_weekend_insights': {
        'partitioning': 'grand_prix_name',
        'file_size': '64MB',
        'target_records': 25
    }
}

# Spark Configuration - Essential settings only
SPARK_CONFIG = {
    # Adaptive Query Execution - Critical for F1 data skew
    'adaptive_enabled': True,
    'shuffle_partitions': 200,
    'advisory_partition_size': '128MB',
    'serializer': 'org.apache.spark.serializer.KryoSerializer',
    'arrow_enabled': True,
    'arrow_batch_size': 10000,
    
    # Gold layer specific optimizations
    'gold_shuffle_partitions': 400,
    'gold_broadcast_threshold': '50MB',
    'gold_advisory_partition_size': '256MB'
}

# Iceberg Configuration - Essential settings only
ICEBERG_CONFIG = {
    'format_version': '2',
    'compression': 'zstd',
    'target_file_size': '128MB',
    'snapshot_retention_days': 7,
    'metadata_compression': 'gzip',
    'row_group_size': '128MB',
    'page_size': '1MB',
    'dict_size': '2MB'
}

# Glue Configuration - Essential settings only
GLUE_CONFIG = {
    'environment': 'glue',
    'target_year': 2024,
    'region': 'us-east-1',
    'enable_spark_ui': True,
    'enable_metrics': True,
    'enable_continuous_log': True,
    'enable_datacatalog': True,
    'enable_auto_scaling': False,
    'max_capacity': 10,
    'worker_type': 'G.2X'
}

# AWS Services Configuration - Essential settings only
AWS_SERVICES_CONFIG = {
    'region': 'us-east-1',
    'glue_database': 'f1_silver_db'
}

# Parquet Configuration - Essential settings only
PARQUET_CONFIG = {
    'compression': 'snappy',
    'row_group_size': 50000
}

# Lineage Configuration - Essential settings only
LINEAGE_CONFIG = {
    'enabled': True,
    's3_prefix': 'lineage/',
    'producer': 'f1-data-pipeline/glue'
}

# Data Validation Configuration
VALIDATION_CONFIG = {
    'batch_size': 10000,
    'timeout_seconds': 300,
    'enabled': True
}

# Unified Configuration
F1_CONFIG = {
    's3': S3_CONFIG,
    'databases': DATABASE_CONFIG,
    'tables': TABLE_CONFIG,
    'spark': SPARK_CONFIG,
    'iceberg': ICEBERG_CONFIG,
    'glue': GLUE_CONFIG,
    'aws_services': AWS_SERVICES_CONFIG,
    'parquet': PARQUET_CONFIG,
    'lineage': LINEAGE_CONFIG,
    'validation': VALIDATION_CONFIG
}

def get_config(key_path: str, default=None):
    """
    Get configuration value by key path (e.g., 's3.bucket', 'tables.sessions_silver.partitioning')
    
    Args:
        key_path: Dot-separated path to configuration value
        default: Default value if key not found
        
    Returns:
        Configuration value or default
    """
    keys = key_path.split('.')
    value = F1_CONFIG
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default
    return value

def get_table_config(table_name: str) -> dict:
    """
    Get complete configuration for a specific table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary with table configuration
    """
    return get_config(f'tables.{table_name}', {})

def get_spark_configs_for_layer(layer: str = 'silver') -> dict:
    """
    Get Spark configurations optimized for specific layer
    
    Args:
        layer: Data layer ('silver' or 'gold')
        
    Returns:
        Dictionary of Spark configurations
    """
    base_configs = SPARK_CONFIG.copy()
    
    if layer == 'gold':
        # Gold layer optimizations
        base_configs['shuffle_partitions'] = base_configs['gold_shuffle_partitions']
        base_configs['broadcast_threshold'] = base_configs['gold_broadcast_threshold']
        base_configs['advisory_partition_size'] = base_configs['gold_advisory_partition_size']
    
    # Remove gold-specific keys
    base_configs.pop('gold_shuffle_partitions', None)
    base_configs.pop('gold_broadcast_threshold', None)
    base_configs.pop('gold_advisory_partition_size', None)
    
    return base_configs

def get_iceberg_properties(table_name: str) -> dict:
    """
    Get Iceberg table properties for a specific table
    
    Args:
        table_name: Name of the table
        
    Returns:
        Dictionary of Iceberg table properties
    """
    table_config = get_table_config(table_name)
    file_size = table_config.get('file_size', '128MB')
    
    # Convert file size to bytes
    size_mapping = {
        '64MB': 67108864,
        '128MB': 134217728,
        '256MB': 268435456
    }
    target_file_size = size_mapping.get(file_size, 134217728)
    
    properties = ICEBERG_CONFIG.copy()
    properties['write.target-file-size-bytes'] = str(target_file_size)
    
    return properties

def get_s3_paths() -> dict:
    """
    Get all S3 paths for the pipeline
    
    Returns:
        Dictionary of S3 paths
    """
    bucket = S3_CONFIG['bucket']
    return {
        'bronze': f"s3://{bucket}/{S3_CONFIG['bronze_prefix']}",
        'silver': f"s3://{bucket}/{S3_CONFIG['silver_prefix']}",
        'gold': f"s3://{bucket}/{S3_CONFIG['gold_prefix']}",
        'warehouse': f"s3://{bucket}/{S3_CONFIG['warehouse_prefix']}",
        'temp': f"s3://{bucket}/{S3_CONFIG['temp_prefix']}",
        'spark_logs': f"s3://{bucket}/{S3_CONFIG['spark_logs_prefix']}"
    }

def get_glue_job_arguments(layer: str = 'silver') -> dict:
    """
    Get Glue job arguments for a specific layer
    
    Args:
        layer: Data layer ('silver' or 'gold')
        
    Returns:
        Dictionary of Glue job arguments
    """
    spark_configs = get_spark_configs_for_layer(layer)
    s3_paths = get_s3_paths()
    
    # Convert Spark configs to Glue format
    conf_pairs = [f"{key}={value}" for key, value in spark_configs.items()]
    conf_string = ' '.join(conf_pairs)
    
    return {
        '--conf': conf_string,
        '--enable-spark-ui': str(GLUE_CONFIG['enable_spark_ui']).lower(),
        '--enable-metrics': str(GLUE_CONFIG['enable_metrics']).lower(),
        '--enable-continuous-cloudwatch-log': str(GLUE_CONFIG['enable_continuous_log']).lower(),
        '--enable-glue-datacatalog': str(GLUE_CONFIG['enable_datacatalog']).lower(),
        '--spark-event-logs-path': f"{s3_paths['spark_logs']}{layer}/",
        '--TempDir': f"{s3_paths['temp']}{layer}/",
        '--enable-auto-scaling': str(GLUE_CONFIG['enable_auto_scaling']).lower(),
        '--max-capacity': str(GLUE_CONFIG['max_capacity']),
        '--worker-type': GLUE_CONFIG['worker_type']
    }

# Helper functions to match existing config manager interface
def get_silver_config() -> dict:
    """Get Silver layer configuration"""
    return {
        'database_name': DATABASE_CONFIG['silver']
    }

def get_gold_config() -> dict:
    """Get Gold layer configuration"""
    return {
        'database_name': DATABASE_CONFIG['gold']
    }

def get_aws_services_config() -> dict:
    """Get AWS services configuration"""
    return AWS_SERVICES_CONFIG

def get_s3_config() -> dict:
    """Get S3 configuration"""
    return S3_CONFIG

def get_cloudwatch_config() -> dict:
    """Get CloudWatch configuration"""
    return {
        'namespace': 'F1Pipeline',
        'environment_dimension': 'glue'
    }

def get_parquet_config() -> dict:
    """Get Parquet configuration"""
    return PARQUET_CONFIG

def get_lineage_config() -> dict:
    """Get Lineage configuration"""
    return LINEAGE_CONFIG

def get_iceberg_config() -> dict:
    """Get Iceberg configuration"""
    return ICEBERG_CONFIG

def get_task_config(task_name: str) -> dict:  # pylint: disable=unused-argument
    """Get task-specific configuration"""
    return {}

def reload() -> None:
    """Reload configuration (no-op for static config)"""
    # No-op for static configuration
