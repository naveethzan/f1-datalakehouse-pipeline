"""
Silver Data Readers Module

This module provides reusable functions for reading data from the Silver layer
for Gold transformations. It implements intelligent broadcast decisions, 
partition pruning, and Iceberg direct reading for optimal performance.

Key Features:
- Centralized Silver data reading patterns
- Context-aware broadcast join decisions  
- Partition pruning (historical vs incremental modes)
- Column selection for performance optimization
- Iceberg direct reading with proper catalog support
- Consistent error handling and logging

Processing Modes:
- 'historical': Full year processing (all GPs for a year)
- 'incremental': Single GP processing (after each race weekend)

Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.
"""

import logging
from typing import Optional, List, Dict, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, broadcast

# Setup logging
logger = logging.getLogger(__name__)


# =============================================================================
# Broadcast Decision Logic
# =============================================================================

def should_use_broadcast(
    table_name: str, 
    processing_mode: str, 
    record_count: Optional[int] = None
) -> bool:
    """
    Determine whether to broadcast a Silver table based on context-aware rules.
    
    Broadcast Strategy:
    - Always broadcast: drivers_silver (~30), sessions_silver (~58), pitstops_silver (~75)
    - Context-aware: qualifying/race results (depends on historical vs incremental)
    - Never broadcast: laps_silver (~20,000)
    
    Args:
        table_name: Silver table name
        processing_mode: 'historical' or 'incremental'
        record_count: Optional record count for dynamic decisions
        
    Returns:
        True if table should be broadcast, False otherwise
    """
    # Always broadcast small master/lookup tables
    always_broadcast = {
        'drivers_silver',     # ~30 records
        'sessions_silver',    # ~58 records per year  
        'pitstops_silver'     # ~75 records per year
    }
    
    if table_name in always_broadcast:
        logger.info(f"🔄 Broadcasting {table_name} (always broadcast - small table)")
        return True
    
    # Never broadcast large tables
    never_broadcast = {
        'laps_silver'  # ~20,000 records per year
    }
    
    if table_name in never_broadcast:
        logger.info(f"⚡ Not broadcasting {table_name} (too large)")
        return False
    
    # Context-aware broadcast for medium tables
    context_aware_tables = {
        'qualifying_results_silver',  # ~500/year, ~20/GP
        'race_results_silver'         # ~500/year, ~20/GP
    }
    
    if table_name in context_aware_tables:
        if processing_mode == 'incremental':
            logger.info(f"🔄 Broadcasting {table_name} (incremental mode - small partition)")
            return True
        else:
            logger.info(f"⚡ Not broadcasting {table_name} (historical mode - large dataset)")
            return False
    
    # Default: don't broadcast unknown tables
    logger.info(f"⚡ Not broadcasting {table_name} (unknown table - safe default)")
    return False


def apply_broadcast_hint(df: DataFrame, table_name: str, processing_mode: str) -> DataFrame:
    """
    Apply broadcast hint to DataFrame based on broadcast decision logic.
    
    Args:
        df: DataFrame to potentially broadcast
        table_name: Silver table name
        processing_mode: 'historical' or 'incremental'
        
    Returns:
        DataFrame with broadcast hint applied if appropriate
    """
    if should_use_broadcast(table_name, processing_mode):
        return broadcast(df)
    return df


# =============================================================================
# Silver Table Readers
# =============================================================================

def read_silver_table(
    spark: SparkSession,
    table_name: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    columns: Optional[List[str]] = None,
    processing_mode: str = "historical",
    catalog_name: str = "glue_catalog",
    database_name: str = "f1_silver_db"
) -> DataFrame:
    """
    Read Silver table with partition pruning, column selection, and broadcast optimization.
    
    Args:
        spark: SparkSession
        table_name: Silver table name (e.g., 'sessions_silver')
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter for incremental processing
        columns: Optional list of columns to select (performance optimization)
        processing_mode: 'historical' or 'incremental' 
        catalog_name: Iceberg catalog name
        database_name: Silver database name
        
    Returns:
        DataFrame with Silver data, potentially with broadcast hint
        
    Raises:
        Exception: If table read fails
    """
    full_table_name = f"{catalog_name}.{database_name}.{table_name}"
    
    # Log read operation
    mode_desc = f"{processing_mode} mode"
    if grand_prix_filter:
        mode_desc += f" (GP: {grand_prix_filter})"
    logger.info(f"📖 Reading {table_name} - {mode_desc}")
    
    try:
        # Read from Iceberg table with partition pruning
        df = spark.table(full_table_name)
        
        # Apply partition filters
        df = _apply_partition_filters(df, table_name, year_filter, grand_prix_filter)
        
        # Apply column selection if specified
        if columns:
            # Ensure partition columns are included
            partition_cols = _get_partition_columns(table_name)
            all_columns = list(set(columns + partition_cols))
            df = df.select(*all_columns)
            logger.info(f"📊 Selected {len(columns)} columns (+{len(partition_cols)} partition cols)")
        
        # Count records for logging and broadcast decisions
        record_count = df.count()
        logger.info(f"📊 Loaded {record_count} records from {table_name}")
        
        # Apply broadcast hint based on context
        df = apply_broadcast_hint(df, table_name, processing_mode)
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Silver table {full_table_name}: {e}")
        raise


def _apply_partition_filters(
    df: DataFrame, 
    table_name: str, 
    year_filter: int, 
    grand_prix_filter: Optional[str]
) -> DataFrame:
    """
    Apply partition filters based on table partitioning strategy.
    
    Args:
        df: DataFrame to filter
        table_name: Silver table name
        year_filter: Year filter
        grand_prix_filter: Optional Grand Prix filter
        
    Returns:
        Filtered DataFrame with partition pruning applied
    """
    from jobs.utils.schemas import get_partitions
    
    partitions = get_partitions(table_name)
    
    # Apply year filter (all Silver tables have year partition)
    df = df.filter(col("year") == year_filter)
    logger.info(f"🔍 Applied year filter: {year_filter}")
    
    # Apply Grand Prix filter if specified and table supports it
    if grand_prix_filter and 'grand_prix_name' in partitions:
        df = df.filter(col("grand_prix_name") == grand_prix_filter)
        logger.info(f"🔍 Applied Grand Prix filter: {grand_prix_filter}")
    elif grand_prix_filter and 'grand_prix_name' not in partitions:
        logger.warning(f"⚠️ {table_name} doesn't support Grand Prix filtering")
    
    return df


def _get_partition_columns(table_name: str) -> List[str]:
    """
    Get partition columns for a Silver table.
    
    Args:
        table_name: Silver table name
        
    Returns:
        List of partition column names
    """
    from jobs.utils.schemas import get_partitions
    
    try:
        return get_partitions(table_name)
    except Exception:
        # Default partition columns for Silver tables
        return ['year']


# =============================================================================
# Convenience Readers for Specific Tables
# =============================================================================

def read_drivers_silver(
    spark: SparkSession,
    year_filter: int,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read drivers_silver table (always broadcast - small master data).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        processing_mode: Processing mode (doesn't affect drivers - always broadcast)
        columns: Optional column selection
        
    Returns:
        Drivers DataFrame with broadcast hint applied
    """
    # Default columns for drivers if not specified
    if columns is None:
        columns = [
            'driver_number', 'broadcast_name', 'full_name', 'team_name',
            'country_code', 'name_acronym', 'is_current'
        ]
    
    return read_silver_table(
        spark=spark,
        table_name='drivers_silver',
        year_filter=year_filter,
        columns=columns,
        processing_mode=processing_mode
    )


def read_sessions_silver(
    spark: SparkSession,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read sessions_silver table (always broadcast - small lookup data).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: Processing mode (doesn't affect sessions - always broadcast)
        columns: Optional column selection
        
    Returns:
        Sessions DataFrame with broadcast hint applied
    """
    # Default columns for sessions if not specified
    if columns is None:
        columns = [
            'session_key', 'session_type', 'session_name', 'meeting_key',
            'grand_prix_name', 'date_start', 'date_end', 'year'
        ]
    
    return read_silver_table(
        spark=spark,
        table_name='sessions_silver',
        year_filter=year_filter,
        grand_prix_filter=grand_prix_filter,
        columns=columns,
        processing_mode=processing_mode
    )


def read_qualifying_results_silver(
    spark: SparkSession,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read qualifying_results_silver table (context-aware broadcast).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: 'historical' (no broadcast) or 'incremental' (broadcast)
        columns: Optional column selection
        
    Returns:
        Qualifying results DataFrame with context-aware broadcast
    """
    return read_silver_table(
        spark=spark,
        table_name='qualifying_results_silver',
        year_filter=year_filter,
        grand_prix_filter=grand_prix_filter,
        columns=columns,
        processing_mode=processing_mode
    )


def read_race_results_silver(
    spark: SparkSession,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read race_results_silver table (context-aware broadcast).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: 'historical' (no broadcast) or 'incremental' (broadcast)
        columns: Optional column selection
        
    Returns:
        Race results DataFrame with context-aware broadcast
    """
    return read_silver_table(
        spark=spark,
        table_name='race_results_silver',
        year_filter=year_filter,
        grand_prix_filter=grand_prix_filter,
        columns=columns,
        processing_mode=processing_mode
    )


def read_laps_silver(
    spark: SparkSession,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read laps_silver table (never broadcast - large dataset).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: Processing mode (doesn't affect laps - never broadcast)
        columns: Optional column selection
        
    Returns:
        Laps DataFrame without broadcast hint
    """
    return read_silver_table(
        spark=spark,
        table_name='laps_silver',
        year_filter=year_filter,
        grand_prix_filter=grand_prix_filter,
        columns=columns,
        processing_mode=processing_mode
    )


def read_pitstops_silver(
    spark: SparkSession,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical",
    columns: Optional[List[str]] = None
) -> DataFrame:
    """
    Read pitstops_silver table (always broadcast - small dataset).
    
    Args:
        spark: SparkSession
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: Processing mode (doesn't affect pitstops - always broadcast)
        columns: Optional column selection
        
    Returns:
        Pitstops DataFrame with broadcast hint applied
    """
    return read_silver_table(
        spark=spark,
        table_name='pitstops_silver',
        year_filter=year_filter,
        grand_prix_filter=grand_prix_filter,
        columns=columns,
        processing_mode=processing_mode
    )


# =============================================================================
# Multi-Table Readers for Transforms
# =============================================================================

def read_silver_tables_for_transform(
    spark: SparkSession,
    table_specs: Dict[str, Dict[str, Any]],
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: str = "historical"
) -> Dict[str, DataFrame]:
    """
    Read multiple Silver tables for a Gold transform with optimized settings.
    
    Args:
        spark: SparkSession
        table_specs: Dict mapping table names to their read specifications
            Format: {
                'table_name': {
                    'columns': ['col1', 'col2'],  # Optional
                    'alias': 'custom_alias'       # Optional
                }
            }
        year_filter: Year to process
        grand_prix_filter: Optional Grand Prix filter
        processing_mode: 'historical' or 'incremental'
        
    Returns:
        Dict mapping table names (or aliases) to DataFrames
        
    Example:
        table_specs = {
            'drivers_silver': {
                'columns': ['driver_number', 'broadcast_name', 'team_name'],
                'alias': 'drivers'
            },
            'sessions_silver': {
                'columns': ['session_key', 'session_type', 'grand_prix_name']
            }
        }
    """
    result = {}
    
    for table_name, spec in table_specs.items():
        columns = spec.get('columns')
        alias = spec.get('alias', table_name)
        
        df = read_silver_table(
            spark=spark,
            table_name=table_name,
            year_filter=year_filter,
            grand_prix_filter=grand_prix_filter,
            columns=columns,
            processing_mode=processing_mode
        )
        
        result[alias] = df
    
    return result


# =============================================================================
# Validation and Utility Functions
# =============================================================================

def validate_silver_data(
    df: DataFrame, 
    table_name: str, 
    year_filter: int, 
    grand_prix_filter: Optional[str] = None
) -> bool:
    """
    Validate Silver data after reading to ensure it meets basic requirements.
    
    Args:
        df: DataFrame to validate
        table_name: Silver table name
        year_filter: Expected year
        grand_prix_filter: Expected Grand Prix (if applicable)
        
    Returns:
        True if validation passes, False otherwise
    """
    if df.count() == 0:
        logger.warning(f"⚠️ No {table_name} data found for year {year_filter}" + 
                      (f", GP {grand_prix_filter}" if grand_prix_filter else ""))
        return False
    
    # Check for required columns based on table type
    required_columns = {
        'drivers_silver': ['driver_number', 'broadcast_name', 'team_name'],
        'sessions_silver': ['session_key', 'session_type', 'grand_prix_name'],
        'qualifying_results_silver': ['session_key', 'driver_number', 'position'],
        'race_results_silver': ['session_key', 'driver_number', 'position', 'points'],
        'laps_silver': ['session_key', 'driver_number', 'lap_number'],
        'pitstops_silver': ['session_key', 'driver_number', 'lap_number']
    }
    
    if table_name in required_columns:
        missing_columns = [col for col in required_columns[table_name] if col not in df.columns]
        if missing_columns:
            logger.error(f"❌ Missing required columns in {table_name}: {missing_columns}")
            return False
    
    logger.info(f"✅ Silver {table_name} data validation passed")
    return True


def get_silver_table_stats(df: DataFrame, table_name: str) -> Dict[str, Any]:
    """
    Get basic statistics for a Silver DataFrame.
    
    Args:
        df: DataFrame to analyze
        table_name: Silver table name
        
    Returns:
        Dict with basic statistics
    """
    record_count = df.count()
    column_count = len(df.columns)
    
    stats = {
        'table_name': table_name,
        'record_count': record_count,
        'column_count': column_count,
        'columns': df.columns
    }
    
    # Add table-specific stats
    if 'year' in df.columns:
        years = [row.year for row in df.select('year').distinct().collect()]
        stats['years'] = sorted(years)
    
    if 'grand_prix_name' in df.columns:
        gps = [row.grand_prix_name for row in df.select('grand_prix_name').distinct().collect()]
        stats['grand_prix_count'] = len(gps)
    
    return stats