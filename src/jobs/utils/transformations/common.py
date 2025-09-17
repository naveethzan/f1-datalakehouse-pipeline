"""
Transformation Utilities for F1 Bronze to Silver Pipeline

This module provides common transformation functions used across all Silver table transformations.
Pure Python functions that can be used with Spark UDFs when needed.

Key Functions:
- normalize_grand_prix_name(): Normalize Grand Prix names for consistent partitioning
- standardize_team_name(): Standardize F1 team names
- calculate_time_millis(): Convert time strings to milliseconds
- add_audit_columns(): Add created/updated timestamps to DataFrames
- calculate_race_points(): F1 points calculation
- standardize_race_status(): Standardize race completion status
"""

import unicodedata
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp


def normalize_grand_prix_name(grand_prix: str) -> str:
    """
    Normalize Grand Prix name for consistent partitioning.
    Matches the Bronze layer partition naming convention.
    
    Examples:
        'Bahrain Grand Prix' -> 'bahrain'
        'São Paulo Grand Prix' -> 'sao_paulo'
        'Emilia Romagna Grand Prix' -> 'emilia_romagna'
        'Saudi Arabian Grand Prix' -> 'saudi_arabian'
    
    Args:
        grand_prix: Grand Prix name to normalize
        
    Returns:
        Normalized grand prix name
    """
    if not grand_prix:
        return ""
    
    # Remove 'Grand Prix' suffix
    normalized = re.sub(r'(\s+|^)grand\s+prix$', '', grand_prix, flags=re.IGNORECASE)
    
    # Handle international characters (São Paulo -> Sao Paulo)
    normalized = unicodedata.normalize('NFD', normalized)
    normalized = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    
    # Convert to lowercase and replace spaces with underscores
    normalized = re.sub(r'[^\w\s]', '', normalized.lower())
    normalized = re.sub(r'\s+', '_', normalized.strip())
    
    return normalized


def standardize_team_name(team_name: str) -> str:
    """
    Standardize F1 team names to consistent short forms.
    
    Examples:
        'Aston Martin Aramco Cognizant F1 Team' -> 'Aston Martin'
        'Scuderia Ferrari' -> 'Ferrari'
        'Oracle Red Bull Racing' -> 'Red Bull Racing'
        'Mercedes-AMG PETRONAS F1 Team' -> 'Mercedes'
    
    Args:
        team_name: Team name to standardize
        
    Returns:
        Standardized team name
    """
    if not team_name:
        return ""
    
    # Team name mappings (2023-2025 teams)
    team_mappings = {
        # Red Bull
        'oracle red bull racing': 'Red Bull Racing',
        'red bull racing': 'Red Bull Racing',
        'red bull': 'Red Bull Racing',
        
        # Ferrari
        'scuderia ferrari': 'Ferrari',
        'ferrari': 'Ferrari',
        
        # Mercedes
        'mercedes-amg petronas': 'Mercedes',
        'mercedes amg petronas': 'Mercedes',
        'mercedes': 'Mercedes',
        
        # McLaren
        'mclaren f1 team': 'McLaren',
        'mclaren': 'McLaren',
        
        # Aston Martin
        'aston martin aramco': 'Aston Martin',
        'aston martin': 'Aston Martin',
        
        # Alpine
        'bwt alpine': 'Alpine',
        'alpine f1 team': 'Alpine',
        'alpine': 'Alpine',
        
        # Williams
        'williams racing': 'Williams',
        'williams': 'Williams',
        
        # AlphaTauri / RB
        'scuderia alphatauri': 'AlphaTauri',
        'alphatauri': 'AlphaTauri',
        'rb f1 team': 'RB',  # 2024 rebrand
        'visa cashapp rb': 'RB',
        
        # Alfa Romeo / Sauber
        'alfa romeo f1 team': 'Alfa Romeo',
        'alfa romeo': 'Alfa Romeo',
        'stake f1 team': 'Sauber',  # 2024 rebrand
        'sauber': 'Sauber',
        
        # Haas
        'moneygram haas': 'Haas',
        'haas f1 team': 'Haas',
        'haas': 'Haas'
    }
    
    # Lowercase for matching
    clean_name = team_name.lower().strip()
    
    # Remove common suffixes
    clean_name = re.sub(r'\s*(f1\s+team|racing|cognizant|aramco|orlen|petronas|stake)\s*', ' ', clean_name)
    clean_name = re.sub(r'\s+', ' ', clean_name).strip()
    
    # Try to match with mapping
    for pattern, standard_name in team_mappings.items():
        if pattern in clean_name:
            return standard_name
    
    # Return cleaned original name if no match found
    # Capitalize first letter of each word
    return ' '.join(word.capitalize() for word in clean_name.split())


def calculate_time_millis(time_str: str) -> Optional[int]:
    """
    Convert time string to milliseconds.
    
    Handles formats:
        - "1:23.456" (MM:SS.mmm) -> milliseconds
        - "23.456" (SS.mmm) -> milliseconds
        - "1:23:45.678" (HH:MM:SS.mmm) -> milliseconds
        - None/empty -> None
    
    Args:
        time_str: Time string to convert
        
    Returns:
        Time in milliseconds or None if invalid
    """
    if not time_str or time_str == "":
        return None
    
    try:
        # Remove any whitespace
        time_str = time_str.strip()
        
        # Handle HH:MM:SS.mmm format
        if time_str.count(':') == 2:
            parts = time_str.split(':')
            hours = int(parts[0])
            minutes = int(parts[1])
            seconds_parts = parts[2].split('.')
            seconds = int(seconds_parts[0])
            millis = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
            
            total_millis = (hours * 3600000) + (minutes * 60000) + (seconds * 1000) + millis
            return total_millis
        
        # Handle MM:SS.mmm format
        elif time_str.count(':') == 1:
            parts = time_str.split(':')
            minutes = int(parts[0])
            seconds_parts = parts[1].split('.')
            seconds = int(seconds_parts[0])
            millis = int(seconds_parts[1]) if len(seconds_parts) > 1 else 0
            
            total_millis = (minutes * 60000) + (seconds * 1000) + millis
            return total_millis
        
        # Handle SS.mmm format
        elif '.' in time_str:
            seconds_parts = time_str.split('.')
            seconds = int(seconds_parts[0])
            millis = int(seconds_parts[1])
            
            total_millis = (seconds * 1000) + millis
            return total_millis
        
        # Handle pure seconds
        else:
            seconds = float(time_str)
            return int(seconds * 1000)
            
    except (ValueError, IndexError, AttributeError):
        return None


def add_audit_columns(df: DataFrame) -> DataFrame:
    """
    Add standard audit columns to a DataFrame.
    
    Adds:
        - created_timestamp: Record creation timestamp
        - updated_timestamp: Record last update timestamp
    
    Args:
        df: DataFrame to add audit columns to
        
    Returns:
        DataFrame with audit columns added
    """
    current_ts = current_timestamp()
    
    return df.withColumn(
        "created_timestamp", current_ts
    ).withColumn(
        "updated_timestamp", current_ts
    )


def calculate_race_points(position: int, fastest_lap: bool = False) -> int:
    """
    Calculate F1 race points based on finish position.
    
    Points system (2022+ regulations):
        1st: 25, 2nd: 18, 3rd: 15, 4th: 12, 5th: 10,
        6th: 8, 7th: 6, 8th: 4, 9th: 2, 10th: 1
        +1 for fastest lap if finishing in top 10
    
    Args:
        position: Finish position (1-20)
        fastest_lap: Whether driver achieved fastest lap
        
    Returns:
        Points earned (0-26)
    """
    # F1 Points system
    points_system = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]
    
    # Base points
    if position <= 0 or position > len(points_system):
        base_points = 0
    else:
        base_points = points_system[position - 1]
    
    # Fastest lap bonus (only if in top 10)
    fastest_lap_points = 0
    if fastest_lap and 1 <= position <= 10:
        fastest_lap_points = 1
    
    return base_points + fastest_lap_points


def standardize_race_status(status: str) -> str:
    """
    Standardize race completion status.
    
    Categories:
        - 'Finished': Completed the race
        - 'DNF': Did Not Finish (mechanical/accident)
        - 'DNS': Did Not Start
        - 'DSQ': Disqualified
        - 'Retired': Retired from race
        - 'Withdrawn': Withdrawn before race
    
    Args:
        status: Raw status string
        
    Returns:
        Standardized status
    """
    if not status:
        return "Unknown"
    
    status_lower = status.lower().strip()
    
    # Finished statuses
    if any(word in status_lower for word in ['finished', 'classified', 'lapped']):
        return 'Finished'
    
    # DNF statuses
    if any(word in status_lower for word in ['dnf', 'accident', 'collision', 'damage', 'crash']):
        return 'DNF'
    
    # Mechanical failures
    if any(word in status_lower for word in ['engine', 'gearbox', 'hydraulic', 'electrical', 
                                             'mechanical', 'power', 'technical']):
        return 'DNF'
    
    # DNS
    if any(word in status_lower for word in ['dns', 'did not start', 'not started']):
        return 'DNS'
    
    # DSQ
    if any(word in status_lower for word in ['dsq', 'disqualified', 'excluded']):
        return 'DSQ'
    
    # Retired
    if 'retired' in status_lower:
        return 'Retired'
    
    # Withdrawn
    if 'withdrawn' in status_lower:
        return 'Withdrawn'
    
    # Default to original if no match
    return status.strip()


def write_to_iceberg(
    df: DataFrame,
    table_name: str,
    write_mode: str = "append",
    catalog_name: str = "glue_catalog",
    database_name: str = "f1_silver_db",
    spark: "SparkSession" = None
) -> None:
    """
    Enhanced utility to write DataFrames to Iceberg tables with comprehensive write mode support.
    
    Supports write modes:
    - 'append': Add new records
    - 'overwrite': Replace entire table
    - 'overwritePartitions': Replace matching partitions (generic)
    - 'overwrite_partitions_year': Replace year-level partitions (HISTORICAL mode)
    - 'overwrite_partitions_year_gp': Replace year+GP partitions (INCREMENTAL mode)
    - 'merge_scd2': SCD Type 2 merge (closes old records, inserts new ones)
    
    Args:
        df: DataFrame to write
        table_name: Target table name (e.g., 'qualifying_results_silver')
        write_mode: Write mode (see above)
        catalog_name: Glue catalog name (default: 'glue_catalog')
        database_name: Target database (default: 'f1_silver_db')
        spark: SparkSession (required for merge_scd2 mode)
        
    Raises:
        Exception: If write operation fails
    """
    import logging
    from pyspark.sql.functions import col, lit, when, current_timestamp
    logger = logging.getLogger(__name__)
    
    full_table_name = f"{catalog_name}.{database_name}.{table_name}"
    record_count = df.count()
    
    # Simple validation
    if write_mode == "merge_scd2" and spark is None:
        raise ValueError("SparkSession is required for merge_scd2 mode")
    
    logger.info(f"💾 Writing to Iceberg table: {full_table_name}")
    logger.info(f"   Mode: {write_mode}")
    logger.info(f"   Records: {record_count}")
    
    try:
        if write_mode == "overwrite":
            # Complete overwrite
            df.writeTo(full_table_name).overwrite()
            
        elif write_mode == "overwritePartitions":
            # Generic overwrite matching partitions
            df.writeTo(full_table_name).overwritePartitions()
            
        elif write_mode == "overwrite_partitions_year":
            # Year-level partition overwrite (HISTORICAL mode)
            # Used by: sessions_silver, qualifying_results_silver, race_results_silver, laps_silver, pitstops_silver
            # Iceberg automatically detects year partition from DataFrame and overwrites matching partitions
            logger.info(f"🗓️ Overwriting year-level partitions for {table_name}")
            df.writeTo(full_table_name).overwritePartitions()
            
        elif write_mode == "overwrite_partitions_year_gp":
            # Year+GP partition overwrite (INCREMENTAL mode)
            # Used by: sessions_silver, qualifying_results_silver, race_results_silver, laps_silver, pitstops_silver
            # Iceberg automatically detects year+grand_prix_name partitions from DataFrame
            logger.info(f"🏁 Overwriting year+GP partitions for {table_name}")
            df.writeTo(full_table_name).overwritePartitions()
            
        elif write_mode == "append":
            # Append new data
            df.writeTo(full_table_name).append()
            
        elif write_mode == "merge_scd2":
            # SCD Type 2 merge logic
            if spark is None:
                raise ValueError("SparkSession is required for merge_scd2 mode")
                
            if record_count == 0:
                logger.info("📝 No SCD changes to merge - skipping")
                return
                
            logger.info(f"🔄 Performing SCD Type 2 merge for {record_count} changed records")
            
            # Perform SCD merge operation
            perform_scd2_merge(spark, df, full_table_name, logger)
            
        else:
            # This should never happen due to validation above, but just in case
            raise ValueError(f"Unsupported write mode: {write_mode}")
            
        logger.info(f"✅ Successfully wrote {record_count} records to {table_name} (mode: {write_mode})")
        
    except Exception as e:
        logger.error(f"❌ Failed to write to {full_table_name}: {e}")
        raise


def perform_scd2_merge(spark: "SparkSession", new_records: DataFrame, target_table: str, logger) -> None:
    """
    Perform SCD Type 2 merge operation with comprehensive error handling.
    
    Enhanced Process:
    1. Validate input data and table access
    2. Close existing current records for affected entities (set valid_to, is_current=False)
    3. Insert new SCD records as current records
    4. Validate merge completion
    
    Args:
        spark: SparkSession
        new_records: DataFrame with new SCD records to insert
        target_table: Full table name (catalog.database.table)
        logger: Logger instance
        
    Raises:
        ValueError: If input validation fails
        RuntimeError: If merge operation fails
    """
    from pyspark.sql.functions import col, lit, when, current_timestamp
    
    # Simple validation
    if new_records.count() == 0:
        logger.info("📝 No SCD records to merge - skipping")
        return
    
    # Get affected driver numbers
    affected_drivers = [row.driver_number for row in new_records.select("driver_number").distinct().collect()]
    logger.info(f"🔄 SCD Merge: Processing {len(affected_drivers)} drivers")
    
    try:
        # Step 1: Close existing current records for affected drivers
        close_existing_sql = f"""
            UPDATE {target_table}
            SET valid_to = current_timestamp(),
                is_current = false,
                updated_timestamp = current_timestamp()
            WHERE driver_number IN ({','.join(map(str, affected_drivers))})
              AND is_current = true
        """
        
        logger.info("🔄 Closing existing current records...")
        spark.sql(close_existing_sql)
        
        # Step 2: Insert new SCD records
        logger.info("🔄 Inserting new SCD records...")
        new_records.writeTo(target_table).append()
        
        logger.info(f"✅ SCD Type 2 merge completed for {len(affected_drivers)} drivers")
        
    except Exception as e:
        logger.error(f"❌ SCD Type 2 merge failed: {e}")
        raise


# =============================================================================
# Drivers Transform Specific Utilities
# =============================================================================

def standardize_driver_team_names(df: DataFrame) -> DataFrame:
    """
    Apply team name standardization using our utility function.
    
    Args:
        df: DataFrame with team_name column
        
    Returns:
        DataFrame with standardized team names
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    # Create UDF for team name standardization
    standardize_team_udf = udf(standardize_team_name, StringType())
    
    return df.withColumn(
        "team_name_std",
        standardize_team_udf(col("team_name"))
    )


def join_with_session_dates(drivers_df: DataFrame, sessions_df: DataFrame) -> DataFrame:
    """
    Join drivers with sessions to get date context for each appearance.
    
    Args:
        drivers_df: Bronze drivers DataFrame
        sessions_df: Bronze sessions DataFrame
        
    Returns:
        Drivers DataFrame with session dates and types
    """
    # Join on session_key
    joined_df = drivers_df.join(
        sessions_df,
        on="session_key",
        how="inner"
    )
    
    # Filter to only qualifying and race sessions
    filtered_df = joined_df.filter(
        col("session_type").isin(["qualifying", "race"])
    )
    
    return filtered_df


def add_total_races(scd2_df: DataFrame, full_drivers_df: DataFrame) -> DataFrame:
    """
    Calculate total races per driver across all teams.
    
    Args:
        scd2_df: SCD Type 2 records
        full_drivers_df: Full drivers data with session info
        
    Returns:
        DataFrame with total_races added
    """
    from pyspark.sql.functions import countDistinct
    
    # Calculate total races per driver (only race sessions)
    races_per_driver = full_drivers_df.filter(
        col("session_type") == "race"
    ).groupBy(
        "driver_number"
    ).agg(
        countDistinct("session_key").alias("total_races")
    )
    
    # Join back to SCD2 records
    result = scd2_df.join(
        races_per_driver,
        on="driver_number",
        how="left"
    )
    
    # Fill nulls with 0 for drivers with no races
    result = result.fillna({"total_races": 0})
    
    return result


def select_final_columns(df: DataFrame) -> DataFrame:
    """
    Select and order final columns for Silver layer.
    
    Args:
        df: DataFrame with all fields
        
    Returns:
        DataFrame with final column selection
    """
    return df.select(
        col("driver_number").cast("int"),
        col("broadcast_name").cast("string"),
        col("full_name").cast("string"),
        col("team_name").cast("string"),
        col("country_code").cast("string"),
        col("team_colour").cast("string"),
        col("name_acronym").cast("string"),
        col("total_races").cast("int"),
        col("valid_from").cast("timestamp"),
        col("valid_to").cast("timestamp"),
        col("is_current").cast("boolean")
    )


# =============================================================================
# Gold Layer Enhancement Utilities
# =============================================================================

def get_partition_filters_for_processing_mode(
    processing_mode: str,
    year: Optional[int] = None,
    grand_prix_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get partition filters for optimal Silver data reading based on processing mode.
    
    Args:
        processing_mode: 'historical' or 'incremental'
        year: Required for both modes
        grand_prix_name: Required for incremental mode
        
    Returns:
        Dict with partition filters for Silver readers
    """
    if processing_mode == 'historical':
        if year is None:
            raise ValueError("Year is required for historical processing mode")
        return {'year': year}
    
    elif processing_mode == 'incremental':
        if year is None or grand_prix_name is None:
            raise ValueError("Year and grand_prix_name are required for incremental processing mode")
        return {'year': year, 'grand_prix_name': grand_prix_name}
    
    else:
        raise ValueError(f"Unsupported processing mode: {processing_mode}. Use 'historical' or 'incremental'")


def validate_gold_schema_compatibility(df: DataFrame, expected_schema: Dict[str, str]) -> List[str]:
    """
    Validate DataFrame schema against expected Gold table schema.
    
    Args:
        df: DataFrame to validate
        expected_schema: Dict mapping column names to expected types
        
    Returns:
        List of validation errors (empty if valid)
    """
    errors = []
    
    # Get actual schema as dict
    actual_schema = {field.name: str(field.dataType) for field in df.schema.fields}
    
    # Check for missing columns
    missing_cols = set(expected_schema.keys()) - set(actual_schema.keys())
    if missing_cols:
        errors.append(f"Missing columns: {sorted(missing_cols)}")
    
    # Check for unexpected columns
    extra_cols = set(actual_schema.keys()) - set(expected_schema.keys())
    if extra_cols:
        errors.append(f"Unexpected columns: {sorted(extra_cols)}")
    
    # Check for type mismatches (for common columns)
    common_cols = set(expected_schema.keys()) & set(actual_schema.keys())
    for col in common_cols:
        if actual_schema[col] != expected_schema[col]:
            errors.append(
                f"Type mismatch for {col}: expected {expected_schema[col]}, got {actual_schema[col]}"
            )
    
    return errors


def apply_gold_transformations(
    df: DataFrame,
    transform_functions: List[callable],
    processing_mode: str,
    table_name: str
) -> DataFrame:
    """
    Apply a series of transformation functions to create Gold layer data.
    
    Args:
        df: Input DataFrame
        transform_functions: List of transformation functions to apply
        processing_mode: 'historical' or 'incremental'
        table_name: Name of target Gold table (for logging)
        
    Returns:
        Transformed DataFrame ready for Gold layer
    """
    import logging
    logger = logging.getLogger(__name__)
    
    result_df = df
    
    logger.info(f"🔧 Applying {len(transform_functions)} transformations for {table_name} ({processing_mode} mode)")
    
    for i, transform_func in enumerate(transform_functions, 1):
        func_name = getattr(transform_func, '__name__', f'transform_{i}')
        
        try:
            logger.info(f"   {i}/{len(transform_functions)}: Applying {func_name}")
            result_df = transform_func(result_df)
            record_count = result_df.count()
            logger.info(f"   ✅ {func_name} completed ({record_count:,} records)")
            
        except Exception as e:
            logger.error(f"   ❌ {func_name} failed: {e}")
            raise RuntimeError(f"Gold transformation failed at step {i} ({func_name}): {e}")
    
    final_count = result_df.count()
    logger.info(f"🏆 Gold transformations completed for {table_name}: {final_count:,} records")
    
    return result_df


def add_gold_metadata_columns(
    df: DataFrame,
    processing_mode: str,
    year: int,
    grand_prix_name: Optional[str] = None
) -> DataFrame:
    """
    Add standard Gold layer metadata columns.
    
    Args:
        df: Input DataFrame
        processing_mode: 'historical' or 'incremental'
        year: Processing year
        grand_prix_name: Grand Prix name (for incremental mode)
        
    Returns:
        DataFrame with Gold metadata columns
    """
    from pyspark.sql.functions import lit, current_timestamp
    
    result_df = df.withColumn(
        "processing_mode", lit(processing_mode)
    ).withColumn(
        "processing_year", lit(year)
    ).withColumn(
        "processed_timestamp", current_timestamp()
    )
    
    # Add GP-specific metadata for incremental processing
    if processing_mode == 'incremental' and grand_prix_name:
        result_df = result_df.withColumn(
            "processing_grand_prix", lit(grand_prix_name)
        )
    
    return result_df


def calculate_gold_statistics(
    df: DataFrame,
    table_name: str,
    group_by_cols: List[str] = None
) -> Dict[str, Any]:
    """
    Calculate statistics for Gold layer data quality monitoring.
    
    Args:
        df: Gold DataFrame to analyze
        table_name: Table name for context
        group_by_cols: Optional columns to group statistics by
        
    Returns:
        Dict with statistics
    """
    import logging
    logger = logging.getLogger(__name__)
    
    stats = {
        'table_name': table_name,
        'total_records': df.count(),
        'column_count': len(df.columns)
    }
    
    # Basic null checks for key columns
    null_counts = {}
    for col in df.columns:
        null_count = df.filter(df[col].isNull()).count()
        if null_count > 0:
            null_counts[col] = null_count
    
    if null_counts:
        stats['null_counts'] = null_counts
    
    # Group-by statistics if requested
    if group_by_cols:
        try:
            group_stats = df.groupBy(*group_by_cols).count().collect()
            stats['group_statistics'] = [
                {col: row[col] for col in group_by_cols + ['count']}
                for row in group_stats
            ]
        except Exception as e:
            logger.warning(f"Failed to calculate group statistics: {e}")
    
    # Log key statistics
    logger.info(f"📊 Gold Statistics - {table_name}:")
    logger.info(f"   📈 Total Records: {stats['total_records']:,}")
    logger.info(f"   📋 Columns: {stats['column_count']}")
    
    if null_counts:
        logger.info(f"   ⚠️ Null Counts: {dict(list(null_counts.items())[:3])}..." if len(null_counts) > 3 else f"   ⚠️ Null Counts: {null_counts}")
    
    return stats


def optimize_gold_dataframe(
    df: DataFrame,
    optimization_level: str = "standard"
) -> DataFrame:
    """
    Apply Gold layer optimizations to DataFrame.
    
    Args:
        df: DataFrame to optimize
        optimization_level: 'light', 'standard', or 'aggressive'
        
    Returns:
        Optimized DataFrame
    """
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"⚡ Applying {optimization_level} optimizations to Gold DataFrame")
    
    result_df = df
    
    if optimization_level in ['standard', 'aggressive']:
        # Cache for multiple actions
        result_df = result_df.cache()
        logger.info("   📦 DataFrame cached")
    
    if optimization_level == 'aggressive':
        # Repartition for better distribution
        partition_count = max(1, result_df.rdd.getNumPartitions() // 2)
        result_df = result_df.repartition(partition_count)
        logger.info(f"   🔄 Repartitioned to {partition_count} partitions")
    
    return result_df


def write_gold_table(
    df: DataFrame,
    table_name: str,
    processing_mode: str,
    year: int,
    grand_prix_name: Optional[str] = None,
    database_name: str = "f1_gold_db"
) -> None:
    """
    Write DataFrame to Gold Iceberg table with appropriate write mode.
    
    Args:
        df: DataFrame to write
        table_name: Gold table name
        processing_mode: 'historical' or 'incremental'
        year: Processing year
        grand_prix_name: Grand Prix name (for incremental)
        database_name: Gold database name (default: f1_gold_db)
    """
    import logging
    logger = logging.getLogger(__name__)
    
    # Determine appropriate write mode
    if processing_mode == 'historical':
        write_mode = 'overwrite'  # Full year replacement
    elif processing_mode == 'incremental':
        write_mode = 'overwritePartitions'  # GP-specific partitions
    else:
        raise ValueError(f"Unsupported processing mode: {processing_mode}")
    
    logger.info(f"🏆 Writing Gold table {table_name} ({processing_mode} mode, {year}" + 
                (f", {grand_prix_name}" if grand_prix_name else "") + ")")
    
    # Write to Gold database using existing function
    write_to_iceberg(
        df=df,
        table_name=table_name,
        write_mode=write_mode,
        database_name=database_name
    )
    
    logger.info(f"✅ Gold table {table_name} written successfully")
