"""
Pitstops Transformation for F1 Bronze to Silver Pipeline

Simple transformation for pitstops data.
Reads directly from Bronze parquet files and writes to Silver Iceberg tables.
Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.

Key transformations:
- Convert pit duration from milliseconds to seconds (DECIMAL format)
- Filter out incomplete pit stops (NULL duration)
- Add placeholder values for complex strategy fields (to be enriched in Gold)
- Only for RACE sessions (not qualifying)

Volume: ~40-60 records per race (low volume, no memory concerns)
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    round as spark_round, floor, abs as spark_abs
)
from pyspark.sql.types import DecimalType

# Import from utils package using absolute imports
from src.jobs.utils import normalize_grand_prix_name, standardize_team_name, add_audit_columns

# Setup logging
logger = logging.getLogger(__name__)


def transform_pitstops(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze pitstop data to Silver format.
    
    Simple transformation keeping complex strategy analysis for Gold layer.
    Only processes RACE pitstops (not qualifying/practice).
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: Optional specific Grand Prix to process
        
    Returns:
        Transformed DataFrame ready for Iceberg write
    """
    logger.info("=" * 60)
    logger.info("Starting Pitstops Transformation")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze pit data with partition filtering
    pit_df = read_bronze_pitstops(spark, bronze_path, year_filter, grand_prix_filter)
    
    if pit_df.count() == 0:
        logger.warning("⚠️ No pitstop data found for specified filters")
        return spark.createDataFrame([], get_pitstops_schema())
    
    # Step 2: Convert pit duration from milliseconds to seconds
    duration_df = convert_pit_duration(pit_df)
    
    # Step 3: Filter valid pit stops (exclude incomplete stops)
    valid_df = filter_valid_pitstops(duration_df)
    
    # Step 4: Add strategy placeholders (complex analysis in Gold)
    strategy_df = add_strategy_placeholders(valid_df)
    
    # Step 5: Clean and add audit columns
    final_df = finalize_pitstop_data(strategy_df)
    
    record_count = final_df.count()
    logger.info(f"✅ Pitstops transformation complete: {record_count} records")
    
    return final_df


def read_bronze_pitstops(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Read Bronze pitstop data with partition pruning.
    Only reads RACE pitstops (session_type=race).
    
    Args:
        spark: Spark session
        bronze_path: Base Bronze S3 path
        year_filter: Year to filter
        grand_prix_filter: Optional Grand Prix to filter
        
    Returns:
        Raw Bronze pitstop DataFrame
    """
    # Build path with race-only filter
    if grand_prix_filter:
        # Specific Grand Prix
        path = f"{bronze_path}/pit/year={year_filter}/grand_prix={grand_prix_filter}/session_type=race"
        logger.info(f"📂 Reading pitstop data from: {path}")
    else:
        # All Grand Prix for the year
        path = f"{bronze_path}/pit/year={year_filter}/*/session_type=race"
        logger.info(f"📂 Reading all pitstop data for year {year_filter}")
    
    try:
        df = spark.read.parquet(path)
        
        # Add partition columns if not present
        if 'year' not in df.columns:
            df = df.withColumn('year', lit(year_filter))
        
        # Extract grand_prix from input file path if not present
        if 'grand_prix' not in df.columns:
            from pyspark.sql.functions import input_file_name, regexp_extract
            df = df.withColumn(
                'grand_prix',
                regexp_extract(input_file_name(), r'grand_prix=([^/]+)', 1)
            )
        
        logger.info(f"📊 Loaded {df.count()} pitstop records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze pitstop data: {e}")
        raise


def convert_pit_duration(df: DataFrame) -> DataFrame:
    """
    Convert pit duration from milliseconds to seconds.
    
    Bronze data has pit_duration in milliseconds (e.g., 876.0 ms).
    Silver schema requires DECIMAL(6,3) in seconds (e.g., 0.876 seconds).
    
    Note: Some entries may have NULL duration (incomplete pit stops).
    
    Args:
        df: Raw pitstop DataFrame
        
    Returns:
        DataFrame with pit duration in seconds
    """
    logger.info("⏱️ Converting pit duration from milliseconds to seconds")
    
    # Convert milliseconds to seconds with 3 decimal places
    # DECIMAL(6,3) means max 999.999 seconds
    return df.withColumn(
        "pit_duration_seconds",
        when(
            col("pit_duration").isNotNull(),
            # Convert ms to seconds and round to 3 decimal places
            spark_round(col("pit_duration") / 1000, 3).cast(DecimalType(6, 3))
        ).otherwise(lit(None))
    )


def filter_valid_pitstops(df: DataFrame) -> DataFrame:
    """
    Filter out incomplete or invalid pit stops.
    
    Valid pit stop criteria:
    - Has non-null pit duration
    - Duration is positive
    - Duration is reasonable (< 999 seconds)
    
    Args:
        df: DataFrame with converted pit duration
        
    Returns:
        DataFrame with only valid pit stops
    """
    logger.info("🔍 Filtering valid pit stops")
    
    # Count before filtering for logging
    total_count = df.count()
    
    # Filter valid pit stops
    valid_df = df.filter(
        col("pit_duration_seconds").isNotNull() &
        (col("pit_duration_seconds") > 0) &
        (col("pit_duration_seconds") < 999)  # Max value for DECIMAL(6,3)
    )
    
    valid_count = valid_df.count()
    filtered_count = total_count - valid_count
    
    if filtered_count > 0:
        logger.warning(f"⚠️ Filtered out {filtered_count} invalid pit stops")
    
    return valid_df


def add_strategy_placeholders(df: DataFrame) -> DataFrame:
    """
    Add placeholder values for complex strategy fields.
    
    These fields require complex analysis and will be calculated in Gold layer:
    - positions_lost_gained: NULL (needs lap position data)
    - undercut_attempt: FALSE (needs competitor analysis)
    - safety_car_stop: FALSE (needs race control data)
    - tire_compound_old/new: NULL (needs stint data)
    
    Args:
        df: DataFrame with valid pit stops
        
    Returns:
        DataFrame with strategy placeholder columns
    """
    logger.info("📋 Adding strategy placeholder columns for Gold layer enrichment")
    
    return df.withColumn(
        # Positions change - requires complex lap analysis
        "positions_lost_gained",
        lit(None).cast("int")
    ).withColumn(
        # Undercut detection - requires competitor pit window analysis
        "undercut_attempt",
        lit(False).cast("boolean")
    ).withColumn(
        # Safety car detection - requires race control messages
        "safety_car_stop",
        lit(False).cast("boolean")
    ).withColumn(
        # Tire compounds - requires stint data
        "tire_compound_old",
        lit(None).cast("string")
    ).withColumn(
        "tire_compound_new",
        lit(None).cast("string")
    )


def finalize_pitstop_data(df: DataFrame) -> DataFrame:
    """
    Clean data and add audit columns for Silver layer.
    
    Selects required columns in correct order and adds timestamps.
    
    Args:
        df: Transformed pitstop DataFrame
        
    Returns:
        Final DataFrame ready for Iceberg write
    """
    logger.info("🧹 Finalizing pitstop data for Silver layer")
    
    # Select and cast columns according to Silver schema
    final_df = df.select(
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("lap_number").cast("int"),
        col("pit_duration_seconds").alias("pit_duration"),  # Already DECIMAL(6,3)
        col("positions_lost_gained"),  # Already INT/NULL
        col("undercut_attempt"),  # Already BOOLEAN
        col("safety_car_stop"),  # Already BOOLEAN
        col("tire_compound_old"),  # Already STRING/NULL
        col("tire_compound_new"),  # Already STRING/NULL
        col("year").cast("int"),
        col("grand_prix").alias("grand_prix_name")  # Rename to match schema
    ).filter(
        # Ensure key columns are not null
        col("session_key").isNotNull() &
        col("driver_number").isNotNull() &
        col("lap_number").isNotNull() &
        col("pit_duration").isNotNull()
    )
    
    # Add audit columns using utils function
    return add_audit_columns(final_df)


def get_pitstops_schema():
    """
    Get the schema for the pitstops_silver table.
    Used when creating empty DataFrames.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("session_key", "bigint"),
        ("driver_number", "int"),
        ("lap_number", "int"),
        ("pit_duration", "decimal(6,3)"),
        ("positions_lost_gained", "int"),
        ("undercut_attempt", "boolean"),
        ("safety_car_stop", "boolean"),
        ("tire_compound_old", "string"),
        ("tire_compound_new", "string"),
        ("year", "int"),
        ("grand_prix_name", "string"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]


def get_pitstop_statistics(df: DataFrame) -> dict:
    """
    Calculate basic pitstop statistics for logging/monitoring.
    
    Args:
        df: Pitstop DataFrame
        
    Returns:
        Dictionary with statistics
    """
    from pyspark.sql.functions import avg, min as spark_min, max as spark_max, stddev
    
    stats_df = df.agg(
        avg("pit_duration").alias("avg_duration"),
        spark_min("pit_duration").alias("min_duration"),
        spark_max("pit_duration").alias("max_duration"),
        stddev("pit_duration").alias("stddev_duration")
    ).collect()[0]
    
    return {
        "avg_duration": float(stats_df["avg_duration"]) if stats_df["avg_duration"] else 0,
        "min_duration": float(stats_df["min_duration"]) if stats_df["min_duration"] else 0,
        "max_duration": float(stats_df["max_duration"]) if stats_df["max_duration"] else 0,
        "stddev_duration": float(stats_df["stddev_duration"]) if stats_df["stddev_duration"] else 0
    }
