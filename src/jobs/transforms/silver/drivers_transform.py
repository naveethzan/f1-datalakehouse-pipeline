"""
Drivers Transformation for F1 Bronze to Silver Pipeline

Streamlined transformation using modular utilities for better maintainability.
Implements SCD Type 2 with both HISTORICAL and INCREMENTAL processing modes.

Key features:
- True incremental SCD Type 2 with change detection
- Efficient Bronze data reading (target GP only in INCREMENTAL mode)
- Modular design with reusable utilities
- Team change tracking with validity periods

Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession

# Import utilities
from src.jobs.utils.data_access.bronze_readers import (
    read_drivers_for_gp, 
    read_sessions_for_gp, 
    read_full_year_drivers, 
    read_full_year_sessions
)
from src.jobs.utils.transformations.scd import (
    process_incremental_scd, 
    build_scd2_records, 
    get_drivers_schema
)
from src.jobs.utils.transformations.common import (
    standardize_driver_team_names, 
    join_with_session_dates, 
    add_total_races, 
    select_final_columns,
    add_audit_columns
)

# Setup logging
logger = logging.getLogger(__name__)


def transform_drivers(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze driver data to Silver format with SCD Type 2.
    
    Streamlined implementation using modular utilities for better maintainability.
    Supports both HISTORICAL (full year) and INCREMENTAL (target GP) processing modes.
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: If None, processes full year (HISTORICAL). If specified, processes only that GP (INCREMENTAL)
        
    Returns:
        Transformed DataFrame ready for Iceberg SCD merge
    """
    processing_mode = "HISTORICAL" if grand_prix_filter is None else "INCREMENTAL"
    
    logger.info("=" * 60)
    logger.info("👤 Drivers Transformation (SCD Type 2 - Modular)")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info(f"Mode: {processing_mode}")
    logger.info(f"   Architecture: Utility-based")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze data
    if processing_mode == "INCREMENTAL":
        drivers_df = read_drivers_for_gp(spark, bronze_path, year_filter, grand_prix_filter)
        sessions_df = read_sessions_for_gp(spark, bronze_path, year_filter, grand_prix_filter)
    else:
        drivers_df = read_full_year_drivers(spark, bronze_path, year_filter)
        sessions_df = read_full_year_sessions(spark, bronze_path, year_filter)
    
    if drivers_df.count() == 0:
        logger.warning(f"⚠️ No driver data found for {processing_mode} mode")
        return spark.createDataFrame([], get_drivers_schema())
    
    # Step 2: Join with sessions and standardize team names
    drivers_with_dates = join_with_session_dates(drivers_df, sessions_df)
    drivers_standardized = standardize_driver_team_names(drivers_with_dates)
    logger.info(f"📊 Processed {drivers_with_dates.count()} driver-session records")
    
    # Step 3: Generate SCD records
    if processing_mode == "INCREMENTAL":
        scd2_records = process_incremental_scd(spark, drivers_standardized, year_filter)
    else:
        scd2_records = build_scd2_records(drivers_standardized)
    
    logger.info(f"Generated {scd2_records.count()} SCD records ({processing_mode})")
    
    # Step 4: Finalize with races calculation and column selection
    with_total_races = add_total_races(scd2_records, drivers_with_dates)
    final_df = select_final_columns(with_total_races)
    result = add_audit_columns(final_df)
    
    logger.info(f"Completed: {result.count()} records ({processing_mode} mode)")
    return result