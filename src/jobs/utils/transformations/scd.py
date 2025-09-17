"""
SCD Type 2 Utilities Module

This module provides reusable SCD (Slowly Changing Dimension) Type 2 processing
functions for F1 data pipeline. These utilities can be used across different
transforms that require SCD processing.

Key Features:
- Incremental SCD change detection
- Historical SCD record building
- Team change tracking for drivers
- Reusable across multiple transforms

Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col, min as spark_min, max as spark_max, 
    row_number, lead, when, lit, last
)

# Setup logging
logger = logging.getLogger(__name__)


def process_incremental_scd(
    spark: SparkSession, 
    current_drivers: DataFrame, 
    year_filter: int,
    silver_table_name: Optional[str] = None
) -> DataFrame:
    """
    Process incremental SCD Type 2 changes by comparing current Bronze data with existing Silver records.
    
    This function:
    1. Reads existing Silver SCD records for affected drivers
    2. Detects team changes by comparing Bronze vs Silver data
    3. Generates SCD records: closes old records, creates new ones
    
    Args:
        spark: SparkSession
        current_drivers: Bronze drivers data (standardized) for current GP
        year_filter: Year being processed
        silver_table_name: Full table name (optional, auto-generated if None)
        
    Returns:
        DataFrame with SCD Type 2 records ready for merge
    """
    logger.info("🔍 Starting incremental SCD change detection...")
    
    # Get unique drivers from current Bronze data
    current_driver_numbers = [row.driver_number for row in current_drivers.select("driver_number").distinct().collect()]
    logger.info(f"📊 Analyzing {len(current_driver_numbers)} drivers for changes")
    
    # Read existing Silver SCD records for these drivers
    if silver_table_name is None:
        from src.jobs.config.job_config import Config
        silver_table_name = f"{Config.CATALOG_NAME}.{Config.SILVER_DATABASE_NAME}.drivers_silver"
    
    try:
        # Read existing Silver records for affected drivers
        existing_scd = spark.table(silver_table_name).filter(
            col("driver_number").isin(current_driver_numbers)
        ).filter(
            col("is_current") == True  # Only current records
        )
        
        existing_count = existing_scd.count()
        logger.info(f"📋 Found {existing_count} existing Silver SCD records")
        
    except Exception as e:
        logger.warning(f"⚠️ Could not read existing Silver records (table might not exist): {e}")
        # If no existing records, treat as new drivers
        existing_scd = spark.createDataFrame([], get_drivers_schema())
    
    # Detect changes by comparing current Bronze with existing Silver
    changes_detected = detect_driver_team_changes(current_drivers, existing_scd)
    
    # Generate SCD records based on detected changes
    scd_records = generate_scd_records_from_changes(changes_detected, current_drivers)
    
    return scd_records


def detect_driver_team_changes(current_bronze: DataFrame, existing_silver: DataFrame) -> DataFrame:
    """
    Detect team changes by comparing current Bronze data with existing Silver SCD records.
    
    Args:
        current_bronze: Current Bronze drivers data (standardized)
        existing_silver: Existing Silver SCD records (current records only)
        
    Returns:
        DataFrame with change detection results
    """
    # Get latest team assignment per driver from current Bronze data
    current_teams = current_bronze.groupBy("driver_number").agg(
        # Use the latest session data for team assignment
        last("team_name_std").alias("current_team"),
        last("broadcast_name").alias("broadcast_name"),
        last("full_name").alias("full_name"),
        last("country_code").alias("country_code"),
        last("team_colour").alias("team_colour"),
        last("name_acronym").alias("name_acronym"),
        max("date_start").alias("change_date")  # Use max session date as change date
    )
    
    # Left join with existing Silver records to compare teams
    comparison = current_teams.alias("curr").join(
        existing_silver.alias("existing"),
        col("curr.driver_number") == col("existing.driver_number"),
        "left"
    ).select(
        col("curr.driver_number"),
        col("curr.current_team"),
        col("existing.team_name").alias("previous_team"),
        col("curr.change_date"),
        col("curr.broadcast_name"),
        col("curr.full_name"),
        col("curr.country_code"),
        col("curr.team_colour"),
        col("curr.name_acronym"),
        # Detect changes: new driver (no previous) or team change
        when(
            col("existing.team_name").isNull(), lit("NEW_DRIVER")
        ).when(
            col("curr.current_team") != col("existing.team_name"), lit("TEAM_CHANGE")
        ).otherwise(lit("NO_CHANGE")).alias("change_type")
    )
    
    changes_count = comparison.filter(col("change_type") != "NO_CHANGE").count()
    logger.info(f"🔄 Detected {changes_count} drivers with changes (new drivers or team changes)")
    
    return comparison


def generate_scd_records_from_changes(changes: DataFrame, current_bronze: DataFrame) -> DataFrame:
    """
    Generate SCD Type 2 records based on detected changes.
    
    For drivers with changes:
    - Creates closure records (sets valid_to for existing records)
    - Creates new SCD records with new team information
    
    For drivers with no changes:
    - Returns empty DataFrame (no SCD changes needed)
    
    Args:
        changes: DataFrame with change detection results
        current_bronze: Current Bronze drivers data
        
    Returns:
        DataFrame with SCD Type 2 records
    """
    # Filter only drivers with actual changes (new drivers or team changes)
    drivers_with_changes = changes.filter(col("change_type") != "NO_CHANGE")
    
    if drivers_with_changes.count() == 0:
        logger.info("📝 No changes detected - no SCD records to generate")
        # Return empty DataFrame with correct schema
        return current_bronze.limit(0).select(
            col("driver_number").cast("int"),
            lit("").alias("broadcast_name").cast("string"),
            lit("").alias("full_name").cast("string"),
            lit("").alias("team_name").cast("string"),
            lit("").alias("country_code").cast("string"),
            lit("").alias("team_colour").cast("string"),
            lit("").alias("name_acronym").cast("string"),
            lit(0).alias("total_races").cast("int"),
            col("change_date").alias("valid_from").cast("timestamp"),
            lit(None).alias("valid_to").cast("timestamp"),
            lit(True).alias("is_current").cast("boolean")
        )
    
    # Generate new SCD records for drivers with changes
    new_scd_records = drivers_with_changes.select(
        col("driver_number").cast("int"),
        col("broadcast_name").cast("string"),
        col("full_name").cast("string"),
        col("current_team").alias("team_name").cast("string"),
        col("country_code").cast("string"),
        col("team_colour").cast("string"),
        col("name_acronym").cast("string"),
        lit(0).alias("total_races").cast("int"),  # Will be calculated later
        col("change_date").alias("valid_from").cast("timestamp"),
        lit(None).alias("valid_to").cast("timestamp"),  # Open-ended (current record)
        lit(True).alias("is_current").cast("boolean")
    )
    
    changes_count = new_scd_records.count()
    logger.info(f"📝 Generated {changes_count} new SCD records for changed drivers")
    
    return new_scd_records


def build_scd2_records(df: DataFrame) -> DataFrame:
    """
    Build SCD Type 2 records tracking team changes for HISTORICAL mode.
    
    Creates one record per driver-team combination with validity periods.
    This is used for complete rebuilds in HISTORICAL mode.
    
    Args:
        df: DataFrame with drivers and standardized team names
        
    Returns:
        DataFrame with SCD Type 2 structure
    """
    # Get the earliest and latest appearance for each driver-team combination
    driver_team_periods = df.groupBy(
        "driver_number",
        "team_name_std"
    ).agg(
        spark_min("date_start").alias("valid_from"),
        spark_max("date_start").alias("last_appearance"),
        # Keep the latest values for other attributes
        last("broadcast_name").alias("broadcast_name"),
        last("full_name").alias("full_name"),
        last("country_code").alias("country_code"),
        last("team_colour").alias("team_colour"),
        last("name_acronym").alias("name_acronym")
    )
    
    # Create window to calculate valid_to dates
    driver_window = Window.partitionBy("driver_number").orderBy("valid_from")
    
    # Add valid_to using lead function
    with_valid_to = driver_team_periods.withColumn(
        "valid_to",
        lead("valid_from").over(driver_window)
    )
    
    # Add is_current flag
    with_current_flag = with_valid_to.withColumn(
        "is_current",
        when(col("valid_to").isNull(), lit(True)).otherwise(lit(False))
    )
    
    # Rename team_name_std to team_name
    result = with_current_flag.withColumnRenamed("team_name_std", "team_name")
    
    return result


def get_drivers_schema():
    """
    Get the schema for the drivers_silver table.
    Used when creating empty DataFrames for SCD processing.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("driver_number", "int"),
        ("broadcast_name", "string"),
        ("full_name", "string"),
        ("team_name", "string"),
        ("country_code", "string"),
        ("team_colour", "string"),
        ("name_acronym", "string"),
        ("total_races", "int"),
        ("valid_from", "timestamp"),
        ("valid_to", "timestamp"),
        ("is_current", "boolean"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]