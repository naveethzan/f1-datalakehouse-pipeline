"""
Bronze Data Readers Module

This module provides reusable functions for reading data from the Bronze layer
across different F1 data transforms. It standardizes Bronze data access patterns
and provides both full-year and Grand Prix specific reading capabilities.

Key Features:
- Standardized Bronze data reading patterns
- Full year data reading (HISTORICAL mode)
- Grand Prix specific data reading (INCREMENTAL mode)
- Consistent error handling and logging
- Reusable across multiple transforms

Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit

# Setup logging
logger = logging.getLogger(__name__)


def read_full_year_drivers(spark: SparkSession, bronze_path: str, year_filter: int) -> DataFrame:
    """
    Read full year of drivers data from Bronze layer (HISTORICAL mode).
    
    Args:
        spark: SparkSession
        bronze_path: Base Bronze S3 path
        year_filter: Year to process
        
    Returns:
        DataFrame with all drivers data for the year
    """
    drivers_path = f"{bronze_path}/drivers/year={year_filter}"
    logger.info(f"📂 Reading drivers from: {drivers_path}")
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(drivers_path)
        
        # Add year column if not present
        if "year" not in df.columns:
            df = df.withColumn("year", lit(year_filter))
        
        logger.info(f"📊 Loaded {df.count()} driver records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze driver data: {e}")
        raise


def read_drivers_for_gp(spark: SparkSession, bronze_path: str, year_filter: int, grand_prix_filter: str) -> DataFrame:
    """
    Read drivers data for a specific Grand Prix from Bronze layer (INCREMENTAL mode).
    
    Args:
        spark: SparkSession
        bronze_path: Base Bronze S3 path
        year_filter: Year to process
        grand_prix_filter: Specific Grand Prix to process
        
    Returns:
        DataFrame with drivers data for the specified GP
    """
    # Read from partitioned path: bronze/drivers/year=2025/grand_prix=bahrain
    drivers_path = f"{bronze_path}/drivers/year={year_filter}/grand_prix={grand_prix_filter}"
    logger.info(f"📂 Reading drivers from: {drivers_path}")
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(drivers_path)
        
        # Add year column if not present
        if "year" not in df.columns:
            df = df.withColumn("year", lit(year_filter))
        
        logger.info(f"📊 Loaded {df.count()} driver records for {grand_prix_filter}")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze driver data for {grand_prix_filter}: {e}")
        raise


def read_full_year_sessions(spark: SparkSession, bronze_path: str, year_filter: int) -> DataFrame:
    """
    Read full year of session_result data from Bronze layer (HISTORICAL mode).
    
    Args:
        spark: SparkSession
        bronze_path: Base Bronze S3 path
        year_filter: Year to process
        
    Returns:
        DataFrame with all session data for the year
    """
    sessions_path = f"{bronze_path}/session_result/year={year_filter}"
    logger.info(f"📂 Reading sessions from: {sessions_path}")
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(sessions_path)
    
        # Select only needed columns
        result_df = df.select(
            col("session_key"),
            col("date_start"),
            col("session_type"),
            col("grand_prix"),
            col("year")
        ).distinct()
        
        logger.info(f"📊 Loaded {result_df.count()} session records")
        return result_df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze session data: {e}")
        raise


def read_sessions_for_gp(spark: SparkSession, bronze_path: str, year_filter: int, grand_prix_filter: str) -> DataFrame:
    """
    Read session data for a specific Grand Prix from Bronze layer (INCREMENTAL mode).
    
    Args:
        spark: SparkSession
        bronze_path: Base Bronze S3 path
        year_filter: Year to process
        grand_prix_filter: Specific Grand Prix to process
        
    Returns:
        DataFrame with session data for the specified GP
    """
    # Read from partitioned path: bronze/session_result/year=2025/grand_prix=bahrain
    sessions_path = f"{bronze_path}/session_result/year={year_filter}/grand_prix={grand_prix_filter}"
    logger.info(f"📂 Reading sessions from: {sessions_path}")
    
    try:
        df = spark.read.option("mergeSchema", "true").parquet(sessions_path)
    
        # Select only needed columns
        result_df = df.select(
            col("session_key"),
            col("date_start"),
            col("session_type"),
            col("grand_prix"),
            col("year")
        ).distinct()
        
        logger.info(f"📊 Loaded {result_df.count()} session records for {grand_prix_filter}")
        return result_df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze session data for {grand_prix_filter}: {e}")
        raise


def read_bronze_data_by_mode(
    spark: SparkSession,
    bronze_path: str, 
    data_type: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None,
    processing_mode: Optional[str] = None
) -> DataFrame:
    """
    Generic Bronze data reader that supports both HISTORICAL and INCREMENTAL modes.
    
    This is a convenience function that automatically chooses the appropriate
    reading method based on the processing mode.
    
    Args:
        spark: SparkSession
        bronze_path: Base Bronze S3 path
        data_type: Type of data to read ('drivers', 'session_result', etc.)
        year_filter: Year to process
        grand_prix_filter: Grand Prix filter (None for full year)
        processing_mode: 'HISTORICAL' or 'INCREMENTAL' (auto-detected if None)
        
    Returns:
        DataFrame with requested Bronze data
        
    Raises:
        ValueError: If data_type is not supported
    """
    # Auto-detect processing mode if not provided
    if processing_mode is None:
        processing_mode = "HISTORICAL" if grand_prix_filter is None else "INCREMENTAL"
    
    logger.info(f"📂 Reading Bronze {data_type} data in {processing_mode} mode")
    
    # Route to appropriate reader based on data type and mode
    if data_type == "drivers":
        if processing_mode == "INCREMENTAL" and grand_prix_filter:
            return read_drivers_for_gp(spark, bronze_path, year_filter, grand_prix_filter)
        else:
            return read_full_year_drivers(spark, bronze_path, year_filter)
    
    elif data_type == "session_result":
        if processing_mode == "INCREMENTAL" and grand_prix_filter:
            return read_sessions_for_gp(spark, bronze_path, year_filter, grand_prix_filter)
        else:
            return read_full_year_sessions(spark, bronze_path, year_filter)
    
    else:
        raise ValueError(f"Unsupported data type: {data_type}. Supported types: drivers, session_result")


def validate_bronze_data(df: DataFrame, data_type: str, year_filter: int, grand_prix_filter: Optional[str] = None) -> bool:
    """
    Validate Bronze data after reading to ensure it meets basic requirements.
    
    Args:
        df: DataFrame to validate
        data_type: Type of data being validated
        year_filter: Expected year
        grand_prix_filter: Expected Grand Prix (if applicable)
        
    Returns:
        True if validation passes, False otherwise
    """
    if df.count() == 0:
        logger.warning(f"⚠️ No {data_type} data found for year {year_filter}" + 
                      (f", GP {grand_prix_filter}" if grand_prix_filter else ""))
        return False
    
    # Check for required columns based on data type
    required_columns = {
        "drivers": ["driver_number", "broadcast_name", "team_name"],
        "session_result": ["session_key", "date_start", "session_type"]
    }
    
    if data_type in required_columns:
        missing_columns = [col for col in required_columns[data_type] if col not in df.columns]
        if missing_columns:
            logger.error(f"❌ Missing required columns in {data_type} data: {missing_columns}")
            return False
    
    logger.info(f"✅ Bronze {data_type} data validation passed")
    return True