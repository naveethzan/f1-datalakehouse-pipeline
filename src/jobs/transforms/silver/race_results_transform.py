"""
Race Results Transformation for F1 Bronze to Silver Pipeline

Simplified transformation for race results data.
Reads directly from Bronze parquet files and writes to Silver Iceberg tables.

Key transformations:
- Validate and recalculate F1 points (25, 18, 15, 12, 10, 8, 6, 4, 2, 1)
- Format race times and calculate gaps
- Standardize race status (Finished, DNF, DNS, DSQ)
- No complex dependencies - grid positions and fastest lap handled in Gold layer
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp,
    round as spark_round, floor, concat, lpad
)

# Import from utils package using absolute imports  
from src.jobs.utils import calculate_race_points, add_audit_columns
from src.jobs.utils import standardize_race_status as utils_standardize_race_status

# Setup logging
logger = logging.getLogger(__name__)


def transform_race_results(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze race results to Silver format.
    
    Simple, direct transformation without complex dependencies.
    Grid positions and fastest lap bonus will be added in Gold layer.
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: Optional specific Grand Prix to process
        
    Returns:
        Transformed DataFrame ready for Iceberg write
    """
    logger.info("=" * 60)
    logger.info("Starting Race Results Transformation")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze race data with partition filtering
    race_df = read_bronze_race(spark, bronze_path, year_filter, grand_prix_filter)
    
    if race_df.count() == 0:
        logger.warning("⚠️ No race data found for specified filters")
        return spark.createDataFrame([], get_race_schema())
    
    # Step 2: Validate and recalculate points
    validated_df = validate_race_points(race_df)
    
    # Step 3: Format race times and gaps
    time_formatted_df = format_race_times(validated_df)
    
    # Step 4: Standardize race status
    status_df = standardize_race_status(time_formatted_df)
    
    # Step 5: Clean and add audit columns
    final_df = finalize_race_data(status_df)
    
    record_count = final_df.count()
    logger.info(f"✅ Race results transformation complete: {record_count} records")
    
    return final_df


def read_bronze_race(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Read Bronze race data with partition pruning.
    
    Args:
        spark: Spark session
        bronze_path: Base Bronze S3 path
        year_filter: Year to filter
        grand_prix_filter: Optional Grand Prix to filter
        
    Returns:
        Raw Bronze race DataFrame
    """
    # Build the path with partition filters
    if grand_prix_filter:
        # Specific Grand Prix
        path = f"{bronze_path}/session_result/year={year_filter}/grand_prix={grand_prix_filter}/session_type=race"
        logger.info(f"📂 Reading race data from: {path}")
    else:
        # All Grand Prix for the year
        path = f"{bronze_path}/session_result/year={year_filter}/*/session_type=race"
        logger.info(f"📂 Reading all race data for year {year_filter}")
    
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
        
        logger.info(f"📊 Loaded {df.count()} race records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze race data: {e}")
        raise


def validate_race_points(df: DataFrame) -> DataFrame:
    """
    Validate and recalculate F1 points based on position.
    
    Points system (current regulations):
    1st: 25, 2nd: 18, 3rd: 15, 4th: 12, 5th: 10,
    6th: 8, 7th: 6, 8th: 4, 9th: 2, 10th: 1
    
    Note: Fastest lap bonus point handled in Gold layer
    
    Args:
        df: Raw race DataFrame
        
    Returns:
        DataFrame with validated points
    """
    logger.info("🏆 Validating and recalculating F1 points")
    
    # F1 points mapping
    points_map = {
        1: 25, 2: 18, 3: 15, 4: 12, 5: 10,
        6: 8, 7: 6, 8: 4, 9: 2, 10: 1
    }
    
    # Create validated_points column based on position
    df_with_validated = df.withColumn(
        "validated_points",
        when(col("position") == 1, 25)
        .when(col("position") == 2, 18)
        .when(col("position") == 3, 15)
        .when(col("position") == 4, 12)
        .when(col("position") == 5, 10)
        .when(col("position") == 6, 8)
        .when(col("position") == 7, 6)
        .when(col("position") == 8, 4)
        .when(col("position") == 9, 2)
        .when(col("position") == 10, 1)
        .otherwise(0)  # Positions 11+ get 0 points
    )
    
    # Cast original points to INT for comparison
    df_with_validated = df_with_validated.withColumn(
        "points", col("points").cast("int")
    )
    
    # Log warnings for mismatches (data quality check)
    mismatches = df_with_validated.filter(
        col("points") != col("validated_points")
    ).select("driver_number", "position", "points", "validated_points")
    
    if mismatches.count() > 0:
        logger.warning(f"⚠️ Found {mismatches.count()} points mismatches")
        for row in mismatches.collect()[:5]:  # Show first 5
            logger.warning(
                f"   Driver {row['driver_number']}: Position={row['position']}, "
                f"API Points={row['points']}, Calculated={row['validated_points']}"
            )
    
    return df_with_validated


def format_race_times(df: DataFrame) -> DataFrame:
    """
    Format race times and calculate gaps.
    
    - Convert duration to HH:MM:SS.mmm format
    - Convert gap_to_leader to milliseconds
    - Winner gets gap = 0
    
    Args:
        df: DataFrame with validated points
        
    Returns:
        DataFrame with formatted times
    """
    logger.info("⏱️ Formatting race times and gaps")
    
    # Calculate time components from duration (seconds)
    df_with_time = df.withColumn(
        "hours", floor(col("duration") / 3600).cast("int")
    ).withColumn(
        "minutes", floor((col("duration") % 3600) / 60).cast("int")
    ).withColumn(
        "seconds", col("duration") % 60
    )
    
    # Format as HH:MM:SS.mmm for race time
    df_with_time = df_with_time.withColumn(
        "time",
        when(
            col("duration").isNotNull() & (col("dnf") == False) & 
            (col("dns") == False) & (col("dsq") == False),
            concat(
                lpad(col("hours").cast("string"), 2, "0"),
                lit(":"),
                lpad(col("minutes").cast("string"), 2, "0"),
                lit(":"),
                # Format seconds with 3 decimal places
                lpad(spark_round(col("seconds"), 3).cast("string"), 6, "0")
            )
        ).otherwise(lit(None))  # NULL for DNF/DNS/DSQ
    ).drop("hours", "minutes", "seconds")
    
    # Calculate gap to winner in milliseconds
    df_with_gap = df_with_time.withColumn(
        "gap_to_winner_millis",
        when(
            col("position") == 1,
            lit(0)  # Winner has 0 gap
        ).when(
            col("gap_to_leader").isNotNull(),
            spark_round(col("gap_to_leader") * 1000, 0).cast("bigint")
        ).otherwise(lit(None))
    )
    
    return df_with_gap


def standardize_race_status(df: DataFrame) -> DataFrame:
    """
    Standardize race completion status using utils function.
    
    Uses the centralized standardize_race_status function from utils
    via UDF for consistency across the pipeline.
    
    Args:
        df: DataFrame with formatted times
        
    Returns:
        DataFrame with standardized status
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType
    
    logger.info("🏁 Standardizing race status using utils function")
    
    # Create UDF from utils function
    standardize_status_udf = udf(utils_standardize_race_status, StringType())
    
    # First create raw status from flags
    df_with_status = df.withColumn(
        "raw_status",
        when(col("dsq") == True, "DSQ")
        .when(col("dns") == True, "DNS")
        .when(col("dnf") == True, "DNF")
        .when(col("position") > 0, "Finished")
        .otherwise("Unknown")
    )
    
    # Apply utils standardization function
    return df_with_status.withColumn(
        "status",
        col("raw_status")
    ).withColumn(
        "standardized_status",
        standardize_status_udf(col("raw_status"))
    ).drop("raw_status")


def finalize_race_data(df: DataFrame) -> DataFrame:
    """
    Clean data and add audit columns for Silver layer.
    
    Note: grid_position and positions_gained will be NULL
    (to be populated in Gold layer from qualifying data)
    
    Args:
        df: Transformed race DataFrame
        
    Returns:
        Final DataFrame ready for Iceberg write
    """
    logger.info("🧹 Finalizing race data for Silver layer")
    
    # Select and cast columns according to Silver schema
    final_df = df.select(
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("position").cast("int"),
        lit(None).cast("int").alias("grid_position"),  # To be added in Gold
        col("points").cast("int"),
        col("validated_points").cast("int"),
        lit(None).cast("int").alias("positions_gained"),  # To be calculated in Gold
        col("time").cast("string"),
        col("gap_to_winner_millis"),
        col("status").cast("string"),
        col("standardized_status").cast("string"),
        col("year").cast("int"),
        col("grand_prix").alias("grand_prix_name")
    ).filter(
        # Ensure key columns are not null
        col("session_key").isNotNull() &
        col("driver_number").isNotNull() &
        col("position").isNotNull()
    )
    
    # Add audit columns using utils function
    return add_audit_columns(final_df)


def get_race_schema():
    """
    Get the schema for the race_results_silver table.
    Used when creating empty DataFrames.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("session_key", "bigint"),
        ("driver_number", "int"),
        ("position", "int"),
        ("grid_position", "int"),
        ("points", "int"),
        ("validated_points", "int"),
        ("positions_gained", "int"),
        ("time", "string"),
        ("gap_to_winner_millis", "bigint"),
        ("status", "string"),
        ("standardized_status", "string"),
        ("year", "int"),
        ("grand_prix_name", "string"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]
