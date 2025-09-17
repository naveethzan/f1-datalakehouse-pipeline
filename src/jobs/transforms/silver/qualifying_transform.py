"""
Qualifying Results Transformation for F1 Bronze to Silver Pipeline

Simplified transformation for qualifying results data.
Reads directly from Bronze parquet files and writes to Silver Iceberg tables.

Key transformations:
- Parse duration array [Q1, Q2, Q3] to individual millisecond columns
- Calculate gap to pole position
- Determine qualifying status (Q3, Q2-OUT, Q1-OUT, DNQ, DSQ)
- Add year and grand_prix from partition values
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, size, element_at, 
    min as spark_min, year, current_timestamp,
    coalesce, round as spark_round
)
from pyspark.sql.window import Window

# Import from utils package using absolute imports
from src.jobs.utils import add_audit_columns

# Setup logging
logger = logging.getLogger(__name__)


def transform_qualifying_results(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze qualifying results to Silver format.
    
    This is a simple, direct transformation without complex inheritance.
    Follows the data flow: Read Bronze → Transform → Return DataFrame
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: Optional specific Grand Prix to process
        
    Returns:
        Transformed DataFrame ready for Iceberg write
    """
    logger.info("=" * 60)
    logger.info("Starting Qualifying Results Transformation")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze qualifying data with partition filtering
    qualifying_df = read_bronze_qualifying(spark, bronze_path, year_filter, grand_prix_filter)
    
    if qualifying_df.count() == 0:
        logger.warning("⚠️ No qualifying data found for specified filters")
        return spark.createDataFrame([], get_qualifying_schema())
    
    # Step 2: Parse duration array to Q1, Q2, Q3 times
    parsed_df = parse_qualifying_times(qualifying_df)
    
    # Step 3: Calculate gap to pole
    gap_df = calculate_gap_to_pole(parsed_df)
    
    # Step 4: Determine qualifying status
    status_df = determine_qualifying_status(gap_df)
    
    # Step 5: Clean and add audit columns
    final_df = finalize_qualifying_data(status_df)
    
    record_count = final_df.count()
    logger.info(f"✅ Qualifying transformation complete: {record_count} records")
    
    return final_df


def read_bronze_qualifying(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Read Bronze qualifying data with partition pruning.
    
    Args:
        spark: Spark session
        bronze_path: Base Bronze S3 path
        year_filter: Year to filter
        grand_prix_filter: Optional Grand Prix to filter
        
    Returns:
        Raw Bronze qualifying DataFrame
    """
    # Build the path with partition filters
    if grand_prix_filter:
        # Specific Grand Prix
        path = f"{bronze_path}/session_result/year={year_filter}/grand_prix={grand_prix_filter}/session_type=qualifying"
        logger.info(f"📂 Reading qualifying data from: {path}")
    else:
        # All Grand Prix for the year
        path = f"{bronze_path}/session_result/year={year_filter}/*/session_type=qualifying"
        logger.info(f"📂 Reading all qualifying data for year {year_filter}")
    
    try:
        df = spark.read.parquet(path)
        
        # Add partition columns if not present (Spark sometimes doesn't include them)
        if 'year' not in df.columns:
            df = df.withColumn('year', lit(year_filter))
        
        # Extract grand_prix from input file path if not present
        if 'grand_prix' not in df.columns:
            from pyspark.sql.functions import input_file_name, regexp_extract
            df = df.withColumn(
                'grand_prix',
                regexp_extract(input_file_name(), r'grand_prix=([^/]+)', 1)
            )
        
        logger.info(f"📊 Loaded {df.count()} qualifying records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze qualifying data: {e}")
        raise


def parse_qualifying_times(df: DataFrame) -> DataFrame:
    """
    Parse duration array [Q1, Q2, Q3] to individual millisecond columns.
    
    The duration array contains lap times in seconds:
    - Index 0: Q1 time
    - Index 1: Q2 time (if driver made it to Q2)
    - Index 2: Q3 time (if driver made it to Q3)
    
    Args:
        df: Raw qualifying DataFrame with duration array
        
    Returns:
        DataFrame with parsed Q1, Q2, Q3 times in milliseconds
    """
    logger.info("⏱️ Parsing qualifying times from duration array")
    
    return df.withColumn(
        # Q1 time - everyone has this (unless DNS/DNQ)
        "q1_time_millis",
        when(
            col("duration").isNotNull() & (size(col("duration")) >= 1),
            spark_round(element_at(col("duration"), 1) * 1000, 0).cast("bigint")
        ).otherwise(lit(None))
    ).withColumn(
        # Q2 time - only if made it to Q2
        "q2_time_millis",
        when(
            col("duration").isNotNull() & (size(col("duration")) >= 2),
            spark_round(element_at(col("duration"), 2) * 1000, 0).cast("bigint")
        ).otherwise(lit(None))
    ).withColumn(
        # Q3 time - only if made it to Q3
        "q3_time_millis",
        when(
            col("duration").isNotNull() & (size(col("duration")) >= 3),
            spark_round(element_at(col("duration"), 3) * 1000, 0).cast("bigint")
        ).otherwise(lit(None))
    ).withColumn(
        # Fastest time across all qualifying sessions
        "fastest_qualifying_time_millis",
        # Use coalesce to get the best non-null time
        # Q3 is typically fastest, then Q2, then Q1
        coalesce(
            col("q3_time_millis"),
            col("q2_time_millis"), 
            col("q1_time_millis")
        )
    )


def calculate_gap_to_pole(df: DataFrame) -> DataFrame:
    """
    Calculate gap to pole position for each driver.
    
    Pole position (P1) gets gap = 0
    Others get gap = their_best_time - pole_time
    
    Args:
        df: DataFrame with parsed qualifying times
        
    Returns:
        DataFrame with gap_to_pole_millis column
    """
    logger.info("📏 Calculating gap to pole position")
    
    # Window to find pole time (minimum) per session
    session_window = Window.partitionBy("session_key")
    
    # Add pole time for the session
    df_with_pole = df.withColumn(
        "pole_time_millis",
        spark_min("fastest_qualifying_time_millis").over(session_window)
    )
    
    # Calculate gap (0 for pole, difference for others)
    return df_with_pole.withColumn(
        "gap_to_pole_millis",
        when(
            col("fastest_qualifying_time_millis").isNotNull() & 
            col("pole_time_millis").isNotNull(),
            (col("fastest_qualifying_time_millis") - col("pole_time_millis")).cast("bigint")
        ).otherwise(lit(None))
    ).drop("pole_time_millis")


def determine_qualifying_status(df: DataFrame) -> DataFrame:
    """
    Determine qualifying status based on progression through sessions.
    
    Status values:
    - Q3: Made it to Q3 (top 10)
    - Q2-OUT: Eliminated in Q2 (P11-15)
    - Q1-OUT: Eliminated in Q1 (P16-20)
    - DSQ: Disqualified
    - DNS: Did Not Start
    - DNQ: Did Not Qualify (other cases)
    
    Args:
        df: DataFrame with qualifying times
        
    Returns:
        DataFrame with qualifying_status column
    """
    logger.info("🏁 Determining qualifying status for each driver")
    
    return df.withColumn(
        "qualifying_status",
        when(col("dsq") == True, "DSQ")
        .when(col("dns") == True, "DNS")
        .when(col("q3_time_millis").isNotNull(), "Q3")
        .when(col("q2_time_millis").isNotNull(), "Q2-OUT")
        .when(col("q1_time_millis").isNotNull(), "Q1-OUT")
        .otherwise("DNQ")
    )


def finalize_qualifying_data(df: DataFrame) -> DataFrame:
    """
    Clean data and add audit columns for Silver layer.
    
    Args:
        df: Transformed qualifying DataFrame
        
    Returns:
        Final DataFrame ready for Iceberg write
    """
    logger.info("🧹 Finalizing qualifying data for Silver layer")
    
    # Select and cast columns according to Silver schema
    final_df = df.select(
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("position").cast("int"),
        col("q1_time_millis"),
        col("q2_time_millis"),
        col("q3_time_millis"),
        col("fastest_qualifying_time_millis"),
        col("gap_to_pole_millis"),
        col("qualifying_status"),
        col("year").cast("int"),
        col("grand_prix").alias("grand_prix_name")  # Rename to match schema
    ).filter(
        # Ensure key columns are not null
        col("session_key").isNotNull() &
        col("driver_number").isNotNull() &
        col("position").isNotNull()
    )
    
    # Add audit columns using utils function
    return add_audit_columns(final_df)


def get_qualifying_schema():
    """
    Get the schema for the qualifying_results_silver table.
    Used when creating empty DataFrames.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("session_key", "bigint"),
        ("driver_number", "int"),
        ("position", "int"),
        ("q1_time_millis", "bigint"),
        ("q2_time_millis", "bigint"),
        ("q3_time_millis", "bigint"),
        ("fastest_qualifying_time_millis", "bigint"),
        ("gap_to_pole_millis", "bigint"),
        ("qualifying_status", "string"),
        ("year", "int"),
        ("grand_prix_name", "string"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]
