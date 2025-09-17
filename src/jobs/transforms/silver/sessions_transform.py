"""
Sessions Transformation for F1 Bronze to Silver Pipeline

Simplified transformation for sessions data.
Reads directly from Bronze parquet files and writes to Silver Iceberg tables.
Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.

Key transformations:
- Extract unique sessions from session_result Bronze data
- Normalize Grand Prix names for partitioning
- Add descriptive session names
- Detect sprint weekends
- Calculate session duration
"""

import logging
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, year, when, unix_timestamp, lit,
    udf, current_timestamp, regexp_replace
)
from pyspark.sql.types import StringType, BooleanType

# Import from utils package using absolute imports
from src.jobs.utils import normalize_grand_prix_name, add_audit_columns

# Setup logging
logger = logging.getLogger(__name__)


def transform_sessions(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze session data to Silver format.
    
    Simple, direct transformation without complex dependencies.
    Reads from Bronze parquet files and extracts unique sessions.
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: Optional specific Grand Prix to process
        
    Returns:
        Transformed DataFrame ready for Iceberg write
    """
    logger.info("=" * 60)
    logger.info("Starting Sessions Transformation")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze session_result data with partition filtering
    session_result_df = read_bronze_session_result(spark, bronze_path, year_filter, grand_prix_filter)
    
    if session_result_df.count() == 0:
        logger.warning("⚠️ No session data found for specified filters")
        return spark.createDataFrame([], get_sessions_schema())
    
    # Step 2: Extract unique sessions from session_result
    sessions_df = extract_unique_sessions(session_result_df)
    logger.info(f"📊 Extracted {sessions_df.count()} unique sessions")
    
    # Step 3: Clean and type cast session data
    cleaned_df = clean_session_data(sessions_df)
    
    # Step 4: Add calculated fields (GP names, duration, sprint detection)
    enriched_df = add_calculated_fields(cleaned_df)
    
    # Step 5: Add audit columns
    final_df = add_audit_columns(enriched_df)
    
    record_count = final_df.count()
    logger.info(f"✅ Sessions transformation complete: {record_count} records")
    
    return final_df
    
def read_bronze_session_result(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Read Bronze session_result data with partition pruning.
    
    Args:
        spark: Spark session
        bronze_path: Base Bronze S3 path
        year_filter: Year to filter
        grand_prix_filter: Optional Grand Prix to filter
        
    Returns:
        Raw Bronze session_result DataFrame
    """
    # Build path with partition filters
    if grand_prix_filter:
        # Specific Grand Prix
        path = f"{bronze_path}/session_result/year={year_filter}/grand_prix={grand_prix_filter}"
        logger.info(f"📂 Reading session data from: {path}")
    else:
        # All Grand Prix for the year
        path = f"{bronze_path}/session_result/year={year_filter}"
        logger.info(f"📂 Reading all session data for year {year_filter}")
    
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
        
        logger.info(f"📊 Loaded {df.count()} session_result records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze session_result data: {e}")
        raise


def extract_unique_sessions(session_result_df: DataFrame) -> DataFrame:
    """
    Extract unique session records from session_result Bronze data.
    
    Bronze session_result contains driver results but we need unique sessions.
    This function extracts one record per session_key.
    
    Args:
        session_result_df: Bronze session_result DataFrame
        
    Returns:
        DataFrame with unique sessions
    """
    # Select relevant columns and get distinct sessions
    sessions_df = session_result_df.select(
        col("session_key"),
        col("meeting_key"),
        col("year"),
        col("grand_prix"),  # Will be normalized later
        col("session_type"),
        col("meeting_name"),
        col("date_start"),
        col("date_end")  # May be null
    ).distinct()
    
    return sessions_df


def clean_session_data(df: DataFrame) -> DataFrame:
    """
    Clean and standardize session data with proper data types.
    
    Args:
        df: Raw sessions DataFrame
        
    Returns:
        Cleaned DataFrame with proper types
    """
    cleaned_df = df.select(
        col("session_key").cast("bigint").alias("session_key"),
        col("session_type").cast("string").alias("session_type"),
        col("meeting_key").cast("bigint").alias("meeting_key"),
        col("meeting_name").cast("string").alias("meeting_name"),
        col("date_start").cast("timestamp").alias("date_start"),
        col("date_end").cast("timestamp").alias("date_end"),
        col("year").cast("int").alias("year"),
        col("grand_prix").cast("string").alias("grand_prix_raw")
    ).filter(
        # Filter out invalid records
        col("session_key").isNotNull() & 
        col("session_type").isNotNull() &
        col("date_start").isNotNull()
    )
    
    return cleaned_df
    
def add_calculated_fields(df: DataFrame) -> DataFrame:
    """
    Add calculated fields to sessions data.
    
    Adds:
    - session_name: Descriptive name based on session_type
    - grand_prix_name: Normalized GP name for partitioning
    - session_duration_minutes: Calculated from start/end times
    - is_sprint_weekend: Sprint weekend detection
    
    Args:
        df: Cleaned sessions DataFrame
        
    Returns:
        DataFrame with calculated fields
    """
    # Create UDF for grand prix normalization
    normalize_gp_udf = udf(normalize_grand_prix_name, StringType())
    
    # Create UDF for sprint weekend detection
    detect_sprint_udf = udf(detect_sprint_weekend, BooleanType())
    
    enriched_df = df.withColumn(
        # Add descriptive session name
        "session_name",
        when(col("session_type") == "qualifying", "Qualifying Session")
        .when(col("session_type") == "race", "Race Session")
        .otherwise(col("session_type"))
    ).withColumn(
        # Normalize grand prix name from meeting_name
        "grand_prix_name",
        normalize_gp_udf(col("meeting_name"))
    ).withColumn(
        # Calculate session duration if date_end exists
        "session_duration_minutes",
        when(
            col("date_end").isNotNull(),
            (unix_timestamp(col("date_end")) - unix_timestamp(col("date_start"))) / 60
        ).otherwise(lit(None)).cast("int")
    ).withColumn(
        # Detect sprint weekends
        "is_sprint_weekend",
        detect_sprint_udf(col("meeting_name"))
    )
    
    # Select final columns in correct order (dropping intermediates)
    final_df = enriched_df.select(
        "session_key",
        "session_type",
        "session_name",
        "meeting_key",
        "grand_prix_name",
        "date_start",
        "date_end",
        "year",
        "session_duration_minutes",
        "is_sprint_weekend"
    )
    
    return final_df


def get_sessions_schema():
    """
    Get the schema for the sessions_silver table.
    Used when creating empty DataFrames.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("session_key", "bigint"),
        ("session_type", "string"),
        ("session_name", "string"),
        ("meeting_key", "bigint"),
        ("grand_prix_name", "string"),
        ("date_start", "timestamp"),
        ("date_end", "timestamp"),
        ("year", "int"),
        ("session_duration_minutes", "int"),
        ("is_sprint_weekend", "boolean"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]


def detect_sprint_weekend(meeting_name: str) -> bool:
    """
    Detect if this is a sprint weekend based on meeting name patterns.
    
    Sprint weekends typically have:
    - "Sprint" in the meeting name
    - Specific Grand Prix locations known to host sprints
    
    Args:
        meeting_name: Meeting name from Bronze data
        
    Returns:
        True if sprint weekend, False otherwise
    """
    if not meeting_name:
        return False
    
    meeting_lower = meeting_name.lower()
    
    # Check for explicit sprint mention
    if "sprint" in meeting_lower:
        return True
    
    # Known sprint weekend locations for 2024/2025
    # These typically host sprint races
    sprint_locations = [
        "china", "shanghai",
        "miami",
        "austria", "spielberg",
        "united states", "austin", "cota",
        "brazil", "interlagos", "sao paulo",
        "qatar", "losail"
    ]
    
    # Check if any sprint location is in the meeting name
    for location in sprint_locations:
        if location in meeting_lower:
            return True
    
    return False
