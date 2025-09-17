"""
Laps Transformation for F1 Bronze to Silver Pipeline

High-performance transformation optimized for volume (25,000+ records/year).
Reads directly from Bronze parquet files and writes to Silver Iceberg tables.
Compatible with AWS Glue 5.0 + Iceberg + Glue Data Catalog.

Key transformations:
- Calculate lap times from sectors when lap_duration is null
- Format lap times as readable strings and milliseconds
- Calculate personal best flags (within driver)
- Calculate session fastest lap flag
- Process by Grand Prix chunks for memory efficiency
- RACE sessions only (no qualifying)

Volume: ~1,000-1,400 records per race × 25 races = ~25,000 records/year
"""

import logging
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, coalesce,
    round as spark_round, floor, concat, lpad,
    min as spark_min, row_number
)
from pyspark.sql.window import Window

# Import from utils package using absolute imports
from src.jobs.utils import add_audit_columns

# Setup logging
logger = logging.getLogger(__name__)


def transform_laps(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Transform Bronze laps data to Silver format.
    
    High-performance transformation designed for volume processing.
    Uses Grand Prix chunking for memory efficiency.
    Only processes RACE laps (not qualifying).
    
    Args:
        spark: Active Spark session with Iceberg configuration
        bronze_path: S3 path to Bronze data (e.g., s3://bucket/bronze)
        year_filter: Year to process (e.g., 2025)
        grand_prix_filter: Specific Grand Prix to process (for chunking)
        
    Returns:
        Transformed DataFrame ready for Iceberg write
    """
    logger.info("=" * 60)
    logger.info("Starting Laps Transformation (High Volume)")
    logger.info(f"   Year: {year_filter}")
    logger.info(f"   Grand Prix: {grand_prix_filter or 'ALL'}")
    logger.info("=" * 60)
    
    # Step 1: Read Bronze laps data (race only) with partition filtering
    laps_df = read_bronze_laps(spark, bronze_path, year_filter, grand_prix_filter)
    
    if laps_df.count() == 0:
        logger.warning("⚠️ No laps data found for specified filters")
        return spark.createDataFrame([], get_laps_schema())
    
    # Cache for multiple operations
    laps_df = laps_df.cache()
    
    try:
        # Step 2: Calculate and format lap times
        time_df = calculate_lap_times(laps_df)
        
        # Step 3: Calculate performance flags
        flags_df = calculate_lap_flags(time_df)
        
        # Step 4: Clean and add audit columns
        final_df = finalize_laps_data(flags_df)
        
        record_count = final_df.count()
        logger.info(f"✅ Laps transformation complete: {record_count:,} records")
        
        return final_df
        
    finally:
        # Release cache to free memory
        laps_df.unpersist()


def read_bronze_laps(
    spark: SparkSession,
    bronze_path: str,
    year_filter: int,
    grand_prix_filter: Optional[str] = None
) -> DataFrame:
    """
    Read Bronze laps data with partition pruning for performance.
    Only reads RACE laps (session_type=race).
    
    Args:
        spark: Spark session
        bronze_path: Base Bronze S3 path
        year_filter: Year to filter
        grand_prix_filter: Optional Grand Prix to filter (for chunking)
        
    Returns:
        Raw Bronze laps DataFrame
    """
    # Build path with race-only filter
    if grand_prix_filter:
        # Specific Grand Prix (used in chunked processing)
        path = f"{bronze_path}/laps/year={year_filter}/grand_prix={grand_prix_filter}/session_type=race"
        logger.info(f"📂 Reading race laps from: {path}")
    else:
        # All Grand Prix for the year (used in full processing)
        path = f"{bronze_path}/laps/year={year_filter}/*/session_type=race"
        logger.info(f"📂 Reading all race laps for year {year_filter}")
    
    try:
        df = spark.read.parquet(path)
        
        # Drop heavy arrays early
        if "segments_sector_1" in df.columns:
            df = df.drop("segments_sector_1", "segments_sector_2", "segments_sector_3")
        
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
        
        logger.info(f"📊 Loaded {df.count():,} lap records")
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to read Bronze laps data: {e}")
        raise


def calculate_lap_times(df: DataFrame) -> DataFrame:
    """
    Calculate total lap time and format as string.
    
    Priority:
    1. Use lap_duration if available
    2. Calculate from sectors (sector_1 + sector_2 + sector_3)
    3. NULL if incomplete data
    
    Args:
        df: Raw laps DataFrame
        
    Returns:
        DataFrame with calculated lap times
    """
    logger.info("⏱️ Calculating lap times from duration and sectors")
    
    # Calculate total lap time in milliseconds
    df_with_time = df.withColumn(
        "lap_time_millis",
        when(
            col("lap_duration").isNotNull(),
            # Use lap_duration directly (convert seconds to milliseconds)
            spark_round(col("lap_duration") * 1000, 0).cast("bigint")
        ).when(
            # Fall back to sector sum if available
            (col("duration_sector_1").isNotNull() & 
             col("duration_sector_2").isNotNull() & 
             col("duration_sector_3").isNotNull()),
            spark_round(
                (col("duration_sector_1") + col("duration_sector_2") + col("duration_sector_3")) * 1000, 
                0
            ).cast("bigint")
        ).otherwise(lit(None))  # Incomplete lap (pit out, etc.)
    )
    
    # Format lap time as readable string (MM:SS.mmm)
    df_with_formatted = df_with_time.withColumn(
        "minutes", floor(col("lap_time_millis") / 60000).cast("int")
    ).withColumn(
        "seconds_decimal", (col("lap_time_millis") % 60000) / 1000
    ).withColumn(
        "lap_time",
        when(
            col("lap_time_millis").isNotNull(),
            concat(
                col("minutes").cast("string"),
                lit(":"),
                # Format seconds with leading zero and 3 decimal places
                lpad(spark_round(col("seconds_decimal"), 3).cast("string"), 6, "0")
            )
        ).otherwise(lit(None))
    ).drop("minutes", "seconds_decimal")
    
    return df_with_formatted


def calculate_lap_flags(df: DataFrame) -> DataFrame:
    """
    Calculate performance flags for each lap.
    
    Flags calculated:
    - is_personal_best: Fastest lap for this driver in this session
    - is_fastest_lap: Fastest lap overall in this session
    
    Uses window functions for efficient calculation.
    
    Args:
        df: DataFrame with lap times
        
    Returns:
        DataFrame with performance flags
    """
    logger.info("🏁 Calculating personal best and fastest lap flags")
    
    # Window specifications for calculations
    driver_window = Window.partitionBy("session_key", "driver_number")
    session_window = Window.partitionBy("session_key")
    
    # Add personal best flag
    df_with_pb = df.withColumn(
        "driver_fastest_time",
        spark_min("lap_time_millis").over(driver_window)
    ).withColumn(
        "is_personal_best",
        when(
            col("lap_time_millis").isNotNull() & 
            (col("lap_time_millis") == col("driver_fastest_time")),
            lit(True)
        ).otherwise(lit(False))
    ).drop("driver_fastest_time")
    
    # Add fastest lap flag (fastest lap in entire session)
    df_with_fastest = df_with_pb.withColumn(
        "session_fastest_time",
        spark_min("lap_time_millis").over(session_window)
    ).withColumn(
        "is_fastest_lap",
        when(
            col("lap_time_millis").isNotNull() & 
            (col("lap_time_millis") == col("session_fastest_time")),
            lit(True)
        ).otherwise(lit(False))
    ).drop("session_fastest_time")
    
    # Handle ties: if multiple laps have same time, only mark first occurrence
    # This prevents multiple laps being marked as "fastest"
    tie_breaker_window = (Window.partitionBy("session_key", "lap_time_millis")
                         .orderBy("driver_number", "lap_number"))
    
    df_with_tie_breaker = df_with_fastest.withColumn(
        "tie_rank",
        row_number().over(tie_breaker_window)
    ).withColumn(
        "is_fastest_lap",
        when(
            col("is_fastest_lap") & (col("tie_rank") == 1),
            lit(True)
        ).otherwise(lit(False))
    ).drop("tie_rank")
    
    return df_with_tie_breaker


def finalize_laps_data(df: DataFrame) -> DataFrame:
    """
    Clean data and add audit columns for Silver layer.
    
    Note: Complex fields (position_at_lap, gaps, tire_compound) are NULL
    and will be populated in Gold layer for advanced analytics.
    
    Args:
        df: Transformed laps DataFrame
        
    Returns:
        Final DataFrame ready for Iceberg write
    """
    logger.info("🧹 Finalizing laps data for Silver layer")
    
    # Select and cast columns according to Silver schema
    final_df = df.select(
        col("session_key").cast("bigint"),
        col("driver_number").cast("int"),
        col("lap_number").cast("int"),
        col("lap_time"),  # Already string
        col("lap_time_millis"),  # Already bigint
        
        # Complex calculations deferred to Gold layer
        lit(None).cast("int").alias("position_at_lap"),
        lit(None).cast("bigint").alias("gap_to_leader_millis"),
        lit(None).cast("bigint").alias("interval_to_ahead_millis"),
        
        # Performance flags
        col("is_personal_best").cast("boolean"),
        col("is_fastest_lap").cast("boolean"),
        
        # Strategy data (not available in laps endpoint)
        lit(None).cast("string").alias("tire_compound"),
        lit(None).cast("string").alias("track_status"),
        
        # Partition and metadata columns
        col("year").cast("int"),
        col("grand_prix").alias("grand_prix_name"),
        lit("race").cast("string").alias("session_type")  # Always race for laps
        
    ).filter(
        # Data quality: ensure key columns are not null
        col("session_key").isNotNull() &
        col("driver_number").isNotNull() &
        col("lap_number").isNotNull()
    )
    
    # Add audit columns using utils function
    return add_audit_columns(final_df)


def get_laps_schema():
    """
    Get the schema for the laps_silver table.
    Used when creating empty DataFrames.
    
    Returns:
        List of (column_name, data_type) tuples
    """
    return [
        ("session_key", "bigint"),
        ("driver_number", "int"),
        ("lap_number", "int"),
        ("lap_time", "string"),
        ("lap_time_millis", "bigint"),
        ("position_at_lap", "int"),
        ("gap_to_leader_millis", "bigint"),
        ("interval_to_ahead_millis", "bigint"),
        ("is_personal_best", "boolean"),
        ("is_fastest_lap", "boolean"),
        ("tire_compound", "string"),
        ("track_status", "string"),
        ("year", "int"),
        ("grand_prix_name", "string"),
        ("session_type", "string"),
        ("created_timestamp", "timestamp"),
        ("updated_timestamp", "timestamp")
    ]


def get_grand_prix_list(spark: SparkSession, bronze_path: str, year: int) -> list:
    """
    Get list of Grand Prix for chunked processing.
    
    Used for historical loads to process one GP at a time for memory efficiency.
    
    Args:
        spark: Spark session
        bronze_path: Bronze S3 path
        year: Year to get GPs for
        
    Returns:
        List of grand_prix names
    """
    try:
        # Read partition structure to get available Grand Prix
        path = f"{bronze_path}/laps/year={year}"
        df = spark.read.option("recursiveFileLookup", "false").parquet(path)
        
        # Extract unique grand_prix values
        gp_list = [row['grand_prix'] for row in 
                  df.select('grand_prix').distinct().collect()]
        
        logger.info(f"Found {len(gp_list)} Grand Prix for chunked processing: {gp_list}")
        return sorted(gp_list)
        
    except Exception as e:
        logger.error(f"❌ Failed to get Grand Prix list: {e}")
        return []


def transform_laps_chunked(
    spark: SparkSession,
    bronze_path: str,
    year: int,
    write_function,
    grand_prix_filter: Optional[str] = None
) -> dict:
    """
    Process Grand Prix in chunks for memory efficiency.
    
    Used for both historical loads (all GPs) and incremental loads (single GP)
    to avoid memory issues with large lap datasets.
    
    Args:
        spark: Spark session
        bronze_path: Bronze S3 path
        year: Year to process
        write_function: Function to write DataFrame to Iceberg
        grand_prix_filter: Optional specific Grand Prix to process (for INCREMENTAL mode)
        
    Returns:
        Dictionary with processing results per GP
    """
    if grand_prix_filter:
        logger.info(f"Starting chunked laps processing for {grand_prix_filter} GP in {year}")
        gp_list = [grand_prix_filter]  # Single GP for INCREMENTAL mode
    else:
        logger.info(f"Starting chunked laps processing for all GPs in year {year}")
        gp_list = get_grand_prix_list(spark, bronze_path, year)  # All GPs for HISTORICAL mode
    
    results = {}
    
    for grand_prix in gp_list:
        try:
            logger.info(f"Processing chunk: {grand_prix}")
            
            # Transform single GP
            laps_df = transform_laps(spark, bronze_path, year, grand_prix)
            
            # Write immediately
            write_function(laps_df, "laps_silver", "append")
            
            results[grand_prix] = {
                'status': 'success',
                'records': laps_df.count()
            }
            
            logger.info(f"Completed {grand_prix}: {results[grand_prix]['records']:,} records")
            
        except Exception as e:
            logger.error(f"❌ Failed processing {grand_prix}: {e}")
            results[grand_prix] = {
                'status': 'failed',
                'error': str(e)
            }
    
    # Summary
    successful = sum(1 for r in results.values() if r['status'] == 'success')
    total_records = sum(r.get('records', 0) for r in results.values())
    
    mode_description = f"{grand_prix_filter} GP" if grand_prix_filter else "all GPs"
    logger.info(f"Chunked processing complete for {mode_description}:")
    logger.info(f"   Successful: {successful}/{len(gp_list)} Grand Prix")
    logger.info(f"   Total records: {total_records:,}")
    
    return results
