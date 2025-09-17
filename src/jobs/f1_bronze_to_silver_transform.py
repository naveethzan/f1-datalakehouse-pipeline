"""
F1 Bronze to Silver Data Transformation - AWS Glue 5.0 Job

This is the main entry point for the Bronze to Silver transformation pipeline.
Processes F1 data from raw Bronze layer (Parquet) to cleaned Silver layer (Iceberg).

Supports two run modes:
1. HISTORICAL: Process entire year (2025) - all Grand Prix races
2. INCREMENTAL: Process specific Grand Prix - for weekly updates

AWS Glue Job Parameters:
    Required:
    --JOB_NAME: Job name (e.g., 'f1-bronze-to-silver-historical')
    --RUN_MODE: 'HISTORICAL' or 'INCREMENTAL'
    
    Optional:
    --GRAND_PRIX: Specific Grand Prix to process (e.g., 'silverstone')
                  Required for INCREMENTAL mode, auto-detected if not provided
    --YEAR: Year to process (default: 2025)
    --BRONZE_BUCKET: S3 bucket containing Bronze data (default: f1-data-lake-naveeth)
    --CATALOG_NAME: Glue catalog name (default: glue_catalog)
    --DATABASE_NAME: Target database name (default: f1_silver_db)
"""

import sys
import time
import logging
import unicodedata
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# PySpark imports
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# AWS Glue imports
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Configuration
from src.jobs.config.job_config import Config, WriteMode

# Utilities
from src.jobs.utils import normalize_grand_prix_name

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_spark_with_iceberg(job_args: Dict[str, str]) -> Tuple[SparkSession, GlueContext, Job]:
    """
    Initialize Spark with Iceberg configuration.
    
    CRITICAL: Configuration must be set BEFORE creating GlueContext.
    
    Args:
        job_args: Dictionary of job arguments
        
    Returns:
        Tuple of (SparkSession, GlueContext, Job)
    """
    logger.info("🚀 Initializing Spark with Iceberg configuration...")
    
    # Use centralized configuration
    bucket_name = Config.BRONZE_BUCKET
    catalog_name = Config.CATALOG_NAME
    
    # CRITICAL: Configure Spark BEFORE creating contexts
    conf = SparkConf()
    
    # Iceberg catalog configuration
    conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", Config.ICEBERG_WAREHOUSE)
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Configuration from config
    conf.set("spark.sql.adaptive.enabled", str(Config.SPARK_ADAPTIVE_ENABLED).lower())
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", str(Config.SPARK_ADAPTIVE_COALESCE_PARTITIONS).lower())
    conf.set("spark.sql.adaptive.skewJoin.enabled", str(Config.SPARK_ADAPTIVE_SKEW_JOIN).lower())
    conf.set("spark.sql.shuffle.partitions", str(Config.SPARK_SHUFFLE_PARTITIONS))
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(Config.SPARK_ADVISORY_PARTITION_SIZE_MB * 1024 * 1024))
    
    # Create contexts with configuration
    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    
    # Initialize Glue job
    job = Job(glue_context)
    job.init(job_args['JOB_NAME'], job_args)
    
    # Validate Iceberg support
    try:
        catalogs = spark.sql("SHOW CATALOGS").collect()
        logger.info(f"✅ Available catalogs: {[row['catalog'] for row in catalogs]}")
        logger.info("✅ Iceberg catalog configuration validated successfully")
    except Exception as e:
        logger.error(f"❌ Failed to validate Iceberg support: {str(e)}")
        raise
    
    return spark, glue_context, job


def detect_latest_grand_prix(spark: SparkSession, bronze_path: str, year: int) -> str:
    """
    Auto-detect the most recent Grand Prix from Bronze metadata.
    Used as fallback when GRAND_PRIX parameter is not provided for INCREMENTAL runs.
    
    Args:
        spark: SparkSession
        bronze_path: Base path to Bronze data
        year: Year to filter
        
    Returns:
        Normalized grand prix name
    """
    logger.info(f"🔍 Auto-detecting latest Grand Prix for year {year}...")
    
    try:
        # Read session_result Bronze data to find latest Grand Prix
        # This has enriched metadata including date_start
        session_df = spark.read.parquet(f"{bronze_path}/session_result/year={year}")
        
        # Get the most recent Grand Prix based on date_start
        latest_row = session_df.select("grand_prix", "date_start") \
                               .filter(col("date_start").isNotNull()) \
                               .orderBy(desc("date_start")) \
                               .first()
        
        if latest_row:
            latest_grand_prix = latest_row["grand_prix"]
            logger.info(f"✅ Detected latest Grand Prix: {latest_grand_prix}")
            return latest_grand_prix
        else:
            raise ValueError(f"No Grand Prix data found for year {year}")
            
    except Exception as e:
        logger.error(f"❌ Failed to detect latest Grand Prix: {str(e)}")
        raise


def create_silver_database(spark: SparkSession) -> None:
    """
    Create Silver database if it doesn't exist using centralized config.
    
    Args:
        spark: SparkSession with Iceberg configuration
    """
    catalog_name = Config.CATALOG_NAME
    database_name = Config.SILVER_DATABASE_NAME
    database_location = Config.DATABASE_LOCATION
    
    logger.info(f"📦 Creating database {catalog_name}.{database_name} if not exists...")
    
    # Create database with proper catalog prefix
    create_db_sql = f"""
        CREATE DATABASE IF NOT EXISTS {catalog_name}.{database_name}
        COMMENT 'F1 Silver layer - cleaned and normalized data'
        LOCATION '{database_location}'
    """
    
    try:
        spark.sql(create_db_sql)
        logger.info(f"✅ Database {catalog_name}.{database_name} ready")
        
        # Show existing tables
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{database_name}")
        table_count = tables_df.count()
        logger.info(f"   Database contains {table_count} tables")
        
    except Exception as e:
        logger.error(f"❌ Failed to create database: {str(e)}")
        raise


def get_table_write_mode(run_mode: str, table_name: str, data_scope: Dict[str, any]) -> str:
    """
    Get appropriate write mode for a specific table with special handling.
    
    NEW: Updated to support incremental SCD logic for drivers.
    
    Args:
        run_mode: 'HISTORICAL' or 'INCREMENTAL'
        table_name: Name of the Silver table
        data_scope: Data scope information from determine_data_scope
        
    Returns:
        Write mode string for this table and run mode
    """
    # Updated: Drivers transform now supports incremental SCD processing
    if table_name == 'drivers_silver':
        logger.info(f"   SCD Type 2 handling: {table_name} using incremental change detection")
        # Drivers uses SCD merge logic for both HISTORICAL and INCREMENTAL modes
        return Config.get_write_mode_for_table(run_mode, table_name)
    
    # Standard table-specific write mode
    return Config.get_write_mode_for_table(run_mode, table_name)


def get_transform_parameters(table_name: str, data_scope: Dict[str, any]) -> Dict[str, any]:
    """
    Get appropriate parameters for transform function calls.
    
    NEW: Updated to support incremental SCD logic for drivers.
    Drivers now properly support both HISTORICAL and INCREMENTAL modes.
    
    Args:
        table_name: Name of the Silver table
        data_scope: Data scope information from determine_data_scope
        
    Returns:
        Dictionary of parameters for transform function
    """
    # Updated: Drivers transform now supports both modes with new incremental SCD logic
    if table_name == 'drivers_silver':
        return {
            'year_filter': int(data_scope['year']),
            'grand_prix_filter': data_scope.get('grand_prix')  # Now properly respects run mode
        }
    
    # Standard parameters for all other transforms
    return {
        'year_filter': int(data_scope['year']),
        'grand_prix_filter': data_scope.get('grand_prix')
    }


def determine_data_scope(job_args: Dict[str, str], spark: SparkSession) -> Dict[str, any]:
    """
    Determine which Bronze data to process based on run mode.
    
    Updated for Phase 2: Removed deprecated write_mode - now handled per-table.
    
    Args:
        job_args: Job arguments including RUN_MODE
        spark: SparkSession for auto-detection if needed
        
    Returns:
        Dictionary with filter conditions and metadata (no write_mode)
    """
    run_mode = job_args['RUN_MODE']
    year = job_args.get('YEAR', str(Config.DEFAULT_YEAR))
    bronze_path = Config.BRONZE_PATH
    
    if run_mode == Config.RUN_MODE_HISTORICAL:
        # Process entire year - all Grand Prix races
        return {
            'year': year,
            'grand_prix': None,  # All Grand Prix
            'filter_condition': f"year = {year}",
            'description': f"All Grand Prix data for {year}"
        }
        
    elif run_mode == Config.RUN_MODE_INCREMENTAL:
        # Process specific Grand Prix
        # Use provided GRAND_PRIX or auto-detect latest
        grand_prix = job_args.get('GRAND_PRIX')
        
        if not grand_prix:
            # Auto-detect latest Grand Prix as fallback
            grand_prix = detect_latest_grand_prix(spark, bronze_path, int(year))
        else:
            # Normalize the provided Grand Prix name
            grand_prix = normalize_grand_prix_name(grand_prix)
        
        return {
            'year': year,
            'grand_prix': grand_prix,
            'filter_condition': f"year = {year} AND grand_prix = '{grand_prix}'",
            'description': f"Data for {grand_prix} Grand Prix in {year}"
        }
        
    else:
        raise ValueError(f"Invalid RUN_MODE: {run_mode}. Must be '{Config.RUN_MODE_HISTORICAL}' or '{Config.RUN_MODE_INCREMENTAL}'")


def process_bronze_to_silver(spark: SparkSession, glue_context: GlueContext, 
                            job_args: Dict[str, str]) -> Dict[str, int]:
    """
    Main orchestration function for Bronze to Silver transformation.
    
    Args:
        spark: SparkSession with Iceberg configuration
        glue_context: AWS Glue context
        job_args: Job arguments
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {}
    
    # Use centralized configuration
    catalog_name = Config.CATALOG_NAME
    database_name = Config.SILVER_DATABASE_NAME
    bronze_path = Config.BRONZE_PATH
    
    # Determine data scope based on run mode
    data_scope = determine_data_scope(job_args, spark)
    
    logger.info("=" * 80)
    logger.info("🏎️ F1 Bronze to Silver Transformation")
    logger.info("=" * 80)
    logger.info(f"   Run Mode: {job_args['RUN_MODE']}")
    logger.info(f"   Data Scope: {data_scope['description']}")
    logger.info(f"   Bronze Path: {bronze_path}")
    logger.info(f"   Target Database: {catalog_name}.{database_name}")
    logger.info(f"   Write Strategy: Table-specific modes (Phase 2)")
    logger.info("=" * 80)
    
    # Create database if not exists
    create_silver_database(spark)
    
    # Import transformation modules with consistent signatures
    try:
        # Import all transformations using absolute paths
        from src.jobs.transforms.silver import (
            transform_sessions,
            transform_drivers,
            transform_qualifying_results,
            transform_race_results,
            transform_laps,
            transform_laps_chunked,
            transform_pitstops
        )
        
        # Import utilities using absolute paths
        from src.jobs.utils import IcebergTableManager, write_to_iceberg
        
        # Initialize table manager  
        table_manager = IcebergTableManager(spark, catalog_name, database_name, Config.BRONZE_BUCKET)
        
        # Create all Silver tables if they don't exist
        logger.info("📋 Ensuring Silver layer tables exist...")
        table_manager.create_all_silver_tables()
        
    except ImportError as e:
        logger.error(f"❌ Required transformation modules not found: {str(e)}")
        logger.error("   All transformation modules must be implemented for the pipeline to work")
        raise
    
    # Transformation configuration
    transformations = [
        {
            "name": "sessions",
            "transform_func": transform_sessions,
            "target_table": "sessions_silver"
        },
        {
            "name": "drivers", 
            "transform_func": transform_drivers,
            "target_table": "drivers_silver"
        },
        {
            "name": "qualifying_results",
            "transform_func": transform_qualifying_results,
            "target_table": "qualifying_results_silver"
        },
        {
            "name": "race_results",
            "transform_func": transform_race_results,
            "target_table": "race_results_silver"
        },
        {
            "name": "laps",
            "transform_func": transform_laps,
            "target_table": "laps_silver",
            "chunked": True  # Special handling for volume
        },
        {
            "name": "pitstops",
            "transform_func": transform_pitstops,
            "target_table": "pitstops_silver"
        }
    ]
    
    # Execute transformations
    failed_transformations = []
    
    for transform_config in transformations:
        transform_name = transform_config['name']
        table_name = transform_config['target_table']
        start_time = datetime.now()
        
        try:
            # Get table-specific write mode and parameters
            write_mode = get_table_write_mode(job_args['RUN_MODE'], table_name, data_scope)
            transform_params = get_transform_parameters(table_name, data_scope)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Processing: {transform_name}")
            logger.info(f"   Target Table: {table_name}")
            logger.info(f"   Write Mode: {write_mode}")
            logger.info(f"   Parameters: year={transform_params['year_filter']}, gp={transform_params['grand_prix_filter']}")
            logger.info(f"{'='*60}")
            
            # Check for chunked processing (primarily for HISTORICAL mode with high volume)
            # Note: Chunked processing now supports both HISTORICAL (all GPs) and INCREMENTAL (single GP)
            if (Config.requires_chunked_processing(table_name) and 
                job_args['RUN_MODE'] == Config.RUN_MODE_HISTORICAL):
                # Use chunked processing for historical laps loads
                gp_description = transform_params['grand_prix_filter'] or 'all Grand Prix'
                logger.info(f"📦 Using chunked processing for {transform_name} ({gp_description})")
                
                # Create a write function compatible with transform_laps_chunked expectations
                def chunked_write_function(df, chunk_table_name, chunk_write_mode):
                    return write_to_iceberg(
                        df, chunk_table_name, chunk_write_mode, catalog_name, database_name
                    )
                
                results = transform_laps_chunked(
                    spark, 
                    bronze_path, 
                    transform_params['year_filter'], 
                    chunked_write_function,
                    transform_params['grand_prix_filter']  # Pass GP filter for INCREMENTAL mode support
                )
                
                # Calculate total records from chunked results
                total_records = sum(r.get('records', 0) for r in results.values())
                stats[transform_name] = total_records
                
            else:
                # Standard processing for all other transforms
                transformed_df = transform_config['transform_func'](
                    spark, 
                    bronze_path, 
                    transform_params['year_filter'], 
                    transform_params['grand_prix_filter']
                )
                
                record_count = transformed_df.count()
                
                if record_count == 0:
                    logger.warning(f"⚠️ No data returned for {transform_name}")
                else:
                    logger.info(f"📊 Transformation returned {record_count:,} records")
                
                # Write to Silver layer with table-specific write mode
                # Pass SparkSession for SCD merge operations (drivers table)
                write_to_iceberg(
                    transformed_df,
                    table_name,
                    write_mode,
                    catalog_name,
                    database_name,
                    spark  # Required for merge_scd2 mode
                )
                
                stats[transform_name] = record_count
            
            # Log completion time
            duration = (datetime.now() - start_time).total_seconds()
            logger.info(f"✅ {transform_name} completed in {duration:.1f} seconds")
            
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            logger.error(f"❌ Failed to process {transform_name} after {duration:.1f} seconds: {str(e)}")
            
            # Log detailed error information
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            stats[transform_name] = 0
            failed_transformations.append(transform_name)
            
            # Stop pipeline if core dependencies fail
            if transform_name in ['sessions', 'drivers']:
                logger.error(f"💥 Critical transformation {transform_name} failed - stopping pipeline")
                raise
            
            # Continue with other transformations for non-critical failures
            logger.warning(f"⏭️ Continuing with remaining transformations")
            continue
    
    # Log summary of any failures
    if failed_transformations:
        logger.warning(f"\n⚠️ {len(failed_transformations)} transformation(s) failed: {', '.join(failed_transformations)}")
    
    return stats


def main():
    """
    Main entry point for the Glue job.
    """
    start_time = datetime.now()
    
    # Parse job arguments - only runtime-specific parameters
    # Required arguments
    required_args = ['JOB_NAME', 'RUN_MODE']
    # Optional arguments  
    optional_args = ['GRAND_PRIX', 'YEAR']
    
    args = getResolvedOptions(sys.argv, required_args, optional_args=optional_args)
    
    # Set defaults from Config for optional arguments
    args['YEAR'] = args.get('YEAR', str(Config.DEFAULT_YEAR))
    args['GRAND_PRIX'] = args.get('GRAND_PRIX', None)  # None triggers auto-detection for INCREMENTAL
    
    # Validate RUN_MODE
    valid_run_modes = [Config.RUN_MODE_HISTORICAL, Config.RUN_MODE_INCREMENTAL]
    if args['RUN_MODE'] not in valid_run_modes:
        raise ValueError(f"Invalid RUN_MODE: {args['RUN_MODE']}. Must be one of {valid_run_modes}")
    
    logger.info(f"🏁 Starting F1 Bronze to Silver Job")
    logger.info(f"   Job Name: {args['JOB_NAME']}")
    logger.info(f"   Run Mode: {args['RUN_MODE']}")
    logger.info(f"   Year: {args['YEAR']}")
    if args['GRAND_PRIX']:
        logger.info(f"   Grand Prix: {args['GRAND_PRIX']}")
    
    try:
        # Initialize Spark with Iceberg
        spark, glue_context, job = initialize_spark_with_iceberg(args)
        
        # Process Bronze to Silver transformation
        stats = process_bronze_to_silver(spark, glue_context, args)
        
        # Log summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("📊 Transformation Summary:")
        logger.info("=" * 80)
        
        total_records = 0
        for table_name, record_count in stats.items():
            logger.info(f"   {table_name}: {record_count:,} records")
            total_records += record_count
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\n   Total records processed: {total_records:,}")
        logger.info(f"   Execution time: {duration:.2f} seconds")
        logger.info("=" * 80)
        
        # Commit the job
        job.commit()
        logger.info("✅ Job completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Job failed with error: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Cleanup
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()
