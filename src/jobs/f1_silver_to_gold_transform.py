"""
F1 Silver to Gold Data Transformation - AWS Glue 5.0 Job

This is the main entry point for the Silver to Gold transformation pipeline.
Processes F1 data from cleaned Silver layer (Iceberg) to analytics Gold layer (Iceberg).

Supports two run modes:
1. HISTORICAL: Process entire year (2025) - all Grand Prix races
2. INCREMENTAL: Process specific Grand Prix - for weekly updates

AWS Glue Job Parameters:
    Required:
    --JOB_NAME: Job name (e.g., 'f1-silver-to-gold-historical')
    --RUN_MODE: 'HISTORICAL' or 'INCREMENTAL'
    
    Optional:
    --GRAND_PRIX: Specific Grand Prix to process (e.g., 'silverstone')
                  Required for INCREMENTAL mode, auto-detected if not provided
    --YEAR: Year to process (default: 2025)
    --CATALOG_NAME: Glue catalog name (default: glue_catalog)
    --SILVER_DATABASE: Source database name (default: f1_silver_db)
    --GOLD_DATABASE: Target database name (default: f1_gold_db)
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
from src.jobs.utils import normalize_grand_prix_name, IcebergTableManager, write_to_iceberg
from src.jobs.utils.data_access.silver_readers import validate_silver_data
from src.jobs.utils.table_management.schemas import get_gold_table_names, generate_gold_table_ddl

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_spark_with_iceberg(job_args: Dict[str, str]) -> Tuple[SparkSession, GlueContext, Job]:
    """
    Initialize Spark with Iceberg configuration for Gold layer processing.
    
    CRITICAL: Configuration must be set BEFORE creating GlueContext.
    
    Args:
        job_args: Dictionary of job arguments
        
    Returns:
        Tuple of (SparkSession, GlueContext, Job)
    """
    logger.info("🚀 Initializing Spark with Iceberg configuration for Gold layer...")
    
    # Use centralized configuration
    catalog_name = Config.CATALOG_NAME
    
    # CRITICAL: Configure Spark BEFORE creating contexts
    conf = SparkConf()
    
    # Iceberg catalog configuration
    conf.set(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.{catalog_name}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set(f"spark.sql.catalog.{catalog_name}.warehouse", Config.ICEBERG_WAREHOUSE)
    conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    
    # Configuration for Gold layer analytics
    conf.set("spark.sql.adaptive.enabled", str(Config.SPARK_ADAPTIVE_ENABLED).lower())
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", str(Config.SPARK_ADAPTIVE_COALESCE_PARTITIONS).lower())
    conf.set("spark.sql.adaptive.skewJoin.enabled", str(Config.SPARK_ADAPTIVE_SKEW_JOIN).lower())
    conf.set("spark.sql.shuffle.partitions", str(Config.SPARK_SHUFFLE_PARTITIONS))
    conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(Config.SPARK_ADVISORY_PARTITION_SIZE_MB * 1024 * 1024))
    
    # Gold layer specific configuration
    conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")
    
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


def detect_latest_grand_prix_from_silver(spark: SparkSession, silver_database: str, year: int) -> str:
    """
    Auto-detect the most recent Grand Prix from Silver layer metadata.
    Used as fallback when GRAND_PRIX parameter is not provided for INCREMENTAL runs.
    
    Args:
        spark: SparkSession
        silver_database: Silver database name
        year: Year to filter
        
    Returns:
        Normalized grand prix name
    """
    logger.info(f"🔍 Auto-detecting latest Grand Prix from Silver layer (year={year})...")
    
    try:
        # Use sessions_silver to find latest Grand Prix based on date_start
        catalog_name = Config.CATALOG_NAME
        sessions_table = f"{catalog_name}.{silver_database}.sessions_silver"
        
        sessions_df = spark.table(sessions_table)
        
        # Get the most recent Grand Prix based on date_start
        latest_row = sessions_df.select("grand_prix_name", "date_start") \
                               .filter(col("date_start").isNotNull()) \
                               .filter(col("year") == year) \
                               .filter(col("session_type") == "Race") \
                               .orderBy(desc("date_start")) \
                               .first()
        
        if latest_row:
            latest_grand_prix = latest_row["grand_prix_name"]
            logger.info(f"✅ Detected latest Grand Prix: {latest_grand_prix}")
            return latest_grand_prix
        else:
            raise ValueError(f"No Grand Prix race data found in Silver layer for year {year}")
            
    except Exception as e:
        logger.error(f"❌ Failed to detect latest Grand Prix from Silver: {str(e)}")
        raise


def create_gold_database(spark: SparkSession, gold_database: str) -> None:
    """
    Create Gold database if it doesn't exist using centralized config.
    
    Args:
        spark: SparkSession with Iceberg configuration
        gold_database: Gold database name
    """
    catalog_name = Config.CATALOG_NAME
    # Use Gold-specific location
    database_location = f"s3://{Config.BRONZE_BUCKET}/gold/"
    
    logger.info(f"📦 Creating Gold database {catalog_name}.{gold_database} if not exists...")
    
    # Create database with proper catalog prefix
    create_db_sql = f"""
        CREATE DATABASE IF NOT EXISTS {catalog_name}.{gold_database}
        COMMENT 'F1 Gold layer - analytics and business intelligence data'
        LOCATION '{database_location}'
    """
    
    try:
        spark.sql(create_db_sql)
        logger.info(f"✅ Gold database {catalog_name}.{gold_database} ready")
        
        # Show existing tables
        tables_df = spark.sql(f"SHOW TABLES IN {catalog_name}.{gold_database}")
        table_count = tables_df.count()
        logger.info(f"   Database contains {table_count} tables")
        
    except Exception as e:
        logger.error(f"❌ Failed to create Gold database: {str(e)}")
        raise


def get_gold_table_write_mode(run_mode: str, table_name: str, data_scope: Dict[str, any]) -> str:
    """
    Get appropriate write mode for a specific Gold table based on partitioning strategy.
    
    Args:
        run_mode: 'HISTORICAL' or 'INCREMENTAL'
        table_name: Name of the Gold table
        data_scope: Data scope information from determine_data_scope
        
    Returns:
        Write mode string for this table and run mode
    """
    # Championship tracker uses year-only partitioning
    if table_name == 'championship_tracker':
        if run_mode == Config.RUN_MODE_INCREMENTAL:
            # For incremental, we still need to overwrite the entire year 
            # because championship standings are cumulative
            logger.info(f"   Championship tracker: {table_name} using year overwrite for cumulative data")
            return "overwrite"  # Overwrite entire table for championship recalculation
        else:
            return "overwrite"  # Historical mode overwrites everything
    
    # All other Gold tables use year + grand_prix_name partitioning
    else:
        if run_mode == Config.RUN_MODE_INCREMENTAL:
            # Incremental mode: overwrite specific year + grand_prix partition
            return "overwrite_partitions"
        else:
            # Historical mode: overwrite entire table
            return "overwrite"


def get_gold_transform_parameters(table_name: str, data_scope: Dict[str, any]) -> Dict[str, any]:
    """
    Get appropriate parameters for Gold transform function calls.
    
    Args:
        table_name: Name of the Gold table
        data_scope: Data scope information from determine_data_scope
        
    Returns:
        Dictionary of parameters for transform function
    """
    # All Gold transforms use the same parameters
    return {
        'year': int(data_scope['year']),
        'grand_prix_filter': data_scope.get('grand_prix'),
        'processing_mode': 'historical' if data_scope.get('grand_prix') is None else 'incremental'
    }


def determine_gold_data_scope(job_args: Dict[str, str], spark: SparkSession, silver_database: str) -> Dict[str, any]:
    """
    Determine which Silver data to process for Gold transformation based on run mode.
    
    Args:
        job_args: Job arguments including RUN_MODE
        spark: SparkSession for auto-detection if needed
        silver_database: Silver database name for auto-detection
        
    Returns:
        Dictionary with filter conditions and metadata
    """
    run_mode = job_args['RUN_MODE']
    year = job_args.get('YEAR', str(Config.DEFAULT_YEAR))
    
    if run_mode == Config.RUN_MODE_HISTORICAL:
        # Process entire year - all Grand Prix races
        return {
            'year': year,
            'grand_prix': None,  # All Grand Prix
            'filter_condition': f"year = {year}",
            'description': f"All Grand Prix analytics for {year}"
        }
        
    elif run_mode == Config.RUN_MODE_INCREMENTAL:
        # Process specific Grand Prix
        # Use provided GRAND_PRIX or auto-detect latest from Silver
        grand_prix = job_args.get('GRAND_PRIX')
        
        if not grand_prix:
            # Auto-detect latest Grand Prix from Silver layer
            grand_prix = detect_latest_grand_prix_from_silver(spark, silver_database, int(year))
        else:
            # Normalize the provided Grand Prix name
            grand_prix = normalize_grand_prix_name(grand_prix)
        
        return {
            'year': year,
            'grand_prix': grand_prix,
            'filter_condition': f"year = {year} AND grand_prix_name = '{grand_prix}'",
            'description': f"Analytics for {grand_prix} Grand Prix in {year}"
        }
        
    else:
        raise ValueError(f"Invalid RUN_MODE: {run_mode}. Must be '{Config.RUN_MODE_HISTORICAL}' or '{Config.RUN_MODE_INCREMENTAL}'")


def process_silver_to_gold(spark: SparkSession, glue_context: GlueContext, 
                          job_args: Dict[str, str]) -> Dict[str, int]:
    """
    Main orchestration function for Silver to Gold transformation.
    
    Args:
        spark: SparkSession with Iceberg configuration
        glue_context: AWS Glue context
        job_args: Job arguments
        
    Returns:
        Dictionary with processing statistics
    """
    stats = {}
    
    # Use configuration
    catalog_name = Config.CATALOG_NAME
    silver_database = job_args.get('SILVER_DATABASE', 'f1_silver_db')
    gold_database = job_args.get('GOLD_DATABASE', 'f1_gold_db')
    
    # Determine data scope based on run mode
    data_scope = determine_gold_data_scope(job_args, spark, silver_database)
    
    logger.info("=" * 80)
    logger.info("🏁 F1 Silver to Gold Transformation")
    logger.info("=" * 80)
    logger.info(f"   Run Mode: {job_args['RUN_MODE']}")
    logger.info(f"   Data Scope: {data_scope['description']}")
    logger.info(f"   Source Database: {catalog_name}.{silver_database}")
    logger.info(f"   Target Database: {catalog_name}.{gold_database}")
    logger.info(f"   Write Strategy: Table-specific modes based on partitioning")
    logger.info("=" * 80)
    
    # Create Gold database if not exists
    create_gold_database(spark, gold_database)
    
    # Import Gold transformation modules
    try:
        from src.jobs.transforms.gold import (
            DriverPerformanceSummaryQualifyingTransform,
            DriverPerformanceSummaryRaceTransform,
            ChampionshipTrackerTransform,
            RaceWeekendInsightsTransform
        )
        
        # Initialize table manager for Gold layer
        table_manager = IcebergTableManager(spark, catalog_name, gold_database, Config.BRONZE_BUCKET)
        
        # Create all Gold tables if they don't exist
        logger.info("📋 Ensuring Gold layer tables exist...")
        table_manager.create_all_gold_tables()
        
    except ImportError as e:
        logger.error(f"❌ Required Gold transformation modules not found: {str(e)}")
        logger.error("   All Gold transformation modules must be implemented for the pipeline to work")
        raise
    
    # Gold transformation configuration in dependency order
    # Critical transforms: qualifying and race
    # Non-critical: championship and weekend insights (derived analytics)
    transformations = [
        {
            "name": "driver_performance_summary_qualifying",
            "transform_class": DriverPerformanceSummaryQualifyingTransform,
            "target_table": "driver_performance_summary_qualifying",
            "critical": True
        },
        {
            "name": "driver_performance_summary_race", 
            "transform_class": DriverPerformanceSummaryRaceTransform,
            "target_table": "driver_performance_summary_race",
            "critical": True
        },
        {
            "name": "championship_tracker",
            "transform_class": ChampionshipTrackerTransform,
            "target_table": "championship_tracker",
            "critical": False  # Derived analytics
        },
        {
            "name": "race_weekend_insights",
            "transform_class": RaceWeekendInsightsTransform,
            "target_table": "race_weekend_insights",
            "critical": False  # Derived analytics
        }
    ]
    
    # Execute transformations in dependency order
    failed_transformations = []
    
    for transform_config in transformations:
        transform_name = transform_config['name']
        table_name = transform_config['target_table']
        transform_class = transform_config['transform_class']
        is_critical = transform_config['critical']
        start_time = datetime.now()
        
        try:
            # Get table-specific write mode and parameters
            write_mode = get_gold_table_write_mode(job_args['RUN_MODE'], table_name, data_scope)
            transform_params = get_gold_transform_parameters(table_name, data_scope)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔄 Processing: {transform_name}")
            logger.info(f"   Target Table: {table_name}")
            logger.info(f"   Write Mode: {write_mode}")
            logger.info(f"   Critical Transform: {is_critical}")
            logger.info(f"   Parameters: year={transform_params['year']}, gp={transform_params['grand_prix_filter']}")
            logger.info(f"{'='*60}")
            
            # Initialize transform instance
            transformer = transform_class()
            
            # Step 1: Read source Silver data
            source_data = transformer.read_source_data(
                spark=spark,
                year=transform_params['year'],
                grand_prix_filter=transform_params['grand_prix_filter'],
                processing_mode=transform_params['processing_mode']
            )
            
            # Step 2: Execute transformation
            transformed_df = transformer.transform(source_data)
            
            record_count = transformed_df.count()
            
            if record_count == 0:
                logger.warning(f"⚠️ No data returned for {transform_name}")
            else:
                logger.info(f"📊 Transformation returned {record_count:,} records")
            
            # Step 3: Write to Gold layer with table-specific write mode
            full_table_name = f"{catalog_name}.{gold_database}.{table_name}"
            
            # Use Iceberg table manager for writing
            if write_mode == "overwrite":
                table_manager.write_table_overwrite(transformed_df, table_name)
            elif write_mode == "overwrite_partitions":
                # For partition overwrite, we need to specify the partition values
                partition_values = {}
                if transform_params['grand_prix_filter']:
                    partition_values['grand_prix_name'] = transform_params['grand_prix_filter']
                partition_values['year'] = transform_params['year']
                table_manager.write_table_partition_overwrite(transformed_df, table_name, partition_values)
            else:
                # Fallback to append mode
                table_manager.write_table_append(transformed_df, table_name)
            
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
            
            # Stop pipeline if critical transformations fail
            if is_critical:
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
    optional_args = ['GRAND_PRIX', 'YEAR', 'SILVER_DATABASE', 'GOLD_DATABASE']
    
    args = getResolvedOptions(sys.argv, required_args, optional_args=optional_args)
    
    # Set defaults from Config for optional arguments
    args['YEAR'] = args.get('YEAR', str(Config.DEFAULT_YEAR))
    args['GRAND_PRIX'] = args.get('GRAND_PRIX', None)  # None triggers auto-detection for INCREMENTAL
    args['SILVER_DATABASE'] = args.get('SILVER_DATABASE', 'f1_silver_db')
    args['GOLD_DATABASE'] = args.get('GOLD_DATABASE', 'f1_gold_db')
    
    # Validate RUN_MODE
    valid_run_modes = [Config.RUN_MODE_HISTORICAL, Config.RUN_MODE_INCREMENTAL]
    if args['RUN_MODE'] not in valid_run_modes:
        raise ValueError(f"Invalid RUN_MODE: {args['RUN_MODE']}. Must be one of {valid_run_modes}")
    
    logger.info(f"🏁 Starting F1 Silver to Gold Job")
    logger.info(f"   Job Name: {args['JOB_NAME']}")
    logger.info(f"   Run Mode: {args['RUN_MODE']}")
    logger.info(f"   Year: {args['YEAR']}")
    logger.info(f"   Silver Database: {args['SILVER_DATABASE']}")
    logger.info(f"   Gold Database: {args['GOLD_DATABASE']}")
    if args['GRAND_PRIX']:
        logger.info(f"   Grand Prix: {args['GRAND_PRIX']}")
    
    try:
        # Initialize Spark with Iceberg
        spark, glue_context, job = initialize_spark_with_iceberg(args)
        
        # Process Silver to Gold transformation
        stats = process_silver_to_gold(spark, glue_context, args)
        
        # Log summary statistics
        logger.info("\n" + "=" * 80)
        logger.info("📊 Gold Transformation Summary:")
        logger.info("=" * 80)
        
        total_records = 0
        for table_name, record_count in stats.items():
            logger.info(f"   {table_name}: {record_count:,} records")
            total_records += record_count
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"\n   Total analytics records processed: {total_records:,}")
        logger.info(f"   Execution time: {duration:.2f} seconds")
        logger.info("=" * 80)
        
        # Commit the job
        job.commit()
        logger.info("✅ Gold layer job completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Gold layer job failed with error: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise
    finally:
        # Cleanup
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    main()