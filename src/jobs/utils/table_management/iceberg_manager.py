"""
Iceberg Table Manager for F1 Silver Layer

Manages creation and maintenance of Iceberg tables in the Silver layer.
Follows proper glue_catalog prefix and SQL DDL approach.

Key Features:
- CREATE TABLE IF NOT EXISTS for safety
- Proper partitioning strategy per table
- AWS Glue 5.0 + Iceberg 1.7.1 compatibility
- Eventual consistency handling
"""

import logging
import time
from typing import Dict, List, Optional
from pyspark.sql import SparkSession

# Import Silver schemas from new schema definitions
from src.jobs.utils.table_management.schemas import get_ddl_columns

# Setup logging
logger = logging.getLogger(__name__)


class IcebergTableManager:
    """
    Manages Iceberg table operations for F1 Silver layer.
    
    This manager handles:
    - Table creation with proper schemas
    - Partitioning strategies per table type
    - Iceberg-specific table properties
    - AWS Glue Catalog integration
    """
    
    def __init__(self, spark: SparkSession, catalog_name: str = "glue_catalog", 
                 database_name: str = "f1_silver_db", bucket_name: str = "f1-data-lake-naveeth"):
        """
        Initialize the Iceberg Table Manager.
        
        Args:
            spark: SparkSession with Iceberg configuration
            catalog_name: Catalog name (default: glue_catalog)
            database_name: Database name (default: f1_silver_db)
            bucket_name: S3 bucket name for table storage
        """
        self.spark = spark
        self.catalog_name = catalog_name
        self.database_name = database_name
        self.bucket_name = bucket_name
        
        # Base location for Silver tables (Option A from discussion)
        self.silver_base_location = f"s3://{bucket_name}/silver"
        
        logger.info(f"Initialized IcebergTableManager for {catalog_name}.{database_name}")
    
    def _get_table_properties(self) -> str:
        """
        Get optimized Iceberg table properties for AWS Glue 5.0.
        
        Returns:
            SQL TBLPROPERTIES string
        """
        return """
        TBLPROPERTIES (
            'write.format.default' = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '5',
            'write.parquet.row-group-size-bytes' = '134217728',
            'read.split.target-size' = '134217728',
            'commit.retry.num-retries' = '3',
            'write.object-storage.enabled' = 'true',
            'format-version' = '2'
        )
        """
    
    def create_all_silver_tables(self) -> Dict[str, bool]:
        """
        Create all Silver layer tables if they don't exist.
        
        This is the main method called by the Bronze to Silver job.
        Tables are created with appropriate schemas and partitioning strategies.
        
        Returns:
            Dictionary with table creation status
        """
        logger.info("=" * 60)
        logger.info("📋 Creating Silver Layer Tables")
        logger.info("=" * 60)
        
        results = {}
        
        # Define tables with their specific partitioning strategies
        tables_config = [
            {
                "name": "sessions_silver",
                "create_func": self._create_sessions_table,
                "description": "F1 session metadata"
            },
            {
                "name": "drivers_silver",
                "create_func": self._create_drivers_table,
                "description": "F1 driver master data"
            },
            {
                "name": "qualifying_results_silver",
                "create_func": self._create_qualifying_results_table,
                "description": "F1 qualifying session results"
            },
            {
                "name": "race_results_silver",
                "create_func": self._create_race_results_table,
                "description": "F1 race results with points"
            },
            {
                "name": "laps_silver",
                "create_func": self._create_laps_table,
                "description": "F1 lap-by-lap data"
            },
            {
                "name": "pitstops_silver",
                "create_func": self._create_pitstops_table,
                "description": "F1 pit stop data"
            }
        ]
        
        # Create each table
        for table_config in tables_config:
            try:
                logger.info(f"\n🔨 Creating table: {table_config['name']}")
                logger.info(f"   Description: {table_config['description']}")
                
                # Check if table exists
                if self._table_exists(table_config['name']):
                    logger.info(f"   ✅ Table already exists, skipping creation")
                    results[table_config['name']] = True
                    continue
                
                # Create the table
                table_config['create_func']()
                results[table_config['name']] = True
                logger.info(f"   ✅ Table created successfully")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to create table {table_config['name']}: {str(e)}")
                results[table_config['name']] = False
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 Table Creation Summary:")
        successful = sum(1 for v in results.values() if v)
        logger.info(f"   Successful: {successful}/{len(results)}")
        for table_name, success in results.items():
            status = "✅" if success else "❌"
            logger.info(f"   {status} {table_name}")
        logger.info("=" * 60)
        
        return results
    
    def _table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the catalog.
        
        Args:
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            self.spark.sql(f"DESCRIBE TABLE {self.catalog_name}.{self.database_name}.{table_name}")
            return True
        except Exception:
            return False
    
    def _create_sessions_table(self) -> None:
        """
        Create sessions_silver table.
        Partitioned by (year) only since sessions span a grand prix.
        """
        table_name = "sessions_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY (year)
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def _create_drivers_table(self) -> None:
        """
        Create drivers_silver table.
        No partitioning - this is master data with SCD Type 1 (latest record per driver).
        """
        table_name = "drivers_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def _create_qualifying_results_table(self) -> None:
        """
        Create qualifying_results_silver table.
        Partitioned by (year, grand_prix_name) for efficient querying.
        """
        table_name = "qualifying_results_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY (year, grand_prix_name)
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def _create_race_results_table(self) -> None:
        """
        Create race_results_silver table.
        Partitioned by (year, grand_prix_name) for efficient querying.
        """
        table_name = "race_results_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY (year, grand_prix_name)
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def _create_laps_table(self) -> None:
        """
        Create laps_silver table.
        Partitioned by (year, grand_prix_name) - this is the largest table.
        """
        table_name = "laps_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY (year, grand_prix_name)
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def _create_pitstops_table(self) -> None:
        """
        Create pitstops_silver table.
        Partitioned by (year, grand_prix_name) for race-specific pit stop data.
        """
        table_name = "pitstops_silver"
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{self.silver_base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY (year, grand_prix_name)
        LOCATION '{table_location}'
        {self._get_table_properties()}
        """
        
        self.spark.sql(create_table_sql)
    
    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table with eventual consistency handling.
        
        Args:
            table_name: Name of the table to drop
            if_exists: If True, use DROP TABLE IF EXISTS
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        
        logger.warning(f"⚠️ Dropping table: {full_table_name}")
        
        try:
            if if_exists:
                self.spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            else:
                self.spark.sql(f"DROP TABLE {full_table_name}")
            
            # Critical: Wait for eventual consistency
            logger.info("⏳ Waiting 60 seconds for AWS Glue Catalog eventual consistency...")
            time.sleep(60)
            
            logger.info(f"✅ Table {full_table_name} dropped successfully")
            
        except Exception as e:
            logger.error(f"❌ Failed to drop table {full_table_name}: {str(e)}")
            raise
    
    def validate_tables(self) -> Dict[str, Dict]:
        """
        Validate all Silver tables exist and have correct structure.
        
        Returns:
            Dictionary with validation results per table
        """
        tables = [
            "sessions_silver",
            "drivers_silver", 
            "qualifying_results_silver",
            "race_results_silver",
            "laps_silver",
            "pitstops_silver"
        ]
        
        results = {}
        
        for table_name in tables:
            full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
            
            try:
                # Check if table exists and get row count
                count_df = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {full_table_name}")
                row_count = count_df.collect()[0]['cnt']
                
                # Get table info
                desc_df = self.spark.sql(f"DESCRIBE TABLE EXTENDED {full_table_name}")
                
                # Get partitioning info
                partitions = []
                for row in desc_df.collect():
                    if row['col_name'] == 'Part 0':
                        partitions.append(row['data_type'])
                
                results[table_name] = {
                    "exists": True,
                    "row_count": row_count,
                    "partitions": partitions,
                    "status": "✅"
                }
                
            except Exception as e:
                results[table_name] = {
                    "exists": False,
                    "error": str(e),
                    "status": "❌"
                }
        
        # Log validation results
        logger.info("\n" + "=" * 60)
        logger.info("📋 Silver Tables Validation Results:")
        logger.info("=" * 60)
        
        for table_name, result in results.items():
            logger.info(f"\n{result['status']} {table_name}:")
            if result['exists']:
                logger.info(f"   Row count: {result['row_count']:,}")
                if result['partitions']:
                    logger.info(f"   Partitions: {result['partitions']}")
            else:
                logger.info(f"   Error: {result['error']}")
        
        return results
    
    def create_all_gold_tables(self) -> Dict[str, bool]:
        """
        Create all Gold layer tables if they don't exist.
        
        This method creates Gold tables with optimized properties for analytics workloads.
        Tables are created with appropriate schemas and partitioning strategies.
        
        Returns:
            Dictionary with table creation status
        """
        logger.info("=" * 60)
        logger.info("🏆 Creating Gold Layer Tables")
        logger.info("=" * 60)
        
        results = {}
        
        # Update base location for Gold tables
        gold_base_location = f"s3://{self.bucket_name}/gold"
        
        # Define Gold tables with their specific partitioning strategies
        gold_tables_config = [
            {
                "name": "driver_performance_summary_qualifying",
                "partitions": "year, grand_prix_name",
                "description": "Qualifying performance metrics"
            },
            {
                "name": "driver_performance_summary_race",
                "partitions": "year, grand_prix_name",
                "description": "Race performance with points and championship progression"
            },
            {
                "name": "championship_tracker",
                "partitions": "year",
                "description": "Championship standings after each race"
            },
            {
                "name": "race_weekend_insights",
                "partitions": "year, grand_prix_name",
                "description": "Weekend highlights and basic race statistics"
            }
        ]
        
        # Create each Gold table
        for table_config in gold_tables_config:
            try:
                table_name = table_config['name']
                partitions = table_config['partitions']
                description = table_config['description']
                
                logger.info(f"\n🔨 Creating Gold table: {table_name}")
                logger.info(f"   Description: {description}")
                logger.info(f"   Partitions: {partitions}")
                
                # Check if table exists
                if self._table_exists(table_name):
                    logger.info(f"   ✅ Table already exists, skipping creation")
                    results[table_name] = True
                    continue
                
                # Create the Gold table
                self._create_gold_table(table_name, partitions, gold_base_location)
                results[table_name] = True
                logger.info(f"   ✅ Gold table created successfully")
                
            except Exception as e:
                logger.error(f"   ❌ Failed to create Gold table {table_config['name']}: {str(e)}")
                results[table_config['name']] = False
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("📊 Gold Table Creation Summary:")
        successful = sum(1 for v in results.values() if v)
        logger.info(f"   Successful: {successful}/{len(results)}")
        for table_name, success in results.items():
            status = "✅" if success else "❌"
            logger.info(f"   {status} {table_name}")
        logger.info("=" * 60)
        
        return results
    
    def _create_gold_table(self, table_name: str, partitions: str, base_location: str) -> None:
        """
        Create a Gold table with appropriate schema and partitioning.
        
        Args:
            table_name: Name of the Gold table
            partitions: Partition specification (e.g., "year" or "year, grand_prix_name")
            base_location: Base S3 location for Gold tables
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        table_location = f"{base_location}/{table_name}"
        
        # Get schema from new schema definitions
        schema_ddl = get_ddl_columns(table_name)
        
        # Gold-specific table properties for analytics
        gold_properties = """
        TBLPROPERTIES (
            'write.target-file-size-bytes' = '268435456',
            'write.parquet.compression-codec' = 'snappy',
            'write.parquet.row-group-size' = '134217728',
            'format' = 'iceberg',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '7',
            'read.split.target-size' = '134217728',
            'commit.retry.num-retries' = '3',
            'write.object-storage.enabled' = 'true',
            'format-version' = '2'
        )
        """
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table_name} (
{schema_ddl}
        ) USING iceberg
        PARTITIONED BY ({partitions})
        LOCATION '{table_location}'
        {gold_properties}
        """
        
        self.spark.sql(create_table_sql)
    
    def write_table_overwrite(self, df, table_name: str) -> None:
        """
        Write DataFrame to Gold table with overwrite mode.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        logger.info(f"📝 Writing to {full_table_name} (overwrite mode)")
        
        df.writeTo(full_table_name).overwritePartitions()
    
    def write_table_partition_overwrite(self, df, table_name: str, partition_values: Dict[str, any]) -> None:
        """
        Write DataFrame to Gold table with partition overwrite mode.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
            partition_values: Dictionary of partition column values
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        logger.info(f"📝 Writing to {full_table_name} (partition overwrite: {partition_values})")
        
        # Build partition filter
        partition_conditions = []
        for col, val in partition_values.items():
            if isinstance(val, str):
                partition_conditions.append(f"{col} = '{val}'")
            else:
                partition_conditions.append(f"{col} = {val}")
        
        partition_filter = " AND ".join(partition_conditions)
        
        # Use Iceberg partition overwrite
        df.writeTo(full_table_name).overwritePartitions()
    
    def write_table_append(self, df, table_name: str) -> None:
        """
        Write DataFrame to Gold table with append mode.
        
        Args:
            df: DataFrame to write
            table_name: Target table name
        """
        full_table_name = f"{self.catalog_name}.{self.database_name}.{table_name}"
        logger.info(f"📝 Writing to {full_table_name} (append mode)")
        
        df.writeTo(full_table_name).append()
