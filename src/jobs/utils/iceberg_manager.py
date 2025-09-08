"""
Iceberg table management utilities for F1 Silver and Gold layers.

This module provides a unified, layer-aware utility for managing Iceberg tables,
including table creation, schema management, maintenance, and validation for
both the Silver (normalized) and Gold (analytics) layers of the data pipeline.
"""

import logging
import time
import traceback
from typing import Type

from pyspark.sql import SparkSession

from jobs.config.schemas import SilverTableSchemas, GoldTableSchemas


class IcebergManager:
    """
    Simplified Iceberg table manager for F1 Silver and Gold layers.
    
    Provides essential operations for table creation and data writing.
    """
    
    def __init__(self,
                 spark: SparkSession,
                 glue_context,
                 bucket_name: str,
                 layer: str):
        """
        Initialize the layer-aware Iceberg table manager.
        
        Args:
            spark: Spark session with Iceberg configuration.
            glue_context: AWS Glue context for DynamicFrame operations.
            bucket_name: S3 bucket for the data lakehouse.
            layer: The data layer to manage ('silver' or 'gold').
        
        Raises:
            ValueError: If an unsupported layer is provided.
        """
        if layer not in ['silver', 'gold']:
            raise ValueError(f"Unsupported layer: '{layer}'. Must be 'silver' or 'gold'.")
            
        self.spark = spark
        self.glue_context = glue_context
        self.bucket_name = bucket_name
        self.layer = layer
        self.database_name = f"f1_{layer}_db"
        self.logger = logging.getLogger(__name__)
        
        # Use unified configuration
        self.iceberg_config = None  # No longer needed with unified config
        
        self.schemas: Type[SilverTableSchemas | GoldTableSchemas]
        if self.layer == 'silver':
            self.schemas = SilverTableSchemas
        else:
            self.schemas = GoldTableSchemas
        
        # Validate Glue 4.0 native Iceberg support
        self._validate_glue4_iceberg_support()
        
        self.logger.info("IcebergManager initialized for '%s' layer, database: '%s'", self.layer, self.database_name)
    
    def _validate_glue4_iceberg_support(self) -> None:
        """
        Simple validation that Glue Catalog is accessible.
        """
        try:
            # Simple check: try to access Glue Catalog
            self.spark.sql("SHOW CATALOGS")
            self.logger.info("✅ Glue Catalog accessible - Iceberg support available")
        except Exception as e:
            self.logger.warning("⚠️ Glue Catalog validation failed: %s", e)
            # Don't fail the pipeline, just log the warning
    
    def create_database(self) -> None:
        """Create the Glue database for the specified layer if it doesn't exist."""
        try:
            comment = f"F1 {self.layer.title()} layer - {'normalized entity tables' if self.layer == 'silver' else 'dashboard-ready analytics tables'}"
            create_db_sql = f"""
                CREATE DATABASE IF NOT EXISTS {self.database_name}
                COMMENT '{comment}'
                LOCATION 's3://{self.bucket_name}/{self.layer}/'
            """
            self.spark.sql(create_db_sql)
            self.logger.info("✅ Created/verified %s database: %s", self.layer, self.database_name)
            
        except Exception as e:
            self.logger.error("Failed to create %s database %s: %s", self.layer, self.database_name, e)
            raise
            
    def create_all_tables(self) -> None:
        """Create all Iceberg tables for the specified layer."""
        try:
            # Ensure the database exists first
            self.logger.info("🏗️ Starting table creation for '%s' layer...", self.layer)
            self.create_database()
            
            table_names = self.schemas.get_all_table_names()
            self.logger.info("📋 Found %d tables to create: %s", len(table_names), table_names)
            
            successful_tables = []
            failed_tables = []
            
            for table_name in table_names:
                try:
                    # Check if table already exists first
                    if self.table_exists(table_name):
                        self.logger.info("✅ Table %s already exists - skipping creation", table_name)
                        successful_tables.append(table_name)
                        continue
                    
                    self.logger.info("🔄 Creating table: %s", table_name)
                    self._create_table_simple(table_name)
                    successful_tables.append(table_name)
                    self.logger.info("✅ Successfully created table: %s", table_name)
                except Exception as table_error:
                    self.logger.error("❌ Failed to create table %s: %s", table_name, table_error)
                    failed_tables.append((table_name, str(table_error)))
                    # Continue with other tables instead of failing immediately
                    continue
            
            # Report results
            if failed_tables:
                self.logger.warning("⚠️ Table creation completed with %d failures:", len(failed_tables))
                for table_name, error in failed_tables:
                    self.logger.warning("  - %s: %s", table_name, error)
                self.logger.info("✅ Successfully processed %d '%s' layer tables: %s", len(successful_tables), self.layer, successful_tables)
            else:
                self.logger.info("✅ Successfully created all %d '%s' layer tables: %s", len(successful_tables), self.layer, successful_tables)
            
        except Exception as e:
            self.logger.error("❌ Failed to create all '%s' tables: %s", self.layer, e)
            self.logger.error("Full traceback: %s", traceback.format_exc())
            raise

    def _create_table_simple(self, table_name: str) -> None:
        """
        Simplified table creation that avoids complex Spark coordination issues.
        Uses basic SQL execution without complex validation.
        """
        try:
            # Get table schema DDL
            columns_ddl = self.schemas.get_ddl_columns(table_name)
            if not columns_ddl:
                raise ValueError(f"No DDL columns found for table: {table_name}")
            
            # Simple CREATE TABLE statement without complex properties
            create_table_sql = f"""CREATE TABLE IF NOT EXISTS glue_catalog.{self.database_name}.{table_name} (
    {columns_ddl}
)
USING ICEBERG
LOCATION 's3://{self.bucket_name}/warehouse/{self.database_name}/{table_name}'"""
            
            self.logger.info("Creating %s table with simple approach: %s", self.layer, table_name)
            self.logger.debug("SQL: %s", create_table_sql)
            
            # Execute with retry logic for connectivity issues
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self.spark.sql(create_table_sql)
                    self.logger.info("✅ Created %s table: %s.%s", self.layer, self.database_name, table_name)
                    return  # Success, exit retry loop
                except Exception as sql_error:
                    if attempt < max_retries - 1:
                        self.logger.warning("⚠️ Table creation attempt %d failed for %s: %s", attempt + 1, table_name, sql_error)
                        self.logger.info("🔄 Retrying in 2 seconds... (attempt %d/%d)", attempt + 2, max_retries)
                        time.sleep(2)
                    else:
                        self.logger.error("❌ All %d attempts failed for %s: %s", max_retries, table_name, sql_error)
                        raise
            
        except Exception as e:
            self.logger.error("❌ Simple table creation failed for %s: %s", table_name, e)
            raise

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the layer's database.
        
        Uses a simple approach that doesn't require complex Spark operations.
        
        Args:
            table_name: Name of the table to check.
            
        Returns:
            True if the table exists, False otherwise.
        """
        try:
            # Use a simple SHOW TABLES query instead of DESCRIBE TABLE
            # This is more reliable and doesn't require table access
            tables_df = self.spark.sql(f"SHOW TABLES IN glue_catalog.{self.database_name}")
            tables = [row.tableName for row in tables_df.collect()]
            return table_name in tables
        except Exception as e:
            self.logger.warning("⚠️ Could not check if table %s exists: %s", table_name, e)
            # If we can't check, assume it doesn't exist to be safe
            return False

    def write_table(self, df, table_name: str) -> None:
        """
        Write DataFrame to Iceberg table using Glue DynamicFrame API.
        
        This method uses AWS Glue's DynamicFrame API for proper Iceberg integration
        with Glue Catalog, ensuring tables are correctly registered and accessible.
        
        Args:
            df: Spark DataFrame to write
            table_name: Full table name (database.table) or just table name
        """
        try:
            # Extract database and table from full table name
            if '.' in table_name:
                database, table = table_name.split('.', 1)
            else:
                table = table_name
                database = self.database_name
            
            # Get record count before writing (for logging)
            record_count = df.count()
            
            # Convert Spark DataFrame to Glue DynamicFrame
            dyf = self.glue_context.create_dynamic_frame.from_options(
                frame=df,
                connection_type="spark",
                connection_options={}
            )
            
            # Use Glue DynamicFrame API for Iceberg writing with schema evolution
            self.glue_context.write_dynamic_frame.from_catalog(
                frame=dyf,
                database=database,
                table_name=table,
                additional_options={
                    "mergeSchema": "true",
                    "upsert": "true",
                    "write.target-file-size-bytes": "134217728",
                    "write.data.compression-codec": "zstd"
                }
            )
            
            self.logger.info("✅ Written %d records to %s.%s using Glue DynamicFrame API", record_count, database, table)
            
        except Exception as e:
            self.logger.error("Failed to write DataFrame to Iceberg table %s: %s", table_name, e)
            raise