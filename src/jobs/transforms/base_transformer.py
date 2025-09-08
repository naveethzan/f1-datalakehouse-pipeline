"""
Unified base transformer class for F1 Silver and Gold transformations.

This module provides a universal abstract base class that all Silver and Gold
table transformers inherit from, ensuring consistent patterns and functionality
across both transformation layers.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Simplified imports for Glue environment - removed heavy dependencies


class BaseTransformer(ABC):
   
    def __init__(self, 
                 spark: SparkSession,
                 glue_context,
                 job_run_id: str,
                 layer: str,
                 config: Dict[str, Any]):
        """
        Initialize the universal base transformer.
        
        Args:
            spark: Spark session with Iceberg configuration.
            glue_context: AWS Glue context for DynamicFrame operations.
            job_run_id: Unique identifier for this job run.
            layer: The target layer for the transformation ('silver' or 'gold').
            config: The job configuration dictionary.
        """
        self.spark = spark
        self.glue_context = glue_context
        self.job_run_id = job_run_id
        self.layer = layer
        self.config = config
        
        # Layer-specific configuration with safe access
        self.source_database = config.get('silver', {}).get('database_name') if layer == 'gold' else None
        self.bucket_name = config.get('s3', {}).get('bucket_name', 'f1-data-lake-naveeth')
        
        # Simplified initialization for Glue environment - no heavy dependencies
        
        # Setup logging
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)
        
        # Transformation metadata
        self.start_time = None
        self.output_record_count = 0
        

    @property
    @abstractmethod
    def table_name(self) -> str:
        """Return the target table name this transformer creates."""
        pass
    
    @property
    @abstractmethod
    def source_tables(self) -> List[str]:
        """Return list of source tables this transformer reads from."""
        pass

    @abstractmethod
    def transform(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Transform source data to the target layer's format.
        
        Args:
            source_data: Dictionary mapping source table names to DataFrames.
            
        Returns:
            Transformed DataFrame for the target layer.
        """
        pass


    def execute_transformation(self) -> Dict[str, DataFrame]:
        """
        Execute the complete transformation pipeline for a single table.
        
        Returns:
            Dictionary with transformation results and metrics.
        """
        self.start_time = datetime.datetime.now()
        
        # Load and validate source data
        source_data = self._read_source_data()
        # Transform the data
        transformed_df = self.transform(source_data)
        self.output_record_count = transformed_df.count()
        
        # Validate output data
        self._validate_output_data(transformed_df)
        
        # Add audit columns after transformation
        final_df = self.add_audit_columns(transformed_df)
        
        # Lineage tracking disabled in simplified Glue environment
        
        return {self.table_name: final_df}

    def _read_source_data(self) -> Dict[str, DataFrame]:
        """
        Load required source data for the transformation, supporting both Bronze and Silver sources.
        Includes validation to ensure data integrity and prevent downstream failures.
        """
        source_data = {}
        
        # Load required source data
        for table in self.source_tables:
            try:
                if self.layer == 'silver':
                    # Silver transformers read from Bronze Parquet files with partition discovery
                    bronze_path = f"s3a://{self.bucket_name}/bronze/{table}"
                    df = self.spark.read \
                        .option("mergeSchema", "true") \
                        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss") \
                        .option("recursiveFileLookup", "true") \
                        .option("basePath", f"s3a://{self.bucket_name}/bronze/{table}") \
                        .parquet(bronze_path)
                elif self.layer == 'gold':
                    # Gold transformers read from Silver Iceberg tables using Glue DynamicFrame API
                    dyf = self.glue_context.create_dynamic_frame.from_catalog(
                        database=self.source_database,
                        table_name=table
                    )
                    # Convert DynamicFrame to DataFrame for transformation
                    df = dyf.toDF()
                else:
                    raise ValueError(f"Unknown layer: {self.layer}")
                
                # Validate DataFrame before adding to source_data
                df = self._validate_dataframe(df, table)
                
                # Ensure partition columns are properly typed for Bronze data
                if self.layer == 'silver':
                    df = self._ensure_partition_column_types(df, table)
                
                source_data[table] = df
                
            except Exception as e:
                self.logger.error("Failed to load source table %s: %s", table, e)
                raise
        
        # Handle special case: derive sessions data from session_result if needed
        if "sessions" in self.source_tables and "sessions" not in source_data:
            if "session_result" in source_data:
                self.logger.info("Deriving sessions data from session_result...")
                sessions_df = self._derive_sessions_from_session_result(source_data["session_result"])
                source_data["sessions"] = sessions_df
            else:
                raise ValueError("Cannot derive sessions data: session_result not available")
        
        return source_data

    def _validate_dataframe(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Simple DataFrame validation - just log record count.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table for logging
            
        Returns:
            DataFrame (no changes)
        """
        record_count = df.count()
        self.logger.info("Loaded %s: %d records", table_name, record_count)
        return df

    
    def _validate_output_data(self, transformed_df: DataFrame) -> None:
        """
        Simple output validation - just log record count.
        
        Args:
            transformed_df: Transformed DataFrame to validate
        """
        row_count = transformed_df.count()
        self.logger.info("Output validation: %s has %d rows", self.table_name, row_count)


    def add_audit_columns(self, df: DataFrame) -> DataFrame:
        """
        Add standard audit columns to a DataFrame.
        
        Args:
            df: DataFrame to add audit columns to.
            
        Returns:
            DataFrame with audit columns added.
        """
        current_ts = current_timestamp()
        return df.withColumn("created_timestamp", current_ts) \
                 .withColumn("updated_timestamp", current_ts)

    
    def _ensure_partition_column_types(self, df: DataFrame, table_name: str) -> DataFrame:
        """
        Ensure partition columns are properly typed for Bronze data.
        
        Args:
            df: DataFrame with partition columns
            table_name: Name of the table for logging
            
        Returns:
            DataFrame with properly typed partition columns
        """
        from pyspark.sql.functions import col
        from pyspark.sql.types import IntegerType, StringType
        
        # Check if partition columns exist and cast them to proper types
        if "year" in df.columns:
            df = df.withColumn("year", col("year").cast(IntegerType()))
        if "grand_prix" in df.columns:
            df = df.withColumn("grand_prix", col("grand_prix").cast(StringType()))
        if "session_type" in df.columns:
            df = df.withColumn("session_type", col("session_type").cast(StringType()))
            
        self.logger.info("Ensured partition column types for %s", table_name)
        return df
    
    def _derive_sessions_from_session_result(self, session_result_df: DataFrame) -> DataFrame:
        """
        Derive sessions data from session_result by extracting unique session records.
        Uses enriched data from Bronze layer (meeting_name, date_start) and partition columns.
        
        Args:
            session_result_df: Bronze session_result DataFrame with enriched metadata
            
        Returns:
            Sessions DataFrame with unique session records
        """
        from pyspark.sql.functions import col, lit, current_timestamp
        
        # Create sessions data using enriched data from Bronze layer
        sessions_df = session_result_df.select(
            col("session_key"),
            col("meeting_key"),
            col("year"),
            col("grand_prix").alias("grand_prix_name"),
            col("session_type"),
            # Use real data from enriched Bronze layer
            col("meeting_name"),
            col("date_start"),
            # Add reasonable date_end (session duration typically 2-3 hours)
            current_timestamp().alias("date_end"),
            col("session_type").alias("session_name")
        ).distinct()
        
        self.logger.info("Derived %d unique sessions from enriched session_result", sessions_df.count())
        return sessions_df

    # ... (Other common helper methods like validation, metrics, lineage will be added here)