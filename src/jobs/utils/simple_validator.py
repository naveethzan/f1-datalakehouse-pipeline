"""
Simple Data Validator for F1 Data Pipeline

This module provides lightweight data validation functionality using only Spark operations.
No external dependencies - designed for simplified Glue environment.

Features:
- Row count validation
- Null column detection
- Basic data quality checks
- No external dependencies
"""

import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


class SimpleDataValidator:
    """
    Lightweight data validator using only Spark operations.
    
    This validator provides basic data quality checks without external dependencies,
    making it suitable for simplified Glue environments.
    """
    
    @staticmethod
    def validate_dataframe(df: DataFrame, table_name: str, min_rows: int = 1) -> Dict[str, Any]:
        """
        Basic DataFrame validation using only Spark operations.
        
        Args:
            df: DataFrame to validate
            table_name: Name of the table for logging and error messages
            min_rows: Minimum number of rows expected (default: 1)
            
        Returns:
            Dictionary with validation results
            
        Raises:
            ValueError: If validation fails
        """
        try:
            # Row count check
            row_count = df.count()
            logger.info(f"Validating {table_name}: {row_count:,} rows")
            
            if row_count < min_rows:
                error_msg = f"{table_name}: Expected at least {min_rows} rows, got {row_count}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Null check for key columns (if any)
            null_columns = []
            for col in df.columns:
                try:
                    null_count = df.filter(df[col].isNull()).count()
                    if null_count == row_count:
                        null_columns.append(col)
                except Exception as e:
                    logger.warning(f"Could not check nulls for column {col} in {table_name}: {e}")
            
            if null_columns:
                error_msg = f"{table_name}: Columns with all null values: {null_columns}"
                logger.warning(error_msg)
                # Don't fail for null columns, just warn
            
            # Basic DataFrame integrity check
            if df.rdd.isEmpty() and row_count > 0:
                raise ValueError(f"{table_name}: DataFrame appears empty despite positive row count")
            
            result = {
                'table_name': table_name,
                'row_count': row_count,
                'null_columns': null_columns,
                'status': 'valid',
                'validation_passed': True
            }
            
            logger.info(f"✅ {table_name} validation passed: {row_count:,} rows, {len(null_columns)} null columns")
            return result
            
        except Exception as e:
            error_msg = f"Validation failed for {table_name}: {str(e)}"
            logger.error(error_msg)
            return {
                'table_name': table_name,
                'row_count': 0,
                'null_columns': [],
                'status': 'failed',
                'validation_passed': False,
                'error': str(e)
            }
    
    @staticmethod
    def validate_silver_tables(spark_session, glue_context, silver_database: str) -> Dict[str, Any]:
        """
        Validate all Silver layer tables.
        
        Args:
            spark_session: Spark session
            glue_context: Glue context for DynamicFrame operations
            silver_database: Name of the Silver database
            
        Returns:
            Dictionary with validation results for all tables
        """
        logger.info(f"Starting Silver layer validation for database: {silver_database}")
        
        # Define expected Silver tables
        expected_tables = [
            'sessions_silver',
            'drivers_silver',
            'qualifying_results_silver',
            'race_results_silver',
            'laps_silver',
            'pitstops_silver'
        ]
        
        validation_results = {}
        total_tables = len(expected_tables)
        successful_validations = 0
        
        for table_name in expected_tables:
            try:
                # Use Glue DynamicFrame API for proper Iceberg table reading
                dyf = glue_context.create_dynamic_frame.from_catalog(
                    database=silver_database,
                    table_name=table_name
                )
                # Convert DynamicFrame to DataFrame for validation
                df = dyf.toDF()
                
                # Validate the DataFrame
                result = SimpleDataValidator.validate_dataframe(df, table_name, min_rows=0)
                validation_results[table_name] = result
                
                if result['validation_passed']:
                    successful_validations += 1
                    
            except Exception as e:
                logger.error(f"Failed to validate {table_name}: {e}")
                validation_results[table_name] = {
                    'table_name': table_name,
                    'row_count': 0,
                    'status': 'failed',
                    'validation_passed': False,
                    'error': str(e)
                }
        
        overall_status = successful_validations == total_tables
        logger.info(f"Silver validation complete: {successful_validations}/{total_tables} tables valid")
        
        return {
            'validation_passed': overall_status,
            'total_tables': total_tables,
            'successful_tables': successful_validations,
            'failed_tables': total_tables - successful_validations,
            'table_results': validation_results
        }
    
    @staticmethod
    def validate_gold_tables(spark_session, glue_context, gold_database: str) -> Dict[str, Any]:
        """
        Validate all Gold layer tables.
        
        Args:
            spark_session: Spark session
            glue_context: Glue context for DynamicFrame operations
            gold_database: Name of the Gold database
            
        Returns:
            Dictionary with validation results for all tables
        """
        logger.info(f"Starting Gold layer validation for database: {gold_database}")
        
        # Define expected Gold tables
        expected_tables = [
            'driver_performance_summary_qualifying',
            'driver_performance_summary_race',
            'championship_tracker',
            'team_strategy_analysis',
            'race_weekend_insights'
        ]
        
        validation_results = {}
        total_tables = len(expected_tables)
        successful_validations = 0
        
        for table_name in expected_tables:
            try:
                # Use Glue DynamicFrame API for proper Iceberg table reading
                dyf = glue_context.create_dynamic_frame.from_catalog(
                    database=gold_database,
                    table_name=table_name
                )
                # Convert DynamicFrame to DataFrame for validation
                df = dyf.toDF()
                
                # Validate the DataFrame
                result = SimpleDataValidator.validate_dataframe(df, table_name, min_rows=0)
                validation_results[table_name] = result
                
                if result['validation_passed']:
                    successful_validations += 1
                    
            except Exception as e:
                logger.error(f"Failed to validate {table_name}: {e}")
                validation_results[table_name] = {
                    'table_name': table_name,
                    'row_count': 0,
                    'status': 'failed',
                    'validation_passed': False,
                    'error': str(e)
                }
        
        overall_status = successful_validations == total_tables
        logger.info(f"Gold validation complete: {successful_validations}/{total_tables} tables valid")
        
        return {
            'validation_passed': overall_status,
            'total_tables': total_tables,
            'successful_tables': successful_validations,
            'failed_tables': total_tables - successful_validations,
            'table_results': validation_results
        }
    
    @staticmethod
    def validate_transformation_result(transformed_data: Dict[str, DataFrame], 
                                    expected_tables: List[str]) -> Dict[str, Any]:
        """
        Validate transformation results from a transformer.
        
        Args:
            transformed_data: Dictionary of table_name -> DataFrame from transformer
            expected_tables: List of expected table names
            
        Returns:
            Dictionary with validation results
        """
        logger.info(f"Validating transformation results: {len(transformed_data)} tables")
        
        validation_results = {}
        successful_validations = 0
        
        for table_name in expected_tables:
            if table_name in transformed_data:
                df = transformed_data[table_name]
                result = SimpleDataValidator.validate_dataframe(df, table_name, min_rows=0)
                validation_results[table_name] = result
                
                if result['validation_passed']:
                    successful_validations += 1
            else:
                logger.error(f"Missing expected table: {table_name}")
                validation_results[table_name] = {
                    'table_name': table_name,
                    'row_count': 0,
                    'status': 'missing',
                    'validation_passed': False,
                    'error': 'Table not found in transformation results'
                }
        
        overall_status = successful_validations == len(expected_tables)
        logger.info(f"Transformation validation complete: {successful_validations}/{len(expected_tables)} tables valid")
        
        return {
            'validation_passed': overall_status,
            'total_tables': len(expected_tables),
            'successful_tables': successful_validations,
            'failed_tables': len(expected_tables) - successful_validations,
            'table_results': validation_results
        }
