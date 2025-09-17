"""
Driver Performance Summary Qualifying Gold Table Transformer.

This module implements the transformation from Silver tables to the
driver_performance_summary_qualifying Gold table, providing qualifying-specific
driver analysis with essential performance metrics.

Updated to align with simplified Gold schema and use enhanced business logic utilities.
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from jobs.transforms.base_transformer import BaseTransformer
from src.jobs.utils.analytics.business_logic import F1BusinessLogic
from src.jobs.utils.analytics.window_functions import WindowFunctionUtils, add_qualifying_season_metrics
from src.jobs.utils.table_management.schemas import get_table_schema
from src.jobs.utils.data_access.silver_readers import (
    read_sessions_silver, read_drivers_silver, read_qualifying_results_silver,
    read_silver_tables_for_transform, validate_silver_data
)
from src.jobs.utils.analytics.broadcast_utils import (
    get_broadcast_config_for_transform, log_broadcast_performance_stats,
    validate_broadcast_decisions
)

logger = logging.getLogger(__name__)


class DriverPerformanceSummaryQualifyingTransform(BaseTransformer):
    """
    Transformer for creating driver_performance_summary_qualifying Gold table.
    
    This transformer creates a simplified, analytics-ready view of qualifying performance
    with essential metrics aligned to the Gold layer schema.
    """

    @property
    def table_name(self) -> str:
        return "driver_performance_summary_qualifying"

    @property
    def source_tables(self) -> List[str]:
        """Return list of Silver tables this transformer reads from."""
        return [
            'sessions_silver',
            'drivers_silver', 
            'qualifying_results_silver'
        ]
    
    def read_source_data(
        self, 
        spark: SparkSession, 
        year: int, 
        grand_prix_filter: Optional[str] = None,
        processing_mode: str = "historical"
    ) -> Dict[str, DataFrame]:
        """
        Read Silver tables using optimized Silver readers with broadcast utilities.
        
        Args:
            spark: SparkSession instance
            year: Year to filter data for
            grand_prix_filter: Optional Grand Prix name filter
            processing_mode: 'historical' or 'incremental' for broadcast optimization
            
        Returns:
            Dictionary of DataFrames from Silver tables with optimal broadcast hints
        """
        import time
        start_time = time.time()
        
        logger.info(f"Reading Silver data for qualifying transform (year={year}, gp={grand_prix_filter}, mode={processing_mode})")
        
        # Get broadcast configuration for this transform
        broadcast_config = get_broadcast_config_for_transform(
            transform_name=self.table_name,
            processing_mode=processing_mode
        )
        
        # Validate broadcast decisions
        warnings = validate_broadcast_decisions(broadcast_config)
        for warning in warnings:
            logger.warning(warning)
        
        try:
            # Define table specifications for reading
            table_specs = {
                'sessions_silver': {
                    'columns': [
                        'session_key', 'session_type', 'session_name', 
                        'grand_prix_name', 'date_start', 'year'
                    ]
                },
                'drivers_silver': {
                    'columns': [
                        'driver_number', 'broadcast_name', 'full_name', 
                        'team_name', 'is_current'
                    ]
                },
                'qualifying_results_silver': {
                    'columns': [
                        'session_key', 'driver_number', 'position',
                        'fastest_qualifying_time_millis', 'year', 'grand_prix_name'
                    ]
                }
            }
            
            # Read Silver tables using the utility
            source_data = read_silver_tables_for_transform(
                spark=spark,
                table_specs=table_specs,
                year_filter=year,
                grand_prix_filter=grand_prix_filter,
                processing_mode=processing_mode
            )
            
            # Filter sessions to qualifying only (post-read filtering)
            sessions_df = source_data['sessions_silver'].filter(
                col("session_type") == "Qualifying"
            )
            source_data['sessions_silver'] = sessions_df
            
            # Filter drivers to current records only
            drivers_df = source_data['drivers_silver'].filter(
                col("is_current") == True
            )
            source_data['drivers_silver'] = drivers_df
            
            # Validate all Silver data
            record_counts = {}
            for table_name, df in source_data.items():
                is_valid = validate_silver_data(df, table_name, year, grand_prix_filter)
                if not is_valid:
                    raise ValueError(f"Silver data validation failed for {table_name}")
                record_counts[table_name] = df.count()
            
            # Log performance statistics
            execution_time = time.time() - start_time
            log_broadcast_performance_stats(
                transform_name=self.table_name,
                broadcast_config=broadcast_config,
                execution_time=execution_time,
                record_counts=record_counts
            )
            
            logger.info(f"Silver data reading completed in {execution_time:.2f}s")
            return source_data
            
        except Exception as e:
            logger.error(f"❌ Failed to read Silver data: {e}")
            raise

    def transform(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Transform Silver data into Gold driver_performance_summary_qualifying table.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with Gold schema for driver_performance_summary_qualifying
        """
        logger.info("Starting qualifying performance transformation")
        
        try:
            # Step 1: Create base qualifying dataset
            base_df = self._create_qualifying_base(source_data)
            logger.info(f"Base qualifying dataset created: {base_df.count()} records")
            
            # Step 2: Calculate gap to pole using F1BusinessLogic
            gap_df = self._calculate_qualifying_gaps(base_df)
            logger.info("Gap to pole calculations completed")
            
            # Step 3: Add made_q3 flag using F1BusinessLogic
            q3_df = self._add_made_q3_flag(gap_df)
            logger.info("Q3 participation flags added")
            
            # Step 4: Add season progression metrics
            progression_df = self._add_season_progression(q3_df)
            logger.info("Season progression metrics added")
            
            # Step 5: Finalize with audit columns
            final_df = self._finalize_qualifying_data(progression_df)
            logger.info(f"Qualifying transform completed: {final_df.count()} records")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Qualifying transform failed: {e}")
            raise

    def _create_qualifying_base(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Create base qualifying dataset by joining Silver tables.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with base qualifying data
        """
        sessions_df = source_data['sessions_silver']
        drivers_df = source_data['drivers_silver']
        qualifying_results_df = source_data['qualifying_results_silver']
        
        # Join sessions, qualifying results, and drivers
        base_df = sessions_df.alias("s") \
            .join(
                qualifying_results_df.alias("qr"), 
                col("s.session_key") == col("qr.session_key"), 
                "inner"
            ) \
            .join(
                drivers_df.alias("d"), 
                col("qr.driver_number") == col("d.driver_number"), 
                "inner"
            ) \
            .select(
                col("s.session_key"),
                col("s.grand_prix_name"),
                col("s.date_start").cast(DateType()).alias("race_date"),
                col("d.driver_number"),
                col("d.broadcast_name").alias("driver_name"),
                col("d.team_name"),
                col("qr.position").alias("qualifying_position"),
                # Convert milliseconds to seconds for gap calculation
                (col("qr.fastest_qualifying_time_millis") / 1000.0).alias("qualifying_time_seconds"),
                col("s.year")
            )
        
        # Add round_number derived from chronological race order within season
        round_number_window = Window.partitionBy("year").orderBy("race_date")
        base_df_with_round = base_df.withColumn(
            "round_number",
            dense_rank().over(round_number_window)
        )
        
        return base_df_with_round
    
    def _calculate_qualifying_gaps(self, df: DataFrame) -> DataFrame:
        """
        Calculate gaps to pole position using F1BusinessLogic.
        
        Args:
            df: DataFrame with base qualifying data
            
        Returns:
            DataFrame with gap_to_pole_seconds column added
        """
        logger.info("⚡ Calculating gaps to pole using F1BusinessLogic")
        
        # Find pole time for each race using window functions
        pole_window = Window.partitionBy("race_date", "grand_prix_name")
        pole_time = min("qualifying_time_seconds").over(pole_window)
        
        # Use F1BusinessLogic for gap calculation
        # This follows the same logic as calculate_gap_to_pole but applied at scale
        return df.withColumn(
            "pole_time_temp",
            pole_time
        ).withColumn(
            "gap_to_pole_seconds",
            when(
                col("qualifying_time_seconds").isNotNull() & 
                col("pole_time_temp").isNotNull(),
                # Ensure gap is never negative (pole position should be 0.0)
                when(
                    col("qualifying_time_seconds") - col("pole_time_temp") >= 0.0,
                    col("qualifying_time_seconds") - col("pole_time_temp")
                ).otherwise(lit(0.0))
            ).otherwise(None).cast(DecimalType(5, 3))
        ).drop("pole_time_temp")
    
    def _add_made_q3_flag(self, df: DataFrame) -> DataFrame:
        """
        Add made_q3 flag using F1BusinessLogic.
        
        Args:
            df: DataFrame with qualifying data
            
        Returns:
            DataFrame with made_q3 column added
        """
        logger.info("🏁 Adding Q3 participation flags using F1BusinessLogic")
        
        # Use F1BusinessLogic UDF for Q3 determination
        made_q3_udf = F1BusinessLogic.calculate_made_q3_spark_udf()
        
        return df.withColumn(
            "made_q3",
            made_q3_udf(col("qualifying_position"))
        )
    
    def _add_season_progression(self, df: DataFrame) -> DataFrame:
        """
        Add season progression metrics using window utilities.
        
        Args:
            df: DataFrame with qualifying data
            
        Returns:
            DataFrame with season progression metrics
        """
        logger.info("📈 Adding qualifying season progression metrics")
        
        # Use WindowFunctionUtils for standardized window functions
        df_with_averages = WindowFunctionUtils.add_running_averages(
            df=df,
            group_cols=["driver_number"],
            avg_cols=["qualifying_position"],
            order_cols=["race_date", "round_number"]
        )
        
        # Rename the generated column to match Gold schema
        return df_with_averages.withColumnRenamed(
            "qualifying_position_running_avg",
            "season_avg_qualifying_position"
        ).withColumn(
            "season_avg_qualifying_position",
            col("season_avg_qualifying_position").cast(DecimalType(4, 1))
        )
    
    def _finalize_qualifying_data(self, df: DataFrame) -> DataFrame:
        """
        Finalize qualifying data with audit columns and correct schema.
        
        Args:
            df: DataFrame with all qualifying transformations applied
            
        Returns:
            DataFrame matching Gold schema exactly
        """
        # Select only columns that match Gold schema with audit columns
        return df.select(
            col("session_key"),
            col("grand_prix_name"),
            col("race_date"),
            col("round_number"),
            col("driver_number"),
            col("driver_name"),
            col("team_name"),
            col("qualifying_position"),
            col("gap_to_pole_seconds"),
            col("made_q3"),
            col("season_avg_qualifying_position"),
            col("year"),
            current_timestamp().alias("created_timestamp"),
            current_timestamp().alias("updated_timestamp")
        )
