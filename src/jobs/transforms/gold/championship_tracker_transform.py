"""
Championship Tracker Gold Table Transformer.

This module transforms Silver race session data into championship progression tracking,
showing how championship standings evolve race by race throughout the season.
RACE SESSIONS ONLY - no qualifying data included.

Updated to align with simplified Gold schema and use enhanced utilities.
"""

import logging
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

from jobs.transforms.base_transformer import BaseTransformer
from src.jobs.utils.analytics.business_logic import F1BusinessLogic
from src.jobs.utils.analytics.window_functions import WindowFunctionUtils, add_driver_season_progression
from src.jobs.utils.table_management.schemas import get_table_schema
from src.jobs.utils.data_access.silver_readers import (
    read_sessions_silver, read_drivers_silver, read_race_results_silver,
    read_silver_tables_for_transform, validate_silver_data
)
from src.jobs.utils.analytics.broadcast_utils import (
    get_broadcast_config_for_transform, log_broadcast_performance_stats,
    validate_broadcast_decisions
)

logger = logging.getLogger(__name__)


class ChampionshipTrackerTransform(BaseTransformer):
    """
    Championship Tracker Gold table transformer.
    
    This transformer creates championship progression tracking showing how
    standings evolve race by race throughout the season.
    
    Gold Schema Columns (Simplified):
    - race_date, grand_prix_name, round_number
    - driver_number, driver_name, team_name
    - finish_position, race_points, season_points_total
    - championship_position, points_gap_to_leader
    - year, created_timestamp, updated_timestamp
    
    Key Features:
    - Year-only partitioning for time-series analysis
    - Cumulative points tracking with proper tie-breaking
    - Points gap to championship leader calculation
    """
    
    @property
    def table_name(self) -> str:
        return "championship_tracker"

    @property
    def source_tables(self) -> List[str]:
        """Return list of Silver tables this transformer reads from."""
        return [
            'sessions_silver',      # Race session metadata
            'drivers_silver',       # Driver information
            'race_results_silver'   # Race results and points
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
        
        logger.info(f"Reading Silver data for championship tracker (year={year}, gp={grand_prix_filter}, mode={processing_mode})")
        
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
                'race_results_silver': {
                    'columns': [
                        'session_key', 'driver_number', 'position',
                        'points', 'validated_points',
                        'year', 'grand_prix_name'
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
            
            # Filter sessions to race only (post-read filtering)
            sessions_df = source_data['sessions_silver'].filter(
                col("session_type") == "Race"
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
        Transform Silver data into Gold championship_tracker table.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with Gold schema for championship_tracker
        """
        logger.info("Starting championship tracker transformation")
        
        try:
            # Step 1: Create base championship dataset
            base_df = self._create_championship_base(source_data)
            logger.info(f"Base championship dataset created: {base_df.count()} records")
            
            # Step 2: Validate and clean race points
            points_df = self._validate_race_points(base_df)
            logger.info("Race points validation completed")
            
            # Step 3: Add cumulative points using WindowFunctionUtils
            cumulative_df = self._add_cumulative_points(points_df)
            logger.info("Cumulative points calculations completed")
            
            # Step 4: Add championship positions with proper tie-breaking
            positions_df = self._add_championship_positions(cumulative_df)
            logger.info("Championship positions calculated")
            
            # Step 5: Add points gaps to leader
            gaps_df = self._add_points_gaps_to_leader(positions_df)
            logger.info("Points gaps to leader calculated")
            
            # Step 6: Finalize with audit columns
            final_df = self._finalize_championship_data(gaps_df)
            logger.info(f"Championship tracker completed: {final_df.count()} records")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Championship tracker transform failed: {e}")
            raise
    
    def _create_championship_base(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Create base championship dataset by joining Silver tables.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with base championship data
        """
        sessions_df = source_data['sessions_silver']
        drivers_df = source_data['drivers_silver']
        race_results_df = source_data['race_results_silver']
        
        # Join sessions, race results, and drivers
        base_df = sessions_df.alias("s") \
            .join(
                race_results_df.alias("rr"), 
                col("s.session_key") == col("rr.session_key"), 
                "inner"
            ) \
            .join(
                drivers_df.alias("d"), 
                col("rr.driver_number") == col("d.driver_number"), 
                "inner"
            ) \
            .select(
                col("s.grand_prix_name"),
                col("s.date_start").cast(DateType()).alias("race_date"),
                col("d.driver_number"),
                col("d.broadcast_name").alias("driver_name"),
                col("d.team_name"),
                col("rr.position").alias("finish_position"),
                # Use validated_points if available, otherwise use raw points
                coalesce(col("rr.validated_points"), col("rr.points")).alias("race_points"),
                col("s.year")
            )
        
        # Add round_number derived from chronological race order within season
        round_number_window = Window.partitionBy("year").orderBy("race_date")
        base_df_with_round = base_df.withColumn(
            "round_number",
            dense_rank().over(round_number_window)
        )
        
        return base_df_with_round
    
    def _validate_race_points(self, df: DataFrame) -> DataFrame:
        """
        Validate and clean race points using F1BusinessLogic.
        
        Args:
            df: DataFrame with championship data
            
        Returns:
            DataFrame with validated race points
        """
        logger.info("🏆 Validating race points using F1BusinessLogic")
        
        # Ensure race_points are cast to proper decimal type and within valid range
        return df.withColumn(
            "race_points",
            when(
                col("race_points").isNotNull() & 
                (col("race_points") >= 0) & 
                (col("race_points") <= 26),  # Max points with fastest lap
                col("race_points").cast(DecimalType(4, 1))
            ).otherwise(lit(0.0).cast(DecimalType(4, 1)))
        )
    
    def _add_cumulative_points(self, df: DataFrame) -> DataFrame:
        """
        Add cumulative points using WindowFunctionUtils.
        
        Args:
            df: DataFrame with championship data
            
        Returns:
            DataFrame with season_points_total column
        """
        logger.info("📈 Adding cumulative points using WindowFunctionUtils")
        
        # Use WindowFunctionUtils to add running totals for points
        df_with_totals = WindowFunctionUtils.add_running_totals(
            df=df,
            group_cols=["driver_number"],
            sum_cols=["race_points"],
            order_cols=["race_date", "round_number"]
        )
        
        # Rename the generated column to match Gold schema
        return df_with_totals.withColumnRenamed(
            "race_points_running_total",
            "season_points_total"
        ).withColumn(
            "season_points_total",
            col("season_points_total").cast(DecimalType(6, 1))
        )
    
    def _add_championship_positions(self, df: DataFrame) -> DataFrame:
        """
        Add championship positions using WindowFunctionUtils with proper tie-breaking.
        
        Args:
            df: DataFrame with cumulative points
            
        Returns:
            DataFrame with championship_position column
        """
        logger.info("🏅 Adding championship positions with proper tie-breaking")
        
        # Use WindowFunctionUtils for championship positions
        return WindowFunctionUtils.add_championship_positions(
            df=df,
            points_col="season_points_total",
            partition_cols=["race_date"],  # Position after each race
            position_col_name="championship_position"
        )
    
    def _add_points_gaps_to_leader(self, df: DataFrame) -> DataFrame:
        """
        Add points gaps to championship leader using WindowFunctionUtils.
        
        Args:
            df: DataFrame with championship positions
            
        Returns:
            DataFrame with points_gap_to_leader column
        """
        logger.info("⚡ Adding points gaps to leader using WindowFunctionUtils")
        
        # Use WindowFunctionUtils for gaps to leader
        return WindowFunctionUtils.add_points_gaps_to_leader(
            df=df,
            points_col="season_points_total",
            partition_cols=["race_date"],  # Gap calculation per race
            gap_col_name="points_gap_to_leader"
        ).withColumn(
            "points_gap_to_leader",
            col("points_gap_to_leader").cast(DecimalType(6, 1))
        )
    
    def _finalize_championship_data(self, df: DataFrame) -> DataFrame:
        """
        Finalize championship data with audit columns and correct schema.
        
        Args:
            df: DataFrame with all championship transformations applied
            
        Returns:
            DataFrame matching Gold schema exactly
        """
        # Select only columns that match Gold schema with audit columns
        return df.select(
            col("race_date"),
            col("grand_prix_name"),
            col("round_number"),
            col("driver_number"),
            col("driver_name"),
            col("team_name"),
            col("finish_position"),
            col("race_points"),
            col("season_points_total"),
            col("championship_position"),
            col("points_gap_to_leader"),
            col("year"),
            current_timestamp().alias("created_timestamp"),
            current_timestamp().alias("updated_timestamp")
        )
