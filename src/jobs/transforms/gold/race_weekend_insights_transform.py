"""
Race Weekend Insights Gold Table Transformer.

This module transforms Silver data into simplified weekend summaries focused
on core insights: pole position, race winner, pole-to-win conversion, and
basic race statistics (finishers, DNFs).

Aligned with simplified Gold schema for efficient analytics queries.
"""

import logging
import time
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import DateType, DecimalType

from jobs.transforms.base_transformer import BaseTransformer
from src.jobs.utils.analytics.business_logic import F1BusinessLogic
from src.jobs.utils.data_access.silver_readers import read_silver_tables_for_transform
from src.jobs.utils.analytics.broadcast_utils import (
    get_broadcast_config_for_transform,
    validate_broadcast_decisions,
    log_broadcast_performance_stats
)
from src.jobs.utils.validation import validate_silver_data

logger = logging.getLogger(__name__)


class RaceWeekendInsightsTransform(BaseTransformer):
    """
    Race Weekend Insights Gold table transformer.
    
    Simplified transform focused on core weekend insights aligned with Gold schema:
    - Pole position and race winner identification
    - Pole-to-win conversion tracking  
    - Basic race statistics (finishers, DNFs)
    - Essential race metadata (date, round, GP name)
    
    Gold Schema Columns:
    - grand_prix_name, race_date, round_number
    - pole_position_driver, race_winner_driver, race_winner_team
    - pole_to_win, total_finishers, dnf_count
    - year, created_timestamp, updated_timestamp
    """

    @property
    def table_name(self) -> str:
        return "race_weekend_insights"

    @property
    def source_tables(self) -> List[str]:
        """Return list of Silver tables this transformer reads from."""
        return [
            'sessions_silver',      # Race session metadata
            'drivers_silver',       # Driver information  
            'qualifying_results_silver',  # Qualifying positions for pole
            'race_results_silver'   # Race results for winner and finisher counts
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
        
        logger.info(f"Reading Silver data for race weekend insights (year={year}, gp={grand_prix_filter}, mode={processing_mode})")
        
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
                        'grand_prix_name', 'date_start', 'round_number', 'year'
                    ]
                },
                'drivers_silver': {
                    'columns': [
                        'driver_number', 'broadcast_name', 'team_name', 'is_current'
                    ]
                },
                'qualifying_results_silver': {
                    'columns': [
                        'session_key', 'driver_number', 'position',
                        'year', 'grand_prix_name'
                    ]
                },
                'race_results_silver': {
                    'columns': [
                        'session_key', 'driver_number', 'position',
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
            
            # Filter sessions to qualifying and race only (post-read filtering)
            sessions_df = source_data['sessions_silver'].filter(
                col("session_type").isin(["Qualifying", "Race"])
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
        Transform Silver data into Gold race_weekend_insights table.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with Gold schema for race_weekend_insights
        """
        logger.info("Starting race weekend insights transformation")
        
        try:
            # Step 1: Create base race weekend dataset
            base_df = self._create_base_weekend_data(source_data)
            logger.info(f"Base weekend dataset created: {base_df.count()} records")
            
            # Step 2: Add pole position information
            pole_df = self._add_pole_position_info(base_df, source_data)
            logger.info("Pole position information added")
            
            # Step 3: Add race winner information
            winner_df = self._add_race_winner_info(pole_df, source_data)
            logger.info("Race winner information added")
            
            # Step 4: Calculate pole-to-win flag
            pole_to_win_df = self._calculate_pole_to_win(winner_df)
            logger.info("Pole-to-win calculations completed")
            
            # Step 5: Add race statistics (finishers, DNFs)
            stats_df = self._add_race_statistics(pole_to_win_df, source_data)
            logger.info("Race statistics calculated")
            
            # Step 6: Finalize with audit columns
            final_df = self._finalize_weekend_insights(stats_df)
            logger.info(f"Race weekend insights completed: {final_df.count()} records")
            
            return final_df
            
        except Exception as e:
            logger.error(f"❌ Race weekend insights transform failed: {e}")
            raise
    
    def _create_base_weekend_data(self, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Create base race weekend dataset from race sessions.
        
        Args:
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with base race weekend information
        """
        # Get race sessions only for the base dataset
        race_sessions = source_data['sessions_silver'].filter(
            col("session_type") == "Race"
        )
        
        return race_sessions.select(
            col("grand_prix_name"),
            col("date_start").cast(DateType()).alias("race_date"),
            col("round_number"),
            col("year")
        ).distinct()
    
    def _add_pole_position_info(self, base_df: DataFrame, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Add pole position driver information to base weekend data.
        
        Args:
            base_df: Base weekend DataFrame
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with pole position information added
        """
        logger.info("🥇 Adding pole position information using F1BusinessLogic")
        
        # Get qualifying sessions and pole positions
        qualifying_sessions = source_data['sessions_silver'].filter(
            col("session_type") == "Qualifying"
        )
        
        # Find pole position winners (position = 1 in qualifying)
        pole_positions = qualifying_sessions.alias("qs") \
            .join(
                source_data['qualifying_results_silver'].alias("qr"),
                col("qs.session_key") == col("qr.session_key"),
                "inner"
            ) \
            .join(
                source_data['drivers_silver'].alias("d"),
                col("qr.driver_number") == col("d.driver_number"),
                "inner"
            ) \
            .filter(col("qr.position") == 1) \
            .select(
                col("qs.grand_prix_name"),
                col("d.broadcast_name").alias("pole_position_driver")
            )
        
        # Left join to handle cases where pole position might be missing
        return base_df.join(
            pole_positions,
            "grand_prix_name",
            "left"
        )
    
    def _add_race_winner_info(self, base_df: DataFrame, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Add race winner information to weekend data.
        
        Args:
            base_df: Base weekend DataFrame with pole position info
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with race winner information added
        """
        logger.info("🏆 Adding race winner information using F1BusinessLogic")
        
        # Get race sessions and race winners
        race_sessions = source_data['sessions_silver'].filter(
            col("session_type") == "Race"
        )
        
        # Find race winners (position = 1 in race results)
        race_winners = race_sessions.alias("rs") \
            .join(
                source_data['race_results_silver'].alias("rr"),
                col("rs.session_key") == col("rr.session_key"),
                "inner"
            ) \
            .join(
                source_data['drivers_silver'].alias("d"),
                col("rr.driver_number") == col("d.driver_number"),
                "inner"
            ) \
            .filter(col("rr.position") == 1) \
            .select(
                col("rs.grand_prix_name"),
                col("d.broadcast_name").alias("race_winner_driver"),
                col("d.team_name").alias("race_winner_team")
            )
        
        # Inner join since every race should have a winner
        return base_df.join(
            race_winners,
            "grand_prix_name",
            "inner"
        )
    
    def _calculate_pole_to_win(self, df: DataFrame) -> DataFrame:
        """
        Calculate pole-to-win flag using F1BusinessLogic.
        
        Args:
            df: DataFrame with pole and race winner information
            
        Returns:
            DataFrame with pole_to_win boolean flag
        """
        logger.info("🚩 Calculating pole-to-win conversions")
        
        # Calculate pole-to-win flag
        return df.withColumn(
            "pole_to_win",
            when(
                col("pole_position_driver").isNotNull() & 
                (col("pole_position_driver") == col("race_winner_driver")),
                lit(True)
            ).otherwise(lit(False))
        )
    
    def _add_race_statistics(self, base_df: DataFrame, source_data: Dict[str, DataFrame]) -> DataFrame:
        """
        Add race statistics (total finishers and DNF count) using F1BusinessLogic.
        
        Args:
            base_df: Base DataFrame with pole and winner info
            source_data: Dictionary containing Silver DataFrames
            
        Returns:
            DataFrame with race statistics added
        """
        logger.info("📊 Calculating race statistics using F1BusinessLogic")
        
        # Get race sessions for statistics
        race_sessions = source_data['sessions_silver'].filter(
            col("session_type") == "Race"
        )
        
        # Calculate finishers and DNF counts using F1 business logic
        race_stats = race_sessions.alias("rs") \
            .join(
                source_data['race_results_silver'].alias("rr"),
                col("rs.session_key") == col("rr.session_key"),
                "inner"
            ) \
            .groupBy("rs.grand_prix_name") \
            .agg(
                # Use F1BusinessLogic.calculate_dnf_count equivalent logic
                count(
                    when(
                        col("rr.position").isNotNull() & 
                        (col("rr.position") >= 1) & 
                        (col("rr.position") <= 20),
                        1
                    )
                ).alias("total_finishers"),
                count(
                    when(
                        col("rr.position").isNull() | 
                        (col("rr.position") < 1) | 
                        (col("rr.position") > 20),
                        1
                    )
                ).alias("dnf_count")
            )
        
        # Join race statistics to base DataFrame
        return base_df.join(
            race_stats,
            base_df.grand_prix_name == race_stats.grand_prix_name,
            "inner"
        ).drop(race_stats.grand_prix_name)
    
    def _finalize_weekend_insights(self, df: DataFrame) -> DataFrame:
        """
        Finalize weekend insights with audit columns and correct schema.
        
        Args:
            df: DataFrame with all weekend insights transformations applied
            
        Returns:
            DataFrame matching Gold schema exactly
        """
        # Select only columns that match Gold schema with audit columns
        return df.select(
            col("grand_prix_name"),
            col("race_date"),
            col("round_number"),
            col("pole_position_driver"),
            col("race_winner_driver"),
            col("race_winner_team"),
            col("pole_to_win"),
            col("total_finishers"),
            col("dnf_count"),
            col("year"),
            current_timestamp().alias("created_timestamp"),
            current_timestamp().alias("updated_timestamp")
        )

