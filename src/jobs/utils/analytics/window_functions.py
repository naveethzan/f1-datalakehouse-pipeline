"""
Window Function Utilities for F1 Gold Layer Transformations.

This module provides reusable window function utilities specifically designed
for F1 analytics calculations including championship progression, ranking,
and season metrics.

Key Features:
- Season progression windows for cumulative calculations
- Championship ranking windows for position calculations  
- Points gap calculations for championship analysis
- Team aggregation windows for constructor standings
- Helper methods for applying window functions to DataFrames
"""

from typing import List, Optional, Dict, Any
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    sum as spark_sum, avg, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lead, lag,
    col, desc, asc, when, lit, coalesce
)
import logging

logger = logging.getLogger(__name__)


class WindowFunctionUtils:
    """
    Utility class for creating and applying window functions in F1 Gold transformations.
    
    This class provides standardized window specifications and DataFrame helper methods
    for common F1 analytics patterns including championship progression, team rankings,
    and season metrics.
    """
    
    # Standard partition and ordering for F1 analytics (defined as properties to avoid PySpark context issues)
    @property
    def DRIVER_PARTITION(self):
        return ["driver_number"]
    
    @property
    def TEAM_PARTITION(self):
        return ["team_name"]
    
    @property 
    def RACE_ORDER(self):
        return ["race_date", "round_number"]
    
    @property
    def POINTS_ORDER_DESC(self):
        return [desc("season_points_total"), asc("driver_number")]
    
    @property
    def CONSTRUCTOR_POINTS_ORDER_DESC(self):
        return [desc("constructor_points_total"), asc("team_name")]
    
    @staticmethod
    def create_season_progression_window(
        partition_cols: List[str], 
        order_cols: Optional[List[str]] = None
    ) -> Window:
        """
        Create window specification for season progression calculations.
        
        Used for running totals like cumulative points, average positions,
        and other season-long metrics that need chronological ordering.
        
        Args:
            partition_cols: Columns to partition by (e.g., ['driver_number'])
            order_cols: Columns to order by (default: race chronological order)
            
        Returns:
            Window specification for running calculations
            
        Examples:
            >>> # For driver season points progression
            >>> window = WindowFunctionUtils.create_season_progression_window(['driver_number'])
            >>> df = df.withColumn('season_points_total', 
            ...                   spark_sum('race_points').over(window))
        """
        if order_cols is None:
            order_cols = ["race_date", "round_number"]  # WindowFunctionUtils.RACE_ORDER
            
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        return window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    @staticmethod  
    def create_championship_ranking_window(
        date_col: str = "race_date",
        partition_cols: Optional[List[str]] = None
    ) -> Window:
        """
        Create window specification for championship position ranking.
        
        Used for calculating championship positions based on points total
        with proper tie-breaking by driver number.
        
        Args:
            date_col: Date column for temporal partitioning
            partition_cols: Additional partition columns (e.g., ['year'] for season-wide ranking)
            
        Returns:
            Window specification for championship ranking
            
        Examples:
            >>> # For championship position at each race
            >>> window = WindowFunctionUtils.create_championship_ranking_window()
            >>> df = df.withColumn('championship_position', 
            ...                   rank().over(window))
        """
        if partition_cols is None:
            partition_cols = [date_col]
        elif date_col not in partition_cols:
            partition_cols = partition_cols + [date_col]
            
        return Window.partitionBy(*partition_cols).orderBy(desc("season_points_total"), asc("driver_number"))
    
    @staticmethod
    def create_constructor_ranking_window(
        date_col: str = "race_date",
        partition_cols: Optional[List[str]] = None  
    ) -> Window:
        """
        Create window specification for constructor championship ranking.
        
        Used for calculating constructor championship positions based on team points.
        
        Args:
            date_col: Date column for temporal partitioning
            partition_cols: Additional partition columns (e.g., ['year'])
            
        Returns:
            Window specification for constructor ranking
        """
        if partition_cols is None:
            partition_cols = [date_col]
        elif date_col not in partition_cols:
            partition_cols = partition_cols + [date_col]
            
        return Window.partitionBy(*partition_cols).orderBy(desc("constructor_points_total"), asc("team_name"))
    
    @staticmethod
    def create_points_gap_window(
        partition_cols: List[str]
    ) -> Window:
        """
        Create window specification for points gap calculations.
        
        Used for calculating gaps to championship leader or position ahead.
        
        Args:
            partition_cols: Partition columns (typically includes race date)
            
        Returns:
            Window specification for gap calculations
        """
        return Window.partitionBy(*partition_cols)
    
    @staticmethod
    def create_recent_form_window(
        partition_cols: List[str],
        periods: int = 3,
        order_cols: Optional[List[str]] = None
    ) -> Window:
        """
        Create window specification for recent form calculations.
        
        Used for calculating metrics over recent races (e.g., last 3 races).
        
        Args:
            partition_cols: Columns to partition by (e.g., ['driver_number'])
            periods: Number of recent periods to include (default: 3)
            order_cols: Columns to order by (default: race chronological order)
            
        Returns:
            Window specification for recent form calculations
        """
        if order_cols is None:
            order_cols = ["race_date", "round_number"]  # WindowFunctionUtils.RACE_ORDER
            
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        return window_spec.rowsBetween(-(periods - 1), Window.currentRow)
    
    @staticmethod
    def create_teammate_comparison_window(
        team_col: str = "team_name",
        race_col: str = "race_date"
    ) -> Window:
        """
        Create window specification for teammate comparisons.
        
        Used for comparing teammates within the same race weekend.
        
        Args:
            team_col: Team column name
            race_col: Race date column name
            
        Returns:
            Window specification for teammate comparisons
        """
        return Window.partitionBy(team_col, race_col).orderBy("driver_number")
    
    @staticmethod
    def add_running_totals(
        df: DataFrame,
        group_cols: List[str],
        sum_cols: List[str],
        order_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Add running total columns to DataFrame using window functions.
        
        Args:
            df: Input DataFrame
            group_cols: Columns to group by (partition)
            sum_cols: Columns to sum (running totals)
            order_cols: Columns to order by (default: race chronological order)
            
        Returns:
            DataFrame with running total columns added
            
        Examples:
            >>> # Add season points total for each driver
            >>> df = WindowFunctionUtils.add_running_totals(
            ...     df, ['driver_number'], ['race_points'])
        """
        if order_cols is None:
            order_cols = WindowFunctionUtils.RACE_ORDER
            
        window_spec = WindowFunctionUtils.create_season_progression_window(
            group_cols, order_cols
        )
        
        result_df = df
        for col_name in sum_cols:
            total_col_name = f"{col_name}_running_total"
            result_df = result_df.withColumn(
                total_col_name,
                spark_sum(col(col_name)).over(window_spec)
            )
            
        return result_df
    
    @staticmethod
    def add_running_averages(
        df: DataFrame,
        group_cols: List[str],
        avg_cols: List[str],
        order_cols: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Add running average columns to DataFrame using window functions.
        
        Args:
            df: Input DataFrame  
            group_cols: Columns to group by (partition)
            avg_cols: Columns to average (running averages)
            order_cols: Columns to order by (default: race chronological order)
            
        Returns:
            DataFrame with running average columns added
        """
        if order_cols is None:
            order_cols = WindowFunctionUtils.RACE_ORDER
            
        window_spec = WindowFunctionUtils.create_season_progression_window(
            group_cols, order_cols
        )
        
        result_df = df
        for col_name in avg_cols:
            avg_col_name = f"{col_name}_running_avg"
            result_df = result_df.withColumn(
                avg_col_name,
                avg(col(col_name)).over(window_spec)
            )
            
        return result_df
    
    @staticmethod
    def add_championship_positions(
        df: DataFrame,
        points_col: str = "season_points_total",
        partition_cols: Optional[List[str]] = None,
        position_col_name: str = "championship_position"
    ) -> DataFrame:
        """
        Add championship position rankings to DataFrame.
        
        Args:
            df: Input DataFrame
            points_col: Points column for ranking
            partition_cols: Partition columns (e.g., ['race_date'] for race-by-race)
            position_col_name: Name for the position column
            
        Returns:
            DataFrame with championship positions added
        """
        if partition_cols is None:
            # Assume we want positions for each race date
            partition_cols = ["race_date"]
            
        window_spec = Window.partitionBy(*partition_cols).orderBy(
            desc(points_col), asc("driver_number")
        )
        
        return df.withColumn(position_col_name, rank().over(window_spec))
    
    @staticmethod
    def add_points_gaps_to_leader(
        df: DataFrame,
        points_col: str = "season_points_total",
        partition_cols: Optional[List[str]] = None,
        gap_col_name: str = "points_gap_to_leader"
    ) -> DataFrame:
        """
        Add points gap to championship leader.
        
        Args:
            df: Input DataFrame
            points_col: Points column for gap calculation
            partition_cols: Partition columns (e.g., ['race_date'])
            gap_col_name: Name for the gap column
            
        Returns:
            DataFrame with points gaps to leader added
        """
        if partition_cols is None:
            partition_cols = ["race_date"]
            
        window_spec = Window.partitionBy(*partition_cols)
        leader_points = spark_max(col(points_col)).over(window_spec)
        
        return df.withColumn(
            gap_col_name,
            leader_points - col(points_col)
        )
    
    @staticmethod
    def add_constructor_aggregations(
        df: DataFrame,
        team_col: str = "team_name",
        partition_cols: Optional[List[str]] = None,
        agg_cols: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Add constructor-level aggregations (sum both drivers' results).
        
        Args:
            df: Input DataFrame (driver-level data)
            team_col: Team column name
            partition_cols: Partition columns (e.g., ['race_date'])
            agg_cols: Dict mapping column names to aggregation types
            
        Returns:
            DataFrame with constructor aggregations added
        """
        if partition_cols is None:
            partition_cols = ["race_date"]
            
        if agg_cols is None:
            agg_cols = {"race_points": "sum"}
            
        window_spec = Window.partitionBy(*(partition_cols + [team_col]))
        
        result_df = df
        for col_name, agg_type in agg_cols.items():
            new_col_name = f"team_{col_name}_{agg_type}"
            
            if agg_type == "sum":
                result_df = result_df.withColumn(
                    new_col_name,
                    spark_sum(col(col_name)).over(window_spec)
                )
            elif agg_type == "avg":
                result_df = result_df.withColumn(
                    new_col_name,
                    avg(col(col_name)).over(window_spec)
                )
            elif agg_type == "max":
                result_df = result_df.withColumn(
                    new_col_name,
                    spark_max(col(col_name)).over(window_spec)
                )
            elif agg_type == "min":
                result_df = result_df.withColumn(
                    new_col_name,
                    spark_min(col(col_name)).over(window_spec)
                )
                
        return result_df
    
    @staticmethod
    def add_lag_lead_comparisons(
        df: DataFrame,
        partition_cols: List[str],
        order_cols: List[str],
        comparison_cols: List[str],
        periods: int = 1
    ) -> DataFrame:
        """
        Add lag/lead comparisons for race-to-race analysis.
        
        Args:
            df: Input DataFrame
            partition_cols: Partition columns (e.g., ['driver_number'])
            order_cols: Order columns (e.g., ['race_date'])
            comparison_cols: Columns to compare across periods
            periods: Number of periods to lag/lead (default: 1)
            
        Returns:
            DataFrame with lag/lead comparison columns added
        """
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        
        result_df = df
        for col_name in comparison_cols:
            # Add lag (previous value)
            result_df = result_df.withColumn(
                f"{col_name}_prev",
                lag(col(col_name), periods).over(window_spec)
            )
            
            # Add lead (next value)  
            result_df = result_df.withColumn(
                f"{col_name}_next",
                lead(col(col_name), periods).over(window_spec)
            )
            
            # Add change from previous
            result_df = result_df.withColumn(
                f"{col_name}_change",
                col(col_name) - coalesce(col(f"{col_name}_prev"), lit(0))
            )
            
        return result_df


class F1WindowPatterns:
    """
    Pre-configured window patterns specific to F1 analytics use cases.
    
    This class provides ready-to-use window specifications for the most common
    F1 analytics patterns, reducing boilerplate code in transforms.
    """
    
    @staticmethod
    def driver_season_progression() -> Window:
        """Window for driver season progression (points, averages, etc.)."""
        return WindowFunctionUtils.create_season_progression_window(
            WindowFunctionUtils.DRIVER_PARTITION
        )
    
    @staticmethod  
    def team_season_progression() -> Window:
        """Window for team season progression (constructor points, etc.)."""
        return WindowFunctionUtils.create_season_progression_window(
            WindowFunctionUtils.TEAM_PARTITION
        )
    
    @staticmethod
    def championship_standings_by_race() -> Window:
        """Window for championship standings after each race."""
        return WindowFunctionUtils.create_championship_ranking_window("race_date")
    
    @staticmethod
    def constructor_standings_by_race() -> Window:
        """Window for constructor standings after each race."""
        return WindowFunctionUtils.create_constructor_ranking_window("race_date")
    
    @staticmethod
    def qualifying_season_averages() -> Window:
        """Window for qualifying season averages."""
        return WindowFunctionUtils.create_season_progression_window(
            WindowFunctionUtils.DRIVER_PARTITION
        )
    
    @staticmethod
    def teammate_race_comparison() -> Window:
        """Window for comparing teammates in the same race."""
        return WindowFunctionUtils.create_teammate_comparison_window()
    
    @staticmethod
    def recent_form_last_3_races() -> Window:
        """Window for recent form over last 3 races."""
        return WindowFunctionUtils.create_recent_form_window(
            WindowFunctionUtils.DRIVER_PARTITION, periods=3
        )


# Convenience functions for common F1 analytics
def add_driver_season_progression(df: DataFrame) -> DataFrame:
    """
    Add standard driver season progression columns.
    
    Adds:
    - season_points_total (cumulative points)
    - championship_position (rank by points)
    - points_gap_to_leader
    """
    # Add running points total
    window_progression = F1WindowPatterns.driver_season_progression()
    df = df.withColumn(
        "season_points_total", 
        spark_sum("race_points").over(window_progression)
    )
    
    # Add championship positions
    df = WindowFunctionUtils.add_championship_positions(df)
    
    # Add gaps to leader
    df = WindowFunctionUtils.add_points_gaps_to_leader(df)
    
    return df


def add_constructor_season_progression(df: DataFrame) -> DataFrame:
    """
    Add standard constructor season progression columns.
    
    Adds:
    - constructor_points_total (cumulative team points)
    - constructor_position (rank by points)
    """
    # Add team points aggregation first
    df = WindowFunctionUtils.add_constructor_aggregations(df)
    
    # Add running constructor totals
    window_progression = F1WindowPatterns.team_season_progression()
    df = df.withColumn(
        "constructor_points_total",
        spark_sum("team_race_points_sum").over(window_progression)
    )
    
    # Add constructor positions
    window_ranking = F1WindowPatterns.constructor_standings_by_race()
    df = df.withColumn(
        "constructor_position",
        rank().over(window_ranking)
    )
    
    return df


def add_qualifying_season_metrics(df: DataFrame) -> DataFrame:
    """
    Add standard qualifying season metrics.
    
    Adds:
    - season_avg_qualifying_position
    - q3_appearances_season_total (if made_q3 column exists)
    """
    window_progression = F1WindowPatterns.qualifying_season_averages()
    
    # Add season average qualifying position
    df = df.withColumn(
        "season_avg_qualifying_position",
        avg("qualifying_position").over(window_progression)
    )
    
    # Add Q3 appearances total if made_q3 column exists
    if "made_q3" in df.columns:
        df = df.withColumn(
            "q3_appearances_season_total",
            spark_sum(when(col("made_q3"), 1).otherwise(0)).over(window_progression)
        )
    
    return df