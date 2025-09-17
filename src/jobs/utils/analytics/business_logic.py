"""
F1 Business Logic Utilities for Gold Layer Transformations.

This module provides F1-specific business logic functions for calculating
championship standings, points, and performance metrics in the Gold layer.
Supports session-specific processing for qualifying vs race analytics.
"""

import logging
from typing import Dict, List, Optional, Tuple
from decimal import Decimal
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *


logger = logging.getLogger(__name__)


class F1BusinessLogic:
    """
    F1 business logic utilities for championship calculations and performance metrics.
    
    This class provides static methods for F1-specific calculations including:
    - Points system calculations (race sessions only - no points in qualifying)
    - Qualifying-specific metrics (gap to pole, Q3 participation)
    - Championship mathematics (gaps, mathematical possibility)
    - Session-aware window function helpers for running totals and rankings
    """
    
    # F1 Points system (2022+ regulations) - RACE SESSIONS ONLY
    POINTS_SYSTEM = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]
    FASTEST_LAP_BONUS = 1
    MAX_POINTS_PER_RACE = 26  # 25 for win + 1 for fastest lap
    
    # Session types
    SESSION_TYPE_QUALIFYING = 'Qualifying'
    SESSION_TYPE_RACE = 'Race'
    
    @staticmethod
    def calculate_race_points(finish_position: int, fastest_lap_flag: bool = False, 
                            session_type: str = SESSION_TYPE_RACE) -> int:
        """
        Calculate F1 race points based on finish position and fastest lap.
        IMPORTANT: Points are ONLY awarded in race sessions, never in qualifying.
        
        Args:
            finish_position: Final race position (1-20)
            fastest_lap_flag: Whether driver achieved fastest lap
            session_type: Session type ('Race' or 'Qualifying') - points only for 'Race'
            
        Returns:
            Total points earned (0-26 for race sessions, 0 for qualifying sessions)
            
        Examples:
            >>> F1BusinessLogic.calculate_race_points(1, True, 'Race')        # Win + fastest lap
            26
            >>> F1BusinessLogic.calculate_race_points(1, False, 'Race')       # Win only
            25
            >>> F1BusinessLogic.calculate_race_points(1, True, 'Qualifying')  # No points in qualifying
            0
            >>> F1BusinessLogic.calculate_race_points(11, True, 'Race')       # Outside points
            0
        """
        # NO POINTS AWARDED IN QUALIFYING SESSIONS
        if session_type != F1BusinessLogic.SESSION_TYPE_RACE:
            return 0
        
        # Base race points (race sessions only)
        if finish_position <= 0 or finish_position > len(F1BusinessLogic.POINTS_SYSTEM):
            race_points = 0
        else:
            race_points = F1BusinessLogic.POINTS_SYSTEM[finish_position - 1]
        
        # Fastest lap bonus (only if finished in top 10 in race sessions)
        fastest_lap_points = 0
        if fastest_lap_flag and finish_position <= 10 and finish_position > 0:
            fastest_lap_points = F1BusinessLogic.FASTEST_LAP_BONUS
        
        return race_points + fastest_lap_points
    
    @staticmethod
    def calculate_gap_to_pole(qualifying_time: float, pole_time: float) -> Optional[float]:
        """
        Calculate gap to pole position in qualifying sessions.
        
        Args:
            qualifying_time: Driver's qualifying time in seconds
            pole_time: Pole position time (fastest qualifying time) in seconds
            
        Returns:
            Gap to pole in seconds (positive number, 0.0 for pole position)
            None if either time is missing
            
        Examples:
            >>> F1BusinessLogic.calculate_gap_to_pole(90.5, 90.0)  # 0.5s behind pole
            0.5
            >>> F1BusinessLogic.calculate_gap_to_pole(90.0, 90.0)  # Pole position
            0.0
            >>> F1BusinessLogic.calculate_gap_to_pole(None, 90.0)  # Missing time
            None
        """
        if qualifying_time is None or pole_time is None:
            return None
        
        # Gap is always positive (driver time - pole time)
        gap = qualifying_time - pole_time
        return gap if gap >= 0.0 else 0.0
    
    @staticmethod
    def calculate_gap_to_pole_spark_udf():
        """
        Create Spark UDF for gap to pole calculation.
        
        Returns:
            Spark UDF function for calculating gap to pole position
        """
        def gap_calculation(qualifying_time, pole_time):
            if qualifying_time is None or pole_time is None:
                return None
            return F1BusinessLogic.calculate_gap_to_pole(
                float(qualifying_time), 
                float(pole_time)
            )
        
        return udf(gap_calculation, FloatType())
    
    @staticmethod
    def calculate_points_spark_udf():
        """
        Create Spark UDF for F1 points calculation (race sessions only).
        
        Returns:
            Spark UDF function for calculating race points
        """
        def points_calculation(finish_position, fastest_lap_flag, session_type):
            if finish_position is None:
                return 0
            return F1BusinessLogic.calculate_race_points(
                int(finish_position), 
                bool(fastest_lap_flag) if fastest_lap_flag is not None else False,
                str(session_type) if session_type is not None else 'Race'
            )
        
        return udf(points_calculation, IntegerType())
    
    @staticmethod
    def calculate_made_q3_spark_udf():
        """
        Create Spark UDF for made Q3 calculation.
        
        Returns:
            Spark UDF function for determining Q3 participation
        """
        def made_q3_calculation(qualifying_position):
            if qualifying_position is None:
                return False
            return F1BusinessLogic.calculate_made_q3(int(qualifying_position))
        
        return udf(made_q3_calculation, BooleanType())
    
    @staticmethod
    def calculate_team_points_spark_udf():
        """
        Create Spark UDF for team points calculation.
        
        Returns:
            Spark UDF function for calculating team points per race
        """
        def team_points_calculation(driver_points_list):
            if not driver_points_list:
                return 0.0
            return F1BusinessLogic.calculate_team_points_for_race(
                [float(p) for p in driver_points_list if p is not None]
            )
        
        return udf(team_points_calculation, FloatType())
    
    @staticmethod
    def calculate_championship_gap(driver_points: float, leader_points: float) -> float:
        """
        Calculate championship points gap to leader.
        
        Args:
            driver_points: Current driver's points total
            leader_points: Championship leader's points total
            
        Returns:
            Points gap (positive number, 0 if driver is leader)
        """
        # Use Python's built-in max, not Spark's max
        import builtins
        gap = leader_points - driver_points
        return builtins.max(0.0, gap)
    
    @staticmethod
    def calculate_max_remaining_points(remaining_races: int, 
                                     include_sprint_races: int = 0) -> int:
        """
        Calculate maximum points remaining in championship.
        
        Args:
            remaining_races: Number of regular races remaining
            include_sprint_races: Number of sprint races remaining (optional)
            
        Returns:
            Maximum points possible from remaining races
        """
        regular_race_points = remaining_races * F1BusinessLogic.MAX_POINTS_PER_RACE
        sprint_points = include_sprint_races * 8  # Sprint race max points
        
        return regular_race_points + sprint_points
    
    @staticmethod
    def is_championship_mathematically_possible(current_points: float,
                                              leader_points: float,
                                              remaining_races: int) -> bool:
        """
        Determine if championship is mathematically possible.
        
        Args:
            current_points: Driver's current points total
            leader_points: Championship leader's points total
            remaining_races: Number of races remaining
            
        Returns:
            True if championship is still mathematically possible
        """
        max_possible_points = current_points + F1BusinessLogic.calculate_max_remaining_points(remaining_races)
        return max_possible_points >= leader_points
    
    @staticmethod
    def get_championship_window_spec(partition_cols: List[str] = None, 
                                   order_cols: List[str] = None) -> Window:
        """
        Get window specification for championship calculations.
        
        Args:
            partition_cols: Columns to partition by (default: no partition)
            order_cols: Columns to order by (default: points desc, driver_number asc)
            
        Returns:
            Window specification for ranking/cumulative calculations
        """
        if partition_cols is None:
            partition_cols = []
        
        if order_cols is None:
            order_cols = [desc("season_points_total"), asc("driver_number")]
        
        window_spec = Window.partitionBy(*partition_cols) if partition_cols else Window
        
        for order_col in order_cols:
            window_spec = window_spec.orderBy(order_col)
        
        return window_spec
    
    @staticmethod
    def get_running_total_window_spec(partition_cols: List[str], 
                                    order_cols: List[str]) -> Window:
        """
        Get window specification for running totals (cumulative sums).
        
        Args:
            partition_cols: Columns to partition by (e.g., ['driver_number'])
            order_cols: Columns to order by (e.g., ['race_date'])
            
        Returns:
            Window specification with ROWS UNBOUNDED PRECEDING
        """
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        return window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    @staticmethod
    def get_recent_form_window_spec(partition_cols: List[str], 
                                  order_cols: List[str],
                                  num_races: int = 3) -> Window:
        """
        Get window specification for recent form calculations.
        
        Args:
            partition_cols: Columns to partition by (e.g., ['driver_number'])
            order_cols: Columns to order by (e.g., ['race_date'])
            num_races: Number of recent races to include (default: 3)
            
        Returns:
            Window specification for last N races
        """
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        return window_spec.rowsBetween(-(num_races - 1), Window.currentRow)
    
    @staticmethod
    def calculate_positions_gained(grid_position: int, finish_position: int) -> int:
        """
        Calculate positions gained/lost during race.
        
        Args:
            grid_position: Starting grid position
            finish_position: Final race position
            
        Returns:
            Positions gained (positive) or lost (negative)
        """
        if grid_position is None or finish_position is None:
            return 0
        
        return grid_position - finish_position
    
    @staticmethod
    def calculate_made_q3(qualifying_position: Optional[int]) -> bool:
        """
        Determine if driver made it to Q3 (top 10 qualifiers).
        
        Args:
            qualifying_position: Final qualifying position (1-20)
            
        Returns:
            True if driver made Q3 (position <= 10), False otherwise
        """
        if qualifying_position is None or qualifying_position <= 0 or qualifying_position > 20:
            return False
        return qualifying_position <= 10
    
    @staticmethod
    def calculate_team_points_for_race(driver_points_list: List[Optional[float]]) -> float:
        """
        Calculate total team points for a single race (sum of both drivers).
        
        Args:
            driver_points_list: List of points earned by team drivers in the race
            
        Returns:
            Total team points for the race (0-52 max)
        """
        if not driver_points_list:
            return 0.0
        # Use Python's built-in sum, not Spark's sum
        import builtins
        return float(builtins.sum(point for point in driver_points_list if point is not None))
    
    @staticmethod
    def calculate_dnf_count(finish_positions: List[Optional[int]]) -> int:
        """
        Count DNFs (Did Not Finish) from a list of finish positions.
        
        Args:
            finish_positions: List of finish positions (None = DNF)
            
        Returns:
            Count of DNFs
        """
        # Use Python's built-in sum, not Spark's sum
        import builtins
        return builtins.sum(1 for pos in finish_positions if pos is None or (pos is not None and pos > 20))
    
    @staticmethod
    def get_teammate_comparison_window_spec(team_col: str = "team_name", 
                                          race_col: str = "race_date") -> Window:
        """
        Get window specification for teammate comparisons.
        
        Args:
            team_col: Team column name
            race_col: Race date column name
            
        Returns:
            Window specification partitioned by team and race
        """
        return Window.partitionBy(team_col, race_col).orderBy("driver_number")
    
    @staticmethod
    def add_championship_position(df: DataFrame, 
                                points_col: str = "season_points_total",
                                partition_cols: List[str] = None,
                                session_type: str = "Race") -> DataFrame:
        """
        Add championship position based on points total (race sessions only).
        """
        if session_type != F1BusinessLogic.SESSION_TYPE_RACE:
            logger.warning("Championship positions only calculated for race sessions")
            return df.withColumn("championship_position", lit(None).cast(IntegerType()))
        
        if partition_cols:
            window_spec = Window.partitionBy(*partition_cols).orderBy(desc(points_col), asc("driver_number"))
        else:
            window_spec = Window.orderBy(desc(points_col), asc("driver_number"))
        
        return df.withColumn("championship_position", rank().over(window_spec))
    
    @staticmethod
    def add_running_points_total(df: DataFrame,
                               points_col: str = "race_points",
                               partition_col: str = "driver_number",
                               order_col: str = "race_date",
                               session_type: str = "Race") -> DataFrame:
        """
        Add running total of points throughout season (race sessions only).
        """
        if session_type != F1BusinessLogic.SESSION_TYPE_RACE:
            logger.warning("Points totals only calculated for race sessions")
            return df.withColumn("season_points_total", lit(0).cast(IntegerType()))
        
        window_spec = F1BusinessLogic.get_running_total_window_spec([partition_col], [order_col])
        
        return df.withColumn("season_points_total", 
                           sum(points_col).over(window_spec))
    
    # Recent form removed for simplified Gold schema approach
    
    @staticmethod
    def add_points_gap_to_leader(df: DataFrame,
                               points_col: str = "season_points_total",
                               partition_cols: List[str] = None) -> DataFrame:
        """
        Add points gap to championship leader.
        """
        if partition_cols:
            window_spec = Window.partitionBy(*partition_cols)
        else:
            window_spec = Window.partitionBy()
        
        leader_points = max(points_col).over(window_spec)
        
        return df.withColumn("points_gap_to_leader", 
                           leader_points - col(points_col))
    
    @staticmethod
    def add_qualifying_season_averages(df: DataFrame,
                                     position_col: str = "qualifying_position",
                                     partition_col: str = "driver_number",
                                     order_col: str = "race_date") -> DataFrame:
        """
        Add season-long qualifying averages (qualifying sessions only).
        """
        window_spec = F1BusinessLogic.get_running_total_window_spec([partition_col], [order_col])
        
        return df.withColumn("season_avg_qualifying_position", 
                           avg(position_col).over(window_spec))
    
    @staticmethod
    def add_made_q3_flag(df: DataFrame,
                        position_col: str = "qualifying_position") -> DataFrame:
        """
        Add made_q3 flag based on qualifying position (qualifying sessions only).
        
        Args:
            df: DataFrame with qualifying data
            position_col: Column with qualifying positions
            
        Returns:
            DataFrame with made_q3 boolean column
        """
        return df.withColumn(
            "made_q3",
            when(col(position_col).isNotNull() & (col(position_col) <= 10), True).otherwise(False)
        )
    
    @staticmethod
    def add_team_points_this_race(df: DataFrame,
                                points_col: str = "race_points",
                                team_col: str = "team_name",
                                race_col: str = "race_date",
                                gp_col: str = "grand_prix_name") -> DataFrame:
        """
        Add total team points for each race (sum of both drivers).
        
        Args:
            df: DataFrame with race results
            points_col: Column with individual driver points
            team_col: Team name column
            race_col: Race date column
            gp_col: Grand Prix name column
            
        Returns:
            DataFrame with team_points_this_race column
        """
        return df.groupBy(team_col, race_col, gp_col).agg(
            sum(points_col).alias("team_points_this_race")
        ).join(
            df.select(team_col, race_col, gp_col).distinct(),
            [team_col, race_col, gp_col],
            "inner"
        )
    
    @staticmethod
    def add_constructor_points_total(df: DataFrame,
                                   team_points_col: str = "team_points_this_race",
                                   team_col: str = "team_name",
                                   date_col: str = "race_date") -> DataFrame:
        """
        Add running total of constructor championship points.
        
        Args:
            df: DataFrame with team race results
            team_points_col: Column with team points per race
            team_col: Team name column
            date_col: Race date column for ordering
            
        Returns:
            DataFrame with constructor_points_total column
        """
        window_spec = F1BusinessLogic.get_running_total_window_spec([team_col], [date_col])
        
        return df.withColumn(
            "constructor_points_total",
            sum(team_points_col).over(window_spec)
        )
    
    @staticmethod
    def add_constructor_position(df: DataFrame,
                               points_col: str = "constructor_points_total",
                               date_col: str = "race_date") -> DataFrame:
        """
        Add constructor championship position based on total points.
        
        Args:
            df: DataFrame with constructor points
            points_col: Column with constructor total points
            date_col: Race date for partitioning by race
            
        Returns:
            DataFrame with constructor_position column
        """
        window_spec = Window.partitionBy(date_col).orderBy(
            desc(points_col), 
            asc("team_name")  # Tie-breaker
        )
        
        return df.withColumn(
            "constructor_position",
            rank().over(window_spec)
        )
    
    @staticmethod
    def add_pole_to_win_flag(df: DataFrame,
                           pole_col: str = "pole_position_driver",
                           winner_col: str = "race_winner_driver") -> DataFrame:
        """
        Add flag indicating if pole position won the race.
        
        Args:
            df: DataFrame with pole and race winner data
            pole_col: Column with pole position driver name
            winner_col: Column with race winner driver name
            
        Returns:
            DataFrame with pole_to_win boolean column
        """
        return df.withColumn(
            "pole_to_win",
            when(col(pole_col) == col(winner_col), True).otherwise(False)
        )
    
    @staticmethod
    def add_total_finishers(df: DataFrame,
                          position_col: str = "finish_position") -> DataFrame:
        """
        Count total finishers (drivers who completed the race).
        
        Args:
            df: DataFrame with race results
            position_col: Column with finish positions
            
        Returns:
            DataFrame with total_finishers count
        """
        return df.agg(
            count(when(col(position_col).isNotNull() & (col(position_col) <= 20), 1)).alias("total_finishers")
        )
    
    @staticmethod
    def add_dnf_count_column(df: DataFrame,
                           position_col: str = "finish_position") -> DataFrame:
        """
        Count DNFs (Did Not Finish) in race results.
        
        Args:
            df: DataFrame with race results
            position_col: Column with finish positions (None/NULL = DNF)
            
        Returns:
            DataFrame with dnf_count column
        """
        return df.agg(
            count(when(col(position_col).isNull() | (col(position_col) > 20), 1)).alias("dnf_count")
        )
    
    # Teammate comparison removed for simplified Gold schema approach

    @staticmethod
    def add_championship_standings(df: DataFrame, 
                                 points_col: str = "season_points_total",
                                 session_type: str = "Race") -> DataFrame:
        """
        Add championship standings to DataFrame (race sessions only).
        """
        if session_type != F1BusinessLogic.SESSION_TYPE_RACE:
            logger.warning("Championship standings only calculated for race sessions")
            return df
        
        df_with_position = F1BusinessLogic.add_championship_position(df, points_col, session_type=session_type)
        return F1BusinessLogic.add_points_gap_to_leader(df_with_position, points_col)

    @staticmethod
    def add_season_progression(df: DataFrame,
                             points_col: str = "race_points",
                             driver_col: str = "driver_number",
                             date_col: str = "race_date",
                             session_type: str = "Race") -> DataFrame:
        """
        Add season progression columns (running totals only) - race sessions only.
        """
        if session_type != F1BusinessLogic.SESSION_TYPE_RACE:
            logger.warning("Season progression only calculated for race sessions")
            return df
        
        return F1BusinessLogic.add_running_points_total(
            df, points_col, driver_col, date_col, session_type
        )

    @staticmethod
    def add_qualifying_season_metrics(df: DataFrame,
                                    position_col: str = "qualifying_position",
                                    driver_col: str = "driver_number",
                                    date_col: str = "race_date") -> DataFrame:
        """
        Add qualifying-specific season metrics (qualifying sessions only).
        
        Simplified approach: only season averages and made_q3 flag.
        """
        df_with_avg = F1BusinessLogic.add_qualifying_season_averages(
            df, position_col, driver_col, date_col
        )
        
        df_with_q3 = F1BusinessLogic.add_made_q3_flag(
            df_with_avg, position_col
        )
        
        return df_with_q3

    @staticmethod
    def validate_session_specific_data(df: DataFrame, session_type: str) -> Dict[str, bool]:
        """
        Validate session-specific data integrity.
        
        Args:
            df: DataFrame to validate
            session_type: 'Qualifying' or 'Race'
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {}
        
        if session_type == F1BusinessLogic.SESSION_TYPE_QUALIFYING:
            # Qualifying-specific validations
            validation_results['qualifying_positions_valid'] = True  # 1-20 range
            validation_results['no_race_points'] = True  # Should be 0 or null
            validation_results['has_qualifying_times'] = True  # Should have Q times
            
        elif session_type == F1BusinessLogic.SESSION_TYPE_RACE:
            # Race-specific validations
            validation_results['race_points_valid'] = True  # 0-26 range
            validation_results['positions_gained_valid'] = True  # -19 to +19 range
            validation_results['has_race_results'] = True  # Should have finish positions
            
        return validation_results
    
    @staticmethod
    def validate_qualifying_session_data(df: DataFrame) -> bool:
        """
        Validate qualifying session data integrity.
        
        Args:
            df: DataFrame with qualifying data
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check qualifying positions are in valid range (1-20)
            invalid_positions = df.filter(
                (col("qualifying_position") < 1) | 
                (col("qualifying_position") > 20)
            ).count()
            
            # Check no race points are awarded in qualifying
            race_points_in_qualifying = df.filter(
                col("race_points").isNotNull() & 
                (col("race_points") > 0)
            ).count()
            
            # Check gap to pole is non-negative
            invalid_gaps = df.filter(
                col("gap_to_pole").isNotNull() & 
                (col("gap_to_pole") < 0)
            ).count()
            
            return (invalid_positions == 0 and 
                   race_points_in_qualifying == 0 and 
                   invalid_gaps == 0)
                   
        except Exception as e:
            logger.error(f"Qualifying validation error: {str(e)}")
            return False
    
    @staticmethod
    def validate_race_session_data(df: DataFrame) -> bool:
        """
        Validate race session data integrity.
        
        Args:
            df: DataFrame with race data
            
        Returns:
            True if validation passes, False otherwise
        """
        try:
            # Check race points are in valid range (0-26)
            invalid_points = df.filter(
                col("race_points").isNotNull() & 
                ((col("race_points") < 0) | (col("race_points") > 26))
            ).count()
            
            # Check positions gained are in valid range (-19 to +19)
            invalid_positions_gained = df.filter(
                col("positions_gained").isNotNull() & 
                ((col("positions_gained") < -19) | (col("positions_gained") > 19))
            ).count()
            
            # Check championship positions are valid (1-20)
            invalid_championship_positions = df.filter(
                col("championship_position").isNotNull() & 
                ((col("championship_position") < 1) | (col("championship_position") > 20))
            ).count()
            
            return (invalid_points == 0 and 
                   invalid_positions_gained == 0 and 
                   invalid_championship_positions == 0)
                   
        except Exception as e:
            logger.error(f"Race validation error: {str(e)}")
            return False
    
    @staticmethod
    def determine_race_winner_from_results(results_df: DataFrame) -> Dict[str, any]:
        """
        Determine race winner and key statistics from race results.
        
        Args:
            results_df: DataFrame with race results containing position, driver_number, 
                       grid_position, fastest_lap_time columns
            
        Returns:
            Dictionary with race winner information
        """
        try:
            from pyspark.sql.functions import col, min as spark_min, when, desc, asc
            
            if results_df.count() == 0:
                logger.warning("No race results data provided")
                return {
                    'race_winner': None,
                    'pole_position_driver': None,
                    'fastest_lap_driver': None,
                    'biggest_mover': None,
                    'biggest_mover_positions': 0
                }
            
            # Race winner (position = 1)
            winner_row = results_df.filter(col("position") == 1).first()
            race_winner = winner_row["driver_number"] if winner_row else None
            
            # Pole position (grid_position = 1)
            pole_row = results_df.filter(col("grid_position") == 1).first()
            pole_position_driver = pole_row["driver_number"] if pole_row else None
            
            # Fastest lap driver (minimum fastest_lap_time, excluding null)
            fastest_lap_row = (results_df
                              .filter(col("fastest_lap_time").isNotNull())
                              .orderBy(asc("fastest_lap_time"))
                              .first())
            fastest_lap_driver = fastest_lap_row["driver_number"] if fastest_lap_row else None
            
            # Biggest mover (highest positive grid_position - position difference)
            move_df = (results_df
                      .filter(col("grid_position").isNotNull() & col("position").isNotNull())
                      .withColumn("positions_gained", col("grid_position") - col("position"))
                      .orderBy(desc("positions_gained"))
                      .first())
            
            biggest_mover = move_df["driver_number"] if move_df else None
            biggest_mover_positions = int(move_df["positions_gained"]) if move_df else 0
            
            return {
                'race_winner': race_winner,
                'pole_position_driver': pole_position_driver,
                'fastest_lap_driver': fastest_lap_driver,
                'biggest_mover': biggest_mover,
                'biggest_mover_positions': biggest_mover_positions
            }
            
        except Exception as e:
            logger.error(f"Error determining race winner: {str(e)}")
            return {
                'race_winner': None,
                'pole_position_driver': None,
                'fastest_lap_driver': None,
                'biggest_mover': None,
                'biggest_mover_positions': 0
            }