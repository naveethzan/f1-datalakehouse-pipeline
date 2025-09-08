"""
Unified table schema definitions for F1 Silver and Gold layers.

This module is the single source of truth for all table schemas,
containing definitions for both Silver (normalized) and Gold (analytics) tables.
"""

from typing import Dict, List, Tuple
from dataclasses import dataclass


# A consistent dataclass for defining columns across all layers
@dataclass
class ColumnDefinition:
    """Represents a table column with metadata."""
    name: str
    data_type: str
    nullable: bool = True
    comment: str = ""


# A more concise, data-driven approach to defining schemas.
SILVER_SCHEMAS = {
    'sessions_silver': [
        ColumnDefinition("session_key", "BIGINT", False, "Unique identifier for F1 session"),
        ColumnDefinition("session_name", "STRING", False, "Official session name (e.g., Race, Qualifying)"),
        ColumnDefinition("session_type", "STRING", False, "Standardized session type"),
        ColumnDefinition("meeting_key", "BIGINT", False, "Unique identifier for race weekend"),
        ColumnDefinition("meeting_name", "STRING", False, "Official meeting name"),
        ColumnDefinition("location", "STRING", False, "Circuit location"),
        ColumnDefinition("country_name", "STRING", False, "Country where session takes place"),
        ColumnDefinition("date_start", "TIMESTAMP", False, "Session start time (UTC)"),
        ColumnDefinition("date_end", "TIMESTAMP", True, "Session end time (UTC)"),
        ColumnDefinition("year", "INT", False, "F1 season year"),
        ColumnDefinition("grand_prix_name", "STRING", False, "Standardized Grand Prix name"),
        ColumnDefinition("session_duration_minutes", "INT", True, "Planned session duration"),
        ColumnDefinition("is_sprint_weekend", "BOOLEAN", False, "Whether this is a sprint weekend"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ],
    'drivers_silver': [
        ColumnDefinition("driver_number", "INT", False, "Official F1 driver number"),
        ColumnDefinition("broadcast_name", "STRING", False, "Name used in broadcasts"),
        ColumnDefinition("full_name", "STRING", False, "Driver full name"),
        ColumnDefinition("team_name", "STRING", False, "Standardized team name"),
        ColumnDefinition("nationality", "STRING", True, "Driver nationality"),
        ColumnDefinition("team_colour", "STRING", True, "Team color hex code"),
        ColumnDefinition("name_acronym", "STRING", True, "Three-letter driver acronym"),
        ColumnDefinition("total_races", "INT", True, "Total races participated in season"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ],
    'qualifying_results_silver': [
        ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
        ColumnDefinition("driver_number", "INT", False, "Reference to drivers table"),
        ColumnDefinition("position", "INT", False, "Final qualifying position"),
        ColumnDefinition("q1_time_millis", "BIGINT", True, "Q1 best time in milliseconds"),
        ColumnDefinition("q2_time_millis", "BIGINT", True, "Q2 best time in milliseconds"),
        ColumnDefinition("q3_time_millis", "BIGINT", True, "Q3 best time in milliseconds"),
        ColumnDefinition("fastest_qualifying_time_millis", "BIGINT", True, "Overall fastest time in milliseconds"),
        ColumnDefinition("gap_to_pole_millis", "BIGINT", True, "Gap to pole position in milliseconds"),
        ColumnDefinition("qualifying_status", "STRING", True, "Qualifying completion status"),
        ColumnDefinition("year", "INT", False, "F1 season year (partition key)"),
        ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name (partition key)"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ],
    'race_results_silver': [
        ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
        ColumnDefinition("driver_number", "INT", False, "Reference to drivers table"),
        ColumnDefinition("position", "INT", False, "Final race position"),
        ColumnDefinition("grid_position", "INT", True, "Starting grid position"),
        ColumnDefinition("points", "INT", False, "Championship points earned"),
        ColumnDefinition("validated_points", "INT", False, "Points after validation rules"),
        ColumnDefinition("positions_gained", "INT", True, "Positions gained/lost from grid"),
        ColumnDefinition("time", "STRING", True, "Race completion time or status"),
        ColumnDefinition("gap_to_winner_millis", "BIGINT", True, "Gap to race winner in milliseconds"),
        ColumnDefinition("status", "STRING", False, "Race completion status"),
        ColumnDefinition("standardized_status", "STRING", False, "Standardized status category"),
        ColumnDefinition("year", "INT", False, "F1 season year (partition key)"),
        ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name (partition key)"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ],
    'laps_silver': [
        ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
        ColumnDefinition("driver_number", "INT", False, "Reference to drivers table"),
        ColumnDefinition("lap_number", "INT", False, "Lap number in session"),
        ColumnDefinition("lap_time", "STRING", True, "Lap time in MM:SS.mmm format"),
        ColumnDefinition("lap_time_millis", "BIGINT", True, "Lap time in milliseconds"),
        ColumnDefinition("position_at_lap", "INT", True, "Driver position at end of lap"),
        ColumnDefinition("gap_to_leader_millis", "BIGINT", True, "Gap to race leader in milliseconds"),
        ColumnDefinition("interval_to_ahead_millis", "BIGINT", True, "Gap to car ahead in milliseconds"),
        ColumnDefinition("is_personal_best", "BOOLEAN", False, "Whether this is driver personal best"),
        ColumnDefinition("is_fastest_lap", "BOOLEAN", False, "Whether this is overall fastest lap"),
        ColumnDefinition("tire_compound", "STRING", True, "Tire compound used (Soft/Medium/Hard)"),
        ColumnDefinition("track_status", "STRING", True, "Track status during lap"),
        ColumnDefinition("year", "INT", False, "F1 season year (partition key)"),
        ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name (partition key)"),
        ColumnDefinition("session_type", "STRING", False, "Session type for filtering"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ],
    'pitstops_silver': [
        ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
        ColumnDefinition("driver_number", "INT", False, "Reference to drivers table"),
        ColumnDefinition("lap_number", "INT", False, "Lap number when pit stop occurred"),
        ColumnDefinition("pit_duration", "DECIMAL(6,3)", False, "Pit stop duration in seconds"),
        ColumnDefinition("positions_lost_gained", "INT", True, "Net positions change due to pit stop"),
        ColumnDefinition("undercut_attempt", "BOOLEAN", False, "Whether this was an undercut attempt"),
        ColumnDefinition("safety_car_stop", "BOOLEAN", False, "Whether pit stop occurred under safety car"),
        ColumnDefinition("tire_compound_old", "STRING", True, "Tire compound before pit stop"),
        ColumnDefinition("tire_compound_new", "STRING", True, "Tire compound after pit stop"),
        ColumnDefinition("year", "INT", False, "F1 season year (partition key)"),
        ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name (partition key)"),
        ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
        ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp"),
    ]
}

class SilverTableSchemas:
    """
    Centralized, data-driven schema definitions for all F1 Silver tables.
    Provides schema definitions with proper data types and business rule documentation.
    """

    @staticmethod
    def get_table_schema(table_name: str) -> List[ColumnDefinition]:
        
        if table_name not in SILVER_SCHEMAS:
            raise ValueError(f"Unknown table: {table_name}. Available tables: {list(SILVER_SCHEMAS.keys())}")
        return SILVER_SCHEMAS[table_name]

    @staticmethod
    def get_ddl_columns(table_name: str) -> str:
       
        schema = SilverTableSchemas.get_table_schema(table_name)
        
        ddl_lines = []
        for col in schema:
            nullable_str = "" if col.nullable else " NOT NULL"
            comment_str = f" COMMENT '{col.comment.replace(chr(39), chr(39)+chr(39))}'" if col.comment else ""
            ddl_lines.append(f"    `{col.name}` {col.data_type}{nullable_str}{comment_str}")
        
        return ",\n".join(ddl_lines)

    @staticmethod
    def get_all_table_names() -> List[str]:
        """Get a list of all Silver table names."""
        return list(SILVER_SCHEMAS.keys())

class GoldTableSchemas:
    
    @staticmethod
    def get_driver_performance_summary_qualifying_schema() -> List[ColumnDefinition]:
        """
        Get schema for driver_performance_summary_qualifying Gold table.
        """
        return [
            ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
            ColumnDefinition("grand_prix_name", "STRING", False, "Standardized Grand Prix name (partition key)"),
            ColumnDefinition("race_date", "DATE", False, "Race date for chronological analysis"),
            ColumnDefinition("round_number", "INT", False, "Round number in season"),
            ColumnDefinition("driver_number", "INT", False, "Official F1 driver number"),
            ColumnDefinition("driver_name", "STRING", False, "Driver broadcast name"),
            ColumnDefinition("team_name", "STRING", False, "Team name for filtering"),
            ColumnDefinition("qualifying_position", "INT", True, "Final qualifying position"),
            ColumnDefinition("qualifying_time", "DECIMAL(8,3)", True, "Qualifying time in seconds"),
            ColumnDefinition("gap_to_pole", "DECIMAL(6,3)", True, "Gap to pole position in seconds"),
            ColumnDefinition("made_q3", "BOOLEAN", False, "Whether driver made it to Q3"),
            ColumnDefinition("teammate_name", "STRING", True, "Teammate name for comparison"),
            ColumnDefinition("beat_teammate", "BOOLEAN", True, "Outqualified teammate"),
            ColumnDefinition("gap_to_teammate", "DECIMAL(6,3)", True, "Gap to teammate in seconds (+/-)"),
            ColumnDefinition("season_avg_qualifying_position", "DECIMAL(4,1)", True, "Average qualifying position this season"),
            ColumnDefinition("poles_season_total", "INT", False, "Total pole positions this season"),
            ColumnDefinition("q3_appearances_season_total", "INT", False, "Total Q3 appearances this season"),
            ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
            ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp")
        ]
    
    @staticmethod
    def get_driver_performance_summary_race_schema() -> List[ColumnDefinition]:
        """
        Get schema for driver_performance_summary_race Gold table.
        """
        return [
            ColumnDefinition("session_key", "BIGINT", False, "Reference to sessions table"),
            ColumnDefinition("grand_prix_name", "STRING", False, "Standardized Grand Prix name (partition key)"),
            ColumnDefinition("race_date", "DATE", False, "Race date for chronological analysis"),
            ColumnDefinition("round_number", "INT", False, "Round number in season"),
            ColumnDefinition("driver_number", "INT", False, "Official F1 driver number"),
            ColumnDefinition("driver_name", "STRING", False, "Driver broadcast name"),
            ColumnDefinition("team_name", "STRING", False, "Team name for filtering"),
            ColumnDefinition("grid_position", "INT", True, "Starting grid position"),
            ColumnDefinition("finish_position", "INT", True, "Final race position"),
            ColumnDefinition("race_points", "DECIMAL(4,1)", False, "Championship points earned"),
            ColumnDefinition("race_status", "STRING", True, "Race status (Finished, DNF, etc.)"),
            ColumnDefinition("positions_gained", "INT", True, "Grid position minus finish position"),
            ColumnDefinition("laps_completed", "INT", True, "Number of laps completed"),
            ColumnDefinition("fastest_lap_achieved", "BOOLEAN", False, "Whether driver achieved fastest lap"),
            ColumnDefinition("total_pit_stops", "INT", True, "Number of pit stops made"),
            ColumnDefinition("avg_pit_stop_time", "DECIMAL(6,3)", True, "Average pit stop time in seconds"),
            ColumnDefinition("teammate_name", "STRING", True, "Teammate name for comparison"),
            ColumnDefinition("beat_teammate", "BOOLEAN", True, "Outfinished teammate"),
            ColumnDefinition("season_points_total", "DECIMAL(6,1)", False, "Running total points using window function"),
            ColumnDefinition("wins_season_total", "INT", False, "Total wins this season"),
            ColumnDefinition("podiums_season_total", "INT", False, "Total podiums this season"),
            ColumnDefinition("dnf_season_total", "INT", False, "Total DNFs this season"),
            ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
            ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp")
        ]
    
    @staticmethod
    def get_championship_tracker_schema() -> List[ColumnDefinition]:
        """
        Get schema for championship_tracker Gold table.
        """
        return [
            ColumnDefinition("race_date", "DATE", False, "Race date for chronological ordering"),
            ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name for context"),
            ColumnDefinition("round_number", "INT", False, "Round number in season (partition key)"),
            ColumnDefinition("driver_number", "INT", False, "Official F1 driver number"),
            ColumnDefinition("driver_name", "STRING", False, "Driver broadcast name"),
            ColumnDefinition("team_name", "STRING", False, "Team name for filtering"),
            ColumnDefinition("finish_position", "INT", True, "Final race position"),
            ColumnDefinition("race_points", "DECIMAL(4,1)", False, "Points earned in this race"),
            ColumnDefinition("season_points_total", "DECIMAL(6,1)", False, "Cumulative points total"),
            ColumnDefinition("championship_position", "INT", False, "Current championship position"),
            ColumnDefinition("points_gap_to_leader", "DECIMAL(6,1)", True, "Points behind leader"),
            ColumnDefinition("last_3_races_points", "DECIMAL(5,1)", True, "Points from last 3 races"),
            ColumnDefinition("wins_total", "INT", False, "Total wins this season"),
            ColumnDefinition("podiums_total", "INT", False, "Total podiums this season"),
            ColumnDefinition("dnf_total", "INT", False, "Total DNFs this season"),
            ColumnDefinition("can_win_championship", "BOOLEAN", False, "Mathematically possible to win"),
            ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
            ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp")
        ]
    
    @staticmethod
    def get_team_strategy_analysis_schema() -> List[ColumnDefinition]:
        """
        Get schema for team_strategy_analysis Gold table.
        """
        return [
            ColumnDefinition("race_date", "DATE", False, "Race date for chronological analysis"),
            ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name for context"),
            ColumnDefinition("round_number", "INT", False, "Round number in season"),
            ColumnDefinition("team_name", "STRING", False, "Team name (partition key)"),
            ColumnDefinition("driver_1_name", "STRING", False, "First driver name"),
            ColumnDefinition("driver_1_finish_position", "INT", True, "First driver finish position"),
            ColumnDefinition("driver_1_points", "DECIMAL(4,1)", False, "First driver points"),
            ColumnDefinition("driver_2_name", "STRING", False, "Second driver name"),
            ColumnDefinition("driver_2_finish_position", "INT", True, "Second driver finish position"),
            ColumnDefinition("driver_2_points", "DECIMAL(4,1)", False, "Second driver points"),
            ColumnDefinition("team_points_this_race", "DECIMAL(5,1)", False, "Total team points for race"),
            ColumnDefinition("best_team_finish", "INT", True, "Best finish position of both drivers"),
            ColumnDefinition("both_drivers_finished", "BOOLEAN", False, "Whether both drivers finished"),
            ColumnDefinition("both_drivers_scored_points", "BOOLEAN", False, "Whether both drivers scored points"),
            ColumnDefinition("total_team_pit_stops", "INT", False, "Combined pit stops for both drivers"),
            ColumnDefinition("avg_team_pit_stop_time", "DECIMAL(6,3)", True, "Average pit stop time in seconds"),
            ColumnDefinition("different_strategies_used", "BOOLEAN", False, "Whether drivers used different strategies"),
            ColumnDefinition("constructor_points_total", "DECIMAL(6,1)", False, "Total constructor points"),
            ColumnDefinition("constructor_position", "INT", False, "Current constructor championship position"),
            ColumnDefinition("points_gap_to_leader_constructor", "DECIMAL(6,1)", True, "Points behind constructor leader"),
            ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
            ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp")
        ]
    
    @staticmethod
    def get_race_weekend_insights_schema() -> List[ColumnDefinition]:
        """
        Get schema for race_weekend_insights Gold table.
        """
        return [
            ColumnDefinition("grand_prix_name", "STRING", False, "Grand Prix name (partition key)"),
            ColumnDefinition("circuit_name", "STRING", False, "Circuit name"),
            ColumnDefinition("race_date", "DATE", False, "Race date for chronological analysis"),
            ColumnDefinition("round_number", "INT", False, "Round number in season"),
            ColumnDefinition("pole_position_driver", "STRING", True, "Driver who achieved pole position"),
            ColumnDefinition("pole_position_time", "DECIMAL(8,3)", True, "Pole position time in seconds"),
            ColumnDefinition("race_winner_driver", "STRING", False, "Race winner name"),
            ColumnDefinition("race_winner_team", "STRING", False, "Race winner team"),
            ColumnDefinition("fastest_lap_driver", "STRING", True, "Driver with fastest lap"),
            ColumnDefinition("pole_to_win", "BOOLEAN", False, "Whether pole position won the race"),
            ColumnDefinition("biggest_position_gainer", "STRING", True, "Driver with most positions gained"),
            ColumnDefinition("biggest_position_gain", "INT", True, "Most positions gained by any driver"),
            ColumnDefinition("total_finishers", "INT", False, "Number of drivers who finished"),
            ColumnDefinition("dnf_count", "INT", False, "Number of DNFs"),
            ColumnDefinition("total_overtakes", "INT", True, "Total overtakes during race"),
            ColumnDefinition("safety_car_periods", "INT", True, "Number of safety car periods"),
            ColumnDefinition("championship_leader_before", "STRING", True, "Championship leader before this race"),
            ColumnDefinition("championship_leader_after", "STRING", True, "Championship leader after this race"),
            ColumnDefinition("championship_leadership_change", "BOOLEAN", False, "Whether championship leadership changed"),
            ColumnDefinition("avg_pit_stops", "DECIMAL(3,1)", True, "Average pit stops per driver"),
            ColumnDefinition("most_popular_tire_strategy", "STRING", True, "Most common tire strategy"),
            ColumnDefinition("entertainment_rating", "DECIMAL(3,1)", True, "Simple 1-10 scale entertainment rating"),
            ColumnDefinition("created_timestamp", "TIMESTAMP", False, "Record creation timestamp"),
            ColumnDefinition("updated_timestamp", "TIMESTAMP", False, "Record last update timestamp")
        ]
    
    @staticmethod
    def get_table_schema(table_name: str) -> List[ColumnDefinition]:
        """
        Get schema for any Gold table by name.
        """
        schema_mapping = {
            'driver_performance_summary_qualifying': GoldTableSchemas.get_driver_performance_summary_qualifying_schema,
            'driver_performance_summary_race': GoldTableSchemas.get_driver_performance_summary_race_schema,
            'championship_tracker': GoldTableSchemas.get_championship_tracker_schema,
            'team_strategy_analysis': GoldTableSchemas.get_team_strategy_analysis_schema,
            'race_weekend_insights': GoldTableSchemas.get_race_weekend_insights_schema
        }
        
        if table_name not in schema_mapping:
            raise ValueError(f"Unknown Gold table: {table_name}. Available tables: {list(schema_mapping.keys())}")
        
        return schema_mapping[table_name]()
    
    @staticmethod
    def get_ddl_columns(table_name: str) -> str:
        """
        Get DDL column definitions for CREATE TABLE statement.
        """
        schema = GoldTableSchemas.get_table_schema(table_name)
        
        ddl_lines = []
        for col in schema:
            nullable = "" if col.nullable else " NOT NULL"
            comment = f" COMMENT '{col.comment.replace(chr(39), chr(39)+chr(39))}'" if col.comment else ""
            ddl_lines.append(f"                {col.name} {col.data_type}{nullable}{comment}")
        
        return ",\n".join(ddl_lines)
    
    @staticmethod
    def get_all_table_names() -> List[str]:
        """
        Get list of all Gold table names.
        """
        return [
            'driver_performance_summary_qualifying',
            'driver_performance_summary_race',
            'championship_tracker',
            'team_strategy_analysis',
            'race_weekend_insights'
        ]
    
    @staticmethod
    def get_partitioning_strategy(table_name: str) -> str:
        """
        Get partitioning strategy for Gold table.
        """
        partitioning_strategies = {
            'driver_performance_summary_qualifying': 'grand_prix_name',
            'driver_performance_summary_race': 'grand_prix_name',
            'championship_tracker': 'round_number', 
            'team_strategy_analysis': 'team_name',
            'race_weekend_insights': 'grand_prix_name'
        }
        
        return partitioning_strategies.get(table_name, 'grand_prix_name')
