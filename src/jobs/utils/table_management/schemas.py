"""
Unified schema definitions for F1 Silver and Gold layers (simplified).

This module defines all table schemas using simple dictionary-based structures
with metadata. It also provides helper functions to generate Iceberg-compatible
DDL column lists and access schema metadata.

"""

from typing import Dict, List

# -----------------------------
# Silver Layer Schemas
# -----------------------------

SILVER_SCHEMAS: Dict[str, Dict] = {
    'sessions_silver': {
        'columns': {
            'session_key':   {'type': 'BIGINT',   'nullable': False, 'comment': 'Unique session identifier'},
            'session_type':  {'type': 'STRING',   'nullable': False, 'comment': 'Session type (qualifying/race)'},
            'session_name':  {'type': 'STRING',   'nullable': False, 'comment': 'Descriptive session name'},
            'meeting_key':   {'type': 'BIGINT',   'nullable': False, 'comment': 'Race weekend identifier'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Normalized Grand Prix name'},
            'date_start':    {'type': 'TIMESTAMP','nullable': False, 'comment': 'Session start time (UTC)'},
            'date_end':      {'type': 'TIMESTAMP','nullable': True,  'comment': 'Session end time (UTC)'},
            'year':          {'type': 'INT',      'nullable': False, 'comment': 'F1 season year'},
            'session_duration_minutes': {'type': 'INT', 'nullable': True, 'comment': 'Planned/actual duration (mins)'},
            'is_sprint_weekend': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Sprint weekend flag'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': ['year']
    },

    'drivers_silver': {
        'columns': {
            'driver_number': {'type': 'INT',     'nullable': False, 'comment': 'Official F1 driver number'},
            'broadcast_name': {'type': 'STRING', 'nullable': False, 'comment': 'Broadcast name'},
            'full_name':     {'type': 'STRING',  'nullable': False, 'comment': 'Driver full name'},
            'team_name':     {'type': 'STRING',  'nullable': False, 'comment': 'Standardized team name'},
            'country_code':  {'type': 'STRING',  'nullable': True,  'comment': 'Driver country code'},
            'team_colour':   {'type': 'STRING',  'nullable': True,  'comment': 'Team color hex code'},
            'name_acronym':  {'type': 'STRING',  'nullable': True,  'comment': 'Three-letter acronym'},
            'total_races':   {'type': 'INT',     'nullable': True,  'comment': 'Total races in season (all teams)'},
            'valid_from':    {'type': 'TIMESTAMP','nullable': False, 'comment': 'SCD2 start timestamp'},
            'valid_to':      {'type': 'TIMESTAMP','nullable': True,  'comment': 'SCD2 end timestamp (NULL if current)'},
            'is_current':    {'type': 'BOOLEAN', 'nullable': False, 'comment': 'SCD2 current record flag'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': []  # Master data - no partitioning
    },

    'qualifying_results_silver': {
        'columns': {
            'session_key':   {'type': 'BIGINT',  'nullable': False, 'comment': 'Reference to session'},
            'driver_number': {'type': 'INT',     'nullable': False, 'comment': 'Reference to driver'},
            'position':      {'type': 'INT',     'nullable': False, 'comment': 'Final qualifying position'},
            'q1_time_millis': {'type': 'BIGINT', 'nullable': True,  'comment': 'Q1 time in ms'},
            'q2_time_millis': {'type': 'BIGINT', 'nullable': True,  'comment': 'Q2 time in ms'},
            'q3_time_millis': {'type': 'BIGINT', 'nullable': True,  'comment': 'Q3 time in ms'},
            'fastest_qualifying_time_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Driver fastest Q time (ms)'},
            'gap_to_pole_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Gap to pole (ms)'},
            'qualifying_status': {'type': 'STRING', 'nullable': True,  'comment': 'Q1/Q2/Q3/DNQ'},
            'year':          {'type': 'INT',      'nullable': False, 'comment': 'Season year'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Normalized Grand Prix name'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': ['year', 'grand_prix_name']
    },

    'race_results_silver': {
        'columns': {
            'session_key':   {'type': 'BIGINT',  'nullable': False, 'comment': 'Reference to session'},
            'driver_number': {'type': 'INT',     'nullable': False, 'comment': 'Reference to driver'},
            'position':      {'type': 'INT',     'nullable': False, 'comment': 'Final race position'},
            'grid_position': {'type': 'INT',     'nullable': True,  'comment': 'Grid start position'},
            'points':        {'type': 'INT',     'nullable': False, 'comment': 'Points earned (raw)'},
            'validated_points': {'type': 'INT',  'nullable': False, 'comment': 'Points after validation'},
            'positions_gained': {'type': 'INT',  'nullable': True,  'comment': 'Grid vs finish delta'},
            'time':          {'type': 'STRING',  'nullable': True,  'comment': 'Race time or status'},
            'gap_to_winner_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Gap to winner (ms)'},
            'status':        {'type': 'STRING',  'nullable': False, 'comment': 'Raw status'},
            'standardized_status': {'type': 'STRING', 'nullable': False, 'comment': 'Standardized status'},
            'year':          {'type': 'INT',      'nullable': False, 'comment': 'Season year'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Normalized Grand Prix name'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': ['year', 'grand_prix_name']
    },

    'laps_silver': {
        'columns': {
            'session_key':   {'type': 'BIGINT',  'nullable': False, 'comment': 'Reference to session'},
            'driver_number': {'type': 'INT',     'nullable': False, 'comment': 'Reference to driver'},
            'lap_number':    {'type': 'INT',     'nullable': False, 'comment': 'Lap number'},
            'lap_time':      {'type': 'STRING',  'nullable': True,  'comment': 'Lap time string'},
            'lap_time_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Lap time in ms'},
            'position_at_lap': {'type': 'INT',   'nullable': True,  'comment': 'Position at end of lap'},
            'gap_to_leader_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Gap to leader (ms)'},
            'interval_to_ahead_millis': {'type': 'BIGINT', 'nullable': True, 'comment': 'Gap to car ahead (ms)'},
            'is_personal_best': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Driver PB flag'},
            'is_fastest_lap': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Session fastest lap flag'},
            'tire_compound': {'type': 'STRING',  'nullable': True,  'comment': 'Tire compound'},
            'track_status':  {'type': 'STRING',  'nullable': True,  'comment': 'Track status code'},
            'year':          {'type': 'INT',      'nullable': False, 'comment': 'Season year'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Normalized Grand Prix name'},
            'session_type':  {'type': 'STRING',  'nullable': False, 'comment': 'Session type'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': ['year', 'grand_prix_name']
    },

    'pitstops_silver': {
        'columns': {
            'session_key':   {'type': 'BIGINT',  'nullable': False, 'comment': 'Reference to session'},
            'driver_number': {'type': 'INT',     'nullable': False, 'comment': 'Reference to driver'},
            'lap_number':    {'type': 'INT',     'nullable': False, 'comment': 'Lap number of pit stop'},
            'pit_duration':  {'type': 'DECIMAL(6,3)', 'nullable': False, 'comment': 'Pit duration in seconds'},
            'positions_lost_gained': {'type': 'INT', 'nullable': True, 'comment': 'Net positions change'},
            'undercut_attempt': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Undercut attempt'},
            'safety_car_stop': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Pit during safety car'},
            'tire_compound_old': {'type': 'STRING', 'nullable': True, 'comment': 'Pre-pit compound'},
            'tire_compound_new': {'type': 'STRING', 'nullable': True, 'comment': 'Post-pit compound'},
            'year':          {'type': 'INT',      'nullable': False, 'comment': 'Season year'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Normalized Grand Prix name'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation time'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update time'}
        },
        'partitions': ['year', 'grand_prix_name']
    }
}

# -----------------------------
# Gold Layer Schemas
# -----------------------------

GOLD_SCHEMAS: Dict[str, Dict] = {
    'driver_performance_summary_qualifying': {
        'columns': {
            'session_key': {'type': 'BIGINT', 'nullable': False, 'comment': 'Reference to sessions table'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Standardized Grand Prix name'},
            'race_date': {'type': 'DATE', 'nullable': False, 'comment': 'Race date for chronological analysis'},
            'round_number': {'type': 'INT', 'nullable': False, 'comment': 'Round number in season'},
            'driver_number': {'type': 'INT', 'nullable': False, 'comment': 'Official F1 driver number'},
            'driver_name': {'type': 'STRING', 'nullable': False, 'comment': 'Driver broadcast name'},
            'team_name': {'type': 'STRING', 'nullable': False, 'comment': 'Team name for filtering'},
            'qualifying_position': {'type': 'INT', 'nullable': True, 'comment': 'Final qualifying position (1-20)'},
            'gap_to_pole_seconds': {'type': 'DECIMAL(5,3)', 'nullable': True, 'comment': 'Gap to pole position in seconds'},
            'made_q3': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Whether driver made it to Q3 (position <= 10)'},
            'season_avg_qualifying_position': {'type': 'DECIMAL(4,1)', 'nullable': True, 'comment': 'Average qualifying position this season'},
            'year': {'type': 'INT', 'nullable': False, 'comment': 'F1 season year'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation timestamp'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update timestamp'}
        },
        'partitions': ['year', 'grand_prix_name']
    },
    
    'driver_performance_summary_race': {
        'columns': {
            'session_key': {'type': 'BIGINT', 'nullable': False, 'comment': 'Reference to sessions table'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Standardized Grand Prix name'},
            'race_date': {'type': 'DATE', 'nullable': False, 'comment': 'Race date for chronological analysis'},
            'round_number': {'type': 'INT', 'nullable': False, 'comment': 'Round number in season'},
            'driver_number': {'type': 'INT', 'nullable': False, 'comment': 'Official F1 driver number'},
            'driver_name': {'type': 'STRING', 'nullable': False, 'comment': 'Driver broadcast name'},
            'team_name': {'type': 'STRING', 'nullable': False, 'comment': 'Team name for filtering'},
            'grid_position': {'type': 'INT', 'nullable': True, 'comment': 'Starting grid position (1-20)'},
            'finish_position': {'type': 'INT', 'nullable': True, 'comment': 'Final race position (1-20)'},
            'race_points': {'type': 'DECIMAL(4,1)', 'nullable': False, 'comment': 'Championship points earned (0-26)'},
            'positions_gained': {'type': 'INT', 'nullable': True, 'comment': 'Grid position minus finish position'},
            'season_points_total': {'type': 'DECIMAL(6,1)', 'nullable': False, 'comment': 'Running total points (cumulative)'},
            'year': {'type': 'INT', 'nullable': False, 'comment': 'F1 season year'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation timestamp'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update timestamp'}
        },
        'partitions': ['year', 'grand_prix_name']
    },
    
    'championship_tracker': {
        'columns': {
            'race_date': {'type': 'DATE', 'nullable': False, 'comment': 'Race date for chronological ordering'},
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Grand Prix name for context'},
            'round_number': {'type': 'INT', 'nullable': False, 'comment': 'Round number in season (for ordering)'},
            'driver_number': {'type': 'INT', 'nullable': False, 'comment': 'Official F1 driver number'},
            'driver_name': {'type': 'STRING', 'nullable': False, 'comment': 'Driver broadcast name'},
            'team_name': {'type': 'STRING', 'nullable': False, 'comment': 'Team name for filtering'},
            'finish_position': {'type': 'INT', 'nullable': True, 'comment': 'Final race position (1-20)'},
            'race_points': {'type': 'DECIMAL(4,1)', 'nullable': False, 'comment': 'Points earned in this race (0-26)'},
            'season_points_total': {'type': 'DECIMAL(6,1)', 'nullable': False, 'comment': 'Cumulative points total'},
            'championship_position': {'type': 'INT', 'nullable': False, 'comment': 'Current championship position (1-20)'},
            'points_gap_to_leader': {'type': 'DECIMAL(6,1)', 'nullable': True, 'comment': 'Points behind leader (0 if leader)'},
            'year': {'type': 'INT', 'nullable': False, 'comment': 'F1 season year'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation timestamp'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update timestamp'}
        },
        'partitions': ['year']  # Year-only partitioning for time-series queries
    },
    
    
    'race_weekend_insights': {
        'columns': {
            'grand_prix_name': {'type': 'STRING', 'nullable': False, 'comment': 'Grand Prix name'},
            'race_date': {'type': 'DATE', 'nullable': False, 'comment': 'Race date for chronological analysis'},
            'round_number': {'type': 'INT', 'nullable': False, 'comment': 'Round number in season'},
            'pole_position_driver': {'type': 'STRING', 'nullable': True, 'comment': 'Driver who achieved pole position'},
            'race_winner_driver': {'type': 'STRING', 'nullable': False, 'comment': 'Race winner name'},
            'race_winner_team': {'type': 'STRING', 'nullable': False, 'comment': 'Race winner team'},
            'pole_to_win': {'type': 'BOOLEAN', 'nullable': False, 'comment': 'Whether pole position won the race'},
            'total_finishers': {'type': 'INT', 'nullable': False, 'comment': 'Number of drivers who finished'},
            'dnf_count': {'type': 'INT', 'nullable': False, 'comment': 'Number of DNFs'},
            'year': {'type': 'INT', 'nullable': False, 'comment': 'F1 season year'},
            'created_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record creation timestamp'},
            'updated_timestamp': {'type': 'TIMESTAMP', 'nullable': False, 'comment': 'Record last update timestamp'}
        },
        'partitions': ['year', 'grand_prix_name']
    }
}

# -----------------------------
# Helper Functions
# -----------------------------

def get_table_schema(table_name: str) -> Dict:
    if table_name in SILVER_SCHEMAS:
        return SILVER_SCHEMAS[table_name]
    if table_name in GOLD_SCHEMAS:
        return GOLD_SCHEMAS[table_name]
    raise ValueError(f"Unknown table: {table_name}")


def get_ddl_columns(table_name: str) -> str:
    """Generate Iceberg-compatible DDL column list for a table."""
    schema = get_table_schema(table_name)
    columns = schema['columns']
    ddl_lines: List[str] = []
    for name, meta in columns.items():
        nullable_str = '' if not meta.get('nullable', True) else ''
        # We express NOT NULL explicitly when needed
        type_str = meta['type'] + (" NOT NULL" if meta.get('nullable', True) is False else "")
        comment = meta.get('comment')
        comment_str = f" COMMENT '{comment.replace("'", "''")}'" if comment else ''
        ddl_lines.append(f"    `{name}` {type_str}{comment_str}")
    return ",\n".join(ddl_lines)


def get_partitions(table_name: str) -> List[str]:
    """Return partition columns for the given table."""
    schema = get_table_schema(table_name)
    return schema.get('partitions', [])


# -----------------------------
# Additional Helper Functions for Gold Layer
# -----------------------------

def get_gold_table_names() -> List[str]:
    """Return list of all Gold table names."""
    return list(GOLD_SCHEMAS.keys())


def generate_gold_table_ddl(table_name: str, database_name: str = "f1_gold_db") -> str:
    """Generate complete CREATE TABLE DDL for a Gold table."""
    if table_name not in GOLD_SCHEMAS:
        raise ValueError(f"Unknown Gold table: {table_name}")
    
    schema = GOLD_SCHEMAS[table_name]
    columns_ddl = get_ddl_columns(table_name)
    partitions = get_partitions(table_name)
    
    # Create partition clause
    if partitions:
        partition_clause = f"PARTITIONED BY ({', '.join(partitions)})"
    else:
        partition_clause = ""
    
    # Create Iceberg properties for Gold tables
    iceberg_properties = """
TBLPROPERTIES (
    'write.target-file-size-bytes'='268435456',
    'write.parquet.compression-codec'='snappy',
    'write.parquet.row-group-size'='134217728',
    'format'='iceberg'
)"""
    
    ddl = f"""CREATE TABLE IF NOT EXISTS {database_name}.{table_name} (
{columns_ddl}
) USING ICEBERG
{partition_clause}
{iceberg_properties};"""
    
    return ddl


def get_gold_table_comment(table_name: str) -> str:
    """Get business description for Gold tables."""
    comments = {
        'driver_performance_summary_qualifying': 'Core qualifying performance metrics with positions, gaps to pole, and season averages',
        'driver_performance_summary_race': 'Essential race performance with positions, points, and cumulative championship progression',
        'championship_tracker': 'Championship standings after each race with points progression and gaps to leader',
        'race_weekend_insights': 'Weekend highlights with pole position, race winner, and basic race statistics'
    }
    return comments.get(table_name, f"Gold analytics table: {table_name}")


def validate_gold_schema_consistency() -> Dict[str, List[str]]:
    """Validate consistency across Gold table schemas."""
    issues = []
    
    # Check that all tables have required audit columns
    required_audit_columns = ['created_timestamp', 'updated_timestamp']
    
    for table_name, schema in GOLD_SCHEMAS.items():
        columns = schema['columns']
        
        # Check audit columns
        missing_audit = [col for col in required_audit_columns if col not in columns]
        if missing_audit:
            issues.append(f"{table_name}: Missing audit columns: {missing_audit}")
        
        # Check year column for partitioning consistency
        if 'year' not in columns:
            issues.append(f"{table_name}: Missing 'year' column for partitioning")
        
        # Check partition column existence
        partitions = schema.get('partitions', [])
        for partition_col in partitions:
            if partition_col not in columns:
                issues.append(f"{table_name}: Partition column '{partition_col}' not found in columns")
    
    return {"validation_issues": issues}


def get_all_table_names() -> List[str]:
    """Return all table names (Silver + Gold)."""
    return list(SILVER_SCHEMAS.keys()) + list(GOLD_SCHEMAS.keys())
