"""
Simple Configuration Module for F1 Bronze to Silver Pipeline

This module provides centralized configuration for the entire pipeline.
All values are hardcoded for simplicity (resume project approach).
Uses a Config object pattern for clean access from other modules.

Usage:
    from config.job_config import Config
    bucket = Config.BRONZE_BUCKET
    database = Config.SILVER_DATABASE_NAME
"""


class WriteMode:
    """
    Write mode constants for Iceberg table operations.
    
    Defines different write strategies based on table partitioning
    and run mode requirements.
    """
    # Basic write modes
    OVERWRITE_ALL = "overwrite"                        # Replace entire table
    APPEND = "append"                                  # Add records only
    
    # Partition-aware write modes (implemented in Phase 6)
    OVERWRITE_YEAR = "overwrite_partitions_year"       # Replace year partition only
    OVERWRITE_YEAR_GP = "overwrite_partitions_year_gp" # Replace year+GP partition only
    
    # Special write modes
    MERGE_SCD2 = "merge_scd2"                          # SCD Type 2 merge logic


class Config:
    """
    Configuration object for F1 Bronze to Silver pipeline.
    All values are hardcoded for simplicity.
    
    Updated with partition-aware write mode strategies.
    """
    
    # ===========================
    # S3 Configuration
    # ===========================
    
    # S3 Bucket
    BRONZE_BUCKET = 'f1-data-lake-naveeth'
    
    # S3 Paths
    BRONZE_PATH = f's3://{BRONZE_BUCKET}/bronze'
    SILVER_PATH = f's3://{BRONZE_BUCKET}/silver'
    ICEBERG_WAREHOUSE = f's3://{BRONZE_BUCKET}/iceberg-warehouse'
    
    # ===========================
    # Database Configuration
    # ===========================
    
    # Catalog and Database
    CATALOG_NAME = 'glue_catalog'
    SILVER_DATABASE_NAME = 'f1_silver_db'
    FULL_DATABASE_NAME = f'{CATALOG_NAME}.{SILVER_DATABASE_NAME}'
    
    # Database Location
    DATABASE_LOCATION = f's3://{BRONZE_BUCKET}/silver/'
    
    # ===========================
    # Table Configuration
    # ===========================
    
    # Silver Table Names
    SILVER_TABLES = [
        'sessions_silver',
        'drivers_silver',
        'qualifying_results_silver',
        'race_results_silver',
        'laps_silver',
        'pitstops_silver'
    ]
    
    # Table Partitioning Strategy (Updated for Phase 2)
    TABLE_PARTITIONS = {
        'sessions_silver': ['year', 'grand_prix_name'],  # Updated: Now partitioned by year and GP
        'drivers_silver': [],  # No partitioning - SCD Type 2 master data
        'qualifying_results_silver': ['year', 'grand_prix_name'],
        'race_results_silver': ['year', 'grand_prix_name'],
        'laps_silver': ['year', 'grand_prix_name'],
        'pitstops_silver': ['year', 'grand_prix_name']
    }
    
    # ===========================
    # Table Write Strategy Configuration (New for Phase 2)
    # ===========================
    
    # Comprehensive write strategy mapping by table and run mode
    TABLE_WRITE_STRATEGIES = {
        'sessions_silver': {
            'partitions': ['year', 'grand_prix_name'],
            'table_type': 'transactional',
            'description': 'Session metadata - partitioned by year and GP',
            'write_modes': {
                'HISTORICAL': WriteMode.OVERWRITE_YEAR,     # Replace all data for target year
                'INCREMENTAL': WriteMode.OVERWRITE_YEAR_GP  # Replace data for target year+GP
            }
        },
        
        'drivers_silver': {
            'partitions': [],
            'table_type': 'scd_type2',
            'description': 'Driver master data with SCD Type 2 temporal tracking',
            'scd_config': {
                'change_detection_columns': ['team_name'],
                'validity_date_source': 'session_date',
                'data_source_filter': 'race'  # Only race sessions for SCD
            },
            'write_modes': {
                'HISTORICAL': WriteMode.MERGE_SCD2,         # SCD merge for target year
                'INCREMENTAL': WriteMode.MERGE_SCD2         # SCD merge for target GP
            }
        },
        
        'qualifying_results_silver': {
            'partitions': ['year', 'grand_prix_name'],
            'table_type': 'transactional',
            'description': 'Qualifying session results - partitioned by year and GP',
            'write_modes': {
                'HISTORICAL': WriteMode.OVERWRITE_YEAR,     # Replace all data for target year
                'INCREMENTAL': WriteMode.OVERWRITE_YEAR_GP  # Replace data for target year+GP
            }
        },
        
        'race_results_silver': {
            'partitions': ['year', 'grand_prix_name'],
            'table_type': 'transactional',
            'description': 'Race session results - partitioned by year and GP',
            'write_modes': {
                'HISTORICAL': WriteMode.OVERWRITE_YEAR,     # Replace all data for target year
                'INCREMENTAL': WriteMode.OVERWRITE_YEAR_GP  # Replace data for target year+GP
            }
        },
        
        'laps_silver': {
            'partitions': ['year', 'grand_prix_name'],
            'table_type': 'transactional',
            'description': 'Lap-by-lap race data - partitioned by year and GP',
            'write_modes': {
                'HISTORICAL': WriteMode.OVERWRITE_YEAR,     # Replace all data for target year
                'INCREMENTAL': WriteMode.OVERWRITE_YEAR_GP  # Replace data for target year+GP
            },
            'special_handling': {
                'chunked_processing': True,
                'chunk_size': 'per_grand_prix'
            }
        },
        
        'pitstops_silver': {
            'partitions': ['year', 'grand_prix_name'],
            'table_type': 'transactional', 
            'description': 'Pit stop data - partitioned by year and GP',
            'write_modes': {
                'HISTORICAL': WriteMode.OVERWRITE_YEAR,     # Replace all data for target year
                'INCREMENTAL': WriteMode.OVERWRITE_YEAR_GP  # Replace data for target year+GP
            }
        }
    }
    
    # ===========================
    # Bronze Data Configuration
    # ===========================
    
    # Bronze Data Types (folder names in Bronze layer)
    BRONZE_DATA_TYPES = [
        'session_result',  # Contains session metadata
        'drivers',         # Driver information
        'laps',           # Lap-by-lap data
        'pit'             # Pit stop data (races only)
    ]
    
    # Session Types to Process
    VALID_SESSION_TYPES = ['qualifying', 'race']  # Exclude practice sessions
    
    # ===========================
    # Default Values
    # ===========================
    
    # Season Configuration
    DEFAULT_YEAR = 2025
    CURRENT_SEASON = 2025
    
    # AWS Configuration
    AWS_REGION = 'us-east-1'
    
    # ===========================
    # Glue Job Configuration
    # ===========================
    
    # Glue Version
    GLUE_VERSION = '5.0'
    
    # Worker Configuration
    WORKER_TYPE = 'G.1X'
    NUMBER_OF_WORKERS = 2
    
    # Job Timeout (minutes)
    JOB_TIMEOUT = 60
    
    # ===========================
    # Spark Configuration
    # ===========================
    
    # Spark Settings
    SPARK_SHUFFLE_PARTITIONS = 200
    SPARK_ADAPTIVE_ENABLED = True
    SPARK_ADAPTIVE_COALESCE_PARTITIONS = True
    SPARK_ADAPTIVE_SKEW_JOIN = True
    
    # Memory Settings (MB)
    SPARK_ADVISORY_PARTITION_SIZE_MB = 128
    SPARK_PARQUET_ROW_GROUP_SIZE_MB = 128
    
    # ===========================
    # Iceberg Configuration
    # ===========================
    
    # Iceberg Table Properties
    ICEBERG_PROPERTIES = {
        'write.format.default': 'parquet',
        'write.parquet.compression-codec': 'snappy',
        'write.metadata.delete-after-commit.enabled': 'true',
        'write.metadata.previous-versions-max': '5',
        'format-version': '2',
        'write.object-storage.enabled': 'true',
        'commit.retry.num-retries': '3'
    }
    
    # ===========================
    # Run Mode Configuration
    # ===========================
    
    # Run Modes
    RUN_MODE_HISTORICAL = 'HISTORICAL'
    RUN_MODE_INCREMENTAL = 'INCREMENTAL'
    
    # Write Modes (fixed per run mode)
    WRITE_MODE_MAPPING = {
        'HISTORICAL': 'overwrite',
        'INCREMENTAL': 'append'
    }
    
    # ===========================
    # Data Processing Configuration
    # ===========================
    
    # Rate Limiting
    API_RATE_LIMIT_SECONDS = 0.2  # For Bronze extraction if needed
    
    # Eventual Consistency Wait Time (seconds)
    EVENTUAL_CONSISTENCY_WAIT = 60
    
    # Batch Sizes
    DEFAULT_BATCH_SIZE = 10000
    
    # ===========================
    # F1 Domain Configuration
    # ===========================
    
    # F1 Points System (2022+ regulations)
    F1_POINTS_SYSTEM = [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]
    F1_FASTEST_LAP_BONUS = 1
    F1_MAX_POINTS_PER_RACE = 26  # 25 for win + 1 for fastest lap
    
    # Tire Compounds
    TIRE_COMPOUNDS = ['SOFT', 'MEDIUM', 'HARD', 'INTERMEDIATE', 'WET']
    
    # ===========================
    # Logging Configuration
    # ===========================
    
    # Log Level
    LOG_LEVEL = 'INFO'
    
    # Log Format
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # ===========================
    # Helper Methods
    # ===========================
    
    @classmethod
    def get_full_table_name(cls, table_name: str) -> str:
        """
        Get fully qualified table name with catalog and database.
        
        Args:
            table_name: Short table name (e.g., 'sessions_silver')
            
        Returns:
            Full table name (e.g., 'glue_catalog.f1_silver_db.sessions_silver')
        """
        return f"{cls.CATALOG_NAME}.{cls.SILVER_DATABASE_NAME}.{table_name}"
    
    @classmethod
    def get_table_location(cls, table_name: str) -> str:
        """
        Get S3 location for a specific table.
        
        Args:
            table_name: Table name
            
        Returns:
            S3 path for table storage
        """
        return f"{cls.SILVER_PATH}/{table_name}"
    
    @classmethod
    def get_table_partitions(cls, table_name: str) -> list:
        """
        Get partition columns for a specific table.
        
        Args:
            table_name: Table name
            
        Returns:
            List of partition column names
        """
        return cls.TABLE_PARTITIONS.get(table_name, [])
    
    @classmethod 
    def get_write_mode_for_table(cls, run_mode: str, table_name: str) -> str:
        """
        Get appropriate write mode for specific table and run mode combination.
        
        This is the new primary method for determining write strategies.
        Replaces the deprecated get_write_mode() method.
        
        Args:
            run_mode: 'HISTORICAL' or 'INCREMENTAL'
            table_name: Name of the Silver table (e.g., 'sessions_silver')
            
        Returns:
            Write mode constant from WriteMode class
            
        Raises:
            ValueError: If table_name or run_mode is invalid
        """
        if table_name not in cls.TABLE_WRITE_STRATEGIES:
            raise ValueError(f"Unknown table: {table_name}. Valid tables: {list(cls.TABLE_WRITE_STRATEGIES.keys())}")
        
        if run_mode not in [cls.RUN_MODE_HISTORICAL, cls.RUN_MODE_INCREMENTAL]:
            raise ValueError(f"Invalid run_mode: {run_mode}. Must be '{cls.RUN_MODE_HISTORICAL}' or '{cls.RUN_MODE_INCREMENTAL}'")
        
        table_strategy = cls.TABLE_WRITE_STRATEGIES[table_name]
        return table_strategy['write_modes'][run_mode]
    
    @classmethod
    def get_table_strategy(cls, table_name: str) -> dict:
        """
        Get complete table strategy configuration.
        
        Args:
            table_name: Name of the Silver table
            
        Returns:
            Dictionary containing all table strategy configuration
            
        Raises:
            ValueError: If table_name is invalid
        """
        if table_name not in cls.TABLE_WRITE_STRATEGIES:
            raise ValueError(f"Unknown table: {table_name}. Valid tables: {list(cls.TABLE_WRITE_STRATEGIES.keys())}")
        
        return cls.TABLE_WRITE_STRATEGIES[table_name]
    
    @classmethod
    def is_scd_type2_table(cls, table_name: str) -> bool:
        """
        Check if table uses SCD Type 2 logic.
        
        Args:
            table_name: Name of the Silver table
            
        Returns:
            True if table uses SCD Type 2, False otherwise
        """
        strategy = cls.get_table_strategy(table_name)
        return strategy.get('table_type') == 'scd_type2'
    
    @classmethod
    def requires_chunked_processing(cls, table_name: str) -> bool:
        """
        Check if table requires chunked processing.
        
        Args:
            table_name: Name of the Silver table
            
        Returns:
            True if chunked processing is required, False otherwise
        """
        strategy = cls.get_table_strategy(table_name)
        special_handling = strategy.get('special_handling', {})
        return special_handling.get('chunked_processing', False)
    
    @classmethod
    def get_write_mode(cls, run_mode: str) -> str:
        """
        DEPRECATED: Get write mode based on run mode only.
        
        This method is deprecated in favor of get_write_mode_for_table()
        which provides table-specific write strategies.
        
        Args:
            run_mode: 'HISTORICAL' or 'INCREMENTAL'
            
        Returns:
            'overwrite' or 'append'
        """
        import warnings
        warnings.warn(
            "get_write_mode() is deprecated. Use get_write_mode_for_table() for table-specific write strategies.",
            DeprecationWarning,
            stacklevel=2
        )
        return cls.WRITE_MODE_MAPPING.get(run_mode, 'append')
    
    @classmethod
    def get_all_table_names(cls) -> list:
        """
        Get list of all configured Silver table names.
        
        Returns:
            List of table names from TABLE_WRITE_STRATEGIES
        """
        return list(cls.TABLE_WRITE_STRATEGIES.keys())
    
    @classmethod
    def validate_table_configuration(cls) -> dict:
        """
        Validate that table configuration is consistent.
        
        Checks that TABLE_PARTITIONS matches TABLE_WRITE_STRATEGIES.
        Useful for configuration validation during startup.
        
        Returns:
            Dictionary with validation results
        """
        results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Check that all tables in WRITE_STRATEGIES have partition info
        for table_name, strategy in cls.TABLE_WRITE_STRATEGIES.items():
            expected_partitions = strategy.get('partitions', [])
            actual_partitions = cls.TABLE_PARTITIONS.get(table_name, [])
            
            if expected_partitions != actual_partitions:
                error_msg = f"Partition mismatch for {table_name}: expected {expected_partitions}, got {actual_partitions}"
                results['errors'].append(error_msg)
                results['valid'] = False
        
        # Check that all tables in SILVER_TABLES have write strategies
        for table_name in cls.SILVER_TABLES:
            if table_name not in cls.TABLE_WRITE_STRATEGIES:
                warning_msg = f"Table {table_name} in SILVER_TABLES but not in TABLE_WRITE_STRATEGIES"
                results['warnings'].append(warning_msg)
        
        return results
    
    @classmethod
    def is_valid_session_type(cls, session_type: str) -> bool:
        """
        Check if session type should be processed.
        
        Args:
            session_type: Session type to check
            
        Returns:
            True if session type should be processed
        """
        return session_type.lower() in cls.VALID_SESSION_TYPES
    
    @classmethod
    def get_bronze_data_path(cls, data_type: str, year: int = None, grand_prix: str = None) -> str:
        """
        Construct Bronze data path with optional filters.
        
        Args:
            data_type: Type of Bronze data (e.g., 'drivers', 'laps')
            year: Optional year filter
            grand_prix: Optional grand prix filter
            
        Returns:
            S3 path to Bronze data
        """
        path = f"{cls.BRONZE_PATH}/{data_type}"
        
        if year:
            path += f"/year={year}"
            if grand_prix:
                path += f"/grand_prix={grand_prix}"
        
        return path


# Create a convenience instance for backward compatibility
# Can be used like: from config.job_config import config
config = Config()
