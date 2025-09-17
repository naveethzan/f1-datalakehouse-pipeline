"""
DAG Configuration for F1 Data Pipeline

Simple configuration for Bronze extraction DAGs.
Focused only on DAG-specific settings and API extraction logic.
"""


class DAGConfig:
    """
    Simple configuration for F1 Airflow DAGs.
    """
    
    # ===========================
    # Core Settings
    # ===========================
    
    # S3 Bucket for Bronze data
    BRONZE_BUCKET = 'f1-data-lake-naveeth'
    BRONZE_PATH = f's3://{BRONZE_BUCKET}/bronze'
    
    # Fixed year for 2025 season
    YEAR = 2025
    
    # AWS Region
    AWS_REGION = 'us-east-1'
    
    # ===========================
    # OpenF1 API Settings
    # ===========================
    
    API_BASE_URL = 'https://api.openf1.org/v1'
    API_RATE_LIMIT_SECONDS = 0.2  # 200ms between requests
    API_TIMEOUT_SECONDS = 30
    API_MAX_RETRIES = 3
    
    # ===========================
    # Data Extraction Rules
    # ===========================
    
    # Endpoints to extract by session type
    ENDPOINTS_BY_SESSION = {
        'qualifying': ['session_result'],
        'race': ['session_result', 'drivers', 'laps', 'pit']
    }
    
    # Session types to process
    VALID_SESSION_TYPES = ['Qualifying', 'Race']
    
    # ===========================
    # DAG Settings
    # ===========================
    
    # Historical Load DAG
    HISTORICAL_DAG_ID = 'f1_historical_load'
    HISTORICAL_DAG_SCHEDULE = None  # Manual trigger
    
    # Weekly Incremental DAG  
    WEEKLY_DAG_ID = 'f1_weekly_incremental'
    WEEKLY_DAG_SCHEDULE = '0 6 * * 1'  # Monday 6 AM UTC
    
    # Default DAG arguments
    DEFAULT_DAG_ARGS = {
        'owner': 'f1-data-engineering',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': 300  # 5 minutes
    }
    
    # ===========================
    # Glue Job Settings
    # ===========================
    
    # Bronze to Silver Glue job
    BRONZE_TO_SILVER_JOB_NAME = 'f1-bronze-to-silver-transform'
    
    # Silver to Gold Glue job
    SILVER_TO_GOLD_JOB_NAME = 'f1-silver-to-gold-transform'
    
    # Backward compatibility
    GLUE_JOB_NAME = BRONZE_TO_SILVER_JOB_NAME
    
    # ===========================
    # Helper Methods
    # ===========================
    
    @classmethod
    def get_endpoints_for_session(cls, session_type: str) -> list:
        """
        Get endpoints to extract based on session type.
        
        Args:
            session_type: 'Qualifying' or 'Race'
            
        Returns:
            List of endpoints to extract
        """
        session_lower = session_type.lower()
        return cls.ENDPOINTS_BY_SESSION.get(session_lower, [])
