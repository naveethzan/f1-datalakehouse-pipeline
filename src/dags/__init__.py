"""
F1 Data Pipeline DAGs

This package contains Airflow DAGs for the F1 data engineering pipeline.
Includes both historical and incremental data extraction workflows.

Modules:
- config: DAG configuration settings
- services: External service integrations (OpenF1 API, S3)
- f1_historical_load_dag: Full year data extraction DAG
- f1_weekly_incremental_dag: Weekly incremental data extraction DAG
"""

# Import configuration
from src.dags.config.dag_config import DAGConfig

# Import services
from src.dags.services.extractor import ServiceExtractor
from src.dags.services.openf1_client import OpenF1Client
from src.dags.services.s3_writer import S3Writer

__all__ = [
    # Configuration
    'DAGConfig',
    # Services
    'ServiceExtractor',
    'OpenF1Client',
    'S3Writer'
]

__version__ = '1.0.0'
__author__ = 'F1 Data Engineering Team'
