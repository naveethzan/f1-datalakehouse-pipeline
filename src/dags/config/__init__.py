"""
DAG Configuration Module

Contains configuration settings for F1 Airflow DAGs.
Centralized configuration for API settings, S3 paths, and DAG parameters.
"""

from src.dags.config.dag_config import DAGConfig

__all__ = [
    'DAGConfig'
]
