"""
F1 Historical Load Pipeline DAG

A comprehensive manual-trigger DAG for loading a complete year of F1 data
through the entire data engineering pipeline.

Pipeline Flow:
1. extract_full_year_bronze_data: Extracts complete year of Bronze data (Qualifying + Race sessions)
2. bronze_to_silver_transform: Transforms all Bronze layer data to Silver layer
3. silver_to_gold_transform: Transforms all Silver layer data to Gold layer analytics

Trigger: Manual execution only (no schedule)
Mode: HISTORICAL processing for complete data backfill
Scope: Full year data processing with comprehensive error handling
"""

import logging
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from src.dags import DAGConfig, ServiceExtractor


logger = logging.getLogger(__name__)


@dag(
    dag_id=DAGConfig.HISTORICAL_DAG_ID,
    description='F1 Historical Pipeline: Complete year backfill -> Bronze -> Silver -> Gold',
    schedule=None,  # Manual trigger only - no automatic scheduling
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DAGConfig.DEFAULT_DAG_ARGS,
    tags=['f1', 'historical', 'backfill', 'bronze', 'silver', 'gold', 'pipeline', 'manual']
)
def f1_historical_load():
    """
    F1 Historical Load Pipeline for complete year data backfill.
    
    Tasks:
    - extract_full_year_bronze_data: Extract complete year of Bronze data (all Qualifying + Race sessions)
    - bronze_to_silver_transform: Execute Bronze to Silver transformation (Glue job)
    - silver_to_gold_transform: Execute Silver to Gold transformation (Glue job)
    
    Flow: Complete Bronze Extraction -> Silver Transform -> Gold Transform
    """
    
    @task
    def extract_full_year_bronze_data():
        """Extract complete year of Bronze data for all Qualifying and Race sessions."""
        
        logger.info(f"Starting historical Bronze extraction for {DAGConfig.YEAR} season")
        
        extractor = ServiceExtractor()
        
        try:
            # Get all sessions for the year
            logger.info(f"\n🔍 Fetching session data for {DAGConfig.YEAR} F1 season...")
            all_sessions = []
            
            # Get Qualifying sessions
            logger.info("Fetching Qualifying sessions...")
            qualifying_sessions = extractor.client.get_sessions(DAGConfig.YEAR, 'Qualifying')
            all_sessions.extend(qualifying_sessions)
            logger.info(f"🏁 Found {len(qualifying_sessions)} Qualifying sessions")
            
            # Get Race sessions  
            logger.info("Fetching Race sessions...")
            race_sessions = extractor.client.get_sessions(DAGConfig.YEAR, 'Race')
            all_sessions.extend(race_sessions)
            logger.info(f"🏆 Found {len(race_sessions)} Race sessions")
            
            total_sessions = len(all_sessions)
            logger.info(f"\n📊 TOTAL SESSIONS TO PROCESS: {total_sessions}")
            
            if total_sessions == 0:
                raise ValueError(f"No sessions found for {DAGConfig.YEAR}. Check year validity.")
            
            # Process each session with comprehensive logging
            logger.info(f"Starting Bronze data extraction for {total_sessions} sessions")
            
            processed_count = 0
            failed_count = 0
            
            for i, session in enumerate(all_sessions, 1):
                session_key = session.get('session_key')
                session_type = session.get('session_type')
                location = session.get('location', 'Unknown')
                session_date = session.get('date_start', 'Unknown')
                
                logger.info(f"Processing session {i}/{total_sessions}: {location} {session_type}")
                
                try:
                    extractor.extract_session_data(session)
                    processed_count += 1
                    logger.info(f"Successfully extracted {session_type} data for {location}")
                    
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Failed to process session {session_key}: {e}")
                    # Continue with next session for historical loads
                    continue
            
            # Final summary
            logger.info(f"Historical extraction summary: {processed_count}/{total_sessions} successful, {failed_count} failed")
            
            if failed_count > 0:
                logger.warning(f"{failed_count} sessions failed to process - check logs above")
                
            if processed_count == 0:
                raise ValueError("No sessions were successfully processed. Check connectivity and data availability.")
                
            logger.info(f"Historical Bronze extraction completed for {DAGConfig.YEAR} - {processed_count} sessions processed")
            
        finally:
            extractor.close()
    
    # =========================================================================
    # Task Definition - Explicit naming for clear pipeline flow
    # =========================================================================
    
    # Step 1: Extract complete year of Bronze data (all sessions)
    extract_bronze_task = extract_full_year_bronze_data()
    
    # Step 2: Transform Bronze to Silver layer (complete year)
    bronze_to_silver_task = GlueJobOperator(
        task_id='bronze_to_silver_transform',
        job_name=DAGConfig.BRONZE_TO_SILVER_JOB_NAME,
        script_args={
            '--JOB_NAME': f'{DAGConfig.BRONZE_TO_SILVER_JOB_NAME}-historical-{DAGConfig.YEAR}',
            '--RUN_MODE': 'HISTORICAL',
            '--YEAR': str(DAGConfig.YEAR)
        },
        region_name=DAGConfig.AWS_REGION,
        wait_for_completion=True
    )
    
    # Step 3: Transform Silver to Gold layer (complete year)
    silver_to_gold_task = GlueJobOperator(
        task_id='silver_to_gold_transform',
        job_name=DAGConfig.SILVER_TO_GOLD_JOB_NAME,
        script_args={
            '--JOB_NAME': f'{DAGConfig.SILVER_TO_GOLD_JOB_NAME}-historical-{DAGConfig.YEAR}',
            '--RUN_MODE': 'HISTORICAL',
            '--YEAR': str(DAGConfig.YEAR)
        },
        region_name=DAGConfig.AWS_REGION,
        wait_for_completion=True
    )
    
    # =========================================================================
    # Pipeline Dependencies - Complete F1 Historical Data Flow
    # =========================================================================
    # Sequential execution: Extract all Bronze data -> Transform to Silver -> Transform to Gold
    # Full year processing in HISTORICAL mode for complete data backfill
    
    extract_bronze_task >> bronze_to_silver_task >> silver_to_gold_task


# Instantiate the DAG
dag = f1_historical_load()
