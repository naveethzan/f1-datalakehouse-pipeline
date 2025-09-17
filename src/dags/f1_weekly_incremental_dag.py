"""
F1 Weekly Incremental Pipeline DAG

A comprehensive weekly scheduled DAG that automatically detects the latest completed Grand Prix
and processes it through the complete F1 data engineering pipeline.

Pipeline Flow:
1. detect_and_extract_latest_grand_prix: Auto-detects latest completed GP and extracts Bronze data
2. bronze_to_silver_transform: Transforms Bronze layer data to Silver layer
3. silver_to_gold_transform: Transforms Silver layer data to Gold layer analytics

Schedule: Every Monday at 6:00 AM UTC (processes previous weekend's race)
Execution: Sequential pipeline with proper error handling and logging
Mode: INCREMENTAL processing for efficient data updates
"""

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from src.dags import DAGConfig, ServiceExtractor


logger = logging.getLogger(__name__)


@dag(
    dag_id=DAGConfig.WEEKLY_DAG_ID,
    description='F1 Weekly Incremental Pipeline: Auto-detect latest GP -> Bronze -> Silver -> Gold',
    schedule=DAGConfig.WEEKLY_DAG_SCHEDULE,  # Every Monday at 6:00 AM UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DAGConfig.DEFAULT_DAG_ARGS,
    tags=['f1', 'incremental', 'auto-detect', 'bronze', 'silver', 'gold', 'pipeline', 'weekly']
)
def f1_weekly_incremental():
    """
    F1 Weekly Incremental Pipeline with auto-detection and explicit task flow.
    
    Tasks:
    - detect_and_extract_latest_grand_prix: Auto-detect latest completed GP and extract Bronze data
    - bronze_to_silver_transform: Execute Bronze to Silver transformation (Glue job)
    - silver_to_gold_transform: Execute Silver to Gold transformation (Glue job)
    
    Flow: Bronze Extraction -> Silver Transform -> Gold Transform
    """
    
    @task
    def detect_and_extract_latest_grand_prix():
        """Auto-detect the latest completed Grand Prix and extract Bronze data."""
        
        logger.info("Starting auto-detection and Bronze extraction")
        extractor = ServiceExtractor()
        
        try:
            # Get all Race sessions for the year to find the latest
            logger.info(f"Scanning {DAGConfig.YEAR} race schedule to find latest completed Grand Prix")
            race_sessions = extractor.client.get_sessions(DAGConfig.YEAR, 'Race')
            
            if not race_sessions:
                raise ValueError(f"No race sessions found for {DAGConfig.YEAR}")
            
            # Find the most recent race that has already happened
            current_date = datetime.utcnow()
            past_races = []
            
            logger.info(f"Found {len(race_sessions)} total race sessions for {DAGConfig.YEAR}")
            logger.info(f"Current date: {current_date.isoformat()}")
            
            for session in race_sessions:
                # Parse session date
                date_str = session.get('date_start', '')
                location = session.get('location', 'Unknown')
                
                if date_str:
                    try:
                        # Parse ISO format date
                        session_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                        # Only include races that have happened
                        if session_date < current_date:
                            past_races.append(session)
                    except Exception as e:
                        logger.warning(f"Could not parse date '{date_str}' for {location}: {e}")
                        continue
            
            if not past_races:
                raise ValueError(f"No completed races found to process for {DAGConfig.YEAR}. Check race schedule.")
            
            # Sort by date and get the most recent
            logger.info(f"Found {len(past_races)} completed races")
            past_races.sort(key=lambda x: x.get('date_start', ''), reverse=True)
            latest_race = past_races[0]
            
            grand_prix_location = latest_race.get('location', 'Unknown')
            race_date = latest_race.get('date_start', 'Unknown')
            
            logger.info(f"Latest Grand Prix selected: {grand_prix_location} ({race_date})")
            
            # Now get both Qualifying and Race sessions for this GP
            all_sessions = []
            
            # Get Qualifying session
            qualifying_sessions = extractor.client.get_sessions(DAGConfig.YEAR, 'Qualifying')
            qualifying_for_gp = [s for s in qualifying_sessions 
                                if s.get('location', '').lower() == grand_prix_location.lower()]
            all_sessions.extend(qualifying_for_gp)
            logger.info(f"Found {len(qualifying_for_gp)} Qualifying session(s)")
            
            # Add the race session we already have
            all_sessions.append(latest_race)
            logger.info(f"Found 1 Race session")
            
            logger.info(f"Total sessions to process: {len(all_sessions)}")
            
            # Process each session
            logger.info(f"Starting Bronze data extraction for {len(all_sessions)} sessions")
            
            for i, session in enumerate(all_sessions, 1):
                session_key = session.get('session_key')
                session_type = session.get('session_type')
                session_date = session.get('date_start', 'Unknown')
                
                logger.info(f"Processing session {i}/{len(all_sessions)}: {session_type}")
                
                try:
                    extractor.extract_session_data(session)
                    logger.info(f"Successfully extracted {session_type} data")
                except Exception as e:
                    logger.error(f"Failed to process session {session_key}: {e}")
                    raise
            
            logger.info(f"Bronze extraction completed successfully for {grand_prix_location} - {len(all_sessions)} sessions processed")
            
            # Return normalized GP name for downstream Glue jobs
            logger.info(f"Normalizing Grand Prix name for downstream tasks")
            
            from src.dags import S3Writer
            writer = S3Writer()
            normalized_gp = writer._normalize_grand_prix_name(grand_prix_location)
            
            logger.info(f"🏷️  Original GP: '{grand_prix_location}'")
            logger.info(f"🏷️  Normalized: '{normalized_gp}'")
            logger.info(f"\n🚀 Bronze extraction task completed! Ready for Bronze-to-Silver transform.")
            
            return normalized_gp
            
        finally:
            extractor.close()

    # Step 1: Detect latest Grand Prix and extract Bronze data
    extract_bronze_task = detect_and_extract_latest_grand_prix()
    
    # Step 2: Transform Bronze to Silver layer
    bronze_to_silver_task = GlueJobOperator(
        task_id='bronze_to_silver_transform',
        job_name=DAGConfig.BRONZE_TO_SILVER_JOB_NAME,
        script_args={
            '--JOB_NAME': f'{DAGConfig.BRONZE_TO_SILVER_JOB_NAME}-incremental-{{ ds }}',
            '--RUN_MODE': 'INCREMENTAL',
            '--YEAR': str(DAGConfig.YEAR),
            '--GRAND_PRIX': '{{ ti.xcom_pull(task_ids="detect_and_extract_latest_grand_prix") }}'  # Use detected GP
        },
        region_name=DAGConfig.AWS_REGION,
        wait_for_completion=True
    )
    
    # Step 3: Transform Silver to Gold layer
    silver_to_gold_task = GlueJobOperator(
        task_id='silver_to_gold_transform',
        job_name=DAGConfig.SILVER_TO_GOLD_JOB_NAME,
        script_args={
            '--JOB_NAME': f'{DAGConfig.SILVER_TO_GOLD_JOB_NAME}-incremental-{{ ds }}',
            '--RUN_MODE': 'INCREMENTAL',
            '--YEAR': str(DAGConfig.YEAR),
            '--GRAND_PRIX': '{{ ti.xcom_pull(task_ids="detect_and_extract_latest_grand_prix") }}'  # Use same detected GP
        },
        region_name=DAGConfig.AWS_REGION,
        wait_for_completion=True
    )
    
    # =========================================================================
    # Pipeline Dependencies - Complete F1 Data Engineering Flow
    # =========================================================================
    # Sequential execution: Auto-detect GP + Extract Bronze -> Transform to Silver -> Transform to Gold
    # Each task passes normalized GP name via XCom for consistency across all layers
    
    extract_bronze_task >> bronze_to_silver_task >> silver_to_gold_task


# Instantiate the DAG
dag = f1_weekly_incremental()
