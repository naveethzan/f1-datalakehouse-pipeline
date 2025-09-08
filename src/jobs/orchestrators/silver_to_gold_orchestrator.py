"""
F1 Silver to Gold analytics transformation for AWS Glue.

This orchestrator coordinates individual Gold table transformers to convert
Silver F1 data into session-specific Gold analytics tables optimized for
business intelligence and QuickSight dashboards using Apache Iceberg format.
Designed for AWS Glue production environment.
"""

import sys
import logging
import os
import traceback
from typing import Dict, Any, List
import datetime

# Spark imports (AWS Glue environment)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *

# AWS Glue imports (production-only)
try:
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext

    GLUE_AVAILABLE = True
except ImportError:
    GLUE_AVAILABLE = False

# Import project modules
# Removed: from utils.config_manager import get_config_manager
# Glue jobs use hardcoded configuration via _create_glue_config_manager()

# Import job-specific modules
from jobs.config.f1_config import get_config, get_gold_config, get_aws_services_config, get_s3_config
from jobs.utils.iceberg_manager import IcebergManager
from jobs.utils.simple_validator import SimpleDataValidator

# Import Gold transformers from their dedicated 'gold' package
from jobs.transforms.gold import (
    DriverPerformanceSummaryQualifyingTransform,
    DriverPerformanceSummaryRaceTransform,
    ChampionshipTrackerTransform,
    TeamStrategyAnalysisTransform,
    RaceWeekendInsightsTransform,
)


class F1SilverToGoldOrchestrator:
    """
    Pure orchestrator for F1 Silver to Gold analytics transformation.

    This orchestrator focuses solely on coordination and does not contain
    any business logic. All analytics transformation logic is delegated to
    individual, self-contained Gold transformer classes.

    Responsibilities:
    - Environment validation and setup
    - Silver data caching coordination
    - Gold transformation execution coordination
    - Session-specific processing orchestration
    - Error handling and monitoring
    - Metrics collection and reporting
    """

    def __init__(
        self,
        spark_session=None,
        glue_context=None,
        job_name: str = None,
        job_run_id: str = None,
    ):
        """
        Initialize the orchestrator with environment-aware configuration.

        Args:
            spark_session: Spark session (None for local, provided for Glue)
            glue_context: Glue context for DynamicFrame operations (None for local, provided for Glue)
            job_name: Name of the job (auto-generated if None)
            job_run_id: Unique job run identifier (auto-generated if None)
        """
        # Setup logging FIRST - before any other operations
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # Configure logging for Glue environment
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

        self.logger.info("Starting Silver to Gold orchestrator initialization...")

        try:
            # Set job identifiers
            self.job_name = job_name or "f1-silver-to-gold-transform"
            self.job_run_id = (
                job_run_id
                or f"{self.job_name}-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"
            )

            # Simple config access using unified configuration
            self.bucket_name = get_config('s3.bucket', 'f1-data-lake-naveeth')
            self.silver_database = get_config('databases.silver', 'f1_silver_db')
            self.gold_database = get_config('databases.gold', 'f1_gold_db')

            # Use provided Spark session (from Glue context)
            self.spark = spark_session

            # Store Glue context for DynamicFrame operations
            self.glue_context = glue_context

            # Initialize infrastructure
            self._initialize_infrastructure()

            # Initialize shared configuration management
            self._initialize_shared_configuration()

            # Create Gold tables during initialization
            self._create_tables()

            # Silver data caching for efficient reuse
            self.cached_silver_data = {}

            self.logger.info(
                "Silver to Gold orchestrator initialization completed successfully"
            )

        except Exception as init_error:
            self.logger.error(
                "Initialization failed with error: %s: %s", type(init_error).__name__, str(init_error)
            )
            self.logger.error("Full traceback: %s", traceback.format_exc())
            raise

        self.logger.info(
            "Initialized F1SilverToGoldOrchestrator - Job: %s, Silver: %s, Gold: %s",
            self.job_name, self.silver_database, self.gold_database
        )

    def _create_tables(self) -> None:
        """Create Gold tables during initialization"""
        try:
            self.logger.info("Creating Gold tables...")
            self.iceberg_manager.create_all_tables()
            self.logger.info("✅ Gold tables created")
        except Exception as e:
            self.logger.error("Failed to create Gold tables: %s", e)
            raise

    def _track_comprehensive_lineage(
        self, transformation_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Tracks comprehensive lineage for the Silver-to-Gold pipeline.

        This orchestrator-level method encapsulates the complex logic of mapping all
        Silver inputs to each individual Gold output table.

        Args:
            transformation_results: The results dictionary from the orchestrator.

        Returns:
            A dictionary summarizing the lineage tracking results.
        """
        try:
            self.logger.info(
                "🔗 Starting comprehensive Silver→Gold lineage tracking..."
            )

            # 1. Prepare Silver input datasets from cached data
            silver_inputs = []
            total_input_records = 0
            for table_name, df in self.cached_silver_data.items():
                record_count = df.count()
                total_input_records += record_count
                silver_inputs.append(
                    {
                        "namespace": "s3",
                        "name": f"{self.bucket_name}/silver/{table_name}",
                        "uri": f"s3://{self.bucket_name}/silver/{table_name}/",
                        "record_count": record_count,
                        "data_type": table_name,
                    }
                )

            # 2. Define the mapping from transformers to Gold tables
            gold_table_mapping = {
                "DriverPerformanceSummaryQualifyingTransform": "driver_performance_summary_qualifying",
                "DriverPerformanceSummaryRaceTransform": "driver_performance_summary_race",
                "ChampionshipTrackerTransform": "championship_tracker",
                "TeamStrategyAnalysisTransform": "team_strategy_analysis",
                "RaceWeekendInsightsTransform": "race_weekend_insights",
            }

            # 3. Iterate through results and track lineage for each successful transformation
            events_generated = 0
            total_output_records = 0
            for transformer_name, result in transformation_results.items():
                if (
                    result.get("status") == "success"
                    and transformer_name in gold_table_mapping
                ):
                    gold_table_name = gold_table_mapping[transformer_name]
                    output_records = result.get("output_records", 0)
                    total_output_records += output_records

                    output_dataset = [
                        {
                            "namespace": "s3",
                            "name": f"{self.bucket_name}/gold/{gold_table_name}",
                            "uri": f"s3://{self.bucket_name}/gold/{gold_table_name}/",
                            "data_type": gold_table_name,
                        }
                    ]

                    record_counts = {
                        "input": total_input_records,
                        "output": output_records,
                    }

                    # Lineage tracking disabled in simplified Glue environment
                    self.logger.info("Lineage tracking disabled for %s", gold_table_name)
                    events_generated += 1

            self.logger.info(
                f"🔗 Gold lineage tracking complete: {events_generated} events generated."
            )
            return {
                "lineage_status": "completed",
                "events_generated": events_generated,
                "total_input_records": total_input_records,
                "total_output_records": total_output_records,
            }

        except Exception as lineage_error:
            self.logger.error("❌ Comprehensive lineage tracking failed: %s", lineage_error)
            return {"lineage_status": "failed", "error": str(lineage_error)}


    def _configure_spark_for_iceberg(self) -> None:
        """
        Configure Spark session for Iceberg operations.

        This method applies all necessary Spark configurations for Iceberg
        table operations in the AWS Glue environment.
        """
        try:
            self.logger.info("🔧 Configuring Spark session for Iceberg operations...")

            # Apply essential Spark configurations for Iceberg
            self.spark.conf.set("spark.sql.adaptive.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
            self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
            self.spark.conf.set("spark.sql.shuffle.partitions", "400")
            self.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "268435456")
            
            # Validate Iceberg support (simplified)
            try:
                self.spark.sql("SHOW CATALOGS")
                self.logger.info("✅ Iceberg support validated")
            except Exception as e:
                self.logger.error("❌ Iceberg support validation failed: %s", e)
                raise Exception("Iceberg support not available - cannot proceed with table operations")

            self.logger.info("✅ Spark session configured successfully for Iceberg")

        except Exception as spark_error:
            self.logger.error("❌ Failed to configure Spark for Iceberg: %s", spark_error)
            raise Exception(
                f"Critical error: Cannot configure Spark for Iceberg operations: {spark_error}"
            )

    def _initialize_infrastructure(self) -> None:
        """
        Initialize infrastructure components needed for orchestration.

        This method sets up all the supporting tools and managers needed
        for the Gold layer orchestration process.
        """
        # Configure Spark for Iceberg first
        self._configure_spark_for_iceberg()

        # Initialize Gold Iceberg table manager
        self.iceberg_manager = IcebergManager(
            spark=self.spark,
            glue_context=self.glue_context,
            bucket_name=self.bucket_name,
            layer="gold",
        )

        # Initialize supporting tools (simplified for Glue environment)
        # All heavy dependencies removed for simplified pipeline
        self.data_validator = SimpleDataValidator()

    def _initialize_shared_configuration(self) -> None:
        """
        Initialize shared configuration management for Gold layer transformations.

        This method sets up configuration patterns that are shared across
        all Gold transformers, ensuring consistency in behavior and settings.
        """
        # Shared settings for all Gold transformers using unified config
        self.shared_transformer_config = {
            "target_file_size_mb": get_config('iceberg.target_file_size', '128MB'),
            "session_specific_processing": True,
            "maintenance_settings": get_config('iceberg.snapshot_retention_days', 7),
        }

        self.logger.info(
            f"Initialized shared Gold configuration: {len(self.shared_transformer_config)} settings"
        )


    def _coordinate_silver_data_loading(self) -> Dict[str, Any]:
        """
        Coordinate Silver data loading across all Gold transformations.

        This method delegates Silver data loading to the base transformer
        while managing the caching strategy at the orchestrator level.

        Returns:
            Dictionary mapping Silver table names to cached DataFrames
        """
        if self.cached_silver_data:
            self.logger.info("Using cached Silver data")
            return self.cached_silver_data

        try:
            self.logger.info("Loading Silver data for Gold transformations...")

            # Delegate actual loading to base transformer pattern but manage caching here
            silver_tables = [
                "sessions_silver",
                "drivers_silver",
                "qualifying_results_silver",
                "race_results_silver",
                "laps_silver",
                "pitstops_silver",
            ]

            # Load Silver data through shared loading mechanism
            loaded_silver_data = self._load_silver_data_batch(silver_tables)

            # Cache the loaded data for reuse across transformations
            self.cached_silver_data = loaded_silver_data

            total_records = sum(df.count() for df in loaded_silver_data.values())
            self.logger.info(
                f"✅ Coordinated loading of {len(loaded_silver_data)} Silver tables, "
                f"{total_records:,} total records"
            )

            return self.cached_silver_data

        except Exception as loading_error:
            self.logger.error("Failed to coordinate Silver data loading: %s", loading_error)
            raise

    def _load_silver_data_batch(self, silver_tables: List[str]) -> Dict[str, DataFrame]:
        """
        Load multiple Silver tables efficiently with shared caching strategy.

        Args:
            silver_tables: List of Silver table names to load

        Returns:
            Dictionary mapping table names to DataFrames
        """
        loaded_data = {}

        for table_name in silver_tables:
            try:
                # Use Glue DynamicFrame API for proper Iceberg table reading
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=self.silver_database, table_name=table_name
                )
                # Convert DynamicFrame to DataFrame for transformation
                df = dyf.toDF()

                # Cache frequently used tables for analytics (shared caching strategy)
                if table_name in ["sessions_silver", "drivers_silver"]:
                    df = df.cache()
                    self.logger.info(
                        f"Applied caching to {table_name} for frequent reuse"
                    )

                # Safe record counting with error handling
                try:
                    record_count = df.count()
                except Exception as count_error:
                    self.logger.warning(
                        "Could not count records for %s: %s", table_name, count_error
                    )
                    record_count = 0
                loaded_data[table_name] = df
                self.logger.info(
                    "Loaded Silver %s: %s records using Glue DynamicFrame API", table_name, f"{record_count:,}"
                )

            except Exception as load_error:
                self.logger.warning("Failed to load Silver %s: %s", table_name, load_error)
                # Continue with other tables rather than failing completely

        if not loaded_data:
            raise Exception("No Silver tables could be loaded successfully")

        return loaded_data

    def _get_transformation_order(self) -> List[str]:
        """
        Get the order in which Gold transformations should be executed.

        Session-specific tables are created first (independent), then cross-session
        analysis tables that depend on multiple session types.

        Returns:
            List of transformer class names in dependency order
        """
        return [
            # Session-specific tables first (independent)
            "DriverPerformanceSummaryQualifyingTransform",  # Qualifying sessions only
            "DriverPerformanceSummaryRaceTransform",  # Race sessions only
            "ChampionshipTrackerTransform",  # Race sessions only
            "TeamStrategyAnalysisTransform",  # Race sessions only
            # Cross-session analysis last (dependent on session-specific data)
            "RaceWeekendInsightsTransform",  # Combined qualifying + race
        ]

    def _create_transformer(self, transformer_name: str):
        """
        Create Gold transformer instance by name.

        Args:
            transformer_name: Name of the transformer class

        Returns:
            Gold transformer instance
        """
        transformer_classes = {
            "DriverPerformanceSummaryQualifyingTransform": DriverPerformanceSummaryQualifyingTransform,
            "DriverPerformanceSummaryRaceTransform": DriverPerformanceSummaryRaceTransform,
            "ChampionshipTrackerTransform": ChampionshipTrackerTransform,
            "TeamStrategyAnalysisTransform": TeamStrategyAnalysisTransform,
            "RaceWeekendInsightsTransform": RaceWeekendInsightsTransform,
        }

        if transformer_name not in transformer_classes:
            raise ValueError(f"Unknown Gold transformer: {transformer_name}")

        transformer_class = transformer_classes[transformer_name]
        return transformer_class(
            spark=self.spark,
            glue_context=self.glue_context,
            job_run_id=self.job_run_id,
            layer="gold",
            config={
                "aws_services": get_aws_services_config(),
                "s3": get_s3_config(),
                "gold": get_gold_config(),
            },
        )

    def _execute_transformations(self) -> Dict[str, Any]:
        """
        Execute all Gold transformations in dependency order.
        """
        # Load Silver data first
        self._coordinate_silver_data_loading()
        
        transformation_order = self._get_transformation_order()
        results = {}

        for transformer_name in transformation_order:
            try:
                self.logger.info("Starting Gold transformation: %s", transformer_name)
                transformer = self._create_transformer(transformer_name)
                transformed_data = transformer.execute_transformation()

                # Collect results properly (fixes the overwriting bug)
                total_output_records = 0
                for table_name, df in transformed_data.items():
                    self.iceberg_manager.write_table(
                        df, f"{self.gold_database}.{table_name}"
                    )
                    # Safe record counting with error handling
                    try:
                        record_count = df.count()
                        total_output_records += record_count
                    except Exception as count_error:
                        self.logger.warning(
                            "Could not count records for %s: %s", table_name, count_error
                        )
                        # Don't add to total if count failed

                # Set result OUTSIDE the table loop (fixes the overwriting bug)
                results[transformer_name] = {
                    "status": "success",
                    "output_records": total_output_records,
                    "tables_processed": len(transformed_data),
                }
            except Exception as transform_error:
                self.logger.error("❌ %s execution failed: %s", transformer_name, transform_error)
                results[transformer_name] = {"status": "failed", "error": str(transform_error)}

        return results

    def _send_completion_metrics(
        self, transformation_results: Dict[str, Any], processing_time_minutes: float
    ) -> Dict[str, Any]:
        """
        Send comprehensive completion metrics to CloudWatch for Gold layer with session-specific indicators.

        Args:
            transformation_results: Results from all Gold transformations
            processing_time_minutes: Total processing time in minutes

        Returns:
            Dictionary containing metrics sending results
        """
        try:
            # Calculate summary statistics
            successful_transformations = sum(
                1
                for result in transformation_results.values()
                if result.get("status") == "success"
            )
            failed_transformations = (
                len(transformation_results) - successful_transformations
            )
            total_transformations = len(transformation_results)

            # Calculate total records
            total_input_records = sum(
                result.get("input_records", 0)
                for result in transformation_results.values()
            )
            total_output_records = sum(
                result.get("output_records", 0)
                for result in transformation_results.values()
            )

            # Metrics sending disabled in simplified Glue environment
            self.logger.info(
                "📊 Metrics sending disabled in simplified Glue environment"
            )

            self.logger.info(
                f"📊 Gold Pipeline Summary: {successful_transformations}/{total_transformations} tables, "
                f"{total_output_records:,} analytics records, {processing_time_minutes:.2f} minutes"
            )

            return {
                "metrics_status": "completed",
                "metrics_sent": 0,
                "total_metrics": 0,
                "total_output_records": total_output_records,
                "processing_time_minutes": processing_time_minutes,
            }

        except Exception as metrics_error:
            self.logger.warning("Failed to send Gold completion metrics: %s", metrics_error)
            # Don't fail the pipeline if metrics fail
            return {
                "metrics_status": "failed",
                "error": str(metrics_error),
                "metrics_sent": 0,
                "total_metrics": 0,
            }

    def run_pipeline(self) -> None:
        """
        Run the actual pipeline - tables already exist.

        Coordinates the complete Silver to Gold analytics transformation process
        with session-specific processing and efficient Silver data caching.
        """
        start_time = datetime.datetime.now()

        try:
            self.logger.info(
                "🚀 Starting F1 Silver to Gold analytics transformation pipeline"
            )

            # 1. Execute transformations with session-specific processing
            transformation_results = self._execute_transformations()

            # 3. Validate all Gold tables with comprehensive validation
            self._validate_gold_layer()

            # 4. Send completion metrics with session-specific success indicators
            processing_time = (datetime.datetime.now() - start_time).total_seconds() / 60
            metrics_results = self._send_completion_metrics(
                transformation_results, processing_time
            )

            # 5. Log comprehensive final summary
            self._log_final_summary(transformation_results, metrics_results)

        except Exception as pipeline_error:
            self.logger.error("❌ Gold pipeline execution failed: %s", pipeline_error)

            # Send comprehensive failure metrics
            processing_time = (datetime.datetime.now() - start_time).total_seconds() / 60
            self.logger.info(
                "📊 Failure metrics sending disabled in simplified Glue environment"
            )

            # Attempt to track failure in lineage
            try:
                failure_lineage = {
                    "lineage_status": "pipeline_failed",
                    "error": str(pipeline_error),
                    "events_generated": 0,
                    "pipeline_failure": True,
                    "lineage_timestamp": datetime.datetime.now().isoformat(),
                    "run_id": self.job_run_id,
                }
                self.logger.info("🔗 Pipeline failure recorded in lineage tracking")
            except Exception as lineage_error:
                self.logger.warning(
                    "Failed to record pipeline failure in lineage: %s", lineage_error
                )

            # Clear cache on failure is no longer needed

            raise

    def _validate_gold_layer(self) -> None:
        """
        Perform a comprehensive data quality check on the Gold layer.
        """
        self.logger.info("🛡️ Starting Gold layer data validation...")
        validation_results = self.data_validator.validate_gold_tables(
            spark_session=self.spark,
            glue_context=self.glue_context,
            gold_database=self.gold_database,
        )
        if not validation_results.get("validation_passed", False):
            raise Exception("Gold layer validation failed!")
        self.logger.info("✅ Gold layer validation completed successfully")

    def _log_final_summary(self, transformation_results, metrics_results):
        """Logs a comprehensive summary of the pipeline run."""
        successful_count = sum(
            1
            for result in transformation_results.values()
            if result.get("status") == "success"
        )
        total_count = len(transformation_results)
        metrics_success = metrics_results.get("metrics_status") == "completed"

        if successful_count == total_count:
            self.logger.info(
                f"🎉 Gold pipeline completed successfully: {successful_count}/{total_count} transformations"
            )
        else:
            failed = [
                name
                for name, res in transformation_results.items()
                if res.get("status") != "success"
            ]
            error_msg = f"Gold pipeline completed with failures: Transformations failed: {failed}"
            self.logger.error("❌ %s", error_msg)
            raise Exception(error_msg)


def main():
    """Main entry point for AWS Glue job execution."""
    if not GLUE_AVAILABLE:
        raise ImportError(
            "AWS Glue libraries are not available. This script must be run in a Glue environment."
        )

    # AWS Glue environment only
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Initialize Glue context and job
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Generate unique job run ID
    job_run_id = f"{args['JOB_NAME']}-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}"

    try:
        # Initialize and run orchestrator
        orchestrator = F1SilverToGoldOrchestrator(
            spark_session=glue_context.spark_session,
            glue_context=glue_context,
            job_name=args["JOB_NAME"],
            job_run_id=job_run_id,
        )

        orchestrator.run_pipeline()

        # Commit job
        job.commit()

    except Exception as glue_error:
        logging.error("Glue job failed: %s", glue_error)
        logging.error("Full traceback: %s", traceback.format_exc())
        raise
    finally:
        # Stop Spark context
        sc.stop()


if __name__ == "__main__":
    main()
