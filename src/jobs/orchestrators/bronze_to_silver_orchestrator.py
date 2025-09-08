"""
F1 Bronze to Silver data transformation for AWS Glue.

This orchestrator coordinates individual table transformers to convert Bronze F1 data
into clean, normalized Silver entity tables using Apache Iceberg format.

"""

from datetime import datetime
import logging
import sys
from typing import Any, Dict, List

from pyspark.context import SparkContext

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from jobs.config.f1_config import get_config, get_silver_config, get_aws_services_config, get_s3_config
from jobs.transforms.silver import (
    DriversTransform,
    LapsTransform,
    PitstopsTransform,
    QualifyingResultsTransform,
    RaceResultsTransform,
    SessionsTransform,
)
from jobs.utils.iceberg_manager import IcebergManager
from jobs.utils.simple_validator import SimpleDataValidator


class F1BronzeToSilverOrchestrator:
    """
    Pure orchestrator for F1 Bronze to Silver data transformation.

    This orchestrator focuses solely on coordination and does not contain
    any business logic. All data transformation logic is delegated to
    individual, self-contained transformer classes.

    """

    def __init__(
        self, spark_session, glue_context, job_name: str = None, job_run_id: str = None
    ):
        """
        Initialize the orchestrator for AWS Glue environment.

        Args:
            spark_session: Spark session from Glue context
            glue_context: Glue context for DynamicFrame operations
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

        self.logger.info("Starting Bronze to Silver orchestrator initialization...")

        try:
            # Set job identifiers
            self.job_name = job_name or "f1-bronze-to-silver-transform"
            self.job_run_id = (
                job_run_id
                or f"{self.job_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            )

            # Simple config access using unified configuration
            self.bronze_bucket = get_config('s3.bucket', 'f1-data-lake-naveeth')
            self.silver_database = get_config('databases.silver', 'f1_silver_db')

            # Use provided Spark session (from Glue context)
            self.spark = spark_session

            # Store Glue context for DynamicFrame operations
            self.glue_context = glue_context

            # Initialize supporting tools
            self._initialize_infrastructure()

            # Create Silver tables during initialization
            self._create_tables()

            self.logger.info(
                "Bronze to Silver orchestrator initialization completed successfully"
            )

        except Exception as e:
            self.logger.error(
                f"Initialization failed with error: {type(e).__name__}: {str(e)}"
            )
            import traceback

            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            raise

        self.logger.info(
            f"Initialized F1BronzeToSilverOrchestrator - Job: {self.job_name}, Bucket: {self.bronze_bucket}"
        )

    def _create_tables(self) -> None:
        """Create Silver tables during initialization"""
        try:
            self.logger.info("Creating Silver tables...")
            self.iceberg_manager.create_all_tables()
            self.logger.info("✅ Silver tables created")
        except Exception as e:
            self.logger.error("Failed to create Silver tables: %s", e)
            raise

    # Spark session initialization removed - using provided Glue session


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
            self.spark.conf.set("spark.sql.shuffle.partitions", "200")
            self.spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")
            
            # Validate Iceberg support (simplified)
            try:
                self.spark.sql("SHOW CATALOGS")
                self.logger.info("✅ Iceberg support validated")
            except Exception as e:
                self.logger.error("❌ Iceberg support validation failed: %s", e)
                raise Exception("Iceberg support not available - cannot proceed with table operations")

            self.logger.info("✅ Spark session configured successfully for Iceberg")

        except Exception as e:
            self.logger.error(f"❌ Failed to configure Spark for Iceberg: {e}")
            raise Exception(
                f"Critical error: Cannot configure Spark for Iceberg operations: {e}"
            )

    def _initialize_infrastructure(self) -> None:
        """
        Initialize infrastructure components needed for orchestration.

        This method sets up all the supporting tools and managers needed
        for the orchestration process.
        """
        # Configure Spark for Iceberg first
        self._configure_spark_for_iceberg()

        # Initialize Iceberg table manager
        self.iceberg_manager = IcebergManager(
            spark=self.spark,
            glue_context=self.glue_context,
            bucket_name=self.bronze_bucket,
            layer="silver",
        )

        # Initialize supporting tools (simplified for Glue environment)
        # All heavy dependencies removed for simplified pipeline
        self.data_validator = SimpleDataValidator()

    # Environment validation removed - AWS Glue environment assumed


    def _get_transformation_order(self) -> List[str]:
        """
        Define the order in which transformations should be executed.

        This method encapsulates the dependency logic between transformers,
        ensuring that independent tables are processed first, followed by
        dependent tables in the correct order.

        Returns:
            List of transformer class names in dependency order
        """
        return [
            "SessionsTransform",  # Independent - session metadata
            "DriversTransform",  # Independent - driver information
            "QualifyingResultsTransform",  # Depends on Sessions + Drivers
            "RaceResultsTransform",  # Depends on Sessions + Drivers
            "LapsTransform",  # Depends on Sessions + Drivers (largest dataset)
            "PitstopsTransform",  # Depends on Sessions + Drivers
        ]

    def _create_transformer(self, transformer_name: str):
        """
        Factory method to create transformer instances.

        This method encapsulates the transformer creation logic and ensures
        consistent initialization parameters across all transformers.

        Args:
            transformer_name: Name of the transformer class

        Returns:
            Configured transformer instance
        """
        transformer_classes = {
            "SessionsTransform": SessionsTransform,
            "DriversTransform": DriversTransform,
            "QualifyingResultsTransform": QualifyingResultsTransform,
            "RaceResultsTransform": RaceResultsTransform,
            "LapsTransform": LapsTransform,
            "PitstopsTransform": PitstopsTransform,
        }

        if transformer_name not in transformer_classes:
            raise ValueError(f"Unknown transformer: {transformer_name}")

        transformer_class = transformer_classes[transformer_name]
        # Create a combined config dictionary for transformer
        transformer_config = {
            "aws_services": get_aws_services_config(),
            "s3": get_s3_config(),
            "silver": get_silver_config(),
        }
        return transformer_class(
            spark=self.spark,
            glue_context=self.glue_context,
            job_run_id=self.job_run_id,
            layer="silver",
            config=transformer_config,
        )

    def _execute_transformations(self) -> Dict[str, Any]:
        """
        Execute all transformations in dependency order.

        This method is the core orchestration logic that coordinates the
        execution of all transformers while handling errors gracefully
        and collecting comprehensive results.

        Returns:
            Dictionary with transformation results for each transformer
        """
        transformation_order = self._get_transformation_order()
        results = {}

        self.logger.info(
            f"🚀 Executing {len(transformation_order)} transformations in dependency order..."
        )

        for transformer_name in transformation_order:
            try:
                self.logger.info(f"🔄 Starting transformation: {transformer_name}")

                transformer = self._create_transformer(transformer_name)
                transformed_data = transformer.execute_transformation()

                # Immediately write the transformed data and collect results
                total_output_records = 0
                for table_name, df in transformed_data.items():
                    self.iceberg_manager.write_table(
                        df, f"{self.silver_database}.{table_name}"
                    )
                    # Safe record counting with error handling
                    try:
                        record_count = df.count()
                        total_output_records += record_count
                    except Exception as count_error:
                        self.logger.warning(
                            f"Could not count records for {table_name}: {count_error}"
                        )
                        # Don't add to total if count failed

                # Set result OUTSIDE the table loop (fixes the overwriting bug)
                results[transformer_name] = {
                    "status": "success",
                    "output_records": total_output_records,
                    "tables_processed": len(transformed_data),
                }
                self.logger.info(
                    f"✅ Completed {transformer_name}: {total_output_records:,} records"
                )

            except Exception as e:
                self.logger.error(f"❌ {transformer_name} execution failed: {e}")
                self.logger.error(f"Error type: {type(e).__name__}")
                results[transformer_name] = {
                    "status": "failed",
                    "error": str(e),
                    "error_type": type(e).__name__,
                }

        return results

    def _send_completion_metrics(
        self, transformation_results: Dict[str, Any], processing_time_minutes: float
    ) -> None:
        """
        Send completion metrics to CloudWatch.

        Args:
            transformation_results: Results from all transformations
            processing_time_minutes: Total processing time in minutes
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
                f"📊 Pipeline Summary: {successful_transformations}/{total_transformations} tables, "
                f"{total_output_records:,} records, {processing_time_minutes:.2f} minutes"
            )

        except Exception as e:
            self.logger.warning(f"Failed to send completion metrics: {e}")
            # Don't fail the pipeline if metrics fail

    def run_pipeline(self) -> None:
        """
        Main orchestration pipeline execution.

        This method coordinates the complete Bronze to Silver transformation
        process with comprehensive error handling and monitoring.
        """
        start_time = datetime.now()

        try:
            self.logger.info("🚀 Starting F1 Bronze to Silver transformation pipeline")

            # Phase 1: Transformation Execution (tables already created in __init__)
            transformation_results = self._execute_transformations()

            # Phase 3: Post-Transformation Validation - SKIPPED
            self.logger.info(
                "✅ Skipping Silver layer data validation - proceeding to pipeline success validation"
            )

            # Phase 4: Results Analysis and Metrics
            processing_time = (datetime.now() - start_time).total_seconds() / 60
            self._send_completion_metrics(transformation_results, processing_time)

            # Phase 5: Final Validation and Commit
            self._validate_pipeline_success(transformation_results)

            self.logger.info(
                f"🎉 Pipeline completed successfully in {processing_time:.2f} minutes"
            )

        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds() / 60
            self.logger.error(
                f"❌ Pipeline execution failed after {processing_time:.2f} minutes: {e}"
            )
            self.logger.error(f"Error type: {type(e).__name__}")
            self.logger.error(f"Error details: {str(e)}")
            import traceback

            self.logger.error(f"Full traceback: {traceback.format_exc()}")
            self.logger.info(
                "📊 Failure metrics sending disabled in simplified Glue environment"
            )
            raise

    def _validate_silver_layer(self) -> None:
        """
        Perform a comprehensive data quality check on the Silver layer.
        """
        try:
            self.logger.info("🛡️ Starting Silver layer data validation...")
            validation_results = self.data_validator.validate_silver_tables(
                spark_session=self.spark,
                glue_context=self.glue_context,
                silver_database=self.silver_database,
            )
            if not validation_results.get("validation_passed", False):
                self.logger.error(
                    f"❌ Silver layer validation failed: {validation_results}"
                )
                raise Exception("Silver layer validation failed!")
            self.logger.info("✅ Silver layer validation completed successfully")
        except Exception as e:
            self.logger.error(f"❌ Silver layer validation error: {e}")
            self.logger.error(f"Error type: {type(e).__name__}")
            raise

    def _validate_pipeline_success(
        self, transformation_results: Dict[str, Any]
    ) -> None:
        """
        Validate that the pipeline completed successfully.

        Args:
            transformation_results: Results from all transformations

        Raises:
            Exception: If any transformations failed
        """
        successful_count = sum(
            1
            for result in transformation_results.values()
            if result.get("status") == "success"
        )
        total_count = len(transformation_results)

        if successful_count == total_count:
            total_records = sum(
                result.get("output_records", 0)
                for result in transformation_results.values()
            )
            self.logger.info(
                f"✅ All {total_count} transformations successful: {total_records:,} total records"
            )
        else:
            failed_transformations = [
                name
                for name, result in transformation_results.items()
                if result.get("status") != "success"
            ]
            error_msg = f"Transformations failed: {failed_transformations}"
            self.logger.error(f"❌ {error_msg}")
            raise Exception(error_msg)


def main():
    """Main entry point for AWS Glue job."""

    # AWS Glue environment
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])

    # Initialize Glue context and job
    sc = SparkContext()
    glue_context = GlueContext(sc)
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Generate unique job run ID
    job_run_id = f"{args['JOB_NAME']}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    try:
        # Initialize and run orchestrator
        orchestrator = F1BronzeToSilverOrchestrator(
            spark_session=glue_context.spark_session,
            glue_context=glue_context,
            job_name=args["JOB_NAME"],
            job_run_id=job_run_id,
        )

        orchestrator.run_pipeline()

        # Commit job
        job.commit()

    except Exception as e:
        logging.error(f"Glue job failed: {e}")
        import traceback

        logging.error(f"Full traceback: {traceback.format_exc()}")
        raise
    finally:
        # Stop Spark context
        sc.stop()


if __name__ == "__main__":
    main()
