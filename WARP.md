# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Development Commands

### Local Development Setup
```bash
# Start the complete development environment (Airflow, Spark, PostgreSQL)
make dev-start

# Build Docker images for development
make dev-build

# View logs from all services
make dev-logs

# Stop services (keeps containers)
make dev-stop

# Shut down and remove all containers
make dev-down

# Restart the entire environment
make dev-restart

# Clean up all Docker resources
make clean
```

### Testing Commands
```bash
# Test AWS Glue Data Catalog and Iceberg integration
make test-iceberg

# Execute Spark jobs directly in development environment
docker-compose exec spark-master python /opt/spark/jobs/your_job.py

# Run Python tests (when test suite is available)
python -m pytest tests/
```

### Build and Deployment
```bash
# Create wheel package for AWS Glue deployment
python setup.py bdist_wheel

# Deploy infrastructure to AWS
scripts/aws_infrastructure_setup.sh

# Deploy code to AWS
scripts/deploy_code_to_aws.sh

# Clean up CloudWatch resources
scripts/cleanup_cloudwatch_resources.sh
```

## Architecture Overview

### Data Pipeline Architecture
This is a **medallion architecture** data pipeline processing Formula 1 data through three layers:

- **Bronze Layer**: Raw data ingestion from OpenF1 API → S3 (Parquet format)
- **Silver Layer**: Cleaned and normalized data using Apache Iceberg tables
- **Gold Layer**: Business-ready aggregated analytics data

### Technology Stack
- **Orchestration**: Apache Airflow (local) / AWS MWAA (production)
- **Processing**: Apache Spark (local) / AWS Glue (production)  
- **Storage**: Apache Iceberg tables on S3
- **Data Catalog**: AWS Glue Data Catalog
- **Query Engine**: AWS Athena

### Key Components

#### DAG Factory Pattern
- `src/dags/dag_factory.py` contains reusable DAG generation logic
- Uses factory pattern to create different pipeline types (end-to-end, backfill)
- Eliminates code duplication across multiple DAGs

#### Data Processing Jobs Structure
- `src/jobs/orchestrators/` - High-level orchestration logic for each layer
- `src/jobs/transforms/` - Data transformation implementations
- `src/jobs/utils/` - Specialized utilities (Iceberg management, business logic)

#### OpenF1 API Integration
- `src/providers/openf1_hook.py` - Custom Airflow hook with rate limiting and retry logic
- Fetches session data for Qualifying and Race sessions only
- Implements proper error handling and connection management

#### Configuration Management
- Environment-specific configs in `config/dev.yaml` and `config/prod.yaml`
- Supports both local development and AWS production environments
- Hardcoded configurations in DAG factory eliminate external dependencies in MWAA

### Development Environment
The local environment runs in Docker Compose with:
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark Master UI**: http://localhost:8081
- **Spark Worker UI**: http://localhost:8082
- **PostgreSQL**: localhost:5433 (Airflow metadata)

### Data Partitioning Strategy
All data is partitioned by:
- Year
- Grand Prix
- Session Type (Race/Qualifying)

### AWS Production Environment
- **Data Storage**: S3 bucket `f1-data-lake-naveeth`
- **Processing**: AWS Glue jobs with G.1X and G.2X workers
- **Orchestration**: AWS MWAA with LocalExecutor
- **Monitoring**: CloudWatch metrics and logs

## Important Notes

### Development vs Production
- Local development uses actual AWS S3 (requires AWS CLI configured)
- Different requirements files for different environments:
  - `requirements.txt` - Local development (full dependencies)
  - `requirements-glue.txt` - AWS Glue (minimal/zero external deps)
  - `requirements-mwaa.txt` - AWS MWAA (Airflow + requests only)

### Data Scope
- **Session Types**: Only Qualifying and Race sessions
- **Target Year**: Currently set to 2025 in configuration
- **Endpoints**: Universal (drivers, laps, session_result) + Race-specific (pit, stints)

### AWS Service Dependencies
- Requires pre-configured AWS infrastructure (S3, Glue Catalog, IAM roles)
- Uses `F1-DataPipeline-Role` IAM role for AWS service access
- Glue Data Catalog database: `f1_data_catalog`
