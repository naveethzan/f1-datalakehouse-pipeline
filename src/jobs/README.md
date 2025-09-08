# F1 Bronze to Silver Transformation Jobs

This directory contains AWS Glue jobs for transforming F1 data from Bronze to Silver layer using Apache Iceberg tables.

## Overview

The Bronze to Silver transformation pipeline processes raw JSON-like F1 data into clean, normalized entity tables optimized for analytical queries. The system uses AWS Glue Spark jobs with Apache Iceberg format for optimal performance and schema evolution.

## Job Structure

### Optimized Architecture

The job follows a **modular, separation-of-concerns architecture** for maintainability and testability:

```
jobs/
├── f1_bronze_to_silver_transform.py          # Slim orchestrator
├── config/                                   # Configuration modules
│   ├── spark_config.py                       # Spark optimization settings
│   ├── iceberg_config.py                     # Iceberg table configurations
│   └── table_schemas.py                      # Silver table schema definitions
├── transformers/                             # Individual table transformers
│   ├── base_transformer.py                   # Abstract base class
│   ├── sessions_transformer.py               # Sessions transformation logic
│   ├── drivers_transformer.py                # Drivers transformation logic
│   ├── qualifying_transformer.py             # Qualifying results logic
│   ├── race_results_transformer.py           # Race results logic
│   ├── laps_transformer.py                   # Laps transformation logic
│   └── pitstops_transformer.py               # Pitstops transformation logic
├── utils/                                    # Job-specific utilities
│   ├── iceberg_manager.py                    # Iceberg table management
│   └── spark_optimizer.py                    # Spark optimization utilities
└── tests/                                    # Comprehensive test suite
    ├── test_transformers.py                  # Unit tests for transformers
    └── fixtures/                             # Test data fixtures
```

### Main Orchestrator: `f1_bronze_to_silver_transform.py`

**Purpose**: Slim orchestrator that coordinates individual table transformers

**Key Features**:
- **Dependency Management**: Executes transformers in proper order (Sessions/Drivers → Others)
- **Error Handling**: Graceful failure handling with detailed logging
- **Metrics Integration**: Comprehensive CloudWatch metrics for monitoring
- **Configuration Management**: Centralized configuration loading and validation

**Configuration**:
- **Glue Version**: 4.0
- **Worker Type**: G.1X (4 vCPU, 16GB RAM)
- **Number of Workers**: 10 (configurable)
- **Timeout**: 60 minutes
- **Max Retries**: 1

### Individual Transformers

Each Silver table has its own dedicated transformer extending `BaseTransformer`:

- **SessionsTransformer**: Session metadata with Grand Prix normalization
- **DriversTransformer**: Driver deduplication with team standardization
- **QualifyingTransformer**: Q1/Q2/Q3 analysis with gap calculations
- **RaceResultsTransformer**: Race results with points validation
- **LapsTransformer**: Lap-by-lap analysis with Window functions (~40k records)
- **PitstopsTransformer**: Pit stop strategy analysis

## Spark Optimizations

The job includes comprehensive Spark optimizations for F1 data processing:

### Iceberg Configuration (Glue 4.0 Native Support)
```python
# Glue 4.0 handles Iceberg configuration automatically via:
# --datalake-formats iceberg
# --enable-glue-datacatalog
# No manual spark.sql.extensions or spark.sql.catalog configurations needed
```

### Performance Optimizations
```python
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
spark.serializer = org.apache.spark.serializer.KryoSerializer
```

### Memory Management
```python
spark.sql.adaptive.advisoryPartitionSizeInBytes = 134217728  # 128MB
spark.sql.execution.arrow.pyspark.enabled = true
spark.sql.files.maxPartitionBytes = 134217728  # 128MB
```

## Silver Tables Schema

### 1. sessions_silver
- **Partitioning**: year, grand_prix_name
- **Target File Size**: 128MB
- **Key Fields**: session_key, session_type, date_start, grand_prix_name

### 2. drivers_silver
- **Partitioning**: None (small table)
- **Target File Size**: 64MB
- **Key Fields**: driver_number, team_name, nationality

### 3. qualifying_results_silver
- **Partitioning**: year, grand_prix_name
- **Target File Size**: 128MB
- **Key Fields**: session_key, driver_number, q1/q2/q3_time_millis, gap_to_pole_millis

### 4. race_results_silver
- **Partitioning**: year, grand_prix_name
- **Target File Size**: 128MB
- **Key Fields**: session_key, driver_number, position, points, positions_gained

### 5. laps_silver (Largest Dataset ~40,000 records)
- **Partitioning**: year, grand_prix_name
- **Target File Size**: 256MB
- **Key Fields**: session_key, driver_number, lap_number, position_at_lap, gap_to_leader_millis

### 6. pitstops_silver
- **Partitioning**: year, grand_prix_name
- **Target File Size**: 128MB
- **Key Fields**: session_key, driver_number, pit_duration, undercut_attempt

## Deployment

### Prerequisites

1. **AWS IAM Role**: Create Glue service role with permissions:
   - S3 read/write access to data lake bucket
   - Glue Catalog access
   - CloudWatch metrics write access

2. **S3 Bucket Structure**:
   ```
   s3://your-bucket/
   ├── bronze/          # Input data
   ├── silver/          # Output Iceberg tables
   ├── scripts/         # Glue job scripts
   ├── temp/           # Temporary files
   └── spark-logs/     # Spark event logs
   ```

### Deploy Glue Job

```bash
# Deploy the Glue job
make deploy-glue-job

# Validate deployment
make validate-glue-job

# Test Spark configuration locally
make test-spark-config
```

### Manual Deployment

```bash
# Upload job script and deploy
python scripts/deploy_glue_job.py --config config/dev.yaml

# Validate deployment
python scripts/deploy_glue_job.py --config config/dev.yaml --validate-only
```

## Configuration

### Environment Variables

Key environment variables for the job:

```bash
# AWS Configuration
AWS_REGION=us-east-1
AWS_PROFILE=default

# S3 Configuration
S3_BUCKET_NAME=f1-data-lake-dev-yourname

# Glue Job Configuration
GLUE_JOB_NAME=f1-bronze-to-silver-transform
GLUE_WORKER_TYPE=G.1X
GLUE_NUMBER_OF_WORKERS=10
```

### Configuration File

The job uses `config/dev.yaml` for configuration:

```yaml
silver:
  database_name: "f1_silver_db"
  target_file_size_mb: 128
  large_table_file_size_mb: 256

glue:
  job_name: "f1-bronze-to-silver-transform"
  glue_version: "4.0"
  worker_type: "G.1X"
  number_of_workers: 10
  timeout_minutes: 60
```

## Monitoring

### CloudWatch Metrics

The job sends the following metrics to CloudWatch:

- `TablesProcessed`: Number of Silver tables created (6)
- `RecordsTransformed`: Total records processed
- `ProcessingTimeMinutes`: Job execution time
- `TransformationSuccess`: Success/failure indicator (1/0)
- `LapsProcessed`: Number of lap records processed (~40,000)
- `DriversDeduplicatedTo`: Final driver count after deduplication (~25)

### Logging

Comprehensive logging is enabled:
- Glue job logs in CloudWatch Logs
- Spark event logs in S3
- Application logs with INFO level

## Testing

### Local Testing

```bash
# Test Spark configuration
python scripts/test_spark_config.py --test-type config

# Test business rules
python scripts/test_spark_config.py --test-type business-rules

# Test all components
python scripts/test_spark_config.py --test-type all
```

### Integration Testing

The job includes built-in validation:
- Schema validation for Bronze data
- Data quality checks with Great Expectations
- Row count validation
- Business rule validation (F1 points, lap times, etc.)

## Troubleshooting

### Common Issues

1. **Out of Memory Errors**:
   - Increase worker count or use larger worker type (G.2X)
   - Adjust `spark.sql.adaptive.advisoryPartitionSizeInBytes`

2. **Slow Performance**:
   - Check data skew in partitions
   - Verify Spark adaptive query execution is enabled
   - Review file sizes and partition strategy

3. **Iceberg Table Issues**:
   - Verify Glue Catalog permissions
   - Check S3 bucket permissions
   - Validate Iceberg extensions configuration

### Debug Commands

```bash
# Check Glue job status
aws glue get-job --job-name f1-bronze-to-silver-transform

# View job runs
aws glue get-job-runs --job-name f1-bronze-to-silver-transform

# Check CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix /aws-glue/jobs
```

## Performance Expectations

- **Processing Time**: < 15 minutes for full dataset
- **Data Volume**: ~40,000 lap records, ~500 other records
- **Storage Reduction**: 30-50% compared to Bronze layer
- **File Sizes**: 128-256MB per file for optimal query performance

## Dependencies

### Python Packages
- `simple-validation`: Lightweight data quality validation using Spark operations only
- `simple-logging`: Basic logging for lineage visibility (replaces openlineage)
- `pyyaml>=6.0`: Configuration management
- `boto3>=1.26.0`: AWS SDK

### AWS Services
- AWS Glue 4.0
- Amazon S3
- AWS Glue Data Catalog
- Amazon CloudWatch
- AWS IAM

## Next Steps

After successful deployment:

1. **Schedule the Job**: Set up Airflow DAG or EventBridge schedule
2. **Monitor Performance**: Set up CloudWatch alarms
3. **Data Quality**: Configure Great Expectations checkpoints
4. **Cost Optimization**: Monitor and adjust worker configuration
5. **Schema Evolution**: Plan for new F1 data fields