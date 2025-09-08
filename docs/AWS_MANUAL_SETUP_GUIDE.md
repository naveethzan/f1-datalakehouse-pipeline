# F1 Data Engineering Pipeline - AWS Manual Setup Guide

This guide provides step-by-step instructions for manually setting up the F1 Data Engineering Pipeline in AWS Console UI. This approach helps you understand each component and learn how AWS services work together.

## 📋 Prerequisites

Before starting, ensure you have:
- AWS Account with appropriate permissions
- S3 bucket `f1-data-lake-naveeth` already created
- AWS CLI configured (for code uploads)
- Basic understanding of AWS services

## 🎯 What We'll Build

- **AWS Glue Jobs**: Bronze→Silver and Silver→Gold transformations
- **MWAA Environment**: Apache Airflow for orchestration
- **Glue Data Catalog**: 3 databases for data organization
- **S3 Bucket Structure**: Organized data layers
- **CloudWatch**: Monitoring, dashboards, and alarms
- **IAM Roles**: Proper permissions for all services
- **OpenLineage**: Data lineage tracking with S3 storage
- **Great Expectations**: 50+ validation rules for F1 data
- **Apache Iceberg**: ACID-compliant tables with Glue Catalog
- **Table Schemas**: 6 Silver tables + 5 Gold analytics tables
- **Data Validation**: Business rule validations for all data types
- **Logging**: Comprehensive logging across all components
- **Metrics**: Transformation metrics, execution time, data quality
- **Alerts**: Job failures, execution time, validation failures

---

## Phase 1: AWS Infrastructure Setup

### 1.1 Create Glue Databases

**Purpose**: Glue Data Catalog databases organize your tables and metadata.

#### Step 1: Navigate to Glue Console
1. Go to [AWS Glue Console](https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=databases)
2. Click **"Add database"** button

#### Step 2: Create Main Database
1. **Database name**: `f1_data_lake`
2. **Description**: `F1 Data Lake Database - Main catalog for F1 data`
3. Click **"Create database"**

#### Step 3: Create Silver Database
1. Click **"Add database"** again
2. **Database name**: `f1_silver_db`
3. **Description**: `F1 Silver Layer Database - Cleaned and validated data`
4. Click **"Create database"**

#### Step 4: Create Gold Database
1. Click **"Add database"** again
2. **Database name**: `f1_gold_db`
3. **Description**: `F1 Gold Layer Database - Business-ready analytics data`
4. Click **"Create database"**

**✅ Verification**: You should see 3 databases in your Glue Data Catalog.

### 1.2 Create Great Expectations Validation Rules

**Purpose**: Great Expectations provides comprehensive data validation with 50+ business rules for F1 data.

#### Step 1: Understand Validation Rules
Your codebase implements these validation categories:

**Universal Validations (All Data Types)**:
- Table row count validation
- Column structure validation
- Session key null checks

**Drivers Data Validations**:
- Driver number range (1-99)
- Name acronym format (3-letter codes)
- Full name null checks

**Results Data Validations**:
- Position range (1-20)
- Points range (0-26)
- Data type validations

**Laps Data Validations**:
- Lap time format validation
- Lap number range (1-100)
- Time data type checks

**Stints Data Validations**:
- Compound type validation
- Stint number validation

#### Step 2: Validation Storage
- **S3 Location**: `s3://f1-data-lake-naveeth/validation-reports/`
- **Format**: JSON reports with validation results
- **Retention**: 30 days for historical analysis

### 1.3 Create OpenLineage Configuration

**Purpose**: OpenLineage tracks complete data lineage from OpenF1 API to Gold analytics tables.

#### Step 1: Understand Lineage Structure
Your codebase implements:

**Event Types**:
- `COMPLETE` events for successful transformations
- `FAILED` events for error tracking
- `START` events for pipeline initiation

**Event Storage**:
- **Events**: `s3://f1-data-lake-naveeth/lineage/events/`
- **Summaries**: `s3://f1-data-lake-naveeth/lineage/summaries/`
- **Partitioning**: Date-based partitioning for efficient querying

**Event Schema** (OpenLineage 1.0.5):
```json
{
  "eventType": "COMPLETE",
  "eventTime": "2025-01-15T10:30:00Z",
  "run": {
    "runId": "f1_bronze_pipeline_20250115",
    "facets": {
      "nominalTime": {"nominalStartTime": "2025-01-15T06:00:00Z"},
      "processing": {
        "dataType": "drivers",
        "recordCount": 25,
        "processingTime": "2025-01-15T10:30:00Z"
      }
    }
  },
  "job": {
    "namespace": "f1-pipeline",
    "name": "bronze_extraction"
  },
  "inputs": [{
    "namespace": "openf1-api",
    "name": "drivers_endpoint",
    "facets": {
      "dataSource": {"uri": "https://api.openf1.org/v1/drivers"}
    }
  }],
  "outputs": [{
    "namespace": "s3://f1-data-lake-naveeth",
    "name": "bronze/drivers/",
    "facets": {
      "dataSource": {"uri": "s3://f1-data-lake-naveeth/bronze/drivers/"},
      "stats": {"rowCount": 25, "size": 12345}
    }
  }]
}
```

---

### 1.2 Create IAM Roles

**Purpose**: IAM roles provide secure access to AWS services without hardcoding credentials.

#### Create Glue Job Role

##### Step 1: Navigate to IAM Console
1. Go to [IAM Console](https://console.aws.amazon.com/iam/home#/roles)
2. Click **"Create role"** button

##### Step 2: Select Trusted Entity
1. Choose **"AWS service"**
2. Select **"Glue"** from the service list
3. Click **"Next"**

##### Step 3: Attach Policies
Attach these AWS managed policies:
- ✅ `AWSGlueServiceRole` - Basic Glue permissions
- ✅ `AmazonS3FullAccess` - S3 read/write access
- ✅ `CloudWatchLogsFullAccess` - Logging permissions
- ✅ `CloudWatchFullAccess` - Metrics and monitoring
- ✅ `AmazonGlueFullAccess` - Full Glue Catalog access

Click **"Next"**

##### Step 4: Role Details
1. **Role name**: `F1GlueJobRole`
2. **Description**: `IAM role for F1 Glue jobs with S3, CloudWatch, and Glue permissions`
3. Click **"Create role"**

#### Create MWAA Role

##### Step 1: Create New Role
1. Click **"Create role"** again
2. Choose **"AWS service"**
3. Select **"Amazon MWAA"** (Managed Workflows for Apache Airflow)
4. Click **"Next"**

##### Step 2: Attach Policies
Attach these AWS managed policies:
- ✅ `AmazonS3FullAccess` - For DAG storage and data access
- ✅ `CloudWatchLogsFullAccess` - For Airflow logs
- ✅ `AmazonGlueFullAccess` - For triggering Glue jobs
- ✅ `CloudWatchFullAccess` - For metrics and monitoring

Click **"Next"**

##### Step 3: Role Details
1. **Role name**: `F1MWAARole`
2. **Description**: `IAM role for F1 MWAA environment with S3, Glue, and CloudWatch permissions`
3. Click **"Create role"**

**✅ Verification**: You should see both roles in your IAM console.

### 1.4 Create Table Schemas

**Purpose**: Your codebase implements comprehensive table schemas for Silver and Gold layers with proper data types and business logic.

#### Silver Layer Tables (6 Tables)

**1. Sessions Silver Table**
```sql
CREATE TABLE f1_silver_db.sessions_silver (
    session_key BIGINT NOT NULL,
    session_name STRING NOT NULL,
    session_type STRING NOT NULL,
    meeting_key BIGINT NOT NULL,
    meeting_name STRING NOT NULL,
    location STRING NOT NULL,
    country_name STRING NOT NULL,
    date_start TIMESTAMP NOT NULL,
    date_end TIMESTAMP,
    year INT NOT NULL,
    grand_prix_name STRING NOT NULL,
    session_duration_minutes INT,
    is_sprint_weekend BOOLEAN NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year, grand_prix_name)
```

**2. Drivers Silver Table**
```sql
CREATE TABLE f1_silver_db.drivers_silver (
    driver_number INT NOT NULL,
    broadcast_name STRING NOT NULL,
    full_name STRING NOT NULL,
    team_name STRING NOT NULL,
    nationality STRING NOT NULL,
    team_colour STRING NOT NULL,
    name_acronym STRING NOT NULL,
    total_races INT,
    wins INT,
    podiums INT,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
)
```

**3. Qualifying Results Silver Table**
```sql
CREATE TABLE f1_silver_db.qualifying_results_silver (
    session_key BIGINT NOT NULL,
    driver_number INT NOT NULL,
    position INT,
    q1_time_millis INT,
    q2_time_millis INT,
    q3_time_millis INT,
    qualifying_time_millis INT,
    gap_to_pole_millis INT,
    qualifying_status STRING NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year, grand_prix_name)
```

**4. Race Results Silver Table**
```sql
CREATE TABLE f1_silver_db.race_results_silver (
    session_key BIGINT NOT NULL,
    driver_number INT NOT NULL,
    position INT,
    points INT NOT NULL,
    grid_position INT,
    positions_gained INT,
    fastest_lap BOOLEAN NOT NULL,
    fastest_lap_time_millis INT,
    total_laps INT,
    race_time_millis INT,
    status STRING NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year, grand_prix_name)
```

**5. Laps Silver Table**
```sql
CREATE TABLE f1_silver_db.laps_silver (
    session_key BIGINT NOT NULL,
    driver_number INT NOT NULL,
    lap_number INT NOT NULL,
    lap_time_millis INT,
    sector_1_time_millis INT,
    sector_2_time_millis INT,
    sector_3_time_millis INT,
    is_personal_best BOOLEAN NOT NULL,
    is_fastest_lap BOOLEAN NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year, grand_prix_name)
```

**6. Pitstops Silver Table**
```sql
CREATE TABLE f1_silver_db.pitstops_silver (
    session_key BIGINT NOT NULL,
    driver_number INT NOT NULL,
    lap_number INT NOT NULL,
    pit_duration_millis INT,
    pit_stop_number INT NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year, grand_prix_name)
```

#### Gold Layer Tables (5 Analytics Tables)

**1. Driver Performance Summary - Qualifying**
```sql
CREATE TABLE f1_gold_db.driver_performance_summary_qualifying (
    session_key BIGINT NOT NULL,
    grand_prix_name STRING NOT NULL,
    race_date DATE NOT NULL,
    round_number INT NOT NULL,
    driver_number INT NOT NULL,
    driver_name STRING NOT NULL,
    team_name STRING NOT NULL,
    qualifying_position INT,
    qualifying_time DECIMAL(8,3),
    gap_to_pole DECIMAL(6,3),
    made_q3 BOOLEAN NOT NULL,
    teammate_name STRING,
    teammate_position INT,
    gap_to_teammate DECIMAL(6,3),
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (grand_prix_name)
```

**2. Driver Performance Summary - Race**
```sql
CREATE TABLE f1_gold_db.driver_performance_summary_race (
    session_key BIGINT NOT NULL,
    grand_prix_name STRING NOT NULL,
    race_date DATE NOT NULL,
    round_number INT NOT NULL,
    driver_number INT NOT NULL,
    driver_name STRING NOT NULL,
    team_name STRING NOT NULL,
    final_position INT,
    points INT NOT NULL,
    grid_position INT,
    positions_gained INT,
    fastest_lap BOOLEAN NOT NULL,
    total_laps INT,
    race_time_seconds DECIMAL(8,3),
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (grand_prix_name)
```

**3. Championship Tracker**
```sql
CREATE TABLE f1_gold_db.championship_tracker (
    year INT NOT NULL,
    round_number INT NOT NULL,
    grand_prix_name STRING NOT NULL,
    race_date DATE NOT NULL,
    driver_number INT NOT NULL,
    driver_name STRING NOT NULL,
    team_name STRING NOT NULL,
    points INT NOT NULL,
    total_points INT NOT NULL,
    championship_position INT NOT NULL,
    points_gap_to_leader INT NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (year)
```

**4. Team Strategy Analysis**
```sql
CREATE TABLE f1_gold_db.team_strategy_analysis (
    session_key BIGINT NOT NULL,
    grand_prix_name STRING NOT NULL,
    race_date DATE NOT NULL,
    team_name STRING NOT NULL,
    driver_1_number INT NOT NULL,
    driver_1_name STRING NOT NULL,
    driver_2_number INT NOT NULL,
    driver_2_name STRING NOT NULL,
    team_total_points INT NOT NULL,
    driver_1_position INT,
    driver_2_position INT,
    team_constructors_points INT NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (grand_prix_name)
```

**5. Race Weekend Insights**
```sql
CREATE TABLE f1_gold_db.race_weekend_insights (
    session_key BIGINT NOT NULL,
    grand_prix_name STRING NOT NULL,
    race_date DATE NOT NULL,
    round_number INT NOT NULL,
    total_drivers INT NOT NULL,
    total_teams INT NOT NULL,
    fastest_qualifying_time DECIMAL(8,3),
    fastest_race_lap DECIMAL(8,3),
    total_race_time_seconds DECIMAL(10,3),
    safety_car_deployed BOOLEAN NOT NULL,
    red_flag_incidents INT NOT NULL,
    created_timestamp TIMESTAMP NOT NULL,
    updated_timestamp TIMESTAMP NOT NULL
) PARTITIONED BY (grand_prix_name)
```

---

## Phase 2: S3 Bucket Structure Setup

**Purpose**: Organize your data lake with proper folder structure for Bronze, Silver, Gold layers, and supporting services.

### Step 1: Navigate to S3 Console
1. Go to [S3 Console](https://console.aws.amazon.com/s3/buckets)
2. Click on your bucket: `f1-data-lake-naveeth`

### Step 2: Create Folder Structure
Create these folders by clicking **"Create folder"**:

#### Data Layer Folders
- `bronze/` - Raw data from OpenF1 API
- `silver/` - Cleaned and validated data
- `gold/` - Business-ready analytics data

#### Supporting Service Folders
- `lineage/` - OpenLineage data lineage events
  - `lineage/events/` - Individual lineage events
  - `lineage/summaries/` - Daily lineage summaries
- `iceberg-warehouse/` - Apache Iceberg table storage
- `validation-reports/` - Great Expectations validation reports
- `deployment/` - Code deployment packages

### Step 3: Set Folder Permissions
For each folder, ensure proper permissions:
1. Click on folder → **"Permissions"** tab
2. Verify bucket policy allows Glue and MWAA roles access

**✅ Verification**: Your bucket should have 8 folders organized as above.

---

## Phase 3: Create Glue Jobs

**Purpose**: Glue jobs perform the actual data transformations from Bronze→Silver→Gold layers.

### 3.1 Create Bronze to Silver Job

#### Step 1: Navigate to Glue Jobs
1. Go to [Glue Jobs Console](https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=jobs)
2. Click **"Create job"**

#### Step 2: Job Properties
Fill in the following details:
- **Job name**: `f1-bronze-to-silver-transform`
- **IAM role**: Select `F1GlueJobRole` from dropdown
- **Job type**: `Spark`
- **Glue version**: `4.0` (supports Iceberg)
- **Language**: `Python 3`
- **Maximum capacity**: `2` (adjust based on your needs)

#### Step 3: Job Parameters
- **Job bookmark**: `Enable` (for incremental processing)
- **Maximum retries**: `1`
- **Timeout**: `60 minutes`
- **Worker type**: `G.1X` (recommended for Iceberg)

#### Step 4: Script Configuration
- **Script location**: `s3://f1-data-lake-naveeth/deployment/f1-pipeline-deployment.zip`
- **Python library path**: `s3://f1-data-lake-naveeth/deployment/f1-pipeline-deployment.zip`

#### Step 5: Advanced Properties
Add these job parameters:
```
--job-language=python
--job-bookmark-option=job-bookmark-enable
--extra-py-files=s3://f1-data-lake-naveeth/deployment/f1-pipeline-deployment.zip
```

Click **"Create job"**

### 3.2 Create Silver to Gold Job

#### Step 1: Create Another Job
1. Click **"Create job"** again
2. Follow the same steps as above with these changes:

#### Step 2: Job Properties
- **Job name**: `f1-silver-to-gold-transform`
- **IAM role**: `F1GlueJobRole`
- **Job type**: `Spark`
- **Glue version**: `4.0`
- **Language**: `Python 3`
- **Maximum capacity**: `2`

#### Step 3: Script Configuration
- **Script location**: `s3://f1-data-lake-naveeth/deployment/f1-pipeline-deployment.zip`
- **Python library path**: `s3://f1-data-lake-naveeth/deployment/f1-pipeline-deployment.zip`

Click **"Create job"**

**✅ Verification**: You should see 2 Glue jobs in your console.

---

## Phase 4: Create MWAA Environment

**Purpose**: MWAA provides managed Apache Airflow for orchestrating your data pipeline.

### Step 1: Create S3 Bucket for MWAA
1. Go to [S3 Console](https://console.aws.amazon.com/s3/buckets)
2. Click **"Create bucket"**
3. **Bucket name**: `f1-mwaa-naveeth` (must be globally unique)
4. **Region**: `us-east-1`
5. Click **"Create bucket"**

### Step 2: Navigate to MWAA Console
1. Go to [MWAA Console](https://console.aws.amazon.com/mwaa/home?region=us-east-1#environments)
2. Click **"Create environment"**

### Step 3: Environment Details
Fill in the following:
- **Name**: `f1-airflow-env`
- **Airflow version**: `2.6.3` (latest stable)
- **Environment class**: `mw1.small` (adjust based on needs)
- **Maximum workers**: `1`

### Step 4: Network Configuration
- **VPC**: Select your default VPC
- **Subnets**: Select 2 subnets in different AZs
- **Security groups**: Create new or use existing
- **Web server access**: `Public` (for easier access)

### Step 5: Execution Role
- **Role ARN**: `arn:aws:iam::YOUR_ACCOUNT_ID:role/F1MWAARole`
  - Replace `YOUR_ACCOUNT_ID` with your actual AWS account ID

### Step 6: Source Code Configuration
- **S3 bucket**: `f1-mwaa-naveeth`
- **DAGs folder**: `dags`
- **Requirements file**: `requirements.txt`

### Step 7: Advanced Configuration
- **Worker logs**: `Enabled`
- **Scheduler logs**: `Enabled`
- **Task logs**: `Enabled`
- **Web server logs**: `Enabled`

Click **"Create environment"**

**⏱️ Note**: MWAA environment creation takes 15-20 minutes.

**✅ Verification**: Environment status should show "Available" when ready.

---

## Phase 5: Upload Code to S3

**Purpose**: Deploy your pipeline code to S3 for Glue jobs and MWAA to access.

### 5.1 Package Your Code

#### Step 1: Create Deployment Package
```bash
# Create deployment directory
# Create deployment package from root directory
# Files are already in correct structure

# Create ZIP package from root
zip -r f1-pipeline-deployment.zip .
cd ..
```

#### Step 2: Upload to S3
```bash
# Upload deployment package
aws s3 cp f1-pipeline-deployment.zip s3://f1-data-lake-naveeth/deployment/

# Upload requirements
aws s3 cp requirements.txt s3://f1-data-lake-naveeth/deployment/
```

### 5.2 Upload DAGs to MWAA

#### Step 1: Upload DAGs
```bash
# Upload DAG files
aws s3 cp dags/ s3://f1-mwaa-naveeth/dags/ --recursive

# Upload requirements
aws s3 cp requirements.txt s3://f1-mwaa-naveeth/requirements.txt
```

**✅ Verification**: Check S3 console to ensure files are uploaded correctly.

---

## Phase 6: CloudWatch Setup

**Purpose**: Monitor your pipeline with dashboards, metrics, and alarms.

### 6.1 Create CloudWatch Dashboard

#### Step 1: Navigate to CloudWatch
1. Go to [CloudWatch Console](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:)
2. Click **"Create dashboard"**

#### Step 2: Dashboard Configuration
1. **Dashboard name**: `F1-Data-Pipeline`
2. Click **"Create dashboard"**

#### Step 3: Add Widgets

##### Add Pipeline Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `F1Pipeline/Bronze`
   - Metric: `sessions_processed`
   - Dimension: `Environment=prod`
4. **Title**: `F1 Pipeline Metrics`
5. Click **"Create widget"**

##### Add Silver Layer Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `F1Pipeline/Silver`
   - Metric: `tables_processed`
   - Dimension: `Environment=prod`
4. **Title**: `Silver Layer Processing`
5. Click **"Create widget"**

##### Add Gold Layer Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `F1Pipeline/Production`
   - Metric: `analytics_tables_processed`
   - Dimension: `Environment=prod`
4. **Title**: `Gold Layer Analytics`
5. Click **"Create widget"**

##### Add Data Quality Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `F1Pipeline/Production`
   - Metric: `validation_success_rate`
   - Dimension: `Environment=prod`
4. **Title**: `Data Quality Validation`
5. Click **"Create widget"**

##### Add Glue Job Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `AWS/Glue`
   - Metric: `glue.driver.aggregate.numFailedTasks`
   - Dimension: `JobName=f1-bronze-to-silver-transform`
4. **Title**: `Glue Job Failures`
5. Click **"Create widget"`

##### Add Transformation Metrics Widget
1. Click **"Add widget"**
2. Choose **"Line"** chart
3. **Metrics**:
   - Namespace: `F1Pipeline/Silver`
   - Metric: `transformation_execution_time`
   - Dimension: `Environment=prod`
4. **Title**: `Transformation Performance`
5. Click **"Create widget"**

### 6.2 Create CloudWatch Alarms

#### Step 1: Navigate to Alarms
1. Go to [CloudWatch Alarms](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:)
2. Click **"Create alarm"**

#### Step 2: Create Glue Job Failure Alarm
1. **Metric**: `AWS/Glue`
2. **Metric name**: `glue.driver.aggregate.numFailedTasks`
3. **Statistic**: `Sum`
4. **Period**: `5 minutes`
5. **Threshold**: `>= 1`
6. **Alarm name**: `F1-Glue-Job-Failures`
7. **Description**: `Alert when F1 Glue jobs fail`
8. Click **"Next"**

#### Step 3: Create Pipeline Execution Time Alarm
1. Click **"Create alarm"** again
2. **Metric**: `F1Pipeline/Production`
3. **Metric name**: `pipeline_execution_time`
4. **Statistic**: `Average`
5. **Period**: `5 minutes`
6. **Threshold**: `> 1800 seconds` (30 minutes)
7. **Alarm name**: `F1-Pipeline-Execution-Time`
8. **Description**: `Alert when F1 pipeline takes too long`
9. Click **"Next"**

#### Step 4: Create Data Quality Alarm
1. Click **"Create alarm"** again
2. **Metric**: `F1Pipeline/Production`
3. **Metric name**: `validation_failures`
4. **Statistic**: `Sum`
5. **Period**: `5 minutes`
6. **Threshold**: `>= 1`
7. **Alarm name**: `F1-Data-Quality-Issues`
8. **Description**: `Alert when data quality validation fails`
9. Click **"Next"**

**✅ Verification**: You should see 3 alarms in your CloudWatch console.

### 6.3 Configure Logging

**Purpose**: Your codebase implements comprehensive logging across all components for debugging and monitoring.

#### Step 1: Understand Logging Structure
Your codebase implements:

**Log Levels**:
- `INFO`: General pipeline progress
- `WARNING`: Non-critical issues
- `ERROR`: Critical failures
- `DEBUG`: Detailed debugging information

**Log Categories**:
- **Pipeline Execution**: Start/end of transformations
- **Data Validation**: Great Expectations results
- **Lineage Tracking**: OpenLineage event generation
- **Metrics**: CloudWatch metrics sending
- **Error Handling**: Exception details and recovery

**Log Storage**:
- **CloudWatch Logs**: `/aws-glue/jobs/f1-bronze-to-silver-transform`
- **CloudWatch Logs**: `/aws-glue/jobs/f1-silver-to-gold-transform`
- **CloudWatch Logs**: `/aws-mwaa/f1-airflow-env`
- **S3 Logs**: `s3://f1-data-lake-naveeth/logs/` (if configured)

#### Step 2: Log Configuration
Your codebase uses these logging configurations:

**Development (dev.yaml)**:
```yaml
logging:
  level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  include_timestamp: true
  log_file: null
```

**Production (prod.yaml)**:
```yaml
logging:
  level: "WARNING"  # Less verbose logging in production
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  include_timestamp: true
  log_file: null
```

### 6.4 Configure Comprehensive Metrics

**Purpose**: Your codebase sends detailed metrics to CloudWatch for comprehensive monitoring.

#### Step 1: Understand Metrics Structure
Your codebase implements these metric categories:

**Bronze Layer Metrics**:
- `sessions_processed`: Number of F1 sessions processed
- `total_records`: Total records extracted
- `extraction_time_seconds`: Time taken for data extraction
- `api_calls_made`: Number of OpenF1 API calls
- `validation_success_rate`: Data validation success percentage

**Silver Layer Metrics**:
- `tables_processed`: Number of Silver tables created
- `transformation_execution_time`: Time for Bronze→Silver transformation
- `input_record_count`: Records processed from Bronze
- `output_record_count`: Records written to Silver
- `data_quality_score`: Overall data quality score

**Gold Layer Metrics**:
- `analytics_tables_processed`: Number of Gold tables created
- `analytics_execution_time`: Time for Silver→Gold transformation
- `window_function_calculations`: Number of window functions executed
- `teammate_comparisons`: Number of teammate comparisons made
- `championship_calculations`: Championship position calculations

**Pipeline Metrics**:
- `pipeline_execution_time`: Total pipeline execution time
- `pipeline_success_rate`: Overall pipeline success rate
- `data_freshness_hours`: Hours since last successful run
- `error_count`: Number of errors encountered
- `retry_count`: Number of retries attempted

#### Step 2: Metrics Namespaces
Your codebase uses these CloudWatch namespaces:

- `F1Pipeline/Bronze`: Bronze layer metrics
- `F1Pipeline/Silver`: Silver layer metrics
- `F1Pipeline/Production`: Production pipeline metrics
- `AWS/Glue`: AWS Glue job metrics
- `AWS/MWAA`: MWAA environment metrics

#### Step 3: Metrics Dimensions
Your codebase uses these dimensions for metrics:

- `Environment`: `dev`, `staging`, `prod`
- `JobName`: Glue job names
- `TableName`: Specific table being processed
- `DataType`: Type of F1 data (drivers, results, laps, etc.)
- `SessionType`: Qualifying or Race
- `GrandPrix`: Specific Grand Prix name

---

## Phase 7: Test and Verify

### 7.1 Test Glue Jobs

#### Step 1: Run Bronze to Silver Job
1. Go to [Glue Jobs Console](https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=jobs)
2. Select `f1-bronze-to-silver-transform`
3. Click **"Run job"**
4. Monitor the job run in the **"History"** tab

#### Step 2: Run Silver to Gold Job
1. Select `f1-silver-to-gold-transform`
2. Click **"Run job"**
3. Monitor the job run

**⏱️ Note**: Jobs may take 5-10 minutes to complete.

### 7.2 Test MWAA Environment

#### Step 1: Access Airflow UI
1. Go to [MWAA Console](https://console.aws.amazon.com/mwaa/home?region=us-east-1#environments)
2. Select `f1-airflow-env`
3. Click **"Open Airflow UI"**
4. Login with your AWS credentials

#### Step 2: Trigger DAGs
1. Find your F1 DAGs in the DAGs list
2. Click **"Trigger DAG"** for `f1_end_to_end_pipeline`
3. Monitor the DAG run in the **"Graph"** view

### 7.3 Verify Data

#### Step 1: Check S3 Data
1. Go to [S3 Console](https://console.aws.amazon.com/s3/buckets)
2. Check `f1-data-lake-naveeth/bronze/` for raw data
3. Check `f1-data-lake-naveeth/silver/` for processed data
4. Check `f1-data-lake-naveeth/gold/` for analytics data

#### Step 2: Check Glue Catalog
1. Go to [Glue Data Catalog](https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=databases)
2. Click on `f1_silver_db` to see Silver tables
3. Click on `f1_gold_db` to see Gold tables

#### Step 3: Check CloudWatch Metrics
1. Go to [CloudWatch Dashboard](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#dashboards:name=F1-Data-Pipeline)
2. Verify metrics are being collected
3. Check alarms status

#### Step 4: Check Great Expectations Validation
1. Go to [S3 Console](https://console.aws.amazon.com/s3/buckets)
2. Check `f1-data-lake-naveeth/validation-reports/` for validation reports
3. Verify JSON reports are generated for each data type

#### Step 5: Check OpenLineage Events
1. Go to [S3 Console](https://console.aws.amazon.com/s3/buckets)
2. Check `f1-data-lake-naveeth/lineage/events/` for lineage events
3. Check `f1-data-lake-naveeth/lineage/summaries/` for daily summaries
4. Verify OpenLineage 1.0.5 schema compliance

#### Step 6: Check Logging
1. Go to [CloudWatch Logs](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:log-groups)
2. Check `/aws-glue/jobs/f1-bronze-to-silver-transform` logs
3. Check `/aws-glue/jobs/f1-silver-to-gold-transform` logs
4. Check `/aws-mwaa/f1-airflow-env` logs
5. Verify log levels and formatting

#### Step 7: Check Iceberg Tables
1. Go to [Glue Data Catalog](https://console.aws.amazon.com/glue/home?region=us-east-1#catalog:tab=databases)
2. Verify all 6 Silver tables are created in `f1_silver_db`
3. Verify all 5 Gold tables are created in `f1_gold_db`
4. Check table properties and partitioning strategies
5. Verify Iceberg format and ACID compliance

---

## 🔧 Configuration Files

Before running, ensure you have these files:

### requirements.txt
```
boto3==1.34.34
botocore==1.34.34
great-expectations==0.18.8
openlineage-airflow==1.2.0
openlineage-spark==1.2.0
pyiceberg==0.5.1
pyyaml==6.0.1
requests==2.31.0
```

### config/prod.yaml
```yaml
environment: "production"
aws_services:
  region: "us-east-1"
  s3_bucket: "f1-data-lake-naveeth"
  cloudwatch_namespace: "F1Pipeline"
  glue_catalog_database: "f1_data_catalog"
s3:
  bucket_name: "f1-data-lake-naveeth"
  bronze_prefix: "bronze/"
  silver_prefix: "silver/"
  gold_prefix: "gold/"
cloudwatch:
  namespace: "F1Pipeline/Production"
  environment_dimension: "prod"
  enabled: true
lineage:
  enabled: true
  s3_prefix: "lineage/"
  producer: "f1-data-pipeline/production"
data_validation:
  enabled: true
  fail_on_critical_errors: true
  warning_threshold_percent: 30
```

---

## 🚨 Troubleshooting

### Common Issues

#### 1. Glue Job Failures
- **Check IAM permissions**: Ensure Glue role has S3 access
- **Check script location**: Verify S3 path is correct
- **Check logs**: Review CloudWatch logs for errors

#### 2. MWAA Environment Issues
- **Check VPC configuration**: Ensure subnets are in different AZs
- **Check IAM role**: Verify MWAA role has correct permissions
- **Check DAGs**: Ensure DAG files are valid Python

#### 3. S3 Access Issues
- **Check bucket policy**: Ensure proper permissions
- **Check IAM roles**: Verify S3 access in role policies
- **Check region**: Ensure all resources are in same region

### Useful Commands

```bash
# Check AWS CLI configuration
aws sts get-caller-identity

# List S3 buckets
aws s3 ls

# Check Glue jobs
aws glue get-jobs --region us-east-1

# Check MWAA environments
aws mwaa list-environments --region us-east-1

# Check CloudWatch alarms
aws cloudwatch describe-alarms --region us-east-1
```

---

## 📊 Monitoring and Maintenance

### Daily Tasks
- Check CloudWatch dashboards for pipeline health
- Review Glue job run history
- Monitor S3 storage usage

### Weekly Tasks
- Review CloudWatch alarms
- Check data quality metrics
- Verify OpenLineage events

### Monthly Tasks
- Review and optimize costs
- Update dependencies
- Review and update IAM permissions

---

## 🎯 Next Steps

After successful setup:

1. **Monitor the pipeline** using CloudWatch dashboards
2. **Set up notifications** for alarms (SNS topics)
3. **Optimize performance** based on usage patterns
4. **Scale resources** as data volume grows
5. **Add more data sources** to the pipeline

---

## 📚 Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [MWAA Documentation](https://docs.aws.amazon.com/mwaa/)
- [CloudWatch Documentation](https://docs.aws.amazon.com/cloudwatch/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [Great Expectations Documentation](https://docs.greatexpectations.io/)

---

**🎉 Congratulations!** You've successfully set up a production-ready F1 Data Engineering Pipeline in AWS. This setup provides a solid foundation for learning and understanding modern data engineering practices.
