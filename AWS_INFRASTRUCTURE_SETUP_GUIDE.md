# AWS Infrastructure Setup Guide

## Overview
This guide documents the AWS infrastructure setup for the F1 Data Engineering Pipeline, including all resources, configurations, and deployment procedures.

## Generated: Fri Sep  5 23:03:11 IST 2025
## AWS Account: 931141161782
## AWS Region: us-east-1

## Table of Contents
1. [Infrastructure Overview](#infrastructure-overview)
2. [Resource Inventory](#resource-inventory)
3. [Configuration Details](#configuration-details)
4. [Deployment Procedures](#deployment-procedures)
5. [Monitoring & Maintenance](#monitoring--maintenance)
6. [Troubleshooting](#troubleshooting)

## Infrastructure Overview

### Architecture
The F1 Data Pipeline uses a modern data lakehouse architecture with:
- **Bronze Layer**: Raw data ingestion
- **Silver Layer**: Cleaned and validated data
- **Gold Layer**: Analytics-ready aggregated data

### Core Components
- **AWS Glue**: ETL jobs for data transformation
- **Apache Iceberg**: Table format for data lakehouse
- **MWAA**: Workflow orchestration
- **S3**: Data storage and artifact hosting
- **CloudWatch**: Monitoring and logging
- **OpenLineage**: Data lineage tracking

## Resource Inventory

### S3 Buckets
#### Data Lake Bucket: f1-data-lake-naveeth
- Status: ✅ Active
- Purpose: Raw data storage, processed data, deployment artifacts
- Encryption: Enabled
- Versioning: Enabled

#### MWAA Bucket: f1-mwaa-naveeth
- Status: ✅ Active
- Purpose: MWAA DAGs, requirements, and configuration
- Encryption: Enabled
- Versioning: Enabled

### IAM Roles
#### Glue Execution Role: F1-DataPipeline-Role
- Status: ✅ Active
- Purpose: Glue job execution and S3 access
- Trust Policy: glue.amazonaws.com

#### MWAA Execution Role: F1-MWAA-ExecutionRole
- Status: ✅ Active
- Purpose: MWAA environment execution
- Trust Policy: airflow.amazonaws.com

### Glue Jobs
#### Bronze to Silver Transform: f1-bronze-to-silver-transform
- Status: ✅ Active
- Glue Version: 4.0
- Python Version: 3
- Purpose: Transform raw data to silver layer

#### Silver to Gold Transform: f1-silver-to-gold-transform
- Status: ✅ Active
- Glue Version: 4.0
- Python Version: 3
- Purpose: Transform silver data to gold analytics tables

### MWAA Environment
#### Environment: f1-airflow-env
- Status: AVAILABLE
- Airflow Version: 2.7.2
- Purpose: Workflow orchestration and scheduling

### CloudWatch Log Groups
#### f1-pipeline-logs
- Status: ✅ Active
- Retention: 30 days

#### f1-glue-logs
- Status: ✅ Active
- Retention: 30 days

#### f1-mwaa-logs
- Status: ✅ Active
- Retention: 30 days


## Configuration Details

### Wheel-Based Deployment
The infrastructure is configured for wheel-based deployment:
- **F1 Pipeline Wheel**: Custom package built from setup.py
- **Dependency Wheels**: Third-party packages (openlineage, great-expectations, etc.)
- **S3 Storage**: Wheels stored in s3://f1-data-lake-naveeth/dependencies/
- **Glue Integration**: Jobs configured to use wheel S3 URIs

### Security Configuration
- **S3 Encryption**: Server-side encryption enabled
- **IAM Roles**: Least privilege access
- **VPC**: MWAA deployed in private subnets
- **CloudWatch**: Comprehensive logging and monitoring

## Deployment Procedures

### Phase 1: Resource Detection
```bash
./scripts/aws_infrastructure_setup.sh --phase1
```

### Phase 2: Update Existing Resources
```bash
./scripts/aws_infrastructure_setup.sh --phase2
```

### Phase 3: Create Missing Resources
```bash
./scripts/aws_infrastructure_setup.sh --phase3
```

### Phase 4: Validation & Testing
```bash
./scripts/aws_infrastructure_setup.sh --phase4
```

### Phase 5: Cleanup & Documentation
```bash
./scripts/aws_infrastructure_setup.sh --phase5
```

### Full Deployment
```bash
./scripts/aws_infrastructure_setup.sh
```

## Monitoring & Maintenance

### CloudWatch Dashboards
- **F1 Pipeline Dashboard**: Monitor job execution and performance
- **Error Dashboard**: Track failures and issues
- **Cost Dashboard**: Monitor AWS costs

### Log Analysis
- **Glue Job Logs**: /aws/glue/f1-bronze-to-silver, /aws/glue/f1-silver-to-gold
- **MWAA Logs**: airflow-f1-airflow-env-*
- **Pipeline Logs**: f1-pipeline-logs

### Regular Maintenance
1. **Weekly**: Review CloudWatch metrics and logs
2. **Monthly**: Clean up old S3 objects and logs
3. **Quarterly**: Review and update IAM policies
4. **Annually**: Review and update Glue versions

## Troubleshooting

### Common Issues
1. **Glue Job Failures**: Check CloudWatch logs and S3 permissions
2. **MWAA Issues**: Verify VPC configuration and security groups
3. **S3 Access**: Check IAM role policies and bucket permissions
4. **Wheel Dependencies**: Verify wheel files exist in S3

### Useful Commands
```bash
# Check Glue job status
aws glue get-job --job-name f1-bronze-to-silver-transform

# List S3 objects
aws s3 ls s3://f1-data-lake-naveeth/dependencies/ --recursive

# Check MWAA environment
aws mwaa get-environment --name f1-airflow-env

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix f1-
```

## Support
For issues or questions:
1. Check CloudWatch logs first
2. Review this documentation
3. Check AWS Console for resource status
4. Contact the development team

---
*This documentation was automatically generated by the AWS Infrastructure Setup script.*
