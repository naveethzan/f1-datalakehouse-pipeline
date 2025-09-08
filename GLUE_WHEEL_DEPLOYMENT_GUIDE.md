# F1 Data Pipeline - Integrated Wheel Deployment Guide

This guide explains the integrated wheel-based deployment approach for the F1 Data Pipeline using a single deployment script.

## 🎯 Overview

The deployment is now handled by a single script: `scripts/deploy_code_to_aws.sh`

This script uses Python wheels (`.whl` files) which provide:
- Better dependency resolution
- Faster installation in Glue
- More reliable package management
- Better compatibility with Glue 4.0
- Single source of truth for all deployments

## 📋 Prerequisites

- Python 3.9+ installed
- pip3 installed
- AWS CLI configured
- Access to S3 buckets: `f1-data-lake-naveeth` and `f1-mwaa-naveeth`

## 🚀 Single Command Deployment

### Full Deployment

```bash
# Deploy everything (wheels + MWAA + Glue jobs)
./scripts/deploy_code_to_aws.sh --version v1.0.0

# Or use timestamp version
./scripts/deploy_code_to_aws.sh
```

### Glue-Only Deployment

```bash
# Update only Glue jobs with new wheels
./scripts/deploy_code_to_aws.sh --glue-only --version v1.0.0
```

### Dry Run

```bash
# Preview what would be deployed
./scripts/deploy_code_to_aws.sh --dry-run --verbose
```

## 🔧 What the Script Does

### 1. Wheel Building
- Builds `f1-pipeline` wheel from `setup.py`
- Builds dependency wheels:
  - `openlineage-python==0.28.0`
  - `great-expectations==0.16.15`
  - `pyiceberg==0.5.1`
  - `urllib3==1.26.18`

### 2. S3 Upload
- Uploads wheels to `s3://f1-data-lake-naveeth/dependencies/{version}/`
- Uploads Glue scripts to `s3://f1-data-lake-naveeth/scripts/`
- Uploads MWAA code to `s3://f1-mwaa-naveeth/`

### 3. Glue Job Updates
- Updates both Glue jobs with wheel S3 URIs
- Removes `--extra-py-files` (ZIP approach)
- Uses `--additional-python-modules` with wheel URIs

### 4. MWAA Deployment
- Deploys DAGs, utils, and config to MWAA bucket
- Maintains ZIP format for Airflow compatibility

## 📁 File Structure

```
project/
├── setup.py                          # Package definition
├── src/                              # Source code
│   ├── jobs/orchestrators/           # Glue job scripts
│   ├── dags/                         # Airflow DAGs
│   └── utils/                        # Utilities
├── scripts/
│   └── deploy_code_to_aws.sh         # Single deployment script
└── deployment/
    ├── wheels/                       # Built wheel files
    └── package/                      # MWAA package
```

## 🎛️ Script Options

```bash
./scripts/deploy_code_to_aws.sh [OPTIONS]

Options:
  --clean           Remove previous code before deploying (default)
  --no-clean        Keep previous code versions
  --dry-run         Show what would be deployed without actually deploying
  --verbose         Enable verbose logging
  --skip-validation Skip code validation steps
  --glue-only       Update only Glue jobs (skip code deployment)
  --version VERSION Set deployment version (default: timestamp)
  --help            Show this help message
```

## 🔍 Verification

After deployment, verify:

1. **Wheels uploaded**: Check S3 bucket for wheel files
2. **Glue jobs updated**: Check job configuration in AWS Console
3. **MWAA updated**: Check DAGs in MWAA environment
4. **Test execution**: Run a Glue job to verify imports work

## 🐛 Troubleshooting

### Common Issues

1. **Wheel build fails**: Check `setup.py` and Python version
2. **S3 upload fails**: Verify AWS credentials and bucket permissions
3. **Glue job fails**: Check wheel URIs and job configuration
4. **Import errors**: Verify wheel compatibility with Glue 4.0

### Logs

Check deployment logs at:
- `logs/deploy_code_{timestamp}.log`

### Manual Verification

```bash
# Check wheels in S3
aws s3 ls s3://f1-data-lake-naveeth/dependencies/latest/

# Check Glue job configuration
aws glue get-job --job-name f1-bronze-to-silver-transform

# Check MWAA files
aws s3 ls s3://f1-mwaa-naveeth/dags/
```

## 🚀 Benefits of Integrated Approach

1. **Single Source of Truth**: One script handles everything
2. **Consistent Deployment**: Same process every time
3. **Error Handling**: Comprehensive error checking and logging
4. **Rollback Support**: Version tracking and rollback capability
5. **Dry Run Support**: Test deployments without making changes
6. **Flexible Options**: Deploy everything or just Glue jobs

## 📝 Migration from ZIP Approach

The script automatically:
- Removes `--extra-py-files` parameter
- Adds `--additional-python-modules` with wheel URIs
- Builds wheels instead of ZIP files
- Maintains backward compatibility for MWAA

## 🔗 AWS Console Links

- **Glue Jobs**: https://console.aws.amazon.com/glue/home?region=us-east-1#etl:tab=jobs
- **MWAA**: https://console.aws.amazon.com/mwaa/home?region=us-east-1#environments
- **S3 Data Lake**: https://console.aws.amazon.com/s3/buckets/f1-data-lake-naveeth
- **S3 MWAA**: https://console.aws.amazon.com/s3/buckets/f1-mwaa-naveeth