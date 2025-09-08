# F1 Data Pipeline - AWS Deployment Scripts

This directory contains comprehensive scripts for deploying the F1 Data Engineering Pipeline to AWS. The scripts have been designed to handle the refactored codebase structure with the new `src/` layout.

## Scripts Overview

### 1. `aws_infrastructure_setup.sh` - AWS Resource Creation

**Purpose**: Creates all necessary AWS infrastructure resources for the F1 pipeline.

**Features**:
- ✅ **Idempotent**: Can be run multiple times safely
- ✅ **Comprehensive Resource Creation**: S3, IAM, Glue, MWAA, CloudWatch
- ✅ **Resource Existence Checks**: Avoids duplicate creation
- ✅ **Proper Dependencies**: Creates resources in correct order
- ✅ **Complete Error Handling**: Detailed logging and error reporting
- ✅ **Dry Run Support**: Preview changes before execution

**Resources Created**:
- **S3 Buckets**: Data lake bucket + MWAA bucket with proper structure
- **IAM Roles**: Glue service role + MWAA execution role with policies
- **Glue Resources**: 3 databases (main, silver, gold) + 2 jobs
- **MWAA Environment**: Complete Airflow environment setup
- **CloudWatch**: Dashboards, alarms, and log groups
- **Integration**: OpenLineage, Great Expectations, Iceberg support

**Usage**:
```bash
# Basic infrastructure setup
./scripts/aws_infrastructure_setup.sh

# Dry run to preview changes
./scripts/aws_infrastructure_setup.sh --dry-run --verbose

# Setup in different region
./scripts/aws_infrastructure_setup.sh --region us-west-2

# Force recreation of existing resources
./scripts/aws_infrastructure_setup.sh --force

# Get help
./scripts/aws_infrastructure_setup.sh --help
```

### 2. `deploy_code_to_aws.sh` - Code Deployment

**Purpose**: Deploys the refactored F1 pipeline codebase to AWS with proper structure.

**Features**:
- ✅ **Clean Deployment**: Removes previous code before deploying new version
- ✅ **Handles New Structure**: Works with refactored `src/` folder layout
- ✅ **Dependency Management**: Proper handling of requirements and packages
- ✅ **Version Tracking**: Tags deployments with versions for rollback
- ✅ **Glue Job Updates**: Updates job configurations with new code
- ✅ **Comprehensive Validation**: Code syntax and structure validation

**Deployment Targets**:
- **Data Lake Bucket**: Main deployment packages + Glue job scripts
- **MWAA Bucket**: DAGs, utils, configs for Airflow
- **Glue Jobs**: Updates job configurations with new script locations
- **Dependencies**: OpenLineage, Great Expectations, Iceberg integration

**Usage**:
```bash
# Standard deployment (clean + deploy)
./scripts/deploy_code_to_aws.sh

# Deploy with specific version
./scripts/deploy_code_to_aws.sh --version v1.0.0

# Dry run to preview deployment
./scripts/deploy_code_to_aws.sh --dry-run --verbose

# Deploy without cleaning previous version
./scripts/deploy_code_to_aws.sh --no-clean

# Skip code validation
./scripts/deploy_code_to_aws.sh --skip-validation

# Get help
./scripts/deploy_code_to_aws.sh --help
```

## Deployment Workflow

### Initial Setup (One-time)

1. **Create AWS Infrastructure**:
   ```bash
   # Preview what will be created
   ./scripts/aws_infrastructure_setup.sh --dry-run --verbose
   
   # Create all resources
   ./scripts/aws_infrastructure_setup.sh
   ```

2. **Deploy Code**:
   ```bash
   # Deploy the codebase
   ./scripts/deploy_code_to_aws.sh --version v1.0.0
   ```

### Regular Updates

For code updates, you only need to run the deployment script:

```bash
# Deploy new version
./scripts/deploy_code_to_aws.sh --version v1.1.0
```

### Rolling Back

If you need to rollback to a previous version:

```bash
# Deploy previous version
./scripts/deploy_code_to_aws.sh --version v1.0.0
```

## Configuration

### Environment Variables

You can customize deployment settings using environment variables:

```bash
# Set custom AWS region
export AWS_REGION=us-west-2

# Set custom bucket names (before running infrastructure setup)
export S3_BUCKET_DATA_LAKE=my-custom-data-lake
export S3_BUCKET_MWAA=my-custom-mwaa

# Deploy with custom settings
./scripts/aws_infrastructure_setup.sh
```

### AWS Credentials

Ensure your AWS credentials are configured:

```bash
# Configure AWS CLI
aws configure

# Or use environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## Troubleshooting

### Common Issues

1. **Permission Errors**:
   - Ensure your AWS credentials have sufficient permissions
   - Check IAM policies for S3, Glue, MWAA, CloudWatch access

2. **Resource Already Exists**:
   - Scripts are idempotent - existing resources are skipped
   - Use `--force` flag if you need to recreate resources

3. **MWAA Environment Creation**:
   - MWAA environments take 20-30 minutes to create
   - Check AWS Console for creation progress

4. **Code Validation Failures**:
   - Check Python syntax in your code
   - Ensure all required files exist in `src/` directory
   - Use `--skip-validation` if needed

### Log Files

Both scripts create detailed log files:

- Infrastructure: `logs/aws_infrastructure_setup_TIMESTAMP.log`
- Deployment: `logs/deploy_code_TIMESTAMP.log`

### Verification

After deployment, verify resources in AWS Console:

- **S3**: Check buckets have proper folder structure and files
- **Glue**: Verify jobs exist and have correct configurations
- **MWAA**: Check environment status and DAG uploads
- **CloudWatch**: Verify dashboards and alarms are created

## Script Features

### Infrastructure Setup Script Features

- 🔍 **Resource Existence Checks**: Avoids duplicate resource creation
- 📊 **Progress Tracking**: Clear phase-by-phase execution
- 🔧 **Comprehensive IAM**: Properly scoped policies for all services
- 📈 **Monitoring Ready**: CloudWatch dashboards and alarms included
- 🚨 **Error Handling**: Detailed error reporting and logging
- 🎯 **Verification**: Post-deployment resource verification

### Code Deployment Script Features

- 🧹 **Clean Deployment**: Removes old code before deploying new
- 📦 **Smart Packaging**: Handles new `src/` structure correctly
- 🏷️ **Version Management**: Tags deployments for easy rollback
- ✅ **Validation**: Code syntax and structure validation
- 🔄 **Glue Integration**: Updates job configurations automatically
- 📝 **Comprehensive Logging**: Detailed deployment tracking

## Support

For issues or questions:

1. Check the log files in the `logs/` directory
2. Verify AWS permissions and credentials
3. Review the AWS Console for resource status
4. Use `--dry-run --verbose` to debug issues

## Replaced Scripts

These new scripts replace the following old scripts:

- ❌ `deploy_to_aws.sh`
- ❌ `redeploy_updated_code.sh`
- ❌ `deploy-aws-infrastructure.sh`
- ❌ `deploy_dags_to_mwaa.sh`
- ❌ `package_and_upload.sh`
- ❌ `quick_upload.sh`

The new scripts provide better functionality, error handling, and support for the refactored codebase structure.
