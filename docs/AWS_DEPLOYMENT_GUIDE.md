# 🚀 AWS Deployment Guide for F1 Data Engineering Pipeline

## 📋 Prerequisites

### 1. AWS Account Setup
- AWS Account with appropriate permissions
- AWS CLI installed and configured
- IAM user with programmatic access

### 2. Required AWS Permissions
Your AWS user/role needs these permissions:
- `S3FullAccess`
- `IAMFullAccess`
- `GlueFullAccess`
- `CloudWatchFullAccess`
- `LogsFullAccess`

## 🏗️ AWS Resources to Create

### **Core Infrastructure (Required)**

#### 1. 🪣 S3 Bucket
- **Purpose**: Data lake storage for Bronze, Silver, Gold layers
- **Name**: `f1-data-lake-dev-naveeth` (replace with your name)
- **Folders**: `bronze/`, `silver/`, `gold/`, `scripts/`, `temp/`, `spark-logs/`, `iceberg-warehouse/`

#### 2. 🔐 IAM Roles
- **GlueServiceRole**: For AWS Glue jobs to access S3 and Glue Catalog
- **AirflowExecutionRole**: For Airflow to trigger Glue jobs and access S3

#### 3. 🗄️ AWS Glue Data Catalog
- **Database**: `f1_data_catalog`
- **Purpose**: Metadata store for Iceberg tables
- **Location**: `s3://f1-data-lake-dev-naveeth/iceberg-warehouse/`

#### 4. 🔧 AWS Glue Jobs
- **f1-bronze-to-silver-transform**: Processes Bronze → Silver data
- **f1-silver-to-gold-transform**: Processes Silver → Gold data
- **Configuration**: Glue 4.0, Python 3, 2 DPU capacity

### **Monitoring & Observability (Recommended)**

#### 5. 📊 CloudWatch
- **Log Groups**: `/aws/glue/f1-*`, `/aws/airflow/f1-pipeline`
- **Dashboard**: `F1-Pipeline-Monitoring`
- **Alarms**: Job failure notifications

#### 6. 📈 CloudWatch Metrics
- Job completion/failure rates
- Data processing volumes
- Performance metrics

## 🚀 Quick Deployment

### Option 1: Automated Script (Recommended)
```bash
# Run the deployment script
./scripts/deploy-aws-infrastructure.sh
```

### Option 2: Manual Setup
Follow the step-by-step commands in the script above.

## 📝 Configuration Files

### 1. Update `.env` file
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here
AWS_DEFAULT_REGION=us-east-1

# S3 Configuration
S3_BUCKET_NAME=f1-data-lake-dev-naveeth
S3_BRONZE_PREFIX=bronze/
S3_SILVER_PREFIX=silver/
S3_GOLD_PREFIX=gold/
```

### 2. Update `config/dev.yaml`
```yaml
s3:
  bucket_name: "f1-data-lake-dev-naveeth"  # Replace with your bucket name

iceberg:
  catalog_type: "glue"
  warehouse_path: "s3://f1-data-lake-dev-naveeth/iceberg-warehouse/"

glue:
  bronze_to_silver:
    script_location: "s3://f1-data-lake-dev-naveeth/scripts/f1_bronze_to_silver_transform.py"
    temp_dir: "s3://f1-data-lake-dev-naveeth/temp/"
    spark_event_logs_path: "s3://f1-data-lake-dev-naveeth/spark-logs/"
  
  silver_to_gold:
    script_location: "s3://f1-data-lake-dev-naveeth/scripts/f1_silver_to_gold_transform.py"
    temp_dir: "s3://f1-data-lake-dev-naveeth/temp/gold/"
    spark_event_logs_path: "s3://f1-data-lake-dev-naveeth/spark-logs/gold/"
```

## 🔄 Deployment Workflow

### 1. Upload Scripts to S3
```bash
# Upload your Python scripts
aws s3 cp jobs/f1_bronze_to_silver_transform.py s3://f1-data-lake-dev-naveeth/scripts/
aws s3 cp jobs/f1_silver_to_gold_transform.py s3://f1-data-lake-dev-naveeth/scripts/
```

### 2. Test Glue Jobs
```bash
# Test Bronze to Silver job
aws glue start-job-run --job-name f1-bronze-to-silver-transform

# Test Silver to Gold job
aws glue start-job-run --job-name f1-silver-to-gold-transform
```

### 3. Monitor Execution
```bash
# Check job status
aws glue get-job-run --job-name f1-bronze-to-silver-transform --run-id <run-id>

# View logs
aws logs describe-log-streams --log-group-name /aws/glue/f1-bronze-to-silver
```

## 💰 Cost Estimation

### Monthly Costs (Approximate)
- **S3 Storage**: $0.023/GB (first 50TB)
- **Glue Jobs**: $0.44/DPU-hour (2 DPU × 1 hour/day = $26.4/month)
- **CloudWatch**: $0.50/GB ingested + $0.03/GB stored
- **Total**: ~$30-50/month for development

### Cost Optimization Tips
- Use Glue job bookmarks to avoid reprocessing
- Implement data lifecycle policies for S3
- Use appropriate DPU capacity for your workload
- Monitor and optimize query performance

## 🔍 Verification Checklist

After deployment, verify:

- [ ] S3 bucket created with proper folder structure
- [ ] IAM roles have correct permissions
- [ ] Glue database exists and is accessible
- [ ] Glue jobs can be started successfully
- [ ] CloudWatch logs are being generated
- [ ] Dashboard shows metrics
- [ ] Alarms are configured

## 🚨 Troubleshooting

### Common Issues

1. **Permission Denied**
   - Check IAM role permissions
   - Verify S3 bucket policies
   - Ensure Glue service role has S3 access

2. **Job Failures**
   - Check CloudWatch logs
   - Verify script location in S3
   - Check DPU capacity limits

3. **Data Not Appearing**
   - Verify S3 paths in configuration
   - Check Glue job bookmarks
   - Ensure proper data formats

### Useful Commands
```bash
# Check S3 bucket contents
aws s3 ls s3://f1-data-lake-dev-naveeth/ --recursive

# List Glue jobs
aws glue list-jobs

# Check Glue database
aws glue get-database --name f1_data_catalog

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix /aws/glue/f1
```

## 🎯 Next Steps

1. **Deploy Infrastructure**: Run the deployment script
2. **Upload Scripts**: Copy your Python files to S3
3. **Test Jobs**: Run Glue jobs manually
4. **Set Up Airflow**: Deploy Airflow to AWS or run locally
5. **Monitor**: Set up alerts and dashboards
6. **Optimize**: Fine-tune performance and costs

## 📚 Additional Resources

- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [AWS CloudWatch Documentation](https://docs.aws.amazon.com/cloudwatch/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
