# F1 Data Pipeline Configuration Guide

## 🎯 **Overview**

This guide explains how to configure the F1 Data Pipeline for both **local development** and **production deployment** using the hybrid approach where data is centralized in S3.

## 📁 **Configuration Files**

### **Development Configuration** (`config/dev.yaml`)
- **Purpose**: Local development with Docker (Airflow + Spark)
- **Data Storage**: S3 (centralized)
- **Data Catalog**: AWS Glue Catalog
- **Compute**: Local Docker containers

### **Production Configuration** (`config/prod.yaml`)
- **Purpose**: AWS production deployment
- **Data Storage**: S3 (same bucket as dev)
- **Data Catalog**: AWS Glue Catalog (same as dev)
- **Compute**: AWS MWAA + AWS Glue

## 🔧 **Environment Detection**

The system automatically detects the environment using the following logic:

```python
def _detect_environment(self) -> str:
    # 1. Check explicit environment variable
    env = os.getenv('F1_ENVIRONMENT', '').lower()
    if env in ['development', 'dev', 'production', 'prod']:
        return 'dev' if env in ['development', 'dev'] else 'prod'
    
    # 2. Check for AWS Glue environment
    if os.getenv('AWS_GLUE_JOB_NAME') or os.getenv('GLUE_VERSION'):
        return 'prod'
    
    # 3. Check for Airflow environment
    if os.getenv('AIRFLOW_HOME') and '/opt/airflow' in os.getenv('AIRFLOW_HOME', ''):
        if os.getenv('AWS_DEFAULT_REGION') and not os.path.exists('/opt/airflow/config/dev.yaml'):
            return 'prod'  # AWS MWAA
        else:
            return 'dev'   # Local Docker
    
    # 4. Default to development
    return 'dev'
```

## 🚀 **Local Development Setup**

### **1. Environment Variables (Optional)**
```bash
export F1_ENVIRONMENT=development
export F1_CONFIG_PATH=config/dev.yaml
```

### **2. Configuration Loading**
```python
from utils.config_manager import ConfigManager

# Auto-detects as development
config = ConfigManager()

# Or explicitly specify
config = ConfigManager('config/dev.yaml')
```

### **3. Key Development Settings**
```yaml
environment: "development"
spark:
  app_name: "F1-Data-Pipeline-Dev"
  master_url: "spark://spark-master:7077"  # Local Spark cluster

glue:
  job_name: "f1-bronze-to-silver-transform"
  worker_type: "G.1X"  # Smaller workers for dev
  number_of_workers: 10

logging:
  level: "INFO"  # Verbose logging for development
```

## 🏭 **Production Deployment**

### **1. Environment Variables**
```bash
export F1_ENVIRONMENT=production
export F1_CONFIG_PATH=/opt/airflow/config/prod.yaml
```

### **2. Configuration Loading**
```python
from utils.config_manager import ConfigManager

# Auto-detects as production in AWS Glue
config = ConfigManager()

# Or explicitly specify
config = ConfigManager('/opt/airflow/config/prod.yaml')
```

### **3. Key Production Settings**
```yaml
environment: "production"
spark:
  app_name: "F1-Data-Pipeline-Prod"
  master_url: null  # Not used in AWS Glue

glue:
  job_name: "f1-bronze-to-silver-transform-prod"
  worker_type: "G.2X"  # Larger workers for production
  number_of_workers: 20
  max_concurrent_runs: 3  # Allow concurrency

logging:
  level: "WARNING"  # Less verbose logging
```

## 📊 **Configuration Comparison**

| Setting | Development | Production | Purpose |
|---------|-------------|------------|---------|
| **Environment** | `development` | `production` | Environment identification |
| **Spark App Name** | `F1-Data-Pipeline-Dev` | `F1-Data-Pipeline-Prod` | Resource identification |
| **Glue Workers** | 10 | 20 | Compute capacity |
| **Worker Type** | `G.1X` | `G.2X` | Worker size |
| **Max Retries** | 1 | 3 | Reliability |
| **Log Level** | `INFO` | `WARNING` | Logging verbosity |
| **Concurrent Runs** | 1 | 3 | Parallelism |
| **Timeout** | 60 min | 120 min | Job duration |

## 🔄 **Hybrid Architecture Benefits**

### **✅ Data Consistency**
- Same S3 bucket for both environments
- Same AWS Glue Catalog
- Same Iceberg tables
- No data synchronization issues

### **✅ Development Efficiency**
- Test with real data
- Same data quality rules
- Same business logic
- No mocking required

### **✅ Production Readiness**
- Same data structure
- Same validation rules
- Same transformation logic
- Seamless deployment

## 🛠 **Usage Examples**

### **Local Development**
```python
# Auto-detects development environment
config = ConfigManager()

# Get S3 configuration
s3_config = config.get_s3_config()
# Returns: {'bucket_name': 'f1-data-lake-naveeth', 'bronze_prefix': 'bronze/', ...}

# Get Spark configuration
spark_config = config.get_spark_config()
# Returns: {'app_name': 'F1-Data-Pipeline-Dev', 'master_url': 'spark://spark-master:7077', ...}

# Check environment
if config.is_development():
    print("Running in development mode")
```

### **Production Deployment**
```python
# Auto-detects production environment in AWS Glue
config = ConfigManager()

# Get Glue configuration
glue_config = config.get_glue_config()
# Returns: {'job_name': 'f1-bronze-to-silver-transform-prod', 'worker_type': 'G.2X', ...}

# Check environment
if config.is_production():
    print("Running in production mode")
```

## 🔍 **Testing Configuration**

Run the configuration test script:

```bash
python3 scripts/test_config.py
```

This will test:
- ✅ Auto-detection
- ✅ Development configuration loading
- ✅ Production configuration loading
- ✅ Environment variable overrides
- ✅ Configuration comparison

## 📝 **Best Practices**

### **1. Environment Variables**
- Use `F1_ENVIRONMENT` to explicitly set environment
- Use `F1_CONFIG_PATH` to specify custom config path
- Set AWS credentials appropriately for each environment

### **2. Configuration Management**
- Keep sensitive data in environment variables
- Use IAM roles in production
- Use AWS profiles for local development

### **3. Deployment**
- Test configuration changes locally first
- Use same S3 bucket for data consistency
- Monitor configuration loading in logs

## 🚨 **Troubleshooting**

### **Configuration Not Found**
```
ConfigurationError: No configuration file found for environment 'prod'
```
**Solution**: Ensure `config/prod.yaml` exists and is accessible

### **Environment Detection Issues**
```
Environment: development (expected: production)
```
**Solution**: Set `F1_ENVIRONMENT=production` environment variable

### **AWS Credentials**
```
AWS credentials not found
```
**Solution**: Configure AWS credentials for the target environment

## 🎉 **Summary**

The F1 Data Pipeline now supports:

- ✅ **Automatic environment detection**
- ✅ **Environment-specific configurations**
- ✅ **Hybrid architecture (local compute, S3 data)**
- ✅ **Seamless local-to-production deployment**
- ✅ **Centralized data and catalog**
- ✅ **Production-ready reliability settings**

Your pipeline is now fully configured for both local development and production deployment! 🚀
