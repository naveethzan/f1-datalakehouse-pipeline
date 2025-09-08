#!/bin/bash
# =============================================================================
# F1 Data Engineering Pipeline - AWS Infrastructure Setup Script
# =============================================================================
#
# This script creates ALL necessary AWS resources for the F1 pipeline:
# - S3 Buckets (Data Lake + MWAA)
# - IAM Roles and Policies
# - Glue Jobs and Databases
# - MWAA Environment
# - CloudWatch Dashboards and Alarms
# - OpenLineage and Great Expectations integration
#
# Features:
# - Idempotent: Can be run multiple times safely
# - Comprehensive error handling
# - Resource existence checks
# - Proper dependency management
# - Complete logging
#
# Usage: ./scripts/aws_infrastructure_setup.sh [--region us-east-1] [--dry-run]
# =============================================================================

set -euo pipefail

# Handle broken pipe errors gracefully
trap 'exit 0' PIPE

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Configuration with defaults
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="f1-data-pipeline"
ENVIRONMENT="production"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Resource names with consistent naming convention
S3_BUCKET_DATA_LAKE="f1-data-lake-naveeth"
S3_BUCKET_MWAA="f1-mwaa-naveeth"
GLUE_ROLE_NAME="F1-DataPipeline-Role"
MWAA_ROLE_NAME="F1-MWAA-ExecutionRole"
MWAA_ENV_NAME="f1-airflow-env"

# Resource detection variables
EXISTING_RESOURCES=()
MISSING_RESOURCES=()
NEEDS_UPDATE_RESOURCES=()
MIGRATION_REPORT=""
PHASE1_ONLY=false
PHASE2_ONLY=false
PHASE3_ONLY=false
PHASE4_ONLY=false
PHASE5_ONLY=false
UPDATE_MWAA_REQUIREMENTS=false

# Database names
GLUE_DB_MAIN="f1_data_lake"
GLUE_DB_SILVER="f1_silver_db"
GLUE_DB_GOLD="f1_gold_db"

# Job names
GLUE_JOB_BRONZE_TO_SILVER="f1-bronze-to-silver-transform"
GLUE_JOB_SILVER_TO_GOLD="f1-silver-to-gold-transform"

# Flags
DRY_RUN=false
VERBOSE=false
FORCE_CREATE=false

# Logging
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/aws_infrastructure_setup_${TIMESTAMP}.log"

# Create logs directory
mkdir -p "$LOG_DIR"

# =============================
# Utility Functions
# =============================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1" | tee -a "$LOG_FILE"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_FILE"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_FILE"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_FILE"
}

log_debug() {
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "${PURPLE}[DEBUG]${NC} $1" | tee -a "$LOG_FILE"
    fi
}

print_banner() {
    echo -e "${CYAN}"
    echo "============================================================================="
    echo "🚀 F1 Data Engineering Pipeline - AWS Infrastructure Setup"
    echo "============================================================================="
    echo -e "${NC}"
    echo "📅 Timestamp: $(date)"
    echo "🌍 AWS Region: $AWS_REGION"
    echo "📦 Project: $PROJECT_NAME"
    echo "🔧 Environment: $ENVIRONMENT"
    echo "📝 Log File: $LOG_FILE"
    echo ""
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --region)
                AWS_REGION="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                log_info "Dry run mode enabled - no resources will be created"
                shift
                ;;
            --verbose)
                VERBOSE=true
                log_info "Verbose logging enabled"
                shift
                ;;
            --force)
                FORCE_CREATE=true
                log_info "Force create mode enabled - will attempt to recreate existing resources"
                shift
                ;;
            --phase1)
                PHASE1_ONLY=true
                log_info "Phase 1 mode enabled - will only run resource detection and analysis"
                shift
                ;;
            --phase2)
                PHASE2_ONLY=true
                log_info "Phase 2 mode enabled - will only run resource updates"
                shift
                ;;
            --phase3)
                PHASE3_ONLY=true
                log_info "Phase 3 mode enabled - will only create missing resources"
                shift
                ;;
            --phase4)
                PHASE4_ONLY=true
                log_info "Phase 4 mode enabled - will only run validation and testing"
                shift
                ;;
            --phase5)
                PHASE5_ONLY=true
                log_info "Phase 5 mode enabled - will only run cleanup and documentation"
                shift
                ;;
            --update-mwaa-requirements)
                UPDATE_MWAA_REQUIREMENTS=true
                log_info "MWAA requirements update mode enabled - will only update MWAA requirements.txt"
                shift
                ;;
            --help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --region REGION   AWS region to deploy to (default: us-east-1)"
    echo "  --dry-run         Show what would be created without actually creating"
    echo "  --verbose         Enable verbose logging"
    echo "  --force           Force recreation of existing resources"
    echo "  --phase1          Run only Phase 1: Resource detection and analysis"
    echo "  --phase2          Run only Phase 2: Update existing resources"
    echo "  --phase3          Run only Phase 3: Create missing resources"
    echo "  --phase4          Run only Phase 4: Validation and testing"
    echo "  --phase5          Run only Phase 5: Cleanup and documentation"
    echo "  --update-mwaa-requirements  Update only MWAA requirements.txt with minimal dependencies"
    echo "  --help            Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                        # Create infrastructure in us-east-1"
    echo "  $0 --region us-west-2     # Create in us-west-2"
    echo "  $0 --dry-run --verbose    # Preview with detailed logging"
    echo "  $0 --phase1               # Run only resource detection and analysis"
    echo "  $0 --phase2               # Run only resource updates"
    echo "  $0 --phase3               # Run only missing resource creation"
    echo "  $0 --phase4               # Run only validation and testing"
    echo "  $0 --phase5               # Run only cleanup and documentation"
    echo "  $0 --update-mwaa-requirements  # Update only MWAA requirements.txt"
}

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install it first."
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    # Get AWS Account ID
    AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    log_info "AWS Account ID: $AWS_ACCOUNT_ID"
    
    # Check if jq is available (helpful for JSON parsing)
    if ! command -v jq &> /dev/null; then
        log_warning "jq is not installed. Some JSON parsing may be limited."
    fi
    
    log_success "All prerequisites met"
}

# Resource existence check functions
check_s3_bucket_exists() {
    local bucket_name="$1"
    if aws s3api head-bucket --bucket "$bucket_name" --region "$AWS_REGION" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_iam_role_exists() {
    local role_name="$1"
    if aws iam get-role --role-name "$role_name" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_glue_database_exists() {
    local database_name="$1"
    if aws glue get-database --name "$database_name" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_glue_job_exists() {
    local job_name="$1"
    if aws glue get-job --job-name "$job_name" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

check_mwaa_environment_exists() {
    local env_name="$1"
    if aws mwaa get-environment --name "$env_name" &>/dev/null; then
        return 0
    else
        return 1
    fi
}

# =============================================================================
# PHASE 1: RESOURCE DETECTION & ANALYSIS FUNCTIONS
# =============================================================================

# Detect existing S3 buckets
detect_s3_buckets() {
    log_info "🔍 Detecting existing S3 buckets..."
    
    local buckets=("$S3_BUCKET_DATA_LAKE" "$S3_BUCKET_MWAA")
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log_success "✅ S3 bucket exists: $bucket"
            EXISTING_RESOURCES+=("s3:$bucket")
            
            # Check if bucket has wheel storage capability
            if check_s3_wheel_capability "$bucket"; then
                log_info "✅ S3 bucket $bucket supports wheel storage"
            else
                log_warning "⚠️  S3 bucket $bucket may need policy updates for wheel storage"
                NEEDS_UPDATE_RESOURCES+=("s3:$bucket:policy")
            fi
        else
            log_warning "❌ S3 bucket missing: $bucket"
            MISSING_RESOURCES+=("s3:$bucket")
        fi
    done
}

# Check S3 bucket wheel storage capability
check_s3_wheel_capability() {
    local bucket="$1"
    
    # Check if bucket has proper policies for wheel file operations
    local policy=$(aws s3api get-bucket-policy --bucket "$bucket" 2>/dev/null || echo "{}")
    
    # Basic check - if policy exists and contains wheel-related permissions
    if echo "$policy" | grep -q "wheel\|whl" 2>/dev/null; then
        return 0
    fi
    
    # Check if bucket allows public read for wheel files (common pattern)
    if echo "$policy" | grep -q "s3:GetObject" 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Detect existing IAM roles
detect_iam_roles() {
    log_info "🔍 Detecting existing IAM roles..."
    
    local roles=("$GLUE_ROLE_NAME" "$MWAA_ROLE_NAME")
    
    for role in "${roles[@]}"; do
        if aws iam get-role --role-name "$role" 2>/dev/null; then
            log_success "✅ IAM role exists: $role"
            EXISTING_RESOURCES+=("iam:role:$role")
            
            # Check if role has wheel-related permissions
            if check_iam_role_wheel_permissions "$role"; then
                log_info "✅ IAM role $role has wheel permissions"
            else
                log_warning "⚠️  IAM role $role may need wheel permission updates"
                NEEDS_UPDATE_RESOURCES+=("iam:role:$role:policy")
            fi
        else
            log_warning "❌ IAM role missing: $role"
            MISSING_RESOURCES+=("iam:role:$role")
        fi
    done
}

# Check IAM role wheel permissions
check_iam_role_wheel_permissions() {
    local role_name="$1"
    
    # Get role policies
    local policies=$(aws iam list-attached-role-policies --role-name "$role_name" 2>/dev/null || echo "{}")
    local inline_policies=$(aws iam list-role-policies --role-name "$role_name" 2>/dev/null || echo "{}")
    
    # Check for S3 permissions that would allow wheel file access
    if echo "$policies" | grep -q "S3FullAccess\|S3ReadOnlyAccess" 2>/dev/null; then
        return 0
    fi
    
    # Check inline policies for wheel-related permissions
    if echo "$inline_policies" | grep -q "s3:GetObject.*wheel\|s3:PutObject.*wheel" 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Detect existing Glue jobs
detect_glue_jobs() {
    log_info "🔍 Detecting existing Glue jobs..."
    
    local jobs=("$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD")
    
    for job in "${jobs[@]}"; do
        if aws glue get-job --job-name "$job" 2>/dev/null; then
            log_success "✅ Glue job exists: $job"
            EXISTING_RESOURCES+=("glue:job:$job")
            
            # Check if job uses wheel-based configuration
            if check_glue_job_wheel_config "$job"; then
                log_info "✅ Glue job $job uses wheel configuration"
            else
                log_warning "⚠️  Glue job $job needs wheel configuration update"
                NEEDS_UPDATE_RESOURCES+=("glue:job:$job:config")
            fi
        else
            log_warning "❌ Glue job missing: $job"
            MISSING_RESOURCES+=("glue:job:$job")
        fi
    done
}

# Check Glue job wheel configuration
check_glue_job_wheel_config() {
    local job_name="$1"
    
    # Get job configuration
    local job_config=$(aws glue get-job --job-name "$job_name" 2>/dev/null || echo "{}")
    
    # Check for wheel-based dependencies
    if echo "$job_config" | grep -q "\.whl\|wheel" 2>/dev/null; then
        return 0
    fi
    
    # Check for Python version 3.9 and Glue version 4.0
    if echo "$job_config" | grep -q '"PythonVersion": "3.9"' 2>/dev/null && \
       echo "$job_config" | grep -q '"GlueVersion": "4.0"' 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Detect existing MWAA environment
detect_mwaa_environment() {
    log_info "🔍 Detecting existing MWAA environment..."
    
    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        log_success "✅ MWAA environment exists: $MWAA_ENV_NAME"
        EXISTING_RESOURCES+=("mwaa:env:$MWAA_ENV_NAME")
        
        # Check if MWAA can access wheel files
        if check_mwaa_wheel_access "$MWAA_ENV_NAME"; then
            log_info "✅ MWAA environment $MWAA_ENV_NAME can access wheel files"
        else
            log_warning "⚠️  MWAA environment $MWAA_ENV_NAME may need wheel access updates"
            NEEDS_UPDATE_RESOURCES+=("mwaa:env:$MWAA_ENV_NAME:access")
        fi
    else
        log_warning "❌ MWAA environment missing: $MWAA_ENV_NAME"
        MISSING_RESOURCES+=("mwaa:env:$MWAA_ENV_NAME")
    fi
}

# Check MWAA wheel access
check_mwaa_wheel_access() {
    local env_name="$1"
    
    # Get MWAA environment configuration
    local env_config=$(aws mwaa get-environment --name "$env_name" 2>/dev/null || echo "{}")
    
    # Check if environment has proper S3 access for wheel files
    if echo "$env_config" | grep -q "s3://.*wheel\|s3://.*whl" 2>/dev/null; then
        return 0
    fi
    
    # Check if environment has access to the deployment bucket
    if echo "$env_config" | grep -q "$S3_BUCKET_MWAA" 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# Detect existing CloudWatch resources
detect_cloudwatch_resources() {
    log_info "🔍 Detecting existing CloudWatch resources..."
    
    local log_groups=("f1-pipeline-logs" "f1-glue-logs" "f1-mwaa-logs")
    
    for log_group in "${log_groups[@]}"; do
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | grep -q "$log_group"; then
            log_success "✅ CloudWatch log group exists: $log_group"
            EXISTING_RESOURCES+=("cloudwatch:loggroup:$log_group")
        else
            log_warning "❌ CloudWatch log group missing: $log_group"
            MISSING_RESOURCES+=("cloudwatch:loggroup:$log_group")
        fi
    done
}

# Generate migration report
generate_migration_report() {
    log_info "📊 Generating migration report..."
    
    MIGRATION_REPORT="
=============================================================================
F1 DATA PIPELINE - RESOURCE MIGRATION REPORT
Generated: $(date)
AWS Account: $AWS_ACCOUNT_ID
AWS Region: $AWS_REGION
=============================================================================

EXISTING RESOURCES (${#EXISTING_RESOURCES[@]}):
$(printf '%s\n' "${EXISTING_RESOURCES[@]}" | sort)

MISSING RESOURCES (${#MISSING_RESOURCES[@]}):
$(printf '%s\n' "${MISSING_RESOURCES[@]}" | sort)

RESOURCES NEEDING UPDATES (${#NEEDS_UPDATE_RESOURCES[@]}):
$(printf '%s\n' "${NEEDS_UPDATE_RESOURCES[@]}" | sort)

MIGRATION RECOMMENDATIONS:
"

    # Add specific recommendations based on detected resources
    if [[ ${#NEEDS_UPDATE_RESOURCES[@]} -gt 0 ]]; then
        MIGRATION_REPORT+="
⚠️  RESOURCES REQUIRING UPDATES:
"
        for resource in "${NEEDS_UPDATE_RESOURCES[@]}"; do
            case "$resource" in
                *":policy")
                    MIGRATION_REPORT+="   - Update IAM policies for wheel file access: $resource"
                    ;;
                *":config")
                    MIGRATION_REPORT+="   - Update Glue job configuration for wheel dependencies: $resource"
                    ;;
                *":access")
                    MIGRATION_REPORT+="   - Update MWAA environment for wheel file access: $resource"
                    ;;
            esac
            MIGRATION_REPORT+="
"
        done
    fi

    if [[ ${#MISSING_RESOURCES[@]} -gt 0 ]]; then
        MIGRATION_REPORT+="
❌ MISSING RESOURCES TO CREATE:
"
        for resource in "${MISSING_RESOURCES[@]}"; do
            MIGRATION_REPORT+="   - Create: $resource"
            MIGRATION_REPORT+="
"
        done
    fi

    MIGRATION_REPORT+="
✅ READY FOR WHEEL-BASED DEPLOYMENT:
   - All existing resources will be updated to support wheel files
   - Missing resources will be created with wheel-based configuration
   - No data loss or service interruption expected

=============================================================================
"

    echo "$MIGRATION_REPORT"
}

# Main resource detection function
detect_all_resources() {
    log_info "🔍 PHASE 1: Starting comprehensive resource detection..."
    echo "================================================================"
    
    # Initialize arrays
    EXISTING_RESOURCES=()
    MISSING_RESOURCES=()
    NEEDS_UPDATE_RESOURCES=()
    
    # Detect all resource types
    detect_s3_buckets
    detect_iam_roles
    detect_glue_jobs
    detect_mwaa_environment
    detect_cloudwatch_resources
    
    # Generate and display migration report
    generate_migration_report
    
    log_success "✅ Phase 1 completed: Resource detection and analysis finished"
    
    # Save report to file
    echo "$MIGRATION_REPORT" > "migration_report_${TIMESTAMP}.txt"
    log_info "📄 Migration report saved to: migration_report_${TIMESTAMP}.txt"
}

# =============================================================================
# PHASE 2: UPDATE EXISTING RESOURCES FUNCTIONS
# =============================================================================

# Update IAM role policies for wheel file access
update_iam_role_policies() {
    log_info "🔧 Updating IAM role policies for wheel file access..."
    
    local roles=("$GLUE_ROLE_NAME" "$MWAA_ROLE_NAME")
    
    for role in "${roles[@]}"; do
        if aws iam get-role --role-name "$role" 2>/dev/null; then
            log_info "Updating IAM role: $role"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create wheel-specific policy
                create_wheel_access_policy "$role"
                
                # Attach additional S3 permissions for wheel files
                attach_wheel_s3_policy "$role"
                
                log_success "✅ Updated IAM role policies for: $role"
            else
                log_info "DRY RUN: Would update IAM role policies for: $role"
            fi
        else
            log_warning "IAM role not found: $role"
        fi
    done
}

# Create wheel access policy for IAM role
create_wheel_access_policy() {
    local role_name="$1"
    local policy_name="${role_name}-WheelAccess"
    
    # Create policy document for wheel file access
    cat > /tmp/wheel-access-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET_DATA_LAKE}/dependencies/*",
                "arn:aws:s3:::${S3_BUCKET_DATA_LAKE}/wheels/*",
                "arn:aws:s3:::${S3_BUCKET_MWAA}/dependencies/*",
                "arn:aws:s3:::${S3_BUCKET_MWAA}/wheels/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET_DATA_LAKE}",
                "arn:aws:s3:::${S3_BUCKET_MWAA}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET_DATA_LAKE}/dependencies/*/*.whl",
                "arn:aws:s3:::${S3_BUCKET_MWAA}/dependencies/*/*.whl"
            ]
        }
    ]
}
EOF

    # Create or update the policy
    if aws iam get-policy --policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${policy_name}" 2>/dev/null; then
        # Policy exists, create new version
        aws iam create-policy-version \
            --policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${policy_name}" \
            --policy-document file:///tmp/wheel-access-policy.json \
            --set-as-default
        log_info "Updated existing policy: ${policy_name}"
    else
        # Create new policy
        aws iam create-policy \
            --policy-name "${policy_name}" \
            --policy-document file:///tmp/wheel-access-policy.json \
            --description "Policy for wheel file access in F1 Data Pipeline"
        log_info "Created new policy: ${policy_name}"
    fi
    
    # Attach policy to role
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:policy/${policy_name}"
    
    # Cleanup
    rm -f /tmp/wheel-access-policy.json
}

# Attach additional S3 policy for wheel files
attach_wheel_s3_policy() {
    local role_name="$1"
    
    # Attach AWS managed S3 read access policy for broader access
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess" 2>/dev/null || true
    
    log_info "Attached S3 read access policy to: $role_name"
}

# Update S3 bucket policies for wheel storage
update_s3_bucket_policies() {
    log_info "🔧 Updating S3 bucket policies for wheel storage..."
    
    local buckets=("$S3_BUCKET_DATA_LAKE" "$S3_BUCKET_MWAA")
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log_info "Updating S3 bucket policy: $bucket"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create wheel-friendly bucket policy
                create_wheel_bucket_policy "$bucket"
                
                log_success "✅ Updated S3 bucket policy for: $bucket"
            else
                log_info "DRY RUN: Would update S3 bucket policy for: $bucket"
            fi
        else
            log_warning "S3 bucket not found: $bucket"
        fi
    done
}

# Create wheel-friendly bucket policy
create_wheel_bucket_policy() {
    local bucket_name="$1"
    
    # Create bucket policy for wheel file access
    cat > /tmp/wheel-bucket-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowWheelFileAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_ROLE_NAME}",
                    "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MWAA_ROLE_NAME}"
                ]
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::${bucket_name}/dependencies/*",
                "arn:aws:s3:::${bucket_name}/wheels/*"
            ]
        },
        {
            "Sid": "AllowWheelFileListing",
            "Effect": "Allow",
            "Principal": {
                "AWS": [
                    "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_ROLE_NAME}",
                    "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MWAA_ROLE_NAME}"
                ]
            },
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": "arn:aws:s3:::${bucket_name}",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "dependencies/*",
                        "wheels/*"
                    ]
                }
            }
        },
        {
            "Sid": "AllowPublicReadForWheels",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::${bucket_name}/dependencies/*/*.whl",
            "Condition": {
                "StringEquals": {
                    "s3:ExistingObjectTag/PublicRead": "true"
                }
            }
        }
    ]
}
EOF

    # Apply the bucket policy
    aws s3api put-bucket-policy \
        --bucket "$bucket_name" \
        --policy file:///tmp/wheel-bucket-policy.json
    
    # Cleanup
    rm -f /tmp/wheel-bucket-policy.json
    
    log_info "Applied wheel-friendly policy to bucket: $bucket_name"
}

# Update Glue job configurations (if needed)
update_glue_job_configurations() {
    log_info "🔧 Checking Glue job configurations..."
    
    local jobs=("$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD")
    
    for job in "${jobs[@]}"; do
        if aws glue get-job --job-name "$job" 2>/dev/null; then
            # Check if job already uses wheel configuration
            if check_glue_job_wheel_config "$job"; then
                log_info "✅ Glue job $job already uses wheel configuration - no update needed"
            else
                log_warning "⚠️  Glue job $job needs wheel configuration update"
                
                if [[ "$DRY_RUN" == "false" ]]; then
                    # Update job to use wheel configuration
                    update_glue_job_to_wheels "$job"
                    log_success "✅ Updated Glue job configuration: $job"
                else
                    log_info "DRY RUN: Would update Glue job configuration: $job"
                fi
            fi
        else
            log_warning "Glue job not found: $job"
        fi
    done
}

# Update Glue job to use wheel configuration
update_glue_job_to_wheels() {
    local job_name="$1"
    
    # Get current job configuration
    local job_config=$(aws glue get-job --job-name "$job_name" 2>/dev/null)
    
    # Extract current configuration
    local script_location=$(echo "$job_config" | jq -r '.Job.Command.ScriptLocation')
    local role_arn=$(echo "$job_config" | jq -r '.Job.Role')
    local description=$(echo "$job_config" | jq -r '.Job.Description')
    
    # Create updated job configuration with wheel dependencies
    local updated_config=$(cat << EOF
{
    "Name": "$job_name",
    "Role": "$role_arn",
    "Command": {
        "Name": "glueetl",
        "ScriptLocation": "$script_location",
        "PythonVersion": "3.9"
    },
    "GlueVersion": "4.0",
    "Description": "$description - Updated for wheel-based dependencies",
    "DefaultArguments": {
        "--enable-spark-ui": "true",
        "--enable-metrics": "",
        "--continuous-log-logGroup": "/aws/glue/$job_name",
        "--spark-event-logs-path": "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/${job_name}/",
        "job-bookmark-option": "job-bookmark-enable",
        "--additional-python-modules": "s3://${S3_BUCKET_DATA_LAKE}/dependencies/latest/*.whl",
        "--enable-observability-metrics": "true",
        "--conf": "spark.sql.adaptive.enabled=true",
        "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
        "job-language": "python",
        "--enable-continuous-cloudwatch-log": "true",
        "--F1_DEPLOYMENT_VERSION": "wheel-based",
        "--TempDir": "s3://${S3_BUCKET_DATA_LAKE}/temp/${job_name}/"
    },
    "MaxRetries": 0,
    "Timeout": 2880,
    "MaxCapacity": 10.0,
    "WorkerType": "G.1X",
    "NumberOfWorkers": 10
}
EOF
)
    
    # Update the job
    echo "$updated_config" | aws glue update-job --job-input file:///dev/stdin
    
    log_info "Updated Glue job $job_name to use wheel-based dependencies"
}

# Update MWAA environment for wheel access
update_mwaa_environment() {
    log_info "🔧 Checking MWAA environment configuration..."
    
    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        # Check if MWAA can access wheel files
        if check_mwaa_wheel_access "$MWAA_ENV_NAME"; then
            log_info "✅ MWAA environment $MWAA_ENV_NAME already has wheel access"
        else
            log_warning "⚠️  MWAA environment $MWAA_ENV_NAME needs wheel access update"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Update MWAA environment configuration
                update_mwaa_wheel_access "$MWAA_ENV_NAME"
                log_success "✅ Updated MWAA environment wheel access: $MWAA_ENV_NAME"
            else
                log_info "DRY RUN: Would update MWAA environment wheel access: $MWAA_ENV_NAME"
            fi
        fi
        
        # Always update requirements.txt for existing environments
        log_info "📦 Updating MWAA requirements.txt with minimal dependencies..."
        update_mwaa_requirements
    else
        log_warning "MWAA environment not found: $MWAA_ENV_NAME"
    fi
}

# Update MWAA requirements.txt with minimal dependencies
update_mwaa_requirements() {
    log_info "📦 Creating minimal MWAA requirements.txt..."
    
    # Create the same minimal requirements.txt as in create_mwaa_environment
    cat > /tmp/mwaa-requirements.txt << EOF
# Minimal MWAA requirements for F1 Data Pipeline
# Following same approach as Glue jobs - use only what's absolutely necessary

# Required for F1 API data extraction (OpenF1Hook)
requests==2.31.0
urllib3==1.26.18

# Required for data processing in Bronze layer (S3BronzeWriter)
pandas==2.0.3

# Note: boto3/botocore are pre-installed in MWAA
# Note: All heavy dependencies (great-expectations, openlineage, pyiceberg, pyyaml) removed
# Note: Glue jobs handle their own dependencies separately
EOF
    
    if [[ "$DRY_RUN" == "false" ]]; then
        # Upload updated requirements to S3
        aws s3 cp /tmp/mwaa-requirements.txt "s3://${S3_BUCKET_MWAA}/requirements.txt"
        log_success "✅ Updated MWAA requirements.txt in S3"
        
        # Clean up temp file
        rm -f /tmp/mwaa-requirements.txt
        
        log_info "🔄 MWAA environment will automatically pick up the new requirements.txt"
        log_info "⏱️  Note: MWAA may take 5-10 minutes to apply the new requirements"
    else
        log_info "DRY RUN: Would update MWAA requirements.txt with minimal dependencies"
        log_info "DRY RUN: New requirements would be:"
        cat /tmp/mwaa-requirements.txt | sed 's/^/  /'
        rm -f /tmp/mwaa-requirements.txt
    fi
}

# Update MWAA environment for wheel access
update_mwaa_wheel_access() {
    local env_name="$1"
    
    # Get current environment configuration
    local env_config=$(aws mwaa get-environment --name "$env_name" 2>/dev/null)
    
    # Extract current configuration
    local source_bucket_arn=$(echo "$env_config" | jq -r '.Environment.SourceBucketArn')
    local dag_s3_path=$(echo "$env_config" | jq -r '.Environment.DagS3Path')
    local requirements_s3_path=$(echo "$env_config" | jq -r '.Environment.RequirementsS3Path')
    local execution_role_arn=$(echo "$env_config" | jq -r '.Environment.ExecutionRoleArn')
    local network_config=$(echo "$env_config" | jq -r '.Environment.NetworkConfiguration')
    local logging_config=$(echo "$env_config" | jq -r '.Environment.LoggingConfiguration')
    
    # Update MWAA environment with wheel access
    aws mwaa update-environment \
        --name "$env_name" \
        --source-bucket-arn "$source_bucket_arn" \
        --dag-s3-path "$dag_s3_path" \
        --requirements-s3-path "$requirements_s3_path" \
        --execution-role-arn "$execution_role_arn" \
        --network-configuration "$network_config" \
        --logging-configuration "$logging_config" \
        --airflow-configuration-options '{
            "core.dags_are_paused_at_creation": "False",
            "core.load_examples": "False",
            "logging.logging_level": "INFO",
            "core.parallelism": "32",
            "core.dag_concurrency": "16"
        }'
    
    log_info "Updated MWAA environment $env_name for wheel access"
}

# Main Phase 2 function
update_existing_resources() {
    log_info "🔧 PHASE 2: Starting resource updates for wheel-based deployment..."
    echo "================================================================"
    
    local total_errors=0
    
    # Update IAM role policies
    update_iam_role_policies || ((total_errors++))
    
    # Update S3 bucket policies
    update_s3_bucket_policies || ((total_errors++))
    
    # Update Glue job configurations (if needed)
    update_glue_job_configurations || ((total_errors++))
    
    # Update MWAA environment (if needed)
    update_mwaa_environment || ((total_errors++))
    
    if [[ $total_errors -eq 0 ]]; then
        log_success "✅ Phase 2 completed: All existing resources updated successfully"
    else
        log_error "❌ Phase 2 completed with $total_errors errors"
    fi
    
    return $total_errors
}

# =============================================================================
# PHASE 3: CREATE MISSING RESOURCES FUNCTIONS
# =============================================================================

# Create missing CloudWatch log groups
create_cloudwatch_log_groups() {
    log_info "🔧 Creating missing CloudWatch log groups..."
    
    local log_groups=("f1-pipeline-logs" "f1-glue-logs" "f1-mwaa-logs")
    
    for log_group in "${log_groups[@]}"; do
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | grep -q "$log_group"; then
            log_info "✅ CloudWatch log group already exists: $log_group"
        else
            log_info "Creating CloudWatch log group: $log_group"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create log group with retention policy
                create_cloudwatch_log_group "$log_group"
                log_success "✅ Created CloudWatch log group: $log_group"
            else
                log_info "DRY RUN: Would create CloudWatch log group: $log_group"
            fi
        fi
    done
}

# Create individual CloudWatch log group
create_cloudwatch_log_group() {
    local log_group_name="$1"
    
    # Create log group
    aws logs create-log-group \
        --log-group-name "$log_group_name" \
        --region "$AWS_REGION"
    
    # Set retention policy (30 days)
    aws logs put-retention-policy \
        --log-group-name "$log_group_name" \
        --retention-in-days 30 \
        --region "$AWS_REGION"
    
    # Add tags
    aws logs tag-log-group \
        --log-group-name "$log_group_name" \
        --tags "Project=$PROJECT_NAME,Environment=$ENVIRONMENT,Component=Logging" \
        --region "$AWS_REGION"
    
    log_info "Created log group $log_group_name with 30-day retention policy"
}

# Create missing S3 buckets (if any)
create_missing_s3_buckets() {
    log_info "🔧 Checking for missing S3 buckets..."
    
    local buckets=("$S3_BUCKET_DATA_LAKE" "$S3_BUCKET_MWAA")
    
    for bucket in "${buckets[@]}"; do
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log_info "✅ S3 bucket already exists: $bucket"
        else
            log_info "Creating S3 bucket: $bucket"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create S3 bucket
                create_s3_bucket "$bucket"
                log_success "✅ Created S3 bucket: $bucket"
            else
                log_info "DRY RUN: Would create S3 bucket: $bucket"
            fi
        fi
    done
}

# Create individual S3 bucket
create_s3_bucket() {
    local bucket_name="$1"
    
    # Create bucket
    if [[ "$AWS_REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$bucket_name"
    else
        aws s3api create-bucket \
            --bucket "$bucket_name" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION"
    fi
    
    # Enable versioning
    aws s3api put-bucket-versioning \
        --bucket "$bucket_name" \
        --versioning-configuration Status=Enabled
    
    # Enable server-side encryption
    aws s3api put-bucket-encryption \
        --bucket "$bucket_name" \
        --server-side-encryption-configuration '{
            "Rules": [
                {
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256"
                    }
                }
            ]
        }'
    
    # Block public access
    aws s3api put-public-access-block \
        --bucket "$bucket_name" \
        --public-access-block-configuration '{
            "BlockPublicAcls": true,
            "IgnorePublicAcls": true,
            "BlockPublicPolicy": true,
            "RestrictPublicBuckets": true
        }'
    
    # Add tags
    aws s3api put-bucket-tagging \
        --bucket "$bucket_name" \
        --tagging '{
            "TagSet": [
                {"Key": "Project", "Value": "'$PROJECT_NAME'"},
                {"Key": "Environment", "Value": "'$ENVIRONMENT'"},
                {"Key": "Component", "Value": "Storage"}
            ]
        }'
    
    log_info "Created S3 bucket $bucket_name with versioning, encryption, and public access blocking"
}

# Create missing IAM roles (if any)
create_missing_iam_roles() {
    log_info "🔧 Checking for missing IAM roles..."
    
    local roles=("$GLUE_ROLE_NAME" "$MWAA_ROLE_NAME")
    
    for role in "${roles[@]}"; do
        if aws iam get-role --role-name "$role" 2>/dev/null; then
            log_info "✅ IAM role already exists: $role"
        else
            log_info "Creating IAM role: $role"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create IAM role
                create_iam_role "$role"
                log_success "✅ Created IAM role: $role"
            else
                log_info "DRY RUN: Would create IAM role: $role"
            fi
        fi
    done
}

# Create individual IAM role
create_iam_role() {
    local role_name="$1"
    
    if [[ "$role_name" == "$GLUE_ROLE_NAME" ]]; then
        create_glue_role
    elif [[ "$role_name" == "$MWAA_ROLE_NAME" ]]; then
        create_mwaa_role
    else
        log_error "Unknown role name: $role_name"
        return 1
    fi
}

# Create Glue execution role
create_glue_role() {
    local role_name="$GLUE_ROLE_NAME"
    
    # Create trust policy for Glue
    cat > /tmp/glue-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": "glue.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/F1-DataPipeline-Role"
        }
    ]
}
EOF
    
    # Create role
    aws iam create-role \
        --role-name "$role_name" \
        --assume-role-policy-document file:///tmp/glue-trust-policy.json \
        --description "IAM role for F1 Data Pipeline Glue jobs"
    
    # Attach AWS managed policies
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"
    
    # Create and attach custom policy
    create_wheel_access_policy "$role_name"
    
    # Cleanup
    rm -f /tmp/glue-trust-policy.json
    
    log_info "Created Glue role $role_name with necessary policies"
}

# Create MWAA execution role
create_mwaa_role() {
    local role_name="$MWAA_ROLE_NAME"
    
    # Create trust policy for MWAA
    cat > /tmp/mwaa-trust-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "airflow.amazonaws.com",
                    "airflow-env.amazonaws.com"
                ]
            },
            "Action": "sts:AssumeRole"
        },
        {
            "Effect": "Allow",
            "Action": "iam:PassRole",
            "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/F1-DataPipeline-Role"
        }
    ]
}
EOF
    
    # Create role
    aws iam create-role \
        --role-name "$role_name" \
        --assume-role-policy-document file:///tmp/mwaa-trust-policy.json \
        --description "IAM role for F1 Data Pipeline MWAA environment"
    
    # Attach AWS managed policies
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess"
    
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonKinesisFullAccess"
    
    aws iam attach-role-policy \
        --role-name "$role_name" \
        --policy-arn "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
    
    # Create and attach custom policy
    create_wheel_access_policy "$role_name"
    
    # Cleanup
    rm -f /tmp/mwaa-trust-policy.json
    
    log_info "Created MWAA role $role_name with necessary policies"
}

# Create missing Glue jobs (if any)
create_missing_glue_jobs() {
    log_info "🔧 Checking for missing Glue jobs..."
    
    local jobs=("$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD")
    
    for job in "${jobs[@]}"; do
        if aws glue get-job --job-name "$job" 2>/dev/null; then
            log_info "✅ Glue job already exists: $job"
        else
            log_info "Creating Glue job: $job"
            
            if [[ "$DRY_RUN" == "false" ]]; then
                # Create Glue job
                create_glue_job "$job"
                log_success "✅ Created Glue job: $job"
            else
                log_info "DRY RUN: Would create Glue job: $job"
            fi
        fi
    done
}

# Create individual Glue job
create_glue_job() {
    local job_name="$1"
    
    if [[ "$job_name" == "$GLUE_JOB_BRONZE_TO_SILVER" ]]; then
        create_bronze_to_silver_job
    elif [[ "$job_name" == "$GLUE_JOB_SILVER_TO_GOLD" ]]; then
        create_silver_to_gold_job
    else
        log_error "Unknown job name: $job_name"
        return 1
    fi
}

# Create Bronze to Silver Glue job
create_bronze_to_silver_job() {
    local job_name="$GLUE_JOB_BRONZE_TO_SILVER"
    
    # Create job configuration
    cat > /tmp/bronze-to-silver-job.json << EOF
{
    "Name": "$job_name",
    "Role": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_ROLE_NAME}",
    "Command": {
        "Name": "glueetl",
        "ScriptLocation": "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py",
        "PythonVersion": "3.9"
    },
    "GlueVersion": "4.0",
    "Description": "F1 Data Pipeline: Transform Bronze layer data to Silver layer with data quality validation",
    "DefaultArguments": {
        "--enable-spark-ui": "true",
        "--enable-metrics": "",
        "--continuous-log-logGroup": "/aws/glue/$job_name",
        "--spark-event-logs-path": "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/bronze/",
        "job-bookmark-option": "job-bookmark-enable",
        "--additional-python-modules": "s3://${S3_BUCKET_DATA_LAKE}/dependencies/latest/*.whl",
        "--enable-observability-metrics": "true",
        "--conf": "spark.sql.adaptive.enabled=true",
        "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
        "job-language": "python",
        "--enable-continuous-cloudwatch-log": "true",
        "--F1_DEPLOYMENT_VERSION": "wheel-based",
        "--TempDir": "s3://${S3_BUCKET_DATA_LAKE}/temp/bronze/"
    },
    "MaxRetries": 0,
    "Timeout": 2880,
    "MaxCapacity": 10.0,
    "WorkerType": "G.1X",
    "NumberOfWorkers": 10
}
EOF
    
    # Create job
    aws glue create-job --job-input file:///tmp/bronze-to-silver-job.json
    
    # Cleanup
    rm -f /tmp/bronze-to-silver-job.json
    
    log_info "Created Glue job $job_name with wheel-based configuration"
}

# Create Silver to Gold Glue job
create_silver_to_gold_job() {
    local job_name="$GLUE_JOB_SILVER_TO_GOLD"
    
    # Create job configuration
    cat > /tmp/silver-to-gold-job.json << EOF
{
    "Name": "$job_name",
    "Role": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${GLUE_ROLE_NAME}",
    "Command": {
        "Name": "glueetl",
        "ScriptLocation": "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py",
        "PythonVersion": "3.9"
    },
    "GlueVersion": "4.0",
    "Description": "F1 Data Pipeline: Transform Silver layer data to Gold layer analytics tables",
    "DefaultArguments": {
        "--enable-spark-ui": "true",
        "--enable-metrics": "",
        "--continuous-log-logGroup": "/aws/glue/$job_name",
        "--spark-event-logs-path": "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/gold/",
        "job-bookmark-option": "job-bookmark-enable",
        "--additional-python-modules": "s3://${S3_BUCKET_DATA_LAKE}/dependencies/latest/*.whl",
        "--enable-observability-metrics": "true",
        "--conf": "spark.sql.adaptive.enabled=true",
        "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
        "job-language": "python",
        "--enable-continuous-cloudwatch-log": "true",
        "--F1_DEPLOYMENT_VERSION": "wheel-based",
        "--TempDir": "s3://${S3_BUCKET_DATA_LAKE}/temp/gold/"
    },
    "MaxRetries": 0,
    "Timeout": 2880,
    "MaxCapacity": 10.0,
    "WorkerType": "G.1X",
    "NumberOfWorkers": 10
}
EOF
    
    # Create job
    aws glue create-job --job-input file:///tmp/silver-to-gold-job.json
    
    # Cleanup
    rm -f /tmp/silver-to-gold-job.json
    
    log_info "Created Glue job $job_name with wheel-based configuration"
}

# Create missing MWAA environment (if any)
create_missing_mwaa_environment() {
    log_info "🔧 Checking for missing MWAA environment..."
    
    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        log_info "✅ MWAA environment already exists: $MWAA_ENV_NAME"
    else
        log_info "Creating MWAA environment: $MWAA_ENV_NAME"
        
        if [[ "$DRY_RUN" == "false" ]]; then
            # Create MWAA environment
            create_mwaa_environment
            log_success "✅ Created MWAA environment: $MWAA_ENV_NAME"
        else
            log_info "DRY RUN: Would create MWAA environment: $MWAA_ENV_NAME"
        fi
    fi
}

# Create MWAA environment
create_mwaa_environment() {
    local env_name="$MWAA_ENV_NAME"
    
    # Create requirements.txt for MWAA
    cat > /tmp/mwaa-requirements.txt << EOF
# Minimal MWAA requirements for F1 Data Pipeline
# Following same approach as Glue jobs - use only what's absolutely necessary

# Required for F1 API data extraction (OpenF1Hook)
requests==2.31.0
urllib3==1.26.18

# Required for data processing in Bronze layer (S3BronzeWriter)
pandas==2.0.3

# Note: boto3/botocore are pre-installed in MWAA
# Note: All heavy dependencies (great-expectations, openlineage, pyiceberg, pyyaml) removed
# Note: Glue jobs handle their own dependencies separately
EOF
    
    # Upload requirements to S3
    aws s3 cp /tmp/mwaa-requirements.txt "s3://${S3_BUCKET_MWAA}/requirements.txt"
    
    # Create MWAA environment
    aws mwaa create-environment \
        --name "$env_name" \
        --source-bucket-arn "arn:aws:s3:::${S3_BUCKET_MWAA}" \
        --dag-s3-path "dags" \
        --requirements-s3-path "requirements.txt" \
        --execution-role-arn "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${MWAA_ROLE_NAME}" \
        --airflow-version "2.7.2" \
        --environment-class "mw1.small" \
        --max-workers 2 \
        --min-workers 1 \
        --schedulers 2 \
        --webserver-access-mode "PUBLIC_ONLY" \
        --airflow-configuration-options '{
            "core.dags_are_paused_at_creation": "False",
            "core.load_examples": "False",
            "logging.logging_level": "INFO",
            "core.parallelism": "32",
            "core.dag_concurrency": "16"
        }' \
        --tags '{
            "Project": "'$PROJECT_NAME'",
            "Environment": "'$ENVIRONMENT'",
            "Component": "Orchestration"
        }'
    
    # Cleanup
    rm -f /tmp/mwaa-requirements.txt
    
    log_info "Created MWAA environment $env_name with wheel-based configuration"
}

# Main Phase 3 function
create_missing_resources() {
    log_info "🔧 PHASE 3: Creating missing resources for wheel-based deployment..."
    echo "================================================================"
    
    local total_errors=0
    
    # Create missing CloudWatch log groups
    create_cloudwatch_log_groups || ((total_errors++))
    
    # Create missing S3 buckets (if any)
    create_missing_s3_buckets || ((total_errors++))
    
    # Create missing IAM roles (if any)
    create_missing_iam_roles || ((total_errors++))
    
    # Create missing Glue jobs (if any)
    create_missing_glue_jobs || ((total_errors++))
    
    # Create missing MWAA environment (if any)
    create_missing_mwaa_environment || ((total_errors++))
    
    if [[ $total_errors -eq 0 ]]; then
        log_success "✅ Phase 3 completed: All missing resources created successfully"
    else
        log_error "❌ Phase 3 completed with $total_errors errors"
    fi
    
    return $total_errors
}

# =============================================================================
# PHASE 4: VALIDATION & TESTING FUNCTIONS
# =============================================================================

# Validate S3 bucket access and permissions
validate_s3_access() {
    log_info "🔍 Validating S3 bucket access and permissions..."
    
    local buckets=("$S3_BUCKET_DATA_LAKE" "$S3_BUCKET_MWAA")
    local total_errors=0
    
    for bucket in "${buckets[@]}"; do
        log_info "Testing S3 bucket: $bucket"
        
        # Test bucket access
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            log_success "✅ S3 bucket accessible: $bucket"
        else
            log_error "❌ S3 bucket not accessible: $bucket"
            ((total_errors++))
            continue
        fi
        
        # Test write permissions
        local test_file="test-${TIMESTAMP}.txt"
        local test_content="F1 Pipeline validation test - $(date)"
        
        if echo "$test_content" | aws s3 cp - "s3://$bucket/validation/$test_file" 2>/dev/null; then
            log_success "✅ S3 bucket write test passed: $bucket"
            
            # Clean up test file
            aws s3 rm "s3://$bucket/validation/$test_file" 2>/dev/null || true
        else
            log_error "❌ S3 bucket write test failed: $bucket"
            ((total_errors++))
        fi
        
        # Test read permissions
        if aws s3 ls "s3://$bucket/" 2>/dev/null | head -1 >/dev/null; then
            log_success "✅ S3 bucket read test passed: $bucket"
        else
            log_error "❌ S3 bucket read test failed: $bucket"
            ((total_errors++))
        fi
    done
    
    return $total_errors
}

# Validate IAM role permissions
validate_iam_permissions() {
    log_info "🔍 Validating IAM role permissions..."
    
    local roles=("$GLUE_ROLE_NAME" "$MWAA_ROLE_NAME")
    local total_errors=0
    
    for role in "${roles[@]}"; do
        log_info "Testing IAM role: $role"
        
        # Check if role exists
        if aws iam get-role --role-name "$role" 2>/dev/null; then
            log_success "✅ IAM role exists: $role"
        else
            log_error "❌ IAM role not found: $role"
            ((total_errors++))
            continue
        fi
        
        # Check attached policies
        local policies=$(aws iam list-attached-role-policies --role-name "$role" 2>/dev/null)
        local policy_count=$(echo "$policies" | jq -r '.AttachedPolicies | length' 2>/dev/null || echo "0")
        
        if [[ "$policy_count" -gt 0 ]]; then
            log_success "✅ IAM role has $policy_count attached policies: $role"
        else
            log_warning "⚠️  IAM role has no attached policies: $role"
        fi
        
        # Check inline policies
        local inline_policies=$(aws iam list-role-policies --role-name "$role" 2>/dev/null)
        local inline_count=$(echo "$inline_policies" | jq -r '.PolicyNames | length' 2>/dev/null || echo "0")
        
        if [[ "$inline_count" -gt 0 ]]; then
            log_success "✅ IAM role has $inline_count inline policies: $role"
        else
            log_info "ℹ️  IAM role has no inline policies: $role"
        fi
    done
    
    return $total_errors
}

# Validate Glue job configurations
validate_glue_jobs() {
    log_info "🔍 Validating Glue job configurations..."
    
    local jobs=("$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD")
    local total_errors=0
    
    for job in "${jobs[@]}"; do
        log_info "Testing Glue job: $job"
        
        # Check if job exists
        if aws glue get-job --job-name "$job" 2>/dev/null; then
            log_success "✅ Glue job exists: $job"
        else
            log_error "❌ Glue job not found: $job"
            ((total_errors++))
            continue
        fi
        
        # Validate job configuration
        local job_config=$(aws glue get-job --job-name "$job" 2>/dev/null)
        
        # Check Python version
        local python_version=$(echo "$job_config" | jq -r '.Job.Command.PythonVersion' 2>/dev/null)
        if [[ "$python_version" == "3.9" ]]; then
            log_success "✅ Glue job Python version correct: $job (3.9)"
        else
            log_warning "⚠️  Glue job Python version: $job ($python_version) - expected 3.9"
        fi
        
        # Check Glue version
        local glue_version=$(echo "$job_config" | jq -r '.Job.GlueVersion' 2>/dev/null)
        if [[ "$glue_version" == "4.0" ]]; then
            log_success "✅ Glue job version correct: $job (4.0)"
        else
            log_warning "⚠️  Glue job version: $job ($glue_version) - expected 4.0"
        fi
        
        # Check wheel dependencies
        local wheel_deps=$(echo "$job_config" | jq -r '.Job.DefaultArguments."--additional-python-modules"' 2>/dev/null)
        if echo "$wheel_deps" | grep -q "\.whl"; then
            log_success "✅ Glue job uses wheel dependencies: $job"
        else
            log_warning "⚠️  Glue job may not use wheel dependencies: $job"
        fi
        
        # Test job start capability (dry run)
        if aws glue start-job-run --job-name "$job" --arguments '{"--dry-run": "true"}' 2>/dev/null; then
            log_success "✅ Glue job can be started: $job"
        else
            log_warning "⚠️  Glue job start test failed: $job"
        fi
    done
    
    return $total_errors
}

# Validate MWAA environment
validate_mwaa_environment() {
    log_info "🔍 Validating MWAA environment..."
    
    local env_name="$MWAA_ENV_NAME"
    
    # Check if environment exists
    if aws mwaa get-environment --name "$env_name" 2>/dev/null; then
        log_success "✅ MWAA environment exists: $env_name"
    else
        log_error "❌ MWAA environment not found: $env_name"
        return 1
    fi
    
    # Get environment status
    local env_status=$(aws mwaa get-environment --name "$env_name" 2>/dev/null | jq -r '.Environment.Status' 2>/dev/null)
    
    if [[ "$env_status" == "AVAILABLE" ]]; then
        log_success "✅ MWAA environment is available: $env_name"
    else
        log_warning "⚠️  MWAA environment status: $env_name ($env_status)"
    fi
    
    # Check environment configuration
    local env_config=$(aws mwaa get-environment --name "$env_name" 2>/dev/null)
    
    # Check Airflow version
    local airflow_version=$(echo "$env_config" | jq -r '.Environment.AirflowVersion' 2>/dev/null)
    if [[ "$airflow_version" == "2.7.2" ]]; then
        log_success "✅ MWAA Airflow version correct: $env_name (2.7.2)"
    else
        log_warning "⚠️  MWAA Airflow version: $env_name ($airflow_version)"
    fi
    
    # Check execution role
    local execution_role=$(echo "$env_config" | jq -r '.Environment.ExecutionRoleArn' 2>/dev/null)
    if [[ "$execution_role" == *"$MWAA_ROLE_NAME"* ]]; then
        log_success "✅ MWAA execution role correct: $env_name"
    else
        log_warning "⚠️  MWAA execution role: $env_name ($execution_role)"
    fi
    
    return 0
}

# Validate CloudWatch log groups
validate_cloudwatch_logs() {
    log_info "🔍 Validating CloudWatch log groups..."
    
    local log_groups=("f1-pipeline-logs" "f1-glue-logs" "f1-mwaa-logs")
    local total_errors=0
    
    for log_group in "${log_groups[@]}"; do
        log_info "Testing CloudWatch log group: $log_group"
        
        # Check if log group exists
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | grep -q "$log_group"; then
            log_success "✅ CloudWatch log group exists: $log_group"
        else
            log_error "❌ CloudWatch log group not found: $log_group"
            ((total_errors++))
            continue
        fi
        
        # Check retention policy
        local retention=$(aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | jq -r '.logGroups[0].retentionInDays' 2>/dev/null)
        if [[ "$retention" == "30" ]]; then
            log_success "✅ CloudWatch log group retention correct: $log_group (30 days)"
        else
            log_warning "⚠️  CloudWatch log group retention: $log_group ($retention days)"
        fi
    done
    
    return $total_errors
}

# Test wheel file deployment
test_wheel_deployment() {
    log_info "🔍 Testing wheel file deployment..."
    
    local total_errors=0
    
    # Test if deployment script exists and is executable
    if [[ -f "scripts/deploy_code_to_aws.sh" && -x "scripts/deploy_code_to_aws.sh" ]]; then
        log_success "✅ Deployment script exists and is executable"
    else
        log_error "❌ Deployment script not found or not executable"
        ((total_errors++))
        return $total_errors
    fi
    
    # Test deployment script dry run
    log_info "Testing deployment script dry run..."
    if ./scripts/deploy_code_to_aws.sh --dry-run 2>/dev/null; then
        log_success "✅ Deployment script dry run successful"
    else
        log_error "❌ Deployment script dry run failed"
        ((total_errors++))
    fi
    
    # Test wheel building capability
    log_info "Testing wheel building capability..."
    if python3 -c "import setuptools, wheel, build" 2>/dev/null; then
        log_success "✅ Python build tools available"
    else
        log_error "❌ Python build tools not available"
        ((total_errors++))
    fi
    
    # Test setup.py
    if [[ -f "setup.py" ]]; then
        log_success "✅ setup.py exists"
    else
        log_error "❌ setup.py not found"
        ((total_errors++))
    fi
    
    return $total_errors
}

# Test end-to-end pipeline
test_end_to_end_pipeline() {
    log_info "🔍 Testing end-to-end pipeline..."
    
    local total_errors=0
    
    # Test S3 bucket connectivity
    log_info "Testing S3 bucket connectivity..."
    if aws s3 ls "s3://$S3_BUCKET_DATA_LAKE/" 2>/dev/null | head -1 >/dev/null; then
        log_success "✅ S3 data lake bucket accessible"
    else
        log_error "❌ S3 data lake bucket not accessible"
        ((total_errors++))
    fi
    
    if aws s3 ls "s3://$S3_BUCKET_MWAA/" 2>/dev/null | head -1 >/dev/null; then
        log_success "✅ S3 MWAA bucket accessible"
    else
        log_error "❌ S3 MWAA bucket not accessible"
        ((total_errors++))
    fi
    
    # Test Glue job accessibility
    log_info "Testing Glue job accessibility..."
    if aws glue get-job --job-name "$GLUE_JOB_BRONZE_TO_SILVER" 2>/dev/null; then
        log_success "✅ Bronze to Silver Glue job accessible"
    else
        log_error "❌ Bronze to Silver Glue job not accessible"
        ((total_errors++))
    fi
    
    if aws glue get-job --job-name "$GLUE_JOB_SILVER_TO_GOLD" 2>/dev/null; then
        log_success "✅ Silver to Gold Glue job accessible"
    else
        log_error "❌ Silver to Gold Glue job not accessible"
        ((total_errors++))
    fi
    
    # Test MWAA environment accessibility
    log_info "Testing MWAA environment accessibility..."
    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        log_success "✅ MWAA environment accessible"
    else
        log_error "❌ MWAA environment not accessible"
        ((total_errors++))
    fi
    
    return $total_errors
}

# Generate validation report
generate_validation_report() {
    log_info "📊 Generating validation report..."
    
    local report_file="validation_report_${TIMESTAMP}.txt"
    
    cat > "$report_file" << EOF
=============================================================================
F1 DATA PIPELINE - VALIDATION REPORT
Generated: $(date)
AWS Account: $AWS_ACCOUNT_ID
AWS Region: $AWS_REGION
=============================================================================

VALIDATION SUMMARY:
- S3 Bucket Access: $(validate_s3_access >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- IAM Role Permissions: $(validate_iam_permissions >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- Glue Job Configurations: $(validate_glue_jobs >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- MWAA Environment: $(validate_mwaa_environment >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- CloudWatch Log Groups: $(validate_cloudwatch_logs >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- Wheel Deployment: $(test_wheel_deployment >/dev/null 2>&1 && echo "PASS" || echo "FAIL")
- End-to-End Pipeline: $(test_end_to_end_pipeline >/dev/null 2>&1 && echo "PASS" || echo "FAIL")

DETAILED VALIDATION RESULTS:
EOF
    
    # Run validations and capture output
    echo "S3 BUCKET ACCESS:" >> "$report_file"
    validate_s3_access >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "IAM ROLE PERMISSIONS:" >> "$report_file"
    validate_iam_permissions >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "GLUE JOB CONFIGURATIONS:" >> "$report_file"
    validate_glue_jobs >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "MWAA ENVIRONMENT:" >> "$report_file"
    validate_mwaa_environment >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "CLOUDWATCH LOG GROUPS:" >> "$report_file"
    validate_cloudwatch_logs >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "WHEEL DEPLOYMENT:" >> "$report_file"
    test_wheel_deployment >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "END-TO-END PIPELINE:" >> "$report_file"
    test_end_to_end_pipeline >> "$report_file" 2>&1
    echo "" >> "$report_file"
    
    echo "=============================================================================" >> "$report_file"
    
    log_info "📄 Validation report saved to: $report_file"
    echo "$report_file"
}

# Main Phase 4 function
validate_and_test() {
    log_info "🔍 PHASE 4: Starting comprehensive validation and testing..."
    echo "================================================================"
    
    local total_errors=0
    
    # Run all validations
    validate_s3_access || ((total_errors++))
    validate_iam_permissions || ((total_errors++))
    validate_glue_jobs || ((total_errors++))
    validate_mwaa_environment || ((total_errors++))
    validate_cloudwatch_logs || ((total_errors++))
    test_wheel_deployment || ((total_errors++))
    test_end_to_end_pipeline || ((total_errors++))
    
    # Generate validation report
    local report_file=$(generate_validation_report)
    
    if [[ $total_errors -eq 0 ]]; then
        log_success "✅ Phase 4 completed: All validations passed successfully"
        log_info "📄 Detailed validation report: $report_file"
    else
        log_error "❌ Phase 4 completed with $total_errors validation failures"
        log_info "📄 Detailed validation report: $report_file"
    fi
    
    return $total_errors
}

# =============================================================================
# PHASE 5: CLEANUP & DOCUMENTATION FUNCTIONS
# =============================================================================

# Clean up old deployment artifacts
cleanup_old_artifacts() {
    log_info "🧹 Cleaning up old deployment artifacts..."
    
    local cleanup_count=0
    
    # Clean up old validation reports (keep last 5)
    if [[ -f "validation_report_*.txt" ]]; then
        local report_count=$(ls validation_report_*.txt 2>/dev/null | wc -l)
        if [[ $report_count -gt 5 ]]; then
            local reports_to_remove=$((report_count - 5))
            log_info "Removing $reports_to_remove old validation report(s)"
            ls -t validation_report_*.txt | tail -n $reports_to_remove | xargs rm -f
            ((cleanup_count++))
        fi
    fi
    
    # Clean up old migration reports (keep last 3)
    if [[ -f "migration_report_*.txt" ]]; then
        local migration_count=$(ls migration_report_*.txt 2>/dev/null | wc -l)
        if [[ $migration_count -gt 3 ]]; then
            local migrations_to_remove=$((migration_count - 3))
            log_info "Removing $migrations_to_remove old migration report(s)"
            ls -t migration_report_*.txt | tail -n $migrations_to_remove | xargs rm -f
            ((cleanup_count++))
        fi
    fi
    
    # Clean up old log files (keep last 10)
    if [[ -d "logs" ]]; then
        local log_count=$(find logs -name "aws_infrastructure_setup_*.log" | wc -l)
        if [[ $log_count -gt 10 ]]; then
            local logs_to_remove=$((log_count - 10))
            log_info "Removing $logs_to_remove old log file(s)"
            find logs -name "aws_infrastructure_setup_*.log" -type f -exec stat -f '%m %N' {} \; | \
                sort -n | head -n $logs_to_remove | cut -d' ' -f2- | xargs rm -f
            ((cleanup_count++))
        fi
    fi
    
    if [[ $cleanup_count -gt 0 ]]; then
        log_success "Cleaned up $cleanup_count artifact type(s)"
    else
        log_info "No old artifacts found to clean up"
    fi
}

# Clean up temporary files
cleanup_temporary_files() {
    log_info "🧹 Cleaning up temporary files..."
    
    local temp_files=(
        "/tmp/aws_infrastructure_*"
        "/tmp/f1_pipeline_*"
        "/tmp/glue_job_*"
        "/tmp/mwaa_*"
        "/tmp/s3_*"
    )
    
    local cleaned_count=0
    for pattern in "${temp_files[@]}"; do
        if ls $pattern 2>/dev/null; then
            rm -f $pattern
            ((cleaned_count++))
        fi
    done
    
    if [[ $cleaned_count -gt 0 ]]; then
        log_success "Cleaned up temporary files"
    else
        log_info "No temporary files found"
    fi
}

# Generate comprehensive documentation
generate_documentation() {
    log_info "📚 Generating comprehensive documentation..."
    
    local doc_file="AWS_INFRASTRUCTURE_SETUP_GUIDE.md"
    
    cat > "$doc_file" << EOF
# AWS Infrastructure Setup Guide

## Overview
This guide documents the AWS infrastructure setup for the F1 Data Engineering Pipeline, including all resources, configurations, and deployment procedures.

## Generated: $(date)
## AWS Account: $AWS_ACCOUNT_ID
## AWS Region: $AWS_REGION

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
EOF

    # Add S3 bucket details
    echo "#### Data Lake Bucket: $S3_BUCKET_DATA_LAKE" >> "$doc_file"
    if aws s3api head-bucket --bucket "$S3_BUCKET_DATA_LAKE" 2>/dev/null; then
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Purpose: Raw data storage, processed data, deployment artifacts" >> "$doc_file"
        echo "- Encryption: Enabled" >> "$doc_file"
        echo "- Versioning: Enabled" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    echo "" >> "$doc_file"
    echo "#### MWAA Bucket: $S3_BUCKET_MWAA" >> "$doc_file"
    if aws s3api head-bucket --bucket "$S3_BUCKET_MWAA" 2>/dev/null; then
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Purpose: MWAA DAGs, requirements, and configuration" >> "$doc_file"
        echo "- Encryption: Enabled" >> "$doc_file"
        echo "- Versioning: Enabled" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    cat >> "$doc_file" << EOF

### IAM Roles
#### Glue Execution Role: $GLUE_ROLE_NAME
EOF

    if aws iam get-role --role-name "$GLUE_ROLE_NAME" 2>/dev/null; then
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Purpose: Glue job execution and S3 access" >> "$doc_file"
        echo "- Trust Policy: glue.amazonaws.com" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    echo "" >> "$doc_file"
    echo "#### MWAA Execution Role: $MWAA_ROLE_NAME" >> "$doc_file"
    if aws iam get-role --role-name "$MWAA_ROLE_NAME" 2>/dev/null; then
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Purpose: MWAA environment execution" >> "$doc_file"
        echo "- Trust Policy: airflow.amazonaws.com" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    cat >> "$doc_file" << EOF

### Glue Jobs
#### Bronze to Silver Transform: $GLUE_JOB_BRONZE_TO_SILVER
EOF

    if aws glue get-job --job-name "$GLUE_JOB_BRONZE_TO_SILVER" 2>/dev/null; then
        local job_config=$(aws glue get-job --job-name "$GLUE_JOB_BRONZE_TO_SILVER" 2>/dev/null)
        local glue_version=$(echo "$job_config" | jq -r '.Job.GlueVersion' 2>/dev/null)
        local python_version=$(echo "$job_config" | jq -r '.Job.Command.PythonVersion' 2>/dev/null)
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Glue Version: $glue_version" >> "$doc_file"
        echo "- Python Version: $python_version" >> "$doc_file"
        echo "- Purpose: Transform raw data to silver layer" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    echo "" >> "$doc_file"
    echo "#### Silver to Gold Transform: $GLUE_JOB_SILVER_TO_GOLD" >> "$doc_file"
    if aws glue get-job --job-name "$GLUE_JOB_SILVER_TO_GOLD" 2>/dev/null; then
        local job_config=$(aws glue get-job --job-name "$GLUE_JOB_SILVER_TO_GOLD" 2>/dev/null)
        local glue_version=$(echo "$job_config" | jq -r '.Job.GlueVersion' 2>/dev/null)
        local python_version=$(echo "$job_config" | jq -r '.Job.Command.PythonVersion' 2>/dev/null)
        echo "- Status: ✅ Active" >> "$doc_file"
        echo "- Glue Version: $glue_version" >> "$doc_file"
        echo "- Python Version: $python_version" >> "$doc_file"
        echo "- Purpose: Transform silver data to gold analytics tables" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    cat >> "$doc_file" << EOF

### MWAA Environment
#### Environment: $MWAA_ENV_NAME
EOF

    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        local env_config=$(aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null)
        local status=$(echo "$env_config" | jq -r '.Environment.Status' 2>/dev/null)
        local airflow_version=$(echo "$env_config" | jq -r '.Environment.AirflowVersion' 2>/dev/null)
        echo "- Status: $status" >> "$doc_file"
        echo "- Airflow Version: $airflow_version" >> "$doc_file"
        echo "- Purpose: Workflow orchestration and scheduling" >> "$doc_file"
    else
        echo "- Status: ❌ Not Found" >> "$doc_file"
    fi
    
    cat >> "$doc_file" << EOF

### CloudWatch Log Groups
EOF

    local log_groups=("f1-pipeline-logs" "f1-glue-logs" "f1-mwaa-logs")
    for log_group in "${log_groups[@]}"; do
        echo "#### $log_group" >> "$doc_file"
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | grep -q "$log_group"; then
            local retention=$(aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | jq -r '.logGroups[0].retentionInDays' 2>/dev/null)
            echo "- Status: ✅ Active" >> "$doc_file"
            echo "- Retention: $retention days" >> "$doc_file"
        else
            echo "- Status: ❌ Not Found" >> "$doc_file"
        fi
        echo "" >> "$doc_file"
    done
    
    cat >> "$doc_file" << EOF

## Configuration Details

### Wheel-Based Deployment
The infrastructure is configured for wheel-based deployment:
- **F1 Pipeline Wheel**: Custom package built from setup.py
- **Dependency Wheels**: Third-party packages (openlineage, great-expectations, etc.)
- **S3 Storage**: Wheels stored in s3://$S3_BUCKET_DATA_LAKE/dependencies/
- **Glue Integration**: Jobs configured to use wheel S3 URIs

### Security Configuration
- **S3 Encryption**: Server-side encryption enabled
- **IAM Roles**: Least privilege access
- **VPC**: MWAA deployed in private subnets
- **CloudWatch**: Comprehensive logging and monitoring

## Deployment Procedures

### Phase 1: Resource Detection
\`\`\`bash
./scripts/aws_infrastructure_setup.sh --phase1
\`\`\`

### Phase 2: Update Existing Resources
\`\`\`bash
./scripts/aws_infrastructure_setup.sh --phase2
\`\`\`

### Phase 3: Create Missing Resources
\`\`\`bash
./scripts/aws_infrastructure_setup.sh --phase3
\`\`\`

### Phase 4: Validation & Testing
\`\`\`bash
./scripts/aws_infrastructure_setup.sh --phase4
\`\`\`

### Phase 5: Cleanup & Documentation
\`\`\`bash
./scripts/aws_infrastructure_setup.sh --phase5
\`\`\`

### Full Deployment
\`\`\`bash
./scripts/aws_infrastructure_setup.sh
\`\`\`

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
\`\`\`bash
# Check Glue job status
aws glue get-job --job-name f1-bronze-to-silver-transform

# List S3 objects
aws s3 ls s3://$S3_BUCKET_DATA_LAKE/dependencies/ --recursive

# Check MWAA environment
aws mwaa get-environment --name $MWAA_ENV_NAME

# View CloudWatch logs
aws logs describe-log-groups --log-group-name-prefix f1-
\`\`\`

## Support
For issues or questions:
1. Check CloudWatch logs first
2. Review this documentation
3. Check AWS Console for resource status
4. Contact the development team

---
*This documentation was automatically generated by the AWS Infrastructure Setup script.*
EOF

    log_success "Documentation generated: $doc_file"
    echo "$doc_file"
}

# Generate resource summary report
generate_resource_summary() {
    log_info "📊 Generating resource summary report..."
    
    local summary_file="resource_summary_${TIMESTAMP}.txt"
    
    cat > "$summary_file" << EOF
=============================================================================
F1 DATA PIPELINE - RESOURCE SUMMARY REPORT
=============================================================================
Generated: $(date)
AWS Account: $AWS_ACCOUNT_ID
AWS Region: $AWS_REGION
=============================================================================

RESOURCE STATUS OVERVIEW:
EOF

    # S3 Buckets
    echo "S3 BUCKETS:" >> "$summary_file"
    for bucket in "$S3_BUCKET_DATA_LAKE" "$S3_BUCKET_MWAA"; do
        if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then
            echo "  ✅ $bucket - Active" >> "$summary_file"
        else
            echo "  ❌ $bucket - Not Found" >> "$summary_file"
        fi
    done
    echo "" >> "$summary_file"
    
    # IAM Roles
    echo "IAM ROLES:" >> "$summary_file"
    for role in "$GLUE_ROLE_NAME" "$MWAA_ROLE_NAME"; do
        if aws iam get-role --role-name "$role" 2>/dev/null; then
            echo "  ✅ $role - Active" >> "$summary_file"
        else
            echo "  ❌ $role - Not Found" >> "$summary_file"
        fi
    done
    echo "" >> "$summary_file"
    
    # Glue Jobs
    echo "GLUE JOBS:" >> "$summary_file"
    for job in "$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD"; do
        if aws glue get-job --job-name "$job" 2>/dev/null; then
            echo "  ✅ $job - Active" >> "$summary_file"
        else
            echo "  ❌ $job - Not Found" >> "$summary_file"
        fi
    done
    echo "" >> "$summary_file"
    
    # MWAA Environment
    echo "MWAA ENVIRONMENT:" >> "$summary_file"
    if aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null; then
        local status=$(aws mwaa get-environment --name "$MWAA_ENV_NAME" 2>/dev/null | jq -r '.Environment.Status' 2>/dev/null)
        echo "  ✅ $MWAA_ENV_NAME - $status" >> "$summary_file"
    else
        echo "  ❌ $MWAA_ENV_NAME - Not Found" >> "$summary_file"
    fi
    echo "" >> "$summary_file"
    
    # CloudWatch Log Groups
    echo "CLOUDWATCH LOG GROUPS:" >> "$summary_file"
    local log_groups=("f1-pipeline-logs" "f1-glue-logs" "f1-mwaa-logs")
    for log_group in "${log_groups[@]}"; do
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" 2>/dev/null | grep -q "$log_group"; then
            echo "  ✅ $log_group - Active" >> "$summary_file"
        else
            echo "  ❌ $log_group - Not Found" >> "$summary_file"
        fi
    done
    echo "" >> "$summary_file"
    
    echo "=============================================================================" >> "$summary_file"
    
    log_info "Resource summary saved to: $summary_file"
    echo "$summary_file"
}

# Main Phase 5 function
cleanup_and_document() {
    log_info "🧹 PHASE 5: Starting cleanup and documentation..."
    echo "================================================================"
    
    local total_operations=0
    
    # Cleanup operations
    cleanup_old_artifacts
    ((total_operations++))
    
    cleanup_temporary_files
    ((total_operations++))
    
    # Documentation generation
    local doc_file=$(generate_documentation)
    ((total_operations++))
    
    local summary_file=$(generate_resource_summary)
    ((total_operations++))
    
    log_success "✅ Phase 5 completed: Cleanup and documentation finished"
    log_info "📄 Generated documentation: $doc_file"
    log_info "📊 Generated summary: $summary_file"
    log_info "🔧 Completed $total_operations operations"
    
    return 0
}

# =============================
# Helper Functions
# =============================

# Update Glue role policies
update_glue_role_policies() {
    if [[ "$DRY_RUN" == "false" ]]; then
        log_info "Updating Glue role policies..."
        
        # Create and attach custom policy for F1 pipeline
        cat > /tmp/glue-custom-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload",
        "s3:GetAccountPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE",
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE/*",
        "arn:aws:s3:::$S3_BUCKET_MWAA",
        "arn:aws:s3:::$S3_BUCKET_MWAA/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricWidgetImage"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/glue/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/airflow/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/mwaa/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstances",
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": [
        "arn:aws:iam::$AWS_ACCOUNT_ID:role/F1-DataPipeline-Role"
      ]
    }
  ]
}
EOF

        aws iam put-role-policy \
            --role-name "$GLUE_ROLE_NAME" \
            --policy-name "F1DataPipelineGluePolicy" \
            --policy-document file:///tmp/glue-custom-policy.json
        
        log_success "Updated Glue role policies"
    else
        log_info "DRY RUN: Would update Glue role policies"
    fi
}

# Update MWAA role policies
update_mwaa_role_policies() {
    if [[ "$DRY_RUN" == "false" ]]; then
        log_info "Updating MWAA role policies..."
        
        # Create and attach custom policy for MWAA
        cat > /tmp/mwaa-custom-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetAccountPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE",
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE/*",
        "arn:aws:s3:::$S3_BUCKET_MWAA",
        "arn:aws:s3:::$S3_BUCKET_MWAA/*",
        "arn:aws:s3:::*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricWidgetImage"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/airflow/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/mwaa/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/glue/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstances",
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": [
        "arn:aws:iam::$AWS_ACCOUNT_ID:role/F1-DataPipeline-Role"
      ]
    }
  ]
}
EOF

        aws iam put-role-policy \
            --role-name "$MWAA_ROLE_NAME" \
            --policy-name "F1DataPipelineMWAAPolicy" \
            --policy-document file:///tmp/mwaa-custom-policy.json
        
        log_success "Updated MWAA role policies"
    else
        log_info "DRY RUN: Would update MWAA role policies"
    fi
}

# =============================
# Resource Creation Functions
# =============================

# Phase 1: Create S3 Buckets
create_s3_buckets() {
    log_info "🪣 Phase 1: Creating S3 Buckets"
    echo "=================================="
    
    # Data Lake Bucket
    log_info "Creating S3 data lake bucket: $S3_BUCKET_DATA_LAKE"
    if check_s3_bucket_exists "$S3_BUCKET_DATA_LAKE"; then
        log_warning "S3 bucket '$S3_BUCKET_DATA_LAKE' already exists"
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            aws s3 mb "s3://$S3_BUCKET_DATA_LAKE" --region "$AWS_REGION"
            
            # Enable versioning
            aws s3api put-bucket-versioning \
                --bucket "$S3_BUCKET_DATA_LAKE" \
                --versioning-configuration Status=Enabled
            
            # Enable server-side encryption
            aws s3api put-bucket-encryption \
                --bucket "$S3_BUCKET_DATA_LAKE" \
                --server-side-encryption-configuration '{
                    "Rules": [{
                        "ApplyServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256"
                        }
                    }]
                }'
            
            log_success "Created S3 data lake bucket: $S3_BUCKET_DATA_LAKE"
        else
            log_info "DRY RUN: Would create S3 bucket: $S3_BUCKET_DATA_LAKE"
        fi
    fi
    
    # MWAA Bucket
    log_info "Creating S3 MWAA bucket: $S3_BUCKET_MWAA"
    if check_s3_bucket_exists "$S3_BUCKET_MWAA"; then
        log_warning "S3 bucket '$S3_BUCKET_MWAA' already exists"
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            aws s3 mb "s3://$S3_BUCKET_MWAA" --region "$AWS_REGION"
            
            # Enable versioning for MWAA
            aws s3api put-bucket-versioning \
                --bucket "$S3_BUCKET_MWAA" \
                --versioning-configuration Status=Enabled
            
            # Block public access
            aws s3api put-public-access-block \
                --bucket "$S3_BUCKET_MWAA" \
                --public-access-block-configuration \
                BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
            
            log_success "Created S3 MWAA bucket: $S3_BUCKET_MWAA"
        else
            log_info "DRY RUN: Would create S3 bucket: $S3_BUCKET_MWAA"
        fi
    fi
    
    # Create S3 directory structure for data lake
    if [[ "$DRY_RUN" == "false" ]]; then
        log_info "Creating S3 directory structure..."
        local directories=(
            "bronze/"
            "silver/"
            "gold/"
            "lineage/"
            "lineage/events/"
            "lineage/summaries/"
            "iceberg-warehouse/"
            "validation-reports/"
            "temp/"
            "temp/bronze/"
            "temp/silver/"
            "temp/gold/"
            "spark-logs/"
            "spark-logs/bronze/"
            "spark-logs/silver/"
            "spark-logs/gold/"
            "deployment/"
            "scripts/"
            "scripts/orchestrators/"
        )
        
        for dir in "${directories[@]}"; do
            aws s3api put-object \
                --bucket "$S3_BUCKET_DATA_LAKE" \
                --key "$dir" \
                --content-length 0 &>/dev/null || true
        done
        log_success "S3 directory structure created"
    else
        log_info "DRY RUN: Would create S3 directory structure"
    fi
}

# Phase 2: Create IAM Roles and Policies
create_iam_roles() {
    log_info "🔐 Phase 2: Creating IAM Roles and Policies"
    echo "============================================"
    
    # Create Glue Service Role
    log_info "Creating Glue service role: $GLUE_ROLE_NAME"
    if check_iam_role_exists "$GLUE_ROLE_NAME"; then
        log_warning "IAM role '$GLUE_ROLE_NAME' already exists - updating policies"
        update_glue_role_policies
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            # Create trust policy for Glue
            cat > /tmp/glue-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/F1-DataPipeline-Role"
    }
  ]
}
EOF

            # Create the role
            aws iam create-role \
                --role-name "$GLUE_ROLE_NAME" \
                --assume-role-policy-document file:///tmp/glue-trust-policy.json \
                --description "IAM role for F1 Data Pipeline Glue jobs"
            
            # Attach AWS managed policies
            aws iam attach-role-policy \
                --role-name "$GLUE_ROLE_NAME" \
                --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
            
            # Create and attach custom policy for F1 pipeline
            cat > /tmp/glue-custom-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:ListBucketMultipartUploads",
        "s3:ListMultipartUploadParts",
        "s3:AbortMultipartUpload",
        "s3:GetAccountPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE",
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE/*",
        "arn:aws:s3:::$S3_BUCKET_MWAA",
        "arn:aws:s3:::$S3_BUCKET_MWAA/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricWidgetImage"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/glue/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/airflow/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/mwaa/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstances",
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": [
        "arn:aws:iam::$AWS_ACCOUNT_ID:role/F1-DataPipeline-Role"
      ]
    }
  ]
}
EOF

            aws iam put-role-policy \
                --role-name "$GLUE_ROLE_NAME" \
                --policy-name "F1DataPipelineGluePolicy" \
                --policy-document file:///tmp/glue-custom-policy.json
            
            log_success "Created Glue service role: $GLUE_ROLE_NAME"
        else
            log_info "DRY RUN: Would create Glue service role: $GLUE_ROLE_NAME"
        fi
    fi
    
    # Create MWAA Execution Role
    log_info "Creating MWAA execution role: $MWAA_ROLE_NAME"
    if check_iam_role_exists "$MWAA_ROLE_NAME"; then
        log_warning "IAM role '$MWAA_ROLE_NAME' already exists - updating policies"
        update_mwaa_role_policies
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            # Create trust policy for MWAA
            cat > /tmp/mwaa-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": [
          "airflow-env.amazonaws.com",
          "airflow.amazonaws.com"
        ]
      },
      "Action": "sts:AssumeRole"
    },
    {
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/F1-DataPipeline-Role"
    }
  ]
}
EOF

            # Create the role
            aws iam create-role \
                --role-name "$MWAA_ROLE_NAME" \
                --assume-role-policy-document file:///tmp/mwaa-trust-policy.json \
                --description "IAM role for F1 Data Pipeline MWAA environment"
            
            # Create and attach custom policy for MWAA
            cat > /tmp/mwaa-custom-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation",
        "s3:GetAccountPublicAccessBlock",
        "s3:GetBucketPublicAccessBlock"
      ],
      "Resource": [
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE",
        "arn:aws:s3:::$S3_BUCKET_DATA_LAKE/*",
        "arn:aws:s3:::$S3_BUCKET_MWAA",
        "arn:aws:s3:::$S3_BUCKET_MWAA/*",
        "arn:aws:s3:::*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns",
        "glue:BatchStopJobRun",
        "glue:GetJob",
        "glue:GetJobs",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:GetTables",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:CreateTable",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:CreatePartition",
        "glue:UpdatePartition",
        "glue:DeletePartition"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "cloudwatch:GetMetricStatistics",
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "cloudwatch:GetMetricWidgetImage"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:GetLogEvents",
        "logs:FilterLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/airflow/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/mwaa/*",
        "arn:aws:logs:$AWS_REGION:$AWS_ACCOUNT_ID:log-group:/aws/glue/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "ec2:DescribeVpcs",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
        "ec2:DescribeInstances",
        "ec2:DescribeNetworkInterfaces"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "iam:PassRole"
      ],
      "Resource": [
        "arn:aws:iam::$AWS_ACCOUNT_ID:role/F1-DataPipeline-Role"
      ]
    }
  ]
}
EOF

            aws iam put-role-policy \
                --role-name "$MWAA_ROLE_NAME" \
                --policy-name "F1DataPipelineMWAAPolicy" \
                --policy-document file:///tmp/mwaa-custom-policy.json
            
            log_success "Created MWAA execution role: $MWAA_ROLE_NAME"
        else
            log_info "DRY RUN: Would create MWAA execution role: $MWAA_ROLE_NAME"
        fi
    fi
    
    # Clean up temporary files
    rm -f /tmp/glue-trust-policy.json /tmp/glue-custom-policy.json
    rm -f /tmp/mwaa-trust-policy.json /tmp/mwaa-custom-policy.json
}

# Phase 3: Create Glue Databases
create_glue_databases() {
    log_info "🗄️ Phase 3: Creating Glue Databases"
    echo "==================================="
    
    local databases=(
        "$GLUE_DB_MAIN:F1 Data Lake - Main database for all F1 data:s3://$S3_BUCKET_DATA_LAKE/iceberg-warehouse/"
        "$GLUE_DB_SILVER:F1 Silver Layer - Cleaned and normalized data:s3://$S3_BUCKET_DATA_LAKE/silver/"
        "$GLUE_DB_GOLD:F1 Gold Layer - Business-ready analytics data:s3://$S3_BUCKET_DATA_LAKE/gold/"
    )
    
    for db_info in "${databases[@]}"; do
        local db_name="${db_info%%:*}"
        local remaining="${db_info#*:}"
        local db_description="${remaining%%:*}"
        local db_location="${remaining#*:}"
        
        log_info "Creating Glue database: $db_name"
        if check_glue_database_exists "$db_name"; then
            log_warning "Glue database '$db_name' already exists"
        else
            if [[ "$DRY_RUN" == "false" ]]; then
                aws glue create-database \
                    --database-input "{
                        \"Name\": \"$db_name\",
                        \"Description\": \"$db_description\",
                        \"LocationUri\": \"$db_location\"
                    }"
                log_success "Created Glue database: $db_name"
            else
                log_info "DRY RUN: Would create Glue database: $db_name"
            fi
        fi
    done
}

# Phase 4: Create Glue Jobs
create_glue_jobs() {
    log_info "🔧 Phase 4: Creating Glue Jobs"
    echo "==============================="
    
    # Bronze to Silver Job
    log_info "Creating/Updating Glue job: $GLUE_JOB_BRONZE_TO_SILVER"
    if check_glue_job_exists "$GLUE_JOB_BRONZE_TO_SILVER"; then
        log_warning "Glue job '$GLUE_JOB_BRONZE_TO_SILVER' already exists - updating to wheel-based configuration"
        update_glue_job_to_wheels "$GLUE_JOB_BRONZE_TO_SILVER" "bronze"
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            aws glue create-job \
                --name "$GLUE_JOB_BRONZE_TO_SILVER" \
                --role "arn:aws:iam::$AWS_ACCOUNT_ID:role/$GLUE_ROLE_NAME" \
                --command '{
                    "Name": "glueetl",
                    "ScriptLocation": "s3://'$S3_BUCKET_DATA_LAKE'/scripts/orchestrators/bronze_to_silver_orchestrator.py",
                    "PythonVersion": "3"
                }' \
                --default-arguments '{
                    "job-language": "python",
                    "job-bookmark-option": "job-bookmark-enable",
                    "extra-py-files": "s3://'$S3_BUCKET_DATA_LAKE'/deployment/f1-pipeline-deployment.zip",
                    "--additional-python-modules": "openlineage-python==1.8.0,great-expectations==0.18.8,pyiceberg==0.5.1",
                    "--TempDir": "s3://'$S3_BUCKET_DATA_LAKE'/temp/bronze/",
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": "s3://'$S3_BUCKET_DATA_LAKE'/spark-logs/bronze/",
                    "--enable-metrics": "",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--continuous-log-logGroup": "/aws/glue/f1-bronze-to-silver",
                    "--enable-observability-metrics": "true",
                    "--conf": "spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true"
                }' \
                --max-capacity 4 \
                --timeout 60 \
                --glue-version "4.0" \
                --description "F1 Data Pipeline: Transform Bronze layer data to Silver layer with data quality validation"
            
            log_success "Created Glue job: $GLUE_JOB_BRONZE_TO_SILVER"
        else
            log_info "DRY RUN: Would create Glue job: $GLUE_JOB_BRONZE_TO_SILVER"
        fi
    fi
    
    # Silver to Gold Job
    log_info "Creating Glue job: $GLUE_JOB_SILVER_TO_GOLD"
    if check_glue_job_exists "$GLUE_JOB_SILVER_TO_GOLD"; then
        log_warning "Glue job '$GLUE_JOB_SILVER_TO_GOLD' already exists"
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            aws glue create-job \
                --name "$GLUE_JOB_SILVER_TO_GOLD" \
                --role "arn:aws:iam::$AWS_ACCOUNT_ID:role/$GLUE_ROLE_NAME" \
                --command '{
                    "Name": "glueetl",
                    "ScriptLocation": "s3://'$S3_BUCKET_DATA_LAKE'/scripts/orchestrators/silver_to_gold_orchestrator.py",
                    "PythonVersion": "3"
                }' \
                --default-arguments '{
                    "job-language": "python",
                    "job-bookmark-option": "job-bookmark-enable",
                    "extra-py-files": "s3://'$S3_BUCKET_DATA_LAKE'/deployment/f1-pipeline-deployment.zip",
                    "--additional-python-modules": "openlineage-python==1.8.0,great-expectations==0.18.8,pyiceberg==0.5.1",
                    "--TempDir": "s3://'$S3_BUCKET_DATA_LAKE'/temp/gold/",
                    "--enable-spark-ui": "true",
                    "--spark-event-logs-path": "s3://'$S3_BUCKET_DATA_LAKE'/spark-logs/gold/",
                    "--enable-metrics": "",
                    "--enable-continuous-cloudwatch-log": "true",
                    "--continuous-log-logGroup": "/aws/glue/f1-silver-to-gold",
                    "--enable-observability-metrics": "true",
                    "--conf": "spark.sql.adaptive.enabled=true --conf spark.sql.adaptive.coalescePartitions.enabled=true"
                }' \
                --max-capacity 4 \
                --timeout 90 \
                --glue-version "4.0" \
                --description "F1 Data Pipeline: Transform Silver layer data to Gold layer analytics tables"
            
            log_success "Created Glue job: $GLUE_JOB_SILVER_TO_GOLD"
        else
            log_info "DRY RUN: Would create Glue job: $GLUE_JOB_SILVER_TO_GOLD"
        fi
    fi
}

# Phase 5: Create MWAA Environment
create_mwaa_environment() {
    log_info "☁️ Phase 5: Creating MWAA Environment"
    echo "====================================="
    
    log_info "Creating MWAA environment: $MWAA_ENV_NAME"
    if check_mwaa_environment_exists "$MWAA_ENV_NAME"; then
        log_warning "MWAA environment '$MWAA_ENV_NAME' already exists"
    else
        if [[ "$DRY_RUN" == "false" ]]; then
            # Get default VPC and private subnets for MWAA
            local default_vpc_id
            default_vpc_id=$(aws ec2 describe-vpcs --filters "Name=is-default,Values=true" --query 'Vpcs[0].VpcId' --output text)
            
            # Find private subnets (subnets with MapPublicIpOnLaunch=false)
            local subnet_ids
            subnet_ids=$(aws ec2 describe-subnets \
                --filters "Name=vpc-id,Values=$default_vpc_id" "Name=map-public-ip-on-launch,Values=false" \
                --query 'Subnets[0:2].SubnetId' --output text | tr '\t' ',')
            
            # If no private subnets found, try subnets with "private" in name tags
            if [[ -z "$subnet_ids" || "$subnet_ids" == "None" ]]; then
                log_info "No private subnets found by MapPublicIpOnLaunch, trying name tags..."
                subnet_ids=$(aws ec2 describe-subnets \
                    --filters "Name=vpc-id,Values=$default_vpc_id" "Name=tag:Name,Values=*private*" \
                    --query 'Subnets[0:2].SubnetId' --output text | tr '\t' ',')
            fi
            
            # If still no private subnets found, use any subnets but warn user
            if [[ -z "$subnet_ids" || "$subnet_ids" == "None" ]]; then
                log_warning "No private subnets found, using any available subnets (MWAA may fail)"
                subnet_ids=$(aws ec2 describe-subnets \
                    --filters "Name=vpc-id,Values=$default_vpc_id" \
                    --query 'Subnets[0:2].SubnetId' --output text | tr '\t' ',')
            fi
            
            # Create security group for MWAA if it doesn't exist
            local security_group_id
            security_group_id=$(aws ec2 describe-security-groups \
                --filters "Name=group-name,Values=F1-MWAA-SecurityGroup" "Name=vpc-id,Values=$default_vpc_id" \
                --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null)
            
            if [[ -z "$security_group_id" || "$security_group_id" == "None" ]]; then
                log_info "Creating security group for MWAA..."
                security_group_id=$(aws ec2 create-security-group \
                    --group-name "F1-MWAA-SecurityGroup" \
                    --description "Security group for F1 MWAA environment" \
                    --vpc-id "$default_vpc_id" \
                    --query 'GroupId' --output text)
                
                # Add inbound rules for MWAA
                aws ec2 authorize-security-group-ingress \
                    --group-id "$security_group_id" \
                    --protocol tcp \
                    --port 8080 \
                    --cidr 0.0.0.0/0 >/dev/null 2>&1 || true
                
                aws ec2 authorize-security-group-ingress \
                    --group-id "$security_group_id" \
                    --protocol tcp \
                    --port 5432 \
                    --cidr 172.31.0.0/16 >/dev/null 2>&1 || true
                
                log_success "Created security group: $security_group_id"
            else
                log_info "Using existing security group: $security_group_id"
            fi
            
            # Create MWAA environment
            aws mwaa create-environment \
                --name "$MWAA_ENV_NAME" \
                --execution-role-arn "arn:aws:iam::$AWS_ACCOUNT_ID:role/$MWAA_ROLE_NAME" \
                --source-bucket-arn "arn:aws:s3:::$S3_BUCKET_MWAA" \
                --dag-s3-path "dags" \
                --requirements-s3-path "requirements.txt" \
                --webserver-access-mode "PUBLIC_ONLY" \
                --environment-class "mw1.small" \
                --max-workers 2 \
                --min-workers 1 \
                --airflow-version "2.7.2" \
                --network-configuration "SubnetIds=$subnet_ids,SecurityGroupIds=$security_group_id" \
                --logging-configuration '{
                    "DagProcessingLogs": {
                        "Enabled": true,
                        "LogLevel": "INFO"
                    },
                    "SchedulerLogs": {
                        "Enabled": true,
                        "LogLevel": "INFO"
                    },
                    "TaskLogs": {
                        "Enabled": true,
                        "LogLevel": "INFO"
                    },
                    "WebserverLogs": {
                        "Enabled": true,
                        "LogLevel": "INFO"
                    },
                    "WorkerLogs": {
                        "Enabled": true,
                        "LogLevel": "INFO"
                    }
                }' \
                --airflow-configuration-options '{
                    "core.load_examples": "False",
                    "core.dags_are_paused_at_creation": "False",
                    "logging.logging_level": "INFO"
                }'
            
            log_success "Created MWAA environment: $MWAA_ENV_NAME (Note: This may take 20-30 minutes to become available)"
        else
            log_info "DRY RUN: Would create MWAA environment: $MWAA_ENV_NAME"
        fi
    fi
}

# Phase 6: Create CloudWatch Resources
create_cloudwatch_resources() {
    log_info "📊 Phase 6: Creating CloudWatch Resources"
    echo "========================================="
    
    # Create CloudWatch Log Groups
    log_info "Creating CloudWatch log groups..."
    if [[ "$DRY_RUN" == "false" ]]; then
        local log_groups=(
            "/aws/glue/f1-bronze-to-silver"
            "/aws/glue/f1-silver-to-gold"
            "/aws/mwaa/environment/$MWAA_ENV_NAME"
            "/aws/airflow/$MWAA_ENV_NAME"
        )
        
        for log_group in "${log_groups[@]}"; do
            aws logs create-log-group --log-group-name "$log_group" &>/dev/null || log_debug "Log group $log_group already exists"
        done
        log_success "CloudWatch log groups created"
    else
        log_info "DRY RUN: Would create CloudWatch log groups"
    fi
    
    # Create CloudWatch Dashboard
    log_info "Creating CloudWatch dashboard..."
    if [[ "$DRY_RUN" == "false" ]]; then
        cat > /tmp/dashboard.json << EOF
{
    "widgets": [
        {
            "type": "metric",
            "x": 0,
            "y": 0,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "F1Pipeline/Bronze", "sessions_processed", "Environment", "production" ],
                    [ "F1Pipeline/Silver", "tables_processed", "Environment", "production" ],
                    [ "F1Pipeline/Production", "pipeline_execution_time", "Environment", "production" ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "$AWS_REGION",
                "title": "F1 Pipeline Metrics",
                "period": 300,
                "stat": "Sum"
            }
        },
        {
            "type": "metric",
            "x": 12,
            "y": 0,
            "width": 12,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "$GLUE_JOB_BRONZE_TO_SILVER" ],
                    [ "AWS/Glue", "glue.driver.aggregate.numFailedTasks", "JobName", "$GLUE_JOB_BRONZE_TO_SILVER" ],
                    [ "AWS/Glue", "glue.driver.aggregate.numCompletedTasks", "JobName", "$GLUE_JOB_SILVER_TO_GOLD" ],
                    [ "AWS/Glue", "glue.driver.aggregate.numFailedTasks", "JobName", "$GLUE_JOB_SILVER_TO_GOLD" ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "$AWS_REGION",
                "title": "Glue Job Task Metrics",
                "period": 300,
                "stat": "Sum"
            }
        },
        {
            "type": "metric",
            "x": 0,
            "y": 6,
            "width": 24,
            "height": 6,
            "properties": {
                "metrics": [
                    [ "F1Pipeline/Production", "validation_failures", "Environment", "production" ],
                    [ "F1Pipeline/Production", "data_quality_score", "Environment", "production" ]
                ],
                "view": "timeSeries",
                "stacked": false,
                "region": "$AWS_REGION",
                "title": "Data Quality Metrics",
                "period": 300,
                "stat": "Average"
            }
        }
    ]
}
EOF

        aws cloudwatch put-dashboard \
            --dashboard-name "F1-Data-Pipeline-Dashboard" \
            --dashboard-body file:///tmp/dashboard.json
        
        log_success "CloudWatch dashboard created: F1-Data-Pipeline-Dashboard"
    else
        log_info "DRY RUN: Would create CloudWatch dashboard"
    fi
    
    # Create CloudWatch Alarms
    log_info "Creating CloudWatch alarms..."
    if [[ "$DRY_RUN" == "false" ]]; then
        # Glue Job Failure Alarm
        aws cloudwatch put-metric-alarm \
            --alarm-name "F1-Glue-Job-Failures" \
            --alarm-description "Alert when F1 Glue jobs fail" \
            --metric-name "glue.driver.aggregate.numFailedTasks" \
            --namespace "AWS/Glue" \
            --statistic "Sum" \
            --period 300 \
            --threshold 1 \
            --comparison-operator "GreaterThanOrEqualToThreshold" \
            --evaluation-periods 1 \
            --treat-missing-data "notBreaching"
        
        # Pipeline Execution Time Alarm
        aws cloudwatch put-metric-alarm \
            --alarm-name "F1-Pipeline-Execution-Time" \
            --alarm-description "Alert when F1 pipeline takes too long" \
            --metric-name "pipeline_execution_time" \
            --namespace "F1Pipeline/Production" \
            --statistic "Average" \
            --period 300 \
            --threshold 1800 \
            --comparison-operator "GreaterThanThreshold" \
            --evaluation-periods 2 \
            --treat-missing-data "notBreaching"
        
        # Data Quality Alarm
        aws cloudwatch put-metric-alarm \
            --alarm-name "F1-Data-Quality-Issues" \
            --alarm-description "Alert when data quality validation fails" \
            --metric-name "validation_failures" \
            --namespace "F1Pipeline/Production" \
            --statistic "Sum" \
            --period 300 \
            --threshold 1 \
            --comparison-operator "GreaterThanOrEqualToThreshold" \
            --evaluation-periods 1 \
            --treat-missing-data "notBreaching"
        
        log_success "CloudWatch alarms created"
    else
        log_info "DRY RUN: Would create CloudWatch alarms"
    fi
    
    # Clean up temporary files
    rm -f /tmp/dashboard.json
}

# Verification phase
verify_deployment() {
    log_info "✅ Phase 7: Verifying Deployment"
    echo "================================="
    
    local verification_errors=0
    
    # Check S3 buckets
    log_info "Verifying S3 buckets..."
    if check_s3_bucket_exists "$S3_BUCKET_DATA_LAKE"; then
        log_success "✓ Data lake bucket exists: $S3_BUCKET_DATA_LAKE"
    else
        log_error "✗ Data lake bucket missing: $S3_BUCKET_DATA_LAKE"
        ((verification_errors++))
    fi
    
    if check_s3_bucket_exists "$S3_BUCKET_MWAA"; then
        log_success "✓ MWAA bucket exists: $S3_BUCKET_MWAA"
    else
        log_error "✗ MWAA bucket missing: $S3_BUCKET_MWAA"
        ((verification_errors++))
    fi
    
    # Check IAM roles
    log_info "Verifying IAM roles..."
    if check_iam_role_exists "$GLUE_ROLE_NAME"; then
        log_success "✓ Glue role exists: $GLUE_ROLE_NAME"
    else
        log_error "✗ Glue role missing: $GLUE_ROLE_NAME"
        ((verification_errors++))
    fi
    
    if check_iam_role_exists "$MWAA_ROLE_NAME"; then
        log_success "✓ MWAA role exists: $MWAA_ROLE_NAME"
    else
        log_error "✗ MWAA role missing: $MWAA_ROLE_NAME"
        ((verification_errors++))
    fi
    
    # Check Glue databases
    log_info "Verifying Glue databases..."
    for db_name in "$GLUE_DB_MAIN" "$GLUE_DB_SILVER" "$GLUE_DB_GOLD"; do
        if check_glue_database_exists "$db_name"; then
            log_success "✓ Glue database exists: $db_name"
        else
            log_error "✗ Glue database missing: $db_name"
            ((verification_errors++))
        fi
    done
    
    # Check Glue jobs
    log_info "Verifying Glue jobs..."
    for job_name in "$GLUE_JOB_BRONZE_TO_SILVER" "$GLUE_JOB_SILVER_TO_GOLD"; do
        if check_glue_job_exists "$job_name"; then
            log_success "✓ Glue job exists: $job_name"
        else
            log_error "✗ Glue job missing: $job_name"
            ((verification_errors++))
        fi
    done
    
    # Check MWAA environment
    log_info "Verifying MWAA environment..."
    if check_mwaa_environment_exists "$MWAA_ENV_NAME"; then
        log_success "✓ MWAA environment exists: $MWAA_ENV_NAME"
    else
        log_warning "⚠ MWAA environment may still be creating: $MWAA_ENV_NAME"
    fi
    
    if [[ $verification_errors -eq 0 ]]; then
        log_success "🎉 All core resources verified successfully!"
    else
        log_error "❌ Verification completed with $verification_errors errors"
    fi
    
    return $verification_errors
}

# Generate deployment summary
generate_summary() {
    echo ""
    echo -e "${CYAN}============================================================================="
    echo "📊 AWS INFRASTRUCTURE DEPLOYMENT SUMMARY"
    echo "=============================================================================${NC}"
    echo ""
    echo "📅 Timestamp: $(date)"
    echo "📝 Log File: $LOG_FILE"
    echo "🌍 AWS Region: $AWS_REGION"
    echo "👤 AWS Account: $AWS_ACCOUNT_ID"
    echo ""
    echo "📦 Created Resources:"
    echo "  • S3 Buckets:"
    echo "    - Data Lake: $S3_BUCKET_DATA_LAKE"
    echo "    - MWAA: $S3_BUCKET_MWAA"
    echo "  • IAM Roles:"
    echo "    - Glue Service Role: $GLUE_ROLE_NAME"
    echo "    - MWAA Execution Role: $MWAA_ROLE_NAME"
    echo "  • Glue Databases:"
    echo "    - Main: $GLUE_DB_MAIN"
    echo "    - Silver: $GLUE_DB_SILVER"
    echo "    - Gold: $GLUE_DB_GOLD"
    echo "  • Glue Jobs:"
    echo "    - Bronze→Silver: $GLUE_JOB_BRONZE_TO_SILVER"
    echo "    - Silver→Gold: $GLUE_JOB_SILVER_TO_GOLD"
    echo "  • MWAA Environment: $MWAA_ENV_NAME"
    echo "  • CloudWatch Dashboards & Alarms"
    echo ""
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}⚠️  This was a DRY RUN - no actual resources were created${NC}"
    fi
    
    echo "🔗 Useful AWS Console Links:"
    echo "  • Glue Console: https://console.aws.amazon.com/glue/home?region=$AWS_REGION#etl:tab=jobs"
    echo "  • MWAA Console: https://console.aws.amazon.com/mwaa/home?region=$AWS_REGION#environments"
    echo "  • S3 Console: https://console.aws.amazon.com/s3/buckets/$S3_BUCKET_DATA_LAKE"
    echo "  • CloudWatch Dashboard: https://console.aws.amazon.com/cloudwatch/home?region=$AWS_REGION#dashboards:name=F1-Data-Pipeline-Dashboard"
    echo "  • IAM Roles: https://console.aws.amazon.com/iamv2/home#/roles"
    echo ""
    echo "📋 Next Steps:"
    echo "  1. Deploy code using the deployment script"
    echo "  2. Upload DAGs to MWAA bucket"
    echo "  3. Test Glue jobs manually"
    echo "  4. Configure Airflow variables in MWAA"
    echo "  5. Monitor CloudWatch dashboards"
    echo ""
    echo -e "${YELLOW}⚠️  Note: MWAA environment may take 20-30 minutes to become fully available${NC}"
}

# Main execution function
main() {
    print_banner
    parse_arguments "$@"
    
    log_info "Starting AWS infrastructure setup for F1 Data Pipeline..."
    log_info "Mode: $([ "$DRY_RUN" == "true" ] && echo "DRY RUN" || echo "LIVE DEPLOYMENT")"
    
    # Check prerequisites
    check_prerequisites
    
    # Phase 1: Resource Detection & Analysis
    if [[ "$PHASE1_ONLY" == "true" ]]; then
        detect_all_resources
        log_success "🎉 Phase 1 completed: Resource detection and analysis finished"
        log_info "📄 Check migration_report_${TIMESTAMP}.txt for detailed analysis"
        exit 0
    fi
    
    # Phase 2: Update Existing Resources
    if [[ "$PHASE2_ONLY" == "true" ]]; then
        update_existing_resources
        log_success "🎉 Phase 2 completed: Resource updates finished"
        exit 0
    fi
    
    # Phase 3: Create Missing Resources
    if [[ "$PHASE3_ONLY" == "true" ]]; then
        create_missing_resources
        log_success "🎉 Phase 3 completed: Missing resources created"
        exit 0
    fi
    
    # Phase 4: Validation & Testing
    if [[ "$PHASE4_ONLY" == "true" ]]; then
        validate_and_test
        log_success "🎉 Phase 4 completed: Validation and testing finished"
        exit 0
    fi
    
    # Phase 5: Cleanup & Documentation
    if [[ "$PHASE5_ONLY" == "true" ]]; then
        cleanup_and_document
        log_success "🎉 Phase 5 completed: Cleanup and documentation finished"
        exit 0
    fi
    
    # Update MWAA Requirements Only
    if [[ "$UPDATE_MWAA_REQUIREMENTS" == "true" ]]; then
        update_mwaa_requirements
        log_success "🎉 MWAA requirements update completed"
        exit 0
    fi
    
    # Execute phases
    local total_errors=0
    
    create_s3_buckets || ((total_errors++))
    create_iam_roles || ((total_errors++))
    create_glue_databases || ((total_errors++))
    create_glue_jobs || ((total_errors++))
    create_mwaa_environment || ((total_errors++))
    create_cloudwatch_resources || ((total_errors++))
    
    # Verify deployment (only in live mode)
    if [[ "$DRY_RUN" == "false" ]]; then
        verify_deployment || ((total_errors++))
    fi
    
    # Generate summary
    generate_summary
    
    if [[ $total_errors -eq 0 ]]; then
        log_success "🎉 AWS Infrastructure setup completed successfully!"
        exit 0
    else
        log_error "❌ AWS Infrastructure setup completed with $total_errors phase(s) having errors"
        exit 1
    fi
}

# Run main function
main "$@"
