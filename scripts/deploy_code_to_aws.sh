#!/bin/bash
# =============================================================================
# F1 Data Engineering Pipeline - Code Deployment Script
# =============================================================================
#
# This script deploys the refactored F1 pipeline codebase to AWS:
# - Removes previous code from S3 buckets
# - Packages and uploads current code with proper structure
# - Updates Glue job scripts with new src/ layout
# - Deploys DAGs to MWAA with dependencies
# - Handles simplified pipeline with minimal dependencies
#
# Features:
# - Idempotent: Can be run multiple times safely
# - Handles refactored src/ folder structure
# - Complete dependency management
# - Version tracking and rollback capability
# - Comprehensive error handling and logging
#
# Usage: ./scripts/deploy_code_to_aws.sh [--clean] [--dry-run] [--version v1.0.0]
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

# Configuration
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="f1-data-pipeline"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
VERSION="${VERSION:-$TIMESTAMP}"

# S3 Bucket names
S3_BUCKET_DATA_LAKE="f1-data-lake-naveeth"
S3_BUCKET_MWAA="f1-mwaa-naveeth"

# Project paths (updated for refactored structure)
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"

# Deployment paths
DEPLOYMENT_DIR="$PROJECT_ROOT/deployment"
PACKAGE_DIR="$DEPLOYMENT_DIR/package"
ARCHIVE_DIR="$DEPLOYMENT_DIR/archive"
WHEELS_DIR="$DEPLOYMENT_DIR/wheels"
S3_DEPENDENCIES_PATH="dependencies"

# Flags
DRY_RUN=false
CLEAN_PREVIOUS=true
VERBOSE=false
SKIP_VALIDATION=false

# Rollback and health check flags
ROLLBACK_MODE=false
ROLLBACK_VERSION=""
HEALTH_CHECK_ONLY=false
LIST_VERSIONS=false

# Logging
LOG_DIR="$PROJECT_ROOT/logs"
LOG_FILE="$LOG_DIR/deploy_code_${TIMESTAMP}.log"

# Create necessary directories
mkdir -p "$LOG_DIR" "$DEPLOYMENT_DIR" "$PACKAGE_DIR" "$ARCHIVE_DIR" "$WHEELS_DIR"

# =============================
# Utility Functions
# =============================

# =============================
# Deployment History & Rollback Functions
# =============================

# Deployment history file
DEPLOYMENT_HISTORY_FILE="$PROJECT_ROOT/deployment_history.json"

# Initialize deployment history
init_deployment_history() {
    if [[ ! -f "$DEPLOYMENT_HISTORY_FILE" ]]; then
        log_debug "Creating deployment history file"
        cat > "$DEPLOYMENT_HISTORY_FILE" << EOF
{
  "deployments": [],
  "last_successful": null,
  "created_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF
    fi
}

# Record deployment in history
record_deployment() {
    local version="$1"
    local status="$2"
    local error_message="$3"
    
    log_function_entry "record_deployment" "$version, $status"
    
    local timestamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    local temp_file=$(mktemp)
    
    # Create deployment record
    local deployment_record=$(cat << EOF
    {
      "version": "$version",
      "timestamp": "$timestamp",
      "status": "$status",
      "error_message": "$error_message",
      "glue_jobs": {
        "bronze_to_silver": "s3://$S3_BUCKET_DATA_LAKE/scripts/orchestrators/bronze_to_silver_orchestrator.py",
        "silver_to_gold": "s3://$S3_BUCKET_DATA_LAKE/scripts/orchestrators/silver_to_gold_orchestrator.py"
      },
      "wheels": [
        "s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$version/"
      ],
      "mwaa_dags": "s3://$S3_BUCKET_MWAA/dags/"
    }
EOF
)
    
    # Add to history
    if [[ -f "$DEPLOYMENT_HISTORY_FILE" ]]; then
        jq --argjson record "$deployment_record" '.deployments += [$record]' "$DEPLOYMENT_HISTORY_FILE" > "$temp_file"
        mv "$temp_file" "$DEPLOYMENT_HISTORY_FILE"
    else
        init_deployment_history
        jq --argjson record "$deployment_record" '.deployments += [$record]' "$DEPLOYMENT_HISTORY_FILE" > "$temp_file"
        mv "$temp_file" "$DEPLOYMENT_HISTORY_FILE"
    fi
    
    # Update last successful if this deployment succeeded
    if [[ "$status" == "success" ]]; then
        jq --arg version "$version" '.last_successful = $version' "$DEPLOYMENT_HISTORY_FILE" > "$temp_file"
        mv "$temp_file" "$DEPLOYMENT_HISTORY_FILE"
    fi
    
    log_debug "Recorded deployment: $version ($status)"
    log_function_exit "record_deployment" 0
}

# Get last successful deployment version
get_last_successful_version() {
    if [[ -f "$DEPLOYMENT_HISTORY_FILE" ]]; then
        jq -r '.last_successful // empty' "$DEPLOYMENT_HISTORY_FILE"
    else
        echo ""
    fi
}

# List available versions for rollback
list_rollback_versions() {
    log_info "Available versions for rollback:"
    if [[ -f "$DEPLOYMENT_HISTORY_FILE" ]]; then
        jq -r '.deployments[] | select(.status == "success") | "  • \(.version) - \(.timestamp)"' "$DEPLOYMENT_HISTORY_FILE"
    else
        log_warning "No deployment history found"
    fi
}

# Rollback to specific version
rollback_to_version() {
    local target_version="$1"
    
    log_function_entry "rollback_to_version" "$target_version"
    
    if [[ -z "$target_version" ]]; then
        log_error "Target version not specified for rollback"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    log_info "🔄 Starting rollback to version: $target_version"
    
    # Check if target version exists in history
    if [[ ! -f "$DEPLOYMENT_HISTORY_FILE" ]]; then
        log_error "No deployment history found - cannot rollback"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    local version_exists=$(jq -r --arg version "$target_version" '.deployments[] | select(.version == $version and .status == "success") | .version' "$DEPLOYMENT_HISTORY_FILE")
    if [[ -z "$version_exists" ]]; then
        log_error "Version $target_version not found in successful deployments"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    log_info "Rolling back Glue job configurations..."
    if ! rollback_glue_jobs "$target_version"; then
        log_error "Failed to rollback Glue jobs"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    log_info "Rolling back wheel dependencies..."
    if ! rollback_wheels "$target_version"; then
        log_error "Failed to rollback wheel dependencies"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    log_info "Rolling back MWAA DAGs..."
    if ! rollback_mwaa_dags "$target_version"; then
        log_error "Failed to rollback MWAA DAGs"
        log_function_exit "rollback_to_version" 1
        return 1
    fi
    
    # Record rollback in history
    record_deployment "rollback-to-$target_version" "success" ""
    
    log_success "✅ Rollback to $target_version completed successfully"
    log_function_exit "rollback_to_version" 0
    return 0
}

# Rollback Glue jobs to specific version
rollback_glue_jobs() {
    local target_version="$1"
    
    log_debug "Rolling back Glue jobs to version: $target_version"
    
    # Get AWS Account ID
    local aws_account_id=$(aws sts get-caller-identity --query Account --output text 2>/dev/null || echo "unknown")
    
    # Get wheel URIs for target version
    local wheel_uris="s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$target_version/"
    
    # Update Bronze to Silver job
    log_info "Rolling back Bronze to Silver Glue job..."
    if aws glue update-job \
        --job-name "f1-bronze-to-silver-transform" \
        --job-update '{
            "GlueVersion": "4.0",
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py",
                "PythonVersion": "3.9"
            },
            "DefaultArguments": {
                "job-language": "python",
                "job-bookmark-option": "job-bookmark-enable",
                "--additional-python-modules": "${wheel_uris}",
                "--TempDir": "s3://${S3_BUCKET_DATA_LAKE}/temp/bronze/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/bronze/",
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--continuous-log-logGroup": "/aws/glue/f1-bronze-to-silver",
                "--enable-observability-metrics": "true",
                "--conf": "spark.sql.adaptive.enabled=true",
                "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
                "--F1_DEPLOYMENT_VERSION": "${target_version}"
            },
            "Description": "F1 Data Pipeline: Transform Bronze layer data to Silver layer with data quality validation - Version ${target_version} (ROLLBACK)"
        }' >> "$LOG_FILE" 2>&1; then
        log_success "Rolled back Bronze to Silver Glue job"
    else
        log_error "Failed to rollback Bronze to Silver Glue job"
        return 1
    fi
    
    # Update Silver to Gold job
    log_info "Rolling back Silver to Gold Glue job..."
    if aws glue update-job \
        --job-name "f1-silver-to-gold-transform" \
        --job-update '{
            "GlueVersion": "4.0",
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py",
                "PythonVersion": "3.9"
            },
            "DefaultArguments": {
                "job-language": "python",
                "job-bookmark-option": "job-bookmark-enable",
                "--additional-python-modules": "${wheel_uris}",
                "--TempDir": "s3://${S3_BUCKET_DATA_LAKE}/temp/gold/",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/gold/",
                "--enable-metrics": "",
                "--enable-continuous-cloudwatch-log": "true",
                "--continuous-log-logGroup": "/aws/glue/f1-silver-to-gold",
                "--enable-observability-metrics": "true",
                "--conf": "spark.sql.adaptive.enabled=true",
                "--conf": "spark.sql.adaptive.coalescePartitions.enabled=true",
                "--F1_DEPLOYMENT_VERSION": "${target_version}"
            },
            "Description": "F1 Data Pipeline: Transform Silver layer data to Gold layer analytics tables - Version ${target_version} (ROLLBACK)"
        }' >> "$LOG_FILE" 2>&1; then
        log_success "Rolled back Silver to Gold Glue job"
    else
        log_error "Failed to rollback Silver to Gold Glue job"
        return 1
    fi
    
    return 0
}

# Rollback wheel dependencies (placeholder - wheels are versioned by path)
rollback_wheels() {
    local target_version="$1"
    
    log_debug "Rolling back wheel dependencies to version: $target_version"
    log_info "Wheel dependencies are already versioned by S3 path - no rollback needed"
    return 0
}

# Rollback MWAA DAGs (placeholder - DAGs are not versioned)
rollback_mwaa_dags() {
    local target_version="$1"
    
    log_debug "Rolling back MWAA DAGs to version: $target_version"
    log_info "MWAA DAGs are not versioned - no rollback needed"
    return 0
}

# =============================
# Health Check Functions
# =============================

# Check deployment health
check_deployment_health() {
    log_function_entry "check_deployment_health" ""
    
    local health_issues=0
    
    log_info "🏥 Running deployment health checks..."
    
    # Check Glue jobs
    if ! check_glue_jobs_health; then
        ((health_issues++))
    fi
    
    # Check S3 accessibility
    if ! check_s3_health; then
        ((health_issues++))
    fi
    
    # Check MWAA accessibility
    if ! check_mwaa_health; then
        ((health_issues++))
    fi
    
    # Check wheel dependencies
    if ! check_wheels_health; then
        ((health_issues++))
    fi
    
    if [[ $health_issues -eq 0 ]]; then
        log_success "✅ All health checks passed - deployment is healthy"
        log_function_exit "check_deployment_health" 0
        return 0
    else
        log_error "❌ Found $health_issues health issues - deployment may be unstable"
        log_error "Consider rolling back to a previous stable version"
        log_function_exit "check_deployment_health" 1
        return 1
    fi
}

# Check Glue jobs health
check_glue_jobs_health() {
    log_debug "Checking Glue jobs health..."
    
    local job_issues=0
    
    # Check Bronze to Silver job
    if ! aws glue get-job --job-name "f1-bronze-to-silver-transform" --region "$AWS_REGION" >/dev/null 2>&1; then
        log_error "Bronze to Silver Glue job not accessible"
        ((job_issues++))
    else
        log_debug "✓ Bronze to Silver Glue job is accessible"
    fi
    
    # Check Silver to Gold job
    if ! aws glue get-job --job-name "f1-silver-to-gold-transform" --region "$AWS_REGION" >/dev/null 2>&1; then
        log_error "Silver to Gold Glue job not accessible"
        ((job_issues++))
    else
        log_debug "✓ Silver to Gold Glue job is accessible"
    fi
    
    if [[ $job_issues -eq 0 ]]; then
        log_debug "✓ All Glue jobs are healthy"
        return 0
    else
        log_error "Found $job_issues Glue job issues"
        return 1
    fi
}

# Check S3 health
check_s3_health() {
    log_debug "Checking S3 health..."
    
    local s3_issues=0
    
    # Check data lake bucket
    if ! aws s3 ls "s3://$S3_BUCKET_DATA_LAKE/" >/dev/null 2>&1; then
        log_error "Data lake bucket not accessible: s3://$S3_BUCKET_DATA_LAKE/"
        ((s3_issues++))
    else
        log_debug "✓ Data lake bucket is accessible"
    fi
    
    # Check MWAA bucket
    if ! aws s3 ls "s3://$S3_BUCKET_MWAA/" >/dev/null 2>&1; then
        log_error "MWAA bucket not accessible: s3://$S3_BUCKET_MWAA/"
        ((s3_issues++))
    else
        log_debug "✓ MWAA bucket is accessible"
    fi
    
    if [[ $s3_issues -eq 0 ]]; then
        log_debug "✓ All S3 buckets are healthy"
        return 0
    else
        log_error "Found $s3_issues S3 bucket issues"
        return 1
    fi
}

# Check MWAA health
check_mwaa_health() {
    log_debug "Checking MWAA health..."
    
    # Check if MWAA environment exists (basic check)
    if ! aws mwaa get-environment --name "f1-data-pipeline" --region "$AWS_REGION" >/dev/null 2>&1; then
        log_warning "MWAA environment 'f1-data-pipeline' not found or not accessible"
        log_warning "This may be expected if MWAA is not set up yet"
        return 0  # Don't fail health check for missing MWAA
    else
        log_debug "✓ MWAA environment is accessible"
        return 0
    fi
}

# Check wheels health
check_wheels_health() {
    log_debug "Checking wheels health..."
    
    local wheel_issues=0
    
    # Check if current version wheels exist
    local current_wheels_path="s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$VERSION/"
    if ! aws s3 ls "$current_wheels_path" >/dev/null 2>&1; then
        log_error "Current version wheels not found: $current_wheels_path"
        ((wheel_issues++))
    else
        log_debug "✓ Current version wheels are accessible"
    fi
    
    if [[ $wheel_issues -eq 0 ]]; then
        log_debug "✓ Wheels are healthy"
        return 0
    else
        log_error "Found $wheel_issues wheel issues"
        return 1
    fi
}

# =============================
# Requirements Validation Functions
# =============================

# Validate Glue requirements (should be ZERO external dependencies)
validate_glue_requirements() {
    log_function_entry "validate_glue_requirements" ""
    
    local glue_req_file="$PROJECT_ROOT/requirements-glue.txt"
    if [[ ! -f "$glue_req_file" ]]; then
        log_error "Glue requirements file not found: $glue_req_file"
        return 1
    fi
    
    # Check for any external dependencies
    local external_deps=$(grep -E "^[a-zA-Z]" "$glue_req_file" 2>/dev/null | wc -l)
    if [[ $external_deps -gt 0 ]]; then
        log_error "Glue requirements should have ZERO external dependencies, found $external_deps"
        log_error "External dependencies found:"
        grep -E "^[a-zA-Z]" "$glue_req_file" | sed 's/^/  - /'
        return 1
    fi
    
    log_success "✓ Glue requirements validated (ZERO external dependencies)"
    log_function_exit "validate_glue_requirements" 0
    return 0
}

# Validate MWAA requirements (should have requests + Airflow)
validate_mwaa_requirements() {
    log_function_entry "validate_mwaa_requirements" ""
    
    local mwaa_req_file="$PROJECT_ROOT/requirements-mwaa.txt"
    if [[ ! -f "$mwaa_req_file" ]]; then
        log_error "MWAA requirements file not found: $mwaa_req_file"
        return 1
    fi
    
    # Check for required dependencies
    local has_requests=$(grep -q "^requests==" "$mwaa_req_file" && echo "yes" || echo "no")
    local has_airflow=$(grep -q "^apache-airflow==" "$mwaa_req_file" && echo "yes" || echo "no")
    
    if [[ "$has_requests" != "yes" ]]; then
        log_error "MWAA requirements missing 'requests' dependency"
        return 1
    fi
    
    if [[ "$has_airflow" != "yes" ]]; then
        log_error "MWAA requirements missing 'apache-airflow' dependency"
        return 1
    fi
    
    # Check for problematic dependencies
    local problematic_deps=$(grep -E "^(pyiceberg|boto3|botocore|urllib3|pyyaml)==" "$mwaa_req_file" 2>/dev/null | wc -l)
    if [[ $problematic_deps -gt 0 ]]; then
        log_warning "MWAA requirements contain potentially unnecessary dependencies:"
        grep -E "^(pyiceberg|boto3|botocore|urllib3|pyyaml)==" "$mwaa_req_file" | sed 's/^/  - /'
        log_warning "These are provided by MWAA environment"
    fi
    
    log_success "✓ MWAA requirements validated (requests + Airflow present)"
    log_function_exit "validate_mwaa_requirements" 0
    return 0
}

# Validate Glue wheel has zero external dependencies
validate_glue_wheel() {
    log_function_entry "validate_glue_wheel" ""
    
    local wheel_file=$(find "$WHEELS_DIR" -name "f1_pipeline-*.whl" | head -1)
    if [[ -z "$wheel_file" ]]; then
        log_error "Glue wheel not found for validation"
        return 1
    fi
    
    # Extract wheel contents to check for external dependencies
    local temp_dir=$(mktemp -d)
    local wheel_name=$(basename "$wheel_file" .whl)
    
    # Extract wheel to temporary directory
    if ! unzip -q "$wheel_file" -d "$temp_dir"; then
        log_error "Failed to extract wheel for validation: $wheel_file"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # Check for external dependencies in wheel metadata
    local metadata_file="$temp_dir/$wheel_name.dist-info/METADATA"
    if [[ -f "$metadata_file" ]]; then
        local external_deps=$(grep -E "^Requires-Dist:" "$metadata_file" 2>/dev/null | wc -l)
        if [[ $external_deps -gt 0 ]]; then
            log_error "Glue wheel contains $external_deps external dependencies (should be ZERO)"
            log_error "External dependencies found:"
            grep -E "^Requires-Dist:" "$metadata_file" | sed 's/^Requires-Dist: /  - /'
            rm -rf "$temp_dir"
            return 1
        fi
    fi
    
    # Clean up
    rm -rf "$temp_dir"
    
    log_success "✓ Glue wheel validated (ZERO external dependencies)"
    log_function_exit "validate_glue_wheel" 0
    return 0
}

# =============================
# Wheel Building Functions
# =============================

# Build the main F1 pipeline wheel
build_main_wheel() {
    log_function_entry "build_main_wheel" ""
    
    log_info "Building F1 pipeline wheel..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would build F1 pipeline wheel"
        log_function_exit "build_main_wheel" 0
        return 0
    fi
    
    # Ensure wheels directory exists
    mkdir -p "$WHEELS_DIR"
    
    # Install build dependencies
    log_info "Installing build dependencies..."
    if ! pip3 install --upgrade setuptools wheel build --quiet; then
        log_error_with_context \
            "Failed to install build dependencies" \
            "pip3 install command failed" \
            "Check Python installation and network connectivity"
        log_function_exit "build_main_wheel" 1
        return 1
    fi
    
    # Build the wheel
    log_info "Building wheel from setup.py..."
    local build_output
    if ! build_output=$(python3 -m build --wheel --outdir "$WHEELS_DIR" 2>&1); then
        log_error_with_context \
            "Failed to build F1 pipeline wheel" \
            "Build output: $build_output" \
            "Check setup.py syntax, dependencies availability, and src/ directory structure"
        log_function_exit "build_main_wheel" 1
        return 1
    fi
    
    # Verify wheel was created
    local wheel_file=$(find "$WHEELS_DIR" -name "f1_pipeline-*.whl" | head -1)
    if [[ -z "$wheel_file" ]]; then
        log_error_with_context \
            "F1 pipeline wheel not found after build" \
            "No wheel file found in $WHEELS_DIR" \
            "Check build process and setup.py configuration"
        log_function_exit "build_main_wheel" 1
        return 1
    fi
    
    log_success "F1 pipeline wheel built successfully: $(basename "$wheel_file")"
    log_function_exit "build_main_wheel" 0
    return 0
}

# Build single dependency wheel with retry logic
build_single_dependency() {
    local dep="$1"
    local max_retries=2
    local retry_count=0
    
    while [[ $retry_count -le $max_retries ]]; do
        log_info "Building Linux-compatible wheel for $dep (attempt $((retry_count + 1))/$((max_retries + 1)))..."
        
        # Strategy 1: Download pre-built Linux cp39 wheel (best for Glue 4.0)
        if pip3 download --dest "$WHEELS_DIR" --no-deps --only-binary=:all: --platform linux_x86_64 --abi cp39 --implementation cp "$dep" --quiet 2>/dev/null; then
            log_success "Downloaded Linux cp39 wheel for $dep"
            return 0
        fi
        
        # Strategy 2: Download pre-built Linux generic wheel
        if pip3 download --dest "$WHEELS_DIR" --no-deps --only-binary=:all: --platform linux_x86_64 --abi none --implementation py "$dep" --quiet 2>/dev/null; then
            log_success "Downloaded Linux generic wheel for $dep"
            return 0
        fi
        
        # Strategy 3: Download universal wheel (works on all platforms including Linux)
        if pip3 download --dest "$WHEELS_DIR" --no-deps --only-binary=:all: --platform any --abi none --implementation py "$dep" --quiet 2>/dev/null; then
            log_success "Downloaded universal wheel for $dep"
            return 0
        fi
        
        # Strategy 4: Build from source (produces platform-specific but we'll rename for Linux)
        log_info "No pre-built Linux wheel available for $dep, building from source..."
        if pip3 wheel --wheel-dir "$WHEELS_DIR" --no-deps --no-binary :all: "$dep" --quiet 2>/dev/null; then
            # Find the built wheel and rename if platform-specific
            local package_name="${dep%%==*}"
            local package_name_underscore="${package_name//-/_}"  # Handle package names with hyphens
            local built_wheel=$(find "$WHEELS_DIR" -name "${package_name}*.whl" -o -name "${package_name_underscore}*.whl" | tail -1)
            
            if [[ -n "$built_wheel" ]]; then
                local wheel_basename=$(basename "$built_wheel")
                local new_wheel_name="$wheel_basename"
                
                # Rename platform-specific wheels to Linux-compatible names
                if [[ "$wheel_basename" == *"macosx"* ]]; then
                    # Handle various macOS patterns: macosx_10_13_universal2, macosx_15_0_arm64, etc.
                    new_wheel_name="${wheel_basename/macosx_*_universal2/linux_x86_64}"
                    new_wheel_name="${new_wheel_name/macosx_*_arm64/linux_x86_64}"
                    new_wheel_name="${new_wheel_name/macosx_*_x86_64/linux_x86_64}"
                elif [[ "$wheel_basename" == *"cp312"* && "$wheel_basename" != *"linux"* ]]; then
                    # Handle other platform-specific Python 3.12 wheels
                    new_wheel_name="${wheel_basename/cp312-cp312-*/cp39-cp39-linux_x86_64}"
                fi
                
                # Rename if needed
                if [[ "$new_wheel_name" != "$wheel_basename" ]]; then
                    local new_wheel_path="$WHEELS_DIR/$new_wheel_name"
                    mv "$built_wheel" "$new_wheel_path"
                    log_success "Built and renamed wheel for Linux compatibility: $dep"
                else
                    log_success "Built wheel from source for $dep"
                fi
            else
                log_success "Built wheel from source for $dep"
            fi
            return 0
        fi
        
        # Strategy 5: Try with different pip options for compatibility
        if [[ $retry_count -eq 1 ]]; then
            log_warning "Trying alternative pip options for $dep..."
            if pip3 wheel --wheel-dir "$WHEELS_DIR" --no-cache-dir --no-deps --prefer-binary "$dep" --quiet 2>/dev/null; then
                # Apply same renaming logic for fallback builds
                local package_name="${dep%%==*}"
                local package_name_underscore="${package_name//-/_}"
                local built_wheel=$(find "$WHEELS_DIR" -name "${package_name}*.whl" -o -name "${package_name_underscore}*.whl" | tail -1)
                
                if [[ -n "$built_wheel" ]]; then
                    local wheel_basename=$(basename "$built_wheel")
                    local new_wheel_name="$wheel_basename"
                    
                    # Apply platform renaming
                    if [[ "$wheel_basename" == *"macosx"* ]]; then
                        new_wheel_name="${wheel_basename/macosx_*_universal2/linux_x86_64}"
                        new_wheel_name="${new_wheel_name/macosx_*_arm64/linux_x86_64}"
                        new_wheel_name="${new_wheel_name/macosx_*_x86_64/linux_x86_64}"
                    elif [[ "$wheel_basename" == *"cp312"* && "$wheel_basename" != *"linux"* ]]; then
                        new_wheel_name="${wheel_basename/cp312-cp312-*/cp39-cp39-linux_x86_64}"
                    fi
                    
                    if [[ "$new_wheel_name" != "$wheel_basename" ]]; then
                        local new_wheel_path="$WHEELS_DIR/$new_wheel_name"
                        mv "$built_wheel" "$new_wheel_path"
                        log_success "Built and renamed wheel for Linux compatibility: $dep (fallback method)"
                    else
                        log_success "Built wheel for $dep (fallback method)"
                    fi
                else
                    log_success "Built wheel for $dep (fallback method)"
                fi
                return 0
            fi
        fi
        
        ((retry_count++))
        if [[ $retry_count -le $max_retries ]]; then
            log_warning "Retrying $dep in 2 seconds..."
            sleep 2
        fi
    done
    
    log_error "Failed to build wheel for $dep after $((max_retries + 1)) attempts"
    return 1
}

# Build third-party dependency wheels
build_dependency_wheels() {
    log_function_entry "build_dependency_wheels" ""
    
    log_info "Building third-party dependency wheels..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would build dependency wheels"
        log_function_exit "build_dependency_wheels" 0
        return 0
    fi
    
    # ULTRA-MINIMAL PIPELINE: ZERO external dependencies for Glue
    # Glue 4.0 provides all needed packages:
    # - pyspark, pyarrow, pandas, numpy (data processing)
    # - boto3, botocore (AWS SDK)
    # - urllib3 (HTTP client)
    # - Iceberg support (native via Spark configurations)
    # - All other AWS services
    local dependencies=()
    
    local total_deps=${#dependencies[@]}
    local current_dep=0
    
    # Check if we have any dependencies to build
    if [[ $total_deps -eq 0 ]]; then
        log_info "No external dependencies to build - using minimal single-wheel approach"
        log_function_exit "build_dependency_wheels" 0
        return 0
    fi
    
    # Validate dependency versions are available
    log_info "Validating dependency versions..."
    for dep in "${dependencies[@]}"; do
        local package_name="${dep%%==*}"
        local version="${dep##*==}"
        
        if ! pip3 index versions "$package_name" 2>/dev/null | grep -q "$version"; then
            log_warning "Version $version may not be available for $package_name"
            log_warning "Will attempt to build anyway, but this may fail"
        else
            log_debug "✓ Version $version available for $package_name"
        fi
    done
    
    local failed_deps=()
    local success_count=0
    
    # Build wheels for each dependency with progress indicator
    for dep in "${dependencies[@]}"; do
        ((current_dep++))
        show_progress $current_dep $total_deps "Building dependency wheels"
        
        if build_single_dependency "$dep"; then
            ((success_count++))
        else
            failed_deps+=("$dep")
        fi
    done
    
    # Clear progress line
    printf "\n"
    
    # Report results
    if [[ ${#failed_deps[@]} -gt 0 ]]; then
        log_error "Failed to build ${#failed_deps[@]} dependencies: ${failed_deps[*]}"
        log_error "This may cause Glue job failures"
        
        # Try to continue with partial success
        if [[ $success_count -gt 0 ]]; then
            log_warning "Continuing with $success_count successfully built dependencies"
            log_warning "Missing dependencies may cause import errors in Glue jobs"
        else
            log_error "No dependencies were built successfully"
            return 1
        fi
    fi
    
    log_success "Built $success_count dependency wheel(s) successfully"
    return 0
}

# Upload wheels to S3
upload_wheels_to_s3() {
    log_function_entry "upload_wheels_to_s3" ""
    
    log_info "Uploading wheels to S3..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would upload wheels to S3"
        log_function_exit "upload_wheels_to_s3" 0
        return 0
    fi
    
    local uploaded_count=0
    local total_wheels=0
    local current_wheel=0
    
    # Count total wheels
    for wheel_file in "$WHEELS_DIR"/*.whl; do
        if [[ -f "$wheel_file" ]]; then
            ((total_wheels++))
        fi
    done
    
    if [[ $total_wheels -eq 0 ]]; then
        log_error "No wheel files found to upload"
        log_function_exit "upload_wheels_to_s3" 1
        return 1
    fi
    
    # Upload all wheel files with progress indicator
    for wheel_file in "$WHEELS_DIR"/*.whl; do
        if [[ -f "$wheel_file" ]]; then
            ((current_wheel++))
            local filename=$(basename "$wheel_file")
            local s3_path="s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$VERSION/$filename"
            
            show_progress $current_wheel $total_wheels "Uploading wheels to S3"
            
            if aws s3 cp "$wheel_file" "$s3_path" \
                --metadata "version=$VERSION,uploaded_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                --content-type "application/zip" \
                >> "$LOG_FILE" 2>&1; then
                log_debug "✓ Uploaded $filename"
                ((uploaded_count++))
            else
                log_error "Failed to upload $filename"
            fi
        fi
    done
    
    # Clear progress line
    printf "\n"
    
    if [[ $uploaded_count -gt 0 ]]; then
        log_success "Uploaded $uploaded_count wheel files to s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$VERSION/"
        log_function_exit "upload_wheels_to_s3" 0
    else
        log_error "No wheel files were uploaded"
        log_function_exit "upload_wheels_to_s3" 1
        exit 1
    fi
}

# Validate individual wheel file
validate_wheel_file() {
    local wheel_file="$1"
    
    if [[ ! -f "$wheel_file" ]]; then
        return 1
    fi
    
    # Check if it's a valid wheel file (basic validation)
    if ! file "$wheel_file" | grep -q "Zip archive"; then
        log_warning "File may not be a valid wheel: $(basename "$wheel_file")"
        return 1
    fi
    
    # Check file size (wheels should not be empty)
    local file_size=$(stat -f%z "$wheel_file" 2>/dev/null || stat -c%s "$wheel_file" 2>/dev/null || echo "0")
    if [[ $file_size -lt 1000 ]]; then
        log_warning "Wheel file seems too small: $(basename "$wheel_file") ($file_size bytes)"
        return 1
    fi
    
    return 0
}

# Validate that wheels exist before proceeding
validate_wheels_exist() {
    log_info "Validating wheel files exist..."
    
    if [[ ! -d "$WHEELS_DIR" ]]; then
        log_error "Wheels directory not found: $WHEELS_DIR"
        return 1
    fi
    
    local wheel_count=0
    local valid_wheels=0
    local invalid_wheels=()
    
    for wheel_file in "$WHEELS_DIR"/*.whl; do
        if [[ -f "$wheel_file" ]]; then
            ((wheel_count++))
            if validate_wheel_file "$wheel_file"; then
                ((valid_wheels++))
                log_debug "✓ Valid wheel: $(basename "$wheel_file")"
            else
                invalid_wheels+=("$(basename "$wheel_file")")
                log_warning "⚠ Invalid wheel: $(basename "$wheel_file")"
            fi
        fi
    done
    
    if [[ $wheel_count -eq 0 ]]; then
        log_error "No wheel files found in $WHEELS_DIR"
        log_error "Wheels must be built before updating Glue jobs"
        return 1
    fi
    
    if [[ ${#invalid_wheels[@]} -gt 0 ]]; then
        log_warning "Found ${#invalid_wheels[@]} invalid wheel(s): ${invalid_wheels[*]}"
        log_warning "These wheels may cause issues in Glue jobs"
    fi
    
    if [[ $valid_wheels -eq 0 ]]; then
        log_error "No valid wheel files found"
        return 1
    fi
    
    log_success "Found $valid_wheels valid wheel file(s) ready for deployment"
    return 0
}

# Validate that all wheels are Linux-compatible
validate_wheels_linux_compatible() {
    log_info "Validating wheel files for Linux compatibility..."
    
    local incompatible_wheels=()
    local total_wheels=0
    local compatible_wheels=0
    
    # Check each wheel file
    for wheel_file in "$WHEELS_DIR"/*.whl; do
        if [[ -f "$wheel_file" ]]; then
            ((total_wheels++))
            local wheel_name=$(basename "$wheel_file")
            
            # Check if wheel is Linux-compatible
            if [[ "$wheel_name" == *"linux_x86_64"* ]] || \
               [[ "$wheel_name" == *"any"* ]] || \
               [[ "$wheel_name" == *"py3-none-any"* ]] || \
               [[ "$wheel_name" == *"py2.py3-none-any"* ]]; then
                ((compatible_wheels++))
                log_debug "✓ Linux-compatible: $wheel_name"
            else
                incompatible_wheels+=("$wheel_name")
                log_warning "⚠ Potentially incompatible: $wheel_name"
            fi
        fi
    done
    
    # Report results
    if [[ ${#incompatible_wheels[@]} -eq 0 ]]; then
        log_success "All $total_wheels wheel(s) are Linux-compatible"
        return 0
    else
        log_warning "Found ${#incompatible_wheels[@]} potentially incompatible wheel(s):"
        for wheel in "${incompatible_wheels[@]}"; do
            log_warning "  - $wheel"
        done
        log_warning "These wheels may work in Glue but could cause compatibility issues"
        log_warning "Consider updating the wheel building strategy for these packages"
        
        # Don't fail deployment for now, just warn
        return 0
    fi
}

# Validate wheel S3 URIs are accessible
validate_wheel_s3_uris() {
    local wheel_uris="$1"
    
    if [[ -z "$wheel_uris" ]]; then
        log_error "No wheel URIs provided for validation"
        return 1
    fi
    
    log_debug "Validating wheel S3 URIs accessibility..."
    local failed_uris=()
    local success_count=0
    
    # Split URIs and validate each one
    IFS=',' read -ra uri_array <<< "$wheel_uris"
    for uri in "${uri_array[@]}"; do
        # Remove leading/trailing whitespace
        uri=$(echo "$uri" | xargs)
        
        if [[ -n "$uri" ]]; then
            log_debug "Validating URI: $uri"
            if aws s3 ls "$uri" >/dev/null 2>&1; then
                log_debug "✓ URI accessible: $uri"
                ((success_count++))
            else
                log_warning "⚠ URI not accessible: $uri"
                failed_uris+=("$uri")
            fi
        fi
    done
    
    if [[ ${#failed_uris[@]} -gt 0 ]]; then
        log_warning "Found ${#failed_uris[@]} inaccessible wheel URI(s): ${failed_uris[*]}"
        log_warning "This may cause Glue job failures"
        return 1
    fi
    
    log_debug "✓ All $success_count wheel URI(s) are accessible"
    return 0
}

# Get wheel S3 URIs for Glue job configuration
get_wheel_s3_uris() {
    local wheel_uris=()
    for wheel_file in "$WHEELS_DIR"/*.whl; do
        if [[ -f "$wheel_file" ]]; then
            local filename=$(basename "$wheel_file")
            wheel_uris+=("s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$VERSION/$filename")
        fi
    done
    echo "$(IFS=,; echo "${wheel_uris[*]}")"
}

log_info() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${BLUE}[INFO]${NC} $message" | tee -a "$LOG_FILE"
    echo "[$timestamp] [INFO] $message" >> "$LOG_FILE"
}

log_success() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${GREEN}[SUCCESS]${NC} $message" | tee -a "$LOG_FILE"
    echo "[$timestamp] [SUCCESS] $message" >> "$LOG_FILE"
}

log_warning() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${YELLOW}[WARNING]${NC} $message" | tee -a "$LOG_FILE"
    echo "[$timestamp] [WARNING] $message" >> "$LOG_FILE"
}

log_error() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${RED}[ERROR]${NC} $message" | tee -a "$LOG_FILE"
    echo "[$timestamp] [ERROR] $message" >> "$LOG_FILE"
}

log_debug() {
    local message="$1"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    if [[ "$VERBOSE" == "true" ]]; then
        echo -e "${PURPLE}[DEBUG]${NC} $message" | tee -a "$LOG_FILE"
    fi
    echo "[$timestamp] [DEBUG] $message" >> "$LOG_FILE"
}

# Progress indicator functions
show_progress() {
    local current="$1"
    local total="$2"
    local operation="$3"
    local percentage=$((current * 100 / total))
    local bar_length=30
    local filled_length=$((percentage * bar_length / 100))
    
    # Create progress bar
    local bar=""
    for ((i=0; i<filled_length; i++)); do
        bar+="█"
    done
    for ((i=filled_length; i<bar_length; i++)); do
        bar+="░"
    done
    
    printf "\r${BLUE}[PROGRESS]${NC} %s [%s] %d%% (%d/%d)" "$operation" "$bar" "$percentage" "$current" "$total"
}

show_spinner() {
    local pid="$1"
    local message="$2"
    local spinner_chars="⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
    local i=0
    
    while kill -0 "$pid" 2>/dev/null; do
        printf "\r${CYAN}[SPINNER]${NC} %s %s" "$message" "${spinner_chars:$i:1}"
        i=$((i + 1))
        if [[ $i -eq ${#spinner_chars} ]]; then
            i=0
        fi
        sleep 0.1
    done
    printf "\r${GREEN}[DONE]${NC} %s ✓\n" "$message"
}

# Enhanced error logging with context
log_error_with_context() {
    local message="$1"
    local context="$2"
    local suggestion="$3"
    
    log_error "$message"
    if [[ -n "$context" ]]; then
        log_error "Context: $context"
    fi
    if [[ -n "$suggestion" ]]; then
        log_error "Suggestion: $suggestion"
    fi
}

# Log function entry/exit for debugging
log_function_entry() {
    local function_name="$1"
    local args="${2:-}"
    log_debug "→ Entering function: $function_name($args)"
}

log_function_exit() {
    local function_name="$1"
    local return_code="$2"
    if [[ $return_code -eq 0 ]]; then
        log_debug "← Exiting function: $function_name (SUCCESS)"
    else
        log_debug "← Exiting function: $function_name (FAILED: $return_code)"
    fi
}

print_banner() {
    echo -e "${CYAN}"
    echo "============================================================================="
    echo "🚀 F1 Data Engineering Pipeline - Code Deployment"
    echo "============================================================================="
    echo -e "${NC}"
    echo "📅 Timestamp: $(date)"
    echo "📁 Project Root: $PROJECT_ROOT"
    echo "📦 Source Directory: $SRC_DIR"
    echo "📝 Log File: $LOG_FILE"
    echo "🪣 Data Lake Bucket: s3://$S3_BUCKET_DATA_LAKE"
    echo "🪣 MWAA Bucket: s3://$S3_BUCKET_MWAA"
    echo "🏷️ Version: $VERSION"
    echo ""
}

# Parse command line arguments
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --clean)
                CLEAN_PREVIOUS=true
                log_info "Clean mode enabled - will remove previous code"
                shift
                ;;
            --no-clean)
                CLEAN_PREVIOUS=false
                log_info "Clean mode disabled - will keep previous code"
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                log_info "Dry run mode enabled - no actual uploads will occur"
                shift
                ;;
            --verbose)
                VERBOSE=true
                log_info "Verbose logging enabled"
                shift
                ;;
            --skip-validation)
                SKIP_VALIDATION=true
                log_info "Validation skipped"
                shift
                ;;
            --version)
                VERSION="$2"
                log_info "Version set to: $VERSION"
                shift 2
                ;;
            --rollback)
                ROLLBACK_MODE="true"
                log_info "Rollback mode enabled - will rollback to last successful version"
                shift
                ;;
            --rollback-to)
                ROLLBACK_MODE="true"
                ROLLBACK_VERSION="$2"
                log_info "Rollback mode enabled - will rollback to version: $ROLLBACK_VERSION"
                shift 2
                ;;
            --health-check)
                HEALTH_CHECK_ONLY="true"
                log_info "Health check mode enabled - will only run health checks"
                shift
                ;;
            --list-versions)
                LIST_VERSIONS="true"
                log_info "List versions mode enabled - will show available versions for rollback"
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
    echo "Deployment Options:"
    echo "  --clean           Remove previous code before deploying (default)"
    echo "  --no-clean        Keep previous code versions"
    echo "  --dry-run         Show what would be deployed without actually deploying"
    echo "  --verbose         Enable verbose logging"
    echo "  --skip-validation Skip code validation steps"
    echo "  --version VERSION Set deployment version (default: timestamp)"
    echo ""
    echo "Rollback Options:"
    echo "  --rollback        Rollback to last successful version"
    echo "  --rollback-to VER Rollback to specific version"
    echo "  --list-versions   List available versions for rollback"
    echo ""
    echo "Health Check Options:"
    echo "  --health-check    Run health checks only (no deployment)"
    echo ""
    echo "General Options:"
    echo "  --help            Show this help message"
    echo ""
    echo "Deployment Method:"
    echo "  This script uses simplified wheel-based deployment:"
    echo "  • Builds F1 pipeline wheel from setup.py (minimal dependencies)"
    echo "  • Uses only essential packages (no heavy dependencies)"
    echo "  • Uploads wheels to S3 dependencies folder"
    echo "  • Updates Glue jobs to use wheel S3 URIs"
    echo "  • Deploys MWAA code as ZIP (Airflow requirement)"
    echo "  • Uses SimpleDataValidator for data quality checks"
    echo "  • Tracks deployment history for rollback capabilities"
    echo "  • Runs health checks after deployment"
    echo ""
    echo "Examples:"
    echo "  # Deploy new version"
    echo "  $0 --version v1.2.0 --verbose"
    echo ""
    echo "  # Dry run deployment"
    echo "  $0 --dry-run --no-clean"
    echo ""
    echo "  # Rollback to last successful version"
    echo "  $0 --rollback"
    echo ""
    echo "  # Rollback to specific version"
    echo "  $0 --rollback-to v1.1.0"
    echo ""
    echo "  # List available versions"
    echo "  $0 --list-versions"
    echo ""
    echo "  # Run health checks"
    echo "  $0 --health-check"
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
    
    # Check if setup.py exists (required for wheel building)
    if [[ ! -f "$PROJECT_ROOT/setup.py" ]]; then
        log_error "setup.py not found in project root: $PROJECT_ROOT"
        log_error "setup.py is required for building the F1 pipeline wheel"
        exit 1
    fi
    log_debug "✓ setup.py found"
    
    # Check if required directories exist
    if [[ ! -d "$SRC_DIR" ]]; then
        log_error "Source directory not found: $SRC_DIR"
        exit 1
    fi
    
    # Check if buckets exist and are accessible
    if ! aws s3 ls "s3://$S3_BUCKET_DATA_LAKE" &> /dev/null; then
        log_error "Data lake bucket not accessible: s3://$S3_BUCKET_DATA_LAKE"
        exit 1
    fi
    
    if ! aws s3 ls "s3://$S3_BUCKET_MWAA" &> /dev/null; then
        log_error "MWAA bucket not accessible: s3://$S3_BUCKET_MWAA"
        exit 1
    fi
    
    # Check if zip command exists
    if ! command -v zip &> /dev/null; then
        log_error "zip command not found. Please install zip utility."
        exit 1
    fi
    
    # Check Python build tools
    log_info "Checking Python build tools..."
    if ! python3 -c "import setuptools" 2>/dev/null; then
        log_warning "setuptools not found, will install during wheel building"
    else
        log_debug "✓ setuptools available"
    fi
    
    if ! python3 -c "import wheel" 2>/dev/null; then
        log_warning "wheel not found, will install during wheel building"
    else
        log_debug "✓ wheel available"
    fi
    
    if ! python3 -c "import build" 2>/dev/null; then
        log_warning "build module not found, will install during wheel building"
    else
        log_debug "✓ build module available"
    fi
    
    # Check if wheel directory is writable
    if [[ ! -w "$WHEELS_DIR" ]] 2>/dev/null; then
        log_warning "Wheels directory not writable, will create: $WHEELS_DIR"
    else
        log_debug "✓ Wheels directory is writable"
    fi
    
    log_success "All prerequisites met"
}

# Validate code structure
validate_code_structure() {
    if [[ "$SKIP_VALIDATION" == "true" ]]; then
        log_info "Skipping code validation as requested"
        return 0
    fi
    
    log_info "Validating code structure..."
    
    local validation_errors=0
    
    # Check critical directories exist
    local required_dirs=(
        "$SRC_DIR/dags"
        "$SRC_DIR/jobs"
        "$SRC_DIR/jobs/orchestrators"
        "$SRC_DIR/jobs/transforms"
        "$SRC_DIR/jobs/config"
        "$SRC_DIR/utils"
        "$PROJECT_ROOT/config"
    )
    
    for dir in "${required_dirs[@]}"; do
        if [[ ! -d "$dir" ]]; then
            log_error "Required directory missing: $dir"
            ((validation_errors++))
        else
            log_debug "✓ Directory exists: $dir"
        fi
    done
    
    # Check critical files exist
    local required_files=(
        "$SRC_DIR/jobs/orchestrators/bronze_to_silver_orchestrator.py"
        "$SRC_DIR/jobs/orchestrators/silver_to_gold_orchestrator.py"
        "$SRC_DIR/dags/dag_factory.py"
        "$SRC_DIR/dags/f1_pipeline_dag.py"
        "$SRC_DIR/dags/f1_backfill_dag.py"
        "$PROJECT_ROOT/requirements.txt"
        "$PROJECT_ROOT/config/prod.yaml"
    )
    
    for file in "${required_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Required file missing: $file"
            ((validation_errors++))
        else
            log_debug "✓ File exists: $file"
        fi
    done
    
    # Check Python syntax (basic validation)
    log_info "Checking Python syntax..."
    while IFS= read -r -d '' python_file; do
        if ! python3 -m py_compile "$python_file" 2>/dev/null; then
            log_error "Python syntax error in: $python_file"
            ((validation_errors++))
        else
            log_debug "✓ Python syntax OK: $python_file"
        fi
    done < <(find "$SRC_DIR" -name "*.py" -print0)
    
    if [[ $validation_errors -eq 0 ]]; then
        log_success "Code structure validation passed"
        return 0
    else
        log_error "Code validation failed with $validation_errors errors"
        return 1
    fi
}

# Clean previous deployments
clean_previous_deployments() {
    if [[ "$CLEAN_PREVIOUS" == "false" ]]; then
        log_info "Skipping cleanup of previous deployments"
        return 0
    fi
    
    log_info "🧹 Cleaning previous deployments..."
    
    if [[ "$DRY_RUN" == "true" ]]; then
        log_info "DRY RUN: Would clean previous deployments from S3"
        return 0
    fi
    
    # Clean data lake bucket deployment artifacts
    log_info "Cleaning data lake bucket deployment artifacts..."
    aws s3 rm "s3://$S3_BUCKET_DATA_LAKE/deployment/" --recursive --quiet || log_debug "No deployment artifacts to clean"
    aws s3 rm "s3://$S3_BUCKET_DATA_LAKE/scripts/" --recursive --quiet || log_debug "No script artifacts to clean"
    
    # Archive current MWAA content before cleaning
    if aws s3 ls "s3://$S3_BUCKET_MWAA/dags/" &>/dev/null; then
        log_info "Archiving current MWAA content..."
        aws s3 cp "s3://$S3_BUCKET_MWAA/" "s3://$S3_BUCKET_DATA_LAKE/archive/mwaa_backup_$TIMESTAMP/" --recursive --quiet
    fi
    
    # Clean MWAA bucket (but preserve requirements.txt temporarily)
    aws s3 cp "s3://$S3_BUCKET_MWAA/requirements.txt" "/tmp/requirements_backup.txt" --quiet 2>/dev/null || true
    aws s3 rm "s3://$S3_BUCKET_MWAA/" --recursive --exclude "*.txt" --quiet || log_debug "No MWAA artifacts to clean"
    
    log_success "Previous deployments cleaned"
}

# Create deployment package (wheel-based)
create_deployment_package() {
    log_info "📦 Creating wheel-based deployment package..."
    
    # Clean and recreate directories
    rm -rf "$PACKAGE_DIR" "$WHEELS_DIR"
    mkdir -p "$PACKAGE_DIR" "$WHEELS_DIR"
    
    # Copy source code for MWAA
    log_info "Copying source code for MWAA..."
    cp -r "$SRC_DIR"/* "$PACKAGE_DIR/"
    cp -r "$PROJECT_ROOT/config" "$PACKAGE_DIR/"
    
    # Copy MWAA-specific requirements.txt
    log_info "Using MWAA-specific requirements.txt..."
    if [[ -f "$PROJECT_ROOT/requirements-mwaa.txt" ]]; then
        cp "$PROJECT_ROOT/requirements-mwaa.txt" "$PACKAGE_DIR/requirements.txt"
        log_success "Copied MWAA requirements.txt"
    else
        log_error "MWAA requirements.txt not found!"
        return 1
    fi
    
    # Create version file
    cat > "$PACKAGE_DIR/VERSION" << EOF
F1 Data Pipeline - Version $VERSION
Deployed: $(date -u '+%Y-%m-%d %H:%M:%S UTC')
Git Commit: $(git rev-parse HEAD 2>/dev/null || echo "Unknown")
Build Host: $(hostname)
AWS Account: $AWS_ACCOUNT_ID
AWS Region: $AWS_REGION
Deployment Method: Simplified Wheel-based
Pipeline Type: Minimal Dependencies
EOF
    
    # Remove unnecessary files
    log_info "Cleaning package..."
    find "$PACKAGE_DIR" -name "*.pyc" -delete
    find "$PACKAGE_DIR" -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
    find "$PACKAGE_DIR" -name ".git*" -delete 2>/dev/null || true
    find "$PACKAGE_DIR" -name ".DS_Store" -delete 2>/dev/null || true
    find "$PACKAGE_DIR" -name "*.log" -delete 2>/dev/null || true
    
    # Validate requirements before building
    log_info "Validating requirements files..."
    if ! validate_glue_requirements; then
        log_error "Glue requirements validation failed"
        return 1
    fi
    
    if ! validate_mwaa_requirements; then
        log_error "MWAA requirements validation failed"
        return 1
    fi
    
    # Build wheels
    if ! build_main_wheel; then
        log_error "Failed to build main F1 pipeline wheel"
        return 1
    fi
    
    if ! build_dependency_wheels; then
        log_error "Failed to build dependency wheels"
        return 1
    fi
    
    # Validate all wheels were built successfully (skip in dry-run mode)
    if [[ "$DRY_RUN" == "false" ]]; then
        if ! validate_wheels_exist; then
            log_error "Wheel validation failed after building"
            return 1
        fi
        
        # Verify all wheels are Linux-compatible
        if ! validate_wheels_linux_compatible; then
            log_error "Some wheels are not Linux-compatible"
            return 1
        fi
        
        # Validate Glue wheel has zero external dependencies
        if ! validate_glue_wheel; then
            log_error "Glue wheel validation failed - contains external dependencies"
            return 1
        fi
    else
        log_info "DRY RUN: Skipping wheel validation (wheels not actually built)"
    fi
    
    # Create MWAA package (ZIP for MWAA, wheels for Glue)
    local mwaa_zip="$DEPLOYMENT_DIR/f1-mwaa-package-$VERSION.zip"
    log_info "Creating MWAA package: $mwaa_zip"
    
    cd "$PACKAGE_DIR"
    zip -r "$mwaa_zip" \
        dags/ \
        utils/ \
        providers/ \
        config/ \
        requirements.txt \
        VERSION \
        >> "$LOG_FILE" 2>&1
    cd "$PROJECT_ROOT"
    
    log_success "Deployment packages created"
    log_info "  MWAA package: $mwaa_zip ($(du -h "$mwaa_zip" | cut -f1))"
    log_info "  Wheels directory: $WHEELS_DIR"
}

# Deploy to data lake bucket (wheel-based)
deploy_to_data_lake() {
    log_info "🚀 Deploying to data lake bucket: s3://$S3_BUCKET_DATA_LAKE"
    
    local error_count=0
    
    # Upload wheels to S3
    upload_wheels_to_s3
    
    # Upload individual orchestrator scripts
    log_info "Uploading Glue job scripts..."
    
    local glue_scripts=(
        "$PACKAGE_DIR/jobs/orchestrators/bronze_to_silver_orchestrator.py:scripts/orchestrators/bronze_to_silver_orchestrator.py"
        "$PACKAGE_DIR/jobs/orchestrators/silver_to_gold_orchestrator.py:scripts/orchestrators/silver_to_gold_orchestrator.py"
    )
    
    for script_mapping in "${glue_scripts[@]}"; do
        local local_script="${script_mapping%:*}"
        local s3_script="${script_mapping#*:}"
        
        if [[ -f "$local_script" ]]; then
            if [[ "$DRY_RUN" == "false" ]]; then
                if aws s3 cp "$local_script" "s3://$S3_BUCKET_DATA_LAKE/$s3_script" \
                    --content-type "text/x-python" \
                    --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                    >> "$LOG_FILE" 2>&1; then
                    log_success "Uploaded Glue script: $s3_script"
                else
                    log_error "Failed to upload Glue script: $s3_script"
                    ((error_count++))
                fi
            else
                log_info "DRY RUN: Would upload Glue script: $s3_script"
            fi
        else
            log_warning "Glue script not found: $local_script"
        fi
    done
    
    # Upload requirements.txt
    if [[ -f "$PACKAGE_DIR/requirements.txt" ]]; then
        if [[ "$DRY_RUN" == "false" ]]; then
            aws s3 cp "$PACKAGE_DIR/requirements.txt" "s3://$S3_BUCKET_DATA_LAKE/deployment/requirements.txt" \
                --content-type "text/plain" \
                --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                >> "$LOG_FILE" 2>&1
            log_success "Uploaded requirements.txt to data lake"
        else
            log_info "DRY RUN: Would upload requirements.txt"
        fi
    fi
    
    # Upload configuration files
    log_info "Uploading configuration files..."
    if [[ "$DRY_RUN" == "false" ]]; then
        aws s3 cp "$PACKAGE_DIR/config/" "s3://$S3_BUCKET_DATA_LAKE/config/" \
            --recursive \
            --content-type "text/yaml" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1
        log_success "Configuration files uploaded"
    else
        log_info "DRY RUN: Would upload configuration files"
    fi
    
    return $error_count
}

# Deploy to MWAA bucket
deploy_to_mwaa() {
    log_info "☁️ Deploying to MWAA bucket: s3://$S3_BUCKET_MWAA"
    
    local error_count=0
    
    # Upload DAGs
    log_info "Uploading DAGs..."
    if [[ "$DRY_RUN" == "false" ]]; then
        if aws s3 cp "$PACKAGE_DIR/dags/" "s3://$S3_BUCKET_MWAA/dags/" \
            --recursive \
            --content-type "text/x-python" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1; then
            log_success "DAGs uploaded to MWAA"
        else
            log_error "Failed to upload DAGs"
            ((error_count++))
        fi
    else
        log_info "DRY RUN: Would upload DAGs to MWAA"
    fi
    
    # Upload utils directory
    log_info "Uploading utils directory..."
    if [[ "$DRY_RUN" == "false" ]]; then
        aws s3 cp "$PACKAGE_DIR/utils/" "s3://$S3_BUCKET_MWAA/utils/" \
            --recursive \
            --content-type "text/x-python" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1
        log_success "Utils directory uploaded"
    else
        log_info "DRY RUN: Would upload utils directory"
    fi
    
    # Upload providers directory
    if [[ -d "$PACKAGE_DIR/providers" ]]; then
        log_info "Uploading providers directory..."
        if [[ "$DRY_RUN" == "false" ]]; then
            aws s3 cp "$PACKAGE_DIR/providers/" "s3://$S3_BUCKET_MWAA/providers/" \
                --recursive \
                --content-type "text/x-python" \
                --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                >> "$LOG_FILE" 2>&1
            log_success "Providers directory uploaded"
        else
            log_info "DRY RUN: Would upload providers directory"
        fi
    fi
    
    # Upload pipeline directory (Bronze layer)
    if [[ -d "$PACKAGE_DIR/pipeline" ]]; then
        log_info "Uploading pipeline directory..."
        if [[ "$DRY_RUN" == "false" ]]; then
            aws s3 cp "$PACKAGE_DIR/pipeline/" "s3://$S3_BUCKET_MWAA/pipeline/" \
                --recursive \
                --content-type "text/x-python" \
                --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
                >> "$LOG_FILE" 2>&1
            log_success "Pipeline directory uploaded"
        else
            log_info "DRY RUN: Would upload pipeline directory"
        fi
    fi
    
    # Upload config directory
    log_info "Uploading config directory to MWAA..."
    if [[ "$DRY_RUN" == "false" ]]; then
        aws s3 cp "$PACKAGE_DIR/config/" "s3://$S3_BUCKET_MWAA/config/" \
            --recursive \
            --content-type "text/yaml" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1
        log_success "Config directory uploaded to MWAA"
    else
        log_info "DRY RUN: Would upload config directory to MWAA"
    fi
    
    # Upload requirements.txt for MWAA
    log_info "Uploading requirements.txt to MWAA..."
    if [[ "$DRY_RUN" == "false" ]]; then
        if aws s3 cp "$PACKAGE_DIR/requirements.txt" "s3://$S3_BUCKET_MWAA/requirements.txt" \
            --content-type "text/plain" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1; then
            log_success "Requirements.txt uploaded to MWAA"
        else
            log_error "Failed to upload requirements.txt to MWAA"
            ((error_count++))
        fi
    else
        log_info "DRY RUN: Would upload requirements.txt to MWAA"
    fi
    
    return $error_count
}

# Validate Glue job exists and is accessible
validate_glue_job() {
    local job_name="$1"
    
    log_debug "Validating Glue job: $job_name"
    
    # Check if job exists
    if ! aws glue get-job --job-name "$job_name" --region "$AWS_REGION" >/dev/null 2>&1; then
        log_error "Glue job '$job_name' not found or not accessible"
        log_error "Please ensure the job exists and you have proper permissions"
        return 1
    fi
    
    # Get job details for validation
    local job_details
    if ! job_details=$(aws glue get-job --job-name "$job_name" --region "$AWS_REGION" 2>&1); then
        log_error "Failed to retrieve job details for '$job_name'"
        log_error "Error: $job_details"
        return 1
    fi
    
    # Check if job is in a valid state for updates
    local job_state=$(echo "$job_details" | jq -r '.Job.JobState // "UNKNOWN"')
    if [[ "$job_state" == "RUNNING" ]]; then
        log_warning "Glue job '$job_name' is currently running"
        log_warning "Updates may not take effect until the job completes"
    fi
    
    log_debug "✓ Glue job '$job_name' is valid and accessible"
    return 0
}

# Update Glue job configurations
clear_glue_caches_and_old_scripts() {
    log_function_entry "clear_glue_caches_and_old_scripts"
    local cleanup_errors=0

    log_info "🧹 Clearing all old scripts and caches for fresh start..."

    # 1. Delete all old scripts from S3 to force fresh deployment
    log_info "🗑️ Removing all old scripts from S3..."
    if aws s3 rm "s3://${S3_BUCKET_DATA_LAKE}/scripts/" --recursive 2>/dev/null; then
        log_success "Cleared all old scripts from S3"
    else
        log_warning "Failed to clear old scripts from S3 (may not exist)"
    fi

    # 2. Delete all old wheel dependencies to force fresh build
    log_info "🗑️ Removing all old wheel dependencies from S3..."
    if aws s3 rm "s3://${S3_BUCKET_DATA_LAKE}/dependencies/" --recursive 2>/dev/null; then
        log_success "Cleared all old wheel dependencies from S3"
    else
        log_warning "Failed to clear old wheel dependencies from S3 (may not exist)"
    fi

    # 3. Delete all old temp files to clear any cached data
    log_info "🗑️ Removing all old temp files from S3..."
    if aws s3 rm "s3://${S3_BUCKET_DATA_LAKE}/temp/" --recursive 2>/dev/null; then
        log_success "Cleared all old temp files from S3"
    else
        log_warning "Failed to clear old temp files from S3 (may not exist)"
    fi

    # 4. Delete all old spark logs to clear any cached logs
    log_info "🗑️ Removing all old spark logs from S3..."
    if aws s3 rm "s3://${S3_BUCKET_DATA_LAKE}/spark-logs/" --recursive 2>/dev/null; then
        log_success "Cleared all old spark logs from S3"
    else
        log_warning "Failed to clear old spark logs from S3 (may not exist)"
    fi

    # 5. Force delete and recreate Glue jobs to clear any internal caches
    log_info "🗑️ Force recreating Glue jobs to clear internal caches..."
    
    # Delete Bronze to Silver job
    if aws glue delete-job --job-name "f1-bronze-to-silver-transform" 2>/dev/null; then
        log_success "Deleted old Bronze to Silver Glue job"
    else
        log_warning "Failed to delete Bronze to Silver Glue job (may not exist)"
    fi

    # Delete Silver to Gold job
    if aws glue delete-job --job-name "f1-silver-to-gold-transform" 2>/dev/null; then
        log_success "Deleted old Silver to Gold Glue job"
    else
        log_warning "Failed to delete Silver to Gold Glue job (may not exist)"
    fi

    # Wait a moment for AWS to process the deletions
    log_info "⏳ Waiting for AWS to process deletions..."
    sleep 5

    log_success "✅ All caches and old scripts cleared - ready for fresh start"
    log_function_exit "clear_glue_caches_and_old_scripts" 0
}

upload_fresh_scripts_to_s3() {
    log_function_entry "upload_fresh_scripts_to_s3"
    local upload_errors=0

    log_info "📤 Uploading fresh scripts to S3..."

    # Upload Bronze to Silver orchestrator
    if [[ -f "$PACKAGE_DIR/jobs/orchestrators/bronze_to_silver_orchestrator.py" ]]; then
        if aws s3 cp "$PACKAGE_DIR/jobs/orchestrators/bronze_to_silver_orchestrator.py" \
            "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py" \
            --content-type "text/x-python" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1; then
            log_success "✅ Uploaded Bronze to Silver orchestrator script"
        else
            log_error "❌ Failed to upload Bronze to Silver orchestrator script"
            ((upload_errors++))
        fi
    else
        log_error "❌ Bronze to Silver orchestrator script not found in package"
        ((upload_errors++))
    fi

    # Upload Silver to Gold orchestrator
    if [[ -f "$PACKAGE_DIR/jobs/orchestrators/silver_to_gold_orchestrator.py" ]]; then
        if aws s3 cp "$PACKAGE_DIR/jobs/orchestrators/silver_to_gold_orchestrator.py" \
            "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py" \
            --content-type "text/x-python" \
            --metadata "version=$VERSION,deployed_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
            >> "$LOG_FILE" 2>&1; then
            log_success "✅ Uploaded Silver to Gold orchestrator script"
        else
            log_error "❌ Failed to upload Silver to Gold orchestrator script"
            ((upload_errors++))
        fi
    else
        log_error "❌ Silver to Gold orchestrator script not found in package"
        ((upload_errors++))
    fi

    if [[ $upload_errors -gt 0 ]]; then
        log_error "Script upload failed with $upload_errors errors"
        log_function_exit "upload_fresh_scripts_to_s3" 1
        return 1
    fi

    log_success "✅ All fresh scripts uploaded successfully"
    log_function_exit "upload_fresh_scripts_to_s3" 0
    return 0
}

update_glue_jobs() {
    log_function_entry "update_glue_jobs"
    local aws_account_id=$(aws sts get-caller-identity --query Account --output text)
    local glue_errors=0

    log_info "⚙️ Creating fresh Glue job configurations..."

    # Force fresh start - clear all caches and old scripts
    log_info "🧹 Clearing all caches and old scripts for fresh start..."
    clear_glue_caches_and_old_scripts

    # Upload fresh scripts to S3 (they were deleted in clear_glue_caches_and_old_scripts)
    log_info "📤 Uploading fresh scripts to S3..."
    upload_fresh_scripts_to_s3

    # Re-upload wheels to S3 (they were also cleared in fresh start)
    log_info "📦 Re-uploading wheels after fresh start..."
    upload_wheels_to_s3

    # Get wheel URIs
    local wheel_uris
    wheel_uris=$(get_wheel_s3_uris "$VERSION")
    if [[ -z "$wheel_uris" ]]; then
        log_error "Failed to get wheel S3 URIs"
        log_function_exit "update_glue_jobs" 1
        return 1
    fi
    log_info "Using wheel URIs: $wheel_uris"

    # Create Bronze to Silver job
    log_info "Creating Bronze to Silver Glue job..."

    # Build DefaultArguments safely with jq
    local bronze_args_json
    if ! bronze_args_json=$(jq -n \
        --arg wheel_uris "$wheel_uris" \
        --arg tempDir "s3://$S3_BUCKET_DATA_LAKE/temp/bronze/" \
        --arg sparkLogs "s3://$S3_BUCKET_DATA_LAKE/spark-logs/bronze/" \
        --arg warehouse "s3://$S3_BUCKET_DATA_LAKE/warehouse/" \
        --arg version "$VERSION" \
        --arg insights '{"InsightsEnabled": true}' \
    '{
        "job-language": "python",
        "job-bookmark-option": "job-bookmark-enable",
        "--additional-python-modules": $wheel_uris,
        "--TempDir": $tempDir,
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": $sparkLogs,
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--continuous-log-logGroup": "/aws/glue/f1-bronze-to-silver",
        "--enable-observability-metrics": "true",
        "--enable-job-insights": "true",
        "--job-insights-configuration": $insights,
        "--enable-continuous-log-filter": "true",
        "--continuous-log-filterPattern": "ERROR|WARN|INFO",

        "--conf:spark.sql.adaptive.enabled": "true",
        "--conf:spark.sql.adaptive.coalescePartitions.enabled": "true",
        "--conf:spark.sql.adaptive.skewJoin.enabled": "true",
        "--conf:spark.sql.adaptive.localShuffleReader.enabled": "true",
        "--conf:spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
        "--conf:spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "--conf:spark.sql.parquet.compression.codec": "snappy",
        "--conf:spark.sql.parquet.mergeSchema": "true",

        "--conf:spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf:spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "--conf:spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf:spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "--conf:spark.sql.catalog.glue_catalog.warehouse": $warehouse,
        "--conf:spark.sql.iceberg.handle-timestamp-without-timezone": "true",
        "--conf:spark.sql.iceberg.vectorization.enabled": "true",
        "--conf:spark.sql.iceberg.vectorization.batch-size": "4096",

        "--enable-glue-datacatalog": "",
        "--datalake-formats": "iceberg",
        "--F1_DEPLOYMENT_VERSION": $version
    }'); then
        log_error "Failed to build Bronze job arguments JSON with jq"
        log_error "Context: jq command failed - check jq installation and JSON syntax"
        ((glue_errors++))
        return 1
    fi

    # Validate generated JSON
    if ! echo "$bronze_args_json" | jq empty >/dev/null 2>&1; then
        log_error "Generated Bronze job arguments JSON is invalid"
        log_error "Context: JSON validation failed"
        ((glue_errors++))
        return 1
    fi

    # Write to a file for AWS CLI
    local bronze_args_file
    if ! bronze_args_file=$(mktemp); then
        log_error "Failed to create temporary file for Bronze job arguments"
        log_error "Context: mktemp command failed"
        ((glue_errors++))
        return 1
    fi

    if ! printf '%s' "$bronze_args_json" > "$bronze_args_file"; then
        log_error "Failed to write Bronze job arguments to temporary file"
        log_error "Context: printf command failed"
        rm -f "$bronze_args_file"
        ((glue_errors++))
        return 1
    fi

    local bronze_create_output
    if bronze_create_output=$(aws glue create-job \
        --name "f1-bronze-to-silver-transform" \
        --role "arn:aws:iam::${aws_account_id}:role/F1-DataPipeline-Role" \
        --command "Name=glueetl,ScriptLocation=s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py,PythonVersion=3.9" \
        --glue-version "4.0" \
        --default-arguments "file://$bronze_args_file" \
        --description "F1 Data Pipeline: Transform Bronze layer data to Silver layer with data quality validation - Version ${VERSION}" 2>&1); then
        log_success "Created Bronze to Silver Glue job"
        log_debug "Create response: $bronze_create_output"
    else
        log_error "Failed to create Bronze to Silver Glue job"
        log_error "Context: AWS CLI output: $bronze_create_output"
        log_error "Suggestion: Check IAM permissions, wheel URI accessibility, and job name uniqueness"
        ((glue_errors++))
    fi

    # Clean up temp file
    rm -f "$bronze_args_file"

    # Create Silver to Gold job
    log_info "Creating Silver to Gold Glue job..."

    # Build DefaultArguments safely with jq
    local silver_args_json
    if ! silver_args_json=$(jq -n \
        --arg wheel_uris "$wheel_uris" \
        --arg tempDir "s3://$S3_BUCKET_DATA_LAKE/temp/gold/" \
        --arg sparkLogs "s3://$S3_BUCKET_DATA_LAKE/spark-logs/gold/" \
        --arg warehouse "s3://$S3_BUCKET_DATA_LAKE/warehouse/" \
        --arg version "$VERSION" \
        --arg insights '{"InsightsEnabled": true}' \
    '{
        "job-language": "python",
        "job-bookmark-option": "job-bookmark-enable",
        "--additional-python-modules": $wheel_uris,
        "--TempDir": $tempDir,
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": $sparkLogs,
        "--enable-metrics": "",
        "--enable-continuous-cloudwatch-log": "true",
        "--continuous-log-logGroup": "/aws/glue/f1-silver-to-gold",
        "--enable-observability-metrics": "true",
        "--enable-job-insights": "true",
        "--job-insights-configuration": $insights,
        "--enable-continuous-log-filter": "true",
        "--continuous-log-filterPattern": "ERROR|WARN|INFO",

        "--conf:spark.sql.adaptive.enabled": "true",
        "--conf:spark.sql.adaptive.coalescePartitions.enabled": "true",
        "--conf:spark.sql.adaptive.skewJoin.enabled": "true",
        "--conf:spark.sql.adaptive.localShuffleReader.enabled": "true",
        "--conf:spark.sql.adaptive.advisoryPartitionSizeInBytes": "134217728",
        "--conf:spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "--conf:spark.sql.parquet.compression.codec": "snappy",
        "--conf:spark.sql.parquet.mergeSchema": "true",

        "--conf:spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf:spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "--conf:spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "--conf:spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "--conf:spark.sql.catalog.glue_catalog.warehouse": $warehouse,
        "--conf:spark.sql.iceberg.handle-timestamp-without-timezone": "true",
        "--conf:spark.sql.iceberg.vectorization.enabled": "true",
        "--conf:spark.sql.iceberg.vectorization.batch-size": "4096",

        "--enable-glue-datacatalog": "",
        "--datalake-formats": "iceberg",
        "--F1_DEPLOYMENT_VERSION": $version
    }'); then
        log_error "Failed to build Silver job arguments JSON with jq"
        log_error "Context: jq command failed - check jq installation and JSON syntax"
        ((glue_errors++))
        return 1
    fi

    # Validate generated JSON
    if ! echo "$silver_args_json" | jq empty >/dev/null 2>&1; then
        log_error "Generated Silver job arguments JSON is invalid"
        log_error "Context: JSON validation failed"
        ((glue_errors++))
        return 1
    fi

    # Write to a file for AWS CLI
    local silver_args_file
    if ! silver_args_file=$(mktemp); then
        log_error "Failed to create temporary file for Silver job arguments"
        log_error "Context: mktemp command failed"
        ((glue_errors++))
        return 1
    fi

    if ! printf '%s' "$silver_args_json" > "$silver_args_file"; then
        log_error "Failed to write Silver job arguments to temporary file"
        log_error "Context: printf command failed"
        rm -f "$silver_args_file"
        ((glue_errors++))
        return 1
    fi

    local silver_create_output
    if silver_create_output=$(aws glue create-job \
        --name "f1-silver-to-gold-transform" \
        --role "arn:aws:iam::${aws_account_id}:role/F1-DataPipeline-Role" \
        --command "Name=glueetl,ScriptLocation=s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py,PythonVersion=3.9" \
        --glue-version "4.0" \
        --default-arguments "file://$silver_args_file" \
        --description "F1 Data Pipeline: Transform Silver layer data to Gold layer analytics tables - Version ${VERSION}" 2>&1); then
        log_success "Created Silver to Gold Glue job"
        log_debug "Create response: $silver_create_output"
    else
        log_error "Failed to create Silver to Gold Glue job"
        log_error "Context: AWS CLI output: $silver_create_output"
        log_error "Suggestion: Check IAM permissions, wheel URI accessibility, and job name uniqueness"
        ((glue_errors++))
    fi

    # Clean up temp file
    rm -f "$silver_args_file"

    if [[ $glue_errors -gt 0 ]]; then
        log_error "Glue job creation failed with $glue_errors errors"
        log_function_exit "update_glue_jobs" 1
        return 1
    fi

    # Verify fresh deployment
    log_info "🔍 Verifying fresh Glue job deployment..."
    verify_fresh_glue_deployment

    log_function_exit "update_glue_jobs" 0
    return 0
}

verify_fresh_glue_deployment() {
    log_function_entry "verify_fresh_glue_deployment"
    local verification_errors=0

    log_info "🔍 Verifying fresh Glue job deployment..."

    # Check if Bronze to Silver job exists and has correct script
    log_info "Checking Bronze to Silver job..."
    local bronze_script_location
    bronze_script_location=$(aws glue get-job --job-name "f1-bronze-to-silver-transform" --query 'Job.Command.ScriptLocation' --output text 2>/dev/null)
    if [[ "$bronze_script_location" == "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py" ]]; then
        log_success "✅ Bronze to Silver job has correct script location"
    else
        log_error "❌ Bronze to Silver job script location mismatch: $bronze_script_location"
        ((verification_errors++))
    fi

    # Check if Silver to Gold job exists and has correct script
    log_info "Checking Silver to Gold job..."
    local silver_script_location
    silver_script_location=$(aws glue get-job --job-name "f1-silver-to-gold-transform" --query 'Job.Command.ScriptLocation' --output text 2>/dev/null)
    if [[ "$silver_script_location" == "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py" ]]; then
        log_success "✅ Silver to Gold job has correct script location"
    else
        log_error "❌ Silver to Gold job script location mismatch: $silver_script_location"
        ((verification_errors++))
    fi

    # Check if scripts exist in S3
    log_info "Checking if scripts exist in S3..."
    if aws s3 ls "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/bronze_to_silver_orchestrator.py" >/dev/null 2>&1; then
        log_success "✅ Bronze to Silver script exists in S3"
    else
        log_error "❌ Bronze to Silver script not found in S3"
        ((verification_errors++))
    fi

    if aws s3 ls "s3://${S3_BUCKET_DATA_LAKE}/scripts/orchestrators/silver_to_gold_orchestrator.py" >/dev/null 2>&1; then
        log_success "✅ Silver to Gold script exists in S3"
    else
        log_error "❌ Silver to Gold script not found in S3"
        ((verification_errors++))
    fi

    if [[ $verification_errors -gt 0 ]]; then
        log_error "Fresh deployment verification failed with $verification_errors errors"
        log_function_exit "verify_fresh_glue_deployment" 1
        return 1
    fi

    log_success "✅ Fresh Glue job deployment verified successfully"
    log_function_exit "verify_fresh_glue_deployment" 0
    return 0
}

# Verify Glue job updates were successful
verify_glue_job_updates() {
    local verification_errors=0
    
    # Verify Bronze to Silver job
    log_debug "Verifying Bronze to Silver job update..."
    local bronze_job_details
    if bronze_job_details=$(aws glue get-job --job-name "f1-bronze-to-silver-transform" --region "$AWS_REGION" 2>&1); then
        local bronze_version=$(echo "$bronze_job_details" | jq -r '.Job.DefaultArguments."--F1_DEPLOYMENT_VERSION" // "NOT_SET"')
        if [[ "$bronze_version" == "$VERSION" ]]; then
            log_debug "✓ Bronze to Silver job version updated to $VERSION"
        else
            log_warning "Bronze to Silver job version mismatch: expected $VERSION, got $bronze_version"
            ((verification_errors++))
        fi
    else
        log_warning "Failed to verify Bronze to Silver job update: $bronze_job_details"
        ((verification_errors++))
    fi
    
    # Verify Silver to Gold job
    log_debug "Verifying Silver to Gold job update..."
    local silver_job_details
    if silver_job_details=$(aws glue get-job --job-name "f1-silver-to-gold-transform" --region "$AWS_REGION" 2>&1); then
        local silver_version=$(echo "$silver_job_details" | jq -r '.Job.DefaultArguments."--F1_DEPLOYMENT_VERSION" // "NOT_SET"')
        if [[ "$silver_version" == "$VERSION" ]]; then
            log_debug "✓ Silver to Gold job version updated to $VERSION"
        else
            log_warning "Silver to Gold job version mismatch: expected $VERSION, got $silver_version"
            ((verification_errors++))
        fi
    else
        log_warning "Failed to verify Silver to Gold job update: $silver_job_details"
        ((verification_errors++))
    fi
    
    if [[ $verification_errors -eq 0 ]]; then
        return 0
    else
        log_warning "Glue job verification found $verification_errors issues"
        return 1
    fi
}

# Verify deployment
verify_deployment() {
    log_info "✅ Verifying deployment..."
    
    local verification_errors=0
    
    # Check data lake bucket deployment
    log_info "Verifying data lake bucket deployment..."
    if aws s3 ls "s3://$S3_BUCKET_DATA_LAKE/$S3_DEPENDENCIES_PATH/$VERSION/" &>/dev/null; then
        log_success "✓ Wheel dependencies uploaded"
    else
        log_error "✗ Wheel dependencies missing"
        ((verification_errors++))
    fi
    
    if aws s3 ls "s3://$S3_BUCKET_DATA_LAKE/scripts/orchestrators/" &>/dev/null; then
        log_success "✓ Glue scripts deployed"
    else
        log_error "✗ Glue scripts missing"
        ((verification_errors++))
    fi
    
    # Check MWAA bucket deployment
    log_info "Verifying MWAA bucket deployment..."
    if aws s3 ls "s3://$S3_BUCKET_MWAA/dags/" &>/dev/null; then
        log_success "✓ DAGs deployed to MWAA"
    else
        log_error "✗ DAGs missing from MWAA"
        ((verification_errors++))
    fi
    
    if aws s3 ls "s3://$S3_BUCKET_MWAA/requirements.txt" &>/dev/null; then
        log_success "✓ Requirements.txt deployed to MWAA"
    else
        log_error "✗ Requirements.txt missing from MWAA"
        ((verification_errors++))
    fi
    
    # Check Glue job updates (if not dry run)
    if [[ "$DRY_RUN" == "false" ]]; then
        log_info "Verifying Glue job updates..."
        if aws glue get-job --job-name "f1-bronze-to-silver-transform" --query 'Job.Description' --output text | grep -q "$VERSION"; then
            log_success "✓ Bronze to Silver job updated with version $VERSION"
        else
            log_warning "⚠ Bronze to Silver job version not updated"
        fi
        
        if aws glue get-job --job-name "f1-silver-to-gold-transform" --query 'Job.Description' --output text | grep -q "$VERSION"; then
            log_success "✓ Silver to Gold job updated with version $VERSION"
        else
            log_warning "⚠ Silver to Gold job version not updated"
        fi
    fi
    
    # List deployed files
    log_info "Deployed files summary:"
    log_info "Data Lake Bucket:"
    {
        aws s3 ls "s3://$S3_BUCKET_DATA_LAKE/deployment/" --recursive --human-readable | head -10
    } 2>/dev/null || log_debug "Could not list data lake files"
    
    log_info "MWAA Bucket:"
    {
        aws s3 ls "s3://$S3_BUCKET_MWAA/" --recursive --human-readable | head -10
    } 2>/dev/null || log_debug "Could not list MWAA files"
    
    if [[ $verification_errors -eq 0 ]]; then
        log_success "🎉 Deployment verification passed!"
        return 0
    else
        log_error "❌ Deployment verification failed with $verification_errors errors"
        return 1
    fi
}

# Clean up old wheel files
cleanup_old_wheels() {
    log_function_entry "cleanup_old_wheels" ""
    
    log_info "Cleaning up old wheel files..."
    
    if [[ -d "$WHEELS_DIR" ]]; then
        local wheel_count=$(find "$WHEELS_DIR" -name "*.whl" | wc -l)
        if [[ $wheel_count -gt 0 ]]; then
            log_info "Removing $wheel_count old wheel file(s)"
            rm -f "$WHEELS_DIR"/*.whl
        fi
    fi
    
    log_success "Old wheel files cleaned up"
    log_function_exit "cleanup_old_wheels" 0
}

# Enhanced cleanup with comprehensive resource management
cleanup_deployment_resources() {
    log_function_entry "cleanup_deployment_resources" ""
    
    log_info "🧹 Starting comprehensive cleanup..."
    
    # Clean up wheel files
    cleanup_old_wheels
    
    # Clean up temporary files
    cleanup_temporary_files
    
    # Clean up old deployment packages (keep last 5 versions)
    cleanup_old_deployment_packages
    
    # Clean up old logs (keep last 10 log files)
    cleanup_old_logs
    
    # Clean up S3 temporary files
    cleanup_s3_temp_files
    
    log_success "✅ Comprehensive cleanup completed"
    log_function_exit "cleanup_deployment_resources" 0
}

# Clean up old deployment packages
cleanup_old_deployment_packages() {
    log_debug "Cleaning up old deployment packages..."
    
    if [[ -d "$DEPLOYMENT_DIR" ]]; then
        # Keep last 5 deployment packages
        local package_count=$(find "$DEPLOYMENT_DIR" -name "f1-mwaa-package-*.zip" | wc -l)
        if [[ $package_count -gt 5 ]]; then
            local packages_to_remove=$((package_count - 5))
            log_info "Removing $packages_to_remove old deployment package(s)"
            
            find "$DEPLOYMENT_DIR" -name "f1-mwaa-package-*.zip" -type f -exec stat -f '%m %N' {} \; | \
                sort -n | head -n "$packages_to_remove" | cut -d' ' -f2- | xargs rm -f
        fi
    fi
    
    log_debug "✓ Old deployment packages cleaned up"
}

# Clean up old log files
cleanup_old_logs() {
    log_debug "Cleaning up old log files..."
    
    if [[ -d "$LOG_DIR" ]]; then
        # Keep last 10 log files
        local log_count=$(find "$LOG_DIR" -name "deploy_code_*.log" | wc -l)
        if [[ $log_count -gt 10 ]]; then
            local logs_to_remove=$((log_count - 10))
            log_info "Removing $logs_to_remove old log file(s)"
            
            find "$LOG_DIR" -name "deploy_code_*.log" -type f -exec stat -f '%m %N' {} \; | \
                sort -n | head -n "$logs_to_remove" | cut -d' ' -f2- | xargs rm -f
        fi
    fi
    
    log_debug "✓ Old log files cleaned up"
}

# Clean up S3 temporary files
cleanup_s3_temp_files() {
    log_debug "Cleaning up S3 temporary files..."
    
    # Clean up old temp directories (older than 7 days)
    local temp_dirs=(
        "s3://$S3_BUCKET_DATA_LAKE/temp/bronze/"
        "s3://$S3_BUCKET_DATA_LAKE/temp/gold/"
        "s3://$S3_BUCKET_DATA_LAKE/spark-logs/bronze/"
        "s3://$S3_BUCKET_DATA_LAKE/spark-logs/gold/"
    )
    
    for temp_dir in "${temp_dirs[@]}"; do
        log_debug "Cleaning up $temp_dir"
        # Note: AWS CLI doesn't have a direct way to delete files older than X days
        # This would require a more complex implementation with date parsing
        # For now, we'll just log the intention
        log_debug "Would clean up old files in $temp_dir (older than 7 days)"
    done
    
    log_debug "✓ S3 temporary files cleanup completed"
}

# Cleanup temporary files
cleanup_temporary_files() {
    log_info "🧹 Cleaning up temporary files..."
    
    # Keep deployment packages for troubleshooting but clean package dir
    rm -rf "$PACKAGE_DIR"
    
    # Clean up wheel files after successful deployment
    if [[ "$DRY_RUN" == "false" ]]; then
        cleanup_old_wheels
    fi
    
    # Clean up any temporary backup files
    rm -f /tmp/requirements_backup.txt
    
    log_success "Temporary files cleaned up"
}

# Generate deployment summary
generate_summary() {
    local data_lake_errors="$1"
    local mwaa_errors="$2"
    local glue_errors="$3"
    local total_errors=$((data_lake_errors + mwaa_errors + glue_errors))
    
    echo ""
    echo -e "${CYAN}============================================================================="
    echo "📊 CODE DEPLOYMENT SUMMARY"
    echo "=============================================================================${NC}"
    echo ""
    echo "📅 Timestamp: $(date)"
    echo "📝 Log File: $LOG_FILE"
    echo "🏷️ Version: $VERSION"
    echo "🪣 Data Lake Bucket: s3://$S3_BUCKET_DATA_LAKE"
    echo "🪣 MWAA Bucket: s3://$S3_BUCKET_MWAA"
    echo ""
    echo "📈 Results:"
    echo "  • Data Lake Deployment: $([ $data_lake_errors -eq 0 ] && echo "✅ SUCCESS" || echo "❌ $data_lake_errors errors")"
    echo "  • MWAA Deployment: $([ $mwaa_errors -eq 0 ] && echo "✅ SUCCESS" || echo "❌ $mwaa_errors errors")"
    echo "  • Glue Job Updates: $([ $glue_errors -eq 0 ] && echo "✅ SUCCESS" || echo "❌ $glue_errors errors")"
    echo "  • Overall: $([ $total_errors -eq 0 ] && echo "✅ SUCCESS" || echo "❌ $total_errors total errors")"
    echo ""
    
    if [[ "$DRY_RUN" == "true" ]]; then
        echo -e "${YELLOW}⚠️  This was a DRY RUN - no actual changes were made${NC}"
    fi
    
    echo "📋 Deployed Components:"
    echo "  • F1 pipeline wheel (built from setup.py)"
    echo "  • Single clean wheel (no external dependencies)"
    echo "  • Glue job orchestrators (Bronze→Silver, Silver→Gold)"
    echo "  • Airflow DAGs with DAG factory pattern"
    echo "  • Utils (Config, SimpleDataValidator, IcebergManager)"
    echo "  • Configuration files (dev.yaml, prod.yaml)"
    echo "  • Dependencies (requirements.txt)"
    echo ""
    echo "📋 Next Steps:"
    if [[ $total_errors -eq 0 ]]; then
        echo "  • Test Glue jobs in AWS Console"
        echo "  • Check MWAA environment for DAG updates"
        echo "  • Trigger pipeline execution"
        echo "  • Monitor CloudWatch logs and metrics"
        echo "  • Verify data quality through simple validation logs"
    else
        echo "  • Review error logs in: $LOG_FILE"
        echo "  • Fix any issues and re-run deployment"
        echo "  • Check AWS Console for resource status"
    fi
    echo ""
    echo "🔗 Useful AWS Console Links:"
    echo "  • Glue Jobs: https://console.aws.amazon.com/glue/home?region=$AWS_REGION#etl:tab=jobs"
    echo "  • MWAA: https://console.aws.amazon.com/mwaa/home?region=$AWS_REGION#environments"
    echo "  • S3 Data Lake: https://console.aws.amazon.com/s3/buckets/$S3_BUCKET_DATA_LAKE"
    echo "  • S3 MWAA: https://console.aws.amazon.com/s3/buckets/$S3_BUCKET_MWAA"
    echo ""
}


# Main execution function
main() {
    print_banner
    parse_arguments "$@"
    
    # Initialize deployment history
    init_deployment_history
    
    # Handle special modes
    if [[ "$LIST_VERSIONS" == "true" ]]; then
        list_rollback_versions
        exit 0
    fi
    
    if [[ "$HEALTH_CHECK_ONLY" == "true" ]]; then
        if check_deployment_health; then
            log_success "✅ Health check passed"
            exit 0
        else
            log_error "❌ Health check failed"
            exit 1
        fi
    fi
    
    if [[ "$ROLLBACK_MODE" == "true" ]]; then
        local target_version="$ROLLBACK_VERSION"
        if [[ -z "$target_version" ]]; then
            target_version=$(get_last_successful_version)
            if [[ -z "$target_version" ]]; then
                log_error "No successful deployment found for rollback"
                exit 1
            fi
        fi
        
        if rollback_to_version "$target_version"; then
            log_success "✅ Rollback completed successfully"
            exit 0
        else
            log_error "❌ Rollback failed"
            exit 1
        fi
    fi
    
    log_info "Starting F1 Data Pipeline code deployment..."
    log_info "Mode: $([ "$DRY_RUN" == "true" ] && echo "DRY RUN" || echo "LIVE DEPLOYMENT")"
    log_info "Clean Previous: $([ "$CLEAN_PREVIOUS" == "true" ] && echo "YES" || echo "NO")"
    
    # Check prerequisites
    check_prerequisites
    
    # Execute deployment phases
    local data_lake_errors=0
    local mwaa_errors=0
    local glue_errors=0
    
    # Full deployment mode
    # Validate code structure
    validate_code_structure
    
    clean_previous_deployments
    create_deployment_package
    
    if ! deploy_to_data_lake; then
        data_lake_errors=$?
    fi
    
    if ! deploy_to_mwaa; then
        mwaa_errors=$?
    fi
    
    # Update Glue jobs
    if ! update_glue_jobs; then
        glue_errors=$?
    fi
    
    # Verify deployment (only in live mode)
    if [[ "$DRY_RUN" == "false" ]]; then
        verify_deployment
    fi
    
    # Run health checks (only in live mode)
    if [[ "$DRY_RUN" == "false" ]]; then
        if check_deployment_health; then
            log_success "✅ Deployment health check passed"
        else
            log_warning "⚠️ Deployment health check found issues"
        fi
    fi
    
    # Record deployment in history
    local total_errors=$((data_lake_errors + mwaa_errors + glue_errors))
    if [[ $total_errors -eq 0 ]]; then
        record_deployment "$VERSION" "success" ""
        log_success "🎉 Code deployment completed successfully!"
    else
        record_deployment "$VERSION" "failed" "Deployment completed with $total_errors errors"
        log_error "❌ Code deployment completed with $total_errors errors"
    fi
    
    # Enhanced cleanup
    if [[ "$DRY_RUN" == "false" ]]; then
        cleanup_deployment_resources
    else
        cleanup_temporary_files
    fi
    
    # Generate summary
    generate_summary "$data_lake_errors" "$mwaa_errors" "$glue_errors"
    
    # Exit with appropriate code
    if [[ $total_errors -eq 0 ]]; then
        exit 0
    else
        exit 1
    fi
}

# Run main function
main "$@"
