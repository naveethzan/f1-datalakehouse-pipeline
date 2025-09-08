#!/bin/bash

# =============================================================================
# CloudWatch Resources Cleanup Script
# =============================================================================
# 
# This script removes unnecessary CloudWatch resources that were created
# for custom metrics and logging that we no longer use after simplifying
# the F1 data pipeline.
#
# What it removes:
# - Custom log groups (f1-glue-logs, f1-mwaa-logs, f1-pipeline-logs)
# - Custom CloudWatch dashboards (if any)
# - Custom metrics (if any)
#
# What it keeps:
# - Glue default log groups (/aws/glue/*) - Required for Glue job execution
# - MWAA default log groups (/aws/mwaa/*) - Required for MWAA execution
#
# =============================================================================

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
AWS_REGION="${AWS_REGION:-us-east-1}"
DRY_RUN="${DRY_RUN:-false}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if AWS CLI is available
check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed or not in PATH"
        exit 1
    fi
    
    # Check AWS credentials
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS credentials not configured or expired"
        exit 1
    fi
    
    log_success "AWS CLI is available and configured"
}

# Remove custom log groups
cleanup_custom_log_groups() {
    log_info "🧹 Cleaning up custom CloudWatch log groups..."
    
    local custom_log_groups=(
        "f1-glue-logs"
        "f1-mwaa-logs" 
        "f1-pipeline-logs"
    )
    
    for log_group in "${custom_log_groups[@]}"; do
        log_info "Checking log group: $log_group"
        
        if aws logs describe-log-groups --log-group-name-prefix "$log_group" --region "$AWS_REGION" &>/dev/null; then
            if [[ "$DRY_RUN" == "true" ]]; then
                log_info "DRY RUN: Would delete log group: $log_group"
            else
                log_info "Deleting log group: $log_group"
                if aws logs delete-log-group --log-group-name "$log_group" --region "$AWS_REGION" 2>/dev/null; then
                    log_success "✅ Deleted log group: $log_group"
                else
                    log_warning "⚠️ Failed to delete log group: $log_group (may not exist)"
                fi
            fi
        else
            log_info "✅ Log group does not exist: $log_group"
        fi
    done
}

# Remove custom CloudWatch dashboards
cleanup_custom_dashboards() {
    log_info "🧹 Cleaning up custom CloudWatch dashboards..."
    
    local custom_dashboards=(
        "F1-Data-Pipeline-Dashboard"
        "F1-Data-Pipeline"
        "F1-Pipeline-Dashboard"
        "F1-Error-Dashboard"
        "F1-Cost-Dashboard"
    )
    
    for dashboard in "${custom_dashboards[@]}"; do
        log_info "Checking dashboard: $dashboard"
        
        if aws cloudwatch get-dashboard --dashboard-name "$dashboard" --region "$AWS_REGION" &>/dev/null; then
            if [[ "$DRY_RUN" == "true" ]]; then
                log_info "DRY RUN: Would delete dashboard: $dashboard"
            else
                log_info "Deleting dashboard: $dashboard"
                if aws cloudwatch delete-dashboards --dashboard-names "$dashboard" --region "$AWS_REGION" 2>/dev/null; then
                    log_success "✅ Deleted dashboard: $dashboard"
                else
                    log_warning "⚠️ Failed to delete dashboard: $dashboard"
                fi
            fi
        else
            log_info "✅ Dashboard does not exist: $dashboard"
        fi
    done
}

# List remaining CloudWatch resources
list_remaining_resources() {
    log_info "📋 Listing remaining CloudWatch resources..."
    
    echo -e "\n${BLUE}=== REMAINING LOG GROUPS ===${NC}"
    aws logs describe-log-groups --region "$AWS_REGION" --query 'logGroups[?contains(logGroupName, `f1`) || contains(logGroupName, `/aws/glue`) || contains(logGroupName, `/aws/mwaa`)].logGroupName' --output table 2>/dev/null || echo "No relevant log groups found"
    
    echo -e "\n${BLUE}=== REMAINING DASHBOARDS ===${NC}"
    aws cloudwatch list-dashboards --region "$AWS_REGION" --query 'DashboardEntries[?contains(DashboardName, `F1`) || contains(DashboardName, `f1`)].DashboardName' --output table 2>/dev/null || echo "No F1-related dashboards found"
    
    echo -e "\n${BLUE}=== CUSTOM METRICS (Last 7 days) ===${NC}"
    aws cloudwatch list-metrics --namespace "F1Pipeline" --region "$AWS_REGION" --query 'Metrics[].MetricName' --output table 2>/dev/null || echo "No F1Pipeline metrics found"
}

# Main cleanup function
main() {
    echo "============================================================================="
    echo "🧹 F1 Data Pipeline - CloudWatch Resources Cleanup"
    echo "============================================================================="
    echo ""
    echo "📅 Timestamp: $(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    echo "🌍 AWS Region: $AWS_REGION"
    echo "🔍 Mode: $([ "$DRY_RUN" == "true" ] && echo "DRY RUN" || echo "LIVE CLEANUP")"
    echo ""
    
    # Check prerequisites
    check_aws_cli
    
    # Perform cleanup
    cleanup_custom_log_groups
    cleanup_custom_dashboards
    
    # List remaining resources
    list_remaining_resources
    
    echo ""
    echo "============================================================================="
    echo "✅ CloudWatch cleanup completed!"
    echo "============================================================================="
    echo ""
    echo "📋 Summary:"
    echo "  • Removed custom log groups (f1-*)"
    echo "  • Removed custom dashboards (if any)"
    echo "  • Kept Glue default log groups (/aws/glue/*)"
    echo "  • Kept MWAA default log groups (/aws/mwaa/*)"
    echo ""
    echo "📋 Next Steps:"
    echo "  • Monitor Glue job execution logs in /aws/glue/*"
    echo "  • Check MWAA logs in /aws/mwaa/*"
    echo "  • Verify no custom metrics are being sent"
    echo ""
}

# Help function
show_help() {
    cat << EOF
F1 Data Pipeline - CloudWatch Resources Cleanup

USAGE:
    $0 [OPTIONS]

OPTIONS:
    --dry-run          Show what would be deleted without actually deleting
    --region REGION    AWS region (default: us-east-1)
    --help             Show this help message

EXAMPLES:
    # Dry run to see what would be deleted
    $0 --dry-run
    
    # Clean up resources in us-west-2
    $0 --region us-west-2
    
    # Live cleanup (default)
    $0

WHAT IT REMOVES:
    • Custom log groups: f1-glue-logs, f1-mwaa-logs, f1-pipeline-logs
    • Custom dashboards: F1-Pipeline-Dashboard, F1-Error-Dashboard, F1-Cost-Dashboard
    • Custom metrics in F1Pipeline namespace

WHAT IT KEEPS:
    • Glue default log groups: /aws/glue/*
    • MWAA default log groups: /aws/mwaa/*
    • Essential AWS service logs

EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN="true"
            shift
            ;;
        --region)
            AWS_REGION="$2"
            shift 2
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

# Run main function
main
