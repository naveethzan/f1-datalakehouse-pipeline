"""
Broadcast Utilities for Gold Layer Transformations

This module provides intelligent broadcast join decisions and utilities
specifically designed for Silver to Gold transformations. It implements
context-aware broadcast strategies based on processing modes and data sizes.

Key Features:
- Context-aware broadcast decisions (historical vs incremental)
- Table-specific broadcast rules based on F1 data patterns
- Integration with Silver readers for optimal performance
- Broadcast hint management and validation

Processing Mode Impact:
- Historical: Conservative broadcast (larger datasets)  
- Incremental: Aggressive broadcast (smaller GP-specific datasets)

Compatible with PySpark broadcast joins and AWS Glue optimization.
"""

import logging
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast

# Setup logging
logger = logging.getLogger(__name__)


# =============================================================================
# Broadcast Decision Configuration
# =============================================================================

# Always broadcast - small master/lookup tables
ALWAYS_BROADCAST_TABLES = {
    'drivers_silver',     # ~30 records (driver master data)
    'sessions_silver',    # ~58 records per year (session metadata)
    'pitstops_silver'     # ~75 records per year (pit stop data)
}

# Never broadcast - large operational tables  
NEVER_BROADCAST_TABLES = {
    'laps_silver'  # ~20,000 records per year (detailed lap data)
}

# Context-aware broadcast - depends on processing mode
CONTEXT_AWARE_TABLES = {
    'qualifying_results_silver',  # ~500/year (historical), ~20/GP (incremental)
    'race_results_silver'         # ~500/year (historical), ~20/GP (incremental)
}

# Broadcast thresholds
BROADCAST_THRESHOLDS = {
    'historical': 100,    # Conservative threshold for full year processing
    'incremental': 1000   # Aggressive threshold for single GP processing
}


# =============================================================================
# Core Broadcast Decision Functions
# =============================================================================

def should_broadcast_table(
    table_name: str, 
    processing_mode: str, 
    record_count: Optional[int] = None,
    force_broadcast: Optional[bool] = None
) -> bool:
    """
    Determine whether to broadcast a table based on intelligent rules.
    
    Decision Logic:
    1. Force override (if specified)
    2. Always broadcast tables (small master data)
    3. Never broadcast tables (large operational data)
    4. Context-aware tables (based on processing mode and size)
    5. Default: don't broadcast unknown tables (safe)
    
    Args:
        table_name: Name of the table to evaluate
        processing_mode: 'historical' or 'incremental'
        record_count: Optional actual record count for dynamic decisions
        force_broadcast: Optional override (True=force broadcast, False=force no broadcast)
        
    Returns:
        True if table should be broadcast, False otherwise
    """
    # Force override takes precedence
    if force_broadcast is not None:
        action = "Broadcasting" if force_broadcast else "Not broadcasting"
        logger.info(f"🔧 {action} {table_name} (forced override)")
        return force_broadcast
    
    # Always broadcast small tables
    if table_name in ALWAYS_BROADCAST_TABLES:
        logger.info(f"✅ Broadcasting {table_name} (always broadcast - small table)")
        return True
    
    # Never broadcast large tables
    if table_name in NEVER_BROADCAST_TABLES:
        logger.info(f"❌ Not broadcasting {table_name} (never broadcast - large table)")
        return False
    
    # Context-aware broadcast decisions
    if table_name in CONTEXT_AWARE_TABLES:
        return _evaluate_context_aware_broadcast(table_name, processing_mode, record_count)
    
    # Unknown tables - safe default is no broadcast
    logger.info(f"❓ Not broadcasting {table_name} (unknown table - safe default)")
    return False


def _evaluate_context_aware_broadcast(
    table_name: str, 
    processing_mode: str, 
    record_count: Optional[int] = None
) -> bool:
    """
    Evaluate broadcast decision for context-aware tables.
    
    Args:
        table_name: Table name
        processing_mode: Processing mode
        record_count: Optional record count
        
    Returns:
        Broadcast decision
    """
    threshold = BROADCAST_THRESHOLDS.get(processing_mode, BROADCAST_THRESHOLDS['historical'])
    
    # Use record count if available for precise decision
    if record_count is not None:
        if record_count <= threshold:
            logger.info(f"✅ Broadcasting {table_name} ({processing_mode}: {record_count} records <= {threshold})")
            return True
        else:
            logger.info(f"❌ Not broadcasting {table_name} ({processing_mode}: {record_count} records > {threshold})")
            return False
    
    # Fallback to mode-based decision
    if processing_mode == 'incremental':
        logger.info(f"✅ Broadcasting {table_name} (incremental mode - likely small partition)")
        return True
    else:
        logger.info(f"❌ Not broadcasting {table_name} (historical mode - likely large dataset)")
        return False


def apply_broadcast_hint(
    df: DataFrame, 
    table_name: str, 
    processing_mode: str,
    record_count: Optional[int] = None,
    force_broadcast: Optional[bool] = None
) -> DataFrame:
    """
    Apply broadcast hint to DataFrame based on decision logic.
    
    Args:
        df: DataFrame to potentially broadcast
        table_name: Table name for decision logic
        processing_mode: 'historical' or 'incremental'
        record_count: Optional record count (will count if not provided)
        force_broadcast: Optional force override
        
    Returns:
        DataFrame with broadcast hint applied if appropriate
    """
    # Count records if not provided (for precise decisions)
    if record_count is None and table_name in CONTEXT_AWARE_TABLES:
        record_count = df.count()
    
    should_broadcast = should_broadcast_table(
        table_name=table_name,
        processing_mode=processing_mode,
        record_count=record_count,
        force_broadcast=force_broadcast
    )
    
    if should_broadcast:
        return broadcast(df)
    return df


# =============================================================================
# Gold Transform Integration Functions  
# =============================================================================

def get_broadcast_config_for_transform(transform_name: str, processing_mode: str) -> Dict[str, bool]:
    """
    Get optimized broadcast configuration for a specific Gold transform.
    
    Args:
        transform_name: Name of the Gold transform
        processing_mode: 'historical' or 'incremental'
        
    Returns:
        Dict mapping table names to broadcast decisions
    """
    # Base configuration - applies to all transforms
    base_config = {
        'drivers_silver': True,    # Always broadcast
        'sessions_silver': True,   # Always broadcast
        'pitstops_silver': True,   # Always broadcast
        'laps_silver': False       # Never broadcast
    }
    
    # Context-aware tables depend on processing mode
    context_config = {}
    for table in CONTEXT_AWARE_TABLES:
        context_config[table] = should_broadcast_table(table, processing_mode)
    
    # Merge configurations
    full_config = {**base_config, **context_config}
    
    logger.info(f"📋 Broadcast config for {transform_name} ({processing_mode} mode):")
    for table, broadcast_decision in full_config.items():
        status = "✅ BROADCAST" if broadcast_decision else "❌ NO BROADCAST"
        logger.info(f"   {table}: {status}")
    
    return full_config


def validate_broadcast_decisions(broadcast_config: Dict[str, bool]) -> List[str]:
    """
    Validate broadcast configuration for potential issues.
    
    Args:
        broadcast_config: Broadcast decisions to validate
        
    Returns:
        List of validation warnings (empty if all good)
    """
    warnings = []
    
    # Check for risky broadcast decisions
    for table_name, should_broadcast in broadcast_config.items():
        if table_name in NEVER_BROADCAST_TABLES and should_broadcast:
            warnings.append(f"⚠️ Broadcasting {table_name} may cause memory issues (large table)")
        
        if table_name in ALWAYS_BROADCAST_TABLES and not should_broadcast:
            warnings.append(f"⚠️ Not broadcasting {table_name} misses optimization opportunity (small table)")
    
    return warnings


# =============================================================================
# Monitoring Functions
# =============================================================================

def log_broadcast_performance_stats(
    transform_name: str,
    broadcast_config: Dict[str, bool],
    execution_time: float,
    record_counts: Optional[Dict[str, int]] = None
) -> None:
    """
    Log broadcast performance statistics for monitoring.
    
    Args:
        transform_name: Name of the transform
        broadcast_config: Broadcast decisions used
        execution_time: Transform execution time in seconds
        record_counts: Optional record counts per table
    """
    broadcast_count = sum(broadcast_config.values())
    total_tables = len(broadcast_config)
    
    logger.info(f"📊 Broadcast Performance Stats - {transform_name}")
    logger.info(f"   ⏱️ Execution Time: {execution_time:.2f}s")
    logger.info(f"   📡 Broadcast Tables: {broadcast_count}/{total_tables}")
    
    if record_counts:
        total_broadcast_records = sum(
            count for table, count in record_counts.items()
            if broadcast_config.get(table, False)
        )
        logger.info(f"   📈 Total Broadcast Records: {total_broadcast_records:,}")
    
    # Log individual table stats
    for table_name, is_broadcast in broadcast_config.items():
        record_info = ""
        if record_counts and table_name in record_counts:
            record_info = f" ({record_counts[table_name]:,} records)"
        
        status = "📡 BROADCAST" if is_broadcast else "⚡ STANDARD"
        logger.info(f"   {status}: {table_name}{record_info}")


def estimate_broadcast_memory_usage(
    broadcast_config: Dict[str, bool],
    record_counts: Dict[str, int],
    avg_record_size_kb: float = 1.0
) -> Dict[str, float]:
    """
    Estimate memory usage for broadcast operations.
    
    Args:
        broadcast_config: Broadcast decisions
        record_counts: Record counts per table
        avg_record_size_kb: Average record size in KB
        
    Returns:
        Dict with memory estimates in MB
    """
    estimates = {}
    
    for table_name, is_broadcast in broadcast_config.items():
        if is_broadcast and table_name in record_counts:
            record_count = record_counts[table_name]
            estimated_mb = (record_count * avg_record_size_kb) / 1024
            estimates[table_name] = estimated_mb
        else:
            estimates[table_name] = 0.0
    
    total_mb = sum(estimates.values())
    estimates['total_mb'] = total_mb
    
    # Log memory usage estimates
    logger.info(f"💾 Estimated broadcast memory usage: {total_mb:.1f} MB")
    for table, mb in estimates.items():
        if mb > 0 and table != 'total_mb':
            logger.info(f"   {table}: {mb:.1f} MB")
    
    return estimates


# =============================================================================
# Utility Functions
# =============================================================================

def get_supported_tables() -> Dict[str, str]:
    """
    Get list of supported tables and their broadcast categories.
    
    Returns:
        Dict mapping table names to broadcast categories
    """
    supported = {}
    
    for table in ALWAYS_BROADCAST_TABLES:
        supported[table] = "always_broadcast"
        
    for table in NEVER_BROADCAST_TABLES:
        supported[table] = "never_broadcast"
        
    for table in CONTEXT_AWARE_TABLES:
        supported[table] = "context_aware"
    
    return supported


def create_broadcast_summary(processing_mode: str) -> str:
    """
    Create a human-readable summary of broadcast strategy.
    
    Args:
        processing_mode: Processing mode
        
    Returns:
        Formatted summary string
    """
    summary = f"🚀 Broadcast Strategy Summary ({processing_mode} mode)\n"
    summary += "=" * 60 + "\n\n"
    
    summary += "✅ ALWAYS BROADCAST (Small Tables):\n"
    for table in sorted(ALWAYS_BROADCAST_TABLES):
        summary += f"   • {table}\n"
    
    summary += "\n❌ NEVER BROADCAST (Large Tables):\n"
    for table in sorted(NEVER_BROADCAST_TABLES):
        summary += f"   • {table}\n"
    
    summary += f"\n🤔 CONTEXT-AWARE ({processing_mode} mode):\n"
    threshold = BROADCAST_THRESHOLDS.get(processing_mode, 100)
    for table in sorted(CONTEXT_AWARE_TABLES):
        decision = "BROADCAST" if processing_mode == 'incremental' else "NO BROADCAST"
        summary += f"   • {table}: {decision} (threshold: {threshold} records)\n"
    
    return summary