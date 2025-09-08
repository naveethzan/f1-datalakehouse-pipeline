# F1 Business Logic Utilities

This module provides F1-specific business logic functions for the Gold layer transformations, implementing the requirements from task 3 of the silver-to-gold-analytics spec.

## Task 3 Requirements Coverage

### ✅ F1 Points Calculation Function (Standard Points + Fastest Lap Bonus)

**Implementation**: `F1BusinessLogic.calculate_race_points()`
- Implements standard F1 points system: [25, 18, 15, 12, 10, 8, 6, 4, 2, 1]
- Adds fastest lap bonus (+1 point) for drivers finishing in top 10
- Handles edge cases (invalid positions, outside points)
- Provides Spark UDF wrapper for DataFrame operations

**Example Usage**:
```python
# Race winner with fastest lap
points = F1BusinessLogic.calculate_race_points(1, True)  # Returns 26

# 10th place with fastest lap  
points = F1BusinessLogic.calculate_race_points(10, True)  # Returns 2

# Outside points with fastest lap
points = F1BusinessLogic.calculate_race_points(11, True)  # Returns 0
```

### ✅ Championship Mathematics Functions (Gap Calculations, Mathematical Possibility)

**Implementation**: Multiple functions for championship analysis
- `calculate_championship_gap()`: Points gap to championship leader
- `calculate_max_remaining_points()`: Maximum points available from remaining races
- `is_championship_mathematically_possible()`: Determines if championship is still winnable

**Addresses Requirements**:
- **2.1**: Supports cumulative points calculations for championship tracking
- **2.2**: Enables championship position ranking by points total
- **2.3**: Provides mathematical foundation for championship viability assessment

**Example Usage**:
```python
# Check if championship is mathematically possible
possible = F1BusinessLogic.is_championship_mathematically_possible(
    current_points=80.0,
    leader_points=100.0, 
    remaining_races=2
)  # Returns True (80 + 2*26 = 132 > 100)

# Calculate points gap to leader
gap = F1BusinessLogic.calculate_championship_gap(85.0, 100.0)  # Returns 15.0
```

### ✅ Window Function Helper Utilities (Running Totals and Rankings)

**Implementation**: `F1WindowFunctions` class with pre-configured window specifications
- `get_running_total_window_spec()`: For cumulative points calculations (Requirement 2.1)
- `get_championship_window_spec()`: For championship position ranking (Requirement 2.2)  
- `get_recent_form_window_spec()`: For last 3 races calculations (Requirement 2.3)
- `get_teammate_comparison_window_spec()`: For teammate performance comparisons

**Convenience Functions**:
- `add_championship_standings()`: Adds position and gap columns
- `add_season_progression()`: Adds running totals and recent form
- `add_running_points_total()`: Implements requirement 2.1
- `add_recent_form_points()`: Implements requirement 2.3

**Example Usage**:
```python
# Add running points total (Requirement 2.1)
df_with_totals = F1WindowFunctions.add_running_points_total(
    df, 
    points_col="race_points",
    partition_col="driver_number", 
    order_col="race_date"
)

# Add championship position (Requirement 2.2)
df_with_position = F1WindowFunctions.add_championship_position(
    df,
    points_col="season_points_total"
)

# Add recent form (Requirement 2.3)
df_with_form = F1WindowFunctions.add_recent_form_points(
    df,
    points_col="race_points",
    partition_col="driver_number",
    order_col="race_date",
    num_races=3
)
```

## Additional Utilities

### Performance Metrics
- `calculate_positions_gained()`: Grid position vs finish position analysis
- `calculate_lap_consistency_score()`: Lap time consistency with outlier removal
- `determine_race_winner_from_results()`: Race summary statistics

### Window Function Patterns
- Championship standings with proper tie-breaking
- Running totals with ROWS UNBOUNDED PRECEDING
- Recent form calculations with sliding windows
- Teammate comparisons within teams

## Integration with Gold Layer

These utilities are designed to be used in the Gold layer transformations:

1. **driver_performance_summary**: Uses points calculation and consistency scoring
2. **championship_tracker**: Uses cumulative points, rankings, and mathematical possibility
3. **team_strategy_analysis**: Uses teammate comparisons and team aggregations  
4. **race_weekend_insights**: Uses race winner determination and performance metrics

## Testing

Comprehensive test suite in `test_f1_business_logic.py` covers:
- All points calculation scenarios (win, podium, outside points, fastest lap combinations)
- Championship mathematics edge cases (leader, behind, mathematically impossible)
- Performance metrics calculations (positions gained, consistency scores)
- Window function specifications and patterns

## Requirements Traceability

| Requirement | Implementation | Test Coverage |
|-------------|----------------|---------------|
| 2.1 - Cumulative points with window functions | `get_running_total_window_spec()`, `add_running_points_total()` | ✅ |
| 2.2 - Championship position ranking | `get_championship_window_spec()`, `add_championship_position()` | ✅ |
| 2.3 - Recent form (last 3 races) | `get_recent_form_window_spec()`, `add_recent_form_points()` | ✅ |

All task 3 requirements have been implemented and tested successfully.