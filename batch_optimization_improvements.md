# Batch Optimization Solutions for 100 Account Limit

## Problem
When CSM account limit is 100 (or 95), PuLP batch optimization fails with "Infeasible" because:
- Some CSMs already have 99-101 accounts
- Hard constraint: `current_count + new_assignments <= max_accounts`
- Not enough total capacity across CSMs for batch of 10 accounts

## Solution 1: Pre-filter CSMs (IMPLEMENTED)
```python
# Added in optimize_with_pulp() at line 1376-1395
min_capacity_needed = 2 if batch_size > 5 else 1
eligible_with_capacity = []
for csm in eligible_csms:
    available_capacity = max_accounts - current_count
    if available_capacity >= min_capacity_needed:
        eligible_with_capacity.append(csm)
```

## Solution 2: Soft Constraints with Slack Variables
Replace hard constraint at line 1424:
```python
# OLD (hard constraint):
prob += current_count + new_assignments <= max_accounts

# NEW (soft constraint with penalty):
slack[csm] = pulp.LpVariable(f"slack_{csm}", lowBound=0, cat='Continuous')
prob += current_count + new_assignments <= max_accounts + slack[csm]
# Add heavy penalty for using slack in objective function
slack_penalty = pulp.lpSum([slack[csm] * 1000000 for csm in eligible_csms])
```

## Solution 3: Dynamic Batch Sizing
Adjust batch size based on total available capacity:
```python
def determine_optimal_batch_size(csm_books, max_accounts, num_accounts):
    """Determine optimal batch size based on available capacity"""
    total_capacity = sum(max(0, max_accounts - book['count'])
                        for book in csm_books.values())

    if total_capacity < num_accounts * 0.5:
        return min(3, num_accounts)  # Very tight capacity - small batches
    elif total_capacity < num_accounts:
        return min(5, num_accounts)  # Moderate capacity
    else:
        return min(10, num_accounts)  # Plenty of capacity
```

## Solution 4: Two-Phase Assignment
1. First pass: Assign to CSMs with most capacity (>10 spots)
2. Second pass: Optimize remaining with tighter constraints

## Recommended Approach
Use Solution 1 (pre-filtering) as primary fix + Solution 3 (dynamic batch sizing) for resilience.

This ensures:
- Batch optimization only considers CSMs with sufficient capacity
- Batch size adapts to available capacity
- Falls back to individual assignment when necessary