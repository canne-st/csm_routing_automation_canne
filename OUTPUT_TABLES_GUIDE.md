# CSM Routing Output Tables Guide

## Overview
This guide explains where to find the outputs from the CSM routing automation system after running single account or batch assignments.

## Output Tables

### 1. **CSM_ROUTING_RECOMMENDATIONS_CANNE** (Primary Output)
**Location**: `DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE`

This table stores ALL recommendations made by the system, whether they were ultimately assigned or not.

**Table Structure**:
```sql
- recommendation_id (AUTO INCREMENT) - Unique ID for each recommendation
- account_id - The account needing CSM assignment
- recommended_csm - The CSM recommended by the algorithm
- recommendation_timestamp - When the recommendation was made
- assignment_method - 'single_optimized' or 'batch_optimized'
- neediness_score - Account neediness metric
- health_score - Account health score
- revenue - Account revenue
- account_segment - 'Residential' or 'Commercial'
- account_level - 'Corporate' or 'Enterprise'
- optimization_score - The optimization score for this pairing
- llm_feedback - Feedback from Claude if LLM review was enabled
- was_assigned - Boolean: TRUE if recommendation was accepted
- actual_assigned_csm - The CSM actually assigned (may differ from recommendation)
- run_id - Unique identifier for the run
- batch_size - Number of accounts in the batch (1 for single)
```

**Query Examples**:

```sql
-- Get all recommendations for today
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE DATE(recommendation_timestamp) = CURRENT_DATE()
ORDER BY recommendation_timestamp DESC;

-- Get recommendations for a specific account
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = 'YOUR_ACCOUNT_ID'
ORDER BY recommendation_timestamp DESC;

-- Get all accepted assignments
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE was_assigned = TRUE
ORDER BY recommendation_timestamp DESC;

-- Get CSM workload distribution
SELECT
    recommended_csm,
    COUNT(*) as total_recommendations,
    SUM(CASE WHEN was_assigned = TRUE THEN 1 ELSE 0 END) as accepted_assignments,
    AVG(optimization_score) as avg_score
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE recommendation_timestamp >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY recommended_csm
ORDER BY accepted_assignments DESC;
```

### 2. **ACCOUNT_CSM_ASSIGNMENTS_CANNE** (Final Assignments)
**Location**: `DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE`

This table stores the FINAL accepted assignments. It's updated when recommendations are approved.

**Table Structure**:
```sql
- account_id (PRIMARY KEY) - The account ID
- csm_name - The assigned CSM name
- assignment_date - When the assignment was made
- assignment_method - How the assignment was determined
- llm_review_feedback - Any LLM review comments
- last_updated - Last update timestamp
```

**Query Examples**:

```sql
-- Get current assignment for an account
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE account_id = 'YOUR_ACCOUNT_ID';

-- Get all accounts assigned to a specific CSM
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE csm_name = 'John Smith'
ORDER BY assignment_date DESC;

-- Get recent assignments
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE assignment_date >= DATEADD(day, -7, CURRENT_DATE())
ORDER BY assignment_date DESC;
```

## Running a Test for Single Account Assignment

### Step 1: Check for Accounts Needing CSM
```sql
-- Find accounts that need CSM assignment
SELECT
    account_id_ob as account_id,
    tenant_id,
    success_transition_status_ob
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE success_transition_status_ob = 'Needs CSM'
    AND account_id_ob IS NOT NULL
LIMIT 10;
```

### Step 2: Check Eligible CSMs
```sql
-- Check CSMs in the active list
SELECT
    active_csm,
    COUNT(*) OVER() as total_eligible_csms
FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
ORDER BY active_csm;
```

### Step 3: Run the Assignment
Execute the Python script:
```bash
python csm_routing_automation.py --mode single --account-id YOUR_ACCOUNT_ID
```

Or for batch:
```bash
python csm_routing_automation.py --mode batch --batch-size 10
```

### Step 4: Check Results

**Immediate Output - Recommendations Table**:
```sql
-- Check the recommendation that was just made
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE run_id = (
    SELECT MAX(run_id)
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
)
ORDER BY optimization_score DESC;
```

**After Approval - Assignments Table**:
```sql
-- Check if recommendation was accepted and assigned
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE last_updated >= DATEADD(minute, -10, CURRENT_TIMESTAMP());
```

## Monitoring and Analytics

### CSM Performance Metrics
```sql
-- Analyze CSM assignment patterns
WITH csm_stats AS (
    SELECT
        recommended_csm,
        COUNT(*) as total_assigned,
        AVG(neediness_score) as avg_neediness,
        AVG(health_score) as avg_health,
        AVG(revenue) as avg_revenue,
        AVG(optimization_score) as avg_optimization_score
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    WHERE was_assigned = TRUE
        AND recommendation_timestamp >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY recommended_csm
)
SELECT
    cs.*,
    rac.active_csm IS NOT NULL as is_still_active
FROM csm_stats cs
LEFT JOIN DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms rac
    ON cs.recommended_csm = rac.active_csm
ORDER BY total_assigned DESC;
```

### Assignment Success Rate
```sql
-- Check recommendation acceptance rate
SELECT
    assignment_method,
    COUNT(*) as total_recommendations,
    SUM(CASE WHEN was_assigned = TRUE THEN 1 ELSE 0 END) as accepted,
    ROUND(100.0 * SUM(CASE WHEN was_assigned = TRUE THEN 1 ELSE 0 END) / COUNT(*), 2) as acceptance_rate
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE recommendation_timestamp >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY assignment_method;
```

### CSM Cooling Period Check
```sql
-- Check recent assignments to respect cooling period
SELECT
    recommended_csm,
    MAX(recommendation_timestamp) as last_assignment_time,
    DATEDIFF(hour, MAX(recommendation_timestamp), CURRENT_TIMESTAMP()) as hours_since_last_assignment
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE was_assigned = TRUE
GROUP BY recommended_csm
HAVING hours_since_last_assignment < 4  -- 4-hour cooling period
ORDER BY last_assignment_time DESC;
```

## Important Notes

1. **Recommendations vs Assignments**:
   - `CSM_ROUTING_RECOMMENDATIONS_CANNE` contains ALL recommendations (proposed assignments)
   - `ACCOUNT_CSM_ASSIGNMENTS_CANNE` contains only ACCEPTED assignments
   - The `was_assigned` flag in recommendations indicates if it was accepted

2. **Filter Applied**:
   - Only CSMs in `DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms` are eligible
   - CSMs not in this table will never appear in recommendations

3. **Cooling Period**:
   - Default 4-hour cooling period between assignments for each CSM
   - Check the recommendations table timestamp to verify cooling period is respected

4. **LLM Review**:
   - If enabled, Claude reviews assignments and provides feedback
   - Feedback is stored in `llm_feedback` column
   - Poor assignments may be flagged for reassignment

## Troubleshooting

### No Recommendations Generated
```sql
-- Check if there are eligible accounts
SELECT COUNT(*) as needs_csm_count
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE success_transition_status_ob = 'Needs CSM';

-- Check if there are eligible CSMs
SELECT COUNT(*) as eligible_csm_count
FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms;

-- Check recent errors in logs (if logging table exists)
-- Review Python script logs for detailed error messages
```

### Recommendations Not Being Assigned
```sql
-- Check recommendations with low scores
SELECT
    account_id,
    recommended_csm,
    optimization_score,
    llm_feedback
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE was_assigned = FALSE
    AND recommendation_timestamp >= DATEADD(day, -1, CURRENT_DATE())
ORDER BY optimization_score ASC
LIMIT 10;
```

This will help identify why certain recommendations might not be getting approved.