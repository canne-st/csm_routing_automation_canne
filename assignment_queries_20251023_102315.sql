-- Account Assignment SQL Queries
-- Generated: 2025-10-23 10:23:15.998666
-- Account ID: TEST_ACC_20251023
-- Recommended CSM: David Kim

-- STEP 1: Verify Account

-- Query to verify account needs CSM assignment
SELECT
    account_id_ob as account_id,
    tenant_id,
    success_transition_status_ob,
    segment,
    account_level
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE account_id_ob = 'TEST_ACC_20251023'
    AND success_transition_status_ob = 'Needs CSM';

-- STEP 2: Get Eligible CSMs

-- Query to get eligible CSMs from filtered list
WITH csm_current_load AS (
    SELECT
        preferred_csm_name as csm_name,
        COUNT(DISTINCT account_id) as current_accounts
    FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
    WHERE calendar_date = (SELECT MAX(calendar_date) FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY)
        AND preferred_csm_role = 'Success Rep'
    GROUP BY preferred_csm_name
)
SELECT
    r.active_csm,
    COALESCE(c.current_accounts, 0) as current_accounts,
    85 - COALESCE(c.current_accounts, 0) as available_capacity
FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms r
LEFT JOIN csm_current_load c ON r.active_csm = c.csm_name
WHERE COALESCE(c.current_accounts, 0) < 85  -- Max capacity
ORDER BY available_capacity DESC;

-- STEP 3: Insert Recommendation

-- Insert recommendation into tracking table
INSERT INTO DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE (
    account_id,
    recommended_csm,
    recommendation_timestamp,
    assignment_method,
    neediness_score,
    health_score,
    revenue,
    account_segment,
    account_level,
    optimization_score,
    llm_feedback,
    was_assigned,
    run_id,
    batch_size
) VALUES (
    'TEST_ACC_20251023',
    'David Kim',
    CURRENT_TIMESTAMP(),
    'single_optimized',
    7.5,
    65,
    125000,
    'Residential',
    'Corporate',
    0.789,
    'Assignment looks balanced. CSM has capacity and good segment match.',
    FALSE,  -- Set to TRUE when assignment is confirmed
    '20251023_102315',
    1
);

-- STEP 4: Check Cooling Period

-- Check if CSM is in cooling period (4 hours)
SELECT
    recommended_csm,
    MAX(recommendation_timestamp) as last_assignment,
    DATEDIFF(hour, MAX(recommendation_timestamp), CURRENT_TIMESTAMP()) as hours_since_assignment,
    CASE
        WHEN DATEDIFF(hour, MAX(recommendation_timestamp), CURRENT_TIMESTAMP()) >= 4 THEN 'Available'
        ELSE 'In Cooling Period'
    END as status
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE recommended_csm = 'David Kim'
    AND was_assigned = TRUE
GROUP BY recommended_csm;

-- STEP 5: Finalize Assignment

-- 1. Update recommendation as assigned
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
SET was_assigned = TRUE,
    actual_assigned_csm = 'David Kim'
WHERE account_id = 'TEST_ACC_20251023'
    AND run_id = '20251023_102315';

-- 2. Insert/Update final assignment
MERGE INTO DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE AS target
USING (
    SELECT
        'TEST_ACC_20251023' as account_id,
        'David Kim' as csm_name,
        CURRENT_TIMESTAMP() as assignment_date,
        'single_optimized' as assignment_method,
        'Assignment approved after optimization and cooling period check' as llm_review_feedback
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    csm_name = source.csm_name,
    assignment_date = source.assignment_date,
    assignment_method = source.assignment_method,
    llm_review_feedback = source.llm_review_feedback,
    last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    account_id, csm_name, assignment_date, assignment_method, llm_review_feedback, last_updated
) VALUES (
    source.account_id, source.csm_name, source.assignment_date,
    source.assignment_method, source.llm_review_feedback, CURRENT_TIMESTAMP()
);

-- STEP 6: Verification Queries

-- 1. Check the recommendation was stored
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = 'TEST_ACC_20251023'
ORDER BY recommendation_timestamp DESC
LIMIT 1;

-- 2. Check if assignment was finalized
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE account_id = 'TEST_ACC_20251023';

-- 3. Check CSM's new workload
SELECT
    'David Kim' as csm_name,
    COUNT(*) + 1 as new_total_accounts,
    85 - (COUNT(*) + 1) as remaining_capacity
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE csm_name = 'David Kim';

-- 4. Check all recommendations from this run
SELECT
    account_id,
    recommended_csm,
    optimization_score,
    was_assigned,
    llm_feedback
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE run_id = '20251023_102315'
ORDER BY optimization_score DESC;

-- 5. Audit trail - see assignment history
SELECT
    recommendation_timestamp,
    account_id,
    recommended_csm,
    optimization_score,
    was_assigned,
    actual_assigned_csm
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = 'TEST_ACC_20251023'
ORDER BY recommendation_timestamp DESC;

