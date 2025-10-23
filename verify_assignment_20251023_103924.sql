
-- 1. Check the recommendation that was just created
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = '0011P000016p9WdQAI'
    AND run_id = '20251023_103924';

-- 2. Check all recommendations for this account
SELECT
    recommendation_timestamp,
    recommended_csm,
    optimization_score,
    was_assigned,
    llm_feedback
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = '0011P000016p9WdQAI'
ORDER BY recommendation_timestamp DESC;

-- 3. Check if CSM is in active list
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
WHERE active_csm = 'Warren Rogers';

-- 4. Check CSM's current workload
SELECT
    COUNT(*) as current_accounts,
    85 - COUNT(*) as available_capacity
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE csm_name = 'Warren Rogers';

-- 5. To approve and finalize the assignment, run:
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
SET was_assigned = TRUE,
    actual_assigned_csm = 'Warren Rogers'
WHERE account_id = '0011P000016p9WdQAI'
    AND run_id = '20251023_103924';

-- Then insert into final assignments table:
MERGE INTO DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE AS target
USING (
    SELECT
        '0011P000016p9WdQAI' as account_id,
        'Warren Rogers' as csm_name,
        CURRENT_TIMESTAMP() as assignment_date,
        'single_optimized' as assignment_method,
        'Approved via single account test' as llm_review_feedback
) AS source
ON target.account_id = source.account_id
WHEN MATCHED THEN UPDATE SET
    csm_name = source.csm_name,
    assignment_date = source.assignment_date,
    assignment_method = source.assignment_method,
    llm_review_feedback = source.llm_review_feedback,
    last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    account_id, csm_name, assignment_date, assignment_method,
    llm_review_feedback, last_updated
) VALUES (
    source.account_id, source.csm_name, source.assignment_date,
    source.assignment_method, source.llm_review_feedback, CURRENT_TIMESTAMP()
);
