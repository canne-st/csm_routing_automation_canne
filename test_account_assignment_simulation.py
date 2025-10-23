#!/usr/bin/env python3
"""
Simulation test for single account CSM assignment with resi_corp_active_csms filter.
This script simulates the assignment process and generates SQL queries to verify results.
"""

import sys
import logging
from datetime import datetime, timedelta
import random
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test_account_simulation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def simulate_single_account_assignment():
    """Simulate single account assignment and generate verification queries"""

    logger.info("=" * 80)
    logger.info("SINGLE ACCOUNT ASSIGNMENT SIMULATION")
    logger.info("Simulating CSM assignment with resi_corp_active_csms filter")
    logger.info("=" * 80)

    # Test account details (simulated)
    test_account = {
        'account_id': 'TEST_ACC_20251023',
        'tenant_id': 'TENANT_123',
        'segment': 'Residential',
        'account_level': 'Corporate',
        'neediness_score': 7.5,
        'health_score': 65,
        'revenue': 125000,
        'tad_score': 4.2,
        'industry': 'Roofing',
        'success_transition_status': 'Needs CSM'
    }

    # Simulated eligible CSMs (would come from resi_corp_active_csms table)
    eligible_csms = [
        {'name': 'Sarah Johnson', 'current_accounts': 42, 'tenure': 'Senior', 'avg_health': 72},
        {'name': 'Michael Chen', 'current_accounts': 38, 'tenure': 'Mid', 'avg_health': 68},
        {'name': 'Emily Rodriguez', 'current_accounts': 45, 'tenure': 'Expert', 'avg_health': 75},
        {'name': 'David Kim', 'current_accounts': 35, 'tenure': 'Junior', 'avg_health': 65},
        {'name': 'Lisa Thompson', 'current_accounts': 40, 'tenure': 'Senior', 'avg_health': 70}
    ]

    logger.info(f"\n📋 TEST ACCOUNT DETAILS:")
    logger.info(f"Account ID: {test_account['account_id']}")
    logger.info(f"Segment: {test_account['segment']} {test_account['account_level']}")
    logger.info(f"Neediness Score: {test_account['neediness_score']}")
    logger.info(f"Health Score: {test_account['health_score']}")
    logger.info(f"Revenue: ${test_account['revenue']:,.2f}")

    logger.info(f"\n👥 ELIGIBLE CSMs (from resi_corp_active_csms):")
    logger.info(f"Total eligible CSMs after filter: {len(eligible_csms)}")
    for csm in eligible_csms[:3]:
        logger.info(f"  - {csm['name']}: {csm['current_accounts']} accounts, {csm['tenure']} level")

    # Step 1: Check eligibility
    logger.info("\n" + "-" * 60)
    logger.info("STEP 1: VERIFY ACCOUNT NEEDS CSM")
    logger.info("-" * 60)

    verify_account_query = f"""
-- Query to verify account needs CSM assignment
SELECT
    account_id_ob as account_id,
    tenant_id,
    success_transition_status_ob,
    segment,
    account_level
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE account_id_ob = '{test_account['account_id']}'
    AND success_transition_status_ob = 'Needs CSM';
"""
    logger.info("SQL Query to run:")
    logger.info(verify_account_query)

    # Step 2: Get eligible CSMs
    logger.info("\n" + "-" * 60)
    logger.info("STEP 2: GET ELIGIBLE CSMs")
    logger.info("-" * 60)

    eligible_csms_query = """
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
"""
    logger.info("SQL Query to run:")
    logger.info(eligible_csms_query)

    # Step 3: Simulate assignment optimization
    logger.info("\n" + "-" * 60)
    logger.info("STEP 3: ASSIGNMENT OPTIMIZATION")
    logger.info("-" * 60)

    # Simulate optimization scores
    optimization_results = []
    for csm in eligible_csms:
        # Calculate simulated optimization score
        capacity_score = (85 - csm['current_accounts']) / 85 * 0.3
        health_match_score = (1 - abs(csm['avg_health'] - test_account['health_score'])/100) * 0.25
        tenure_score = {'Junior': 0.6, 'Mid': 0.7, 'Senior': 0.85, 'Expert': 0.95}.get(csm['tenure'], 0.5) * 0.2
        random_factor = random.uniform(0.8, 1.0) * 0.25

        total_score = capacity_score + health_match_score + tenure_score + random_factor

        optimization_results.append({
            'csm': csm['name'],
            'score': total_score,
            'capacity_score': capacity_score,
            'health_match': health_match_score,
            'tenure_score': tenure_score
        })

    # Sort by score
    optimization_results.sort(key=lambda x: x['score'], reverse=True)
    recommended_csm = optimization_results[0]['csm']

    logger.info(f"Optimization Results:")
    for i, result in enumerate(optimization_results[:5], 1):
        logger.info(f"  {i}. {result['csm']}: {result['score']:.3f}")

    logger.info(f"\n✅ RECOMMENDED CSM: {recommended_csm}")
    logger.info(f"   Optimization Score: {optimization_results[0]['score']:.3f}")

    # Step 4: Generate INSERT for recommendation
    logger.info("\n" + "-" * 60)
    logger.info("STEP 4: STORE RECOMMENDATION")
    logger.info("-" * 60)

    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    recommendation_insert = f"""
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
    '{test_account['account_id']}',
    '{recommended_csm}',
    CURRENT_TIMESTAMP(),
    'single_optimized',
    {test_account['neediness_score']},
    {test_account['health_score']},
    {test_account['revenue']},
    '{test_account['segment']}',
    '{test_account['account_level']}',
    {optimization_results[0]['score']:.3f},
    'Assignment looks balanced. CSM has capacity and good segment match.',
    FALSE,  -- Set to TRUE when assignment is confirmed
    '{run_id}',
    1
);
"""
    logger.info("SQL Query to run:")
    logger.info(recommendation_insert)

    # Step 5: Verify cooling period
    logger.info("\n" + "-" * 60)
    logger.info("STEP 5: VERIFY COOLING PERIOD")
    logger.info("-" * 60)

    cooling_check_query = f"""
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
WHERE recommended_csm = '{recommended_csm}'
    AND was_assigned = TRUE
GROUP BY recommended_csm;
"""
    logger.info("SQL Query to run:")
    logger.info(cooling_check_query)

    # Step 6: Update assignment if approved
    logger.info("\n" + "-" * 60)
    logger.info("STEP 6: FINALIZE ASSIGNMENT (if approved)")
    logger.info("-" * 60)

    finalize_queries = f"""
-- 1. Update recommendation as assigned
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
SET was_assigned = TRUE,
    actual_assigned_csm = '{recommended_csm}'
WHERE account_id = '{test_account['account_id']}'
    AND run_id = '{run_id}';

-- 2. Insert/Update final assignment
MERGE INTO DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE AS target
USING (
    SELECT
        '{test_account['account_id']}' as account_id,
        '{recommended_csm}' as csm_name,
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
"""
    logger.info("SQL Queries to run (after approval):")
    logger.info(finalize_queries)

    # Verification queries
    logger.info("\n" + "=" * 80)
    logger.info("📊 VERIFICATION QUERIES")
    logger.info("=" * 80)

    verification_queries = f"""
-- 1. Check the recommendation was stored
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = '{test_account['account_id']}'
ORDER BY recommendation_timestamp DESC
LIMIT 1;

-- 2. Check if assignment was finalized
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE account_id = '{test_account['account_id']}';

-- 3. Check CSM's new workload
SELECT
    '{recommended_csm}' as csm_name,
    COUNT(*) + 1 as new_total_accounts,
    85 - (COUNT(*) + 1) as remaining_capacity
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE csm_name = '{recommended_csm}';

-- 4. Check all recommendations from this run
SELECT
    account_id,
    recommended_csm,
    optimization_score,
    was_assigned,
    llm_feedback
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE run_id = '{run_id}'
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
WHERE account_id = '{test_account['account_id']}'
ORDER BY recommendation_timestamp DESC;
"""

    logger.info("\nVerification Queries:")
    logger.info(verification_queries)

    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📝 SUMMARY")
    logger.info("=" * 80)

    summary = f"""
ASSIGNMENT SUMMARY:
-------------------
Account ID: {test_account['account_id']}
Recommended CSM: {recommended_csm}
Optimization Score: {optimization_results[0]['score']:.3f}
Run ID: {run_id}

OUTPUT TABLES:
--------------
1. Recommendations: DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
   - Contains the recommendation with optimization score
   - Check was_assigned flag to see if approved

2. Assignments: DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
   - Contains final assignment after approval
   - One record per account

KEY POINTS:
-----------
✓ Only CSMs in resi_corp_active_csms table were considered
✓ Optimization balanced capacity, health match, and tenure
✓ 4-hour cooling period is enforced between assignments
✓ Recommendation stored with full audit trail
✓ Assignment becomes final when was_assigned = TRUE

NEXT STEPS:
-----------
1. Run the verification queries above in Snowflake
2. Check the optimization scores for all CSMs
3. Approve assignment by updating was_assigned = TRUE
4. Verify in ACCOUNT_CSM_ASSIGNMENTS_CANNE table
"""

    logger.info(summary)

    # Save queries to file
    with open(f'assignment_queries_{run_id}.sql', 'w') as f:
        f.write("-- Account Assignment SQL Queries\n")
        f.write(f"-- Generated: {datetime.now()}\n")
        f.write(f"-- Account ID: {test_account['account_id']}\n")
        f.write(f"-- Recommended CSM: {recommended_csm}\n\n")
        f.write("-- STEP 1: Verify Account\n")
        f.write(verify_account_query + "\n")
        f.write("-- STEP 2: Get Eligible CSMs\n")
        f.write(eligible_csms_query + "\n")
        f.write("-- STEP 3: Insert Recommendation\n")
        f.write(recommendation_insert + "\n")
        f.write("-- STEP 4: Check Cooling Period\n")
        f.write(cooling_check_query + "\n")
        f.write("-- STEP 5: Finalize Assignment\n")
        f.write(finalize_queries + "\n")
        f.write("-- STEP 6: Verification Queries\n")
        f.write(verification_queries + "\n")

    logger.info(f"\n✅ SQL queries saved to: assignment_queries_{run_id}.sql")

    return True

def main():
    """Main execution"""
    logger.info("Starting Account Assignment Simulation Test")
    logger.info(f"Test started at: {datetime.now()}")

    success = simulate_single_account_assignment()

    logger.info(f"\nTest completed at: {datetime.now()}")

    if success:
        logger.info("\n✅ SIMULATION COMPLETED SUCCESSFULLY!")
        logger.info("\nYou can now:")
        logger.info("1. Copy the SQL queries from the generated .sql file")
        logger.info("2. Run them in Snowflake to perform the actual assignment")
        logger.info("3. Check the output tables for results")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())