#!/usr/bin/env python3
"""
Test single actual account assignment using the main CSM routing automation.
This script uses the existing implementation to process one real account.
"""

import sys
import os
import logging
from datetime import datetime
import pandas as pd

# Import the main automation class
from csm_routing_automation import CSMRoutingAutomation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'single_account_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_single_account():
    """Test CSM assignment for a single actual account"""

    logger.info("=" * 80)
    logger.info("SINGLE ACCOUNT CSM ASSIGNMENT TEST")
    logger.info("Using main csm_routing_automation.py implementation")
    logger.info("=" * 80)

    # Create automation instance
    automation = CSMRoutingAutomation()

    # Connect to Snowflake
    logger.info("\n1. Connecting to Snowflake...")
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake. Please check credentials in .env file")
        logger.info("\nCreate a .env file with:")
        logger.info("SNOWFLAKE_USER=your_username")
        logger.info("SNOWFLAKE_PASSWORD=your_password")
        logger.info("SNOWFLAKE_ACCOUNT=your_account")
        logger.info("SNOWFLAKE_WAREHOUSE=your_warehouse")
        return False

    logger.info("✓ Connected to Snowflake successfully")

    try:
        # Get ONE account that needs CSM
        logger.info("\n2. Finding a single account that needs CSM assignment...")

        query = """
        SELECT
            account_id_ob as account_id,
            tenant_id,
            success_transition_status_ob
        FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL o
        WHERE success_transition_status_ob = 'Needs CSM'
            AND account_id_ob IS NOT NULL
            -- EXCLUDE accounts that already have recommendations today
            AND NOT EXISTS (
                SELECT 1
                FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE r
                WHERE r.account_id = o.account_id_ob
                AND DATE(r.recommendation_timestamp) = CURRENT_DATE()
            )
            -- EXCLUDE accounts that are already assigned
            AND NOT EXISTS (
                SELECT 1
                FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE a
                WHERE a.account_id = o.account_id_ob
            )
        LIMIT 1
        """

        needs_csm_df = automation.execute_query(query)

        if needs_csm_df.empty:
            logger.warning("No accounts found with status 'Needs CSM'")
            logger.info("\nTo create test data, run this SQL:")
            logger.info("UPDATE DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL")
            logger.info("SET success_transition_status_ob = 'Needs CSM'")
            logger.info("WHERE account_id_ob = 'YOUR_ACCOUNT_ID' LIMIT 1;")
            return False

        # Snowflake returns uppercase column names, so standardize to lowercase
        needs_csm_df.columns = [col.lower() for col in needs_csm_df.columns]
        account_id = needs_csm_df.iloc[0]['account_id']
        logger.info(f"✓ Found account: {account_id}")

        # Enrich account data
        logger.info("\n3. Enriching account data...")
        enriched_df = automation.enrich_account_data(needs_csm_df)

        if enriched_df.empty:
            logger.error("Failed to enrich account data")
            return False

        account = enriched_df.iloc[0]

        # Check if it's Residential Corporate
        if account.get('segment') != 'Residential' or account.get('account_level') != 'Corporate':
            logger.warning(f"Account is {account.get('segment')} {account.get('account_level')}, not Residential Corporate")
            logger.info("Looking for a Residential Corporate account instead...")

            # Try to find a Residential Corporate account
            query = """
            WITH enriched AS (
                SELECT
                    o.account_id_ob as account_id,
                    o.tenant_id,
                    o.success_transition_status_ob,
                    COALESCE(o.segment, 'Residential') as segment,
                    COALESCE(o.account_level, 'Corporate') as account_level
                FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL o
                WHERE o.success_transition_status_ob = 'Needs CSM'
                    AND o.account_id_ob IS NOT NULL
            )
            SELECT *
            FROM enriched
            WHERE segment = 'Residential'
                AND account_level = 'Corporate'
            LIMIT 1
            """

            needs_csm_df = automation.execute_query(query)
            if needs_csm_df.empty:
                logger.warning("No Residential Corporate accounts need CSM assignment")
                return False

            needs_csm_df.columns = [col.lower() for col in needs_csm_df.columns]
            account_id = needs_csm_df.iloc[0]['account_id']
            enriched_df = automation.enrich_account_data(needs_csm_df)
            account = enriched_df.iloc[0]

        logger.info(f"✓ Account Details:")
        logger.info(f"  - Account ID: {account.get('account_id')}")
        logger.info(f"  - Segment: {account.get('segment', 'Unknown')}")
        logger.info(f"  - Level: {account.get('account_level', 'Unknown')}")
        logger.info(f"  - Neediness Score: {account.get('neediness_score', 0)}")
        logger.info(f"  - Health Score: {account.get('health_score', 0)}")
        logger.info(f"  - Revenue: ${account.get('revenue', 0):,.2f}")

        # Get current CSM books (with resi_corp_active_csms filter)
        logger.info("\n4. Getting eligible CSMs (filtered by resi_corp_active_csms)...")
        csm_books = automation.get_current_csm_books(min_account_threshold=5)

        if not csm_books:
            logger.error("No eligible CSMs found after filtering!")
            logger.info("\nCheck if CSMs exist in resi_corp_active_csms table:")
            logger.info("SELECT COUNT(*) FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms;")
            return False

        logger.info(f"✓ Found {len(csm_books)} eligible CSMs")
        logger.info("\nTop 5 CSMs by availability:")
        sorted_csms = sorted(csm_books.items(), key=lambda x: x[1]['count'])[:5]
        for csm, data in sorted_csms:
            logger.info(f"  - {csm}: {data['count']} accounts (capacity: {85 - data['count']})")

        # Create recommendations table if needed
        logger.info("\n5. Ensuring recommendations table exists...")
        automation.create_recommendations_table()

        # Run single account assignment
        logger.info("\n6. Running assignment optimization...")

        # Generate run_id
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Use the single account optimization
        csm, score = automation.assign_single_account_optimized(
            account,  # Pass as Series, not dict
            csm_books,
            excluded_csms=[]  # No excluded CSMs for this test
        )

        if not csm:
            logger.error("No suitable CSM found for the account")
            return False

        logger.info(f"✓ Recommended CSM: {csm}")
        logger.info(f"  Optimization Score: {score:.3f}")

        # Store the recommendation
        logger.info("\n7. Storing recommendation in database...")

        automation.store_recommendation(
            account_id=account['account_id'],
            csm_name=csm,
            account_data=account,
            optimization_score=score,
            method='single_optimized',
            run_id=run_id,
            batch_size=1,
            llm_feedback='Single account test assignment'
        )

        logger.info("✓ Recommendation stored in CSM_ROUTING_RECOMMENDATIONS_CANNE")

        # Verify the recommendation was stored
        logger.info("\n8. Verifying recommendation was stored...")

        verify_query = f"""
        SELECT
            recommendation_id,
            account_id,
            recommended_csm,
            optimization_score,
            recommendation_timestamp,
            was_assigned
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE account_id = '{account['account_id']}'
            AND run_id = '{run_id}'
        ORDER BY recommendation_timestamp DESC
        LIMIT 1
        """

        result = automation.execute_query(verify_query)

        if not result.empty:
            result.columns = [col.lower() for col in result.columns]
            rec = result.iloc[0]
            logger.info("✓ Recommendation verified in database:")
            logger.info(f"  - Recommendation ID: {rec.get('recommendation_id')}")
            logger.info(f"  - Account ID: {rec.get('account_id')}")
            logger.info(f"  - Recommended CSM: {rec.get('recommended_csm')}")
            if rec.get('optimization_score') is not None:
                logger.info(f"  - Optimization Score: {rec.get('optimization_score'):.3f}")
            else:
                logger.info(f"  - Optimization Score: {rec.get('optimization_score')}")
            logger.info(f"  - Timestamp: {rec.get('recommendation_timestamp')}")
            logger.info(f"  - Was Assigned: {rec.get('was_assigned')}")
        else:
            logger.warning("Could not verify recommendation in database")

        # Generate SQL queries for further verification
        logger.info("\n" + "=" * 80)
        logger.info("SQL QUERIES TO CHECK RESULTS")
        logger.info("=" * 80)

        queries = f"""
-- 1. Check the recommendation that was just created
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = '{account['account_id']}'
    AND run_id = '{run_id}';

-- 2. Check all recommendations for this account
SELECT
    recommendation_timestamp,
    recommended_csm,
    optimization_score,
    was_assigned,
    llm_feedback
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE account_id = '{account['account_id']}'
ORDER BY recommendation_timestamp DESC;

-- 3. Check if CSM is in active list
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
WHERE active_csm = '{csm}';

-- 4. Check CSM's current workload
SELECT
    COUNT(*) as current_accounts,
    85 - COUNT(*) as available_capacity
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE csm_name = '{csm}';

-- 5. To approve and finalize the assignment, run:
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
SET was_assigned = TRUE,
    actual_assigned_csm = '{csm}'
WHERE account_id = '{account['account_id']}'
    AND run_id = '{run_id}';

-- Then insert into final assignments table:
MERGE INTO DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE AS target
USING (
    SELECT
        '{account['account_id']}' as account_id,
        '{csm}' as csm_name,
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
"""

        logger.info("\nSQL Queries:")
        logger.info(queries)

        # Save queries to file
        query_file = f'verify_assignment_{run_id}.sql'
        with open(query_file, 'w') as f:
            f.write(queries)

        logger.info(f"\n✓ SQL queries saved to: {query_file}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)
        logger.info(f"\n✅ Successfully processed single account assignment!")
        logger.info(f"\nAccount: {account['account_id']}")
        logger.info(f"Recommended CSM: {csm}")
        logger.info(f"Optimization Score: {score:.3f}")
        logger.info(f"Run ID: {run_id}")
        logger.info(f"\n📊 Recommendation stored in: CSM_ROUTING_RECOMMENDATIONS_CANNE")
        logger.info(f"📊 To finalize, update was_assigned = TRUE and insert into ACCOUNT_CSM_ASSIGNMENTS_CANNE")

        return True

    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        logger.exception("Full error:")
        return False

    finally:
        # Close Snowflake connection
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
            logger.info("\nClosed Snowflake connection")

def main():
    """Main entry point"""
    logger.info("Starting Single Account CSM Assignment Test")
    logger.info(f"Test started at: {datetime.now()}")
    logger.info("\nThis test will:")
    logger.info("1. Connect to Snowflake using credentials from .env")
    logger.info("2. Find ONE account needing CSM assignment")
    logger.info("3. Get eligible CSMs (filtered by resi_corp_active_csms)")
    logger.info("4. Run optimization to find best CSM")
    logger.info("5. Store recommendation in CSM_ROUTING_RECOMMENDATIONS_CANNE")
    logger.info("6. Generate SQL queries to verify and finalize")

    success = test_single_account()

    logger.info(f"\nTest completed at: {datetime.now()}")

    if success:
        logger.info("\n✅ TEST SUCCESSFUL!")
        logger.info("\nNext steps:")
        logger.info("1. Run the SQL queries to verify the recommendation")
        logger.info("2. Update was_assigned = TRUE to approve")
        logger.info("3. Check ACCOUNT_CSM_ASSIGNMENTS_CANNE for final assignment")
    else:
        logger.error("\n❌ TEST FAILED")
        logger.info("\nTroubleshooting:")
        logger.info("1. Check .env file has correct Snowflake credentials")
        logger.info("2. Verify accounts exist with status 'Needs CSM'")
        logger.info("3. Ensure resi_corp_active_csms table has data")
        logger.info("4. Review the log file for specific errors")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())