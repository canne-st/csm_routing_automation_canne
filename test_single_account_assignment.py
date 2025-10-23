#!/usr/bin/env python3
"""
Test script for single account CSM assignment with resi_corp_active_csms filter.
This script tests the end-to-end flow of assigning a single account to a CSM.
"""

import sys
import logging
from datetime import datetime
from csm_routing_automation import CSMRoutingAutomation
import pandas as pd

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test_single_account_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_single_account_assignment():
    """Test single account assignment with the resi_corp_active_csms filter"""

    logger.info("=" * 80)
    logger.info("SINGLE ACCOUNT ASSIGNMENT TEST")
    logger.info("Testing CSM assignment with resi_corp_active_csms filter")
    logger.info("=" * 80)

    try:
        # Initialize the CSM routing automation
        logger.info("\nStep 1: Initializing CSM Routing Automation...")
        automation = CSMRoutingAutomation()

        # Get accounts needing CSM assignment
        logger.info("\n" + "-" * 60)
        logger.info("Step 2: Getting accounts that need CSM assignment")
        logger.info("-" * 60)

        needs_csm_df = automation.get_needs_csm_accounts()

        if needs_csm_df.empty:
            logger.warning("No accounts found with status 'Needs CSM'")
            logger.info("\nTo test, you can manually update an account in Snowflake:")
            logger.info("UPDATE DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL")
            logger.info("SET success_transition_status_ob = 'Needs CSM'")
            logger.info("WHERE account_id_ob = 'YOUR_TEST_ACCOUNT_ID'")
            return False

        logger.info(f"Found {len(needs_csm_df)} accounts needing CSM assignment")

        # Take the first account for testing
        test_account = needs_csm_df.iloc[0]
        test_account_id = test_account['account_id']
        logger.info(f"\nUsing test account: {test_account_id}")

        # Enrich account data
        logger.info("\n" + "-" * 60)
        logger.info("Step 3: Enriching account data")
        logger.info("-" * 60)

        enriched_df = automation.enrich_account_data(pd.DataFrame([test_account]))

        if not enriched_df.empty:
            enriched_account = enriched_df.iloc[0]
            logger.info(f"Account details:")
            logger.info(f"  - Account ID: {enriched_account.get('account_id', 'N/A')}")
            logger.info(f"  - Segment: {enriched_account.get('segment', 'N/A')}")
            logger.info(f"  - Level: {enriched_account.get('account_level', 'N/A')}")
            logger.info(f"  - Neediness Score: {enriched_account.get('neediness_score', 'N/A')}")
            logger.info(f"  - Health Score: {enriched_account.get('health_score', 'N/A')}")
            logger.info(f"  - Revenue: ${enriched_account.get('revenue', 0):,.2f}")

        # Get current CSM books
        logger.info("\n" + "-" * 60)
        logger.info("Step 4: Getting current CSM books (filtered by resi_corp_active_csms)")
        logger.info("-" * 60)

        csm_books = automation.get_current_csm_books(min_account_threshold=5)

        if not csm_books:
            logger.error("No eligible CSMs found after filtering!")
            return False

        logger.info(f"Found {len(csm_books)} eligible CSMs after filtering")
        logger.info("\nTop 5 eligible CSMs by account count:")
        sorted_csms = sorted(csm_books.items(), key=lambda x: x[1]['count'])[:5]
        for csm, data in sorted_csms:
            logger.info(f"  - {csm}: {data['count']} accounts, {data['tenure_category']} tenure")

        # Perform single account assignment
        logger.info("\n" + "-" * 60)
        logger.info("Step 5: Running single account assignment optimization")
        logger.info("-" * 60)

        # Run the assignment for the single account
        single_account_data = enriched_df.to_dict('records')[0] if not enriched_df.empty else test_account.to_dict()

        logger.info(f"\nAssigning account {test_account_id}...")

        # Call the single account assignment method
        recommended_csm, scores = automation.assign_single_account_optimized(
            single_account_data,
            csm_books,
            automation.eligible_csm_list,
            cooling_period_hours=4
        )

        if recommended_csm:
            logger.info(f"\n✅ ASSIGNMENT SUCCESSFUL!")
            logger.info(f"Account {test_account_id} assigned to: {recommended_csm}")
            logger.info(f"\nAssignment scores:")
            if scores:
                sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:5]
                for csm, score in sorted_scores:
                    logger.info(f"  - {csm}: {score:.3f}")
        else:
            logger.warning(f"No suitable CSM found for account {test_account_id}")
            return False

        # Store the recommendation in the database
        logger.info("\n" + "-" * 60)
        logger.info("Step 6: Storing recommendation in database")
        logger.info("-" * 60)

        # Create recommendations table if it doesn't exist
        automation.create_recommendations_table()

        # Store the recommendation
        recommendation_data = {
            'account_id': test_account_id,
            'recommended_csm': recommended_csm,
            'assignment_method': 'single_optimized',
            'neediness_score': single_account_data.get('neediness_score', 0),
            'health_score': single_account_data.get('health_score', 0),
            'revenue': single_account_data.get('revenue', 0),
            'account_segment': single_account_data.get('segment', 'Unknown'),
            'account_level': single_account_data.get('account_level', 'Unknown'),
            'optimization_score': scores.get(recommended_csm, 0) if scores else 0,
            'was_assigned': False,  # Set to True when actually assigned
            'run_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'batch_size': 1
        }

        rec_df = pd.DataFrame([recommendation_data])
        automation.store_recommendations(rec_df)

        logger.info(f"Recommendation stored in: {automation.recommendations_table}")

        # Query to verify the recommendation was stored
        verify_query = f"""
        SELECT * FROM {automation.recommendations_table}
        WHERE account_id = '{test_account_id}'
        ORDER BY recommendation_timestamp DESC
        LIMIT 1
        """

        result = automation.execute_query(verify_query)
        if not result.empty:
            logger.info("\nRecommendation verified in database:")
            logger.info(f"  - Recommendation ID: {result.iloc[0].get('recommendation_id', 'N/A')}")
            logger.info(f"  - Timestamp: {result.iloc[0].get('recommendation_timestamp', 'N/A')}")

        # Show output table information
        logger.info("\n" + "=" * 80)
        logger.info("OUTPUT TABLE INFORMATION")
        logger.info("=" * 80)

        logger.info("\n📊 Tables to check for outputs:")
        logger.info("\n1. RECOMMENDATIONS TABLE (All recommendations made):")
        logger.info(f"   Table: {automation.recommendations_table}")
        logger.info("   Query to check:")
        logger.info(f"   SELECT * FROM {automation.recommendations_table}")
        logger.info(f"   WHERE account_id = '{test_account_id}'")
        logger.info("   ORDER BY recommendation_timestamp DESC;")

        logger.info("\n2. ASSIGNMENTS TABLE (Final assignments after approval):")
        logger.info(f"   Table: {automation.assignments_table}")
        logger.info("   Query to check:")
        logger.info(f"   SELECT * FROM {automation.assignments_table}")
        logger.info(f"   WHERE account_id = '{test_account_id}';")

        logger.info("\n3. Check all recent recommendations:")
        logger.info(f"   SELECT * FROM {automation.recommendations_table}")
        logger.info("   WHERE recommendation_timestamp >= CURRENT_DATE")
        logger.info("   ORDER BY recommendation_timestamp DESC;")

        logger.info("\n4. Check CSM workload after assignment:")
        logger.info(f"   SELECT recommended_csm, COUNT(*) as accounts_assigned")
        logger.info(f"   FROM {automation.recommendations_table}")
        logger.info("   WHERE was_assigned = TRUE")
        logger.info("   GROUP BY recommended_csm")
        logger.info("   ORDER BY accounts_assigned DESC;")

        # Additional information
        logger.info("\n" + "-" * 60)
        logger.info("IMPORTANT NOTES:")
        logger.info("-" * 60)
        logger.info("1. The recommendation is stored but NOT yet assigned")
        logger.info("2. To actually assign the account, run the full automation or update was_assigned = TRUE")
        logger.info("3. The CSM must be in resi_corp_active_csms table to be eligible")
        logger.info("4. Check logs for any CSMs filtered out by the new table")

        return True

    except Exception as e:
        logger.error(f"\n❌ Test failed with error: {str(e)}")
        logger.exception("Full error traceback:")
        return False

def main():
    """Main execution function"""
    logger.info("Starting Single Account Assignment Test")
    logger.info(f"Test started at: {datetime.now()}")
    logger.info("\nThis test will:")
    logger.info("1. Find an account needing CSM assignment")
    logger.info("2. Get eligible CSMs (filtered by resi_corp_active_csms)")
    logger.info("3. Run optimization to find best CSM match")
    logger.info("4. Store recommendation in database")
    logger.info("5. Show you where to find the outputs")

    success = test_single_account_assignment()

    logger.info(f"\nTest completed at: {datetime.now()}")

    if success:
        logger.info("\n✅ TEST COMPLETED SUCCESSFULLY!")
        logger.info("\nNext steps:")
        logger.info("1. Check the recommendations table for the assignment")
        logger.info("2. Review the CSM selected and optimization scores")
        logger.info("3. If satisfied, update was_assigned = TRUE to confirm")
    else:
        logger.error("\n❌ TEST FAILED!")
        logger.info("\nTroubleshooting:")
        logger.info("1. Check if there are accounts with status 'Needs CSM'")
        logger.info("2. Verify resi_corp_active_csms table has eligible CSMs")
        logger.info("3. Review the log file for detailed error messages")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())