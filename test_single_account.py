#!/usr/bin/env python
# coding: utf-8

"""
Test script to run CSM routing automation for a single account
"""

import logging
import sys
from csm_routing_automation import CSMRoutingAutomation

# Setup logging for test
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_single_account.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_single_account():
    """Test the automation with a single account"""

    logger.info("=" * 80)
    logger.info("STARTING SINGLE ACCOUNT CSM ROUTING TEST")
    logger.info("=" * 80)

    try:
        # Initialize the automation
        logger.info("Initializing CSM Routing Automation...")
        automation = CSMRoutingAutomation(
            config_file='properties.json',
            limits_file='csm_category_limits.json'
        )

        # Connect to Snowflake
        logger.info("Connecting to Snowflake...")
        if not automation.connect_snowflake():
            logger.error("Failed to connect to Snowflake")
            return False

        logger.info("Successfully connected to Snowflake")

        # Check for accounts needing CSM
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Checking for accounts needing CSM assignment...")
        logger.info("=" * 80)

        needs_csm_df = automation.get_needs_csm_accounts()

        if needs_csm_df.empty:
            logger.warning("No accounts found with 'Needs CSM' status")
            logger.info("\nQuerying to show sample of recent accounts:")

            # Show some recent accounts for context
            sample_query = """
            SELECT TOP 10
                account_id_ob,
                tenant_id,
                success_transition_status_ob
            FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
            WHERE account_id_ob IS NOT NULL
            ORDER BY account_id_ob DESC
            """
            sample_df = automation.execute_query(sample_query)
            if not sample_df.empty:
                logger.info(f"\nRecent accounts in onboarding table:")
                for _, row in sample_df.iterrows():
                    logger.info(f"  Account: {row['account_id_ob']}, Status: {row['success_transition_status_ob']}")

            return False

        logger.info(f"Found {len(needs_csm_df)} accounts needing CSM assignment")

        # Debug: print column names
        logger.info(f"Dataframe columns: {needs_csm_df.columns.tolist()}")

        # Take only the first account for testing
        single_account_df = needs_csm_df.head(1)
        account_id = single_account_df.iloc[0]['account_id']

        logger.info(f"\nProcessing single account: {account_id}")

        # Enrich the account data
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Enriching account data...")
        logger.info("=" * 80)

        enriched_df = automation.enrich_account_data(single_account_df)

        if enriched_df.empty:
            logger.error("Failed to enrich account data")
            return False

        account_info = enriched_df.iloc[0]
        logger.info("\nEnriched Account Details:")
        logger.info(f"  Account ID: {account_info.get('account_id')}")
        logger.info(f"  Tenant ID: {account_info.get('tenant_id')}")
        logger.info(f"  Segment: {account_info.get('segment', 'Unknown')}")
        logger.info(f"  Account Level: {account_info.get('account_level', 'Unknown')}")
        logger.info(f"  Neediness Score: {account_info.get('neediness_score', 0)}")
        logger.info(f"  Neediness Category: {account_info.get('neediness_category', 'Unknown')}")
        logger.info(f"  Health Segment: {account_info.get('health_segment', 'Unknown')}")
        logger.info(f"  Revenue: ${account_info.get('revenue', 0):,.2f}")
        logger.info(f"  Tech Count: {account_info.get('tech_count', 0)}")
        logger.info(f"  TAD Score: {account_info.get('tad_score', 0)}")
        logger.info(f"  Churn Risk: {account_info.get('churn_stage', 'Not at risk')}")

        # Check if it's a Residential Corporate account
        is_resi_corp = (
            account_info.get('segment') == 'Residential' and
            account_info.get('account_level') == 'Corporate'
        )

        if not is_resi_corp:
            logger.warning(f"Account is not Residential Corporate (Segment: {account_info.get('segment')}, Level: {account_info.get('account_level')})")
            logger.info("Note: Current automation only processes Residential Corporate accounts")
            # Continue anyway for testing

        # Get current CSM books
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Getting current CSM book data...")
        logger.info("=" * 80)

        csm_books = automation.get_current_csm_books()

        if not csm_books:
            logger.error("No CSM books found")
            return False

        logger.info(f"Found {len(csm_books)} CSMs with current assignments")
        logger.info("\nCSM Summary:")

        # Show top 5 CSMs by different metrics
        csm_list = []
        for csm, info in csm_books.items():
            csm_list.append({
                'name': csm,
                'accounts': info['count'],
                'neediness': info['total_neediness'],
                'tenure_months': info.get('tenure_months', 0),
                'tenure_category': info.get('tenure_category', 'Unknown'),
                'red_pct': (info.get('health_distribution', {}).get('Red', 0) / max(info['count'], 1)) * 100
            })

        # Sort by account count
        csm_list.sort(key=lambda x: x['accounts'])
        logger.info("\nCSMs with fewest accounts:")
        for csm in csm_list[:3]:
            logger.info(f"  {csm['name']}: {csm['accounts']} accounts, {csm['tenure_category']} ({csm['tenure_months']} months)")

        # Create recommendations table if needed
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Setting up recommendations tracking...")
        logger.info("=" * 80)

        automation.create_recommendations_table()

        # Process the single account assignment
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Running optimization for single account...")
        logger.info("=" * 80)

        # Use the same logic as the main run method but for single account
        if is_resi_corp:
            resi_corp_df = enriched_df
        else:
            logger.info("Processing non-Residential Corporate account for testing...")
            resi_corp_df = enriched_df

        # Run the assignment
        csm, score = automation.assign_single_account_optimized(resi_corp_df.iloc[0], csm_books)

        if csm:
            logger.info(f"\n" + "=" * 50)
            logger.info(f"ASSIGNMENT RESULT:")
            logger.info(f"  Account {account_id} -> CSM {csm}")
            logger.info(f"  Optimization Score: {score:.2f}")
            logger.info(f"=" * 50)

            assignments = {account_id: csm}

            # Get CSM details
            csm_info = csm_books.get(csm, {})
            logger.info(f"\nAssigned CSM Details:")
            logger.info(f"  Name: {csm}")
            logger.info(f"  Current Accounts: {csm_info.get('count', 0)}")
            logger.info(f"  After Assignment: {csm_info.get('count', 0) + 1}")
            logger.info(f"  Tenure: {csm_info.get('tenure_category', 'Unknown')} ({csm_info.get('tenure_months', 0)} months)")
            logger.info(f"  Health Distribution: Red={csm_info.get('health_distribution', {}).get('Red', 0)}, Yellow={csm_info.get('health_distribution', {}).get('Yellow', 0)}, Green={csm_info.get('health_distribution', {}).get('Green', 0)}")

            # LLM Review (if available)
            if automation.claude_client:
                logger.info("\n" + "=" * 80)
                logger.info("STEP 6: Getting LLM review of assignment...")
                logger.info("=" * 80)

                should_rerun, llm_feedback, revised_assignments = automation.review_assignments_with_llm(
                    assignments, resi_corp_df, csm_books
                )

                logger.info(f"\nLLM Review Result:")
                logger.info(f"  Approved: {'No - Suggests Rerun' if should_rerun else 'Yes'}")
                logger.info(f"  Feedback: {llm_feedback}")

                if should_rerun:
                    logger.warning("LLM suggests reconsidering this assignment")
            else:
                logger.info("\nLLM review skipped (no API key configured)")
                llm_feedback = None

            # Update in Snowflake
            logger.info("\n" + "=" * 80)
            logger.info("STEP 7: Updating assignment in Snowflake...")
            logger.info("=" * 80)

            # Actually update in Snowflake
            success = automation.update_assignments_in_snowflake(assignments, llm_feedback)
            if success:
                logger.info("✓ Assignment successfully saved to Snowflake ACCOUNT_CSM_ASSIGNMENTS_CANNE table")
            else:
                logger.error("Failed to save assignment to Snowflake")

            # Generate balance report
            logger.info("\n" + "=" * 80)
            logger.info("STEP 8: Balance Report")
            logger.info("=" * 80)

            automation.generate_balance_report(csm_books)

        else:
            logger.error(f"No eligible CSM found for account {account_id}")
            return False

        # Close Snowflake connection
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
            logger.info("\nSnowflake connection closed")

        logger.info("\n" + "=" * 80)
        logger.info("TEST COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    # Run the test
    success = test_single_account()

    if success:
        logger.info("\n✅ Single account test completed successfully")
        sys.exit(0)
    else:
        logger.error("\n❌ Single account test failed")
        sys.exit(1)