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

        # Take 3-4 accounts for testing
        test_accounts_df = needs_csm_df.head(4)  # Get up to 4 accounts
        account_ids = test_accounts_df['account_id'].tolist()

        logger.info(f"\nProcessing {len(test_accounts_df)} accounts for testing:")
        for acc_id in account_ids:
            logger.info(f"  - {acc_id}")

        # Enrich the account data
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Enriching account data...")
        logger.info("=" * 80)

        enriched_df = automation.enrich_account_data(test_accounts_df)

        if enriched_df.empty:
            logger.error("Failed to enrich account data")
            return False

        logger.info(f"\nEnriched {len(enriched_df)} Account Details:")
        for idx, account_info in enriched_df.iterrows():
            logger.info(f"\nAccount {idx + 1}:")
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

        # Filter for Residential Corporate accounts (but process all for testing)
        resi_corp_mask = (
            (enriched_df['segment'] == 'Residential') &
            (enriched_df['account_level'] == 'Corporate')
        )

        resi_corp_count = resi_corp_mask.sum()
        logger.info(f"\nFound {resi_corp_count} Residential Corporate accounts out of {len(enriched_df)} total")

        # Get current CSM books
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Getting current CSM book data...")
        logger.info("=" * 80)

        # Use minimum account threshold from configuration
        min_accounts = automation.limits.get('residential_corporate', {}).get('min_accounts_for_eligibility', 5)
        csm_books = automation.get_current_csm_books(min_account_threshold=min_accounts)

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

        # Process multiple accounts assignment
        logger.info("\n" + "=" * 80)
        logger.info(f"STEP 5: Running optimization for {len(enriched_df)} accounts...")
        logger.info("=" * 80)

        # Process all accounts for testing
        # FIXED: Ensure no duplicate account_ids before processing
        resi_corp_df = enriched_df.drop_duplicates(subset=['account_id'], keep='first')
        if len(enriched_df) > len(resi_corp_df):
            logger.info(f"Removed {len(enriched_df) - len(resi_corp_df)} duplicate records before processing")

        logger.info(f"Processing {len(resi_corp_df)} unique accounts for assignment")

        assignments = {}

        # Check if we should use batch or single processing
        if len(resi_corp_df) == 1:
            # Single account - use optimized best fit
            logger.info("Processing single account with optimized best fit")
            account = resi_corp_df.iloc[0]
            csm, score = automation.assign_single_account_optimized(account, csm_books)
            if csm:
                assignments[account['account_id']] = csm
                # Update the csm_books for tracking
                csm_books[csm]['count'] += 1
                csm_books[csm]['total_neediness'] += account.get('neediness_score', 0)
        else:
            # Multiple accounts - use PuLP optimization for batch
            logger.info(f"Processing {len(resi_corp_df)} accounts with PuLP batch optimization")
            assignments = automation.optimize_batch_with_pulp(resi_corp_df, csm_books)

        if assignments:
            logger.info(f"\n" + "=" * 50)
            logger.info(f"ASSIGNMENT RESULTS:")
            for account_id, csm in assignments.items():
                account_data = resi_corp_df[resi_corp_df['account_id'] == account_id].iloc[0]
                logger.info(f"  Account {account_id} (Neediness: {account_data['neediness_score']}) -> CSM {csm}")
            logger.info(f"=" * 50)

            # Get CSM details for all assigned CSMs
            assigned_csms = set(assignments.values())
            logger.info(f"\nAssigned CSM Details:")
            for csm in assigned_csms:
                csm_info = csm_books.get(csm, {})
                accounts_assigned = list(assignments.values()).count(csm)
                logger.info(f"\n  CSM: {csm}")
                logger.info(f"    Current Accounts: {csm_info.get('count', 0)}")
                logger.info(f"    New Assignments: {accounts_assigned}")
                logger.info(f"    After Assignment: {csm_info.get('count', 0) + accounts_assigned}")
                logger.info(f"    Tenure: {csm_info.get('tenure_category', 'Unknown')} ({csm_info.get('tenure_months', 0)} months)")
                logger.info(f"    Health Distribution: Red={csm_info.get('health_distribution', {}).get('Red', 0)}, Yellow={csm_info.get('health_distribution', {}).get('Yellow', 0)}, Green={csm_info.get('health_distribution', {}).get('Green', 0)}")

            # LLM Review (if available)
            if automation.claude_client:
                logger.info("\n" + "=" * 80)
                logger.info("STEP 6: Getting LLM review of assignment...")
                logger.info("=" * 80)

                # Keep track of original assignments for comparison
                original_assignments = assignments.copy()

                should_rerun, llm_feedback, revised_assignments = automation.review_assignments_with_llm(
                    assignments, resi_corp_df, csm_books
                )

                logger.info(f"\nLLM Review Result:")
                logger.info(f"  Approved: {'No - Suggests Rerun' if should_rerun else 'Yes'}")
                logger.info(f"  Feedback: {llm_feedback}")

                if should_rerun:
                    logger.warning("LLM suggests reconsidering this assignment")

                    # Check if LLM provided revised assignments
                    if revised_assignments and revised_assignments != assignments:
                        logger.info("\n" + "=" * 80)
                        logger.info("STEP 6b: Applying LLM's revised assignments...")
                        logger.info("=" * 80)

                        # Get run_id for tracking
                        import datetime
                        run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

                        # Log the changes and update recommendations
                        for account_id, new_csm in revised_assignments.items():
                            old_csm = assignments.get(account_id)
                            if old_csm != new_csm:
                                logger.info(f"  Reassigning {account_id}: {old_csm} -> {new_csm}")

                                # Update the recommendation in the database
                                automation.update_recommendation_after_llm(
                                    account_id=account_id,
                                    new_csm=new_csm,
                                    original_csm=old_csm,
                                    llm_feedback=llm_feedback,
                                    run_id=run_id
                                )

                        # Use the revised assignments
                        assignments = revised_assignments
                        logger.info("✓ Applied LLM's recommended reassignments")
                    else:
                        # LLM rejected but didn't provide alternatives - rerun optimization
                        logger.info("\n" + "=" * 80)
                        logger.info("STEP 6b: Rerunning optimization with adjusted parameters...")
                        logger.info("=" * 80)

                        # Exclude CSMs that were problematic
                        excluded_csms = []
                        if 'Riley Bond' in [csm for csm in assignments.values()]:
                            excluded_csms.append('Riley Bond')
                            logger.info(f"  Excluding Riley Bond (new CSM with too many assignments)")

                        # Rerun optimization with exclusions
                        if len(resi_corp_df) == 1:
                            account = resi_corp_df.iloc[0]
                            csm, score = automation.assign_single_account_optimized(
                                account, csm_books, excluded_csms=excluded_csms
                            )
                            if csm:
                                assignments = {account['account_id']: csm}
                        else:
                            assignments = automation.optimize_batch_with_pulp(
                                resi_corp_df, csm_books, excluded_csms=excluded_csms
                            )

                        logger.info(f"✓ Reoptimized assignments completed")

                        # Re-review with LLM (one more time)
                        should_rerun2, llm_feedback, _ = automation.review_assignments_with_llm(
                            assignments, resi_corp_df, csm_books
                        )

                        if should_rerun2:
                            logger.error("LLM still not satisfied after reoptimization. Manual review required.")
                            return False
                else:
                    # LLM approved - store final recommendations with LLM approval
                    logger.info("LLM approved the assignments")
                    import datetime
                    run_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

                    # Store approved recommendations
                    for account_id, csm in assignments.items():
                        account_data = resi_corp_df[resi_corp_df['account_id'] == account_id].iloc[0]
                        automation.store_recommendation(
                            account_id=account_id,
                            csm_name=csm,
                            account_data=account_data,
                            optimization_score=100,  # High score for LLM approved
                            method='llm_approved',
                            run_id=f"{run_id}_approved",
                            batch_size=len(assignments),
                            llm_feedback="LLM approved assignment"
                        )
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
            logger.error(f"No eligible CSM found for any accounts")
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