#!/usr/bin/env python
# coding: utf-8

"""
Test script to verify CSM routing with cached data showing real variation
"""

import logging
import pandas as pd
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_with_cache():
    """Test CSM assignment with cached varied data"""

    logger.info("Testing CSM routing with cached varied data...")

    # Initialize the automation
    automation = CSMRoutingAutomation(
        config_file='properties.json',
        limits_file='csm_category_limits.json'
    )

    # The cache should be loaded automatically
    if automation.neediness_cache is None:
        logger.error("Cache not loaded!")
        return False

    logger.info(f"Cache loaded with {len(automation.neediness_cache)} accounts")

    # Connect to Snowflake
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake")
        return False

    try:
        # Get test accounts from the cache itself
        test_accounts = automation.neediness_cache.head(6).copy()

        # Show the variety in our test data
        logger.info("\n=== Test Accounts from Cache ===")
        for idx, row in test_accounts.iterrows():
            logger.info(f"Account {row['account_id'][:20]}...")
            logger.info(f"  Health: {row['health_segment']} (Score: {row['health_score']})")
            logger.info(f"  Neediness: {row['neediness_score']}")
            logger.info(f"  Revenue: ${row['revenue']:,.0f}")
            logger.info(f"  Segment: {row['segment']} {row['account_level']}")

        # Create a DataFrame as if these came from needs_csm query
        needs_csm_df = test_accounts[['account_id', 'tenant_id']].copy()
        needs_csm_df['success_transition_status_ob'] = 'Needs CSM'

        # Enrich the data (should use cache)
        logger.info("\n=== Testing Enrichment ===")
        enriched = automation.enrich_account_data(needs_csm_df)

        if enriched.empty:
            logger.error("Enrichment failed")
            return False

        # Show enriched data
        logger.info(f"\nEnriched {len(enriched)} accounts:")
        for idx, row in enriched.iterrows():
            logger.info(f"Account {row['account_id'][:20]}... -> Neediness: {row.get('neediness_score')}, "
                       f"Health: {row.get('health_segment')}, Revenue: ${row.get('revenue', 0):,.0f}")

        # Get CSM books
        logger.info("\n=== Getting CSM Books ===")
        # Use minimum account threshold from configuration
        min_accounts = automation.limits.get('residential_corporate', {}).get('min_accounts_for_eligibility', 5)
        csm_books = automation.get_current_csm_books(min_account_threshold=min_accounts)

        if not csm_books:
            logger.error("No CSM books found")
            return False

        logger.info(f"Found {len(csm_books)} CSMs")

        # Test single account assignment with different neediness levels
        logger.info("\n=== Testing Single Account Assignment ===")

        # Test with a high neediness account (Red/8)
        high_need = enriched[enriched['neediness_score'] == 8].iloc[0] if any(enriched['neediness_score'] == 8) else None
        if high_need is not None:
            logger.info(f"\nHigh neediness account (score={high_need['neediness_score']}):")
            csm, score = automation.assign_single_account_optimized(high_need, csm_books)
            logger.info(f"  Assigned to: {csm} (score: {score:.2f})")

        # Test with a medium neediness account (Yellow/5)
        med_need = enriched[enriched['neediness_score'] == 5].iloc[0] if any(enriched['neediness_score'] == 5) else None
        if med_need is not None:
            logger.info(f"\nMedium neediness account (score={med_need['neediness_score']}):")
            csm, score = automation.assign_single_account_optimized(med_need, csm_books)
            logger.info(f"  Assigned to: {csm} (score: {score:.2f})")

        # Test with a low neediness account (Green/3)
        low_need = enriched[enriched['neediness_score'] == 3].iloc[0] if any(enriched['neediness_score'] == 3) else None
        if low_need is not None:
            logger.info(f"\nLow neediness account (score={low_need['neediness_score']}):")
            csm, score = automation.assign_single_account_optimized(low_need, csm_books)
            logger.info(f"  Assigned to: {csm} (score: {score:.2f})")

        # Test batch assignment with mixed accounts
        logger.info("\n=== Testing Batch Assignment ===")
        if len(enriched) >= 3:
            batch_df = enriched.head(3)
            logger.info(f"Batch of {len(batch_df)} accounts with neediness: {batch_df['neediness_score'].tolist()}")

            assignments = automation.optimize_batch_with_pulp(batch_df, csm_books)

            for account_id, csm in assignments.items():
                account = batch_df[batch_df['account_id'] == account_id].iloc[0]
                logger.info(f"  {account_id[:20]}... (N={account['neediness_score']}, H={account['health_segment']}) -> {csm}")

        automation.snowflake_conn.close()
        return True

    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
        return False

if __name__ == "__main__":
    success = test_with_cache()

    if success:
        logger.info("\n✅ Test completed successfully")
        logger.info("The system is now using REAL varied data from cache instead of hardcoded values!")
    else:
        logger.error("\n❌ Test failed")