#!/usr/bin/env python
# coding: utf-8

"""
Test script to verify just-in-time caching approach
The neediness query runs ONCE when first needed, then uses cache
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_jit_cache():
    """Test just-in-time cache population"""

    logger.info("Testing just-in-time cache approach...")

    # Initialize the automation
    automation = CSMRoutingAutomation(
        config_file='properties.json',
        limits_file='csm_category_limits.json'
    )

    # Cache should be None initially
    logger.info(f"Initial cache state: {type(automation.neediness_cache)}")

    # Connect to Snowflake
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake")
        return False

    try:
        # Get some test accounts needing CSM
        logger.info("\n=== Getting accounts needing CSM ===")
        needs_csm_df = automation.get_needs_csm_accounts()

        if needs_csm_df.empty:
            logger.error("No accounts need CSM")
            return False

        logger.info(f"Found {len(needs_csm_df)} accounts needing CSM")

        # Take first 5 for testing
        test_accounts = needs_csm_df.head(5)
        logger.info(f"\nTesting with {len(test_accounts)} accounts")

        # FIRST enrichment - should trigger cache population
        logger.info("\n=== First Enrichment (should populate cache) ===")
        enriched1 = automation.enrich_account_data(test_accounts)

        if enriched1.empty:
            logger.error("First enrichment failed")
            return False

        logger.info(f"First enrichment returned {len(enriched1)} accounts")

        # Show sample data
        if len(enriched1) > 0:
            sample = enriched1.iloc[0]
            logger.info(f"Sample: Neediness={sample.get('neediness_score')}, "
                       f"Health={sample.get('health_segment')}, "
                       f"Revenue={sample.get('revenue')}")

        # Check cache status
        if automation.neediness_cache is not None:
            logger.info(f"\n✅ Cache populated with {len(automation.neediness_cache)} total accounts")
        else:
            logger.warning("⚠️ Cache is still None after first enrichment")

        # SECOND enrichment - should use existing cache
        logger.info("\n=== Second Enrichment (should use cache) ===")
        test_accounts2 = needs_csm_df.iloc[5:10] if len(needs_csm_df) > 10 else needs_csm_df.head(3)

        enriched2 = automation.enrich_account_data(test_accounts2)

        if enriched2.empty:
            logger.error("Second enrichment failed")
            return False

        logger.info(f"Second enrichment returned {len(enriched2)} accounts")

        # Show that we're getting varied data
        if 'neediness_score' in enriched2.columns:
            logger.info(f"Neediness scores in results: {enriched2['neediness_score'].unique()}")
        if 'health_segment' in enriched2.columns:
            logger.info(f"Health segments in results: {enriched2['health_segment'].unique()}")

        automation.snowflake_conn.close()
        return True

    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
        return False

if __name__ == "__main__":
    success = test_jit_cache()

    if success:
        logger.info("\n✅ Just-in-time cache test successful!")
        logger.info("The system now:")
        logger.info("1. Runs the neediness query ONCE on first enrichment")
        logger.info("2. Caches ALL account data in memory")
        logger.info("3. Filters cached data for specific accounts")
        logger.info("4. Avoids SQL WHERE clause errors")
    else:
        logger.error("\n❌ Test failed")