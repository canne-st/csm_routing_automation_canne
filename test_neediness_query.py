#!/usr/bin/env python
# coding: utf-8

"""
Test script to check if the comprehensive neediness scoring query works
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_neediness_query():
    """Test the comprehensive neediness scoring query"""

    logger.info("Testing comprehensive neediness scoring query...")

    # Initialize the automation
    automation = CSMRoutingAutomation(
        config_file='properties.json',
        limits_file='csm_category_limits.json'
    )

    # Connect to Snowflake
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake")
        return False

    try:
        # Get a few test accounts
        test_query = """
        SELECT TOP 2
            account_id_ob as account_id,
            tenant_id,
            success_transition_status_ob
        FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
        WHERE success_transition_status_ob = 'Needs CSM'
            AND account_id_ob IS NOT NULL
        """

        test_accounts = automation.execute_query(test_query)
        test_accounts.columns = [col.lower() for col in test_accounts.columns]

        if test_accounts.empty:
            logger.error("No test accounts found")
            return False

        logger.info(f"Testing with accounts: {test_accounts['account_id'].tolist()}")

        # Test enrichment
        logger.info("\n=== Testing Enrichment ===")
        enriched = automation.enrich_account_data(test_accounts)

        if not enriched.empty:
            logger.info(f"Enrichment returned {len(enriched)} records")
            logger.info(f"Columns: {enriched.columns.tolist()}")

            # Show first account details
            if len(enriched) > 0:
                account = enriched.iloc[0]
                logger.info("\nFirst Account Details:")
                logger.info(f"  Account ID: {account.get('account_id')}")
                logger.info(f"  Neediness Score: {account.get('neediness_score')}")
                logger.info(f"  Health Score: {account.get('health_score')}")
                logger.info(f"  Revenue: {account.get('revenue')}")
                logger.info(f"  Segment: {account.get('segment')}")
                logger.info(f"  Account Level: {account.get('account_level')}")
                logger.info(f"  Industry: {account.get('industry')}")
                logger.info(f"  TAD Score: {account.get('tad_score')}")

                # Check if values are still hardcoded
                if (account.get('neediness_score') == 5 and
                    account.get('health_score') == 70 and
                    account.get('revenue') == 100000):
                    logger.warning("⚠️  Values appear to be hardcoded (neediness=5, health=70, revenue=100000)")
                else:
                    logger.info("✅ Values appear to be from real data!")

        automation.snowflake_conn.close()
        return True

    except Exception as e:
        logger.error(f"Error during test: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_neediness_query()

    if success:
        logger.info("\n✅ Test completed")
    else:
        logger.error("\n❌ Test failed")