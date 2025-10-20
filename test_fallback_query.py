#!/usr/bin/env python
# coding: utf-8

"""
Test the fallback query directly
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_fallback():
    # Initialize the automation
    automation = CSMRoutingAutomation(
        config_file='properties.json',
        limits_file='csm_category_limits.json'
    )

    # Connect to Snowflake
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake")
        return False

    # Test the fallback query
    query = """
    SELECT TOP 100
        a.account_id,
        a.account_name,
        a.tenant_id,
        70 as health_score,
        'Yellow' as health_segment,
        100000 as revenue,
        5 as neediness_score,
        'Residential' as segment,
        'Corporate' as account_level,
        'Roofing' as industry,
        0 as tad_score,
        5 as tech_count,
        'Medium' as neediness_category
    FROM DSV_WAREHOUSE.PUBLIC.VW_SALESFORCE_ACCOUNT a
    WHERE a.account_id IS NOT NULL
    """

    logger.info("Testing basic fallback query...")
    df = automation.execute_query(query)

    if not df.empty:
        logger.info(f"✅ Query successful! Retrieved {len(df)} accounts")
        logger.info(f"Columns: {df.columns.tolist()}")
        return True
    else:
        logger.error("Query returned no data")
        return False

if __name__ == "__main__":
    test_fallback()