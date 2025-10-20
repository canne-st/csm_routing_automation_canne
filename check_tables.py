#!/usr/bin/env python
# coding: utf-8

"""
Check what tables and columns are available
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize the automation
automation = CSMRoutingAutomation(
    config_file='properties.json',
    limits_file='csm_category_limits.json'
)

# Connect to Snowflake
if not automation.connect_snowflake():
    logger.error("Failed to connect to Snowflake")
else:
    # Check what we can query from the onboarding view (which we know works)
    query = """
    SELECT TOP 5
        account_id_ob,
        tenant_id,
        tenant_name,
        success_transition_status_ob
    FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
    WHERE account_id_ob IS NOT NULL
    """

    logger.info("Testing onboarding view...")
    df = automation.execute_query(query)

    if not df.empty:
        logger.info(f"✅ Onboarding view works! Columns: {df.columns.tolist()}")

    # Now test with health data
    query2 = """
    SELECT TOP 5
        account_id,
        core_health_score,
        core_health_score_color
    FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY
    WHERE is_current = TRUE
        AND account_id IS NOT NULL
    """

    logger.info("\nTesting customer history view...")
    df2 = automation.execute_query(query2)

    if not df2.empty:
        logger.info(f"✅ Customer history works! Columns: {df2.columns.tolist()}")
        logger.info(f"Sample health colors: {df2['core_health_score_color'].unique()}")

    automation.snowflake_conn.close()