#!/usr/bin/env python
# coding: utf-8

"""
Test script to create a minimal working cache with just the accounts that need CSM
"""

import logging
import pandas as pd
from csm_routing_automation import CSMRoutingAutomation
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_minimal_cache():
    """Create minimal cache for accounts needing CSM"""

    logger.info("Creating minimal cache for testing...")

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
        # Simple query to get accounts needing CSM with basic real data
        query = """
        WITH needs_csm AS (
            SELECT DISTINCT
                account_id_ob as account_id,
                tenant_id,
                tenant_name
            FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
            WHERE success_transition_status_ob = 'Needs CSM'
                AND account_id_ob IS NOT NULL
            LIMIT 100
        )
        SELECT
            nc.account_id,
            nc.tenant_id,
            nc.tenant_name,

            -- Get some real variation in values
            CASE
                WHEN MOD(HASH(nc.account_id), 3) = 0 THEN 'Red'
                WHEN MOD(HASH(nc.account_id), 3) = 1 THEN 'Yellow'
                ELSE 'Green'
            END as health_segment,

            CASE
                WHEN MOD(HASH(nc.account_id), 3) = 0 THEN 45
                WHEN MOD(HASH(nc.account_id), 3) = 1 THEN 70
                ELSE 85
            END as health_score,

            -- Varied neediness scores
            CASE
                WHEN MOD(HASH(nc.account_id), 3) = 0 THEN 8  -- Red = High neediness
                WHEN MOD(HASH(nc.account_id), 3) = 1 THEN 5  -- Yellow = Medium
                ELSE 3  -- Green = Low
            END as neediness_score,

            -- Varied revenue
            CASE
                WHEN MOD(HASH(nc.account_id), 4) = 0 THEN 500000
                WHEN MOD(HASH(nc.account_id), 4) = 1 THEN 250000
                WHEN MOD(HASH(nc.account_id), 4) = 2 THEN 100000
                ELSE 50000
            END as revenue,

            -- Mix of segments
            CASE
                WHEN MOD(HASH(nc.account_id), 2) = 0 THEN 'Residential'
                ELSE 'Commercial'
            END as segment,

            -- Mix of levels
            CASE
                WHEN MOD(HASH(nc.account_id), 3) = 0 THEN 'Enterprise'
                WHEN MOD(HASH(nc.account_id), 3) = 1 THEN 'Corporate'
                ELSE 'SMB'
            END as account_level,

            -- Other fields with defaults
            0 as tad_score,
            5 as tech_count,
            'Roofing' as industry,
            'Not at risk' as churn_stage,
            'US/Pacific' as timezone,
            0 as is_parent_account,
            0 as related_tenants

        FROM needs_csm nc
        """

        logger.info("Executing test query for cache...")
        df = automation.execute_query(query)

        if df.empty:
            logger.error("Query returned no data")
            return False

        logger.info(f"Retrieved data for {len(df)} accounts")

        # Standardize column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Show data distribution
        logger.info("\nData Distribution:")
        if 'health_segment' in df.columns:
            logger.info("Health Segments:")
            logger.info(df['health_segment'].value_counts().to_string())

        if 'neediness_score' in df.columns:
            logger.info("\nNeediness Scores:")
            logger.info(df['neediness_score'].value_counts().to_string())

        if 'segment' in df.columns:
            logger.info("\nSegments:")
            logger.info(df['segment'].value_counts().to_string())

        # Save to CSV
        df.to_csv("neediness_cache_latest.csv", index=False)
        logger.info(f"\n✅ Saved test cache to neediness_cache_latest.csv")

        # Save metadata
        import json
        metadata = {
            'generated_at': datetime.now().strftime("%Y%m%d_%H%M%S"),
            'total_accounts': len(df),
            'type': 'test_data_with_variation',
            'note': 'Using hash-based variation for testing real optimization'
        }

        with open('neediness_cache_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

        automation.snowflake_conn.close()
        return True

    except Exception as e:
        logger.error(f"Error creating cache: {str(e)}")
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
        return False

if __name__ == "__main__":
    success = create_minimal_cache()

    if success:
        logger.info("\n✅ Test cache created successfully")
        logger.info("The cache now has varied data for testing real optimization")
    else:
        logger.error("\n❌ Cache creation failed")