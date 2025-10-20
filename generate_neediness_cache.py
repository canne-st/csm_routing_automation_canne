#!/usr/bin/env python
# coding: utf-8

"""
Script to generate and cache neediness scoring data for all accounts
Run this once to create a CSV cache of all account neediness data
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

def generate_neediness_cache():
    """Generate neediness cache for all accounts"""

    logger.info("Starting neediness data cache generation...")

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
        # Try the simple query first, fall back to comprehensive if needed
        import os
        if os.path.exists('neediness_scoring_simple.sql'):
            logger.info("Using simplified neediness query")
            with open('neediness_scoring_simple.sql', 'r') as f:
                neediness_query = f.read()
        else:
            logger.info("Using comprehensive neediness query")
            with open('neediness_scoring_query.sql', 'r') as f:
                neediness_query = f.read()

        logger.info("Executing comprehensive neediness query for ALL accounts...")
        logger.info("This may take several minutes...")

        # Execute the full query
        start_time = datetime.now()
        df = automation.execute_query(neediness_query)
        end_time = datetime.now()

        if df.empty:
            logger.error("Query returned no data")
            return False

        logger.info(f"Query completed in {(end_time - start_time).total_seconds():.2f} seconds")
        logger.info(f"Retrieved data for {len(df)} accounts")

        # Standardize column names
        df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

        # Show sample data
        logger.info("\nSample data (first 5 accounts):")
        sample_cols = ['account_id', 'neediness_score', 'health_score', 'revenue',
                      'segment', 'account_level', 'tad_score']

        # Only show columns that exist
        existing_cols = [col for col in sample_cols if col in df.columns]
        if existing_cols and len(df) > 0:
            logger.info(df[existing_cols].head().to_string())

        # Check data quality
        logger.info("\nData quality check:")
        if 'neediness_score' in df.columns:
            logger.info(f"  Neediness Score - Min: {df['neediness_score'].min()}, "
                       f"Max: {df['neediness_score'].max()}, "
                       f"Mean: {df['neediness_score'].mean():.2f}")

        if 'health_score' in df.columns:
            logger.info(f"  Health Score - Min: {df['health_score'].min()}, "
                       f"Max: {df['health_score'].max()}, "
                       f"Mean: {df['health_score'].mean():.2f}")

        if 'revenue' in df.columns:
            logger.info(f"  Revenue - Min: ${df['revenue'].min():,.0f}, "
                       f"Max: ${df['revenue'].max():,.0f}, "
                       f"Mean: ${df['revenue'].mean():,.0f}")

        # Save to CSV with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"neediness_cache_{timestamp}.csv"
        df.to_csv(filename, index=False)
        logger.info(f"\n✅ Saved neediness data to {filename}")

        # Also save as latest version for easy access
        df.to_csv("neediness_cache_latest.csv", index=False)
        logger.info(f"✅ Also saved as neediness_cache_latest.csv")

        # Save metadata
        metadata = {
            'generated_at': timestamp,
            'total_accounts': len(df),
            'columns': list(df.columns),
            'file_name': filename
        }

        import json
        with open('neediness_cache_metadata.json', 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"✅ Saved metadata to neediness_cache_metadata.json")

        automation.snowflake_conn.close()
        return True

    except Exception as e:
        logger.error(f"Error generating cache: {str(e)}")
        if automation.snowflake_conn:
            automation.snowflake_conn.close()
        return False

if __name__ == "__main__":
    success = generate_neediness_cache()

    if success:
        logger.info("\n✅ Cache generation completed successfully")
        logger.info("You can now use neediness_cache_latest.csv for enrichment")
    else:
        logger.error("\n❌ Cache generation failed")