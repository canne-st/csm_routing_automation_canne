#!/usr/bin/env python
# coding: utf-8

"""
Script to check for duplicate recommendations in CSM_ROUTING_RECOMMENDATIONS_CANNE table
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_duplicates():
    """Check for duplicate recommendations in the database"""

    # Initialize the automation
    automation = CSMRoutingAutomation(
        config_file='properties.json',
        limits_file='csm_category_limits.json'
    )

    # Connect to Snowflake
    if not automation.connect_snowflake():
        logger.error("Failed to connect to Snowflake")
        return

    try:
        cursor = automation.snowflake_conn.cursor()

        # Check for duplicates in the last 5 minutes (from most recent test)
        query = f"""
        WITH recent_records AS (
            SELECT
                recommendation_id,
                account_id,
                recommended_csm,
                recommendation_timestamp,
                assignment_method,
                run_id,
                neediness_score
            FROM {automation.recommendations_table}
            WHERE recommendation_timestamp >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
            ORDER BY recommendation_timestamp DESC
        ),
        duplicate_check AS (
            SELECT
                account_id,
                COUNT(*) as record_count,
                COUNT(DISTINCT recommended_csm) as unique_csms,
                COUNT(DISTINCT run_id) as unique_runs,
                LISTAGG(DISTINCT recommended_csm, ', ') as all_csms,
                MIN(recommendation_timestamp) as first_rec,
                MAX(recommendation_timestamp) as last_rec
            FROM recent_records
            GROUP BY account_id
        )
        SELECT * FROM duplicate_check
        ORDER BY record_count DESC, account_id
        """

        cursor.execute(query)
        results = cursor.fetchall()

        logger.info("\n=== Duplicate Check Results (Last 5 minutes) ===")

        duplicates_found = False
        for row in results:
            account_id, count, unique_csms, unique_runs, csms, first, last = row
            if count > 1:
                duplicates_found = True
                logger.warning(f"Account {account_id}: {count} records, {unique_csms} unique CSMs ({csms})")
            else:
                logger.info(f"Account {account_id}: {count} record, CSM: {csms}")

        if not duplicates_found:
            logger.info("✅ No duplicates found! All accounts have single records.")

        # Show summary statistics
        summary_query = f"""
        SELECT
            COUNT(DISTINCT account_id) as unique_accounts,
            COUNT(*) as total_records,
            COUNT(DISTINCT run_id) as unique_runs,
            MIN(recommendation_timestamp) as earliest,
            MAX(recommendation_timestamp) as latest
        FROM {automation.recommendations_table}
        WHERE recommendation_timestamp >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        """

        cursor.execute(summary_query)
        summary = cursor.fetchone()

        if summary:
            logger.info(f"\n=== Summary Statistics ===")
            logger.info(f"Unique accounts: {summary[0]}")
            logger.info(f"Total records: {summary[1]}")
            logger.info(f"Unique runs: {summary[2]}")
            logger.info(f"Time range: {summary[3]} to {summary[4]}")

            if summary[0] == summary[1]:
                logger.info("✅ Record count matches unique account count - no duplicates!")

        cursor.close()
        automation.snowflake_conn.close()

    except Exception as e:
        logger.error(f"Error checking duplicates: {str(e)}")

if __name__ == "__main__":
    check_duplicates()