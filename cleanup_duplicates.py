#!/usr/bin/env python
# coding: utf-8

"""
Script to clean up duplicate recommendations from CSM_ROUTING_RECOMMENDATIONS_CANNE table
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def cleanup_duplicates():
    """Remove duplicate recommendations from the database"""

    logger.info("Starting duplicate cleanup process...")

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
        # Query to identify duplicates
        check_query = f"""
        WITH duplicate_check AS (
            SELECT
                account_id,
                run_id,
                assignment_method,
                COUNT(*) as duplicate_count,
                MIN(recommendation_id) as keep_id
            FROM {automation.recommendations_table}
            WHERE recommendation_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
            GROUP BY account_id, run_id, assignment_method
            HAVING COUNT(*) > 1
        )
        SELECT
            COUNT(DISTINCT account_id) as duplicate_accounts,
            SUM(duplicate_count - 1) as records_to_remove
        FROM duplicate_check
        """

        cursor = automation.snowflake_conn.cursor()
        cursor.execute(check_query)
        result = cursor.fetchone()

        if result and result[1] and result[1] > 0:
            logger.info(f"Found {result[0]} accounts with duplicates, {result[1]} duplicate records to remove")

            # Delete duplicates keeping the first record for each account/run/method combination
            delete_query = f"""
            DELETE FROM {automation.recommendations_table}
            WHERE recommendation_id IN (
                WITH duplicate_records AS (
                    SELECT
                        recommendation_id,
                        account_id,
                        run_id,
                        assignment_method,
                        ROW_NUMBER() OVER (
                            PARTITION BY account_id, run_id, assignment_method
                            ORDER BY recommendation_id
                        ) as rn
                    FROM {automation.recommendations_table}
                    WHERE recommendation_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
                )
                SELECT recommendation_id
                FROM duplicate_records
                WHERE rn > 1
            )
            """

            cursor.execute(delete_query)
            deleted_count = cursor.rowcount
            automation.snowflake_conn.commit()
            logger.info(f"Successfully deleted {deleted_count} duplicate records")

        else:
            logger.info("No duplicates found in recent recommendations")

        cursor.close()

        # Show current state
        summary_query = f"""
        SELECT
            COUNT(DISTINCT account_id) as unique_accounts,
            COUNT(*) as total_records,
            COUNT(DISTINCT run_id) as unique_runs
        FROM {automation.recommendations_table}
        WHERE recommendation_timestamp >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        """

        cursor = automation.snowflake_conn.cursor()
        cursor.execute(summary_query)
        result = cursor.fetchone()

        if result:
            logger.info(f"Current state: {result[0]} unique accounts, {result[1]} total records, {result[2]} unique runs")

        cursor.close()
        automation.snowflake_conn.close()

        return True

    except Exception as e:
        logger.error(f"Error during cleanup: {str(e)}")
        return False

if __name__ == "__main__":
    success = cleanup_duplicates()

    if success:
        logger.info("Cleanup completed successfully")
    else:
        logger.error("Cleanup failed")