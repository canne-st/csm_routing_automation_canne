#!/usr/bin/env python
# coding: utf-8

"""
Script to show detailed recent records from CSM_ROUTING_RECOMMENDATIONS_CANNE table
"""

import logging
from csm_routing_automation import CSMRoutingAutomation

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_recent_records():
    """Show detailed recent records"""

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

        # Get detailed records from the last 5 minutes
        query = f"""
        SELECT
            account_id,
            recommended_csm,
            assignment_method,
            run_id,
            recommendation_timestamp,
            was_assigned,
            llm_feedback
        FROM {automation.recommendations_table}
        WHERE recommendation_timestamp >= DATEADD(minute, -5, CURRENT_TIMESTAMP())
        ORDER BY account_id, recommendation_timestamp
        """

        cursor.execute(query)
        results = cursor.fetchall()

        logger.info("\n=== Recent Records (Last 5 minutes) ===")

        current_account = None
        for row in results:
            account_id, csm, method, run_id, timestamp, assigned, feedback = row

            if account_id != current_account:
                logger.info(f"\nAccount: {account_id}")
                current_account = account_id

            logger.info(f"  - {timestamp}: {csm} ({method}) - Run: {run_id[-10:]}, Assigned: {assigned}")
            if feedback:
                logger.info(f"    LLM Feedback: {feedback[:100]}...")

        cursor.close()
        automation.snowflake_conn.close()

    except Exception as e:
        logger.error(f"Error checking records: {str(e)}")

if __name__ == "__main__":
    check_recent_records()