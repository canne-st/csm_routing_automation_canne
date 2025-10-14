#!/usr/bin/env python
# coding: utf-8

"""
Script to verify the CSM assignments were saved correctly
"""

from csm_routing_automation import CSMRoutingAutomation
import pandas as pd

# Initialize
automation = CSMRoutingAutomation(
    config_file='properties.json',
    limits_file='csm_category_limits.json'
)

# Connect
if automation.connect_snowflake():
    print("\n" + "=" * 80)
    print("CHECKING CSM_ROUTING_RECOMMENDATIONS_CANNE TABLE")
    print("=" * 80)

    # Check recommendations
    query1 = """
    SELECT
        account_id,
        recommended_csm,
        recommendation_timestamp,
        neediness_score,
        optimization_score,
        was_assigned
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    ORDER BY recommendation_timestamp DESC
    LIMIT 5
    """

    df1 = automation.execute_query(query1)
    if not df1.empty:
        print("\nRecent recommendations:")
        print(df1.to_string())
    else:
        print("\nNo recommendations found")

    print("\n" + "=" * 80)
    print("CHECKING ACCOUNT_CSM_ASSIGNMENTS_CANNE TABLE")
    print("=" * 80)

    # Check assignments
    query2 = """
    SELECT
        account_id,
        csm_name,
        assignment_date,
        assignment_method,
        created_by
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    ORDER BY assignment_date DESC
    LIMIT 5
    """

    df2 = automation.execute_query(query2)
    if not df2.empty:
        print("\nRecent assignments:")
        print(df2.to_string())
    else:
        print("\nNo assignments found")

    automation.snowflake_conn.close()
    print("\n✅ Verification complete")
else:
    print("Failed to connect to Snowflake")