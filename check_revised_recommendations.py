#!/usr/bin/env python
# coding: utf-8

"""
Script to check both original and revised recommendations
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
    print("CHECKING ALL RECOMMENDATIONS (Original + Revised)")
    print("=" * 80)

    # Get the latest batch of recommendations
    query = """
    SELECT
        account_id,
        recommended_csm,
        assignment_method,
        recommendation_timestamp,
        was_assigned,
        llm_feedback,
        run_id
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    WHERE DATE(recommendation_timestamp) = CURRENT_DATE()
    ORDER BY recommendation_timestamp DESC
    LIMIT 20
    """

    df = automation.execute_query(query)
    if not df.empty:
        # Convert columns to lowercase
        df.columns = [col.lower() for col in df.columns]
        
        print("\nToday's recommendations (showing assignment flow):")
        print(df[['account_id', 'recommended_csm', 'assignment_method', 
                 'was_assigned', 'recommendation_timestamp']].to_string())

        # Show summary
        print("\n" + "=" * 80)
        print("SUMMARY OF FINAL ASSIGNMENTS")
        print("=" * 80)
        
        # Get only the final assignments
        final_query = """
        SELECT DISTINCT
            a.account_id,
            a.csm_name as final_csm,
            r.recommended_csm as last_recommendation,
            r.assignment_method
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE a
        LEFT JOIN (
            SELECT account_id, recommended_csm, assignment_method,
                   ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY recommendation_timestamp DESC) as rn
            FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
            WHERE was_assigned = TRUE
        ) r ON a.account_id = r.account_id AND r.rn = 1
        WHERE DATE(a.assignment_date) = CURRENT_DATE()
        ORDER BY a.assignment_date DESC
        LIMIT 10
        """
        
        final_df = automation.execute_query(final_query)
        if not final_df.empty:
            # Convert columns to lowercase
            final_df.columns = [col.lower() for col in final_df.columns]
            
            print("\nFinal assignments (what's actually in production):")
            for _, row in final_df.iterrows():
                method = row.get('assignment_method', 'unknown')
                if method == 'llm_revised':
                    print(f"  {row['account_id']}: {row['final_csm']} (LLM revised)")
                else:
                    print(f"  {row['account_id']}: {row['final_csm']} ({method})")
                    
        # Show revision tracking
        print("\n" + "=" * 80)
        print("REVISION TRACKING")
        print("=" * 80)
        
        revisions_query = """
        SELECT 
            account_id,
            MIN(CASE WHEN assignment_method NOT LIKE '%revised%' THEN recommended_csm END) as original_csm,
            MAX(CASE WHEN assignment_method = 'llm_revised' THEN recommended_csm END) as revised_csm,
            COUNT(*) as recommendation_count
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE DATE(recommendation_timestamp) = CURRENT_DATE()
        GROUP BY account_id
        HAVING COUNT(*) > 1
        ORDER BY account_id
        """
        
        revisions_df = automation.execute_query(revisions_query)
        if not revisions_df.empty:
            # Convert columns to lowercase
            revisions_df.columns = [col.lower() for col in revisions_df.columns]
            
            print("\nAccounts with revised assignments:")
            for _, row in revisions_df.iterrows():
                print(f"  {row['account_id']}: {row['original_csm']} -> {row['revised_csm']}")
        else:
            print("\nNo revised assignments found")
    else:
        print("\nNo recommendations found for today")

    automation.snowflake_conn.close()
    print("\n✅ Check complete")
else:
    print("Failed to connect to Snowflake")
