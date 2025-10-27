#!/usr/bin/env python3
"""
Analyze validation metrics to show before/after breakdown of health segments for CSMs
"""

import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'APP_ANALYTICS'),
        database='DSV_WAREHOUSE',
        schema='DATA_SCIENCE'
    )

def analyze_csm_metrics():
    """Analyze CSM metrics before and after assignments"""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 80)
    print("CSM ROUTING VALIDATION METRICS REPORT")
    print("=" * 80)
    print(f"Report Generated: {datetime.now()}")
    print()

    # Get recent assignments from both tables
    print("1. RECENT CSM ASSIGNMENT ACTIVITY (Last 24 hours)")
    print("-" * 60)

    query = """
    WITH recent_activity AS (
        -- Get recent recommendations
        SELECT
            recommended_csm as csm_name,
            account_id,
            health_segment,
            neediness_score,
            recommendation_timestamp as timestamp,
            'Recommendation' as source
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE recommendation_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())

        UNION ALL

        -- Get recent actual assignments
        SELECT
            csm_name,
            account_id,
            health_segment,
            neediness_score,
            assignment_date as timestamp,
            'Assignment' as source
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
        WHERE assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    )
    SELECT
        csm_name,
        COUNT(DISTINCT account_id) as accounts_assigned,
        AVG(neediness_score) as avg_neediness,
        SUM(CASE WHEN health_segment = 'Red' THEN 1 ELSE 0 END) as red_accounts,
        SUM(CASE WHEN health_segment = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
        SUM(CASE WHEN health_segment = 'Green' THEN 1 ELSE 0 END) as green_accounts
    FROM recent_activity
    GROUP BY csm_name
    ORDER BY accounts_assigned DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        print(f"{'CSM Name':<25} {'Accounts':<10} {'Avg Need':<10} {'Red':<8} {'Yellow':<8} {'Green':<8}")
        print("-" * 70)
        for row in results:
            csm, accounts, avg_need, red, yellow, green = row
            print(f"{csm:<25} {accounts:<10} {avg_need:<10.1f} {red:<8} {yellow:<8} {green:<8}")
    else:
        print("No recent assignments found")

    print()

    # Get current CSM book distribution
    print("2. CURRENT CSM BOOK HEALTH DISTRIBUTION")
    print("-" * 60)

    query = """
    WITH csm_books AS (
        SELECT
            responsible_csm as csm_name,
            COUNT(*) as total_accounts,
            AVG(NEEDINESS_SCORE) as avg_neediness,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Red' THEN 1 ELSE 0 END) as red_accounts,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Green' THEN 1 ELSE 0 END) as green_accounts
        FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
        WHERE REAL_ESTATE_MARKET = 'Residential'
            AND MANAGEMENT_LEVEL = 'Corporate Accounts'
            AND responsible_csm IS NOT NULL
            AND responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
        GROUP BY responsible_csm
        HAVING COUNT(*) >= 5
    )
    SELECT
        csm_name,
        total_accounts,
        avg_neediness,
        red_accounts,
        yellow_accounts,
        green_accounts,
        ROUND(100.0 * red_accounts / total_accounts, 1) as red_pct,
        ROUND(100.0 * yellow_accounts / total_accounts, 1) as yellow_pct,
        ROUND(100.0 * green_accounts / total_accounts, 1) as green_pct
    FROM csm_books
    ORDER BY total_accounts DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        print(f"{'CSM Name':<25} {'Total':<8} {'Avg Need':<10} {'Red %':<8} {'Yel %':<8} {'Grn %':<8}")
        print("-" * 70)

        total_red = 0
        total_yellow = 0
        total_green = 0
        total_accounts = 0

        for row in results:
            csm, total, avg_need, red, yellow, green, red_pct, yellow_pct, green_pct = row
            print(f"{csm:<25} {total:<8} {avg_need:<10.2f} {red_pct:<8.1f} {yellow_pct:<8.1f} {green_pct:<8.1f}")
            total_red += red
            total_yellow += yellow
            total_green += green
            total_accounts += total

        print("-" * 70)
        print(f"{'TOTALS':<25} {total_accounts:<8} {'---':<10} "
              f"{100.0*total_red/total_accounts:<8.1f} "
              f"{100.0*total_yellow/total_accounts:<8.1f} "
              f"{100.0*total_green/total_accounts:<8.1f}")

    print()

    # Analyze assignment diversity
    print("3. ASSIGNMENT DIVERSITY METRICS")
    print("-" * 60)

    query = """
    WITH last_hour_assignments AS (
        SELECT
            csm_name,
            COUNT(DISTINCT account_id) as accounts
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
        WHERE assignment_date >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        GROUP BY csm_name
    ),
    last_24hr_assignments AS (
        SELECT
            csm_name,
            COUNT(DISTINCT account_id) as accounts
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
        WHERE assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        GROUP BY csm_name
    )
    SELECT
        COALESCE(h1.csm_name, h24.csm_name) as csm_name,
        COALESCE(h1.accounts, 0) as last_hour,
        COALESCE(h24.accounts, 0) as last_24_hours
    FROM last_hour_assignments h1
    FULL OUTER JOIN last_24hr_assignments h24 ON h1.csm_name = h24.csm_name
    ORDER BY last_24_hours DESC, last_hour DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        unique_csms_1hr = sum(1 for r in results if r[1] > 0)
        unique_csms_24hr = sum(1 for r in results if r[2] > 0)

        print(f"Unique CSMs assigned (last hour): {unique_csms_1hr}")
        print(f"Unique CSMs assigned (last 24 hours): {unique_csms_24hr}")
        print()

        print(f"{'CSM Name':<25} {'Last Hour':<12} {'Last 24 Hours':<15}")
        print("-" * 55)
        for csm, h1, h24 in results[:10]:  # Show top 10
            print(f"{csm:<25} {h1:<12} {h24:<15}")

        # Calculate distribution metrics
        assignments = [r[2] for r in results if r[2] > 0]
        if assignments:
            max_assignments = max(assignments)
            avg_assignments = sum(assignments) / len(assignments)
            print()
            print(f"Max assignments per CSM: {max_assignments}")
            print(f"Avg assignments per CSM: {avg_assignments:.1f}")
            print(f"Distribution ratio (max/avg): {max_assignments/avg_assignments:.2f}x")

            if max_assignments/avg_assignments > 3:
                print("⚠️  WARNING: Uneven distribution detected (>3x ratio)")
            else:
                print("✅ Good distribution achieved (<3x ratio)")

    print()
    print("4. KEY VALIDATION METRICS")
    print("-" * 60)

    # Check for Han Pham and Michelle Booth specifically
    query = """
    SELECT
        csm_name,
        COUNT(DISTINCT account_id) as recent_assignments
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    WHERE assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        AND csm_name IN ('Han Pham', 'Michelle Booth')
    GROUP BY csm_name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    print("Critical CSM Check (Problem CSMs from before):")
    for csm, count in results:
        print(f"  - {csm}: {count} assignments in last 24 hours")

    if not results or all(r[1] <= 5 for r in results):
        print("  ✅ Successfully prevented repetitive assignments!")

    cursor.close()
    conn.close()

    print()
    print("=" * 80)
    print("END OF REPORT")
    print("=" * 80)

if __name__ == "__main__":
    analyze_csm_metrics()