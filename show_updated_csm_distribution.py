#!/usr/bin/env python3
"""
Show updated CSM distribution after new assignments
Combines existing portfolios with new assignments to show the complete picture
"""

import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import os
import json
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from tabulate import tabulate

def load_config(config_file='properties.json'):
    """Load configuration from JSON file"""
    with open(config_file, 'r') as f:
        return json.load(f)

def get_connection():
    """Create Snowflake connection using properties.json"""
    config = load_config()

    # Decode the private key from the JSON config
    private_key_pem = config["SNOWFLAKE_PRIVATE_KEY"].replace('\\n', '\n').encode()

    private_key_obj = serialization.load_pem_private_key(
        private_key_pem,
        password=None,
        backend=default_backend()
    )

    # Convert to DER format for Snowflake
    private_key = private_key_obj.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        user=config["SNOWFLAKE_USER"],
        private_key=private_key,
        account=config["snowflake_account_prod"],
        warehouse=config["snowflake_warehouse"],
        database=config["snowflake_database"],
        schema=config["snowflake_schema"],
        role=config["snowflake_role"]
    )

def get_updated_distribution(hours_back=24):
    """Get the updated CSM distribution including new assignments"""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 100)
    print("📊 UPDATED CSM DISTRIBUTION AFTER NEW ASSIGNMENTS")
    print("=" * 100)
    print(f"Report Generated: {datetime.now()}")
    print(f"Looking back {hours_back} hours for new assignments\n")

    # Query to get CURRENT portfolios + NEW assignments
    query = f"""
    WITH
    -- Get current CSM portfolios (BEFORE state)
    current_portfolios AS (
        SELECT
            responsible_csm as csm_name,
            account_id,
            CORE_HEALTH_SCORE_COLOR as health_segment,
            'existing' as source
        FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
        WHERE REAL_ESTATE_MARKET = 'Residential'
            AND MANAGEMENT_LEVEL = 'Corporate Accounts'
            AND responsible_csm IS NOT NULL
            AND responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
    ),

    -- Get NEW assignments from the last {hours_back} hours
    new_assignments AS (
        SELECT
            csm_name,
            account_id,
            health_segment,
            'new_assignment' as source,
            assignment_date
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
        WHERE assignment_date >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
    ),

    -- Combine both (new assignments override existing if same account)
    combined_portfolios AS (
        -- Get all existing accounts that were NOT reassigned
        SELECT
            csm_name,
            account_id,
            health_segment,
            source
        FROM current_portfolios cp
        WHERE NOT EXISTS (
            SELECT 1
            FROM new_assignments na
            WHERE na.account_id = cp.account_id
        )

        UNION ALL

        -- Add all new assignments
        SELECT
            csm_name,
            account_id,
            health_segment,
            source
        FROM new_assignments
    ),

    -- Calculate metrics per CSM
    csm_metrics AS (
        SELECT
            csm_name,
            COUNT(DISTINCT account_id) as total_accounts,
            SUM(CASE WHEN health_segment = 'Red' THEN 1 ELSE 0 END) as red_accounts,
            SUM(CASE WHEN health_segment = 'Yellow' THEN 1 ELSE 0 END) as yellow_accounts,
            SUM(CASE WHEN health_segment = 'Green' THEN 1 ELSE 0 END) as green_accounts,
            SUM(CASE WHEN source = 'new_assignment' THEN 1 ELSE 0 END) as new_accounts
        FROM combined_portfolios
        GROUP BY csm_name
    ),

    -- Get BEFORE state for comparison
    before_metrics AS (
        SELECT
            responsible_csm as csm_name,
            COUNT(*) as before_total,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Red' THEN 1 ELSE 0 END) as before_red,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Yellow' THEN 1 ELSE 0 END) as before_yellow,
            SUM(CASE WHEN CORE_HEALTH_SCORE_COLOR = 'Green' THEN 1 ELSE 0 END) as before_green
        FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
        WHERE REAL_ESTATE_MARKET = 'Residential'
            AND MANAGEMENT_LEVEL = 'Corporate Accounts'
            AND responsible_csm IS NOT NULL
            AND responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
        GROUP BY responsible_csm
    )

    -- Join everything together
    SELECT
        COALESCE(cm.csm_name, bm.csm_name) as csm_name,
        COALESCE(cm.total_accounts, 0) as total_accounts,
        COALESCE(cm.red_accounts, 0) as red_accounts,
        COALESCE(cm.yellow_accounts, 0) as yellow_accounts,
        COALESCE(cm.green_accounts, 0) as green_accounts,
        COALESCE(cm.new_accounts, 0) as new_accounts_added,
        COALESCE(bm.before_total, 0) as before_total,
        COALESCE(bm.before_red, 0) as before_red,
        COALESCE(bm.before_yellow, 0) as before_yellow,
        COALESCE(bm.before_green, 0) as before_green
    FROM csm_metrics cm
    FULL OUTER JOIN before_metrics bm ON cm.csm_name = bm.csm_name
    WHERE COALESCE(cm.total_accounts, bm.before_total, 0) > 0
    ORDER BY cm.new_accounts DESC, cm.total_accounts DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        # Prepare data for the AFTER state table
        after_table_data = []
        for row in results:
            csm_name, total, red, yellow, green, new_added, before_total, b_red, b_yellow, b_green = row

            # Calculate percentages
            red_pct = (100.0 * red / total) if total > 0 else 0
            yellow_pct = (100.0 * yellow / total) if total > 0 else 0
            green_pct = (100.0 * green / total) if total > 0 else 0

            # Add to table with emojis and formatting
            after_table_data.append([
                csm_name,
                total,
                f"🔴 {red} ({red_pct:.0f}%)",
                f"🟡 {yellow} ({yellow_pct:.0f}%)",
                f"🟢 {green} ({green_pct:.0f}%)",
                f"+{new_added}" if new_added > 0 else "-"
            ])

        # Display the AFTER distribution table
        headers = ["CSM", "Total", "Red", "Yellow", "Green", "New Added"]
        print("\n📊 AFTER ASSIGNMENTS - Updated CSM Distribution:")
        print(tabulate(after_table_data, headers=headers, tablefmt="grid"))

        # Calculate totals
        total_accounts = sum(r[1] for r in after_table_data)
        total_red = sum(r[2] for r in results)
        total_yellow = sum(r[3] for r in results)
        total_green = sum(r[4] for r in results)
        total_new = sum(r[5] for r in results)

        print(f"\n📈 SUMMARY:")
        print(f"  Total Accounts: {total_accounts}")
        print(f"  Total New Assignments: {total_new}")
        print(f"  Overall Red %: {100.0*total_red/total_accounts:.1f}%")
        print(f"  Overall Yellow %: {100.0*total_yellow/total_accounts:.1f}%")
        print(f"  Overall Green %: {100.0*total_green/total_accounts:.1f}%")

        # Show CSMs who got the most new assignments
        print("\n🎯 TOP CSMs BY NEW ASSIGNMENTS:")
        top_csms = sorted([(r[0], r[5]) for r in results if r[5] > 0], key=lambda x: x[1], reverse=True)[:10]
        for csm, new_count in top_csms:
            print(f"  • {csm}: +{new_count} new accounts")

        # Show distribution changes
        print("\n📊 DISTRIBUTION CHANGES (CSMs with significant changes):")
        for row in results[:10]:
            csm_name, total, red, yellow, green, new_added, before_total, b_red, b_yellow, b_green = row
            if new_added > 0:
                red_change = red - b_red
                yellow_change = yellow - b_yellow
                green_change = green - b_green
                print(f"\n  {csm_name}:")
                print(f"    Total: {before_total} → {total} (+{total - before_total})")
                print(f"    Red: {b_red} → {red} ({'+' if red_change >= 0 else ''}{red_change})")
                print(f"    Yellow: {b_yellow} → {yellow} ({'+' if yellow_change >= 0 else ''}{yellow_change})")
                print(f"    Green: {b_green} → {green} ({'+' if green_change >= 0 else ''}{green_change})")

    else:
        print("No data found")

    # Also get recent assignment details
    print("\n" + "=" * 100)
    print("📋 RECENT ASSIGNMENT DETAILS (Last 24 hours):")
    print("=" * 100)

    detail_query = f"""
    SELECT
        csm_name,
        account_id,
        health_segment,
        neediness_score,
        assignment_date
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    WHERE assignment_date >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
    ORDER BY assignment_date DESC
    LIMIT 20
    """

    cursor.execute(detail_query)
    detail_results = cursor.fetchall()

    if detail_results:
        detail_data = []
        for row in detail_results:
            csm, account, health, neediness, date = row
            health_icon = "🔴" if health == "Red" else "🟡" if health == "Yellow" else "🟢"
            detail_data.append([
                csm,
                account,
                f"{health_icon} {health}",
                f"{neediness:.2f}" if neediness else "N/A",
                date.strftime("%Y-%m-%d %H:%M")
            ])

        headers = ["CSM", "Account ID", "Health", "Neediness", "Assignment Time"]
        print(tabulate(detail_data, headers=headers, tablefmt="grid"))

    cursor.close()
    conn.close()

    print("\n" + "=" * 100)
    print("END OF REPORT")
    print("=" * 100)

if __name__ == "__main__":
    # Run the analysis
    get_updated_distribution(hours_back=24)