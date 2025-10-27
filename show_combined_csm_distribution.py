#!/usr/bin/env python3
"""
Show combined CSM distribution - existing portfolios + new assignments
This gives the complete AFTER state of CSM portfolios
"""

import snowflake.connector
from datetime import datetime, timedelta
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
        schema=config["snowflake_ds_schema"],
        role=config["snowflake_role"]
    )

def show_combined_distribution():
    """Show the combined distribution of existing + new assignments"""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 120)
    print("📊 COMBINED CSM DISTRIBUTION (EXISTING PORTFOLIOS + NEW ASSIGNMENTS)")
    print("=" * 120)
    print(f"Report Generated: {datetime.now()}\n")

    # First, let's get the new assignments from the last 24 hours
    print("🔍 STEP 1: Getting recent new assignments...")
    new_assignments_query = """
    SELECT
        csm_name,
        COUNT(*) as new_accounts_added
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    WHERE assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY csm_name
    """

    cursor.execute(new_assignments_query)
    new_assignments = {row[0]: row[1] for row in cursor.fetchall()}

    print(f"   Found {sum(new_assignments.values())} new assignments across {len(new_assignments)} CSMs\n")

    # Get CSMs' existing portfolio sizes from recommendations table
    print("🔍 STEP 2: Getting CSM portfolio sizes from recommendations...")
    portfolio_query = """
    -- Get unique CSMs and their recent portfolio info from recommendations
    WITH csm_portfolio AS (
        SELECT DISTINCT
            recommended_csm as csm_name,
            existing_accounts
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE recommendation_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            AND existing_accounts IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY recommended_csm ORDER BY recommendation_timestamp DESC) = 1
    )
    SELECT
        csm_name,
        existing_accounts
    FROM csm_portfolio
    WHERE existing_accounts > 0
    ORDER BY existing_accounts DESC
    """

    cursor.execute(portfolio_query)
    portfolio_results = cursor.fetchall()

    # Build the combined view
    combined_data = []

    if portfolio_results:
        print(f"   Found portfolio data for {len(portfolio_results)} CSMs\n")

        total_before = 0
        total_new = 0
        total_after = 0

        for csm_name, existing_accounts in portfolio_results:
            new_added = new_assignments.get(csm_name, 0)
            total_after_csm = existing_accounts + new_added

            # Calculate change percentage
            change_pct = (new_added / existing_accounts * 100) if existing_accounts > 0 else 0

            combined_data.append([
                csm_name,
                existing_accounts,
                f"+{new_added}" if new_added > 0 else "-",
                total_after_csm,
                f"+{change_pct:.1f}%" if new_added > 0 else "-"
            ])

            total_before += existing_accounts
            total_new += new_added
            total_after += total_after_csm

        # Sort by total after (descending)
        combined_data.sort(key=lambda x: x[3], reverse=True)

        # Display the main table
        print("📊 COMBINED PORTFOLIO VIEW (BEFORE + NEW = AFTER):")
        print("=" * 100)
        headers = ["CSM Name", "Before", "New Added", "After Total", "Change %"]
        print(tabulate(combined_data[:30], headers=headers, tablefmt="grid"))  # Show top 30

        if len(combined_data) > 30:
            print(f"\n... and {len(combined_data) - 30} more CSMs")

        # Summary statistics
        print("\n" + "=" * 100)
        print("📈 SUMMARY STATISTICS:")
        print("=" * 100)
        print(f"  Total CSMs: {len(combined_data)}")
        print(f"  Total Accounts BEFORE: {total_before}")
        print(f"  Total NEW Assignments: {total_new}")
        print(f"  Total Accounts AFTER: {total_after}")
        print(f"  Overall Growth: +{total_new/total_before*100:.1f}%")

        # Distribution analysis
        after_totals = [row[3] for row in combined_data]
        avg_after = sum(after_totals) / len(after_totals)
        max_after = max(after_totals)
        min_after = min(after_totals)

        print(f"\n📊 DISTRIBUTION METRICS (AFTER):")
        print(f"  Average accounts per CSM: {avg_after:.1f}")
        print(f"  Maximum accounts (any CSM): {max_after}")
        print(f"  Minimum accounts (any CSM): {min_after}")
        print(f"  Distribution ratio (max/avg): {max_after/avg_after:.2f}x")

        # Check CSMs with highest loads
        print(f"\n🔝 TOP 10 CSMs BY TOTAL ACCOUNTS (AFTER):")
        for i, row in enumerate(combined_data[:10], 1):
            csm, before, new, after, change = row
            print(f"  {i:2}. {csm:<25} {after:3} accounts (was {before}, {new} added)")

        # Check our problem CSMs
        print(f"\n🔍 CHECKING SPECIFIC CSMs:")
        problem_csms = ['Han Pham', 'Michelle Booth']
        for row in combined_data:
            if row[0] in problem_csms:
                csm, before, new, after, change = row
                print(f"  {csm:<20} {after:3} accounts (was {before}, {new} added) - {change}")

    else:
        print("❌ No portfolio data found in recommendations table")
        print("\nTrying alternative approach using just new assignments...")

        # Just show the new assignments if we can't get portfolio data
        if new_assignments:
            print("\n📋 NEW ASSIGNMENTS ONLY (couldn't retrieve existing portfolio data):")
            headers = ["CSM Name", "New Accounts Added"]
            table_data = [[csm, count] for csm, count in sorted(new_assignments.items(), key=lambda x: x[1], reverse=True)]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))

    # Also try to get health segment distribution if possible
    print("\n" + "=" * 120)
    print("📊 HEALTH SEGMENT ANALYSIS (from recommendations table):")
    print("=" * 120)

    health_query = """
    SELECT
        recommended_csm as csm_name,
        health_segment,
        COUNT(*) as count
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    WHERE recommendation_timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        AND health_segment IS NOT NULL
    GROUP BY recommended_csm, health_segment
    ORDER BY recommended_csm, health_segment
    """

    cursor.execute(health_query)
    health_results = cursor.fetchall()

    if health_results:
        # Organize by CSM
        csm_health = {}
        for csm, health, count in health_results:
            if csm not in csm_health:
                csm_health[csm] = {'Red': 0, 'Yellow': 0, 'Green': 0}
            csm_health[csm][health] = count

        # Create table
        health_table = []
        total_red = 0
        total_yellow = 0
        total_green = 0

        for csm, health_counts in sorted(csm_health.items()):
            total = sum(health_counts.values())
            red_pct = 100.0 * health_counts['Red'] / total if total > 0 else 0
            yellow_pct = 100.0 * health_counts['Yellow'] / total if total > 0 else 0
            green_pct = 100.0 * health_counts['Green'] / total if total > 0 else 0

            health_table.append([
                csm,
                total,
                f"🔴 {health_counts['Red']} ({red_pct:.0f}%)",
                f"🟡 {health_counts['Yellow']} ({yellow_pct:.0f}%)",
                f"🟢 {health_counts['Green']} ({green_pct:.0f}%)"
            ])

            total_red += health_counts['Red']
            total_yellow += health_counts['Yellow']
            total_green += health_counts['Green']

        headers = ["CSM", "Total", "Red", "Yellow", "Green"]
        print("New Accounts Health Distribution by CSM:")
        print(tabulate(health_table[:15], headers=headers, tablefmt="grid"))

        if len(health_table) > 15:
            print(f"... and {len(health_table) - 15} more CSMs")

        total_all = total_red + total_yellow + total_green
        if total_all > 0:
            print(f"\nOverall Health Distribution of New Assignments:")
            print(f"  🔴 Red:    {total_red:3} accounts ({100.0*total_red/total_all:.1f}%)")
            print(f"  🟡 Yellow: {total_yellow:3} accounts ({100.0*total_yellow/total_all:.1f}%)")
            print(f"  🟢 Green:  {total_green:3} accounts ({100.0*total_green/total_all:.1f}%)")
    else:
        print("No health segment data found in recommendations")

    cursor.close()
    conn.close()

    print("\n" + "=" * 120)
    print("END OF REPORT")
    print("=" * 120)

if __name__ == "__main__":
    show_combined_distribution()