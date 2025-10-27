#!/usr/bin/env python3
"""
Get the updated CSM portfolio health distribution after new assignments
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

def get_health_distribution():
    """Get current health distribution for all CSMs including new assignments"""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 120)
    print("📊 CSM PORTFOLIO HEALTH DISTRIBUTION (AFTER NEW ASSIGNMENTS)")
    print("=" * 120)
    print(f"Report Generated: {datetime.now()}\n")

    # Get the combined portfolio with health scores
    query = """
    WITH existing_portfolio AS (
        -- Get current CSM portfolios from the main table
        SELECT
            responsible_csm as csm_name,
            account_id,
            CORE_HEALTH_SCORE_COLOR as health_segment
        FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
        WHERE REAL_ESTATE_MARKET = 'Residential'
            AND MANAGEMENT_LEVEL = 'Corporate Accounts'
            AND responsible_csm IS NOT NULL
            AND responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
            AND LENGTH(account_id) > 0
    ),
    new_assignments AS (
        -- Get recent assignments (last 24 hours)
        SELECT
            a.csm_name,
            a.account_id
        FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE a
        WHERE a.assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    ),
    new_accounts_with_health AS (
        -- Get health scores for newly assigned accounts
        SELECT
            n.csm_name,
            n.account_id,
            s.CORE_HEALTH_SCORE_COLOR as health_segment
        FROM new_assignments n
        LEFT JOIN DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V s
            ON n.account_id = s.account_id
            AND s.REAL_ESTATE_MARKET = 'Residential'
            AND s.MANAGEMENT_LEVEL = 'Corporate Accounts'
    ),
    combined_portfolio AS (
        -- Combine existing and new
        SELECT csm_name, account_id, health_segment FROM existing_portfolio
        UNION ALL
        SELECT csm_name, account_id, health_segment FROM new_accounts_with_health
    ),
    health_counts AS (
        SELECT
            csm_name,
            COUNT(DISTINCT account_id) as total_accounts,
            SUM(CASE WHEN health_segment = 'Red' THEN 1 ELSE 0 END) as red_count,
            SUM(CASE WHEN health_segment = 'Yellow' THEN 1 ELSE 0 END) as yellow_count,
            SUM(CASE WHEN health_segment = 'Green' THEN 1 ELSE 0 END) as green_count,
            SUM(CASE WHEN health_segment IS NULL OR health_segment NOT IN ('Red','Yellow','Green') THEN 1 ELSE 0 END) as unknown_count
        FROM combined_portfolio
        WHERE csm_name IS NOT NULL
        GROUP BY csm_name
    ),
    new_assignment_counts AS (
        SELECT
            csm_name,
            COUNT(*) as new_accounts
        FROM new_assignments
        GROUP BY csm_name
    )
    SELECT
        h.csm_name,
        h.total_accounts,
        COALESCE(n.new_accounts, 0) as new_accounts,
        h.red_count,
        h.yellow_count,
        h.green_count,
        h.unknown_count
    FROM health_counts h
    LEFT JOIN new_assignment_counts n ON h.csm_name = n.csm_name
    ORDER BY h.total_accounts DESC, h.csm_name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        # Create formatted table
        table_data = []
        for row in results:
            csm, total, new, red, yellow, green, unknown = row

            # Calculate percentages
            if total > 0:
                red_pct = (red / total) * 100
                yellow_pct = (yellow / total) * 100
                green_pct = (green / total) * 100
            else:
                red_pct = yellow_pct = green_pct = 0

            # Format the row
            table_data.append([
                csm,
                f"{total} (+{new})" if new > 0 else str(total),
                f"{red} ({red_pct:.1f}%)",
                f"{yellow} ({yellow_pct:.1f}%)",
                f"{green} ({green_pct:.1f}%)"
            ])

        # Display the table
        headers = ["CSM Name", "Total Accounts", "🔴 Red", "🟡 Yellow", "🟢 Green"]
        print(tabulate(table_data[:30], headers=headers, tablefmt="grid"))

        if len(table_data) > 30:
            print(f"\n... and {len(table_data) - 30} more CSMs")

        # Summary statistics
        total_accounts = sum(r[1] for r in results)
        total_new = sum(r[2] for r in results)
        total_red = sum(r[3] for r in results)
        total_yellow = sum(r[4] for r in results)
        total_green = sum(r[5] for r in results)

        print("\n" + "=" * 120)
        print("📈 SUMMARY:")
        print("=" * 120)
        print(f"Total CSMs: {len(results)}")
        print(f"Total Accounts: {total_accounts} (including {total_new} new assignments)")
        print(f"\nOverall Health Distribution:")
        print(f"  🔴 Red:    {total_red:,} ({100*total_red/total_accounts:.1f}%)")
        print(f"  🟡 Yellow: {total_yellow:,} ({100*total_yellow/total_accounts:.1f}%)")
        print(f"  🟢 Green:  {total_green:,} ({100*total_green/total_accounts:.1f}%)")

        # Identify CSMs with best/worst health distributions
        health_data = [(r[0], r[1], r[3], r[4], r[5]) for r in results if r[1] > 50]  # Only CSMs with 50+ accounts
        if health_data:
            # Best green percentage
            best_green = sorted(health_data, key=lambda x: x[4]/x[1] if x[1] > 0 else 0, reverse=True)[:5]
            print(f"\n🏆 Best Health Distributions (Highest Green %):")
            for csm, total, red, yellow, green in best_green:
                print(f"  {csm}: {100*green/total:.1f}% Green ({green}/{total} accounts)")

            # Worst red percentage
            worst_red = sorted(health_data, key=lambda x: x[2]/x[1] if x[1] > 0 else 0, reverse=True)[:5]
            print(f"\n⚠️  Highest Risk Portfolios (Highest Red %):")
            for csm, total, red, yellow, green in worst_red:
                print(f"  {csm}: {100*red/total:.1f}% Red ({red}/{total} accounts)")

    cursor.close()
    conn.close()

    print("\n" + "=" * 120)
    print("END OF REPORT")
    print("=" * 120)

if __name__ == "__main__":
    get_health_distribution()