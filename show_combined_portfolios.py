#!/usr/bin/env python3
"""
Show CSM portfolio distribution combining existing portfolios with new assignments
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

def get_csm_books():
    """Get current CSM book counts from the main accounts table"""
    conn = get_connection()
    cursor = conn.cursor()

    # Get all active CSMs and their current account counts
    query = """
    WITH current_books AS (
        SELECT
            responsible_csm as csm_name,
            COUNT(DISTINCT account_id) as account_count
        FROM DSV_WAREHOUSE.PUBLIC_DATA_SETS.SALESFORCE_ACCOUNT_ALL_W_CHURN_SCORE_V
        WHERE REAL_ESTATE_MARKET = 'Residential'
            AND MANAGEMENT_LEVEL = 'Corporate Accounts'
            AND responsible_csm IS NOT NULL
            AND responsible_csm IN (
                SELECT active_csm
                FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
            )
            AND LENGTH(account_id) > 0
        GROUP BY responsible_csm
    ),
    all_csms AS (
        SELECT active_csm as csm_name
        FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
    )
    SELECT
        a.csm_name,
        COALESCE(c.account_count, 0) as account_count
    FROM all_csms a
    LEFT JOIN current_books c ON a.csm_name = c.csm_name
    ORDER BY account_count DESC, a.csm_name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    conn.close()
    return results

def get_new_assignments():
    """Get new assignments from the last 24 hours"""
    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT
        csm_name,
        COUNT(DISTINCT account_id) as new_accounts
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    WHERE assignment_date >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY csm_name
    """

    cursor.execute(query)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    # Convert to dictionary for easy lookup
    return {csm: count for csm, count in results}

def show_combined_portfolios():
    """Show the combined CSM portfolios including new assignments"""

    print("=" * 120)
    print("📊 CSM PORTFOLIO DISTRIBUTION (INCLUDING NEW ASSIGNMENTS)")
    print("=" * 120)
    print(f"Report Generated: {datetime.now()}\n")

    # Get current book counts
    csm_books = get_csm_books()

    # Get new assignments
    new_assignments = get_new_assignments()

    # Combine the data
    table_data = []
    total_accounts_before = 0
    total_accounts_after = 0
    total_new = 0

    for csm, current_count in csm_books:
        new_count = new_assignments.get(csm, 0)
        after_count = current_count + new_count

        total_accounts_before += current_count
        total_accounts_after += after_count
        total_new += new_count

        # Format the row
        if new_count > 0:
            table_data.append([
                csm,
                current_count,
                f"+{new_count}",
                after_count,
                f"{after_count - current_count:+d}"
            ])
        else:
            table_data.append([
                csm,
                current_count,
                "-",
                current_count,
                "0"
            ])

    # Display the table
    headers = ["CSM Name", "Before", "New Assignments", "After", "Change"]
    print(tabulate(table_data[:30], headers=headers, tablefmt="grid"))

    if len(table_data) > 30:
        print(f"\n... and {len(table_data) - 30} more CSMs")

    # Summary statistics
    print("\n" + "=" * 120)
    print("📈 SUMMARY:")
    print("=" * 120)
    print(f"Total CSMs: {len(csm_books)}")
    print(f"CSMs with new assignments: {len([1 for csm, _ in csm_books if csm in new_assignments])}")
    print(f"Total accounts before: {total_accounts_before:,}")
    print(f"New assignments (last 24h): {total_new:,}")
    print(f"Total accounts after: {total_accounts_after:,}")
    print(f"Average accounts per CSM: {total_accounts_after / len(csm_books):.1f}")

    # Show CSMs with most new assignments
    if new_assignments:
        print("\n🎯 TOP CSMs BY NEW ASSIGNMENTS:")
        sorted_new = sorted(new_assignments.items(), key=lambda x: x[1], reverse=True)[:5]
        for csm, count in sorted_new:
            print(f"  {csm}: +{count} accounts")

    # Show current distribution ranges
    print("\n📊 PORTFOLIO SIZE DISTRIBUTION (AFTER NEW ASSIGNMENTS):")

    # Calculate distribution after new assignments
    combined_counts = []
    for csm, current_count in csm_books:
        new_count = new_assignments.get(csm, 0)
        combined_counts.append(current_count + new_count)

    ranges = [
        (0, 50, "0-50 accounts"),
        (51, 75, "51-75 accounts"),
        (76, 85, "76-85 accounts"),
        (86, 95, "86-95 accounts"),
        (96, 100, "96-100 accounts"),
        (101, float('inf'), "101+ accounts")
    ]

    for min_val, max_val, label in ranges:
        count = len([c for c in combined_counts if min_val <= c <= max_val])
        if count > 0:
            csms_in_range = [csm for csm, curr in csm_books
                            if min_val <= curr + new_assignments.get(csm, 0) <= max_val][:3]
            print(f"  {label}: {count} CSMs (e.g., {', '.join(csms_in_range[:3])}{'...' if len(csms_in_range) > 3 else ''})")

    print("\n" + "=" * 120)
    print("END OF REPORT")
    print("=" * 120)

if __name__ == "__main__":
    show_combined_portfolios()