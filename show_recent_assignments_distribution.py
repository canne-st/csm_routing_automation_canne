#!/usr/bin/env python3
"""
Show recent CSM assignments distribution
Focuses on what was actually assigned from ACCOUNT_CSM_ASSIGNMENTS_CANNE table
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
        schema=config["snowflake_ds_schema"],  # Use DATA_SCIENCE schema
        role=config["snowflake_role"]
    )

def show_recent_assignments(hours_back=24):
    """Show recent assignments distribution"""
    conn = get_connection()
    cursor = conn.cursor()

    print("=" * 100)
    print("📊 RECENT CSM ASSIGNMENTS DISTRIBUTION")
    print("=" * 100)
    print(f"Report Generated: {datetime.now()}")
    print(f"Looking back {hours_back} hours for new assignments\n")

    # Get recent assignments grouped by CSM
    # Note: ACCOUNT_CSM_ASSIGNMENTS_CANNE doesn't have health_segment or neediness_score
    query = f"""
    SELECT
        csm_name,
        COUNT(*) as total_assigned,
        MIN(assignment_date) as first_assignment,
        MAX(assignment_date) as last_assignment,
        COUNT(DISTINCT DATE(assignment_date)) as days_active
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    WHERE assignment_date >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
    GROUP BY csm_name
    ORDER BY total_assigned DESC
    """

    cursor.execute(query)
    results = cursor.fetchall()

    if results:
        # Prepare table data
        table_data = []
        total_all = 0

        for row in results:
            csm, total, first_assign, last_assign, days_active = row

            # Calculate time range
            time_range = f"{first_assign.strftime('%m/%d %H:%M')} - {last_assign.strftime('%m/%d %H:%M')}"

            # Add to table
            table_data.append([
                csm,
                total,
                days_active,
                time_range
            ])

            # Track totals
            total_all += total

        # Display table
        headers = ["CSM", "Total Accounts", "Days Active", "Assignment Period"]
        print("📋 CSMs WHO RECEIVED NEW ASSIGNMENTS:")
        print(tabulate(table_data, headers=headers, tablefmt="grid"))

        # Show totals
        print(f"\n📈 SUMMARY:")
        print(f"  Total Accounts Assigned: {total_all}")
        print(f"  Total CSMs Involved: {len(results)}")
        print(f"  Average accounts per CSM: {total_all/len(results):.1f}")

        # Check for distribution issues
        max_assignments = max(r[1] for r in results)
        avg_assignments = total_all / len(results)
        ratio = max_assignments / avg_assignments

        print(f"\n📊 DISTRIBUTION METRICS:")
        print(f"  Max assignments to single CSM: {max_assignments}")
        print(f"  Distribution ratio (max/avg): {ratio:.2f}x")

        if ratio > 3:
            print("  ⚠️  WARNING: Uneven distribution detected (>3x ratio)")
        else:
            print("  ✅ Good distribution achieved (<3x ratio)")

        # Show specific CSMs with high assignments
        print("\n🎯 TOP 5 CSMs BY NEW ASSIGNMENTS:")
        for i, row in enumerate(results[:5]):
            csm = row[0]
            total = row[1]
            print(f"  {i+1}. {csm}: {total} accounts")

        # Check for Han Pham and Michelle Booth specifically
        print("\n🔍 CHECKING KNOWN PROBLEMATIC CSMs:")
        problem_csms = ['Han Pham', 'Michelle Booth']
        for row in results:
            if row[0] in problem_csms:
                csm = row[0]
                total = row[1]
                pct = 100.0 * total / total_all
                print(f"  {csm}: {total} accounts ({pct:.1f}% of all assignments)")
                if pct > 20:
                    print(f"    ⚠️  HIGH CONCENTRATION - {csm} got >20% of assignments!")

        # Show detailed recent assignments
        print("\n" + "=" * 100)
        print("📋 SAMPLE OF RECENT ASSIGNMENTS (Last 20):")
        print("=" * 100)

        detail_query = f"""
        SELECT
            csm_name,
            account_id,
            assignment_date,
            assignment_method
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
                csm, account, date, method = row
                detail_data.append([
                    csm,
                    account,
                    method,
                    date.strftime("%Y-%m-%d %H:%M")
                ])

            headers = ["CSM", "Account ID", "Method", "Assignment Time"]
            print(tabulate(detail_data, headers=headers, tablefmt="grid"))

    else:
        print("❌ No assignments found in the last {hours_back} hours")
        print("\nThis could mean:")
        print("  - No assignments were made in this period")
        print("  - The automation hasn't run yet")
        print("  - There's an issue with the ACCOUNT_CSM_ASSIGNMENTS_CANNE table")

    # Also check recommendations table for comparison
    print("\n" + "=" * 100)
    print("📊 COMPARING WITH RECOMMENDATIONS TABLE:")
    print("=" * 100)

    rec_query = f"""
    SELECT
        COUNT(*) as total_recommendations,
        COUNT(DISTINCT recommended_csm) as unique_csms,
        MIN(recommendation_timestamp) as first_rec,
        MAX(recommendation_timestamp) as last_rec
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    WHERE recommendation_timestamp >= DATEADD(hour, -{hours_back}, CURRENT_TIMESTAMP())
    """

    cursor.execute(rec_query)
    rec_result = cursor.fetchone()

    if rec_result:
        total_rec, unique_csms, first_rec, last_rec = rec_result
        if total_rec > 0:
            print(f"  Recommendations made: {total_rec}")
            print(f"  Unique CSMs recommended: {unique_csms}")
            print(f"  First recommendation: {first_rec}")
            print(f"  Last recommendation: {last_rec}")
        else:
            print("  No recommendations found in this period")

    cursor.close()
    conn.close()

    print("\n" + "=" * 100)
    print("END OF REPORT")
    print("=" * 100)

if __name__ == "__main__":
    # Default to 24 hours but can be adjusted
    show_recent_assignments(hours_back=24)

    # Also show last 7 days for broader view
    print("\n\n")
    print("🔄 SHOWING 7-DAY VIEW FOR COMPARISON:")
    show_recent_assignments(hours_back=168)  # 7 days = 168 hours