#!/usr/bin/env python3
"""
Debug CSM account counts to find the discrepancy
"""

import pandas as pd
from csm_routing_automation import CSMRoutingAutomation

def debug_csm_counts():
    """Debug CSM account counting logic"""

    # Create automation instance
    automation = CSMRoutingAutomation()

    # Connect to Snowflake
    print("Connecting to Snowflake...")
    if not automation.connect_snowflake():
        print("Failed to connect to Snowflake")
        return

    # Populate cache
    print("Populating neediness cache...")
    automation.populate_neediness_cache()

    # Now check what get_current_csm_books returns
    print("\nGetting CSM books...")
    csm_books = automation.get_current_csm_books()

    # Check Gohar's entry
    if 'Gohar Grigoryan' in csm_books:
        gohar_data = csm_books['Gohar Grigoryan']
        print(f"\nGohar Grigoryan in csm_books:")
        print(f"  Count returned by get_current_csm_books: {gohar_data['count']}")
        print(f"  (This is what optimizer sees)")
    else:
        print("\nGohar Grigoryan NOT in csm_books!")

    # Now check the actual cache
    cache = automation.neediness_cache
    if cache is not None:
        # All Gohar's accounts
        gohar_all = cache[cache['responsible_csm'] == 'Gohar Grigoryan']
        print(f"\nGohar's accounts in cache:")
        print(f"  Total accounts (all segments): {gohar_all['account_id'].nunique()}")

        # Residential Corporate only (what get_current_csm_books filters)
        gohar_resi_corp = cache[
            (cache['responsible_csm'] == 'Gohar Grigoryan') &
            (cache['segment'] == 'Residential') &
            (cache['account_level'] == 'Corporate')
        ]
        print(f"  Residential Corporate only: {gohar_resi_corp['account_id'].nunique()}")

        # Show segment breakdown
        segment_counts = gohar_all.groupby('segment')['account_id'].nunique()
        print(f"\n  Segment breakdown:")
        for segment, count in segment_counts.items():
            print(f"    {segment}: {count}")

    # Close connection
    if automation.snowflake_conn:
        automation.snowflake_conn.close()

if __name__ == "__main__":
    debug_csm_counts()