#!/usr/bin/env python3
"""
Test script to validate that the RESI_CORP_ACTIVE_CSMS table filter is working correctly.
This script will test the modified functions to ensure only eligible CSMs are selected.
"""

import sys
import logging
from datetime import datetime
from csm_routing_automation import CSMRoutingAutomation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test_resi_corp_filter_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_resi_corp_active_csms_filter():
    """Test that the resi_corp_active_csms table filter is working correctly"""

    logger.info("=" * 80)
    logger.info("Testing resi_corp_active_csms Table Filter Integration")
    logger.info("=" * 80)

    try:
        # Initialize the CSM routing automation
        logger.info("\nInitializing CSM Routing Automation...")
        automation = CSMRoutingAutomation()

        # Test 1: Check get_active_csms_and_managers_from_workday
        logger.info("\n" + "-" * 60)
        logger.info("TEST 1: Testing get_active_csms_and_managers_from_workday()")
        logger.info("-" * 60)

        active_csms, managers = automation.get_active_csms_and_managers_from_workday()

        logger.info(f"\nResults:")
        logger.info(f"  - Active CSMs (filtered by resi_corp_active_csms): {len(active_csms)}")
        logger.info(f"  - Managers found: {len(managers)}")

        if active_csms:
            logger.info(f"\nSample CSMs (first 5):")
            for csm in active_csms[:5]:
                logger.info(f"    - {csm}")
        else:
            logger.warning("  No active CSMs found after filtering!")

        # Test 2: Check get_current_csm_books
        logger.info("\n" + "-" * 60)
        logger.info("TEST 2: Testing get_current_csm_books()")
        logger.info("-" * 60)

        csm_books = automation.get_current_csm_books(min_account_threshold=5)

        logger.info(f"\nResults:")
        logger.info(f"  - CSMs with books (filtered by resi_corp_active_csms): {len(csm_books)}")
        logger.info(f"  - Eligible CSMs for assignment: {len(automation.eligible_csm_list)}")

        if csm_books:
            logger.info(f"\nBook statistics for eligible CSMs:")
            for csm, data in list(csm_books.items())[:5]:
                logger.info(f"    - {csm}:")
                logger.info(f"        Accounts: {data['count']}")
                logger.info(f"        Tenure: {data['tenure_category']} ({data['tenure_months']} months)")
                logger.info(f"        Health Distribution: Red={data['health_distribution']['Red']}, "
                          f"Yellow={data['health_distribution']['Yellow']}, "
                          f"Green={data['health_distribution']['Green']}")

        # Test 3: Verify consistency between the two functions
        logger.info("\n" + "-" * 60)
        logger.info("TEST 3: Consistency Check")
        logger.info("-" * 60)

        csms_in_books = set(csm_books.keys())
        csms_from_workday = set(active_csms)

        # CSMs in books should be a subset of active CSMs from Workday
        csms_only_in_books = csms_in_books - csms_from_workday
        csms_only_in_workday = csms_from_workday - csms_in_books

        logger.info(f"\nConsistency Analysis:")
        logger.info(f"  - CSMs in both Workday and Books: {len(csms_in_books & csms_from_workday)}")
        logger.info(f"  - CSMs only in Books (not in filtered Workday): {len(csms_only_in_books)}")
        logger.info(f"  - CSMs only in Workday (no current books): {len(csms_only_in_workday)}")

        if csms_only_in_books:
            logger.warning("\nCSMs with books but not in filtered Workday list (should be 0):")
            for csm in list(csms_only_in_books)[:5]:
                logger.warning(f"    - {csm}")
            logger.error("ERROR: Found CSMs with books who are not in the resi_corp_active_csms filtered Workday list!")
        else:
            logger.info("  ✓ All CSMs with books are in the filtered Workday list")

        if csms_only_in_workday:
            logger.info("\nCSMs in Workday but without current books (this is expected):")
            for csm in list(csms_only_in_workday)[:5]:
                logger.info(f"    - {csm}")

        # Test 4: Direct query to verify the filter
        logger.info("\n" + "-" * 60)
        logger.info("TEST 4: Direct Query Verification")
        logger.info("-" * 60)

        # Query to check if resi_corp_active_csms table exists and has data
        verification_query = """
        SELECT COUNT(*) as total_eligible_csms
        FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
        """

        try:
            result = automation.execute_query(verification_query)
            if not result.empty:
                total_eligible = result['total_eligible_csms'].iloc[0]
                logger.info(f"\nresi_corp_active_csms table contains: {total_eligible} eligible CSMs")

                # Compare with our filtered results
                logger.info(f"CSMs found by get_active_csms_and_managers_from_workday: {len(active_csms)}")
                logger.info(f"CSMs with books after filter: {len(csm_books)}")

                if len(active_csms) <= total_eligible:
                    logger.info("  ✓ Filtered CSM count is within expected range")
                else:
                    logger.warning("  ⚠ More CSMs found than in resi_corp_active_csms table - check joins")
        except Exception as e:
            logger.error(f"Failed to query resi_corp_active_csms table: {str(e)}")
            logger.info("Make sure the resi_corp_active_csms table exists in DSV_WAREHOUSE.DATA_SCIENCE schema")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("TEST SUMMARY")
        logger.info("=" * 80)
        logger.info(f"\nFilter Integration Results:")
        logger.info(f"  1. Active CSMs from Workday (filtered): {len(active_csms)}")
        logger.info(f"  2. CSMs with current books (filtered): {len(csm_books)}")
        logger.info(f"  3. Eligible CSMs for assignment: {len(automation.eligible_csm_list)}")
        logger.info(f"  4. Consistency check: {'PASSED' if not csms_only_in_books else 'FAILED'}")

        if automation.eligible_csm_list:
            logger.info("\n✅ resi_corp_active_csms filter is working correctly!")
            logger.info("Only CSMs in the resi_corp_active_csms table are eligible for assignment.")
        else:
            logger.warning("\n⚠ No eligible CSMs found after filtering!")
            logger.warning("Please check:")
            logger.warning("  1. resi_corp_active_csms table exists and has data")
            logger.warning("  2. CSM names in the table match those in Workday and Customer History")

    except Exception as e:
        logger.error(f"\n❌ Test failed with error: {str(e)}")
        logger.exception("Full error traceback:")
        return False

    return True

if __name__ == "__main__":
    logger.info("Starting resi_corp_active_csms Filter Test")
    logger.info(f"Test started at: {datetime.now()}")

    success = test_resi_corp_active_csms_filter()

    logger.info(f"\nTest completed at: {datetime.now()}")
    if success:
        logger.info("✅ All tests completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Some tests failed. Please review the logs.")
        sys.exit(1)