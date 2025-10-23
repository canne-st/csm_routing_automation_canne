#!/usr/bin/env python3
"""
Test single account by running the main automation.
The main automation will:
1. Find accounts needing CSM
2. Run optimization
3. Review with LLM (if API key configured)
4. Write to ACCOUNT_CSM_ASSIGNMENTS_CANNE
"""

import sys
import os
import logging
from datetime import datetime

# Import the main automation class
from csm_routing_automation import CSMRoutingAutomation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'test_run_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def test_single_account():
    """Run the main automation which handles everything"""

    logger.info("=" * 80)
    logger.info("RUNNING MAIN CSM ROUTING AUTOMATION")
    logger.info("=" * 80)

    # Create automation instance
    automation = CSMRoutingAutomation()

    # Run the main automation - it handles EVERYTHING:
    # 1. Connects to Snowflake
    # 2. Gets accounts needing CSM
    # 3. Runs optimization
    # 4. Reviews with LLM (if API key exists)
    # 5. Writes to ACCOUNT_CSM_ASSIGNMENTS_CANNE
    logger.info("\nStarting main automation flow...")
    # TEST MODE: Process only 1 account
    automation.run(test_limit=1)

    logger.info("\n" + "=" * 80)
    logger.info("AUTOMATION COMPLETE")
    logger.info("=" * 80)
    logger.info("\nCheck the following tables:")
    logger.info("1. CSM_ROUTING_RECOMMENDATIONS_CANNE - for optimization results")
    logger.info("2. ACCOUNT_CSM_ASSIGNMENTS_CANNE - for final assignments")

    return True

def main():
    """Main entry point"""
    logger.info("Starting CSM Routing Automation Test")
    logger.info(f"Started at: {datetime.now()}")

    success = test_single_account()

    logger.info(f"\nCompleted at: {datetime.now()}")

    if success:
        logger.info("\n✅ SUCCESSFUL!")
    else:
        logger.error("\n❌ FAILED")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())