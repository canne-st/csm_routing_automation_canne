# How to Run CSM Routing Automation

## Overview
The main script `csm_routing_automation.py` is the production implementation that assigns CSMs to accounts and stores records in the database tables.

## Prerequisites

### 1. Snowflake Credentials
Create a `.env` file in the project directory with your Snowflake credentials:
```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=DSV_WAREHOUSE
SNOWFLAKE_SCHEMA=DATA_SCIENCE
SNOWFLAKE_ROLE=your_role
```

### 2. Ensure Accounts Need CSM Assignment
Check if there are accounts to process:
```sql
SELECT COUNT(*) as accounts_needing_csm
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE success_transition_status_ob = 'Needs CSM'
    AND account_id_ob IS NOT NULL;
```

If no accounts, create test data:
```sql
-- Update a test account to need CSM
UPDATE DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
SET success_transition_status_ob = 'Needs CSM'
WHERE account_id_ob = 'YOUR_TEST_ACCOUNT_ID';
```

### 3. Verify resi_corp_active_csms Table
Ensure eligible CSMs exist:
```sql
SELECT COUNT(*) as eligible_csms
FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms;
```

If empty, add eligible CSMs:
```sql
INSERT INTO DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms (active_csm)
VALUES ('John Smith'), ('Jane Doe'), ('Sarah Johnson');
```

## Running the Script

### Option 1: Single Run (Recommended for Testing)
```bash
# Run once and exit
python csm_routing_automation.py
```

### Option 2: Modify for Single Execution
Edit `csm_routing_automation.py` main() function to run once:
```python
def main():
    """Main entry point for the automation"""
    automation = CSMRoutingAutomation()

    # Run once instead of loop
    try:
        automation.run()
        logger.info("Automation completed successfully")
    except Exception as e:
        logger.error(f"Automation failed: {str(e)}")
```

### Option 3: Run with Custom Parameters
Create a wrapper script:
```python
# run_single_assignment.py
from csm_routing_automation import CSMRoutingAutomation
import logging

logging.basicConfig(level=logging.INFO)

# Create automation instance
automation = CSMRoutingAutomation()

# Connect to Snowflake
if automation.connect_snowflake():
    # Run the assignment process
    automation.run()

    # Check results
    print("\nCheck results in Snowflake:")
    print("SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE")
    print("WHERE DATE(recommendation_timestamp) = CURRENT_DATE()")
else:
    print("Failed to connect to Snowflake")
```

## What Happens When You Run

### 1. Connection Phase
- Connects to Snowflake using credentials
- Validates connection

### 2. Data Collection Phase
- Queries accounts with `success_transition_status_ob = 'Needs CSM'`
- Enriches account data with business metrics
- Filters for Residential Corporate accounts only

### 3. CSM Eligibility Phase
- Gets active CSMs from Workday
- **Filters through resi_corp_active_csms table** (NEW!)
- Gets current CSM books and workload
- Excludes managers and CSMs below minimum threshold

### 4. Assignment Optimization
- **Single Account**: Uses weighted scoring algorithm
- **Multiple Accounts**: Uses PuLP linear programming
- Considers:
  - CSM capacity (max 85 accounts)
  - Health score matching
  - Revenue distribution
  - Neediness balance
  - Cooling period (4 hours)

### 5. Storage Phase
Records are inserted into:

#### CSM_ROUTING_RECOMMENDATIONS_CANNE
- Every recommendation is stored here
- Includes optimization scores
- Tracks `was_assigned` flag
- Full audit trail

#### ACCOUNT_CSM_ASSIGNMENTS_CANNE
- Only accepted assignments
- Updated when `was_assigned = TRUE`
- One record per account

### 6. LLM Review (Optional)
- If Claude API is configured
- Reviews assignments for balance
- May trigger re-optimization

## Checking Results

### View Latest Recommendations
```sql
SELECT
    account_id,
    recommended_csm,
    optimization_score,
    was_assigned,
    recommendation_timestamp
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE DATE(recommendation_timestamp) = CURRENT_DATE()
ORDER BY recommendation_timestamp DESC;
```

### View Accepted Assignments
```sql
SELECT *
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
WHERE DATE(last_updated) = CURRENT_DATE();
```

### Check CSM Workload
```sql
SELECT
    recommended_csm,
    COUNT(*) as accounts_assigned,
    AVG(optimization_score) as avg_score
FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
WHERE was_assigned = TRUE
GROUP BY recommended_csm
ORDER BY accounts_assigned DESC;
```

## Troubleshooting

### No Records Being Inserted

1. **Check Snowflake Connection**
   ```bash
   python -c "from csm_routing_automation import CSMRoutingAutomation; a = CSMRoutingAutomation(); print(a.connect_snowflake())"
   ```

2. **Check for Accounts Needing CSM**
   ```sql
   SELECT COUNT(*) FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
   WHERE success_transition_status_ob = 'Needs CSM';
   ```

3. **Check Eligible CSMs**
   ```sql
   SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms;
   ```

4. **Check Logs**
   Look for log files in the project directory with timestamps

### Script Hangs
- Usually waiting for Snowflake connection
- Check credentials in `.env` file
- Verify network access to Snowflake

### No CSMs Found
- Verify resi_corp_active_csms table has data
- Check name format matches between systems
- Review filtered CSM counts in logs

## Important Notes

1. **Filter Applied**: Only CSMs in `resi_corp_active_csms` table are eligible
2. **Segment Focus**: Only processes Residential Corporate accounts
3. **Cooling Period**: 4-hour minimum between assignments per CSM
4. **Capacity Limit**: Max 85 accounts per CSM
5. **Minimum Threshold**: CSMs need at least 5 accounts to be eligible

## Production Deployment

For production, consider:
1. Using a scheduler (cron, Airflow) instead of the infinite loop
2. Setting up proper logging to a central location
3. Implementing monitoring and alerts
4. Creating a config file for parameters
5. Adding error recovery and retry logic