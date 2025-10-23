# resi_corp_active_csms Table Integration

## Overview
This document describes the integration of the `resi_corp_active_csms` table to filter eligible CSMs for account assignment in the CSM Routing Automation system.

## Purpose
The `resi_corp_active_csms` table acts as a whitelist to ensure only eligible CSMs are considered for account assignment. This filtering mechanism helps exclude CSMs who may be:
- On leave or transitioning roles
- Not assigned to Residential Corporate segment
- Part of special teams or pilot programs
- Otherwise ineligible for new account assignments

## Changes Made

### 1. Modified Functions

#### `get_active_csms_and_managers_from_workday()` (Lines 530-639)
**Location**: `/csm_routing_automation.py:530`

**Changes**:
- Added CTE wrapper around existing Workday query
- Added INNER JOIN with `DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms` table
- Join condition: `w.CSM = r.active_csm`
- Added logging to show filtered vs unfiltered counts
- Only returns CSMs present in both Workday AND the active CSMs table

**SQL Pattern**:
```sql
WITH workday_csms AS (
    -- Original Workday query
)
SELECT w.CSM, w.Manager
FROM workday_csms w
INNER JOIN DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms r
    ON w.CSM = r.active_csm
```

#### `get_current_csm_books()` (Lines 660-832)
**Location**: `/csm_routing_automation.py:660`

**Changes**:
- Added INNER JOIN with `DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms` table
- Join condition: `chd.preferred_csm_name = r.active_csm`
- Added pre-filter count query for logging comparison
- Enhanced logging to show filtering impact

**SQL Pattern**:
```sql
SELECT DISTINCT ...
FROM DSV_WAREHOUSE.POST_SALES.VW_CUSTOMER_HISTORY_DAILY chd
INNER JOIN DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms r
    ON chd.preferred_csm_name = r.active_csm
WHERE ...
```

### 2. Enhanced Logging

Both functions now provide detailed logging showing:
- Total CSMs before filtering
- CSMs remaining after RESI_CORP_ACTIVE_CSMS filter
- Number of CSMs filtered out
- Final eligible CSM list

Example log output:
```
Retrieved 45 active CSMs from Workday (filtered by resi_corp_active_csms table)
Filtered out 12 CSMs who are not in resi_corp_active_csms
CSMs with books before resi_corp_active_csms filter: 57
CSMs filtered out by resi_corp_active_csms: 12
Final eligible CSMs for assignment: 45
```

### 3. Test Script

Created `/test_resi_corp_filter.py` to validate the integration:
- Tests both modified functions
- Verifies consistency between Workday and book data
- Checks resi_corp_active_csms table directly
- Provides comprehensive test summary

## Expected Table Structure

The `resi_corp_active_csms` table should have at minimum:
```sql
CREATE TABLE DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms (
    active_csm VARCHAR(100) PRIMARY KEY  -- Full name matching Workday format
    -- Additional columns as needed
);
```

**Important**: The `active_csm` column must match the format used in:
- Workday: `CONCAT(legal_first_name, ' ', legal_last_name)`
- Customer History: `preferred_csm_name`

## Impact on Assignment Logic

### Before Integration
- Any CSM marked as active in Workday could be assigned accounts
- CSMs from all segments could potentially receive Residential Corporate accounts
- No centralized control over CSM eligibility

### After Integration
- Only CSMs in `resi_corp_active_csms` table are eligible
- Provides centralized control over CSM pool
- Prevents assignments to ineligible CSMs
- Maintains data consistency across systems

## Testing

Run the test script to validate the integration:
```bash
python test_resi_corp_filter.py
```

The test will:
1. Verify the filter is working in both functions
2. Check consistency between data sources
3. Confirm only eligible CSMs are selected
4. Validate the resi_corp_active_csms table exists and has data

## Rollback Plan

If you need to temporarily disable the filter (not recommended):

1. Remove the INNER JOIN from `get_active_csms_and_managers_from_workday()`
2. Remove the INNER JOIN from `get_current_csm_books()`
3. Revert logging changes

However, this should only be done if the `resi_corp_active_csms` table is not properly populated.

## Maintenance

### Adding New CSMs
Insert eligible CSMs into the table:
```sql
INSERT INTO DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms (active_csm)
VALUES ('John Smith'), ('Jane Doe');
```

### Removing CSMs
Delete ineligible CSMs:
```sql
DELETE FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms
WHERE active_csm IN ('John Smith');
```

### Bulk Updates
Consider creating a stored procedure or scheduled job to maintain this table based on business rules.

## Monitoring

Monitor the logs for:
- Large numbers of filtered CSMs (might indicate table not updated)
- No eligible CSMs (table might be empty or names mismatched)
- Consistency warnings between Workday and book data

## Troubleshooting

### No CSMs Found After Filtering
1. Check if `resi_corp_active_csms` table exists
2. Verify table has data: `SELECT COUNT(*) FROM DSV_WAREHOUSE.DATA_SCIENCE.resi_corp_active_csms`
3. Check name format matches between systems
4. Review logs for specific filtering counts

### Name Mismatch Issues
Ensure names match exactly between:
- Workday: `legal_first_name + ' ' + legal_last_name`
- resi_corp_active_csms: `active_csm`
- Customer History: `preferred_csm_name`

Consider using UPPER() or TRIM() in joins if needed.

### Performance Issues
The INNER JOIN should be efficient if:
- `active_csm` is indexed in `resi_corp_active_csms`
- Table is relatively small (< 1000 rows expected)

If performance degrades, consider:
- Adding index: `CREATE INDEX idx_active_csm ON resi_corp_active_csms(active_csm)`
- Using EXISTS clause instead of JOIN
- Caching the eligible CSM list

## Future Enhancements

Consider adding to `resi_corp_active_csms`:
- `effective_date` - When CSM becomes eligible
- `expiry_date` - When CSM eligibility ends
- `segment` - Specific segment assignment
- `capacity_override` - Custom capacity limits
- `notes` - Reason for inclusion/exclusion

This would enable more sophisticated filtering and capacity management.