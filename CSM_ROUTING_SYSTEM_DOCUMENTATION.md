# CSM Routing Automation System Documentation

## System Overview
The CSM Routing Automation system automatically assigns Customer Success Managers (CSMs) to new accounts that require CSM support. The system queries Snowflake for accounts with `success_transition_status_ob = 'Needs CSM'` and intelligently assigns them to available CSMs based on multiple optimization factors.

## Key Features
- **Automated Assignment**: Processes accounts requiring CSM assignment from Snowflake
- **Intelligent Optimization**: Uses PuLP linear programming for batch assignments
- **LLM Review**: Claude Sonnet 3.5 reviews assignments for quality assurance
- **Database Tracking**: Stores recommendations and assignments in Snowflake tables
- **Workday Integration**: Only assigns to active CSMs verified in Workday
- **Tenure-Based Assignment**: Considers CSM experience levels in assignments
- **Health Score Balancing**: Distributes Red/Yellow/Green accounts evenly

## Architecture

### Components
1. **csm_routing_automation.py**: Main automation engine
2. **test_csm_assignment.py**: Testing script for single/batch assignments
3. **create_tables.py**: Database table creation script
4. **verify_assignments.py**: Assignment verification script
5. **properties.json**: Configuration file with credentials
6. **csm_category_limits.json**: Business rules configuration

### Database Tables
All tables are in `DSV_WAREHOUSE.DATA_SCIENCE` schema with `_CANNE` suffix:

#### CSM_ROUTING_RECOMMENDATIONS_CANNE
- Tracks all CSM recommendations made by the system
- Includes optimization scores and assignment status
- Maintains full audit trail of recommendations

#### ACCOUNT_CSM_ASSIGNMENTS_CANNE
- Stores actual CSM assignments
- One record per account (PRIMARY KEY on account_id)
- Includes LLM feedback and assignment metadata

## Assignment Logic

### 1. Account Selection
```sql
SELECT DISTINCT account_id_ob as account_id, tenant_id
FROM DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
WHERE success_transition_status_ob = 'Needs CSM'
AND account_id_ob IS NOT NULL
```

### 2. Account Enrichment
The system enriches each account with:
- **Segment**: Enterprise/Mid-Market/Small Business/Residential
- **Account Level**: Corporate/Individual/Unknown
- **Neediness Score**: 0-10+ scale based on multiple factors
- **Health Segment**: Red/Yellow/Green
- **Revenue**: Total account revenue
- **TAD Score**: Technical assessment score
- **Churn Risk**: Based on SVOT signals

### 3. CSM Book Analysis
For each active CSM, the system calculates:
- Current account count
- Total neediness score
- Health distribution (Red/Yellow/Green percentages)
- Tenure category (New/Junior/Mid/Senior/Expert)
- Recent assignment timestamps (for cooling periods)

### 4. Optimization Strategy

#### Single Account Assignment
Uses weighted scoring with factors:
- **Book Size Balance**: Penalizes overloaded CSMs
- **Neediness Balance**: Distributes high-neediness accounts
- **Health Distribution**: Balances Red/Yellow/Green accounts
- **Tenure Match**: Assigns complex accounts to experienced CSMs
- **Recency Penalty**: Implements cooling periods between assignments

#### Batch Assignment (2+ accounts)
Uses PuLP linear programming to:
- Minimize total deviation from target metrics
- Ensure fair distribution across CSMs
- Respect capacity constraints (max 85 accounts per CSM)
- Balance workload globally

### 5. LLM Review
Claude Sonnet 3.5 reviews assignments considering:
- CSM workload balance
- Account complexity matching CSM experience
- Health score distribution
- Revenue impact
- Historical performance

## Business Rules

### Residential Corporate Focus
- Primary focus on Residential segment + Corporate level accounts
- Max 85 accounts per CSM
- 4-hour cooling period between assignments to same CSM

### CSM Eligibility
CSMs must be:
- Active in Workday system
- Not exceeding capacity limits
- Appropriate tenure for account complexity

## Running the System

### Prerequisites
1. Snowflake access with proper credentials
2. Anthropic API key for LLM review
3. Python environment with required packages

### Test Execution
```bash
# Create necessary tables
python create_tables.py

# Run test with 1-4 accounts
python test_csm_assignment.py

# Verify assignments
python verify_assignments.py
```

### Production Execution
```python
from csm_routing_automation import CSMRoutingAutomation

# Initialize
automation = CSMRoutingAutomation(
    config_file='properties.json',
    limits_file='csm_category_limits.json'
)

# Connect and run
if automation.connect_snowflake():
    automation.run_automated_routing()
```

## Monitoring and Verification

### Key Metrics to Monitor
1. **Assignment Success Rate**: Percentage of accounts successfully assigned
2. **CSM Utilization**: Distribution of accounts across CSMs
3. **Health Balance**: Red/Yellow/Green distribution per CSM
4. **LLM Approval Rate**: Percentage of assignments approved by LLM

### Verification Queries
```sql
-- Recent recommendations
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
ORDER BY recommendation_timestamp DESC
LIMIT 10;

-- Current assignments
SELECT * FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
ORDER BY assignment_date DESC
LIMIT 10;

-- CSM workload distribution
SELECT csm_name, COUNT(*) as account_count
FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
GROUP BY csm_name
ORDER BY account_count DESC;
```

## Error Handling

### Common Issues and Solutions
1. **Column Case Sensitivity**: System automatically converts to lowercase
2. **Missing Tables**: Run create_tables.py to create required tables
3. **Inactive CSMs**: System filters using Workday active status
4. **JSON Serialization**: Handles numpy/Decimal types automatically

## Configuration

### properties.json
- Snowflake credentials and connection details
- Anthropic API key for LLM integration
- Database and schema specifications

### csm_category_limits.json
- Max accounts per CSM (default: 85)
- Cooling period hours (default: 4)
- Segment-specific limits

## Security Considerations
- Private key authentication for Snowflake
- Secure storage of API keys
- Audit trail for all assignments
- Read-only access to source tables

## Future Enhancements
1. Real-time assignment triggers
2. Advanced ML-based matching
3. Historical performance integration
4. Automated rebalancing capabilities
5. Dashboard for monitoring assignments

## Support and Maintenance
- Log files: test_single_account.log
- Error tracking in recommendations table
- LLM feedback stored for analysis
- Full audit trail maintained

## Version History
- v1.0: Initial implementation with CSV-based processing
- v2.0: Direct Snowflake integration
- v3.0: LLM review integration
- v4.0: Workday filtering and _CANNE table structure

---
Generated: 2025-10-14
System Status: Operational
Last Test: 4 accounts successfully assigned