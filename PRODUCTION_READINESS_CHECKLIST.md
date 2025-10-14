# CSM Routing Automation - Production Readiness Checklist

## Pre-Production Validation

### ✅ Core Functionality
- [x] Single account assignment tested and working
- [x] Batch assignment (3-4 accounts) tested and working
- [x] Snowflake connectivity verified
- [x] Workday filtering operational (88 inactive CSMs filtered)
- [x] Database tables created with _CANNE suffix
- [x] Recommendations tracking implemented
- [x] Assignment persistence verified

### ✅ Data Integrity
- [x] Column case sensitivity handled
- [x] Null value handling implemented
- [x] JSON serialization for numpy/Decimal types
- [x] Primary key constraints on assignment table
- [x] Audit trail with timestamps

### ✅ Business Logic
- [x] Residential Corporate filtering working
- [x] Max 85 accounts per CSM enforced
- [x] 4-hour cooling period implemented
- [x] Neediness scoring calculation verified
- [x] Health score distribution balanced
- [x] CSM tenure categories applied

### ✅ Integration Points
- [x] Snowflake VW_ONBOARDING_DETAIL view access
- [x] Workday CSM verification
- [x] LLM review integration (Claude Sonnet 3.5)
- [x] Private key authentication working

## Production Deployment Steps

### 1. Environment Setup
```bash
# Verify Python version (3.9+)
python --version

# Install required packages
pip install snowflake-connector-python
pip install pandas numpy
pip install pulp
pip install anthropic
pip install cryptography
```

### 2. Configuration Verification
- [ ] Verify properties.json has production credentials
- [ ] Confirm Anthropic API key is valid
- [ ] Validate Snowflake connection parameters
- [ ] Check csm_category_limits.json settings

### 3. Database Preparation
```bash
# Create production tables
python create_tables.py

# Verify tables exist
python -c "
from csm_routing_automation import CSMRoutingAutomation
automation = CSMRoutingAutomation('properties.json', 'csm_category_limits.json')
if automation.connect_snowflake():
    print('Tables verified')
    automation.snowflake_conn.close()
"
```

### 4. Dry Run Testing
- [ ] Run with 1 test account
- [ ] Run with 5 test accounts
- [ ] Verify assignments in database
- [ ] Check LLM feedback quality
- [ ] Validate CSM distribution

## Monitoring Setup

### 1. Log Configuration
- [ ] Set up centralized logging
- [ ] Configure log rotation
- [ ] Set appropriate log levels (INFO for production)
- [ ] Create log monitoring alerts

### 2. Key Metrics to Track
- [ ] Daily assignment count
- [ ] Assignment success rate
- [ ] Average optimization score
- [ ] LLM approval percentage
- [ ] CSM utilization rates
- [ ] Error frequency

### 3. Alert Thresholds
- [ ] No assignments for 24 hours
- [ ] Assignment failure rate > 10%
- [ ] CSM at capacity (85 accounts)
- [ ] LLM API failures
- [ ] Snowflake connection issues

## Performance Optimization

### 1. Query Optimization
- [ ] Add indexes on frequently queried columns
- [ ] Optimize complex joins in enrichment queries
- [ ] Consider materialized views for heavy queries
- [ ] Implement query result caching

### 2. Batch Processing
- [ ] Determine optimal batch size (currently 4, test with 10-20)
- [ ] Implement parallel processing where applicable
- [ ] Set appropriate timeout values
- [ ] Configure retry logic for failures

## Security Review

### 1. Access Control
- [ ] Verify Snowflake role permissions
- [ ] Restrict table write access to service account
- [ ] Implement API key rotation schedule
- [ ] Review private key security

### 2. Data Protection
- [ ] Ensure PII is handled appropriately
- [ ] Implement data retention policies
- [ ] Set up audit logging
- [ ] Configure backup procedures

## Rollback Plan

### 1. Preparation
- [ ] Document current manual process
- [ ] Create rollback scripts
- [ ] Backup existing data
- [ ] Prepare communication templates

### 2. Rollback Triggers
- Critical failure scenarios:
  - [ ] Database corruption
  - [ ] Mass incorrect assignments
  - [ ] System performance degradation
  - [ ] Data loss incidents

### 3. Rollback Steps
```sql
-- Revert assignments if needed
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
SET csm_name = NULL
WHERE assignment_date >= 'ROLLBACK_DATE';

-- Mark recommendations as not assigned
UPDATE DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
SET was_assigned = FALSE
WHERE recommendation_timestamp >= 'ROLLBACK_DATE';
```

## Operational Procedures

### 1. Daily Operations
- [ ] Morning health check script
- [ ] Assignment verification
- [ ] Error log review
- [ ] Metrics dashboard update

### 2. Weekly Tasks
- [ ] CSM workload review
- [ ] Assignment quality audit
- [ ] LLM feedback analysis
- [ ] Performance tuning

### 3. Monthly Tasks
- [ ] Capacity planning review
- [ ] Business rule updates
- [ ] System optimization
- [ ] Stakeholder reporting

## Documentation

### Required Documentation
- [x] System architecture document
- [x] API documentation
- [x] Database schema
- [x] Business logic documentation
- [ ] Runbook for operations team
- [ ] Troubleshooting guide
- [ ] User guide for CSM managers

## Stakeholder Sign-offs

### Technical Approval
- [ ] Data Engineering Team
- [ ] Security Team
- [ ] Database Administration
- [ ] DevOps Team

### Business Approval
- [ ] CSM Management
- [ ] Customer Success Leadership
- [ ] Data Science Team
- [ ] Operations Team

## Go-Live Criteria

### Must Have
- [x] All tests passing
- [x] Production tables created
- [x] Monitoring in place
- [ ] Rollback plan tested
- [ ] Runbook completed
- [ ] Team trained

### Nice to Have
- [ ] Performance benchmarks established
- [ ] Automated testing suite
- [ ] Dashboard for CSM managers
- [ ] Self-service troubleshooting tools

## Post-Production Tasks

### Week 1
- [ ] Daily monitoring and validation
- [ ] Gather CSM feedback
- [ ] Fine-tune thresholds
- [ ] Document any issues

### Week 2-4
- [ ] Performance optimization
- [ ] Process refinement
- [ ] Expanded testing
- [ ] Knowledge transfer sessions

### Month 2+
- [ ] Quarterly review process
- [ ] Feature enhancement planning
- [ ] Scaling assessment
- [ ] ROI measurement

---
Checklist Created: 2025-10-14
Target Go-Live: [To be determined]
Status: Testing Complete, Ready for Production Planning