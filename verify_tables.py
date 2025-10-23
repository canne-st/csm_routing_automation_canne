#!/usr/bin/env python3
"""Verify the state of CSM routing tables"""

import json
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

# Load config
with open('properties.json') as f:
    config = json.load(f)

# Connect to Snowflake using same method as main automation
private_key = serialization.load_pem_private_key(
    config["SNOWFLAKE_PRIVATE_KEY"].replace('\\n', '\n').encode(),
    password=None,
    backend=default_backend()
)

conn = snowflake.connector.connect(
    user=config["SNOWFLAKE_USER"],
    private_key=private_key,
    account=config["snowflake_account_prod"],
    warehouse=config["snowflake_warehouse"],
    database=config["snowflake_database"],
    schema=config["snowflake_schema"],
    role=config["snowflake_role"]
)

cursor = conn.cursor()

print('=' * 80)
print('CSM ROUTING TABLE VERIFICATION')
print('=' * 80)

# Check recommendations
cursor.execute('''
    SELECT COUNT(*) as total,
           COUNT(CASE WHEN llm_reviewed = TRUE THEN 1 END) as reviewed,
           COUNT(CASE WHEN llm_approved = TRUE THEN 1 END) as approved,
           COUNT(CASE WHEN llm_reviewed IS NULL OR llm_reviewed = FALSE THEN 1 END) as unreviewed
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
''')
rec = cursor.fetchone()
print(f'\nCSM_ROUTING_RECOMMENDATIONS_CANNE:')
print(f'  Total recommendations: {rec[0]}')
print(f'  LLM reviewed: {rec[1]}')
print(f'  LLM approved: {rec[2]}')
print(f'  Not reviewed: {rec[3]}')

# Check assignments
cursor.execute('''
    SELECT COUNT(*) as total
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
''')
assignments = cursor.fetchone()[0]
print(f'\nACCOUNT_CSM_ASSIGNMENTS_CANNE:')
print(f'  Total assignments: {assignments}')

# Show recent assignments
cursor.execute('''
    SELECT account_id, assigned_csm_name, assignment_timestamp
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    ORDER BY assignment_timestamp DESC
    LIMIT 3
''')
recent = cursor.fetchall()
if recent:
    print(f'\n  Recent assignments:')
    for row in recent:
        print(f'    {row[0]} -> {row[1]} at {row[2]}')

# Check if there are unreviewed recommendations
if rec[3] > 0:
    print(f'\n⚠️  WARNING: {rec[3]} recommendations have not been reviewed by LLM!')
    print('  This suggests the LLM review step may not be running automatically.')
    
    cursor.execute('''
        SELECT account_id, recommended_csm, recommendation_timestamp
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE llm_reviewed IS NULL OR llm_reviewed = FALSE
        ORDER BY recommendation_timestamp DESC
        LIMIT 3
    ''')
    unreviewed = cursor.fetchall()
    if unreviewed:
        print(f'\n  Sample unreviewed recommendations:')
        for row in unreviewed:
            print(f'    {row[0]} -> {row[1]} at {row[2]}')

cursor.close()
conn.close()

print('\n' + '=' * 80)
print('SUMMARY:')
if assignments > 0:
    print(f'✅ Found {assignments} records in final assignments table')
else:
    print('❌ No records found in final assignments table')
    
if rec[3] > 0:
    print(f'❌ {rec[3]} recommendations have not been reviewed by LLM')
else:
    print('✅ All recommendations have been reviewed by LLM')
print('=' * 80)
