#!/usr/bin/env python3
"""Quick script to check table state"""

import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    role=os.getenv('SNOWFLAKE_ROLE')
)

cursor = conn.cursor()

print('=' * 60)
print('CHECKING CSM ROUTING TABLES')
print('=' * 60)

# Check recommendations
cursor.execute('''
    SELECT COUNT(*) as total,
           COUNT(CASE WHEN llm_reviewed = TRUE THEN 1 END) as reviewed,
           COUNT(CASE WHEN llm_approved = TRUE THEN 1 END) as approved
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
''')
rec = cursor.fetchone()
print(f'\nRECOMMENDATIONS TABLE:')
print(f'  Total recommendations: {rec[0]}')
print(f'  LLM reviewed: {rec[1]}')
print(f'  LLM approved: {rec[2]}')

# Check assignments
cursor.execute('''
    SELECT COUNT(*) as total
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
''')
assignments = cursor.fetchone()[0]
print(f'\nFINAL ASSIGNMENTS TABLE:')
print(f'  Total assignments: {assignments}')

# Check last 5 assignments
cursor.execute('''
    SELECT account_id, assigned_csm_name, assignment_timestamp
    FROM DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE
    ORDER BY assignment_timestamp DESC
    LIMIT 5
''')
last_assignments = cursor.fetchall()
if last_assignments:
    print(f'\nLast 5 assignments:')
    for row in last_assignments:
        print(f'  {row[0]} -> {row[1]} at {row[2]}')

# Check for unreviewed recommendations
cursor.execute('''
    SELECT COUNT(*) as unreviewed
    FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
    WHERE llm_reviewed IS NULL OR llm_reviewed = FALSE
''')
unreviewed = cursor.fetchone()[0]
if unreviewed > 0:
    print(f'\n⚠️ WARNING: {unreviewed} recommendations have not been reviewed by LLM')

    cursor.execute('''
        SELECT account_id, recommended_csm, recommendation_timestamp
        FROM DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE
        WHERE llm_reviewed IS NULL OR llm_reviewed = FALSE
        ORDER BY recommendation_timestamp DESC
        LIMIT 5
    ''')
    unreviewed_recs = cursor.fetchall()
    print(f'\nFirst 5 unreviewed recommendations:')
    for row in unreviewed_recs:
        print(f'  {row[0]} -> {row[1]} at {row[2]}')

cursor.close()
conn.close()

print('\n' + '=' * 60)
