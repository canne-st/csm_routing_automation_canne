#!/usr/bin/env python
# coding: utf-8

"""
Script to create all necessary tables for CSM routing automation
"""

import json
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config(config_file):
    """Load configuration from JSON file"""
    with open(config_file, 'r') as f:
        return json.load(f)

def private_key_deserializer(private_key_str):
    """Deserialize private key for Snowflake connection"""
    private_key = load_pem_private_key(
        private_key_str.encode(),
        password=None,
    )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def create_tables():
    """Create all necessary tables in DATA_SCIENCE schema with _CANNE suffix"""

    # Load configuration
    config = load_config('properties.json')

    # Connect to Snowflake
    logger.info("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        user=config['SNOWFLAKE_USER'],
        private_key=private_key_deserializer(config['SNOWFLAKE_PRIVATE_KEY']),
        account=config['snowflake_account_prod'],
        warehouse=config['snowflake_warehouse'],
        database=config['snowflake_database'],
        schema=config['snowflake_ds_schema'],
        role=config['snowflake_role']
    )

    cursor = conn.cursor()
    logger.info("Connected to Snowflake successfully")

    # Create CSM_ROUTING_RECOMMENDATIONS_CANNE table
    logger.info("Creating CSM_ROUTING_RECOMMENDATIONS_CANNE table...")
    create_recommendations_query = """
    CREATE TABLE IF NOT EXISTS DSV_WAREHOUSE.DATA_SCIENCE.CSM_ROUTING_RECOMMENDATIONS_CANNE (
        recommendation_id NUMBER AUTOINCREMENT PRIMARY KEY,
        account_id VARCHAR(50),
        recommended_csm VARCHAR(100),
        recommendation_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        assignment_method VARCHAR(50),
        neediness_score NUMBER,
        health_score NUMBER,
        revenue NUMBER,
        account_segment VARCHAR(50),
        account_level VARCHAR(50),
        optimization_score FLOAT,
        llm_feedback VARCHAR(500),
        was_assigned BOOLEAN DEFAULT FALSE,
        actual_assigned_csm VARCHAR(100),
        assignment_date TIMESTAMP_NTZ,
        run_id VARCHAR(50),
        batch_size NUMBER
    )
    """
    cursor.execute(create_recommendations_query)
    logger.info("✓ CSM_ROUTING_RECOMMENDATIONS_CANNE table created/verified")

    # Create ACCOUNT_CSM_ASSIGNMENTS_CANNE table
    logger.info("Creating ACCOUNT_CSM_ASSIGNMENTS_CANNE table...")
    create_assignments_query = """
    CREATE TABLE IF NOT EXISTS DSV_WAREHOUSE.DATA_SCIENCE.ACCOUNT_CSM_ASSIGNMENTS_CANNE (
        account_id VARCHAR(50) PRIMARY KEY,
        csm_name VARCHAR(100),
        assignment_date TIMESTAMP_NTZ,
        assignment_method VARCHAR(50),
        llm_review_feedback TEXT,
        last_updated TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        created_by VARCHAR(50) DEFAULT 'CSM_ROUTING_AUTOMATION'
    )
    """
    cursor.execute(create_assignments_query)
    logger.info("✓ ACCOUNT_CSM_ASSIGNMENTS_CANNE table created/verified")

    # Commit the changes
    conn.commit()
    logger.info("All tables created successfully in DSV_WAREHOUSE.DATA_SCIENCE schema with _CANNE suffix")

    # Show table information
    logger.info("\nVerifying tables...")

    cursor.execute("SHOW TABLES LIKE '%_CANNE' IN DSV_WAREHOUSE.DATA_SCIENCE")
    tables = cursor.fetchall()

    logger.info("\nTables with _CANNE suffix in DATA_SCIENCE schema:")
    for table in tables:
        logger.info(f"  - {table[1]}")

    cursor.close()
    conn.close()
    logger.info("\nSnowflake connection closed")

if __name__ == "__main__":
    try:
        create_tables()
        logger.info("\n✅ All tables created successfully!")
    except Exception as e:
        logger.error(f"\n❌ Error creating tables: {str(e)}", exc_info=True)