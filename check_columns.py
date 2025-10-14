import json
import snowflake.connector
import logging
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load config
with open('properties.json', 'r') as f:
    config = json.load(f)

# Parse private key
private_key = load_pem_private_key(
    config['SNOWFLAKE_PRIVATE_KEY'].encode('utf-8'),
    password=None,
    backend=default_backend()
)

# Connect
conn = snowflake.connector.connect(
    user=config['SNOWFLAKE_USER'],
    private_key=private_key,
    account=config['snowflake_account_prod'],
    warehouse=config['snowflake_warehouse'],
    database='DSV_SHARE',  # Use the right database
    schema='PUBLIC',
    role=config['snowflake_role']
)

cursor = conn.cursor()

# Check columns in VW_ONBOARDING_DETAIL
query = """
DESCRIBE TABLE DSV_SHARE.PUBLIC.VW_ONBOARDING_DETAIL
"""

cursor.execute(query)
columns = cursor.fetchall()

print("\nColumns in VW_ONBOARDING_DETAIL:")
for col in columns:
    print(f"  - {col[0]}")

cursor.close()
conn.close()
