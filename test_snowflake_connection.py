#!/usr/bin/env python
"""
Test script to verify Snowflake connection.
Usage: python test_snowflake_connection.py
"""

import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python not installed.")
    print("Run: pip install snowflake-connector-python")
    sys.exit(1)

# Load .env file if it exists
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"Loaded .env from: {env_path}")
except ImportError:
    pass

# Load config from environment variables
SF_CONFIG = {
    "account": os.getenv("SF_ACCOUNT"),
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "database": os.getenv("SF_DATABASE"),
    "schema": os.getenv("SF_SCHEMA"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
}

print("=" * 70)
print("Snowflake Connection Test")
print("=" * 70)
print(f"Timestamp: {datetime.now().isoformat()}")
print()

# Check if required env vars are set
required_vars = ["SF_ACCOUNT", "SF_USER", "SF_PASSWORD"]
missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    print("❌ ERROR: Missing required environment variables:")
    for var in missing_vars:
        print(f"   - {var}")
    print()
    print("Please set these in your .env file or environment.")
    sys.exit(1)

print("✓ All required environment variables are set")
print(f"  DEBUG: SF_ACCOUNT = {os.getenv('SF_ACCOUNT')}")
print()

# Try to connect
print("Connecting to Snowflake...")
print(f"  Account:   {SF_CONFIG['account']}")
print(f"  User:      {SF_CONFIG['user']}")
print(f"  Database:  {SF_CONFIG['database']}")
print(f"  Schema:    {SF_CONFIG['schema']}")
print(f"  Warehouse: {SF_CONFIG['warehouse']}")
print()

try:
    conn = snowflake.connector.connect(**SF_CONFIG)
    print("✅ Successfully connected to Snowflake!")
    print()

    # Test a simple query
    print("Running test query: SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();")
    cur = conn.cursor()
    cur.execute("SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();")
    result = cur.fetchone()
    
    print(f"  Current User:      {result[0]}")
    print(f"  Current Role:      {result[1]}")
    print(f"  Current Warehouse: {result[2]}")
    print()

    # Check if extract_watermark table exists
    print("Checking for 'extract_watermark' table...")
    cur.execute(
        f"""
        SELECT EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = 'EXTRACT_WATERMARK' 
            AND table_schema = '{SF_CONFIG['schema'].upper()}'
            AND table_catalog = '{SF_CONFIG['database'].upper()}'
        ) AS table_exists
        """
    )
    exists = cur.fetchone()[0]
    if exists:
        print("✅ extract_watermark table exists")
    else:
        print("⚠️  extract_watermark table NOT found")
        print(f"    (Expected in {SF_CONFIG['database']}.{SF_CONFIG['schema']})")
    
    print()
    cur.close()
    conn.close()
    print("=" * 70)
    print("✅ Snowflake connection test PASSED")
    print("=" * 70)
    sys.exit(0)

except Exception as e:
    print()
    print("=" * 70)
    print(f"❌ Snowflake connection test FAILED")
    print("=" * 70)
    print(f"Error: {type(e).__name__}: {str(e)}")
    print()
    print("Troubleshooting tips:")
    print("  1. Verify SF_ACCOUNT format: '<account_id>.<region>' (e.g., 'xy12345.us-east-1')")
    print("  2. Check SF_USER and SF_PASSWORD are correct")
    print("  3. Ensure Snowflake user has permissions to access the database/schema")
    print("  4. Verify network connectivity to Snowflake")
    sys.exit(1)
