#!/usr/bin/env python
"""
Simple Postgres connection test.
Usage: python test_postgres_connection.py
"""
import os
import sys
from pathlib import Path
from datetime import datetime

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# load .env if present
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"Loaded .env from: {env_path}")
except ImportError:
    pass

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DATABASE", "app_db"),
    "user": os.getenv("PG_USER", "etl_reader"),
    "password": os.getenv("PG_PASSWORD", ""),
}

print("="*60)
print("Postgres Connection Test")
print("="*60)
print(f"Timestamp: {datetime.now().isoformat()}")
print()

missing = [k for k,v in PG_CONFIG.items() if v in (None, "", 0)]
if missing:
    print("❌ Missing Postgres configuration:")
    for k in missing:
        print(f"  - {k}")
    sys.exit(1)

print(f"Connecting to Postgres at {PG_CONFIG['host']}:{PG_CONFIG['port']}/{PG_CONFIG['dbname']} as {PG_CONFIG['user']}")
try:
    conn = psycopg2.connect(**PG_CONFIG)
    cur = conn.cursor()
    cur.execute('SELECT version();')
    ver = cur.fetchone()[0]
    print("✅ Connected. Server version:", ver)

    # Check for customers table existence
    cur.execute("SELECT to_regclass('public.customers')")
    exists = cur.fetchone()[0]
    if exists:
        print("✅ 'customers' table exists: public.customers")
    else:
        print("⚠ 'customers' table not found (public.customers)")

    cur.close()
    conn.close()
    print("\n✅ Postgres connection test PASSED")
    sys.exit(0)
except Exception as e:
    print("\n❌ Postgres connection test FAILED")
    print(type(e).__name__, str(e))
    sys.exit(1)
