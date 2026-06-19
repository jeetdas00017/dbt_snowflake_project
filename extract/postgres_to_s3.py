"""
Extract source tables from Postgres and land them as Parquet files in S3.

Pattern: incremental "high-watermark" extraction based on `updated_at`.
  - Watermarks are stored in Snowflake (extract_watermark)
  - Each run writes ONLY new/changed rows to:
        s3://<bucket>/raw/<table>/dt=<execution_date>/<table>_<run_ts>.parquet
  - The function returns the S3 key (or "" if no new data), which is
    pushed to XCom and consumed by the load step (load/s3_to_snowflake.py)

Tables covered: customers, products, orders
"""

import os
import logging
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
import psycopg2
import snowflake.connector

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — wire these to Airflow Connections/Variables in production
# ---------------------------------------------------------------------------
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "source-postgres.internal"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DATABASE", "app_db"),
    "user": os.getenv("PG_USER", "etl_reader"),
    "password": os.getenv("PG_PASSWORD"),
}

SF_CONFIG = {
    "account": os.getenv("SF_ACCOUNT", "xy12345.region"),
    "user": os.getenv("SF_USER", "etl_user"),
    "password": os.getenv("SF_PASSWORD"),
    "database": os.getenv("SF_DATABASE", "analytics"),
    "schema": os.getenv("SF_SCHEMA", "control"),
    "warehouse": os.getenv("SF_WAREHOUSE", "compute_wh"),
    "role": os.getenv("SF_ROLE", "etl_role"),
}

S3_BUCKET = os.getenv("S3_RAW_BUCKET", "my-company-dwh-raw")
S3_PREFIX = "raw"
S3_ENDPOINT = os.getenv("S3_ENDPOINT")  # Optional custom endpoint (e.g. MinIO)


def get_s3_client():
    """Return a boto3 S3 client using explicit env creds or default chain.

    Honors `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`,
    and optional `S3_ENDPOINT` for S3-compatible services.
    """
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")

    config = Config(signature_version="s3v4")

    params = {}
    if aws_region:
        params["region_name"] = aws_region
    if S3_ENDPOINT:
        params["endpoint_url"] = S3_ENDPOINT

    if aws_key and aws_secret:
        params["aws_access_key_id"] = aws_key
        params["aws_secret_access_key"] = aws_secret

    return boto3.client("s3", config=config, **params)

# table_name -> incremental column used as the watermark
TABLE_CONFIG = {
    "customers": {"incremental_col": "updated_at"},
    "products": {"incremental_col": "updated_at"},
    "orders": {"incremental_col": "updated_at"},
}


def get_pg_connection():
    return psycopg2.connect(**PG_CONFIG)


def get_sf_connection():
    return snowflake.connector.connect(**SF_CONFIG)


def get_last_watermark(table_name: str) -> str:
    """Return the last successfully-extracted watermark for a table."""
    query = """
        SELECT COALESCE(MAX(last_extracted_at), '1900-01-01 00:00:00')
        FROM extract_watermark
        WHERE table_name = %s
    """
    conn = get_sf_connection()
    try:
        cur = conn.cursor()
        cur.execute(query, (table_name,))
        result = cur.fetchone()
        return str(result[0]) if result else '1900-01-01 00:00:00'
    finally:
        conn.close()


def update_watermark(table_name: str, watermark_value: str) -> None:
    """Persist the new high-watermark after a successful extract."""
    conn = get_sf_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"DELETE FROM extract_watermark WHERE table_name = '{table_name}';")
        cur.execute(
            f"""
            INSERT INTO extract_watermark (table_name, last_extracted_at, updated_at)
            VALUES ('{table_name}', '{watermark_value}', '{datetime.now(timezone.utc).isoformat()}');
            """
        )
        conn.commit()
    finally:
        conn.close()


def extract_table_to_s3(table_name: str, **context) -> str:
    """
    Pull rows where `updated_at` > last watermark from Postgres and write
    them to S3 as a single Parquet file (Snappy compressed).

    Returns the S3 key written, or "" if there was no new data
    (used by downstream COPY task to skip the Snowflake COPY FROM S3).
    """
    cfg = TABLE_CONFIG[table_name]
    incremental_col = cfg["incremental_col"]

    watermark = get_last_watermark(table_name)
    logger.info("Extracting '%s' where %s > %s", table_name, incremental_col, watermark)

    query = f"""
        SELECT *
        FROM {table_name}
        WHERE {incremental_col} > %(watermark)s
        ORDER BY {incremental_col} ASC
    """

    pg_conn = get_pg_connection()
    try:
        df = pd.read_sql(query, pg_conn, params={"watermark": watermark})
    finally:
        pg_conn.close()

    if df.empty:
        logger.info("No new/updated rows for '%s'. Nothing written to S3.", table_name)
        return ""

    execution_date = context["ds"]        # e.g. 2026-06-15
    run_ts = context["ts_nodash"]          # e.g. 20260615T120000

    file_name = f"{table_name}_{run_ts}.parquet"
    local_path = f"/tmp/{file_name}"
    s3_key = f"{S3_PREFIX}/{table_name}/dt={execution_date}/{file_name}"

    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, local_path, compression="snappy")

    s3_client = get_s3_client()
    logger.info("Uploading to S3 bucket=%s key=%s endpoint=%s", S3_BUCKET, s3_key, S3_ENDPOINT)
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)
    os.remove(local_path)

    logger.info("Wrote %s row(s) for '%s' to s3://%s/%s", len(df), table_name, S3_BUCKET, s3_key)

    new_watermark = df[incremental_col].max()
    update_watermark(table_name, str(new_watermark))

    return s3_key


# ---------------------------------------------------------------------------
# Standalone run: python postgres_to_s3.py <table_name>
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys

    table = sys.argv[1] if len(sys.argv) > 1 else "customers"
    now = datetime.utcnow()
    fake_context = {"ds": now.strftime("%Y-%m-%d"), "ts_nodash": now.strftime("%Y%m%dT%H%M%S")}
    result = extract_table_to_s3(table, **fake_context)
    print(f"S3 key written: {result or '(no new data)'}")
