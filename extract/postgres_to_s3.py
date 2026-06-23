import os
import time
import logging
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.config import Config
import psycopg2
import snowflake.connector
from dotenv import load_dotenv

# Load .env file
load_dotenv()

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
PG_CONFIG = {
    "host": os.getenv("PG_HOST", "postgres_dw"),
    "port": os.getenv("PG_PORT", "5432"),
    "dbname": os.getenv("PG_DATABASE"),
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
}

SF_CONFIG = {
    "account": os.getenv("SF_ACCOUNT"),
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "database": os.getenv("SF_DATABASE"),
    "schema": os.getenv("SF_SCHEMA"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
}

S3_BUCKET = os.getenv("S3_RAW_BUCKET")
S3_PREFIX = os.getenv("S3_RAW_PREFIX")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")

TABLE_CONFIG = ("customers", "products", "orders")


# ---------------------------------------------------------------------
# Utility Functions
# ---------------------------------------------------------------------
def get_s3_client():
    logger.info("Creating S3 client")

    params = {}
    if os.getenv("AWS_DEFAULT_REGION"):
        params["region_name"] = os.getenv("AWS_DEFAULT_REGION")

    if os.getenv("AWS_ACCESS_KEY_ID"):
        params["aws_access_key_id"] = os.getenv("AWS_ACCESS_KEY_ID")
        params["aws_secret_access_key"] = os.getenv("AWS_SECRET_ACCESS_KEY")

    return boto3.client(
        "s3",
        config=Config(signature_version="s3v4"),
        **params
    )


def get_pg_connection():
    logger.info("Opening PostgreSQL connection")
    logger.info("PG_HOST=%s", PG_CONFIG["host"])
    logger.info("PG_PORT=%s", PG_CONFIG["port"])
    logger.info("PG_DATABASE=%s", PG_CONFIG["dbname"])
    logger.info("PG_USER=%s", PG_CONFIG["user"])
    return psycopg2.connect(**PG_CONFIG)


def get_sf_connection():
    logger.info("Opening Snowflake connection")
    logger.info("SF_USER=%s", os.getenv("SF_USER"))
    logger.info("SF_ACCOUNT=%s", os.getenv("SF_ACCOUNT"))

    return snowflake.connector.connect(**SF_CONFIG)


def normalize_timestamp_columns(df: pd.DataFrame) -> pd.DataFrame:
    timestamp_cols = [
        col for col in df.columns
        if col.lower().endswith(("_at", "_date"))
    ]

    if not timestamp_cols:
        return df

    logger.info("Normalizing timestamp columns for parquet: %s", timestamp_cols)

    for col in timestamp_cols:
        if col not in df.columns:
            continue

        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            parsed = pd.to_datetime(df[col], errors="coerce", utc=False)
            if parsed.isna().all() and df[col].notna().any():
                logger.warning(
                    "Timestamp parse produced all nulls for %s; original dtype=%s",
                    col,
                    df[col].dtype,
                )
            elif parsed.isna().sum() > 0:
                logger.warning(
                    "Timestamp parse produced %s nulls for %s",
                    parsed.isna().sum(),
                    col,
                )
            df[col] = parsed

        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)

        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].astype("datetime64[us]")
            except Exception as exc:
                logger.warning(
                    "Failed to cast column %s to datetime64[us]: %s",
                    col,
                    exc,
                )
        else:
            try:
                df[col] = df[col].astype("datetime64[us]")
            except Exception as exc:
                logger.warning(
                    "Failed to downcast column %s to datetime64[us]: %s",
                    col,
                    exc,
                )

    return df


# ---------------------------------------------------------------------
# Latest Timestamp Functions
# ---------------------------------------------------------------------
def get_latest_timestamp(table_name: str) -> str:
    logger.info("Fetching latest timestamp for table=%s", table_name)

    conn = get_sf_connection()

    try:
        query = """
            SELECT MAX(last_extracted_at) as latest_timestamp
            FROM ETL_CONTROL.extract_latest_timestamp
            WHERE table_name = %s
        """

        cur = conn.cursor()
        cur.execute(query, (table_name))
        result = cur.fetchone()

        latest_timestamp = (str(result[0]))

        logger.info(
            "Latest Code Processed for %s at %s",table_name,latest_timestamp)

        return latest_timestamp

    finally:
        conn.close()


def update_latest_timestamp(table_name: str, latest_timestamp_value: str):

    logger.info("Updating timestamp | table=%s | latest_timestamp=%s",table_name,latest_timestamp_value)

    conn = get_sf_connection()

    try:
        cur = conn.cursor()

        cur.execute("DELETE FROM ETL_CONTROL.extract_latest_timestamp WHERE table_name = %s",(table_name))

        cur.execute(
            """INSERT INTO ETL_CONTROL.extract_latest_timestamp(table_name,last_extracted_at,updated_at) VALUES (%s, %s, %s)""",
            (table_name,latest_timestamp_value,datetime.now(timezone.utc))
        )

        conn.commit()

        logger.info(
            "Timestamp updated successfully for table=%s",
            table_name
        )

    finally:
        conn.close()


# ---------------------------------------------------------------------
# Main Extract Function
# ---------------------------------------------------------------------
def extract_table_to_s3(**context):

    start_time = time.time()

    logger.info("=" * 80)
    logger.info("STARTING EXTRACTION")

    s3_keys = []

    try:
        for tablename in TABLE_CONFIG: 
            logger.info("current tablename = {}".format(tablename))

            latest_timestamp = get_latest_timestamp(tablename)

            logger.info(
                "Extracting records for table=%s where %s > %s",tablename,"updated_at",latest_timestamp
                )

            query = f"""SELECT * FROM postgres_table.{tablename} WHERE updated_at > %(latest_timestamp)s ORDER BY updated_at desc"""

            query_start = time.time()

            pg_conn = get_pg_connection()

            try:
                df = pd.read_sql(query,pg_conn,params={"latest_timestamp": latest_timestamp})

            finally:
                pg_conn.close()

            logger.info("Query completed in %.2f sec",time.time() - query_start)

            logger.info("Rows extracted: %s",len(df))

            if df.empty:
                logger.info("No new data found for table=%s", tablename)
                continue

            df = normalize_timestamp_columns(df)

            execution_date = context["ds"]
            run_ts = context["ts_nodash"]

            file_name = f"{tablename}_{run_ts}.parquet"
            local_path = f"/tmp/{file_name}"

            s3_key = (f"{S3_PREFIX}/"f"{tablename}/"f"dt={execution_date}/"f"{file_name}")

            logger.info("Creating parquet file: %s",local_path)

            parquet_start = time.time()

            table = pa.Table.from_pandas(df)

            pq.write_table(
                table,
                local_path,
                compression="snappy",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
                use_deprecated_int96_timestamps=False,
            )

            logger.info("Parquet generation completed in %.2f sec",time.time() - parquet_start)

            logger.info("Uploading file to s3://%s/%s",S3_BUCKET,s3_key)

            upload_start = time.time()

            s3_client = get_s3_client()

            s3_client.upload_file(local_path,S3_BUCKET,s3_key)

            logger.info("Upload completed in %.2f sec",time.time() - upload_start)

            os.remove(local_path)

            logger.info("Local file removed: %s",local_path)

            updated_timestamp = str(df["updated_at"].max())

            logger.info("Max updated timestamp extracted: %s",updated_timestamp)

            update_latest_timestamp(tablename,updated_timestamp)

            logger.info("SUCCESS | table=%s | rows=%s | duration=%.2f sec",tablename,len(df),time.time() - start_time)

            logger.info("=" * 80)

            s3_keys.append(
                {
                    "table": tablename,
                    "s3_key": s3_key
                }
            )
        logger.info("=" * 80)
        logger.info("ALL TABLES COMPLETED")
        logger.info("Files Created=%s", s3_keys)

        return s3_keys


    except Exception as e:

        logger.exception("FAILED | table=%s | error=%s",tablename,str(e))
        raise

