import os
from dotenv import load_dotenv

load_dotenv()


def _get_env(name: str, default=None, required: bool = False):
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Environment variable {name} is required")
    return value


PG_CONFIG = {
    "host": _get_env("PG_HOST", "postgres_dw"),
    "port": _get_env("PG_PORT", "5432"),
    "dbname": _get_env("PG_DATABASE", required=True),
    "user": _get_env("PG_USER", required=True),
    "password": _get_env("PG_PASSWORD", required=True),
}

SF_CONFIG = {
    "account": _get_env("SF_ACCOUNT", required=True),
    "user": _get_env("SF_USER", required=True),
    "password": _get_env("SF_PASSWORD", required=True),
    "database": _get_env("SF_DATABASE", required=True),
    "schema": _get_env("SF_SCHEMA", required=True),
    "warehouse": _get_env("SF_WAREHOUSE", required=True),
    "role": _get_env("SF_ROLE", required=True),
}

S3_BUCKET = _get_env("S3_RAW_BUCKET", required=True)
S3_PREFIX = _get_env("S3_RAW_PREFIX", "raw")
S3_ENDPOINT = _get_env("S3_ENDPOINT")
S3_REGION = _get_env("AWS_DEFAULT_REGION")
AWS_ACCESS_KEY_ID = _get_env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _get_env("AWS_SECRET_ACCESS_KEY")

SOURCE_SCHEMA = _get_env("PG_SOURCE_SCHEMA", "postgres_table")
TIMESTAMP_COLUMN = _get_env("PG_TIMESTAMP_COLUMN", "updated_at")
CONTROL_SCHEMA = _get_env("ETL_CONTROL_SCHEMA", "ETL_CONTROL")
CONTROL_TABLE = _get_env("ETL_CONTROL_TABLE", "extract_latest_timestamp")
TABLE_CONFIG = tuple(
    value.strip()
    for value in _get_env("ETL_TABLES", "customers,products,orders").split(",")
    if value.strip()
)

PARQUET_COMPRESSION = _get_env("PARQUET_COMPRESSION", "snappy")
PARQUET_TIMESTAMP_UNIT = _get_env("PARQUET_TIMESTAMP_UNIT", "us")


def build_s3_key(table_name: str, execution_date: str, run_ts: str) -> str:
    return f"{S3_PREFIX}/{table_name}/dt={execution_date}/{table_name}_{run_ts}.parquet"


def control_table_name() -> str:
    return f"{CONTROL_SCHEMA}.{CONTROL_TABLE}"
