import os
import pandas as pd

from extract.utils.config import TABLE_CONFIG, SOURCE_SCHEMA, TIMESTAMP_COLUMN, build_s3_key, S3_BUCKET
from extract.utils.db_utils import get_pg_connection
from extract.utils.parquet_utils import write_dataframe_to_parquet
from extract.repository import get_latest_timestamp, update_latest_timestamp
from extract.utils.s3_utils import upload_file_to_s3
from extract.utils.transform_utils import normalize_timestamp_columns
from extract.utils.logging_config import logger


def _build_extract_query(table_name: str) -> str:
    return (
        f"SELECT * FROM {SOURCE_SCHEMA}.{table_name} "
        f"WHERE {TIMESTAMP_COLUMN} > %(latest_timestamp)s "
        f"ORDER BY {TIMESTAMP_COLUMN} DESC"
    )


def _read_table(table_name: str, latest_timestamp: str) -> pd.DataFrame:
    query = _build_extract_query(table_name)
    logger.info("Reading table %s with latest_timestamp=%s", table_name, latest_timestamp)
    with get_pg_connection() as pg_conn:
        df = pd.read_sql(query, pg_conn, params={"latest_timestamp": latest_timestamp})
    return df


def _write_extract_output(table_name: str, df: pd.DataFrame, execution_date: str, run_ts: str, execution_type: str) -> dict:
    local_path = os.path.join("/tmp", f"{table_name}_{run_ts}.parquet")

    if execution_type == "s3":
        write_dataframe_to_parquet(df, local_path)
        s3_key = build_s3_key(table_name, execution_date, run_ts)
        upload_file_to_s3(local_path, S3_BUCKET, s3_key)
        os.remove(local_path)
        return {"table": table_name, "rows": len(df), "s3_key": s3_key}

    if execution_type == "local":
        write_dataframe_to_parquet(df, local_path)
        return {"table": table_name, "rows": len(df), "local_path": local_path}

    raise ValueError(f"Unsupported execution_type: {execution_type}")


def extract_table(table_name: str, execution_date: str, run_ts: str, execution_type: str = "s3") -> dict:
    latest_timestamp = get_latest_timestamp(table_name)
    logger.info("Extracting table=%s with latest_timestamp=%s", table_name, latest_timestamp)

    df = _read_table(table_name, latest_timestamp)
    logger.info("Rows extracted for %s: %s", table_name, len(df))

    if df.empty:
        logger.info("No new data for table=%s", table_name)
        return {"table": table_name, "rows": 0, "status": "no_data"}

    df = normalize_timestamp_columns(df)
    result = _write_extract_output(table_name, df, execution_date, run_ts, execution_type)

    updated_timestamp = str(df[TIMESTAMP_COLUMN].max())
    update_latest_timestamp(table_name, updated_timestamp)
    logger.info("Updated latest timestamp for table=%s to %s", table_name, updated_timestamp)

    return result


def extract_all(execution_date: str, run_ts: str, execution_type: str = "s3") -> list[dict]:
    results = []
    for table_name in TABLE_CONFIG:
        logger.info("Starting extraction for table=%s", table_name)
        results.append(extract_table(table_name, execution_date, run_ts, execution_type))
    logger.info("Completed extraction for %s tables", len(results))
    return results
