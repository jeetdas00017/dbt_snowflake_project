from datetime import datetime, timezone

from extract.utils.db_utils import get_sf_connection
from extract.utils.config import SOURCE_SCHEMA, TIMESTAMP_COLUMN, control_table_name
from extract.utils.logging_config import logger


def get_latest_timestamp(table_name: str) -> str:
    logger.info("Fetching latest timestamp for table=%s", table_name)
    conn = get_sf_connection()

    try:
        query = (
            f"SELECT MAX(last_extracted_at) as latest_timestamp "
            f"FROM {control_table_name()} "
            f"WHERE table_name = %s"
        )
        cur = conn.cursor()
        cur.execute(query, (table_name,))
        result = cur.fetchone()
        latest_timestamp = str(result[0]) if result and result[0] is not None else "1900-01-01"

        logger.info("Latest timestamp for %s: %s", table_name, latest_timestamp)
        return latest_timestamp
    finally:
        conn.close()


def update_latest_timestamp(table_name: str, latest_timestamp_value: str):
    logger.info("Updating latest timestamp for table=%s", table_name)
    conn = get_sf_connection()

    try:
        cur = conn.cursor()
        cur.execute(
            f"DELETE FROM {control_table_name()} WHERE table_name = %s",
            (table_name,),
        )
        cur.execute(
            f"INSERT INTO {control_table_name()} (table_name, last_extracted_at, updated_at) VALUES (%s, %s, %s)",
            (table_name, latest_timestamp_value, datetime.now(timezone.utc)),
        )
        conn.commit()
        logger.info("Timestamp updated successfully for table=%s", table_name)
    finally:
        conn.close()
