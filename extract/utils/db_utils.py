import psycopg2
import snowflake.connector

from extract.utils.config import PG_CONFIG, SF_CONFIG
from extract.utils.logging_config import logger


def get_pg_connection():
    logger.info("Opening PostgreSQL connection")
    logger.debug("PG_CONFIG=%s", {k: v for k, v in PG_CONFIG.items() if k != "password"})
    return psycopg2.connect(**PG_CONFIG)


def get_sf_connection():
    logger.info("Opening Snowflake connection")
    logger.debug("SF_CONFIG=%s", {k: v for k, v in SF_CONFIG.items() if k != "password"})
    return snowflake.connector.connect(**SF_CONFIG)
