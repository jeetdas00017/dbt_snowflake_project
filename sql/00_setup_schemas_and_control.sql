-- =====================================================================
-- 00_setup_schemas_and_control.sql
-- Run once during environment setup (e.g. via a one-time Airflow task
-- or a migration tool such as Flyway / sqlfluff / dbt run-operation).
-- =====================================================================

-- Landing zone for the latest incremental batch from S3
CREATE SCHEMA IF NOT EXISTS stg;

-- Operational metadata / watermarks for the extractor
CREATE SCHEMA IF NOT EXISTS etl_control;

-- ---------------------------------------------------------------------
-- Watermark table used by extract/postgres_to_s3.py to track the last
-- successfully extracted `updated_at` per source table.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS etl_control.extract_watermark (
    table_name        VARCHAR(100)  NOT NULL,
    last_extracted_at TIMESTAMP     NOT NULL,
    updated_at        TIMESTAMP     NOT NULL,
    PRIMARY KEY (table_name)
);

-- Seed initial watermarks so the first run does a full extract
INSERT INTO etl_control.extract_watermark (table_name, last_extracted_at, updated_at)
SELECT t.table_name, '1900-01-01 00:00:00'::timestamp, getdate()
FROM (VALUES ('customers'), ('products'), ('orders')) AS t(table_name)
WHERE NOT EXISTS (
    SELECT 1 FROM etl_control.extract_watermark w WHERE w.table_name = t.table_name
);
