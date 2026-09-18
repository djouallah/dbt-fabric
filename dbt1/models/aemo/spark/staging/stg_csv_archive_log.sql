-- Spark view over the archive log download_aemo.py writes to the landing lakehouse.
-- Fabric Spark 3.5 has no read_files(), so read parquet with the path datasource syntax
-- (parquet.`path`). A VIEW, like duckrun/ducklake/dwh: the read needs no materialization, and
-- a table here was one more copy of the log to keep in step with the parquet for nothing.
-- (iceberg is the one engine that must materialize it — its catalog has no CREATE VIEW.)
{{ config(materialized='view', schema='landing') }}

SELECT
    source_type,
    source_filename,
    archive_path,
    archived_at,
    row_count,
    source_url,
    etag,
    csv_filename
FROM parquet.`{{ get_root_path() }}/csv_raw_archive_log.parquet`
