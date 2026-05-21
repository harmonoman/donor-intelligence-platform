-- Reference only. Table is created programmatically via ensure_log_table_exists()
-- in pipelines/utils/log_run.py. This file documents the intended schema.
-- Do not execute this file directly.

-- Metadata Table: pipeline_run_log
--
-- Append-only audit trail for every pipeline task execution.
-- Think of this as the flight recorder for the pipeline.
-- Every task writes one row. Nothing is ever updated or deleted.
--
-- Table is NOT partitioned for MVP.
-- Query pattern: ORDER BY timestamp DESC to see recent runs.
--
-- Fields:
--   run_id           Unique identifier for this pipeline run
--   execution_date   The logical date being processed (from --execution-date)
--   task_name        Name of the pipeline task (e.g. ingest_raw, build_staging)
--   row_count_input  Rows read or available at task start
--   row_count_output Rows written or merged at task end
--   status           PASS or FAIL only
--   timestamp        UTC timestamp when this log entry was written

CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.metadata.pipeline_run_log` (
    run_id           STRING    NOT NULL,
    execution_date   DATE      NOT NULL,
    task_name        STRING    NOT NULL,
    row_count_input  INTEGER,
    row_count_output INTEGER,
    status           STRING    NOT NULL,
    timestamp        TIMESTAMP NOT NULL
)
