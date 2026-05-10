# Pipeline Run Log
## Ticket 6.1: pipeline_run_log Metadata Table

The pipeline_run_log table is an append-only audit trail that tracks
every pipeline task execution. Think of it as the flight recorder for
the pipeline. Every task writes one row when it completes.

---

## Table

`metadata.pipeline_run_log`

## Fields

| Field | Type | Description |
|---|---|---|
| run_id | STRING | Unique identifier for this pipeline run. Convention: "{execution_date}-{task_name}" |
| execution_date | DATE | The logical date being processed |
| task_name | STRING | Name of the pipeline task (e.g. ingest_raw, build_staging) |
| row_count_input | INTEGER | Rows available at task start |
| row_count_output | INTEGER | Rows written or merged at task end |
| status | STRING | PASS or FAIL only |
| timestamp | TIMESTAMP | UTC timestamp when this log entry was written |

## Design Decisions

- Append-only. No updates, no deletes, no merges.
- No partitioning for MVP. Table is small and infrequently queried.
- Status is validated before insert. Any value other than PASS or FAIL raises an error.
- Timestamp is set at write time in UTC.

## Usage

```python
from pipelines.utils.log_run import log_run

log_run(
    run_id="2024-01-01-ingest-raw",
    execution_date="2024-01-01",
    task_name="ingest_raw",
    row_count_input=0,
    row_count_output=15264403,
    status="PASS",
)
```

## Example Query

```sql
SELECT *
FROM `project-id.metadata.pipeline_run_log`
ORDER BY timestamp DESC
LIMIT 20
```

## Query Recent Failures

```sql
SELECT run_id, execution_date, task_name, timestamp
FROM `project-id.metadata.pipeline_run_log`
WHERE status = 'FAIL'
ORDER BY timestamp DESC
LIMIT 10
```
