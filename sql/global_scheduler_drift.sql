-- Global Scheduler Drift Forever
WITH data AS (
    select task_id,
           dag_id,
           start_date,
           extract(epoch from start_date - queued_dttm) as scheduler_drift
    from task_instance
)
SELECT
    avg(scheduler_drift),
    PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY scheduler_drift) as p50,
    PERCENTILE_CONT(0.9) WITHIN GROUP(ORDER BY scheduler_drift) as p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP(ORDER BY scheduler_drift) as p99,
    max(scheduler_drift)
FROM data;