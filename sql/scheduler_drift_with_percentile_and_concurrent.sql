-- Scheduler Drift by Hour by including avg_count_per_minute
WITH scheduler_drift_by_task AS (
    SELECT task_id,
           dag_id,
           date_trunc('hour', start_date) as sd_hour,
           EXTRACT(EPOCH FROM start_date - queued_dttm) AS scheduler_drift
    FROM task_instance
), agg_drift_by_hour AS (
    select
        sd_hour,
        percentile_cont(0.50) within group (order by scheduler_drift) as p50,
        percentile_cont(0.90) within group (order by scheduler_drift) as p90,
        percentile_cont(0.99) within group (order by scheduler_drift) as p99,
        max(scheduler_drift) as max
    from scheduler_drift_by_task
    group by sd_hour
), tasks_scheduled_per_minute AS (
    SELECT
        date_trunc('hour', sd_min) as sd_hour,
        avg(c) as avg_scheduled_per_minute,
        sum(c) as total_scheduled
    FROM (
         SELECT date_trunc('minute', start_date) as sd_min,
                count(1) as c
         FROM task_instance
         GROUP BY sd_min
    ) as sq
    GROUP BY sd_hour
)
SELECT adbh.sd_hour, p50, p90, p99, max, avg_scheduled_per_minute, total_scheduled
FROM agg_drift_by_hour adbh
JOIN tasks_scheduled_per_minute tspm ON adbh.sd_hour = tspm.sd_hour
ORDER BY 1;
