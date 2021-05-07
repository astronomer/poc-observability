# Observability + Metrics

## Goal 
- Assist metrics based refactorings.
- Get a deeper view into issues with core airflow components.

# Notes
First pass - sql w/ `task_instance` table
Second pass - log aggregation
https://github.com/apache/airflow/blob/1.10.14/airflow/jobs/scheduler_job.py#L1127-L1130
https://www.notion.so/astronomerio/Worker-Performance-a9ff26765b384f20a50a8504456c7c78
