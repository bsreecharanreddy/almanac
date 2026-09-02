-- Exactly one row per (repo_id, activity_date). A bad incremental filter or
-- a self-unioning full-refresh would duplicate a repo-day without failing.
select
    repo_id,
    activity_date,
    count(*) as row_count
from {{ ref('agg_repo_daily') }}
group by repo_id, activity_date
having count(*) > 1
