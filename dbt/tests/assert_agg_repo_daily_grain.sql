-- Exactly one row per (repo_id, activity_date) -- the daily grain,
-- asserted on every `dbt build`. A bad incremental filter that
-- double-counts a day, or a full-refresh that unions the batch with
-- itself, would each duplicate a repo-day without failing loudly.
--
-- A dbt test passes when this query returns zero rows.
select
    repo_id,
    activity_date,
    count(*) as row_count
from {{ ref('agg_repo_daily') }}
group by repo_id, activity_date
having count(*) > 1
