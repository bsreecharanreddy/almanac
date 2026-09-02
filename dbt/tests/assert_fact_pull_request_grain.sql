-- Exactly one row per (repo_id, pr_number) -- the accumulating-snapshot grain.
-- A broken unique_key, a double-counting incremental filter, or a self-unioning
-- full-refresh each produce duplicate PR rows without failing loudly.
select
    repo_id,
    pr_number,
    count(*) as row_count
from {{ ref('fact_pull_request') }}
group by repo_id, pr_number
having count(*) > 1
