-- Exactly one row per (repo_id, pr_number) -- the accumulating-snapshot
-- grain, asserted on every `dbt build` rather than trusted to the merge
-- key. A broken `unique_key`, a bad incremental filter that double-counts,
-- or a full-refresh that unions the batch with itself would each produce
-- duplicate PR rows, and none of those fails loudly on its own.
--
-- A dbt test passes when this query returns zero rows.
select
    repo_id,
    pr_number,
    count(*) as row_count
from {{ ref('fact_pull_request') }}
group by repo_id, pr_number
having count(*) > 1
