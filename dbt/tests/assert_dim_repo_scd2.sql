-- Exactly one current row (dbt_valid_to is null) per repo_id (§10 SCD2). A broken
-- check_cols edit, a hash collision, or a rerun on stale staging yields zero
-- current rows (invisible downstream) or more than one (an ambiguous as-of join).
select
    repo_id,
    count(*) as current_row_count
from {{ ref('dim_repo') }}
where dbt_valid_to is null
group by repo_id
having count(*) != 1
