-- Exactly one `dbt_valid_to IS NULL` per `repo_id`, always -- Task 3 Step 1,
-- design doc §10's SCD2 goal. Asserted as an invariant on every `dbt
-- build`/`dbt test`, not assumed from a snapshot run looking clean: a
-- broken `check_cols` edit, a hash collision, or a rerun against a stale
-- staging table would produce either zero current rows for a repo
-- (silently invisible downstream) or more than one (an ambiguous "as of
-- now" join) -- neither of which fails loudly on its own.
--
-- A dbt test passes when this query returns zero rows.
select
    repo_id,
    count(*) as current_row_count
from {{ ref('dim_repo') }}
where dbt_valid_to is null
group by repo_id
having count(*) != 1
