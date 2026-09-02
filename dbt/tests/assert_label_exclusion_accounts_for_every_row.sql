-- Every PR row is either in the label or carries a stated reason it is
-- not -- `label_exclusion` is null exactly when
-- `time_to_first_response_seconds` is non-null. A row that is null on both
-- is a silently missing label (§5.1: exclusions are stated, never a gap).
--
-- A dbt test passes when this query returns zero rows.
select
    repo_id,
    pr_number,
    label_exclusion,
    time_to_first_response_seconds
from {{ ref('fact_pull_request') }}
where (label_exclusion is null) != (time_to_first_response_seconds is not null)
