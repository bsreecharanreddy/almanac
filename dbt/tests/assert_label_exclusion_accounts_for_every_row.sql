-- label_exclusion is null exactly when time_to_first_response_seconds is non-null:
-- every PR is either in the label or carries a stated reason it is not. Null on
-- both is a silently missing label (§5.1: exclusions are stated, never a gap).
select
    repo_id,
    pr_number,
    label_exclusion,
    time_to_first_response_seconds
from {{ ref('fact_pull_request') }}
where (label_exclusion is null) != (time_to_first_response_seconds is not null)
