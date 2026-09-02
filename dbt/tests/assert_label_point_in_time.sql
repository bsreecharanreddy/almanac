-- The label never reads an event at or after its own `as_of` (design doc
-- §2, §5.1). Concretely, on every `dbt build`:
--   * a first response cannot predate the PR opening
--   * the label (seconds from open to first response) is never negative
-- A row here is a leak: `first_response_at` matched an event that is not
-- actually a response to this PR -- most likely legacy issue/PR number
-- reuse slipping through `int_pr_events`.
--
-- A dbt test passes when this query returns zero rows.
select
    repo_id,
    pr_number,
    opened_at,
    first_response_at,
    time_to_first_response_seconds
from {{ ref('fact_pull_request') }}
where (first_response_at is not null and opened_at is not null and first_response_at < opened_at)
   or time_to_first_response_seconds < 0
