{% snapshot dim_repo %}
{{
    config(
        strategy='check',
        unique_key='repo_id',
        check_cols=['repo_name'],
        file_format='delta',
    )
}}

-- SCD2 on repo identity. `repo_id` is the natural key -- design doc §12
-- trap 7 measured it stable across all three schema eras (1,997/2,000
-- legacy, 2,000/2,000 modern), while `repo_name` is exactly the column
-- that changes (trap 8: 5,757 renames measured in a 24-hour sample, at
-- least one repo renamed twice inside the window).
--
-- `check_cols=['repo_name']` uses dbt's stock `check` strategy, and its
-- generated comparison is already null-safe: it explicitly OR's in both
-- null-to-value and value-to-null transitions rather than a bare `!=`, so
-- a repo whose name goes null (a deleted repo, §12 trap 11) closes its
-- current row as a real transition instead of being silently ignored.
--
-- Case-sensitive on purpose, and no `lower()` may be added here: Spark
-- string comparison already is case-sensitive, and a case-only rename
-- (`GLB` -> `glb`) is measured to exist and must be detected, not folded
-- away as a no-op.
--
-- One row per `repo_id` "as of this run": ranked by `created_at` so the
-- most recently observed name wins, with `event_id` as a deterministic
-- tie-break for two events landing at the identical instant.
with ranked as (

    select
        repo_id,
        repo_name,
        created_at,
        row_number() over (
            partition by repo_id
            order by created_at desc, event_id desc
        ) as rn
    from {{ source('silver', 'events') }}
    where repo_id is not null

)

select
    repo_id,
    repo_name
from ranked
where rn = 1

{% endsnapshot %}
