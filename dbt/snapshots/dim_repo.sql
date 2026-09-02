{% snapshot dim_repo %}
{{
    config(
        strategy='check',
        unique_key='repo_id',
        check_cols=['repo_name'],
        file_format='delta',
    )
}}

-- SCD2 on repo identity: `repo_id` is the natural key (§12 trap 7, stable
-- across all three eras), `repo_name` is what changes (trap 8, 5,757
-- renames in 24h). dbt's stock `check` strategy is already null-safe for a
-- name going null (deleted repo, trap 11). Case-sensitive, no `lower()`: a
-- case-only rename (`GLB` -> `glb`) is measured to exist and must be
-- detected. One row per repo as of this run, newest name first, `event_id`
-- as the tie-break.
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
