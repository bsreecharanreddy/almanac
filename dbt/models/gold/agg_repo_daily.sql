{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['repo_id', 'activity_date'],
        file_format='delta',
    )
}}

{#- `contract: enforced` + `on_schema_change: fail` are set in schema.yml. -#}

{#-
  Daily activity per repo, on **event time** (`created_at`), not ingest
  time. Same incremental strategy as `fact_pull_request`: any (repo, date)
  touched by a new event is recomputed in full from `silver.events`, so a
  late event for an earlier day corrects that day's row rather than being
  added to a stale total or double-counted on a re-run.

  `stars_gained`, never a running total: `WatchEvent` *is* a star (§12
  trap 1 -- GitHub renamed the feature in 2012, never the event), and
  there is no un-star event (trap 2), so a cumulative star count is not
  derivable from this stream and "gained" is the only honest name.

  `commits_pushed` sums `payload.size` (Silver's `push_size`), never
  `size(commits)` -- the `commits` array is capped at 20 (trap 3), so a
  1,000-commit push would read as 20.
-#}

{%- set agg_columns -%}
    repo_id,
    activity_date,
    events_total,
    bot_events,
    stars_gained,
    forks,
    prs_opened,
    prs_closed,
    issues_opened,
    pushes,
    commits_pushed,
    distinct_commits_pushed,
    last_ingested_at
{%- endset -%}

with events as (
    select
        repo_id,
        to_date(created_at) as activity_date,
        ingested_at,
        actor_login,
        event_type,
        event_action,
        push_size,
        push_distinct_size
    from {{ source('silver', 'events') }}
    where repo_id is not null
),

{% if is_incremental() %}

touched as (
    select distinct repo_id, activity_date
    from events
    where ingested_at > (
        select coalesce(max(last_ingested_at), to_timestamp('1970-01-01 00:00:00'))
        from {{ this }}
    )
),

scope as (
    select events.* from events join touched using (repo_id, activity_date)
),

{% else %}

scope as ( select * from events ),

{% endif %}

daily as (
    select
        repo_id,
        activity_date,
        count(*) as events_total,
        count_if(coalesce({{ almanac_is_bot('actor_login') }}, false)) as bot_events,
        count_if(event_type = 'WatchEvent') as stars_gained,
        count_if(event_type = 'ForkEvent') as forks,
        count_if(event_type = 'PullRequestEvent' and event_action = 'opened') as prs_opened,
        count_if(event_type = 'PullRequestEvent' and event_action = 'closed') as prs_closed,
        count_if(event_type = 'IssuesEvent' and event_action = 'opened') as issues_opened,
        count_if(event_type = 'PushEvent') as pushes,
        coalesce(sum(if(event_type = 'PushEvent', push_size, 0)), 0) as commits_pushed,
        coalesce(sum(if(event_type = 'PushEvent', push_distinct_size, 0)), 0)
            as distinct_commits_pushed,
        max(ingested_at) as last_ingested_at
    from scope
    group by repo_id, activity_date
)

select {{ agg_columns }} from daily

{% if is_incremental() %}
union all
select {{ agg_columns }}
from {{ this }} as prior
where not exists (
    select 1 from touched
    where touched.repo_id = prior.repo_id and touched.activity_date = prior.activity_date
)
{% endif %}
