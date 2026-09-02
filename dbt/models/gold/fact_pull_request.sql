{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['repo_id', 'pr_number'],
        file_format='delta',
        on_schema_change='append_new_columns',
    )
}}

-- One row per pull request, as an **accumulating snapshot**: columns fill
-- in as lifecycle events arrive, and the earliest timestamp always wins
-- when batches land out of order (design doc §4.3).
--
-- Every column is derived from an **event-level field** -- the event's own
-- `created_at`, `actor_login`, `event_type`, `event_action` -- never from
-- the `payload.pull_request` object nested inside the event (§4.3a). That
-- object was cut from 48 fields to 5 in October 2025 (§12 trap 12); a
-- payload-native build stops working there, an event-native one does not.
-- `merged` and `draft` are the one documented exception: they are not
-- recoverable from events at all, so they come from Silver's already-parsed
-- `pr_merged` / `pr_draft` (era-bound, null where the era lacks them).
--
-- `(repo_id, pr_number)` is the natural key. `repo_id` is stable across all
-- three eras (§12 trap 7) and `number` is one of the five fields the
-- October reduction left intact, so the key holds everywhere.

with pr_events as (

    select
        repo_id,
        pr_number,
        created_at,
        event_type,
        event_action,
        actor_login,
        pr_merged,
        pr_draft,
        ingested_at
    from {{ source('silver', 'events') }}
    where pr_number is not null
      and event_type in ('PullRequestEvent', 'PullRequestReviewEvent')

    {% if is_incremental() %}
      -- Only events not already folded into a stored row. `ingested_at` is
      -- processing time and advances monotonically across a resumable
      -- backfill (each day is fetched after the one before), so a strict
      -- `>` on the stored high-water mark neither re-reads nor skips.
      and ingested_at > (
          select coalesce(max(last_ingested_at), to_timestamp('1970-01-01 00:00:00'))
          from {{ this }}
      )
    {% endif %}

),

opened as (

    select
        repo_id,
        pr_number,
        min(created_at) as opened_at,
        -- The author is the actor of the opened event (§4.3a), taken from
        -- the earliest one so a duplicate re-send cannot change it.
        min_by(actor_login, created_at) as author_login,
        min_by(pr_draft, created_at) as draft
    from pr_events
    where event_type = 'PullRequestEvent' and event_action = 'opened'
    group by repo_id, pr_number

),

closed as (

    select
        repo_id,
        pr_number,
        min(created_at) as closed_at,
        min_by(pr_merged, created_at) as merged
    from pr_events
    where event_type = 'PullRequestEvent' and event_action = 'closed'
    group by repo_id, pr_number

),

reviewed as (

    select
        repo_id,
        pr_number,
        min(created_at) as first_review_at
    from pr_events
    where event_type = 'PullRequestReviewEvent'
    group by repo_id, pr_number

),

batch as (

    select
        keys.repo_id,
        keys.pr_number,
        opened.opened_at,
        opened.author_login,
        opened.draft,
        closed.closed_at,
        closed.merged,
        reviewed.first_review_at,
        keys.last_ingested_at
    from (
        select repo_id, pr_number, max(ingested_at) as last_ingested_at
        from pr_events
        group by repo_id, pr_number
    ) as keys
    left join opened using (repo_id, pr_number)
    left join closed using (repo_id, pr_number)
    left join reviewed using (repo_id, pr_number)

),

reconciled as (

    {% if is_incremental() %}
    -- Fold the batch into the stored row. `least()` skips nulls in Spark,
    -- so a later batch that carries no `opened` event cannot overwrite a
    -- real `opened_at` with null -- which a plain `MERGE ... UPDATE SET`
    -- would. Identity columns (`author_login`, `merged`, `draft`) prefer
    -- the stored value once set; a PR does not change author.
    select
        coalesce(batch.repo_id, prior.repo_id) as repo_id,
        coalesce(batch.pr_number, prior.pr_number) as pr_number,
        least(batch.opened_at, prior.opened_at) as opened_at,
        coalesce(prior.author_login, batch.author_login) as author_login,
        least(batch.closed_at, prior.closed_at) as closed_at,
        coalesce(prior.merged, batch.merged) as merged,
        coalesce(prior.draft, batch.draft) as draft,
        least(batch.first_review_at, prior.first_review_at) as first_review_at,
        greatest(batch.last_ingested_at, prior.last_ingested_at) as last_ingested_at
    from batch
    full outer join {{ this }} as prior using (repo_id, pr_number)
    {% else %}
    select * from batch
    {% endif %}

)

select
    repo_id,
    pr_number,
    opened_at,
    author_login,
    closed_at,
    first_review_at,
    merged,
    draft,
    -- Right-censoring (§5.1): no terminal event observed. Flagged, never
    -- given a fabricated outcome; Task 5 excludes these from the label and
    -- reports the rate.
    closed_at is null as is_censored,
    last_ingested_at
from reconciled
