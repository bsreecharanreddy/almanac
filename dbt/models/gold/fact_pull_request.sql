{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['repo_id', 'pr_number'],
        file_format='delta',
    )
}}

{#-
  One row per pull request, as an accumulating snapshot (design doc §4.3):
  columns fill in as lifecycle events arrive, and out-of-order batches
  preserve the earliest timestamp.

  `contract: enforced` and `on_schema_change: fail` are set together in
  schema.yml (dbt validates that pairing there, not here): a column added
  or retyped is a deliberate change that should break the build, not drift
  in silently.

  Every column is derived from an event-level field via `int_pr_events` --
  never `payload.pull_request.*`, which October 2025 gutted (§12 trap 12).
  `merged` / `draft` are the one documented exception, carried from
  Silver's parsed `pr_merged` / `pr_draft`, era-bound and null-safe.

  Incremental strategy: any PR touched by a new event this batch is
  **recomputed in full** from `int_pr_events`, not folded scalar by scalar.
  That is what makes out-of-order arrival and author-exclusion correct
  without carrying per-actor response detail in the row -- when the
  `opened` event finally lands, the whole PR is rebuilt with its complete
  response history, and the author is excluded properly. Untouched rows
  pass through from `{{ this }}` unchanged. `last_ingested_at` is the
  high-water mark that decides "touched".
-#}

{%- set fact_columns -%}
    repo_id,
    pr_number,
    opened_at,
    author_login,
    author_is_bot,
    closed_at,
    merged,
    draft,
    first_review_at,
    first_response_at,
    time_to_first_response_seconds,
    is_censored,
    label_exclusion,
    last_ingested_at
{%- endset -%}

with events as ( select * from {{ ref('int_pr_events') }} ),

{% if is_incremental() %}

touched as (
    select distinct repo_id, pr_number
    from events
    where ingested_at > (
        select coalesce(max(last_ingested_at), to_timestamp('1970-01-01 00:00:00'))
        from {{ this }}
    )
),

scope as (
    select events.* from events join touched using (repo_id, pr_number)
),

{% else %}

scope as ( select * from events ),

{% endif %}

opened as (
    select
        repo_id,
        pr_number,
        min(created_at) as opened_at,
        min_by(actor_login, created_at) as author_login,
        min_by(pr_draft, created_at) as draft
    from scope
    where kind = 'opened'
    group by repo_id, pr_number
),

closed as (
    select
        repo_id,
        pr_number,
        min(created_at) as closed_at,
        min_by(pr_merged, created_at) as merged
    from scope
    where kind = 'closed'
    group by repo_id, pr_number
),

reviews as (
    select repo_id, pr_number, min(created_at) as first_review_at
    from scope
    where kind = 'review'
    group by repo_id, pr_number
),

-- Earliest response per actor, so the PR author can be excluded once known.
-- Bots stay in -- §5.1 segments by is_bot and never drops the population.
responses as (
    select repo_id, pr_number, actor_login, min(created_at) as responded_at
    from scope
    where kind in ('review', 'review_comment', 'issue_comment')
    group by repo_id, pr_number, actor_login
),

assembled as (
    select
        watermark.repo_id,
        watermark.pr_number,
        watermark.last_ingested_at,
        opened.opened_at,
        opened.author_login,
        opened.draft,
        closed.closed_at,
        closed.merged,
        reviews.first_review_at
    from (
        select repo_id, pr_number, max(ingested_at) as last_ingested_at
        from scope
        group by repo_id, pr_number
    ) as watermark
    left join opened using (repo_id, pr_number)
    left join closed using (repo_id, pr_number)
    left join reviews using (repo_id, pr_number)
),

first_response as (
    select
        assembled.repo_id,
        assembled.pr_number,
        min(responses.responded_at) as first_response_at
    from assembled
    join responses using (repo_id, pr_number)
    where assembled.author_login is not null
      and responses.actor_login <> assembled.author_login
      -- A "response" stamped before the PR opened is not a response to it
      -- (§2, and legacy issue/PR number reuse can otherwise match one).
      and (assembled.opened_at is null or responses.responded_at >= assembled.opened_at)
    group by assembled.repo_id, assembled.pr_number
),

computed as (
    select
        assembled.repo_id,
        assembled.pr_number,
        assembled.opened_at,
        assembled.author_login,
        coalesce({{ almanac_is_bot('assembled.author_login') }}, false) as author_is_bot,
        assembled.closed_at,
        assembled.merged,
        assembled.draft,
        assembled.first_review_at,
        first_response.first_response_at,
        case
            when assembled.author_login is not null
             and not coalesce(assembled.draft, false)
             and assembled.opened_at is not null
             and first_response.first_response_at is not null
            then unix_timestamp(first_response.first_response_at)
                 - unix_timestamp(assembled.opened_at)
        end as time_to_first_response_seconds,
        assembled.closed_at is null as is_censored,
        -- Why this PR is out of the trainable population, stated rather
        -- than the row silently missing (§5.1). NULL means "in the label",
        -- and NULL iff `time_to_first_response_seconds` is non-null.
        case
            when assembled.author_login is null then 'author_unobserved'
            when coalesce(assembled.draft, false) then 'draft'
            when assembled.opened_at is null then 'open_unobserved'
            when first_response.first_response_at is null and assembled.closed_at is null
                then 'right_censored'
            when first_response.first_response_at is null then 'closed_no_response'
        end as label_exclusion,
        assembled.last_ingested_at
    from assembled
    left join first_response using (repo_id, pr_number)
)

select {{ fact_columns }} from computed

{% if is_incremental() %}
union all
select {{ fact_columns }}
from {{ this }} as prior
where not exists (
    select 1 from touched
    where touched.repo_id = prior.repo_id and touched.pr_number = prior.pr_number
)
{% endif %}
