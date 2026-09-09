{{ config(materialized='view') }}

-- Every PR-relevant event, one shape. "Which event types feed a PR" is
-- decided here so the fact never re-derives it. IssueCommentEvent is the
-- awkward case: `is_pr_comment` is null pre-2015 (0 of 194 measured), so a
-- legacy issue comment counts only when (repo_id, pr_number) matches a seen
-- PullRequestEvent. A modern `is_pr_comment = false` is a real issue comment
-- and is dropped.

with silver as (

    select
        repo_id,
        pr_number,
        created_at,
        actor_login,
        ingested_at,
        event_type,
        event_action,
        pr_merged,
        pr_draft,
        is_pr_comment
    from {{ source('silver', 'events') }}
    where pr_number is not null

),

known_prs as (

    select distinct repo_id, pr_number
    from silver
    where event_type = 'PullRequestEvent'

),

lifecycle_and_reviews as (

    select silver.* from silver
    -- 'merged' is the reduced era's close: it replaced the rich era's
    -- pull_request.merged FIELD with a distinct ACTION, so a PR that merged
    -- after Oct 2025 never emits 'closed' at all. Measured on 2026-09-05:
    -- 204,748 'merged' rows against 16,204 'closed', where the 2025 reference
    -- day carries only {opened, closed, reopened}. Filtering on the rich era's
    -- vocabulary dropped every reduced-era merge before Gold could see it.
    where (event_type = 'PullRequestEvent' and event_action in ('opened', 'closed', 'merged'))
       or event_type in ('PullRequestReviewEvent', 'PullRequestReviewCommentEvent')

),

pr_issue_comments as (

    select silver.*
    from silver
    left join known_prs using (repo_id, pr_number)
    where silver.event_type = 'IssueCommentEvent'
      and (
          silver.is_pr_comment = true
          or (silver.is_pr_comment is null and known_prs.repo_id is not null)
      )

),

pr_events as (

    select * from lifecycle_and_reviews
    union all
    select * from pr_issue_comments

)

select
    repo_id,
    pr_number,
    created_at,
    actor_login,
    ingested_at,
    pr_merged,
    pr_draft,
    case
        when event_type = 'PullRequestEvent' and event_action = 'opened' then 'opened'
        -- Both spellings of a close collapse to one kind, so every
        -- downstream branch (closed_at, merged, is_censored, the label)
        -- stays era-agnostic rather than each learning the difference.
        when event_type = 'PullRequestEvent'
             and event_action in ('closed', 'merged') then 'closed'
        when event_type = 'PullRequestReviewEvent' then 'review'
        when event_type = 'PullRequestReviewCommentEvent' then 'review_comment'
        when event_type = 'IssueCommentEvent' then 'issue_comment'
    end as kind
from pr_events
