{{ config(materialized='view') }}

-- Every PR-relevant event, flattened to one shape. The seam between
-- `silver.events` and the PR fact + label: "which event types feed a PR"
-- is decided here, once, so the fact never re-derives it.
--
-- `IssueCommentEvent` is the awkward one. Modern events carry
-- `issue.pull_request`, so `is_pr_comment` is a real boolean. Legacy
-- events never carry it -- 0 of 194 measured -- so `is_pr_comment` is null
-- pre-2015, and a legacy issue comment counts as being on a PR only when
-- its `(repo_id, pr_number)` matches a PR we have actually seen a
-- `PullRequestEvent` for. GitHub numbers issues and PRs from a single
-- per-repo sequence, so that match cannot collide. A modern
-- `is_pr_comment = false` is a genuine issue comment and is dropped.

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
    where (event_type = 'PullRequestEvent' and event_action in ('opened', 'closed'))
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
        when event_type = 'PullRequestEvent' and event_action = 'closed' then 'closed'
        when event_type = 'PullRequestReviewEvent' then 'review'
        when event_type = 'PullRequestReviewCommentEvent' then 'review_comment'
        when event_type = 'IssueCommentEvent' then 'issue_comment'
    end as kind
from pr_events
