# Findings — the 2014 / 2025 schema eras, measured

**Date:** 2026-09-01
**Method:** field-path diff over 2,000 sampled events from each era —
`2014-06-12-14` and `2025-03-15-14` — via `almanac.explore.schema`.
Every number below is a count over those samples, not an estimate.

**Verdict: design doc §12 trap 7 was directionally right and wrong on
every specific.** Three findings change the design.

---

## 1. Legacy events have no `id` at all — 0 of 2,000

| Era | Events carrying `id` |
|---|---|
| legacy (2014-06) | **0 / 2,000** |
| modern (2025-03) | 2,000 / 2,000 |

**This breaks the design as written.** Bronze specifies `event_id`
"from `id`" (§4.1), Silver specifies an `event_id_not_null` rule and
dedup on `event_id` (§4.2), and design doc §12 trap 4 makes dedup on
`event_id` a functional requirement. None of that is possible for the
legacy era, because the field does not exist.

**Recommended resolution, to be confirmed when Phase 1 is planned:**
derive a deterministic surrogate for legacy events by hashing the
canonical raw record. This is not a workaround — it is *better* suited to
the actual requirement. Trap 4's duplicates are the same event repeated
across an hour-file boundary, so they are byte-identical, so a content
hash collides exactly when it should. The `event_id` column then carries
either the native id (modern) or a content hash (legacy), with
`event_id_source` recording which.

## 2. `actor` changes type, not just shape — string → object

| Era | Type of `actor` |
|---|---|
| legacy | **`str` in 2,000 / 2,000** — e.g. `"senseiurata"` |
| modern | `dict` in 2,000 / 2,000 — `{id, login, display_login, url, ...}` |

The design doc called this "a different actor representation." It is a
**type change**, which is materially worse: a parser expecting an object
does not degrade, it raises. Legacy carries the richer detail in a
separate top-level `actor_attributes` object (`login`, `name`, `email`,
`company`, `location`, `blog`, `gravatar_id`, `type`).

**Note there is no numeric actor id anywhere in the legacy era** — only a
login string. Since logins are mutable and ids are not, actor identity is
strictly weaker before 2015, and any cross-era actor join is a join on a
mutable key.

## 3. Legacy timestamps are not UTC — every one is `-07:00`

| Era | `created_at` suffix | Example |
|---|---|---|
| legacy | **`-07:00` in 2,000 / 2,000** | `2014-06-12T14:05:31-07:00` |
| modern | `Z` in 2,000 / 2,000 | `2025-03-15T14:00:00Z` |

The early API emitted US Pacific time. **Parsing a legacy timestamp as
naive UTC shifts it by seven hours** — silently, with no error, past every
schema check. Given that this project's entire premise is point-in-time
correctness, that is the single most dangerous finding in this document.

Any legacy timestamp must be parsed offset-aware and converted to UTC.
The existing `spark.sql.session.timeZone=UTC` setting does not save us:
it governs how Spark *displays and computes*, not how a string with an
explicit offset gets read.

---

## Corrections to design doc §12 trap 7

| Design doc claimed | Measured reality |
|---|---|
| `repository` instead of `repo` | ✅ **Correct.** Legacy `repository`, modern `repo` |
| "a different actor representation" | ⚠️ **Understated** — it is a string vs. object type change, and legacy has no actor id |
| legacy-only types: `DownloadEvent`, `FollowEvent`, `GistEvent` | ❌ **Not present in 2014-06.** The only legacy-only type observed is **`TeamAddEvent`** (3 events). Those three types were evidently retired before mid-2014 |
| (not mentioned) | ❗ **Legacy has no `id` field** |
| (not mentioned) | ❗ **Legacy timestamps carry a `-07:00` offset** |

## Correction to design doc §12 trap 10 (language coverage)

The doc says language "is not on most events" and appears only nested in
PR payloads. That holds for the modern era. **It is wrong for legacy**,
where `repository.language` is populated on **1,712 / 2,000 events
(85.6%)** directly on the event. Legacy language coverage is therefore
*better* than modern, which is the opposite of what the doc implies.

---

## Full field diff

**Top-level keys**

- legacy: `actor`, `actor_attributes`, `created_at`, `payload`, `public`, `repository`, `type`, `url`
- modern: `actor`, `created_at`, `id`, `org`, `payload`, `public`, `repo`, `type`

**Only in legacy (37 paths)** — `actor` (as scalar), `url`,
`actor_attributes.{blog,company,email,gravatar_id,location,login,name,type}`,
`payload.{comment_id,commit,issue_id,repository,shas,team}`,
`repository.{created_at,description,fork,forks,has_downloads,has_issues,has_wiki,homepage,id,language,master_branch,name,open_issues,organization,owner,private,pushed_at,size,stargazers,url,watchers}`

**Only in modern (22 paths)** — `id`,
`actor.{avatar_url,display_login,gravatar_id,id,login,url}`,
`org.{avatar_url,gravatar_id,id,login,url}`,
`payload.{before,commits,distinct_size,forkee,push_id,repository_id,review}`,
`repo.{id,name,url}`

**Push commit representation changed:** legacy `payload.shas`, modern
`payload.commits` with `distinct_size`. Design doc §12 trap 3's guidance
to prefer `payload.size`/`distinct_size` over `size(commits)` applies to
the modern era only.

**Repo identity is stable across eras.** `repository.id` is present on
1,997 / 2,000 legacy events and `repo.id` on 2,000 / 2,000 modern —
so `repo_id` works as the SCD2 natural key in both, which the design
depends on.
