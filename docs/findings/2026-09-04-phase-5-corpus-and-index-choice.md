# Findings — the real Phase 5 corpus, and what could (and couldn't) be measured before choosing an index

**Date:** 2026-09-04
**Question:** §8.3a's Task 1 — re-measure the real embedding corpus against
the live Q3 2025 backfill (not the stale 5%-sample projection), and pull
the real Vector Search SKU rate, before choosing Vector Search vs. FAISS.
**Method:** `system.billing.list_prices` and two `get_json_object` counts
against `delta.`abfss://bronze@almanaclakekoctmh.dfs.core.windows.net/events``
(the real 92-day, 341M-row Bronze table), via the Serverless Starter
Warehouse, started for this and left to auto-stop (10 min idle).

---

## The real corpus — measured, not projected

Bronze carries the whole GH Archive event as one `raw_json` string (§3.3:
Bronze never transforms); text is extracted with `get_json_object`, LIKE-
prefiltered by event type before parsing to keep the scan cheap.

```sql
SELECT COUNT(*), SUM(CASE WHEN get_json_object(raw_json, '$.payload.pull_request.title') IS NOT NULL
                          AND get_json_object(raw_json, '$.payload.pull_request.body') IS NOT NULL THEN 1 ELSE 0 END)
FROM delta.`abfss://bronze@almanaclakekoctmh.dfs.core.windows.net/events`
WHERE raw_json LIKE '%"type":"PullRequestEvent"%'
  AND get_json_object(raw_json, '$.payload.action') = 'opened'
```

| | Opened events | With non-null title + body |
|---|---|---|
| PRs | 13,179,648 | **10,162,741** |
| Issues | 5,161,766 | **4,761,832** |
| **Total real candidate texts** | | **14,924,573** |

Query wall time: ~2.5 min (PRs) on a Small serverless SQL warehouse — a
full-quarter scan, not a sample.

**This supersedes §8.3's "~4.6M at 100%".** That number was a 30-day,
one-hour-rate projection taken before Tier 3 ever ran. The real, unsampled
92-day quarter carries **~3.2x** that many candidate texts. Restated per
§8.3a's own Gate 1 warning: this is not the old measurement being wrong,
it measured a different, smaller thing than what now actually exists.

At dimension 384 (`all-MiniLM-L6-v2`) and a Standard endpoint's ~4M-vector
per-unit capacity (§8.3a, from Databricks' stated ~2M-at-dim-768 figure),
**14.9M vectors needs ~4 Vector Search units** on a Standard endpoint —
still a knowable, bounded number, just not the one-unit fit §8.3a
speculated might be plausible.

## What could be confirmed live today

- **Region: `westus3` supports AI Search**, confirmed directly against
  Microsoft's [feature-region-support](https://learn.microsoft.com/en-us/azure/databricks/resources/feature-region-support)
  table (the "AI and machine learning features availability" section) —
  not assumed from the product being "generally available" overall.
- **Account access: real, not just documented.** `databricks vector-search-endpoints list-endpoints`
  returns `[]` with exit 0 — a clean empty list, not a permission or
  entitlement error, which is what an unprovisioned account would show
  instead.
- **Correction to §8.3a's own text**: the CLI's `create-endpoint --help`
  lists `STORAGE_OPTIMIZED` as a supported `endpoint_type` value alongside
  `STANDARD` — §8.3a had hedged this as unverified from a Terraform-
  registry search result alone. Corrected here, in place, per this
  project's own correction discipline (§8.3a is not edited retroactively;
  this findings doc is where the correction lives).
- **The product's rename is real but partial.** The CLI exposes both
  `vector-search-endpoints`/`vector-search-indexes` (no preview marker)
  and a newer `ai-search` command group marked **`*Public Preview*`**.
  The PyPI-level rename (`databricks-ai-search` canonical,
  `databricks-vectorsearch` a deprecated shim, confirmed in §8.3a) is
  ahead of the CLI/API surface, which still treats the *older* naming as
  the stable, non-preview interface. **Decision: Task 3 targets
  `databricks_vector_search_endpoint`/`databricks_vector_search_index`
  (Terraform) and the `vector-search-endpoints`/`vector-search-indexes`
  CLI surface — the GA interface — not the Public Preview `ai-search`
  rename**, since nothing in this plan needs whatever the preview surface
  adds and this project's own standing caution (the Storage-Optimized-vs-
  Standard hedge above, the Standard-UC-tier-discontinuation lesson) is to
  prefer the stable surface absent a concrete reason not to.

## What could not be confirmed, and why

**`system.billing.list_prices` returns zero rows for any SKU containing
`VECTOR` or `SEARCH`**, despite the region and account both confirming
access above. Every other DBU rate this project has needed (Premium Jobs
Compute, Photon, Model Serving) showed up in this same table once queried
correctly — this is the first time the table itself has had nothing to
show, not a permissions problem repeating the two false diagnoses
`2026-09-03-measured-dbus.md` already recorded. The most likely
explanation, not yet confirmed: Vector Search/AI Search may meter through
Azure Marketplace rather than Databricks' own `system.billing` SKU
catalog on this cloud — a real, open gap, stated as such rather than
papered over with a guessed number.

**A minimal smoke-test endpoint (`STANDARD`, no index) was not created.**
Creating one — the direct way to learn whether provisioning succeeds and
to populate a real billing row afterward — was blocked by this session's
own tool-permission classifier as a new kind of billable, ad hoc resource
outside Terraform's tracked state. Not worked around; surfaced instead,
per this project's own "confirm hard-to-reverse or outward-facing actions"
discipline. **Decision, and it stays inside §8.3a's own rule, not a
deviation from it:** rather than a separate untracked CLI smoke test, the
real endpoint gets created once, for real, Terraform-tracked, as Task 3
itself — folding "does it provision, and what does it actually cost"
into the same real step that builds the index for real, rather than a
throwaway resource that would need its own manual cleanup. If that real
attempt reveals a cost or an entitlement problem, **that** is the FAISS
trigger — §8.3a's fallback clause was already written to fire on a real
measured problem, not on cost being merely unknown in advance.

## What this settles for Task 3

Proceed toward **Vector Search, `STANDARD` endpoint type**, sized for
~14.9M vectors (~4 units) — region and account access are both real
positive signals, and point-in-time filtering was already confirmed clean
live (§8.3a). The cost half of §8.3's decision rule is answered by Task
3's own real Terraform apply, checked against billing shortly after,
against the same ~10%-of-remaining-credit bar — not assumed favorable
here just because nothing measured today ruled it out.
