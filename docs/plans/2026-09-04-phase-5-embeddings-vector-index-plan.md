# Phase 5: embeddings + vector index (implementation plan)

Written from design doc §8.3/§8.3a (`docs/design/2026-09-01-almanac-system-design.md`),
which has the full evidence and decisions this plan wires into code. Written
and executed in one continuous session, same register as
`docs/plans/2026-09-04-phase-4-classification-plan.md`: interfaces and test
intent, not full inline code, since there is no separate review pass between
writing this and implementing it. TDD discipline, `make check` before every
commit, one STATUS.md verification-log row per task, unchanged.

**Ordering principle:** every real-money step (embedding the real corpus,
standing up the index, retraining) happens once, last, in Task 7 — after
everything upstream of it is built and tested against small local fixtures.
This is the same shape every prior phase's real cloud run followed.

## Task 1: Measure before deciding — real corpus size and the index SKU rate

Not a code task — a cloud-verification task, cheap (Serverless Starter
Warehouse queries, the same pattern §5.3's SLA-threshold measurement and
Phase 2's DBU-rate lookup both used; started for this, stopped after).

**What gets measured, against the real `almanac_dbx.gold`/`silver` Q3 2025
data, and committed to a findings doc before Task 2 starts:**
- PR title+body and issue title+body non-null text counts — the real
  replacement for §8.3's stale 5%-sample projection (§8.3a).
- Real Vector Search SKU rate via `system.billing.list_prices`, the same
  method that resolved DBU rates in `2026-09-03-measured-dbus.md`.

**Decision made from these two numbers, per §8.3a's rule:** Vector Search
unless the real rate prices the corpus above ~10% of remaining credit, in
which case FAISS. Whichever wins, the choice and its arithmetic are
committed to `docs/findings/2026-09-04-phase-5-corpus-and-index-choice.md`
before any embedding code is written — this is Gate 1 applied structurally,
not left to a checklist step.

## Task 2: The embedding pipeline

**Files:**
- New: `src/almanac/embed/pipeline.py`, `src/almanac/embed/__init__.py`
- Test: `tests/unit/test_embed_pipeline.py`
- Modify: `pyproject.toml` (`ml` extras group gains `sentence-transformers`,
  version floor checked live against Task 1's date, not carried over from
  this plan)

**Interfaces:**
- `extract_texts(events: DataFrame, *, event_type: Literal["pr", "issue"]) -> DataFrame` — one row per PR-opened or issue-opened event carrying non-null title/body, `(entity_key, event_time, text, text_hash)`; `text_hash` (`sha256` of the concatenated title+body) is the idempotency key.
- `embed_texts(texts: pd.Series, *, model_name: str = "sentence-transformers/all-MiniLM-L6-v2", batch_size: int = 64) -> np.ndarray` — thin wrapper around `SentenceTransformer(...).encode(...)`, injectable model instance so unit tests never download real weights (mirrors `train.py`'s injectable-clock pattern from the REST source work).
- `run_embedding_pipeline(spark, *, silver_path, embeddings_path, event_type, existing_hashes: set[str] | None) -> int` — incremental: `extract_texts` minus `existing_hashes` (already-embedded rows, read from the target Delta table's `text_hash` column), embed only the delta, append (not overwrite) to the Delta table. Returns rows written. Full recompute is provably wrong here in a way `run_features`'s full-overwrite isn't (that plan's own stated Deferred item) — an embedding is expensive per row and a PR's text never changes after Bronze lands it, so re-embedding unchanged rows on every run would be pure waste, not just a simplification left for later.

**Tests to write:** `extract_texts` on a small fixture with a null-body row (excluded) and a duplicate-text row (same `text_hash`, still two output rows — hash identifies *content*, not entity); `embed_texts` against an injected fake encoder returning a fixed-shape array, asserting shape and that batching doesn't reorder; `run_embedding_pipeline`'s incremental skip — seed `existing_hashes`, assert only the new row's text reaches the (fake) encoder.

## Task 3: The index — Vector Search or FAISS, per Task 1's decision

**Files (Vector Search branch):**
- New: `infra/terraform/vector_search.tf` — `databricks_vector_search_endpoint` (`endpoint_type` confirmed against the live provider schema at write time — the product's rename from "Vector Search" to "AI Search", confirmed live 2026-09-04 via PyPI (`databricks-ai-search` is now canonical, `databricks-vectorsearch` a deprecated shim), makes the exact current resource/type names worth reconfirming rather than assumed from search results alone), `databricks_vector_search_index` synced from Task 2's Delta table, `primary_key = entity_key`, metadata columns including `event_time` for the point-in-time filter.
- Modify: `pyproject.toml` (`ml` extras gain `databricks-ai-search`, not the deprecated `databricks-vectorsearch` name — Gate 2's own finding applied, not just recorded).

**Files (FAISS branch, if Task 1 decides that way):**
- New: `src/almanac/embed/index.py` — `build_faiss_index(embeddings_path) -> faiss.Index`, rebuilt per batch (§8.3's "rebuild cost matters more than query latency"), persisted to the `features` container alongside `event_time`/`entity_key` sidecar arrays for the metadata filter FAISS itself doesn't provide natively.
- Modify: `pyproject.toml` (`ml` extras gain `faiss-cpu`).

**Tests to write (either branch):** a query against a small known set of
vectors returns the expected nearest neighbor by construction (one
obviously-closest point among decoys); the point-in-time filter actually
excludes a closer-but-too-late neighbor — the test this task exists to
pass, mirroring the leakage suite's own boundary-test shape (§4.4a).

## Task 4: `pr_similarity` — the point-in-time-correct feature group

**Files:**
- New: `src/almanac/features/similarity.py`
- Modify: `tests/integration/test_features_leakage.py` (extend)
- Test: `tests/unit/test_features_similarity.py`

**Interfaces:**
- `query_similar(index, entity_key, as_of, *, k=10) -> list[Neighbor]` — wraps whichever index Task 3 built; the metadata filter (`event_time < as_of`) is passed at the query, not applied after — the same "excluded, not filtered post-hoc" discipline `as_of_join`'s own docstring states.
- `compute_pr_similarity(spine: DataFrame, index, resolutions: DataFrame) -> DataFrame` — for each spine row, `query_similar` then joins each neighbor against `resolutions` (`(repo_id, pr_number) -> (closed_at, breach)`) to compute `similar_prior_breach_rate`: **null when every neighbor is unresolved as of the spine row's own `as_of_timestamp`**, not zero — §8.3a's second leakage axis, tested explicitly, not left to be caught by inspection.
- Does **not** go through `FeatureTableSpec`/`run_features` (§8.3a: this is not a pure `DataFrame -> DataFrame` transform, it queries an external index) — a separate small runner, not a forced fit into the existing one.

**Tests to write:** the point-in-time-correct case (neighbor opened and
resolved before `as_of`, contributes to the rate); the unresolved-neighbor
case (neighbor opened before `as_of` but not yet closed — contributes to
similarity stats, excluded from the breach-rate numerator, and the
all-unresolved case yields `null` not `0.0`); a neighbor opened *after*
`as_of` never appears at all (Task 3's index-level filter is the actual
enforcement; this test is the feature-group-level proof it holds end to
end).

## Task 5: `POST /similar-prs`, demoed not served

**Files:**
- New: `src/almanac/embed/query.py` (or extends `similarity.py` if small enough by the time Task 4 lands — decided then, not pre-committed)
- Test: `tests/unit/test_embed_query.py`

**Interfaces:**
- `similar_prs(repo_id, pr_number, as_of, *, k=10) -> list[dict]` — the whole of §6's `POST /similar-prs` contract (ANN neighbours + similarity scores), as a plain callable, not a route. §5.2's precedent stands: no custom API service exists yet, and this phase's "concrete reason" is the index, not a service — so it stays a callable, demoed via a script/CLI invocation and a test, the same way the serving endpoint was demoed via direct `curl`/CLI rather than a wrapping service (`docs/findings/2026-09-04-serving-endpoint-measured.md`).

**Tests to write:** one call against Task 3's test index, asserting the
contract shape (neighbours + scores) and that `as_of` is honoured (reuses
Task 4's fixture rather than inventing a third one).

## Task 6: Downstream lift — retrain, compare to the champion, ship or don't

**Files:**
- Modify: `src/almanac/model/dataset.py` (`build_classification_frame` gains an optional `similarity_features: DataFrame | None` join)
- Modify: `src/almanac/model/train.py` (no new candidates dict shape — same `CLASSIFIER_CANDIDATES` sweep, run twice: with and without the similarity columns)
- Test: `tests/unit/test_model_dataset.py`, `tests/unit/test_model_train.py` (extend both)

**Interfaces:**
- `build_classification_frame(..., similarity_frame: DataFrame | None = None)` — left-joined on `(repo_id, pr_number)` when given, columns absent (not zero-filled) when not — every existing call site and test passes `None` and is untouched.
- Comparison is two full `train_classifier` runs (with/without) against the
  **same split**, both logged to MLflow in the same experiment; promotion
  compares the with-similarity run's `average_precision` against the
  **registered champion's** 0.612, not against its own without-similarity
  sibling — §8.3a's stated gate exactly. `beats_baseline`'s existing
  boolean shape is reused for this comparison, not a new field invented
  for what is the same kind of decision one level up.

**Tests to write:** the join adds columns only when `similarity_frame` is
given, proving the `None` path is byte-identical to before this task
(regression test against Phase 4's existing fixture); a synthetic frame
where the similarity column carries the signal, proving the with-run can
in fact win when the world says it should (mirrors Task 12's own
no-signal-passes-first-try discipline from the classification plan).

## Task 7: Runner/CLI, Terraform, and the real cloud run

**Files:**
- Modify: `src/almanac/embed/pipeline.py` (CLI entrypoint, `run_cli`-wrapped per the defect-9 precedent — every job-task entrypoint in this project now goes through it)
- New: `infra/terraform/embeddings.tf` — `databricks_job.embeddings` (Task 2's pipeline, then Task 3's index sync/rebuild), same job-cluster shape as `train_model`
- Modify: `infra/terraform/databricks.tf`'s `train_model` job — parameters gain the similarity-frame path once Task 6 exists, **only if Task 6's real run beats the champion** (mirrors §5.3's exact "supersedes the objective this job ran under, not a parallel pipeline" precedent — if it doesn't win, this file does not change)

**The real run, in order:** Task 1's decided scope through Task 2's
pipeline for real → Task 3's index built/synced for real → Task 4's
features computed for real → Task 6's comparison run for real, against
the still-live `westus3` workspace and the still-live $184 credit (§8.3a).
Cost, defects, and the measured PR-AUC delta (a real win or an honest
null) get written up the same way every prior real cloud run in this
project has been: `docs/findings/2026-09-0X-phase-5-measured.md`, one
findings doc per this project's own convention, not folded into STATUS.md
prose alone.

**Tests to write:** none new — this task is the real-money verification
of Tasks 1–6's already-tested code, same shape as Phase 4's Task 9/Task 13.

## Task 8: Wrap-up — exit gate, README, STATUS.md

Same shape as every prior phase's closing task. The README's Mermaid
diagram's `V` node (currently `todo`, per the R/E update from Phase 4's
wrap-up) moves to `done` in this commit, same rule as every prior phase —
the diagram updates in the same commit as the change it depicts, not a
follow-up. STATUS.md's verification log gets Tasks 1–7's rows plus this
closing one, stating the actual measured outcome (lift or null) rather
than a plan.

## Exit gate

| Gate | How it's verified |
|---|---|
| The index-technology decision is made from a real-measured corpus and a real-measured SKU rate, not the stale 5%-sample projection | Task 1's findings doc, committed before Task 2 |
| Point-in-time filtering applies to retrieval on both axes — query-side (neighbor candidacy) and label-side (neighbor outcome knowability) | Task 3's filter test + Task 4's unresolved-neighbor test |
| No self-join / brute-force O(n²) retrieval path exists at real scale | Task 3/4 route every real query through the chosen index, never a Spark self-join over the full corpus |
| The embedding pipeline is incremental, not a full recompute | Task 2's skip-already-embedded test |
| No feature addition ships without beating the current registered champion | Task 6's champion-comparison gate, same shape as §5.1's baseline gate one level up |
| `POST /similar-prs` is demoed against the real, live index | Task 7's real cloud run |
| Measured lift or an honest documented null result | Task 7's findings doc — matches the phase table's own stated gate for Phase 5 verbatim |

## Deferred, carried from §8.3a

Hybrid/lexical search and reranking; embedding models other than
`all-MiniLM-L6-v2`; a standalone issue-dedup product surface; GPU
inference; the `@challenger` retraining workflow; `GET /features/{id}?as_of=`
as a custom API service. None of this plan's tasks touch them.
