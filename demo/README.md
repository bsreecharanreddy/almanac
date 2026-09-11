# Almanac — work-queue risk, running locally

[Source](https://github.com/bsreecharanreddy/almanac) — one archived hour
per schema era from GH Archive, scored by the real registered champion
(LightGBM, MLflow model registry version 2).

**This is not the full dataset.** The platform this demo is drawn from
has a measured backfill of 341,060,851 rows over Q3 2025, for a measured
$11.96. This app scores three fixture hours — one per schema era — so
most point-in-time features are legitimately null here; the queue and
coverage panels show exactly why, rather than hiding it.

Four tabs: the ranked intervention queue, the champion's own per-feature
contributions for any row, a bounded agent's replayed answer checked by a
deterministic grounding verifier (no model call, no network happens in
this app), and the Bronze/Silver medallion each fixture hour was built
from.

Free tier. No billable cloud resource runs behind this app.

## Running it

Locally: `make demo` from the repo root (needs `uv`).

Deployed: [Streamlit Community Cloud](https://streamlit.io/cloud), free
tier, built from `demo/requirements.txt` (a pinned export of exactly the
`demo` + `ml-scoring` + `agent` extras, regenerated with `uv export
--extra demo --extra ml-scoring --extra agent --no-dev --no-emit-project
--no-hashes --no-header --format requirements-txt -o demo/requirements.txt`
whenever `uv.lock` changes) with `demo/app.py` as the main file.
