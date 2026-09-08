# Architecture decision records

Eight decisions, written **retrospectively** — each was made and recorded
in `docs/findings/` as it happened, and these consolidate them into one
form so a reader can find them without reading the whole findings series.
That framing is deliberate: an ADR written after the fact is a summary,
not a reconstruction, and every one below cites the measurement that
settled it rather than restating the conclusion.

Each names an alternative that was **genuinely considered** — in several
cases built, measured and abandoned, not merely listed.

| # | Decision | Settled by |
|---|---|---|
| [0001](0001-hand-rolled-as-of-join.md) | The as-of join is this project's own code, not a managed feature store | Design §4.4, checked live against Feature Engineering in UC and Feast |
| [0002](0002-do-not-enable-photon.md) | Do not enable Photon | A 3-replicate A/B, then measured DBUs that overturned its own cost verdict |
| [0003](0003-vector-search-over-faiss.md) | Databricks Vector Search over a self-hosted FAISS index | The real 14,924,573-text corpus and the live SKU rate |
| [0004](0004-dedup-on-write-not-watermark.md) | Deduplicate on write with a Delta `MERGE`, not a Spark watermark | 18.0% of a live window arrives behind a 10-minute watermark |
| [0005](0005-unity-catalog-lineage.md) | Unity Catalog's own lineage, not OpenLineage | 18,102 lineage rows already present with zero instrumentation |
| [0006](0006-aibi-over-power-bi.md) | Databricks AI/BI dashboards, not Power BI | Power BI Desktop is Windows-only; both cloud paths are gated |
| [0007](0007-lakebase-separate-root-module.md) | A second Terraform root module in `centralus` for Lakebase | Lakebase's 19 supported regions exclude `westus3` |
| [0008](0008-batch-scoring-beside-serving.md) | Score the quarter in batch beside the served endpoint | The endpoint returns a class, not a probability |

## What is deliberately not here

Decisions still open, or settled but not yet acted on, live in
`docs/limitations.md` and the findings docs. An ADR here means the
decision is made and the code reflects it.
