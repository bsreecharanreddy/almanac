# ADR-0002: Do not enable Photon

**Status:** Accepted 2026-09-03. Strengthened, not reversed, by a
same-day correction.

## Context

Photon is a per-DBU premium. Design doc §8.2 pre-registered a hypothesis
**before any measurement**: Photon helps Silver and Gold materially,
helps Bronze little (Bronze is mostly JSON parsing), and the blend may be
a wash or a loss.

## Decision

Photon stays off for every job in this stack.

## What was measured

Three replicate pairs of `databricks_job.photon_ab` over Phase 1's
calibration slice, all six arms SUCCESS, all reporting
`compressed_gb = 2.0124815106391907` — byte-identical, so every pair
measured the same data.

| Layer | r1 | r2 | r3 | mean | range | verdict |
|---|---|---|---|---|---|---|
| Bronze | 1.18 | 1.05 | 1.23 | 1.15 | 1.05–1.23 | **indeterminate** |
| Silver | 1.96 | 2.40 | 2.06 | **2.14** | 1.96–2.40 | helped |
| Gold | 1.45 | 1.29 | 1.42 | **1.38** | 1.29–1.45 | helped |

**Bronze is withheld, and that is the methodological point.** The
decision rule — the whole observed range must clear 1.10 — was fixed
before replicate 3 ran. Bronze's mean of 1.15 clears it and two of three
replicates clear it, but the range straddles. Switching to the mean after
seeing the data is exactly what the rule exists to prevent. The reason is
the noise floor: re-running an *identical* arm spreads 2.3–29.9%, so a
~15% Bronze effect is smaller than the noise of the arms measuring it.

## Alternatives considered

**Enable Photon on Silver and Gold only**, where the effect is robust.
Rejected on cost once DBUs were measured: `paid_for_itself: False`,
`cost_off $0.40` against `cost_on $0.74` — Photon costs **1.85x for a
1.31x speedup**.

## The correction that matters

The original break-even table was **arithmetically wrong**: it used
`V = $0.474/node-hour` as the VM rate, which is VM (`$0.249`) *plus* an
assumed DBU cost, double-counting DBUs into `V` while also charging them
through `d`. Corrected against the measured DBU rate, break-even is
**1.31 / 1.54 / 1.65**, not the 1.55 / 1.96 / 2.16 originally published.
The measured multiplier is **2.297** (range 2.118–2.394), clearing every
threshold, corrected or not.

**Both errors flattered Photon**, which is why the recommendation survived
them — but this went from "a wash on assumed inputs" to "a measured loss
with no plausible input that rescues it". The strength of the conclusion
changed even though the conclusion did not.

## Consequences

The real lever was never Photon. Bronze is **63.8% of execution time** and
single-threaded gzip — the layer Photon helps least, and the one that made
this question ambiguous in the first place.

**Evidence:** `docs/findings/2026-09-03-photon-ab.md` (carrying its own
correction in place), `docs/findings/2026-09-03-measured-dbus.md`.
