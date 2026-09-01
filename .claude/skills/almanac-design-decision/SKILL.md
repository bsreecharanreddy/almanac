---
name: almanac-design-decision
description: Use when making or recording any Almanac design decision — choosing a technology, region, model, schema, or architecture; changing a decision already recorded; or turning a measurement into a claim a later decision will rest on. Enforces evidence sufficiency before conclusion, live web validation against current industry practice, and where the decision gets written down.
---

# Design decisions in Almanac

Three gates, in order. A decision that skips gate 1 is the failure mode
this project has already hit twice.

## Gate 1 — Is the evidence enough to conclude from?

**Incident-derived. This has gone wrong twice, both times the same way.**

| Claim | Sample it came from | What happened |
|---|---|---|
| "2026 firehose volume fell 76%" | **one** truncated hour | Wrong. Four more hours showed total volume ~unchanged (~160K); only PR activity collapsed. Shipped, then corrected in `535cbeb`. |
| "VM SKUs are restricted subscription-wide" | **two** regions (eastus2, eastus) | Wrong. centralus and westus3 offer the SKU normally. Nearly triggered a serverless rewrite of design doc §8.1 on a false premise. |

Both were ~90 seconds of extra measurement away from being caught. Both
were stated as conclusions before that measurement was made.

Before a measurement becomes a claim, answer out loud:

1. **How many samples is this?** State the actual `n`. "A few hours",
   "both regions", "the top 30" are not numbers.
2. **Is `n` more than one, along the dimension I am generalizing over?**
   Generalizing over *time* needs multiple time points. Over *regions*
   needs multiple regions. One hour says nothing about a quarter; two
   regions say nothing about a subscription.
3. **What would the cheapest disconfirming check cost?** If the answer is
   under a few minutes, **run it before concluding, not after.**
4. **Is the sample selected in a way that guarantees the answer?** The
   bot-classification finding is the worked example: volume-ranked top-30
   logins were 100% true bots *because bots are high-volume by
   definition*. Ranking by distinct login instead revealed 86.7% false
   positives. A sample drawn along the axis you are measuring proves
   nothing.

State the `n` **in the claim itself**, in the findings doc and in
STATUS.md. "Measured across four regions" is a claim a reader can check;
"measured" is not.

If evidence is genuinely thin and widening it is genuinely expensive, say
so explicitly and label the claim provisional. That is allowed. Quietly
presenting it as settled is not.

## Gate 2 — Validate against current practice, live

**Standing rule, set by the user 2026-09-01: every design decision gets
checked against a live web search before it is recorded.** Training data
lags; this project targets current-generation choices, and "what I already
believed" is not a source.

- Search for the current recommended practice, then read the **primary**
  source — vendor docs, first-party API, spec — not a blog or aggregator.
  The pricing findings exist because every third-party figure checked
  turned out approximate.
- Prefer a first-party API over a docs page where one exists. Azure Retail
  Prices API over a pricing page; `az vm list-skus` over a region table.
- **Record what you checked and when**, with links, in the findings or
  design doc. A validation nobody can re-run is not a validation.
- Watch specifically for: deprecations and discontinuations, newer
  generations of the thing being chosen, and limits/quotas that constrain
  it. The Standard-tier discontinuation (2026-04-01) turned a recorded
  *decision* into something that was actually *forced* — worth knowing
  before writing it up as a trade-off we made.

For decisions touching an AI/LLM capability, the retrieval, agent
boundary, evaluation, and guardrail patterns all move fast enough that
gate 2 is doing real work, not ceremony.

## Gate 3 — Write it where it will be found

A decision not written down gets re-litigated. Each goes to exactly one
home — duplicating it means one copy goes stale:

- **`docs/findings/YYYY-MM-DD-<topic>.md`** — the measurement, its method
  (exact command or API), its `n`, and what it rules out. Include the
  command so it can be re-run.
- **`docs/design/…`** — the decision itself and its consequences, when it
  changes architecture.
- **`docs/STATUS.md`** — one verification-log row, **in the same commit as
  the work**. Never a follow-up commit.
- **Code comments** — only the *why*, next to the thing constrained. A
  region default carrying its reason survives; a wiki page does not.

### Correcting a recorded decision

Both incidents above became corrections. Correct **in place, marked**, and
do not silently delete the original reasoning:

- Keep the original analysis when it still answers a real question; append
  a dated correction noting what was wrong and what forced the change.
- Say which claim was overstated and why, so a reader can calibrate how
  much to trust neighboring claims. This matters more than looking
  consistent.
- If a superseded decision is left in place deliberately, say that it is
  superseded and by what.

## Red flags

| Thought | Reality |
|---|---|
| "Both the ones I checked agree, so it's general" | `n=2` disproved once already, in this exact repo. |
| "I'll verify after writing it up" | The write-up is the claim. Verify first. |
| "The docs table doesn't list it, so it's unsupported" | A compatibility table is not a support matrix. Query the live API. |
| "I know this technology well" | Gate 2 exists because the field moved since training. |
| "It's obvious which option wins" | Then stating the measured numbers costs nothing. |
| "Recording this is bureaucracy" | The two incidents above each cost more than writing them down would have. |
