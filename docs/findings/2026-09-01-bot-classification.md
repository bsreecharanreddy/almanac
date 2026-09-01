# Findings — bot classification, and why volume-weighted precision lies

**Date:** 2026-09-01
**Sample:** 6,002,410 events (see `2026-09-01-dataset-measurements.md`)
**Outcome:** the inherited rule was wrong on its stated failure mode, wrong
in the opposite direction from what was documented, and has been changed.
Evidence for ADR-005.

---

## 1. The documented false positives are not false positives

The rule inherited from the source material was:

```
is_bot = login ENDSWITH '[bot]'
      OR login IN (curated list)
      OR login RLIKE '(?i)(bot|automation|ci)$'
```

with `robotframework` and `Abbott` named as its known false positives.

**Neither matches it.** The clause is anchored at the end:
`robotframework` ends in "work", `Abbott` ends in "tt". They would be
false positives of an *unanchored* search for "bot", which is not the
rule as written. Verified directly, and now pinned by test so the claim
cannot drift back in.

## 2. Volume-weighted inspection said the rule was fine. It was not.

Ranking regex-clause matches by event count gives a top 30 that is
**100% genuine bots** — `regro-cf-autotick-bot`, `wingetbot`,
`k8s-ci-robot`, `ursabot`, `hubot`, `BrewTestBot`, `facebook-github-bot`,
`renovate-bot`. Inspect that list and the rule looks excellent.

That inspection is **structurally incapable of finding the problem.**
Bots are high-volume *by definition* — that is what makes them bots — so
volume-weighting surfaces true positives and buries false ones in the
tail. The precision that matters for a business rule is per **distinct
login**, not per event.

Counting distinct logins instead:

| Clause | Distinct logins matched |
|---|---|
| regex heuristic | 3,132 |
| `[bot]` suffix | 2,110 |
| matched nothing | 1,022,194 |

Broken down by which alternative fired:

| Alternative | Distinct logins |
|---|---|
| `bot$` | 2,051 |
| **`ci$`** | **1,002** |
| `automation$` | 79 |

## 3. The `ci$` clause is 86.7% false positives

Of the 1,002 distinct logins ending in "ci":

| Shape | Count | Share | Example |
|---|---|---|---|
| Bare `…ci` | **869** | **86.7%** | `AlexandruPopovici`, `BarisYazici` |
| Separated `…-ci` | 120 | 12.0% | `swift-ci`, `aws-sdk-rust-ci` |
| camelCase `…CI` | 13 | 1.3% | `VenlyCI`, `CheckmkCI` |

The bare group is overwhelmingly **human surnames** — Turkish (`Akinci`,
`Yazici`, `Ekinci`, `Avci`, `Ayranci`, `Baltaci`, `Kulekci`) and Italian
(`Federici`, `Popovici`, `Falcucci`, `Bramucci`, `Dedominici`, `Vicci`).
These are real people, and the rule was labelling every one of them a bot.

The separated group is overwhelmingly real automation: `LinuxServer-CI`,
`Syncfusion-CI`, `apicurio-ci`, `akeyless-ci`, `bonita-ci`, `chaos-ci`.

**A separator is the discriminator**, and it is measurable rather than a
matter of taste.

## 4. The rule, as changed

```python
BOT_REGEX = re.compile(r"(?i:(bot|automation)$)|(?i:[-_.]ci$)|[a-z0-9]CI$")
```

The third alternative is **case-sensitive on purpose**: it requires a
literal uppercase `CI` preceded by a lowercase character, which admits
`VenlyCI` and `CheckmkCI` while still rejecting all-caps human names like
`AlperenYABACI` and `AitanaESCI`.

**Precision gained:** 869 distinct human logins no longer misclassified.

**Recall cost, accepted and pinned by test:** logins that *embed* a CI
service rather than suffixing it — `cw-circleci`, `seek-oss-circleci` —
are now missed, because "circleci" has "e" before the final "ci", not a
separator. Recoverable via the curated list if it ever matters. Recorded
rather than papered over, so the tradeoff stays visible.

---

## The transferable lesson

**Precision measured on the head of a skewed distribution is not
precision.** The entity most likely to trip a heuristic is also the
entity most likely to dominate by volume, so eyeballing top-N by count
confirms the rule while the errors sit in a tail you never look at.

Any classification rule over a power-law population — bots, spam,
fraud, abuse — needs its error rate computed per distinct entity, not per
event. Had this rule shipped, every metric segmented by human-vs-bot
would have quietly moved ~869 real contributors into the bot bucket, and
the dashboards would have looked entirely reasonable.
