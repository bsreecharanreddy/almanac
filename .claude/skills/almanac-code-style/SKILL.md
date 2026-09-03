---
name: almanac-code-style
description: Use while writing or modifying any code in this repo, and before calling a code-writing task done — a checklist for simplifying logic, naming, edge-case coverage, deduplication, guard clauses, composition over inheritance, mapping-based dispatch over if/elif chains, generators, context managers, and comment/docstring discipline (structured and sparse, never narrative paragraphs). Not incident-derived like this repo's other skills — a deliberate, user-directed standing convention (2026-09-02), recorded as an exception rather than pretending it grew from a mistake this project made.
---

# Code style in Almanac

**Why this one is different from every other skill here.** CLAUDE.md's
own rule is that tooling gets written only after a real, named incident —
"writing one now would be guessing at what its bugs look like." This
skill has no incident behind it; it exists because the user asked for it
directly, as a standing convention rather than a lesson learned. Follow
it, but don't let its existence imply a mistake happened that didn't.

**This is a checklist for judgment, not a set of rules to apply
mechanically.** Every item below has a real counter-case where applying
it would make the code worse. Reaching for the pattern without checking
whether *this* case fits it is the same mistake as not reaching for it at
all — cargo-culting in either direction.

## The checklist

**1. Simplify the logic.** If a line needs a comment to explain *what* it
does (not *why*), it is usually not simpler, just shorter. This repo's
existing style already favors this — `payloads.parse_events`,
`quality.apply_rules` read top to bottom with no nested branching. Match
that register in new code.

**2. Name for what a reader needs, not for what the code does.**
`SILVER_COLUMNS`, `_content_hash`, `expected_hours` all say what the
caller gets, not how it's computed. A name that restates the
implementation (`filter_and_map_rows`) is doing the comment's job badly.

**3. Handle edge cases explicitly, at the point they matter.** This repo
already has a strong culture here — `coalesce(expr, False)` on every
quality rule, null-safe `<=>` in every SCD2 comparison, era-bound nulls
(`draft`, `pr_merged`) treated as "unknown" rather than folded to `False`.
Extend the same discipline to code that isn't a Spark transform: a Python
script or a dbt macro should ask "what happens on empty input, a null, a
single row, a duplicate key" before being called done, the same way
`silver.py` and `eras.py` already do.

**4. Reduce duplication — but only real duplication.** Two blocks that
look similar but encode two different invariants are not duplication;
extracting them into one function is the mistake, not the fix (that
function will need a parameter to decide which invariant applies, and
now nobody can read it without knowing which caller they're looking at).
Duplication worth removing is the same *fact* stated twice, one of which
will go stale silently — exactly the shape of the two-places-one-contract
failures already recorded in STATUS.md's verification log (Phase 1 Tasks
4–6), and exactly why `tests/helpers.py`'s `RAW_SCHEMA` / `raw()` exist as
one shared thing instead of two. The three `git commit` hooks in this
directory are the same worked example at the tooling layer: two copies of
`files_for_commit` were tolerable, a third was the actual duplication —
now in `lib.sh`, sourced by all three.

**5. Guard clauses over nested conditionals.** Return or raise on the
exceptional case first; let the normal path read at the top level rather
than three `if`s deep. `add_ingestion_metadata`'s naive-datetime check is
the existing model: it raises immediately and the real work is unindented
below it.

**6. Composition over inheritance.** This repo has no class hierarchy
anywhere, and that's not an oversight — `SourceConfig` and `QualityRule`
are composed frozen Pydantic models, not a base/subclass pair, and Spark's
own `DataFrame -> DataFrame` transform style is functions composed by the
caller (`silver.py`'s `apply_rules(deduplicate(normalize_events(...)))`),
never a class one transform subclasses another to extend. If a new piece
of code seems to want inheritance to share behavior, that is usually a
sign the shared part should be its own function or a small composed
object instead.

**7. A mapping over an if/elif chain — on a fixed, closed set of
values.** `tests/helpers.py`'s `_parsed_defaults()` dict is the existing
model. The payoff shows up at three or more branches dispatching on a
value from a known, bounded set — an enum (`SchemaEra`), an event type, a
severity. It is **not** a universal replacement for `if`: a two-way
branch, a chain of genuinely different *conditions* (not a single value
switching on itself), or dispatch on an open-ended/growing set (a new
GitHub event type must route through `event_type_present`'s `warn`, not
require a new dict entry to avoid crashing) should usually stay an `if`.

**8. Generators, where the whole collection doesn't need to exist at
once — in plain Python.** This applies to the Python-side scripts
(`scripts/*.py`), never to a PySpark `DataFrame`: Spark's execution is
already lazy and distributed, so "make this a generator" is a category
error there — the transform functions in `pipeline/` are correct as
`DataFrame -> DataFrame`, not `Iterator[Row] -> Iterator[Row]`. Reach for
a generator only where real Python-side iteration happens over something
that could be large and is only walked once (e.g. `hours_in_range`, which
already yields rather than building a list).

**9. Context managers for anything acquired outside the shared Spark
session.** A temp file, a subprocess handle, a DB cursor — `with`, so
cleanup runs even on the exception path. The `spark` fixture in
`conftest.py` is this project's existing model for the *shape* of the
idea even though a `SparkSession` isn't opened with a bare `with`: it
`yield`s the resource and guarantees `.stop()` runs after, which is
exactly what a context manager formalizes. New code acquiring a resource
that isn't already covered by that fixture should use an actual `with`,
not a manual try/finally reinventing it.

**10. Comments and docstrings: structured, not narrative.** The reader is
an interviewer at a frontier lab or a FAANG-tier company. The code
carries the story; comments are sparse and load-bearing.

- **No "what" comments.** `# increment the counter` over `count += 1` is
  noise — the fix is a better name or a smaller function, not the comment.
- **A comment earns its place by explaining "why"**: a non-obvious
  constraint, a measured fact, an edge case that bit someone. `# .part
  then rename: a crash never leaves a half-written marker` stays; a
  paragraph restating the algorithm goes.
- **One-line docstrings on functions.** No multi-paragraph docstrings,
  no parameter tables (unless explicitly asked). A function that seems to
  need one is usually two functions, or its name and types are doing too
  little.
- **Module docstrings are one line** — what it is for, not how it came
  to exist.
- **No tutorial prose, measured-number essays, or chat leaks in code.**
  That story belongs in `docs/` (design docs, findings, STATUS), where a
  reader who wants it finds it and a reader who wants the code isn't
  wading through it.

The Phase 0–1 modules were swept once against this rule in a dedicated
pass (2026-09-02). Going forward it applies as code is written; a later
opportunistic reformat of untouched code is still out of scope per
CLAUDE.md's scope discipline.

## When this runs

Apply it while writing new code, and re-read the diff against it before
calling a task done — the same checkpoint moment `docs/STATUS.md` updates
happen at. It is **not** a license to touch unrelated code while working
on something else; CLAUDE.md's scope-discipline rule ("don't reformat or
'improve' adjacent code... while touching a file for something else")
still governs. A file that needs a real pass against this checklist and
isn't already part of the current task's diff is a note for a future,
explicitly scoped task, not a silent addition to this one's commit.
