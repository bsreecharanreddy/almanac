# Findings — five defects between a green `terraform plan` and a paid run

**Date:** 2026-09-02
**Context:** Phase 2 Tasks 8–9, the Tier 3 backfill burn (92 days, Q3 2025,
~185 GB gz, derived cost $31.71 of the $184 credit — `2026-09-01-cluster-throughput.md`)
and the Photon A/B comparison, both gated behind an explicit user go-ahead
per `CLAUDE.md`.
**Jobs:** `databricks_job.backfill` (id `804815089153164`),
`databricks_job.photon_ab` (id `308819203755958`), workspace
`almanac-dbx`, `westus3`.

`terraform validate` and `terraform plan` were clean throughout — every
defect below is a **runtime** fact about this workspace or this cluster
that no static check could see. Recorded here because three of the five
were caught for the price of a probe or a read, not a cluster launch; the
other two together cost a few cents against the $31.71 the full run would
have cost had either failure mode been silently-wrong instead of
fail-fast.

## The five, in the order found

| # | Found by | Defect | Fix |
|---|---|---|---|
| 1 | Probe, before any cluster ran | `variables.tf`'s `backfill_checkpoint_dir` / `photon_ab_out_dir` defaulted to `/dbfs/FileStore/...` — **public DBFS root is disabled on this workspace** (`databricks fs cp` → `Error: Public DBFS root is disabled`), the same restriction Task 7's calibration run hit once as one of four failed-run causes, now confirmed to cover `FileStore` specifically | Two Unity Catalog managed volumes, `almanac_dbx.burn.{checkpoints,photon_ab}` — `/Volumes/...` is FUSE-mounted and pathlib-compatible, not subject to the restriction. Verified writable by the same probe before trusting it. |
| 2 | Config read, before any cluster ran | `backfill_python_file` / `photon_ab_python_file` / `almanac_wheel` defaulted to `/Workspace/Repos/almanac/...`, which assumes a Databricks Repo linked to this repo's GitHub remote — not configured | `databricks sync` to `/Workspace/Shared/almanac/` instead; three defaults updated to match. Chose `Shared` over a personal `/Workspace/Users/<email>/...` path so a personal email isn't what ends up committed. |
| 3 | Run 1 (`588210730480473`), first real trigger | `databricks.tf`'s `backfill` task never passed `--source-config`, so `scripts/backfill.py` fell back to its relative default (`conf/sources/gharchive.yml`), which resolves against the repo root — true for `make`/CI, false for a job task's working directory on a cluster. `FileNotFoundError` before a single byte of data was read. | New `source_config_workspace_path` variable, wired into both jobs' `parameters`. |
| 4 | Reading `photon_ab.py`'s own `argparse` against the terraform that calls it — never actually run | `databricks_job.photon_ab`'s task parameters opened directly with `--photon`/`--no-photon`, but `main()` dispatches on a required subcommand (`run`/`compare`); the literal token `"run"` was never in the parameter list, which fails before any flag is parsed | Added `"run"` as the first parameter. Fixed in the same commit as #3, one turn ahead of ever triggering that job — caught by reading, not by paying for the same class of failure a second time. |
| 5 | Run 2 (`347766258130286`), second real trigger | `_land_bronze` (and `scripts/calibrate.py`'s identical pattern) called `spark.read.text(str(result.path))` — a bare, scheme-less path. Spark resolves that against Hadoop's `fs.defaultFS`, which is `dbfs:/` on this cluster, **not** the real local disk (`/local_disk0/...`) the file was actually downloaded to via plain Python I/O. Failed `UnsupportedOperationException: Public DBFS root is disabled` on a path that was never really under DBFS at all. Invisible in local dev/CI, where Spark's own default is already `file:///`. | `result.path.as_uri()` in both call sites — forces the `file://` scheme regardless of the cluster's default FS. `scripts/build_silver_fixture.py` has the identical pattern at its one call site and was deliberately left alone: it only ever runs via `local_session()`, never on a cluster, so it was never exposed. New regression test sets a hostile `fs.defaultFS` and asserts the read still finds the file. |

## What it cost

| Run | Outcome | Cost |
|---|---|---|
| `588210730480473` | `FileNotFoundError`, failed inside the first minute of Spark being live | a few cents (cluster provisioning + <1 min compute) |
| `347766258130286` | `UnsupportedOperationException`, failed after fetching real data for day 1 | a few cents |
| #1, #2, #4 | caught by a probe or a read, no cluster launched for these | $0 |

Two failed runs against a derived $31.71 full-run cost — and both failed
*fast*, on the very first day, rather than partway through 92 days of
silently-wrong output. `terraform plan` after each fix reported **no
changes**, confirming the committed defaults now match the live,
verified-working state exactly, so a future `apply` cannot silently
revert any of the four terraform-level fixes.

## The pattern across all five

None of these are exotic, and none would show up in `terraform validate`,
`ruff`, `mypy`, or the local test suite — the same shape as
`2026-09-01-cluster-throughput.md`'s four failed calibration runs, and the
same lesson as Phase 1 Task 2's naive-datetime trap: **a defect that is
structurally invisible from where you're standing is not a defect you
failed to catch, it's a defect the vantage point cannot see.** Local Spark
defaults `fs.defaultFS` to `file:///`; CI never touches a Databricks
Repo or a DBFS root; nothing in this repo's own test suite launches a
real job cluster. The two that were caught for free (#1, #2, #4) were
caught by treating the workspace as a live thing to probe and the
terraform config as something to read against the script it drives,
rather than trusting either because it parsed.
