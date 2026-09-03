# Findings — nine defects between a green `terraform plan` and a paid run

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
that no static check could see. Recorded here because three of the nine
were caught for the price of a probe or a read, not a cluster launch; the
other six together cost well under a dollar against the $31.71 the full
run would have cost had any of these failure modes been silently-wrong
instead of fail-fast.

## The nine, in the order found

| # | Found by | Defect | Fix |
|---|---|---|---|
| 1 | Probe, before any cluster ran | `variables.tf`'s `backfill_checkpoint_dir` / `photon_ab_out_dir` defaulted to `/dbfs/FileStore/...` — **public DBFS root is disabled on this workspace** (`databricks fs cp` → `Error: Public DBFS root is disabled`), the same restriction Task 7's calibration run hit once as one of four failed-run causes, now confirmed to cover `FileStore` specifically | Two Unity Catalog managed volumes, `almanac_dbx.burn.{checkpoints,photon_ab}` — `/Volumes/...` is FUSE-mounted and pathlib-compatible, not subject to the restriction. Verified writable by the same probe before trusting it. |
| 2 | Config read, before any cluster ran | `backfill_python_file` / `photon_ab_python_file` / `almanac_wheel` defaulted to `/Workspace/Repos/almanac/...`, which assumes a Databricks Repo linked to this repo's GitHub remote — not configured | `databricks sync` to `/Workspace/Shared/almanac/` instead; three defaults updated to match. Chose `Shared` over a personal `/Workspace/Users/<email>/...` path so a personal email isn't what ends up committed. |
| 3 | Run 1 (`588210730480473`), first real trigger | `databricks.tf`'s `backfill` task never passed `--source-config`, so `scripts/backfill.py` fell back to its relative default (`conf/sources/gharchive.yml`), which resolves against the repo root — true for `make`/CI, false for a job task's working directory on a cluster. `FileNotFoundError` before a single byte of data was read. | New `source_config_workspace_path` variable, wired into both jobs' `parameters`. |
| 4 | Reading `photon_ab.py`'s own `argparse` against the terraform that calls it — never actually run | `databricks_job.photon_ab`'s task parameters opened directly with `--photon`/`--no-photon`, but `main()` dispatches on a required subcommand (`run`/`compare`); the literal token `"run"` was never in the parameter list, which fails before any flag is parsed | Added `"run"` as the first parameter. Fixed in the same commit as #3, one turn ahead of ever triggering that job — caught by reading, not by paying for the same class of failure a second time. |
| 5 | Run 2 (`347766258130286`), second real trigger | `_land_bronze` (and `scripts/calibrate.py`'s identical pattern) called `spark.read.text(str(result.path))` — a bare, scheme-less path. Spark resolves that against Hadoop's `fs.defaultFS`, which is `dbfs:/` on this cluster, **not** the real local disk (`/local_disk0/...`) the file was actually downloaded to via plain Python I/O. Failed `UnsupportedOperationException: Public DBFS root is disabled` on a path that was never really under DBFS at all. Invisible in local dev/CI, where Spark's own default is already `file:///`. | `result.path.as_uri()` in both call sites — forces the `file://` scheme regardless of the cluster's default FS. `scripts/build_silver_fixture.py` has the identical pattern at its one call site and was deliberately left alone: it only ever runs via `local_session()`, never on a cluster, so it was never exposed. New regression test sets a hostile `fs.defaultFS` and asserts the read still finds the file. |
| 6 | Run 3 (`958902350643134`), third real trigger | The Bronze write to `abfss://bronze@almanaclakekoctmh.dfs.core.windows.net/events` failed `Invalid configuration value detected for fs.azure.account.key` 88s into real compute — this workspace's only Unity Catalog storage credential (`almanac_dbx`, visible via `databricks storage-credentials list`) is scoped to the metastore's own managed storage account, which is what actually backs the UC volumes defect #1 introduced; it was never wired to this project's own lake storage account at all, Unity Catalog or legacy key-based. `terraform plan` never caught it because nothing was missing an argument — the gap was an entire resource class that was never declared. | New `infra/terraform/unity_catalog.tf`: an `azurerm_databricks_access_connector` (system-assigned managed identity) granted `Storage Blob Data Contributor` on the lake storage account via `azurerm_role_assignment`, wired into a `databricks_storage_credential` (Azure managed identity), with a `databricks_external_location` + `databricks_grants` (`READ_FILES`/`WRITE_FILES`/`CREATE_EXTERNAL_TABLE`) per medallion container (bronze/silver/gold/features — all four, not just the two in current use, so gold's turn doesn't repeat this same discovery later). `terraform plan` after: 11 to add, 0 to change, 0 to destroy. |
| 7 | Run 4 (`1069556816504534`), fourth real trigger, first to clear defect #6 | 45 minutes into real compute, the Bronze write in `day.py`'s `_land_bronze` failed `FAILED_READ_FILE.FILE_NOT_EXIST` reading `file:/local_disk0/almanac-staging/2025-07-01-0.json.gz`. `fetch_hours()` downloads each hour's `.json.gz` via plain Python I/O in the driver process; `spark.read.text(result.path.as_uri())` then reads that same path back as a **distributed** Spark job. `/local_disk0` is per-node ephemeral disk, and the `medallion` job cluster runs `num_workers = 4` (multi-node, by design, for Tasks 8-9's fetch parallelization) — any read task Spark schedules on a worker node other than the driver has no such file locally. Same class as defect #5 (a `/local_disk0` path only the driver can see), but a different failure mode: #5 was a wrong URI scheme, fixed once and for all with `.as_uri()`; this is the same correct URI pointing at storage that is fundamentally node-local on a cluster that is not. Invisible in local dev/CI, which runs single-node (`local_session()`, driver == executor). Affects `photon_ab.py` too — it calls the identical `process_day()` → `_land_bronze()` path and both of `photon_ab`'s job clusters also run 4 workers — so fixed in both jobs, not just backfill. | `backfill_staging_dir` new Terraform variable, default `/Volumes/almanac_dbx/burn/staging` — a new Unity Catalog managed volume (`databricks volumes create almanac_dbx burn staging MANAGED`), same shape as defect #1's `checkpoints`/`photon_ab` volumes: FUSE-mounted identically on every cluster node, so a worker's read sees the same file the driver wrote. Wired into both `databricks_job.backfill` and `databricks_job.photon_ab`'s `--staging-dir`. `terraform plan` after: 0 to add, 2 to change (both jobs' `spark_python_task.parameters`), 0 to destroy. `photon_ab`'s `--warehouse`/`--metastore` were deliberately left on `/local_disk0`: Gold's dbt run hasn't been exercised on this cluster yet, so it's unconfirmed whether it hits the same problem, and an embedded Derby metastore's file locking is not verified safe over a FUSE volume the way a plain file read is — flagged for a future run, not fixed speculatively. |
| 8 | Run 5 (`1111960589912821`), fifth real trigger, first to clear defect #7; **misdiagnosed once**, corrected by run 6 (`156595311273405`) | 83 seconds into real compute the same Bronze write failed `[PATH_NOT_FOUND] Path does not exist: file:/Volumes/almanac_dbx/burn/staging/2025-07-01-0.json.gz. SQLSTATE: 42K03`, on hour 0 of day 1. The real defect is the **`file:` scheme**, which defect #7's own fix made wrong: Databricks defines `file:/` as access relative to the driver's local root, resolved by the JVM's `LocalFileSystem`, while a Unity Catalog volume must be handed to Spark as the bare `/Volumes/...` path (or `dbfs:/Volumes/...`) so it routes through the UC filesystem — every Spark example in the volumes docs uses the bare path and `file:/` appears nowhere for volumes. The driver's *Python* process reaches `/Volumes` through the FUSE mount, which is why all 24 downloads succeeded and the files were verifiably on the volume; Spark's JVM, given `file:/Volumes/...`, is looking somewhere else entirely and correctly reports the path as absent. So `.as_uri()` — added for defect #5, when staging was genuinely local disk and a bare path wrongly resolved against `dbfs:/` — became exactly backwards the moment #7 moved staging onto a volume. **First hypothesis, wrong:** that the file was written but not yet *visible*, i.e. FUSE read-after-write lag, inferred from `databricks fs ls` showing all 24 files with matching sizes after the cluster had terminated. That reading was consistent with the evidence but not implied by it — a permanently unreadable path looks identical to a not-yet-readable one at one sample. Run 6 falsified it: a bounded `PATH_NOT_FOUND` retry (3 attempts, 2s apart) failed identically, and the staged files were still sitting on the volume hours later, so no amount of waiting was ever going to clear it. | `almanac.burn.day.spark_path()`: bare path when the file is under `/Volumes`, `.as_uri()` otherwise — `is_relative_to`, not `startswith`, so a local `/Volumes_backup` is not mistaken for the mount. Keeps both lessons instead of trading one for the other, since #5's local-disk case and #8's volume case need opposite schemes and the staging dir is configurable between them. The retry, `Settings.max_land_attempts`/`land_retry_seconds`, and its three tests were **removed**, not kept as insurance: they were written for a transient that the evidence no longer supports, and leaving them would have embedded the wrong explanation in a load-bearing comment. Three new unit tests in `tests/unit/test_spark_path.py` pin both branches and the lookalike-prefix case; the existing hostile-`fs.defaultFS` integration test still covers #5's branch end to end. |
| 9 | Run 7 (`227270110474807`), the run that **succeeded** | The backfill wrote all 92 days — 341,060,851 Bronze rows, 165.987 GB, zero missing hours — printed its full JSON summary, and then reported **FAILED**. The error is `SystemExit: 0`. `scripts/backfill.py` ended with the most ordinary entrypoint idiom in Python, `sys.exit(main())`, and `main()` returned 0. Databricks runs a `spark_python_task` inside an **IPython** shell, where `SystemExit` propagates as an exception and marks the task failed *even when the code is zero* — the run log carries IPython's own tell, `UserWarning: To exit: use 'exit', 'quit', or Ctrl-D`. Note the direction: every earlier defect failed loudly while doing nothing wrong to the data; this one **succeeded completely and reported failure**, which is the safer direction but still wrong, because anything keying off job status — an orchestrator, a retry policy, a portfolio demo — reads it as a failed run. Latent in two more entrypoints that had never been exercised: `scripts/photon_ab.py` (so the Photon A/B would have reported FAILED on success) and `src/almanac/gold/runner.py`. | New `almanac.cli.run_cli(main)`: exit non-zero on failure, return normally on success, never `sys.exit(0)`. A shell that treats a bare return as success and a non-zero exit as failure sees the same thing either way, so nothing is given up. Applied at all three entrypoints that can run as a job task — not to `build_fixtures.py` / `measure_dataset.py` / `size_embeddings.py`, which only ever run locally, the same reasoning that left `build_silver_fixture.py` alone for defect #5. Four new tests in `tests/unit/test_cli.py`: success must not raise, and 1/2/255 must still exit with that code, so the fix cannot silently swallow a real failure. |

## What it cost

| Run | Outcome | Cost |
|---|---|---|
| `588210730480473` | `FileNotFoundError`, failed inside the first minute of Spark being live | a few cents (cluster provisioning + <1 min compute) |
| `347766258130286` | `UnsupportedOperationException`, failed after fetching real data for day 1 | a few cents |
| `958902350643134` | `Invalid configuration value detected for fs.azure.account.key`, failed 88s into real compute, after fetching real data for day 1 | a few cents |
| `1069556816504534` | `FAILED_READ_FILE.FILE_NOT_EXIST`, failed after 45 min of real compute (the first run to clear defect #6 and reach sustained execution) | most of an hour of 4-worker `Standard_D4ds_v6` compute — the most expensive failure of the five real triggers |
| `1111960589912821` | `PATH_NOT_FOUND`, failed after 83s of real compute (the first run to clear defect #7) | ~90s of compute plus cluster setup — cheap, because it failed fast |
| `156595311273405` | `PATH_NOT_FOUND` again, identically, after 67s — the run that falsified defect #8's first diagnosis | ~70s of compute plus cluster setup |
| `227270110474807` | **SUCCESS** — 92/92 days, 341,060,851 rows, 165.987 GB, zero missing hours; reported FAILED only because of defect #9 | **$11.96** — 5.047 billed cluster-hours at $2.370/hr, **38% of the $31.71 estimate** and 6.5% of the $184 credit |
| #1, #2, #4 | caught by a probe or a read, no cluster launched for these | $0 |

Six failed runs against a derived $31.71 full-run cost. The first three
failed *fast*, on the very first day; the fourth (defect #7) is the one
that didn't — it cleared every earlier defect and ran real Spark for 45
minutes before failing, which is exactly the shape the earlier fixes were
supposed to prevent and didn't fully, since #7 is a different failure
mode discovered only once the first four were out of the way. The fifth
and sixth (defect #8) cleared #7 and failed fast again, on the very first
hour of the very first day, for a reason that only exists *because* #7's
fix worked: moving staging onto a UC volume silently inverted the URI
scheme that defect #5 had established as correct. `terraform plan`
after each fix reported **no changes** (or, for #6, exactly the eleven
new resources and nothing touched on anything existing; for #7, exactly
the two jobs' `--staging-dir` and nothing else; #8 needed no `terraform
apply` at all, only a wheel re-upload to the same already-committed
path), confirming the committed defaults now match the live,
verified-working state exactly, so a future `apply` cannot silently
revert any of the terraform-level fixes.

The sixth run is the one worth keeping for its own sake: it cost ~70
seconds of compute to learn that a plausible, evidence-consistent
diagnosis was wrong. Defect #8's first fix treated `PATH_NOT_FOUND` as a
timing problem because the files were provably on the volume — but "the
file is there and Spark can't see it" has two explanations, and only one
of them is fixed by waiting. The retry was the cheaper thing to try and
the wrong thing to try: it tested a hypothesis instead of establishing
which component was actually failing, which is the exact failure loop
`CLAUDE.md` warns about. What settled it was reading the scheme in the
error string against the vendor docs for the storage type — available for
free, before run 6, had the question been "which filesystem is resolving
this path?" instead of "why might this be slow?".

## Open, not diagnosed — 7 staging files survived cleanup

Recorded as an observation because the cause is **not** established, and
this document has already been wrong once today by writing a plausible
hypothesis down as a cause (defect #8).

**Observed:** after the successful run, `/Volumes/almanac_dbx/burn/staging`
still held 7 files — `2025-09-30` hours 3–9, ~600 MB. Only the final day.
`_clear_downloads` raised nothing, and the summary printed after it, so the
loop completed.

**Ruled out:** a write-after-clear race (the files were written 21:37:13,
the clear ran at ~21:40:31), and delete-path eventual consistency on the
volume (still present on a re-list 19 minutes later, and again after that).

**Two candidates, neither confirmed:**

1. *Cluster shutdown truncating in-flight FUSE deletes.* Fits the evidence
   well — it predicts **only the last day** is affected, which is exactly
   what was seen. Every earlier day had minutes of subsequent activity for
   its deletes to flush; the final day's had ~8 seconds before the driver
   exited and the cluster tore down.
2. *`Path.glob()` returning a lazy generator while the loop unlinks from the
   same directory.* A real POSIX hazard — mutating a directory during
   `readdir` iteration leaves it undefined whether not-yet-returned entries
   appear. But it predicts leftovers **scattered across many days**, and 91
   of 92 days cleared perfectly. Largely ruled out by that alone.

**Deliberately not fixed.** Materialising the glob with `list()` is a
one-word change and defensible on its own merits, but shipping it now would
repeat defect #8's exact mistake: a plausible fix for an unconfirmed cause,
which then reads as a solved problem to the next person. Impact is a
one-time ~600 MB on a volume, and a re-run of that day overwrites the files
anyway.

**What would settle it:** run one day, then list staging twice — once while
the cluster is still alive, once after it terminates. Candidate 1 predicts
the files are gone in the first listing and present in the second; candidate
2 predicts they are present in both.

## The pattern across all nine

None of these are exotic, and none would show up in `terraform validate`,
`ruff`, `mypy`, or the local test suite — the same shape as
`2026-09-01-cluster-throughput.md`'s four failed calibration runs, and the
same lesson as Phase 1 Task 2's naive-datetime trap: **a defect that is
structurally invisible from where you're standing is not a defect you
failed to catch, it's a defect the vantage point cannot see.** Local Spark
defaults `fs.defaultFS` to `file:///`; CI never touches a Databricks
Repo, a DBFS root, or a storage credential; nothing in this repo's own
test suite launches a real job cluster or grants real Azure IAM. The
three that were caught for free (#1, #2, #4) were caught by treating the
workspace as a live thing to probe and the terraform config as something
to read against the script it drives, rather than trusting either because
it parsed — but #6 is the one none of that discipline could have caught
in advance: `terraform validate` and `plan` are only as complete as the
resources someone thought to declare, and a storage credential that was
simply never written down produces no diff to notice, only a runtime
failure once a cluster actually tries to use the path.

Defect #8 adds a second pattern the others don't show: **a fix can
invalidate the premise another fix depends on, silently.** `.as_uri()`
was correct
for as long as staging was local disk, and became wrong the moment #7
moved staging to a volume — two files apart, no shared symbol, nothing a
type checker or a test could connect. The regression test written for #5
still passed, because it tests the local-disk branch that is still there
and still right. What was missing was any test of the case #7 had just
created. The generalizable form: when a fix changes *where* something
lives, re-check every decision that was made about how to address it.
