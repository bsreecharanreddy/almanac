"""`fact_pull_request` is built from the event stream, never embedded payloads.

Design doc §4.3a is a construction rule that a future edit will quietly
break: reading `payload.pull_request.created_at` is easier than deriving
`opened_at` from the `opened` event, and it stops working in October 2025
(§12 trap 12). This is a grep-style guard, deliberately crude, so the rule
is enforced mechanically rather than trusted to code review.

`merged` and `draft` are the one documented exception -- they are genuinely
not recoverable from events alone (§4.3a) and come from `payload` via
Silver's already-parsed `pr_merged` / `pr_draft` columns, era-bound and
null-safe. The model still never names a raw `payload.*` path itself.
"""

from pathlib import Path

MODEL = Path("dbt/models/gold/fact_pull_request.sql")

# The embedded lifecycle object §4.3a forbids. `payload.action` and
# `payload.number` are explicitly in bounds and are parsed in Silver, not
# here, so the model reads neither -- it selects from `silver.events`.
FORBIDDEN_PATHS = (
    "payload.pull_request",
    "payload.user",
    "payload.pull_request.created_at",
    "pull_request.user",
)


def test_the_model_exists() -> None:
    assert MODEL.exists(), f"{MODEL} not created yet"


def _sql_without_comments() -> str:
    """The model's SQL with `--` line comments stripped.

    The guard is about what the query *reads*, not what a comment explains
    -- and this model's docstring names `payload.pull_request` precisely to
    document why it does not read it. No string literal in this SQL
    contains `--`, so a plain line-wise strip is exact here.
    """
    lines = (line.split("--", 1)[0] for line in MODEL.read_text().splitlines())
    return "\n".join(lines).lower()


def test_no_column_reads_an_embedded_payload_object() -> None:
    sql = _sql_without_comments()
    hits = [path for path in FORBIDDEN_PATHS if path in sql]
    assert not hits, (
        f"{MODEL} references embedded payload objects {hits}; §4.3a requires "
        "every fact column to come from an event-level field (via Silver)."
    )


def test_the_model_selects_from_silver_not_a_raw_payload() -> None:
    """The positive half: it reads the parsed Silver contract."""
    sql = _sql_without_comments()
    assert "source('silver', 'events')" in sql or 'source("silver", "events")' in sql
