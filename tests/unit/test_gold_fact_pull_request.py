"""Gold's PR models are built from the event stream, never embedded payloads.

Design doc §4.3a is a construction rule that a future edit will quietly
break: reading `payload.pull_request.created_at` is easier than deriving
`opened_at` from the `opened` event, and it stops working in October 2025
(§12 trap 12). This is a grep-style guard, deliberately crude, so the rule
is enforced mechanically rather than trusted to code review.

`merged` and `draft` are the one documented exception -- they are genuinely
not recoverable from events alone (§4.3a) and come from `payload` via
Silver's already-parsed `pr_merged` / `pr_draft` columns, era-bound and
null-safe. Neither model names a raw `payload.*` path itself.
"""

import re
from pathlib import Path

FACT = Path("dbt/models/gold/fact_pull_request.sql")
INT = Path("dbt/models/gold/int_pr_events.sql")

# The embedded lifecycle object §4.3a forbids. `payload.action` and
# `payload.number` are explicitly in bounds and are parsed in Silver, so
# the models read neither -- `int_pr_events` reads `silver.events`, the fact
# reads `int_pr_events`.
FORBIDDEN_PATHS = (
    "payload.pull_request",
    "payload.user",
    "payload.pull_request.created_at",
    "pull_request.user",
)


def _sql_without_comments(path: Path) -> str:
    """`path`'s SQL with `--` line comments and `{# #}` Jinja blocks stripped.

    The guard is about what a query *reads*, not what a comment explains --
    and these files name `payload.pull_request` precisely to document why
    they do not read it. No string literal in this SQL contains `--`.
    """
    text = re.sub(r"\{#.*?#\}", "", path.read_text(), flags=re.DOTALL)
    lines = (line.split("--", 1)[0] for line in text.splitlines())
    return "\n".join(lines).lower()


def test_both_models_exist() -> None:
    assert FACT.exists(), f"{FACT} not created yet"
    assert INT.exists(), f"{INT} not created yet"


def test_no_column_reads_an_embedded_payload_object() -> None:
    for path in (FACT, INT):
        sql = _sql_without_comments(path)
        hits = [p for p in FORBIDDEN_PATHS if p in sql]
        assert not hits, (
            f"{path} references embedded payload objects {hits}; §4.3a requires "
            "every fact column to come from an event-level field."
        )


def test_int_pr_events_reads_silver_and_the_fact_reads_int_pr_events() -> None:
    int_sql = _sql_without_comments(INT)
    assert "source('silver', 'events')" in int_sql or 'source("silver", "events")' in int_sql

    fact_sql = _sql_without_comments(FACT)
    assert "ref('int_pr_events')" in fact_sql or 'ref("int_pr_events")' in fact_sql
    assert "source(" not in fact_sql, "the fact reads the int model, not silver directly"
