"""Gold's PR models are built from the event stream, never embedded payloads."""

import re
from pathlib import Path

FACT = Path("dbt/models/gold/fact_pull_request.sql")
INT = Path("dbt/models/gold/int_pr_events.sql")

# The embedded lifecycle objects §4.3a forbids. The models read Silver
# columns / int_pr_events, never these.
FORBIDDEN_PATHS = (
    "payload.pull_request",
    "payload.user",
    "payload.pull_request.created_at",
    "pull_request.user",
)


def _sql_without_comments(path: Path) -> str:
    """`path`'s SQL with `--` line comments and `{# #}` Jinja blocks stripped."""
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
