"""`fact_pull_request`'s accumulating-snapshot behaviour, across real dbt runs."""

import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql import functions as F

pytestmark = [pytest.mark.spark, pytest.mark.integration]

_SILVER_SCHEMA = (
    "repo_id long, pr_number long, created_at timestamp, event_type string, "
    "event_action string, actor_login string, pr_merged boolean, pr_draft boolean, "
    "is_pr_comment boolean, ingested_at timestamp"
)

_Event = tuple[
    int, int, datetime, str, str | None, str | None, bool | None, bool | None, bool | None, datetime
]


def _event(
    repo_id: int,
    pr_number: int,
    created_at: datetime,
    ingested_at: datetime,
    *,
    event_type: str,
    action: str | None = None,
    actor: str | None = None,
    merged: bool | None = None,
    draft: bool | None = None,
    is_pr_comment: bool | None = None,
) -> _Event:
    """One Silver row in the shape `int_pr_events` reads."""
    return (
        repo_id,
        pr_number,
        created_at,
        event_type,
        action,
        actor,
        merged,
        draft,
        is_pr_comment,
        ingested_at,
    )


def _append_silver(spark: SparkSession, path: Path, rows: list[_Event]) -> None:
    """More of the archive landing -- `append`, never a replacement."""
    spark.createDataFrame(rows, _SILVER_SCHEMA).write.format("delta").mode("append").save(str(path))


def _build_gold(paths: dict[str, Path]) -> None:
    # `+fact_pull_request`: the fact plus its upstream `int_pr_events` view.
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "almanac.gold.runner",
            "--warehouse",
            str(paths["warehouse"]),
            "--metastore",
            str(paths["metastore"]),
            "--target-path",
            str(paths["target_path"]),
            "--silver-path",
            str(paths["silver_path"]),
            "build",
            "--select",
            "+fact_pull_request",
            # cautious: skip the fact -> dim_repo relationships test, whose
            # other parent this partial build does not include. `make dbt`
            # (full build) and test_gold_contracts.py cover that test.
            "--indirect-selection",
            "cautious",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, f"dbt build failed:\n{result.stdout}\n{result.stderr}"


_TS_COLUMNS = ("opened_at", "closed_at", "first_review_at", "first_response_at")


def _fact(spark: SparkSession, warehouse: Path) -> DataFrame:
    """The fact, with every timestamp column projected to an epoch second."""
    raw = spark.read.format("delta").load(str(warehouse / "gold.db" / "fact_pull_request"))
    ts = [f"unix_timestamp({c}) as {c}" for c in _TS_COLUMNS]
    others = [c for c in raw.columns if c not in _TS_COLUMNS]
    return raw.selectExpr(*others, *ts)


def epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def _one(df: DataFrame, repo_id: int, pr_number: int) -> Row:
    rows = df.filter((df.repo_id == repo_id) & (df.pr_number == pr_number)).collect()
    assert len(rows) == 1, f"expected exactly one row for ({repo_id}, {pr_number}), got {rows}"
    return rows[0]


@pytest.fixture
def gold_paths(tmp_path: Path) -> dict[str, Path]:
    silver = tmp_path / "silver"
    return {
        "warehouse": tmp_path / "warehouse",
        "metastore": tmp_path / "metastore",
        "target_path": tmp_path / "target",
        "silver_path": silver,
        "clean_path": silver / "clean",
        "quarantine_path": silver / "quarantine",
    }


@pytest.fixture
def empty_quarantine(spark: SparkSession, gold_paths: dict[str, Path]) -> None:
    """`register_silver_sources` registers both tables, so the quarantine path must exist."""
    spark.createDataFrame([], _SILVER_SCHEMA).write.format("delta").save(
        str(gold_paths["quarantine_path"])
    )


T0 = datetime(2025, 8, 13, 0, 0, tzinfo=UTC)
T1 = datetime(2025, 8, 14, 0, 0, tzinfo=UTC)
T2 = datetime(2025, 8, 15, 0, 0, tzinfo=UTC)


def test_out_of_order_open_after_close_keeps_both_timestamps(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """`opened` arriving in a later batch than `closed` still keeps the earliest timestamps."""
    c_close = datetime(2025, 8, 13, 15, 0, tzinfo=UTC)
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [_event(700, 1, c_close, T0, event_type="PullRequestEvent", action="closed", merged=True)],
    )
    _build_gold(gold_paths)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [_event(700, 1, c_open, T1, event_type="PullRequestEvent", action="opened", actor="alice")],
    )
    _build_gold(gold_paths)

    row = _one(_fact(spark, gold_paths["warehouse"]), 700, 1)
    assert row["opened_at"] == epoch(c_open)
    assert row["closed_at"] == epoch(c_close), "a later null batch must not clobber closed_at"
    assert row["author_login"] == "alice"
    assert row["merged"] is True


def test_one_row_per_pr_accumulates_every_lifecycle_column(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """opened, first review and closed land in three runs; the snapshot still accumulates."""
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)
    c_review_late = datetime(2025, 8, 13, 18, 0, tzinfo=UTC)
    c_review_early = datetime(2025, 8, 13, 12, 0, tzinfo=UTC)
    c_close = datetime(2025, 8, 14, 10, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(
                701,
                5,
                c_open,
                T0,
                event_type="PullRequestEvent",
                action="opened",
                actor="bob",
                draft=False,
            ),
            _event(701, 5, c_review_late, T0, event_type="PullRequestReviewEvent", actor="carol"),
        ],
    )
    _build_gold(gold_paths)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(701, 5, c_review_early, T1, event_type="PullRequestReviewEvent", actor="dave"),
            _event(
                701, 5, c_close, T1, event_type="PullRequestEvent", action="closed", merged=True
            ),
        ],
    )
    _build_gold(gold_paths)

    fact = _fact(spark, gold_paths["warehouse"])
    assert fact.filter((fact.repo_id == 701) & (fact.pr_number == 5)).count() == 1
    row = _one(fact, 701, 5)
    assert row["opened_at"] == epoch(c_open)
    assert row["first_review_at"] == epoch(c_review_early), "earliest review across all batches"
    assert row["closed_at"] == epoch(c_close)
    assert row["author_login"] == "bob"
    assert row["draft"] is False
    assert row["is_censored"] is False


def test_a_pr_open_at_the_window_edge_is_flagged_censored(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """No terminal event observed -> `is_censored`, never a fabricated close."""
    c_open = datetime(2025, 8, 15, 9, 0, tzinfo=UTC)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(
                702, 9, c_open, T2, event_type="PullRequestEvent", action="opened", actor="erin"
            ),
        ],
    )
    _build_gold(gold_paths)

    row = _one(_fact(spark, gold_paths["warehouse"]), 702, 9)
    assert row["closed_at"] is None
    assert row["is_censored"] is True


def test_second_build_merges_rather_than_rebuilding(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """Phase 2 exit gate: a Gold model's second run is a `MERGE`, read from the Delta log."""
    c_open = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)
    row = [_event(703, 2, c_open, T0, event_type="PullRequestEvent", action="opened", actor="ivan")]

    _append_silver(spark, gold_paths["clean_path"], row)
    _build_gold(gold_paths)
    _append_silver(spark, gold_paths["clean_path"], row)  # same event again, later batch
    _build_gold(gold_paths)

    table = str(gold_paths["warehouse"] / "gold.db" / "fact_pull_request")
    ops = [
        r["operation"]
        for r in spark.sql(f"describe history delta.`{table}`").orderBy("version").collect()
    ]
    assert ops[0] == "CREATE OR REPLACE TABLE AS SELECT"
    assert "MERGE" in ops[1:], f"second build did not merge; ops were {ops}"
    assert _fact(spark, gold_paths["warehouse"]).filter(F.col("repo_id") == 703).count() == 1


# --- The label: time_to_first_human_response (§5.1) ------------------------

OPEN = datetime(2025, 8, 13, 9, 0, tzinfo=UTC)


def _comment(
    repo_id: int,
    pr_number: int,
    at: datetime,
    ingested: datetime,
    actor: str,
    *,
    genuine_issue: bool = False,
) -> _Event:
    """An IssueCommentEvent, on a PR unless `genuine_issue`."""
    return _event(
        repo_id,
        pr_number,
        at,
        ingested,
        event_type="IssueCommentEvent",
        actor=actor,
        is_pr_comment=not genuine_issue,
    )


def test_label_counts_only_non_author_human_responses(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """One build, four PRs, each pinning one attribution rule."""
    rows: list[_Event] = [
        # PR 1: the author's own comment is not a response; the reviewer's is.
        _event(900, 1, OPEN, T0, event_type="PullRequestEvent", action="opened", actor="ann"),
        _comment(900, 1, OPEN + timedelta(hours=1), T0, "ann"),
        _event(
            900, 1, OPEN + timedelta(hours=3), T0, event_type="PullRequestReviewEvent", actor="bo"
        ),
        # PR 2: a genuine issue comment (is_pr_comment = false) never counts.
        _event(900, 2, OPEN, T0, event_type="PullRequestEvent", action="opened", actor="ann"),
        _comment(900, 2, OPEN + timedelta(hours=1), T0, "cy", genuine_issue=True),
        # PR 3: opened by a bot; a human review still lands as the response.
        _event(
            900, 3, OPEN, T0, event_type="PullRequestEvent", action="opened", actor="renovate[bot]"
        ),
        _event(
            900, 3, OPEN + timedelta(hours=2), T0, event_type="PullRequestReviewEvent", actor="di"
        ),
        # PR 4: legacy shape -- no PullRequestReviewEvent in that era, so the
        # label rests on a review *comment* from someone other than the author.
        _event(
            1400,
            4,
            OPEN,
            T0,
            event_type="PullRequestEvent",
            action="opened",
            actor="el",
            draft=None,
        ),
        _event(
            1400,
            4,
            OPEN + timedelta(hours=5),
            T0,
            event_type="PullRequestReviewCommentEvent",
            actor="fi",
        ),
    ]
    _append_silver(spark, gold_paths["clean_path"], rows)
    _build_gold(gold_paths)
    fact = _fact(spark, gold_paths["warehouse"])

    pr1 = _one(fact, 900, 1)
    assert pr1["first_response_at"] == epoch(OPEN + timedelta(hours=3)), (
        "author's own comment skipped"
    )
    assert pr1["time_to_first_response_seconds"] == 3 * 3600

    pr2 = _one(fact, 900, 2)
    assert pr2["first_response_at"] is None, "a genuine issue comment is not a PR response"
    assert pr2["label_exclusion"] == "right_censored"

    pr3 = _one(fact, 900, 3)
    assert pr3["author_is_bot"] is True
    assert pr3["first_response_at"] == epoch(OPEN + timedelta(hours=2)), (
        "bot PRs are flagged, not dropped"
    )

    pr4 = _one(fact, 1400, 4)
    assert pr4["first_review_at"] is None, "no PullRequestReviewEvent exists in the legacy era"
    assert pr4["first_response_at"] == epoch(OPEN + timedelta(hours=5))
    assert pr4["time_to_first_response_seconds"] == 5 * 3600


def test_label_exclusions_are_stated_never_silent(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    rows: list[_Event] = [
        # Draft: a response exists, but a draft accrues no SLA time.
        _event(
            910,
            1,
            OPEN,
            T0,
            event_type="PullRequestEvent",
            action="opened",
            actor="ann",
            draft=True,
        ),
        _event(
            910, 1, OPEN + timedelta(hours=2), T0, event_type="PullRequestReviewEvent", actor="bo"
        ),
        # Null draft (legacy) is unknown, not "not a draft is false" -> still labelled.
        _event(
            910,
            2,
            OPEN,
            T0,
            event_type="PullRequestEvent",
            action="opened",
            actor="ann",
            draft=None,
        ),
        _event(
            910, 2, OPEN + timedelta(hours=2), T0, event_type="PullRequestReviewEvent", actor="bo"
        ),
        # No observed open -> author unknown -> the exclusion cannot be evaluated.
        _event(
            910, 3, OPEN + timedelta(hours=1), T0, event_type="PullRequestReviewEvent", actor="bo"
        ),
        # Opened and closed, but no response ever -> a stated exclusion, not
        # a row with a null label and a null reason.
        _event(910, 4, OPEN, T0, event_type="PullRequestEvent", action="opened", actor="ann"),
        _event(
            910,
            4,
            OPEN + timedelta(hours=4),
            T0,
            event_type="PullRequestEvent",
            action="closed",
            merged=False,
        ),
    ]
    _append_silver(spark, gold_paths["clean_path"], rows)
    _build_gold(gold_paths)
    fact = _fact(spark, gold_paths["warehouse"])

    draft = _one(fact, 910, 1)
    assert draft["label_exclusion"] == "draft"
    assert draft["time_to_first_response_seconds"] is None

    null_draft = _one(fact, 910, 2)
    assert null_draft["label_exclusion"] is None, "a null draft flag is not a draft"
    assert null_draft["time_to_first_response_seconds"] == 2 * 3600

    no_response = _one(fact, 910, 4)
    assert no_response["label_exclusion"] == "closed_no_response"
    assert no_response["time_to_first_response_seconds"] is None

    # The core invariant: label is non-null exactly when there is no exclusion.
    mismatched = fact.filter(
        "(label_exclusion is null) != (time_to_first_response_seconds is not null)"
    )
    assert mismatched.count() == 0

    orphan = _one(fact, 910, 3)
    assert orphan["opened_at"] is None
    assert orphan["label_exclusion"] == "author_unobserved"
    assert orphan["time_to_first_response_seconds"] is None


def test_label_is_stable_when_a_later_response_arrives(
    spark: SparkSession, gold_paths: dict[str, Path], empty_quarantine: None
) -> None:
    """Once the first response is recorded, a later one does not move the label."""
    first = OPEN + timedelta(hours=2)
    later = OPEN + timedelta(hours=6)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [
            _event(920, 1, OPEN, T0, event_type="PullRequestEvent", action="opened", actor="ann"),
            _event(920, 1, first, T0, event_type="PullRequestReviewEvent", actor="bo"),
        ],
    )
    _build_gold(gold_paths)
    assert _one(_fact(spark, gold_paths["warehouse"]), 920, 1)["first_response_at"] == epoch(first)

    _append_silver(
        spark,
        gold_paths["clean_path"],
        [_event(920, 1, later, T1, event_type="PullRequestReviewEvent", actor="cy")],
    )
    _build_gold(gold_paths)
    row = _one(_fact(spark, gold_paths["warehouse"]), 920, 1)
    assert row["first_response_at"] == epoch(first), "a later response must not move the label"
    assert row["time_to_first_response_seconds"] == 2 * 3600
