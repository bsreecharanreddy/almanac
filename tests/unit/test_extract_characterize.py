"""Window characterization: is a candidate window fit to run the pipeline over?

Design doc 4.8 decision 2. The 2026 firehose is intermittently degraded --
hour-9 file size swings 50x across 14 consecutive days -- so a window picked
by recency lands on the degraded side and produces an uncomputable label and
degenerate panels. Quality is measured before use, and a bad window is
refused rather than rendered.

Every fixture below is a real measurement (2026-09-08), not an invention.
"""

import gzip
import json
from pathlib import Path

from almanac.extract.characterize import (
    MIN_OPENED_PRS,
    HourCounts,
    count_hour,
    main,
    profile,
)

# --- the calibration set, measured by downloading and parsing real files ---

# 2026-09-05 09:00 -- a healthy hour: 37.3% PullRequestEvent, review signal
# at 33.0% (IssueComment 18.2 + PRReview 7.7 + PRReviewComment 7.1).
HEALTHY_2026 = HourCounts(
    total_events=74_595,
    opened_prs=10_544,
    type_counts={
        "PullRequestEvent": 27_796,
        "IssueCommentEvent": 13_597,
        "IssuesEvent": 11_075,
        "WatchEvent": 6_283,
        "PullRequestReviewEvent": 5_726,
        "PullRequestReviewCommentEvent": 5_267,
    },
    pr_payloads_with_draft=0,
)

# 2026-09-08 14:00 -- degraded: 94% PushEvent, PullRequestEvent under 1%,
# PullRequestReviewEvent absent entirely.
DEGRADED_2026 = HourCounts(
    total_events=87_563,
    opened_prs=308,
    type_counts={
        "PushEvent": 82_434,
        "CreateEvent": 1_857,
        "PullRequestEvent": 851,
        "DeleteEvent": 820,
        "IssueCommentEvent": 433,
    },
    pr_payloads_with_draft=0,
)

# 2025-08-13 14:00 -- the rich-era reference the champion was trained on.
RICH_2025 = HourCounts(
    total_events=167_303,
    opened_prs=6_702,
    type_counts={
        "PullRequestEvent": 13_181,
        "IssueCommentEvent": 20_000,
        "PullRequestReviewEvent": 4_943,
        "PushEvent": 80_000,
    },
    pr_payloads_with_draft=13_301,
)


def test_a_healthy_2026_window_is_accepted() -> None:
    assert profile([HEALTHY_2026] * 24).failed_rules == ()


def test_the_rich_2025_reference_is_accepted() -> None:
    """The thresholds are calibrated so the training window itself passes.
    A gate that rejects the data the champion was trained on is miscalibrated.
    """
    assert profile([RICH_2025] * 24).failed_rules == ()


def test_a_degraded_window_is_refused_and_says_which_rules_failed() -> None:
    """The refusal is the feature, and it names rules -- not a boolean.

    Mirrors quality.py's `_failed_rules` array, for the same reason: a
    refusal you cannot analyse by rule is one you cannot act on.
    """
    failed = profile([DEGRADED_2026] * 24).failed_rules

    assert failed, "a 94%-PushEvent window must not be usable"
    assert "pull_request_share" in failed
    assert "review_signal_share" in failed


def test_a_window_too_small_to_calibrate_is_refused() -> None:
    """Page 1 draws a 10-decile calibration curve; under ~1,000 opened PRs
    that is fewer than 100 per decile and the panel stops meaning anything.
    """
    thin = HourCounts(
        total_events=10_000,
        opened_prs=20,
        type_counts={"PullRequestEvent": 4_000, "IssueCommentEvent": 3_000},
        pr_payloads_with_draft=0,
    )

    assert "opened_prs" in profile([thin]).failed_rules


def test_the_schema_era_is_reported_not_judged() -> None:
    """A reduced-era window is usable -- Task 2 recovers `merged` from the
    action -- but the champion reads `is_draft`, so the era must be visible
    rather than silently accepted.
    """
    assert profile([HEALTHY_2026]).carries_draft is False
    assert profile([RICH_2025]).carries_draft is True


def test_an_empty_window_is_refused_rather_than_dividing_by_zero() -> None:
    assert "no_events" in profile([]).failed_rules
    assert "no_events" in profile([HourCounts(0, 0, {}, 0)]).failed_rules


def test_count_hour_reads_a_real_archive_file_shape(tmp_path: Path) -> None:
    """The I/O edge, against the reduced-era payload shape measured 2026-09-08."""
    reduced_pr = {"base": {}, "head": {}, "id": 9, "number": 7, "url": "u"}
    events = [
        {"type": "PullRequestEvent", "payload": {"action": "opened", "pull_request": reduced_pr}},
        {"type": "PullRequestEvent", "payload": {"action": "merged", "pull_request": reduced_pr}},
        {"type": "PushEvent", "payload": {}},
        {"type": "IssueCommentEvent", "payload": {}},
    ]
    path = tmp_path / "2026-09-08-14.json.gz"
    with gzip.open(path, "wt") as f:
        f.write("\n".join(json.dumps(e) for e in events))

    counts = count_hour(path)

    assert counts.total_events == 4
    assert counts.opened_prs == 1  # only `opened`, not `merged`
    assert counts.type_counts["PullRequestEvent"] == 2
    assert counts.pr_payloads_with_draft == 0


def test_count_hour_sees_the_rich_era_draft_field(tmp_path: Path) -> None:
    path = tmp_path / "2025-08-13-9.json.gz"
    with gzip.open(path, "wt") as f:
        f.write(
            json.dumps(
                {
                    "type": "PullRequestEvent",
                    "payload": {"action": "opened", "pull_request": {"draft": False}},
                }
            )
        )

    assert count_hour(path).pr_payloads_with_draft == 1


def test_a_corrupt_line_does_not_abort_the_count(tmp_path: Path) -> None:
    """Truncated captures are a measured reality (S12 trap 5): two of six
    hours sampled on 2026-09-01 were truncated. One bad line must not cost
    the whole hour's characterization.
    """
    path = tmp_path / "2026-09-08-3.json.gz"
    with gzip.open(path, "wt") as f:
        f.write(json.dumps({"type": "PushEvent", "payload": {}}) + "\n{ truncated")

    assert count_hour(path).total_events == 1


def test_the_cli_exits_nonzero_on_a_refused_window(tmp_path: Path) -> None:
    """A gate that reports but always exits 0 is a report, not a gate."""
    path = tmp_path / "2026-09-08-14.json.gz"
    with gzip.open(path, "wt") as f:
        f.write("\n".join(json.dumps({"type": "PushEvent", "payload": {}}) for _ in range(50)))

    assert main([str(tmp_path)]) == 1


def test_the_cli_exits_zero_on_a_usable_window(tmp_path: Path) -> None:
    path = tmp_path / "2026-09-05-9.json.gz"
    reduced_pr = {"base": {}, "head": {}, "id": 9, "number": 7, "url": "u"}
    events = [
        {"type": "PullRequestEvent", "payload": {"action": "opened", "pull_request": reduced_pr}}
        for _ in range(MIN_OPENED_PRS)
    ] + [{"type": "IssueCommentEvent", "payload": {}} for _ in range(200)]
    with gzip.open(path, "wt") as f:
        f.write("\n".join(json.dumps(e) for e in events))

    assert main([str(tmp_path)]) == 0


def test_the_cli_refuses_a_directory_with_no_archive_files(tmp_path: Path) -> None:
    """Silence must not read as success."""
    assert main([str(tmp_path)]) == 1
