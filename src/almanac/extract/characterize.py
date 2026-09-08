"""Is a candidate archive window fit to run the pipeline over?

Design doc 4.8 decision 2: the 2026 firehose is intermittently degraded, so
window quality is measured before use and a bad window is refused.
"""

import argparse
import gzip
import json
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

from almanac.cli import run_cli

# Events that carry a *response* to a PR -- what the SLA label is computed from.
# A window can be full of PullRequestEvents and still be unusable if nobody
# reviewed anything in it.
RESPONSE_TYPES: tuple[str, ...] = (
    "IssueCommentEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
)

# Calibrated against four measured hours (2026-09-08), not chosen by feel:
#
#   2026-09-05 09:00  healthy   PR share 37.3%  response 33.0%
#   2025-08-13 14:00  rich ref  PR share  7.9%  response 14.9%
#   2026-09-08 14:00  degraded  PR share  1.0%  response  0.5%
#
# 3% sits below both healthy readings with margin and 3x above the degraded
# one. The rich-era hour is the binding case: a gate that rejects the window
# the champion was trained on is miscalibrated, not strict.
MIN_PULL_REQUEST_SHARE = 0.03
MIN_RESPONSE_SHARE = 0.03

# Page 1 draws a 10-decile calibration curve. Below ~1,000 opened PRs that is
# under 100 per decile and the panel stops carrying signal.
MIN_OPENED_PRS = 1_000


@dataclass(frozen=True)
class HourCounts:
    """One archive hour, reduced to the only things the gate reads."""

    total_events: int
    opened_prs: int
    type_counts: dict[str, int]
    pr_payloads_with_draft: int


@dataclass(frozen=True)
class WindowProfile:
    """What a window contains, and which rules it fails.

    `failed_rules` is a tuple rather than a boolean for the same reason
    quality.py's `_failed_rules` is an array: a refusal you cannot analyse by
    rule is one nobody can act on.
    """

    hours: int
    total_events: int
    opened_prs: int
    pull_request_share: float
    response_share: float
    carries_draft: bool
    failed_rules: tuple[str, ...] = field(default=())

    @property
    def usable(self) -> bool:
        return not self.failed_rules


def _share(part: int, whole: int) -> float:
    return part / whole if whole else 0.0


def profile(hours: Sequence[HourCounts]) -> WindowProfile:
    """Characterize a window and name every rule it fails. Pure: no I/O."""
    total = sum(h.total_events for h in hours)
    opened = sum(h.opened_prs for h in hours)
    pr_events = sum(h.type_counts.get("PullRequestEvent", 0) for h in hours)
    responses = sum(h.type_counts.get(t, 0) for h in hours for t in RESPONSE_TYPES)

    pr_share = _share(pr_events, total)
    response_share = _share(responses, total)

    failed: list[str] = []
    if not total:
        # Checked first: every share below is 0.0 on an empty window, which
        # would otherwise report three failures for one cause.
        failed.append("no_events")
    else:
        if pr_share < MIN_PULL_REQUEST_SHARE:
            failed.append("pull_request_share")
        if response_share < MIN_RESPONSE_SHARE:
            failed.append("review_signal_share")
        if opened < MIN_OPENED_PRS:
            failed.append("opened_prs")

    return WindowProfile(
        hours=len(hours),
        total_events=total,
        opened_prs=opened,
        pull_request_share=pr_share,
        response_share=response_share,
        # Reported, never judged: a reduced-era window is usable (Task 2
        # recovers `merged` from the action) but the champion reads
        # `is_draft`, so the caller has to be able to see the era.
        carries_draft=any(h.pr_payloads_with_draft > 0 for h in hours),
        failed_rules=tuple(failed),
    )


def count_hour(path: Path) -> HourCounts:
    """Reduce one downloaded archive hour to its counts. The I/O edge.

    A malformed line is skipped rather than fatal: truncated captures are a
    measured reality (S12 trap 5 -- two of six hours sampled on 2026-09-01
    were truncated), and losing a whole hour's characterization to one bad
    line would make the gate less usable exactly where it matters most.
    """
    types: Counter[str] = Counter()
    opened = 0
    with_draft = 0
    total = 0

    with gzip.open(path, "rt", errors="replace") as handle:
        for line in handle:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            total += 1
            types[event.get("type", "")] += 1
            if event.get("type") != "PullRequestEvent":
                continue
            payload = event.get("payload") or {}
            if payload.get("action") == "opened":
                opened += 1
            if "draft" in (payload.get("pull_request") or {}):
                with_draft += 1

    return HourCounts(
        total_events=total,
        opened_prs=opened,
        type_counts=dict(types),
        pr_payloads_with_draft=with_draft,
    )


def _report(p: WindowProfile) -> str:
    lines = [
        f"hours              {p.hours}",
        f"events             {p.total_events:,}",
        f"opened PRs         {p.opened_prs:,}",
        f"PullRequestEvent   {p.pull_request_share:.1%}  (min {MIN_PULL_REQUEST_SHARE:.0%})",
        f"review signal      {p.response_share:.1%}  (min {MIN_RESPONSE_SHARE:.0%})",
        f"carries pr.draft   {p.carries_draft}",
    ]
    if p.usable:
        lines.append("\nUSABLE")
    else:
        lines.append("\nREFUSED: " + ", ".join(p.failed_rules))
        # Named, not implied: the caller's next move is to pick another
        # window, and it needs to know a rerun on the same one cannot help.
        lines.append("This window cannot support the pipeline. Choose another.")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """Characterize a directory of downloaded archive hours; refuse a bad one.

    Exit code, not just output: a gate that always exits 0 is a report.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path, help="directory of *.json.gz archive hours")
    args = parser.parse_args(argv)

    files = sorted(args.directory.glob("*.json.gz"))
    if not files:
        # An empty directory is refused rather than silently profiled as an
        # empty window: silence must not read as success.
        print(f"REFUSED: no *.json.gz files in {args.directory}")
        return 1

    result = profile([count_hour(f) for f in files])
    print(_report(result))
    return 0 if result.usable else 1


if __name__ == "__main__":  # pragma: no cover
    run_cli(main)
