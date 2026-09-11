"""The per-run trace on disk -- `verify()` written beside its transcript (design S6 Task 7).

The window's own run gets one retroactively, committed here, so the false claim
has a machine-readable record of *why* it is false and not only the findings-doc
prose. `make check` fails if a committed trace drifts from what `verify` now says.
"""

from pathlib import Path

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import GroundingTrace, verify, write_trace

_TRANSCRIPTS = Path(__file__).parents[1] / "fixtures" / "transcripts"
_LIVE = _TRANSCRIPTS / "2026-09-10-live-predict-explain.json"
_FLIPPED = _TRANSCRIPTS / "2026-09-10-live-predict-explain-directions-flipped.json"


def test_write_trace_round_trips_through_the_file(tmp_path: Path) -> None:
    transcript = load_transcript(_LIVE)
    path = tmp_path / "run.grounding.json"

    returned = write_trace(transcript, path)

    assert GroundingTrace.model_validate_json(path.read_text()) == returned == verify(transcript)


def test_the_committed_window_trace_is_current_and_names_the_relationship_failure() -> None:
    committed = _LIVE.with_suffix(".grounding.json")
    assert committed.exists(), "the window run's trace is committed beside its transcript"

    trace = GroundingTrace.model_validate_json(committed.read_text())

    assert trace == verify(load_transcript(_LIVE)), "the committed trace is stale -- rebuild it"
    assert trace.verdict == "ungrounded"
    training = next(c for c in trace.claims if c.claim_type == "training_provenance")
    assert not training.traced
    assert training.required_field == "training_data_delta_versions"


def test_the_flipped_window_trace_is_committed_and_records_the_direction_failure() -> None:
    trace = GroundingTrace.model_validate_json(_FLIPPED.with_suffix(".grounding.json").read_text())

    assert trace == verify(load_transcript(_FLIPPED))
    assert [d.feature for d in trace.directions if not d.agrees] == [
        "prior_pr_count",
        "prior_merge_rate",
        "prs_opened_to_date",
    ]
