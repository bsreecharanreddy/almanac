import gzip
import json
from pathlib import Path

MAX_FIXTURE_BYTES = 5 * 1_048_576


def _load(path: Path) -> list[dict[str, object]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return [json.loads(line) for line in fh]


def test_modern_fixture_parses_and_has_modern_shape(modern_events_path: Path) -> None:
    events = _load(modern_events_path)
    assert len(events) > 100
    first = events[0]
    assert {"id", "type", "actor", "repo", "created_at"} <= first.keys()
    # The 2015+ shape uses `repo`. Task 6 measures how 2014 differs.
    assert "repo" in first


def test_legacy_fixture_parses(legacy_events_path: Path) -> None:
    events = _load(legacy_events_path)
    assert len(events) > 100
    assert "type" in events[0]


def test_reduced_fixture_parses_and_has_the_cut_payload(reduced_events_path: Path) -> None:
    events = _load(reduced_events_path)
    assert len(events) > 100
    prs = [e for e in events if e["type"] == "PullRequestEvent"]
    assert prs, "fixture must contain at least one PullRequestEvent to prove the cut"
    payload = prs[0]["payload"]
    assert isinstance(payload, dict)
    pull_request = payload["pull_request"]
    assert isinstance(pull_request, dict)
    assert "merged" not in pull_request, "REDUCED_V3 dropped merge outcome from the payload"


def test_fixtures_are_small_enough_to_commit(
    modern_events_path: Path, legacy_events_path: Path, reduced_events_path: Path
) -> None:
    for path in (modern_events_path, legacy_events_path, reduced_events_path):
        assert path.stat().st_size < MAX_FIXTURE_BYTES, f"{path} too large to commit"
