from datetime import UTC, datetime

import pytest

from almanac.explore.measure import BotMatch, classify_bot, duplicate_ratio, rename_events


def test_duplicate_ratio_counts_extra_copies_not_distinct_ids() -> None:
    # 4 events, 3 distinct -> 1 extra copy out of 4 = 0.25
    assert duplicate_ratio(["a", "b", "c", "a"]) == pytest.approx(0.25)


def test_duplicate_ratio_of_unique_stream_is_zero() -> None:
    assert duplicate_ratio(["a", "b", "c"]) == 0.0


def test_duplicate_ratio_of_empty_stream_is_zero_not_error() -> None:
    assert duplicate_ratio([]) == 0.0


def test_bot_suffix_match() -> None:
    assert classify_bot("dependabot[bot]") is BotMatch.SUFFIX


def test_bot_regex_match_is_reported_separately_from_suffix() -> None:
    # Reported separately precisely so its false-positive rate can be
    # measured rather than assumed (design doc §12, trap 6).
    assert classify_bot("nightly-ci") is BotMatch.REGEX


@pytest.mark.parametrize("login", ["robotframework", "Abbott"])
def test_the_documented_false_positives_are_not_actually_false_positives(login: str) -> None:
    """The inherited claim about this rule was wrong, and it is pinned here."""
    assert classify_bot(login) is BotMatch.NONE


# Real logins a bare `ci$` clause wrongly flagged (869 of 1,002 ci-ending
# logins -- docs/findings/2026-09-01-bot-classification.md).
@pytest.mark.parametrize(
    "login",
    [
        "AlexandruPopovici",
        "BarisYazici",
        "AlpAkinci",
        "AtillaTorosAvci",
        "ArmandoFalcucci",
        "Bramucci",
        "AlperenYABACI",
        "AitanaESCI",
        "ApplySci",
    ],
)
def test_human_surnames_ending_in_ci_are_not_bots(login: str) -> None:
    """The single largest false-positive source, fixed by requiring a separator."""
    assert classify_bot(login) is BotMatch.NONE


@pytest.mark.parametrize(
    "login",
    ["swift-ci", "aws-sdk-rust-ci", "LinuxServer-CI", "yugabyte-ci", "VenlyCI", "CheckmkCI"],
)
def test_real_ci_accounts_are_still_caught(login: str) -> None:
    """Tightening the rule must not cost the true positives it exists for."""
    assert classify_bot(login) is BotMatch.REGEX


@pytest.mark.parametrize("login", ["cw-circleci", "seek-oss-circleci"])
def test_known_recall_cost_of_requiring_a_separator(login: str) -> None:
    """Pinned as a KNOWN MISS, not an oversight."""
    assert classify_bot(login) is BotMatch.NONE


@pytest.mark.parametrize("login", ["hubot", "ursabot", "k8s-ci-robot", "harness-automation"])
def test_bots_without_the_bracket_suffix_are_still_caught(login: str) -> None:
    """The whole point of the regex clause: bots the [bot] suffix misses."""
    assert classify_bot(login) is BotMatch.REGEX


def test_ordinary_login_is_not_a_bot() -> None:
    assert classify_bot("octocat") is BotMatch.NONE


def test_rename_detected_when_same_repo_id_changes_name() -> None:
    obs = [
        (1, "old/name", datetime(2025, 1, 1, tzinfo=UTC)),
        (1, "new/name", datetime(2025, 2, 1, tzinfo=UTC)),
    ]
    renames = rename_events(obs)
    assert len(renames) == 1
    assert renames[0].repo_id == 1
    assert renames[0].from_name == "old/name"
    assert renames[0].to_name == "new/name"


def test_repeated_same_name_is_not_a_rename() -> None:
    obs = [
        (1, "same/name", datetime(2025, 1, 1, tzinfo=UTC)),
        (1, "same/name", datetime(2025, 2, 1, tzinfo=UTC)),
    ]
    assert rename_events(obs) == []


def test_observations_are_ordered_before_comparison() -> None:
    # Out-of-order input must not fabricate a reversed rename.
    obs = [
        (1, "new/name", datetime(2025, 2, 1, tzinfo=UTC)),
        (1, "old/name", datetime(2025, 1, 1, tzinfo=UTC)),
    ]
    renames = rename_events(obs)
    assert len(renames) == 1
    assert renames[0].from_name == "old/name"
