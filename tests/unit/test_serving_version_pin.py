"""The served model version must be the champion, not a stale pin.

`databricks_model_serving.entity_version` takes a registry version number;
the provider has no alias form (databricks/databricks 1.131.0), so the pin
cannot follow `@champion` on its own. On 2026-09-08 it drifted: the
re-score registered version 2 and moved the alias, and the Terraform still
read "1" -- the retracted random-split model, which the next apply would
have served behind an endpoint documented with the new number.

Pinning is deliberate (an unrelated apply must not swap the served model).
This is what makes the pin loud when it goes stale.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TERRAFORM = ROOT / "infra" / "terraform" / "databricks.tf"
FINDING = ROOT / "docs" / "findings" / "2026-09-08-champion-rescored-temporal-split.md"

# The finding reads the alias back from the live registry rather than
# inferring it, so it is the in-repo source of truth for the version.
_CHAMPION_IN_FINDING = re.compile(r"champion -> version_num (\d+)")
_PINNED_IN_TERRAFORM = re.compile(r'^\s*entity_version\s*=\s*"(\d+)"', re.MULTILINE)


def _champion_version() -> str:
    match = _CHAMPION_IN_FINDING.search(FINDING.read_text())
    assert match, "the finding no longer records the alias readback"
    return match.group(1)


def _pinned_versions() -> list[str]:
    return _PINNED_IN_TERRAFORM.findall(TERRAFORM.read_text())


def test_the_serving_pin_is_the_champion_version() -> None:
    """A re-score that moves the alias and forgets the Terraform fails here."""
    assert _pinned_versions() == [_champion_version()]


def test_the_retracted_version_one_is_not_served() -> None:
    """Version 1 is the random-split model whose PR-AUC was withdrawn."""
    assert "1" not in _pinned_versions()
