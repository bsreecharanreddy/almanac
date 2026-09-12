"""The committed champion loads offline and scores a pinned vector."""

import json
import math

import numpy as np
import yaml

from almanac.demo.champion import CHAMPION_DIR, champion_provenance, load_champion
from almanac.model.train import FEATURE_COLUMNS

# Measured 2026-09-11 against the committed artifact, via booster_.predict.
# Pinned so a silently swapped model fails the suite instead of shipping.
_PINNED_VECTOR = [3.0, 0.5, 120.0, 4.0, 7.0, 0.03, float("nan"), 0.0, 1.0, 14.0]
_PINNED_SCORE = 0.6937982497227584


def test_the_artifact_is_committed() -> None:
    assert (CHAMPION_DIR / "MLmodel").is_file()
    assert (CHAMPION_DIR / "model.skops").is_file()


def test_it_loads_with_no_network_and_scores_the_pinned_vector() -> None:
    model = load_champion()
    x = np.array([_PINNED_VECTOR], dtype=np.float64)
    assert math.isclose(model.booster_.predict(x)[0], _PINNED_SCORE, rel_tol=1e-12)


def test_the_signature_names_every_feature_the_trainer_uses() -> None:
    """The artifact pins the feature contract, so it is never restated beside it.

    Parses the signature rather than substring-matching the raw YAML: MLflow
    wraps the long `inputs` scalar across lines, so a plain
    `'"name": "..."' in text` check breaks on whichever feature name happens
    to fall across the wrap -- events_total_to_date, measured 2026-09-11 --
    even though the signature does name it.
    """
    mlmodel = yaml.safe_load((CHAMPION_DIR / "MLmodel").read_text())
    signature_names = {field["name"] for field in json.loads(mlmodel["signature"]["inputs"])}
    for column in FEATURE_COLUMNS:
        assert column in signature_names, f"{column} missing from the model signature"


def test_provenance_is_read_from_the_artifact_not_hardcoded() -> None:
    provenance = champion_provenance()
    assert provenance["model_version"] == "2"
    assert provenance["run_id"] == "5f6a71bd9e4f4b6d8f7ea0a6454c0ca5"
