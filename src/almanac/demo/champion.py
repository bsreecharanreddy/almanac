"""Load the committed champion. No registry call, no network, no credentials."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

# MUST precede any mlflow import -- see the module docstring there.
from almanac.model.native import mlflow

_REPO_ROOT = Path(__file__).resolve().parents[3]
CHAMPION_DIR = _REPO_ROOT / "demo" / "model" / "champion"


def load_champion(path: Path | None = None) -> Any:
    """The registered champion, from disk. Returns an `LGBMClassifier`."""
    return mlflow.lightgbm.load_model(str(path or CHAMPION_DIR))


def champion_provenance(path: Path | None = None) -> dict[str, str]:
    """Version, run and registered name -- read from the artifact, never hardcoded."""
    root = path or CHAMPION_DIR
    meta = yaml.safe_load((root / "registered_model_meta").read_text())
    mlmodel = yaml.safe_load((root / "MLmodel").read_text())
    return {
        "registered_model": str(meta["model_name"]),
        "model_version": str(meta["model_version"]),
        "run_id": str(mlmodel["run_id"]),
    }
