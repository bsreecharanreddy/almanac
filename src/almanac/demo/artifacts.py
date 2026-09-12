"""Read the committed demo artifacts. No Spark, no network."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
DEMO_DATA_DIR = _REPO_ROOT / "demo" / "data"


def _read_json(name: str, root: Path | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = json.loads(((root or DEMO_DATA_DIR) / name).read_text())
    return payload


def load_queue(root: Path | None = None) -> pd.DataFrame:
    """The scored population, highest risk first."""
    return pd.read_parquet((root or DEMO_DATA_DIR) / "queue.parquet")


def load_coverage(root: Path | None = None) -> dict[str, Any]:
    return _read_json("coverage.json", root)


def load_medallion(root: Path | None = None) -> dict[str, Any]:
    return _read_json("medallion.json", root)
