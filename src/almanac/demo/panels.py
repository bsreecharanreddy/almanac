"""Panel data, prepared. Pure functions -- no Streamlit import lives here."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from almanac.agent.grounding import GroundingTrace, verify
from almanac.agent.transcript import load_transcript
from almanac.model.contributions import BASELINE_COLUMN, contribution_frame
from almanac.model.train import FEATURE_COLUMNS


def coverage_rows(coverage: dict[str, Any]) -> pd.DataFrame:
    """Per-feature null counts, which is the half a coverage number usually hides."""
    total = int(coverage["total_rows"])
    rows = [
        {
            "feature": name,
            "non_null": int(stats["non_null"]),
            "null": total - int(stats["non_null"]),
            "pct_null": round(100.0 * (total - int(stats["non_null"])) / total, 1)
            if total
            else 0.0,
        }
        for name, stats in coverage["features"].items()
    ]
    return pd.DataFrame(rows).sort_values("pct_null", ascending=False).reset_index(drop=True)


def contributions_for(model: Any, row: pd.Series) -> pd.DataFrame:
    """The champion's own per-feature contributions, strongest pull first."""
    features = pd.DataFrame([row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
    contributed = contribution_frame(model.booster_, features).iloc[0]
    frame = pd.DataFrame(
        {
            "feature": FEATURE_COLUMNS,
            "contribution": [float(contributed[c]) for c in FEATURE_COLUMNS],
        }
    )
    frame["direction"] = frame["contribution"].map(
        lambda v: "increases_risk" if v > 0 else "decreases_risk"
    )
    order = frame["contribution"].abs().sort_values(ascending=False).index
    return frame.loc[order].reset_index(drop=True)


def baseline_for(model: Any, row: pd.Series) -> float:
    """The baseline the contributions move from. Named, because it is not a feature."""
    features = pd.DataFrame([row[FEATURE_COLUMNS]], columns=FEATURE_COLUMNS)
    return float(contribution_frame(model.booster_, features).iloc[0][BASELINE_COLUMN])


def grounding_for(path: Path) -> GroundingTrace:
    """Verify a committed transcript. No model call."""
    return verify(load_transcript(path))
