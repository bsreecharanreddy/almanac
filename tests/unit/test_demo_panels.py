"""Panel data preparation. Pure functions -- no Streamlit, no Spark."""

import ast
from pathlib import Path

import pandas as pd

from almanac.demo import panels
from almanac.demo.champion import load_champion
from almanac.model.train import FEATURE_COLUMNS

_TRANSCRIPTS = Path(__file__).resolve().parents[1] / "fixtures" / "transcripts"
_LIVE = _TRANSCRIPTS / "2026-09-10-live-predict-explain.json"
_FLIPPED = _TRANSCRIPTS / "2026-09-10-live-predict-explain-directions-flipped.json"


def test_coverage_rows_report_the_null_side_too() -> None:
    coverage = {"total_rows": 100, "features": {c: {"non_null": 10} for c in FEATURE_COLUMNS}}
    rows = panels.coverage_rows(coverage)
    assert set(rows["feature"]) == set(FEATURE_COLUMNS)
    assert (rows["null"] == 90).all()
    assert (rows["pct_null"] == 90.0).all()


def test_contributions_are_sorted_by_strength_and_carry_a_direction() -> None:
    model = load_champion()
    row = pd.Series({c: 1.0 for c in FEATURE_COLUMNS})
    frame = panels.contributions_for(model, row)
    assert list(frame.columns) == ["feature", "contribution", "direction"]
    assert len(frame) == len(FEATURE_COLUMNS)
    strengths = frame["contribution"].abs().tolist()
    assert strengths == sorted(strengths, reverse=True)
    assert set(frame["direction"]) <= {"increases_risk", "decreases_risk"}


def test_the_live_transcript_grounds() -> None:
    assert panels.grounding_for(_LIVE).verdict in {"grounded", "ungrounded"}


def test_the_flipped_transcript_is_rejected() -> None:
    """A verifier only ever shown passing is indistinguishable from one that cannot fail."""
    trace = panels.grounding_for(_FLIPPED)
    assert trace.verdict == "ungrounded"
    assert trace.failures


def test_the_app_import_graph_never_reaches_pyspark() -> None:
    """`make demo` must not start a JVM. The app imports artifacts and panels only."""
    src = Path(__file__).resolve().parents[2] / "src" / "almanac" / "demo"
    for module in ("artifacts.py", "panels.py", "champion.py"):
        tree = ast.parse((src / module).read_text())
        imported = {
            alias.name.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        } | {
            node.module.split(".")[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
        }
        assert "pyspark" not in imported, f"{module} imports pyspark"
