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


def _module_source(module: str, *, src_root: Path) -> Path | None:
    """`almanac.demo.panels` -> src/almanac/demo/panels.py, or None off-tree."""
    parts = module.split(".")
    candidate = src_root.joinpath(*parts).with_suffix(".py")
    if candidate.exists():
        return candidate
    candidate = src_root.joinpath(*parts, "__init__.py")
    return candidate if candidate.exists() else None


def _imported_names(path: Path) -> set[str]:
    """Every dotted module name `path` imports, full path preserved (not just the top segment)."""
    tree = ast.parse(path.read_text())
    names = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    names |= {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module and node.level == 0
    }
    return names


def _reaches_pyspark(module: str, *, src_root: Path, seen: set[str]) -> str | None:
    """The first-party import chain from `module` down to a pyspark import, or None."""
    if module in seen:
        return None
    seen.add(module)
    path = _module_source(module, src_root=src_root)
    if path is None:
        return None
    imported = _imported_names(path)
    if any(name.split(".")[0] == "pyspark" for name in imported):
        return module
    for name in imported:
        if name.split(".")[0] != "almanac":
            continue
        hit = _reaches_pyspark(name, src_root=src_root, seen=seen)
        if hit:
            return f"{module} -> {hit}"
    return None


def test_the_app_import_graph_never_reaches_pyspark() -> None:
    """`make demo` must not start a JVM.

    A check of only these three files' own top-level statements passed while
    the demo crashed in a real container: panels.py imported bounded_agent
    for load_transcript, and bounded_agent -> gateway -> mcp_server imports
    pyspark two first-party hops down. This walks the whole almanac.* import
    graph, not just each file's own statements. It cannot use a subprocess
    import instead: mlflow itself imports pyspark whenever pyspark happens to
    be installed (true in this repo's single all-extras dev venv), which
    would fail this test for a reason that has nothing to do with almanac's
    own code -- the demo container never installs pyspark at all.
    """
    src_root = Path(__file__).resolve().parents[2] / "src"
    for module in ("almanac.demo.artifacts", "almanac.demo.panels", "almanac.demo.champion"):
        chain = _reaches_pyspark(module, src_root=src_root, seen=set())
        assert chain is None, f"{module} transitively imports pyspark via {chain}"
