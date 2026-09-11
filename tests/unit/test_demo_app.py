"""The app, driven headlessly. Layout, not logic -- logic is tested in panels."""

from pathlib import Path
from typing import Any

import pytest

from almanac.demo.architecture import NODES

streamlit_testing = pytest.importorskip("streamlit.testing.v1")
AppTest = streamlit_testing.AppTest

_APP = Path(__file__).resolve().parents[2] / "demo" / "app.py"


def _run() -> Any:
    app = AppTest.from_file(str(_APP), default_timeout=60)
    app.run()
    return app


def test_it_runs_without_exception() -> None:
    assert not _run().exception


def test_it_declares_its_scale_on_load() -> None:
    """Phase 8 shipped a dashboard rendering 200 healthy-looking rows against a
    horizon 340 days wrong. Plausible rendering of a wrong number is this
    project's most expensive recorded failure, and this is its highest-exposure
    surface.
    """
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "one archived hour" in text
    assert "341,060,851" in text


def test_the_queue_renders_no_identity() -> None:
    """docs/pseudonymization.md -- surrogates and ranks, never a login or owner/repo."""
    app = _run()
    rendered = " ".join(str(frame.value.columns.tolist()) for frame in app.dataframe)
    for forbidden in ("actor_login", "author_login", "repo_name", "repo_full_name"):
        assert forbidden not in rendered


def test_the_coverage_panel_states_why_the_features_are_null() -> None:
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "point-in-time" in text
    assert "not a defect" in text


def test_the_explain_tab_names_the_baseline_as_not_a_feature() -> None:
    """LightGBM returns n_features + 1 columns and the last is the expected value.

    Treating it as a feature is a silent off-by-one, which is why it is labelled.
    """
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "baseline" in text


def test_the_agent_tab_shows_a_rejection_not_only_a_pass() -> None:
    text = " ".join(m.value for m in _run().markdown).lower()
    assert "ungrounded" in text


def test_the_medallion_tab_shows_every_era_and_its_quarantine() -> None:
    app = _run()
    text = " ".join(m.value for m in app.markdown)
    for event_date in ("2025-08-13", "2014-06-12", "2025-11-03"):
        assert event_date in text
    assert "quarantine" in text.lower()


def test_the_architecture_tab_renders_every_node_with_no_exception() -> None:
    """AppTest cannot see inside a custom component's canvas, so the diagram
    alone would leave every node's label and link untestable here -- exactly
    why the tab also renders a plain list, checked directly rather than
    trusted to exist."""
    app = _run()
    assert not app.exception
    labels = {e.label for e in app.expander}
    text = " ".join(m.value for m in app.markdown)
    for node in NODES:
        assert node.label in labels
        for link in node.links:
            assert link in text
