"""The app, driven headlessly. Layout, not logic -- logic is tested in panels."""

from pathlib import Path
from typing import Any

import pytest

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
