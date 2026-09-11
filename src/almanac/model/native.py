"""Import LightGBM before MLflow, or scoring segfaults under two OpenMP runtimes.

Measured 2026-09-11 (macOS arm64, Python 3.12, lightgbm 4.7.0, mlflow 3.16.0):
this order succeeds 10 of 10 runs; every other order tried fails 10 of 10 with
SIGSEGV and no traceback. MLflow imports lightgbm lazily, by which point numpy
has bound its own OpenMP runtime and a second copy loads.

Import this module before mlflow anywhere the champion is loaded or scored.
`tests/unit/test_model_native.py` fails the build if a demo module does not.
"""

from __future__ import annotations

import lightgbm  # noqa: F401  -- imported for its side effect: bind OpenMP first.
import mlflow


def load_lightgbm_first() -> None:
    """No-op. Calling it documents that the import above is deliberate."""
    return None


__all__ = ["load_lightgbm_first", "mlflow"]
