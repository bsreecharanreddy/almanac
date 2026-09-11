"""The import order that keeps LightGBM from segfaulting under a second OpenMP runtime."""

import ast
from pathlib import Path

import pytest

_SRC = Path(__file__).resolve().parents[2] / "src" / "almanac"
_DEMO_PKG = _SRC / "demo"


def _import_order(path: Path) -> list[str]:
    """Top-level module names imported by `path`, in source order."""
    tree = ast.parse(path.read_text())
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            names.append(node.module.split(".")[0])
    return names


def test_native_module_imports_lightgbm_before_mlflow() -> None:
    order = _import_order(_SRC / "model" / "native.py")
    assert "lightgbm" in order, "native.py must import lightgbm"
    assert "mlflow" in order, "native.py must import mlflow"
    assert order.index("lightgbm") < order.index("mlflow")


@pytest.mark.parametrize("module", sorted(_DEMO_PKG.glob("*.py")) if _DEMO_PKG.exists() else [])
def test_no_demo_module_reaches_mlflow_ahead_of_lightgbm(module: Path) -> None:
    """A module importing mlflow must go through native.py, which orders it.

    Measured 2026-09-11: the wrong order segfaults 10 of 10 runs, with no
    traceback to debug from. A comment cannot fail a build; this can.
    """
    order = _import_order(module)
    if "mlflow" not in order:
        return
    assert "lightgbm" in order and order.index("lightgbm") < order.index("mlflow"), (
        f"{module.name} imports mlflow without lightgbm first -- import "
        f"almanac.model.native before mlflow, or the process dies with SIGSEGV"
    )
