"""The ``photon_ab compare`` subcommand: arg wiring and the arm-file round trip."""

import json
from collections.abc import Mapping
from pathlib import Path

import pytest
from scripts.photon_ab import main

_OFF: dict[str, object] = {
    "photon": False,
    "compressed_gb": 2.0,
    "bronze_seconds": 200.0,
    "silver_seconds": 100.0,
    "gold_seconds": 100.0,
    "dbus_consumed": 1.0,
}
_ON: dict[str, object] = {
    **_OFF,
    "photon": True,
    "silver_seconds": 55.0,
    "gold_seconds": 50.0,
    "dbus_consumed": 1.5,
}


def _write(path: Path, arm: Mapping[str, object]) -> Path:
    path.write_text(json.dumps(arm))
    return path


def test_compare_reads_two_arm_files_and_exits_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    rc = main(
        [
            "compare",
            "--off",
            str(_write(tmp_path / "off.json", _OFF)),
            "--on",
            str(_write(tmp_path / "on.json", _ON)),
            "--usd-per-dbu",
            "0.30",
            "--usd-per-node-hour",
            "0.249",
            "--num-nodes",
            "5",
        ]
    )
    assert rc == 0

    printed = json.loads(capsys.readouterr().out)
    assert set(printed["layers"]) == {"bronze", "silver", "gold"}
    assert printed["layers"]["silver"]["verdict"] == "helped"
    assert isinstance(printed["paid_for_itself"], bool)


def test_compare_rejects_arms_in_the_wrong_order(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Photon-off arm first"):
        main(
            [
                "compare",
                "--off",
                str(_write(tmp_path / "a.json", _ON)),
                "--on",
                str(_write(tmp_path / "b.json", _OFF)),
                "--usd-per-dbu",
                "0.30",
                "--usd-per-node-hour",
                "0.249",
                "--num-nodes",
                "5",
            ]
        )


def test_an_unknown_subcommand_exits(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        main(["measure"])
