"""``compare_arms`` -- the §8.2 table and its verdict, as arithmetic."""

import json

import pytest

from almanac.burn.photon import ArmMeasurement, PhotonComparison, compare_arms

# Rates from docs/findings/2026-09-01-cluster-throughput.md.
_USD_PER_DBU = 0.30
_USD_PER_NODE_HOUR = 0.249
_NUM_NODES = 5


def _arm(photon: bool, *, bronze: float, silver: float, gold: float, dbus: float) -> ArmMeasurement:
    return ArmMeasurement(
        photon=photon,
        compressed_gb=2.0,
        bronze_seconds=bronze,
        silver_seconds=silver,
        gold_seconds=gold,
        dbus_consumed=dbus,
    )


def _compare(off: ArmMeasurement, on: ArmMeasurement) -> PhotonComparison:
    return compare_arms(
        off,
        on,
        usd_per_dbu=_USD_PER_DBU,
        usd_per_node_hour=_USD_PER_NODE_HOUR,
        num_nodes=_NUM_NODES,
    )


def test_the_pre_registered_hypothesis_shape_is_detected() -> None:
    off = _arm(False, bronze=100, silver=100, gold=100, dbus=1.0)
    on = _arm(True, bronze=98, silver=60, gold=55, dbus=1.4)

    result = _compare(off, on)

    verdicts = {r.layer: r.verdict for r in result.layers}
    assert verdicts == {"bronze": "no material change", "silver": "helped", "gold": "helped"}
    assert result.hypothesis_holds is True


def test_a_photon_run_that_does_not_pay_for_itself_is_reported_as_such() -> None:
    """§8.2's allowed null result: a small wall-clock saving swamped by DBU burn."""
    off = _arm(False, bronze=200, silver=40, gold=40, dbus=1.0)
    on = _arm(True, bronze=195, silver=38, gold=38, dbus=1.9)

    result = _compare(off, on)

    assert result.dbu_overhead == pytest.approx(1.9)
    assert result.paid_for_itself is False
    assert result.hypothesis_holds is False


def test_photon_pays_off_when_the_speedup_holds_dbus_flat() -> None:
    off = _arm(False, bronze=100, silver=200, gold=200, dbus=2.0)
    on = _arm(True, bronze=95, silver=90, gold=85, dbus=2.0)

    result = _compare(off, on)

    assert result.paid_for_itself is True


def test_arms_must_be_passed_off_then_on() -> None:
    off = _arm(False, bronze=1, silver=1, gold=1, dbus=1.0)
    on = _arm(True, bronze=1, silver=1, gold=1, dbus=1.0)
    with pytest.raises(ValueError, match="Photon-off arm first"):
        _compare(on, off)


def test_speedup_is_none_when_the_photon_arm_recorded_no_time() -> None:
    off = _arm(False, bronze=10, silver=10, gold=10, dbus=1.0)
    on = _arm(True, bronze=10, silver=10, gold=0, dbus=1.0)
    result = _compare(off, on)
    gold = next(r for r in result.layers if r.layer == "gold")
    assert gold.speedup is None
    assert gold.verdict == "no data"


def test_summary_is_json_shaped_and_complete() -> None:
    off = _arm(False, bronze=100, silver=100, gold=100, dbus=1.0)
    on = _arm(True, bronze=98, silver=60, gold=55, dbus=1.4)
    comparison = _compare(off, on)

    assert {r.layer for r in comparison.layers} == {"bronze", "silver", "gold"}
    assert set(comparison.summary()) == {
        "layers",
        "dbu_overhead",
        "cost_off_usd",
        "cost_on_usd",
        "paid_for_itself",
        "hypothesis_holds",
    }
    assert json.dumps(comparison.summary())
