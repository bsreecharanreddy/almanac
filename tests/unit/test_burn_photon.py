"""``compare_arms`` -- the §8.2 table and its verdict, as arithmetic."""

import json

import pytest

from almanac.burn.photon import (
    INDETERMINATE,
    ArmMeasurement,
    PhotonComparison,
    compare_arms,
)

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


def _compare(*pairs: tuple[ArmMeasurement, ArmMeasurement]) -> PhotonComparison:
    """One (off, on) pair per replicate; a single pair is the n=1 case."""
    return compare_arms(
        [off for off, _ in pairs],
        [on for _, on in pairs],
        usd_per_dbu=_USD_PER_DBU,
        usd_per_node_hour=_USD_PER_NODE_HOUR,
        num_nodes=_NUM_NODES,
    )


def test_the_pre_registered_hypothesis_shape_is_detected() -> None:
    off = _arm(False, bronze=100, silver=100, gold=100, dbus=1.0)
    on = _arm(True, bronze=98, silver=60, gold=55, dbus=1.4)

    result = _compare((off, on))

    verdicts = {r.layer: r.verdict for r in result.layers}
    assert verdicts == {"bronze": "no material change", "silver": "helped", "gold": "helped"}
    assert result.hypothesis_holds is True


def test_a_photon_run_that_does_not_pay_for_itself_is_reported_as_such() -> None:
    """§8.2's allowed null result: a small wall-clock saving swamped by DBU burn."""
    off = _arm(False, bronze=200, silver=40, gold=40, dbus=1.0)
    on = _arm(True, bronze=195, silver=38, gold=38, dbus=1.9)

    result = _compare((off, on))

    assert result.dbu_overhead == pytest.approx(1.9)
    assert result.paid_for_itself is False
    assert result.hypothesis_holds is False


def test_photon_pays_off_when_the_speedup_holds_dbus_flat() -> None:
    off = _arm(False, bronze=100, silver=200, gold=200, dbus=2.0)
    on = _arm(True, bronze=95, silver=90, gold=85, dbus=2.0)

    result = _compare((off, on))

    assert result.paid_for_itself is True


def test_arms_must_be_passed_off_then_on() -> None:
    off = _arm(False, bronze=1, silver=1, gold=1, dbus=1.0)
    on = _arm(True, bronze=1, silver=1, gold=1, dbus=1.0)
    with pytest.raises(ValueError, match="Photon-off arms first"):
        _compare((on, off))


def test_speedup_is_none_when_the_photon_arm_recorded_no_time() -> None:
    off = _arm(False, bronze=10, silver=10, gold=10, dbus=1.0)
    on = _arm(True, bronze=10, silver=10, gold=0, dbus=1.0)
    result = _compare((off, on))
    gold = next(r for r in result.layers if r.layer == "gold")
    assert gold.speedup is None
    assert gold.verdict == "no data"


def test_summary_is_json_shaped_and_complete() -> None:
    off = _arm(False, bronze=100, silver=100, gold=100, dbus=1.0)
    on = _arm(True, bronze=98, silver=60, gold=55, dbus=1.4)
    comparison = _compare((off, on))

    assert {r.layer for r in comparison.layers} == {"bronze", "silver", "gold"}
    assert set(comparison.summary()) == {
        "replicates",
        "conclusive",
        "layers",
        "dbu_overhead",
        "cost_off_usd",
        "cost_on_usd",
        "paid_for_itself",
        "hypothesis_holds",
    }
    assert json.dumps(comparison.summary())


def test_replicates_that_straddle_the_threshold_are_indeterminate() -> None:
    """The real case, measured 2026-09-03: bronze came in at 1.18, 1.05 and 1.23
    across three replicate pairs. The mean clears 1.10 and the range does not,
    and the run-to-run spread of a single arm reached 30% -- larger than the
    effect. A verdict here would be reporting noise."""
    pairs = [
        (
            _arm(False, bronze=213.9, silver=63.4, gold=60.9, dbus=0.0),
            _arm(True, bronze=180.6, silver=32.3, gold=42.1, dbus=0.0),
        ),
        (
            _arm(False, bronze=216.2, silver=72.5, gold=52.5, dbus=0.0),
            _arm(True, bronze=206.1, silver=30.2, gold=40.7, dbus=0.0),
        ),
        (
            _arm(False, bronze=211.3, silver=63.0, gold=68.2, dbus=0.0),
            _arm(True, bronze=171.8, silver=30.6, gold=48.1, dbus=0.0),
        ),
    ]

    result = _compare(*pairs)

    verdicts = {r.layer: r.verdict for r in result.layers}
    assert verdicts["bronze"] == INDETERMINATE
    assert verdicts["silver"] == "helped"
    assert verdicts["gold"] == "helped"
    assert result.conclusive is False


def test_an_indeterminate_layer_makes_the_hypothesis_unreportable() -> None:
    """None, not False. 'Refuted' and 'cannot tell' are different claims, and
    False is the stronger one -- runs 693303490917119 and 325049271562727
    returned opposite bronze verdicts from single runs."""
    pairs = [
        (
            _arm(False, bronze=100, silver=100, gold=100, dbus=0.0),
            _arm(True, bronze=98, silver=50, gold=50, dbus=0.0),
        ),
        (
            _arm(False, bronze=100, silver=100, gold=100, dbus=0.0),
            _arm(True, bronze=80, silver=50, gold=50, dbus=0.0),
        ),
    ]

    assert _compare(*pairs).hypothesis_holds is None


def test_the_mean_is_reported_even_when_the_verdict_is_withheld() -> None:
    """The central estimate stays visible; it just does not decide the verdict."""
    pairs = [
        (
            _arm(False, bronze=100, silver=100, gold=100, dbus=0.0),
            _arm(True, bronze=98, silver=50, gold=50, dbus=0.0),
        ),
        (
            _arm(False, bronze=100, silver=100, gold=100, dbus=0.0),
            _arm(True, bronze=80, silver=50, gold=50, dbus=0.0),
        ),
    ]
    bronze = next(r for r in _compare(*pairs).layers if r.layer == "bronze")
    assert bronze.speedups == [pytest.approx(1.0204, rel=1e-3), pytest.approx(1.25)]
    assert bronze.speedup == pytest.approx(1.14, abs=0.01)


def test_arms_must_be_paired() -> None:
    off = _arm(False, bronze=1, silver=1, gold=1, dbus=1.0)
    on = _arm(True, bronze=1, silver=1, gold=1, dbus=1.0)
    with pytest.raises(ValueError, match="equal replicate counts"):
        compare_arms([off, off], [on], usd_per_dbu=1.0, usd_per_node_hour=1.0, num_nodes=1)


def test_cost_is_per_run_not_per_replicate() -> None:
    """Three replicates of the same arm must not cost three times as much."""
    off = _arm(False, bronze=100, silver=100, gold=100, dbus=1.0)
    on = _arm(True, bronze=50, silver=50, gold=50, dbus=1.0)

    one = _compare((off, on))
    three = _compare((off, on), (off, on), (off, on))

    assert three.cost_off == pytest.approx(one.cost_off)
    assert three.cost_on == pytest.approx(one.cost_on)


def test_the_cost_verdict_is_withheld_without_dbu_totals() -> None:
    """Measured 2026-09-03: the job passes no --dbus, so both arms record 0.0.
    Cost then collapses to VM time and 'paid for itself' would just mean
    'Photon was faster' -- which it is, and which is not the question."""
    off = _arm(False, bronze=200, silver=100, gold=100, dbus=0.0)
    on = _arm(True, bronze=100, silver=50, gold=50, dbus=0.0)

    result = _compare((off, on))

    assert result.paid_for_itself is None
    assert result.summary()["paid_for_itself"] is None
