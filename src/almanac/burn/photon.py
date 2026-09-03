"""The §8.2 Photon A/B as arithmetic: two measured arms in, a per-layer verdict out.

The hypothesis is pre-registered in design doc §8.2 and is not edited after the
result: Photon helps silver and gold materially, helps bronze little or nothing
(bronze is almost all JSON parsing, which Photon covers only partially), and the
blend may be a wash or a loss.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from statistics import mean

_LAYERS = ("bronze", "silver", "gold")
_MATERIAL_SPEEDUP = 1.10
INDETERMINATE = "indeterminate"


@dataclass(frozen=True)
class ArmMeasurement:
    photon: bool
    compressed_gb: float
    bronze_seconds: float
    silver_seconds: float
    gold_seconds: float
    dbus_consumed: float

    @property
    def _by_layer(self) -> dict[str, float]:
        return {
            "bronze": self.bronze_seconds,
            "silver": self.silver_seconds,
            "gold": self.gold_seconds,
        }

    def layer_seconds(self, layer: str) -> float:
        return self._by_layer[layer]

    @property
    def wall_seconds(self) -> float:
        return sum(self._by_layer.values())


@dataclass(frozen=True)
class LayerResult:
    """One layer across every replicate pair, not a single run.

    Measured 2026-09-03: the same arm re-run varies by up to 30% on this
    cluster, which is larger than the effect on some layers. A verdict read off
    one run is therefore a coin flip -- runs 693303490917119 and 325049271562727
    returned opposite answers for bronze. The replicates stay a list so the
    verdict can say "indeterminate" instead of picking whichever run it saw.
    """

    layer: str
    seconds_off: tuple[float, ...]
    seconds_on: tuple[float, ...]

    @property
    def speedups(self) -> list[float]:
        """One seconds_off / seconds_on per replicate pair; above 1 is faster."""
        return [
            off / on for off, on in zip(self.seconds_off, self.seconds_on, strict=True) if on > 0
        ]

    @property
    def speedup(self) -> float | None:
        """The central estimate. Reported, but never the basis of the verdict."""
        return round(mean(self.speedups), 2) if self.speedups else None

    @property
    def verdict(self) -> str:
        """Range-based: the whole observed range must clear the threshold.

        A mean that clears it while the range straddles is exactly the case
        that cannot be distinguished from run-to-run noise at this sample size.
        """
        speedups = self.speedups
        if not speedups:
            return "no data"
        low, high = min(speedups), max(speedups)
        if low >= _MATERIAL_SPEEDUP:
            return "helped"
        if high <= 1 / _MATERIAL_SPEEDUP:
            return "hurt"
        if high < _MATERIAL_SPEEDUP and low > 1 / _MATERIAL_SPEEDUP:
            return "no material change"
        return INDETERMINATE


@dataclass(frozen=True)
class PhotonComparison:
    layers: list[LayerResult]
    cost_off: float
    cost_on: float
    dbu_overhead: float
    dbus_measured: bool

    @property
    def paid_for_itself(self) -> bool | None:
        """``None`` without DBU totals, which the job cannot know until billing settles.

        Photon's whole cost penalty is that its nodes consume more DBUs per
        hour -- the $/DBU rate is identical (2026-09-01-azure-pricing.md). So a
        verdict computed from VM time alone reduces to "Photon was faster" and
        systematically favours Photon. Better to refuse the question.
        """
        if not self.dbus_measured:
            return None
        return self.cost_on < self.cost_off

    @property
    def conclusive(self) -> bool:
        """False if any layer's replicates straddle the materiality threshold."""
        return all(r.verdict != INDETERMINATE for r in self.layers)

    @property
    def hypothesis_holds(self) -> bool | None:
        """§8.2's prediction, checked: bronze not materially helped, silver and gold are.

        ``None`` when a layer is indeterminate. Returning ``False`` there would
        report the hypothesis as refuted when the measurement simply cannot
        separate the effect from noise -- a different claim, and a stronger one
        than the data supports.
        """
        if not self.conclusive:
            return None
        by_layer = {r.layer: r.verdict for r in self.layers}
        return (
            by_layer.get("bronze") in {"no material change", "hurt", "no data"}
            and by_layer.get("silver") == "helped"
            and by_layer.get("gold") == "helped"
        )

    def summary(self) -> dict[str, object]:
        return {
            "replicates": len(self.layers[0].seconds_off) if self.layers else 0,
            "layers": {
                r.layer: {
                    "seconds_off": [round(s, 1) for s in r.seconds_off],
                    "seconds_on": [round(s, 1) for s in r.seconds_on],
                    "speedups": [round(s, 2) for s in r.speedups],
                    "speedup_mean": r.speedup,
                    "verdict": r.verdict,
                }
                for r in self.layers
            },
            "conclusive": self.conclusive,
            "dbu_overhead": round(self.dbu_overhead, 3),
            "cost_off_usd": round(self.cost_off, 2),
            "cost_on_usd": round(self.cost_on, 2),
            "paid_for_itself": self.paid_for_itself,
            "hypothesis_holds": self.hypothesis_holds,
        }


def compare_arms(
    off: Sequence[ArmMeasurement],
    on: Sequence[ArmMeasurement],
    *,
    usd_per_dbu: float,
    usd_per_node_hour: float,
    num_nodes: int,
) -> PhotonComparison:
    """Per-layer speedups plus the cost verdict; rates are the caller's, never hardcoded.

    Takes replicates, not one run per arm, because the verdict is a threshold
    comparison and the noise floor here can exceed the effect (see LayerResult).
    """
    if not off or not on:
        raise ValueError("compare_arms needs at least one measurement per arm")
    if len(off) != len(on):
        raise ValueError("compare_arms needs the arms paired: equal replicate counts")
    if any(a.photon for a in off) or not all(a.photon for a in on):
        raise ValueError("compare_arms takes the Photon-off arms first, the Photon-on arms second")

    layers = [
        LayerResult(
            layer=name,
            seconds_off=tuple(a.layer_seconds(name) for a in off),
            seconds_on=tuple(a.layer_seconds(name) for a in on),
        )
        for name in _LAYERS
    ]
    dbus_off, dbus_on = sum(a.dbus_consumed for a in off), sum(a.dbus_consumed for a in on)
    return PhotonComparison(
        layers=layers,
        cost_off=_run_cost(off, usd_per_dbu, usd_per_node_hour, num_nodes),
        cost_on=_run_cost(on, usd_per_dbu, usd_per_node_hour, num_nodes),
        dbu_overhead=dbus_on / dbus_off if dbus_off > 0 else 0.0,
        dbus_measured=dbus_off > 0 and dbus_on > 0,
    )


def _run_cost(
    arms: Sequence[ArmMeasurement], usd_per_dbu: float, usd_per_node_hour: float, num_nodes: int
) -> float:
    """Mean cost of one run of this arm, so replicate count cannot inflate it."""
    vm = num_nodes * usd_per_node_hour * mean([a.wall_seconds for a in arms]) / 3600
    return vm + mean([a.dbus_consumed for a in arms]) * usd_per_dbu
