"""The §8.2 Photon A/B as arithmetic: two measured arms in, a per-layer verdict out.

The hypothesis is pre-registered in design doc §8.2 and is not edited after the
result: Photon helps silver and gold materially, helps bronze little or nothing
(bronze is almost all JSON parsing, which Photon covers only partially), and the
blend may be a wash or a loss.
"""

from __future__ import annotations

from dataclasses import dataclass

_LAYERS = ("bronze", "silver", "gold")
_MATERIAL_SPEEDUP = 1.10


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
    layer: str
    seconds_off: float
    seconds_on: float

    @property
    def speedup(self) -> float | None:
        """seconds_off / seconds_on; above 1 means Photon was faster."""
        return round(self.seconds_off / self.seconds_on, 2) if self.seconds_on > 0 else None

    @property
    def verdict(self) -> str:
        speedup = self.speedup
        if speedup is None:
            return "no data"
        if speedup >= _MATERIAL_SPEEDUP:
            return "helped"
        if speedup <= 1 / _MATERIAL_SPEEDUP:
            return "hurt"
        return "no material change"


@dataclass(frozen=True)
class PhotonComparison:
    layers: list[LayerResult]
    cost_off: float
    cost_on: float
    dbu_overhead: float

    @property
    def paid_for_itself(self) -> bool:
        return self.cost_on < self.cost_off

    @property
    def hypothesis_holds(self) -> bool:
        """§8.2's prediction, checked: bronze not materially helped, silver and gold are."""
        by_layer = {r.layer: r.verdict for r in self.layers}
        return (
            by_layer.get("bronze") in {"no material change", "hurt", "no data"}
            and by_layer.get("silver") == "helped"
            and by_layer.get("gold") == "helped"
        )

    def summary(self) -> dict[str, object]:
        return {
            "layers": {
                r.layer: {
                    "seconds_off": round(r.seconds_off, 1),
                    "seconds_on": round(r.seconds_on, 1),
                    "speedup": r.speedup,
                    "verdict": r.verdict,
                }
                for r in self.layers
            },
            "dbu_overhead": round(self.dbu_overhead, 3),
            "cost_off_usd": round(self.cost_off, 2),
            "cost_on_usd": round(self.cost_on, 2),
            "paid_for_itself": self.paid_for_itself,
            "hypothesis_holds": self.hypothesis_holds,
        }


def compare_arms(
    off: ArmMeasurement,
    on: ArmMeasurement,
    *,
    usd_per_dbu: float,
    usd_per_node_hour: float,
    num_nodes: int,
) -> PhotonComparison:
    """Per-layer speedups plus the cost verdict; rates are the caller's, never hardcoded."""
    if off.photon or not on.photon:
        raise ValueError("compare_arms takes the Photon-off arm first, the Photon-on arm second")

    layers = [
        LayerResult(
            layer=name,
            seconds_off=off.layer_seconds(name),
            seconds_on=on.layer_seconds(name),
        )
        for name in _LAYERS
    ]
    dbu_overhead = on.dbus_consumed / off.dbus_consumed if off.dbus_consumed > 0 else 0.0
    return PhotonComparison(
        layers=layers,
        cost_off=_run_cost(off, usd_per_dbu, usd_per_node_hour, num_nodes),
        cost_on=_run_cost(on, usd_per_dbu, usd_per_node_hour, num_nodes),
        dbu_overhead=dbu_overhead,
    )


def _run_cost(
    arm: ArmMeasurement, usd_per_dbu: float, usd_per_node_hour: float, num_nodes: int
) -> float:
    vm = num_nodes * usd_per_node_hour * arm.wall_seconds / 3600
    return vm + arm.dbus_consumed * usd_per_dbu
