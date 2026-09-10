"""The grounding verifier's record: what every number and claim in an answer traced to.

Task 2 defines the trace; Tasks 3, 4 and 7 fill it. The verdict is a decision, not
a score -- design doc S6, "deterministic, not LLM-as-judge" -- because a build gate
needs a yes or no.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field

from almanac.agent.schemas import Strict

NumberSource = Literal["exact", "rounded", "truncated", "unmatched"]
Direction = Literal["increases_risk", "decreases_risk"]


class NumberCheck(Strict):
    """One numeric literal from the answer, and the tool value it did or did not trace to."""

    literal: str
    source: NumberSource
    tool: str | None = None
    field_path: str | None = None


class ClaimCheck(Strict):
    """One typed claim, and whether its number traced to the field that claim type requires."""

    claim: str
    claim_type: str
    required_field: str
    traced: bool
    detail: str | None = None


class DirectionCheck(Strict):
    """One directional statement from the answer, checked against the contribution's sign."""

    claim: str
    feature: str
    claimed: Direction
    actual: Direction
    agrees: bool


class GroundingTrace(Strict):
    """Everything the verifier examined for one answer, and its verdict.

    One per agent run, serialized beside its transcript (Task 9). `failures` is
    the human-readable list of what went wrong -- empty exactly when the verdict
    is `grounded`.
    """

    verdict: Literal["grounded", "ungrounded"]
    numbers: list[NumberCheck] = Field(default_factory=list)
    claims: list[ClaimCheck] = Field(default_factory=list)
    directions: list[DirectionCheck] = Field(default_factory=list)
    failures: list[str] = Field(default_factory=list)

    @property
    def grounded(self) -> bool:
        return self.verdict == "grounded"
