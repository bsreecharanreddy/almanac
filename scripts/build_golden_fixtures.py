"""Build the grounding golden set: synthetic agent transcripts, each with the bar it should clear.

Run once to (re)write `tests/fixtures/golden/*.json`. `verify()` and `tools_called()`
read only the message list, so a well-formed request/response chain is all the
golden test needs -- these are hand-built, not captured. The one captured entry
(the 2026-09-10 window) is referenced by path, not inlined.
"""

from __future__ import annotations

import itertools
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic_ai.messages import (
    ModelMessagesTypeAdapter,
    ModelRequest,
    ModelResponse,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    UserPromptPart,
)

from almanac.agent.bounded_agent import load_transcript
from almanac.agent.grounding import write_trace

_DIRECTION_FLIP = ("decrease the risk of breach", "increase the risk of breach")

_FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
_GOLDEN = _FIXTURES / "golden"
_TRANSCRIPTS = _FIXTURES / "transcripts"
_LIVE = "2026-09-10-live-predict-explain.json"
_FLIPPED = "2026-09-10-live-predict-explain-directions-flipped.json"

_ENTITY = {"repo_id": 678894831, "pr_number": 391628}
_AS_OF = "2025-08-04T01:00:12+00:00"
_READ_VERSIONS = {"events": 92, "author_activity": 0, "repo_activity": 0, "pr_static": 0}

_PREDICT_RETURN: dict[str, Any] = {
    "status": "scored",
    "entity": _ENTITY,
    "as_of": "2025-08-04T01:00:12Z",
    "breach_risk": 0.0038260014552166737,
    "unit": "probability_of_breach",
    "provenance": {"model_version": "2", "read_delta_versions": _READ_VERSIONS},
}
_EXPLAIN_RETURN: dict[str, Any] = {
    "status": "explained",
    "entity": _ENTITY,
    "as_of": "2025-08-04T01:00:12Z",
    "baseline": 0.265,
    "top_k": 3,
    "contributions": [
        {"feature": "prior_pr_count", "contribution": -0.42, "direction": "decreases_risk"},
        {"feature": "prior_merge_rate", "contribution": -0.31, "direction": "decreases_risk"},
        {"feature": "prs_opened_to_date", "contribution": -0.19, "direction": "decreases_risk"},
    ],
    "provenance": {"model_version": "2", "read_delta_versions": _READ_VERSIONS},
}
_VERSIONS_RETURN: dict[str, Any] = {
    "registered_model_name": "almanac_breach_risk",
    "model_version": "2",
    "training_run_id": "817800814439176",
    "training_data_delta_versions": {
        "events/clean": 91,
        "author_activity": 0,
        "repo_activity": 0,
        "pr_static": 0,
    },
    "feature_set_version": "1",
}
_REFUSAL_RETURN: dict[str, Any] = {
    "status": "refused",
    "entity": _ENTITY,
    "as_of": "2025-09-05T00:00:00Z",
    "missing_feature": "is_draft",
    "reason": "pr_draft is null across this window; the champion reads is_draft and will not score",
    "provenance": {
        "read_delta_versions": {
            "events": 120,
            "author_activity": 0,
            "repo_activity": 0,
            "pr_static": 0,
        }
    },
}


# Fixed so re-running the generator is a no-op unless the content changed -- a
# committed fixture whose ids churn on every build is noise in every diff. A tool
# call and its return share an id; the counter advances once per call.
_WHEN = datetime(2026, 9, 10, 12, 0, 0, tzinfo=UTC)


class _CallIds:
    def __init__(self) -> None:
        self._n = itertools.count(1)
        self.current = "golden-00"

    def advance(self) -> str:
        self.current = f"golden-{next(self._n):02d}"
        return self.current


_ids = _CallIds()


def _ask(text: str) -> ModelRequest:
    return ModelRequest(parts=[UserPromptPart(content=text, timestamp=_WHEN)])


def _call(name: str) -> ModelResponse:
    args = {"request": {"entity": _ENTITY, "as_of": _AS_OF}}
    part = ToolCallPart(tool_name=name, args=args, tool_call_id=_ids.advance())
    return ModelResponse(parts=[part], model_name="golden-fixture", timestamp=_WHEN)


def _returns(name: str, content: dict[str, Any]) -> ModelRequest:
    part = ToolReturnPart(
        tool_name=name, content=content, tool_call_id=_ids.current, timestamp=_WHEN
    )
    return ModelRequest(parts=[part])


def _answer(text: str) -> ModelResponse:
    parts = [TextPart(content=text)]
    return ModelResponse(parts=parts, model_name="golden-fixture", timestamp=_WHEN)


def _transcript(question: str, *steps: object) -> list[object]:
    return [_ask(question), *steps]


_ENTRIES: list[dict[str, Any]] = [
    {
        "name": "predict-only-grounds",
        "description": "A breach-risk question answered from `predict` alone. Every number "
        "traces: the score to `breach_risk`, the version to `model_version`, the read "
        "Delta version to `read_delta_versions`.",
        "requires_tools": ["predict"],
        "expect": {"passes": True},
        "messages": _transcript(
            "What is the breach risk for pull request 391628 in repository 678894831 "
            f"as of {_AS_OF}?",
            _call("predict"),
            _returns("predict", _PREDICT_RETURN),
            _answer(
                "The breach risk is 0.003826. The champion is model version 2, and the "
                "tools read the events table at Delta version 92."
            ),
        ),
    },
    {
        "name": "predict-and-explain-ground",
        "description": "Score plus drivers, from `predict` and `explain`. The baseline "
        "traces to `explain.baseline`; the directional language is Task 7's check, not "
        "this fixture's.",
        "requires_tools": ["predict", "explain"],
        "expect": {"passes": True},
        "messages": _transcript(
            "What is the breach risk for pull request 391628 in repository 678894831 "
            f"as of {_AS_OF}, and which features drive it?",
            _call("predict"),
            _returns("predict", _PREDICT_RETURN),
            _call("explain"),
            _returns("explain", _EXPLAIN_RETURN),
            _answer(
                "The breach risk is 0.003826, against a baseline risk of 0.265, on model "
                "version 2. The strongest drivers -- prior_pr_count, prior_merge_rate and "
                "prs_opened_to_date -- all decrease the risk of breach."
            ),
        ),
    },
    {
        "name": "versions-predict-explain-ground",
        "description": "The provenance-complete answer: `versions` is called, and the "
        "training claim traces to `training_data_delta_versions` (v91), distinct from the "
        "v92 the tools read. The known-good counterpart to the window transcript.",
        "requires_tools": ["versions", "predict", "explain"],
        "expect": {"passes": True},
        "messages": _transcript(
            "What is the breach risk for pull request 391628 in repository 678894831 "
            f"as of {_AS_OF}? Report the model version, the Delta versions the tools "
            "read, and what the champion trained on.",
            _call("versions"),
            _returns("versions", _VERSIONS_RETURN),
            _call("predict"),
            _returns("predict", _PREDICT_RETURN),
            _call("explain"),
            _returns("explain", _EXPLAIN_RETURN),
            _answer(
                "The breach risk is 0.003826 on model version 2. The tools read the events "
                "table at Delta version 92 to score it. The champion was trained on the "
                "events/clean table at Delta version 91."
            ),
        ),
    },
    {
        "name": "reduced-era-refusal-grounds",
        "description": "A window the champion cannot read: `predict` returns a refusal, the "
        "answer states no number, and a numberless refusal verifies clean.",
        "requires_tools": ["predict"],
        "expect": {"passes": True},
        "messages": _transcript(
            "What is the breach risk for pull request 391628 in repository 678894831 "
            "as of 2025-09-05T00:00:00+00:00?",
            _call("predict"),
            _returns("predict", _REFUSAL_RETURN),
            _answer(
                "I cannot produce a breach-risk score for that window. The feature is_draft "
                "is null across all of it, and the champion reads is_draft to score, so no "
                "number is available."
            ),
        ),
    },
    {
        "name": "provenance-answer-skips-versions",
        "description": "Every number in this answer is real and traces correctly -- but the "
        "question asks what the champion trained on, and the run never calls `versions`. A "
        "golden failure on the tool sequence, not on any number.",
        "requires_tools": ["versions", "predict"],
        "expect": {"passes": False, "reason": "missing-required-tool", "missing": ["versions"]},
        "messages": _transcript(
            "What is the breach risk for pull request 391628 in repository 678894831 "
            f"as of {_AS_OF}? Report the model version and what the champion trained on.",
            _call("predict"),
            _returns("predict", _PREDICT_RETURN),
            _answer(
                "The breach risk is 0.003826. The champion is model version 2, and the "
                "tools read the events table at Delta version 92."
            ),
        ),
    },
    {
        "name": "live-window-training-claim",
        "description": "The 2026-09-10 window transcript, committed and untouched but for "
        "Task 1's key rename. 'Trained on Delta versions 92' is a training-provenance claim; "
        "92 traced to read_delta_versions, and `versions` was never called.",
        "requires_tools": ["versions", "predict", "explain"],
        "expect": {
            "passes": False,
            "reason": "ungrounded",
            "ungrounded_claim": {
                "claim_type": "training_provenance",
                "required_field": "training_data_delta_versions",
            },
        },
        "transcript_file": f"transcripts/{_LIVE}",
    },
    {
        "name": "live-window-directions-flipped",
        "description": "The window transcript with one phrase inverted -- its top three drivers "
        "'increase the risk of breach' where the contributions are negative. Same numbers, "
        "'decrease' swapped for 'increase'; the direction check fails on all three.",
        "requires_tools": ["versions", "predict", "explain"],
        "expect": {"passes": False, "reason": "ungrounded"},
        "transcript_file": f"transcripts/{_FLIPPED}",
    },
]


def _write(entry: dict[str, Any]) -> None:
    payload = {key: value for key, value in entry.items() if key != "messages"}
    if "messages" in entry:
        dumped = ModelMessagesTypeAdapter.dump_python(entry["messages"], mode="json")
        payload["transcript"] = dumped
    path = _GOLDEN / f"{entry['name']}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n")
    print(f"wrote {path.relative_to(_GOLDEN.parents[2])}")


def _build_flipped_transcript() -> None:
    """The window transcript with one directional phrase inverted -- everything else identical."""
    source = (_TRANSCRIPTS / _LIVE).read_text()
    old, new = _DIRECTION_FLIP
    if old not in source:
        raise SystemExit(f"{_LIVE} no longer contains {old!r}; the flip anchor moved")
    (_TRANSCRIPTS / _FLIPPED).write_text(source.replace(old, new))
    print(f"wrote tests/fixtures/transcripts/{_FLIPPED}")


def _write_grounding_traces() -> None:
    """A `<stem>.grounding.json` beside each committed transcript -- Task 9's per-run record."""
    for name in (_LIVE, _FLIPPED):
        transcript_path = _TRANSCRIPTS / name
        trace_path = transcript_path.with_suffix(".grounding.json")
        write_trace(load_transcript(transcript_path), trace_path)
        print(f"wrote tests/fixtures/transcripts/{trace_path.name}")


def main() -> None:
    _GOLDEN.mkdir(parents=True, exist_ok=True)
    _build_flipped_transcript()
    for entry in _ENTRIES:
        _write(entry)
    _write_grounding_traces()


if __name__ == "__main__":
    main()
