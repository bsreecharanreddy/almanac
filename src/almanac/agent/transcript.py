"""Transcript I/O, split out of bounded_agent so reading one never imports pyspark.

save_transcript/load_transcript touch only pydantic_ai's message types. Kept
in bounded_agent.py, they dragged its whole live-agent import chain (gateway
-> mcp_server -> pyspark) into anything that just wants to replay a
transcript -- which is exactly what broke the demo, whose entire point is to
not need a JVM.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter


def save_transcript(path: Path, transcript: Sequence[ModelMessage]) -> None:
    """The run's messages as JSON, so no later run has to pay for the same answer."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(ModelMessagesTypeAdapter.dump_json(list(transcript), indent=2))


def load_transcript(path: Path) -> list[ModelMessage]:
    """A recorded run, back as messages."""
    return ModelMessagesTypeAdapter.validate_json(path.read_bytes())
