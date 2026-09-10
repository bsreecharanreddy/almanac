"""The broker in front of the MCP server: an allow-list, read-only, and a record of every call."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from mcp.server.mcpserver import MCPServer
from mcp.types import CallToolResult
from pydantic import BaseModel, ConfigDict, Field

from almanac.agent.mcp_server import TOOL_NAMES, tool_payload

Outcome = Literal["ok", "refused", "failed"]


class ToolRefusedError(Exception):
    """The gateway declined to call something. Never raised from inside a tool.

    Distinct from `schemas.Refusal`, which is a tool's own answer about the
    data. This one says the call never happened.
    """


class AuditRecord(BaseModel):
    """One call as the log keeps it: what was asked, what came back, how long it took."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Defaulted rather than passed, so the three places that write a record
    # cannot disagree about which clock stamps it.
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    tool: str = Field(min_length=1)
    arguments: dict[str, Any]
    outcome: Outcome
    latency_ms: float = Field(ge=0)
    result: dict[str, Any] | None = None
    reason: str | None = None
    # Which model answered, on a model call. Read back from the response rather
    # than inferred from config, because with a fallback in the chain the
    # configured primary is a guess (model_gateway.py).
    model: str | None = None


class AuditLog:
    """Append-only by construction: the only write opens in append mode, so a
    record already on disk has no code path here that could rewrite it.

    JSON Lines rather than one JSON document because a crash then costs the
    last call rather than the whole session's log.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, record: AuditRecord) -> None:
        """One record, one line, one open -- concurrent sessions interleave rather than clobber."""
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(record.model_dump_json() + "\n")

    def records(self) -> list[AuditRecord]:
        """Every call written to this log, in the order it was made."""
        if not self._path.exists():
            return []
        with self._path.open(encoding="utf-8") as handle:
            return [AuditRecord.model_validate_json(line) for line in handle if line.strip()]


async def allow_list(server: MCPServer, *, reviewed: Sequence[str] = TOOL_NAMES) -> frozenset[str]:
    """The names this gateway will call: reviewed by a person *and* declared read-only.

    Two gates, because each catches what the other cannot. The reviewed list is
    the security property -- a tool the server gains, of any kind, cannot be
    called until someone edits a constant, which is a change a reviewer sees.
    The read-only check is the consistency one, catching a reviewed tool that
    quietly lost its annotation.

    Neither is a claim about what a tool's body does. The gateway cannot know
    that, and a test that greps tool source for `write` would be theatre; what
    it can guarantee is that nothing outside the reviewed four ever runs.
    """
    declared = {
        tool.name
        for tool in await server.list_tools()
        if tool.annotations is not None and tool.annotations.read_only_hint
    }
    undeclared = sorted(set(reviewed) - declared)
    if undeclared:
        raise ValueError(
            f"{', '.join(undeclared)} are on the reviewed list but the server does not declare "
            "them read-only; serving the rest would quietly narrow the surface rather than fail"
        )
    return frozenset(reviewed)


class ToolGateway:
    """Every call to the tool surface goes through here, or the audit log is a lie.

    The allow-list is passed in rather than read from the server, so a tool the
    server gains after construction is not silently in scope.
    """

    def __init__(self, server: MCPServer, allowed: frozenset[str], audit: AuditLog) -> None:
        self._server = server
        self._allowed = allowed
        self._audit = audit

    @property
    def allowed(self) -> frozenset[str]:
        return self._allowed

    async def call(self, name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
        """One tool call: refused, or run and recorded either way."""
        asked = dict(arguments or {})

        if name not in self._allowed:
            reason = (
                f"{name!r} is not an allowed tool; available: {', '.join(sorted(self._allowed))}"
            )
            # Recorded before raising: a refused call is the most interesting
            # line in a security log, and one that leaves no trace is the
            # failure this log exists to make visible.
            self._audit.append(
                AuditRecord(
                    tool=name, arguments=asked, outcome="refused", latency_ms=0.0, reason=reason
                )
            )
            raise ToolRefusedError(reason)

        started = time.perf_counter()
        try:
            result = await self._server.call_tool(name, asked)
            payload = tool_payload(_called(result, name))
        except Exception as error:
            self._audit.append(
                AuditRecord(
                    tool=name,
                    arguments=asked,
                    outcome="failed",
                    latency_ms=_since(started),
                    reason=repr(error),
                )
            )
            raise

        self._audit.append(
            AuditRecord(
                tool=name,
                arguments=asked,
                outcome="ok",
                latency_ms=_since(started),
                result=payload,
            )
        )
        return payload


def _called(result: object, name: str) -> CallToolResult:
    """`call_tool` can also return an elicitation request; none of the four ask for one."""
    if not isinstance(result, CallToolResult):
        raise ValueError(f"{name} asked the caller for input, which no reading tool should do")
    return result


def _since(started: float) -> float:
    return (time.perf_counter() - started) * 1000
