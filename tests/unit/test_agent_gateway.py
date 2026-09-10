"""The gateway in front of the tool surface: an allow-list, read-only, and a
record of every call.

Built on a stub server rather than the real one, because the central test here
needs a write-capable tool to exist and `mcp_server.py` offers no way to build
one. That is the point -- the write path has to be constructed deliberately, in
a test, to prove the gateway refuses it.
"""

from collections.abc import Sequence
from pathlib import Path

import anyio
import pytest
from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from pydantic import BaseModel

from almanac.agent.gateway import AuditLog, ToolGateway, ToolRefusedError, allow_list
from almanac.agent.mcp_server import TOOL_NAMES


class _Request(BaseModel):
    value: int = 0


class _Answer(BaseModel):
    value: int


def _stub_server(
    *,
    declared_read_only: Sequence[str] = TOOL_NAMES,
    write_tool: str | None = None,
    failing: str | None = None,
) -> tuple[MCPServer, list[str]]:
    """The four reviewed names, plus optionally a tool that records having run."""
    ran: list[str] = []

    def answer(request: _Request) -> _Answer:
        """A stand-in for a reading tool."""
        ran.append("read")
        return _Answer(value=request.value * 2)

    def mutate(request: _Request) -> _Answer:
        """The tool that must never run: registered write-capable, on purpose."""
        ran.append("write")
        return _Answer(value=request.value)

    def explode(request: _Request) -> _Answer:
        """A reading tool whose body fails."""
        raise RuntimeError("the tool itself exploded")

    server = MCPServer(name="stub", version="0")
    for name in TOOL_NAMES:
        read_only = ToolAnnotations(read_only_hint=True) if name in declared_read_only else None
        body = explode if name == failing else answer
        server.add_tool(body, name=name, annotations=read_only, structured_output=True)
    if write_tool is not None:
        server.add_tool(
            mutate,
            name=write_tool,
            annotations=ToolAnnotations(read_only_hint=False, destructive_hint=True),
            structured_output=True,
        )
    return server, ran


def _gateway(server: MCPServer, tmp_path: Path) -> tuple[ToolGateway, AuditLog]:
    audit = AuditLog(tmp_path / "audit.jsonl")
    allowed = anyio.run(lambda: allow_list(server))
    return ToolGateway(server, allowed, audit), audit


def test_the_allow_list_is_the_reviewed_names_not_whatever_the_server_offers(
    tmp_path: Path,
) -> None:
    server, _ = _stub_server(write_tool="delete_features")

    allowed = anyio.run(lambda: allow_list(server))

    assert allowed == frozenset(TOOL_NAMES)
    assert "delete_features" not in allowed


def test_a_reviewed_tool_the_server_stops_declaring_read_only_fails_the_build() -> None:
    server, _ = _stub_server(declared_read_only=[name for name in TOOL_NAMES if name != "predict"])

    with pytest.raises(ValueError, match="predict"):
        anyio.run(lambda: allow_list(server))


def test_an_unregistered_name_is_refused_with_a_message_naming_what_is_available(
    tmp_path: Path,
) -> None:
    server, _ = _stub_server()
    gateway, _audit = _gateway(server, tmp_path)

    with pytest.raises(ToolRefusedError) as refusal:
        anyio.run(lambda: gateway.call("drop_features", {}))

    for name in TOOL_NAMES:
        assert name in str(refusal.value)


def test_a_write_capable_tool_is_refused_and_its_body_never_runs(tmp_path: Path) -> None:
    """The plan's mutation criterion: a write tool on the server must be refused,
    not called and then written down as if calling it were fine.
    """
    server, ran = _stub_server(write_tool="delete_features")
    gateway, audit = _gateway(server, tmp_path)

    with pytest.raises(ToolRefusedError):
        anyio.run(lambda: gateway.call("delete_features", {"request": {"value": 1}}))

    assert ran == []
    assert [record.outcome for record in audit.records()] == ["refused"]


def test_every_call_lands_in_the_audit_log_in_the_order_it_was_made(tmp_path: Path) -> None:
    server, _ = _stub_server()
    gateway, audit = _gateway(server, tmp_path)

    async def three_calls() -> None:
        for name in ("versions", "predict", "explain"):
            await gateway.call(name, {"request": {"value": 1}})

    anyio.run(three_calls)

    assert [record.tool for record in audit.records()] == ["versions", "predict", "explain"]


def test_the_log_appends_across_sessions_rather_than_truncating(tmp_path: Path) -> None:
    server, _ = _stub_server()
    path = tmp_path / "audit.jsonl"
    allowed = anyio.run(lambda: allow_list(server))

    async def one_session(tool: str) -> None:
        await ToolGateway(server, allowed, AuditLog(path)).call(tool, {"request": {"value": 1}})

    for name in ("predict", "explain"):
        anyio.run(one_session, name)

    assert [record.tool for record in AuditLog(path).records()] == ["predict", "explain"]


def test_a_tool_that_raises_is_recorded_as_failed_and_the_error_propagates(
    tmp_path: Path,
) -> None:
    server, _ = _stub_server(failing="predict")
    gateway, audit = _gateway(server, tmp_path)

    with pytest.raises(Exception, match=r"predict"):
        anyio.run(lambda: gateway.call("predict", {"request": {"value": 1}}))

    record = audit.records()[-1]
    assert record.outcome == "failed"
    assert record.result is None


def test_an_audit_record_carries_the_arguments_the_result_and_a_latency(tmp_path: Path) -> None:
    server, _ = _stub_server()
    gateway, audit = _gateway(server, tmp_path)

    payload = anyio.run(lambda: gateway.call("predict", {"request": {"value": 21}}))

    record = audit.records()[-1]
    assert payload == {"value": 42}
    assert record.arguments == {"request": {"value": 21}}
    assert record.result == {"value": 42}
    assert record.latency_ms > 0
    assert record.timestamp.tzinfo is not None
