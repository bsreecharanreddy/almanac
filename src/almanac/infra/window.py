"""One command up, one command down, with Phase 6's four teardown traps as checks."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

# The only addresses either command may touch. Everything else in
# infra/terraform/ is either live and must not be disturbed, or deliberately
# destroyed and expensive to bring back: on 2026-09-07 a bare `terraform plan`
# read "6 to add", five of which were Phase 5's and Phase 6's torn-down stacks
# (~$19/day idle, and neither scales to zero).
#
# The three dashboards joined on 2026-09-08 (Task 9b). They cost nothing while
# the warehouse is down, but they are torn down with it so the window leaves
# nothing behind -- and so a destroy plan that touches only these addresses is
# the whole of what Phase 7 provisioned.
WINDOW_TARGETS: tuple[str, ...] = (
    "databricks_sql_endpoint.reporting",
    'databricks_dashboard.reporting["review-sla-risk"]',
    'databricks_dashboard.reporting["model-platform-health"]',
    'databricks_dashboard.reporting["developer-engagement"]',
)

TERRAFORM_DIR = "infra/terraform"
PLAN_FILE = "window.tfplan"

# Read for identity instead of `terraform output` -- see `workspace_url`.
WORKSPACE_ADDRESS = "azurerm_databricks_workspace.this"

# `no-op` is not a change, and a data source `read` is not one either.
_INERT = frozenset({"no-op", "read"})


class WindowError(Exception):
    """Raised before terraform is allowed to change anything."""


class Terraform(Protocol):
    """Runs one terraform subcommand in the root module and returns its stdout."""

    def __call__(self, *args: str, capture: bool = True) -> str: ...


@dataclass(frozen=True)
class PlannedChange:
    """One address a plan would touch, and what it would do to it."""

    address: str
    actions: tuple[str, ...]

    def does(self, action: str) -> bool:
        return action in self.actions


def planned_changes(plan: dict[str, Any]) -> list[PlannedChange]:
    """Every address a `terraform show -json <planfile>` document would actually change."""
    changes = [
        PlannedChange(change["address"], tuple(change["change"]["actions"]))
        for change in plan.get("resource_changes", [])
    ]
    return [change for change in changes if not _INERT.issuperset(change.actions)]


def out_of_scope(changes: Sequence[PlannedChange], allowed: Iterable[str]) -> list[PlannedChange]:
    """Changes to addresses the window never asked for -- the whole point of the guard."""
    permitted = set(allowed)
    return [change for change in changes if change.address not in permitted]


def doing(changes: Sequence[PlannedChange], action: str) -> list[PlannedChange]:
    """Changes carrying `action`, e.g. the creates a destroy plan must never contain."""
    return [change for change in changes if change.does(action)]


def _refuse(problem: str, changes: Sequence[PlannedChange], remedy: str) -> None:
    listed = ", ".join(f"{change.address} ({'+'.join(change.actions)})" for change in changes)
    raise WindowError(f"{problem}: {listed}. {remedy}")


def check_up_plan(changes: Sequence[PlannedChange], targets: Sequence[str]) -> None:
    """Refuse a bring-up that reaches past the window, or that removes anything."""
    if strays := out_of_scope(changes, targets):
        _refuse(
            "plan would change resources outside the window",
            strays,
            "Phase 5's and Phase 6's stacks are destroyed on purpose; recreating them bills daily.",
        )
    if removals := doing(changes, "delete"):
        _refuse(
            "plan would destroy resources during bring-up",
            removals,
            "State has drifted from config. Read the plan before letting it run.",
        )


def check_sync_plan(changes: Sequence[PlannedChange], targets: Sequence[str]) -> None:
    """Refuse a teardown while config and state still disagree about the targets.

    Terraform destroys from *prior state*, so an attribute with no remote
    counterpart -- `force_destroy` is the one that cost Phase 6 an afternoon --
    never reaches the provider until an `apply` has written it into state. A
    plain plan over the targets is the general form of that check: if it is not
    a no-op, the destroy would run against attributes the provider never saw.
    """
    if changes:
        _refuse(
            "config and state disagree before teardown",
            changes,
            f"Run `terraform apply {' '.join(f'-target={t}' for t in targets)}` first, "
            "then retry the teardown.",
        )


def check_destroy_plan(changes: Sequence[PlannedChange], targets: Sequence[str]) -> None:
    """Refuse a teardown that creates anything, or that cascades past the window."""
    if creations := doing(changes, "create"):
        _refuse(
            "destroy plan would create resources",
            creations,
            "Destroyed-but-still-in-config resources plan as creates; -target the window only.",
        )
    if strays := out_of_scope(changes, targets):
        _refuse(
            "destroy plan reaches past the window",
            strays,
            "Phase 5's targeted destroy pulled in a job, and a deleted job loses its run history.",
        )


def state_resources(state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Address -> attributes, from `terraform show -json`. This root module has no child modules."""
    resources = state.get("values", {}).get("root_module", {}).get("resources", [])
    return {resource["address"]: resource.get("values", {}) for resource in resources}


def workspace_url(state: dict[str, Any]) -> str:
    """The workspace URL, read from state rather than from `terraform output`.

    `terraform output` drops any output whose resource is gone: 3 of this
    repo's 17 declared outputs are already missing for that reason. Phase 6
    lost all of them, and the next Databricks call failed as an *auth* error.
    """
    workspace = state_resources(state).get(WORKSPACE_ADDRESS)
    if workspace is None:
        raise WindowError(
            f"{WORKSPACE_ADDRESS} is not in terraform state, so the workspace URL cannot be "
            "resolved. This is not an authentication failure -- check the state, not the token."
        )
    return f"https://{workspace['workspace_url']}"


def survivors(state: dict[str, Any], targets: Iterable[str]) -> list[str]:
    """Targets still in state after their own destroy -- terraform's exit code is not proof.

    Phase 6's external location reported dependent tables and volumes that had
    already been verified gone, with identical counts on retry: orphaned
    metadata, not a race.
    """
    present = state_resources(state)
    return [target for target in targets if target in present]


def _summarize(changes: Sequence[PlannedChange]) -> str:
    lines = [f"  {'+'.join(change.actions)} {change.address}" for change in changes]
    return "\n".join(lines) if lines else "  (nothing)"


def _guarded_plan(
    terraform: Terraform, targets: Sequence[str], *, destroy: bool = False
) -> list[PlannedChange]:
    if not targets:
        # No -target flags is a whole-stack plan, which is the one thing every
        # guard below exists to prevent. Refuse before terraform is invoked.
        raise WindowError("no window targets given; a plan with no -target covers the whole stack")
    flags = [f"-target={target}" for target in targets]
    terraform("plan", *(["-destroy"] if destroy else []), *flags, f"-out={PLAN_FILE}")
    return planned_changes(json.loads(terraform("show", "-json", PLAN_FILE)))


def up(terraform: Terraform, targets: Sequence[str], *, apply: bool) -> str:
    """Plan the window, refuse anything wider, and apply exactly the plan that was checked."""
    changes = _guarded_plan(terraform, targets)
    check_up_plan(changes, targets)
    if not apply:
        return f"window plan (not applied):\n{_summarize(changes)}"

    # Applying the saved plan file, never `apply -target=...`: the latter
    # re-plans, so what runs is not what was guarded.
    terraform("apply", PLAN_FILE, capture=False)

    state = json.loads(terraform("show", "-json"))
    resources = state_resources(state)
    ids = [f"{target}={resources.get(target, {}).get('id', 'unknown')}" for target in targets]
    return f"window up at {workspace_url(state)}: {', '.join(ids)}"


def down(terraform: Terraform, targets: Sequence[str], *, apply: bool) -> str:
    """Take the window down, and prove from state that it went."""
    live = survivors(json.loads(terraform("show", "-json")), targets)
    if not live:
        return f"window already down: none of {', '.join(targets)} are in state"

    check_sync_plan(_guarded_plan(terraform, live), live)

    changes = _guarded_plan(terraform, live, destroy=True)
    check_destroy_plan(changes, live)
    if not apply:
        return f"window destroy plan (not applied):\n{_summarize(changes)}"

    terraform("apply", PLAN_FILE, capture=False)

    left = survivors(json.loads(terraform("show", "-json")), live)
    if left:
        raise WindowError(
            f"destroy reported success but {', '.join(left)} are still in state. "
            "Check for orphaned metadata before assuming the resource is gone."
        )
    return f"window down: {', '.join(live)} removed and confirmed absent from state"


def terraform_in(directory: str) -> Terraform:
    """The real runner. `-chdir` must precede the subcommand."""

    def run(*args: str, capture: bool = True) -> str:
        completed = subprocess.run(
            ["terraform", f"-chdir={directory}", *args],
            check=True,
            capture_output=capture,
            text=True,
        )
        return completed.stdout if capture else ""

    return run


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["up", "down"])
    parser.add_argument("--chdir", default=TERRAFORM_DIR)
    parser.add_argument(
        "--apply", action="store_true", help="Run it. Without this, plan and guard only."
    )
    args = parser.parse_args()

    action = up if args.command == "up" else down
    try:
        print(action(terraform_in(args.chdir), WINDOW_TARGETS, apply=args.apply))
    except WindowError as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
