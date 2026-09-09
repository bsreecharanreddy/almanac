"""One command up and down for the Lakebase stack, with its region as a guard."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

from almanac.infra.window import (
    PlannedChange,
    Terraform,
    WindowError,
    _guarded_plan,
    check_destroy_plan,
    check_sync_plan,
    planned_changes,
    state_resources,
    survivors,
    terraform_in,
    workspace_url,
)

TERRAFORM_DIR = "infra/terraform-lakebase"
PLAN_FILE = "window.tfplan"

WORKSPACE_ADDRESS = "azurerm_databricks_workspace.this"

# Microsoft Learn's Lakebase region list, read 2026-09-06 (Phase 6 Task 9) and
# re-confirmed by experiment on 2026-09-08: the identical
# databricks_database_instance that hung in westus3 reached AVAILABLE in
# centralus under the same config, subscription and credentials.
#
# Default-deny. A region missing from this list has not been checked, and
# "not checked" must not read as "fine" -- that is how westus3 got targeted.
LAKEBASE_REGIONS: frozenset[str] = frozenset(
    {
        "australiaeast",
        "brazilsouth",
        "canadacentral",
        "centralindia",
        "centralus",
        "eastus",
        "eastus2",
        "francecentral",
        "germanywestcentral",
        "japaneast",
        "koreacentral",
        "northeurope",
        "norwayeast",
        "southcentralus",
        "southeastasia",
        "swedencentral",
        "uksouth",
        "westeurope",
        "westus2",
    }
)

# The provider bootstrap: `terraform apply` cannot run cold here, because the
# databricks provider is configured from a workspace URL that does not exist
# until the workspace does, so the Databricks *data sources* fail to resolve on
# a first plan.
STAGE_ONE_TARGETS: tuple[str, ...] = (
    "azurerm_databricks_workspace.this",
    "azurerm_storage_container.stream",
    "azurerm_role_assignment.uc_storage",
)


def check_region(location: str) -> None:
    """Refuse a region Lakebase does not serve, before terraform is invoked.

    The message names the *symptom* rather than only the rule. A create in an
    unsupported region does not fail: it returns "temporarily unavailable" and
    then hangs, while every other Databricks API answers instantly -- which
    reads exactly like a vendor outage and was diagnosed as one on 2026-09-08,
    at a cost of 44 minutes.
    """
    if location not in LAKEBASE_REGIONS:
        raise WindowError(
            f"{location!r} is not a Lakebase region, so the database instance can never be "
            "created there. It will not fail cleanly: the create returns 'temporarily "
            "unavailable' and then hangs while every other Databricks API answers instantly, "
            "which looks like an outage and is not one. A Lakebase project inherits its "
            "workspace's region and cannot be moved. Supported: "
            f"{', '.join(sorted(LAKEBASE_REGIONS))}."
        )


def workspace_location(state: dict[str, Any]) -> str | None:
    """The region the workspace was actually built in, per state."""
    workspace = state_resources(state).get(WORKSPACE_ADDRESS)
    return None if workspace is None else str(workspace.get("location", ""))


def _whole_module_plan(terraform: Terraform, *, destroy: bool = False) -> list[PlannedChange]:
    """Plan the entire root module. Untargeted on purpose: here the module *is* the window."""
    terraform("plan", *(["-destroy"] if destroy else []), f"-out={PLAN_FILE}")
    return planned_changes(json.loads(terraform("show", "-json", PLAN_FILE)))


def up(terraform: Terraform, *, apply: bool, location: str) -> str:
    """Two staged applies, with the region checked before either one runs."""
    check_region(location)
    if not apply:
        return f"lakebase window: region {location} ok; two-stage apply not run (no --apply)"

    # Stage 1: the Azure layer. Targeted, because the provider for stage 2 is
    # configured from a workspace that does not exist yet.
    _guarded_plan(terraform, STAGE_ONE_TARGETS)
    terraform("apply", PLAN_FILE, capture=False)

    state = json.loads(terraform("show", "-json"))
    built = workspace_location(state)
    if built and built not in LAKEBASE_REGIONS:
        raise WindowError(
            f"stage 1 built the workspace in {built!r}, which is not a Lakebase region. "
            "The argument says what was requested; state says what was built. Destroy before "
            "stage 2 rather than provisioning a stack whose instance can never come up."
        )

    # Stage 2: Unity Catalog, the Lakebase instance, and the job.
    _whole_module_plan(terraform)
    terraform("apply", PLAN_FILE, capture=False)

    url = workspace_url(json.loads(terraform("show", "-json")))
    return f"lakebase window up in {built or location} at {url}"


def down(terraform: Terraform, *, apply: bool) -> str:
    """Tear the whole module down, reporting the count first and proving it went."""
    live_state = json.loads(terraform("show", "-json"))
    present = sorted(state_resources(live_state))
    if not present:
        return "lakebase window already down: nothing in state"

    # The force_destroy trap, generalised: terraform destroys from prior state,
    # so an attribute with no remote counterpart never reaches the provider
    # until an apply has written it in.
    check_sync_plan(_whole_module_plan(terraform), present)

    changes = _whole_module_plan(terraform, destroy=True)
    check_destroy_plan(changes, present)
    if not apply:
        return f"lakebase destroy plan (not applied): {len(changes)} to destroy"

    terraform("apply", PLAN_FILE, capture=False)

    left = survivors(json.loads(terraform("show", "-json")), present)
    if left:
        raise WindowError(
            f"destroy reported success but {', '.join(left)} are still in state. "
            "Check for orphaned metadata before assuming the resources are gone."
        )
    return f"lakebase window down: {len(present)} resources removed and confirmed absent from state"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["up", "down"])
    parser.add_argument("--chdir", default=TERRAFORM_DIR)
    parser.add_argument("--location", default="centralus")
    parser.add_argument(
        "--apply", action="store_true", help="Run it. Without this, plan and guard only."
    )
    args = parser.parse_args(argv)

    terraform = terraform_in(args.chdir)
    try:
        if args.command == "up":
            print(up(terraform, apply=args.apply, location=args.location))
        else:
            print(down(terraform, apply=args.apply))
    except WindowError as refusal:
        print(f"refused: {refusal}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
