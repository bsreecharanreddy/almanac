"""The Lakebase window's guards -- chiefly the region check that cost 44 minutes.

Phase 8 Task 7 targeted `databricks_database_instance.online_store` in the
**westus3** module. Lakebase names 19 supported regions and westus3 is not
one; the create hung for 44 minutes returning "temporarily unavailable"
while every other Databricks API answered instantly, and that was written
up as a vendor outage. It was not. These tests make the constraint
executable so the same hang cannot be re-diagnosed by hand.
"""

import json

import pytest

from almanac.infra import lakebase_window as lw
from almanac.infra.window import WindowError

from .test_infra_window import FakeTerraform, plan, state

WORKSPACE = "azurerm_databricks_workspace.this"


def test_westus3_is_refused_before_terraform_is_invoked() -> None:
    """The whole incident, as one assertion."""
    with pytest.raises(WindowError, match="westus3"):
        lw.check_region("westus3")


def test_centralus_is_admitted() -> None:
    """Measured 2026-09-06: a Lakebase region that also offers the node SKU."""
    lw.check_region("centralus")


def test_the_refusal_names_the_symptom_not_just_the_rule() -> None:
    """A hang plus "temporarily unavailable" reads exactly like an outage.

    Saying only "unsupported region" would leave the next reader to
    rediscover why their apply is hanging, which is the 44 minutes.
    """
    with pytest.raises(WindowError) as refusal:
        lw.check_region("westus3")

    message = str(refusal.value)
    assert "temporarily unavailable" in message
    assert "outage" in message


def test_an_unknown_region_is_refused_rather_than_assumed_fine() -> None:
    """Default-deny: a region absent from the list has not been checked."""
    with pytest.raises(WindowError):
        lw.check_region("mars-central-1")


def test_up_runs_the_azure_layer_before_the_full_apply() -> None:
    """`terraform apply` cannot run cold: the databricks provider is configured
    from a workspace URL that does not exist until the workspace does.
    """
    terraform = FakeTerraform(
        plans=[plan((WORKSPACE, ["create"]))],
        states=[
            state(
                (
                    WORKSPACE,
                    {"workspace_url": "adb-2.4.azuredatabricks.net", "location": "centralus"},
                )
            )
        ],
    )

    lw.up(terraform, apply=True, location="centralus")

    # The targeting is on the *plan*; both applies replay a saved plan file,
    # because `apply -target=` re-plans and would run something other than
    # what the guards just checked.
    plans = [call for call in terraform.calls if call[0] == "plan"]
    applies = [call for call in terraform.calls if call[0] == "apply"]

    assert len(applies) == 2, "two stages, not one"
    assert all(call[1] == lw.PLAN_FILE for call in applies), "apply must replay the saved plan"
    assert any("-target=" in flag for flag in plans[0]), "stage 1 must be targeted"
    assert not any("-target=" in flag for flag in plans[1]), "stage 2 must be the whole module"


def test_up_refuses_a_bad_region_without_calling_terraform() -> None:
    """Refusing after the apply has started is refusing too late."""
    terraform = FakeTerraform()

    with pytest.raises(WindowError, match="westus3"):
        lw.up(terraform, apply=True, location="westus3")

    assert terraform.calls == [], "terraform must not be invoked at all"


def test_up_verifies_the_created_workspace_landed_in_the_region_asked_for() -> None:
    """The argument says what was requested; state says what was built."""
    terraform = FakeTerraform(
        plans=[plan((WORKSPACE, ["create"]))],
        states=[
            state(
                (WORKSPACE, {"workspace_url": "adb-2.4.azuredatabricks.net", "location": "westus3"})
            )
        ],
    )

    with pytest.raises(WindowError, match="westus3"):
        lw.up(terraform, apply=True, location="centralus")


def test_down_reports_the_destroy_count_before_applying_it() -> None:
    """Phase 5's targeted destroy pulled in a job and would have thrown away
    the run history that was the evidence for four real runs.
    """
    live = state(
        (WORKSPACE, {"workspace_url": "adb-2.4.azuredatabricks.net", "location": "centralus"})
    )
    terraform = FakeTerraform(
        plans=[plan(), plan((WORKSPACE, ["delete"]))],
        states=[live, live],
    )

    message = lw.down(terraform, apply=False)

    assert "1" in message, "the count is the check"
    assert "apply" not in terraform.subcommands, "a dry run must not apply"


def test_down_refuses_when_config_and_state_disagree() -> None:
    """`force_destroy` has no remote counterpart, so it never reaches the
    provider until an apply writes it into state (the README's trap 3).
    """
    live = state(
        (WORKSPACE, {"workspace_url": "adb-2.4.azuredatabricks.net", "location": "centralus"})
    )
    terraform = FakeTerraform(
        plans=[plan(("databricks_external_location.stream", ["update"]))],
        states=[live],
    )

    with pytest.raises(WindowError, match="disagree"):
        lw.down(terraform, apply=True)


def test_down_refuses_to_trust_the_exit_code() -> None:
    """Phase 6's external location reported dependents already verified gone."""
    live = state(
        (WORKSPACE, {"workspace_url": "adb-2.4.azuredatabricks.net", "location": "centralus"})
    )
    terraform = FakeTerraform(
        plans=[plan(), plan((WORKSPACE, ["delete"]))],
        states=[live, live, live],
    )

    with pytest.raises(WindowError, match="still in state"):
        lw.down(terraform, apply=True)


def test_down_on_an_empty_stack_says_so_rather_than_planning() -> None:
    terraform = FakeTerraform(states=[json.dumps({"format_version": "1.0", "values": {}})])

    assert "already down" in lw.down(terraform, apply=True)
    assert "plan" not in terraform.subcommands
