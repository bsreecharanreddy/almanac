"""The window guards: every trap Phase 6's teardown hit, asserted before terraform runs."""

import json
from typing import Any

import pytest

from almanac.infra import window

# The five addresses that were in config but not in state on 2026-09-07, when a
# bare `terraform plan` read "6 to add". Recreating them costs ~$19/day idle.
TORN_DOWN = (
    "databricks_database_instance.online_store",
    "databricks_vector_search_endpoint.embeddings",
    "databricks_vector_search_index.pr_issue_embeddings",
    "databricks_job.streaming",
    "databricks_volume.stream_landing",
)

WAREHOUSE = "databricks_sql_endpoint.reporting"
WORKSPACE = (window.WORKSPACE_ADDRESS, {"workspace_url": "adb-1.0.azuredatabricks.net"})


def plan(*changes: tuple[str, list[str]]) -> str:
    """A `terraform show -json <planfile>` document carrying exactly these changes."""
    return json.dumps(
        {
            "format_version": "1.0",
            "resource_changes": [
                {
                    "address": address,
                    "mode": "managed",
                    "type": address.split(".")[0],
                    "name": address.split(".")[1],
                    "change": {"actions": actions},
                }
                for address, actions in changes
            ],
        }
    )


def state(*resources: tuple[str, dict[str, Any]]) -> str:
    """A `terraform show -json` state document. This root module has no child modules."""
    return json.dumps(
        {
            "format_version": "1.0",
            "values": {
                "outputs": {},
                "root_module": {
                    "resources": [
                        {
                            "address": address,
                            "mode": "managed",
                            "type": address.split(".")[0],
                            "name": address.split(".")[1],
                            "values": values,
                        }
                        for address, values in resources
                    ]
                },
            },
        }
    )


def changes(*actions: tuple[str, list[str]]) -> list[window.PlannedChange]:
    return window.planned_changes(json.loads(plan(*actions)))


class FakeTerraform:
    """Records every argv it is handed; replays queued documents, last one repeating."""

    def __init__(self, plans: list[str] | None = None, states: list[str] | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self._plans = plans or ["{}"]
        self._states = states or ["{}"]

    def __call__(self, *args: str, capture: bool = True) -> str:
        self.calls.append(args)
        if args[0] != "show":
            return ""
        queue = self._plans if len(args) == 3 else self._states
        return queue.pop(0) if len(queue) > 1 else queue[0]

    @property
    def subcommands(self) -> list[str]:
        return [call[0] for call in self.calls]


def test_planned_changes_ignores_no_ops_and_data_reads() -> None:
    document = json.loads(plan((WAREHOUSE, ["create"]), ("databricks_job.gold", ["no-op"])))
    document["resource_changes"].append(
        {
            "address": "data.databricks_current_user.me",
            "mode": "data",
            "type": "databricks_current_user",
            "name": "me",
            "change": {"actions": ["read"]},
        }
    )

    assert [change.address for change in window.planned_changes(document)] == [WAREHOUSE]


def test_out_of_scope_names_every_resurrected_resource() -> None:
    proposed = changes((WAREHOUSE, ["create"]), *((address, ["create"]) for address in TORN_DOWN))

    assert [c.address for c in window.out_of_scope(proposed, [WAREHOUSE])] == list(TORN_DOWN)


def test_up_refuses_a_plan_that_would_resurrect_torn_down_infrastructure() -> None:
    proposed = changes(
        (WAREHOUSE, ["create"]), ("databricks_database_instance.online_store", ["create"])
    )

    with pytest.raises(window.WindowError, match="online_store"):
        window.check_up_plan(proposed, [WAREHOUSE])


def test_up_refuses_a_plan_that_would_destroy_anything() -> None:
    """A delete during bring-up means state drifted; it is never what `up` was asked for."""
    with pytest.raises(window.WindowError, match="destroy"):
        window.check_up_plan(changes((WAREHOUSE, ["delete", "create"])), [WAREHOUSE])


def test_up_accepts_a_plan_that_only_creates_the_window() -> None:
    window.check_up_plan(changes((WAREHOUSE, ["create"])), [WAREHOUSE])


def test_destroy_refuses_a_plan_that_creates_anything() -> None:
    """Trap 4: with resources destroyed but still in config, a plan proposes recreating them."""
    proposed = changes(
        (WAREHOUSE, ["delete"]), ("databricks_vector_search_endpoint.embeddings", ["create"])
    )

    with pytest.raises(window.WindowError, match="create"):
        window.check_destroy_plan(proposed, [WAREHOUSE])


def test_destroy_refuses_a_dependent_it_was_not_asked_to_remove() -> None:
    """Phase 5: a targeted destroy pulled in a job, and a deleted job loses its run history."""
    proposed = changes((WAREHOUSE, ["delete"]), ("databricks_job.pr_similarity", ["delete"]))

    with pytest.raises(window.WindowError, match="pr_similarity"):
        window.check_destroy_plan(proposed, [WAREHOUSE])


def test_destroy_accepts_a_plan_that_only_removes_the_window() -> None:
    window.check_destroy_plan(changes((WAREHOUSE, ["delete"])), [WAREHOUSE])


def test_teardown_refuses_while_config_and_state_still_disagree() -> None:
    """Trap 3: `force_destroy = true` never reaches the provider until an apply writes it."""
    with pytest.raises(window.WindowError, match="apply"):
        window.check_sync_plan(changes((WAREHOUSE, ["update"])), [WAREHOUSE])


def test_teardown_precondition_passes_when_state_matches_config() -> None:
    window.check_sync_plan(changes((WAREHOUSE, ["no-op"])), [WAREHOUSE])


def test_workspace_url_comes_from_state_and_says_so_when_it_cannot() -> None:
    """Trap 1: `terraform output` empties out, and the symptom presents as an auth error."""
    assert (
        window.workspace_url(json.loads(state(WORKSPACE))) == "https://adb-1.0.azuredatabricks.net"
    )

    with pytest.raises(window.WindowError, match="not an authentication failure"):
        window.workspace_url(json.loads(state()))


def test_survivors_names_what_a_destroy_left_behind() -> None:
    """Trap 2: the external location refused to delete, citing dependents already gone."""
    left = json.loads(state((WAREHOUSE, {"id": "abc"})))

    assert window.survivors(left, [WAREHOUSE, "databricks_dashboard.sla_risk"]) == [WAREHOUSE]
    assert window.survivors(json.loads(state()), [WAREHOUSE]) == []


def test_up_never_reaches_apply_when_the_guard_fires() -> None:
    terraform = FakeTerraform(
        plans=[plan((WAREHOUSE, ["create"]), ("databricks_job.streaming", ["create"]))]
    )

    with pytest.raises(window.WindowError):
        window.up(terraform, [WAREHOUSE], apply=True)

    assert "apply" not in terraform.subcommands


def test_up_applies_the_saved_plan_rather_than_replanning() -> None:
    """`apply <planfile>` executes the guarded plan; `apply -target=...` would re-plan."""
    terraform = FakeTerraform(
        plans=[plan((WAREHOUSE, ["create"]))],
        states=[state(WORKSPACE, (WAREHOUSE, {"id": "wh-1"}))],
    )

    summary = window.up(terraform, [WAREHOUSE], apply=True)

    apply_call = next(call for call in terraform.calls if call[0] == "apply")
    assert window.PLAN_FILE in apply_call
    assert not any(argument.startswith("-target=") for argument in apply_call)
    assert "wh-1" in summary


def test_up_stops_at_the_guarded_plan_unless_asked_to_apply() -> None:
    terraform = FakeTerraform(plans=[plan((WAREHOUSE, ["create"]))])

    window.up(terraform, [WAREHOUSE], apply=False)

    assert "apply" not in terraform.subcommands


def test_down_removes_the_window_and_confirms_it_from_state() -> None:
    terraform = FakeTerraform(
        plans=[plan((WAREHOUSE, ["no-op"])), plan((WAREHOUSE, ["delete"]))],
        states=[state(WORKSPACE, (WAREHOUSE, {"id": "wh-1"})), state(WORKSPACE)],
    )

    summary = window.down(terraform, [WAREHOUSE], apply=True)

    assert terraform.subcommands.count("plan") == 2
    assert "apply" in terraform.subcommands
    assert "down" in summary


def test_down_is_a_no_op_when_the_window_is_already_gone() -> None:
    terraform = FakeTerraform(states=[state(WORKSPACE)])

    summary = window.down(terraform, [WAREHOUSE], apply=True)

    assert terraform.subcommands == ["show"]
    assert "already down" in summary


def test_down_reports_a_target_that_outlived_its_own_destroy() -> None:
    terraform = FakeTerraform(
        plans=[plan((WAREHOUSE, ["no-op"])), plan((WAREHOUSE, ["delete"]))],
        states=[state((WAREHOUSE, {"id": "wh-1"}))],
    )

    with pytest.raises(window.WindowError, match="still in state"):
        window.down(terraform, [WAREHOUSE], apply=True)


def test_an_empty_target_set_never_reaches_terraform() -> None:
    """No -target flags is a whole-stack plan -- the one thing the guards exist to prevent."""
    terraform = FakeTerraform()

    with pytest.raises(window.WindowError, match="whole stack"):
        window.up(terraform, [], apply=True)

    assert terraform.calls == []


def test_the_window_targets_are_exactly_what_phase_7_provisions() -> None:
    """The warehouse plus §7's three dashboards, and nothing else. A target added
    here without thought is how a torn-down stack gets resurrected.
    """
    assert window.WINDOW_TARGETS == (
        "databricks_sql_endpoint.reporting",
        'databricks_dashboard.reporting["review-sla-risk"]',
        'databricks_dashboard.reporting["model-platform-health"]',
        'databricks_dashboard.reporting["developer-engagement"]',
    )
