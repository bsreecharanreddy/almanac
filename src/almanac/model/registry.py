"""Unity Catalog registration for the trained model -- pure naming logic
is unit-tested here; register_champion's live call is exercised for real
only during Phase 4's cloud verification step (design doc §5.2), the same
deferral Phase 3 Task 5 used for its own UC registration SQL.
"""

import mlflow
from mlflow import MlflowClient

CHAMPION_ALIAS = "champion"


def registered_model_name(catalog: str, schema: str, model_name: str = "pr_review_sla_risk") -> str:
    """The three-part `catalog.schema.model` name UC registration targets."""
    return f"{catalog}.{schema}.{model_name}"


def register_champion(model_uri: str, *, name: str, registry_uri: str = "databricks-uc") -> str:
    """Register `model_uri` under `name` and move the `@champion` alias to it."""
    mlflow.set_registry_uri(registry_uri)
    version = mlflow.register_model(model_uri, name)
    MlflowClient().set_registered_model_alias(
        name=name, alias=CHAMPION_ALIAS, version=version.version
    )
    return f"models:/{name}@{CHAMPION_ALIAS}"
