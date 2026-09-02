"""Declarative source configuration. Pure; no Spark, no I/O beyond a read."""

from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class Severity(StrEnum):
    """What a failed rule does to the record.

    ``REJECT`` quarantines it. ``WARN`` annotates it but leaves it in the
    clean set -- used where a field is genuinely optional in some era.
    """

    REJECT = "reject"
    WARN = "warn"


class QualityRule(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    expression: str = Field(min_length=1)
    severity: Severity


class SourceConfig(BaseModel):
    """A source, declared entirely in data.

    Onboarding a second source must require a new YAML file and no new
    Python -- see design doc §4.5a and the Phase 2 gate.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    url_template: str = Field(min_length=1)
    format: str = Field(min_length=1)
    partition_by: list[str] = Field(min_length=1)
    quality_rules: list[QualityRule]

    @field_validator("quality_rules")
    @classmethod
    def _rule_names_unique(cls, rules: list[QualityRule]) -> list[QualityRule]:
        names = [r.name for r in rules]
        if len(names) != len(set(names)):
            raise ValueError("quality rule names must be unique")
        return rules

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls.model_validate(yaml.safe_load(path.read_text()))


# --- The second source: everything below is new Python §4.5a's "zero new
# --- Python" claim did not survive. A gzip-file mirror reuses `SourceConfig`
# --- untouched; a paginated, authenticated, rate-limited API does not.


class AuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    scheme: Literal["bearer"] = "bearer"
    token_env: str = Field(min_length=1)


class RateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    requests_per_hour: int = Field(gt=0)
    header_remaining: str = "X-RateLimit-Remaining"
    header_reset: str = "X-RateLimit-Reset"


class RestSourceConfig(BaseModel):
    """A REST API source. Distinct from ``SourceConfig`` on purpose -- the
    shapes do not overlap, and pretending they did (one model with every
    field optional) would make both unreadable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1)
    kind: Literal["rest_api"]
    url_template: str = Field(min_length=1)
    list_url_template: str = Field(min_length=1)
    enrichment_fields: list[str] = Field(min_length=1)
    auth: AuthConfig
    rate_limit: RateLimitConfig
    max_attempts: int = Field(default=3, gt=0)

    @classmethod
    def load(cls, path: Path) -> Self:
        return cls.model_validate(yaml.safe_load(path.read_text()))
