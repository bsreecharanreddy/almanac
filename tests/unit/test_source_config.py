from pathlib import Path

import pytest
from pydantic import ValidationError

from almanac.pipeline.source import Severity, SourceConfig

CONF = Path(__file__).resolve().parents[2] / "conf" / "sources" / "gharchive.yml"


def test_gharchive_config_loads() -> None:
    cfg = SourceConfig.load(CONF)
    assert cfg.name == "gharchive"
    assert "{year}" in cfg.url_template
    assert cfg.partition_by == ["event_date", "event_hour"]


def test_quality_rules_are_named_and_typed() -> None:
    cfg = SourceConfig.load(CONF)
    assert cfg.quality_rules, "at least one rule must be declared"
    for rule in cfg.quality_rules:
        assert rule.name
        assert rule.expression
        assert isinstance(rule.severity, Severity)


def test_rule_names_are_unique() -> None:
    # _failed_rules is keyed by name; a duplicate makes a failure untraceable.
    names = [r.name for r in SourceConfig.load(CONF).quality_rules]
    assert len(names) == len(set(names))


def test_unknown_field_is_rejected(tmp_path: Path) -> None:
    # A typo in YAML must fail loudly, not be silently ignored.
    p = tmp_path / "bad.yml"
    p.write_text(
        "name: x\nurl_template: 'u'\npartition_by: [a]\nformat: json\nquality_rules: []\ntyop: 1\n"
    )
    with pytest.raises(ValidationError):
        SourceConfig.load(p)


def test_duplicate_rule_names_are_rejected(tmp_path: Path) -> None:
    # The validator is unreachable from the real config, so without this
    # test it could be deleted and the suite would stay green.
    p = tmp_path / "dupe.yml"
    p.write_text(
        "name: x\n"
        "url_template: 'u'\n"
        "partition_by: [a]\n"
        "format: json\n"
        "quality_rules:\n"
        "  - {name: r, expression: 'a IS NOT NULL', severity: reject}\n"
        "  - {name: r, expression: 'b IS NOT NULL', severity: warn}\n"
    )
    with pytest.raises(ValidationError, match="unique"):
        SourceConfig.load(p)
