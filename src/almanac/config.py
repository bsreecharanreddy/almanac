"""Runtime settings. The only place defaults live."""

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Environment-overridable settings, prefix ``ALMANAC_``."""

    model_config = SettingsConfigDict(env_prefix="ALMANAC_", frozen=True)

    archive_base_url: str = "https://data.gharchive.org"
    data_dir: Path = _REPO_ROOT / "data"
    fixture_dir: Path = _REPO_ROOT / "tests" / "fixtures"
    # Gold's managed tables and the Derby metastore. Under gitignored data/:
    # a metastore is a build artifact, not a source file.
    warehouse_dir: Path = _REPO_ROOT / "data" / "warehouse"
    metastore_dir: Path = _REPO_ROOT / "data" / "metastore"
    http_timeout_seconds: float = 60.0
    max_fetch_attempts: int = 3
    # Concurrent hourly downloads during a backfill; Phase 1 measured a third of
    # billed time as single-threaded HTTP (2026-09-01-cluster-throughput.md).
    fetch_concurrency: int = 8
