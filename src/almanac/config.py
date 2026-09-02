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
    # Gold's managed tables, and the embedded Derby database that remembers
    # they exist. Both live under `data/`, which is gitignored: a metastore
    # is a local build artifact, not a source file.
    warehouse_dir: Path = _REPO_ROOT / "data" / "warehouse"
    metastore_dir: Path = _REPO_ROOT / "data" / "metastore"
    http_timeout_seconds: float = 60.0
    max_fetch_attempts: int = 3
