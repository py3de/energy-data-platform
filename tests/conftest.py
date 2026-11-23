import pytest


@pytest.fixture(autouse=True)
def offline_env(tmp_path, monkeypatch):
    """Every test gets its own warehouse and quarantine folder, fixtures instead of network, and a pinned 'now'."""
    monkeypatch.setenv("EDP_DB_PATH", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("EDP_QUARANTINE_DIR", str(tmp_path / "quarantine"))
    monkeypatch.setenv("EDP_OFFLINE", "1")
    monkeypatch.setenv("EDP_NOW", "2026-11-17T23:00:00Z")
    monkeypatch.delenv("EDP_CHAOS", raising=False)
    return tmp_path
