from datetime import datetime

from energy_platform import db
from energy_platform.ingest import ingest


def rows(table):
    with db.connect() as con:
        return con.execute(f"select count(*) from {table}").fetchone()[0]


def test_first_load_fetches_all_published_hours():
    results = ingest()
    assert all(r["status"] == "success" for r in results)
    assert rows("raw.smard_series") == 5 * 48      # 5 series x 48 published hours (unpublished nulls skipped)
    assert rows("raw.weather_hourly") == 3 * 48    # 3 locations x 48 hours


def test_reloading_is_idempotent_and_uses_the_lookback_window():
    ingest()
    second = ingest()
    assert rows("raw.smard_series") == 240
    assert all(r["inserted"] == 0 and r["changed"] == 0 for r in second)
    # window starts 48h before the newest loaded hour (2026-11-17 22:00), not at "now - 14 days"
    assert second[0]["window_start"] == datetime(2026, 11, 15, 22)


def test_watermark_is_the_newest_loaded_hour():
    ingest()
    with db.connect() as con:
        assert db.get_watermark(con, "smard", "solar_generation") == datetime(2026, 11, 17, 22)


def test_revised_values_are_detected_and_applied(monkeypatch):
    ingest()
    monkeypatch.setenv("EDP_CHAOS", "revision")
    results = {r["key"]: r for r in ingest(("smard",))}
    assert results["solar_generation"]["changed"] == 1
    with db.connect() as con:
        value = con.execute("select value from raw.smard_series where series_id = 'solar_generation' and ts_utc = '2026-11-17 11:00'").fetchone()[0]
    assert value == 5164.11 + 250.0


def test_schema_change_is_quarantined_and_only_that_series_fails(monkeypatch, offline_env):
    monkeypatch.setenv("EDP_CHAOS", "schema_change")
    results = {r["key"]: r for r in ingest(("smard",))}
    assert results["day_ahead_price"]["status"] == "failed"
    assert "SchemaDriftError" in results["day_ahead_price"]["error"]
    assert all(r["status"] == "success" for k, r in results.items() if k != "day_ahead_price")
    assert list((offline_env / "quarantine").glob("smard_day_ahead_price_*.json"))
    with db.connect() as con:
        assert db.get_watermark(con, "smard", "day_ahead_price") is None


def test_unit_change_blocks_the_load(monkeypatch):
    monkeypatch.setenv("EDP_CHAOS", "unit_change")
    [result] = ingest(("weather",))
    assert result["status"] == "failed" and "unexpected units" in result["error"]
    assert rows("raw.weather_hourly") == 0


def test_after_an_outage_the_next_run_catches_up(monkeypatch):
    monkeypatch.setenv("EDP_CHAOS", "api_outage")
    assert all(r["status"] == "failed" for r in ingest(("smard",)))
    assert rows("raw.smard_series") == 0
    monkeypatch.delenv("EDP_CHAOS")
    assert all(r["status"] == "success" for r in ingest(("smard",)))
    assert rows("raw.smard_series") == 240
    with db.connect() as con:
        failed = con.execute("select count(*) from meta.load_runs where status = 'failed'").fetchone()[0]
    assert failed == 5  # the outage stays visible in the run log
