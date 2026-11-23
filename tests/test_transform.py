"""Runs the real dbt project against the offline fixtures."""

from energy_platform import db
from energy_platform.cli import _transform
from energy_platform.ingest import ingest


def test_healthy_data_passes_all_models_and_tests():
    ingest()
    assert _transform([]) == 0
    with db.connect() as con:
        hours, complete = con.execute("select count(*), count(*) filter (where is_complete) from marts.fct_energy_hourly").fetchone()
        days = con.execute("select count(*) from marts.mart_daily_energy").fetchone()[0]
    assert (hours, complete, days) == (48, 48, 2)


def test_invalid_value_fails_the_build_and_stops_downstream_models():
    ingest()
    with db.connect() as con:
        con.execute("update raw.smard_series set value = -500 where series_id = 'solar_generation' and ts_utc = '2026-11-17 11:00'")
    assert _transform([]) == 1
    with db.connect() as con:
        failing = con.execute("select count(*) from test_failures.not_negative_fct_energy_hourly_solar_mwh").fetchone()[0]
        mart_exists = con.execute("select count(*) from information_schema.tables where table_schema = 'marts' and table_name = 'mart_daily_energy'").fetchone()[0]
    assert failing == 1
    assert mart_exists == 0  # skipped: bad data never reached the daily mart


def test_incremental_run_applies_a_revision(monkeypatch):
    ingest()
    assert _transform([]) == 0
    monkeypatch.setenv("EDP_CHAOS", "revision")
    ingest(("smard",))
    assert _transform([]) == 0  # reconciliation still passes
    with db.connect() as con:
        solar = con.execute("select solar_mwh from marts.fct_energy_hourly where hour_utc = '2026-11-17 11:00'").fetchone()[0]
    assert solar == 5164.11 + 250.0
