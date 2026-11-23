from energy_platform.cli import _dbt


def test_dagster_definitions_load():
    assert _dbt(["parse"]) == 0  # Dagster builds its dbt assets from the manifest
    from orchestration.definitions import defs

    keys = {k.to_user_string() for k in defs.resolve_all_asset_keys()}
    assert {"raw/smard_series", "raw/weather_hourly", "marts/fct_energy_hourly", "marts/mart_daily_energy"} <= keys
    assert defs.resolve_job_def("refresh_energy_data") is not None
