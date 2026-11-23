"""Dagster orchestration: ingestion assets, the dbt project as assets, checks, schedule and failure alerts.

    raw/smard_series ─┐
                      ├─> dbt: staging -> intermediate -> marts (+ all dbt tests)
    raw/weather_hourly┘

- Ingestion assets use the same keys as the dbt sources, so Dagster shows one connected lineage graph.
- Ingestion retries with exponential backoff (API hiccups); schema drift is NOT retried, because
  retrying a changed format only repeats the failure. It fails fast and points to the quarantine file.
- Asset checks on the raw tables: freshness (newest hour) and run health (last load succeeded).
- A run-failure sensor writes an incident record that the runbook (and later an AI agent) starts from.

Start the UI:  dagster dev -m orchestration.definitions
"""

import json
import os
from datetime import timedelta
from pathlib import Path

import dagster as dg
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from energy_platform import db
from energy_platform.config import ROOT, db_path, now_utc
from energy_platform.ingest import ingest_smard, ingest_weather
from energy_platform.sources import SchemaDriftError

DBT_DIR = ROOT / "transform"
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)
dbt_project.prepare_if_dev()  # parses the dbt project when running `dagster dev`

INGEST_RETRY = dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)
SMARD_KEY = dg.AssetKey(["raw", "smard_series"])
WEATHER_KEY = dg.AssetKey(["raw", "weather_hourly"])


def _materialize(results) -> dg.MaterializeResult:
    failed = [r for r in results if r.status != "success"]
    if failed:
        message = "; ".join(f"{r.key}: {r.error}" for r in failed)
        if any("SchemaDriftError" in (r.error or "") for r in failed):
            # Don't retry: the source changed its format. A human has to look at the quarantined payload.
            raise dg.Failure(description=f"Schema drift, not retrying. {message}", allow_retries=False)
        raise RuntimeError(message)  # transient (e.g. API outage): retried by INGEST_RETRY
    return dg.MaterializeResult(metadata={
        "rows_fetched": sum(r.fetched for r in results),
        "rows_inserted": sum(r.inserted for r in results),
        "rows_changed (revisions)": sum(r.changed for r in results),
        "window": f"{results[0].window_start} -> {results[0].window_end}",
        "batches": dg.MetadataValue.json([r.batch_id for r in results]),
    })


@dg.asset(key=SMARD_KEY, group_name="ingestion", retry_policy=INGEST_RETRY,
          description="Hourly SMARD series (price, solar, wind, load), incremental with a 48h lookback.")
def smard_series() -> dg.MaterializeResult:
    with db.connect() as con:
        return _materialize(ingest_smard(con))


@dg.asset(key=WEATHER_KEY, group_name="ingestion", retry_policy=INGEST_RETRY,
          description="Hourly Open-Meteo weather for Hamburg, Munich and Frankfurt, with unit checks.")
def weather_hourly() -> dg.MaterializeResult:
    with db.connect() as con:
        return _materialize(ingest_weather(con))


def _freshness_check(table: str, max_age_hours: int) -> dg.AssetCheckResult:
    with db.connect() as con:
        newest = con.execute(f"select max(ts_utc) from {table}").fetchone()[0]
    age = (now_utc() - newest) if newest else None
    ok = age is not None and age <= timedelta(hours=max_age_hours)
    return dg.AssetCheckResult(
        passed=ok,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={"newest_hour_utc": str(newest), "age_hours": round(age.total_seconds() / 3600, 1) if age else None,
                  "max_age_hours": max_age_hours},
    )


@dg.asset_check(asset=SMARD_KEY, description="Newest SMARD hour is at most 30 hours old (SMARD publishes with delay).")
def smard_freshness() -> dg.AssetCheckResult:
    return _freshness_check("raw.smard_series", 30)


@dg.asset_check(asset=WEATHER_KEY, description="Newest weather hour is at most 6 hours old.")
def weather_freshness() -> dg.AssetCheckResult:
    return _freshness_check("raw.weather_hourly", 6)


@dg.asset_check(asset=SMARD_KEY, description="The latest load of every SMARD series succeeded.")
def smard_last_load_succeeded() -> dg.AssetCheckResult:
    with db.connect() as con:
        rows = con.execute("""
            select source, status, error from (
              select *, row_number() over (partition by source order by started_at desc) as rn
              from meta.load_runs where source like 'smard:%') where rn = 1""").fetchall()
    failed = [{"source": s, "error": e} for s, st, e in rows if st != "success"]
    return dg.AssetCheckResult(passed=not failed, metadata={"failed": dg.MetadataValue.json(failed)})


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def energy_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """All dbt models and tests. `dbt build` runs each model's tests right after it, so a failing
    test stops downstream models from being built on bad data."""
    yield from dbt.cli(["build"], context=context).stream()


refresh_job = dg.define_asset_job("refresh_energy_data", selection=dg.AssetSelection.all())

# SMARD updates its data during the day; every 6 hours is plenty and stays polite to a public API.
refresh_schedule = dg.ScheduleDefinition(job=refresh_job, cron_schedule="17 */6 * * *", execution_timezone="Europe/Berlin")


@dg.run_failure_sensor(description="Writes an incident record for every failed run (input for the runbook / AI doctor).")
def record_incident(context: dg.RunFailureSensorContext):
    folder = Path(os.getenv("EDP_INCIDENT_DIR", ROOT / "data" / "incidents"))
    folder.mkdir(parents=True, exist_ok=True)
    incident = {
        "run_id": context.dagster_run.run_id,
        "job": context.dagster_run.job_name,
        "failure": context.failure_event.message,
        "warehouse": str(db_path()),
        "next_step": "See docs/RUNBOOK.md; `edp status` shows the load-run log.",
    }
    (folder / f"{context.dagster_run.run_id}.json").write_text(json.dumps(incident, indent=2), encoding="utf-8")
    context.log.error(f"Incident recorded: {incident['failure']}")


defs = dg.Definitions(
    assets=[smard_series, weather_hourly, energy_dbt_models],
    asset_checks=[smard_freshness, weather_freshness, smard_last_load_succeeded],
    jobs=[refresh_job],
    schedules=[refresh_schedule],
    sensors=[record_incident],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
