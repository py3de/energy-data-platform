"""Incremental ingestion into the raw layer.

For every source key (a SMARD series, or the weather location set):
  window = [watermark - LOOKBACK, now)     or [now - INITIAL_LOAD, now) on the first run
  fetch  -> validate schema -> idempotent upsert -> advance watermark -> log the run

Design decisions:
- The watermark is the latest timestamp actually loaded, not "now". If a source publishes late,
  the next run still asks for the missing hours.
- Every run re-reads a lookback window, so revised values are picked up and counted as changes.
- A failure (API down, schema drift) marks the run as failed and leaves the watermark untouched,
  so the next successful run automatically catches up. Nothing partial is committed.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from energy_platform import db
from energy_platform.config import INITIAL_LOAD, LOOKBACK, SMARD_SERIES, WEATHER_LOCATIONS, now_utc
from energy_platform.http import client as http_client
from energy_platform.sources import open_meteo, smard


@dataclass
class RunResult:
    source: str
    key: str
    batch_id: str
    status: str
    window_start: datetime
    window_end: datetime
    fetched: int = 0
    inserted: int = 0
    changed: int = 0
    error: str | None = None


def _window(con, source: str, key: str, since: datetime | None) -> tuple[datetime, datetime]:
    end = now_utc()
    if since is not None:
        return since, end
    watermark = db.get_watermark(con, source, key)
    return (watermark - LOOKBACK if watermark else end - INITIAL_LOAD), end


def _run(con, source: str, key: str, since: datetime | None, fetch, table: str, key_cols: list[str], value_cols: list[str], to_rows) -> RunResult:
    start, end = _window(con, source, key, since)
    batch_id = f"{source}-{key}-{uuid.uuid4().hex[:8]}"
    result = RunResult(source, key, batch_id, "running", start, end)
    db.start_run(con, batch_id, source if key == "all" else f"{source}:{key}", start, end)
    in_tx = False
    try:
        fetched = fetch(start, end)
        loaded_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        rows = to_rows(fetched, loaded_at, batch_id)
        con.execute("begin transaction")
        in_tx = True
        result.inserted, result.changed = db.upsert(con, table, key_cols, value_cols, rows)
        if fetched:
            newest = max(r[key_cols.index("ts_utc")] for r in rows)
            current = db.get_watermark(con, source, key)
            db.set_watermark(con, source, key, max(newest, current) if current else newest)
        con.execute("commit")
        in_tx = False
        result.fetched, result.status = len(fetched), "success"
    except Exception as exc:  # noqa: BLE001 - every failure is recorded in the run log, then re-raised
        if in_tx:
            con.execute("rollback")
        result.status, result.error = "failed", f"{type(exc).__name__}: {exc}"
    db.finish_run(con, batch_id, result.status, result.fetched, result.inserted, result.changed, result.error)
    return result


def ingest_smard(con, since: datetime | None = None) -> list[RunResult]:
    results = []
    with http_client() as http:
        for series in SMARD_SERIES:
            results.append(_run(
                con, "smard", series.series_id, since,
                fetch=lambda s, e, series=series: smard.fetch(http, series, s, e),
                table="raw.smard_series",
                key_cols=["series_id", "ts_utc"],
                value_cols=["filter_id", "region", "value"],
                to_rows=lambda fetched, at, batch, series=series: [
                    (series.series_id, ts, series.filter_id, series.region, value, at, batch) for ts, value in fetched
                ],
            ))
    return results


def ingest_weather(con, since: datetime | None = None) -> list[RunResult]:
    with http_client() as http:
        return [_run(
            con, "open_meteo", "all", since,
            fetch=lambda s, e: open_meteo.fetch(http, WEATHER_LOCATIONS, s, e),
            table="raw.weather_hourly",
            key_cols=["location", "ts_utc"],
            value_cols=["latitude", "longitude", "temperature_2m", "wind_speed_100m", "shortwave_radiation"],
            to_rows=lambda fetched, at, batch: [
                (loc, ts, lat, lon, temp, wind, rad, at, batch) for loc, lat, lon, ts, temp, wind, rad in fetched
            ],
        )]


def ingest(sources: tuple[str, ...] = ("smard", "weather"), since: datetime | None = None) -> list[dict]:
    results: list[RunResult] = []
    with db.connect() as con:
        if "smard" in sources:
            results += ingest_smard(con, since)
        if "weather" in sources:
            results += ingest_weather(con, since)
    return [asdict(r) for r in results]


def parse_since(value: str | None) -> datetime | None:
    """'2026-11-01' or '7d' (days before now) for backfills."""
    if not value:
        return None
    if value.endswith("d") and value[:-1].isdigit():
        return now_utc() - timedelta(days=int(value[:-1]))
    return datetime.fromisoformat(value)
