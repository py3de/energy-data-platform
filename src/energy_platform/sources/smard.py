"""SMARD (Bundesnetzagentur) electricity market data.

The chart-data API serves each series in weekly chunks:
  /app/chart_data/{filter}/{region}/index_hour.json                 -> {"timestamps": [chunk starts, ms]}
  /app/chart_data/{filter}/{region}/{filter}_{region}_hour_{ts}.json -> {"meta_data": {...}, "series": [[ms, value|null], ...]}
Hours that are not published yet are null. Data: Bundesnetzagentur | SMARD.de (CC BY 4.0).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx
from pydantic import BaseModel, ValidationError, field_validator

from energy_platform.config import SmardSeries
from energy_platform.sources import SchemaDriftError, quarantine

BASE_URL = "https://www.smard.de/app/chart_data"
CHUNK = timedelta(days=7)


class SmardIndex(BaseModel):
    timestamps: list[int]

    @field_validator("timestamps")
    @classmethod
    def not_empty(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("index has no chunks")
        return v


class SmardChunk(BaseModel):
    meta_data: dict
    series: list[tuple[int, float | None]]

    @field_validator("series")
    @classmethod
    def hourly(cls, v: list[tuple[int, float | None]]) -> list[tuple[int, float | None]]:
        steps = {b[0] - a[0] for a, b in zip(v, v[1:])}
        if steps and steps != {3_600_000}:
            raise ValueError(f"expected hourly steps, got {sorted(steps)[:3]} ms")
        return v


def _ms_to_utc(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def _get(client: httpx.Client, url: str, model: type[BaseModel], series: SmardSeries):
    response = client.get(url)
    response.raise_for_status()
    payload = response.json()
    try:
        return model.model_validate(payload)
    except ValidationError as exc:
        reason = f"{series.series_id}: unexpected response format ({exc.errors()[0]['msg']})"
        path = quarantine(f"smard_{series.series_id}", payload, reason)
        raise SchemaDriftError(f"{reason}. Payload quarantined at {path}") from exc


def fetch(client: httpx.Client, series: SmardSeries, start: datetime, end: datetime) -> list[tuple[datetime, float]]:
    """Published (non-null) hourly values with start <= ts < end, fetched chunk by chunk."""
    base = f"{BASE_URL}/{series.filter_id}/{series.region}"
    index = _get(client, f"{base}/index_hour.json", SmardIndex, series)
    rows: list[tuple[datetime, float]] = []
    for chunk_ms in index.timestamps:
        chunk_start = _ms_to_utc(chunk_ms)
        if chunk_start + CHUNK <= start or chunk_start >= end:
            continue  # this week's chunk is entirely outside the load window
        chunk = _get(client, f"{base}/{series.filter_id}_{series.region}_hour_{chunk_ms}.json", SmardChunk, series)
        for ms, value in chunk.series:
            ts = _ms_to_utc(ms)
            if value is not None and start <= ts < end:
                rows.append((ts, value))
    return rows
