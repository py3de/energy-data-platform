"""Configuration: what we load, from where, and the knobs that control incremental behaviour."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SmardSeries:
    """One SMARD time series. Filter IDs come from the SMARD chart-data API."""

    series_id: str
    filter_id: int
    region: str
    unit: str


SMARD_SERIES: tuple[SmardSeries, ...] = (
    SmardSeries("day_ahead_price", 4169, "DE-LU", "EUR/MWh"),
    SmardSeries("solar_generation", 4068, "DE", "MWh"),
    SmardSeries("wind_onshore_generation", 4067, "DE", "MWh"),
    SmardSeries("wind_offshore_generation", 1225, "DE", "MWh"),
    SmardSeries("grid_load", 410, "DE", "MWh"),
)


@dataclass(frozen=True)
class WeatherLocation:
    name: str
    latitude: float
    longitude: float


# North (wind), south (solar) and centre of Germany, averaged into a simple national weather signal.
WEATHER_LOCATIONS: tuple[WeatherLocation, ...] = (
    WeatherLocation("hamburg", 53.55, 9.99),
    WeatherLocation("munich", 48.14, 11.58),
    WeatherLocation("frankfurt", 50.11, 8.68),
)
WEATHER_VARIABLES = ("temperature_2m", "wind_speed_100m", "shortwave_radiation")
WEATHER_UNITS = {"temperature_2m": "°C", "wind_speed_100m": "km/h", "shortwave_radiation": "W/m²"}


def db_path() -> Path:
    return Path(os.getenv("EDP_DB_PATH", ROOT / "data" / "warehouse.duckdb"))


def quarantine_dir() -> Path:
    return Path(os.getenv("EDP_QUARANTINE_DIR", ROOT / "data" / "quarantine"))


def offline() -> bool:
    """Offline mode serves recorded API responses from fixtures/ (tests, CI, demos without network)."""
    return os.getenv("EDP_OFFLINE", "false").lower() in ("1", "true", "yes")


def now_utc() -> datetime:
    """'Now' can be pinned with EDP_NOW so offline runs are deterministic."""
    pinned = os.getenv("EDP_NOW")
    if pinned:
        return datetime.fromisoformat(pinned.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    return datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0)


# Re-read this many hours before the watermark on every run: SMARD revises recent values and
# weather reanalysis changes for a while, so a pure "only newer than last time" load would miss fixes.
LOOKBACK = timedelta(hours=int(os.getenv("EDP_LOOKBACK_HOURS", "48")))
# How far back the very first load goes when there is no watermark yet.
INITIAL_LOAD = timedelta(days=int(os.getenv("EDP_INITIAL_DAYS", "14")))
