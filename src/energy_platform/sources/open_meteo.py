"""Open-Meteo hourly weather for a few German locations (free, no API key).

One request returns a list with one object per location. Besides the shape, the units are
checked: a silent switch from km/h to mph would otherwise flow straight into the warehouse.
Weather data by Open-Meteo.com (CC BY 4.0).
"""

from __future__ import annotations

from datetime import datetime

import httpx
from pydantic import BaseModel, ValidationError, model_validator

from energy_platform.config import WEATHER_UNITS, WEATHER_VARIABLES, WeatherLocation
from energy_platform.sources import SchemaDriftError, quarantine

BASE_URL = "https://api.open-meteo.com/v1/forecast"


class LocationResponse(BaseModel):
    latitude: float
    longitude: float
    hourly_units: dict[str, str]
    hourly: dict[str, list]

    @model_validator(mode="after")
    def check(self) -> "LocationResponse":
        missing = [v for v in ("time", *WEATHER_VARIABLES) if v not in self.hourly]
        if missing:
            raise ValueError(f"missing hourly variables {missing}")
        lengths = {k: len(v) for k, v in self.hourly.items()}
        if len(set(lengths.values())) > 1:
            raise ValueError(f"hourly arrays have different lengths {lengths}")
        wrong = {v: self.hourly_units.get(v) for v, unit in WEATHER_UNITS.items() if self.hourly_units.get(v) != unit}
        if wrong:
            raise ValueError(f"unexpected units {wrong}, expected {WEATHER_UNITS}")
        return self


def fetch(client: httpx.Client, locations: tuple[WeatherLocation, ...], start: datetime, end: datetime):
    """Rows of (location, lat, lon, ts_utc, temperature, wind_speed, radiation) with start <= ts < end."""
    params = {
        "latitude": ",".join(str(l.latitude) for l in locations),
        "longitude": ",".join(str(l.longitude) for l in locations),
        "hourly": ",".join(WEATHER_VARIABLES),
        "start_date": start.date().isoformat(),
        "end_date": end.date().isoformat(),
        "timezone": "UTC",
    }
    response = client.get(BASE_URL, params=params)
    response.raise_for_status()
    payload = response.json()
    payload = payload if isinstance(payload, list) else [payload]
    try:
        if len(payload) != len(locations):
            raise ValueError(f"expected {len(locations)} locations, got {len(payload)}")
        parsed = [LocationResponse.model_validate(p) for p in payload]
    except (ValidationError, ValueError) as exc:
        msg = exc.errors()[0]["msg"] if isinstance(exc, ValidationError) else str(exc)
        reason = f"open_meteo: unexpected response ({msg})"
        path = quarantine("open_meteo", payload, reason)
        raise SchemaDriftError(f"{reason}. Payload quarantined at {path}") from exc

    rows = []
    for loc, data in zip(locations, parsed):
        h = data.hourly
        for i, t in enumerate(h["time"]):
            ts = datetime.fromisoformat(t)
            if start <= ts < end:  # the API also returns future forecast hours; we only keep observed ones
                rows.append((loc.name, data.latitude, data.longitude, ts,
                             h["temperature_2m"][i], h["wind_speed_100m"][i], h["shortwave_radiation"][i]))
    return rows
