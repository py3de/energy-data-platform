"""HTTP client for the source APIs, with an offline mode and failure injection.

Offline mode (EDP_OFFLINE=1) answers requests from recorded real responses in fixtures/, so tests,
CI and demos run without network. EDP_CHAOS injects realistic source failures into those responses
(see energy_platform.chaos) so every failure path in the runbook can be reproduced on demand.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

import httpx

from energy_platform.config import ROOT, offline

FIXTURES = ROOT / "fixtures"


def _chaos() -> str:
    return os.getenv("EDP_CHAOS", "")


class FixtureTransport(httpx.BaseTransport):
    """Serves recorded SMARD / Open-Meteo responses, optionally mutated by a chaos scenario."""

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        url = request.url
        if url.host.endswith("smard.de"):
            name = url.path.rsplit("/", 1)[-1]
            parts = url.path.strip("/").split("/")  # app/chart_data/<filter>/<region>/<file>
            filter_id, region = parts[2], parts[3]
            path = FIXTURES / "smard" / (f"index_{filter_id}_{region}_hour.json" if name.startswith("index_") else name)
            if not path.exists():
                return httpx.Response(404, json={"error": "not found"})
            payload = json.loads(path.read_text(encoding="utf-8"))
            if "series" in payload:
                payload = self._mutate_smard(payload, int(filter_id))
            return httpx.Response(200, json=payload)

        if url.host.endswith("open-meteo.com"):
            start = datetime.fromisoformat(url.params["start_date"])
            end = datetime.fromisoformat(url.params["end_date"])
            data = json.loads((FIXTURES / "open_meteo" / "hourly_2026-11-15T23_48h.json").read_text(encoding="utf-8"))
            for loc in data:  # keep only the requested date range, like the real API
                keep = [i for i, t in enumerate(loc["hourly"]["time"]) if start.date() <= datetime.fromisoformat(t).date() <= end.date()]
                loc["hourly"] = {k: [v[i] for i in keep] for k, v in loc["hourly"].items()}
            if _chaos() == "unit_change":
                for loc in data:
                    loc["hourly_units"]["wind_speed_100m"] = "mp/h"
            return httpx.Response(200, json=data)

        return httpx.Response(404, json={"error": f"no fixture for {url}"})

    @staticmethod
    def _mutate_smard(payload: dict, filter_id: int) -> dict:
        chaos = _chaos()
        if chaos == "schema_change" and filter_id == 4169:
            # The provider starts sending objects instead of [timestamp, value] pairs.
            payload["series"] = [{"t": t, "v": v} for t, v in payload["series"]]
        elif chaos == "revision" and filter_id == 4068:
            # The provider corrects a past solar value (hour 12 of the sample, inside the lookback window).
            payload["series"][36][1] = round(payload["series"][36][1] + 250.0, 2)
        elif chaos == "api_outage":
            raise httpx.ConnectError("simulated outage: SMARD did not respond")
        return payload


def client() -> httpx.Client:
    transport = FixtureTransport() if offline() else httpx.HTTPTransport(retries=2)
    return httpx.Client(transport=transport, timeout=30, headers={"User-Agent": "energy-data-platform/0.1"})
