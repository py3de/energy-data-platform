"""Reproducible failure scenarios ("chaos drills") for the runbook.

Each scenario starts from a fresh, healthy demo warehouse (offline fixtures, separate DB file, so
your real data is never touched), injects one realistic failure, runs the step that should catch it,
and shows what the platform did. docs/RUNBOOK.md explains how to diagnose and fix each one.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from typing import Callable

from energy_platform import db
from energy_platform.config import ROOT


@dataclass(frozen=True)
class Scenario:
    name: str
    what_breaks: str
    expected_detection: str
    runbook_section: str


SCENARIOS: dict[str, Scenario] = {s.name: s for s in [
    Scenario("schema_change", "SMARD sends the price series as objects instead of [timestamp, value] pairs.",
             "Ingestion validation: SchemaDriftError, payload quarantined, only that series fails, watermark unchanged.",
             "1. Source schema change"),
    Scenario("unit_change", "Open-Meteo reports wind speed in mp/h instead of km/h.",
             "Ingestion unit check: SchemaDriftError, payload quarantined, nothing loaded.",
             "2. Unit change"),
    Scenario("api_outage", "SMARD does not respond.",
             "Load runs marked failed, watermarks unchanged; the next successful run catches up automatically.",
             "3. API outage"),
    Scenario("revision", "SMARD corrects a solar value from 12 hours ago.",
             "Lookback window re-reads it: 1 changed row in the run log; the incremental fact table is updated; reconciliation passes.",
             "4. Revised source values"),
    Scenario("negative_value", "A corrupt solar value of -500 MWh lands in the raw layer.",
             "dbt test not_negative fails on fct_energy_hourly; the daily mart is skipped, so the bad value never reaches it.",
             "5. Invalid values"),
    Scenario("missing_hours", "Three hours disappear from the raw layer after the fact table was built.",
             "Reconciliation test: fact table no longer matches raw -> error.",
             "6. Raw and fact out of sync"),
    Scenario("stale_source", "No new SMARD data has arrived for three days.",
             "dbt source freshness: error on raw.smard_series.",
             "7. Stale source"),
]}


def _demo_env() -> None:
    os.environ["EDP_OFFLINE"] = "1"
    os.environ["EDP_NOW"] = "2026-11-17T23:00:00Z"
    os.environ.setdefault("EDP_DEMO_DB", str(ROOT / "data" / "demo.duckdb"))
    os.environ["EDP_DB_PATH"] = os.environ["EDP_DEMO_DB"]
    os.environ["EDP_QUARANTINE_DIR"] = str(ROOT / "data" / "demo_quarantine")


def reset_demo(ingest: Callable, transform: Callable) -> None:
    """Fresh, healthy demo warehouse from fixtures."""
    _demo_env()
    os.environ.pop("EDP_CHAOS", None)
    for path in (os.environ["EDP_DB_PATH"], os.environ["EDP_DB_PATH"] + ".wal"):
        if os.path.exists(path):
            os.remove(path)
    shutil.rmtree(os.environ["EDP_QUARANTINE_DIR"], ignore_errors=True)
    ingest()
    transform([])


def run(name: str, ingest: Callable, transform: Callable, freshness: Callable) -> int:
    """Apply one scenario on a fresh demo warehouse and run the step that should detect it. Returns an exit code."""
    scenario = SCENARIOS[name]
    print(f"\n=== Chaos drill: {name} ===\nBreaks:   {scenario.what_breaks}\nExpected: {scenario.expected_detection}\n")
    print("Preparing a healthy demo warehouse ...")
    reset_demo(ingest, transform)

    if name in ("schema_change", "unit_change", "api_outage", "revision"):
        os.environ["EDP_CHAOS"] = name
        try:
            results = ingest()
        finally:
            os.environ.pop("EDP_CHAOS", None)
        if name == "revision":
            code = transform([])
        else:
            code = 1 if any(r["status"] == "failed" for r in results) else 0
    elif name == "negative_value":
        with db.connect() as con:
            con.execute("update raw.smard_series set value = -500 where series_id = 'solar_generation' and ts_utc = '2026-11-17 11:00:00'")
        code = transform([])
    elif name == "missing_hours":
        with db.connect() as con:
            con.execute("delete from raw.smard_series where ts_utc between '2026-11-17 03:00:00' and '2026-11-17 05:00:00'")
        code = transform([])
    else:  # stale_source
        with db.connect() as con:
            con.execute("update raw.smard_series set _loaded_at = _loaded_at - interval 3 days")
        code = freshness()

    print(f"\nRunbook: docs/RUNBOOK.md -> '{scenario.runbook_section}'.  Inspect with: edp status --demo")
    return code
