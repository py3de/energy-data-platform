# Runbook

What to do when the pipeline fails. Every scenario below can be reproduced safely on a separate
demo warehouse with `edp chaos <scenario>`, so the detection and the fix can be practised.

**First steps for any failure**

1. `edp status` shows the watermarks, the last load runs (with errors) and any quarantined payloads.
2. In Dagster, open the failed run. The step that failed tells you the layer: ingestion asset, dbt model or dbt test.
3. Failed runs also leave an incident record in `data/incidents/` (written by the `record_incident` sensor).
4. dbt keeps the rows that failed a test in the `test_failures` schema, so you can query exactly what was wrong.

| # | Scenario | Detected by | Drill |
|---|---|---|---|
| 1 | Source schema change | Ingestion validation (pydantic) | `edp chaos schema_change` |
| 2 | Unit change | Ingestion unit check | `edp chaos unit_change` |
| 3 | API outage | Load-run log, Dagster retries | `edp chaos api_outage` |
| 4 | Revised source values | Lookback window, run log `changed` | `edp chaos revision` |
| 5 | Invalid values | dbt `not_negative` / `value_between` | `edp chaos negative_value` |
| 6 | Raw and fact out of sync | dbt reconciliation test | `edp chaos missing_hours` |
| 7 | Stale source | dbt source freshness, Dagster freshness check | `edp chaos stale_source` |

---

## 1. Source schema change

**Symptom:** a `smard:<series>` load run fails with `SchemaDriftError: ... unexpected response format`.
Other series keep loading; Dagster does **not** retry (retrying a changed format only repeats the failure).

**Why it's safe:** the response is validated before anything is written. The bad payload is saved to
`data/quarantine/`, the transaction is not started, and the watermark stays where it was.

**Diagnose:**
- Open the quarantined JSON. It contains the reason and the full payload.
- Compare it with the expected shape in `sources/smard.py` (`SmardChunk`: `series` is a list of `[ms, value]`).

**Fix:** update the parser and its pydantic model for the new format, add the new payload as a fixture
and a test, then run `edp ingest --source smard`. Because the watermark didn't move, the run
automatically catches up on everything it missed.

## 2. Unit change

**Symptom:** `open_meteo:all` fails with `unexpected units {...}`.

**Why it matters:** a switch from km/h to mph would not break anything technically. It would quietly
make every wind number wrong by a factor of 1.6. That's why units are part of the contract.

**Fix:** if the change is intended, convert at ingestion (never in reports) and update `WEATHER_UNITS`.
If it isn't, pin the unit explicitly in the request (Open-Meteo supports `wind_speed_unit=kmh`).

## 3. API outage

**Symptom:** load runs fail with `ConnectError` / timeouts. Dagster retries the ingestion asset
3 times with exponential backoff.

**What happens to the data:** nothing is lost. Watermarks only move on success, so the next successful
run requests the full gap. The `raw` freshness checks turn yellow if the outage lasts long.

**Fix:** usually none. Wait and re-run (`edp run`). For a long outage, check the provider's status, and
if needed backfill explicitly: `edp ingest --since 2026-11-10`.

## 4. Revised source values

**Symptom (not a failure):** a load run shows `changed > 0`.

**Why:** SMARD corrects recent values. Every run re-reads a 48-hour lookback window
(`EDP_LOOKBACK_HOURS`), the upsert detects values that differ, and `fct_energy_hourly` rebuilds its
last 48 hours on every run (`delete+insert` on `hour_utc`). The reconciliation test proves raw and fact agree.

**When to act:** if revisions happen further back than the lookback, raise `EDP_LOOKBACK_HOURS` or run
`edp ingest --since <date>` followed by `edp transform --full-refresh`.

## 5. Invalid values

**Symptom:** `dbt build` fails on `not_negative_fct_energy_hourly_solar_mwh` (or `value_between` for prices
outside the EPEX limits of -500 to 4000 EUR/MWh). Downstream models are **skipped**, so bad data
doesn't reach `mart_daily_energy`.

**Diagnose:**
```sql
select * from test_failures.not_negative_fct_energy_hourly_solar_mwh;   -- the failing rows
select * from meta.load_runs where batch_id = '<_batch_id of that row>'; -- which load brought it in
```

**Fix:** if the source itself is wrong, re-fetch the period (`edp ingest --since ...`). If the source
publishes it that way, decide on a rule (null it, clip it, or flag it) and implement it in staging, with a test.

## 6. Raw and fact out of sync

**Symptom:** `assert_generation_reconciles_with_raw` fails: per-series totals in the fact table differ
from the raw layer over the same hours.

**Typical causes:** raw rows were deleted or rewritten outside the pipeline, or a change happened further
back than the fact table's 48-hour incremental window.

**Fix:** restore the raw data (`edp ingest --since <date>`), then rebuild the fact table from scratch:
`edp transform --full-refresh`. Reconciliation must pass afterwards.

## 7. Stale source

**Symptom:** `edp freshness` reports `ERROR STALE` for `raw.smard_series` (no new load for more than 50 hours),
or the Dagster `smard_freshness` check warns.

**Diagnose:** `edp status`. Are the runs failing (go to 1–3), or succeeding but fetching 0 rows? The latter means
the provider stopped publishing. Check smard.de.

**Fix:** depends on the cause. Freshness thresholds are in `transform/models/sources.yml`.
