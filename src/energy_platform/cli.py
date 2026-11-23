"""`edp`: command line for the energy data platform.

    edp run                       ingest (incremental) + dbt build
    edp ingest --since 7d         backfill the last 7 days
    edp transform --full-refresh  rebuild the fact table from scratch
    edp freshness                 dbt source freshness
    edp status                    watermarks, recent load runs, quarantined payloads
    edp chaos list | edp chaos <scenario>   reproducible failure drills (see docs/RUNBOOK.md)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from energy_platform import chaos, db
from energy_platform.config import ROOT, db_path, quarantine_dir
from energy_platform.ingest import ingest as run_ingest
from energy_platform.ingest import parse_since

DBT_DIR = ROOT / "transform"


def _dbt(args: list[str]) -> int:
    from dbt.cli.main import dbtRunner

    os.environ["EDP_DB_PATH"] = str(Path(db_path()).resolve())
    result = dbtRunner().invoke([*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    return 0 if result.success else 1


def _transform(extra: list[str]) -> int:
    return _dbt(["build", *extra])


def _freshness() -> int:
    return _dbt(["source", "freshness"])


def _ingest(sources=("smard", "weather"), since=None) -> list[dict]:
    results = run_ingest(sources, since)
    for r in results:
        mark = "OK  " if r["status"] == "success" else "FAIL"
        detail = f"fetched={r['fetched']} inserted={r['inserted']} changed={r['changed']}" if r["status"] == "success" else r["error"]
        name = f"{r['source']}:{r['key']}"
        print(f"  [{mark}] {name:<36} {detail}")
    return results


def _status() -> None:
    print(f"Warehouse: {db_path()}\n\nWatermarks (newest hour loaded):")
    with db.connect() as con:
        for source, key, wm in con.execute("select source, key, watermark_ts from meta.watermarks order by 1, 2").fetchall():
            print(f"  {source + ':' + key:<36} {wm}")
        print("\nRecent load runs:")
        for row in con.execute("""select started_at, source, status, rows_fetched, rows_inserted, rows_changed, coalesce(error, '')
                                  from meta.load_runs order by started_at desc limit 12""").fetchall():
            print(f"  {row[0]:%Y-%m-%d %H:%M:%S}  {row[1]:<32} {row[2]:<8} fetched={row[3]} inserted={row[4]} changed={row[5]} {row[6][:70]}")
    files = sorted(Path(quarantine_dir()).glob("*.json")) if Path(quarantine_dir()).exists() else []
    print(f"\nQuarantined payloads: {len(files)}")
    for f in files[-5:]:
        print(f"  {f}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="edp", description="Energy data platform")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_ing = sub.add_parser("ingest", help="Incremental load into the raw layer")
    p_ing.add_argument("--source", choices=["smard", "weather", "all"], default="all")
    p_ing.add_argument("--since", help="Backfill start: a date (2026-11-01) or days ago (7d)")
    p_tr = sub.add_parser("transform", help="dbt build (models + tests)")
    p_tr.add_argument("--full-refresh", action="store_true")
    p_run = sub.add_parser("run", help="ingest + transform")
    p_run.add_argument("--since")
    sub.add_parser("freshness", help="dbt source freshness")
    p_st = sub.add_parser("status", help="Watermarks, load runs, quarantine")
    p_st.add_argument("--demo", action="store_true", help="Show the chaos-drill demo warehouse")
    p_ch = sub.add_parser("chaos", help="Run a failure drill on a separate demo warehouse")
    p_ch.add_argument("scenario", choices=["list", *chaos.SCENARIOS])
    args = parser.parse_args(argv)

    if args.cmd == "ingest":
        sources = ("smard", "weather") if args.source == "all" else (args.source,)
        results = _ingest(sources, parse_since(args.since))
        return 1 if any(r["status"] == "failed" for r in results) else 0
    if args.cmd == "transform":
        return _transform(["--full-refresh"] if args.full_refresh else [])
    if args.cmd == "run":
        results = _ingest(since=parse_since(args.since))
        code = _transform([])
        return code or (1 if any(r["status"] == "failed" for r in results) else 0)
    if args.cmd == "freshness":
        return _freshness()
    if args.cmd == "status":
        if args.demo:
            chaos._demo_env()
        _status()
        return 0
    if args.cmd == "chaos":
        if args.scenario == "list":
            for s in chaos.SCENARIOS.values():
                print(f"  {s.name:<15} {s.what_breaks}")
            return 0
        return chaos.run(args.scenario, ingest=lambda: _ingest(), transform=_transform, freshness=_freshness)
    return 0


if __name__ == "__main__":
    sys.exit(main())
