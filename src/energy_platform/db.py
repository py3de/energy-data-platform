"""DuckDB warehouse: raw tables, watermarks and the load-run log.

Raw tables have primary keys, so re-loading the same hour can never create duplicates:
loads are idempotent upserts. Every load is recorded in meta.load_runs with what it fetched,
inserted and changed, which is what you look at first when something goes wrong.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import duckdb

from energy_platform.config import db_path

DDL = """
create schema if not exists raw;
create schema if not exists meta;

create table if not exists raw.smard_series (
    series_id   varchar   not null,
    filter_id   integer   not null,
    region      varchar   not null,
    ts_utc      timestamp not null,
    value       double,
    _loaded_at  timestamp not null,
    _batch_id   varchar   not null,
    primary key (series_id, ts_utc)
);

create table if not exists raw.weather_hourly (
    location            varchar   not null,
    latitude            double    not null,
    longitude           double    not null,
    ts_utc              timestamp not null,
    temperature_2m      double,
    wind_speed_100m     double,
    shortwave_radiation double,
    _loaded_at          timestamp not null,
    _batch_id           varchar   not null,
    primary key (location, ts_utc)
);

create table if not exists meta.watermarks (
    source       varchar   not null,
    key          varchar   not null,
    watermark_ts timestamp not null,
    updated_at   timestamp not null,
    primary key (source, key)
);

create table if not exists meta.load_runs (
    batch_id      varchar   primary key,
    source        varchar   not null,
    started_at    timestamp not null,
    finished_at   timestamp,
    status        varchar   not null,      -- running | success | failed
    window_start  timestamp,
    window_end    timestamp,
    rows_fetched  integer,
    rows_inserted integer,
    rows_changed  integer,
    error         varchar
);
"""


@contextmanager
def connect(path: Path | None = None):
    path = Path(path or db_path())
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(path))
    try:
        con.execute(DDL)
        yield con
    finally:
        con.close()


def get_watermark(con, source: str, key: str) -> datetime | None:
    row = con.execute("select watermark_ts from meta.watermarks where source = ? and key = ?", [source, key]).fetchone()
    return row[0] if row else None


def set_watermark(con, source: str, key: str, ts: datetime) -> None:
    con.execute(
        """insert into meta.watermarks values (?, ?, ?, now())
           on conflict (source, key) do update set watermark_ts = excluded.watermark_ts, updated_at = excluded.updated_at""",
        [source, key, ts],
    )


def start_run(con, batch_id: str, source: str, window_start: datetime, window_end: datetime) -> None:
    con.execute(
        "insert into meta.load_runs (batch_id, source, started_at, status, window_start, window_end) values (?, ?, now(), 'running', ?, ?)",
        [batch_id, source, window_start, window_end],
    )


def finish_run(con, batch_id: str, status: str, fetched: int = 0, inserted: int = 0, changed: int = 0, error: str | None = None) -> None:
    con.execute(
        """update meta.load_runs set finished_at = now(), status = ?, rows_fetched = ?, rows_inserted = ?,
           rows_changed = ?, error = ? where batch_id = ?""",
        [status, fetched, inserted, changed, error, batch_id],
    )


def upsert(con, table: str, key_cols: list[str], value_cols: list[str], rows: list[tuple]) -> tuple[int, int]:
    """Idempotent upsert via a staging table. Returns (inserted, changed).

    'changed' counts existing rows whose values differ, e.g. a source revising a past hour.
    Unchanged rows are left untouched, so their _loaded_at keeps telling when they last changed.
    """
    if not rows:
        return 0, 0
    cols = key_cols + value_cols + ["_loaded_at", "_batch_id"]
    con.execute(f"create or replace temp table _stage as select {', '.join(cols)} from {table} limit 0")
    con.executemany(f"insert into _stage values ({', '.join('?' * len(cols))})", rows)
    on = " and ".join(f"t.{k} = s.{k}" for k in key_cols)
    differs = " or ".join(f"t.{c} is distinct from s.{c}" for c in value_cols)
    inserted = con.execute(f"select count(*) from _stage s left join {table} t on {on} where t.{key_cols[0]} is null").fetchone()[0]
    changed = con.execute(f"select count(*) from _stage s join {table} t on {on} where {differs}").fetchone()[0]
    con.execute(f"delete from {table} t using _stage s where {on} and ({differs})")
    con.execute(f"insert into {table} ({', '.join(cols)}) select s.* from _stage s anti join {table} t on {on}")
    con.execute("drop table _stage")
    return inserted, changed
