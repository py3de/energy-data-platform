-- One row per series and hour. Unpublished hours never reach the raw layer, so no null filtering is needed here.
select
    series_id,
    ts_utc        as hour_utc,
    value,
    _loaded_at    as loaded_at
from {{ source('raw', 'smard_series') }}
