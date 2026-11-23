{{
  config(
    materialized='incremental',
    unique_key='hour_utc',
    incremental_strategy='delete+insert',
    on_schema_change='fail',
  )
}}
-- One row per hour: market, generation, load and weather side by side.
--
-- Incremental with a lookback: each run rebuilds the last 48 hours already in the table plus
-- everything newer. Late or revised source values inside that window replace the old rows
-- (delete+insert on hour_utc), older history is left untouched.

with energy as (
    select * from {{ ref('int_energy__hourly') }}
    {% if is_incremental() %}
    where hour_utc >= (select max(hour_utc) - interval 48 hours from {{ this }})
    {% endif %}
),

weather as (
    select * from {{ ref('int_weather__germany_hourly') }}
)

select
    e.hour_utc,
    timezone('Europe/Berlin', timezone('UTC', e.hour_utc))                 as hour_local,
    e.price_eur_mwh,
    e.solar_mwh,
    e.wind_onshore_mwh,
    e.wind_offshore_mwh,
    e.solar_mwh + e.wind_onshore_mwh + e.wind_offshore_mwh                  as wind_solar_mwh,
    e.grid_load_mwh,
    round((e.solar_mwh + e.wind_onshore_mwh + e.wind_offshore_mwh) / nullif(e.grid_load_mwh, 0), 4)
                                                                            as wind_solar_share,
    w.avg_temperature_c,
    w.avg_wind_speed_100m_kmh,
    w.avg_shortwave_radiation_wm2,
    coalesce(w.weather_locations, 0)                                        as weather_locations,
    (e.price_eur_mwh is not null and e.solar_mwh is not null and e.wind_onshore_mwh is not null
     and e.wind_offshore_mwh is not null and e.grid_load_mwh is not null
     and coalesce(w.weather_locations, 0) = 3)                              as is_complete,
    greatest(e.loaded_at, coalesce(w.loaded_at, e.loaded_at))               as source_loaded_at,
    now()::timestamp                                                        as updated_at
from energy e
left join weather w using (hour_utc)
