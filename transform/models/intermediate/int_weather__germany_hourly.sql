-- A simple national weather signal: the average over the north, south and centre locations.
select
    hour_utc,
    avg(temperature_c)            as avg_temperature_c,
    avg(wind_speed_100m_kmh)      as avg_wind_speed_100m_kmh,
    avg(shortwave_radiation_wm2)  as avg_shortwave_radiation_wm2,
    count(*)                      as weather_locations,
    max(loaded_at)                as loaded_at
from {{ ref('stg_open_meteo__weather') }}
group by hour_utc
