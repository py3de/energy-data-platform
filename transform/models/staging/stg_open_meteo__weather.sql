select
    location,
    ts_utc               as hour_utc,
    temperature_2m       as temperature_c,
    wind_speed_100m      as wind_speed_100m_kmh,
    shortwave_radiation as shortwave_radiation_wm2,
    _loaded_at           as loaded_at
from {{ source('raw', 'weather_hourly') }}
