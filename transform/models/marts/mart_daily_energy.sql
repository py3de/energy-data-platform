-- One row per German delivery day (Europe/Berlin), the grain energy traders and analysts use.
select
    cast(hour_local as date)                                   as delivery_day,
    count(*)                                                   as hours,
    count(*) filter (where is_complete)                        as complete_hours,
    round(avg(price_eur_mwh), 2)                               as avg_price_eur_mwh,
    min(price_eur_mwh)                                         as min_price_eur_mwh,
    max(price_eur_mwh)                                         as max_price_eur_mwh,
    count(*) filter (where price_eur_mwh < 0)                  as negative_price_hours,
    round(sum(solar_mwh), 0)                                   as solar_mwh,
    round(sum(wind_onshore_mwh + wind_offshore_mwh), 0)        as wind_mwh,
    round(sum(grid_load_mwh), 0)                               as grid_load_mwh,
    round(sum(wind_solar_mwh) / nullif(sum(grid_load_mwh), 0), 4) as wind_solar_share,
    round(avg(avg_wind_speed_100m_kmh), 1)                     as avg_wind_speed_100m_kmh,
    round(corr(price_eur_mwh, wind_solar_share), 3)            as price_vs_wind_solar_corr
from {{ ref('fct_energy_hourly') }}
group by 1
