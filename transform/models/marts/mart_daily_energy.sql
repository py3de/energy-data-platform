-- One row per German delivery day (Europe/Berlin), the grain energy traders and analysts use.

with hourly as (
    select
        *,
        cast(hour_local as date) as delivery_day
    from {{ ref('fct_energy_hourly') }}
),

-- Rank each day's hours by price: the most expensive hour gets rank 1.
-- Hours without a price are left out, so a missing value can never be the "peak".
-- On a tie, the earlier hour wins, so the result is the same on every run.
ranked_prices as (
    select
        delivery_day,
        hour(hour_local) as local_hour,
        row_number() over (
            partition by delivery_day
            order by price_eur_mwh desc, hour_local
        ) as price_rank
    from hourly
    where price_eur_mwh is not null
),

peak_hours as (
    select
        delivery_day,
        local_hour as peak_price_hour
    from ranked_prices
    where price_rank = 1
),

daily as (
    select
        delivery_day,
        count(*)                                                     as hours,
        count(*) filter (where is_complete)                          as complete_hours,
        round(avg(price_eur_mwh), 2)                                 as avg_price_eur_mwh,
        min(price_eur_mwh)                                           as min_price_eur_mwh,
        max(price_eur_mwh)                                           as max_price_eur_mwh,
        count(*) filter (where price_eur_mwh < 0)                    as negative_price_hours,
        round(sum(solar_mwh), 0)                                     as solar_mwh,
        round(sum(wind_onshore_mwh + wind_offshore_mwh), 0)          as wind_mwh,
        round(sum(grid_load_mwh), 0)                                 as grid_load_mwh,
        round(sum(wind_solar_mwh) / nullif(sum(grid_load_mwh), 0), 4) as wind_solar_share,
        round(avg(avg_wind_speed_100m_kmh), 1)                       as avg_wind_speed_100m_kmh,
        round(corr(price_eur_mwh, wind_solar_share), 3)              as price_vs_wind_solar_corr
    from hourly
    group by delivery_day
)

select
    d.*,
    p.peak_price_hour
from daily d
left join peak_hours p using (delivery_day)