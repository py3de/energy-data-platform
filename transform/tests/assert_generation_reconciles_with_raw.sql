-- Reconciliation: for every series, the sum in the fact table must equal the sum in the raw layer
-- over the same hours. A mismatch means rows were lost, duplicated or not refreshed after a revision.
{{ config(severity='error') }}

with raw_totals as (
    select series_id, round(sum(value), 2) as raw_total
    from {{ source('raw', 'smard_series') }}
    where ts_utc in (select hour_utc from {{ ref('fct_energy_hourly') }})
    group by series_id
),

fact_totals as (
    select 'day_ahead_price' as series_id, round(sum(price_eur_mwh), 2) as fact_total from {{ ref('fct_energy_hourly') }}
    union all select 'solar_generation', round(sum(solar_mwh), 2) from {{ ref('fct_energy_hourly') }}
    union all select 'wind_onshore_generation', round(sum(wind_onshore_mwh), 2) from {{ ref('fct_energy_hourly') }}
    union all select 'wind_offshore_generation', round(sum(wind_offshore_mwh), 2) from {{ ref('fct_energy_hourly') }}
    union all select 'grid_load', round(sum(grid_load_mwh), 2) from {{ ref('fct_energy_hourly') }}
)

select r.series_id, r.raw_total, f.fact_total
from raw_totals r
join fact_totals f using (series_id)
where abs(r.raw_total - coalesce(f.fact_total, 0)) > 0.01
