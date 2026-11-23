-- Long -> wide: one row per hour with every SMARD series as a column.
select
    hour_utc,
    max(case when series_id = 'day_ahead_price'          then value end) as price_eur_mwh,
    max(case when series_id = 'solar_generation'         then value end) as solar_mwh,
    max(case when series_id = 'wind_onshore_generation'  then value end) as wind_onshore_mwh,
    max(case when series_id = 'wind_offshore_generation' then value end) as wind_offshore_mwh,
    max(case when series_id = 'grid_load'                then value end) as grid_load_mwh,
    max(loaded_at)                                                         as loaded_at
from {{ ref('stg_smard__series') }}
group by hour_utc
