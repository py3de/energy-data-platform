-- Completeness: no hour may be missing between the first and last hour in the fact table.
-- A gap usually means a source published late or a load failed; warn so it is visible but not blocking.
{{ config(severity='warn') }}

with bounds as (
    select min(hour_utc) as first_hour, max(hour_utc) as last_hour from {{ ref('fct_energy_hourly') }}
),
expected as (
    select unnest(generate_series(first_hour, last_hour, interval 1 hour)) as hour_utc from bounds
)
select e.hour_utc as missing_hour
from expected e
anti join {{ ref('fct_energy_hourly') }} f using (hour_utc)
