{{ config(tags=['noaa_isd']) }}

-- Daily station rollup -- the canonical "what a BI consumer actually
-- wants fast reads on" shape for this data: one row per station per day.
-- A table, not a view -- expensive to recompute over the full dataset,
-- cheap to store, and this is exactly the kind of aggregate that doesn't
-- need to be recomputed on every read.

select
    station_id,
    cast(observed_at as date) as observation_date,
    count(*) as observation_count,
    avg(air_temp_c) as avg_temp_c,
    min(air_temp_c) as min_temp_c,
    max(air_temp_c) as max_temp_c,
    avg(dew_point_c) as avg_dew_point_c,
    avg(wind_speed_mps) as avg_wind_speed_mps,
    max(wind_speed_mps) as max_wind_speed_mps,
    avg(sea_level_pressure_hpa) as avg_sea_level_pressure_hpa
from {{ ref('silver_noaa_isd_observations') }}
where is_valid_observation
group by station_id, cast(observed_at as date)
