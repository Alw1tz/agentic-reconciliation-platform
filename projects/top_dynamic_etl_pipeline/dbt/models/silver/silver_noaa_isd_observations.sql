{{ config(tags=['noaa_isd']) }}

-- Semantic reshaping on top of BRONZE, not re-deriving scaling/sentinel
-- logic already done there (see sql_generator.py) -- combine the two
-- station-id halves into one identifier, combine date + HHMM time into a
-- real timestamp, and add a plausibility flag from the documented MIN/MAX
-- ranges rather than trusting every parsed row blindly.

with bronze as (
    select * from {{ source('bronze', 'bronze_noaa_isd_observation') }}
)

select
    -- wban_station_id is legitimately NULL for most non-US stations
    -- (BRONZE correctly NULLs the documented "99999 = not applicable"
    -- sentinel) -- that's the common case internationally, not a rare
    -- edge case, and || propagates NULL through the whole concatenation
    -- if left unguarded. COALESCE back to the sentinel string so
    -- station_id stays populated either way.
    usaf_station_id || '-' || coalesce(wban_station_id, '99999') as station_id,

    -- observation_time_hhmm is an integer like 1345 for 13:45 -- split
    -- into hours/minutes via div/mod and add as minutes-since-midnight,
    -- rather than string-concatenating date + time and re-parsing.
    dateadd(
        minute,
        mod(observation_time_hhmm, 100) + floor(observation_time_hhmm / 100) * 60,
        observation_date::timestamp_ntz
    ) as observed_at,

    report_type_code,
    latitude,
    longitude,
    elevation_m,
    wind_direction_angle,
    wind_speed_mps,
    ceiling_height_m,
    visibility_m,
    air_temp_c,
    dew_point_c,
    sea_level_pressure_hpa,

    -- Plausibility flag from the format spec's own documented MIN/MAX,
    -- not a re-derivation of the sentinel-to-NULL logic BRONZE already
    -- applied -- a row can be "present" (non-NULL) and still physically
    -- implausible if something upstream of NOAA's own QC slipped through.
    (
        (latitude is null or latitude between -90 and 90)
        and (longitude is null or longitude between -180 and 180)
        and (elevation_m is null or elevation_m between -400 and 8850)
    ) as is_valid_observation,

    source_file_name,
    load_ts
from bronze
