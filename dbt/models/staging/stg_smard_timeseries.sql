{{
  config(
    materialized = 'view',
    description  = 'Cleaned SMARD timeseries. Converts Unix-ms timestamps to proper datetime/date types, deduplicates, and excludes NULL values.'
  )
}}

with raw as (
    select
        filter_id,
        region,
        resolution,
        timestamp_ms,
        value,
        ingestion_date,
        fetched_at,

        -- Convert Unix milliseconds → UTC timestamp
        timestamp_millis(timestamp_ms)                                      as ts_utc,

        -- Date in Berlin local time (CET/CEST)
        date(timestamp_millis(timestamp_ms), 'Europe/Berlin')               as date_berlin

    from {{ source('smard_raw', 'stg_smard_timeseries_raw') }}
    where value is not null
),

-- Deduplicate: keep latest fetch for same (filter_id, region, timestamp_ms)
deduped as (
    select *
    from raw
    qualify row_number() over (
        partition by filter_id, region, timestamp_ms
        order by fetched_at desc
    ) = 1
)

select
    filter_id,
    region,
    resolution,
    timestamp_ms,
    ts_utc,
    date_berlin                     as date,
    extract(year  from date_berlin) as year,
    extract(month from date_berlin) as month,

    -- MWh value (for day resolution: MWh per day)
    value                           as mwh,

    ingestion_date,
    fetched_at
from deduped
