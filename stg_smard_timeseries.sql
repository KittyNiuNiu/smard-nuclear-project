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
        timestamp_millis(timestamp_ms)                  as ts_utc,

        -- Date in Berlin local time (CET/CEST).
        -- SMARD timestamps are nominally UTC but the data represents
        -- German local hours — convert so date aggregations make sense.
        date(timestamp_millis(timestamp_ms), 'Europe/Berlin') as date_berlin

    from {{ source('smard_raw', 'stg_smard_timeseries_raw') }}
    where value is not null   -- SMARD emits nulls for hours with no data
),

-- Deduplicate: if the same (filter_id, region, timestamp_ms) was loaded multiple
-- times (e.g. backfill + incremental overlap), keep the latest fetch.
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
    extract(year  from ts_utc)      as year,
    extract(month from ts_utc)      as month,
    extract(hour  from ts_utc)      as hour_of_day,

    -- MWh value (generation / consumption unit in SMARD is MWh per hour = average MW)
    value                           as mwh,

    ingestion_date,
    fetched_at
from deduped
