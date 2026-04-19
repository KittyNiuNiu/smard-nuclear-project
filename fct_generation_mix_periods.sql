{{
  config(
    materialized = 'table',
    description  = '''
      Daily average generation (GWh) per energy source, split by before/after
      the April 15 2023 nuclear phase-out. This is the primary model for
      Looker Studio Tile 1: "Before vs After" stacked bar chart.
      Uses the ±12-month symmetric window to isolate seasonal effects.
    '''
  )
}}

with daily_by_source as (
    select
        filter_id,
        energy_source,
        energy_source_de,
        category,
        comparison_window,
        date,
        -- Daily total in GWh (hourly MWh / 1000 * 24 hours implicit from sum)
        sum(mwh) / 1000.0               as gwh_day
    from {{ ref('fct_hourly_generation') }}
    where comparison_window in ('year_before', 'year_after')
    group by 1, 2, 3, 4, 5, 6
),

period_averages as (
    select
        filter_id,
        energy_source,
        energy_source_de,
        category,
        comparison_window,
        avg(gwh_day)                    as avg_gwh_per_day,
        sum(gwh_day)                    as total_gwh_period,
        count(distinct date)            as days_in_period
    from daily_by_source
    group by 1, 2, 3, 4, 5
),

-- Compute percentage share of total generation per period for relative comparison
with_share as (
    select
        *,
        sum(avg_gwh_per_day) over (partition by comparison_window)  as total_avg_gwh_per_day_period,
        100.0 * avg_gwh_per_day
            / nullif(sum(case when category like 'generation%' then avg_gwh_per_day else 0 end)
                     over (partition by comparison_window), 0)      as pct_of_generation
    from period_averages
)

select
    filter_id,
    energy_source,
    energy_source_de,
    category,
    comparison_window,

    -- Human-readable label for dashboard filter
    case comparison_window
        when 'year_before' then 'Apr 2022 – Apr 2023 (Before phase-out)'
        when 'year_after'  then 'Apr 2023 – Apr 2024 (After phase-out)'
    end                             as period_label,

    round(avg_gwh_per_day, 2)       as avg_gwh_per_day,
    round(total_gwh_period, 0)      as total_gwh_period,
    days_in_period,
    round(pct_of_generation, 2)     as pct_of_generation_mix

from with_share
order by comparison_window, category, energy_source
