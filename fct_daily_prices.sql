{{
  config(
    materialized = 'table',
    description  = '''
      Daily average wholesale electricity prices for Germany and France.
      Also computes the DE-FR price spread per day.
      Primary model for Looker Studio Tile 2: time series with annotation
      at the April 15 2023 nuclear phase-out date.
    '''
  )
}}

with daily_prices as (
    select
        date,
        year,
        month,
        country,
        phaseout_period,
        avg(eur_per_mwh)    as avg_price_eur_per_mwh,
        min(eur_per_mwh)    as min_price_eur_per_mwh,
        max(eur_per_mwh)    as max_price_eur_per_mwh,

        -- Count negative price hours (interesting signal post phase-out)
        countif(eur_per_mwh < 0)    as negative_price_hours,
        count(*)                    as total_price_hours
    from {{ ref('fct_hourly_prices') }}
    group by 1, 2, 3, 4, 5
),

-- Pivot to one row per date so we can compute the spread
pivoted as (
    select
        date,
        year,
        month,
        phaseout_period,
        max(case when country = 'Germany' then avg_price_eur_per_mwh end)     as de_avg_price,
        max(case when country = 'France'  then avg_price_eur_per_mwh end)     as fr_avg_price,
        max(case when country = 'Germany' then negative_price_hours end)      as de_negative_hours,
        max(case when country = 'France'  then negative_price_hours end)      as fr_negative_hours
    from daily_prices
    group by 1, 2, 3, 4
)

select
    date,
    year,
    month,
    phaseout_period,
    round(de_avg_price, 2)                                      as de_avg_price_eur_mwh,
    round(fr_avg_price, 2)                                      as fr_avg_price_eur_mwh,
    -- Positive spread = Germany more expensive than France
    round(de_avg_price - fr_avg_price, 2)                       as de_fr_price_spread,
    de_negative_hours,
    fr_negative_hours,

    -- Rolling 30-day average to smooth the daily noise in Looker Studio
    round(avg(de_avg_price) over (order by date rows between 29 preceding and current row), 2)
        as de_price_30d_avg,
    round(avg(fr_avg_price) over (order by date rows between 29 preceding and current row), 2)
        as fr_price_30d_avg

from pivoted
order by date
