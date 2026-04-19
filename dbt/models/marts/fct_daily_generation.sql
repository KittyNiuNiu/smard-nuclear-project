{{
  config(
    materialized           = 'table',
    partition_by           = {'field': 'date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by             = ['filter_id'],
    description            = '''
      Daily electricity generation (and consumption) for Germany.
      Partitioned by date (DAY) — dashboard queries filter on date ranges.
      Clustered by filter_id — dashboard groups by energy source.
    '''
  )
}}

select
    ts.filter_id,
    df.filter_name_en                   as energy_source,
    df.filter_name_de                   as energy_source_de,
    df.category,
    ts.region,
    ts.date,
    ts.year,
    ts.month,
    ts.mwh,

    -- Flag for before/after nuclear phase-out (April 15, 2023)
    case
        when ts.date < cast('{{ var("nuclear_phaseout_date") }}' as date)
        then 'before'
        else 'after'
    end                                 as phaseout_period,

    -- 12-month symmetric comparison window
    case
        when ts.date between
            date_sub(cast('{{ var("nuclear_phaseout_date") }}' as date), interval 365 day)
            and date_sub(cast('{{ var("nuclear_phaseout_date") }}' as date), interval 1 day)
        then 'year_before'
        when ts.date between
            cast('{{ var("nuclear_phaseout_date") }}' as date)
            and date_add(cast('{{ var("nuclear_phaseout_date") }}' as date), interval 364 day)
        then 'year_after'
        else 'outside_comparison_window'
    end                                 as comparison_window

from {{ ref('stg_smard_timeseries') }}  as ts
left join {{ ref('dim_filters') }}      as df
    using (filter_id)

where
    ts.filter_id in (
        1223, 1224, 1225, 1226, 1227, 1228,
        4066, 4067, 4068, 4069, 4070, 4071,
        410
    )
    and ts.region = 'DE'
    and ts.date between
        cast('{{ var("analysis_start_date") }}' as date)
        and cast('{{ var("analysis_end_date") }}' as date)