{{
  config(
    materialized           = 'table',
    partition_by           = {'field': 'date', 'data_type': 'date', 'granularity': 'day'},
    cluster_by             = ['region'],
    description            = '''
      Daily day-ahead wholesale prices for Germany/Luxembourg and France.
      Partitioned by date, clustered by region.
    '''
  )
}}

select
    ts.filter_id,
    df.filter_name_en                   as price_area,
    case ts.filter_id
        when 4169 then 'Germany'
        when 254 then 'France'
    end                                 as country,
    ts.date,
    ts.year,
    ts.month,
    ts.mwh                              as eur_per_mwh,

    case
        when ts.date < cast('{{ var("nuclear_phaseout_date") }}' as date)
        then 'before'
        else 'after'
    end                                 as phaseout_period

from {{ ref('stg_smard_timeseries') }}  as ts
left join {{ ref('dim_filters') }}      as df
    using (filter_id)

where
    ts.filter_id in (4169, 254)
    and ts.date between
        cast('{{ var("analysis_start_date") }}' as date)
        and cast('{{ var("analysis_end_date") }}' as date)
