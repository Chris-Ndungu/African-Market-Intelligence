with facts as (
    select * from {{ ref('fact_macroeconomic_indicators') }}
),

latest_fx as (
    select
        country_code,
        fx_rate_usd as latest_fx_rate_usd
    from {{ ref('fact_fx_rates') }}
    qualify row_number() over (
        partition by country_code
        order by ingestion_date desc, loaded_at desc
    ) = 1
),

countries as (
    select * from {{ref('dim_country')}}
), 

indicators as (
    select * from {{ref('dim_indicator')}}
)

select
    f.indicator_key,
    f.report_year,

    -- Country Context
    c.country_name,
    c.country_code,
    
    -- Indicator Context
    i.indicator_name,
    i.category,
    i.metric_unit,
    
    -- Metrics
    f.indicator_value,
    fx.latest_fx_rate_usd

from facts f 
left join countries c on f.country_code = c.country_code
left join indicators i on f.indicator_code = i.indicator_code
left join latest_fx fx on f.country_code = fx.country_code
