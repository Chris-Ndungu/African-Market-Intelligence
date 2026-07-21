with facts as (
    select * from {{ ref('fact_macroeconomic_indicators') }}
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
    f.indicator_value

from facts f 
left join countries c on f.country_code = c.country_code
left join indicators i on f.indicator_code = i.indicator_code
