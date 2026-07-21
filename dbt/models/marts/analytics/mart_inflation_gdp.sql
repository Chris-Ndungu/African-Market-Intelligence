with facts as (
    select * from {{ ref('fact_macroeconomic_indicators') }}
),

countries as (
    select * from {{ ref('dim_country') }}
),

indicators as (
    select * from {{ ref('dim_indicator') }}
)

select
    f.indicator_key,
    f.report_year,
    f.report_date,
    
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

-- Filter strictly for GDP and Inflation codes (World Bank standard codes)
where f.indicator_code in (
    'NY.GDP.MKTP.CD',  -- GDP (current US$)
    'NY.GDP.PCAP.CD',  -- GDP per capita (current US$)
    'FP.CPI.TOTL.ZG'   -- Inflation, consumer prices (annual %)
)
