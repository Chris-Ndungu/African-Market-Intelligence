-- models/staging/imf/stg_imf__indicators.sql

with source_data as (
    select * from {{ source('imf_raw', 'raw_imf') }} 
),

renamed_and_casted as (
    select
        -- Primary key
        trim(s.record_id) as record_id, 

        -- Country attributes (reads country_code from raw, aliases to country_id for warehouse consistency)
        trim(s.country_code) as country_id,

        -- Metric attributes
        trim(s.indicator_code) as indicator_code,
        trim(s.indicator_name) as indicator_name,
        trim(s.category) as category,
        trim(s.unit) as metric_unit,       -- Maps 'unit' from raw schema
        coalesce(s.is_forecast, false) as is_forecast,
        trim(s.source) as data_source,

        -- Temporal attributes
        cast(s.year as int64) as report_year,
        parse_date('%Y-%m-%d', concat(cast(s.year as string), '-12-31')) as report_date,

        -- Measure value
        cast(s.value as float64) as indicator_value,
        
        -- Audit / Lineage
        cast(s.loaded_at as timestamp) as loaded_at,
        cast(s.ingestion_date as date) as ingestion_date

    from source_data as s
    where s.year is not null
),

deduplicated as (
    select 
        *,
        row_number() over(
            partition by country_id, indicator_code, report_year
            order by loaded_at desc, ingestion_date desc
        ) as row_num
    from renamed_and_casted
)

select
    record_id,
    country_id,
    indicator_code,
    indicator_name,
    category,
    metric_unit,
    is_forecast,
    data_source,
    report_year,
    report_date,
    indicator_value,
    loaded_at,
    ingestion_date
from deduplicated
where row_num = 1