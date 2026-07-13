-- models/staging/world_bank/stg_world_bank__indicators.sql
with source_data as (
    select * from {{ source('world_bank_raw', 'raw_world_bank') }} 
),

renamed_and_casted as (
    select
        -- Primary key
        trim(s.record_id) as record_id, 

        -- Country attributes
        trim(s.country_code) as country_code,
        trim(s.country_name) as country_name,

        -- Metric attributes
        trim(s.indicator_key) as indicator_key,
        trim(s.indicator_code) as indicator_code,
        trim(s.indicator_name) as indicator_name,
        trim(s.category) as category,
        trim(s.unit) as metric_unit,
        trim(s.source) as data_source,  -- Explicit s.source fixes the STRUCT issue!

        -- Temporal attributes
        cast(s.year as int64) as report_year,
        parse_date('%Y-%m-%d', concat(cast(s.year as string), '-12-31')) as report_date,

        -- Measure value
        cast(s.value as numeric) as indicator_value,
        
        -- Audit / Lineage
        cast(s.loaded_at as timestamp) as loaded_at,
        cast(s.ingestion_date as date) as ingestion_date

    from source_data as s  -- Added table alias 's'
    where s.year is not null
),

deduplicated as (
    select 
        *,
        row_number() over(
            partition by country_code, indicator_code, report_year
            order by loaded_at desc
        ) as row_num

    from renamed_and_casted
)

select
    indicator_key,
    record_id,
    country_code,
    country_name,
    indicator_code,
    indicator_name,
    category,
    metric_unit,
    data_source,
    report_year,
    report_date,
    indicator_value,
    loaded_at,
    ingestion_date
from deduplicated
where row_num = 1
