-- models/staging/imf/stg_imf__indicators.sql

with source_data as (
    select * from {{ source('fx_rates_raw', 'raw_fx_rates') }} 
),

renamed_and_casted as (
    select
        -- MD5 surrogate key generated from base currency, country_code and ingestion_date
        to_hex(md5(concat(
            coalesce(trim(base_currency), ''), '-',
            coalesce(trim(country_code), ''), '-',
            cast(ingestion_date as string)
        ))) as record_id,

        -- Primary key
        trim(country_id) as country_id, 

        -- Country attributes (reads country_code from raw, aliases to country_id for warehouse consistency)
        trim(country_code) as country_id,

        -- Metric attributes
        trim(base_currency) as base_currency,
        trim(source) as data_source,

        -- Exchange rate metric (USD rate)
        cast(fx_rate_usd as numeric) as fx_rate_usd

        -- Temporal attributes
        cast(loaded_at as timestamp) as loaded_at,
        cast(ingestion_date as date) as ingestion_date

    from source_data
    where fx_rate_usd is not null
),

deduplicated as (
    select 
        *,
        row_number() over(
            partition by base_currency, country_code, rate_date
            order by loaded_at desc, ingestion_date desc
        ) as row_num
    from renamed_and_casted
)

select
    record_id,
    country_id,
    country_code,
    base_currency,
    source,
    fx_rate_usd,
    loaded_at,
    ingestion_date
from deduplicated
where row_num = 1