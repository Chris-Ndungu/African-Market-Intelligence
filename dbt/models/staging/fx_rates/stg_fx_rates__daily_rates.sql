-- Standardize raw country identifiers to ISO3 using the country seed.

with country_seed as (
    select
        upper(trim(country_code)) as iso3_code,
        upper(trim(iso2_code)) as iso2_code
    from {{ ref('dim_country_seed') }}
),

source_data as (
    select * from {{ source('fx_rates_raw', 'raw_fx_rates') }} 
),

renamed_and_casted as (
    select
        -- MD5 surrogate key generated from base currency, country_code and ingestion_date
        to_hex(md5(concat(
            coalesce(trim(base_currency), ''), '-',
            coalesce(trim(country_id), ''), '-',
            cast(ingestion_date as string)
        ))) as record_id,

        -- Primary key
        coalesce(
            country.iso3_code,
            if(length(upper(trim(raw.country_id))) = 3, upper(trim(raw.country_id)), null)
        ) as country_code,

        -- Metric attributes
        trim(base_currency) as base_currency,
        trim(source) as data_source,

        -- Exchange rate metric (USD rate)
        cast(fx_rate_usd as numeric) as fx_rate_usd,
        cast(rate_timestamp as timestamp) as rate_timestamp,

        -- Temporal attributes
        cast(loaded_at as timestamp) as loaded_at,
        cast(ingestion_date as date) as ingestion_date

    from source_data as raw
    left join country_seed as country
        on upper(trim(raw.country_id)) in (country.iso2_code, country.iso3_code)
    where raw.fx_rate_usd is not null
),

deduplicated as (
    select 
        *,
        row_number() over(
            partition by base_currency, country_code, ingestion_date
            order by loaded_at desc, rate_timestamp desc
        ) as row_num
    from renamed_and_casted
)

select
    record_id,
    country_code,
    base_currency,
    data_source,
    fx_rate_usd,
    rate_timestamp,
    loaded_at,
    ingestion_date
from deduplicated
where row_num = 1