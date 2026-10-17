{{
    config(
        materialized='table',
        partition_by={'field': 'ingestion_date', 'data_type': 'date'},
        cluster_by=['country_code']
    )
}}

select
    record_id,
    country_code,
    base_currency,
    data_source,
    fx_rate_usd,
    rate_timestamp,
    loaded_at,
    ingestion_date
from {{ ref('stg_fx_rates__daily_rates') }}