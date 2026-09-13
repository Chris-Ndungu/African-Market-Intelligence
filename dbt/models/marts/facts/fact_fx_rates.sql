{{ config(materialized='table') }}

select
    record_id,
    country_id as country_code,
    base_currency,
    data_source,
    fx_rate_usd,
    loaded_at,
    ingestion_date
from {{ ref('stg_fx_rates__daily_rates') }}