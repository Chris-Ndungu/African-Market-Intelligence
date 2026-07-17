{{ config(materialized='table') }}

select
    indicator_key, -- Primary key for fact table
    record_id,     -- ineage pointer back to raw ingestion
    country_code,  --Foreign key to dim_countries
    indicator_code,  -- Foreign key to dim_indicators
    report_year,
    report_date,
    indicator_value
from {{ ref('stg_world_bank__indicators') }}