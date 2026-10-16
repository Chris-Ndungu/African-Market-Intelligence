{{ config(materialized = 'table') }}

select
    country_code,
    country_name,
    region,
    sub_region,
    currency_code,
    income_group,
    capital
from {{ ref('dim_country_seed') }}
