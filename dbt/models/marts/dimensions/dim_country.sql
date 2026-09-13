{{ config(materialized = 'table') }}

with country_ids as (
    select country_code from {{ ref('stg_world_bank__indicators') }}
    union distinct
    select country_id as country_code from {{ ref('stg_imf__indicators') }}
    union distinct
    select country_id as country_code from {{ ref('stg_fx_rates__daily_rates') }}
),

country_names as (
    select
        country_code,
        max(country_name) as country_name
    from {{ ref('stg_world_bank__indicators') }}
    group by country_code
)

select
    ids.country_code,
    names.country_name
from country_ids as ids
left join country_names as names using (country_code)
