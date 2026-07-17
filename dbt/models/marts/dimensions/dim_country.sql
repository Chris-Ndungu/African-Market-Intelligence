{{ config(materialized = 'table') }}
select distinct
    country_code, -- Primary Key
    country_name
from {{ ref('stg_world_bank__indicators') }}
