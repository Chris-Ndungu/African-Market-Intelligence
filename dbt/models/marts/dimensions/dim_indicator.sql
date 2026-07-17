{{ config(materialized='table') }}

select distinct
    indicator_code, -- Primary Keya
    indicator_name,
    category,
    metric_unit,
    data_source
from {{ ref('stg_world_bank__indicators') }}
where indicator_code is not null