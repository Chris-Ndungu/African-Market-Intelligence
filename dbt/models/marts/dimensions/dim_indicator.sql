{{ config(materialized='table') }}

with indicators as (
    select
        indicator_code,
        indicator_name,
        category,
        metric_unit,
        data_source
    from {{ ref('stg_world_bank__indicators') }}

    union all

    select
        indicator_code,
        indicator_name,
        category,
        metric_unit,
        data_source
    from {{ ref('stg_imf__indicators') }}
)

select
    indicator_code,
    max(indicator_name) as indicator_name,
    max(category) as category,
    max(metric_unit) as metric_unit,
    max(data_source) as data_source
from indicators
where indicator_code is not null
group by indicator_code