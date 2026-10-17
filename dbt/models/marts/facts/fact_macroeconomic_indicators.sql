{{ config(materialized='table') }}

with world_bank as (
    select
        to_hex(md5(concat(
            'WB-', country_code, '-', indicator_code, '-', cast(report_year as string)
        ))) as indicator_key,
        record_id,
        country_code,
        indicator_code,
        report_year,
        report_date,
        indicator_value,
        data_source,
        false as is_forecast,
        loaded_at,
        ingestion_date
    from {{ ref('stg_world_bank__indicators') }}
),

imf as (
    select
        to_hex(md5(concat(
            'IMF-', country_code, '-', indicator_code, '-', cast(report_year as string)
        ))) as indicator_key,
        record_id,
        country_code,
        indicator_code,
        report_year,
        report_date,
        indicator_value,
        data_source,
        is_forecast,
        loaded_at,
        ingestion_date
    from {{ ref('stg_imf__indicators') }}
)

select * from world_bank
union all
select * from imf