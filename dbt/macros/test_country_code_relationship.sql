{% test country_code_relationship(model, column_name) %}

with country_codes as (
    select
        upper(trim(country_code)) as iso3_code,
        upper(trim(iso2_code)) as iso2_code
    from {{ ref('dim_country') }}
),
records as (
    select {{ adapter.quote(column_name) }} as country_code
    from {{ model }}
)

select records.country_code
from records
left join country_codes
    on upper(trim(cast(records.country_code as string))) = country_codes.iso3_code
    or upper(trim(cast(records.country_code as string))) = country_codes.iso2_code
where records.country_code is not null
    and country_codes.iso3_code is null

{% endtest %}
