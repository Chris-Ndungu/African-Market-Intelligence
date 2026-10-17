with rates as (
    select * from {{ ref('fact_fx_rates') }}
),

rates_with_change as (
    select
        *,
        safe_divide(
            fx_rate_usd - lag(fx_rate_usd) over (
                partition by country_code, base_currency
                order by ingestion_date
            ),
            lag(fx_rate_usd) over (
                partition by country_code, base_currency
                order by ingestion_date
            )
        ) as daily_change,
        lag(fx_rate_usd, 365) over (
            partition by country_code, base_currency
            order by ingestion_date
        ) as fx_rate_usd_yoy
    from rates
),

windowed_rates as (
    select
        *,
        avg(fx_rate_usd) over (
            partition by country_code, base_currency
            order by unix_date(ingestion_date)
            range between 29 preceding and current row
        ) as fx_30d_avg,
        stddev_pop(daily_change) over (
            partition by country_code, base_currency
            order by unix_date(ingestion_date)
            range between 29 preceding and current row
        ) * sqrt(252) * 100 as volatility_score
    from rates_with_change
)

select
    record_id,
    country_code,
    base_currency,
    ingestion_date,
    fx_rate_usd,
    fx_30d_avg,
    safe_divide(fx_rate_usd - fx_rate_usd_yoy, fx_rate_usd_yoy) * 100 as fx_yoy_change_pct,
    volatility_score,
    data_source,
    loaded_at
from windowed_rates