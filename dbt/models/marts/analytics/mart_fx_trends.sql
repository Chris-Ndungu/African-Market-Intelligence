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
        ) as daily_change
    from rates
),

-- Rate from one year earlier by calendar date, not by row offset, so missed
-- ingestion days don't shift the comparison. Allows up to a week of gap
-- before the year-ago point; otherwise the YoY change is null.
year_ago as (
    select
        later.record_id,
        earlier.fx_rate_usd as fx_rate_usd_yoy
    from rates as later
    join rates as earlier
        on earlier.country_code = later.country_code
        and earlier.base_currency = later.base_currency
        and earlier.ingestion_date between date_sub(later.ingestion_date, interval 372 day)
            and date_sub(later.ingestion_date, interval 365 day)
    qualify row_number() over (
        partition by later.record_id
        order by earlier.ingestion_date desc
    ) = 1
),

windowed_rates as (
    select
        *,
        avg(fx_rate_usd) over (
            partition by country_code, base_currency
            order by unix_date(ingestion_date)
            range between 29 preceding and current row
        ) as fx_30d_avg,
        -- Rates are captured every calendar day, so annualise with 365, not 252.
        stddev_pop(daily_change) over (
            partition by country_code, base_currency
            order by unix_date(ingestion_date)
            range between 29 preceding and current row
        ) * sqrt(365) * 100 as volatility_score
    from rates_with_change
)

select
    windowed_rates.record_id,
    windowed_rates.country_code,
    windowed_rates.base_currency,
    windowed_rates.ingestion_date,
    windowed_rates.fx_rate_usd,
    windowed_rates.fx_30d_avg,
    safe_divide(
        windowed_rates.fx_rate_usd - year_ago.fx_rate_usd_yoy,
        year_ago.fx_rate_usd_yoy
    ) * 100 as fx_yoy_change_pct,
    windowed_rates.volatility_score,
    windowed_rates.data_source,
    windowed_rates.loaded_at
from windowed_rates
left join year_ago
    on year_ago.record_id = windowed_rates.record_id
