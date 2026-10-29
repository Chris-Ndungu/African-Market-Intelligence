with facts as (
    select * from {{ ref('fact_macroeconomic_indicators') }}
),

latest_fx as (
    select
        country_code,
        fx_rate_usd,
        ingestion_date,
        loaded_at
    from {{ ref('fact_fx_rates') }}
    qualify row_number() over (
        partition by country_code
        order by ingestion_date desc, loaded_at desc
    ) = 1
),

fx_one_year_ago as (
    select
        latest.country_code,
        prior.fx_rate_usd as fx_rate_usd_year_ago
    from latest_fx as latest
    left join {{ ref('fact_fx_rates') }} as prior
        on prior.country_code = latest.country_code
        and prior.ingestion_date <= date_sub(latest.ingestion_date, interval 365 day)
    qualify row_number() over (
        partition by latest.country_code
        order by prior.ingestion_date desc
    ) = 1
),

fx_daily_changes as (
    select
        country_code,
        ingestion_date,
        safe_divide(
            fx_rate_usd - lag(fx_rate_usd) over (
                partition by country_code
                order by ingestion_date
            ),
            lag(fx_rate_usd) over (
                partition by country_code
                order by ingestion_date
            )
        ) as daily_change
    from {{ ref('fact_fx_rates') }}
),

fx_volatility as (
    select
        latest.country_code,
        -- Rates are captured every calendar day, so annualise with 365, not 252.
        stddev_pop(changes.daily_change) * sqrt(365) * 100
            as annualized_volatility_pct
    from latest_fx as latest
    join fx_daily_changes as changes
        on changes.country_code = latest.country_code
        and changes.ingestion_date >= date_sub(
            latest.ingestion_date,
            interval 30 day
        )
    group by latest.country_code
    having count(changes.daily_change) >= 2
),

fx_scores as (
    select
        latest.country_code,
        least(greatest(
            safe_divide(
                greatest(
                    safe_divide(
                        (latest.fx_rate_usd - prior.fx_rate_usd_year_ago)
                            * 100,
                        prior.fx_rate_usd_year_ago
                    ),
                    0
                ),
                50
            ) * 100,
            0
        ), 100) as depreciation_score,
        least(greatest(
            safe_divide(volatility.annualized_volatility_pct, 50) * 100,
            0
        ), 100) as volatility_score
    from latest_fx as latest
    left join fx_one_year_ago as prior using (country_code)
    left join fx_volatility as volatility using (country_code)
),

risk_indicator_rows as (
    select
        country_code,
        case
            when indicator_code in ('PCPIPCH', 'PCPIEPCH', 'FP.CPI.TOTL.ZG')
                then 'inflation'
            when indicator_code = 'GGXWDG_NGDP'
                then 'debt'
            when indicator_code in ('LUR', 'SL.UEM.TOTL.ZS')
                then 'unemployment'
        end as risk_metric,
        report_year,
        indicator_value,
        indicator_code,
        data_source
    from facts
    -- Score on actuals only; IMF forecast years would otherwise win on report_year.
    where not is_forecast
        and indicator_code in (
        'PCPIPCH',
        'PCPIEPCH',
        'FP.CPI.TOTL.ZG',
        'GGXWDG_NGDP',
        'LUR',
        'SL.UEM.TOTL.ZS'
    )
),

latest_risk_inputs as (
    select *
    from risk_indicator_rows
    qualify row_number() over (
        partition by country_code, risk_metric
        order by
            report_year desc,
            case indicator_code
                when 'PCPIPCH' then 1
                when 'GGXWDG_NGDP' then 1
                when 'LUR' then 1
                else 0
            end desc,
            case when data_source = 'IMF DataMapper' then 1 else 0 end desc
    ) = 1
),

macro_scores as (
    select
        country_code,
        least(greatest(
            safe_divide(
                max(if(risk_metric = 'inflation', indicator_value, null)),
                25
            ) * 100,
            0
        ), 100) as inflation_score,
        least(greatest(
            safe_divide(
                max(if(risk_metric = 'debt', indicator_value, null)) - 30,
                90
            ) * 100,
            0
        ), 100) as debt_score,
        least(greatest(
            safe_divide(
                max(if(risk_metric = 'unemployment', indicator_value, null)) - 5,
                25
            ) * 100,
            0
        ), 100) as unemployment_score
    from latest_risk_inputs
    group by country_code
),

country_scores as (
    select
        country.country_code,
        macro.inflation_score,
        macro.debt_score,
        macro.unemployment_score,
        safe_divide(
            coalesce(fx.depreciation_score, 0) + coalesce(fx.volatility_score, 0),
            if(fx.depreciation_score is not null, 1, 0)
                + if(fx.volatility_score is not null, 1, 0)
        ) as fx_score
    from {{ ref('dim_country') }} as country
    left join macro_scores as macro using (country_code)
    left join fx_scores as fx using (country_code)
),

risk_scores as (
    select
        country_code,
        round(safe_divide(
            coalesce(inflation_score, 0)
                + coalesce(debt_score, 0)
                + coalesce(unemployment_score, 0)
                + coalesce(fx_score, 0),
            if(inflation_score is not null, 1, 0)
                + if(debt_score is not null, 1, 0)
                + if(unemployment_score is not null, 1, 0)
                + if(fx_score is not null, 1, 0)
        ), 1) as risk_score,
        if(inflation_score is not null, 1, 0)
            + if(debt_score is not null, 1, 0)
            + if(unemployment_score is not null, 1, 0)
            + if(fx_score is not null, 1, 0) as risk_component_count
    from country_scores
),

countries as (
    select * from {{ ref('dim_country') }}
),

indicators as (
    select * from {{ ref('dim_indicator') }}
)

select
    f.indicator_key,
    f.report_year,
    c.country_name,
    c.country_code,
    i.indicator_name,
    i.category,
    i.metric_unit,
    f.indicator_value,
    latest_fx.fx_rate_usd as latest_fx_rate_usd,
    risk.risk_score,
    risk.risk_component_count
from facts as f
left join countries as c on f.country_code = c.country_code
left join indicators as i on f.indicator_code = i.indicator_code
left join latest_fx on f.country_code = latest_fx.country_code
left join risk_scores as risk on f.country_code = risk.country_code
