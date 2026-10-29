{% docs __overview__ %}
# African Market Intelligence Pipeline

Daily, tested macroeconomic and FX data for **54 African countries**, modelled
in BigQuery as a star schema with analytics-ready marts and a transparent
0–100 country risk score.

Source code: [github.com/Chris-Ndungu/African-Market-Intelligence](https://github.com/Chris-Ndungu/African-Market-Intelligence)

## Start here

- [`mart_country_profile`](#!/model/model.african_market_intelligence.mart_country_profile) —
  annual indicators per country with the latest FX rate and risk score.
  Open the lineage graph (bottom-right button) to see the whole flow.
- [`mart_fx_trends`](#!/model/model.african_market_intelligence.mart_fx_trends) —
  daily FX rates with 30-day average, year-over-year change and volatility.
- [`mart_inflation_gdp`](#!/model/model.african_market_intelligence.mart_inflation_gdp) —
  annual GDP, GDP per capita and CPI inflation by country.

## Sources

| Source | Data | Cadence |
|---|---|---|
| World Bank API | GDP, population, trade, FDI, debt, credit, mobile subscriptions | Daily ingestion |
| IMF DataMapper | Inflation, growth, current account, government debt and balance, unemployment | Daily ingestion |
| ExchangeRate-API | FX rates vs USD for all 54 countries | Daily ingestion |

Python scripts land unmodified records in the raw dataset. A failed API
fetch fails the run, so a partial load never replaces good data.

## Layers

1. **Staging** — typed, renamed, deduplicated views; every country code is
   normalised to ISO3 against the country seed.
2. **Dimensions** — `dim_country` (all 54 countries) and `dim_indicator`.
3. **Facts** — `fact_macroeconomic_indicators` (World Bank + IMF, annual) and
   `fact_fx_rates` (daily).
4. **Marts** — the three analytics models above.

## Country risk score

An equal-weight average of the available components, each clamped to 0–100:
inflation (0–25%), public debt (30–120% of GDP), unemployment (5–30%) and FX
(depreciation and annualised 30-day volatility, 0–50%). IMF forecast years
are excluded, so the score reflects reported data only. It is a comparative
indicator, not a credit rating.

## Quality

Tests cover uniqueness and nulls on keys, country-code integrity against the
seed, the risk-score range, a 54-country assertion, and source freshness.
Orchestrated daily by Airflow; CI runs Ruff, pytest and `dbt parse` on every
pull request.

_Column data types are not shown: this site is generated without querying
BigQuery._
{% enddocs %}
