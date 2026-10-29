# 🌍 African Market Intelligence Pipeline

[![CI](https://github.com/Chris-Ndungu/African-Market-Intelligence/actions/workflows/ci.yml/badge.svg)](https://github.com/Chris-Ndungu/African-Market-Intelligence/actions/workflows/ci.yml)
[![dbt docs](https://github.com/Chris-Ndungu/African-Market-Intelligence/actions/workflows/dbt-docs.yml/badge.svg)](https://chris-ndungu.github.io/African-Market-Intelligence/)

> **Automated macroeconomic data pipeline covering 54 African countries** — from raw API ingestion to query-optimised BigQuery analytics models, refreshed daily.

**[Explore the live dbt docs and lineage graph →](https://chris-ndungu.github.io/African-Market-Intelligence/#!/model/model.african_market_intelligence.mart_country_profile?g_v=1)**

Built to demonstrate production-style data engineering: batch ingestion, dbt modelling, orchestration, and data quality — on a real-world African macroeconomic dataset.

---

## 🧩 The Problem

Organisations operating in African markets — investors, development finance institutions, consultancies, and multinationals — face a persistent problem: macroeconomic data is **fragmented, inconsistently formatted, and manually intensive to gather**.

| Pain Point | Current State | Impact |
|---|---|---|
| Fragmented data sources | World Bank, IMF, and national statistics portals each use different formats and access methods | Analysts spend 3–5 hours per country gathering baseline data |
| No single source of truth | Exchange rate data in one spreadsheet, GDP in another, CPI in a third | Inconsistent figures across reports, version control failures |
| Manual refresh cycles | Data updated manually when someone remembers — weekly at best | Decisions made on stale figures, no alerting when data goes out of date |
| No data quality controls | No automated checks for missing values, outliers, or schema changes | Silent data quality failures go undetected until they surface in a report |

This pipeline closes that gap.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│   World Bank API  ·  IMF Data API  ·  ExchangeRate-API           │
└────────────────────────┬─────────────────────────────────────────┘
                         │ Daily ingestion
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│         Python scripts  (world_bank.py · imf.py · fx_rates.py)   │
│         HTTP fetch → schema validation → BigQuery write          │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│              BIGQUERY — RAW DATASET                              │
│   raw_world_bank  ·  raw_imf  ·  raw_fx_rates                    │
│   Unmodified source records — landing zone only                  │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                  DBT TRANSFORMATION LAYER                        │
│                                                                  │
│   Staging      stg_world_bank · stg_imf · stg_fx_rates           │
│       ↓        (type casting, renaming, deduplication)           │
│   Dimensions   dim_country · dim_indicator                       │
│       ↓                                                          │
│   Facts        fact_macroeconomic_indicators · fact_fx_rates     │
│       ↓                                                          │
│   Marts        mart_country_profile                              │
│                mart_fx_trends                                    │
│                mart_inflation_gdp                                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│              BIGQUERY — ANALYTICS DATASET                        │
│   Query-optimised · tested · documented · dashboard-ready        │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION                                 │
│   Airflow DAG — daily at 02:00 UTC · retry logic · alerting      │
└──────────────────────────────────────────────────────────────────┘
```

---

## ⚙️ Tech Stack

| Layer | Tool | Why |
|---|---|---|
| Ingestion | **Python 3.10+** | HTTP clients, schema validation, GCP auth |
| Data Warehouse | **Google BigQuery** | Scalable, serverless, native GCP — free tier sufficient |
| Transformation | **dbt Core 1.8+** (Docker image pins 1.11) | Version-controlled SQL, built-in testing, lineage docs |
| BQ Adapter | **dbt-bigquery** | Official BigQuery adapter for dbt |
| Orchestration | **Apache Airflow 3.x** | Industry-standard, retry logic, task dependencies |
| Containerisation | **Docker / Compose** | Local Airflow, reproducible environment |
| Visualisation | **Looker Studio** (planned) | Native BigQuery connector, zero hosting cost |

---

## 📊 Key Metrics This Pipeline Produces

- **Annual macro indicators** per country from World Bank and IMF, refreshed daily
- **FX trends** — 30-day rate average, YoY rate change, and annualized volatility
- **Country risk score** — 0–100 composite of available macro and FX indicators
- **Financial access proxies** — private credit/GDP and mobile cellular subscriptions
- **Data freshness** — raw source thresholds are checked by the Airflow DAG

---

## 🗂️ Data Model

### BigQuery Datasets

| Dataset | Purpose | Refresh |
|---|---|---|
| `raw_african_macro` | Unmodified API records — landing zone only; tables are created by the ingestion scripts | Daily ingestion |
| `dbt_african_macro` | dbt staging views plus dimension, fact, and mart tables (set in `dbt/profiles.yml`) | Daily dbt run |

### Fact Table — `fact_macroeconomic_indicators`

| Column | Type | Key | Description |
|---|---|---|---|
| `indicator_key` | STRING | PK | Stable source-prefixed key for an annual country indicator |
| `country_code` | STRING | FK | Canonical ISO3 country code |
| `indicator_code` | STRING | | Original World Bank or IMF indicator code |
| `report_year` | INT64 | | Observation year |
| `indicator_value` | NUMERIC/FLOAT64 | | Source indicator value |
| `data_source` | STRING | | World Bank or IMF |
| `loaded_at` | TIMESTAMP | | Ingestion timestamp |

### Dimension Tables

```
dim_country                 dim_indicator
├── country_code PK (ISO3)  ├── indicator_code PK
├── iso2_code               ├── indicator_name
├── country_name            ├── category
├── region / sub_region     ├── metric_unit
├── currency_code           └── data_source
├── income_group
└── capital
```

### Mart Tables

| Mart | Description | Key Columns |
|---|---|---|
| `mart_country_profile` | Annual country/indicator records enriched with latest FX and risk score | `indicator_key`, `country_code`, `report_year`, `indicator_value`, `latest_fx_rate_usd`, `risk_score`, `risk_component_count` |
| `mart_fx_trends` | Exchange rate time series for all 54 countries | `fx_rate_usd`, `fx_30d_avg`, `fx_yoy_change_pct`, `volatility_score` |
| `mart_inflation_gdp` | Annual World Bank GDP, GDP per capita, and CPI inflation by country (long format: one row per country, indicator, and year) | `country_code`, `report_year`, `indicator_name`, `indicator_value` |

The macroeconomic fact table combines World Bank and IMF indicators. FX observations are stored separately in `fact_fx_rates` because they are daily time-series records rather than annual macroeconomic indicators.

### Country identifiers and risk-score definition

ISO3 is the canonical country key in all dbt staging, fact, and mart models.
World Bank, IMF, and FX source identifiers are uppercased and mapped through
`dim_country_seed` at the staging boundary; ISO2 and ISO3 values are both
accepted as inputs. Unmapped two-letter identifiers become null and fail
staging quality tests rather than continuing as noncanonical keys.

`mart_country_profile.risk_score` ranges from 0 (lower measured risk) to 100
(higher measured risk). It equally averages the available component groups;
missing groups are excluded, and `risk_component_count` shows the number
included (0–4):

| Component | Input | Normalization to 0–100 |
|---|---|---|
| Inflation | Latest actual (non-forecast) annual CPI inflation | 0% → 0; 25% or more → 100 |
| Public debt | IMF public debt (% of GDP), latest actual year | 30% → 0; 120% or more → 100 |
| Unemployment | Latest actual (non-forecast) unemployment rate | 5% → 0; 30% or more → 100 |
| FX | Mean of depreciation and annualized 30-day realized volatility | 50% depreciation / volatility → 100; appreciation does not add depreciation risk |

Each component is clamped to 0–100. IMF forecast years (`is_forecast = true`)
are excluded, so the score reflects reported data only. The FX group averages whichever of its two
subcomponents are available. This is a transparent comparative indicator, not
a credit rating or a forecast of default.

The daily `fact_fx_rates` model is rebuilt from staging on each run and
partitioned by `ingestion_date`. This avoids BigQuery DML (`MERGE`), which is
not available when the project has no billing account. The current FX dataset
is small enough for a full rebuild; if it grows substantially, enable billing
before switching the model back to an incremental strategy. Macro indicator
models are also rebuilt on each run so historical source revisions are
reflected.

When upgrading an existing BigQuery deployment from the prior FX table schema,
run this one-time backfill before the scheduled DAG: load the seed, rebuild the
staging views, then recreate the partitioned FX fact:

```bash
cd dbt
dbt seed --select dim_country_seed --profiles-dir . --target dev
dbt run --select staging.* --profiles-dir . --target dev
dbt run --select fact_fx_rates --profiles-dir . --target dev
```

The Airflow DAG runs the seed before the models on every scheduled execution.

---

## 🌐 Data Sources

| Source | Data Provided | Access | Cadence |
|---|---|---|---|
| [World Bank API](https://data.worldbank.org/) | GDP, population, trade, private credit/GDP, mobile subscriptions | REST — free, no key | Daily |
| [IMF DataMapper API](https://www.imf.org/external/datamapper/api/help) | Inflation, GDP, current account, government debt and balance, unemployment, population | REST — free, no key | Daily |
| [ExchangeRate-API](https://www.exchangerate-api.com/) | FX rates vs USD for all 54 countries | REST — free tier | Daily |

---

## 🔍 dbt Test Coverage

| Test | Model | Type | On Failure |
|---|---|---|---|
| Indicator key is unique and non-null | `fact_macroeconomic_indicators` | Built-in | DAG fails |
| Country codes map to the seed using ISO2 or ISO3 | Staging and facts | Custom generic test | DAG fails |
| Indicator categories use the configured set | `dim_indicator` | Accepted values | DAG fails |
| One FX rate per country per ingestion date; repeated provider timestamps warn | `fact_fx_rates` | dbt_utils | DAG fails (stale rates warn only) |
| Risk score stays between 0 and 100 | `mart_country_profile` | Accepted range | DAG fails |
| Country dimension has exactly 54 countries | `dim_country` | Singular SQL test | DAG fails |
| Raw source freshness is within thresholds | World Bank, IMF, FX sources | `dbt source freshness` | Airflow task fails on error threshold |

---

## 🔁 Airflow DAG — `african_macro_pipeline`

**Schedule:** Daily at `02:00 UTC`

```
ingest_world_bank ──┐
ingest_imf        ──┼──► check_dbt_source_freshness ──► run_dbt_seed
ingest_fx_rates   ──┘     (runs dbt deps first)              │
                                                              ▼
   run_dbt_staging ──► run_dbt_dimensions ──► run_dbt_facts ──► run_dbt_marts
                                                                      │
                                                                      ▼
                                              run_dbt_tests ──► notify_on_success
```

Ingestion and model tasks retry **3 times with exponential backoff
(5 → 10 → 20 minutes)**. `run_dbt_tests` does not retry, because a failing data
test is deterministic, and `check_dbt_source_freshness` retries once after
2 minutes to cover a transient `dbt deps` failure. Any failed API fetch fails its ingestion task, so a partly fetched source
never overwrites the day's good data. The country
seed is loaded before model runs so `dim_country` is available to downstream
models. Source freshness is checked after ingestion and fails when a raw table
exceeds its configured error threshold. Failure triggers an email alert
(set `ALERT_EMAIL` and `AIRFLOW_CONN_SMTP_DEFAULT` in `.env`).

---

## 🗺️ Repo Structure

```
african-market-intelligence/
│
├── ingestion/
│   ├── world_bank.py          # World Bank API ingestion
│   ├── imf.py                 # IMF DataMapper ingestion
│   ├── fx_rates.py            # ExchangeRate-API ingestion
│   └── bigquery_merge.py      # Partition-replace loader shared by all three
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml           # Reads GCP_PROJECT and GOOGLE_APPLICATION_CREDENTIALS
│   ├── packages.yml           # dbt_utils
│   ├── seeds/
│   │   └── dim_country_seed.csv  # Country attributes for all 54 African countries
│   ├── macros/                # Custom generic test: country_code_relationship
│   ├── models/
│   │   ├── overview.md        # Landing page of the published dbt docs
│   │   ├── staging/           # stg_world_bank, stg_imf, stg_fx_rates
│   │   └── marts/
│   │       ├── dimensions/    # dim_country, dim_indicator
│   │       ├── facts/         # fact_macroeconomic_indicators, fact_fx_rates
│   │       └── analytics/     # Country, FX, and inflation/GDP marts
│   └── tests/                 # Singular SQL assertions
│
├── airflow/
│   └── dags/
│       └── african_macro_pipeline_dag.py
│
├── infrastructure/
│   ├── docker-compose.yaml    # Local Airflow 3 (CeleryExecutor)
│   ├── Dockerfile             # Airflow image with dbt-bigquery
│   └── config/airflow.cfg
│
├── tests/                     # pytest unit tests for ingestion
├── .github/workflows/
│   ├── ci.yml                 # Ruff, pytest, dbt parse (+ BigQuery run when secrets exist)
│   └── dbt-docs.yml           # Builds dbt docs and publishes them to GitHub Pages
├── pytest.ini · ruff.toml     # Test path and lint target settings
├── .env.example
├── requirements.txt
├── Makefile
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites

- Google Cloud account with a BigQuery project
- GCP service account JSON key with BigQuery read/write permissions
- Docker Desktop (for local Airflow)
- Python 3.10+ with pip

### Setup

```bash
# 1. Clone the repo
git clone https://github.com/Chris-Ndungu/African-Market-Intelligence.git
cd African-Market-Intelligence

# 2. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Set GCP_PROJECT, BigQuery settings, and GOOGLE_APPLICATION_CREDENTIALS to
# the absolute host path of your service account JSON key. Generate fresh
# values for FERNET_KEY, AIRFLOW__API__SECRET_KEY and
# AIRFLOW__API_AUTH__JWT_SECRET (commands are in .env.example);
# Docker Compose refuses to start without them.

# 5. Create the raw BigQuery dataset (tables are created on first ingestion)
bq mk --dataset --location=US your-project:raw_african_macro

# 6. Run ingestion scripts
make ingest-all

# 7. Run dbt transformations
make dbt-run

# 8. Run dbt tests
make dbt-test

# 9. Start Airflow (local)
make airflow-up
```

### Makefile Commands

```bash
make ingest-all      # Run all three ingestion scripts
make dbt-deps        # Install dbt packages (the other dbt targets run this first)
make dbt-run         # Run all dbt models in order
make dbt-seed        # Load the country dimension seed
make dbt-test        # Run full dbt test suite
make dbt-docs        # Generate and serve docs at http://localhost:8081
make airflow-up      # Start Airflow via Docker Compose
make airflow-down    # Stop Airflow
```

`dbt-run` seeds `dim_country_seed.csv` before building models so the
`dim_country` dimension always contains all 54 countries, including countries
without current observations. Country names and current income groups follow
the [World Bank country API](https://api.worldbank.org/v2/country); capitals
reflect current administrative capitals (including Gitega in Burundi).
Region and sub-region follow the
[UN M49 geographic groupings](https://unstats.un.org/unsd/methodology/m49/),
and currency codes are ISO 4217.

### Published dbt documentation

The [dbt documentation site](https://chris-ndungu.github.io/African-Market-Intelligence/)
is generated and published to GitHub Pages on pushes to `main`. It includes
model documentation and lineage without querying BigQuery, so column data types
are not shown. In a fork, set the repository's Pages build and deployment
source to **GitHub Actions** to enable publishing. The workflow can also be
started manually from the Actions tab.

### Running a single source

Each ingestion script also runs on its own. `--country` and `--indicator`
filters are **dry-run only**: every load replaces the whole `ingestion_date`
partition, so writing a filtered subset would delete the rest of the day's data.

```bash
python ingestion/imf.py --indicator PCPIPCH --dry-run
python ingestion/fx_rates.py --country KEN --dry-run
```

### Testing and CI

Run ingestion unit tests and lint checks locally with:

```bash
pytest -q
ruff check ingestion tests
```

The GitHub Actions workflow runs Ruff, pytest, and `dbt parse` for pull requests
and pushes to `main`. It runs `dbt seed`, `dbt run`, and `dbt test` against
BigQuery when the repository has the `GCP_CI_SERVICE_ACCOUNT_JSON` and
`GCP_PROJECT` Actions secrets configured. The CI service account must be able to
read the `raw_african_macro` source tables and create/update objects in the
configured `ci_dbt` dataset. The BigQuery-backed test step is skipped when those
secrets are unavailable (for example, on forked pull requests).

The Airflow DAG runs `dbt source freshness` after ingestion. Each raw source
warns after 25 hours and fails the DAG after 48 hours without a successful load.
Country relationship tests normalize country codes against the seed's ISO2 and
ISO3 identifiers, and a dbt singular test enforces exactly 54 seeded countries.

---

## 🧠 Engineering Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Transformation tool | dbt Core | Version-controlled SQL, built-in testing, automatic lineage. No JVM overhead. |
| Orchestration | Apache Airflow | Industry standard. Retry logic and failure alerting are first-class features. |
| Warehouse | BigQuery | GCP free tier sufficient for this project. dbt-bigquery adapter is mature. |
| Ingestion pattern | Daily batch | Macroeconomic data changes daily at most. Streaming adds complexity with no benefit here. |
| Data modelling | Star schema | Demonstrates formal modelling knowledge. Easy to extend with new indicators or countries. |
| Containerisation | Docker Compose | Reproducible environment. Airflow is painful to install natively on any OS. |
| Raw loads | Replace the day's partition with load jobs | Works in the BigQuery sandbox, which has no DML. Filtered CLI runs are dry-run only, so a partial load can never delete the rest of a day's data. |
| Failure policy | Any failed API fetch fails the task | A partly fetched source never overwrites good data; Airflow retries the whole task. |

---

## 📦 Output Use Cases

**1. Exchange Rate Trend Analysis**
Query `mart_fx_trends` to track African currency movements vs USD. Join `dim_country` to segment by region or income group. Use the `volatility_score` column to identify high-risk currency periods. Relevant for investment analysts monitoring FX risk.

**2. Inflation vs GDP Comparison**
Query `mart_inflation_gdp` for annual cross-country comparisons. Pivot GDP per capita against CPI inflation to spot high-inflation, low-income economies and track how each country moves over time.

**3. Country-Level Economic Monitoring**
Query `mart_country_profile` by country and indicator to explore annual macroeconomic observations enriched with the latest FX rate and the documented `risk_score`. Use `dim_country` for the stable 54-row country reference dimension.

---

## ⚠️ Known Data Limitations

- World Bank and IMF annual GDP figures can lag by 1–2 years. This pipeline ingests the latest available value; `dbt source freshness` monitors how recently data was loaded, not how current the source figures are.
- Not all 54 countries have complete data for every indicator. Missing values are surfaced explicitly as `NULL` in mart tables — never filled or estimated.
- FX rate free tier APIs have monthly call limits. The ingestion script handles `429` responses with exponential backoff.

---

## 🔭 What I'd Do Next

- Incremental dbt models for `fact_fx_rates` once billing (and therefore `MERGE`) is enabled
- Terraform for the BigQuery datasets, service account, and IAM bindings
- Volume and anomaly checks (row-count deltas, `dbt_expectations`) alongside the existing schema tests
- Separate dev and prod dbt targets, with CI building into an isolated dataset per pull request
- A published Looker Studio dashboard for the country risk score and FX volatility

---

## 👤 Author

Built by **Chris Ndungu** — Data Engineer based in Nairobi, Kenya.

[LinkedIn](https://www.linkedin.com/in/chris-ndungu/) · [Email](mailto:chrisndungu.tech@gmail.com)