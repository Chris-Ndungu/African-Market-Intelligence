# 🌍 African Market Intelligence Pipeline

> **Automated macroeconomic data pipeline covering 54 African countries** — from raw API ingestion to query-optimised BigQuery analytics models, refreshed daily.

Built to demonstrate production-grade data engineering: batch ingestion, dbt modelling, orchestration, and data quality — on a real-world African macroeconomic dataset.

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
│   Dimensions   dim_country · dim_date · dim_indicator            │
│       ↓                                                          │
│   Fact         fact_economic_indicators                          │
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
| Transformation | **dbt Core 1.11+** | Version-controlled SQL, built-in testing, lineage docs |
| BQ Adapter | **dbt-bigquery** | Official BigQuery adapter for dbt |
| Orchestration | **Apache Airflow 3.x** | Industry-standard, retry logic, task dependencies |
| Containerisation | **Docker / Compose** | Local Airflow, reproducible environment |
| Visualisation | **Looker Studio** | Native BigQuery connector, zero hosting cost |

---

## 📊 Key Metrics This Pipeline Produces

- **Latest GDP, CPI, and FX rate** per country — refreshed daily
- **Exchange rate trends** — 30-day average, YoY change, volatility score
- **Inflation vs GDP** — paired quarterly for cross-country comparison
- **Country risk score** — derived from macro indicator composites
- **Data freshness status** — surfaced per country via dbt source tests

---

## 🗂️ Data Model

### BigQuery Datasets

| Dataset | Purpose | Refresh |
|---|---|---|
| `raw_african_macro` | Unmodified API records — landing zone only | Daily ingestion |
| `staging_african_macro` | Cleaned, typed, renamed versions of raw tables | Daily dbt run |
| `analytics_african_macro` | Dimension, fact, and mart tables — what analysts query | Daily dbt run |

### Fact Table — `fact_economic_indicators`

| Column | Type | Key | Description |
|---|---|---|---|
| `indicator_id` | STRING | PK | Hash of country_id + indicator_type + date_id |
| `country_id` | STRING | FK | References dim_country |
| `date_id` | STRING | FK | References dim_date |
| `indicator_type` | STRING | | GDP, CPI, FX_RATE, TRADE_BALANCE |
| `value` | FLOAT64 | | Numeric value of the indicator |
| `unit` | STRING | | USD, %, Index |
| `source` | STRING | | World Bank, IMF, ExchangeRate-API |
| `loaded_at` | TIMESTAMP | | Ingestion timestamp — used for freshness tests |

### Dimension Tables

```
dim_country          dim_date              dim_indicator
├── country_id PK    ├── date_id PK        ├── indicator_id PK
├── country_name     ├── full_date         ├── indicator_name
├── region           ├── year              ├── category
├── sub_region       ├── quarter           ├── unit
├── currency_code    ├── month             ├── source
├── income_group     ├── week_of_year      └── description
└── capital          └── is_year_end
```

### Mart Tables

| Mart | Description | Key Columns |
|---|---|---|
| `mart_country_profile` | Latest snapshot per country — one row per country | `latest_gdp_usd`, `latest_cpi`, `latest_fx_rate_usd`, `gdp_growth_yoy`, `risk_score` |
| `mart_fx_trends` | Exchange rate time series for all 54 countries | `fx_rate_usd`, `fx_30d_avg`, `fx_yoy_change_pct`, `volatility_score` |
| `mart_inflation_gdp` | Paired inflation and GDP data by country and quarter | `gdp_usd`, `gdp_growth_pct`, `inflation_rate`, `real_gdp_growth` |

---

## 🌐 Data Sources

| Source | Data Provided | Access | Cadence |
|---|---|---|---|
| [World Bank API](https://data.worldbank.org/) | GDP, population, trade balance | REST — free, no key | Daily |
| [IMF Data API](https://data.imf.org/) | CPI, inflation, current account | REST — free, no key | Daily |
| [ExchangeRate-API](https://www.exchangerate-api.com/) | FX rates vs USD — 54 currencies | REST — free tier | Daily |

---

## 🔍 dbt Test Coverage

| Test | Model | Type | On Failure |
|---|---|---|---|
| `indicator_id` is unique | `fact_economic_indicators` | Built-in | DAG fails, alert sent |
| `country_id` not null | `fact_economic_indicators` | Built-in | DAG fails, alert sent |
| `value` not null | `fact_economic_indicators` | Built-in | DAG fails, alert sent |
| `country_id` exists in `dim_country` | `fact_economic_indicators` | Relationship | DAG fails, alert sent |
| `value > 0` for GDP indicators | `fact_economic_indicators` | Custom SQL | DAG fails, alert sent |
| No future-dated records | `fact_economic_indicators` | Custom SQL | DAG fails, alert sent |
| Freshness — `loaded_at` < 25h ago | `fact_economic_indicators` | Source freshness | Warning — logged |
| `mart_country_profile` covers 54 rows | `mart_country_profile` | Custom SQL | Warning — logged |

---

## 🔁 Airflow DAG — `african_macro_pipeline_dag`

**Schedule:** Daily at `02:00 UTC`

```
ingest_world_bank ──┐
ingest_imf        ──┼──► load_to_bq_raw ──► run_dbt_staging
ingest_fx_rates   ──┘                              │
                                                   ▼
                                         run_dbt_dimensions
                                                   │
                                                   ▼
                                           run_dbt_facts
                                                   │
                                                   ▼
                                           run_dbt_marts
                                                   │
                                                   ▼
                                           run_dbt_tests
                                                   │
                                                   ▼
                                        notify_on_completion
```

All tasks have retry logic: **3 retries with 5-minute delays**. Failure triggers an email alert with full task logs.

---

## 🗺️ Repo Structure

```
african-market-intelligence/
│
├── ingestion/
│   ├── world_bank.py          # World Bank API ingestion
│   ├── imf.py                 # IMF Data API ingestion
│   ├── fx_rates.py            # Exchange rate API ingestion
│   └── config.py              # GCP project, dataset, API keys
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml.example
│   ├── models/
│   │   ├── staging/           # stg_world_bank, stg_imf, stg_fx_rates
│   │   ├── dimensions/        # dim_country, dim_date, dim_indicator
│   │   ├── facts/             # fact_economic_indicators
│   │   └── marts/             # mart_country_profile, mart_fx_trends, mart_inflation_gdp
│   ├── tests/                 # Custom SQL assertions
│   └── seeds/                 # dim_country seed CSV (54 countries)
│
├── airflow/
│   └── dags/
│       └── african_macro_pipeline_dag.py
│
├── infrastructure/
│   ├── docker-compose.yml     # Airflow local setup
│   └── bigquery/
│       └── create_tables.sql  # Raw table DDL
│
├── docs/
│   ├── architecture.png
│   └── data_model.png
│
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
# Add your GCP project ID, BigQuery dataset name, and service account key path

# 5. Create BigQuery raw tables
bq mk --dataset your-project:raw_african_macro
bq query --use_legacy_sql=false < infrastructure/bigquery/create_tables.sql

# 6. Run ingestion scripts
make ingest-all

# 7. Run dbt transformations
make dbt-run

# 8. Run dbt tests
make dbt-test

# 9. Start Airflow (local)
cd infrastructure && docker-compose up -d
```

### Makefile Commands

```bash
make ingest-all      # Run all three ingestion scripts
make dbt-run         # Run all dbt models in order
make dbt-test        # Run full dbt test suite
make dbt-docs        # Generate and serve dbt lineage docs
make airflow-up      # Start Airflow via Docker Compose
make airflow-down    # Stop Airflow
```

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

---

## 📦 Output Use Cases

**1. Exchange Rate Trend Analysis**
Query `mart_fx_trends` to track African currency movements vs USD. Segment by region or income group. Use the `volatility_score` column to identify high-risk currency periods. Relevant for investment analysts monitoring FX risk.

**2. Inflation vs GDP Comparison**
Query `mart_inflation_gdp` for cross-country macro comparisons by quarter. Plot real GDP growth against inflation rate to identify countries in stagflation, high-growth, or stable trajectories.

**3. Country-Level Economic Monitoring**
Query `mart_country_profile` for a single-row snapshot of any of the 54 countries — most recent GDP, CPI, FX rate, and derived `risk_score`. Designed to power country scorecards in Looker Studio or any BI tool connected to BigQuery.

---

## ⚠️ Known Data Limitations

- World Bank and IMF annual GDP figures can lag by 1–2 years. This pipeline ingests the latest available value and flags staleness via dbt source freshness tests.
- Not all 54 countries have complete data for every indicator. Missing values are surfaced explicitly as `NULL` in mart tables — never filled or estimated.
- FX rate free tier APIs have monthly call limits. The ingestion script handles `429` responses with exponential backoff.

---

## 🎥 Demo

[▶ Watch the 3-minute pipeline walkthrough](#) 

---

## 👤 Author

Built by **Chris Ndungu** — Data Engineer based in Nairobi, Kenya.

[LinkedIn](https://www.linkedin.com/in/chris-ndungu/) · [Email](chrisndungu.tech@gmail.com)