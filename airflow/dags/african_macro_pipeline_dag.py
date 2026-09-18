"""
african_macro_pipeline_dag.py
─────────────────────────────────────────────────────────────────────────────
Airflow DAG — African Market Intelligence Pipeline

Schedule: Daily at 02:00 UTC
Tasks:
    1. ingest_world_bank       — runs ingestion/world_bank.py
    2. ingest_imf              — runs ingestion/imf.py
    3. ingest_fx_rates         — runs ingestion/fx_rates.py (parallel with above)
    4. run_dbt_staging         — dbt run --select staging.*
    5. run_dbt_dimensions      — dbt run --select dimensions.*
    6. run_dbt_facts           — dbt run --select facts.*
    7. run_dbt_marts           — dbt run --select marts.*
    8. run_dbt_tests           — dbt test
    9. notify_on_success       — logs completion summary

Dependency graph:

    ingest_world_bank ──┐
    ingest_imf        ──┼──► run_dbt_staging ──► run_dbt_dimensions
    ingest_fx_rates   ──┘         ──► run_dbt_facts ──► run_dbt_marts
                                          ──► run_dbt_tests ──► notify_on_success

Author : Chris Njoroge
Project: African Market Intelligence Pipeline
"""

import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, chain
from airflow.task.trigger_rule import TriggerRule

# ── PATHS ─────────────────────────────────────────────────────────────────────
# Resolve project root relative to this DAG file
# Structure assumed:
#   project/
#     airflow/dags/african_macro_pipeline_dag.py   ← this file
#     ingestion/world_bank.py
#     ingestion/imf.py
#     ingestion/fx_rates.py
#     dbt/

DAG_DIR      = Path(__file__).resolve().parent          # airflow/dags/
AIRFLOW_DIR  = DAG_DIR.parent                           # airflow/
PROJECT_ROOT = AIRFLOW_DIR.parent                       # project root
INGESTION_DIR = PROJECT_ROOT / "ingestion"
DBT_DIR       = PROJECT_ROOT / "dbt"

# ── ENVIRONMENT ───────────────────────────────────────────────────────────────
# These are read from the Airflow environment / .env file
GCP_PROJECT  = os.getenv("GCP_PROJECT")
BQ_DATASET   = os.getenv("BQ_RAW_DATASET")
DBT_PROFILES = os.getenv("DBT_PROFILES_DIR", str(Path.home() / ".dbt"))

log = logging.getLogger(__name__)

# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
default_args = {
    "owner":             "Chris",
    "depends_on_past":   False,
    "email":             [os.getenv("ALERT_EMAIL", "chris@example.com")],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":   timedelta(minutes=30),
}


# ─────────────────────────────────────────────────────────────────────────────
# PYTHON CALLABLES
# ─────────────────────────────────────────────────────────────────────────────

def run_ingestion_script(script_name: str, **context) -> dict:
    """
    Runs a Python ingestion script as a subprocess.
    """
    script_path = INGESTION_DIR / script_name

    if not script_path.exists():
        raise FileNotFoundError(
            f"Ingestion script not found: {script_path}. "
            f"Expected it at {INGESTION_DIR}/{script_name}"
        )

    env = os.environ.copy()

    if GCP_PROJECT is None or BQ_DATASET is None:
        raise ValueError("GCP_PROJECT and BQ_DATASET must be configured")

    env["GCP_PROJECT"] = GCP_PROJECT
    env["BQ_RAW_DATASET"] = BQ_DATASET

    log.info(f"Running ingestion script: {script_path}")
    log.info(f"GCP Project: {GCP_PROJECT} | BQ Dataset: {BQ_DATASET}")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
        env=env,
        check=False,
    )

    # Stream script stdout to Airflow task logs
    if result.stdout:
        for line in result.stdout.strip().split("\n"):
            log.info(f"[{script_name}] {line}")

    if result.stderr:
        for line in result.stderr.strip().split("\n"):
            log.warning(f"[{script_name}] STDERR: {line}")

    if result.returncode != 0:
        raise RuntimeError(
            f"{script_name} exited with code {result.returncode}. "
            f"Check task logs above for details."
        )

    log.info(f"{script_name} completed successfully.")
    return {"script": script_name, "exit_code": result.returncode}


def run_world_bank(**context) -> dict:
    return run_ingestion_script("world_bank.py", **context)


def run_imf(**context) -> dict:
    return run_ingestion_script("imf.py", **context)


def run_fx_rates(**context) -> dict:
    return run_ingestion_script("fx_rates.py", **context)


def notify_success(**context) -> None:
    """
    Logs a completion summary after all tasks succeed.
    Extend this to send a Slack message or email report.
    """
    dag_run    = context["dag_run"]
    logical_dt = context["logical_date"].strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info("PIPELINE COMPLETE — African Market Intelligence")
    log.info(f"  DAG run ID    : {dag_run.run_id}")
    log.info(f"  Logical date  : {logical_dt}")
    log.info(f"  GCP Project   : {GCP_PROJECT}")
    log.info(f"  BQ Dataset    : {BQ_DATASET}")
    log.info("  Tasks complete: ingest → dbt staging → dims → facts → marts → tests")
    log.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="african_macro_pipeline",
    description="Daily ingestion of African macroeconomic data — World Bank, IMF, FX rates → BigQuery → dbt",
    schedule="0 2 * * *",          # 02:00 UTC daily
    start_date=datetime(2026, 1, 1,tzinfo=timezone.utc),
    catchup=False,                  # don't backfill historical runs on first deploy
    max_active_runs=1,              # prevent overlapping daily runs
    tags=["african-macro", "ingestion", "dbt", "bigquery"],
    default_args=default_args,
    doc_md="""
## African Market Intelligence Pipeline

Daily batch pipeline that ingests macroeconomic data for 54 African countries
from three sources (World Bank, IMF, FX rates), transforms it through a dbt
modelling layer, and serves analytics-ready tables in BigQuery.

### Schedule
Daily at **02:00 UTC**

### Sources
| Task | Source | Records (approx) |
|---|---|---|
| ingest_world_bank | World Bank API | ~15,000/day |
| ingest_imf | IMF DataMapper API | ~22,000/day |
| ingest_fx_rates | ExchangeRate-API | ~54/day |

### dbt Models
Staging → Dimensions → Facts → Marts

### On failure
All tasks retry 3x with exponential backoff (5m → 10m → 20m).
Failure triggers an email alert to the owner.
    """,
) as dag:

    # ── INGESTION TASKS (run in parallel) ─────────────────────────

    ingest_world_bank = PythonOperator(
        task_id="ingest_world_bank",
        python_callable=run_world_bank,
        doc_md="Fetches GDP, inflation, trade, population data for 54 countries from the World Bank API and loads to `raw_world_bank`.",
    )

    ingest_imf = PythonOperator(
        task_id="ingest_imf",
        python_callable=run_imf,
        doc_md="Fetches 10 IMF DataMapper indicators for 54 countries and loads to `raw_imf`.",
    )

    ingest_fx_rates = PythonOperator(
        task_id="ingest_fx_rates",
        python_callable=run_fx_rates,
        doc_md="Fetches daily FX rates vs USD for 54 African currencies and loads to `raw_fx_rates`.",
    )

    # ── DBT TASKS ─────────────────────────────────────────────────
    # Each dbt task runs after all three ingestion tasks complete.
    # dbt tasks are chained sequentially — staging must succeed before
    # dimensions run, dimensions before facts, and so on.

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select staging.* --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Runs all dbt staging models: `stg_world_bank`, `stg_imf`, `stg_fx_rates`.",
    )

    run_dbt_dimensions = BashOperator(
        task_id="run_dbt_dimensions",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select dimensions.* --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Runs all dbt dimension models: `dim_country`, `dim_date`, `dim_indicator`.",
    )

    run_dbt_facts = BashOperator(
        task_id="run_dbt_facts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select facts.* --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Runs the fact model: `fact_economic_indicators`.",
    )

    run_dbt_marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt run --select marts.* --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="Runs all mart models: `mart_country_profile`, `mart_fx_trends`, `mart_inflation_gdp`.",
    )

    run_dbt_tests = BashOperator(
        task_id="run_dbt_tests",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES} --target prod"
        ),
        doc_md="""
Runs the full dbt test suite:
- Uniqueness and not-null on fact table
- Referential integrity (dim_country, dim_date)
- Custom SQL assertions (value > 0 for GDP, no future-dated records)
- Source freshness check
        """,
    )

    # ── NOTIFICATION ──────────────────────────────────────────────

    notify = PythonOperator(
        task_id="notify_on_success",
        python_callable=notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Logs a completion summary once all upstream tasks succeed.",
    )

    # ── DEPENDENCIES ──────────────────────────────────────────────
    #
    # Ingestion tasks run in parallel, then dbt runs sequentially:
    #
    #   ingest_world_bank ──┐
    #   ingest_imf        ──┼──► run_dbt_staging
    #   ingest_fx_rates   ──┘         │
    #                                 ▼
    #                        run_dbt_dimensions
    #                                 │
    #                                 ▼
    #                           run_dbt_facts
    #                                 │
    #                                 ▼
    #                           run_dbt_marts
    #                                 │
    #                                 ▼
    #                           run_dbt_tests
    #                                 │
    #                                 ▼
    #                         notify_on_success

    chain(
        [ingest_world_bank, ingest_imf, ingest_fx_rates],
        run_dbt_staging,
        run_dbt_dimensions,
        run_dbt_facts,
        run_dbt_marts,
        run_dbt_tests,
        notify,
    )