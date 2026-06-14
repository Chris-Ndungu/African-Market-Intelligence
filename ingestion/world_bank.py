"""
world_bank.py
─────────────────────────────────────────────────────────────────────────────
Ingests macroeconomic indicators for 54 African countries from the World Bank
API and loads them into BigQuery (raw layer).

World Bank API docs: https://datahelpdesk.worldbank.org/knowledgebase/articles/898590

Usage:
    python world_bank.py                  # ingest all indicators, all countries
    python world_bank.py --dry-run        # fetch and validate, skip BQ write
    python world_bank.py --country KE     # single country (ISO2 code)
    python world_bank.py --indicator GDP  # single indicator group

Author : Chris Ndungu
Project: African Market Intelligence Pipeline
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

# ── ENV ───────────────────────────────────────────────────────────────────────
load_dotenv()

GCP_PROJECT   = os.getenv("GCP_PROJECT")
BQ_DATASET    = os.getenv("BQ_RAW_DATASET", "raw_african_macro")
BQ_TABLE      = os.getenv("BQ_WB_TABLE",    "raw_world_bank")
BQ_LOCATION   = os.getenv("BQ_LOCATION",    "US")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
WB_BASE_URL   = "https://api.worldbank.org/v2"
PER_PAGE      = 1000          # max records per API page
MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0           # seconds — doubles on each retry
REQUEST_DELAY = 0.25          # seconds between requests (rate limiting)

# ── 54 AFRICAN COUNTRIES (ISO2 codes) ─────────────────────────────────────────
AFRICAN_COUNTRIES = [
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CV", "CM", "CF", "TD",
    "KM", "CG", "CD", "CI", "DJ", "EG", "GQ", "ER", "SZ", "ET",
    "GA", "GM", "GH", "GN", "GW", "KE", "LS", "LR", "LY", "MG",
    "MW", "ML", "MR", "MU", "MA", "MZ", "NA", "NE", "NG", "RW",
    "ST", "SN", "SL", "SO", "ZA", "SS", "SD", "TZ", "TG", "TN",
    "UG", "ZM", "ZW", "SC",
]

# ── INDICATORS ────────────────────────────────────────────────────────────────
# Format: { group_name: { "code": WB_indicator_code, "unit": unit_label } }
INDICATORS = {
    "GDP_CURRENT_USD": {
        "code": "NY.GDP.MKTP.CD",
        "name": "GDP (current USD)",
        "unit": "USD",
        "category": "GDP",
    },
    "GDP_GROWTH_ANNUAL": {
        "code": "NY.GDP.MKTP.KD.ZG",
        "name": "GDP growth (annual %)",
        "unit": "%",
        "category": "GDP",
    },
    "GDP_PER_CAPITA_USD": {
        "code": "NY.GDP.PCAP.CD",
        "name": "GDP per capita (current USD)",
        "unit": "USD",
        "category": "GDP",
    },
    "POPULATION_TOTAL": {
        "code": "SP.POP.TOTL",
        "name": "Population, total",
        "unit": "persons",
        "category": "DEMOGRAPHICS",
    },
    "INFLATION_CPI": {
        "code": "FP.CPI.TOTL.ZG",
        "name": "Inflation, consumer prices (annual %)",
        "unit": "%",
        "category": "INFLATION",
    },
    "CURRENT_ACCOUNT_GDP": {
        "code": "BN.CAB.XOKA.GD.ZS",
        "name": "Current account balance (% of GDP)",
        "unit": "%",
        "category": "TRADE",
    },
    "EXPORTS_GOODS_SERVICES": {
        "code": "NE.EXP.GNFS.CD",
        "name": "Exports of goods and services (current USD)",
        "unit": "USD",
        "category": "TRADE",
    },
    "IMPORTS_GOODS_SERVICES": {
        "code": "NE.IMP.GNFS.CD",
        "name": "Imports of goods and services (current USD)",
        "unit": "USD",
        "category": "TRADE",
    },
    "FOREIGN_DIRECT_INVESTMENT": {
        "code": "BX.KLT.DINV.CD.WD",
        "name": "Foreign direct investment, net inflows (BoP, current USD)",
        "unit": "USD",
        "category": "INVESTMENT",
    },
    "EXTERNAL_DEBT_STOCKS": {
        "code": "DT.DOD.DECT.CD",
        "name": "External debt stocks, total (DOD, current USD)",
        "unit": "USD",
        "category": "DEBT",
    },
    "UNEMPLOYMENT_RATE": {
        "code": "SL.UEM.TOTL.ZS",
        "name": "Unemployment, total (% of total labor force)",
        "unit": "%",
        "category": "LABOUR",
    },
    "GROSS_NATIONAL_SAVINGS": {
        "code": "NY.GNS.ICTR.ZS",
        "name": "Gross national expenditure (% of GDP)",
        "unit": "%",
        "category": "SAVINGS",
    },
}

# ── BIGQUERY SCHEMA ───────────────────────────────────────────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("record_id",       "STRING",    "REQUIRED"),  # PK
    bigquery.SchemaField("country_code",    "STRING",    "REQUIRED"),  # ISO2
    bigquery.SchemaField("country_name",    "STRING",    "NULLABLE"),
    bigquery.SchemaField("indicator_key",   "STRING",    "REQUIRED"),  # e.g. GDP_CURRENT_USD
    bigquery.SchemaField("indicator_code",  "STRING",    "REQUIRED"),  # e.g. NY.GDP.MKTP.CD
    bigquery.SchemaField("indicator_name",  "STRING",    "NULLABLE"),
    bigquery.SchemaField("category",        "STRING",    "NULLABLE"),
    bigquery.SchemaField("year",            "INTEGER",   "REQUIRED"),
    bigquery.SchemaField("value",           "FLOAT64",   "NULLABLE"),  # NULL = data not available
    bigquery.SchemaField("unit",            "STRING",    "NULLABLE"),
    bigquery.SchemaField("source",          "STRING",    "REQUIRED"),
    bigquery.SchemaField("raw_response",    "STRING",    "NULLABLE"),  # full API JSON blob
    bigquery.SchemaField("loaded_at",       "TIMESTAMP", "REQUIRED"),  # ingestion timestamp
    bigquery.SchemaField("ingestion_date",  "DATE",      "REQUIRED"),  # partition column
]


# ─────────────────────────────────────────────────────────────────────────────
# API FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_with_retry(url: str, params: dict, retries: int = MAX_RETRIES) -> dict | None:
    """
    GET a URL with automatic retry on transient errors (429, 5xx).
    Returns parsed JSON or None on total failure.
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=30)

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                log.warning(f"Rate limited (429). Waiting {wait}s before retry {attempt}/{retries}.")
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = RETRY_BACKOFF ** attempt
                log.warning(f"Server error {resp.status_code}. Waiting {wait}s before retry {attempt}/{retries}.")
                time.sleep(wait)
                continue

            log.error(f"Unexpected status {resp.status_code} for URL: {url}")
            return None

        except requests.exceptions.Timeout:
            log.warning(f"Request timed out (attempt {attempt}/{retries}). URL: {url}")
            time.sleep(RETRY_BACKOFF ** attempt)

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error (attempt {attempt}/{retries}): {e}")
            time.sleep(RETRY_BACKOFF ** attempt)

    log.error(f"All {retries} retries exhausted for URL: {url}")
    return None


def fetch_indicator_for_countries(
    indicator_key: str,
    indicator_meta: dict,
    countries: list[str],
    start_year: int = 2000,
) -> list[dict]:
    """
    Fetch a single World Bank indicator for a list of countries.

    The World Bank API accepts a semicolon-separated list of country codes,
    so we batch all 54 countries into a single request per indicator.
    Returns a list of clean record dicts ready for BigQuery insertion.
    """
    indicator_code = indicator_meta["code"]
    country_string = ";".join(countries)  # e.g. "KE;NG;ZA;EG;..."
    loaded_at = datetime.now(timezone.utc)
    ingestion_date = loaded_at.date().isoformat()
    records = []
    skipped = 0

    # ── Paginated fetch ───────────────────────────────────────────
    page = 1
    total_pages = 1  # will be updated from first response

    while page <= total_pages:
        url = f"{WB_BASE_URL}/country/{country_string}/indicator/{indicator_code}"
        params = {
            "format":    "json",
            "per_page":  PER_PAGE,
            "page":      page,
            "mrv":       25,          # most recent 25 years
            "date":      f"{start_year}:2030",
        }

        raw = fetch_with_retry(url, params)

        if raw is None:
            log.error(f"Failed to fetch {indicator_key} page {page}. Skipping remaining pages.")
            break

        # World Bank wraps response: [metadata_dict, data_list]
        if not isinstance(raw, list) or len(raw) < 2:
            log.error(f"Unexpected response structure for {indicator_key}: {str(raw)[:200]}")
            break

        meta_block = raw[0]
        data_block = raw[1]

        # Update pagination from first response
        if page == 1:
            total_pages = int(meta_block.get("pages", 1))
            total_records = int(meta_block.get("total", 0))
            log.info(
                f"  {indicator_key}: {total_records} records across "
                f"{total_pages} page(s) for {len(countries)} countries."
            )

        if not data_block:
            log.warning(f"  {indicator_key} page {page}: empty data block.")
            break

        # ── Parse each data point ─────────────────────────────────
        for entry in data_block:
            try:
                country_code = entry.get("countryiso3code") or entry.get("country", {}).get("id", "")
                country_name = entry.get("country", {}).get("value", "")
                year_str = entry.get("date", "")
                value_raw = entry.get("value")

                # Skip aggregate / regional entries (e.g. "Sub-Saharan Africa")
                if len(country_code) != 3 or not country_code.isalpha():
                    skipped += 1
                    continue

                # Skip non-year date entries (some indicators use quarterly format)
                if not year_str.isdigit():
                    skipped += 1
                    continue

                year = int(year_str)
                value = float(value_raw) if value_raw is not None else None

                # Stable record ID — deterministic, supports idempotency
                record_id = hashlib.md5(
                    f"{country_code}|{indicator_key}|{year}".encode()
                ).hexdigest()

                records.append({
                    "record_id":      record_id,
                    "country_code":   country_code,
                    "country_name":   country_name,
                    "indicator_key":  indicator_key,
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_meta["name"],
                    "category":       indicator_meta["category"],
                    "year":           year,
                    "value":          value,
                    "unit":           indicator_meta["unit"],
                    "source":         "World Bank",
                    "raw_response":   json.dumps(entry),
                    "loaded_at":      loaded_at.isoformat(),
                    "ingestion_date": ingestion_date,
                })

            except (KeyError, ValueError, TypeError) as e:
                log.warning(f"  Skipping malformed record: {e} — {str(entry)[:120]}")
                skipped += 1
                continue

        page += 1
        time.sleep(REQUEST_DELAY)  # be polite to the API

    log.info(
        f"  {indicator_key}: parsed {len(records)} records, "
        f"skipped {skipped} (aggregates / malformed)."
    )
    return records


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_table(client: bigquery.Client) -> bigquery.Table:
    """
    Ensure the raw BigQuery table exists. Creates it if not.
    Partitioned by ingestion_date for cost-efficient querying.
    """
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        table = client.get_table(table_ref)
        log.info(f"Table exists: {table_ref}")
        return table
    except Exception:
        pass  # table doesn't exist — create it

    log.info(f"Creating table: {table_ref}")
    table = bigquery.Table(table_ref, schema=BQ_SCHEMA)

    # Partition by ingestion_date to keep daily loads cheap to query
    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date",
    )

    # Cluster by country and indicator for fast filtered queries
    table.clustering_fields = ["country_code", "indicator_key"]

    table.description = (
        "Raw World Bank macroeconomic indicators for 54 African countries. "
        "Unmodified source records — do not query directly; use staging models."
    )

    try:
        table = client.create_table(table)
        log.info(f"Created table: {table_ref}")
        return table
    except GoogleCloudError as e:
        log.error(f"Failed to create table: {e}")
        raise


def load_to_bigquery(
    client: bigquery.Client,
    records: list[dict],
    dry_run: bool = False,
) -> int:
    """
    Write records to BigQuery using WRITE_TRUNCATE on today's partition.

    WRITE_TRUNCATE on the ingestion_date partition means:
    - Running twice on the same day overwrites rather than duplicates
    - Historical partitions are never touched
    - Idempotent by design
    """
    if not records:
        log.warning("No records to load.")
        return 0

    if dry_run:
        log.info(f"[DRY RUN] Would load {len(records)} records to BigQuery.")
        log.info(f"[DRY RUN] Sample record:\n{json.dumps(records[0], indent=2)}")
        return len(records)

    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    today = datetime.now(timezone.utc).date().isoformat()

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        # Target today's partition only — leave historical data untouched
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingestion_date",
        ),
        clustering_fields=["country_code", "indicator_key"],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    # BigQuery load jobs work in batches of 10,000 rows max
    BATCH_SIZE = 10_000
    total_loaded = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = -(-len(records) // BATCH_SIZE)  # ceiling division

        log.info(f"Loading batch {batch_num}/{total_batches} ({len(batch)} rows)...")

        try:
            job = client.load_table_from_json(
                batch,
                f"{table_ref}${today.replace('-', '')}",  # target specific partition
                job_config=job_config,
            )
            job.result()  # wait for job to complete

            if job.errors:
                log.error(f"BigQuery load errors in batch {batch_num}: {job.errors}")
            else:
                total_loaded += len(batch)
                log.info(f"  Batch {batch_num} loaded successfully.")

        except GoogleCloudError as e:
            log.error(f"BigQuery load failed for batch {batch_num}: {e}")
            raise

    return total_loaded


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Basic validation before BigQuery write.
    Returns (valid_records, invalid_records).
    """
    valid, invalid = [], []

    for record in records:
        errors = []

        if not record.get("record_id"):
            errors.append("Missing record_id")
        if not record.get("country_code"):
            errors.append("Missing country_code")
        if not record.get("indicator_key"):
            errors.append("Missing indicator_key")
        if not isinstance(record.get("year"), int) or record["year"] < 1960 or record["year"] > 2030:
            errors.append(f"Invalid year: {record.get('year')}")
        if record.get("value") is not None and not isinstance(record["value"], float):
            errors.append(f"Value is not float: {type(record.get('value'))}")

        if errors:
            log.debug(f"Invalid record ({errors}): {str(record)[:100]}")
            invalid.append(record)
        else:
            valid.append(record)

    if invalid:
        log.warning(f"Validation: {len(valid)} valid, {len(invalid)} invalid records.")
    else:
        log.info(f"Validation: all {len(valid)} records passed.")

    return valid, invalid


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run(
    countries: list[str],
    indicators: dict,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Main ingestion run. Returns a summary dict for Airflow logging.
    """
    start_time = time.time()
    log.info("=" * 70)
    log.info("African Market Intelligence — World Bank Ingestion")
    log.info(f"Countries  : {len(countries)}")
    log.info(f"Indicators : {len(indicators)}")
    log.info(f"Dry run    : {dry_run}")
    log.info(f"BQ target  : {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    log.info("=" * 70)

    # ── Fetch ─────────────────────────────────────────────────────
    all_records: list[dict] = []

    for idx, (indicator_key, indicator_meta) in enumerate(indicators.items(), 1):
        log.info(
            f"\n[{idx}/{len(indicators)}] Fetching: {indicator_key} "
            f"({indicator_meta['code']})"
        )
        records = fetch_indicator_for_countries(indicator_key, indicator_meta, countries)
        all_records.extend(records)

    log.info(f"\nTotal records fetched: {len(all_records)}")

    # ── Validate ──────────────────────────────────────────────────
    valid_records, invalid_records = validate_records(all_records)

    # ── Load ──────────────────────────────────────────────────────
    loaded = 0
    if valid_records:
        if not dry_run:
            bq_client = bigquery.Client(project=GCP_PROJECT)
            get_or_create_table(bq_client)
            loaded = load_to_bigquery(bq_client, valid_records, dry_run)
        else:
            loaded = load_to_bigquery(None, valid_records, dry_run=True)

    elapsed = round(time.time() - start_time, 1)

    summary = {
        "status":          "success",
        "countries":       len(countries),
        "indicators":      len(indicators),
        "records_fetched": len(all_records),
        "records_valid":   len(valid_records),
        "records_invalid": len(invalid_records),
        "records_loaded":  loaded,
        "duration_seconds": elapsed,
        "dry_run":         dry_run,
    }

    log.info("\n" + "=" * 70)
    log.info("INGESTION COMPLETE")
    for k, v in summary.items():
        log.info(f"  {k:<25} {v}")
    log.info("=" * 70)

    return summary


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Ingest World Bank macroeconomic data for 54 African countries into BigQuery."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate data but do not write to BigQuery.",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Run for a single country (ISO2 code, e.g. KE). Default: all 54.",
    )
    parser.add_argument(
        "--indicator",
        type=str,
        default=None,
        help="Run for a single indicator group key (e.g. GDP_CURRENT_USD). Default: all.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    # ── Resolve countries ─────────────────────────────────────────
    if args.country:
        country_iso2 = args.country.upper()
        if country_iso2 not in AFRICAN_COUNTRIES:
            log.error(
                f"'{country_iso2}' is not in the supported African countries list. "
                f"Valid options: {', '.join(AFRICAN_COUNTRIES)}"
            )
            sys.exit(1)
        countries = [country_iso2]
    else:
        countries = AFRICAN_COUNTRIES

    # ── Resolve indicators ────────────────────────────────────────
    if args.indicator:
        key = args.indicator.upper()
        if key not in INDICATORS:
            log.error(
                f"'{key}' is not a recognised indicator key. "
                f"Valid options: {', '.join(INDICATORS.keys())}"
            )
            sys.exit(1)
        indicators = {key: INDICATORS[key]}
    else:
        indicators = INDICATORS

    # ── Validate env vars ─────────────────────────────────────────
    if not args.dry_run:
        missing = [v for v in ["GCP_PROJECT"] if not os.getenv(v)]
        if missing:
            log.error(
                f"Missing required environment variables: {', '.join(missing)}. "
                f"Copy .env.example to .env and fill in your values."
            )
            sys.exit(1)

    # ── Run ───────────────────────────────────────────────────────
    try:
        summary = run(countries, indicators, dry_run=args.dry_run)
        if summary["records_invalid"] > 0:
            log.warning(
                f"{summary['records_invalid']} invalid records were skipped. "
                f"Check logs above for details."
            )
        sys.exit(0)

    except KeyboardInterrupt:
        log.info("Interrupted by user.")
        sys.exit(0)

    except Exception as e:
        log.exception(f"Ingestion failed with unhandled error: {e}")
        sys.exit(1)