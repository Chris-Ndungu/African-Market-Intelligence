"""
fx_rates.py
─────────────────────────────────────────────────────────────────────────────
Ingests fx rates for 54 African countries from the ExchangeRate-API and loads them into BigQuery (raw layer).

IMF DataMapper API docs: https://www.exchangerate-api.com/docs

Usage:
    python fx_rates.py                         # ingest all rates, all countries
    python fx_rates.py --dry-run               # fetch and validate, skip BQ write
    python fx_rates.py --country KEN           # single country (ISO3 code)

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

GCP_PROJECT = os.getenv("GCP_PROJECT")
FX_RATES_KEY = os.getenv("FX_RATES_KEY")
BQ_DATASET  = os.getenv("BQ_RAW_DATASET", "raw_african_macro")
BQ_TABLE    = os.getenv("BQ_IMF_TABLE",   "raw_fx_rates")
BQ_LOCATION = os.getenv("BQ_LOCATION",    "US")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
FX_BASE_URL  = "https://v6.exchangerate-api.com/v6/"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0    # seconds — doubles on each retry
REQUEST_DELAY = 0.6    # seconds between requests — IMF rate limit is ~10/5s

# ── 54 AFRICAN COUNTRIES (ISO3 codes) ───────────────────────────────────────────
COUNTRY_ISO3_TO_CURRENCY = {
    "DZA": "DZD", "AGO": "AOA", "BEN": "XOF", "BWA": "BWP", "BDI": "BIF",
    "CPV": "CVE", "CMR": "XAF", "CAF": "XAF", "TCD": "XAF", "COM": "KMF",
    "COG": "XAF", "COD": "CDF", "CIV": "XOF", "DJI": "DJF", "EGY": "EGP",
    "GNQ": "XAF", "ERI": "ERN", "SWZ": "SZL", "ETH": "ETB", "GAB": "XAF",
    "GMB": "GMD", "GHA": "GHS", "GIN": "GNF", "GNB": "XOF", "KEN": "KES",
    "LSO": "LSL", "LBR": "LRD", "LBY": "LYD", "MDG": "MGA", "MWI": "MWK",
    "MLI": "XOF", "MRT": "MRU", "MUS": "MUR", "MAR": "MAD", "MOZ": "MZN",
    "NAM": "NAD", "NER": "XOF", "NGA": "NGN", "RWA": "RWF", "STP": "STN",
    "SEN": "XOF", "SYC": "SCR", "SLE": "SLE", "SOM": "SOS", "ZAF": "ZAR",
    "SSD": "SSP", "SDN": "SDG", "TZA": "TZS", "TGO": "XOF", "TUN": "TND",
    "UGA": "UGX", "ZMB": "ZMW", "ZWE": "ZWG"
}

# ── BIGQUERY SCHEMA ───────────────────────────────────────────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("country_id",      "STRING",    "REQUIRED"),  # ISO3
    bigquery.SchemaField("currency_code",   "STRING",    "REQUIRED"),  # ISO3
    bigquery.SchemaField("base_currency",   "STRING",    "REQUIRED"),  # e.g., 'USD'
    bigquery.SchemaField("fx_rate_usd",     "FLOAT64",   "NULLABLE"),
    bigquery.SchemaField("rate_timestamp",  "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("source",          "STRING",    "REQUIRED"),
    bigquery.SchemaField("loaded_at",       "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("ingestion_date",  "DATE",      "REQUIRED"),
]


# ─────────────────────────────────────────────────────────────────────────────
# API FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_exchange_rate(countries: list[str]) -> list[dict[str, Any]]:
    """
    Fetches daily exchange rates for a given list of ISO3 country codes from ExchangeRate-API.
    
    Args:
        countries: List of ISO 3166-1 alpha-3 country codes (e.g., ["KEN", "NGA", "ZAF"])
        
    Returns:
        List of formatted dictionary payloads ready for BigQuery raw landing ingestion.
    """
    api_key = FX_RATES_KEY
    if not api_key:
        # Fallback to the free unauthenticated endpoint if API key is not present
        url = f"{FX_BASE_URL}"
    else:
        url = f"{FX_BASE_URL}{api_key}/latest/USD"

    try:
        response  = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        print(f"Data: {data}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch exchange rates from API: {e}")

    # Check for explicit error responses
    if data.get("reslts") == "error":
        raise ValueError(f"ExchangeRate-API error: {data.get('error-type', 'Unknown error')}")

    conversion_rates = data.get("rates", {}) or data.get("conversion_rates", {})
    ingested_at = datetime.now(timezone.utc).isoformat()
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    # Get provider calculation timestamp or default to execution time
    last_update = data.get("time_last_update_unix")
    rate_timestamp = (
        datetime.fromtimestamp(last_update, tz=timezone.utc).isoformat()
        if last_update
        else ingested_at
    )
 
    records = []
    for country_code in countries:
        country_iso3 = country_code.upper()
        currency_code = COUNTRY_ISO3_TO_CURRENCY.get(country_iso3)

        if not currency_code:
            continue # Skip unmapped country codes

        rate = conversion_rates.get(currency_code)
        if rate is None:
            continue    # Currency rate unavailable in API

        records.append({
            "country_id": country_iso3,
            "currency_code": currency_code,
            "base_currency": "USD",
            "fx_rate_usd": float(rate),
            "rate_timestamp": rate_timestamp,
            "loaded_at": ingested_at,
            "source": "ExchangeRate-API",
            "ingestion_date": ingestion_date
        })

    return records


# ─────────────────────────────────────────────────────────────────────────────
# BIGQUERY
# ─────────────────────────────────────────────────────────────────────────────

def get_or_create_table(client: bigquery.Client) -> bigquery.Table:
    """
    Ensure the raw BigQuery table exists. Creates it with partitioning
    and clustering if not.
    """
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    try:
        table = client.get_table(table_ref)
        log.info(f"Table exists: {table_ref}")
        return table
    except Exception:
        pass

    log.info(f"Creating table: {table_ref}")
    table = bigquery.Table(table_ref, schema=BQ_SCHEMA)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingestion_date",
    )
    table.clustering_fields = ["country_id", "currency_code"]
    table.description = (
        "Raw fx rates for 54 African countries. "
        "Includes current Exchange rates. "
        "Unmodified source records — use staging models for analysis."
    )

    try:
        table = client.create_table(table)
        log.info(f"Created: {table_ref}")
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
    Write records to BigQuery targeting today's partition with WRITE_TRUNCATE.
    Idempotent — re-running on the same day overwrites, never duplicates.
    """
    if not records:
        log.warning("No records to load.")
        return 0

    if dry_run:
        log.info(f"[DRY RUN] Would load {len(records)} records to BigQuery.")
        log.info(f"[DRY RUN] Sample record:\n{json.dumps(records[0], indent=2)}")
        return len(records)

    table_ref  = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    today      = datetime.now(timezone.utc).date().strftime("%Y%m%d")
    BATCH_SIZE = 10_000
    total_loaded = 0

    for i in range(0, len(records), BATCH_SIZE):
        batch     = records[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = -(-len(records) // BATCH_SIZE)

        # Truncate partition on first batch, append on subsequent batches
        write_disp = (
            bigquery.WriteDisposition.WRITE_TRUNCATE
            if batch_num == 1
            else bigquery.WriteDisposition.WRITE_APPEND
        )

        job_config = bigquery.LoadJobConfig(
            schema = BQ_SCHEMA,
            write_disposition = write_disp,
            source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )

        log.info(f"Loading batch {batch_num}/{total_batches} ({len(batch)} rows)...")

        try:
            job = client.load_table_from_json(
                batch,
                f"{table_ref}${today}",
                job_config=job_config,
            )
            job.result()

            if job.errors:
                log.error(f"BigQuery errors in batch {batch_num}: {job.errors}")
            else:
                total_loaded += len(batch)
                log.info(f"  Batch {batch_num} loaded.")

        except GoogleCloudError as e:
            log.error(f"BigQuery load failed: {e}")
            raise

    return total_loaded


# ─────────────────────────────────────────────────────────────────────────────
# VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

def validate_records(records: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Basic pre-load validation. Returns (valid, invalid).
    """
    valid, invalid = [], []

    for r in records:
        errors = []

        if not r.get("currency_code"):
            errors.append("Missing currency_code")
        if not r.get("country_id"):
            errors.append("Missing country_id")
        if not r.get("base_currency"):
            errors.append("Missing base_currency")

        rate = r.get("fx_rate_usd")
        if rate is None:
            errors.append("Missing fx_rate_usd")
        elif not isinstance(rate, (float, int)) or rate <= 0:
            errors.append(f"Invalid fx_rate_usd: {rate}")

        if not r.get("rate_timestamp"):
            errors.append("Missing rate_timestamp")

        if errors:
            log.warning(f"Invalid record {errors}: {str(r)[:100]}")
            invalid.append(r)
        else:
            valid.append(r)

    if invalid:
        log.warning(f"Validation: {len(valid)} valid, {len(invalid)} invalid.")
    else:
        log.info(f"Validation: all {len(valid)} records passed.")

    return valid, invalid


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run(
    countries: list[str],
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Main ingestion run. Returns a summary dict for Airflow logging.
    """
    start_time = time.time()
    log.info("=" * 70)
    log.info("African Market Intelligence — ExchangeRate-API Ingestion")
    log.info(f"Target Countries  : {len(countries)}")
    log.info(f"Dry run    : {dry_run}")
    log.info(f"BQ target  : {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    log.info("=" * 70)

    # 1. Fetch single daily payload (contains all currency pairs)
    log.info("\nFetching global exchange rates...")
    all_records= fetch_exchange_rate(countries)
    log.info(f"Total records processed: {len(all_records)}")

    # 2. Validate extracted records
    valid_records, invalid_records = validate_records(all_records)

    # 3. Load to BigQuery or simulate dry run
    loaded = 0
    if valid_records:
        if dry_run:
            log.info(f"[DRY RUN] Processed {len(valid_records)} valid records.")
            loaded = len(valid_records)
        else:
            bq_client = bigquery.Client(project=GCP_PROJECT)
            get_or_create_table(bq_client)
            loaded = load_to_bigquery(bq_client, valid_records, dry_run=False)

    elapsed = round(time.time() - start_time, 1)

    summary = {
        "status":            "success",
        "countries":         len(countries),
        "records_fetched":   len(all_records),
        "records_valid":     len(valid_records),
        "records_invalid":   len(invalid_records),
        "records_loaded":    loaded,
        "duration_seconds":  elapsed,
        "dry_run":           dry_run,
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
        description="Ingest ExchangeRate-API data for 54 African countries into BigQuery."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and validate but do not write to BigQuery.",
    )
    parser.add_argument(
        "--country",
        type=str,
        default=None,
        help="Run for a single country (ISO3 code, e.g. KEN). Default: all 54.",
    )
    parser.add_argument(
        "--indicator",
        type=str,
        default=None,
        help="Run for a single indicator (e.g. PCPIPCH). Default: all.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    TARGET_COUNTRIES = [
            "AGO", "BDI", "BEN", "BFA", "CAF", "CIV", "CMR", "COD", "COG", "COM",
            "CPV", "DJI", "DZA", "EGY", "ERI", "ETH", "GAB", "GHA", "GIN", "GMB",
            "GNB", "GNQ", "KEN", "LBR", "LBY", "LSO", "MAR", "MDG", "MLI", "MOZ",
            "MRT", "MUS", "MWI", "NAM", "NER", "NGA", "RWA", "SDN", "SEN", "SLE",
            "SOM", "SSD", "STP", "SWZ", "SYC", "TCD", "TGO", "TUN", "TZA", "UGA",
            "ZAF", "ZMB", "ZWE"
        ]

    # ── Resolve countries ─────────────────────────────────────────
    if args.country:
        code = args.country.upper()
        if code not in TARGET_COUNTRIES :
            log.error(
                f"'{code}' is not in the supported African countries list. "
                f"Valid options: {', '.join(TARGET_COUNTRIES    )}"
            )
            sys.exit(1)
        countries = [code]
    else:
        countries = TARGET_COUNTRIES    

    # ── Validate env ──────────────────────────────────────────────
    if not args.dry_run:
        if not GCP_PROJECT:
            log.error("GCP_PROJECT is not set. Copy .env.example to .env and fill in your values.")
            sys.exit(1)

    # ── Run ───────────────────────────────────────────────────────
    try:
        summary = run(countries, dry_run=args.dry_run)
        if summary["records_invalid"] > 0:
            log.warning(f"{summary['records_invalid']} invalid records were skipped.")
        sys.exit(0)

    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)

    except Exception as e:
        log.exception(f"Ingestion failed: {e}")
        sys.exit(1)