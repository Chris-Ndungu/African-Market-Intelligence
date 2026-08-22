"""
imf.py
─────────────────────────────────────────────────────────────────────────────
Ingests macroeconomic indicators for 54 African countries from the IMF
DataMapper API (v1) and loads them into BigQuery (raw layer).

IMF DataMapper API docs: https://www.imf.org/external/datamapper/api/help

Usage:
    python imf.py                         # ingest all indicators, all countries
    python imf.py --dry-run               # fetch and validate, skip BQ write
    python imf.py --country KEN           # single country (ISO3 code)
    python imf.py --indicator PCPIPCH     # single indicator code

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
BQ_DATASET  = os.getenv("BQ_RAW_DATASET", "raw_african_macro")
BQ_TABLE    = os.getenv("BQ_IMF_TABLE",   "raw_imf")
BQ_LOCATION = os.getenv("BQ_LOCATION",    "US")

# ── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── CONSTANTS ─────────────────────────────────────────────────────────────────
IMF_BASE_URL  = "https://www.imf.org/external/datamapper/api/v1"
MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0    # seconds — doubles on each retry
REQUEST_DELAY = 0.6    # seconds between requests — IMF rate limit is ~10/5s

# ── 54 AFRICAN COUNTRIES (ISO3 codes — IMF uses ISO3) ─────────────────────────
AFRICAN_COUNTRIES_ISO3 = [
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COG", "COD", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO", "TUN",
    "UGA", "ZMB", "ZWE", "SYC",
]

# ── INDICATORS ────────────────────────────────────────────────────────────────
# IMF DataMapper indicator codes + metadata
# Browse all available: https://www.imf.org/external/datamapper/api/v1/indicators
INDICATORS = {
    "PCPIPCH": {
        "name":     "Inflation, end of period consumer prices (Annual % change)",
        "unit":     "%",
        "category": "INFLATION",
    },
    "PCPIEPCH": {
        "name":     "Inflation, average consumer prices (Annual % change)",
        "unit":     "%",
        "category": "INFLATION",
    },
    "NGDP_RPCH": {
        "name":     "Real GDP growth (Annual % change)",
        "unit":     "%",
        "category": "GDP",
    },
    "NGDPDPC": {
        "name":     "GDP per capita, current prices (USD)",
        "unit":     "USD",
        "category": "GDP",
    },
    "NGDPD": {
        "name":     "GDP, current prices (USD billions)",
        "unit":     "USD_BN",
        "category": "GDP",
    },
    "BCA_NGDPD": {
        "name":     "Current account balance (% of GDP)",
        "unit":     "%",
        "category": "TRADE",
    },
    "GGXWDG_NGDP": {
        "name":     "General government gross debt (% of GDP)",
        "unit":     "%",
        "category": "DEBT",
    },
    "GGXCNL_NGDP": {
        "name":     "General government net lending/borrowing (% of GDP)",
        "unit":     "%",
        "category": "FISCAL",
    },
    "LUR": {
        "name":     "Unemployment rate (% of total labor force)",
        "unit":     "%",
        "category": "LABOUR",
    },
    "LP": {
        "name":     "Population (millions of persons)",
        "unit":     "MILLIONS",
        "category": "DEMOGRAPHICS",
    },
}

# ── BIGQUERY SCHEMA ───────────────────────────────────────────────────────────
BQ_SCHEMA = [
    bigquery.SchemaField("record_id",       "STRING",    "REQUIRED"),
    bigquery.SchemaField("country_code",    "STRING",    "REQUIRED"),  # ISO3
    bigquery.SchemaField("indicator_code",  "STRING",    "REQUIRED"),  # IMF code e.g. PCPIPCH
    bigquery.SchemaField("indicator_name",  "STRING",    "NULLABLE"),
    bigquery.SchemaField("category",        "STRING",    "NULLABLE"),
    bigquery.SchemaField("year",            "INTEGER",   "REQUIRED"),
    bigquery.SchemaField("value",           "FLOAT64",   "NULLABLE"),
    bigquery.SchemaField("unit",            "STRING",    "NULLABLE"),
    bigquery.SchemaField("is_forecast",     "BOOLEAN",   "NULLABLE"),  # IMF includes forecasts
    bigquery.SchemaField("source",          "STRING",    "REQUIRED"),
    bigquery.SchemaField("raw_response",    "STRING",    "NULLABLE"),
    bigquery.SchemaField("loaded_at",       "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("ingestion_date",  "DATE",      "REQUIRED"),
]

CURRENT_YEAR = datetime.now().year


# ─────────────────────────────────────────────────────────────────────────────
# API FETCHING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_with_retry(url: str, params: dict | None, retries: int = MAX_RETRIES) -> dict | None:
    """
    GET a URL with automatic retry on transient errors.
    Returns parsed JSON or None on total failure.
    """
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, params=params, timeout=30)

            if response.status_code == 200:
                return response.json()

            if response.status_code == 429:
                wait = RETRY_BACKOFF ** attempt
                log.warning(f"Rate limited (429). Waiting {wait}s — retry {attempt}/{retries}.")
                time.sleep(wait)
                continue

            if response.status_code >= 500:
                wait = RETRY_BACKOFF ** attempt
                log.warning(f"Server error {response.status_code}. Waiting {wait}s — retry {attempt}/{retries}.")
                time.sleep(wait)
                continue

            log.error(f"Status {response.status_code} for: {url}")
            return None

        except requests.exceptions.Timeout:
            log.warning(f"Timeout (attempt {attempt}/{retries}): {url}")
            time.sleep(RETRY_BACKOFF ** attempt)

        except requests.exceptions.ConnectionError as e:
            log.warning(f"Connection error (attempt {attempt}/{retries}): {e}")
            time.sleep(RETRY_BACKOFF ** attempt)

    log.error(f"All {retries} retries exhausted: {url}")
    return None


def fetch_indicator(
    indicator_code: str,
    indicator_meta: dict,
    countries: list[str],
    start_year: int = 2010,
) -> list[dict]:
    """
    Fetch a single IMF indicator for all African countries.

    IMF DataMapper URL pattern:
        /api/v1/{indicator_code}/{country1}/{country2}/...?periods=2000,2001,...

    The API returns a nested structure:
        {
          "values": {
            "PCPIPCH": {
              "KEN": { "2020": 5.3, "2021": 6.1, ... },
              "NGA": { "2020": 12.9, ... },
              ...
            }
          }
        }

    IMF includes forecast years beyond the current year — we flag these
    with is_forecast=True so dbt models can filter them if needed.
    """
    loaded_at      = datetime.now(timezone.utc)
    ingestion_date = loaded_at.date().isoformat()
    records        = []
    skipped        = 0

    # Build years list: 2000 to current year + 2 (IMF includes near-term forecasts)
    years = list(range(start_year, CURRENT_YEAR + 3))
    periods_param = ",".join(str(y) for y in years)

    # IMF accepts countries in the URL path: /PCPIPCH/KEN/NGA/ZAF/...
    country_path = "/".join(countries)
    url = f"{IMF_BASE_URL}/{indicator_code}"
    params = {"periods": periods_param}

    log.info(f"  Fetching: {url}")
    raw = fetch_with_retry(url, params)

    if raw is None:
        log.error(f"  Failed to fetch {indicator_code}. Skipping.")
        return []

    # ── Parse response ────────────────────────────────────────────
    values_block = raw.get("values", {})
    indicator_data = values_block.get(indicator_code, {})

    if not indicator_data:
        log.warning(f"  {indicator_code}: empty values block in response.")
        return []

    for country_code, year_values in indicator_data.items():

        # Skip entries that aren't in our target country list
        if country_code not in countries:
            skipped += 1
            continue

        if not isinstance(year_values, dict):
            skipped += 1
            continue

        for year_str, value_raw in year_values.items():

            try:
                if not year_str.isdigit():
                    skipped += 1
                    continue

                year = int(year_str)

                # Years beyond current year are IMF forecasts
                is_forecast = year > CURRENT_YEAR

                value = float(value_raw) if value_raw is not None else None

                record_id = hashlib.md5(
                    f"{country_code}|{indicator_code}|{year}".encode()
                ).hexdigest()

                records.append({
                    "record_id":      record_id,
                    "country_code":   country_code,
                    "indicator_code": indicator_code,
                    "indicator_name": indicator_meta["name"],
                    "category":       indicator_meta["category"],
                    "year":           year,
                    "value":          value,
                    "unit":           indicator_meta["unit"],
                    "is_forecast":    is_forecast,
                    "source":         "IMF DataMapper",
                    "raw_response":   json.dumps({country_code: {year_str: value_raw}}),
                    "loaded_at":      loaded_at.isoformat(),
                    "ingestion_date": ingestion_date,
                })

            except (ValueError, TypeError) as e:
                log.warning(f"  Skipping malformed entry ({country_code}, {year_str}): {e}")
                skipped += 1
                continue

    log.info(
        f"  {indicator_code}: {len(records)} records parsed, "
        f"{skipped} skipped."
    )
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
    table.clustering_fields = ["country_code", "indicator_code"]
    table.description = (
        "Raw IMF DataMapper macroeconomic indicators for 54 African countries. "
        "Includes both historical data and near-term IMF forecasts (is_forecast=TRUE). "
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

    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingestion_date",
        ),
        clustering_fields=["country_code", "indicator_code"],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    for i in range(0, len(records), BATCH_SIZE):
        batch     = records[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = -(-len(records) // BATCH_SIZE)

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

        if not r.get("record_id"):
            errors.append("Missing record_id")
        if not r.get("country_code"):
            errors.append("Missing country_code")
        if not r.get("indicator_code"):
            errors.append("Missing indicator_code")
        if not isinstance(r.get("year"), int) or not (1960 <= r["year"] <= 2035):
            errors.append(f"Invalid year: {r.get('year')}")
        if r.get("value") is not None and not isinstance(r["value"], float):
            errors.append(f"Value is not float: {type(r.get('value'))}")

        if errors:
            log.debug(f"Invalid record {errors}: {str(r)[:100]}")
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
    indicators: dict,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Main ingestion run. Returns a summary dict for Airflow logging.
    """
    start_time = time.time()
    log.info("=" * 70)
    log.info("African Market Intelligence — IMF DataMapper Ingestion")
    log.info(f"Countries  : {len(countries)}")
    log.info(f"Indicators : {len(indicators)}")
    log.info(f"Dry run    : {dry_run}")
    log.info(f"BQ target  : {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    log.info("=" * 70)

    all_records: list[dict] = []

    for idx, (indicator_code, indicator_meta) in enumerate(indicators.items(), 1):
        log.info(f"\n[{idx}/{len(indicators)}] {indicator_code} — {indicator_meta['name']}")

        records = fetch_indicator(indicator_code, indicator_meta, countries)
        all_records.extend(records)

        # Respect IMF rate limit between indicator requests
        time.sleep(REQUEST_DELAY)

    log.info(f"\nTotal records fetched: {len(all_records)}")

    valid_records, invalid_records = validate_records(all_records)

    loaded = 0
    if valid_records:
        bq_client = None if dry_run else bigquery.Client(project=GCP_PROJECT)
        if bq_client:
            get_or_create_table(bq_client)

            loaded = load_to_bigquery(bq_client, valid_records, dry_run=dry_run)

    elapsed = round(time.time() - start_time, 1)

    summary = {
        "status":            "success",
        "countries":         len(countries),
        "indicators":        len(indicators),
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
        description="Ingest IMF DataMapper macroeconomic data for 54 African countries into BigQuery."
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

    # ── Resolve countries ─────────────────────────────────────────
    if args.country:
        code = args.country.upper()
        if code not in AFRICAN_COUNTRIES_ISO3:
            log.error(
                f"'{code}' is not in the supported African countries list. "
                f"Valid options: {', '.join(AFRICAN_COUNTRIES_ISO3)}"
            )
            sys.exit(1)
        countries = [code]
    else:
        countries = AFRICAN_COUNTRIES_ISO3

    # ── Resolve indicators ────────────────────────────────────────
    if args.indicator:
        key = args.indicator.upper()
        if key not in INDICATORS:
            log.error(
                f"'{key}' is not a recognised indicator. "
                f"Valid options: {', '.join(INDICATORS.keys())}"
            )
            sys.exit(1)
        indicators = {key: INDICATORS[key]}
    else:
        indicators = INDICATORS

    # ── Validate env ──────────────────────────────────────────────
    if not args.dry_run:
        if not GCP_PROJECT:
            log.error("GCP_PROJECT is not set. Copy .env.example to .env and fill in your values.")
            sys.exit(1)

    # ── Run ───────────────────────────────────────────────────────
    try:
        summary = run(countries, indicators, dry_run=args.dry_run)
        if summary["records_invalid"] > 0:
            log.warning(f"{summary['records_invalid']} invalid records were skipped.")
        sys.exit(0)

    except KeyboardInterrupt:
        log.info("Interrupted.")
        sys.exit(0)

    except Exception as e:
        log.exception(f"Ingestion failed: {e}")
        sys.exit(1)