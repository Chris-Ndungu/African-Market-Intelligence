"""Helpers for idempotent, partitioned BigQuery ingestion."""

from datetime import date

from google.cloud import bigquery


def load_partitioned_records(
    client: bigquery.Client,
    records: list[dict],
    schema: list[bigquery.SchemaField],
    table_ref: str,
    clustering_fields: list[str],
    batch_size: int = 10_000,
) -> int:
    """Replace date partitions using load jobs matching the table clustering."""
    if not records:
        return 0
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    table_parts = table_ref.split(".")
    if len(table_parts) != 3 or any(not part.replace("-", "").replace("_", "").isalnum() for part in table_parts):
        raise ValueError(f"Invalid BigQuery table reference: {table_ref}")

    records_by_date: dict[str, list[dict]] = {}
    for record in records:
        ingestion_date = record.get("ingestion_date")
        if hasattr(ingestion_date, "isoformat"):
            ingestion_date = ingestion_date.isoformat()
        if not isinstance(ingestion_date, str):
            raise TypeError("Each record must have ingestion_date in YYYY-MM-DD format")
        try:
            date_key = date.fromisoformat(ingestion_date).strftime("%Y%m%d")
        except ValueError as exc:
            raise ValueError(
                f"Invalid ingestion_date: {ingestion_date!r}; expected YYYY-MM-DD"
            ) from exc
        records_by_date.setdefault(date_key, []).append(record)

    total_loaded = 0
    for date_key, partition_records in records_by_date.items():
        for offset in range(0, len(partition_records), batch_size):
            batch = partition_records[offset : offset + batch_size]
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field="ingestion_date",
                ),
                clustering_fields=clustering_fields,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=(
                    bigquery.WriteDisposition.WRITE_TRUNCATE
                    if offset == 0
                    else bigquery.WriteDisposition.WRITE_APPEND
                ),
            )
            load_job = client.load_table_from_json(
                batch,
                f"{table_ref}${date_key}",
                job_config=job_config,
            )
            load_job.result()
            if load_job.errors:
                raise RuntimeError(f"BigQuery load failed: {load_job.errors}")
            total_loaded += len(batch)

    return total_loaded