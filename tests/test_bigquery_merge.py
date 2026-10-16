from unittest.mock import MagicMock

from google.cloud import bigquery

from ingestion.bigquery_merge import load_partitioned_records


def test_loads_replace_partition_then_append_batches_without_dml():
    client = MagicMock()
    load_job = MagicMock(errors=None)
    client.load_table_from_json.return_value = load_job
    schema = [
        bigquery.SchemaField("record_id", "STRING"),
        bigquery.SchemaField("value", "FLOAT64"),
        bigquery.SchemaField("ingestion_date", "DATE"),
    ]
    records = [
        {"record_id": "one", "value": 1.0, "ingestion_date": "2026-10-03"},
        {"record_id": "two", "value": 2.0, "ingestion_date": "2026-10-03"},
    ]

    loaded = load_partitioned_records(
        client,
        records,
        schema,
        "project.dataset.raw_metrics",
        clustering_fields=["record_id", "value"],
        batch_size=1,
    )

    assert loaded == 2
    assert client.load_table_from_json.call_count == 2
    first_config = client.load_table_from_json.call_args_list[0].kwargs["job_config"]
    second_config = client.load_table_from_json.call_args_list[1].kwargs["job_config"]
    assert first_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE
    assert second_config.write_disposition == bigquery.WriteDisposition.WRITE_APPEND

    assert client.load_table_from_json.call_args_list[0].args[1] == (
        "project.dataset.raw_metrics$20261003"
    )
    assert client.load_table_from_json.call_args_list[1].args[1] == (
        "project.dataset.raw_metrics$20261003"
    )
    assert first_config.time_partitioning.field == "ingestion_date"
    assert first_config.clustering_fields == ["record_id", "value"]
    client.query.assert_not_called()
    client.delete_table.assert_not_called()