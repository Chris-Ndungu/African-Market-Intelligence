from unittest.mock import Mock

import pytest
import requests

from ingestion import fx_rates, imf, world_bank


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


@pytest.mark.parametrize("module", [world_bank, imf])
def test_retries_rate_limit_and_server_error(module, monkeypatch):
    responses = iter(
        [
            FakeResponse(429, {}),
            FakeResponse(503, {}),
            FakeResponse(200, {"ok": True}),
        ]
    )
    get = Mock(side_effect=lambda *args, **kwargs: next(responses))
    waits = []
    monkeypatch.setattr(module.requests, "get", get)
    monkeypatch.setattr(module.time, "sleep", waits.append)

    result = module.fetch_with_retry("https://example.test", {}, retries=3)

    assert result == {"ok": True}
    assert get.call_count == 3
    assert waits == [module.RETRY_BACKOFF**1, module.RETRY_BACKOFF**2]


def test_fx_retries_rate_limit_and_server_error(monkeypatch):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", None)
    responses = iter(
        [
            FakeResponse(429, {}),
            FakeResponse(503, {}),
            FakeResponse(200, {"rates": {"KES": 129.75}}),
        ]
    )
    get = Mock(side_effect=lambda *args, **kwargs: next(responses))
    waits = []
    monkeypatch.setattr(fx_rates.requests, "get", get)
    monkeypatch.setattr(fx_rates.time, "sleep", waits.append)

    records = fx_rates.fetch_exchange_rate(["KEN"])

    assert get.call_count == 3
    assert waits == [fx_rates.RETRY_BACKOFF**1, fx_rates.RETRY_BACKOFF**2]
    assert records[0]["fx_rate_usd"] == 129.75


def test_world_bank_parses_sample_and_skips_non_year_date(monkeypatch):
    payload = [
        {"pages": 1, "total": 3},
        [
            {
                "countryiso3code": "KEN",
                "country": {"id": "KE", "value": "Kenya"},
                "date": "2024",
                "value": 5.25,
            },
            {
                "countryiso3code": "KEN",
                "country": {"id": "KE", "value": "Kenya"},
                "date": "2023",
                "value": None,
            },
            {
                "countryiso3code": "KEN",
                "country": {"id": "KE", "value": "Kenya"},
                "date": "2024Q1",
                "value": 3.0,
            },
        ],
    ]
    monkeypatch.setattr(world_bank, "fetch_with_retry", lambda *args, **kwargs: payload)
    monkeypatch.setattr(world_bank.time, "sleep", lambda _: None)

    records = world_bank.fetch_indicator_for_countries(
        "GDP_CURRENT_USD",
        {"code": "NY.GDP.MKTP.CD", "name": "GDP", "category": "GDP", "unit": "USD"},
        ["KEN"],
    )

    assert [record["year"] for record in records] == [2024, 2023]
    assert records[0]["value"] == 5.25
    assert records[1]["value"] is None
    valid, invalid = world_bank.validate_records(records)
    assert valid == records
    assert invalid == []


def test_imf_parses_sample_and_skips_non_year_and_unknown_country(monkeypatch):
    payload = {
        "values": {
            "PCPIPCH": {
                "KEN": {"2024": 4.8, "2023Q1": 5.1, "2022": None},
                "USA": {"2024": 2.9},
            }
        }
    }
    monkeypatch.setattr(imf, "fetch_with_retry", lambda *args, **kwargs: payload)

    records = imf.fetch_indicator(
        "PCPIPCH",
        {"name": "Inflation", "category": "INFLATION", "unit": "Percent"},
        ["KEN"],
    )

    assert [record["year"] for record in records] == [2024, 2022]
    assert records[0]["value"] == 4.8
    assert records[1]["value"] is None
    valid, invalid = imf.validate_records(records)
    assert valid == records
    assert invalid == []


def test_fx_skips_unmapped_currencies_and_null_rates(monkeypatch):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", None)
    monkeypatch.setattr(
        fx_rates.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(
            200,
            {"rates": {"KES": None, "NGN": 1330.0}},
        ),
    )

    records = fx_rates.fetch_exchange_rate(["KEN", "NGA", "XXX"])

    assert len(records) == 1
    assert records[0]["country_id"] == "NGA"
    assert records[0]["currency_code"] == "NGN"


def test_world_bank_indicator_catalog_includes_credit_and_mobile_coverage():
    assert world_bank.INDICATORS["PRIVATE_CREDIT_GDP"] == {
        "code": "FS.AST.PRVT.GD.ZS",
        "name": "Domestic credit to private sector (% of GDP)",
        "unit": "%",
        "category": "FINANCIAL",
    }
    assert world_bank.INDICATORS["MOBILE_SUBSCRIPTIONS"]["code"] == "IT.CEL.SETS.P2"


@pytest.mark.parametrize(
    ("validator", "record", "required_fields"),
    [
        (
            world_bank.validate_records,
            {
                "record_id": "sample",
                "country_code": "KEN",
                "indicator_key": "GDP",
                "year": 2024,
                "value": 1.0,
            },
            ("record_id", "country_code", "indicator_key", "year"),
        ),
        (
            imf.validate_records,
            {
                "record_id": "sample",
                "country_code": "KEN",
                "indicator_code": "PCPIPCH",
                "year": 2024,
                "value": 1.0,
            },
            ("record_id", "country_code", "indicator_code", "year"),
        ),
        (
            fx_rates.validate_records,
            {
                "country_id": "KEN",
                "currency_code": "KES",
                "base_currency": "USD",
                "fx_rate_usd": 129.0,
                "rate_timestamp": "2024-01-01T00:00:00+00:00",
            },
            (
                "country_id",
                "currency_code",
                "base_currency",
                "fx_rate_usd",
                "rate_timestamp",
            ),
        ),
    ],
)
def test_validators_reject_records_missing_required_fields(
    validator, record, required_fields
):
    for required_field in required_fields:
        incomplete_record = record.copy()
        del incomplete_record[required_field]

        valid, invalid = validator([incomplete_record])

        assert valid == []
        assert invalid == [incomplete_record]


@pytest.mark.parametrize("module", [world_bank, imf, fx_rates])
def test_table_lookup_errors_other_than_not_found_are_propagated(module, monkeypatch):
    monkeypatch.setattr(module, "GCP_PROJECT", "test-project")
    client = Mock()
    client.get_table.side_effect = PermissionError("permission denied")

    with pytest.raises(PermissionError, match="permission denied"):
        module.get_or_create_table(client)

    client.create_table.assert_not_called()


@pytest.mark.parametrize(
    ("module", "fetch_name", "run_args", "expected_summary_key"),
    [
        (
            world_bank,
            "fetch_indicator_for_countries",
            (["KEN"], {"GDP": {"code": "NY.GDP.MKTP.CD"}}),
            "records_fetched",
        ),
        (
            imf,
            "fetch_indicator",
            (["KEN"], {"PCPIPCH": {"name": "Inflation"}}),
            "records_fetched",
        ),
        (
            fx_rates,
            "fetch_exchange_rate",
            (["KEN"],),
            "records_fetched",
        ),
    ],
)
def test_dry_run_does_not_create_bigquery_client(
    module, fetch_name, run_args, expected_summary_key, monkeypatch
):
    fetch = Mock(
        return_value=[
            {
                "record_id": "sample",
                "country_code": "KEN",
                "country_id": "KEN",
                "indicator_key": "GDP",
                "indicator_code": "PCPIPCH",
                "year": 2024,
                "value": 1.0,
                "currency_code": "KES",
                "base_currency": "USD",
                "fx_rate_usd": 129.0,
                "rate_timestamp": "2024-01-01T00:00:00+00:00",
            }
        ]
    )
    monkeypatch.setattr(module, fetch_name, fetch)
    monkeypatch.setattr(
        module.bigquery,
        "Client",
        Mock(side_effect=AssertionError("dry run must not connect to BigQuery")),
    )
    monkeypatch.setattr(module.time, "sleep", lambda _: None)

    if module is world_bank:
        summary = module.run(*run_args, dry_run=True)
    elif module is imf:
        summary = module.run(run_args[0], run_args[1], dry_run=True)
    else:
        summary = module.run(*run_args, dry_run=True)

    assert summary["dry_run"] is True
    assert summary[expected_summary_key] == 1
    fetch.assert_called_once()
