import csv
import importlib
from pathlib import Path

import pytest
import requests

from ingestion import fx_rates


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self.payload


def test_retries_transient_http_response(monkeypatch, capsys):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", None)
    responses = iter(
        [
            FakeResponse(429, {}),
            FakeResponse(
                200,
                {
                    "result": "success",
                    "time_last_update_unix": 1792108801,
                    "conversion_rates": {"KES": 129.75},
                },
            ),
        ]
    )
    calls = []

    def get(url, **kwargs):
        calls.append((url, kwargs["timeout"]))
        return next(responses)

    monkeypatch.setattr(
        fx_rates.requests,
        "get",
        get,
    )
    waits = []
    monkeypatch.setattr(fx_rates.time, "sleep", waits.append)

    records = fx_rates.fetch_exchange_rate(["KEN"])

    assert len(calls) == 2
    assert calls[0][0] == fx_rates.FX_FREE_TIER_URL
    assert waits == [fx_rates.RETRY_BACKOFF]
    assert records[0]["country_id"] == "KEN"
    assert records[0]["currency_code"] == "KES"
    assert records[0]["fx_rate_usd"] == 129.75
    assert "Data:" not in capsys.readouterr().out


def test_retries_connection_error(monkeypatch):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", None)
    responses = iter(
        [
            requests.exceptions.ConnectionError("connection reset"),
            FakeResponse(200, {"conversion_rates": {"KES": 129.75}}),
        ]
    )

    def get(url, timeout):
        assert url == fx_rates.FX_FREE_TIER_URL
        assert timeout == 10
        response = next(responses)
        if isinstance(response, Exception):
            raise response
        return response

    monkeypatch.setattr(fx_rates.requests, "get", get)
    waits = []
    monkeypatch.setattr(fx_rates.time, "sleep", waits.append)

    assert len(fx_rates.fetch_exchange_rate(["KEN"])) == 1
    assert waits == [fx_rates.RETRY_BACKOFF]


def test_raises_exchange_rate_api_error(monkeypatch):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", "test-api-key")

    def get(url, timeout):
        assert url == f"{fx_rates.FX_BASE_URL}test-api-key/latest/USD"
        assert timeout == 10
        return FakeResponse(200, {"result": "error", "error-type": "invalid-key"})

    monkeypatch.setattr(
        fx_rates.requests,
        "get",
        get,
    )

    with pytest.raises(ValueError, match="invalid-key"):
        fx_rates.fetch_exchange_rate(["KEN"])


def test_rejects_success_response_without_rates(monkeypatch):
    monkeypatch.setattr(fx_rates, "FX_RATES_KEY", None)

    def get(url, timeout):
        assert url == fx_rates.FX_FREE_TIER_URL
        assert timeout == 10
        return FakeResponse(200, {"result": "success"})

    monkeypatch.setattr(fx_rates.requests, "get", get)

    with pytest.raises(ValueError, match="missing conversion rates"):
        fx_rates.fetch_exchange_rate(["KEN"])


def test_reads_fx_specific_table_environment_variable(monkeypatch):
    monkeypatch.setenv("BQ_FX_TABLE", "custom_fx_rates")
    monkeypatch.setenv("BQ_IMF_TABLE", "wrong_table")
    original_table = fx_rates.BQ_TABLE
    try:
        importlib.reload(fx_rates)
        assert fx_rates.BQ_TABLE == "custom_fx_rates"
    finally:
        fx_rates.BQ_TABLE = original_table


def test_country_seed_covers_all_supported_fx_countries():
    seed_path = Path(__file__).parents[1] / "dbt" / "seeds" / "dim_country_seed.csv"
    with seed_path.open(newline="") as seed_file:
        seed_rows = list(csv.DictReader(seed_file))

    seed_countries = {
        row["country_code"]: row["currency_code"]
        for row in seed_rows
    }
    iso2_codes = {row["iso2_code"] for row in seed_rows}

    assert len(seed_countries) == 54
    assert len(iso2_codes) == 54
    assert seed_countries == fx_rates.COUNTRY_ISO3_TO_CURRENCY


def test_fx_targets_every_seeded_country():
    seed_path = Path(__file__).parents[1] / "dbt" / "seeds" / "dim_country_seed.csv"
    with seed_path.open(newline="") as seed_file:
        seed_countries = {row["country_code"] for row in csv.DictReader(seed_file)}

    assert len(fx_rates.TARGET_COUNTRIES) == 54
    assert "BWA" in fx_rates.TARGET_COUNTRIES
    assert set(fx_rates.TARGET_COUNTRIES) == seed_countries
