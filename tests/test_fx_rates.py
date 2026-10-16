import importlib

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
    assert waits == [fx_rates.RETRY_BACKOFF]
    assert records[0]["country_id"] == "KEN"
    assert records[0]["currency_code"] == "KES"
    assert records[0]["fx_rate_usd"] == 129.75
    assert "Data:" not in capsys.readouterr().out


def test_retries_connection_error(monkeypatch):
    responses = iter(
        [
            requests.exceptions.ConnectionError("connection reset"),
            FakeResponse(200, {"conversion_rates": {"KES": 129.75}}),
        ]
    )

    def get(url, timeout):
        assert url.startswith(fx_rates.FX_BASE_URL)
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
    def get(url, timeout):
        assert url.startswith(fx_rates.FX_BASE_URL)
        assert timeout == 10
        return FakeResponse(200, {"result": "error", "error-type": "invalid-key"})

    monkeypatch.setattr(
        fx_rates.requests,
        "get",
        get,
    )

    with pytest.raises(ValueError, match="invalid-key"):
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
