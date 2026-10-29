from unittest.mock import Mock

import pytest

from ingestion import world_bank


def _entry(country_code, year, value):
    return {
        "countryiso3code": country_code,
        "country": {"id": country_code, "value": country_code},
        "date": str(year),
        "value": value,
    }


def test_world_bank_fetches_country_chunks_and_all_pages(monkeypatch):
    requests = []

    def fetch(url, params):
        requests.append((url, params.copy()))
        if params["page"] == 2:
            return [{"pages": 2, "total": 2}, [_entry("NGA", 2023, 2.5)]]
        if "C000;C001;C002;C003;C004;C005;C006;C007;C008;C009" in url:
            return [{"pages": 2, "total": 2}, [_entry("KEN", 2024, 1.5)]]
        return [{"pages": 1, "total": 1}, [_entry("ZAF", 2022, None)]]

    monkeypatch.setattr(world_bank, "fetch_with_retry", fetch)
    monkeypatch.setattr(world_bank.time, "sleep", Mock())

    records = world_bank.fetch_indicator_for_countries(
        "GDP",
        {"code": "NY.GDP.MKTP.CD", "name": "GDP", "category": "GDP", "unit": "USD"},
        [f"C{index:03d}" for index in range(10)] + ["C010"],
        start_year=2000,
    )

    assert len(requests) == 3
    assert [params["page"] for _, params in requests] == [1, 2, 1]
    assert requests[0][1]["date"] == "2000:2030"
    assert "C010" in requests[2][0]
    assert [(record["country_code"], record["year"]) for record in records] == [
        ("KEN", 2024),
        ("NGA", 2023),
        ("ZAF", 2022),
    ]
    assert records[0]["value"] == 1.5
    assert records[2]["value"] is None
    assert records[0]["source"] == "World Bank"
    assert world_bank.validate_records(records) == (records, [])


def test_world_bank_rejects_unrecoverable_http_status_without_retry(monkeypatch):
    get = Mock(return_value=Mock(status_code=404))
    monkeypatch.setattr(world_bank.requests, "get", get)
    monkeypatch.setattr(world_bank.time, "sleep", Mock())

    assert world_bank.fetch_with_retry("https://example.test", {}, retries=3) is None
    get.assert_called_once()


def test_world_bank_validation_rejects_bad_year_and_non_float_value():
    invalid_record = {
        "record_id": "bad-record",
        "country_code": "KEN",
        "indicator_key": "GDP",
        "year": 1959,
        "value": 5,
    }

    valid, invalid = world_bank.validate_records([invalid_record])

    assert valid == []
    assert invalid == [invalid_record]


def test_world_bank_unexpected_api_response_fails_the_run(monkeypatch):
    # A malformed response must fail the run so a partial load never replaces
    # the day's partition.
    monkeypatch.setattr(world_bank, "fetch_with_retry", lambda *args, **kwargs: {"error": "bad"})
    monkeypatch.setattr(world_bank.time, "sleep", Mock())

    with pytest.raises(RuntimeError, match="Unexpected World Bank response for GDP"):
        world_bank.fetch_indicator_for_countries(
            "GDP",
            {"code": "NY.GDP.MKTP.CD", "name": "GDP", "category": "GDP", "unit": "USD"},
            ["KEN"],
        )


def test_world_bank_empty_data_block_returns_no_records(monkeypatch):
    monkeypatch.setattr(
        world_bank,
        "fetch_with_retry",
        lambda *args, **kwargs: [{"pages": 1, "total": 0}, None],
    )
    monkeypatch.setattr(world_bank.time, "sleep", Mock())

    records = world_bank.fetch_indicator_for_countries(
        "GDP",
        {"code": "NY.GDP.MKTP.CD", "name": "GDP", "category": "GDP", "unit": "USD"},
        ["KEN"],
    )

    assert records == []
