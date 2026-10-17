from unittest.mock import Mock

from ingestion import imf


def test_imf_parses_values_and_marks_forecast_years(monkeypatch):
    monkeypatch.setattr(imf, "CURRENT_YEAR", 2024)
    fetch = Mock(
        return_value={
            "values": {
                "PCPIPCH": {
                    "KEN": {
                        "2023": 6.1,
                        "2024": 4.8,
                        "2025": 5.2,
                        "2026": None,
                        "2024Q1": 7.0,
                        "bad-year": 9.0,
                    },
                    "USA": {"2024": 2.9},
                    "NGA": ["malformed"],
                }
            }
        }
    )
    monkeypatch.setattr(imf, "fetch_with_retry", fetch)

    records = imf.fetch_indicator(
        "PCPIPCH",
        {"name": "Inflation", "category": "INFLATION", "unit": "Percent"},
        ["KEN", "NGA"],
        start_year=2023,
    )

    fetch.assert_called_once()
    url, params = fetch.call_args.args
    assert url.endswith("/PCPIPCH")
    assert params == {"periods": "2023,2024,2025,2026"}
    assert [(record["year"], record["is_forecast"]) for record in records] == [
        (2023, False),
        (2024, False),
        (2025, True),
        (2026, True),
    ]
    assert [record["value"] for record in records] == [6.1, 4.8, 5.2, None]
    assert all(record["country_code"] == "KEN" for record in records)
    assert all(record["source"] == "IMF DataMapper" for record in records)
    assert imf.validate_records(records) == (records, [])


def test_imf_returns_empty_list_when_request_fails_or_values_are_empty(monkeypatch):
    monkeypatch.setattr(imf, "fetch_with_retry", lambda *args, **kwargs: None)

    failed = imf.fetch_indicator(
        "PCPIPCH",
        {"name": "Inflation", "category": "INFLATION", "unit": "Percent"},
        ["KEN"],
    )

    monkeypatch.setattr(
        imf,
        "fetch_with_retry",
        lambda *args, **kwargs: {"values": {"PCPIPCH": {}}},
    )
    empty = imf.fetch_indicator(
        "PCPIPCH",
        {"name": "Inflation", "category": "INFLATION", "unit": "Percent"},
        ["KEN"],
    )

    assert failed == []
    assert empty == []


def test_imf_rejects_unrecoverable_http_status_without_retry(monkeypatch):
    get = Mock(return_value=Mock(status_code=404))
    monkeypatch.setattr(imf.requests, "get", get)
    monkeypatch.setattr(imf.time, "sleep", Mock())

    assert imf.fetch_with_retry("https://example.test", {}, retries=3) is None
    get.assert_called_once()


def test_imf_validation_rejects_bad_year_and_non_float_value():
    invalid_record = {
        "record_id": "bad-record",
        "country_code": "KEN",
        "indicator_code": "PCPIPCH",
        "year": 2036,
        "value": 5,
    }

    valid, invalid = imf.validate_records([invalid_record])

    assert valid == []
    assert invalid == [invalid_record]
