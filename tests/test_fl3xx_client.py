from datetime import datetime, timezone

import pytest

from fl3xx_client import Fl3xxApiConfig, compute_fetch_dates, fetch_flights


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code

    def json(self):
        return self._payload

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise RuntimeError("HTTP error")


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def get(self, url, params=None, headers=None, timeout=None, verify=None):
        self.calls.append(
            {
                "url": url,
                "params": params,
                "headers": headers,
                "timeout": timeout,
                "verify": verify,
            }
        )
        return self.response


def test_compute_fetch_dates_uses_today_and_two_days_later():
    reference = datetime(2025, 10, 2, 12, 0, tzinfo=timezone.utc)
    start, end = compute_fetch_dates(reference)
    assert start.isoformat() == "2025-10-02"
    assert end.isoformat() == "2025-10-04"


def test_fetch_flights_builds_expected_request_parameters():
    response = FakeResponse([
        {"bookingIdentifier": "TEST"},
    ])
    session = FakeSession(response)
    config = Fl3xxApiConfig(api_token="token123")
    now = datetime(2025, 10, 2, 5, 0, tzinfo=timezone.utc)

    flights, metadata = fetch_flights(config, session=session, now=now)

    assert flights == [{"bookingIdentifier": "TEST"}]
    assert metadata["from_date"] == "2025-10-02"
    assert metadata["to_date"] == "2025-10-04"
    assert metadata["time_zone"] == "UTC"
    assert metadata["value"] == "ALL"
    assert metadata["hash"]
    assert metadata["request_url"] == config.base_url

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == config.base_url
    assert call["params"]["from"] == "2025-10-02"
    assert call["params"]["to"] == "2025-10-04"
    assert call["params"]["timeZone"] == "UTC"
    assert call["params"]["value"] == "ALL"
    assert call["headers"]["Authorization"] == "Bearer token123"
    assert call["timeout"] == config.timeout
    assert call["verify"] == config.verify_ssl


def test_build_headers_supports_custom_auth_header_name():
    config = Fl3xxApiConfig(auth_header="Token abc123", auth_header_name="X-Auth-Token")

    headers = config.build_headers()

    assert headers["X-Auth-Token"] == "Token abc123"
    assert "Authorization" not in headers or headers["Authorization"] != "Token abc123"


def test_fetch_flights_accepts_payload_wrapped_in_items_list():
    payload = {"items": [{"bookingIdentifier": "ABC"}]}
    response = FakeResponse(payload)
    session = FakeSession(response)
    config = Fl3xxApiConfig(api_token="token123")
    flights, _ = fetch_flights(config, session=session)
    assert flights == [{"bookingIdentifier": "ABC"}]


def test_fetch_flights_raises_for_unexpected_payload_structure():
    response = FakeResponse({"unexpected": "value"})
    session = FakeSession(response)
    config = Fl3xxApiConfig(api_token="token123")
    with pytest.raises(ValueError):
        fetch_flights(config, session=session)
