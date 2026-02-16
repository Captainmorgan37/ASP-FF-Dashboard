from datetime import datetime, timezone

import pytest

from fl3xx_client import (
    Fl3xxApiConfig,
    compute_fetch_dates,
    enrich_flights_with_crew,
    enrich_flights_with_postflight_delay_codes,
    fetch_flight_crew,
    fetch_flight_postflight,
    fetch_flights,
)


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
    def __init__(self, response=None, response_map=None):
        self.response = response
        self.response_map = response_map or {}
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
        if self.response_map:
            if url not in self.response_map:
                raise AssertionError(f"Unexpected URL requested: {url}")
            return self.response_map[url]
        if self.response is None:
            raise AssertionError("No response configured for FakeSession")
        return self.response

    def close(self):  # pragma: no cover - compatibility shim
        pass


def test_compute_fetch_dates_overlap_previous_day_and_following_day():
    reference = datetime(2025, 10, 2, 12, 0, tzinfo=timezone.utc)
    start, end = compute_fetch_dates(reference)
    assert start.isoformat() == "2025-10-01"
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
    assert metadata["from_date"] == "2025-10-01"
    assert metadata["to_date"] == "2025-10-04"
    assert metadata["time_zone"] == "UTC"
    assert metadata["value"] == "ALL"
    assert metadata["hash"]
    assert metadata["request_url"] == config.base_url

    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == config.base_url
    assert call["params"]["from"] == "2025-10-01"
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


def test_build_headers_supports_custom_token_scheme_and_header_name():
    config = Fl3xxApiConfig(
        api_token="abc123",
        auth_header_name="X-Auth-Token",
        api_token_scheme="Token",
    )

    headers = config.build_headers()

    assert headers["X-Auth-Token"] == "Token abc123"


def test_build_headers_allows_empty_token_scheme():
    config = Fl3xxApiConfig(api_token="abc123", api_token_scheme="")

    headers = config.build_headers()

    assert headers["Authorization"] == "abc123"


def test_build_headers_defaults_to_raw_token_for_custom_header_name():
    config = Fl3xxApiConfig(api_token="abc123", auth_header_name="X-Auth-Token")

    headers = config.build_headers()

    assert headers["X-Auth-Token"] == "abc123"


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


def test_fetch_flight_crew_uses_expected_endpoint():
    crew_payload = [
        {"role": "CMD", "firstName": "Stuart", "lastName": "Weaver"},
    ]
    expected_url = "https://app.fl3xx.us/api/external/flight/123/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    crew = fetch_flight_crew(config, 123, session=session)

    assert crew == crew_payload
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"] == expected_url
    assert call["headers"]["Authorization"] == "Bearer token123"


def test_fetch_flight_crew_handles_payload_wrapped_in_items_mapping():
    crew_payload = {
        "items": {
            "1": {"role": "CMD", "firstName": "Alex", "lastName": "Doe"},
            "2": {"role": "FO", "firstName": "Jamie", "lastName": "Smith"},
        }
    }
    expected_url = "https://app.fl3xx.us/api/external/flight/456/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    crew = fetch_flight_crew(config, 456, session=session)

    assert crew == [
        {"role": "CMD", "firstName": "Alex", "lastName": "Doe"},
        {"role": "FO", "firstName": "Jamie", "lastName": "Smith"},
    ]


def test_fetch_flight_crew_supports_crew_members_key():
    crew_payload = {
        "crewMembers": [
            {"role": "CMD", "firstName": "Alex", "lastName": "Doe"},
            {"role": "FO", "firstName": "Jamie", "lastName": "Smith"},
        ]
    }
    expected_url = "https://app.fl3xx.us/api/external/flight/789/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    crew = fetch_flight_crew(config, 789, session=session)

    assert crew == [
        {"role": "CMD", "firstName": "Alex", "lastName": "Doe"},
        {"role": "FO", "firstName": "Jamie", "lastName": "Smith"},
    ]


def test_fetch_flight_crew_supports_crews_key():
    crew_payload = {
        "flightId": 123456,
        "externalReference": None,
        "crews": [
            {
                "role": "CMD",
                "firstName": "Stuart",
                "middleName": "Reid",
                "lastName": "Weaver",
                "logName": "sweaver@airsprint.com",
            },
            {
                "role": "FO",
                "firstName": "Jason",
                "middleName": "Alexander",
                "lastName": "MacNeil",
                "logName": "jmacneil@airsprint.com",
            },
        ],
    }
    expected_url = "https://app.fl3xx.us/api/external/flight/123456/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    crew = fetch_flight_crew(config, 123456, session=session)

    assert crew == crew_payload["crews"]


def test_fetch_flight_crew_returns_empty_list_for_none_items():
    crew_payload = {"items": None}
    expected_url = "https://app.fl3xx.us/api/external/flight/987/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    crew = fetch_flight_crew(config, 987, session=session)

    assert crew == []


def test_enrich_flights_with_crew_populates_names():
    flights = [{"flightId": 123}]
    crew_payload = [
        {"role": "CMD", "firstName": "Stuart", "lastName": "Weaver"},
        {"role": "FO", "firstName": "Jason", "lastName": "MacNeil"},
    ]
    expected_url = "https://app.fl3xx.us/api/external/flight/123/crew"
    session = FakeSession(response_map={expected_url: FakeResponse(crew_payload)})
    config = Fl3xxApiConfig(api_token="token123")

    summary = enrich_flights_with_crew(config, flights, force=True, session=session)

    assert summary["fetched"] == 1
    assert summary["updated"] is True
    assert flights[0]["picName"] == "Stuart Weaver"
    assert flights[0]["sicName"] == "Jason MacNeil"
    assert flights[0]["crewMembers"] == crew_payload


def test_enrich_flights_with_crew_skips_when_names_present_and_not_forced():
    flights = [{"flightId": 123, "picName": "Existing PIC", "sicName": "Existing SIC"}]
    session = FakeSession()
    config = Fl3xxApiConfig(api_token="token123")

    summary = enrich_flights_with_crew(config, flights, force=False, session=session)

    assert summary["fetched"] == 0
    assert summary["updated"] is False
    assert len(session.calls) == 0


def test_fetch_flight_postflight_uses_expected_endpoint():
    payload = {"time": {"dep": {"delayOffBlockReasons": ["Flow|EDCT|ATC"]}}}
    expected_url = "https://app.fl3xx.us/api/external/flight/321/postflight"
    session = FakeSession(response_map={expected_url: FakeResponse(payload)})
    config = Fl3xxApiConfig(api_token="token123")

    postflight = fetch_flight_postflight(config, 321, session=session)

    assert postflight == payload
    assert len(session.calls) == 1
    assert session.calls[0]["url"] == expected_url


def test_enrich_flights_with_postflight_delay_codes_only_fetches_eligible_flights():
    flights = [
        {
            "flightId": 1,
            "blockOffEstUTC": "2026-02-16T15:00:00Z",
            "realDateOUT": "2026-02-16T15:20:00Z",
        },
        {
            "flightId": 2,
            "blockOffEstUTC": "2026-02-16T15:00:00Z",
            "realDateOUT": "2026-02-16T15:10:00Z",
        },
    ]
    payload = {"time": {"dep": {"delayOffBlockReasons": ["Flow|EDCT|ATC"]}}}
    expected_url = "https://app.fl3xx.us/api/external/flight/1/postflight"
    session = FakeSession(response_map={expected_url: FakeResponse(payload)})
    config = Fl3xxApiConfig(api_token="token123")

    summary = enrich_flights_with_postflight_delay_codes(config, flights, delay_threshold_minutes=15, session=session)

    assert summary["eligible"] == 1
    assert summary["fetched"] == 1
    assert summary["updated"] is True
    assert len(session.calls) == 1
    assert flights[0]["delayOffBlockReasons"] == ["Flow|EDCT|ATC"]
    assert flights[0]["delayOffBlockReason"] == "Flow|EDCT|ATC"
    assert "delayOffBlockReasons" not in flights[1]


def test_enrich_flights_with_postflight_delay_codes_skips_when_reason_already_present():
    flights = [
        {
            "flightId": 1,
            "blockOffEstUTC": "2026-02-16T15:00:00Z",
            "realDateOUT": "2026-02-16T15:20:00Z",
            "delayOffBlockReasons": ["Crew"],
        }
    ]
    session = FakeSession()
    config = Fl3xxApiConfig(api_token="token123")

    summary = enrich_flights_with_postflight_delay_codes(config, flights, delay_threshold_minutes=15, session=session)

    assert summary["eligible"] == 1
    assert summary["fetched"] == 0
    assert summary["updated"] is False
    assert len(session.calls) == 0


def test_enrich_flights_with_postflight_delay_codes_only_attempts_once_when_no_reasons_returned():
    flights = [
        {
            "flightId": 1,
            "blockOffEstUTC": "2026-02-16T15:00:00Z",
            "realDateOUT": "2026-02-16T15:20:00Z",
        }
    ]
    payload = {"time": {"dep": {"delayOffBlockReasons": []}}}
    expected_url = "https://app.fl3xx.us/api/external/flight/1/postflight"
    session = FakeSession(response_map={expected_url: FakeResponse(payload)})
    config = Fl3xxApiConfig(api_token="token123")

    first = enrich_flights_with_postflight_delay_codes(config, flights, delay_threshold_minutes=15, session=session)
    second = enrich_flights_with_postflight_delay_codes(config, flights, delay_threshold_minutes=15, session=session)

    assert first["fetched"] == 1
    assert second["fetched"] == 0
    assert len(session.calls) == 1
    assert flights[0]["postflightAttemptedAt"]
    assert flights[0]["delayOffBlockReasons"] == []

