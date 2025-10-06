from __future__ import annotations

from datetime import datetime, timezone

import pytest

from flightaware_status import (
    FlightAwareStatusConfig,
    build_status_payload,
    derive_event_times,
    fetch_flights_for_ident,
    parse_timestamp,
)


class DummyResponse:
    def __init__(self, payload, *, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self._closed = False

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, expected_url: str, payload):
        self.expected_url = expected_url
        self.payload = payload
        self.closed = False
        self.requests = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        if url != self.expected_url:
            raise AssertionError(f"Unexpected URL {url}")
        return DummyResponse(self.payload)

    def close(self):
        self.closed = True


def test_fetch_flights_for_ident_handles_nested_payload():
    config = FlightAwareStatusConfig(api_key="demo")
    payload = {
        "data": [
            {"ident": "ASP1", "actual_out": "2024-01-01T10:00:00Z"},
            {"ident": "ASP2", "actual_out": "2024-01-02T10:00:00Z"},
        ]
    }
    session = DummySession(config.build_url("flights/ASP1"), payload)

    flights = fetch_flights_for_ident(config, "ASP1", session=session)

    assert len(flights) == 2
    assert flights[0]["ident"] == "ASP1"
    assert not session.closed
    assert session.requests[0][1]["headers"]["x-apikey"] == "demo"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("2024-01-01T00:00:00Z", datetime(2024, 1, 1, tzinfo=timezone.utc)),
        ({"epoch": 1704067200}, datetime(2024, 1, 1, tzinfo=timezone.utc)),
        ({"iso": "2024-01-01T00:00:00+02:00"}, datetime(2023, 12, 31, 22, tzinfo=timezone.utc)),
        (1704067200, datetime(2024, 1, 1, tzinfo=timezone.utc)),
        (None, None),
        ("", None),
    ],
)
def test_parse_timestamp(value, expected):
    assert parse_timestamp(value) == expected


def test_derive_event_times_prioritises_actual():
    payload = {
        "actual_out": "2024-01-01T01:02:00Z",
        "estimated_in": "2024-01-01T03:00:00Z",
        "actual_in": "2024-01-01T02:58:00Z",
        "edct_out": {"epoch": 1704066000},
    }
    events = derive_event_times(payload)

    assert events["Departure"].isoformat() == "2024-01-01T01:02:00+00:00"
    assert events["Arrival"].isoformat() == "2024-01-01T02:58:00+00:00"
    assert events["ArrivalForecast"].isoformat() == "2024-01-01T03:00:00+00:00"
    assert events["EDCT"].isoformat() == "2023-12-31T23:40:00+00:00"


def test_build_status_payload_adds_deltas():
    schedule_out = datetime(2024, 1, 1, 1, 0, tzinfo=timezone.utc)
    schedule_in = datetime(2024, 1, 1, 3, 0, tzinfo=timezone.utc)
    events = {
        "Departure": datetime(2024, 1, 1, 1, 5, tzinfo=timezone.utc),
        "Arrival": datetime(2024, 1, 1, 3, 10, tzinfo=timezone.utc),
        "ArrivalForecast": datetime(2024, 1, 1, 3, 5, tzinfo=timezone.utc),
        "EDCT": datetime(2024, 1, 1, 0, 50, tzinfo=timezone.utc),
    }

    status_map = build_status_payload(events, scheduled_departure=schedule_out, scheduled_arrival=schedule_in)

    assert status_map["Departure"]["delta_min"] == 5
    assert status_map["Arrival"]["delta_min"] == 10
    assert status_map["ArrivalForecast"]["delta_min"] == 5
    assert "EDCT" not in status_map  # actual departure suppresses EDCT marker

