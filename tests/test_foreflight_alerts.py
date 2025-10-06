import pytest

from foreflight_alerts import (
    DEFAULT_FLIGHT_ALERT_EVENTS,
    ForeFlightAlert,
    ForeFlightApiConfig,
    configure_test_alerts,
    ensure_alert_subscription,
    list_alerts,
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
    def __init__(self, responses=None):
        self.responses = responses or []
        self.calls = []

    def _pop_response(self):
        if not self.responses:
            raise AssertionError("No more responses configured")
        return self.responses.pop(0)

    def get(self, url, *, headers=None, timeout=None, verify=None):
        self.calls.append(("GET", url, headers, None))
        return self._pop_response()

    def post(self, url, *, json=None, headers=None, timeout=None, verify=None):
        self.calls.append(("POST", url, headers, json))
        return self._pop_response()

    def put(self, url, *, json=None, headers=None, timeout=None, verify=None):
        self.calls.append(("PUT", url, headers, json))
        return self._pop_response()

    def close(self):
        pass


@pytest.fixture
def default_config():
    return ForeFlightApiConfig(api_key="demo-key")


def test_list_alerts_normalises_various_payloads(default_config):
    payload = {
        "items": [
            {"id": "1", "identifier": "ASP501", "events": ["OUT", "OFF"]},
            {"uuid": "2", "tail": "ASP653", "events": ["ON", "IN"]},
        ]
    }
    session = FakeSession([FakeResponse(payload)])

    alerts = list_alerts(default_config, session=session)

    assert [alert.identifier for alert in alerts] == ["ASP501", "ASP653"]
    assert alerts[0].events == ["out", "off"]
    assert alerts[1].events == ["on", "in"]


def test_ensure_alert_subscription_creates_new_alert(default_config):
    session = FakeSession([
        FakeResponse([]),  # list existing alerts
        FakeResponse({"id": "abc", "identifier": "ASP556", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ])

    alert = ensure_alert_subscription(default_config, "ASP556", session=session)

    assert alert.identifier == "ASP556"
    assert alert.events == DEFAULT_FLIGHT_ALERT_EVENTS

    assert session.calls[0][0] == "GET"
    assert session.calls[1][0] == "POST"
    assert session.calls[1][3]["identifier"] == "ASP556"
    assert session.calls[1][3]["events"] == DEFAULT_FLIGHT_ALERT_EVENTS


def test_ensure_alert_subscription_updates_existing_when_events_differ(default_config):
    existing = {"id": "alert-1", "identifier": "ASP668", "events": ["out"]}
    session = FakeSession([
        FakeResponse([existing]),  # list existing alerts
        FakeResponse({"id": "alert-1", "identifier": "ASP668", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ])

    alert = ensure_alert_subscription(default_config, "ASP668", session=session)

    assert alert.events == DEFAULT_FLIGHT_ALERT_EVENTS
    assert session.calls[1][0] == "PUT"
    assert session.calls[1][1].endswith("/alert-1")


def test_configure_test_alerts_reuses_session(default_config):
    responses = [
        FakeResponse([]),
        FakeResponse({"id": "1", "identifier": "ASP501", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
        FakeResponse([]),
        FakeResponse({"id": "2", "identifier": "ASP653", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ]
    session = FakeSession(responses)

    alerts = configure_test_alerts(default_config, ["ASP501", "ASP653"], session=session)

    assert [alert.identifier for alert in alerts] == ["ASP501", "ASP653"]
    assert [call[0] for call in session.calls] == ["GET", "POST", "GET", "POST"]
