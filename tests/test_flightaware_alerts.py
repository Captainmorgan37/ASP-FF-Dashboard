import pytest

from flightaware_alerts import (
    DEFAULT_FLIGHT_ALERT_EVENTS,
    FlightAwareApiConfig,
    configure_test_alerts,
    delete_alert_subscription,
    ensure_alert_subscription,
    list_alerts,
    set_default_alert_endpoint,
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

    def delete(self, url, *, headers=None, timeout=None, verify=None):
        self.calls.append(("DELETE", url, headers, None))
        return self._pop_response()

    def close(self):
        pass


@pytest.fixture
def default_config():
    return FlightAwareApiConfig(api_key="demo-key")


def test_list_alerts_normalises_various_payloads(default_config):
    payload = {
        "items": [
            {"id": "1", "ident": "ASP501", "events": ["OUT", "OFF"]},
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
        FakeResponse({"id": "abc", "ident": "ASP556", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ])

    alert = ensure_alert_subscription(default_config, "ASP556", session=session)

    assert alert.identifier == "ASP556"
    assert alert.events == DEFAULT_FLIGHT_ALERT_EVENTS

    assert session.calls[0][0] == "GET"
    assert session.calls[1][0] == "POST"
    assert session.calls[1][3]["ident"] == "ASP556"
    assert session.calls[1][3]["events"] == DEFAULT_FLIGHT_ALERT_EVENTS
    assert session.calls[1][2]["x-apikey"] == "demo-key"


def test_ensure_alert_subscription_updates_existing_when_events_differ(default_config):
    existing = {"id": "alert-1", "ident": "ASP668", "events": ["out"]}
    session = FakeSession([
        FakeResponse([existing]),  # list existing alerts
        FakeResponse({"id": "alert-1", "ident": "ASP668", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ])

    alert = ensure_alert_subscription(default_config, "ASP668", session=session)

    assert alert.events == DEFAULT_FLIGHT_ALERT_EVENTS
    assert session.calls[1][0] == "PUT"
    assert session.calls[1][1].endswith("/alert-1")


def test_ensure_alert_subscription_updates_when_target_url_changes(default_config):
    existing = {
        "id": "alert-1",
        "ident": "ASP668",
        "events": DEFAULT_FLIGHT_ALERT_EVENTS,
        "target_url": "https://old.example",
    }
    session = FakeSession([
        FakeResponse([existing]),
        FakeResponse({
            "id": "alert-1",
            "ident": "ASP668",
            "events": DEFAULT_FLIGHT_ALERT_EVENTS,
            "target_url": "https://new.example",
        }),
    ])

    alert = ensure_alert_subscription(
        default_config,
        "ASP668",
        target_url="https://new.example",
        session=session,
    )

    assert alert.target_url == "https://new.example"
    assert session.calls[1][0] == "PUT"
    assert session.calls[1][3]["target_url"] == "https://new.example"


def test_ensure_alert_subscription_no_update_when_target_url_matches(default_config):
    existing = {
        "id": "alert-1",
        "ident": "ASP668",
        "events": DEFAULT_FLIGHT_ALERT_EVENTS,
        "target_url": "https://same.example",
    }
    session = FakeSession([
        FakeResponse([existing]),
    ])

    alert = ensure_alert_subscription(
        default_config,
        "ASP668",
        target_url="https://same.example",
        session=session,
    )

    assert alert.target_url == "https://same.example"
    assert len(session.calls) == 1
    assert session.calls[0][0] == "GET"


def test_configure_test_alerts_reuses_session(default_config):
    responses = [
        FakeResponse([]),
        FakeResponse({"id": "1", "ident": "ASP501", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
        FakeResponse([]),
        FakeResponse({"id": "2", "ident": "ASP653", "events": DEFAULT_FLIGHT_ALERT_EVENTS}),
    ]
    session = FakeSession(responses)

    alerts = configure_test_alerts(default_config, ["ASP501", "ASP653"], session=session)

    assert [alert.identifier for alert in alerts] == ["ASP501", "ASP653"]
    assert [call[0] for call in session.calls] == ["GET", "POST", "GET", "POST"]


def test_set_default_alert_endpoint_sends_payload(default_config):
    session = FakeSession([
        FakeResponse({"status": "ok"}),
    ])

    result = set_default_alert_endpoint(default_config, "https://alerts.example", session=session)

    assert result == {"status": "ok"}
    assert session.calls[0][0] == "PUT"
    assert session.calls[0][1].endswith("/endpoint")
    assert session.calls[0][3] == {"target_url": "https://alerts.example"}


def test_delete_alert_subscription_invokes_delete(default_config):
    session = FakeSession([
        FakeResponse({}, 204),
    ])

    delete_alert_subscription(default_config, "alert-1", session=session)

    assert session.calls[0][0] == "DELETE"
    assert session.calls[0][1].endswith("/alert-1")
