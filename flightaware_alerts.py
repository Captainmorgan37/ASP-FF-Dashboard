"""Utilities for configuring FlightAware AeroAPI flight alert subscriptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional, Sequence

import requests

DEFAULT_FLIGHTAWARE_ALERTS_URL = "https://aeroapi.flightaware.com/aeroapi/alerts"
DEFAULT_FLIGHT_ALERT_EVENTS = ["out", "off", "on", "in"]


@dataclass(frozen=True)
class FlightAwareApiConfig:
    """Configuration for interacting with the FlightAware AeroAPI Alerts endpoint."""

    base_url: str = DEFAULT_FLIGHTAWARE_ALERTS_URL
    api_key: Optional[str] = None
    extra_headers: Mapping[str, str] | None = None
    verify_ssl: bool = True
    timeout: int = 30

    def build_headers(self) -> Mapping[str, str]:
        headers = {"Accept": "application/json"}
        if self.extra_headers:
            headers.update(dict(self.extra_headers))
        if self.api_key:
            headers.setdefault("x-apikey", str(self.api_key))
        return headers

    @property
    def alerts_url(self) -> str:
        return self.base_url.rstrip("/")


@dataclass
class FlightAwareAlert:
    """Representation of a FlightAware flight alert subscription."""

    identifier: str
    events: List[str]
    alert_id: Optional[str] = None
    description: Optional[str] = None
    target_url: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "FlightAwareAlert":
        identifier = str(
            payload.get("identifier")
            or payload.get("ident")
            or payload.get("tail")
            or payload.get("registration")
            or payload.get("aircraft")
            or ""
        ).strip()
        events = [str(evt).lower() for evt in payload.get("events", []) if isinstance(evt, str)]
        alert_id = payload.get("id") or payload.get("alert_id") or payload.get("uuid")
        description = payload.get("description") or payload.get("label") or payload.get("name")
        target_url = (
            payload.get("target_url")
            or payload.get("url")
            or payload.get("delivery_url")
            or payload.get("targetURL")
        )
        return cls(
            identifier=identifier,
            events=events,
            alert_id=str(alert_id) if alert_id else None,
            description=str(description) if description else None,
            target_url=str(target_url) if target_url else None,
        )


def _normalise_alerts(payload: object) -> List[FlightAwareAlert]:
    if payload is None:
        return []
    if isinstance(payload, Mapping):
        for key in ("items", "alerts", "data", "results"):
            if key in payload and isinstance(payload[key], Iterable):
                return _normalise_alerts(payload[key])
        return [FlightAwareAlert.from_payload(payload)]
    if isinstance(payload, Iterable):
        alerts: List[FlightAwareAlert] = []
        for item in payload:
            if isinstance(item, Mapping):
                alerts.append(FlightAwareAlert.from_payload(item))
        return alerts
    raise ValueError("Unsupported FlightAware alerts payload structure")


def list_alerts(
    config: FlightAwareApiConfig,
    *,
    session: Optional[requests.Session] = None,
) -> List[FlightAwareAlert]:
    """Return the current alert subscriptions configured for the account."""

    http = session or requests.Session()
    response = http.get(
        config.alerts_url,
        headers=config.build_headers(),
        timeout=config.timeout,
        verify=config.verify_ssl,
    )
    response.raise_for_status()
    payload = response.json()
    return _normalise_alerts(payload)


def _build_alert_payload(
    tail: str,
    events: Sequence[str],
    description: Optional[str],
    target_url: Optional[str],
) -> Mapping[str, object]:
    seen = set()
    clean_events = []
    for event in events:
        lower = str(event).lower()
        if lower not in seen:
            seen.add(lower)
            clean_events.append(lower)
    body: dict[str, object] = {
        "ident": tail,
        "events": clean_events,
    }
    if description:
        body["description"] = description
    if target_url:
        body["target_url"] = target_url
    return body


def _alert_matches_tail(alert: FlightAwareAlert, tail: str) -> bool:
    return alert.identifier.replace("-", "").upper() == tail.replace("-", "").upper()


def ensure_alert_subscription(
    config: FlightAwareApiConfig,
    tail: str,
    *,
    events: Optional[Sequence[str]] = None,
    description: Optional[str] = None,
    target_url: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> FlightAwareAlert:
    """Ensure that an alert subscription exists for the requested tail.

    The function returns the resulting :class:`FlightAwareAlert` definition.
    If an alert already exists but differs from the desired configuration it
    will be updated in-place.
    """

    desired_events = list(events or DEFAULT_FLIGHT_ALERT_EVENTS)
    http = session or requests.Session()
    close_session = session is None
    try:
        existing_alerts = list_alerts(config, session=http)
        match = next((alert for alert in existing_alerts if _alert_matches_tail(alert, tail)), None)
        payload = _build_alert_payload(tail, desired_events, description, target_url)

        if match and sorted(match.events) == sorted(evt.lower() for evt in desired_events) and (
            description is None or match.description == description
        ) and (target_url is None or match.target_url == target_url):
            return match

        if match and match.alert_id:
            url = f"{config.alerts_url}/{match.alert_id}"
            response = http.put(
                url,
                json=payload,
                headers=config.build_headers(),
                timeout=config.timeout,
                verify=config.verify_ssl,
            )
        else:
            response = http.post(
                config.alerts_url,
                json=payload,
                headers=config.build_headers(),
                timeout=config.timeout,
                verify=config.verify_ssl,
            )
        response.raise_for_status()
        return FlightAwareAlert.from_payload(response.json())
    finally:
        if close_session:
            http.close()


def configure_test_alerts(
    config: FlightAwareApiConfig,
    tails: Iterable[str],
    *,
    events: Optional[Sequence[str]] = None,
    description_prefix: str = "Test Flight Alert",
    target_url: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> List[FlightAwareAlert]:
    """Create or update alert subscriptions for a collection of tail numbers."""

    http = session or requests.Session()
    close_session = session is None
    try:
        results: List[FlightAwareAlert] = []
        for tail in tails:
            description = f"{description_prefix} {tail}".strip()
            alert = ensure_alert_subscription(
                config,
                tail,
                events=events,
                description=description,
                target_url=target_url,
                session=http,
            )
            results.append(alert)
        return results
    finally:
        if close_session:
            http.close()


def set_default_alert_endpoint(
    config: FlightAwareApiConfig,
    target_url: str,
    *,
    session: Optional[requests.Session] = None,
) -> Mapping[str, object]:
    """Configure the account-wide default delivery endpoint for alerts."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.put(
            f"{config.alerts_url}/endpoint",
            json={"target_url": target_url},
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return response.json() or {"status": "ok"}
    finally:
        if close_session:
            http.close()


def delete_alert_subscription(
    config: FlightAwareApiConfig,
    alert_id: str,
    *,
    session: Optional[requests.Session] = None,
) -> None:
    """Delete a configured alert subscription by identifier."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.delete(
            f"{config.alerts_url}/{alert_id}",
            headers=config.build_headers(),
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
    finally:
        if close_session:
            http.close()
