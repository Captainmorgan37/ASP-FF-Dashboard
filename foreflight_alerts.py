"""Utilities for configuring ForeFlight flight alert subscriptions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Mapping, Optional, Sequence

import requests

DEFAULT_FOREFLIGHT_ALERTS_URL = "https://api.foreflight.com/alerts/v1/alerts"
DEFAULT_FLIGHT_ALERT_EVENTS = ["out", "off", "on", "in"]


@dataclass(frozen=True)
class ForeFlightApiConfig:
    """Configuration for interacting with the ForeFlight Alerts API."""

    base_url: str = DEFAULT_FOREFLIGHT_ALERTS_URL
    api_key: Optional[str] = None
    auth_header: Optional[str] = None
    auth_header_name: str = "Authorization"
    api_key_scheme: Optional[str] = "Bearer"
    extra_headers: Mapping[str, str] | None = None
    verify_ssl: bool = True
    timeout: int = 30

    def build_headers(self) -> Mapping[str, str]:
        headers = {"Accept": "application/json"}
        if self.extra_headers:
            headers.update(dict(self.extra_headers))

        header_name = self.auth_header_name or "Authorization"
        if self.auth_header:
            headers[header_name] = self.auth_header
        elif self.api_key:
            token = str(self.api_key)
            scheme = (self.api_key_scheme or "").strip()
            headers[header_name] = f"{scheme} {token}".strip() if scheme else token
        return headers

    @property
    def alerts_url(self) -> str:
        return self.base_url.rstrip("/")


@dataclass
class ForeFlightAlert:
    """Representation of a ForeFlight flight alert subscription."""

    identifier: str
    events: List[str]
    alert_id: Optional[str] = None
    label: Optional[str] = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, object]) -> "ForeFlightAlert":
        identifier = str(
            payload.get("identifier")
            or payload.get("tail")
            or payload.get("aircraft")
            or payload.get("registration")
            or ""
        ).strip()
        events = [str(evt).lower() for evt in payload.get("events", []) if isinstance(evt, str)]
        alert_id = payload.get("id") or payload.get("uuid")
        label = payload.get("label") or payload.get("name")
        return cls(
            identifier=identifier,
            events=events,
            alert_id=str(alert_id) if alert_id else None,
            label=str(label) if label else None,
        )


def _normalise_alerts(payload: object) -> List[ForeFlightAlert]:
    if payload is None:
        return []
    if isinstance(payload, Mapping):
        for key in ("items", "alerts", "data", "results"):
            if key in payload and isinstance(payload[key], Iterable):
                return _normalise_alerts(payload[key])
        # A mapping could represent a single alert definition
        return [ForeFlightAlert.from_payload(payload)]
    if isinstance(payload, Iterable):
        alerts: List[ForeFlightAlert] = []
        for item in payload:
            if isinstance(item, Mapping):
                alerts.append(ForeFlightAlert.from_payload(item))
        return alerts
    raise ValueError("Unsupported ForeFlight alerts payload structure")


def list_alerts(config: ForeFlightApiConfig, *, session: Optional[requests.Session] = None) -> List[ForeFlightAlert]:
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


def _build_alert_payload(tail: str, events: Sequence[str], label: Optional[str]) -> Mapping[str, object]:
    seen = set()
    clean_events = []
    for event in events:
        lower = str(event).lower()
        if lower not in seen:
            seen.add(lower)
            clean_events.append(lower)
    body: dict[str, object] = {
        "identifier": tail,
        "events": clean_events,
    }
    if label:
        body["label"] = label
    return body


def _alert_matches_tail(alert: ForeFlightAlert, tail: str) -> bool:
    return alert.identifier.replace("-", "").upper() == tail.replace("-", "").upper()


def ensure_alert_subscription(
    config: ForeFlightApiConfig,
    tail: str,
    *,
    events: Optional[Sequence[str]] = None,
    label: Optional[str] = None,
    session: Optional[requests.Session] = None,
) -> ForeFlightAlert:
    """Ensure that an alert subscription exists for the requested tail.

    The function returns the resulting :class:`ForeFlightAlert` definition.
    If an alert already exists but differs from the desired configuration it
    will be updated in-place.
    """

    desired_events = list(events or DEFAULT_FLIGHT_ALERT_EVENTS)
    http = session or requests.Session()
    close_session = session is None
    try:
        existing_alerts = list_alerts(config, session=http)
        match = next((alert for alert in existing_alerts if _alert_matches_tail(alert, tail)), None)
        payload = _build_alert_payload(tail, desired_events, label)

        if match and sorted(match.events) == sorted(evt.lower() for evt in desired_events) and (
            label is None or match.label == label
        ):
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
        return ForeFlightAlert.from_payload(response.json())
    finally:
        if close_session:
            http.close()


def configure_test_alerts(
    config: ForeFlightApiConfig,
    tails: Iterable[str],
    *,
    events: Optional[Sequence[str]] = None,
    label_prefix: str = "Test Flight Alert",
    session: Optional[requests.Session] = None,
) -> List[ForeFlightAlert]:
    """Create or update alert subscriptions for a collection of tail numbers."""

    http = session or requests.Session()
    close_session = session is None
    try:
        results: List[ForeFlightAlert] = []
        for tail in tails:
            label = f"{label_prefix} {tail}".strip()
            alert = ensure_alert_subscription(
                config,
                tail,
                events=events,
                label=label,
                session=http,
            )
            results.append(alert)
        return results
    finally:
        if close_session:
            http.close()
