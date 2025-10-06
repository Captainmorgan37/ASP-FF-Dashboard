"""Helpers for pulling recent FlightAware AeroAPI flight status data."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping, MutableMapping, Optional, Sequence

import requests
from dateutil import parser as dateparse


DEFAULT_AEROAPI_BASE_URL = "https://aeroapi.flightaware.com/aeroapi"


@dataclass(frozen=True)
class FlightAwareStatusConfig:
    """Configuration for querying FlightAware AeroAPI status endpoints."""

    base_url: str = DEFAULT_AEROAPI_BASE_URL
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

    def build_url(self, path: str) -> str:
        return f"{self.base_url.rstrip('/')}/{path.lstrip('/')}"


def _normalise_flights(payload: object) -> list[Mapping[str, object]]:
    if payload is None:
        return []
    if isinstance(payload, Mapping):
        for key in ("flights", "data", "results", "items"):
            if key in payload and isinstance(payload[key], Iterable):
                return _normalise_flights(payload[key])
        return [payload]
    if isinstance(payload, Iterable):
        flights: list[Mapping[str, object]] = []
        for item in payload:
            if isinstance(item, Mapping):
                flights.append(item)
        return flights
    raise ValueError("Unsupported FlightAware flights payload structure")


def fetch_flights_for_ident(
    config: FlightAwareStatusConfig,
    ident: str,
    *,
    session: Optional[requests.Session] = None,
    params: Optional[Mapping[str, object]] = None,
) -> list[Mapping[str, object]]:
    """Return the recent flight status records for the requested identifier."""

    http = session or requests.Session()
    close_session = session is None
    try:
        response = http.get(
            config.build_url(f"flights/{ident}"),
            headers=config.build_headers(),
            params=params,
            timeout=config.timeout,
            verify=config.verify_ssl,
        )
        response.raise_for_status()
        return _normalise_flights(response.json())
    finally:
        if close_session:
            http.close()


def _extract_mapping_value(value: object, keys: Sequence[str]) -> object:
    if not isinstance(value, Mapping):
        return None
    for key in keys:
        if key in value and value[key] is not None:
            return value[key]
    return None


def parse_timestamp(value: object) -> Optional[datetime]:
    """Parse heterogeneous timestamp representations from AeroAPI payloads."""

    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    if isinstance(value, Mapping):
        nested = _extract_mapping_value(
            value,
            [
                "time",
                "iso",
                "iso8601",
                "timestamp",
                "value",
                "datetime",
            ],
        )
        if nested is not None:
            return parse_timestamp(nested)
        epoch = _extract_mapping_value(value, ["epoch", "epoch_time", "epochtime"])
        if epoch is not None:
            try:
                return datetime.fromtimestamp(float(epoch), tz=timezone.utc)
            except (TypeError, ValueError):
                return None
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        try:
            dt = dateparse.parse(value)
        except (ValueError, TypeError):
            return None
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def _first_timestamp(payload: Mapping[str, object], keys: Sequence[str]) -> Optional[datetime]:
    for key in keys:
        if key in payload:
            ts = parse_timestamp(payload.get(key))
            if ts is not None:
                return ts
    return None


DEPARTURE_KEYS = (
    "actual_off",
    "actual_out",
    "estimated_out",
    "scheduled_out",
    "scheduled_off",
    "filed_departure_time",
)

ARRIVAL_KEYS = (
    "actual_on",
    "actual_in",
)

ETA_KEYS = (
    "estimated_in",
    "estimated_on",
    "scheduled_in",
    "scheduled_on",
)

EDCT_KEYS = (
    "edct_out",
    "edct_departure_runway_time",
    "edct_time",
)


def derive_event_times(payload: Mapping[str, object]) -> Mapping[str, Optional[datetime]]:
    """Extract high-level event timestamps from a flight payload."""

    return {
        "Departure": _first_timestamp(payload, DEPARTURE_KEYS),
        "Arrival": _first_timestamp(payload, ARRIVAL_KEYS),
        "ArrivalForecast": _first_timestamp(payload, ETA_KEYS),
        "EDCT": _first_timestamp(payload, EDCT_KEYS),
    }


def build_status_payload(
    event_times: Mapping[str, Optional[datetime]],
    *,
    scheduled_departure: Optional[datetime],
    scheduled_arrival: Optional[datetime],
) -> MutableMapping[str, Mapping[str, object]]:
    """Construct status-event payloads compatible with the dashboard store."""

    def _delta(actual: Optional[datetime], scheduled: Optional[datetime]) -> Optional[int]:
        if actual is None or scheduled is None:
            return None
        return int(round((actual - scheduled).total_seconds() / 60.0))

    status_map: MutableMapping[str, Mapping[str, object]] = {}

    dep_time = event_times.get("Departure")
    if dep_time:
        status_map["Departure"] = {
            "status": "🟢 DEPARTED",
            "actual_time_utc": dep_time.isoformat(),
            "delta_min": _delta(dep_time, scheduled_departure),
            "source": "aeroapi",
        }

    eta_time = event_times.get("ArrivalForecast")
    if eta_time:
        status_map["ArrivalForecast"] = {
            "status": "🟦 ARRIVING SOON",
            "actual_time_utc": eta_time.isoformat(),
            "delta_min": _delta(eta_time, scheduled_arrival),
            "source": "aeroapi",
        }

    arr_time = event_times.get("Arrival")
    if arr_time:
        status_map["Arrival"] = {
            "status": "🟣 ARRIVED",
            "actual_time_utc": arr_time.isoformat(),
            "delta_min": _delta(arr_time, scheduled_arrival),
            "source": "aeroapi",
        }

    edct_time = event_times.get("EDCT")
    if edct_time and "Departure" not in status_map:
        status_map["EDCT"] = {
            "status": "🟪 EDCT",
            "actual_time_utc": edct_time.isoformat(),
            "delta_min": None,
            "source": "aeroapi",
        }

    return status_map

