"""Utilities for interacting with the FL3XX external flight API."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Dict, Iterable, List, MutableMapping, Optional, Tuple

import requests


DEFAULT_FL3XX_BASE_URL = "https://app.fl3xx.us/api/external/flight/flights"


@dataclass(frozen=True)
class Fl3xxApiConfig:
    """Configuration for issuing requests to the FL3XX API."""

    base_url: str = DEFAULT_FL3XX_BASE_URL
    api_token: Optional[str] = None
    auth_header: Optional[str] = None
    auth_header_name: str = "Authorization"
    extra_headers: Dict[str, str] = field(default_factory=dict)
    verify_ssl: bool = True
    timeout: int = 30
    extra_params: Dict[str, str] = field(default_factory=dict)

    def build_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.auth_header:
            header_name = self.auth_header_name or "Authorization"
            headers[header_name] = self.auth_header
        elif self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        headers.update(self.extra_headers)
        return headers


def compute_fetch_dates(now: Optional[datetime] = None) -> Tuple[date, date]:
    """Return the inclusive date range that should be requested from the API.

    The requirement is to load flights starting today and including the
    following day. Because the FL3XX API treats the ``to`` parameter as
    exclusive, we advance it by two days so that flights that depart on the
    following day are included.
    """

    current = now or datetime.now(timezone.utc)
    start = current.date()
    end = start + timedelta(days=2)
    return start, end


def _normalise_payload(data: Any) -> List[Dict[str, Any]]:
    if isinstance(data, list):
        return data
    if isinstance(data, MutableMapping):
        if "items" in data and isinstance(data["items"], Iterable):
            items = list(data["items"])
            if all(isinstance(item, MutableMapping) for item in items):
                return items  # type: ignore[return-value]
        raise ValueError("Unsupported FL3XX API payload structure: mapping without 'items' list")
    raise ValueError("Unsupported FL3XX API payload structure")


def fetch_flights(
    config: Fl3xxApiConfig,
    *,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    session: Optional[requests.Session] = None,
    now: Optional[datetime] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Retrieve flights from the FL3XX API and return them with metadata."""

    reference_time = now or datetime.now(timezone.utc)
    if from_date is None or to_date is None:
        default_from, default_to = compute_fetch_dates(reference_time)
        if from_date is None:
            from_date = default_from
        if to_date is None:
            to_date = default_to

    params: Dict[str, str] = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "timeZone": "UTC",
        "value": "ALL",
    }
    params.update(config.extra_params)

    headers = config.build_headers()

    http = session or requests.Session()
    response = http.get(
        config.base_url,
        params=params,
        headers=headers,
        timeout=config.timeout,
        verify=config.verify_ssl,
    )
    response.raise_for_status()
    payload = response.json()
    flights = _normalise_payload(payload)

    digest_input = json.dumps(flights, sort_keys=True, ensure_ascii=False).encode("utf-8")
    digest = hashlib.sha256(digest_input).hexdigest()
    fetched_at = reference_time.isoformat().replace("+00:00", "Z")

    metadata = {
        "from_date": from_date.isoformat(),
        "to_date": to_date.isoformat(),
        "time_zone": params["timeZone"],
        "value": params["value"],
        "fetched_at": fetched_at,
        "hash": digest,
        "request_url": config.base_url,
        "request_params": params,
    }
    return flights, metadata


__all__ = ["Fl3xxApiConfig", "DEFAULT_FL3XX_BASE_URL", "compute_fetch_dates", "fetch_flights"]
