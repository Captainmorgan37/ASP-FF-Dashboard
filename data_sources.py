"""Helpers for loading schedule data from interchangeable sources."""
from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Literal, Optional

import pandas as pd

ScheduleSource = Literal["csv_upload", "fl3xx_api"]


@dataclass
class ScheduleData:
    """Container describing the schedule dataframe and its origin."""

    frame: pd.DataFrame
    source: ScheduleSource
    raw_bytes: Optional[bytes] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


def _load_csv_schedule(csv_bytes: bytes, metadata: Optional[Dict[str, Any]] = None) -> ScheduleData:
    frame = pd.read_csv(BytesIO(csv_bytes))
    return ScheduleData(frame=frame, source="csv_upload", raw_bytes=csv_bytes, metadata=metadata or {})


def _format_utc_timestamp(value: Any) -> str:
    """Convert ISO8601 timestamps to the dd.mm.yyyy HH:MM layout used in the CSV export."""

    if value in (None, ""):
        return ""

    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(timestamp):
        return ""

    if isinstance(timestamp, pd.Series):
        # Should not happen for scalar values, but be defensive.
        timestamp = timestamp.iloc[0]

    if isinstance(timestamp, pd.Timestamp):
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize("UTC")
        else:
            timestamp = timestamp.tz_convert("UTC")
        return timestamp.strftime("%d.%m.%Y %H:%M")

    if isinstance(timestamp, datetime):
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        else:
            timestamp = timestamp.astimezone(timezone.utc)
        return timestamp.strftime("%d.%m.%Y %H:%M")

    return ""


def _compute_flight_time(off_block: Any, on_block: Any) -> str:
    """Return an HH:MM duration string between off/on block times."""

    off_ts = pd.to_datetime(off_block, utc=True, errors="coerce")
    on_ts = pd.to_datetime(on_block, utc=True, errors="coerce")

    if pd.isna(off_ts) or pd.isna(on_ts):
        return ""

    delta = on_ts - off_ts
    if pd.isna(delta):
        return ""

    total_minutes = int(round(delta.total_seconds() / 60))
    if total_minutes < 0:
        return ""

    hours, minutes = divmod(total_minutes, 60)
    return f"{hours:02d}:{minutes:02d}"


FL3XX_SCHEDULE_COLUMNS = [
    "Booking",
    "Off-Block (Sched)",
    "On-Block (Sched)",
    "From (ICAO)",
    "To (ICAO)",
    "Flight time (Est)",
    "PIC",
    "SIC",
    "Account",
    "Aircraft",
    "Aircraft Type",
    "Workflow",
]


def _normalize_flights_for_schedule(flights: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    """Transform FL3XX flight dictionaries into the dashboard's CSV-friendly structure."""

    rows = []
    for flight in flights:
        if not isinstance(flight, dict):
            continue

        booking = flight.get("bookingIdentifier") or flight.get("bookingReference") or ""
        account = flight.get("accountName") or flight.get("accountReference") or ""
        aircraft = flight.get("registrationNumber") or flight.get("requestedAircraftType") or ""
        aircraft_type = flight.get("aircraftCategory") or ""
        workflow = flight.get("workflowCustomName") or flight.get("workflow") or ""

        off_block = _format_utc_timestamp(flight.get("blockOffEstUTC"))
        on_block = _format_utc_timestamp(flight.get("blockOnEstUTC"))

        rows.append(
            {
                "Booking": str(booking or "").strip(),
                "Off-Block (Sched)": off_block,
                "On-Block (Sched)": on_block,
                "From (ICAO)": (flight.get("airportFrom") or "").strip() if isinstance(flight.get("airportFrom"), str) else str(flight.get("airportFrom") or ""),
                "To (ICAO)": (flight.get("airportTo") or "").strip() if isinstance(flight.get("airportTo"), str) else str(flight.get("airportTo") or ""),
                "Flight time (Est)": _compute_flight_time(flight.get("blockOffEstUTC"), flight.get("blockOnEstUTC")),
                "PIC": "",
                "SIC": "",
                "Account": str(account or "").strip(),
                "Aircraft": str(aircraft or "").strip(),
                "Aircraft Type": str(aircraft_type or "").strip(),
                "Workflow": str(workflow or "").strip(),
            }
        )

    frame = pd.DataFrame(rows, columns=FL3XX_SCHEDULE_COLUMNS)
    if frame.empty:
        return pd.DataFrame(columns=FL3XX_SCHEDULE_COLUMNS)
    return frame.fillna("")


def _load_fl3xx_api_schedule(metadata: Optional[Dict[str, Any]] = None) -> ScheduleData:
    if metadata is None:
        raise ValueError("metadata is required when loading from the FL3XX API source")

    flights = metadata.get("flights")
    if flights is None:
        raise ValueError("metadata['flights'] is required for FL3XX API loading")

    frame = _normalize_flights_for_schedule(flights)

    # Avoid returning large raw payloads in metadata; keep a lightweight summary.
    meta_copy = dict(metadata)
    if "flights" in meta_copy:
        meta_copy["flight_count"] = len(frame)
        meta_copy.pop("flights")

    return ScheduleData(frame=frame, source="fl3xx_api", raw_bytes=None, metadata=meta_copy)


def load_schedule(
    source: ScheduleSource,
    *,
    csv_bytes: Optional[bytes] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> ScheduleData:
    """Return the current schedule dataframe for the requested data source."""

    if source == "csv_upload":
        if csv_bytes is None:
            raise ValueError("csv_bytes is required when loading from the CSV upload source")
        return _load_csv_schedule(csv_bytes, metadata=metadata)
    if source == "fl3xx_api":
        return _load_fl3xx_api_schedule(metadata=metadata)
    raise ValueError(f"Unsupported schedule source: {source}")
