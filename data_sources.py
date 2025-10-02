"""Helpers for loading schedule data from interchangeable sources."""
from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO
from typing import Any, Dict, Literal, Optional

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
        raise NotImplementedError("FL3XX API loader not implemented yet")
    raise ValueError(f"Unsupported schedule source: {source}")
