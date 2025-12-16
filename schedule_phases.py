"""Helpers for classifying schedule rows into flight phases."""
from __future__ import annotations

from typing import Any, Iterable, Mapping

try:  # pandas is optional when running inside the NiceGUI app
    import pandas as pd  # type: ignore
except Exception:  # pragma: no cover - pandas is optional at runtime
    pd = None  # type: ignore

SCHEDULE_PHASE_LANDED = "landed"
SCHEDULE_PHASE_ENROUTE = "enroute"
SCHEDULE_PHASE_TO_DEPART = "to_depart"

SCHEDULE_PHASES: tuple[tuple[str, str, str, bool], ...] = (
    (
        SCHEDULE_PHASE_LANDED,
        "Landed flights",
        "Flights that have already landed or parked on the blocks.",
        False,
    ),
    (
        SCHEDULE_PHASE_ENROUTE,
        "Enroute flights",
        "Flights that are airborne or have already gone blocks off.",
        False,
    ),
    (
        SCHEDULE_PHASE_TO_DEPART,
        "To depart",
        "Flights that are waiting to depart.",
        True,
    ),
)

LANDED_VALUE_FIELDS = (
    "Landing (UTC)",
    "Landing (FA)",
    "Landing",
    "Landing Time",
    "On Block (UTC)",
    "On Block",
    "On-Block (Actual)",
    "Actual On",
    "Actual In",
    "Arrival (UTC)",
    "Arrival Time",
    "Arrived",
)

ENROUTE_VALUE_FIELDS = (
    "Takeoff (UTC)",
    "Takeoff (FA)",
    "Takeoff",
    "Departure (UTC)",
    "Departure Time",
    "Off Block (UTC)",
    "Off Block",
    "Blocks Off",
    "Actual Off",
    "Actual Out",
)

STAGE_TEXT_FIELDS = (
    "Stage Progress",
    "Status",
    "Stage",
    "Flight Status",
)

LANDED_KEYWORDS = ("landed", "on block", "blocks on", "arrived", "arrival", "on ground")
LANDED_EXCLUDE_KEYWORDS = ("delayed arrival",)
ENROUTE_KEYWORDS = (
    "airborne",
    "enroute",
    "en route",
    "departed",
    "block off",
    "blocks off",
    "off block",
    "takeoff",
    "in flight",
    "delayed arrival",
)


PHASE_COLUMN_EXCLUDES: dict[str, tuple[str, ...]] = {
    SCHEDULE_PHASE_LANDED: (
        "Departs In",
        "Arrives In",
        "Downline Risk",
    ),
    SCHEDULE_PHASE_ENROUTE: (
        "Departs In",
        "Landing (UTC)",
        "Landing (FA)",
        "On Block (UTC)",
        "Block On (UTC)",
        "Downline Risk",
    ),
    SCHEDULE_PHASE_TO_DEPART: (
        "ETA",
        "ETA (FA)",
        "Arrives In",
        "Off Block",
        "Off Block (UTC)",
        "Off-Block (Sched)",
        "Off Block (Sched)",
        "Takeoff",
        "Takeoff (UTC)",
        "Takeoff (FA)",
        "Landing",
        "Landing (UTC)",
        "Landing (FA)",
        "On Block",
        "On Block (UTC)",
        "On-Block (Sched)",
        "On Block (Sched)",
    ),
}


def _normalize_column_name(name: str | None) -> str:
    if not name:
        return ""
    return name.strip().lower()


PHASE_COLUMN_EXCLUDES_NORMALIZED = {
    phase: {_normalize_column_name(col) for col in columns if col}
    for phase, columns in PHASE_COLUMN_EXCLUDES.items()
}


def filtered_columns_for_phase(phase: str, columns: Iterable[str]) -> list[str]:
    excluded = PHASE_COLUMN_EXCLUDES_NORMALIZED.get(phase, set())
    filtered = [
        column for column in columns if _normalize_column_name(column) not in excluded
    ]
    return filtered or list(columns)


__all__ = [
    "SCHEDULE_PHASE_LANDED",
    "SCHEDULE_PHASE_ENROUTE",
    "SCHEDULE_PHASE_TO_DEPART",
    "SCHEDULE_PHASES",
    "PHASE_COLUMN_EXCLUDES",
    "filtered_columns_for_phase",
    "categorize_dataframe_by_phase",
    "categorize_rows_by_phase",
    "row_phase",
]


def _value_is_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if pd is not None:
        try:
            return bool(pd.notna(value))
        except Exception:  # pragma: no cover - defensive
            pass
    return True


def _row_has_values(row: Mapping[str, Any], fields: Iterable[str]) -> bool:
    for field in fields:
        if not field:
            continue
        if _value_is_present(row.get(field)):
            return True
    return False


def _row_matches_keywords(
    row: Mapping[str, Any],
    keywords: Iterable[str],
    exclude_keywords: Iterable[str] | None = None,
) -> bool:
    excludes = tuple(exclude_keywords or ())
    for field in STAGE_TEXT_FIELDS:
        value = row.get(field)
        if not isinstance(value, str):
            continue
        text = value.lower()
        if any(exclude in text for exclude in excludes):
            continue
        if any(keyword in text for keyword in keywords):
            return True
    return False


def row_phase(row: Mapping[str, Any]) -> str:
    if _row_matches_keywords(row, LANDED_KEYWORDS, LANDED_EXCLUDE_KEYWORDS) or _row_has_values(
        row, LANDED_VALUE_FIELDS
    ):
        return SCHEDULE_PHASE_LANDED
    if _row_matches_keywords(row, ENROUTE_KEYWORDS) or _row_has_values(row, ENROUTE_VALUE_FIELDS):
        return SCHEDULE_PHASE_ENROUTE
    return SCHEDULE_PHASE_TO_DEPART


def categorize_rows_by_phase(rows: list[Mapping[str, Any]]) -> dict[str, list[Mapping[str, Any]]]:
    buckets = {phase: [] for phase, *_ in SCHEDULE_PHASES}
    for row in rows:
        buckets.setdefault(row_phase(row), []).append(row)
    return buckets


def categorize_dataframe_by_phase(df):  # type: ignore[no-untyped-def]
    """Return DataFrame subsets grouped by phase (requires pandas)."""

    if pd is None:  # pragma: no cover - pandas should be available in Streamlit app
        raise RuntimeError("pandas is required to categorize a DataFrame")

    if df.empty:
        return {phase: df.iloc[0:0].copy() for phase, *_ in SCHEDULE_PHASES}

    phase_series = df.apply(row_phase, axis=1)
    buckets: dict[str, Any] = {}
    for phase, *_ in SCHEDULE_PHASES:
        mask = phase_series == phase
        buckets[phase] = df[mask].copy()
    return buckets
