"""Helpers for sorting schedule tables."""
from __future__ import annotations

from datetime import timedelta
from typing import Iterable


def _coerce_arrives_in_seconds(value: object) -> float:
    """Return a numeric sort key for an ``Arrives In`` countdown string."""

    if isinstance(value, timedelta):
        return value.total_seconds()

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        text = value.strip()
        if not text or text == "—":
            return float("inf")

        negative = text.startswith("-")
        if negative:
            text = text[1:]

        parts = text.split(":")
        try:
            hours = int(parts[0])
            minutes = int(parts[1]) if len(parts) > 1 else 0
        except ValueError:
            return float("inf")

        seconds = hours * 3600 + minutes * 60
        return -seconds if negative else seconds

    return float("inf")


def sort_enroute_rows(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    """Return enroute rows ordered by the lowest ``Arrives In`` countdown first."""

    return sorted(rows, key=lambda row: _coerce_arrives_in_seconds(row.get("Arrives In")))


__all__ = ["sort_enroute_rows"]
