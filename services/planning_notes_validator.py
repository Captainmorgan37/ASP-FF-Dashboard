from __future__ import annotations

import re
from dataclasses import dataclass


_PLANNED_LEG_RE = re.compile(
    r"^\s*(?P<day>\d{1,2})(?P<month>[A-Z]{3})\s*-\s*(?P<origin>[A-Z0-9]{3,4})\s*-\s*(?P<destination>[A-Z0-9]{3,4})\s*$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class PlannedLeg:
    day: int
    month: str
    origin: str
    destination: str


def _split_planning_lines(planning_notes: str | None) -> list[str]:
    """Split planning notes into normalized, non-empty lines.

    Supports notes coming in as real multiline text or as escaped "\\n" payloads.
    """

    raw = str(planning_notes or "").replace("\\r", "\n")
    raw = raw.replace("\\n", "\n")
    return [line.strip() for line in raw.splitlines() if line.strip()]


def extract_planned_legs(planning_notes: str | None) -> list[PlannedLeg]:
    """Return all schedule legs found in planning notes."""

    legs: list[PlannedLeg] = []
    for line in _split_planning_lines(planning_notes):
        match = _PLANNED_LEG_RE.match(line)
        if not match:
            continue
        legs.append(
            PlannedLeg(
                day=int(match.group("day")),
                month=match.group("month").upper(),
                origin=match.group("origin").upper(),
                destination=match.group("destination").upper(),
            )
        )
    return legs


def _notes_request_specific_aircraft(planning_notes: str | None) -> bool:
    """True when notes include a clear owner/requester aircraft request."""

    for line in _split_planning_lines(planning_notes):
        normalized = line.lower()
        if "request" not in normalized:
            continue
        if re.search(r"\b(cj\d\+?|citation\s*jet\d\+?)\b", normalized):
            return True
    return False


def validate_workflow_against_planning_notes(
    workflow: str | None,
    planning_notes: str | None,
) -> tuple[bool, str]:
    """Validate whether workflow intent is supported by planning notes.

    Current business logic:
    - For non-FEX Guaranteed workflows, only require at least one planned leg.
    - For FEX Guaranteed workflows, require at least one planned leg and an
      explicit aircraft request in notes.
    """

    workflow_text = str(workflow or "").strip().lower()
    legs = extract_planned_legs(planning_notes)
    if not legs:
        return False, "no recognizable schedule legs were found in planning notes"

    if "fex guaranteed" not in workflow_text:
        return True, "validated"

    if _notes_request_specific_aircraft(planning_notes):
        return True, "validated"

    return False, "FEX Guaranteed requires an explicit aircraft request in planning notes"


__all__ = [
    "PlannedLeg",
    "extract_planned_legs",
    "validate_workflow_against_planning_notes",
]
