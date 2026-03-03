from __future__ import annotations

from dataclasses import dataclass

DEFAULT_LEG_CAP_FACTOR = 1.2
OCEANIC_CJ_LEG_CAP_FACTOR = 1.5
LAND_PROXIMITY_LIMIT_NM = 200.0


@dataclass(frozen=True)
class FlightContext:
    """Inputs used by the fuel-stop advisor."""

    aircraft_type: str
    overwater_extent_nm: float
    route_distance_nm: float
    oceanic_segment_start_nm: float | None = None
    oceanic_segment_end_nm: float | None = None


@dataclass(frozen=True)
class StopCandidate:
    """Potential fuel-stop airport expressed in route-relative terms."""

    ident: str
    distance_along_route_nm: float
    distance_to_nearest_land_nm: float
    leg_factor: float


def is_cj_flight(aircraft_type: str) -> bool:
    normalized = (aircraft_type or "").strip().upper()
    return normalized.startswith("CJ")


def has_oceanic_profile(context: FlightContext) -> bool:
    return is_cj_flight(context.aircraft_type) and context.overwater_extent_nm >= 200.0


def leg_cap_factor(context: FlightContext) -> float:
    if has_oceanic_profile(context):
        return OCEANIC_CJ_LEG_CAP_FACTOR
    return DEFAULT_LEG_CAP_FACTOR


def target_stop_distance_nm(context: FlightContext) -> float:
    """
    Return preferred stop location along the route.

    Baseline flights use route midpoint. Oceanic CJ flights are biased toward the
    oceanic segment so the post-stop leg carries more fuel endurance through
    sparser-airport portions.
    """

    midpoint = max(context.route_distance_nm, 0.0) / 2.0
    if not has_oceanic_profile(context):
        return midpoint

    start = context.oceanic_segment_start_nm
    end = context.oceanic_segment_end_nm
    if start is None or end is None or end <= start:
        return midpoint

    oceanic_span = end - start
    return start + (0.35 * oceanic_span)


def rank_stop_candidates(
    context: FlightContext, candidates: list[StopCandidate]
) -> list[StopCandidate]:
    """Filter and rank candidates based on land-distance, cap factor, and placement."""

    cap = leg_cap_factor(context)
    target_nm = target_stop_distance_nm(context)

    eligible = [
        candidate
        for candidate in candidates
        if candidate.distance_to_nearest_land_nm <= LAND_PROXIMITY_LIMIT_NM
        and candidate.leg_factor <= cap
    ]

    return sorted(
        eligible,
        key=lambda candidate: (
            abs(candidate.distance_along_route_nm - target_nm),
            candidate.leg_factor,
            candidate.distance_to_nearest_land_nm,
            candidate.ident,
        ),
    )
