from fuel_stop_advisor import (
    DEFAULT_LEG_CAP_FACTOR,
    OCEANIC_CJ_LEG_CAP_FACTOR,
    FlightContext,
    StopCandidate,
    has_oceanic_profile,
    leg_cap_factor,
    rank_stop_candidates,
    target_stop_distance_nm,
)


def test_leg_cap_relaxes_for_oceanic_cj() -> None:
    context = FlightContext(
        aircraft_type="CJ3",
        overwater_extent_nm=260,
        route_distance_nm=1800,
        oceanic_segment_start_nm=700,
        oceanic_segment_end_nm=1200,
    )

    assert has_oceanic_profile(context)
    assert leg_cap_factor(context) == OCEANIC_CJ_LEG_CAP_FACTOR


def test_default_cap_used_for_non_oceanic_or_non_cj() -> None:
    non_cj = FlightContext(aircraft_type="G550", overwater_extent_nm=400, route_distance_nm=1800)
    short_overwater_cj = FlightContext(aircraft_type="CJ2", overwater_extent_nm=140, route_distance_nm=900)

    assert leg_cap_factor(non_cj) == DEFAULT_LEG_CAP_FACTOR
    assert leg_cap_factor(short_overwater_cj) == DEFAULT_LEG_CAP_FACTOR


def test_oceanic_target_moves_toward_oceanic_segment() -> None:
    context = FlightContext(
        aircraft_type="CJ4",
        overwater_extent_nm=300,
        route_distance_nm=1800,
        oceanic_segment_start_nm=900,
        oceanic_segment_end_nm=1500,
    )

    assert target_stop_distance_nm(context) == 1110


def test_ranking_allows_1_5_for_oceanic_cj_and_keeps_land_rule() -> None:
    context = FlightContext(
        aircraft_type="CJ4",
        overwater_extent_nm=350,
        route_distance_nm=1800,
        oceanic_segment_start_nm=900,
        oceanic_segment_end_nm=1500,
    )

    ranked = rank_stop_candidates(
        context,
        [
            StopCandidate("MID", distance_along_route_nm=900, distance_to_nearest_land_nm=150, leg_factor=1.15),
            StopCandidate("OCE", distance_along_route_nm=1120, distance_to_nearest_land_nm=180, leg_factor=1.45),
            StopCandidate("TOO_FAR_FROM_LAND", distance_along_route_nm=1110, distance_to_nearest_land_nm=230, leg_factor=1.1),
            StopCandidate("OVER_CAP", distance_along_route_nm=1110, distance_to_nearest_land_nm=120, leg_factor=1.55),
        ],
    )

    assert [candidate.ident for candidate in ranked] == ["OCE", "MID"]
