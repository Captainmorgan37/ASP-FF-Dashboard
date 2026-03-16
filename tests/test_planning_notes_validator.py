from services.planning_notes_validator import (
    extract_planned_legs,
    validate_workflow_against_planning_notes,
)


def test_extract_planned_legs_supports_escaped_newlines() -> None:
    notes = "01APR- KSUN-KLAX\\n02APR-KLAX-KSUN\\n-\\n24hr Club CJ3+ owner requesting a CJ3+"

    legs = extract_planned_legs(notes)

    assert len(legs) == 2
    assert legs[0].origin == "KSUN"
    assert legs[0].destination == "KLAX"
    assert legs[1].origin == "KLAX"
    assert legs[1].destination == "KSUN"


def test_fex_guaranteed_validates_with_owner_aircraft_request() -> None:
    notes = """01APR- KSUN-KLAX
02APR-KLAX-KSUN
04APR-KSUN-CYVR
-
24hr Club CJ3+ owner requesting a CJ3+"""

    valid, reason = validate_workflow_against_planning_notes("FEX Guaranteed", notes)

    assert valid is True
    assert reason == "validated"


def test_fex_guaranteed_requires_specific_aircraft_request() -> None:
    notes = "01APR- KSUN-KLAX\n02APR-KLAX-KSUN"

    valid, reason = validate_workflow_against_planning_notes("FEX Guaranteed", notes)

    assert valid is False
    assert "explicit aircraft request" in reason
