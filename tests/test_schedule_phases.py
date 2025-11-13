from schedule_phases import (
    SCHEDULE_PHASE_ENROUTE,
    SCHEDULE_PHASE_LANDED,
    SCHEDULE_PHASE_TO_DEPART,
    categorize_rows_by_phase,
    row_phase,
)


def test_delayed_arrival_status_treated_as_enroute():
    row = {"Status": "Delayed Arrival"}
    assert row_phase(row) == SCHEDULE_PHASE_ENROUTE


def test_arrived_status_still_maps_to_landed():
    row = {"Status": "Arrived"}
    assert row_phase(row) == SCHEDULE_PHASE_LANDED


def test_rows_with_landing_timestamp_are_landed():
    row = {"Landing (UTC)": "2024-02-01T12:30:00Z"}
    assert row_phase(row) == SCHEDULE_PHASE_LANDED


def test_rows_without_stage_or_events_default_to_departures():
    rows = [{"Booking": "FLX-001"}]
    buckets = categorize_rows_by_phase(rows)
    assert buckets[SCHEDULE_PHASE_TO_DEPART][0]["Booking"] == "FLX-001"
