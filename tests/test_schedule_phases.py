from schedule_phases import (
    SCHEDULE_PHASE_ENROUTE,
    SCHEDULE_PHASE_LANDED,
    SCHEDULE_PHASE_TO_DEPART,
    categorize_rows_by_phase,
    filtered_columns_for_phase,
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


def test_to_depart_columns_hide_arrival_related_fields():
    columns = [
        "Booking",
        "ETA",
        "ETA (FA)",
        "Arrives In",
        "Off Block",
        "On Block",
    ]
    filtered = filtered_columns_for_phase(SCHEDULE_PHASE_TO_DEPART, columns)
    assert "ETA" not in filtered
    assert "ETA (FA)" not in filtered
    assert "Arrives In" not in filtered
    assert "Off Block" not in filtered
    assert "On Block" not in filtered
    assert filtered == ["Booking"]


def test_enroute_columns_hide_departure_countdown():
    columns = ["Booking", "Departs In", "Arrives In"]
    filtered = filtered_columns_for_phase(SCHEDULE_PHASE_ENROUTE, columns)
    assert "Departs In" not in filtered
    assert "Arrives In" in filtered


def test_enroute_columns_hide_landing_and_block_on_timestamps():
    columns = [
        "Booking",
        "Landing (UTC)",
        "Landing (FA)",
        "On Block (UTC)",
        "Block On (UTC)",
    ]
    filtered = filtered_columns_for_phase(SCHEDULE_PHASE_ENROUTE, columns)
    assert filtered == ["Booking"]


def test_landed_columns_hide_countdowns():
    columns = ["Booking", "Departs In", "Arrives In", "On Block"]
    filtered = filtered_columns_for_phase(SCHEDULE_PHASE_LANDED, columns)
    assert "Departs In" not in filtered
    assert "Arrives In" not in filtered
    assert "On Block" in filtered
