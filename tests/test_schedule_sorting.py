from datetime import timedelta

from schedule_sorting import sort_enroute_rows


def test_sort_enroute_rows_orders_by_countdown():
    rows = [
        {"Booking": "late", "Arrives In": "01:10"},
        {"Booking": "soonest", "Arrives In": "00:05"},
        {"Booking": "overdue", "Arrives In": "-00:15"},
        {"Booking": "unknown", "Arrives In": "—"},
    ]

    sorted_rows = sort_enroute_rows(rows)

    assert [row["Booking"] for row in sorted_rows] == [
        "overdue",
        "soonest",
        "late",
        "unknown",
    ]


def test_sort_enroute_rows_handles_numeric_and_timedelta_values():
    rows = [
        {"Booking": "minutes", "Arrives In": 600},  # 10 minutes
        {"Booking": "delta", "Arrives In": timedelta(minutes=5)},
        {"Booking": "string", "Arrives In": "00:02"},
    ]

    sorted_rows = sort_enroute_rows(rows)

    assert [row["Booking"] for row in sorted_rows] == [
        "string",
        "delta",
        "minutes",
    ]
