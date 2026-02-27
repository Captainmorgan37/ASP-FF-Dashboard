from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def _load_sync_helper():
    source_path = Path("ASP FF Dashboard.py")
    module = ast.parse(source_path.read_text())
    target = None
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name == "_sync_automated_takeoff_to_fl3xx_postflight":
            target = node
            break
    if target is None:
        raise RuntimeError("sync helper not found")

    mini = ast.Module(body=[target], type_ignores=[])
    ast.fix_missing_locations(mini)

    calls = []

    def fake_sync(config, flight_id, takeoff_unix_ms):
        calls.append({"config": config, "flight_id": flight_id, "takeoff_unix_ms": takeoff_unix_ms})
        return {"updated": True}

    namespace = {
        "pd": pd,
        "datetime": datetime,
        "timezone": timezone,
        "timedelta": timedelta,
        "Any": object,
        "Fl3xxApiConfig": object,
        "_parse_iso8601": lambda value: datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None,
        "sync_postflight_takeoff_if_empty": fake_sync,
    }
    exec(compile(mini, filename=str(source_path), mode="exec"), namespace)
    return namespace["_sync_automated_takeoff_to_fl3xx_postflight"], calls


def test_sync_helper_only_processes_recent_enroute_departures():
    fn, calls = _load_sync_helper()
    now = datetime.now(timezone.utc)
    recent_dep_received = (now - timedelta(minutes=2)).isoformat().replace("+00:00", "Z")

    frame = pd.DataFrame(
        [
            {
                "Aircraft": "C-FASF",
                "_Fl3xxFlightId": "111",
                "_DepActual_ts": pd.Timestamp(now - timedelta(minutes=3)),
                "_ArrActual_ts": pd.NaT,
                "_OnBlock_UTC": pd.NaT,
                "_LegKey": "ABC12",
                "Booking": "ABC12",
            },
            {
                "Aircraft": "C-FASF",
                "_Fl3xxFlightId": "222",
                "_DepActual_ts": pd.Timestamp(now - timedelta(minutes=3)),
                "_ArrActual_ts": pd.Timestamp(now),
                "_OnBlock_UTC": pd.NaT,
                "_LegKey": "DEF34",
                "Booking": "DEF34",
            },
        ]
    )

    events = {
        "ABC12": {"Departure": {"received_at": recent_dep_received}},
        "DEF34": {"Departure": {"received_at": recent_dep_received}},
    }

    summary = fn(
        object(),
        frame,
        enabled_tails={"CFASF"},
        events_lookup=lambda leg_key, booking: events.get(leg_key, {}),
        recent_departure_window_minutes=20,
    )

    assert summary["attempted"] == 1
    assert summary["updated"] == 1
    assert len(calls) == 1
    assert calls[0]["flight_id"] == "111"


def test_sync_helper_skips_when_departure_event_not_recent():
    fn, calls = _load_sync_helper()
    now = datetime.now(timezone.utc)
    old_dep_received = (now - timedelta(minutes=45)).isoformat().replace("+00:00", "Z")

    frame = pd.DataFrame(
        [
            {
                "Aircraft": "C-FASF",
                "_Fl3xxFlightId": "111",
                "_DepActual_ts": pd.Timestamp(now - timedelta(minutes=3)),
                "_ArrActual_ts": pd.NaT,
                "_OnBlock_UTC": pd.NaT,
                "_LegKey": "ABC12",
                "Booking": "ABC12",
            }
        ]
    )

    events = {"ABC12": {"Departure": {"received_at": old_dep_received}}}

    summary = fn(
        object(),
        frame,
        enabled_tails={"CFASF"},
        events_lookup=lambda leg_key, booking: events.get(leg_key, {}),
        recent_departure_window_minutes=20,
    )

    assert summary["attempted"] == 0
    assert summary["updated"] == 0
    assert calls == []
