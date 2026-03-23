from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _load_ingest_helper():
    source_path = Path("ASP FF Dashboard.py")
    source = source_path.read_text()
    module = ast.parse(source, filename=str(source_path))

    targets: list[ast.FunctionDef] = []
    wanted = {"parse_iso_to_utc", "_ingest_fl3xx_actuals"}
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name in wanted:
            targets.append(node)

    if len(targets) != len(wanted):
        raise RuntimeError("Required FL3XX ingest helpers not found")

    mini = ast.Module(body=targets, type_ignores=[])
    ast.fix_missing_locations(mini)

    calls: list[tuple[object, ...]] = []

    namespace = {
        "datetime": datetime,
        "timezone": timezone,
        "timedelta": timedelta,
        "Any": object,
        "dateparse": __import__("dateutil.parser", fromlist=["parser"]),
        "_to_iso8601_z": lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z") if dt else None,
        "upsert_status": lambda *args: calls.append(args),
    }
    exec(compile(mini, filename=str(source_path), mode="exec"), namespace)
    return namespace["_ingest_fl3xx_actuals"], calls


def test_ingest_fl3xx_actuals_discards_epoch_placeholder_on_block():
    ingest, calls = _load_ingest_helper()

    flight = {
        "bookingIdentifier": "QOUFX1",
        "realDateOUT": "2026-03-23T20:57:00.000Z",
        "realDateIN": "1970-01-01T00:00:00.000Z",
        "flightStatus": "On Block",
    }

    ingest([flight])

    assert flight["_OffBlock_UTC"] == "2026-03-23T20:57:00.000Z"
    assert flight["_OnBlock_UTC"] is None
    assert flight["_FlightStatus"] == "On Block"
    assert calls == [("QOUFX1", "OffBlock", "Off Block", "2026-03-23T20:57:00.000Z", None)]


def test_ingest_fl3xx_actuals_discards_future_on_block_time():
    ingest, calls = _load_ingest_helper()
    future = datetime.now(timezone.utc) + timedelta(hours=2)

    flight = {
        "bookingIdentifier": "FUTURE1",
        "realDateOUT": "2026-03-23T20:57:00.000Z",
        "realDateIN": future.isoformat().replace("+00:00", "Z"),
    }

    ingest([flight])

    assert flight["_OnBlock_UTC"] is None
    assert calls == [("FUTURE1", "OffBlock", "Off Block", "2026-03-23T20:57:00.000Z", None)]


def test_ingest_fl3xx_actuals_keeps_plausible_on_block_time():
    ingest, calls = _load_ingest_helper()
    on_block = datetime.now(timezone.utc) - timedelta(minutes=10)

    flight = {
        "bookingIdentifier": "VALID1",
        "realDateOUT": "2026-03-23T20:57:00.000Z",
        "realDateIN": on_block.isoformat().replace("+00:00", "Z"),
    }

    ingest([flight])

    assert flight["_OnBlock_UTC"] == on_block.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    assert len(calls) == 2
    assert calls[1][0:3] == ("VALID1", "OnBlock", "On Block")
