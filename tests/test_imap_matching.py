import ast
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparse
from dateutil.tz import tzoffset

MODULE_PATH = Path(__file__).resolve().parents[1] / "ASP FF Dashboard.py"


_DEF_NAMES = {
    "normalize_iata",
    "derive_iata_from_icao",
    "_airport_token_variants",
    "choose_booking_for_event",
    "_parse_time_token_to_utc",
}


with MODULE_PATH.open("r", encoding="utf-8") as fp:
    MODULE_SOURCE = fp.read()

MODULE_AST = ast.parse(MODULE_SOURCE, filename=str(MODULE_PATH))

_FUNCTION_SRC: dict[str, str] = {}
for node in MODULE_AST.body:
    if isinstance(node, ast.FunctionDef) and node.name in _DEF_NAMES:
        src = ast.get_source_segment(MODULE_SOURCE, node)
        if src:
            _FUNCTION_SRC[node.name] = src

missing = _DEF_NAMES - _FUNCTION_SRC.keys()
if missing:
    raise RuntimeError(f"Missing functions in dashboard module: {sorted(missing)}")

_namespace: dict[str, object] = {
    "pd": pd,
    "datetime": datetime,
    "timezone": timezone,
    "timedelta": timedelta,
    "dateparse": dateparse,
    "tzoffset": tzoffset,
    "TZINFOS": {
        "UTC": tzoffset("UTC", 0),
        "GMT": tzoffset("GMT", 0),
        "EST": tzoffset("EST", -5 * 3600),
        "EDT": tzoffset("EDT", -4 * 3600),
        "CST": tzoffset("CST", -6 * 3600),
        "CDT": tzoffset("CDT", -5 * 3600),
        "MST": tzoffset("MST", -7 * 3600),
        "MDT": tzoffset("MDT", -6 * 3600),
        "PST": tzoffset("PST", -8 * 3600),
        "PDT": tzoffset("PDT", -7 * 3600),
    },
    "ICAO_TO_IATA_MAP": {},
    "IATA_TO_ICAO_MAP": {},
}

exec(
    "\n\n".join(
        _FUNCTION_SRC[name] for name in [
            "normalize_iata",
            "derive_iata_from_icao",
            "_airport_token_variants",
            "choose_booking_for_event",
            "_parse_time_token_to_utc",
        ]
    ),
    _namespace,
)

choose_booking_for_event = _namespace["choose_booking_for_event"]
_parse_time_token_to_utc = _namespace["_parse_time_token_to_utc"]


def test_choose_booking_handles_missing_timestamp_for_prior_leg():
    df_clean = pd.DataFrame(
        [
            {
                "Booking": "1001",
                "Aircraft": "C-FASP",
                "From_IATA": "YUL",
                "From_ICAO": "CYUL",
                "To_IATA": "TEB",
                "To_ICAO": "KTEB",
                "ETD_UTC": pd.Timestamp("2024-01-14T23:30:00Z"),
                "ETA_UTC": pd.Timestamp("2024-01-15T02:45:00Z"),
            },
            {
                "Booking": "1002",
                "Aircraft": "C-FASP",
                "From_IATA": "YYZ",
                "From_ICAO": "CYYZ",
                "To_IATA": "MDW",
                "To_ICAO": "KMDW",
                "ETD_UTC": pd.Timestamp("2024-01-16T12:00:00Z"),
                "ETA_UTC": pd.Timestamp("2024-01-16T15:15:00Z"),
            },
        ]
    )

    _namespace["df_clean"] = df_clean
    _namespace["ICAO_TO_IATA_MAP"] = {"CYUL": "YUL", "KTEB": "TEB", "CYYZ": "YYZ", "KMDW": "MDW"}
    _namespace["IATA_TO_ICAO_MAP"] = {"YUL": "CYUL", "TEB": "KTEB", "YYZ": "CYYZ", "MDW": "KMDW"}

    subj_info = {
        "from_airport": "CYUL",
        "to_airport": "KTEB",
    }
    tails_dashed = ["C-FASP"]

    match = choose_booking_for_event(subj_info, tails_dashed, "Departure", None)

    assert match is not None
    assert match["Booking"] == "1001"

    far_timestamp = datetime(2024, 1, 16, 12, 0, tzinfo=timezone.utc)
    match_far = choose_booking_for_event(subj_info, tails_dashed, "Departure", far_timestamp)

    assert match_far is None


def test_choose_booking_rejects_far_timestamp_even_if_single_candidate():
    df_clean = pd.DataFrame(
        [
            {
                "Booking": "2001",
                "Aircraft": "C-FASP",
                "From_IATA": "YUL",
                "From_ICAO": "CYUL",
                "To_IATA": "TEB",
                "To_ICAO": "KTEB",
                "ETD_UTC": pd.Timestamp("2024-02-01T12:00:00Z"),
                "ETA_UTC": pd.Timestamp("2024-02-01T15:00:00Z"),
            },
            {
                "Booking": "2002",
                "Aircraft": "C-FAKE",
                "From_IATA": "YYZ",
                "From_ICAO": "CYYZ",
                "To_IATA": "MDW",
                "To_ICAO": "KMDW",
                "ETD_UTC": pd.Timestamp("2024-02-02T12:00:00Z"),
                "ETA_UTC": pd.Timestamp("2024-02-02T15:00:00Z"),
            },
        ]
    )

    _namespace["df_clean"] = df_clean
    _namespace["ICAO_TO_IATA_MAP"] = {"CYUL": "YUL", "KTEB": "TEB", "CYYZ": "YYZ", "KMDW": "MDW"}
    _namespace["IATA_TO_ICAO_MAP"] = {"YUL": "CYUL", "TEB": "KTEB", "YYZ": "CYYZ", "MDW": "KMDW"}

    subj_info = {
        "from_airport": None,
        "to_airport": None,
    }
    tails_dashed = ["C-FASP"]

    far_dt = pd.Timestamp("2024-02-01T22:30:00Z").to_pydatetime()
    match = choose_booking_for_event(subj_info, tails_dashed, "Arrival", far_dt)

    assert match is None


def test_choose_booking_without_timestamp_and_multiple_candidates_returns_none():
    df_clean = pd.DataFrame(
        [
            {
                "Booking": "3001",
                "Aircraft": "C-FASP",
                "From_IATA": "YUL",
                "From_ICAO": "CYUL",
                "To_IATA": "TEB",
                "To_ICAO": "KTEB",
                "ETD_UTC": pd.Timestamp("2024-03-10T12:00:00Z"),
                "ETA_UTC": pd.Timestamp("2024-03-10T15:00:00Z"),
            },
            {
                "Booking": "3002",
                "Aircraft": "C-FASP",
                "From_IATA": "YYZ",
                "From_ICAO": "CYYZ",
                "To_IATA": "MDW",
                "To_ICAO": "KMDW",
                "ETD_UTC": pd.Timestamp("2024-03-11T16:00:00Z"),
                "ETA_UTC": pd.Timestamp("2024-03-11T19:00:00Z"),
            },
        ]
    )

    _namespace["df_clean"] = df_clean
    _namespace["ICAO_TO_IATA_MAP"] = {"CYUL": "YUL", "KTEB": "TEB", "CYYZ": "YYZ", "KMDW": "MDW"}
    _namespace["IATA_TO_ICAO_MAP"] = {"YUL": "CYUL", "TEB": "KTEB", "YYZ": "CYYZ", "MDW": "KMDW"}

    subj_info = {
        "from_airport": None,
        "to_airport": None,
    }
    tails_dashed = ["C-FASP"]

    match = choose_booking_for_event(subj_info, tails_dashed, "Departure", None)

    assert match is None


def test_parse_time_token_handles_prior_local_day():
    base_hdr = datetime(2024, 4, 2, 5, 15, tzinfo=timezone.utc)

    parsed = _parse_time_token_to_utc("11:20 PM EDT", base_hdr)

    assert parsed == datetime(2024, 4, 2, 3, 20, tzinfo=timezone.utc)
