import ast
from pathlib import Path

import pandas as pd
from dateutil import parser as dateparse
from datetime import datetime, timezone
import re


def _load_inline_editor_helpers():
    source = Path("ASP FF Dashboard.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    target_names = [
        "_coerce_reference_datetime",
        "_from_editor_datetime",
        "_datetimes_equal",
        "_apply_inline_editor_updates",
    ]
    func_map = {}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name in target_names:
            func_map[node.name] = ast.get_source_segment(source, node)

    namespace = {
        "pd": pd,
        "re": re,
        "dateparse": dateparse,
        "datetime": datetime,
        "timezone": timezone,
    }

    class DummyStreamlit:
        def __init__(self):
            self.session_state = {}
            self._rerun_called = False

        def rerun(self):  # pragma: no cover - invoked to mirror behaviour
            self._rerun_called = True

    dummy_st = DummyStreamlit()
    namespace["st"] = dummy_st

    tail_calls = {"upsert": [], "delete": []}
    status_calls = {"upsert": [], "delete": []}

    def upsert_tail_override(key, value):
        tail_calls["upsert"].append((key, value))

    def delete_tail_override(key):
        tail_calls["delete"].append(key)

    def upsert_status(key, event_type, status, actual_time, delta):
        status_calls["upsert"].append((key, event_type, status, actual_time, delta))

    def delete_status(key, event_type):
        status_calls["delete"].append((key, event_type))

    namespace.update(
        {
            "upsert_tail_override": upsert_tail_override,
            "delete_tail_override": delete_tail_override,
            "upsert_status": upsert_status,
            "delete_status": delete_status,
        }
    )

    for name in target_names:
        exec(func_map[name], namespace)  # noqa: S102 - executing extracted helper source

    return namespace, dummy_st, tail_calls, status_calls


def test_inline_editor_prefers_leg_key_when_labelled():
    namespace, st_stub, tail_calls, status_calls = _load_inline_editor_helpers()
    apply_updates = namespace["_apply_inline_editor_updates"]

    original_df = pd.DataFrame(
        {
            "Booking": ["B1", "B1"],
            "_LegKey": ["B1#L1", "B1#L2"],
            "Aircraft": ["C-GALX", "C-FALC"],
            "Takeoff (FA)": ["2024-01-01 10:00", ""],
            "EDCT (UTC)": ["", ""],
            "ETA (FA)": ["", ""],
            "Landing (FA)": ["", ""],
        }
    )

    edited_df = original_df.rename(columns={"_LegKey": "Leg Identifier"}).copy()
    edited_df.loc[0, "Takeoff (FA)"] = "2024-01-01 10:45"

    base_df = pd.DataFrame(
        {
            "Booking": ["B1", "B1"],
            "_LegKey": ["B1#L1", "B1#L2"],
            "Aircraft": ["C-GALX", "C-FALC"],
            "_DepActual_ts": [
                pd.Timestamp("2024-01-01 10:00", tz="UTC"),
                pd.NaT,
            ],
            "_EDCT_ts": [pd.NaT, pd.NaT],
            "_ETA_FA_ts": [pd.NaT, pd.NaT],
            "_ArrActual_ts": [pd.NaT, pd.NaT],
            "ETD_UTC": [
                pd.Timestamp("2024-01-01 09:30", tz="UTC"),
                pd.Timestamp("2024-01-01 12:00", tz="UTC"),
            ],
            "ETA_UTC": [
                pd.Timestamp("2024-01-01 11:00", tz="UTC"),
                pd.Timestamp("2024-01-01 14:00", tz="UTC"),
            ],
        }
    )

    apply_updates(original_df, edited_df, base_df)

    assert status_calls["upsert"] == [
        (
            "B1#L1",
            "Departure",
            "🟢 DEPARTED",
            "2024-01-01T10:45:00+00:00",
            75,
        )
    ]
    assert status_calls["delete"] == []
    assert tail_calls == {"upsert": [], "delete": []}
    assert st_stub._rerun_called is True



def test_inline_editor_edct_updates_and_is_persisted_by_leg_key():
    namespace, st_stub, tail_calls, status_calls = _load_inline_editor_helpers()
    apply_updates = namespace["_apply_inline_editor_updates"]

    original_df = pd.DataFrame(
        {
            "Booking": ["B2"],
            "_LegKey": ["B2#L1"],
            "Aircraft": ["C-GXYZ"],
            "Takeoff (FA)": [""],
            "EDCT (UTC)": [""],
            "ETA (FA)": [""],
            "Landing (FA)": [""],
        }
    )

    edited_df = original_df.copy()
    edited_df.loc[0, "EDCT (UTC)"] = "2024-01-02 09:15"

    base_df = pd.DataFrame(
        {
            "Booking": ["B2"],
            "_LegKey": ["B2#L1"],
            "Aircraft": ["C-GXYZ"],
            "_DepActual_ts": [pd.NaT],
            "_EDCT_ts": [pd.NaT],
            "_ETA_FA_ts": [pd.NaT],
            "_ArrActual_ts": [pd.NaT],
            "ETD_UTC": [pd.Timestamp("2024-01-02 08:30", tz="UTC")],
            "ETA_UTC": [pd.Timestamp("2024-01-02 10:30", tz="UTC")],
        }
    )

    apply_updates(original_df, edited_df, base_df)

    assert status_calls["upsert"] == [
        (
            "B2#L1",
            "EDCT",
            "🟪 EDCT",
            "2024-01-02T09:15:00+00:00",
            45,
        )
    ]
    assert status_calls["delete"] == []
    assert tail_calls == {"upsert": [], "delete": []}
    assert st_stub._rerun_called is True
