"""NiceGUI entrypoint for running the dashboard on AWS App Runner.

This module provides a lightweight, Streamlit-free interface built with
NiceGUI.  It keeps the data loading utilities from the existing code base and
adds inline notification controls so the application can evolve beyond the
Streamlit constraints that caused problems on App Runner.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Iterable

import pandas as pd
from nicegui import app as nicegui_app
from nicegui import ui
from nicegui.events import UploadEventArguments

from data_sources import FL3XX_SCHEDULE_COLUMNS, ScheduleData, load_schedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _format_metadata(schedule: ScheduleData | None) -> str:
    """Return a human friendly description of the active schedule."""

    if schedule is None:
        return "No schedule loaded. Upload a CSV or fetch from the API to begin."

    pieces: list[str] = [f"Source: {schedule.source}"]
    metadata = schedule.metadata or {}

    filename = metadata.get("filename") or metadata.get("name")
    if filename:
        pieces.append(f"File: {filename}")

    uploaded_at = metadata.get("uploaded_at") or metadata.get("updated_at")
    if uploaded_at:
        pieces.append(f"Updated: {uploaded_at}")

    flight_count = len(schedule.frame.index)
    pieces.append(f"Flights: {flight_count}")

    return " · ".join(pieces)


def _rows_from_schedule(schedule: ScheduleData | None) -> list[dict[str, object]]:
    if schedule is None or schedule.frame.empty:
        return []
    frame = schedule.frame.fillna("")
    return frame.to_dict(orient="records")


def _table_columns(columns: Iterable[str]) -> list[dict[str, object]]:
    return [
        {"name": name, "label": name, "field": name, "align": "left", "sortable": True}
        for name in columns
    ]


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


# ---------------------------------------------------------------------------
# UI state and event handlers
# ---------------------------------------------------------------------------


schedule_state = SimpleNamespace(data=None)  # type: ignore[attr-defined]
table_component: ui.table | None = None
status_label: ui.label | None = None
notification_log: ui.log | None = None


def _refresh_table() -> None:
    if table_component is None:
        return
    table_component.rows = _rows_from_schedule(schedule_state.data)
    table_component.update()


def _refresh_status() -> None:
    if status_label is None:
        return
    status_label.text = _format_metadata(schedule_state.data)


def _handle_schedule_loaded(schedule: ScheduleData, success_message: str) -> None:
    schedule_state.data = schedule
    _refresh_table()
    _refresh_status()
    ui.notify(success_message, type="positive")


def load_schedule_from_upload(event: UploadEventArguments) -> None:
    """Parse a CSV file uploaded through the UI and populate the table."""

    content = event.content.read()
    if not content:
        ui.notify("Uploaded file is empty", type="warning")
        return

    metadata = {"filename": event.name, "uploaded_at": _utc_timestamp()}
    schedule = load_schedule("csv_upload", csv_bytes=content, metadata=metadata)
    _handle_schedule_loaded(schedule, f"Loaded {len(schedule.frame)} flights from {event.name}")


def simulate_fetch_from_fl3xx() -> None:
    """Demonstrate loading flights from the FL3XX API path.

    The production implementation would call ``fetch_flights`` from
    ``fl3xx_client``.  For now we build a tiny dataframe to show how the
    schedule table updates without requiring external credentials.
    """

    sample_rows = pd.DataFrame(
        [
            {
                "Booking": "FLX-001",
                "Off-Block (Sched)": "01.03.2024 15:00",
                "On-Block (Sched)": "01.03.2024 18:15",
                "From (ICAO)": "CYUL",
                "To (ICAO)": "KBOS",
                "Flight time (Est)": "03:15",
                "PIC": "Doe",
                "SIC": "Roe",
                "Account": "Demo",
                "Aircraft": "C-GXYZ",
                "Aircraft Type": "CL35",
                "Workflow": "Confirmed",
            },
        ],
        columns=FL3XX_SCHEDULE_COLUMNS,
    )

    schedule = ScheduleData(
        frame=sample_rows,
        source="fl3xx_api",
        raw_bytes=None,
        metadata={"updated_at": _utc_timestamp(), "flight_count": 1},
    )
    _handle_schedule_loaded(schedule, "Loaded sample flight from FL3XX API")


def send_notification(message_box: ui.textarea) -> None:
    message = (message_box.value or "").strip()
    if not message:
        ui.notify("Add a message before sending a notification", type="warning")
        return

    timestamp = _utc_timestamp()
    ui.notify("Notification sent ✅", type="positive")
    if notification_log is not None:
        notification_log.push(f"{timestamp} · {message}")


# ---------------------------------------------------------------------------
# Page layout
# ---------------------------------------------------------------------------


nicegui_app.add_static_files("/static", os.path.join(os.path.dirname(__file__), "docs"))

ui.page_title("FF Dashboard (NiceGUI)")

with ui.header().classes("items-center justify-between"):
    ui.label("FF Dashboard (App Runner)").classes("text-lg font-medium")
    ui.button("Load sample flight", on_click=simulate_fetch_from_fl3xx).props("color=primary")

with ui.column().classes("w-full max-w-6xl mx-auto gap-6 py-4"):
    with ui.card().classes("w-full"):
        status_label = ui.label(_format_metadata(None)).classes("text-sm text-gray-600")
        ui.separator()
        with ui.row().classes("w-full gap-4"):
            ui.upload(on_upload=load_schedule_from_upload)
            message_box = ui.textarea("Notification message").props("rows=3 auto-grow")
            ui.button(
                "Send inline notification",
                on_click=lambda: send_notification(message_box),
            ).props("color=secondary")

        with ui.expansion("Notification history", icon="notifications").classes("w-full"):
            notification_log = ui.log(max_lines=50).classes("h-40")

    with ui.card().classes("w-full"):
        ui.label("Schedule").classes("text-base font-medium mb-2")
        table_component = ui.table(
            columns=_table_columns(FL3XX_SCHEDULE_COLUMNS),
            rows=[],
            row_key="Booking",
        ).classes("w-full")
        table_component.props("dense flat bordered")


# ---------------------------------------------------------------------------
# Application entrypoint
# ---------------------------------------------------------------------------


def _port() -> int:
    try:
        return int(os.getenv("PORT", "8080"))
    except ValueError:
        return 8080


ui.run(
    host="0.0.0.0",
    port=_port(),
    show=False,
)

