"""NiceGUI entrypoint for running the dashboard on AWS App Runner."""
from __future__ import annotations  # optional on Python 3.11

# --- make local vendor/ available before any imports ---
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "vendor"))

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Iterable
from secrets_diagnostics import SecretSection, collect_secret_diagnostics


# --- pandas guard (never crash the process if it’s missing) ---
try:
    import pandas as pd
    PANDAS_ERROR = None
except Exception as e:
    pd = None  # type: ignore
    PANDAS_ERROR = e

# --- NiceGUI guard; start a tiny HTTP server if NiceGUI is missing ---
NICEGUI_ERROR = None
try:
    from nicegui import app as nicegui_app
    from nicegui import ui
    from nicegui.events import UploadEventArguments
except Exception as e:
    NICEGUI_ERROR = e

def _port() -> int:
    try:
        return int(os.getenv("PORT", "8080"))
    except ValueError:
        return 8080

if NICEGUI_ERROR:
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            msg = (
                "<h1>App Runner is up</h1>"
                f"<p>NiceGUI failed to import: <code>{NICEGUI_ERROR}</code></p>"
                "<p>Check requirements.txt and build logs for 'nicegui OK'.</p>"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)

    if __name__ == "__main__":
        HTTPServer(("0.0.0.0", _port()), Handler).serve_forever()
    raise SystemExit(0)

# Tell python-socketio/engineio to ping frequently (helps proxies)
import socketio
import engineio
try:
    # These attributes exist in recent versions; guarded to be safe
    engineio.async_drivers.gevent  # no-op reference to ensure engineio is loaded
except Exception:
    pass


# --- data_sources guard (ensures IMPORT_ERROR is always defined) ---
IMPORT_ERROR = None
try:
    from data_sources import FL3XX_SCHEDULE_COLUMNS, ScheduleData, load_schedule
except Exception as e:
    IMPORT_ERROR = e
    # Fallbacks so the app can still run
    FL3XX_SCHEDULE_COLUMNS = [
        "Booking", "Off-Block (Sched)", "On-Block (Sched)",
        "From (ICAO)", "To (ICAO)", "Flight time (Est)",
        "PIC", "SIC", "Account", "Aircraft", "Aircraft Type", "Workflow",
    ]
    class ScheduleData:  # minimal shim
        def __init__(self, frame, source, raw_bytes=None, metadata=None):
            self.frame = frame
            self.source = source
            self.raw_bytes = raw_bytes
            self.metadata = metadata or {}
    def load_schedule(*args, **kwargs):
        raise RuntimeError(f"data_sources not available: {e!r}")




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
# secrets UI state
secret_state = SimpleNamespace(sections=[])



def _refresh_table() -> None:
    if table_component is None:
        return
    table_component.rows = _rows_from_schedule(schedule_state.data)
    table_component.update()


def _refresh_status() -> None:
    if status_label is None:
        return
    status_label.text = _format_metadata(schedule_state.data)

# First render of secrets diagnostics
try:
    secret_state.sections = collect_secret_diagnostics()
except Exception as _e:  # defensive: don’t crash UI if diagnostics fail
    secret_state.sections = []


def _render_secret_sections(container: ui.column, sections: list[SecretSection]) -> None:
    container.clear()

    if not sections:
        with container:
            ui.label(
                "No secret-driven integrations detected."
            ).classes("text-sm text-gray-600")
        return

    columns = [
        {"name": "item", "label": "Item", "field": "item", "align": "left"},
        {"name": "status", "label": "Status", "field": "status", "align": "left"},
        {"name": "source", "label": "Source", "field": "source", "align": "left"},
        {"name": "detail", "label": "Details", "field": "detail", "align": "left"},
    ]

    for section in sections:
        with container:
            with ui.expansion(section.title, value=section.has_warning).classes("w-full"):
                rows = [
                    {
                        "item": row.item,
                        "status": row.status,
                        "source": row.source,
                        "detail": row.detail,
                    }
                    for row in section.rows
                ]
                table = ui.table(columns=columns, rows=rows).classes("w-full")
                table.props("dense flat bordered")


def refresh_secret_diagnostics() -> None:
    secret_state.sections = collect_secret_diagnostics()
    if secret_sections_container is not None:
        _render_secret_sections(secret_sections_container, secret_state.sections)
    ui.notify("Secrets diagnostics refreshed", type="positive")


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

    try:
        schedule = load_schedule("csv_upload", csv_bytes=content, metadata=metadata)
    except Exception as exc:  # pragma: no cover - defensive guard for runtime issues
        ui.notify(f"Unable to parse {event.name}: {exc}", type="negative")
        print(f"CSV upload failed for {event.name}: {exc}")
        return

    _handle_schedule_loaded(
        schedule,
        f"Loaded {len(schedule.frame)} flights from {event.name}",
    )


_SAMPLE_FLIGHT = {
    "bookingIdentifier": "FLX-001",
    "blockOffEstUTC": "2024-03-01T15:00:00Z",
    "blockOnEstUTC": "2024-03-01T18:15:00Z",
    "airportFrom": "CYUL",
    "airportTo": "KBOS",
    "picName": "Doe",
    "sicName": "Roe",
    "accountName": "Demo",
    "registrationNumber": "C-GXYZ",
    "aircraftCategory": "CL35",
    "workflowCustomName": "Confirmed",
}


def simulate_fetch_from_fl3xx() -> None:
    """Demonstrate populating the schedule via the FL3XX normalization path."""

    if IMPORT_ERROR:
        ui.notify(
            "FL3XX helpers are unavailable in this build; upload a CSV instead.",
            type="warning",
        )
        print(f"Sample flight skipped because data_sources import failed: {IMPORT_ERROR}")
        return

    metadata = {
        "flights": [_SAMPLE_FLIGHT],
        "updated_at": _utc_timestamp(),
        "sample": True,
    }

    try:
        schedule = load_schedule("fl3xx_api", metadata=metadata)
    except Exception as exc:  # pragma: no cover - runtime safety net
        ui.notify(f"Unable to load sample flight: {exc}", type="negative")
        print(f"Sample FL3XX load failed: {exc}")
        return

    _handle_schedule_loaded(
        schedule,
        "Loaded sample flight using the FL3XX API formatter",
    )


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


# CHANGED: only register /static if folder exists
docs_dir = os.path.join(os.path.dirname(__file__), "docs")
if os.path.isdir(docs_dir):
    nicegui_app.add_static_files("/static", docs_dir)


ui.page_title("FF Dashboard (NiceGUI)")

with ui.header().classes("items-center justify-between"):
    ui.label("FF Dashboard (App Runner)").classes("text-lg font-medium")
    ui.button("Load sample flight", on_click=simulate_fetch_from_fl3xx).props("color=primary")

# NEW: visible warning if import failed
if IMPORT_ERROR:
    with ui.message_bar():
        ui.icon('warning')
        ui.label(f"data_sources import failed: {IMPORT_ERROR}. Using fallback columns.")

if PANDAS_ERROR:
    with ui.message_bar():
        ui.icon('warning')
        ui.label(f"pandas not available: {PANDAS_ERROR}. Demo features limited.")



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

    with ui.card().classes("w-full"):
        with ui.row().classes("items-center justify-between w-full"):
            ui.label("Secrets diagnostics").classes("text-base font-medium")
            ui.button("Refresh", on_click=refresh_secret_diagnostics).props("outline")

        container = ui.column().classes("w-full gap-2 mt-2")
        secret_sections_container = container  # type: ignore[assignment]
        _render_secret_sections(container, secret_state.sections)


# ---------------------------------------------------------------------------
# Application entrypoint
# ---------------------------------------------------------------------------

def _port() -> int:
    import os
    try:
        return int(os.getenv("PORT", "8080"))
    except ValueError:
        return 8080

try:
    # NiceGUI exposes the socket.io server via ui.run options in newer versions;
    # when not available, this is safe to skip.
    ui.config.socket_io_ping_interval = 20   # seconds
    ui.config.socket_io_ping_timeout = 30    # seconds
except Exception:
    pass


ui.run(
    host="0.0.0.0",
    port=_port(),
    show=False,
    proxy_headers=True,
    forwarded_allow_ips="*",
    http="h11",          # <- prefer h11 (HTTP/1.1) behind App Runner
    ws="wsproto",        # <- use wsproto WS implementation (more tolerant behind proxies)
    uvicorn_logging_level="debug",
)
