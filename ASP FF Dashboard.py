# daily_ops_dashboard.py  — FF Dashboard with inline Notify + local ETA conversion

import os
import re
import json
import sqlite3
import imaplib, email
from collections import defaultdict
from collections.abc import Iterable, Mapping
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from typing import Any

import pandas as pd
import streamlit as st
from dateutil import parser as dateparse
from dateutil.tz import tzoffset
from pathlib import Path
import tzlocal  # for local-time HHMM in the notify message
import pytz  # NEW: for airport-local ETA conversion
import requests

try:
    import boto3
    from boto3.dynamodb.conditions import Key
except ImportError:  # pragma: no cover - optional dependency
    boto3 = None
    Key = None

from data_sources import ScheduleData, ScheduleSource, load_schedule
from fl3xx_client import (
    DEFAULT_FL3XX_BASE_URL,
    Fl3xxApiConfig,
    compute_flights_digest,
    enrich_flights_with_crew,
    fetch_flights,
)
from flightaware_status import (
    DEFAULT_AEROAPI_BASE_URL,
    FlightAwareStatusConfig,
    build_status_payload,
    derive_event_times,
    fetch_flights_for_ident,
)
from schedule_phases import (
    SCHEDULE_PHASES,
    categorize_dataframe_by_phase,
    filtered_columns_for_phase,
)
# Global lookup maps populated after loading airport metadata. Define them early so
# helper functions can reference the names during the initial Streamlit run.
ICAO_TZ_MAP: dict[str, str] = {}
ICAO_TO_IATA_MAP: dict[str, str] = {}
IATA_TO_ICAO_MAP: dict[str, str] = {}
LOCAL_TZ = tzlocal.get_localzone()

# ============================
# Page config
# ============================
st.set_page_config(page_title="Daily Ops Dashboard (Schedule + Status)", layout="wide")
st.title("Daily Ops Dashboard (Schedule + Status)")

if "show_utc_clock" not in st.session_state:
    st.session_state["show_utc_clock"] = True

_clock_placeholder = st.empty()


def _render_floating_clock(enabled: bool) -> None:
    if enabled:
        _clock_placeholder.markdown(
            """
            <div id="utc-clock" style="position: fixed; top: 10px; right: 16px; z-index: 2000;">
              <div style="background: rgba(255, 255, 255, 0.94); color: #0f172a; padding: 6px 12px; border-radius: 10px; font-family: 'Inter', system-ui, -apple-system, sans-serif; font-size: 14px; font-weight: 500; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.12); border: 1px solid rgba(15, 23, 42, 0.08);">
                <span id="utc-clock-label"></span>
              </div>
            </div>
            <script>
            const utcClockEl = document.getElementById('utc-clock-label');
            const pad = (n) => String(n).padStart(2, '0');
            const renderUtcTime = () => {
              const now = new Date();
              const ts = `${now.getUTCFullYear()}-${pad(now.getUTCMonth() + 1)}-${pad(now.getUTCDate())} ${pad(now.getUTCHours())}:${pad(now.getUTCMinutes())}:${pad(now.getUTCSeconds())} UTC`;
              if (utcClockEl) utcClockEl.textContent = ts;
            };
            renderUtcTime();
            if (window.utcClockInterval) { clearInterval(window.utcClockInterval); }
            window.utcClockInterval = setInterval(renderUtcTime, 1000);
            </script>
            """,
            unsafe_allow_html=True,
        )
    else:
        _clock_placeholder.empty()


caption_col, toggle_col = st.columns([5, 1])
with caption_col:
    st.caption(
        "Times shown in **UTC**. Some airports may be blank (non-ICAO). "
        "Rows with non-tail placeholders (e.g., “Remove OCS”, “Add EMB”) are hidden."
    )
with toggle_col:
    st.toggle("Show UTC clock", key="show_utc_clock")

_render_floating_clock(st.session_state.get("show_utc_clock", True))

# Shared styles for flashing landed rows awaiting block-on confirmation
st.markdown(
    """
    <style>
    @keyframes landed-on-alert {
      0%   { background-color: rgba(255, 193, 7, 0.18); }
      50%  { background-color: rgba(255, 241, 118, 0.65); }
      100% { background-color: rgba(255, 193, 7, 0.18); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "inline_edit_toast" in st.session_state:
    st.success(st.session_state.pop("inline_edit_toast"))

# ============================
# Auto-refresh controls (no page reload fallback)
# ============================
refresh_sec = st.number_input(
    "Refresh every (sec)", min_value=5, max_value=600, value=180, step=5
)

try:
    from streamlit_autorefresh import st_autorefresh
    st_autorefresh(interval=int(refresh_sec * 1000), key="ops_auto_refresh")
except Exception:
    st.warning(
        "Auto-refresh requires the 'streamlit-autorefresh' package. "
        "Add `streamlit-autorefresh` to requirements.txt."
    )

# ============================
# SQLite persistence (statuses + CSV)
# ============================
DB_PATH = "status_store.db"

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS status_events (
            booking TEXT NOT NULL,
            event_type TEXT NOT NULL,  -- 'Departure' | 'Arrival' | 'ArrivalForecast' | 'Diversion' | 'EDCT'
            status TEXT NOT NULL,      -- canonical label for that event
            actual_time_utc TEXT,
            delta_min INTEGER,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (booking, event_type)
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS csv_store (
            id INTEGER PRIMARY KEY CHECK (id=1),
            name TEXT,
            content BLOB,
            uploaded_at TEXT NOT NULL
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS email_cursor (
            mailbox TEXT PRIMARY KEY,
            last_uid INTEGER
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS tail_overrides (
            booking TEXT PRIMARY KEY,
            tail TEXT,
            updated_at TEXT NOT NULL
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS fl3xx_cache (
            id INTEGER PRIMARY KEY CHECK (id=1),
            payload TEXT,
            hash TEXT,
            fetched_at TEXT,
            from_date TEXT,
            to_date TEXT
        )
        """)

def load_status_map() -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("""
            SELECT booking, event_type, status, actual_time_utc, delta_min
            FROM status_events
        """).fetchall()
    m = {}
    for booking, event_type, status, actual, delta in rows:
        m.setdefault(booking, {})[event_type] = {
            "status": status, "actual_time_utc": actual, "delta_min": delta
        }
    return m

def upsert_status(booking, event_type, status, actual_time_iso, delta_min):
    if delta_min is None:
        delta_value = None
    else:
        try:
            if pd.isna(delta_min):
                delta_value = None
            else:
                delta_value = int(delta_min)
        except (TypeError, ValueError):
            delta_value = None
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO status_events (booking, event_type, status, actual_time_utc, delta_min, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(booking, event_type) DO UPDATE SET
                status=excluded.status,
                actual_time_utc=excluded.actual_time_utc,
                delta_min=excluded.delta_min,
                updated_at=datetime('now')
        """, (booking, event_type, status, actual_time_iso, delta_value))

def delete_status(booking: str, event_type: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM status_events WHERE booking=? AND event_type=?", (booking, event_type))


def save_csv_to_db(name: str, content_bytes: bytes):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO csv_store (id, name, content, uploaded_at)
            VALUES (1, ?, ?, datetime('now'))
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                content=excluded.content,
                uploaded_at=datetime('now')
        """, (name, content_bytes))

def load_csv_from_db():
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT name, content, uploaded_at FROM csv_store WHERE id=1").fetchone()
    if row:
        return row[0], row[1], row[2]
    return None, None, None

def get_last_uid(mailbox: str) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT last_uid FROM email_cursor WHERE mailbox=?", (mailbox,)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0

def set_last_uid(mailbox: str, uid: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO email_cursor (mailbox, last_uid)
        VALUES (?, ?)
        ON CONFLICT(mailbox) DO UPDATE SET last_uid=excluded.last_uid
        """, (mailbox, int(uid)))

def load_tail_overrides() -> dict[str, str]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute("SELECT booking, tail FROM tail_overrides").fetchall()
    return {str(booking): tail for booking, tail in rows if tail}

def upsert_tail_override(booking: str, tail: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO tail_overrides (booking, tail, updated_at)
            VALUES (?, ?, datetime('now'))
            ON CONFLICT(booking) DO UPDATE SET
                tail=excluded.tail,
                updated_at=datetime('now')
        """, (booking, tail))

def delete_tail_override(booking: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM tail_overrides WHERE booking=?", (booking,))


def load_fl3xx_cache():
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT payload, hash, fetched_at, from_date, to_date FROM fl3xx_cache WHERE id=1"
        ).fetchone()
    if not row:
        return None
    payload_json, digest, fetched_at, from_date, to_date = row
    try:
        flights = json.loads(payload_json) if payload_json else []
    except json.JSONDecodeError:
        flights = []
    return {
        "flights": flights,
        "hash": digest or "",
        "fetched_at": fetched_at,
        "from_date": from_date,
        "to_date": to_date,
    }


def save_fl3xx_cache(
    flights: list[dict[str, object]],
    *,
    digest: str,
    from_date: str,
    to_date: str,
    fetched_at: str,
):
    payload_json = json.dumps(flights, ensure_ascii=False)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO fl3xx_cache (id, payload, hash, fetched_at, from_date, to_date)
            VALUES (1, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                payload=excluded.payload,
                hash=excluded.hash,
                fetched_at=excluded.fetched_at,
                from_date=excluded.from_date,
                to_date=excluded.to_date
            """,
            (payload_json, digest, fetched_at, from_date, to_date),
        )


init_db()

# ============================
# Helpers
# ============================
FL3XX_REFRESH_MINUTES = 5


def _parse_iso8601(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _to_iso8601_z(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _secret_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return default


def _coerce_mapping(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, Mapping):
            return parsed
    return None


def _read_secret_headers(value: Any) -> Mapping[str, str] | None:
    mapping = _coerce_mapping(value)
    if mapping:
        return {str(k): str(v) for k, v in mapping.items()}
    return None


def _clean_schedule_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    return None


def _read_streamlit_secret(key: str) -> Any | None:
    try:
        secrets_obj = st.secrets
    except Exception:
        return None

    value: Any | None = None

    # Many Streamlit runtimes expose ``.get`` and mapping helpers, but not all.
    getter = getattr(secrets_obj, "get", None)
    if callable(getter):
        try:
            value = getter(key)
        except Exception:
            value = None
    if value is not None:
        return value

    try:
        if isinstance(secrets_obj, Mapping) and key in secrets_obj:
            return secrets_obj[key]
    except Exception:
        value = None

    try:
        if isinstance(secrets_obj, Mapping):
            normalized = str(key).lower()
            for existing_key in secrets_obj:
                try:
                    if str(existing_key).lower() == normalized:
                        return secrets_obj[existing_key]
                except Exception:
                    continue
    except Exception:
        value = None

    try:
        value = secrets_obj[key]  # type: ignore[index]
    except Exception:
        value = None
    if value is not None:
        return value

    try:
        value = getattr(secrets_obj, key)
    except Exception:
        value = None
    if value is not None:
        return value

    try:
        raw_store = getattr(secrets_obj, "_secrets", None)
        if isinstance(raw_store, Mapping) and key in raw_store:
            return raw_store[key]
    except Exception:
        return None

    return None


def _mapping_get(mapping: Any, key: str) -> Any | None:
    if not isinstance(mapping, Mapping):
        return None

    try:
        if key in mapping:
            return mapping[key]
    except Exception:
        pass

    try:
        normalized = str(key).lower()
        for existing_key in mapping:
            try:
                if str(existing_key).lower() == normalized:
                    return mapping[existing_key]
            except Exception:
                continue
    except Exception:
        pass

    getter = getattr(mapping, "get", None)
    if callable(getter):
        try:
            value = getter(key)
        except Exception:
            value = None
        else:
            if value is not None:
                return value

    try:
        return mapping[key]
    except Exception:
        return None


def _normalize_secret_value(value: Any, *, allow_blank: bool = False) -> Any | None:
    if value is None:
        return None
    if isinstance(value, str):
        trimmed = value.strip()
        if not trimmed and not allow_blank:
            return None
        return trimmed
    return value


def _fl3xx_secret_diagnostics_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    def _add_row(source: str, present: bool, details: str) -> None:
        rows.append(
            {
                "Source": source,
                "Detected": "✅" if present else "⚠️",
                "Details": details,
            }
        )

    for key in ("fl3xx_api", "FL3XX_API", "FL3XX"):
        raw = _read_streamlit_secret(key)
        mapping = _coerce_mapping(raw)
        if mapping is None:
            if raw is None:
                _add_row(f"st.secrets['{key}']", False, "Secret not defined")
            else:
                _add_row(
                    f"st.secrets['{key}']",
                    False,
                    f"Found {type(raw).__name__}; expected a mapping with credential keys.",
                )
            continue

        api_token = _normalize_secret_value(mapping.get("api_token"), allow_blank=True)
        auth_header = _normalize_secret_value(
            mapping.get("auth_header") or mapping.get("authorization"), allow_blank=True
        )

        _add_row(
            f"st.secrets['{key}']['api_token']",
            bool(api_token),
            "Value provided" if api_token else "Missing or blank value",
        )
        _add_row(
            f"st.secrets['{key}']['auth_header']",
            bool(auth_header),
            "Value provided" if auth_header else "Missing or blank value",
        )

    for secret_key in ("FL3XX_TOKEN", "FL3XX_API_TOKEN"):
        secret_value = _normalize_secret_value(
            _read_streamlit_secret(secret_key), allow_blank=True
        )
        if secret_value is not None:
            _add_row(
                f"st.secrets['{secret_key}']",
                bool(secret_value),
                "Value provided" if secret_value else "Value present but blank",
            )
        else:
            _add_row(f"st.secrets['{secret_key}']", False, "Secret not defined")

    for env_key in ("FL3XX_TOKEN", "FL3XX_API_TOKEN"):
        env_token = _normalize_secret_value(os.getenv(env_key), allow_blank=True)
        _add_row(
            f"env:{env_key}",
            bool(env_token),
            "Environment variable set" if env_token else "Environment variable not set",
        )

    base_url_secret = _normalize_secret_value(
        _read_streamlit_secret("FL3XX_BASE_URL"), allow_blank=True
    )
    if base_url_secret:
        _add_row(
            "st.secrets['FL3XX_BASE_URL']",
            True,
            "Custom base URL set",
        )
    else:
        _add_row(
            "st.secrets['FL3XX_BASE_URL']",
            True,
            f"Not set; defaulting to {DEFAULT_FL3XX_BASE_URL}",
        )

    base_url_env = _normalize_secret_value(os.getenv("FL3XX_BASE_URL"), allow_blank=True)
    if base_url_env:
        _add_row(
            "env:FL3XX_BASE_URL",
            True,
            "Custom base URL set",
        )
    else:
        _add_row(
            "env:FL3XX_BASE_URL",
            True,
            f"Not set; defaulting to {DEFAULT_FL3XX_BASE_URL}",
        )

    auth_header_name = _normalize_secret_value(
        _read_streamlit_secret("FL3XX_AUTH_HEADER_NAME") or os.getenv("FL3XX_AUTH_HEADER_NAME"),
        allow_blank=True,
    )
    if auth_header_name:
        _add_row("FL3XX_AUTH_HEADER_NAME", True, f"Using header name '{auth_header_name}'")
    else:
        _add_row(
            "FL3XX_AUTH_HEADER_NAME",
            True,
            "Falling back to default Authorization header",
        )

    return rows


def _flightaware_aeroapi_secret_diagnostics_rows() -> list[dict[str, str]]:
    """Return diagnostics for FlightAware AeroAPI configuration."""

    rows: list[dict[str, str]] = []

    def _add_row(source: str, present: bool, details: str) -> None:
        rows.append(
            {
                "Source": source,
                "Detected": "✅" if present else "⚠️",
                "Details": details,
            }
        )

    api_key = _normalize_secret_value(_resolve_secret("FLIGHTAWARE_API_KEY"), allow_blank=True)
    _add_row(
        "FLIGHTAWARE_API_KEY",
        bool(api_key),
        "Value provided" if api_key else "Missing or blank value",
    )

    base_url = _normalize_secret_value(_resolve_secret("FLIGHTAWARE_API_BASE"), allow_blank=True)
    if base_url:
        _add_row("FLIGHTAWARE_API_BASE", True, f"Using custom base URL '{base_url}'")
    else:
        _add_row(
            "FLIGHTAWARE_API_BASE",
            True,
            f"Defaulting to {DEFAULT_AEROAPI_BASE_URL}",
        )

    extra_headers = _read_secret_headers(_resolve_secret("FLIGHTAWARE_EXTRA_HEADERS"))
    if extra_headers:
        _add_row(
            "FLIGHTAWARE_EXTRA_HEADERS",
            True,
            f"{len(extra_headers)} header(s) detected",
        )

    timeout_val = _normalize_secret_value(_resolve_secret("FLIGHTAWARE_TIMEOUT"), allow_blank=True)
    if timeout_val:
        _add_row("FLIGHTAWARE_TIMEOUT", True, f"Custom timeout = {timeout_val}s")

    verify_val = _normalize_secret_value(_resolve_secret("FLIGHTAWARE_VERIFY_SSL"), allow_blank=True)
    if verify_val is not None:
        _add_row("FLIGHTAWARE_VERIFY_SSL", True, f"verify_ssl = {verify_val}")

    return rows


def _flightaware_webhook_secret_diagnostics_rows() -> list[dict[str, str]]:
    """Return diagnostics for the FlightAware webhook/DynamoDB integration."""

    rows: list[dict[str, str]] = []

    def _add_row(source: str, present: bool, details: str) -> None:
        rows.append(
            {
                "Source": source,
                "Detected": "✅" if present else "⚠️",
                "Details": details,
            }
        )

    for key in ("AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
        value = _normalize_secret_value(_resolve_secret(key), allow_blank=True)
        _add_row(key, bool(value), "Value provided" if value else "Missing or blank value")

    table_name = _normalize_secret_value(
        _resolve_secret("FLIGHTAWARE_ALERTS_TABLE"), allow_blank=True
    )
    if table_name:
        _add_row("FLIGHTAWARE_ALERTS_TABLE", True, f"Using table '{table_name}'")
    else:
        _add_row("FLIGHTAWARE_ALERTS_TABLE", True, "Defaulting to 'fa-oooi-alerts'")

    session_token = _normalize_secret_value(_resolve_secret("AWS_SESSION_TOKEN"), allow_blank=True)
    if session_token is not None:
        _add_row(
            "AWS_SESSION_TOKEN",
            bool(session_token),
            "Value provided" if session_token else "Session token not set",
        )

    if boto3 is None or Key is None:
        _add_row(
            "boto3",
            False,
            "Install boto3 to enable DynamoDB connectivity diagnostics.",
        )
    else:
        diag_ok, diag_msg = _diag_flightaware_webhook()
        _add_row(
            "DynamoDB connectivity",
            diag_ok,
            "Diagnostics succeeded" if diag_ok else diag_msg,
        )

    return rows


def _imap_secret_diagnostics_rows() -> list[dict[str, str]]:
    """Return diagnostics for the IMAP inbox integration."""

    rows: list[dict[str, str]] = []

    def _add_row(source: str, present: bool, details: str) -> None:
        rows.append(
            {
                "Source": source,
                "Detected": "✅" if present else "⚠️",
                "Details": details,
            }
        )

    for key in ("IMAP_HOST", "IMAP_USER", "IMAP_PASS"):
        value = _normalize_secret_value(_resolve_secret(key), allow_blank=True)
        _add_row(key, bool(value), "Value provided" if value else "Missing or blank value")

    folder = _normalize_secret_value(
        _resolve_secret("IMAP_FOLDER", default="INBOX"), allow_blank=True
    )
    if folder:
        _add_row("IMAP_FOLDER", True, f"Using folder '{folder}'")

    sender = _normalize_secret_value(_resolve_secret("IMAP_SENDER"), allow_blank=True)
    if sender is not None:
        _add_row(
            "IMAP_SENDER",
            bool(sender),
            "Sender filter provided" if sender else "Sender filter not set",
        )

    rows.append(
        {
            "Source": "IMAP processing mode",
            "Detected": "✅",
            "Details": "EDCT-only processing is enabled",
        }
    )

    return rows


def _collect_secret_diagnostics() -> list[tuple[str, list[dict[str, str]]]]:
    """Gather diagnostics for all secret-driven integrations."""

    return [
        ("FL3XX API", _fl3xx_secret_diagnostics_rows()),
        ("FlightAware AeroAPI", _flightaware_aeroapi_secret_diagnostics_rows()),
        ("FlightAware webhook (DynamoDB)", _flightaware_webhook_secret_diagnostics_rows()),
        ("Flight status email inbox", _imap_secret_diagnostics_rows()),
    ]


def _resolve_secret(*keys: str, default: Any | None = None, allow_blank: bool = False) -> Any | None:
    for key in keys:
        candidates = [key]
        if key.lower() != key:
            candidates.append(key.lower())

        for candidate in candidates:
            value = _normalize_secret_value(
                _read_streamlit_secret(candidate), allow_blank=allow_blank
            )

            if value is None and "_" in candidate:
                section, _, nested = candidate.partition("_")
                section_value = _read_streamlit_secret(section)
                if isinstance(section_value, Mapping):
                    for nested_key in {nested, nested.upper(), nested.lower()}:
                        nested_value = _normalize_secret_value(
                            _mapping_get(section_value, nested_key), allow_blank=allow_blank
                        )
                        if nested_value is not None:
                            value = nested_value
                            break

            if value is None:
                env_names = [
                    key,
                    candidate,
                    key.upper(),
                    key.lower(),
                    candidate.upper(),
                    candidate.lower(),
                ]
                for env_name in dict.fromkeys(env_names):
                    env_val = _normalize_secret_value(
                        os.getenv(env_name), allow_blank=allow_blank
                    )
                    if env_val is not None:
                        value = env_val
                        break

            if value is not None:
                return value

    return default


def build_flightaware_status_config() -> FlightAwareStatusConfig | None:
    api_key = _resolve_secret("FLIGHTAWARE_API_KEY")
    if not api_key:
        return None

    base = _resolve_secret("FLIGHTAWARE_API_BASE")
    if not base:
        base = DEFAULT_AEROAPI_BASE_URL

    timeout_val = _resolve_secret("FLIGHTAWARE_TIMEOUT")
    try:
        timeout = int(timeout_val) if timeout_val is not None else 30
    except (TypeError, ValueError):
        timeout = 30

    verify = _secret_bool(_resolve_secret("FLIGHTAWARE_VERIFY_SSL"), default=True)

    extra_headers = _read_secret_headers(_resolve_secret("FLIGHTAWARE_EXTRA_HEADERS"))

    return FlightAwareStatusConfig(
        base_url=str(base),
        api_key=str(api_key),
        extra_headers=extra_headers,
        verify_ssl=verify,
        timeout=timeout,
    )


def build_flightaware_webhook_config() -> dict[str, Any] | None:
    """Read the DynamoDB webhook configuration from Streamlit secrets."""

    if boto3 is None or Key is None:
        return None

    region = _resolve_secret("AWS_REGION")
    if not region:
        return None

    table_name = _resolve_secret("FLIGHTAWARE_ALERTS_TABLE") or "fa-oooi-alerts"

    access_key = _resolve_secret("AWS_ACCESS_KEY_ID")
    secret_key = _resolve_secret("AWS_SECRET_ACCESS_KEY")
    session_token = _resolve_secret("AWS_SESSION_TOKEN")

    per_ident_val = _resolve_secret("FLIGHTAWARE_ALERTS_PER_IDENT")
    try:
        per_ident = int(per_ident_val) if per_ident_val is not None else 25
    except (TypeError, ValueError):
        per_ident = 25

    cache_ttl_val = _resolve_secret("FLIGHTAWARE_ALERTS_CACHE_TTL")
    try:
        cache_ttl = int(cache_ttl_val) if cache_ttl_val is not None else 20
    except (TypeError, ValueError):
        cache_ttl = 20

    return {
        "region": str(region),
        "table_name": str(table_name),
        "access_key": str(access_key) if access_key else None,
        "secret_key": str(secret_key) if secret_key else None,
        "session_token": str(session_token) if session_token else None,
        "per_ident": per_ident,
        "cache_ttl": cache_ttl,
    }


def _diag_flightaware_webhook() -> tuple[bool, str]:
    """Perform a lightweight connectivity check for the FlightAware webhook table."""

    if boto3 is None or Key is None:
        return False, "boto3 is not installed. Add boto3 to requirements.txt to enable DynamoDB access."

    required = [
        "AWS_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "FLIGHTAWARE_ALERTS_TABLE",
    ]
    missing = []
    for name in required:
        value = _resolve_secret(name)
        if not value:
            if name == "FLIGHTAWARE_ALERTS_TABLE":
                value = "fa-oooi-alerts"
            if not value:
                missing.append(name)
    if missing:
        return False, f"Missing secrets: {', '.join(sorted(missing))}"

    try:
        session = boto3.Session(
            aws_access_key_id=_resolve_secret("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=_resolve_secret("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=_resolve_secret("AWS_SESSION_TOKEN"),
            region_name=_resolve_secret("AWS_REGION"),
        )
        table = session.resource("dynamodb").Table(
            _resolve_secret("FLIGHTAWARE_ALERTS_TABLE") or "fa-oooi-alerts"
        )
    except Exception as exc:  # pragma: no cover - environment specific
        return False, f"DynamoDB session setup failed: {exc}"

    try:
        table.load()
    except Exception as exc:  # pragma: no cover - environment specific
        return False, f"DynamoDB DescribeTable failed: {exc}"

    try:
        table.query(
            KeyConditionExpression=Key("ident").eq("ASP501"),
            Limit=1,
            ScanIndexForward=False,
        )
    except Exception as exc:  # pragma: no cover - environment specific
        # Permission or table errors should surface, but empty result sets are fine
        return False, f"DynamoDB query failed: {exc}"

    return True, "ok"


def _coerce_dynamodb_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        try:
            integral = value.to_integral_value()
        except Exception:
            integral = None
        if integral is not None and integral == value:
            return int(integral)
        return float(value)
    if isinstance(value, list):
        return [_coerce_dynamodb_value(v) for v in value]
    if isinstance(value, dict):
        return {k: _coerce_dynamodb_value(v) for k, v in value.items()}
    return value


def _parse_dynamodb_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, Decimal):
        value = float(value)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        return parse_any_dt_string_to_utc(value)
    return None


def fetch_flightaware_webhook_events(
    idents: list[str],
    config: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Fetch recent FlightAware webhook alerts from DynamoDB for the provided idents."""

    if not idents:
        return []
    if boto3 is None or Key is None:
        raise RuntimeError("boto3 is required to fetch FlightAware webhook alerts")

    cache = st.session_state.setdefault("_webhook_alert_cache", {})
    cache_key = (tuple(sorted(idents)), config.get("table_name"))
    ttl_seconds = int(config.get("cache_ttl", 20))
    now = datetime.now(timezone.utc)
    cached = cache.get(cache_key)
    if cached:
        fetched_at = cached.get("fetched_at")
        if isinstance(fetched_at, datetime) and (now - fetched_at).total_seconds() <= ttl_seconds:
            return cached.get("items", [])

    session = boto3.Session(
        aws_access_key_id=config.get("access_key"),
        aws_secret_access_key=config.get("secret_key"),
        aws_session_token=config.get("session_token"),
        region_name=config.get("region"),
    )
    table = session.resource("dynamodb").Table(str(config.get("table_name")))

    per_ident = int(config.get("per_ident", 25))
    rows: list[dict[str, Any]] = []

    for ident in idents:
        ident_clean = str(ident or "").strip()
        if not ident_clean:
            continue
        try:
            resp = table.query(
                KeyConditionExpression=Key("ident").eq(ident_clean),
                ScanIndexForward=False,
                Limit=per_ident,
            )
        except Exception as exc:
            raise RuntimeError(f"DynamoDB query failed for ident {ident_clean}: {exc}") from exc

        for item in resp.get("Items", []):
            normalised = {str(k): _coerce_dynamodb_value(v) for k, v in item.items()}
            normalised.setdefault("ident", ident_clean)
            normalised.setdefault("received_at", normalised.get("received_at") or normalised.get("timestamp"))
            event_dt = None
            for key in ("source_ts", "event_time", "event_ts", "eventTime", "occurred_at", "received_at"):
                event_dt = _parse_dynamodb_timestamp(normalised.get(key))
                if event_dt:
                    break
            normalised["_event_dt"] = event_dt
            rows.append(normalised)

    rows.sort(key=lambda rec: rec.get("_event_dt") or datetime.min)

    cache[cache_key] = {"fetched_at": now, "items": rows}
    return rows


def fetch_aeroapi_status_updates(
    frame: pd.DataFrame,
    config: FlightAwareStatusConfig,
) -> dict[str, dict[str, dict[str, object]]]:
    if frame.empty:
        return {}

    updates: dict[str, dict[str, dict[str, object]]] = {}

    with requests.Session() as session:
        for tail, group in frame.groupby("Aircraft"):
            if tail is None or (isinstance(tail, float) and pd.isna(tail)):
                continue
            tail_str = str(tail).strip()
            if not tail_str:
                continue

            try:
                flights = fetch_flights_for_ident(config, tail_str, session=session)
            except Exception as exc:
                raise RuntimeError(f"FlightAware fetch failed for {tail_str}: {exc}") from exc

            if not flights:
                continue

            remaining = list(flights)

            for _, row in group.sort_values("ETD_UTC").iterrows():
                sched_dep = _clean_schedule_ts(row.get("ETD_UTC"))
                sched_arr = _clean_schedule_ts(row.get("ETA_UTC"))

                best_idx: int | None = None
                best_score: float | None = None
                best_events: dict[str, Any] | None = None

                for idx, flight in enumerate(remaining):
                    events = dict(derive_event_times(flight))
                    anchor = events.get("Departure") or events.get("ArrivalForecast") or events.get("Arrival")
                    if anchor is None:
                        continue
                    if sched_dep is not None:
                        score = abs((anchor - sched_dep).total_seconds())
                    else:
                        score = (0 if events.get("Departure") else 1_000_000) + idx

                    if best_idx is None or score < best_score:
                        best_idx = idx
                        best_score = score
                        best_events = events

                if best_idx is None or best_events is None:
                    continue

                flight = remaining.pop(best_idx)
                status_payload = build_status_payload(
                    best_events,
                    scheduled_departure=sched_dep,
                    scheduled_arrival=sched_arr,
                )
                if not status_payload:
                    continue

                identifier = (
                    flight.get("ident")
                    or flight.get("fa_flight_id")
                    or flight.get("registration")
                    or tail_str
                )
                flight_id = flight.get("fa_flight_id") or flight.get("flight_id") or flight.get("uuid")

                for payload in status_payload.values():
                    payload.setdefault("identifier", str(identifier).strip())
                    if flight_id:
                        payload.setdefault("flightaware_id", str(flight_id))

                leg_key = row.get("_LegKey") or row.get("Booking")
                if leg_key:
                    updates.setdefault(str(leg_key), {}).update(status_payload)

    return updates


def _build_fl3xx_config_from_secrets() -> Fl3xxApiConfig:
    # The dashboard module normally defines several helper functions at the
    # top level. Unit tests import just this function's source from the legacy
    # filename (with spaces) and execute it in isolation, so we provide
    # fallbacks when those helpers are missing from the execution namespace.
    coerce_mapping = globals().get("_coerce_mapping")
    if coerce_mapping is None:
        import json as _json
        from collections.abc import Mapping as _Mapping

        def coerce_mapping(value):
            if isinstance(value, _Mapping):
                return value
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped:
                    return None
                try:
                    parsed = _json.loads(stripped)
                except _json.JSONDecodeError:
                    return None
                if isinstance(parsed, _Mapping):
                    return parsed
            return None

    read_streamlit_secret = globals().get("_read_streamlit_secret")
    if read_streamlit_secret is None:
        from collections.abc import Mapping as _Mapping

        def read_streamlit_secret(key):
            try:
                secrets_obj = st.secrets
            except Exception:
                return None

            value = None

            getter = getattr(secrets_obj, "get", None)
            if callable(getter):
                try:
                    value = getter(key)
                except Exception:
                    value = None
            if value is not None:
                return value

            try:
                if isinstance(secrets_obj, _Mapping) and key in secrets_obj:
                    return secrets_obj[key]
            except Exception:
                value = None

            try:
                if isinstance(secrets_obj, _Mapping):
                    normalized = str(key).lower()
                    for existing_key in secrets_obj:
                        try:
                            if str(existing_key).lower() == normalized:
                                return secrets_obj[existing_key]
                        except Exception:
                            continue
            except Exception:
                value = None

            try:
                value = secrets_obj[key]  # type: ignore[index]
            except Exception:
                value = None
            if value is not None:
                return value

            try:
                value = getattr(secrets_obj, key)
            except Exception:
                value = None
            if value is not None:
                return value

            try:
                raw_store = getattr(secrets_obj, "_secrets", None)
                if isinstance(raw_store, _Mapping) and key in raw_store:
                    return raw_store[key]
            except Exception:
                return None

            return None

    mapping_get = globals().get("_mapping_get")
    if mapping_get is None:
        from collections.abc import Mapping as _Mapping

        def mapping_get(mapping, key):
            if not isinstance(mapping, _Mapping):
                return None

            try:
                if key in mapping:
                    return mapping[key]
            except Exception:
                pass

            try:
                normalized = str(key).lower()
                for existing_key in mapping:
                    try:
                        if str(existing_key).lower() == normalized:
                            return mapping[existing_key]
                    except Exception:
                        continue
            except Exception:
                pass

            getter = getattr(mapping, "get", None)
            if callable(getter):
                try:
                    value = getter(key)
                except Exception:
                    value = None
                else:
                    if value is not None:
                        return value

            try:
                return mapping[key]
            except Exception:
                return None

    normalize_secret_value = globals().get("_normalize_secret_value")
    if normalize_secret_value is None:

        def normalize_secret_value(value, *, allow_blank=False):
            if value is None:
                return None
            if isinstance(value, str):
                stripped = value.strip()
                if not stripped and not allow_blank:
                    return None
                return stripped
            return value

    secret_bool = globals().get("_secret_bool")
    if secret_bool is None:

        def secret_bool(value, *, default=False):
            if isinstance(value, bool):
                return value
            if value is None:
                return default
            if isinstance(value, (int, float)):
                return bool(value)
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}
            return default

    read_secret_headers = globals().get("_read_secret_headers")
    if read_secret_headers is None:

        def read_secret_headers(value):
            mapping = coerce_mapping(value)
            if mapping:
                return {str(k): str(v) for k, v in mapping.items()}
            return None

    merged: dict[str, Any] = {}
    for key in ("fl3xx_api", "FL3XX_API", "FL3XX"):
        value = coerce_mapping(read_streamlit_secret(key))
        if value:
            merged.update(dict(value))

    base_url_value = (
        merged.get("base_url")
        or read_streamlit_secret("FL3XX_BASE_URL")
        or os.getenv("FL3XX_BASE_URL")
        or DEFAULT_FL3XX_BASE_URL
    )
    base_url = str(base_url_value)

    api_token = (
        merged.get("api_token")
        or read_streamlit_secret("FL3XX_TOKEN")
        or os.getenv("FL3XX_TOKEN")
        or read_streamlit_secret("FL3XX_API_TOKEN")
        or os.getenv("FL3XX_API_TOKEN")
    )
    if api_token is not None:
        api_token = str(api_token)

    auth_header = merged.get("auth_header") or merged.get("authorization")
    if auth_header is not None:
        auth_header = str(auth_header)

    auth_header_name_value = (
        merged.get("auth_header_name")
        or merged.get("authorization_header_name")
        or read_streamlit_secret("FL3XX_AUTH_HEADER_NAME")
        or os.getenv("FL3XX_AUTH_HEADER_NAME")
    )
    if auth_header_name_value is not None:
        auth_header_name = str(auth_header_name_value)
    else:
        auth_header_name = "Authorization"

    token_scheme_value = (
        merged.get("api_token_scheme")
        or merged.get("token_scheme")
        or merged.get("token_type")
        or read_streamlit_secret("FL3XX_API_TOKEN_SCHEME")
        or os.getenv("FL3XX_API_TOKEN_SCHEME")
    )
    if token_scheme_value is None:
        if auth_header_name.lower() == "authorization":
            api_token_scheme = "Bearer"
        else:
            api_token_scheme = ""
    else:
        api_token_scheme = str(token_scheme_value)

    headers = coerce_mapping(merged.get("headers"))
    extra_headers = {str(k): str(v) for k, v in headers.items()} if headers else {}

    params = coerce_mapping(merged.get("params"))
    extra_params = {str(k): str(v) for k, v in params.items()} if params else {}

    verify_ssl_value = merged.get("verify_ssl", True)
    if isinstance(verify_ssl_value, str):
        verify_ssl = verify_ssl_value.strip().lower() not in {"0", "false", "no"}
    else:
        verify_ssl = bool(verify_ssl_value)

    timeout_value = merged.get("timeout", 30)
    try:
        timeout = int(timeout_value)
    except (TypeError, ValueError):
        timeout = 30

    return Fl3xxApiConfig(
        base_url=base_url,
        api_token=api_token,
        auth_header=auth_header,
        auth_header_name=auth_header_name,
        api_token_scheme=api_token_scheme,
        extra_headers=extra_headers,
        verify_ssl=verify_ssl,
        timeout=timeout,
        extra_params=extra_params,
    )


def _has_fl3xx_credentials_configured() -> bool:
    """Return True when either an auth header or API token is available."""

    try:
        config = _build_fl3xx_config_from_secrets()
    except Exception:
        return False

    return bool(config.auth_header or config.api_token)


def _ingest_fl3xx_actuals(flights: list[dict[str, Any]]) -> None:
    """Normalize FL3XX real block times and persist them as status events."""

    def _first_value(payload: dict[str, Any], keys: tuple[str, ...]) -> str | None:
        for key in keys:
            if key not in payload:
                continue
            value = payload.get(key)
            if value is None:
                continue
            if isinstance(value, str):
                stripped = value.strip()
                if stripped:
                    return stripped
            elif isinstance(value, datetime):
                iso_val = _to_iso8601_z(value)
                if iso_val:
                    return iso_val
            else:
                text = str(value).strip()
                if text:
                    return text
        return None

    for flight in flights:
        if not isinstance(flight, dict):
            continue

        off_raw = _first_value(
            flight,
            (
                "_OffBlock_UTC",
                "realDateOUT",
                "realDateOut",
                "realDateOutUTC",
                "blockOffActualUTC",
            ),
        )
        on_raw = _first_value(
            flight,
            (
                "_OnBlock_UTC",
                "realDateIN",
                "realDateIn",
                "realDateInUTC",
                "blockOnActualUTC",
            ),
        )

        off_dt = parse_iso_to_utc(off_raw) if off_raw else None
        on_dt = parse_iso_to_utc(on_raw) if on_raw else None

        flight["_OffBlock_UTC"] = _to_iso8601_z(off_dt) if off_dt else None
        flight["_OnBlock_UTC"] = _to_iso8601_z(on_dt) if on_dt else None
        flight["_FlightStatus"] = (flight.get("flightStatus") or "").strip()

        booking_key = (
            flight.get("bookingIdentifier")
            or flight.get("bookingReference")
            or flight.get("quoteId")
        )
        if not booking_key:
            continue

        booking_str = str(booking_key)
        if off_dt:
            upsert_status(booking_str, "OffBlock", "Off Block", _to_iso8601_z(off_dt), None)
        if on_dt:
            upsert_status(booking_str, "OnBlock", "On Block", _to_iso8601_z(on_dt), None)


def _get_fl3xx_schedule(
    config: Fl3xxApiConfig | None = None,
    now: datetime | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if config is None:
        config = _build_fl3xx_config_from_secrets()
    if not config.auth_header and not config.api_token:
        raise RuntimeError(
            "FL3XX API credentials are not configured. Set 'api_token' or 'auth_header' "
            "in Streamlit secrets (fl3xx_api) or the FL3XX_API_TOKEN environment variable. "
            "Use 'auth_header_name' to override the header name when providing an auth token."
        )

    current_time = now or datetime.now(timezone.utc)
    cache_entry = load_fl3xx_cache()
    last_fetch = _parse_iso8601(cache_entry.get("fetched_at")) if cache_entry else None
    refresh_due = cache_entry is None or last_fetch is None or (
        current_time - last_fetch >= timedelta(minutes=FL3XX_REFRESH_MINUTES)
    )

    if refresh_due:
        flights, metadata = fetch_flights(config, now=current_time)
        crew_summary = enrich_flights_with_crew(config, flights, force=True)
        digest = compute_flights_digest(flights)
        _ingest_fl3xx_actuals(flights)
        changed = cache_entry is None or cache_entry.get("hash") != digest
        save_fl3xx_cache(
            flights,
            digest=digest,
            from_date=metadata["from_date"],
            to_date=metadata["to_date"],
            fetched_at=metadata["fetched_at"],
        )
        fetched_dt = _parse_iso8601(metadata.get("fetched_at"))
        next_refresh = None if fetched_dt is None else fetched_dt + timedelta(minutes=FL3XX_REFRESH_MINUTES)
        metadata.update(
            {
                "hash": digest,
                "changed": changed,
                "used_cache": False,
                "refresh_interval_minutes": FL3XX_REFRESH_MINUTES,
                "next_refresh_after": _to_iso8601_z(next_refresh),
                "crew_fetch_count": crew_summary["fetched"],
                "crew_fetch_errors": crew_summary["errors"],
                "crew_updated": crew_summary["updated"],
            }
        )
        return flights, metadata

    if not cache_entry:
        # Should not happen because refresh_due would be True, but keep defensive fallback.
        flights, metadata = fetch_flights(config, now=current_time)
        crew_summary = enrich_flights_with_crew(config, flights, force=True)
        digest = compute_flights_digest(flights)
        _ingest_fl3xx_actuals(flights)
        save_fl3xx_cache(
            flights,
            digest=digest,
            from_date=metadata["from_date"],
            to_date=metadata["to_date"],
            fetched_at=metadata["fetched_at"],
        )
        metadata.update(
            {
                "hash": digest,
                "changed": True,
                "used_cache": False,
                "refresh_interval_minutes": FL3XX_REFRESH_MINUTES,
                "next_refresh_after": _to_iso8601_z(
                    _parse_iso8601(metadata.get("fetched_at")) + timedelta(minutes=FL3XX_REFRESH_MINUTES)
                    if metadata.get("fetched_at")
                    else None
                ),
                "crew_fetch_count": crew_summary["fetched"],
                "crew_fetch_errors": crew_summary["errors"],
                "crew_updated": crew_summary["updated"],
            }
        )
        return flights, metadata

    flights = cache_entry.get("flights", [])
    _ingest_fl3xx_actuals(flights)
    crew_summary = enrich_flights_with_crew(config, flights, force=False)
    digest = compute_flights_digest(flights) if flights else cache_entry.get("hash") or ""

    if crew_summary["updated"] and flights:
        fetched_at = cache_entry.get("fetched_at") or _to_iso8601_z(last_fetch) or _to_iso8601_z(current_time)
        save_fl3xx_cache(
            flights,
            digest=digest,
            from_date=str(cache_entry.get("from_date") or ""),
            to_date=str(cache_entry.get("to_date") or ""),
            fetched_at=fetched_at or _to_iso8601_z(current_time),
        )

    next_refresh = None
    if last_fetch is not None:
        next_refresh = last_fetch + timedelta(minutes=FL3XX_REFRESH_MINUTES)

    cached_params = {
        "from": cache_entry.get("from_date"),
        "to": cache_entry.get("to_date"),
        "timeZone": "UTC",
        "value": "ALL",
    }
    cached_params.update(config.extra_params)

    metadata = {
        "from_date": cache_entry.get("from_date"),
        "to_date": cache_entry.get("to_date"),
        "time_zone": "UTC",
        "value": "ALL",
        "fetched_at": cache_entry.get("fetched_at"),
        "hash": digest,
        "changed": False,
        "used_cache": True,
        "refresh_interval_minutes": FL3XX_REFRESH_MINUTES,
        "next_refresh_after": _to_iso8601_z(next_refresh),
        "request_url": config.base_url,
        "request_params": cached_params,
        "crew_fetch_count": crew_summary["fetched"],
        "crew_fetch_errors": crew_summary["errors"],
        "crew_updated": crew_summary["updated"],
    }

    return flights, metadata


TZINFOS = {
    "UTC":  tzoffset("UTC", 0),
    "GMT":  tzoffset("GMT", 0),

    "AST":  tzoffset("AST",  -4*3600),
    "ADT":  tzoffset("ADT",  -3*3600),
    "EST":  tzoffset("EST",  -5*3600),
    "EDT":  tzoffset("EDT",  -4*3600),
    "CST":  tzoffset("CST",  -6*3600),
    "CDT":  tzoffset("CDT",  -5*3600),
    "MST":  tzoffset("MST",  -7*3600),
    "MDT":  tzoffset("MDT",  -6*3600),
    "PST":  tzoffset("PST",  -8*3600),
    "PDT":  tzoffset("PDT",  -7*3600),

    "AKST": tzoffset("AKST", -9*3600),
    "AKDT": tzoffset("AKDT", -8*3600),
    "HST":  tzoffset("HST", -10*3600),

    "NST":  tzoffset("NST",  -(3*3600 + 1800)),
    "NDT":  tzoffset("NDT",  -(2*3600 + 1800)),
}

FAKE_TAIL_PATTERNS = [
    re.compile(r"^\s*(add|remove)\b", re.I),
    re.compile(r"\b(ocs|emb)\b", re.I),
]

TURNAROUND_MIN_GAP_MINUTES = 45  # warn when ground time between legs drops below 45 minutes
NO_ACTIVITY_GAP_THRESHOLD = pd.Timedelta(hours=3)


def _max_valid_timestamp(*values):
    valid = [pd.Timestamp(v) for v in values if pd.notna(v)]
    if not valid:
        return pd.NaT
    return max(valid)


def _min_valid_timestamp(*values):
    valid = [pd.Timestamp(v) for v in values if pd.notna(v)]
    if not valid:
        return pd.NaT
    return min(valid)


def _format_gap_duration(td: pd.Timedelta) -> str:
    if td is None or pd.isna(td):
        return ""
    total_minutes = int(td.total_seconds() // 60)
    hours, minutes = divmod(total_minutes, 60)
    if hours and minutes:
        return f"{hours}h {minutes:02d}m"
    if hours:
        return f"{hours}h"
    return f"{minutes}m"


def _build_gap_notice_row(template: pd.DataFrame, gap_start, gap_end, gap_td):
    base = {}
    for col in template.columns:
        if pd.api.types.is_datetime64_any_dtype(template[col].dtype):
            base[col] = pd.NaT
        else:
            base[col] = None

    for col in ["ETD_UTC", "ETA_UTC", "_DepActual_ts", "_ETA_FA_ts", "_ArrActual_ts", "_EDCT_ts"]:
        if col in base:
            base[col] = pd.NaT

    if "_DelayPriority" in base:
        base["_DelayPriority"] = 0
    if "_TurnMinutes" in base:
        base["_TurnMinutes"] = pd.NA
    if "_LegKey" in base:
        base["_LegKey"] = ""
    if "is_real_leg" in base:
        base["is_real_leg"] = False

    gap_window = (
        f"{gap_start.strftime('%d.%m %H:%MZ')} → {gap_end.strftime('%d.%m %H:%MZ')}"
        if pd.notna(gap_start) and pd.notna(gap_end)
        else ""
    )
    duration_txt = _format_gap_duration(gap_td)
    message = "No flight activity planned"
    if gap_window:
        message = f"{message} · {gap_window}"
    if duration_txt:
        message = f"{message} ({duration_txt})"

    text_fill_cols = [
        "Booking", "Aircraft", "Aircraft Type", "Departs In", "Arrives In",
        "Turn Time", "PIC", "Status", "Type",
    ]
    for col in text_fill_cols:
        if col in base:
            base[col] = "—" if col != "Status" else "No flight activity planned"

    if "Status" in base:
        base["Status"] = "No flight activity planned"
    if "Type" in base:
        base["Type"] = "Notice"
    if "SIC" in base:
        base["SIC"] = "—"
    if "Workflow" in base:
        base["Workflow"] = "—"
    if "Account" in base:
        base["Account"] = "—"

    base["Route"] = f"— {message} —"
    base["TypeBadge"] = "⏸️"
    base["_GapRow"] = True

    return pd.DataFrame([base], columns=template.columns)


def insert_gap_notice_rows(frame: pd.DataFrame, threshold: pd.Timedelta = NO_ACTIVITY_GAP_THRESHOLD) -> pd.DataFrame:
    if frame.empty or len(frame) < 2:
        frame = frame.copy()
        if "_GapRow" not in frame.columns:
            frame["_GapRow"] = False
        return frame

    frame = frame.copy()
    if "_GapRow" not in frame.columns:
        frame["_GapRow"] = False

    pieces = []
    idxs = list(frame.index)
    for pos, idx in enumerate(idxs):
        pieces.append(frame.loc[[idx]])
        if pos == len(idxs) - 1:
            continue

        next_idx = idxs[pos + 1]
        cur_end = _max_valid_timestamp(frame.at[idx, "ETD_UTC"], frame.at[idx, "ETA_UTC"])
        next_start = _min_valid_timestamp(frame.at[next_idx, "ETD_UTC"], frame.at[next_idx, "ETA_UTC"])

        if pd.isna(cur_end) or pd.isna(next_start):
            continue

        gap_td = next_start - cur_end
        if pd.isna(gap_td) or gap_td < threshold:
            continue

        pieces.append(_build_gap_notice_row(frame, cur_end, next_start, gap_td))

    combined = pd.concat(pieces, ignore_index=True)
    combined["_GapRow"] = combined["_GapRow"].fillna(False)
    return combined

def is_real_tail(tail: str) -> bool:
    if not isinstance(tail, str) or not tail.strip():
        return False
    for pat in FAKE_TAIL_PATTERNS:
        if pat.search(tail):
            return False
    return True

def parse_utc_ddmmyyyy_hhmmz(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.replace("Z", "z", regex=False).str.replace("z", "", regex=False)
    return pd.to_datetime(s, format="%d.%m.%Y %H:%M", errors="coerce", utc=True)

def fmt_td(td):
    if td is None or pd.isna(td):
        return "—"
    if isinstance(td, pd.Timedelta):
        td = td.to_pytimedelta()
    sign = "-" if td.total_seconds() < 0 else ""
    td_abs = abs(td)
    hours, remainder = divmod(int(td_abs.total_seconds()), 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"

def flight_time_hhmm_from_decimal(hours_decimal) -> str:
    try:
        mins = int(round(float(hours_decimal) * 60))
        return f"{mins // 60:02d}:{mins % 60:02d}"
    except Exception:
        return "—"

def classify_account(account_val: str) -> str:
    if isinstance(account_val, str) and "airsprint inc" in account_val.lower():
        return "OCS"
    return "Owner"

def type_badge(flight_type: str) -> str:
    return {"OCS": "🟢 OCS", "Owner": "🔵 Owner"}.get(flight_type, "⚪︎")

def format_account_value(account_val) -> str:
    """Return a display-friendly account string."""
    if account_val is None:
        return "—"
    try:
        if pd.isna(account_val):
            return "—"
    except Exception:
        pass
    account_str = str(account_val).strip()
    if not account_str:
        return "—"
    if account_str.lower() == "nan":
        return "—"
    return account_str

def fmt_dt_utc(dt: datetime | None) -> str:
    if not dt or (isinstance(dt, float) and pd.isna(dt)):
        return "—"
    return dt.astimezone(timezone.utc).strftime("%d.%m.%Y %H:%M")

def parse_iso_to_utc(dt_str: str | None) -> datetime | None:
    if not dt_str:
        return None
    try:
        dt = dateparse.parse(dt_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

import re, requests, streamlit as st


def _normalize_delay_reason(delay_reason: str | None) -> str:
    reason = (delay_reason or "").strip()
    return reason if reason else "Unknown"


def _format_notes_line(notes: str | None) -> str | None:
    note = (notes or "").strip()
    return f"Notes: {note}" if note else None


def _build_delay_msg(
    tail: str,
    booking: str,
    minutes_delta: int,
    new_eta_hhmm: str,
    account: str | None = None,
    delay_reason: str | None = None,
    notes: str | None = None,
) -> str:
    # Tail like "C-FASW" or "CFASW" → "CFASW"
    tail_disp = (tail or "").replace("-", "").upper()
    label = "LATE" if int(minutes_delta) >= 0 else "EARLY"
    mins = abs(int(minutes_delta))

    account_disp = format_account_value(account)

    # If caller already passed "HHMM LT"/"HHMM UTC", keep it; else normalize HHMM → "HHMM LT"
    s = (new_eta_hhmm or "").strip()
    if not s:
        eta_disp = ""
    else:
        if re.search(r"\b(LT|UTC)\b$", s):
            eta_disp = s
        else:
            # Accept "2032" or "20:32" and tag as LT by default
            digits = re.sub(r"[^0-9]", "", s)
            if len(digits) in (3, 4):
                digits = digits.zfill(4)
                eta_disp = f"{digits} LT"
            else:
                eta_disp = f"{s} LT"

    lines = [
        f"TAIL#/BOOKING#: {tail_disp}//{booking}",
        f"Account: {account_disp}",
        f"{label}: {mins} minutes",
        f"UPDATED ETA: {eta_disp}",
        f"Delay Reason: {_normalize_delay_reason(delay_reason)}",
    ]
    notes_line = _format_notes_line(notes)
    if notes_line:
        lines.append(notes_line)
    return "\n".join(lines)

def post_to_telus_team(team: str, text: str) -> tuple[bool, str]:
    telus_hooks = _read_streamlit_secret("TELUS_WEBHOOKS")
    if isinstance(telus_hooks, Mapping):
        url = _mapping_get(telus_hooks, team)
    else:
        url = None
    if not url:
        return False, f"No webhook configured for team '{team}'."
    try:
        r = requests.post(url, json={"text": text}, timeout=10)
        ok = 200 <= r.status_code < 300
        return ok, ("" if ok else f"{r.status_code}: {r.text[:200]}")
    except Exception as e:
        return False, str(e)

def notify_delay_chat(
    team: str,
    tail: str,
    booking: str,
    minutes_delta: int,
    new_eta_hhmm: str,
    account: str | None = None,
    delay_reason: str | None = None,
    notes: str | None = None,
):
    msg = _build_delay_msg(
        tail,
        booking,
        minutes_delta,
        new_eta_hhmm,
        account=account,
        delay_reason=delay_reason,
        notes=notes,
    )
    ok, err = post_to_telus_team(team, msg)
    if ok:
        st.success("Posted to TELUS BC team.")
    else:
        st.error(f"Post failed: {err}")

# Drop-in replacement: single time box (no writes back to the widget key)
def utc_datetime_picker(label: str, key: str, initial_dt_utc: datetime | None = None) -> datetime:
    """Stateful UTC datetime picker with ONE time box that accepts 1005, 10:05, 4pm, etc."""
    if initial_dt_utc is None:
        initial_dt_utc = datetime.now(timezone.utc)

    date_key = f"{key}__date"
    time_txt_key = f"{key}__time_txt"   # bound to st.text_input
    time_obj_key = f"{key}__time_obj"   # internal parsed time we control

    # Seed once
    if date_key not in st.session_state:
        st.session_state[date_key] = initial_dt_utc.date()
    if time_obj_key not in st.session_state:
        st.session_state[time_obj_key] = initial_dt_utc.time().replace(microsecond=0)
    if time_txt_key not in st.session_state:
        # only seed the textbox once; after that, don't programmatically change it
        st.session_state[time_txt_key] = st.session_state[time_obj_key].strftime("%H:%M")

    d = st.date_input(f"{label} — Date (UTC)", key=date_key)
    txt = st.text_input(
        f"{label} — Time (UTC)",
        key=time_txt_key,
        placeholder="e.g., 1005 or 10:05 or 4pm",
    )

    def _parse_loose_time(s: str):
        s = (s or "").strip().lower().replace(" ", "")
        if not s:
            return None
        # am/pm suffix
        ampm = None
        if s.endswith("am") or s.endswith("pm"):
            ampm = s[-2:]
            s = s[:-2]

        hh = mm = None
        if ":" in s:
            try:
                hh_str, mm_str = s.split(":", 1)
                hh, mm = int(hh_str), int(mm_str)
            except Exception:
                return None
        elif s.isdigit():
            if len(s) in (3, 4):          # HHMM (accept 3 or 4 digits)
                s = s.zfill(4)
                hh, mm = int(s[:2]), int(s[2:])
            elif len(s) in (1, 2):        # HH
                hh, mm = int(s), 0
            else:
                return None
        else:
            return None

        if ampm:
            if hh == 12:
                hh = 0
            if ampm == "pm":
                hh += 12
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None

        return datetime.strptime(f"{hh:02d}:{mm:02d}", "%H:%M").time()

    parsed = _parse_loose_time(txt)
    if parsed is not None:
        # update only the internal time; DO NOT write back to time_txt_key
        st.session_state[time_obj_key] = parsed

    return datetime.combine(st.session_state[date_key], st.session_state[time_obj_key]).replace(tzinfo=timezone.utc)

def _airport_timezone(icao: str | None):
    icao = (icao or "").strip().upper()
    tzname = ICAO_TZ_MAP.get(icao)
    if tzname:
        try:
            return pytz.timezone(tzname)
        except Exception:
            pass
    return LOCAL_TZ

def _local_departure_date_parts(ts: pd.Timestamp | datetime | None, icao: str | None) -> tuple[date | None, str, int]:
    """Return (local_date, label, day_delta) for a departure timestamp at the airport's local time."""
    if ts is None or pd.isna(ts):
        return None, "Date pending", 999

    tz = _airport_timezone(icao)
    try:
        ts_local = pd.Timestamp(ts)
        if ts_local.tzinfo is None:
            ts_local = ts_local.tz_localize(timezone.utc)
        ts_local = ts_local.tz_convert(tz)
    except Exception:
        return None, "Date pending", 999

    local_date = ts_local.date()
    today_local = datetime.now(tz).date()
    day_delta = (local_date - today_local).days
    base_label = ts_local.strftime("%a %b %d")

    if day_delta == 0:
        label = f"Today · {base_label}"
    elif day_delta == 1:
        label = f"Tomorrow · {base_label}"
    else:
        label = base_label

    return local_date, label, day_delta

def local_hhmm(ts: pd.Timestamp | datetime | None, icao: str) -> str:
    """Return 'HHMM LT' at the airport's local time (fallback 'HHMM UTC')."""
    if ts is None or pd.isna(ts):
        return ""
    icao = (icao or "").upper()
    try:
        if icao in ICAO_TZ_MAP:
            tz = pytz.timezone(ICAO_TZ_MAP[icao])
            return pd.Timestamp(ts).tz_convert(tz).strftime("%H%M LT")
        return pd.Timestamp(ts).strftime("%H%M UTC")
    except Exception:
        return pd.Timestamp(ts).strftime("%H%M UTC")

def _select_arrival_baseline(row: pd.Series) -> tuple[pd.Timestamp | None, str]:
    """Return the best arrival timestamp (Actual → ETA_FA → Scheduled) and its label."""
    actual = row.get("_ArrActual_ts")
    if actual is not None and pd.notna(actual):
        return pd.Timestamp(actual), "Actual"

    eta_fa = row.get("_ETA_FA_ts")
    if eta_fa is not None and pd.notna(eta_fa):
        return pd.Timestamp(eta_fa), "ETA (FA)"

    sched = row.get("ETA_UTC")
    if sched is not None and pd.notna(sched):
        return pd.Timestamp(sched), "Sched ETA"

    return None, ""

def compute_turnaround_windows(df: pd.DataFrame) -> pd.DataFrame:
    """Build turnaround windows (arrival → next departure) for each aircraft."""
    if df is None or df.empty:
        return pd.DataFrame()

    if "Aircraft" not in df.columns or "ETD_UTC" not in df.columns:
        return pd.DataFrame()

    work = df[df["ETD_UTC"].notna()].copy()
    if work.empty:
        return pd.DataFrame()

    work = work.sort_values(["Aircraft", "ETD_UTC"])
    rows = []

    for tail, group in work.groupby("Aircraft"):
        group = group.sort_values("ETD_UTC").reset_index(drop=True)
        if len(group) < 2:
            continue

        for idx in range(len(group) - 1):
            current = group.iloc[idx]
            next_leg = group.iloc[idx + 1]

            next_dep_actual = next_leg.get("_DepActual_ts")
            if next_dep_actual is not None and pd.notna(next_dep_actual):
                continue  # next leg already departed; no upcoming window to monitor

            arr_ts, arrival_source = _select_arrival_baseline(current)
            next_etd = next_leg.get("ETD_UTC")

            if arr_ts is None or pd.isna(arr_ts) or next_etd is None or pd.isna(next_etd):
                continue

            next_etd = pd.Timestamp(next_etd)
            gap = next_etd - pd.Timestamp(arr_ts)

            rows.append({
                "Aircraft": tail,
                "CurrentBooking": current.get("Booking", ""),
                "CurrentRoute": current.get("Route", ""),
                "ArrivalSource": arrival_source,
                "ArrivalUTC": pd.Timestamp(arr_ts),
                "NextBooking": next_leg.get("Booking", ""),
                "NextRoute": next_leg.get("Route", ""),
                "NextFromICAO": next_leg.get("From_ICAO", ""),
                "NextFromIATA": next_leg.get("From_IATA", ""),
                "NextETDUTC": next_etd,
                "TurnDelta": gap,
                "TurnMinutes": int(round(gap.total_seconds() / 60.0)),
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values(["NextETDUTC", "Aircraft", "CurrentBooking"]).reset_index(drop=True)
    return result


def _format_turn_timestamp(ts) -> str:
    if ts is None or pd.isna(ts):
        return "—"
    try:
        return pd.Timestamp(ts).strftime("%H:%MZ")
    except Exception:
        return "—"


def build_downline_risk_map(
    turnaround_df: pd.DataFrame, threshold_min: int
) -> tuple[dict[str, dict[str, object]], list[dict[str, object]]]:
    """Return booking → risk info and a summary grouped by local departure date."""

    if turnaround_df is None or turnaround_df.empty:
        return {}, []

    risky = turnaround_df[
        turnaround_df["TurnMinutes"].notna()
        & (turnaround_df["TurnMinutes"] < threshold_min)
    ].copy()

    if risky.empty:
        return {}, []

    risk_map: dict[str, dict[str, object]] = {}
    all_legs: list[dict[str, object]] = []
    grouped_by_date: dict[date | None, dict[str, list[dict[str, object]]]] = {}
    date_meta: dict[date | None, dict[str, object]] = {}

    for tail, group in risky.groupby("Aircraft"):
        group = group.sort_values("NextETDUTC")

        for _, row in group.iterrows():
            next_booking = str(row.get("NextBooking") or "").strip()
            if not next_booking:
                continue

            next_etd = row.get("NextETDUTC")
            next_from_icao = str(row.get("NextFromICAO") or "").strip().upper()
            local_date, date_label, day_delta = _local_departure_date_parts(next_etd, next_from_icao)

            info = {
                "aircraft": tail,
                "source_booking": str(row.get("CurrentBooking") or "").strip(),
                "next_booking": next_booking,
                "turn_minutes": row.get("TurnMinutes"),
                "arrival_label": _format_turn_timestamp(row.get("ArrivalUTC")),
                "arrival_source": str(row.get("ArrivalSource") or "").strip(),
                "next_etd_label": _format_turn_timestamp(next_etd),
                "next_route": str(row.get("NextRoute") or "").strip(),
                "next_etd_utc": next_etd,
                "next_from_icao": next_from_icao,
                "next_local_date": local_date,
                "next_local_date_label": date_label,
                "next_local_day_delta": day_delta,
            }

            risk_map[next_booking] = info
            all_legs.append(info)

            tails_for_date = grouped_by_date.setdefault(local_date, {})
            tails_for_date.setdefault(tail, []).append(info)

            meta = date_meta.setdefault(
                local_date,
                {"date": local_date, "label": date_label, "day_delta": day_delta},
            )
            meta["day_delta"] = min(meta.get("day_delta", day_delta), day_delta)
            if not meta.get("label"):
                meta["label"] = date_label

    if not all_legs:
        return {}, []

    summary: list[dict[str, object]] = []
    for date_key, tails_map in grouped_by_date.items():
        tail_entries: list[dict[str, object]] = []
        for tail, legs in tails_map.items():
            legs_sorted = sorted(
                legs,
                key=lambda leg: leg.get("next_etd_utc") if pd.notna(leg.get("next_etd_utc")) else pd.Timestamp.max,
            )
            tail_entries.append({"aircraft": tail, "legs": legs_sorted})

        tail_entries.sort(key=lambda t: (t.get("aircraft") or "").upper())
        meta = date_meta.get(date_key, {})
        summary.append(
            {
                "date": date_key,
                "label": meta.get("label") or "Upcoming departures",
                "day_delta": meta.get("day_delta", 999),
                "tails": tail_entries,
                "collapsed": meta.get("day_delta", 999) > 0,
            }
        )

    summary.sort(
        key=lambda entry: (
            entry.get("day_delta", 999) != 0,
            entry.get("day_delta", 999),
            entry.get("date") or datetime.max.date(),
        )
    )

    return risk_map, summary

def _late_early_label(delta_min: int) -> tuple[str, int]:
    # positive => late; negative => early
    label = "LATE" if delta_min >= 0 else "EARLY"
    return label, abs(int(delta_min))

def build_stateful_notify_message(
    row: pd.Series,
    delay_reason: str | None = None,
    notes: str | None = None,
) -> str:
    """
    Build a Telus BC message whose contents depend on flight state.
    Priority for reason if multiple cells are red:
      1) Landing (FA)   2) ETA(FA)   3) Takeoff (FA)
    """
    tail = row.get("Aircraft", "")
    booking = row.get("Booking", "")
    dep_ts   = row.get("_DepActual_ts")
    arr_ts   = row.get("_ArrActual_ts")
    eta_fa   = row.get("_ETA_FA_ts")
    etd_est  = row.get("ETD_UTC")
    eta_est  = row.get("ETA_UTC")
    from_icao= row.get("From_ICAO", "")
    to_icao  = row.get("To_ICAO", "")
    account_val = row.get("Account")
    account_line = f"Account: {format_account_value(account_val)}"

    # Compute cell variances (same definitions as styling)
    dep_var = (dep_ts - etd_est) if (pd.notna(dep_ts) and pd.notna(etd_est)) else None
    eta_var = (eta_fa - eta_est) if (pd.notna(eta_fa) and pd.notna(eta_est)) else None
    arr_var = (arr_ts - eta_est) if (pd.notna(arr_ts) and pd.notna(eta_est)) else None

    # Derive state & choose priority reason
    notes_line = _format_notes_line(notes)

    if pd.notna(arr_var):  # ARRIVED late/early
        delta_min = int(round(arr_var.total_seconds()/60.0))
        label, mins = _late_early_label(delta_min)
        arr_local = local_hhmm(arr_ts, to_icao)
        lines = [
            f"TAIL#/BOOKING#: {(tail or '').replace('-', '').upper()}//{booking}",
            account_line,
            f"{label}: {mins} minutes",
            f"ARRIVAL: {arr_local}",
            f"Delay Reason: {_normalize_delay_reason(delay_reason)}",
        ]
        if notes_line:
            lines.append(notes_line)
        return "\n".join(lines)

    if pd.notna(eta_var):  # ENROUTE with FA ETA variance
        delta_min = int(round(eta_var.total_seconds()/60.0))
        label, mins = _late_early_label(delta_min)
        eta_local = local_hhmm(eta_fa, to_icao)
        lines = [
            f"TAIL#/BOOKING#: {(tail or '').replace('-', '').upper()}//{booking}",
            account_line,
            f"{label}: {mins} minutes",
            f"UPDATED ETA: {eta_local}",
            f"Delay Reason: {_normalize_delay_reason(delay_reason)}",
        ]
        if notes_line:
            lines.append(notes_line)
        return "\n".join(lines)

    if pd.notna(dep_var):  # TAKEOFF variance (usually already enroute)
        delta_min = int(round(dep_var.total_seconds()/60.0))
        label, mins = _late_early_label(delta_min)
        dep_local = local_hhmm(dep_ts, from_icao)
        lines = [
            f"TAIL#/BOOKING#: {(tail or '').replace('-', '').upper()}//{booking}",
            account_line,
            f"{label}: {mins} minutes",
            f"TAKEOFF (FA): {dep_local}",
            f"Delay Reason: {_normalize_delay_reason(delay_reason)}",
        ]
        if notes_line:
            lines.append(notes_line)
        return "\n".join(lines)

    # Fallback: keep current generic builder (should rarely hit with our panel filters)
    return _build_delay_msg(
        tail=tail,
        booking=booking,
        minutes_delta=int(_default_minutes_delta(row)),
        new_eta_hhmm=get_local_eta_str(row),  # ETA LT or UTC
        account=account_val,
        delay_reason=delay_reason,
        notes=notes,
    )


# ---------- Subject-aware parsing ----------
SUBJ_TAIL_RE = re.compile(r"\bC-[A-Z0-9]{4}\b")
SUBJ_CALLSIGN_RE = re.compile(r"\bASP\d{3,4}\b")

SUBJ_PATTERNS = {
    "Arrival": re.compile(
        r"\barrived\b.*\bat\s+(?P<at>[A-Z]{3,4})\b(?:.*\bfrom\s+(?P<from>[A-Z]{3,4})\b)?",
        re.I,
    ),
    "ArrivalForecast": re.compile(
        r"\b(?:expected to arrive|arriving soon)\b.*\bat\s+(?P<at>[A-Z]{3,4})\b.*\bin\s+(?P<mins>\d+)\s*(?:min|mins|minutes)\b",
        re.I,
    ),
    "Departure": re.compile(
        r"\b(?:has\s+)?departed\b.*?(?:from\s+)?(?P<from>[A-Z]{3,4})\b(?:.*?\b(?:for|to)\s+(?P<to>[A-Z]{3,4})\b)?",
        re.I,
    ),
    "Diversion": re.compile(
        r"\bdiverted\s+to\b.*?(?:\(|\b)(?P<to>[A-Z]{3,4})\b",
        re.I,
    ),
}

SUBJ_DIVERSION_FROM_PAREN_RE = re.compile(
    r"\bfrom\b[^()]*\(\s*(?P<code>[A-Z]{3,4})\s*\)",
    re.I,
)
SUBJ_DIVERSION_FROM_TOKEN_RE = re.compile(
    r"\bfrom\s+(?P<code>[A-Z]{3,4})\b",
    re.I,
)

def parse_subject_line(subject: str, now_utc: datetime):
    if not subject:
        return {"event_type": None}
    tail_m = SUBJ_TAIL_RE.search(subject)
    tail = tail_m.group(0) if tail_m else None
    callsign_m = SUBJ_CALLSIGN_RE.search(subject)
    callsign = callsign_m.group(0) if callsign_m else None

    result = {"event_type": None, "tail": tail, "callsign": callsign,
              "at_airport": None, "from_airport": None, "to_airport": None,
              "minutes_until": None, "actual_time_utc": None}

    m = SUBJ_PATTERNS["Arrival"].search(subject)
    if m:
        result["event_type"] = "Arrival"
        result["at_airport"] = m.group("at")
        result["from_airport"] = m.groupdict().get("from")
        return result

    m = SUBJ_PATTERNS["ArrivalForecast"].search(subject)
    if m:
        result["event_type"] = "ArrivalForecast"
        result["at_airport"] = m.group("at")
        result["minutes_until"] = int(m.group("mins"))
        result["actual_time_utc"] = now_utc + timedelta(minutes=result["minutes_until"])
        return result

    m = SUBJ_PATTERNS["Departure"].search(subject)
    if m:
        result["event_type"] = "Departure"
        result["from_airport"] = m.group("from")
        result["to_airport"] = m.groupdict().get("to")
        return result

    m = SUBJ_PATTERNS["Diversion"].search(subject)
    if m:
        result["event_type"] = "Diversion"
        to_token = m.groupdict().get("to")
        if to_token:
            result["to_airport"] = to_token.strip().upper()

        from_token = m.groupdict().get("from")
        if not from_token:
            m_from = SUBJ_DIVERSION_FROM_PAREN_RE.search(subject)
            if not m_from:
                m_from = SUBJ_DIVERSION_FROM_TOKEN_RE.search(subject)
            if m_from:
                from_token = m_from.group("code")
        if from_token:
            result["from_airport"] = from_token.strip().upper()
        return result

    return result

# ---- Parse explicit datetime anywhere in text ----
def parse_any_datetime_to_utc(text: str) -> datetime | None:
    m_iso = re.search(r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(Z|[+\-]\d{2}:?\d{2})?", text)
    if m_iso:
        try:
            dt = dateparse.parse(m_iso.group(0), tzinfos=TZINFOS)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass
    m2_date = re.search(r"\b(\d{1,2}[./-]\d{1,2}[./-]\d{2,4}|[A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})\b", text)
    m2_time = re.search(r"\b(\d{1,2}:\d{2}(:\d{2})?)\s*(Z|UTC|[+\-]\d{2}:?\d{2}|[A-Z]{2,4})?\b", text, re.I)
    try_strings = []
    if m2_date and m2_time:
        try_strings.append(m2_date.group(0) + " " + m2_time.group(0))
    elif m2_time:
        assumed = datetime.now(timezone.utc).strftime("%Y-%m-%d ") + m2_time.group(0)
        try_strings.append(assumed)

    for s in try_strings:
        try:
            dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except Exception:
            continue
    return None

def parse_any_dt_string_to_utc(s: str) -> datetime | None:
    if not s:
        return None
    try:
        dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

# Email Date header → UTC
def get_email_date_utc(msg) -> datetime | None:
    try:
        d = msg.get('Date')
        if not d:
            return None
        dt = parsedate_to_datetime(d)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def extract_event(text: str):
    if re.search(r"\bEDCT\b|Expected Departure Clearance Time", text, re.I):
        return "EDCT"
    if re.search(r"\bdiverted\b", text, re.I): return "Diversion"
    if re.search(r"\barriv(?:ed|al)\b", text, re.I): return "Arrival"
    if re.search(r"\bdepart(?:ed|ure)\b", text, re.I): return "Departure"
    return None

def extract_candidates(text: str):
    """
    Return (bookings, tails_dashed, event).
    - bookings: 5-char tokens that match real bookings in the CSV (unchanged)
    - tails_dashed: dashed tails from the text + dashed tails from ASP mapping
    - event: coarse type from keywords
    """
    # bookings (unchanged)
    bookings_all = set(re.findall(r"\b([A-Z0-9]{5})\b", text or ""))
    valid_bookings = set(df_clean["Booking"].astype(str).unique().tolist()) if 'df_clean' in globals() else set()
    bookings = sorted([b for b in bookings_all if b in valid_bookings]) if valid_bookings else sorted(bookings_all)

    # dashed tails from literal matches
    literal_dashed = set(re.findall(r"\bC-[A-Z0-9]{4}\b", (text or "").upper()))

    # dashed tails from ASP callsigns via your mapping
    # (tail_from_asp(text) must return values like 'C-FSEF', 'C-FLAS', etc.)
    mapped_dashed = set(tail_from_asp(text))

    tails_dashed = sorted(literal_dashed | mapped_dashed)

    event = extract_event(text or "")
    return bookings, tails_dashed, event



# ---- BODY parsers ----
BODY_DEPARTURE_RE = re.compile(
    r"departed\s+.*?\((?P<from>[A-Z]{3,4})\)\s+at\s+"
    r"(?P<dep_time>\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)"
    r".*?en\s*route\s+to\s+.*?\((?P<to>[A-Z]{3,4})\)"
    r".*?(?:(?:(?:estimated\s+(?:time\s+of\s+)?arrival|ETA)\s*(?:at|of)?\s+"
    r"(?P<eta_time>\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)))?",
    re.I
)
BODY_ARRIVAL_RE = re.compile(
    r"arrived\s+at\s+.*?\((?P<at>[A-Z]{3,4})\)\s+at\s+(?P<arr_time>\d{1,2}:\d{2}\s*[A-Z0-9 ]+)"
    r".*?from\s+.*?\((?P<from>[A-Z]{3,4})\)",
    re.I
)
BODY_DIVERSION_RE = re.compile(
    r"en\s*route\s+from\s+.*?\(\s*(?P<from>[A-Z]{3,4})\s*\)"
    r".*?diverted\s+to\s+.*?\(\s*(?P<divert_to>[A-Z]{3,4})\s*\)",
    re.I | re.S,
)
ETA_ANY_RE = re.compile(
    r"(?:estimated\s+(?:time\s+of\s+)?arrival|ETA)\s*(?:at|of)?\s+"
    r"(\d{1,2}:\d{2}(?:\s*[AP]M)?(?:\s*[A-Z]{2,4})?)",
    re.I
)

# ---- EDCT email parsers ----
EDCT_FROM_TO_RE = re.compile(
    r"your flight from\s+(?P<from>[A-Z]{3,4})\s+to\s+(?P<to>[A-Z]{3,4})",
    re.I
)
EDCT_EDCT_RE = re.compile(r"^\s*EDCT:\s*(?P<edct_line>.+)$", re.I | re.M)
EDCT_EXP_ARR_RE = re.compile(r"^\s*Expected Arrival Time:\s*(?P<eta_line>.+)$", re.I | re.M)
EDCT_ORIG_DEP_RE = re.compile(r"^\s*Original Departure Time:\s*(?P<orig_line>.+)$", re.I | re.M)

def _parse_time_token_to_utc(time_token: str, base_date_utc: datetime) -> datetime | None:
    if not time_token:
        return None
    base_day = (base_date_utc or datetime.now(timezone.utc)).date()
    s = f"{base_day.isoformat()} {time_token}"
    try:
        dt = dateparse.parse(s, fuzzy=True, tzinfos=TZINFOS)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_utc = dt.astimezone(timezone.utc)
        if base_date_utc is not None:
            base_utc = base_date_utc.astimezone(timezone.utc)
            delta = dt_utc - base_utc
            if delta > timedelta(hours=12):
                dt_utc -= timedelta(days=1)
            elif delta < -timedelta(hours=12):
                dt_utc += timedelta(days=1)
        return dt_utc
    except Exception:
        return None

def parse_body_firstline(event: str, body: str, email_date_utc: datetime) -> dict:
    info = {}
    if not body:
        return info
    m = BODY_DEPARTURE_RE.search(body)
    if event == "Departure" and m:
        info["from"] = m.group("from")
        info["to"] = m.group("to")
        info["dep_time_utc"] = _parse_time_token_to_utc(m.group("dep_time"), email_date_utc)
        if m.group("eta_time"):
            info["eta_time_utc"] = _parse_time_token_to_utc(m.group("eta_time"), email_date_utc)
        if not info.get("eta_time_utc"):
            m_eta = ETA_ANY_RE.search(body)
            if m_eta:
                info["eta_time_utc"] = _parse_time_token_to_utc(m_eta.group(1), email_date_utc)
        return info
    m = BODY_ARRIVAL_RE.search(body)
    if event == "Arrival" and m:
        info["at"] = m.group("at")
        info["from"] = m.group("from")
        info["arr_time_utc"] = _parse_time_token_to_utc(m.group("arr_time"), email_date_utc)
        return info
    if event == "Diversion":
        m_div = BODY_DIVERSION_RE.search(body)
        if m_div:
            info["from"] = m_div.group("from")
            info["divert_to"] = m_div.group("divert_to")
    # If the event wasn't detected as "Departure", try to capture an ETA anywhere.
    m_eta = ETA_ANY_RE.search(body)
    if m_eta:
        info["eta_time_utc"] = _parse_time_token_to_utc(m_eta.group(1), email_date_utc)
    return info

def parse_body_edct(body: str) -> dict:
    """Return {'from','to','edct_time_utc','expected_arrival_utc','original_dep_utc'} if present."""
    info = {}
    if not body:
        return info
    mft = EDCT_FROM_TO_RE.search(body)
    if mft:
        info["from"] = mft.group("from").upper()
        info["to"] = mft.group("to").upper()
    m_edct = EDCT_EDCT_RE.search(body)
    if m_edct:
        info["edct_time_utc"] = parse_any_dt_string_to_utc(m_edct.group("edct_line"))
    m_eta = EDCT_EXP_ARR_RE.search(body)
    if m_eta:
        info["expected_arrival_utc"] = parse_any_dt_string_to_utc(m_eta.group("eta_line"))
    m_orig = EDCT_ORIG_DEP_RE.search(body)
    if m_orig:
        info["original_dep_utc"] = parse_any_dt_string_to_utc(m_orig.group("orig_line"))
    return info

# ============================
# CSV IATA/ICAO support
# ============================
def normalize_iata(code: str) -> str:
    c = (code or "").strip().upper()
    return c if len(c) == 3 else ""

def derive_iata_from_icao(icao: str) -> str:
    c = (icao or "").strip().upper()
    if len(c) != 4:
        return ""
    mapped = ICAO_TO_IATA_MAP.get(c)
    if mapped:
        return mapped
    if c[0] in ("C", "K"):
        return c[1:]
    return ""

def display_airport(icao: str, iata: str) -> str:
    """Return the best airport token available for display."""

    i = (icao or "").strip().upper()
    a = (iata or "").strip().upper()

    # Prefer the "ICAO" value when provided, even if it is not a
    # four-character identifier. Some FL3XX API responses supply an IATA
    # identifier (or another local code such as an FAA/TC identifier) in this
    # field when no ICAO exists. Showing that token avoids a blank schedule
    # entry and allows downstream FlightAware matching logic to work with the
    # displayed value.
    if i:
        return i

    if a:
        return a

    return "—"


def _airport_token_variants(code: str) -> set[str]:
    """Return possible comparison tokens (ICAO + derived IATA) for a code."""

    tokens: set[str] = set()
    c = (code or "").strip().upper()
    if not c:
        return tokens
    if len(c) == 4:
        tokens.add(c)
        derived = derive_iata_from_icao(c)
        if derived:
            tokens.add(derived)
    elif len(c) == 3:
        tokens.add(c)
        mapped_icao = IATA_TO_ICAO_MAP.get(c)
        if mapped_icao:
            tokens.add(mapped_icao)
    return tokens


def _parse_route_mismatch_status(status_text: str):
    """Parse stored RouteMismatch status JSON → dict with normalized fields."""

    if not status_text:
        return None
    try:
        data = json.loads(status_text)
        if isinstance(data, dict):
            email_raw = str(data.get("email_to_raw") or "").strip().upper()
            tokens = data.get("email_tokens")
            if isinstance(tokens, list):
                token_set = {
                    str(tok).strip().upper()
                    for tok in tokens
                    if isinstance(tok, str) and str(tok).strip()
                }
            else:
                token_set = set()
            if not token_set and email_raw:
                token_set = _airport_token_variants(email_raw)
            return {
                "email_to_raw": email_raw,
                "email_tokens": sorted(token_set),
            }
    except (json.JSONDecodeError, TypeError, ValueError):
        pass

    fallback = status_text.strip().upper()
    if not fallback:
        return None
    tokens = _airport_token_variants(fallback)
    return {
        "email_to_raw": fallback,
        "email_tokens": sorted(tokens),
    }

# ---- Booking chooser ----
def choose_booking_for_event(
    subj_info: dict,
    tails_dashed: list[str],
    event: str,
    event_dt_utc: datetime | None,
) -> pd.Series | None:
    cand = df_clean.copy()
    total_len = len(cand)
    route_filter_hit = False
    if tails_dashed:
        cand = cand[cand["Aircraft"].isin(tails_dashed)]  # CSV is dashed
        if cand.empty:
            return None

    raw_at   = (subj_info.get("at_airport") or "").strip().upper()
    raw_from = (subj_info.get("from_airport") or "").strip().upper()
    raw_to   = (subj_info.get("to_airport") or "").strip().upper()

    def match_token(cdf, col_iata, col_icao, token):
        nonlocal route_filter_hit
        token_norm = (token or "").strip().upper()
        if not token_norm:
            return cdf

        tok_iata = normalize_iata(token_norm)
        tok_icao = token_norm if len(token_norm) == 4 else ""

        icao_series = (
            cdf[col_icao]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )
        iata_series = (
            cdf[col_iata]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.upper()
        )

        if tok_iata:
            derived_mask = (
                (icao_series.str.len() == 4)
                & icao_series.str[0].isin(["C", "K"])
                & (icao_series.str[1:] == tok_iata)
            )
            if derived_mask.any():
                route_filter_hit = True
                return cdf[derived_mask]

            mapped_icao = IATA_TO_ICAO_MAP.get(tok_iata)
            if mapped_icao:
                mapped_mask = icao_series == mapped_icao
                if mapped_mask.any():
                    route_filter_hit = True
                    return cdf[mapped_mask]

            iata_mask = iata_series == tok_iata
            if iata_mask.any():
                route_filter_hit = True
                return cdf[iata_mask]

        if tok_icao:
            icao_mask = icao_series == tok_icao
            if icao_mask.any():
                route_filter_hit = True
                return cdf[icao_mask]

            mapped_iata = ICAO_TO_IATA_MAP.get(tok_icao)
            if mapped_iata:
                mapped_mask = iata_series == mapped_iata
                if mapped_mask.any():
                    route_filter_hit = True
                    return cdf[mapped_mask]

        return cdf.iloc[0:0]
    
    if event in ("Arrival", "ArrivalForecast"):
        if raw_at:
            cand = match_token(cand, "To_IATA", "To_ICAO", raw_at)
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        sched_col = "ETA_UTC"

    elif event in ("Departure", "EDCT"):
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        if raw_to:
            # Only constrain on the destination if it actually matches something.
            # This allows us to still match the scheduled leg when FlightAware emails
            # mention a different arrival airport (which we flag separately as a
            # route mismatch alert).
            cand_to = match_token(cand, "To_IATA", "To_ICAO", raw_to)
            if not cand_to.empty:
                cand = cand_to
        sched_col = "ETD_UTC"
    
    elif event == "Diversion":
        # Diversions happen closer to arrival — match on ETA
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        sched_col = "ETA_UTC"
    
    else:
        # Fallback: use dep side if given
        if raw_from:
            cand = match_token(cand, "From_IATA", "From_ICAO", raw_from)
        sched_col = "ETD_UTC"
    
    

    if cand.empty:
        return None

    cand = cand.copy()

    if event_dt_utc is not None:
        cand = cand[cand[sched_col].notna()].copy()
        if cand.empty:
            return None

    if event_dt_utc is None:
        cand = cand.sort_values(sched_col)
        if len(cand) == 1:
            best = cand.iloc[0]
            return best.drop(labels=["Δ"]) if "Δ" in best else best
        return None

    cand["Δ"] = (cand[sched_col] - event_dt_utc).abs()
    cand = cand.sort_values("Δ")

    MAX_WINDOW = pd.Timedelta(hours=12) if event == "Diversion" else pd.Timedelta(hours=3)
    best = cand.iloc[0]
    best_delta = best.get("Δ")

    def _strip_delta(row: pd.Series) -> pd.Series:
        return row.drop(labels=["Δ"]) if "Δ" in row else row

    if pd.notna(best_delta) and best_delta <= MAX_WINDOW:
        return _strip_delta(best)

    if pd.isna(best_delta):
        if event_dt_utc is None:
            return _strip_delta(best)
        return None

    if len(cand) == 1:
        # When the event timestamp is unknown (or cannot be compared) fall back
        # to the lone candidate.  Otherwise require that the timestamp based
        # distance check passed; relying solely on route/tail heuristics caused
        # stale webhook events to be applied to future legs.
        if event_dt_utc is None or pd.isna(best_delta):
            return best.drop(labels=["Δ"]) if "Δ" in best else best

        sched_val = best.get(sched_col)
        sched_dt: datetime | None = None
        if isinstance(sched_val, pd.Timestamp):
            if pd.notna(sched_val):
                sched_dt = sched_val.to_pydatetime()
        elif isinstance(sched_val, datetime):
            sched_dt = sched_val
            if sched_dt.tzinfo is None:
                sched_dt = sched_dt.replace(tzinfo=timezone.utc)

        if route_filter_hit and sched_dt is not None and event_dt_utc >= sched_dt:
            return best.drop(labels=["Δ"]) if "Δ" in best else best

        return None

    return None

def select_leg_row_for_booking(booking: str | None, event: str, event_dt_utc: datetime | None) -> pd.Series | None:
    if not booking:
        return None
    subset = df_clean[df_clean["Booking"].astype(str) == str(booking)].copy()
    if subset.empty:
        return None
    if len(subset) == 1:
        return subset.iloc[0]

    if event in ("Arrival", "ArrivalForecast", "Diversion"):
        sched_col = "ETA_UTC"
    else:
        sched_col = "ETD_UTC"

    if event_dt_utc is None:
        subset = subset.sort_values(sched_col)
        return subset.iloc[0]

    subset["Δ"] = (subset[sched_col] - event_dt_utc).abs()
    subset = subset.sort_values("Δ")
    best = subset.iloc[0]
    return best.drop(labels=["Δ"]) if "Δ" in best else best

# ============================
# Controls
# ============================
_delay_threshold_secret = _resolve_secret("DELAY_THRESHOLD_MIN", "DELAY_THRESHOLD_MINUTES")
delay_threshold_min = 15
if _delay_threshold_secret not in (None, ""):
    try:
        candidate = int(_delay_threshold_secret)
    except (TypeError, ValueError):
        st.warning("Invalid DELAY_THRESHOLD_MIN value; defaulting to 15 minutes.")
    else:
        if 1 <= candidate <= 120:
            delay_threshold_min = candidate
        else:
            st.warning("DELAY_THRESHOLD_MIN must be between 1 and 120 minutes; defaulting to 15 minutes.")

# --- ASP Callsign ↔ Tail mapping -------------------------------------------
# You can optionally put this in Streamlit secrets as:
# ASP_MAP:
#   ASP574: CFSEF
#   ASP503: CFASW
#   ... (etc)
#
# If not in secrets, we fall back to the bundled defaults defined below.

DEFAULT_ASP_MAP_TEXT = """\
C-GASL\tASP816
C-FASV\tASP812
C-FLAS\tASP820
C-FJAS\tASP822
C-FASF\tASP827
C-GASE\tASP846
C-GASK\tASP839
C-GXAS\tASP826
C-GBAS\tASP875
C-FSNY\tASP858
C-FSYX\tASP844
C-FSBR\tASP814
C-FSRX\tASP864
C-FSJR\tASP877
C-FASQ\tASP821
C-FSDO\tASP836

C-FASP\tASP519
C-FASR\tASP524
C-FASW\tASP503
C-FIAS\tASP511
C-GASR\tASP510
C-GZAS\tASP508

C-FASY\tASP489
C-GASW\tASP554
C-GAAS\tASP567
C-FNAS\tASP473
C-GNAS\tASP642
C-GFFS\tASP595
C-FSFS\tASP654
C-GFSX\tASP609
C-FSFO\tASP668
C-FSNP\tASP675
C-FSQX\tASP556
C-FSFP\tASP686
C-FSEF\tASP574
C-FSDN\tASP548
C-GFSD\tASP655
C-FSUP\tASP653
C-FSRY\tASP565
C-GFSJ\tASP501
C-GIAS\tASP531
"""

def _parse_asp_map_text(txt: str) -> dict[str, str]:
    callsign_to_tail = {}
    for line in (txt or "").splitlines():
        parts = [p for p in re.split(r"[,\t ]+", line.strip()) if p]
        if len(parts) < 2: 
            continue
        tail_with_dash, asp = parts[0].upper(), parts[1].upper()
        if asp.startswith("ASP"):
            callsign_to_tail[asp] = tail_with_dash  # keep the dash
    return callsign_to_tail


# Prefer secrets if present
_secrets_map = _read_streamlit_secret("ASP_MAP")
if isinstance(_secrets_map, dict) and _secrets_map:
    ASP_MAP = {k.upper(): v.upper() for k, v in _secrets_map.items()}
else:
    ASP_MAP = _parse_asp_map_text(DEFAULT_ASP_MAP_TEXT)

def tail_from_asp(text: str) -> list[str]:
    """
    Find any ASP callsigns (ASP###/ASP####) inside text, return the list of mapped tails.
    """
    if not text:
        return []
    found = re.findall(r"\bASP\d{3,4}\b", text.upper())
    tails = []
    for asp in found:
        t = ASP_MAP.get(asp)
        if t:
            tails.append(t)
    return sorted(set(tails))


def _normalise_tail_token(value: Any) -> str:
    token = str(value or "").strip().upper()
    if not token:
        return ""
    token = token.replace(" ", "")
    if "-" in token:
        return token
    if token.startswith("C") and len(token) >= 4:
        return token[:2] + "-" + token[2:]
    return token


IDENT_TO_TAIL_MAP: dict[str, str] = {}
TAIL_TO_ASP_MAP: dict[str, str] = {}
for ident, tail in ASP_MAP.items():
    tail_norm = _normalise_tail_token(tail)
    if not tail_norm:
        continue
    ident_norm = ident.strip().upper()
    IDENT_TO_TAIL_MAP[ident_norm] = tail_norm
    TAIL_TO_ASP_MAP.setdefault(tail_norm, ident_norm)
    TAIL_TO_ASP_MAP.setdefault(tail_norm.replace("-", ""), ident_norm)


def collect_active_webhook_idents(frame: pd.DataFrame) -> list[str]:
    if frame.empty:
        return []
    idents: set[str] = set()
    aircraft_series = frame.get("Aircraft")
    if aircraft_series is None:
        return []
    for value in aircraft_series.fillna(""):
        tail_norm = _normalise_tail_token(value)
        if not tail_norm:
            continue
        ident = TAIL_TO_ASP_MAP.get(tail_norm) or TAIL_TO_ASP_MAP.get(tail_norm.replace("-", ""))
        if ident:
            idents.add(ident)
    return sorted(idents)


WEBHOOK_EVENT_MAP = {
    "out": "Departure",
    "off": "Departure",
    "departure": "Departure",
    "takeoff": "Departure",
    "on": "Arrival",
    "in": "Arrival",
    "arrival": "Arrival",
    "landed": "Arrival",
    "diverted": "Diversion",
    "diversion": "Diversion",
    "div": "Diversion",
}


def _normalise_airport_code(value: Any) -> str | None:
    token = str(value or "").strip().upper()
    return token or None


def apply_flightaware_webhook_updates(
    records: list[dict[str, Any]],
    *,
    events_map: dict[str, dict[str, dict[str, Any]]],
) -> int:
    if not records:
        return 0

    applied = 0

    def _first_valid_dt(candidates: list[tuple[str, Any]]) -> tuple[datetime | None, str | None]:
        for key, value in candidates:
            if value is None:
                continue
            if isinstance(value, Mapping):
                continue
            dt = _parse_dynamodb_timestamp(value)
            if dt:
                return dt, key
        return None, None

    for record in records:
        raw_event = str(record.get("event") or record.get("event_type") or "").strip().lower()
        event_type = WEBHOOK_EVENT_MAP.get(raw_event)
        if not event_type:
            continue

        event_dt = record.get("_event_dt")
        if not isinstance(event_dt, datetime):
            event_dt = None

        ident = str(record.get("ident") or record.get("identifier") or "").strip().upper()

        tail_candidates: list[str] = []
        raw_tail = record.get("aircraft") or record.get("tail") or record.get("registration")
        tail_norm = _normalise_tail_token(raw_tail)
        if tail_norm:
            tail_candidates.append(tail_norm)
        if ident:
            mapped_tail = IDENT_TO_TAIL_MAP.get(ident)
            if mapped_tail:
                tail_candidates.append(mapped_tail)

        tail_candidates = sorted({t for t in tail_candidates if t})

        origin = _normalise_airport_code(
            record.get("origin")
            or record.get("from")
            or record.get("departure")
            or record.get("from_airport")
        )
        destination = _normalise_airport_code(
            record.get("destination")
            or record.get("to")
            or record.get("arrival")
            or record.get("to_airport")
        )

        subj_info = {
            "event_type": event_type,
            "tail": tail_candidates[0] if tail_candidates else None,
            "from_airport": origin,
            "to_airport": destination,
            "at_airport": destination,
        }

        forecast_candidates: list[tuple[str, Any]] = [
            ("eta", record.get("eta")),
            ("eta_ts", record.get("eta_ts")),
            ("estimated_in", record.get("estimated_in")),
            ("estimated_on", record.get("estimated_on")),
        ]
        raw_payload = record.get("raw") if isinstance(record.get("raw"), Mapping) else None
        if isinstance(raw_payload, Mapping):
            forecast_candidates.append(("raw.eta", raw_payload.get("eta")))
            forecast_candidates.append(("raw.estimated_in", raw_payload.get("estimated_in")))
            forecast_candidates.append(("raw.estimated_on", raw_payload.get("estimated_on")))
            flight_payload = raw_payload.get("flight") if isinstance(raw_payload.get("flight"), Mapping) else None
            if isinstance(flight_payload, Mapping):
                for key in ("eta", "estimated_in", "estimated_on", "scheduled_in", "scheduled_on"):
                    forecast_candidates.append((f"raw.flight.{key}", flight_payload.get(key)))
        forecast_dt, forecast_source = _first_valid_dt(forecast_candidates)

        match_dt = event_dt or forecast_dt
        if match_dt is None and not tail_candidates and not origin and not destination:
            # Without any timing or routing information we cannot reliably match a leg.
            continue

        match_row = choose_booking_for_event(subj_info, tail_candidates, event_type, match_dt)
        if match_row is None:
            continue

        booking = str(match_row.get("Booking") or "")
        leg_key = str(match_row.get("_LegKey") or booking)
        if not leg_key:
            continue

        leg_events = events_map.setdefault(leg_key, {})

        if event_type in ("Arrival", "Diversion"):
            sched_raw = match_row.get("ETA_UTC")
        else:
            sched_raw = match_row.get("ETD_UTC")
        sched_dt = _clean_schedule_ts(sched_raw)

        if event_dt is not None:
            existing_payload = leg_events.get(event_type)
            if existing_payload:
                existing_dt = parse_iso_to_utc(existing_payload.get("actual_time_utc"))
                if existing_dt and existing_dt >= event_dt:
                    event_dt = None
            if event_dt is not None:
                delta_min = None
                if sched_dt is not None:
                    delta_min = int(round((event_dt - sched_dt).total_seconds() / 60.0))

                if event_type == "Arrival":
                    status_label = "🟣 ARRIVED"
                elif event_type == "Departure":
                    status_label = "🟢 DEPARTED"
                else:
                    divert_display = destination or subj_info.get("to_airport") or "—"
                    status_label = f"🔷 DIVERTED to {divert_display}"

                payload = {
                    "status": status_label,
                    "actual_time_utc": event_dt.isoformat(),
                    "delta_min": delta_min,
                    "source": "webhook",
                    "ident": ident or None,
                    "raw_event": raw_event,
                    "received_at": record.get("received_at"),
                }

                if event_type == "Diversion":
                    payload["divert_to"] = divert_display

                leg_events[event_type] = payload
                upsert_status(leg_key, event_type, status_label, payload["actual_time_utc"], delta_min)
                applied += 1

        if forecast_dt is not None:
            # Webhook payloads may include an ETA alongside departure or enroute events.
            # Apply the forecast regardless of the triggering event type so the dashboard
            # can surface the latest arrival estimate.
            existing_forecast = leg_events.get("ArrivalForecast")
            update_forecast = True
            if existing_forecast:
                existing_dt = parse_iso_to_utc(existing_forecast.get("actual_time_utc"))
                if existing_dt and existing_dt == forecast_dt:
                    update_forecast = False
            if update_forecast:
                delta_min_forecast = None
                if sched_dt is not None:
                    delta_min_forecast = int(round((forecast_dt - sched_dt).total_seconds() / 60.0))

                forecast_payload = {
                    "status": "🟦 ARRIVING SOON",
                    "actual_time_utc": forecast_dt.isoformat(),
                    "delta_min": delta_min_forecast,
                    "source": "webhook",
                    "ident": ident or None,
                    "raw_event": raw_event,
                    "received_at": record.get("received_at"),
                    "_forecast_source": forecast_source,
                }

                leg_events["ArrivalForecast"] = forecast_payload
                upsert_status(
                    leg_key,
                    "ArrivalForecast",
                    forecast_payload["status"],
                    forecast_payload["actual_time_utc"],
                    delta_min_forecast,
                )
                applied += 1

    return applied
# ---------------------------------------------------------------------------


# ============================
# Schedule data source selection
# ============================
btn_cols = st.columns([1, 3, 1])
with btn_cols[0]:
    if st.button("Reset data & clear cache"):
        for k in ("csv_bytes", "csv_name", "csv_uploaded_at", "status_updates"):
            st.session_state.pop(k, None)
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM status_events")
            conn.execute("DELETE FROM csv_store")
            conn.execute("DELETE FROM email_cursor")
            conn.execute("DELETE FROM tail_overrides")
            conn.execute("DELETE FROM fl3xx_cache")
        st.rerun()

schedule_payload: ScheduleData | None = None
schedule_metadata: dict[str, Any] = {}
fl3xx_flights_payload: list[dict[str, Any]] | None = None

config = _build_fl3xx_config_from_secrets()
if not config.auth_header and not config.api_token:
    st.error(
        "FL3XX API credentials are not configured. Provide an API token or a pre-built "
        "authorization header to load the automatic schedule."
    )
    st.info("Open the **Secrets diagnostics** panel above for a breakdown of detected FL3XX credentials.")
    with st.expander("How to supply FL3XX credentials", expanded=True):
        st.markdown(
            "- Add a `[fl3xx_api]` section to Streamlit secrets with `api_token` or `auth_header`.\n"
            "- Alternatively, set the `FL3XX_API_TOKEN` environment variable (App Runner secret).\n"
            "- Optional: set `FL3XX_AUTH_HEADER_NAME` when the API expects a different header name."
        )
        diag_rows = _fl3xx_secret_diagnostics_rows()
        if diag_rows:
            st.dataframe(pd.DataFrame(diag_rows), width="stretch")
        else:
            st.caption("No FL3XX credential hints detected in secrets or environment variables.")
    st.stop()

cache_entry = load_fl3xx_cache()

def _render_fl3xx_status(metadata: dict) -> None:
    status_bits = [
        "FL3XX flights fetched at",
        (metadata.get("fetched_at") or "unknown time"),
        "UTC.",
    ]
    if metadata.get("used_cache"):
        status_bits.append("Using cached schedule data.")
    else:
        status_bits.append("Latest API response loaded.")
    if metadata.get("changed"):
        status_bits.append("Changes detected since previous fetch.")
    else:
        status_bits.append("No schedule changes detected.")
    next_refresh = metadata.get("next_refresh_after")
    if next_refresh:
        status_bits.append(f"Next refresh after {next_refresh}.")

    crew_fetch_count = metadata.get("crew_fetch_count")
    if crew_fetch_count is not None:
        status_bits.append(f"Crew fetch attempts: {crew_fetch_count}.")

    crew_updated = metadata.get("crew_updated")
    if crew_updated is True:
        status_bits.append("Crew roster updated.")
    elif crew_updated is False:
        status_bits.append("Crew roster unchanged.")

    crew_errors = metadata.get("crew_fetch_errors")
    if crew_errors:
        status_bits.append(f"{len(crew_errors)} crew fetch error(s).")
    elif crew_errors is not None:
        status_bits.append("No crew fetch errors.")

    st.caption(" ".join(status_bits))

    if crew_errors:
        st.warning(
            "One or more crew fetch errors occurred while refreshing crew data."
        )
        with st.expander("Crew fetch error details", expanded=False):
            for error in crew_errors:
                st.write(error)

try:
    flights, api_metadata = _get_fl3xx_schedule(config=config)
    fl3xx_flights_payload = flights
    schedule_metadata = api_metadata
    schedule_payload = load_schedule("fl3xx_api", metadata={"flights": flights, **api_metadata})
    _render_fl3xx_status(api_metadata)
except Exception as exc:
    if cache_entry:
        st.warning(
            "Unable to refresh FL3XX flights. Using cached data fetched at "
            f"{cache_entry.get('fetched_at') or 'unknown time'} UTC.\n{exc}"
        )
        fallback_metadata = {
            "from_date": cache_entry.get("from_date"),
            "to_date": cache_entry.get("to_date"),
            "time_zone": "UTC",
            "value": "ALL",
            "fetched_at": cache_entry.get("fetched_at"),
            "hash": cache_entry.get("hash"),
            "used_cache": True,
            "changed": False,
            "refresh_interval_minutes": FL3XX_REFRESH_MINUTES,
            "next_refresh_after": None,
            "request_url": config.base_url,
            "request_params": {
                "from": cache_entry.get("from_date"),
                "to": cache_entry.get("to_date"),
                "timeZone": "UTC",
                "value": "ALL",
                **config.extra_params,
            },
            "crew_fetch_count": 0,
            "crew_fetch_errors": [],
            "crew_updated": False,
        }
        flights = cache_entry.get("flights", [])
        fl3xx_flights_payload = flights
        schedule_metadata = fallback_metadata
        schedule_payload = load_schedule("fl3xx_api", metadata={"flights": flights, **fallback_metadata})
        _render_fl3xx_status(fallback_metadata)
    else:
        st.error(f"Unable to load FL3XX flights: {exc}")
        st.stop()

if schedule_payload is None:
    st.error("Schedule data unavailable.")
    st.stop()

df_raw = schedule_payload.frame

# Harmonize legacy column names before validation so older CSV exports remain compatible
column_renames = {}
if "Off-Block (Sched)" not in df_raw.columns and "Off-Block (Est)" in df_raw.columns:
    column_renames["Off-Block (Est)"] = "Off-Block (Sched)"
if "On-Block (Sched)" not in df_raw.columns and "On-Block (Est)" in df_raw.columns:
    column_renames["On-Block (Est)"] = "On-Block (Sched)"
if column_renames:
    df_raw = df_raw.rename(columns=column_renames)

# ============================
# Parse & normalize
# ============================
expected_cols = [
    "Booking", "Off-Block (Sched)", "On-Block (Sched)",
    "From (ICAO)", "To (ICAO)",
    "Flight time (Est)", "PIC", "SIC",
    "Account", "Aircraft", "Aircraft Type", "Workflow"
]
missing = [c for c in expected_cols if c not in df_raw.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

df = df_raw.copy()
df["Booking"] = df["Booking"].fillna("").astype(str).str.strip()
df["Aircraft"] = df["Aircraft"].fillna("").astype(str).str.strip()
df["_OffBlock_UTC"] = pd.NaT
df["_OnBlock_UTC"] = pd.NaT
df["_FlightStatusRaw"] = ""
_tail_override_map = load_tail_overrides()
if _tail_override_map:
    df["Aircraft"] = [
        _tail_override_map.get(str(booking), tail) or tail
        for booking, tail in zip(df["Booking"], df["Aircraft"])
    ]
df["ETD_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["Off-Block (Sched)"])
df["ETA_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["On-Block (Sched)"])

df["From_ICAO"] = df["From (ICAO)"].astype(str).str.strip().str.upper().replace({"NAN": ""})
df["To_ICAO"]   = df["To (ICAO)"].astype(str).str.strip().str.upper().replace({"NAN": ""})

if "From (IATA)" in df_raw.columns:
    df["From_IATA"] = df_raw["From (IATA)"].astype(str).str.strip().str.upper()
else:
    df["From_IATA"] = df["From_ICAO"].apply(derive_iata_from_icao)

if "To (IATA)" in df_raw.columns:
    df["To_IATA"] = df_raw["To (IATA)"].astype(str).str.strip().str.upper()
else:
    df["To_IATA"] = df["To_ICAO"].apply(derive_iata_from_icao)

df["is_real_leg"] = df["Aircraft"].apply(is_real_tail)
df = df[df["is_real_leg"]].copy()

if not df.empty:
    booking_sizes = df.groupby("Booking", dropna=False)["Booking"].transform("size")
    booking_order = df.groupby("Booking", dropna=False).cumcount() + 1

    def _compose_leg_key(row_booking: str, seq: int, total: int, idx: int) -> str:
        base = (row_booking or "").strip()
        if not base:
            base = f"LEG-{idx+1}"
        if total <= 1:
            return base
        return f"{base}#L{seq}"

    df["_LegKey"] = [
        _compose_leg_key(b, seq, total, idx)
        for b, seq, total, idx in zip(
            df["Booking"], booking_order, booking_sizes, df.index
        )
    ]

    if fl3xx_flights_payload:
        actual_map: dict[str, list[dict[str, str | None]]] = defaultdict(list)
        for flight in fl3xx_flights_payload:
            if not isinstance(flight, dict):
                continue
            booking_key = (
                flight.get("bookingIdentifier")
                or flight.get("bookingReference")
                or flight.get("quoteId")
                or ""
            )
            booking_str = str(booking_key)
            off_iso = flight.get("_OffBlock_UTC") or flight.get("realDateOUT") or flight.get("realDateOut")
            on_iso = flight.get("_OnBlock_UTC") or flight.get("realDateIN") or flight.get("realDateIn")
            status_val = (flight.get("_FlightStatus") or flight.get("flightStatus") or "").strip()
            actual_map[booking_str].append({
                "off": off_iso,
                "on": on_iso,
                "status": status_val,
            })

        booking_seq = defaultdict(int)
        off_vals: list[str | None] = []
        on_vals: list[str | None] = []
        status_vals: list[str] = []

        for booking in df["Booking"]:
            seq_idx = booking_seq[booking]
            booking_seq[booking] += 1
            entries = actual_map.get(booking, [])
            off_iso = None
            on_iso = None
            status_val = ""
            if seq_idx < len(entries):
                payload = entries[seq_idx]
                off_iso = payload.get("off")
                on_iso = payload.get("on")
                status_val = (payload.get("status") or "").strip()
            off_vals.append(off_iso)
            on_vals.append(on_iso)
            status_vals.append(status_val)

        df["_OffBlock_UTC"] = pd.to_datetime(off_vals, utc=True, errors="coerce")
        df["_OnBlock_UTC"] = pd.to_datetime(on_vals, utc=True, errors="coerce")
        df["_FlightStatusRaw"] = status_vals
else:
    df["_LegKey"] = pd.Series(dtype=str, index=df.index)

if "_OffBlock_UTC" in df.columns:
    df["_OffBlock_UTC"] = pd.to_datetime(df["_OffBlock_UTC"], utc=True, errors="coerce")
else:
    df["_OffBlock_UTC"] = pd.to_datetime(pd.Series(pd.NaT, index=df.index), utc=True, errors="coerce")

if "_OnBlock_UTC" in df.columns:
    df["_OnBlock_UTC"] = pd.to_datetime(df["_OnBlock_UTC"], utc=True, errors="coerce")
else:
    df["_OnBlock_UTC"] = pd.to_datetime(pd.Series(pd.NaT, index=df.index), utc=True, errors="coerce")

df["_FlightStatusRaw"] = df.get("_FlightStatusRaw", pd.Series("", index=df.index)).fillna("").astype(str)

if _tail_override_map and not df.empty:
    df["Aircraft"] = [
        _tail_override_map.get(leg_key, _tail_override_map.get(str(booking), tail)) or tail
        for leg_key, booking, tail in zip(df["_LegKey"], df["Booking"], df["Aircraft"])
    ]

df["Type"] = df["Account"].apply(classify_account)
df["TypeBadge"] = df["Type"].apply(type_badge)

df["From"] = [display_airport(i, a) for i, a in zip(df["From_ICAO"], df["From_IATA"])]
df["To"]   = [display_airport(i, a) for i, a in zip(df["To_ICAO"], df["To_IATA"])]
df["Route"] = df["From"] + " → " + df["To"]

now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

df_clean = df.copy()

# ============================
# FlightAware webhook integration
# ============================

webhook_status_placeholder = st.empty()
webhook_diag_placeholder = st.empty()
webhook_config = build_flightaware_webhook_config()
try:
    webhook_diag_ok, webhook_diag_msg = _diag_flightaware_webhook()
except Exception as diag_exc:  # pragma: no cover - defensive fallback
    webhook_diag_ok, webhook_diag_msg = False, f"Diagnostics failed: {diag_exc}"

if webhook_diag_ok:
    webhook_diag_placeholder.success("FlightAware webhook integration detected.")
else:
    webhook_diag_placeholder.warning(
        f"FlightAware webhook integration unavailable: {webhook_diag_msg}"
    )

use_webhook_alerts = bool(webhook_config)

# ============================
# Email-driven status + enrich FA/EDCT times
# ============================
events_map = load_status_map()
if st.session_state.get("status_updates"):
    for key, upd in st.session_state["status_updates"].items():
        et = upd.get("type") or "Unknown"
        events_map.setdefault(key, {})[et] = upd

webhook_records: list[dict[str, Any]] = []
applied_webhook = 0

should_fetch_webhook = use_webhook_alerts
if should_fetch_webhook:
    webhook_idents = collect_active_webhook_idents(df_clean)
    if webhook_idents:
        try:
            webhook_records = fetch_flightaware_webhook_events(webhook_idents, webhook_config)
            if use_webhook_alerts:
                applied_webhook = apply_flightaware_webhook_updates(webhook_records, events_map=events_map)
        except Exception as exc:
            if use_webhook_alerts:
                webhook_status_placeholder.error(f"FlightAware webhook error: {exc}")
        else:
            if use_webhook_alerts:
                if applied_webhook:
                    webhook_status_placeholder.info(
                        f"FlightAware webhook applied {applied_webhook} update(s)."
                    )
                else:
                    webhook_status_placeholder.caption(
                        "FlightAware webhook returned no new updates for the current schedule."
                    )
    else:
        if use_webhook_alerts:
            webhook_status_placeholder.caption(
                "FlightAware webhook has no mapped ASP callsigns for the current schedule."
            )

def _events_for_leg(leg_key: str, booking: str) -> dict:
    rec = events_map.get(leg_key)
    if rec:
        return rec
    return events_map.get(booking, {})

def _compute_event_presence(frame: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Return boolean Series indicating which legs have departure/arrival events."""
    has_dep_flags = [
        "Departure" in _events_for_leg(leg_key, booking)
        for leg_key, booking in zip(frame["_LegKey"], frame["Booking"])
    ]
    has_arr_flags = [
        "Arrival" in _events_for_leg(leg_key, booking)
        for leg_key, booking in zip(frame["_LegKey"], frame["Booking"])
    ]
    return (
        pd.Series(has_dep_flags, index=frame.index, dtype=bool),
        pd.Series(has_arr_flags, index=frame.index, dtype=bool),
    )

def compute_status_row(leg_key, booking, dep_utc, eta_utc) -> str:
    rec = _events_for_leg(leg_key, booking)
    now = datetime.now(timezone.utc)
    thr = timedelta(minutes=int(delay_threshold_min))

    has_dep  = "Departure" in rec
    has_arr  = "Arrival" in rec
    has_div  = "Diversion" in rec

    def _clean_datetime(value):
        if value is None:
            return None
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return None

    dep_sched = _clean_datetime(dep_utc)
    eta_sched = _clean_datetime(eta_utc)

    dep_actual_raw = rec.get("Departure", {}).get("actual_time_utc")
    dep_actual = _clean_datetime(parse_iso_to_utc(dep_actual_raw)) if dep_actual_raw else None

    eta_forecast_raw = rec.get("ArrivalForecast", {}).get("actual_time_utc")
    eta_forecast = _clean_datetime(parse_iso_to_utc(eta_forecast_raw)) if eta_forecast_raw else None

    arr_actual_raw = rec.get("Arrival", {}).get("actual_time_utc")
    arr_actual = _clean_datetime(parse_iso_to_utc(arr_actual_raw)) if arr_actual_raw else None

    def _within_threshold(actual, scheduled):
        if actual is None or scheduled is None:
            return False
        return abs(actual - scheduled) <= thr

    def _is_late(actual, scheduled):
        if actual is None or scheduled is None:
            return False
        return (actual - scheduled) > thr

    def _is_early(actual, scheduled):
        if actual is None or scheduled is None:
            return False
        return (scheduled - actual) > thr

    if has_div:
        return rec["Diversion"].get("status", "🔷 DIVERTED")

    if has_arr:
        if eta_sched and arr_actual:
            if _is_late(arr_actual, eta_sched):
                return "🔴 Arrived (Delay)"
            if _is_early(arr_actual, eta_sched):
                return "🟢 Arrived (Early)"
            if _within_threshold(arr_actual, eta_sched):
                return "🟣 Arrived (On Sched)"
        return "🟣 Arrived"

    if has_dep and not has_arr:
        if eta_sched and eta_forecast and _is_late(eta_forecast, eta_sched):
            return "🟠 Delayed Arrival"
        if eta_sched and eta_forecast and _within_threshold(eta_forecast, eta_sched):
            if dep_sched and dep_actual:
                if _is_late(dep_actual, dep_sched):
                    return "🔴 Departed (Delay)"
                if _is_early(dep_actual, dep_sched):
                    return "🟢 Departed (Early)"
                if _within_threshold(dep_actual, dep_sched):
                    return "🟢 Departed (On Sched)"
        if dep_sched and dep_actual:
            if _is_late(dep_actual, dep_sched):
                return "🔴 Departed (Delay)"
            if _is_early(dep_actual, dep_sched):
                return "🟢 Departed (Early)"
        return "🟢 Departed"

    if dep_sched:
        return "🟡 SCHEDULED" if now <= dep_sched + thr else "🔴 DELAY"
    return "🟡 SCHEDULED"

df["Status"] = [
    compute_status_row(leg_key, booking, dep, eta)
    for leg_key, booking, dep, eta in zip(df["_LegKey"], df["Booking"], df["ETD_UTC"], df["ETA_UTC"])
]

# Pull persisted times
dep_actual_list, eta_fore_list, arr_actual_list, edct_list = [], [], [], []
dep_stage_list: list[str | None] = []
arr_stage_list: list[str | None] = []
route_mismatch_flags: list[bool] = []
route_mismatch_msgs: list[str] = []
def _canonical_stage(value: Any) -> str | None:
    if not value:
        return None
    token = str(value).strip().lower()
    if not token:
        return None
    stage_aliases = {
        "out": "out",
        "departure": "out",
        "off": "off",
        "takeoff": "off",
        "on": "on",
        "arrival": "in",
        "in": "in",
        "landed": "in",
    }
    return stage_aliases.get(token)


_STAGE_LABEL_MAP = {
    "out": "OUT",
    "off": "OFF",
    "on": "ON",
    "in": "IN",
}

_STAGE_COLOR_MAP = {
    "out": "#1e88e5",
    "off": "#2e7d32",
    "on": "#fb8c00",
    "in": "#6a1b9a",
}


def _format_stage_time(ts: pd.Timestamp | datetime | None, stage: str | None) -> str:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return "—"
    try:
        if pd.isna(ts):
            return "—"
    except Exception:
        pass

    if isinstance(ts, pd.Timestamp):
        ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    ts = ts.astimezone(timezone.utc)
    label = _STAGE_LABEL_MAP.get(stage or "")
    time_str = ts.strftime("%H:%MZ")
    return f"{label} · {time_str}" if label else time_str


for idx, (leg_key, booking) in enumerate(zip(df["_LegKey"], df["Booking"])):
    rec = _events_for_leg(leg_key, booking)

    dep_payload = rec.get("Departure", {})
    arr_payload = rec.get("Arrival", {})

    dep_actual_list.append(parse_iso_to_utc(dep_payload.get("actual_time_utc")))
    eta_fore_list.append(parse_iso_to_utc(rec.get("ArrivalForecast", {}).get("actual_time_utc")))
    arr_actual_list.append(parse_iso_to_utc(arr_payload.get("actual_time_utc")))
    edct_list.append(parse_iso_to_utc(rec.get("EDCT", {}).get("actual_time_utc")))

    dep_stage = _canonical_stage(dep_payload.get("raw_event"))
    arr_stage = _canonical_stage(arr_payload.get("raw_event"))
    dep_stage_list.append(dep_stage)
    arr_stage_list.append(arr_stage)

    mismatch_flag = False
    mismatch_msg = ""
    mismatch_event = rec.get("RouteMismatch")
    if mismatch_event is not None:
        payload = _parse_route_mismatch_status(mismatch_event.get("status"))
        if payload:
            sched_tokens = set()
            row = df.iloc[idx]
            sched_tokens.update(_airport_token_variants(row.get("To_ICAO")))
            sched_tokens.update(_airport_token_variants(row.get("To_IATA")))

            email_tokens = set(payload.get("email_tokens") or [])
            if not email_tokens and payload.get("email_to_raw"):
                email_tokens = _airport_token_variants(payload.get("email_to_raw"))

            if sched_tokens and email_tokens and email_tokens.isdisjoint(sched_tokens):
                mismatch_flag = True
                mismatch_msg = payload.get("email_to_raw") or (next(iter(email_tokens)) if email_tokens else "")

    route_mismatch_flags.append(mismatch_flag)
    route_mismatch_msgs.append(mismatch_msg)

# Display columns
# Takeoff (FA): show EDCT (purple) until a true Departure arrives, then overwrite with actual time
takeoff_display = []
for dep_dt, edct_dt, stage in zip(dep_actual_list, edct_list, dep_stage_list):
    if dep_dt:
        takeoff_display.append(_format_stage_time(dep_dt, stage))
    elif edct_dt:
        takeoff_display.append(f"EDCT · {edct_dt.astimezone(timezone.utc).strftime('%H:%MZ')}")
    else:
        takeoff_display.append("—")

landing_display = []
for arr_dt, stage in zip(arr_actual_list, arr_stage_list):
    if arr_dt:
        landing_display.append(_format_stage_time(arr_dt, stage))
    else:
        landing_display.append("—")

df["Takeoff (FA)"] = takeoff_display
df["ETA (FA)"]     = [fmt_dt_utc(x) for x in eta_fore_list]
df["Landing (FA)"] = landing_display

df["_DepStage"] = dep_stage_list
df["_ArrStage"] = arr_stage_list

# Hidden raw timestamps for styling/calcs (do NOT treat EDCT as actual)
df["_DepActual_ts"] = pd.to_datetime(dep_actual_list, utc=True)     # True actual OUT only
df["_ETA_FA_ts"]    = pd.to_datetime(eta_fore_list,   utc=True)
df["_ArrActual_ts"] = pd.to_datetime(arr_actual_list, utc=True)
df["_EDCT_ts"]      = pd.to_datetime(edct_list,       utc=True)

if not df.empty:
    eta_fa_series = df["_ETA_FA_ts"]
    eta_countdown_source = eta_fa_series.combine_first(df["ETA_UTC"])
    countdown_now = pd.Timestamp.now(tz=timezone.utc)
    df["Arrives In"] = (eta_countdown_source - countdown_now).apply(fmt_td)

df["_RouteMismatch"] = route_mismatch_flags
df["_RouteMismatchMsg"] = route_mismatch_msgs
for idx in df.index[df["_RouteMismatch"]]:
    msg = df.at[idx, "_RouteMismatchMsg"]
    if isinstance(msg, str) and msg:
        df.at[idx, "Route"] = f"{df.at[idx, 'Route']} · ⚠️ FA email to {msg}"

# Blank countdowns when appropriate
has_dep_series, has_arr_series = _compute_event_presence(df)

turnaround_df = compute_turnaround_windows(df)

turn_info_map = {}
downline_risk_map: dict[str, dict[str, object]] = {}
downline_risk_summary: list[dict[str, object]] = []
if not turnaround_df.empty:
    for _, turn_row in turnaround_df.iterrows():
        booking_val = turn_row.get("CurrentBooking")
        if not booking_val or pd.isna(booking_val):
            continue

        minutes_val = turn_row.get("TurnMinutes")
        if minutes_val is not None and not pd.isna(minutes_val):
            try:
                minutes_int = int(minutes_val)
            except Exception:
                minutes_int = None
        else:
            minutes_int = None

        if minutes_int is not None:
            info_text = f"{minutes_int}m"
            if minutes_int < TURNAROUND_MIN_GAP_MINUTES:
                info_text = f"⚠️ {info_text}"
        else:
            info_text = "—"

        turn_info_map[booking_val] = {
            "text": info_text,
            "minutes": minutes_int,
        }

    downline_risk_map, downline_risk_summary = build_downline_risk_map(
        turnaround_df, TURNAROUND_MIN_GAP_MINUTES
    )

df["_TurnMinutes"] = df["Booking"].map(lambda b: turn_info_map.get(b, {}).get("minutes"))
df["Turn Time"] = df["Booking"].map(lambda b: turn_info_map.get(b, {}).get("text", "—"))
df["Turn Time"] = df["Turn Time"].fillna("—")

df["_DownlineRisk"] = df["Booking"].map(
    lambda b: bool(downline_risk_map.get(str(b).strip() or ""))
)


def _format_downline_risk_text(booking_val: str) -> str:
    payload = downline_risk_map.get(str(booking_val).strip() or "")
    if not payload:
        return "—"

    minutes = payload.get("turn_minutes")
    minutes_txt = f"{int(minutes)}m" if minutes is not None and not pd.isna(minutes) else "<45m"
    source_booking = payload.get("source_booking") or "previous leg"
    window_txt = ""
    if payload.get("arrival_label") != "—" or payload.get("next_etd_label") != "—":
        window_txt = f" ({payload.get('arrival_label')} → {payload.get('next_etd_label')})"

    return f"Short turn after {source_booking}: {minutes_txt}{window_txt}".strip()


df["Downline Risk"] = df["Booking"].map(_format_downline_risk_text)

df.loc[has_dep_series, "Departs In"] = "—"
df.loc[has_arr_series, "Arrives In"] = "—"

# ============================
# Downline risk monitor
# ============================
st.markdown("### Downline risk monitor")
if downline_risk_summary:
    st.caption(
        "Next legs on the same tail with less than 45 minutes between arrival and the "
        "following departure. Grouped by local departure date (today expanded; future days collapsed)."
    )
    for entry in downline_risk_summary:
        section_label = entry.get("label") or "Upcoming departures"
        tails = entry.get("tails", [])
        expanded = not bool(entry.get("collapsed", False))
        with st.expander(section_label, expanded=expanded):
            if not tails:
                st.caption("No downline legs in this date bucket.")
                continue
            for tail_entry in tails:
                st.markdown(f"**{tail_entry.get('aircraft') or 'Unknown tail'}**")
                for leg in tail_entry.get("legs", []):
                    route_txt = f" · {leg.get('next_route')}" if leg.get("next_route") else ""
                    arrival_label = leg.get("arrival_label") or "—"
                    arrival_source = leg.get("arrival_source")
                    if arrival_source:
                        arrival_label = (
                            f"{arrival_label} ({arrival_source})" if arrival_label != "—" else arrival_source
                        )
                    next_window = f"{arrival_label} → {leg.get('next_etd_label') or '—'}"
                    minutes_txt = leg.get("turn_minutes")
                    minutes_txt = (
                        f"{int(minutes_txt)}m" if minutes_txt is not None and not pd.isna(minutes_txt) else "<45m"
                    )
                    st.markdown(
                        f"- {leg.get('next_booking') or 'Next leg'}{route_txt}: {minutes_txt} after "
                        f"{leg.get('source_booking') or 'previous leg'} ({next_window})"
                    )
else:
    st.caption("No downline legs currently flagged for short turns.")

# ============================
# Quick Filters
# ============================
st.markdown("### Quick Filters")
tails_opts = sorted(df["Aircraft"].dropna().unique().tolist())
airports_opts = sorted(pd.unique(pd.concat([df["From"].fillna("—"), df["To"].fillna("—")], ignore_index=True)).tolist())
workflows_opts = sorted(df["Workflow"].fillna("").unique().tolist())

f1, f2, f3 = st.columns([1, 1, 1])
with f1:
    tails_sel = st.multiselect("Tail(s)", tails_opts, default=[])
with f2:
    airports_sel = st.multiselect("Airport(s) (matches From OR To)", airports_opts, default=[])
with f3:
    workflows_sel = st.multiselect("Workflow(s)", workflows_opts, default=[])

if tails_sel:
    df = df[df["Aircraft"].isin(tails_sel)]
if airports_sel:
    df = df[df["From"].isin(airports_sel) | df["To"].isin(airports_sel)]
if workflows_sel:
    df = df[df["Workflow"].isin(workflows_sel)]

st.caption("Limit the view to the operational window while retaining legs that already departed.")
window_hours = st.slider(
    "Show flights departing within the next (hours)",
    min_value=1,
    max_value=48,
    value=3,
    step=1,
)

if window_hours and not df.empty:
    etd_series = pd.to_datetime(df.get("ETD_UTC"), errors="coerce", utc=True)
    cutoff_future = now_utc + pd.Timedelta(hours=int(window_hours))
    upcoming_mask = etd_series.notna() & (etd_series <= cutoff_future)
    keep_mask = has_dep_series | upcoming_mask
    df = df[keep_mask].copy()

# ============================
# Post-arrival visibility controls
# ============================
v1, v2 = st.columns([1, 1])
with v1:
    # highlight toggle removed; green overlay always applied
    auto_hide_on_block = st.checkbox("Auto-hide on block", value=True)
with v2:
    hide_hours = st.number_input(
        "Hide on block after (hours)", min_value=1, max_value=24, value=1, step=1
    )

# Hide legs that have been on block more than N hours ago (if enabled)
now_utc = datetime.now(timezone.utc)
if auto_hide_on_block:
    cutoff_hide = now_utc - pd.Timedelta(hours=int(hide_hours))
    on_block_series = pd.to_datetime(df.get("_OnBlock_UTC"), errors="coerce", utc=True)
    df = df[~(on_block_series.notna() & (on_block_series < cutoff_hide))].copy()

# (Re)compute these after filtering so masks align cleanly
has_dep_series, has_arr_series = _compute_event_presence(df)
df.loc[has_dep_series, "Departs In"] = "—"
df.loc[has_arr_series, "Arrives In"] = "—"


# ============================
# Sort, compute row/cell highlights, display
# ============================
# Keep your default chronological sort first
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()

delay_thr_td    = pd.Timedelta(minutes=int(delay_threshold_min))   # e.g., 15m
row_red_thr_td  = pd.Timedelta(minutes=max(30, int(delay_threshold_min)))  # ≥30m

now_utc = datetime.now(timezone.utc)

# Row-level operational "no-email" delays
no_dep = ~has_dep_series
dep_lateness = now_utc - df["ETD_UTC"]
row_dep_yellow = df["ETD_UTC"].notna() & no_dep & (dep_lateness > delay_thr_td) & (dep_lateness < row_red_thr_td)
row_dep_red    = df["ETD_UTC"].notna() & no_dep & (dep_lateness >= row_red_thr_td)

has_dep = has_dep_series
no_arr = ~has_arr_series
eta_baseline = df["_ETA_FA_ts"].where(df["_ETA_FA_ts"].notna(), df["ETA_UTC"])
eta_lateness = now_utc - eta_baseline
row_arr_yellow = eta_baseline.notna() & has_dep & no_arr & (eta_lateness > delay_thr_td) & (eta_lateness < row_red_thr_td)
row_arr_red    = eta_baseline.notna() & has_dep & no_arr & (eta_lateness >= row_red_thr_td)

row_yellow = row_dep_yellow | row_arr_yellow
row_red    = row_dep_red    | row_arr_red

# Cell-level red accents (do not count EDCT as actual departure)
dep_delay        = df["_DepActual_ts"] - df["ETD_UTC"]   # True takeoff - Off-Block (Sched)
eta_fa_vs_sched  = df["_ETA_FA_ts"]    - df["ETA_UTC"]   # ETA (FA) - On-Block (Sched)
arr_vs_sched     = df["_ArrActual_ts"] - df["ETA_UTC"]   # Landing (FA) - On-Block (Sched)
# Landed legs green mask (keep this near your other masks)
row_green = df["_ArrActual_ts"].notna()


cell_dep = dep_delay.notna()       & (dep_delay       > delay_thr_td)
cell_eta = eta_fa_vs_sched.notna() & (eta_fa_vs_sched > delay_thr_td)
cell_arr = arr_vs_sched.notna()    & (arr_vs_sched    > delay_thr_td)

st.subheader("Schedule")
option_cols = st.columns([1, 1, 1])
with option_cols[0]:
    show_account_column = st.checkbox(
        "Show Account column",
        value=False,
        help="Display the Account value from the active schedule in the table.",
    )
with option_cols[1]:
    show_sic_column = st.checkbox(
        "Show SIC column",
        value=False,
        help="Display the SIC value from the active schedule in the table.",
    )
with option_cols[2]:
    show_workflow_column = st.checkbox(
        "Show Workflow column",
        value=False,
        help="Display the Workflow value from the active schedule in the table.",
    )

# Placeholder so the Enhanced Flight Following section renders ahead of the main table
enhanced_ff_container = st.container()

# Compute a delay priority (2 = red, 1 = yellow, 0 = normal)
delay_priority = (row_red.astype(int) * 2 + row_yellow.astype(int))
df["_DelayPriority"] = delay_priority

# ---- Build a view that keeps REAL datetimes for sorting, but shows time-only ----
display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off Block (UTC)", "Takeoff (UTC)", "Landing (UTC)", "On Block (UTC)", "Stage Progress",
    "Off-Block (Sched)", "ETA (FA)",
    "On-Block (Sched)",
    "Departs In", "Arrives In", "Turn Time", "Downline Risk",
    "PIC", "SIC", "Workflow", "Status"
]

if show_account_column:
    try:
        insert_at = display_cols.index("Aircraft")
    except ValueError:
        insert_at = len(display_cols)
    display_cols.insert(insert_at, "Account")

if not show_sic_column:
    display_cols = [c for c in display_cols if c != "SIC"]

if not show_workflow_column:
    display_cols = [c for c in display_cols if c != "Workflow"]

view_df = df.copy()

if show_account_column and "Account" in view_df.columns:
    view_df["Account"] = view_df["Account"].map(format_account_value)

view_df["_GapRow"] = False
view_df = insert_gap_notice_rows(view_df)

# Ensure gap flag remains boolean after any transforms
if "_GapRow" in view_df.columns:
    view_df["_GapRow"] = view_df["_GapRow"].fillna(False).astype(bool)

if "_RouteMismatch" in view_df.columns:
    view_df["_RouteMismatch"] = view_df["_RouteMismatch"].fillna(False).astype(bool)

if "_RouteMismatchMsg" in view_df.columns:
    view_df["_RouteMismatchMsg"] = view_df["_RouteMismatchMsg"].fillna("")

if "_DownlineRisk" in view_df.columns:
    view_df["_DownlineRisk"] = view_df["_DownlineRisk"].fillna(False).astype(bool)

# Keep underlying dtypes as datetimes for sorting:
view_df["Off-Block (Sched)"] = view_df["ETD_UTC"]          # datetime
view_df["On-Block (Sched)"]  = view_df["ETA_UTC"]          # datetime
view_df["ETA (FA)"]          = view_df["_ETA_FA_ts"]       # datetime or NaT
view_df["Landing (FA)"]      = view_df["_ArrActual_ts"]   # datetime or NaT

# Takeoff (FA) needs "EDCT " prefix when we only have EDCT and no real OUT;
# we'll keep it as a STRING column (sorting by this one won't be chronological — others will).
def _takeoff_display(row):
    if pd.notna(row["_DepActual_ts"]):
        return _format_stage_time(row["_DepActual_ts"], row.get("_DepStage"))
    if pd.notna(row["_EDCT_ts"]):
        return "EDCT · " + row["_EDCT_ts"].strftime("%H:%MZ")
    return "—"


def _landing_display(row):
    if pd.notna(row["_ArrActual_ts"]):
        return _format_stage_time(row["_ArrActual_ts"], row.get("_ArrStage"))
    return "—"


view_df["Takeoff (FA)"] = view_df.apply(_takeoff_display, axis=1)
view_df["Landing (FA)"] = view_df.apply(_landing_display, axis=1)

view_df["Off Block (UTC)"] = view_df["_OffBlock_UTC"]
view_df["Takeoff (UTC)"]   = view_df["_DepActual_ts"]
view_df["Landing (UTC)"]   = view_df["_ArrActual_ts"]
view_df["On Block (UTC)"]  = view_df["_OnBlock_UTC"]

def _stage_badge(row):
    off_block = row.get("_OffBlock_UTC")
    if pd.isna(off_block):
        edct_ts = row.get("_EDCT_ts")
        if pd.notna(edct_ts):
            return "🟪 EDCT · " + edct_ts.strftime("%H:%MZ")

    badges: list[str] = []
    if pd.notna(off_block):
        badges.append("🟡 Off")
    if pd.notna(row.get("_DepActual_ts")):
        badges.append("🟢 Airborne")
    if pd.notna(row.get("_ArrActual_ts")):
        badges.append("🟢 Landed")
    if pd.notna(row.get("_OnBlock_UTC")):
        badges.append("🟣 On")
    return " → ".join(badges) if badges else "—"

view_df["Stage Progress"] = view_df.apply(_stage_badge, axis=1)

df_display = view_df[display_cols].copy()

# ----------------- Notify helpers used by buttons -----------------
local_tz = LOCAL_TZ

def _default_minutes_delta(row) -> int:
    if pd.notna(row["_ETA_FA_ts"]) and pd.notna(row["ETA_UTC"]):
        return int(round((row["_ETA_FA_ts"] - row["ETA_UTC"]).total_seconds() / 60.0))
    return 0

# NEW: airport-local ETA string for notifications (HHMM LT) with fallback to UTC
DEFAULT_ICAO_TZ_MAP = {
    "CYYZ": "America/Toronto",
    "CYUL": "America/Toronto",
    "CYOW": "America/Toronto",
    "CYHZ": "America/Halifax",
    "CYVR": "America/Vancouver",
    "CYYC": "America/Edmonton",
    "CYEG": "America/Edmonton",
    "CYWG": "America/Winnipeg",
    "CYQB": "America/Toronto",
    "CYXE": "America/Regina",
    "CYQR": "America/Regina",
    # add more as needed
}


ICAO_TO_IATA_MAP: dict[str, str] = {}
IATA_TO_ICAO_MAP: dict[str, str] = {}


def load_airport_metadata() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Return timezone and ICAO/IATA lookup dictionaries from the airport CSV."""

    timezone_map: dict[str, str] = DEFAULT_ICAO_TZ_MAP.copy()
    icao_to_iata: dict[str, str] = {}
    iata_to_icao: dict[str, str] = {}

    csv_path = Path(__file__).with_name("Airport TZ")
    if not csv_path.exists():
        return timezone_map, icao_to_iata, iata_to_icao

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:  # pragma: no cover - informative fallback only
        print(f"Unable to load airport metadata from {csv_path}: {exc}")
        return timezone_map, icao_to_iata, iata_to_icao

    if df.empty:
        return timezone_map, icao_to_iata, iata_to_icao

    col_map = {col.lower(): col for col in df.columns}
    icao_col = col_map.get("icao")
    tz_col = col_map.get("tz")
    iata_col = col_map.get("iata")

    if not icao_col:
        return timezone_map, icao_to_iata, iata_to_icao

    df = df.dropna(subset=[icao_col])
    if df.empty:
        return timezone_map, icao_to_iata, iata_to_icao

    df[icao_col] = df[icao_col].astype(str).str.strip().str.upper()
    df = df[df[icao_col].str.len() == 4]

    if tz_col:
        valid_timezones = set(pytz.all_timezones)
        tz_series = df[tz_col].astype(str).str.strip()
        df[tz_col] = tz_series
        tz_mask = tz_series.isin(valid_timezones)
        if tz_mask.any():
            timezone_map.update(
                df.loc[tz_mask, [icao_col, tz_col]]
                .drop_duplicates(subset=icao_col, keep="first")
                .set_index(icao_col)[tz_col]
                .to_dict()
            )

    if iata_col:
        df[iata_col] = df[iata_col].astype(str).str.strip().str.upper()
        valid_iata = df[iata_col].str.len() == 3
        if valid_iata.any():
            dedup = df.loc[valid_iata, [icao_col, iata_col]].drop_duplicates(subset=icao_col, keep="first")
            for icao, iata in dedup[[icao_col, iata_col]].itertuples(index=False, name=None):
                if icao and iata:
                    icao_to_iata.setdefault(icao, iata)
                    iata_to_icao.setdefault(iata, icao)

    return timezone_map, icao_to_iata, iata_to_icao


ICAO_TZ_MAP, ICAO_TO_IATA_MAP, IATA_TO_ICAO_MAP = load_airport_metadata()


def get_local_eta_str(row) -> str:
    base = row["_ETA_FA_ts"] if pd.notna(row["_ETA_FA_ts"]) else row["ETA_UTC"]
    if pd.isna(base):
        return ""
    icao = str(row["To_ICAO"]).upper()
    if not icao or len(icao) != 4:
        return pd.Timestamp(base).strftime("%H%M UTC")
    try:
        tzname = ICAO_TZ_MAP.get(icao)
        if not tzname:
            return pd.Timestamp(base).strftime("%H%M UTC")
        local = pytz.timezone(tzname)
        ts = pd.Timestamp(base).tz_convert(local)
        return ts.strftime("%H%M LT")
    except Exception:
        return pd.Timestamp(base).strftime("%H%M UTC")

# ---------- Styling masks + _style_ops (define before building styler) ----------
_base = view_df  # same frame used to make df_display; contains internal *_ts columns
now_utc = datetime.now(timezone.utc)

delay_thr_td    = pd.Timedelta(minutes=int(delay_threshold_min))           # e.g. 15
row_red_thr_td  = pd.Timedelta(minutes=max(30, int(delay_threshold_min)))  # 30+

# Row-level operational delays (no-email state)
no_dep = _base["_DepActual_ts"].isna()
dep_lateness = now_utc - _base["ETD_UTC"]
row_dep_yellow = _base["ETD_UTC"].notna() & no_dep & (dep_lateness > delay_thr_td) & (dep_lateness < row_red_thr_td)
row_dep_red    = _base["ETD_UTC"].notna() & no_dep & (dep_lateness >= row_red_thr_td)

has_dep = _base["_DepActual_ts"].notna()
no_arr  = _base["_ArrActual_ts"].isna()
eta_baseline = _base["_ETA_FA_ts"].where(_base["_ETA_FA_ts"].notna(), _base["ETA_UTC"])
eta_lateness = now_utc - eta_baseline
row_arr_yellow = eta_baseline.notna() & has_dep & no_arr & (eta_lateness > delay_thr_td) & (eta_lateness < row_red_thr_td)
row_arr_red    = eta_baseline.notna() & has_dep & no_arr & (eta_lateness >= row_red_thr_td)

row_yellow = (row_dep_yellow | row_arr_yellow)
row_red    = (row_dep_red    | row_arr_red)

# Cell-level variance checks
dep_delay        = _base["_DepActual_ts"] - _base["ETD_UTC"]   # Takeoff (FA) − Off-Block (Sched)
eta_fa_vs_sched  = _base["_ETA_FA_ts"]    - _base["ETA_UTC"]   # ETA(FA) − On-Block (Sched)
arr_vs_sched     = _base["_ArrActual_ts"] - _base["ETA_UTC"]   # Landing (FA) − On-Block (Sched)

if "Status" in _base.columns:
    depart_delay_status = _base["Status"].eq("🔴 Departed (Delay)")
else:
    depart_delay_status = pd.Series(False, index=_base.index)

cell_dep = (
    dep_delay.notna()
    & (dep_delay > delay_thr_td)
    & (~depart_delay_status)
)
cell_eta = eta_fa_vs_sched.notna() & (eta_fa_vs_sched > delay_thr_td)
cell_arr = arr_vs_sched.notna()    & (arr_vs_sched    > delay_thr_td)

# Landed-leg green overlay
row_green = _base["_ArrActual_ts"].notna()

# Flash landed legs that have not yet gone on blocks for 15+ minutes
landed_overdue = (
    _base["_ArrActual_ts"].notna()
    & _base["_OnBlock_UTC"].isna()
    & ((now_utc - _base["_ArrActual_ts"]) >= pd.Timedelta(minutes=15))
)

# EDCT purple (until true departure is received)
idx_edct = _base["_EDCT_ts"].notna() & _base["_DepActual_ts"].isna()

if "_TurnMinutes" in _base.columns:
    turn_warn = _base["_TurnMinutes"].notna() & (_base["_TurnMinutes"] < TURNAROUND_MIN_GAP_MINUTES)
else:
    turn_warn = pd.Series(False, index=_base.index)

def _style_ops(x: pd.DataFrame):
    styles = pd.DataFrame("", index=x.index, columns=x.columns)

    # 1) Row backgrounds: YELLOW then RED
    row_y_css = "background-color: rgba(255, 193, 7, 0.18); border-left: 6px solid #ffc107;"
    row_r_css = "background-color: rgba(255, 82, 82, 0.18); border-left: 6px solid #ff5252;"
    styles.loc[row_yellow.reindex(x.index, fill_value=False), :] = row_y_css
    styles.loc[row_red.reindex(x.index,    fill_value=False), :] = row_r_css

    # 2) GREEN overlay for landed legs (applied after Y/R so it wins at row level)
    row_g_css = "background-color: rgba(76, 175, 80, 0.18); border-left: 6px solid #4caf50;"
    styles.loc[row_green.reindex(x.index, fill_value=False), :] = row_g_css

    # 2b) FLASHING amber overlay for landed legs missing block-on times
    flash_css = "background-color: rgba(255, 193, 7, 0.18); border-left: 6px solid #f59e0b; animation: landed-on-alert 1.15s ease-in-out infinite;"
    styles.loc[landed_overdue.reindex(x.index, fill_value=False), :] = flash_css

    # 3) Pink overlay for planned inactivity gaps
    if "_GapRow" in _base.columns:
        gap_css = "background-color: rgba(255, 128, 171, 0.28); border-left: 6px solid #ff80ab; font-weight: 600;"
        gap_mask = _base["_GapRow"].reindex(x.index, fill_value=False)
        styles.loc[gap_mask, :] = gap_css

    # 4) Cell-level red accents (apply after row colors so cells stay visible even on green rows)
    cell_css = "background-color: rgba(255, 82, 82, 0.25);"
    mask_dep = cell_dep.reindex(x.index, fill_value=False)
    mask_eta = cell_eta.reindex(x.index, fill_value=False)
    mask_arr = cell_arr.reindex(x.index, fill_value=False)

    if "_DepStage" in _base.columns and "Takeoff (FA)" in x.columns:
        stage_series = _base["_DepStage"].astype(str).str.lower().replace({"nan": "", "none": ""})
        for stage_key in ("out", "off"):
            if stage_key not in _STAGE_COLOR_MAP:
                continue
            mask_stage = stage_series.eq(stage_key).reindex(x.index, fill_value=False)
            stage_css = f"color: {_STAGE_COLOR_MAP[stage_key]}; font-weight: 600;"
            styles.loc[mask_stage, "Takeoff (FA)"] = (
                styles.loc[mask_stage, "Takeoff (FA)"].fillna("") + stage_css
            )

    if "_ArrStage" in _base.columns and "Landing (FA)" in x.columns:
        stage_series = _base["_ArrStage"].astype(str).str.lower().replace({"nan": "", "none": ""})
        for stage_key in ("on", "in"):
            if stage_key not in _STAGE_COLOR_MAP:
                continue
            mask_stage = stage_series.eq(stage_key).reindex(x.index, fill_value=False)
            stage_css = f"color: {_STAGE_COLOR_MAP[stage_key]}; font-weight: 600;"
            styles.loc[mask_stage, "Landing (FA)"] = (
                styles.loc[mask_stage, "Landing (FA)"].fillna("") + stage_css
            )

    if "Takeoff (FA)" in x.columns:
        styles.loc[mask_dep, "Takeoff (FA)"] = (
            styles.loc[mask_dep, "Takeoff (FA)"].fillna("") + cell_css
        )
    if "ETA (FA)" in x.columns:
        styles.loc[mask_eta, "ETA (FA)"] = (
            styles.loc[mask_eta, "ETA (FA)"].fillna("") + cell_css
        )
    if "Landing (FA)" in x.columns:
        styles.loc[mask_arr, "Landing (FA)"] = (
            styles.loc[mask_arr, "Landing (FA)"].fillna("") + cell_css
        )

    if "Status" in x.columns:
        delay_statuses = {
            "🔴 Arrived (Delay)",
            "🟠 Delayed Arrival",
            "🔴 DELAY",
        }
        mask_status = _base["Status"].isin(delay_statuses).reindex(x.index, fill_value=False)
        styles.loc[mask_status, "Status"] = (
            styles.loc[mask_status, "Status"].fillna("") + cell_css
        )

    # 5) EDCT purple on Takeoff (FA) (applied last so it wins for that cell)
    cell_edct_css = "background-color: rgba(155, 81, 224, 0.28); border-left: 6px solid #9b51e0;"
    mask_edct = idx_edct.reindex(x.index, fill_value=False)
    if "Takeoff (FA)" in x.columns:
        styles.loc[mask_edct, "Takeoff (FA)"] = (
            styles.loc[mask_edct, "Takeoff (FA)"].fillna("") + cell_edct_css
        )

    if "Turn Time" in x.columns:
        turn_css = "background-color: rgba(255, 82, 82, 0.2); font-weight: 600;"
        mask_turn = turn_warn.reindex(x.index, fill_value=False)
        styles.loc[mask_turn, "Turn Time"] = (
            styles.loc[mask_turn, "Turn Time"].fillna("") + turn_css
        )

    if "_DownlineRisk" in _base.columns and "Downline Risk" in x.columns:
        risk_css = "background-color: rgba(255, 128, 171, 0.24); font-weight: 600; border-left: 6px solid #ec407a;"
        risk_mask = _base["_DownlineRisk"].reindex(x.index, fill_value=False)
        styles.loc[risk_mask, "Downline Risk"] = (
            styles.loc[risk_mask, "Downline Risk"].fillna("") + risk_css
        )

    if "_RouteMismatch" in _base.columns and "Route" in x.columns:
        route_css = "background-color: rgba(244, 67, 54, 0.35); color: #b71c1c; font-weight: 700;"
        route_mask = _base["_RouteMismatch"].reindex(x.index, fill_value=False)
        styles.loc[route_mask, "Route"] = (
            styles.loc[route_mask, "Route"].fillna("") + route_css
        )

    return styles
# ---------- end styling block ----------

# Time-only display, but keep sorting by underlying datetimes
fmt_map = {
    "Off-Block (Sched)": lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "On-Block (Sched)":  lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "ETA (FA)":          lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "Landing (FA)":      lambda v: v if isinstance(v, str) else (v.strftime("%H:%MZ") if pd.notna(v) else "—"),
    "Off Block (UTC)":   lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "Takeoff (UTC)":     lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "Landing (UTC)":     lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    "On Block (UTC)":    lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    # NOTE: "Takeoff (FA)" is already a string with optional EDCT prefix
}


def _render_schedule_table(df_subset: pd.DataFrame, phase: str) -> None:
    if df_subset.empty:
        st.caption("No flights in this phase right now.")
        return

    visible_columns = filtered_columns_for_phase(phase, df_subset.columns)
    view = df_subset.loc[:, visible_columns]

    styler = view.style
    if hasattr(styler, "hide_index"):
        styler = styler.hide_index()
    else:
        styler = styler.hide(axis="index")

    try:
        active_fmt_map = {col: fmt for col, fmt in fmt_map.items() if col in view.columns}
        styler = styler.apply(_style_ops, axis=None).format(active_fmt_map)
        st.dataframe(styler, width="stretch")
    except Exception:
        st.warning("Styling disabled (env compatibility). Showing plain table.")
        tmp = view.copy()
        for c in ["Off-Block (Sched)", "On-Block (Sched)", "ETA (FA)", "Landing (FA)"]:
            if c in tmp.columns:
                tmp[c] = tmp[c].apply(lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—")
        st.dataframe(tmp, width="stretch")

def _normalize_booking_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    try:
        if pd.isna(value):
            return ""
    except Exception:
        pass
    return str(value).strip()


def _booking_series(df: pd.DataFrame) -> pd.Series:
    if "Booking" in df.columns:
        source = df["Booking"]
    elif "bookingIdentifier" in df.columns:
        source = df["bookingIdentifier"]
    else:
        return pd.Series([""] * len(df), index=df.index, dtype=str)

    return source.apply(_normalize_booking_value)


with enhanced_ff_container:
    st.markdown("#### Enhanced Flight Following")

    enhanced_toggle_key = "enhanced_ff_enabled"
    enhanced_selected_key = "enhanced_ff_selected"
    enhanced_cache_key = "enhanced_ff_selected_cache"

    initial_toggle = st.session_state.get(enhanced_toggle_key, False)
    enhanced_enabled = st.checkbox(
        "Enhanced Flight Following Requested",
        value=initial_toggle,
        key=enhanced_toggle_key,
        help="Track a subset of flights that need enhanced monitoring.",
    )

    selected_ids_raw = st.session_state.get(enhanced_selected_key, [])
    if not isinstance(selected_ids_raw, list):
        try:
            selected_ids_raw = list(selected_ids_raw)
        except TypeError:
            selected_ids_raw = [selected_ids_raw]
    selected_ids = [
        _normalize_booking_value(val)
        for val in selected_ids_raw
        if _normalize_booking_value(val)
    ]

    # Normalize the stored selections before any widgets with the same key are instantiated.
    if selected_ids != selected_ids_raw or enhanced_selected_key not in st.session_state:
        st.session_state[enhanced_selected_key] = selected_ids

    cache = st.session_state.get(enhanced_cache_key, {}) or {}

    working_df = df.copy()
    working_df["Booking"] = _booking_series(working_df)
    working_df = working_df[working_df["Booking"] != ""]

    # Normalize the booking field used for display so multiselect choices and the
    # rendered table share the same, trimmed identifier values even after
    # auto-refresh.
    display_bookings = _booking_series(df_display)

    # Normalize the booking field used for display so multiselect choices and the
    # rendered table share the same, trimmed identifier values even after
    # auto-refresh.
    display_bookings = df_display["Booking"].astype(str).str.strip()

    if working_df.empty:
        if selected_ids:
            labels = {val: f"{val} · not in current schedule" for val in selected_ids}
            st.multiselect(
                "Select flights",
                options=selected_ids,
                default=selected_ids,
                key=enhanced_selected_key,
                format_func=lambda val: labels.get(val, val),
                help=(
                    "Choose one or more flights that require Enhanced Flight Following. "
                    "Selections stay visible while the schedule refreshes."
                ),
                disabled=True,
            )
            cached_rows = list(cache.values())
            if cached_rows:
                selected_df = pd.DataFrame(cached_rows)
                selected_df = selected_df.reindex(columns=df_display.columns, fill_value="")
                st.caption("Showing last known details for selected flights.")
                st.dataframe(selected_df, width="stretch")
            else:
                st.caption("Selected flights will reappear once the schedule reloads.")
        else:
            st.caption("Load a schedule to select flights for Enhanced Flight Following.")
    elif not enhanced_enabled:
        st.caption("Enhanced Flight Following is turned off.")
    else:
        booking_labels: dict[str, str] = {}
        for _, row in working_df.iterrows():
            booking = row.get("Booking", "")
            if not booking or booking in booking_labels:
                continue
            route = str(row.get("Route", "")).strip()
            if not route:
                origin = str(row.get("From", "")).strip()
                destination = str(row.get("To", "")).strip()
                if origin or destination:
                    route = f"{origin or '???'} → {destination or '???'}"
            label = f"{booking} · {route}" if route else booking
            booking_labels[str(booking)] = label

        options = list(booking_labels.keys())

        # Preserve any previously selected flights even if they are filtered out
        # of the current schedule (e.g., due to auto-hide settings).
        missing_selected = [val for val in selected_ids if val not in options]
        if missing_selected:
            for val in missing_selected:
                booking_labels[val] = f"{val} · not in current schedule"
            options.extend(missing_selected)

        if options:
            selected = st.multiselect(
                "Select flights",
                options=options,
                key=enhanced_selected_key,
                default=selected_ids,
                format_func=lambda val: booking_labels.get(val, val),
                help="Choose one or more flights that require Enhanced Flight Following.",
            )
            selected_ids = selected
        else:
            st.caption(
                "Schedule data is temporarily unavailable; keeping prior Enhanced Flight Following selections."
            )

        # Cache last known row data for selected flights so the section stays
        # populated even if filters temporarily remove them from the live view.
        for idx, row in df_display.iterrows():
            booking = display_bookings.iat[idx]
            if booking in selected_ids and booking:
                cache[booking] = row.to_dict()
        if selected_ids:
            cache = {
                booking: data for booking, data in cache.items() if booking in selected_ids
            }
        st.session_state[enhanced_cache_key] = cache

        if not selected_ids:
            st.caption("No flights selected for Enhanced Flight Following yet.")
        else:
            selected_mask = display_bookings.isin(selected_ids)
            selected_df = df_display.loc[selected_mask].copy()
            if selected_df.empty:
                cached_rows = list(cache.values())
                if cached_rows:
                    selected_df = pd.DataFrame(cached_rows)
                    selected_df = selected_df.reindex(columns=df_display.columns, fill_value="")
                    st.caption(
                        "Showing last known details for flights missing from the current schedule."
                    )
                else:
                    st.caption(
                        "Selected flights are no longer present in the current schedule."
                    )
            else:
                st.caption("Enhanced Flight Following flights")
                selected_styler = selected_df.style
                if hasattr(selected_styler, "hide_index"):
                    selected_styler = selected_styler.hide_index()
                else:  # pragma: no cover - Streamlit < 1.25 fallback
                    selected_styler = selected_styler.hide(axis="index")
                try:
                    selected_styler = selected_styler.apply(_style_ops, axis=None).format(fmt_map)
                    st.dataframe(selected_styler, width="stretch")
                except Exception:
                    st.dataframe(selected_df, width="stretch")

# Helpers for inline editing (data_editor expects naive datetimes)
def _format_editor_datetime(ts):
    """Return a pre-filled string for the inline editor (UTC, minute precision)."""
    if ts is None:
        return ""
    try:
        if pd.isna(ts):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(ts, pd.Timestamp):
        dt = ts.to_pydatetime()
    elif isinstance(ts, datetime):
        dt = ts
    else:
        return ""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc)
    dt = dt.replace(second=0, microsecond=0)
    return dt.strftime("%Y-%m-%d %H:%M")

def _coerce_reference_datetime(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(val, pd.Timestamp):
        dt = val.to_pydatetime()
    elif isinstance(val, datetime):
        dt = val
    elif isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        try:
            dt = dateparse.parse(s)
        except Exception:
            return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _from_editor_datetime(val, reference=None):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    ref_dt = _coerce_reference_datetime(reference)
    if isinstance(val, pd.Timestamp):
        dt = val.to_pydatetime()
    elif isinstance(val, datetime):
        dt = val
    elif isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        hhmm_digits = None
        if re.fullmatch(r"\d{1,4}", s):
            digits = s
            if len(digits) <= 2:
                hour = int(digits)
                minute = 0
            else:
                padded = digits.zfill(4)
                hour = int(padded[:2])
                minute = int(padded[2:])
            hhmm_digits = (hour, minute)
        else:
            colon_time = re.fullmatch(r"(\d{1,2}):(\d{2})", s)
            if colon_time:
                hhmm_digits = (int(colon_time.group(1)), int(colon_time.group(2)))
        if hhmm_digits is not None:
            hour, minute = hhmm_digits
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                return None
            base = ref_dt or datetime.now(timezone.utc)
            dt = base.replace(hour=hour, minute=minute, second=0, microsecond=0)
        else:
            default_base = ref_dt or datetime.now(timezone.utc)
            try:
                dt = dateparse.parse(s, default=default_base.replace(microsecond=0))
            except Exception:
                return None
    else:
        return None
    try:
        if pd.isna(dt):
            return None
    except (TypeError, ValueError):
        pass
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    try:
        if pd.isna(dt):
            return None
    except (TypeError, ValueError):
        pass
    return dt.replace(second=0, microsecond=0)

def _datetimes_equal(a, b):
    if a is None and b is None:
        return True
    if (a is None) != (b is None):
        return False
    return pd.Timestamp(a) == pd.Timestamp(b)

# Apply inline edits persisted via st.data_editor
def _apply_inline_editor_updates(original_df: pd.DataFrame, edited_df: pd.DataFrame, base_df: pd.DataFrame):
    if edited_df is None or original_df is None or original_df.empty:
        return

    frames = [frame for frame in (original_df, edited_df, base_df) if isinstance(frame, pd.DataFrame)]
    for frame in frames:
        if "_LegKey" not in frame.columns and "Leg Identifier" in frame.columns:
            frame["_LegKey"] = frame["Leg Identifier"]
        if "_LegKey" in frame.columns:
            frame["_LegKey"] = frame["_LegKey"].astype(str)

    key_col = "_LegKey" if all(
        "_LegKey" in frame.columns for frame in (original_df, edited_df, base_df)
    ) else "Booking"

    orig_idx = original_df.set_index(key_col)
    edited_idx = edited_df.set_index(key_col)
    if orig_idx.empty or edited_idx.empty:
        return

    base_lookup = base_df.drop_duplicates(subset=[key_col]).set_index(key_col)
    leg_lookup = None
    if "_LegKey" in base_df.columns:
        leg_lookup = base_df.drop_duplicates(subset=["_LegKey"]).set_index("_LegKey")

    time_saved = 0
    time_cleared = 0
    tail_saved = 0
    tail_cleared = 0

    for key, row in edited_idx.iterrows():
        if key not in orig_idx.index or key not in base_lookup.index:
            continue

        base_row = base_lookup.loc[key]
        orig_row = orig_idx.loc[key]

        if isinstance(base_row, pd.DataFrame):
            if "_LegKey" in row and "_LegKey" in base_row.columns:
                match = base_row[base_row["_LegKey"].astype(str) == str(row.get("_LegKey"))]
                if not match.empty:
                    base_row = match.iloc[0]
                else:
                    base_row = base_row.iloc[0]
            else:
                base_row = base_row.iloc[0]
        if isinstance(orig_row, pd.DataFrame):
            if "_LegKey" in row and "_LegKey" in orig_row.columns:
                match = orig_row[orig_row["_LegKey"].astype(str) == str(row.get("_LegKey"))]
                if not match.empty:
                    orig_row = match.iloc[0]
                else:
                    orig_row = orig_row.iloc[0]
            else:
                orig_row = orig_row.iloc[0]

        booking_val = str(base_row.get("Booking", "")) if isinstance(base_row, pd.Series) else ""
        leg_key_val = None
        if isinstance(base_row, pd.Series):
            leg_key_val = base_row.get("_LegKey")
        if leg_key_val is None and leg_lookup is not None:
            lookup_key = str(row.get("_LegKey")) if "_LegKey" in row else None
            if lookup_key and lookup_key in leg_lookup.index:
                leg_key_val = lookup_key
        if leg_key_val is None:
            leg_key_val = key
        leg_key_str = str(leg_key_val)

        # Tail overrides (expected tail registration)
        new_tail_raw = row.get("Aircraft", "")
        new_tail = "" if new_tail_raw is None else str(new_tail_raw).strip()
        if new_tail.lower() == "nan":
            new_tail = ""
        orig_tail_raw = orig_row.get("Aircraft", "")
        orig_tail = "" if orig_tail_raw is None else str(orig_tail_raw).strip()

        if new_tail != orig_tail:
            if new_tail:
                cleaned_tail = new_tail.upper()
                upsert_tail_override(leg_key_str, cleaned_tail)
                tail_saved += 1
            else:
                delete_tail_override(leg_key_str)
                tail_cleared += 1

        # Time overrides for selected columns
        for col, event_type, status_label, planned_col in [
            ("Takeoff (FA)", "Departure", "🟢 DEPARTED", "ETD_UTC"),
            ("ETA (FA)", "ArrivalForecast", "🟦 ARRIVING SOON", "ETA_UTC"),
            ("Landing (FA)", "Arrival", "🟣 ARRIVED", "ETA_UTC"),
        ]:
            planned_raw = base_row.get(planned_col)
            orig_val = _from_editor_datetime(orig_row.get(col), reference=planned_raw)
            ref_candidate = _coerce_reference_datetime(orig_row.get(col))
            if ref_candidate is None:
                ref_candidate = _coerce_reference_datetime(planned_raw)
            new_val = _from_editor_datetime(row.get(col), reference=ref_candidate)

            if _datetimes_equal(orig_val, new_val):
                continue

            if new_val is None:
                delete_status(leg_key_str, event_type)
                if st.session_state.get("status_updates", {}).get(leg_key_str, {}).get("type") == event_type:
                    st.session_state["status_updates"].pop(leg_key_str, None)
                time_cleared += 1
                continue

            planned = planned_raw
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((pd.Timestamp(new_val) - planned).total_seconds() / 60.0))

            upsert_status(leg_key_str, event_type, status_label, new_val.isoformat(), delta_min)
            st.session_state.setdefault("status_updates", {})
            st.session_state["status_updates"][leg_key_str] = {
                **st.session_state["status_updates"].get(leg_key_str, {}),
                "type": event_type,
                "actual_time_utc": new_val.isoformat(),
                "delta_min": delta_min,
                "status": status_label,
                "booking": booking_val,
            }
            time_saved += 1

    if any([time_saved, time_cleared, tail_saved, tail_cleared]):
        parts = []
        if time_saved:
            parts.append(f"saved {time_saved} time value{'s' if time_saved != 1 else ''}")
        if time_cleared:
            parts.append(f"cleared {time_cleared} time value{'s' if time_cleared != 1 else ''}")
        if tail_saved:
            parts.append(f"updated tail for {tail_saved} flight{'s' if tail_saved != 1 else ''}")
        if tail_cleared:
            parts.append(f"cleared tail override for {tail_cleared} flight{'s' if tail_cleared != 1 else ''}")

        summary = ", ".join(parts)
        st.session_state["inline_edit_toast"] = f"Inline edits applied: {summary}."
        st.rerun()

# ----------------- Schedule render with inline Notify -----------------

schedule_buckets = categorize_dataframe_by_phase(df_display)

for phase, title, description, expanded in SCHEDULE_PHASES:
    with st.expander(title, expanded=expanded):
        if description:
            st.caption(description)
        _render_schedule_table(schedule_buckets.get(phase, df_display.iloc[0:0]), phase)

# ----------------- Inline editor for manual overrides -----------------
with st.expander("Inline manual updates (UTC)", expanded=False):
    st.caption(
        "Double-click a cell to adjust actual times or expected tail registrations. "
        "Values are stored in SQLite and override email updates until a new message arrives. "
        "Enter times as HHMM (e.g., 2004), HH:MM, or 4pm — the scheduled day is used automatically."
    )

    gap_mask = view_df["_GapRow"] if "_GapRow" in view_df.columns else pd.Series(False, index=view_df.index)
    editable_source = view_df[~gap_mask].copy()

    if editable_source.empty:
        st.info("No flights available for inline edits.")
    else:
        inline_editor = editable_source[["Booking", "_LegKey", "Aircraft", "_DepActual_ts", "_ETA_FA_ts", "_ArrActual_ts"]].copy()
        inline_editor = inline_editor.rename(columns={
            "_DepActual_ts": "Takeoff (FA)",
            "_ETA_FA_ts": "ETA (FA)",
            "_ArrActual_ts": "Landing (FA)",
        })
        inline_editor["Booking"] = inline_editor["Booking"].astype(str)
        inline_editor["_LegKey"] = inline_editor["_LegKey"].astype(str)
        inline_editor["Aircraft"] = inline_editor["Aircraft"].fillna("").astype(str)
        for col in ["Takeoff (FA)", "ETA (FA)", "Landing (FA)"]:
            inline_editor[col] = inline_editor[col].apply(_format_editor_datetime)

        inline_original = inline_editor.copy(deep=True)

        edited_inline = st.data_editor(
            inline_editor,
            key="schedule_inline_editor",
            hide_index=True,
            num_rows="fixed",
            width="stretch",
            column_order=["Booking", "_LegKey", "Aircraft", "Takeoff (FA)", "ETA (FA)", "Landing (FA)"],
            column_config={
                "Booking": st.column_config.Column("Booking", disabled=True, help="Booking reference (read-only)."),
                "_LegKey": st.column_config.Column(
                    "Leg Identifier",
                    disabled=True,
                    help="Unique key for this specific leg (updates apply per leg).",
                ),
                "Aircraft": st.column_config.TextColumn(
                    "Expected Tail",
                    help="Override the tail registration shown in the schedule.",
                    max_chars=16,
                ),
                "Takeoff (FA)": st.column_config.TextColumn(
                    "Takeoff (FA)",
                    help=(
                        "FlightAware departure (UTC). Enter HHMM (24h), HH:MM, or phrases like "
                        "'4pm'. The scheduled day is used unless you specify a date."
                    ),
                    max_chars=32,
                ),
                "ETA (FA)": st.column_config.TextColumn(
                    "ETA (FA)",
                    help=(
                        "FlightAware ETA (UTC). Enter HHMM (24h), HH:MM, or natural language times. "
                        "Use words like 'tomorrow' if the arrival slips a day."
                    ),
                    max_chars=32,
                ),
                "Landing (FA)": st.column_config.TextColumn(
                    "Landing (FA)",
                    help=(
                        "FlightAware arrival (UTC). Enter HHMM (24h), HH:MM, or phrases such as '830pm'. "
                        "Leave blank to clear an override."
                    ),
                    max_chars=32,
                ),
            },
        )

        _apply_inline_editor_updates(inline_original, edited_inline, editable_source)

# -------- Quick Notify (cell-level delays only, with priority reason) --------
_show = df  # NOTE: keep original index; do NOT reset here

thr = pd.Timedelta(minutes=int(delay_threshold_min))  # same threshold as styling

# Variances vs schedule (same as styling)
dep_var = _show["_DepActual_ts"] - _show["ETD_UTC"]   # Takeoff (FA) − Off-Block (Sched)
eta_var = _show["_ETA_FA_ts"]    - _show["ETA_UTC"]   # ETA(FA) − On-Block (Sched)
arr_var = _show["_ArrActual_ts"] - _show["ETA_UTC"]   # Landing (FA) − On-Block (Sched)

if "Status" in _show.columns:
    depart_delay_status_qn = _show["Status"].eq("🔴 Departed (Delay)")
else:
    depart_delay_status_qn = pd.Series(False, index=_show.index)

cell_dep = (
    dep_var.notna()
    & (dep_var > thr)
    & (~depart_delay_status_qn)
)
cell_eta = eta_var.notna() & (eta_var > thr)
cell_arr = arr_var.notna() & (arr_var > thr)

# Only flights with any red cell accent
any_cell_delay = cell_dep | cell_eta | cell_arr
_delayed = _show[any_cell_delay].copy()  # keep original index for mask lookup

def _mins(td: pd.Timedelta | None) -> int:
    if td is None or pd.isna(td): return 0
    return int(round(td.total_seconds() / 60.0))

def _min_word(n: int) -> str:
    return "min" if abs(int(n)) == 1 else "mins"

def _top_reason(idx) -> tuple[str, int]:
    """
    Return (reason_text, minutes) using priority:
    Landing (FA) > ETA(FA) > Takeoff (FA)
    """
    if bool(cell_arr.loc[idx]):
        m = _mins(arr_var.loc[idx])
        return (f"🔴 Aircraft **arrived** {m} {_min_word(m)} later than **scheduled**.", m)
    if bool(cell_eta.loc[idx]):
        m = _mins(eta_var.loc[idx])
        return (f"🔴 Current **ETA** is {m} {_min_word(m)} past **scheduled ETA**.", m)
    if bool(cell_dep.loc[idx]):
        m = _mins(dep_var.loc[idx])
        return (f"🔴 **FlightAware takeoff** was {m} {_min_word(m)} later than **scheduled**.", m)
    return ("Delay detected by rules, details not classifiable.", 0)

with st.expander("Quick Notify (cell-level delays only)", expanded=False):
    if _delayed.empty:
        st.caption("No triggered cell-level delays right now 🎉")
    else:
        st.caption("Click to post a one-click update to Telus BC. ETA shows destination **local time**.")
        for idx, row in _delayed.iterrows():  # use original index
            booking_str = str(row["Booking"])
            info_col, reason_col, btn_col = st.columns([12, 6, 3])
            with info_col:
                etd_txt = row["ETD_UTC"].strftime("%H:%MZ") if pd.notna(row["ETD_UTC"]) else "—"
                eta_local = get_local_eta_str(row) or "—"
                reason_text, _reason_min = _top_reason(idx)
                st.markdown(
                    f"**{row['Booking']} · {row['Aircraft']}** — {row['Route']}  "
                    f"· **ETD** {etd_txt} · **ETA** {eta_local} · {row['Status']}  \n"
                    f"{reason_text}"
                )
            with reason_col:
                reason_key = f"delay_reason_{booking_str}_{idx}"
                st.text_input("Delay Reason", key=reason_key, placeholder="Enter delay details")
                notes_key = f"delay_notes_{booking_str}_{idx}"
                st.text_area(
                    "Notes",
                    key=notes_key,
                    placeholder="Optional context shared at the end of the Telus post",
                    height=80,
                )
            with btn_col:
                btn_key = f"notify_{booking_str}_{idx}"
                if st.button("📣 Notify", key=btn_key):
                    telus_hooks = _read_streamlit_secret("TELUS_WEBHOOKS")
                    if isinstance(telus_hooks, Mapping):
                        teams = list(telus_hooks.keys())
                    else:
                        teams = []
                    if not teams:
                        st.error("No TELUS teams configured in secrets.")
                    else:
                        reason_val = st.session_state.get(reason_key, "")
                        notes_val = st.session_state.get(notes_key, "")
                        ok, err = post_to_telus_team(
                            team=teams[0],
                            text=build_stateful_notify_message(
                                row,
                                delay_reason=reason_val,
                                notes=notes_val,
                            ),
                        )
                        if ok:
                            st.success(f"Notified {row['Booking']} ({row['Aircraft']})")
                        else:
                            st.error(f"Failed: {err}")

# -------- end Quick Notify panel --------



st.caption(
    "Row colors (operational): **yellow** = 15–29 min late without matching email, **red** = ≥30 min late. "
    "Cell accents: red = variance (Takeoff (FA)>Off-Block Sched, ETA(FA)>On-Block Sched, Landing (FA)>On-Block Sched). "
    "EDCT shows in purple in Takeoff (FA) until a Departure email is received."
)

# ----------------- end schedule render -----------------


# ============================
# Mailbox Polling (IMAP)
# ============================

# 1) IMAP secrets/config FIRST
IMAP_HOST = _resolve_secret("IMAP_HOST")
IMAP_USER = _resolve_secret("IMAP_USER")
IMAP_PASS = _resolve_secret("IMAP_PASS")
IMAP_FOLDER = _resolve_secret("IMAP_FOLDER", default="INBOX") or "INBOX"
IMAP_SENDER = _resolve_secret("IMAP_SENDER")  # e.g., alerts@flightaware.com
# 2) Define the polling function BEFORE the UI uses it
def imap_poll_once(max_to_process: int = 25, debug: bool = False, edct_only: bool = True) -> int:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        return 0

    M = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        # --- login + select
        try:
            M.login(IMAP_USER, IMAP_PASS)
        except imaplib.IMAP4.error as e:
            st.error(f"IMAP login failed: {e}")
            return -1

        typ, _ = M.select(IMAP_FOLDER)
        if typ != "OK":
            st.error(f"Could not open folder {IMAP_FOLDER}")
            return -1

        # --- search new UIDs
        last_uid = get_last_uid(IMAP_USER + ":" + IMAP_FOLDER)
        if IMAP_SENDER:
            typ, data = M.uid('search', None, 'FROM', f'"{IMAP_SENDER}"', f'UID {last_uid+1}:*')
            if typ != "OK" or not data or not data[0]:
                if debug:
                    st.warning(f'No matches for FROM filter "{IMAP_SENDER}". Falling back to unfiltered UID search.')
                typ, data = M.uid('search', None, f'UID {last_uid+1}:*')
        else:
            typ, data = M.uid('search', None, f'UID {last_uid+1}:*')

        if typ != "OK":
            st.error("IMAP search failed")
            return -1

        uids = [int(x) for x in (data[0].split() if data and data[0] else [])]
        if not uids:
            return 0

        # --- process emails
        applied = 0
        for uid in sorted(uids)[:max_to_process]:
            booking = None
            text = ""
            try:
                typ, msg_data = M.uid('fetch', str(uid), '(RFC822)')
                if typ != "OK" or not msg_data or not msg_data[0]:
                    set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                    continue

                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)

                subject = msg.get('Subject', '') or ''
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                            break
                    if not body:
                        for part in msg.walk():
                            if part.get_content_type().startswith("text/"):
                                body = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='ignore')
                                break
                else:
                    payload = msg.get_payload(decode=True)
                    if payload:
                        body = payload.decode(msg.get_content_charset() or 'utf-8', errors='ignore')

                text = f"{subject}\n{body}"
                now_utc = datetime.now(timezone.utc)

                subj_info = parse_subject_line(subject, now_utc)
                event = subj_info.get("event_type")

                hdr_dt = get_email_date_utc(msg)
                explicit_dt = parse_any_datetime_to_utc(text)
                body_info = parse_body_firstline(event, body, hdr_dt or now_utc)
                edct_info = parse_body_edct(body)

                if edct_only and event not in {None, "EDCT"}:
                    set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                    continue

                if event == "Diversion":
                    if body_info.get("from"):
                        subj_info.setdefault("from_airport", body_info["from"])
                    if body_info.get("divert_to"):
                        subj_info.setdefault("to_airport", body_info["divert_to"])

                # EDCT normalization
                if not event and (edct_info.get("edct_time_utc") or re.search(r"\bEDCT\b|Expected Departure Clearance Time", text, re.I)):
                    event = "EDCT"

                if edct_only and event != "EDCT":
                    set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                    continue

                # Choose timestamps
                match_dt_utc = None
                if event == "Departure":
                    match_dt_utc = (
                        body_info.get("dep_time_utc")
                        or explicit_dt
                        or subj_info.get("actual_time_utc")
                        or hdr_dt
                    )
                    actual_dt_utc = (
                        body_info.get("dep_time_utc")
                        or subj_info.get("actual_time_utc")
                        or explicit_dt
                        or hdr_dt
                        or now_utc
                    )
                elif event == "Arrival":
                    match_dt_utc = (
                        body_info.get("arr_time_utc")
                        or explicit_dt
                        or subj_info.get("actual_time_utc")
                        or hdr_dt
                    )
                    actual_dt_utc = (
                        body_info.get("arr_time_utc")
                        or subj_info.get("actual_time_utc")
                        or explicit_dt
                        or hdr_dt
                        or now_utc
                    )
                elif event == "ArrivalForecast":
                    match_dt_utc = (
                        body_info.get("eta_time_utc")
                        or explicit_dt
                        or subj_info.get("actual_time_utc")
                        or hdr_dt
                    )
                    actual_dt_utc = (
                        body_info.get("eta_time_utc")
                        or subj_info.get("actual_time_utc")
                        or explicit_dt
                        or hdr_dt
                        or now_utc
                    )
                elif event == "EDCT":
                    match_dt_utc = edct_info.get("edct_time_utc") or explicit_dt or hdr_dt
                    actual_dt_utc = (
                        edct_info.get("edct_time_utc")
                        or explicit_dt
                        or hdr_dt
                        or now_utc
                    )
                else:
                    match_dt_utc = explicit_dt or subj_info.get("actual_time_utc") or hdr_dt
                    actual_dt_utc = (
                        subj_info.get("actual_time_utc")
                        or explicit_dt
                        or hdr_dt
                        or now_utc
                    )

                # --- dashed tails (literal + ASP mapped)
                tails_dashed = []
                if subj_info.get("tail"):
                    tails_dashed.append(subj_info["tail"].upper())
                tails_dashed += re.findall(r"\bC-[A-Z0-9]{4}\b", text.upper())
                tails_dashed += tail_from_asp(text)  # must return dashed like 'C-FSEF'
                tails_dashed = sorted(set(tails_dashed))

                # Try explicit booking first
                bookings, _tails_unused, _evt_unused = extract_candidates(text)
                booking_token = bookings[0] if bookings else None

                selected_row = select_leg_row_for_booking(booking_token, event, match_dt_utc)
                if selected_row is None:
                    match_row = choose_booking_for_event(subj_info, tails_dashed, event, match_dt_utc)
                    if match_row is not None:
                        selected_row = match_row

                if selected_row is not None:
                    booking = str(selected_row.get("Booking", booking_token or ""))
                    leg_key = str(selected_row.get("_LegKey") or booking)
                else:
                    booking = booking_token
                    leg_key = str(booking) if booking else None

                if leg_key and event == "Departure" and selected_row is not None:
                    sched_tokens = set()
                    sched_tokens.update(_airport_token_variants(selected_row.get("To_ICAO")))
                    sched_tokens.update(_airport_token_variants(selected_row.get("To_IATA")))

                    email_tokens = set()
                    email_to_raw = ""
                    for candidate in [body_info.get("to"), subj_info.get("to_airport")]:
                        token = (candidate or "").strip().upper()
                        if not token:
                            continue
                        if not email_to_raw:
                            email_to_raw = token
                        email_tokens.update(_airport_token_variants(token))

                    if sched_tokens and email_tokens:
                        mismatch_ts = actual_dt_utc or hdr_dt or now_utc
                        if email_tokens.isdisjoint(sched_tokens):
                            payload = {
                                "email_to_raw": email_to_raw,
                                "email_tokens": sorted(email_tokens),
                            }
                            if mismatch_ts:
                                payload["detected_at"] = mismatch_ts.isoformat()
                            upsert_status(
                                leg_key,
                                "RouteMismatch",
                                json.dumps(payload),
                                mismatch_ts.isoformat() if mismatch_ts else None,
                                None,
                            )
                        else:
                            delete_status(leg_key, "RouteMismatch")

                if not (leg_key and event and actual_dt_utc):
                    set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                    continue

                # Planned time for delta
                planned = None
                if selected_row is not None:
                    planned = (
                        selected_row.get("ETA_UTC")
                        if event in ("Arrival", "ArrivalForecast")
                        else selected_row.get("ETD_UTC")
                    )
                delta_min = None
                if planned is not None and pd.notna(planned):
                    delta_min = int(round((actual_dt_utc - planned).total_seconds() / 60.0))

                # Status label
                if event == "Diversion":
                    status = f"🔷 DIVERTED to {subj_info.get('to_airport','—')}"
                elif event == "ArrivalForecast":
                    status = "🟦 ARRIVING SOON"
                elif event == "Arrival":
                    status = "🟣 ARRIVED"
                elif event == "Departure":
                    status = "🟢 DEPARTED"
                elif event == "EDCT":
                    status = "🟪 EDCT"
                else:
                    set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)
                    continue

                # Persist + session mirror
                st.session_state.setdefault("status_updates", {})
                st.session_state["status_updates"][leg_key] = {
                    "type": event,
                    "actual_time_utc": actual_dt_utc.isoformat(),
                    "delta_min": delta_min,
                    "status": status,
                    "booking": booking,
                }
                upsert_status(leg_key, event, status, actual_dt_utc.isoformat(), delta_min)

                # EDCT may include an expected arrival—save as forecast
                if edct_info.get("expected_arrival_utc"):
                    upsert_status(leg_key, "ArrivalForecast", "🟦 ARRIVING SOON", edct_info["expected_arrival_utc"].isoformat(), None)
                elif event == "Departure" and body_info.get("eta_time_utc"):
                    upsert_status(leg_key, "ArrivalForecast", "🟦 ARRIVING SOON", body_info["eta_time_utc"].isoformat(), None)

                applied += 1

            except Exception as e:
                if debug:
                    st.warning(f"IMAP parse error on UID {uid}: {e}")
            finally:
                # Always advance the cursor so we don't reprocess this email
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, uid)

        return applied

    finally:
        try:
            M.logout()
        except Exception:
            pass


# 3) Now the indicator that reflects the polling status
st.markdown("### Mailbox Polling")
if IMAP_SENDER:
    st.caption(f'IMAP filter: **From = {IMAP_SENDER}**')
else:
    st.caption("IMAP filter: **From = ALL senders**")

imap_poll_enabled = _secret_bool(_resolve_secret("IMAP_POLL_ENABLED"), default=True)
imap_debug = _secret_bool(_resolve_secret("IMAP_DEBUG"), default=False)

_max_per_poll_secret = _resolve_secret("IMAP_MAX_PER_POLL")
max_per_poll = 200
if _max_per_poll_secret not in (None, ""):
    try:
        max_candidate = int(_max_per_poll_secret)
    except (TypeError, ValueError):
        st.warning("Invalid IMAP_MAX_PER_POLL value; defaulting to 200 emails per poll.")
    else:
        if 10 <= max_candidate <= 1000:
            max_per_poll = max_candidate
        else:
            st.warning("IMAP_MAX_PER_POLL must be between 10 and 1000; defaulting to 200 emails per poll.")

st.caption("IMAP polling processes EDCT notifications only; FlightAware alerts are handled via webhook integration.")

if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
    st.warning("Set IMAP_HOST / IMAP_USER / IMAP_PASS (and optionally IMAP_SENDER/IMAP_FOLDER) in Streamlit secrets.")
elif not imap_poll_enabled:
    st.info("IMAP polling is disabled via the IMAP_POLL_ENABLED setting.")
else:
    try:
        applied = imap_poll_once(max_to_process=int(max_per_poll), debug=imap_debug)
    except Exception as e:
        st.error(f"IMAP polling error: {e}")
    else:
        if applied < 0:
            st.error("IMAP polling encountered an error. See messages above for details.")
        elif applied == 0:
            st.success("IMAP polling operational (no new emails detected on this refresh).")
        else:
            st.success(f"IMAP polling operational – applied {applied} update(s) this refresh.")
