# daily_ops_dashboard.py  — FF Dashboard with inline Notify + local ETA conversion

import re
import html
import json
import sqlite3
import imaplib, email
from email.utils import parsedate_to_datetime
from io import BytesIO
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st
from dateutil import parser as dateparse
from dateutil.tz import tzoffset
from pathlib import Path
import tzlocal  # for local-time HHMM in the notify message
import pytz  # NEW: for airport-local ETA conversion

# ============================
# Page config
# ============================
st.set_page_config(page_title="Daily Ops Dashboard (Schedule + Status)", layout="wide")
st.title("Daily Ops Dashboard (Schedule + Status)")
st.caption(
    "Times shown in **UTC**. Some airports may be blank (non-ICAO). "
    "Rows with non-tail placeholders (e.g., “Remove OCS”, “Add EMB”) are hidden."
)

if "inline_edit_toast" in st.session_state:
    st.success(st.session_state.pop("inline_edit_toast"))

# ============================
# Auto-refresh controls (no page reload fallback)
# ============================
ar1, ar2 = st.columns([1, 1])
with ar1:
    auto_refresh = st.checkbox("Auto-refresh", value=True, help="Re-run the app so countdowns update.")
with ar2:
    refresh_sec = st.number_input("Refresh every (sec)", min_value=5, max_value=120, value=30, step=5)

if auto_refresh:
    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=int(refresh_sec * 1000), key="ops_auto_refresh")
    except Exception:
        st.warning(
            "Auto-refresh requires the 'streamlit-autorefresh' package. "
            "Add `streamlit-autorefresh` to requirements.txt (or disable Auto-refresh)."
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

init_db()

# ============================
# Helpers
# ============================
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


def _build_delay_msg(
    tail: str,
    booking: str,
    minutes_delta: int,
    new_eta_hhmm: str,
    account: str | None = None,
    delay_reason: str | None = None,
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
    return "\n".join(lines)

def post_to_telus_team(team: str, text: str) -> tuple[bool, str]:
    url = st.secrets.get("TELUS_WEBHOOKS", {}).get(team)
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
):
    msg = _build_delay_msg(
        tail,
        booking,
        minutes_delta,
        new_eta_hhmm,
        account=account,
        delay_reason=delay_reason,
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
                "NextETDUTC": next_etd,
                "TurnDelta": gap,
                "TurnMinutes": int(round(gap.total_seconds() / 60.0)),
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values(["NextETDUTC", "Aircraft", "CurrentBooking"]).reset_index(drop=True)
    return result

def _late_early_label(delta_min: int) -> tuple[str, int]:
    # positive => late; negative => early
    label = "LATE" if delta_min >= 0 else "EARLY"
    return label, abs(int(delta_min))

def build_stateful_notify_message(row: pd.Series, delay_reason: str | None = None) -> str:
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
        return "\n".join(lines)

    # Fallback: keep current generic builder (should rarely hit with our panel filters)
    return _build_delay_msg(
        tail=tail,
        booking=booking,
        minutes_delta=int(_default_minutes_delta(row)),
        new_eta_hhmm=get_local_eta_str(row),  # ETA LT or UTC
        account=account_val,
        delay_reason=delay_reason,
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
        return dt.astimezone(timezone.utc)
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
    if len(c) == 4 and c[0] in ("C", "K"):
        return c[1:]
    return ""

def display_airport(icao: str, iata: str) -> str:
    i = (icao or "").strip().upper()
    a = (iata or "").strip().upper()
    if i and len(i) == 4:
        return i
    if a and len(a) == 3:
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
def choose_booking_for_event(subj_info: dict, tails_dashed: list[str], event: str, event_dt_utc: datetime) -> pd.Series | None:
    cand = df_clean.copy()
    if tails_dashed:
        cand = cand[cand["Aircraft"].isin(tails_dashed)]  # CSV is dashed
        if cand.empty:
            return None

    raw_at   = (subj_info.get("at_airport") or "").strip().upper()
    raw_from = (subj_info.get("from_airport") or "").strip().upper()
    raw_to   = (subj_info.get("to_airport") or "").strip().upper()

    def match_token(cdf, col_iata, col_icao, token):
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
                return cdf[derived_mask]

            iata_mask = iata_series == tok_iata
            if iata_mask.any():
                return cdf[iata_mask]

        if tok_icao:
            icao_mask = icao_series == tok_icao
            if icao_mask.any():
                return cdf[icao_mask]

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
    cand["Δ"] = (cand[sched_col] - event_dt_utc).abs()
    cand = cand.sort_values("Δ")

    MAX_WINDOW = pd.Timedelta(hours=12) if event == "Diversion" else pd.Timedelta(hours=3)
    best = cand.iloc[0]
    if best["Δ"] <= MAX_WINDOW:
        return best.drop(labels=["Δ"]) if "Δ" in best else best
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
c1, c2, c3 = st.columns([1, 1, 1])
with c1:
    show_only_upcoming = st.checkbox("Show only upcoming departures", value=True)
with c2:
    limit_next_hours = st.checkbox("Limit to next X hours", value=False)
with c3:
    next_hours = st.number_input("X hours (for filter above)", min_value=1, max_value=48, value=6)

delay_threshold_min = st.number_input("Delay threshold (minutes)", min_value=1, max_value=120, value=15)

# --- ASP Callsign ↔ Tail mapping -------------------------------------------
# You can optionally put this in Streamlit secrets as:
# ASP_MAP:
#   ASP574: CFSEF
#   ASP503: CFASW
#   ... (etc)
#
# If not in secrets, we display a text area to paste/edit the list at runtime.

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
_secrets_map = st.secrets.get("ASP_MAP", None)
if isinstance(_secrets_map, dict) and _secrets_map:
    ASP_MAP = {k.upper(): v.upper() for k, v in _secrets_map.items()}
else:
    with st.expander("Callsign ↔ Tail mapping (ASP → Tail)", expanded=False):
        map_text = st.text_area(
            "Paste mapping (Tail then ASP, separated by tab/space, one pair per line):",
            value=DEFAULT_ASP_MAP_TEXT,
            height=200,
        )
    ASP_MAP = _parse_asp_map_text(map_text)

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
# ---------------------------------------------------------------------------


# ============================
# File upload with persistence
# ============================
uploaded = st.file_uploader("Upload your daily flights CSV (FL3XX export)", type=["csv"], key="flights_csv")

def _load_csv_from_bytes(b: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(b))

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
        st.rerun()

# Priority order: upload → session → DB
if uploaded is not None:
    csv_bytes = uploaded.getvalue()
    st.session_state["csv_bytes"] = csv_bytes
    st.session_state["csv_name"] = uploaded.name
    st.session_state["csv_uploaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    save_csv_to_db(uploaded.name, csv_bytes)
    df_raw = _load_csv_from_bytes(csv_bytes)
elif "csv_bytes" in st.session_state:
    df_raw = _load_csv_from_bytes(st.session_state["csv_bytes"])
    st.caption(
        f"Using cached CSV: **{st.session_state.get('csv_name','flights.csv')}** "
        f"(uploaded {st.session_state.get('csv_uploaded_at','')})"
    )
else:
    name, content, uploaded_at = load_csv_from_db()
    if content is not None:
        st.session_state["csv_bytes"] = content
        st.session_state["csv_name"] = name or "flights.csv"
        st.session_state["csv_uploaded_at"] = uploaded_at or ""
        df_raw = _load_csv_from_bytes(content)
        st.caption(
            f"Loaded CSV from storage: **{st.session_state['csv_name']}** "
            f"(uploaded {st.session_state['csv_uploaded_at']})"
        )
    else:
        st.info("Upload today’s FL3XX flights CSV to begin.")
        st.stop()

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
else:
    df["_LegKey"] = pd.Series(dtype=str, index=df.index)

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
# Email-driven status + enrich FA/EDCT times
# ============================
events_map = load_status_map()
if st.session_state.get("status_updates"):
    for key, upd in st.session_state["status_updates"].items():
        et = upd.get("type") or "Unknown"
        events_map.setdefault(key, {})[et] = upd

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

    if has_div:
        return rec["Diversion"].get("status", "🔷 DIVERTED")
    if has_arr:
        return "🟣 ARRIVED"
    if has_dep and not has_arr:
        fore_eta = None
        if "ArrivalForecast" in rec:
            fore_eta = parse_iso_to_utc(rec["ArrivalForecast"].get("actual_time_utc"))
        eta_for_status = fore_eta or eta_utc
        if pd.notna(eta_for_status) and now > eta_for_status + thr:
            return "🟠 LATE ARRIVAL"
        return "🟢 DEPARTED"
    if pd.notna(dep_utc):
        return "🟡 SCHEDULED" if now <= dep_utc + thr else "🔴 DELAY"
    return "🟡 SCHEDULED"

df["Status"] = [
    compute_status_row(leg_key, booking, dep, eta)
    for leg_key, booking, dep, eta in zip(df["_LegKey"], df["Booking"], df["ETD_UTC"], df["ETA_UTC"])
]

# Pull persisted times
dep_actual_list, eta_fore_list, arr_actual_list, edct_list = [], [], [], []
route_mismatch_flags: list[bool] = []
route_mismatch_msgs: list[str] = []
for idx, (leg_key, booking) in enumerate(zip(df["_LegKey"], df["Booking"])):
    rec = _events_for_leg(leg_key, booking)
    dep_actual_list.append(parse_iso_to_utc(rec.get("Departure", {}).get("actual_time_utc")))
    eta_fore_list.append(parse_iso_to_utc(rec.get("ArrivalForecast", {}).get("actual_time_utc")))
    arr_actual_list.append(parse_iso_to_utc(rec.get("Arrival", {}).get("actual_time_utc")))
    edct_list.append(parse_iso_to_utc(rec.get("EDCT", {}).get("actual_time_utc")))

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
for dep_dt, edct_dt in zip(dep_actual_list, edct_list):
    if dep_dt:
        takeoff_display.append(fmt_dt_utc(dep_dt))
    elif edct_dt:
        takeoff_display.append(f"EDCT - {fmt_dt_utc(edct_dt)}")
    else:
        takeoff_display.append("—")

df["Takeoff (FA)"] = takeoff_display
df["ETA (FA)"]     = [fmt_dt_utc(x) for x in eta_fore_list]
df["Landing (FA)"] = [fmt_dt_utc(x) for x in arr_actual_list]

# Hidden raw timestamps for styling/calcs (do NOT treat EDCT as actual)
df["_DepActual_ts"] = pd.to_datetime(dep_actual_list, utc=True)     # True actual OUT only
df["_ETA_FA_ts"]    = pd.to_datetime(eta_fore_list,   utc=True)
df["_ArrActual_ts"] = pd.to_datetime(arr_actual_list, utc=True)
df["_EDCT_ts"]      = pd.to_datetime(edct_list,       utc=True)

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

df["_TurnMinutes"] = df["Booking"].map(lambda b: turn_info_map.get(b, {}).get("minutes"))
df["Turn Time"] = df["Booking"].map(lambda b: turn_info_map.get(b, {}).get("text", "—"))
df["Turn Time"] = df["Turn Time"].fillna("—")

df.loc[has_dep_series, "Departs In"] = "—"
df.loc[has_arr_series, "Arrives In"] = "—"

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

# Limit to upcoming legs / next-hour window if requested
if show_only_upcoming or limit_next_hours:
    etd_series = pd.to_datetime(df["ETD_UTC"], errors="coerce", utc=True)
    df["ETD_UTC"] = etd_series

    visibility_mask = pd.Series(True, index=df.index)

    if show_only_upcoming:
        has_dep_for_filter = has_dep_series.reindex(df.index).fillna(False)
        visibility_mask &= ~has_dep_for_filter

    if limit_next_hours:
        window_start = datetime.now(timezone.utc)
        window_end = window_start + timedelta(hours=int(next_hours))
        window_mask = etd_series.notna() & etd_series.between(window_start, window_end)
        if not show_only_upcoming:
            # Keep legs without an ETD only when we're not forcing "upcoming" only
            window_mask = window_mask | etd_series.isna()
        visibility_mask &= window_mask

    df = df[visibility_mask].copy()

# ============================
# Post-arrival visibility controls
# ============================
v1, v2 = st.columns([1, 1])
with v1:
    highlight_landed_legs = st.checkbox("Highlight landed legs", value=True)
with v2:
    auto_hide_landed = st.checkbox("Auto-hide landed", value=True)
hide_hours = st.number_input("Hide landed after (hours)", min_value=1, max_value=24, value=2, step=1)

# Hide legs that landed more than N hours ago (if enabled)
now_utc = datetime.now(timezone.utc)
if auto_hide_landed:
    cutoff_hide = now_utc - pd.Timedelta(hours=int(hide_hours))
    df = df[~(df["_ArrActual_ts"].notna() & (df["_ArrActual_ts"] < cutoff_hide))].copy()

# (Re)compute these after filtering so masks align cleanly
has_dep_series, has_arr_series = _compute_event_presence(df)
df.loc[has_dep_series, "Departs In"] = "—"
df.loc[has_arr_series, "Arrives In"] = "—"


# ============================
# Sort, compute row/cell highlights, display
# ============================
# Keep your default chronological sort first
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()
df["_orig_order"] = range(len(df))  # for stable ordering when Delayed View is off

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

# ---- New: Delayed View controls next to the title ----
head_toggle_col, head_title_col = st.columns([1.6, 8.4])
with head_toggle_col:
    delayed_view = st.checkbox("Delayed View", value=False, help="Show RED (≥30m) first, then YELLOW (15–29m).")
    show_account_column = st.checkbox(
        "Show Account column",
        value=False,
        help="Display the Account value from the uploaded CSV in the schedule table.",
    )
    show_sic_column = st.checkbox(
        "Show SIC column",
        value=True,
        help="Display the SIC value from the uploaded CSV in the schedule table.",
    )
    show_workflow_column = st.checkbox(
        "Show Workflow column",
        value=True,
        help="Display the Workflow value from the uploaded CSV in the schedule table.",
    )
with head_title_col:
    st.subheader("Schedule")

# Compute a delay priority (2 = red, 1 = yellow, 0 = normal)
delay_priority = (row_red.astype(int) * 2 + row_yellow.astype(int))
df["_DelayPriority"] = delay_priority

# If Delayed View is on: optionally filter, then sort by priority
df_view = df.copy()
if delayed_view:
    df_view = df_view[df_view["_DelayPriority"] > 0].copy()
    # Keep chronological order within each priority by using the earlier sort's order
    df_view = df_view.sort_values(
        by=["_DelayPriority", "_orig_order"],
        ascending=[False, True]
    )

# ---- Build a view that keeps REAL datetimes for sorting, but shows time-only ----
display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off-Block (Sched)", "Takeoff (FA)", "ETA (FA)",
    "On-Block (Sched)", "Landing (FA)",
    "Departs In", "Arrives In", "Turn Time",
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

view_df = (df_view if delayed_view else df).copy()

if show_account_column and "Account" in view_df.columns:
    view_df["Account"] = view_df["Account"].map(format_account_value)

view_df["_GapRow"] = False
if not delayed_view:
    view_df = insert_gap_notice_rows(view_df)
else:
    view_df = view_df.reset_index(drop=True)

# Ensure gap flag remains boolean after any transforms
if "_GapRow" in view_df.columns:
    view_df["_GapRow"] = view_df["_GapRow"].fillna(False).astype(bool)

if "_RouteMismatch" in view_df.columns:
    view_df["_RouteMismatch"] = view_df["_RouteMismatch"].fillna(False).astype(bool)

if "_RouteMismatchMsg" in view_df.columns:
    view_df["_RouteMismatchMsg"] = view_df["_RouteMismatchMsg"].fillna("")

# Keep underlying dtypes as datetimes for sorting:
view_df["Off-Block (Sched)"] = view_df["ETD_UTC"]          # datetime
view_df["On-Block (Sched)"]  = view_df["ETA_UTC"]          # datetime
view_df["ETA (FA)"]          = view_df["_ETA_FA_ts"]       # datetime or NaT
view_df["Landing (FA)"]      = view_df["_ArrActual_ts"]   # datetime or NaT

# Takeoff (FA) needs "EDCT " prefix when we only have EDCT and no real OUT;
# we'll keep it as a STRING column (sorting by this one won't be chronological — others will).
def _takeoff_display(row):
    if pd.notna(row["_DepActual_ts"]):
        return row["_DepActual_ts"].strftime("%H:%MZ")
    if pd.notna(row["_EDCT_ts"]):
        return "EDCT " + row["_EDCT_ts"].strftime("%H:%MZ")
    return "—"

view_df["Takeoff (FA)"] = view_df.apply(_takeoff_display, axis=1)

df_display = view_df[display_cols].copy()

# ----------------- Notify helpers used by buttons -----------------
local_tz = tzlocal.get_localzone()

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


def load_icao_timezone_map() -> dict[str, str]:
    """Return a mapping of ICAO -> Olson timezone string."""

    mapping: dict[str, str] = DEFAULT_ICAO_TZ_MAP.copy()
    csv_path = Path(__file__).with_name("Airport TZ")
    if not csv_path.exists():
        return mapping

    try:
        df = pd.read_csv(csv_path, usecols=["icao", "tz"])
    except Exception as exc:  # pragma: no cover - informative fallback only
        print(f"Unable to load airport timezone data from {csv_path}: {exc}")
        return mapping

    if df.empty:
        return mapping

    valid_timezones = set(pytz.all_timezones)
    df = df.dropna(subset=["icao", "tz"])
    df["icao"] = df["icao"].astype(str).str.strip().str.upper()
    df["tz"] = df["tz"].astype(str).str.strip()
    df = df[df["icao"].str.len() == 4]
    df = df[df["tz"].isin(valid_timezones)]

    if df.empty:
        return mapping

    mapping.update(df.drop_duplicates(subset="icao", keep="first").set_index("icao")["tz"].to_dict())
    return mapping


ICAO_TZ_MAP = load_icao_timezone_map()


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

cell_dep = dep_delay.notna()       & (dep_delay       > delay_thr_td)
cell_eta = eta_fa_vs_sched.notna() & (eta_fa_vs_sched > delay_thr_td)
cell_arr = arr_vs_sched.notna()    & (arr_vs_sched    > delay_thr_td)

# Landed-leg green overlay
row_green = _base["_ArrActual_ts"].notna()

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
    if 'highlight_landed_legs' in globals() and highlight_landed_legs:
        row_g_css = "background-color: rgba(76, 175, 80, 0.18); border-left: 6px solid #4caf50;"
        styles.loc[row_green.reindex(x.index, fill_value=False), :] = row_g_css

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

    styles.loc[mask_dep, "Takeoff (FA)"] = (
        styles.loc[mask_dep, "Takeoff (FA)"].fillna("") + cell_css
    )
    styles.loc[mask_eta, "ETA (FA)"] = (
        styles.loc[mask_eta, "ETA (FA)"].fillna("") + cell_css
    )
    styles.loc[mask_arr, "Landing (FA)"] = (
        styles.loc[mask_arr, "Landing (FA)"].fillna("") + cell_css
    )

    # 5) EDCT purple on Takeoff (FA) (applied last so it wins for that cell)
    cell_edct_css = "background-color: rgba(155, 81, 224, 0.28); border-left: 6px solid #9b51e0;"
    mask_edct = idx_edct.reindex(x.index, fill_value=False)
    styles.loc[mask_edct, "Takeoff (FA)"] = (
        styles.loc[mask_edct, "Takeoff (FA)"].fillna("") + cell_edct_css
    )

    if "Turn Time" in x.columns:
        turn_css = "background-color: rgba(255, 82, 82, 0.2); font-weight: 600;"
        mask_turn = turn_warn.reindex(x.index, fill_value=False)
        styles.loc[mask_turn, "Turn Time"] = (
            styles.loc[mask_turn, "Turn Time"].fillna("") + turn_css
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
    "Landing (FA)":      lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—",
    # NOTE: "Takeoff (FA)" is already a string with optional EDCT prefix
}

def render_flightaware_link(tail) -> str:
    """Return a FlightAware anchor tag for real tails, else the raw tail text."""

    if tail is None:
        return ""

    try:
        if pd.isna(tail):
            return ""
    except (TypeError, ValueError):
        pass

    tail_text = str(tail).strip()
    if not tail_text or tail_text.lower() == "nan":
        return ""

    if not is_real_tail(tail_text):
        return html.escape(tail_text)

    normalized = re.sub(r"-", "", tail_text).upper()
    href = f"https://www.flightaware.com/live/flight/{normalized}"
    anchor_text = html.escape(tail_text.upper())
    return (
        f'<a href="{href}" target="_blank" rel="noopener noreferrer">{anchor_text}</a>'
    )

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

    key_col = "_LegKey" if all(
        col in frame.columns for frame, col in (
            (original_df, "_LegKey"),
            (edited_df, "_LegKey"),
            (base_df, "_LegKey"),
        )
    ) else "Booking"

    orig_idx = original_df.set_index(key_col)
    edited_idx = edited_df.set_index(key_col)
    if orig_idx.empty or edited_idx.empty:
        return

    base_lookup = base_df.drop_duplicates(subset=[key_col]).set_index(key_col)

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
            base_row = base_row.iloc[0]
        if isinstance(orig_row, pd.DataFrame):
            orig_row = orig_row.iloc[0]

        booking_val = str(base_row.get("Booking", "")) if isinstance(base_row, pd.Series) else ""

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
                upsert_tail_override(str(key), cleaned_tail)
                tail_saved += 1
            else:
                delete_tail_override(str(key))
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
                delete_status(str(key), event_type)
                if st.session_state.get("status_updates", {}).get(key, {}).get("type") == event_type:
                    st.session_state["status_updates"].pop(key, None)
                time_cleared += 1
                continue

            planned = planned_raw
            delta_min = None
            if planned is not None and pd.notna(planned):
                delta_min = int(round((pd.Timestamp(new_val) - planned).total_seconds() / 60.0))

            upsert_status(str(key), event_type, status_label, new_val.isoformat(), delta_min)
            st.session_state.setdefault("status_updates", {})
            st.session_state["status_updates"][key] = {
                **st.session_state["status_updates"].get(key, {}),
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

styler = df_display.style
if hasattr(styler, "hide_index"):
    styler = styler.hide_index()
else:
    styler = styler.hide(axis="index")

try:
    styler = (
        styler.apply(_style_ops, axis=None)
        .format(fmt_map)
        .format({"Aircraft": render_flightaware_link}, escape=None)
    )
    try:
        if "_schedule_table_css" not in st.session_state:
            st.markdown(
                """
                <style>
                .schedule-table-container {
                    overflow-x: auto;
                    width: 100%;
                }
                .schedule-table-container table {
                    width: 100%;
                }
                .schedule-table-container th,
                .schedule-table-container td {
                    white-space: nowrap;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )
            st.session_state["_schedule_table_css"] = True
        st.markdown(
            f"<div class='schedule-table-container'>{styler.to_html()}</div>",
            unsafe_allow_html=True,
        )
    except Exception:
        st.dataframe(styler, use_container_width=True)
except Exception:
    st.warning("Styling disabled (env compatibility). Showing plain table.")
    tmp = df_display.copy()
    for c in ["Off-Block (Sched)", "On-Block (Sched)", "ETA (FA)", "Landing (FA)"]:
        tmp[c] = tmp[c].apply(lambda v: v.strftime("%H:%MZ") if pd.notna(v) else "—")
    st.dataframe(tmp, use_container_width=True)

# ----------------- Inline editor for manual overrides -----------------
with st.expander("Inline manual updates (UTC)", expanded=True):
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
            use_container_width=True,
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
_show = (df_view if delayed_view else df)  # NOTE: keep original index; do NOT reset here

thr = pd.Timedelta(minutes=int(delay_threshold_min))  # same threshold as styling

# Variances vs schedule (same as styling)
dep_var = _show["_DepActual_ts"] - _show["ETD_UTC"]   # Takeoff (FA) − Off-Block (Sched)
eta_var = _show["_ETA_FA_ts"]    - _show["ETA_UTC"]   # ETA(FA) − On-Block (Sched)
arr_var = _show["_ArrActual_ts"] - _show["ETA_UTC"]   # Landing (FA) − On-Block (Sched)

cell_dep = dep_var.notna() & (dep_var > thr)
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

with st.expander("Quick Notify (cell-level delays only)", expanded=bool(len(_delayed) > 0)):
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
            with btn_col:
                btn_key = f"notify_{booking_str}_{idx}"
                if st.button("📣 Notify", key=btn_key):
                    teams = list(st.secrets.get("TELUS_WEBHOOKS", {}).keys())
                    if not teams:
                        st.error("No TELUS teams configured in secrets.")
                    else:
                        reason_val = st.session_state.get(reason_key, "")
                        ok, err = post_to_telus_team(
                            team=teams[0],
                            text=build_stateful_notify_message(row, delay_reason=reason_val),
                        )
                        if ok:
                            st.success(f"Notified {row['Booking']} ({row['Aircraft']})")
                        else:
                            st.error(f"Failed: {err}")

# -------- end Quick Notify panel --------



if delayed_view:
    st.caption("Delayed View: showing only **RED** (≥30m) and **YELLOW** (15–29m) flights.")
else:
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
IMAP_HOST = st.secrets.get("IMAP_HOST")
IMAP_USER = st.secrets.get("IMAP_USER")
IMAP_PASS = st.secrets.get("IMAP_PASS")
IMAP_FOLDER = st.secrets.get("IMAP_FOLDER", "INBOX")
IMAP_SENDER = st.secrets.get("IMAP_SENDER")  # e.g., alerts@flightaware.com


# 2) Define the polling function BEFORE the UI uses it
def imap_poll_once(max_to_process: int = 25, debug: bool = False) -> int:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        return 0

    M = imaplib.IMAP4_SSL(IMAP_HOST)
    try:
        # --- login + select
        try:
            M.login(IMAP_USER, IMAP_PASS)
        except imaplib.IMAP4.error as e:
            st.error(f"IMAP login failed: {e}")
            return 0

        typ, _ = M.select(IMAP_FOLDER)
        if typ != "OK":
            st.error(f"Could not open folder {IMAP_FOLDER}")
            return 0

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
            return 0

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

                if event == "Diversion":
                    if body_info.get("from"):
                        subj_info.setdefault("from_airport", body_info["from"])
                    if body_info.get("divert_to"):
                        subj_info.setdefault("to_airport", body_info["divert_to"])

                # EDCT normalization
                if not event and (edct_info.get("edct_time_utc") or re.search(r"\bEDCT\b|Expected Departure Clearance Time", text, re.I)):
                    event = "EDCT"

                # Choose actual timestamp
                if event == "Departure":
                    actual_dt_utc = body_info.get("dep_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
                elif event == "Arrival":
                    actual_dt_utc = body_info.get("arr_time_utc") or explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc
                elif event == "EDCT":
                    actual_dt_utc = edct_info.get("edct_time_utc") or explicit_dt or hdr_dt or now_utc
                else:
                    actual_dt_utc = explicit_dt or subj_info.get("actual_time_utc") or hdr_dt or now_utc

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

                selected_row = select_leg_row_for_booking(booking_token, event, actual_dt_utc)
                if selected_row is None:
                    match_row = choose_booking_for_event(subj_info, tails_dashed, event, actual_dt_utc)
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


# 3) Now the UI that uses the function
st.markdown("### Mailbox Polling")
if IMAP_SENDER:
    st.caption(f'IMAP filter: **From = {IMAP_SENDER}**')
else:
    st.caption("IMAP filter: **From = ALL senders**")

enable_poll = st.checkbox(
    "Enable IMAP polling",
    value=False,
    help="Poll the mailbox for FlightAware/FlightBridge alerts and auto-apply updates.",
)

if enable_poll:
    if not (IMAP_HOST and IMAP_USER and IMAP_PASS):
        st.warning("Set IMAP_HOST / IMAP_USER / IMAP_PASS (and optionally IMAP_SENDER/IMAP_FOLDER) in Streamlit secrets.")
    else:
        c1, c2, c3 = st.columns([1, 1, 1])
        with c1:
            debug_poll = st.checkbox("Debug IMAP (verbose)", value=False)
        with c2:
            poll_on_refresh = st.checkbox("Poll automatically on refresh", value=True)
        with c3:
            if st.button("Reset IMAP cursor only", key="reset_imap_cursor", help="Sets the last processed UID to 0; saved statuses remain."):
                set_last_uid(IMAP_USER + ":" + IMAP_FOLDER, 0)
                st.success("IMAP cursor reset to 0 (statuses preserved).")

        max_per_poll = st.number_input("Max emails per poll", min_value=10, max_value=1000, value=200, step=10)

        if st.button("Poll now", key="poll_now"):
            applied = imap_poll_once(max_to_process=int(max_per_poll), debug=debug_poll)
            st.success(f"Applied {applied} update(s) from mailbox.")

        if poll_on_refresh:
            try:
                applied = imap_poll_once(max_to_process=int(max_per_poll), debug=debug_poll)
                if applied:
                    st.info(f"Auto-poll applied {applied} update(s).")
            except Exception as e:
                st.error(f"Auto-poll error: {e}")
