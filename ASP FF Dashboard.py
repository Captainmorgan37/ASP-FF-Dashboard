# daily_ops_dashboard.py
import re
import sqlite3
from io import BytesIO
from datetime import datetime, timezone, timedelta

import pandas as pd
import streamlit as st

# ============================
# Page config
# ============================
st.set_page_config(page_title="Daily Ops Dashboard (Schedule + Status)", layout="wide")
st.title("Daily Ops Dashboard (Schedule + Status)")
st.caption(
    "Times shown in **UTC**. Some airports may be blank (non-ICAO). "
    "Rows with non-tail placeholders (e.g., “Remove OCS”, “Add EMB”) are hidden."
)

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
            event_type TEXT NOT NULL,  -- 'Departure' or 'Arrival'
            status TEXT NOT NULL,      -- '🟢 DEPARTED', '🔴 DELAY', '🟣 ARRIVED', etc.
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
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO status_events (booking, event_type, status, actual_time_utc, delta_min, updated_at)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(booking, event_type) DO UPDATE SET
                status=excluded.status,
                actual_time_utc=excluded.actual_time_utc,
                delta_min=excluded.delta_min,
                updated_at=datetime('now')
        """, (booking, event_type, status, actual_time_iso,
              int(delta_min) if delta_min is not None else None))

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

init_db()

# ============================
# Helpers
# ============================
FAKE_TAIL_PATTERNS = [
    re.compile(r"^\s*(add|remove)\b", re.I),
    re.compile(r"\b(ocs|emb)\b", re.I),
]

def is_real_tail(tail: str) -> bool:
    """Treat anything that is NOT obviously a placeholder as a real tail."""
    if not isinstance(tail, str) or not tail.strip():
        return False
    for pat in FAKE_TAIL_PATTERNS:
        if pat.search(tail):
            return False
    return True  # be permissive; only filter known placeholders

def parse_utc_ddmmyyyy_hhmmz(series: pd.Series) -> pd.Series:
    """Convert '18.09.2025 00:05z' -> tz-aware UTC timestamps."""
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
    # Your rule: "AirSprint Inc." => OCS; anything else => Owner
    if isinstance(account_val, str) and "airsprint inc" in account_val.lower():
        return "OCS"
    return "Owner"

def type_badge(flight_type: str) -> str:
    # Basic "color-coding" via emoji badges
    return {"OCS": "🟢 OCS", "Owner": "🔵 Owner"}.get(flight_type, "⚪︎")

def default_status(dep_utc: pd.Timestamp, eta_utc: pd.Timestamp) -> str:
    now = datetime.now(timezone.utc)
    if pd.notna(dep_utc) and now < dep_utc:
        return "🟡 SCHEDULED"
    if pd.notna(dep_utc) and now >= dep_utc and (pd.isna(eta_utc) or now < eta_utc):
        return "🟢 DEPARTED"
    if pd.notna(eta_utc) and now >= eta_utc:
        return "🟣 ARRIVED"
    return "🟡 SCHEDULED"

def utc_datetime_picker(label: str, default_dt_utc: datetime) -> datetime:
    """Older-Streamlit-friendly date+time picker that returns a UTC-aware datetime."""
    d = st.date_input(f"{label} — Date (UTC)", value=default_dt_utc.date(), key=f"{label}-date")
    t = st.time_input(
        f"{label} — Time (UTC)",
        value=default_dt_utc.time().replace(microsecond=0),
        key=f"{label}-time"
    )
    return datetime.combine(d, t).replace(tzinfo=timezone.utc)

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

# ============================
# File upload with persistence (session + SQLite)
# ============================
uploaded = st.file_uploader(
    "Upload your daily flights CSV (FL3XX export)",
    type=["csv"],
    key="flights_csv"
)

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
        st.rerun()  # safe in modern Streamlit; falls back internally

# Priority 1: new upload
if uploaded is not None:
    csv_bytes = uploaded.getvalue()
    st.session_state["csv_bytes"] = csv_bytes
    st.session_state["csv_name"] = uploaded.name
    st.session_state["csv_uploaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    save_csv_to_db(uploaded.name, csv_bytes)  # persist to DB
    df_raw = _load_csv_from_bytes(csv_bytes)

# Priority 2: existing session cache
elif "csv_bytes" in st.session_state:
    df_raw = _load_csv_from_bytes(st.session_state["csv_bytes"])
    st.caption(
        f"Using cached CSV: **{st.session_state.get('csv_name','flights.csv')}** "
        f"(uploaded {st.session_state.get('csv_uploaded_at','')})"
    )

# Priority 3: DB fallback (fresh session / restart / redeploy)
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

# ============================
# Parse & normalize
# ============================
expected_cols = [
    "Booking", "Off-Block (Est)", "On-Block (Est)",
    "From (ICAO)", "To (ICAO)",
    "Flight time (Est)", "PIC", "SIC",
    "Account", "Aircraft", "Aircraft Type", "Workflow"
]
missing = [c for c in expected_cols if c not in df_raw.columns]
if missing:
    st.error(f"Missing expected columns: {missing}")
    st.stop()

df = df_raw.copy()
df["ETD_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["Off-Block (Est)"])
df["ETA_UTC"] = parse_utc_ddmmyyyy_hhmmz(df["On-Block (Est)"])

# Filter out fake/non-tail rows
df["is_real_leg"] = df["Aircraft"].apply(is_real_tail)
df = df[df["is_real_leg"]].copy()

# Classify flight type (OCS vs Owner)
df["Type"] = df["Account"].apply(classify_account)
df["TypeBadge"] = df["Type"].apply(type_badge)

# Route formatting and blanks handling
df["From"] = df["From (ICAO)"].fillna("—").replace({"nan": "—"})
df["To"] = df["To (ICAO)"].fillna("—").replace({"nan": "—"})
df["Route"] = df["From"] + " → " + df["To"]

# Flight time format
df["Sched FT"] = df["Flight time (Est)"].apply(flight_time_hhmm_from_decimal)

# Relative time columns
now_utc = datetime.now(timezone.utc)
df["Departs In"] = (df["ETD_UTC"] - now_utc).apply(fmt_td)
df["Arrives In"] = (df["ETA_UTC"] - now_utc).apply(fmt_td)

# Default status
df["Status"] = [default_status(dep, eta) for dep, eta in zip(df["ETD_UTC"], df["ETA_UTC"])]

# Keep a pre-filter, cleaned copy for simulator & lookups
df_clean = df.copy()

# ============================
# Merge persisted statuses
# ============================
persisted = load_status_map()

def merged_status(booking, default_val):
    rec = persisted.get(booking, {})
    # Prefer Arrival over Departure if both exist
    if "Arrival" in rec:
        return rec["Arrival"]["status"]
    if "Departure" in rec:
        return rec["Departure"]["status"]
    return default_val

df["Status"] = [merged_status(b, s) for b, s in zip(df["Booking"], df["Status"])]

# Overlay any in-session updates (more recent than DB if present)
if "status_updates" in st.session_state and st.session_state["status_updates"]:
    su = st.session_state["status_updates"]
    df["Status"] = [su.get(b, {}).get("status", s) for b, s in zip(df["Booking"], df["Status"])]

# ============================
# Quick Filters (TAIL / AIRPORT / WORKFLOW)
# ============================
st.markdown("### Quick Filters")

tails_opts = sorted(df["Aircraft"].dropna().unique().tolist())
airports_opts = sorted(
    pd.unique(pd.concat([df["From"].fillna("—"), df["To"].fillna("—")], ignore_index=True)).tolist()
)
workflows_opts = sorted(df["Workflow"].fillna("").unique().tolist())

f1, f2, f3 = st.columns([1, 1, 1])
with f1:
    tails_sel = st.multiselect("Tail(s)", tails_opts, default=[])
with f2:
    airports_sel = st.multiselect("Airport(s) (matches From OR To)", airports_opts, default=[])
with f3:
    workflows_sel = st.multiselect("Workflow(s)", workflows_opts, default=[])

# Apply filters
if tails_sel:
    df = df[df["Aircraft"].isin(tails_sel)]
if airports_sel:
    df = df[df["From"].isin(airports_sel) | df["To"].isin(airports_sel)]
if workflows_sel:
    df = df[df["Workflow"].isin(workflows_sel)]

# Time-window filters
now_utc = datetime.now(timezone.utc)
if limit_next_hours:
    window_end = now_utc + pd.Timedelta(hours=int(next_hours))
    df = df[(df["ETD_UTC"] >= now_utc - pd.Timedelta(minutes=5)) & (df["ETD_UTC"] <= window_end)]
elif show_only_upcoming:
    df = df[df["ETD_UTC"] >= now_utc - pd.Timedelta(minutes=5)]

# Sort & display
df = df.sort_values(by=["ETD_UTC", "ETA_UTC"], ascending=[True, True]).copy()

display_cols = [
    "TypeBadge", "Booking", "Aircraft", "Aircraft Type", "Route",
    "Off-Block (Est)", "On-Block (Est)", "Departs In", "Arrives In",
    "PIC", "SIC", "Workflow", "Status"
]

st.subheader(f"Schedule  ·  {len(df)} flight(s) shown")
st.caption(f"Last updated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ')}")
st.dataframe(df[display_cols], use_container_width=True)

# ============================
# Email Updates (Simulator)
# ============================
st.markdown("### Email Updates (Simulator)")
with st.expander("Simulate a FlightBridge email alert → update status / detect delays"):
    if "status_updates" not in st.session_state:
        st.session_state.status_updates = {}  # booking -> dict

    booking_choices = df_clean["Booking"].unique().tolist()  # allow updates even if filtered out
    if booking_choices:
        sel_booking = st.selectbox("Booking to update", booking_choices)
        update_type = st.radio("Alert Type", ["Departure", "Arrival"], horizontal=True)
        actual_time_utc = utc_datetime_picker("Actual time", datetime.now(timezone.utc))

        if st.button("Apply Update"):
            row = df_clean[df_clean["Booking"] == sel_booking]
            if not row.empty:
                planned = row["ETD_UTC"].iloc[0] if update_type == "Departure" else row["ETA_UTC"].iloc[0]
                delta_min = None
                if pd.notna(planned) and isinstance(planned, pd.Timestamp):
                    delta_min = int(round((actual_time_utc - planned).total_seconds() / 60.0))

                status = "🟢 DEPARTED" if update_type == "Departure" else "🟣 ARRIVED"
                if delta_min is not None and abs(delta_min) >= int(delay_threshold_min):
                    status = "🔴 DELAY" if update_type == "Departure" else "🟠 LATE ARRIVAL"

                # Session overlay
                st.session_state.status_updates[sel_booking] = {
                    "type": update_type,
                    "actual_time_utc": actual_time_utc.isoformat(),
                    "delta_min": delta_min,
                    "status": status,
                }
                # Persist to DB
                upsert_status(
                    booking=sel_booking,
                    event_type=update_type,
                    status=status,
                    actual_time_iso=actual_time_utc.isoformat(),
                    delta_min=delta_min
                )
                st.success(f"Applied update to {sel_booking}: {status} (Δ {delta_min} min)")
            else:
                st.error("Booking not found in the current dataset.")

    if st.session_state.get("status_updates"):
        st.write("Current updates (session):")
        st.json(st.session_state.status_updates)

st.caption(
    "Next step: wire Gmail/Outlook/Power Automate to parse FlightBridge alert emails into "
    "`(booking, event, actual_time)` and call the same logic as the simulator above."
)
